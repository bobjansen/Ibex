// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "execution_profile_internal.hpp"

namespace {

using namespace ibex;

TEST_CASE("operator profiling records pulls, rows, and pooled work", "[runtime][profile]") {
    auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/2,
                                                                    /*report=*/false);
    runtime::Table table;
    table.add_column("x", Column<std::int64_t>{1, 2, 3});
    ir::ScanNode scan{ir::NodeId{42}, "trades"};
    auto op = runtime::profile_operator(std::make_unique<runtime::TableSourceOperator>(table),
                                        profile, scan);

    REQUIRE(op->next().value().has_value());
    REQUIRE_FALSE(op->next().value().has_value());

    auto* stage = profile->stage("test pool stage");
    {
        runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
        runtime::WorkerPool pool{2};
        auto batch = pool.submit(2, [](std::size_t) {
            // Give the steady clock a non-zero interval without making the test
            // depend on a particular scheduler or duration.
            const auto until = std::chrono::steady_clock::now() + std::chrono::microseconds(50);
            while (std::chrono::steady_clock::now() < until) {
            }
        });
        batch.wait();
    }

    const auto rows = profile->snapshot();
    const auto scan_row = std::ranges::find_if(
        rows, [](const auto& row) { return row.node_id == 42 && row.label == "scan trades"; });
    REQUIRE(scan_row != rows.end());
    CHECK(scan_row->calls == 2);
    CHECK(scan_row->chunks == 1);
    CHECK(scan_row->rows == 3);
    CHECK(scan_row->span_ns > 0);

    const auto pool_row =
        std::ranges::find_if(rows, [](const auto& row) { return row.label == "test pool stage"; });
    REQUIRE(pool_row != rows.end());
    CHECK(pool_row->pool_tasks == 2);
    CHECK(pool_row->pool_work_ns > 0);

    // The stage that submitted work has occupancy; the scan, which submitted
    // none, has exactly zero. That is the distinction the number exists to draw.
    CHECK(runtime::profile_row_occupancy(*pool_row, 2) > 0.0);
    CHECK(runtime::profile_row_occupancy(*scan_row, 2) == 0.0);
}

TEST_CASE("profile summary measures the serial fraction from exclusive time",
          "[runtime][profile]") {
    // Spans are deliberately larger than the self times and would overlap if
    // summed — the summary must not use them.
    //
    // The serial split used to be the per-operator binary "did this operator
    // draw any worker help at all", which credited a fanning-out operator with
    // ZERO serial time however much of its self time was a prefix sum, a
    // first-occurrence merge, or a park in `wait()`. It is now
    // `self - barrier_wait`, per operator, so partial parallelism reads as
    // partial.
    const auto make_row = [](std::string label, std::uint64_t self_ns, std::uint64_t span_ns,
                             std::uint64_t pool_ns, std::uint64_t barrier_wait_ns = 0,
                             std::uint64_t barriers = 0) {
        runtime::ExecutionProfileSnapshotRow row;
        row.label = std::move(label);
        row.next_self_ns = self_ns;
        row.span_ns = span_ns;
        row.pool_work_ns = pool_ns;
        row.barrier_wait_ns = barrier_wait_ns;
        row.barriers = barriers;
        return row;
    };
    std::vector<runtime::ExecutionProfileSnapshotRow> rows;
    // Fanned out, and spent 2ms of its 3ms of self time parked at two barriers.
    rows.push_back(make_row("helped", 3'000'000, 20'000'000, 8'000'000, 2'000'000, 2));
    rows.push_back(make_row("serial", 1'000'000, 20'000'000, 0));

    const auto summary = runtime::summarize_execution_profile(rows, /*wall_ms=*/10.0,
                                                              /*workers=*/4);
    CHECK(summary.self_ms == 4.0);  // 3ms + 1ms, exclusive, no double count
    // 1ms of genuinely serial work inside "helped", plus all of "serial".
    // The old accounting reported 1.0 here and hid the first millisecond.
    CHECK(summary.serial_self_ms == 2.0);
    CHECK(summary.barrier_wait_ms == 2.0);
    CHECK(summary.barriers == 2);
    CHECK(summary.serial_fraction == 0.5);
    CHECK(summary.amdahl_ceiling == 2.0);  // 1 / 0.5
    CHECK(summary.pool_work_ms == 8.0);
    CHECK(summary.occupancy == 0.2);  // 8ms of a 10ms x 4-worker budget
}

TEST_CASE("profile summary separates barrier parking from serial work", "[runtime][profile]") {
    // The distinction the scheduler decision rests on: two operators with
    // identical self time and identical worker help, differing only in whether
    // that self time was spent waiting or working. A summary that cannot tell
    // them apart cannot say whether a work-participating join has anything to
    // reclaim.
    const auto row = [](std::uint64_t self_ns, std::uint64_t barrier_wait_ns) {
        runtime::ExecutionProfileSnapshotRow r;
        r.next_self_ns = self_ns;
        r.span_ns = self_ns;
        r.pool_work_ns = 10'000'000;
        r.barrier_wait_ns = barrier_wait_ns;
        r.barriers = 1;
        return r;
    };

    const auto parked = runtime::summarize_execution_profile({row(4'000'000, 4'000'000)}, 4.0, 4);
    CHECK(parked.serial_self_ms == 0.0);  // all wait, no serial work
    CHECK(parked.barrier_wait_ms == 4.0);
    CHECK(parked.serial_fraction == 0.0);

    const auto working = runtime::summarize_execution_profile({row(4'000'000, 0)}, 4.0, 4);
    CHECK(working.serial_self_ms == 4.0);  // all serial work, no wait
    CHECK(working.barrier_wait_ms == 0.0);
    CHECK(working.serial_fraction == 1.0);
}

TEST_CASE("profile summary clamps a barrier wait that exceeds its scope", "[runtime][profile]") {
    // Self time and barrier wait are sampled by different clocks, and a scope
    // that ends mid-wait could otherwise drive the subtraction negative — which
    // on unsigned arithmetic would not read as "slightly off" but as an
    // enormous serial fraction.
    runtime::ExecutionProfileSnapshotRow row;
    row.next_self_ns = 1'000'000;
    row.span_ns = 1'000'000;
    row.pool_work_ns = 5'000'000;
    row.barrier_wait_ns = 3'000'000;  // longer than the scope that contains it
    row.barriers = 1;

    const auto summary = runtime::summarize_execution_profile({row}, 1.0, 4);
    CHECK(summary.serial_self_ms == 0.0);
    CHECK(summary.barrier_wait_ms == 1.0);  // clamped to the self time
    CHECK(summary.serial_fraction == 0.0);
}

TEST_CASE("profile summary tolerates an empty profile and a zero budget", "[runtime][profile]") {
    const auto empty = runtime::summarize_execution_profile({}, 0.0, 0);
    CHECK(empty.self_ms == 0.0);
    CHECK(empty.serial_fraction == 0.0);
    CHECK(empty.occupancy == 0.0);
    CHECK(runtime::profile_row_occupancy({}, 0) == 0.0);
}

TEST_CASE("a stage thread's work is not charged to the caller", "[runtime][profile]") {
    // The bug this closes: `PipelinedStageOperator`'s producer is not a pool
    // thread, so `on_worker_pool_thread()` is false there and its scopes landed
    // in `next_self_ns` — main-thread self time — while running CONCURRENTLY
    // with the real main thread. That made `self_ms` exceed `wall_ms` on every
    // query that stages a breaker, and inflated the serial fraction that the
    // scheduler decision is read from by >=77ms across PDS-H SF-1.
    auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/4,
                                                                    /*report=*/false);
    auto* stage_entry = profile->stage("staged");

    std::thread producer([&] {
        const runtime::StageThreadScope stage_thread;
        const runtime::ExecutionProfileScope scope(stage_entry, runtime::ProfilePhase::Next);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    });
    producer.join();

    const auto rows = profile->snapshot();
    const auto row = std::ranges::find_if(rows, [](const auto& e) { return e.label == "staged"; });
    REQUIRE(row != rows.end());
    CHECK(row->stage_self_ns > 0);
    CHECK(row->next_self_ns == 0);  // not the caller's
    CHECK(row->pool_next_ns == 0);  // and not a worker's either
    // The span belongs to the consumer's pull, not to a producer beside it.
    CHECK(row->span_ns == 0);

    // And it stays out of the self/serial totals.
    const auto summary = runtime::summarize_execution_profile(rows, /*wall_ms=*/5.0,
                                                              /*workers=*/4);
    CHECK(summary.self_ms == 0.0);
    CHECK(summary.serial_self_ms == 0.0);
    CHECK(summary.stage_self_ms > 0.0);
}

TEST_CASE("submitting a batch records a barrier and its wait", "[runtime][profile]") {
    // End-to-end through the real pool, because the summary tests above feed
    // `summarize_execution_profile` hand-built rows and so cannot see the
    // wire-up at all: deleting the `record_execution_profile_barrier` call in
    // `WorkerPool::submit` leaves every one of them passing.
    runtime::WorkerPool pool{4};

    SECTION("one submit is one barrier, and the park is timed") {
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/4,
                                                                        /*report=*/false);
        auto* stage = profile->stage("barrier");
        {
            const runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
            // Long enough that the submitting thread demonstrably parks, so the
            // recorded wait is a real measurement rather than a rounding of zero.
            auto batch = pool.submit(
                4, [](std::size_t) { std::this_thread::sleep_for(std::chrono::milliseconds(5)); });
            batch.wait();
        }
        const auto rows = profile->snapshot();
        const auto row =
            std::ranges::find_if(rows, [](const auto& entry) { return entry.label == "barrier"; });
        REQUIRE(row != rows.end());
        CHECK(row->barriers == 1);
        CHECK(row->barrier_wait_ns > 0);
        // The park is a subset of the scope that contains it.
        CHECK(row->barrier_wait_ns <= row->next_self_ns);
    }

    SECTION("barriers count submits, not waits") {
        // `wait()` is idempotent and the destructor may wait again; counting
        // there would double-count. Three submits, four waits.
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/4,
                                                                        /*report=*/false);
        auto* stage = profile->stage("count");
        {
            const runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
            for (int i = 0; i < 3; ++i) {
                auto batch = pool.submit(2, [](std::size_t) {});
                batch.wait();
                batch.wait();  // idempotent; must not count again
            }
        }
        const auto rows = profile->snapshot();
        const auto row =
            std::ranges::find_if(rows, [](const auto& entry) { return entry.label == "count"; });
        REQUIRE(row != rows.end());
        CHECK(row->barriers == 3);
    }

    SECTION("a batch waited only by its destructor is still accounted") {
        // The destructor is one of three wait sites. A caller that never calls
        // wait() still parks there, and an unaccounted park is precisely the
        // blind spot this measurement exists to close.
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/4,
                                                                        /*report=*/false);
        auto* stage = profile->stage("dtor");
        {
            const runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
            auto batch = pool.submit(
                4, [](std::size_t) { std::this_thread::sleep_for(std::chrono::milliseconds(5)); });
            // no explicit wait(); ~Batch blocks
        }
        const auto rows = profile->snapshot();
        const auto row =
            std::ranges::find_if(rows, [](const auto& entry) { return entry.label == "dtor"; });
        REQUIRE(row != rows.end());
        CHECK(row->barriers == 1);
        CHECK(row->barrier_wait_ns > 0);
    }
}

TEST_CASE("worker time is attributed before the batch is settled", "[runtime][profile]") {
    // The batch's owner must not be able to observe completion — and so drop the
    // profile state — while a worker is still writing into it. `run_task` has to
    // record before decrementing `remaining`, otherwise a worker is still inside
    // the profile after `wait()` has already returned.
    //
    // The pool is deliberately built ONCE, outside the loop. Destroying a pool
    // joins its threads, which would synchronise the workers for us and make
    // this pass either way — that is exactly what an earlier version of this
    // test did, and it could not tell the two orderings apart. Here `wait()` is
    // the only synchronisation, so a worker that records afterwards is still
    // running when the snapshot is taken and the count comes up short.
    runtime::WorkerPool pool{4};
    for (int repeat = 0; repeat < 500; ++repeat) {
        auto profile = std::make_shared<runtime::ExecutionProfileState>(/*worker_budget=*/4,
                                                                        /*report=*/false);
        auto* stage = profile->stage("attribution");
        {
            runtime::ExecutionProfileScope scope(stage, runtime::ProfilePhase::Next);
            auto batch = pool.submit(4, [](std::size_t) {});
            batch.wait();
        }
        const auto rows = profile->snapshot();
        const auto row = std::ranges::find_if(
            rows, [](const auto& entry) { return entry.label == "attribution"; });
        REQUIRE(row != rows.end());
        REQUIRE(row->pool_tasks == 4);
        profile.reset();
    }
}

}  // namespace
