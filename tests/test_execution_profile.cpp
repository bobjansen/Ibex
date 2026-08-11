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
    // Two operators: one that drew worker help, one that drew none. Spans are
    // deliberately larger than the self times and would overlap if summed —
    // the summary must not use them.
    const auto make_row = [](std::string label, std::uint64_t self_ns, std::uint64_t span_ns,
                             std::uint64_t pool_ns) {
        runtime::ExecutionProfileSnapshotRow row;
        row.label = std::move(label);
        row.next_self_ns = self_ns;
        row.span_ns = span_ns;
        row.pool_work_ns = pool_ns;
        return row;
    };
    std::vector<runtime::ExecutionProfileSnapshotRow> rows;
    rows.push_back(make_row("helped", 3'000'000, 20'000'000, 8'000'000));
    rows.push_back(make_row("serial", 1'000'000, 20'000'000, 0));

    const auto summary = runtime::summarize_execution_profile(rows, /*wall_ms=*/10.0,
                                                              /*workers=*/4);
    CHECK(summary.self_ms == 4.0);         // 3ms + 1ms, exclusive, no double count
    CHECK(summary.serial_self_ms == 1.0);  // only the operator with no pool work
    CHECK(summary.serial_fraction == 0.25);
    CHECK(summary.amdahl_ceiling == 4.0);  // 1 / 0.25
    CHECK(summary.pool_work_ms == 8.0);
    CHECK(summary.occupancy == 0.2);  // 8ms of a 10ms x 4-worker budget
}

TEST_CASE("profile summary reports no ceiling when nothing ran serially", "[runtime][profile]") {
    runtime::ExecutionProfileSnapshotRow helped;
    helped.next_self_ns = 5'000'000;
    helped.span_ns = 5'000'000;
    helped.pool_work_ns = 1'000'000;
    const std::vector<runtime::ExecutionProfileSnapshotRow> rows{helped};
    const auto summary = runtime::summarize_execution_profile(rows, 5.0, 2);
    CHECK(summary.serial_fraction == 0.0);
    CHECK(summary.amdahl_ceiling == 0.0);  // "no ceiling observed", not "1x"
}

TEST_CASE("profile summary tolerates an empty profile and a zero budget", "[runtime][profile]") {
    const auto empty = runtime::summarize_execution_profile({}, 0.0, 0);
    CHECK(empty.self_ms == 0.0);
    CHECK(empty.serial_fraction == 0.0);
    CHECK(empty.occupancy == 0.0);
    CHECK(runtime::profile_row_occupancy({}, 0) == 0.0);
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
