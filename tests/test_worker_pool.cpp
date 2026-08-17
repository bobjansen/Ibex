// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Worker-pool unit tests (runtime multithreading, Phase 1).
//
// The pool is deliberately tiny, so what is worth testing is its contract with
// the parallel island: every submitted worker body runs exactly once, `wait()`
// really waits, an escaping exception is reported deterministically rather than
// terminating the process, and a Batch never outlives its bodies.

#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>

using namespace ibex;

namespace {

/// Save one `IBEX_*` variable and restore it on scope exit, so a test may set it
/// freely without leaking the setting into whatever Catch2 runs next.
class EnvGuard {
   public:
    explicit EnvGuard(const char* name) : name_(name) {
        if (const char* v = std::getenv(name); v != nullptr) {  // NOLINT(concurrency-mt-unsafe)
            saved_ = v;
        }
    }
    ~EnvGuard() {
        if (saved_.has_value()) {
            runtime::set_env(name_, *saved_);
        } else {
            runtime::unset_env(name_);
        }
    }
    EnvGuard(const EnvGuard&) = delete;
    auto operator=(const EnvGuard&) -> EnvGuard& = delete;
    EnvGuard(EnvGuard&&) = delete;
    auto operator=(EnvGuard&&) -> EnvGuard& = delete;

   private:
    const char* name_;
    std::optional<std::string> saved_;
};

}  // namespace

TEST_CASE("WorkerPool runs each worker body exactly once", "[runtime][worker_pool]") {
    runtime::WorkerPool pool(4);
    REQUIRE(pool.size() == 4);

    std::mutex mutex;
    std::multiset<std::size_t> seen;
    auto batch = pool.submit(4, [&](std::size_t id) {
        const std::scoped_lock lock(mutex);
        seen.insert(id);
    });
    batch.wait();

    REQUIRE(seen.size() == 4);
    for (std::size_t id = 0; id < 4; ++id) {
        CHECK(seen.count(id) == 1);
    }
}

TEST_CASE("WorkerPool clamps the worker count to the pool size", "[runtime][worker_pool]") {
    runtime::WorkerPool pool(2);
    std::atomic<std::size_t> ran{0};
    auto batch = pool.submit(16, [&](std::size_t) { ran.fetch_add(1); });
    batch.wait();
    CHECK(ran.load() == 2);

    // A zero count still runs one worker rather than silently doing nothing.
    std::atomic<std::size_t> ran_zero{0};
    auto zero_batch = pool.submit(0, [&](std::size_t) { ran_zero.fetch_add(1); });
    zero_batch.wait();
    CHECK(ran_zero.load() == 1);
}

TEST_CASE("WorkerPool reuses its threads across batches", "[runtime][worker_pool]") {
    // The pool is process-owned precisely so no query pays thread creation.
    // Many small batches must therefore all be servable by the same threads.
    runtime::WorkerPool pool(3);
    std::atomic<std::size_t> total{0};
    for (int round = 0; round < 50; ++round) {
        auto batch = pool.submit(3, [&](std::size_t) { total.fetch_add(1); });
        batch.wait();
    }
    CHECK(total.load() == 150);
}

TEST_CASE("WorkerPool::wait rethrows the lowest worker's exception", "[runtime][worker_pool]") {
    runtime::WorkerPool pool(4);
    // Several workers throw. The reported failure must be the lowest worker id,
    // not whichever thread happened to finish first — the island depends on the
    // same rule (lowest morsel sequence) for a deterministic error.
    auto batch = pool.submit(4, [](std::size_t id) {
        if (id > 0) {
            throw std::runtime_error("worker " + std::to_string(id));
        }
    });
    CHECK_THROWS_WITH(batch.wait(), "worker 1");
}

TEST_CASE("WorkerPool batch destruction waits for its bodies", "[runtime][worker_pool]") {
    // A Batch that let its workers outlive it would leave them touching freed
    // captures. The destructor must join, even when wait() was never called.
    runtime::WorkerPool pool(2);
    std::atomic<bool> finished{false};
    {
        std::atomic<std::size_t> counter{0};
        {
            auto batch = pool.submit(2, [&](std::size_t) {
                for (int i = 0; i < 1000; ++i) {
                    counter.fetch_add(1);
                }
            });
        }  // ~Batch joins here
        CHECK(counter.load() == 2000);
        finished.store(true);
    }
    CHECK(finished.load());
}

TEST_CASE("decode_thread_count separates the decode pool from the compute budget",
          "[runtime][worker_pool]") {
    // The two budgets are different numbers on purpose: decode is
    // memory-latency bound and pays for threads past the core count, compute is
    // not and does not. Measured pinned at 2 cores, the suite went 3247ms ->
    // 2832ms with a doubled pool; at 8 cores the same doubling LOST 9%. So the
    // policy has to bend at both ends, and this pins that shape.
    const std::size_t cores = runtime::compute_thread_count();
    const std::size_t decode = runtime::decode_thread_count();

    // Never fewer decode threads than cores -- that would starve the scan
    // pipeline, which is the one consumer sized against the pool.
    CHECK(decode >= cores);
    // And never unbounded: past the memory system's saturation point the extra
    // threads are contention, which is why this is a clamp and not a multiplier.
    CHECK(decode <= std::max<std::size_t>(cores, 2 * cores));

    if (cores == 1) {
        // One core has nothing to overlap, and a second pool thread would flip
        // the `pool.size() < 2` guards on -- buying pipeline machinery for a
        // workload that cannot use it. Measured a 3.5% loss.
        CHECK(decode == 1);
    }
}

TEST_CASE("compute_thread_count is at least one", "[runtime][worker_pool]") {
    CHECK(runtime::compute_thread_count() >= 1);
    CHECK(runtime::process_worker_pool().size() >= 1);
}

TEST_CASE("process worker pool can be shut down and recreated", "[runtime][worker_pool]") {
    std::atomic<std::size_t> ran{0};
    {
        auto batch = runtime::process_worker_pool().submit(
            1, [&](std::size_t) { ran.fetch_add(1, std::memory_order_relaxed); });
        batch.wait();
    }
    runtime::shutdown_process_worker_pool();

    {
        auto batch = runtime::process_worker_pool().submit(
            1, [&](std::size_t) { ran.fetch_add(1, std::memory_order_relaxed); });
        batch.wait();
    }
    CHECK(ran.load() == 2);
}

TEST_CASE("on_worker_pool_thread distinguishes pool threads from the caller",
          "[runtime][worker_pool]") {
    // The guard against nested parallelism. Anything reachable from inside a
    // parallel island — update_table's field split, today — checks this before
    // submitting work of its own, because submitting from a worker deadlocks
    // the pool. A flag that is never observed to be true would silence that
    // check rather than enforce it.
    runtime::WorkerPool pool(2);

    CHECK_FALSE(runtime::on_worker_pool_thread());

    std::atomic<int> saw_true{0};
    std::atomic<int> ran{0};
    {
        auto batch = pool.submit(2, [&](std::size_t) {
            ran.fetch_add(1, std::memory_order_relaxed);
            if (runtime::on_worker_pool_thread()) {
                saw_true.fetch_add(1, std::memory_order_relaxed);
            }
        });
        batch.wait();
    }
    CHECK(ran.load() == 2);
    CHECK(saw_true.load() == 2);

    // And it is not left set on the calling thread afterwards.
    CHECK_FALSE(runtime::on_worker_pool_thread());
}

// `IBEX_PARALLEL` has to answer both ways.
//
// Parallel islands are on by default, so a switch that could only turn them ON
// would leave a user hitting a threading bug -- or an A/B measuring the feature
// -- with no way to turn them off. That is exactly the kind of gap nothing else
// would catch: the flag would look like it worked in every test that sets it to
// "1", which is the only value the old one understood.
TEST_CASE("IBEX_PARALLEL can turn parallel islands off as well as on",
          "[runtime][parallel][worker_pool]") {
    const EnvGuard guard("IBEX_PARALLEL");

    SECTION("unset leaves the caller's choice alone") {
        runtime::unset_env("IBEX_PARALLEL");
        CHECK_FALSE(runtime::parallel_enabled_from_env().has_value());
    }

    SECTION("true-ish values ask for on") {
        for (const char* value : {"1", "on", "true", "yes"}) {
            runtime::set_env("IBEX_PARALLEL", value);
            CAPTURE(value);
            REQUIRE(runtime::parallel_enabled_from_env().has_value());
            CHECK(runtime::parallel_enabled_from_env().value());
        }
    }

    SECTION("false-ish values ask for off") {
        for (const char* value : {"0", "off", "false", "no"}) {
            runtime::set_env("IBEX_PARALLEL", value);
            CAPTURE(value);
            REQUIRE(runtime::parallel_enabled_from_env().has_value());
            CHECK_FALSE(runtime::parallel_enabled_from_env().value());
        }
    }

    SECTION("an unrecognized value leaves the choice alone rather than guessing") {
        runtime::set_env("IBEX_PARALLEL", "maybe");
        CHECK_FALSE(runtime::parallel_enabled_from_env().has_value());
    }
}

// The other two execution switches parse exactly like `IBEX_PARALLEL`.
//
// They share one `env_flag` parser precisely so the accepted spellings cannot
// drift apart -- `IBEX_PARALLEL=off` working while `IBEX_JOIN_PROBE=off` is
// silently ignored is the failure this pins down, and it is invisible in any
// test that only ever writes "0".
TEST_CASE("execution switches share one on/off spelling", "[runtime][parallel][worker_pool]") {
    const auto check_flag = [](const char* name, auto read) {
        const EnvGuard guard(name);
        CAPTURE(name);

        runtime::unset_env(name);
        CHECK_FALSE(read().has_value());  // unset leaves the caller's choice alone

        for (const char* value : {"1", "on", "true", "yes"}) {
            runtime::set_env(name, value);
            CAPTURE(value);
            REQUIRE(read().has_value());
            CHECK(read().value());
        }
        for (const char* value : {"0", "off", "false", "no"}) {
            runtime::set_env(name, value);
            CAPTURE(value);
            REQUIRE(read().has_value());
            CHECK_FALSE(read().value());
        }

        // Unrecognized: leave the choice alone rather than guessing a direction.
        runtime::set_env(name, "maybe");
        CHECK_FALSE(read().has_value());
    };

    check_flag("IBEX_STREAM_SCAN", runtime::stream_scans_from_env);
    check_flag("IBEX_JOIN_PROBE", runtime::parallel_join_probe_from_env);
    check_flag("IBEX_PARALLEL", runtime::parallel_enabled_from_env);
}

// The I8 contract: the environment reaches these settings through
// `configure_parallel_from_env` and the `ExecutionContext`, and nowhere else.
//
// Both switches used to be a `getenv` at each of their three use sites. Six
// reads of two variables is six authorities, each free to disagree with the
// context the rest of the engine obeys -- and a caller that built its own
// context had no way to say "ignore the environment" for them, which is the
// spelling `interpret()`'s no-context overload exists to contrast with.
TEST_CASE("stream-scan and join-probe switches reach the ExecutionContext",
          "[runtime][parallel][worker_pool]") {
    const EnvGuard stream_guard("IBEX_STREAM_SCAN");
    const EnvGuard probe_guard("IBEX_JOIN_PROBE");

    SECTION("both default to on") {
        const runtime::ExecutionContext exec;
        CHECK(exec.stream_scans);
        CHECK(exec.parallel_join_probe);
    }

    SECTION("the environment turns each one off independently") {
        runtime::set_env("IBEX_STREAM_SCAN", "0");
        runtime::unset_env("IBEX_JOIN_PROBE");
        runtime::ExecutionContext exec;
        runtime::configure_parallel_from_env(exec);
        CHECK_FALSE(exec.stream_scans);
        CHECK(exec.parallel_join_probe);

        runtime::unset_env("IBEX_STREAM_SCAN");
        runtime::set_env("IBEX_JOIN_PROBE", "off");
        runtime::ExecutionContext other;
        runtime::configure_parallel_from_env(other);
        CHECK(other.stream_scans);
        CHECK_FALSE(other.parallel_join_probe);
    }

    SECTION("an unset variable leaves a caller's explicit choice alone") {
        runtime::unset_env("IBEX_STREAM_SCAN");
        runtime::unset_env("IBEX_JOIN_PROBE");
        runtime::ExecutionContext exec;
        exec.stream_scans = false;
        exec.parallel_join_probe = false;
        runtime::configure_parallel_from_env(exec);
        CHECK_FALSE(exec.stream_scans);
        CHECK_FALSE(exec.parallel_join_probe);
    }

    SECTION("the environment overrides a caller's choice when it is set") {
        runtime::set_env("IBEX_STREAM_SCAN", "yes");
        runtime::set_env("IBEX_JOIN_PROBE", "yes");
        runtime::ExecutionContext exec;
        exec.stream_scans = false;
        exec.parallel_join_probe = false;
        runtime::configure_parallel_from_env(exec);
        CHECK(exec.stream_scans);
        CHECK(exec.parallel_join_probe);
    }
}
