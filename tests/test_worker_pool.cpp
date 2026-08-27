// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Worker-pool unit tests (runtime multithreading, Phase 1).
//
// The pool is deliberately tiny, so what is worth testing is its contract with
// the morsel pipeline: every submitted worker body runs exactly once, `wait()`
// really waits, an escaping exception is reported deterministically rather than
// terminating the process, and a Batch never outlives its bodies.

#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>

#include "execution_profile_internal.hpp"

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
    // not whichever thread happened to finish first — the pipeline depends on the
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

TEST_CASE("WorkerPool task group runs incremental work and joins once", "[runtime][worker_pool]") {
    runtime::WorkerPool pool(4);
    std::atomic<std::size_t> ran{0};
    auto group = pool.task_group();
    for (std::size_t i = 0; i < 32; ++i) {
        group.submit([&] { ran.fetch_add(1, std::memory_order_relaxed); });
    }
    group.wait();
    group.wait();
    CHECK(ran.load(std::memory_order_relaxed) == 32);
}

TEST_CASE("WorkerPool cooperatively completes nested batches from every worker",
          "[runtime][worker_pool]") {
    // All outer bodies reach the nested wait together. A conventional fixed
    // pool deadlocks here: each worker is occupied by a parent while all child
    // tasks remain queued. Cooperative waiting must let the parents drain that
    // queue themselves.
    runtime::WorkerPool pool(4);
    std::atomic<std::size_t> parents_ready{0};
    std::atomic<std::size_t> children_ran{0};
    auto outer = pool.submit(4, [&](std::size_t) {
        parents_ready.fetch_add(1, std::memory_order_release);
        while (parents_ready.load(std::memory_order_acquire) != 4) {
            std::this_thread::yield();
        }
        auto child = pool.submit(4, [&](std::size_t) { children_ran.fetch_add(1); });
        child.wait();
    });
    outer.wait();
    CHECK(children_ran.load() == 16);
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
    // morsel pipeline — update_table's field split, today — checks this before
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

// The runtime has two species of thread, and only one used to be countable.
//
// A stage thread (`PipelinedStageOperator`'s producer) is long-lived and parks on
// its consumer's backpressure, which a fixed-size pool cannot host. So the raw
// std::thread is correct — but before it was tagged, nothing could tell it from
// the calling thread, and the profiler charged its work to main-thread self time.
TEST_CASE("a stage thread is distinguishable from a pool worker and the caller",
          "[runtime][worker_pool][parallel]") {
    CHECK_FALSE(runtime::on_stage_thread());
    CHECK_FALSE(runtime::on_worker_pool_thread());

    SECTION("the flag is set for the scope and cleared after") {
        {
            const runtime::StageThreadScope stage;
            CHECK(runtime::on_stage_thread());
            // The two kinds are mutually exclusive: a stage thread is not a
            // worker, so it does NOT take the nested-parallelism serial path.
            // That is deliberate — its child chain must keep its fan-outs.
            CHECK_FALSE(runtime::on_worker_pool_thread());
        }
        CHECK_FALSE(runtime::on_stage_thread());
    }

    SECTION("the flag is per thread, not global") {
        std::atomic<bool> seen_on_other{true};
        {
            const runtime::StageThreadScope stage;
            std::thread other([&] { seen_on_other = runtime::on_stage_thread(); });
            other.join();
        }
        CHECK_FALSE(seen_on_other.load());
    }

    SECTION("the peak counts concurrent stage threads") {
        std::atomic<int> ready{0};
        std::atomic<bool> release{false};
        const auto body = [&] {
            const runtime::StageThreadScope stage;
            ++ready;
            while (!release.load()) {
                std::this_thread::yield();
            }
        };
        std::thread a(body);
        std::thread b(body);
        std::thread c(body);
        while (ready.load() < 3) {
            std::this_thread::yield();
        }
        // All three are live simultaneously, so the peak must have advanced by
        // at least three — a `store(live)` instead of a monotonic raise would
        // lose increments under this race.
        const std::size_t peak = runtime::stage_thread_peak();
        release = true;
        a.join();
        b.join();
        c.join();
        // Three were live at once, so the high-water mark is at least three.
        // Not `before + 3`: the peak tracks CONCURRENT threads, not a running
        // total, so an earlier test's scope does not add to it.
        CHECK(peak >= 3);
        // And it is a high-water mark: it does not fall when they exit.
        CHECK(runtime::stage_thread_peak() >= peak);
    }
}

// The execution switches all parse the same way.
//
// They share one `env_flag` parser precisely so the accepted spellings cannot
// drift apart -- `IBEX_STREAM_SCAN=off` working while `IBEX_JOIN_PROBE=off` is
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

// `idle_between` is pure arithmetic over two samples, so the cases that
// matter are testable without a pool at all — and they are exactly the cases a
// plain "sum of parks" counter gets wrong.
TEST_CASE("idle_between clips intervals to the sampling window",
          "[runtime][worker_pool][profile]") {
    using runtime::IdleSample;
    const auto idle_ns = [](const IdleSample& begin, const IdleSample& end) {
        return runtime::idle_between(begin, end).count();
    };

    SECTION("a thread parked for the whole window is fully counted") {
        // The case a counter cannot see: the thread never wakes, so it never
        // adds to any total. Parked since 500, window is [1000, 3000].
        IdleSample begin;
        begin.at_ns = 1000;
        begin.threads = {{0, 500}};
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{0, 500}};
        CHECK(idle_ns(begin, end) == 2000);
    }

    SECTION("park time from before the window is not charged to it") {
        // Parked since 500, woke at 1400 having closed a 900ns park. Only the
        // 400ns inside [1000, 3000] is ours.
        IdleSample begin;
        begin.at_ns = 1000;
        begin.threads = {{0, 500}};
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{900, 0}};
        CHECK(idle_ns(begin, end) == 400);
    }

    SECTION("a park that opens inside the window counts from where it opened") {
        IdleSample begin;
        begin.at_ns = 1000;
        begin.threads = {{0, 0}};
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{0, 2500}};
        CHECK(idle_ns(begin, end) == 500);
    }

    SECTION("closed and still-open parks in one window add up") {
        // Closed 300ns of parks, then parked again at 2600 and stayed there.
        IdleSample begin;
        begin.at_ns = 1000;
        begin.threads = {{0, 0}};
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{300, 2600}};
        CHECK(idle_ns(begin, end) == 700);
    }

    SECTION("threads that appear only in the later sample count from zero") {
        // A pool created after the window opened: its threads have no baseline.
        IdleSample begin;
        begin.at_ns = 1000;
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{250, 0}, {0, 2000}};
        CHECK(idle_ns(begin, end) == 250 + 1000);
    }

    SECTION("a fully busy window reports no idle") {
        IdleSample begin;
        begin.at_ns = 1000;
        begin.threads = {{700, 0}};
        IdleSample end;
        end.at_ns = 3000;
        end.threads = {{700, 0}};
        CHECK(idle_ns(begin, end) == 0);
    }
}

TEST_CASE("a real pool reports the idle its threads actually took",
          "[runtime][worker_pool][profile]") {
    constexpr std::size_t kThreads = 4;
    runtime::WorkerPool pool(kThreads);
    // Let every thread reach its park before the window opens, so the measured
    // idle is the window itself rather than thread startup.
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    const auto begin = runtime::sample_pool_idle();
    const auto wall_start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto wall_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - wall_start)
                             .count();
    const auto idle = runtime::idle_between(begin, runtime::sample_pool_idle()).count();

    // A quiet pool of N threads idles for at least N times the window. A lower
    // bound, not an equality: the ledger registry is process-wide, so the
    // process pool's own parked threads are in this sum too.
    CHECK(idle >= wall_ns * static_cast<long long>(kThreads));

    // And work removes idle. Measured as a RATE — threads-worth of idle per unit
    // of wall time — because that is what subtracts out the background pool,
    // which idles at the same rate in both windows. An absolute comparison would
    // be measuring whatever else the test binary happens to have spun up.
    const auto busy_begin = runtime::sample_pool_idle();
    const auto busy_start = std::chrono::steady_clock::now();
    {
        auto batch = pool.submit(kThreads, [](std::size_t) {
            std::this_thread::sleep_for(std::chrono::milliseconds(40));
        });
        batch.wait();
    }
    const auto busy_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::steady_clock::now() - busy_start)
                             .count();
    const auto busy_idle = runtime::idle_between(busy_begin, runtime::sample_pool_idle()).count();

    const double quiet_rate = static_cast<double>(idle) / static_cast<double>(wall_ns);
    const double busy_rate = static_cast<double>(busy_idle) / static_cast<double>(busy_ns);
    // All four threads were occupied for nearly the whole busy window; require at
    // least three threads' worth of idle to have disappeared.
    CHECK(busy_rate < quiet_rate - 3.0);
}

TEST_CASE("a stage thread's backpressure park and lifetime are both accounted",
          "[runtime][worker_pool][profile]") {
    // The producer's park is the one piece of runtime idle with no operator to
    // charge it to: it happens between pulls, outside every profile scope, so
    // unlike a worker's park there is nothing to subtract it from. It gets its
    // own ledger, and `stage_live_ms` is the denominator that makes the number
    // checkable rather than merely printed.
    const auto park_begin = runtime::sample_stage_park();
    const auto live_begin = runtime::sample_stage_live();

    std::atomic<bool> release{false};
    std::mutex mutex;
    std::condition_variable parked;
    constexpr auto kPark = std::chrono::milliseconds(60);

    std::thread producer([&] {
        const runtime::StageThreadScope stage_thread;
        REQUIRE(runtime::on_stage_thread());
        {
            // Exactly what `produce()` does when the ring is full.
            const runtime::RingWaitScope ring_wait;
            std::unique_lock lock(mutex);
            parked.wait(lock, [&] { return release.load(); });
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    });

    std::this_thread::sleep_for(kPark);
    {
        const std::scoped_lock lock(mutex);
        release.store(true);
    }
    parked.notify_all();
    producer.join();

    const auto park_ns = runtime::idle_between(park_begin, runtime::sample_stage_park()).count();
    const auto live_ns = runtime::idle_between(live_begin, runtime::sample_stage_live()).count();

    // The park is measured, not rounded to zero — the state before this change.
    CHECK(park_ns >= std::chrono::nanoseconds(kPark).count() * 9 / 10);
    // And it is a subset of the thread's lifetime, which also covers the work
    // after the park. Both bounds matter: a ledger that double-counted, or one
    // that leaked the interval past the thread's death, would break the upper.
    CHECK(live_ns > park_ns);
    CHECK(park_ns < live_ns);

    // After the thread exits, nothing accrues. A park left open at exit — the
    // cancelled-while-parked path — would make this window grow without bound.
    const auto quiet_begin = runtime::sample_stage_park();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK(runtime::idle_between(quiet_begin, runtime::sample_stage_park()).count() == 0);
}

TEST_CASE("stage ledger slots are reused rather than accumulated",
          "[runtime][worker_pool][profile]") {
    // Stage threads are spawned per staged breaker per query. Without reuse a
    // long REPL session would leak a registry slot per breaker ever executed.
    const auto before = runtime::sample_stage_live().threads.size();
    for (int i = 0; i < 20; ++i) {
        std::thread([] {
            const runtime::StageThreadScope stage_thread;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }).join();
    }
    const auto after = runtime::sample_stage_live().threads.size();
    // Serial threads, so one slot suffices for all twenty. Allow a little slack
    // for a stage thread another test left running concurrently.
    CHECK(after <= before + 3);
}
