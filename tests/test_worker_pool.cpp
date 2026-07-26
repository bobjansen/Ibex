// Worker-pool unit tests (runtime multithreading, Phase 1).
//
// The pool is deliberately tiny, so what is worth testing is its contract with
// the parallel island: every submitted worker body runs exactly once, `wait()`
// really waits, an escaping exception is reported deterministically rather than
// terminating the process, and a Batch never outlives its bodies.

#include <ibex/runtime/worker_pool.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>

using namespace ibex;

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

TEST_CASE("default_thread_count is at least one", "[runtime][worker_pool]") {
    CHECK(runtime::default_thread_count() >= 1);
    CHECK(runtime::process_worker_pool().size() >= 1);
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
    struct EnvGuard {
        std::optional<std::string> saved;
        EnvGuard() {
            if (const char* v = std::getenv("IBEX_PARALLEL"); v != nullptr) {
                saved = v;
            }
        }
        ~EnvGuard() {
            if (saved.has_value()) {
                ::setenv("IBEX_PARALLEL", saved->c_str(), 1);
            } else {
                ::unsetenv("IBEX_PARALLEL");
            }
        }
        EnvGuard(const EnvGuard&) = delete;
        auto operator=(const EnvGuard&) -> EnvGuard& = delete;
        EnvGuard(EnvGuard&&) = delete;
        auto operator=(EnvGuard&&) -> EnvGuard& = delete;
    } const guard;

    SECTION("unset leaves the caller's choice alone") {
        ::unsetenv("IBEX_PARALLEL");
        CHECK_FALSE(runtime::parallel_enabled_from_env().has_value());
    }

    SECTION("true-ish values ask for on") {
        for (const char* value : {"1", "on", "true", "yes"}) {
            ::setenv("IBEX_PARALLEL", value, 1);
            CAPTURE(value);
            REQUIRE(runtime::parallel_enabled_from_env().has_value());
            CHECK(runtime::parallel_enabled_from_env().value());
        }
    }

    SECTION("false-ish values ask for off") {
        for (const char* value : {"0", "off", "false", "no"}) {
            ::setenv("IBEX_PARALLEL", value, 1);
            CAPTURE(value);
            REQUIRE(runtime::parallel_enabled_from_env().has_value());
            CHECK_FALSE(runtime::parallel_enabled_from_env().value());
        }
    }

    SECTION("an unrecognized value leaves the choice alone rather than guessing") {
        ::setenv("IBEX_PARALLEL", "maybe", 1);
        CHECK_FALSE(runtime::parallel_enabled_from_env().has_value());
    }
}
