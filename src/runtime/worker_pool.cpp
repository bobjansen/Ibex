// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <charconv>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "execution_profile_internal.hpp"

namespace ibex::runtime {

struct WorkerPool::Batch::State {
    std::function<void(std::size_t)> body;

    std::mutex mutex;
    std::condition_variable done;
    std::size_t remaining = 0;
    std::exception_ptr error;
    std::size_t error_worker = 0;
    ExecutionProfileEntry* profile_entry = nullptr;
};

namespace {

struct Task {
    std::shared_ptr<WorkerPool::Batch::State> state;
    std::size_t worker_id = 0;
};

/// Runs one worker body and settles its slot in the batch. Exceptions are
/// captured rather than propagated: a throw out of a pool thread would
/// terminate the process, and the batch's owner rethrows deterministically
/// (lowest worker id wins) from `wait()`.
void run_task(Task const& task) {
    auto& state = *task.state;
    const auto profile_start = state.profile_entry == nullptr
                                   ? std::chrono::steady_clock::time_point{}
                                   : std::chrono::steady_clock::now();
    std::exception_ptr caught;
    try {
        state.body(task.worker_id);
    } catch (...) {
        caught = std::current_exception();
    }
    // Attribute the worker's time BEFORE settling the batch. `profile_entry`
    // points into the query's profile state, and nothing here keeps that alive:
    // the moment `remaining` reaches zero the waiter may return, finish the
    // query, and drop the last reference to it. Recording afterwards leaves a
    // window — narrow, and only when profiling is on — in which this thread
    // writes into freed memory.
    if (state.profile_entry != nullptr) {
        record_execution_profile_worker(state.profile_entry,
                                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                                            std::chrono::steady_clock::now() - profile_start));
    }
    {
        const std::lock_guard lock(state.mutex);
        if (caught != nullptr && (state.error == nullptr || task.worker_id < state.error_worker)) {
            state.error = caught;
            state.error_worker = task.worker_id;
        }
        --state.remaining;
    }
    state.done.notify_all();
}

[[nodiscard]] auto env_value(const char* name) -> std::string_view {
    const char* raw = std::getenv(name);  // NOLINT(concurrency-mt-unsafe)
    return raw == nullptr ? std::string_view{} : std::string_view{raw};
}

}  // namespace

// Apple's libc++ (macOS clang-werror leg) doesn't ship std::jthread yet.
// jthread buys nothing here beyond auto-join on destruction (no stop_token
// use), so std::thread with an explicit join in ~WorkerPool is equivalent.
#ifdef __cpp_lib_jthread
using PoolThread = std::jthread;
#else
using PoolThread = std::thread;
#endif

struct WorkerPool::Impl {
    std::mutex mutex;
    std::condition_variable work;
    std::deque<Task> queue;
    bool stopping = false;
    std::vector<PoolThread> threads;
};

// Declared rather than included: `invariant_violation` lives in the
// interpreter's internal header, and this file is a lower-level primitive that
// should not depend on it. Defined in interpreter.cpp, same library.
[[noreturn]] void invariant_violation(std::string_view detail);

namespace {

// True on a thread owned by some WorkerPool. Set for the thread's whole life,
// not per task, so it is correct anywhere down a task's call stack.
//
// Lives in this host TU rather than an inline variable in the header: bundled
// plugins statically link runtime code and would otherwise each get their own
// copy (the RTLD_LOCAL trap that also governs the pool singleton and the query
// lease).
thread_local bool t_on_pool_thread = false;

}  // namespace

auto on_worker_pool_thread() noexcept -> bool {
    return t_on_pool_thread;
}

WorkerPool::WorkerPool(std::size_t threads)
    : impl_(std::make_unique<Impl>()), threads_(threads == 0 ? 1 : threads) {
    impl_->threads.reserve(threads_);
    for (std::size_t i = 0; i < threads_; ++i) {
        impl_->threads.emplace_back([this] {
            t_on_pool_thread = true;
            while (true) {
                Task task;
                {
                    std::unique_lock lock(impl_->mutex);
                    impl_->work.wait(lock,
                                     [this] { return impl_->stopping || !impl_->queue.empty(); });
                    if (impl_->stopping && impl_->queue.empty()) {
                        return;
                    }
                    task = std::move(impl_->queue.front());
                    impl_->queue.pop_front();
                }
                run_task(task);
            }
        });
    }
}

WorkerPool::~WorkerPool() {
    {
        const std::lock_guard lock(impl_->mutex);
        impl_->stopping = true;
    }
    impl_->work.notify_all();
#ifdef __cpp_lib_jthread
    // ~jthread joins; queued work drains first so no batch is left unsettled.
#else
    for (auto& thread : impl_->threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
#endif
}

auto WorkerPool::submit(std::size_t worker_count, std::function<void(std::size_t)> body) -> Batch {
    // Reentrant submission cannot work with this pool and must never be a
    // silent hang. The worker set is fixed and every Batch is waited on (if not
    // explicitly, then by its destructor), so a worker that submits and waits
    // blocks one of the threads its own work needs. With enough of them the
    // pool deadlocks, and it would do so only under a particular interleaving.
    // Callers that might run on a pool thread ask `on_worker_pool_thread()`
    // first and take their serial path.
    if (on_worker_pool_thread()) {
        invariant_violation(
            "WorkerPool::submit called from a pool worker — nested submission deadlocks; "
            "check on_worker_pool_thread() and run serially instead");
    }
    const std::size_t count = std::clamp<std::size_t>(worker_count, 1, threads_);
    auto state = std::make_shared<Batch::State>();
    state->body = std::move(body);
    state->remaining = count;
    state->profile_entry = current_execution_profile_entry();
    {
        const std::lock_guard lock(impl_->mutex);
        for (std::size_t i = 0; i < count; ++i) {
            impl_->queue.push_back(Task{.state = state, .worker_id = i});
        }
    }
    impl_->work.notify_all();
    return Batch{std::move(state)};
}

WorkerPool::Batch::Batch(Batch&&) noexcept = default;

auto WorkerPool::Batch::operator=(Batch&& other) noexcept -> Batch& {
    if (this != &other) {
        // Never abandon a running batch: its bodies still reference whatever
        // the previous owner captured.
        if (state_ != nullptr) {
            std::unique_lock lock(state_->mutex);
            state_->done.wait(lock, [this] { return state_->remaining == 0; });
        }
        state_ = std::move(other.state_);
    }
    return *this;
}

WorkerPool::Batch::~Batch() {
    if (state_ == nullptr) {
        return;
    }
    std::unique_lock lock(state_->mutex);
    state_->done.wait(lock, [this] { return state_->remaining == 0; });
}

void WorkerPool::Batch::wait() {
    if (state_ == nullptr) {
        return;
    }
    std::exception_ptr error;
    {
        std::unique_lock lock(state_->mutex);
        state_->done.wait(lock, [this] { return state_->remaining == 0; });
        error = std::exchange(state_->error, nullptr);
    }
    if (error != nullptr) {
        std::rethrow_exception(error);
    }
}

auto default_thread_count() -> std::size_t {
    const auto raw = env_value("IBEX_THREADS");
    if (!raw.empty() && raw != "auto") {
        std::size_t parsed = 0;
        const auto* end = raw.data() + raw.size();
        const auto result = std::from_chars(raw.data(), end, parsed);
        if (result.ec == std::errc{} && result.ptr == end && parsed > 0) {
            return parsed;
        }
        return 1;
    }
    const unsigned hardware = std::thread::hardware_concurrency();
    return hardware == 0 ? 1 : static_cast<std::size_t>(hardware);
}

auto process_worker_pool() -> WorkerPool& {
    // Function-local static: constructed on first use (thread-safe since C++11)
    // and joined during normal process teardown.
    static WorkerPool pool(default_thread_count());
    return pool;
}

auto parallel_enabled_from_env() -> std::optional<bool> {
    const auto raw = env_value("IBEX_PARALLEL");
    if (raw == "1" || raw == "on" || raw == "true" || raw == "yes") {
        return true;
    }
    if (raw == "0" || raw == "off" || raw == "false" || raw == "no") {
        return false;
    }
    return std::nullopt;
}

auto source_chunk_rows_from_env() -> std::size_t {
    const auto raw = env_value("IBEX_CHUNK_ROWS");
    if (raw.empty()) {
        return 0;
    }
    std::size_t parsed = 0;
    const auto* end = raw.data() + raw.size();
    const auto result = std::from_chars(raw.data(), end, parsed);
    if (result.ec == std::errc{} && result.ptr == end) {
        return parsed;
    }
    return 0;
}

auto morsel_rows_from_env() -> std::size_t {
    const auto raw = env_value("IBEX_MORSEL_ROWS");
    if (raw.empty()) {
        return 0;
    }
    std::size_t parsed = 0;
    const auto* end = raw.data() + raw.size();
    const auto result = std::from_chars(raw.data(), end, parsed);
    if (result.ec == std::errc{} && result.ptr == end) {
        return parsed;
    }
    return 0;
}

}  // namespace ibex::runtime
