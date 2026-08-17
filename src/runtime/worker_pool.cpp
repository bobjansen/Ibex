// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <condition_variable>
#include <cstddef>
#include <cstdio>
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
    // Drop any park this thread accumulated before the body began, so a
    // previous task's leftover cannot be charged to this one.
    if (state.profile_entry != nullptr) {
        (void)take_pool_park_ns();
    }
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
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - profile_start);
        // A worker that parked on ring backpressure was not working. Counting
        // that park as worker time made a blocked worker read as a busy one and
        // made `occupancy` overstate how much of the machine was in use.
        // Clamped: the two are sampled by the same clock but the subtraction
        // must not underflow on an unsigned duration.
        const auto parked = std::min(take_pool_park_ns(), elapsed);
        record_execution_profile_worker(state.profile_entry, elapsed - parked);
        record_execution_profile_pool_idle(state.profile_entry, parked);
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

// Same reasoning as `t_on_pool_thread`: a host TU rather than an inline header
// variable, so a bundled plugin's statically linked copy cannot disagree.
thread_local bool t_on_stage_thread = false;

std::atomic<std::size_t> g_stage_threads_live{0};
std::atomic<std::size_t> g_stage_threads_peak{0};

}  // namespace

auto on_worker_pool_thread() noexcept -> bool {
    return t_on_pool_thread;
}

auto on_stage_thread() noexcept -> bool {
    return t_on_stage_thread;
}

StageThreadScope::StageThreadScope() noexcept {
    t_on_stage_thread = true;
    const std::size_t live = g_stage_threads_live.fetch_add(1, std::memory_order_relaxed) + 1;
    // Raise the peak monotonically. A plain `store(max)` would race two threads
    // starting at once into losing one of the increments.
    std::size_t peak = g_stage_threads_peak.load(std::memory_order_relaxed);
    while (peak < live &&
           !g_stage_threads_peak.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
    }
}

StageThreadScope::~StageThreadScope() {
    g_stage_threads_live.fetch_sub(1, std::memory_order_relaxed);
    t_on_stage_thread = false;
}

auto stage_thread_peak() noexcept -> std::size_t {
    return g_stage_threads_peak.load(std::memory_order_relaxed);
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
    // One submit is one fork-join round trip: every batch is waited on before
    // its captures die, so counting here needs no matching hook in `wait()`
    // (which is idempotent, and which the destructor may call again).
    record_execution_profile_barrier(state->profile_entry);
    {
        const std::lock_guard lock(impl_->mutex);
        for (std::size_t i = 0; i < count; ++i) {
            impl_->queue.push_back(Task{.state = state, .worker_id = i});
        }
    }
    impl_->work.notify_all();
    return Batch{std::move(state)};
}

namespace {

/// Block until every worker body of `state` has returned, charging the wall
/// time to the operator that submitted the batch.
///
/// One helper for all three wait sites (`wait()`, the move-assign, the
/// destructor) so none of them can be the one that forgets to account for
/// itself — a barrier the profile cannot see is exactly the blind spot this
/// measurement exists to remove.
///
/// The clock is read only when profiling installed an entry, so an unprofiled
/// run pays one null check per wait rather than two `steady_clock::now()`.
void wait_for_batch(WorkerPool::Batch::State& state, std::unique_lock<std::mutex>& lock) {
    if (state.profile_entry == nullptr) {
        state.done.wait(lock, [&state] { return state.remaining == 0; });
        return;
    }
    const auto start = std::chrono::steady_clock::now();
    state.done.wait(lock, [&state] { return state.remaining == 0; });
    record_execution_profile_barrier_wait(state.profile_entry,
                                          std::chrono::duration_cast<std::chrono::nanoseconds>(
                                              std::chrono::steady_clock::now() - start));
}

}  // namespace

WorkerPool::Batch::Batch(Batch&&) noexcept = default;

auto WorkerPool::Batch::operator=(Batch&& other) noexcept -> Batch& {
    if (this != &other) {
        // Never abandon a running batch: its bodies still reference whatever
        // the previous owner captured.
        if (state_ != nullptr) {
            std::unique_lock lock(state_->mutex);
            wait_for_batch(*state_, lock);
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
    wait_for_batch(*state_, lock);
}

void WorkerPool::Batch::wait() {
    if (state_ == nullptr) {
        return;
    }
    std::exception_ptr error;
    {
        std::unique_lock lock(state_->mutex);
        wait_for_batch(*state_, lock);
        error = std::exchange(state_->error, nullptr);
    }
    if (error != nullptr) {
        std::rethrow_exception(error);
    }
}

auto compute_thread_count() -> std::size_t {
    // `IBEX_THREADS` was this knob's name until the compute budget and the pool
    // size were split apart. Left unhandled the rename fails SILENTLY and in the
    // worst possible direction: the old name is ignored, the fallback is
    // `hardware_concurrency()`, and a run meant to be pinned to one core quietly
    // spawns twenty-four threads. That cost a measurement here before the
    // warning existed, and a benchmark script is exactly the kind of caller that
    // would never notice. Warn once rather than aliasing, so stale callers get
    // fixed instead of silently kept working.
    static const bool warned = [] {
        if (!env_value("IBEX_THREADS").empty() && env_value("IBEX_CORES").empty()) {
            std::fputs(
                "ibex: IBEX_THREADS is no longer read; it was split into "
                "IBEX_CORES (compute budget) and IBEX_DECODE_THREADS (pool size). "
                "Ignoring it and using the detected core count.\n",
                stderr);
        }
        return true;
    }();
    (void)warned;
    const auto raw = env_value("IBEX_CORES");
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

/// Positive integer from `name`, or `fallback` when unset/unparseable/zero.
/// Every pool-sizing knob goes through this so they all answer the same way.
auto env_size(const char* name, std::size_t fallback) -> std::size_t {
    const auto raw = env_value(name);
    if (raw.empty()) {
        return fallback;
    }
    std::size_t parsed = 0;
    const auto* end = raw.data() + raw.size();
    const auto result = std::from_chars(raw.data(), end, parsed);
    return (result.ec == std::errc{} && result.ptr == end && parsed > 0) ? parsed : fallback;
}

auto decode_thread_count() -> std::size_t {
    // How many threads the process pool gets. Sized for DECODE, which is
    // memory-latency bound: below saturation, extra decode threads fill stalls
    // a core-sized budget leaves empty. Compute is not latency bound and does
    // not want them, so compute paths clamp to `ExecutionContext::
    // parallel_threads` (pinned to the core count in
    // `configure_parallel_from_env`) and only the scan pipeline draws on the
    // whole pool.
    //
    // The policy is "oversubscribe until the memory system saturates, then
    // stop", because a flat multiplier is measurably wrong at both ends. Suite
    // totals, pinned, varying only the pool size:
    //
    //     cores   1x      2x      3x
    //       1   3787    3919    3947     <- oversubscribing LOSES
    //       2   3247    2832    2868     <- 2x wins by 12.8%
    //       4   2320    2165    2189     <- 2x wins by 6.7%
    //       8   1809    1971    1959     <- oversubscribing LOSES by 9%
    //
    // At one core there is nothing to overlap with and a second thread only
    // flips the `pool.size() < 2` guards on, buying pipeline machinery for a
    // workload that cannot use it. At eight the memory system is already
    // saturated and the extra threads are pure contention. In between, decode
    // stalls dominate and doubling pays.
    //
    // The saturation point is where THIS box stops improving, so it is a
    // property of the memory system rather than of the code, and it is
    // `IBEX_DECODE_SATURATION` precisely so it can be swept on a new machine
    // instead of guessed. Four points on one box is not a heuristic; it is the
    // first row of the table one would be derived from. Re-derive before
    // trusting any of this on the AWS 4-physical-core box (benchmarking/aws).
    const std::size_t saturation = env_size("IBEX_DECODE_SATURATION", 8);
    const std::size_t cores = compute_thread_count();
    if (const std::size_t forced = env_size("IBEX_DECODE_THREADS", 0); forced != 0) {
        return forced;  // absolute override, for A/B and for other boxes
    }
    if (cores < 2) {
        return cores;
    }
    return std::min(cores * 2, std::max(cores, saturation));
}

namespace {

struct ProcessWorkerPool {
    std::mutex mutex;
    std::unique_ptr<WorkerPool> pool;
};

auto process_worker_pool_state() -> ProcessWorkerPool& {
    // Deliberately never destroy this registry at CRT/process teardown.
    //
    // On Windows, RStudio's R-session shutdown reaches C++ static destruction
    // after its worker-thread bookkeeping has started to disappear. Destroying
    // a pool then attempts to join those threads and can leave rsession.exe
    // stuck in WaitForSingleObject. The operating system owns process teardown
    // and reclaims this tiny registry and any still-live workers atomically.
    //
    // This is not a leak on an ordinary DLL unload: R_unload_ibex() calls
    // shutdown_process_worker_pool() first, which joins and releases `pool`.
    // The heap registry itself must outlive the DLL's static destruction so
    // that process exit does not perform a second, unsafe teardown.
    static auto* state = new ProcessWorkerPool();  // NOLINT(cppcoreguidelines-owning-memory)
    return *state;
}

}  // namespace

auto process_worker_pool() -> WorkerPool& {
    auto& state = process_worker_pool_state();
    const std::lock_guard lock(state.mutex);
    if (state.pool == nullptr) {
        state.pool = std::make_unique<WorkerPool>(decode_thread_count());
    }
    return *state.pool;
}

void shutdown_process_worker_pool() {
    auto& state = process_worker_pool_state();
    std::unique_ptr<WorkerPool> pool;
    {
        const std::lock_guard lock(state.mutex);
        pool = std::move(state.pool);
    }
    // Destroy outside the singleton mutex: joining a worker can take arbitrary
    // time, and no code needs to hold the registry lock while doing so.
    pool.reset();
}

namespace {

/// The one spelling of an on/off `IBEX_*` switch: `nullopt` when unset or
/// unrecognized, so the caller keeps whatever it already chose.
///
/// Every such switch answers BOTH ways on purpose. All three are on by default,
/// so a variable that could only turn its feature on would leave no way to turn
/// it off — which is exactly what someone hitting a bug in it, or A/B-ing it,
/// needs. Shared so the accepted spellings cannot drift apart between them.
[[nodiscard]] auto env_flag(const char* name) -> std::optional<bool> {
    const auto raw = env_value(name);
    if (raw == "1" || raw == "on" || raw == "true" || raw == "yes") {
        return true;
    }
    if (raw == "0" || raw == "off" || raw == "false" || raw == "no") {
        return false;
    }
    return std::nullopt;
}

}  // namespace

auto parallel_enabled_from_env() -> std::optional<bool> {
    return env_flag("IBEX_PARALLEL");
}

auto stream_scans_from_env() -> std::optional<bool> {
    return env_flag("IBEX_STREAM_SCAN");
}

auto parallel_join_probe_from_env() -> std::optional<bool> {
    return env_flag("IBEX_JOIN_PROBE");
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
