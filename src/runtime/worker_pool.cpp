// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
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
    // A batch remembers how to help its owning pool make progress. This is
    // deliberately kept on the state rather than inferred from thread-local
    // state: a pool worker may wait on a batch belonging to any WorkerPool.
    std::function<bool()> assist_one;

    std::mutex mutex;
    std::condition_variable done;
    std::size_t remaining = 0;
    std::exception_ptr error;
    std::size_t error_worker = 0;
    ExecutionProfileEntry* profile_entry = nullptr;
    bool account_wait = true;
};

struct WorkerPool::TaskGroup::State {
    explicit State(WorkerPool* owner)
        : pool(owner), profile_entry(current_execution_profile_entry()) {}

    WorkerPool* pool = nullptr;
    std::vector<WorkerPool::Batch> batches;
    ExecutionProfileEntry* profile_entry = nullptr;
    bool barrier_recorded = false;
    bool waited = false;
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
void run_task(const Task& task) {
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

[[nodiscard]] auto steady_ns() noexcept -> std::uint64_t {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                          std::chrono::steady_clock::now().time_since_epoch())
                                          .count());
}

/// One thread's time inside one state, in a form a sampler can window.
///
/// Two fields rather than one running total because a total alone cannot answer
/// the question. A thread parked for a whole query never wakes inside it, so it
/// never adds to `closed_ns`; only `open_since_ns` reveals that it was idle the
/// entire time. Both are needed, and `idle_between` combines them.
struct IntervalLedger {
    std::atomic<std::uint64_t> closed_ns{0};
    /// Start of the interval in progress, or 0 when the thread is not in it.
    std::atomic<std::uint64_t> open_since_ns{0};
};

void interval_open(IntervalLedger& ledger) noexcept {
    ledger.open_since_ns.store(steady_ns(), std::memory_order_relaxed);
}

void interval_close(IntervalLedger& ledger) noexcept {
    const auto started = ledger.open_since_ns.exchange(0, std::memory_order_relaxed);
    if (started != 0) {
        ledger.closed_ns.fetch_add(steady_ns() - started, std::memory_order_relaxed);
    }
}

/// A stage thread tracks two intervals: how long it has existed, and how long it
/// has been parked on backpressure. One slot holds both, so the pair cannot
/// drift out of correspondence the way two separate registries could.
struct StageLedger {
    IntervalLedger live;
    IntervalLedger park;
};

template <typename Ledger>
struct LedgerRegistry {
    std::mutex mutex;
    // Deque, not vector: the sampler and the owning thread both hold references
    // into this, and a vector would invalidate them on growth.
    std::deque<Ledger> ledgers;
    // Slots whose thread has exited, available for reuse. Pool threads never
    // retire so their registry never uses this. Stage threads are spawned per
    // staged breaker per query, and without reuse a long REPL session would
    // accumulate one slot per breaker ever executed.
    std::vector<Ledger*> free;
};

/// Never destroyed, for the same reason `process_worker_pool_state` is not: a
/// runtime thread may outlive static destruction on a host that unloads the
/// library, and it writes to its ledger on every park.
template <typename Ledger>
auto ledger_registry() -> LedgerRegistry<Ledger>& {
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory)
    static auto* registry = new LedgerRegistry<Ledger>();
    return *registry;
}

template <typename Ledger>
auto acquire_ledger() -> Ledger& {
    auto& registry = ledger_registry<Ledger>();
    const std::lock_guard lock(registry.mutex);
    if (!registry.free.empty()) {
        Ledger* reused = registry.free.back();
        registry.free.pop_back();
        // Deliberately NOT reset. `idle_between` reads a slot's `closed_ns` as a
        // delta across the window, so zeroing it on reuse would make that delta
        // underflow and silently drop the reused slot's time. A slot stands for
        // "some thread of this species", not one particular thread, and for a
        // SUM that is all the identity needed.
        return *reused;
    }
    return registry.ledgers.emplace_back();
}

template <typename Ledger>
void release_ledger(Ledger& ledger) {
    auto& registry = ledger_registry<Ledger>();
    const std::lock_guard lock(registry.mutex);
    registry.free.push_back(&ledger);
}

/// Read every slot's `project(slot)` interval into a sample.
template <typename Ledger, typename Project>
[[nodiscard]] auto sample_ledgers(Project project) -> IdleSample {
    IdleSample sample;
    sample.at_ns = steady_ns();
    auto& registry = ledger_registry<Ledger>();
    const std::lock_guard lock(registry.mutex);
    sample.threads.reserve(registry.ledgers.size());
    for (const auto& slot : registry.ledgers) {
        const IntervalLedger& ledger = project(slot);
        sample.threads.emplace_back(ledger.closed_ns.load(std::memory_order_relaxed),
                                    ledger.open_since_ns.load(std::memory_order_relaxed));
    }
    return sample;
}

/// This stage thread's slot, or null off a stage thread. Host TU rather than an
/// inline header variable, same RTLD_LOCAL reason as `t_on_stage_thread`.
thread_local StageLedger* t_stage_ledger = nullptr;

}  // namespace

auto sample_pool_idle() -> IdleSample {
    return sample_ledgers<IntervalLedger>(
        [](const IntervalLedger& ledger) -> const IntervalLedger& { return ledger; });
}

auto sample_stage_park() -> IdleSample {
    return sample_ledgers<StageLedger>(
        [](const StageLedger& slot) -> const IntervalLedger& { return slot.park; });
}

auto sample_stage_live() -> IdleSample {
    return sample_ledgers<StageLedger>(
        [](const StageLedger& slot) -> const IntervalLedger& { return slot.live; });
}

void stage_park_begin() noexcept {
    if (t_stage_ledger != nullptr) {
        interval_open(t_stage_ledger->park);
    }
}

void stage_park_end() noexcept {
    if (t_stage_ledger != nullptr) {
        interval_close(t_stage_ledger->park);
    }
}

auto idle_between(const IdleSample& begin, const IdleSample& end) -> std::chrono::nanoseconds {
    std::uint64_t total = 0;
    for (std::size_t i = 0; i < end.threads.size(); ++i) {
        const auto [closed_end, start_end] = end.threads[i];
        const auto [closed_begin, start_begin] =
            i < begin.threads.size() ? begin.threads[i] : std::pair<std::uint64_t, std::uint64_t>{};
        // Parks that CLOSED during the window. A park already running at `begin`
        // closes inside it with its whole length, including the part that
        // happened before the window opened — subtract that prefix, or a pool
        // that idled for an hour would charge the hour to the next query.
        std::uint64_t idle = closed_end >= closed_begin ? closed_end - closed_begin : 0;
        if (start_begin != 0 && start_begin != start_end && begin.at_ns > start_begin) {
            idle -= std::min(idle, begin.at_ns - start_begin);
        }
        // The park still OPEN at `end`, clipped to the window's start. When it is
        // the same park seen at `begin` this is the whole window, which is the
        // never-woke case a pure counter cannot see.
        if (start_end != 0) {
            const std::uint64_t from = std::max(start_end, begin.at_ns);
            if (end.at_ns > from) {
                idle += end.at_ns - from;
            }
        }
        total += idle;
    }
    return std::chrono::nanoseconds(static_cast<std::chrono::nanoseconds::rep>(total));
}

auto on_worker_pool_thread() noexcept -> bool {
    return t_on_pool_thread;
}

auto on_stage_thread() noexcept -> bool {
    return t_on_stage_thread;
}

StageThreadScope::StageThreadScope() noexcept {
    t_on_stage_thread = true;
    t_stage_ledger = &acquire_ledger<StageLedger>();
    interval_open(t_stage_ledger->live);
    const std::size_t live = g_stage_threads_live.fetch_add(1, std::memory_order_relaxed) + 1;
    // Raise the peak monotonically. A plain `store(max)` would race two threads
    // starting at once into losing one of the increments.
    std::size_t peak = g_stage_threads_peak.load(std::memory_order_relaxed);
    while (peak < live &&
           !g_stage_threads_peak.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
    }
}

StageThreadScope::~StageThreadScope() {
    if (t_stage_ledger != nullptr) {
        // Closing the park here is defence, not a live path: every park site
        // opens its interval through a stack-scoped `RingWaitScope`, whose
        // destructor closes it on the cancelled path as well as the normal one.
        // Deleting this line therefore breaks no test, which was verified rather
        // than assumed. It stays because the cost is one relaxed exchange at
        // thread exit and the failure it guards against is silent and
        // cumulative: a slot released with its park still open reads as parked
        // forever, inflating every later query's window with time no thread
        // spent. A future park site that forgets its scope should not be able to
        // corrupt an unrelated measurement.
        interval_close(t_stage_ledger->park);
        interval_close(t_stage_ledger->live);
        release_ledger(*t_stage_ledger);
        t_stage_ledger = nullptr;
    }
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
        impl_->threads.emplace_back([this, &ledger = acquire_ledger<IntervalLedger>()] {
            t_on_pool_thread = true;
            while (true) {
                Task task;
                {
                    std::unique_lock lock(impl_->mutex);
                    const auto ready = [this] { return impl_->stopping || !impl_->queue.empty(); };
                    // Only clock an actual park. When work is already queued the
                    // predicate is true on entry and this costs one branch, which
                    // matters because taking a task is the pool's hot path.
                    if (!ready()) {
                        interval_open(ledger);
                        impl_->work.wait(lock, ready);
                        interval_close(ledger);
                    }
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
    const std::size_t count = std::clamp<std::size_t>(worker_count, 1, threads_);
    auto state = std::make_shared<Batch::State>();
    state->body = std::move(body);
    state->remaining = count;
    state->profile_entry = current_execution_profile_entry();
    state->assist_one = [this] {
        Task task;
        {
            const std::lock_guard pool_lock(impl_->mutex);
            if (impl_->queue.empty()) {
                return false;
            }
            task = std::move(impl_->queue.front());
            impl_->queue.pop_front();
        }
        run_task(task);
        return true;
    };
    // A public submit is one fork-join round trip. TaskGroup uses the private
    // unbarriered submission below and accounts its single join at group wait.
    // Counting here needs no matching hook in Batch::wait() (which is
    // idempotent, and which the destructor may call again).
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

/// Block until every worker body of `state` has returned. A pool worker helps
/// its own pool while waiting, so a saturated set of parents can safely fork a
/// child batch instead of deadlocking with every thread parked.
///
/// One helper for all three wait sites (`wait()`, the move-assign, the
/// destructor) so none of them can be the one that forgets to account for
/// itself — a barrier the profile cannot see is exactly the blind spot this
/// measurement exists to remove.
///
/// The clock is read only when profiling installed an entry, so an unprofiled
/// run pays one null check per wait rather than two `steady_clock::now()`.
void wait_for_batch(WorkerPool::Batch::State& state, std::unique_lock<std::mutex>& lock) {
    if (!on_worker_pool_thread()) {
        if (state.profile_entry == nullptr || !state.account_wait) {
            state.done.wait(lock, [&state] { return state.remaining == 0; });
            return;
        }
        const auto start = std::chrono::steady_clock::now();
        state.done.wait(lock, [&state] { return state.remaining == 0; });
        record_execution_profile_barrier_wait(state.profile_entry,
                                              std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - start));
        return;
    }

    std::chrono::nanoseconds parked{0};
    while (state.remaining != 0) {
        lock.unlock();
        const bool helped = state.assist_one();
        lock.lock();
        if (helped || state.remaining == 0) {
            continue;
        }
        if (state.profile_entry == nullptr || !state.account_wait) {
            state.done.wait(lock, [&state] { return state.remaining == 0; });
        } else {
            const auto start = std::chrono::steady_clock::now();
            state.done.wait(lock, [&state] { return state.remaining == 0; });
            parked += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - start);
        }
    }
    if (state.profile_entry != nullptr && state.account_wait) {
        record_execution_profile_barrier_wait(state.profile_entry, parked);
    }
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

WorkerPool::TaskGroup::TaskGroup(TaskGroup&&) noexcept = default;

auto WorkerPool::TaskGroup::operator=(TaskGroup&& other) noexcept -> TaskGroup& {
    if (this != &other) {
        if (state_ != nullptr) {
            try {
                wait();
            } catch (...) {
                // Move assignment has the same no-throw ownership contract as
                // Batch: explicit wait() is where task errors are observed.
            }
        }
        state_ = std::move(other.state_);
    }
    return *this;
}

WorkerPool::TaskGroup::~TaskGroup() {
    if (state_ != nullptr) {
        try {
            wait();
        } catch (...) {
            // Destruction is a lifetime join, not an error-observation point.
        }
    }
}

void WorkerPool::TaskGroup::submit(std::function<void()> body) {
    if (state_ == nullptr || state_->waited) {
        invariant_violation("WorkerPool::TaskGroup::submit after wait");
    }
    state_->batches.push_back(state_->pool->submit_unbarriered(std::move(body)));
}

void WorkerPool::TaskGroup::wait() {
    if (state_ == nullptr || state_->waited) {
        return;
    }
    state_->waited = true;
    if (!state_->barrier_recorded && !state_->batches.empty()) {
        record_execution_profile_barrier(state_->profile_entry);
        state_->barrier_recorded = true;
    }
    const auto start = state_->profile_entry == nullptr ? std::chrono::steady_clock::time_point{}
                                                        : std::chrono::steady_clock::now();
    for (auto& batch : state_->batches) {
        batch.wait();
    }
    if (state_->profile_entry != nullptr && !state_->batches.empty()) {
        record_execution_profile_barrier_wait(state_->profile_entry,
                                              std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  std::chrono::steady_clock::now() - start));
    }
}

auto WorkerPool::task_group() -> TaskGroup {
    return TaskGroup{std::make_shared<TaskGroup::State>(this)};
}

auto WorkerPool::submit_unbarriered(std::function<void()> body) -> Batch {
    const std::size_t count = 1;
    auto state = std::make_shared<Batch::State>();
    state->body = [body = std::move(body)](std::size_t) { body(); };
    state->remaining = count;
    state->profile_entry = current_execution_profile_entry();
    state->account_wait = false;
    state->assist_one = [this] {
        Task task;
        {
            const std::lock_guard pool_lock(impl_->mutex);
            if (impl_->queue.empty()) {
                return false;
            }
            task = std::move(impl_->queue.front());
            impl_->queue.pop_front();
        }
        run_task(task);
        return true;
    };
    {
        const std::lock_guard lock(impl_->mutex);
        impl_->queue.push_back(Task{.state = state, .worker_id = 0});
    }
    impl_->work.notify_one();
    return Batch{std::move(state)};
}

auto ExecutionContext::can_fan_out() const -> bool {
    return compute_budget() >= 2;
}

auto ExecutionContext::compute_budget() const -> std::size_t {
    // Defined here rather than in the header because this is where the two
    // thread budgets are decided: `compute_thread_count` next to it, and
    // `decode_thread_count` below with the table showing why they differ. One
    // file owning both is what keeps a compute path from quietly acquiring the
    // decode budget, which is exactly what the open-coded fallback did.
    return parallel_threads != 0 ? parallel_threads : compute_thread_count();
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

auto existing_worker_pool_size() -> std::size_t {
    auto& state = process_worker_pool_state();
    const std::lock_guard lock(state.mutex);
    return state.pool == nullptr ? 0 : state.pool->size();
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
