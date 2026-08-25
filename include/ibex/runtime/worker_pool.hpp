// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// Process-owned worker pool for the runtime's morsel pipelines
/// (`plans/runtime-multithreading-plan.md`, Phase 1).
///
/// Deliberately small and boring, as the plan requires: pre-spawned threads, a
/// single mutex+condvar task queue, and a completion latch. There is no work
/// stealing, no futures/continuations, and no DAG scheduler — the parallel
/// pipeline's coordination (a shared morsel cursor and an ordered merger) lives
/// in the pipeline operator, not here.
///
/// The one-query-at-a-time invariant (Phase 0 item 6, `query_lease.hpp`) is
/// what lets this stay simple: there is no cross-query fairness, priority, or
/// preemption to arbitrate.
class WorkerPool {
   public:
    /// A submitted batch of worker bodies. Move-only; `wait()` must complete
    /// before anything the body captured is destroyed, so the destructor waits.
    class Batch {
       public:
        /// Shared completion state. Public only so the pool's own task loop can
        /// name it; it has no user-facing API.
        struct State;

        Batch() = default;
        Batch(const Batch&) = delete;
        auto operator=(const Batch&) -> Batch& = delete;
        Batch(Batch&&) noexcept;
        auto operator=(Batch&&) noexcept -> Batch&;
        ~Batch();

        /// Blocks until every worker body has returned. Rethrows the first
        /// exception (by worker id) that escaped a body. Idempotent.
        void wait();

       private:
        friend class WorkerPool;
        explicit Batch(std::shared_ptr<State> state) : state_(std::move(state)) {}
        std::shared_ptr<State> state_;
    };

    explicit WorkerPool(std::size_t threads);
    WorkerPool(const WorkerPool&) = delete;
    auto operator=(const WorkerPool&) -> WorkerPool& = delete;
    WorkerPool(WorkerPool&&) = delete;
    auto operator=(WorkerPool&&) -> WorkerPool& = delete;
    ~WorkerPool();

    /// Number of pooled threads (>= 1).
    [[nodiscard]] auto size() const noexcept -> std::size_t { return threads_; }

    /// Runs `body(worker_id)` for `worker_id` in `[0, worker_count)` on pooled
    /// threads and returns immediately. The caller keeps consuming results
    /// while the batch runs, so this must not block.
    ///
    /// `worker_count` is clamped to `size()`. The calling thread is *not* one
    /// of the workers: in a morsel pipeline it is the ordered merger's
    /// consumer, and must stay free to release completed morsels.
    ///
    /// Reentrant: a pool worker waiting on a child batch cooperatively executes
    /// queued work until that batch completes. This prevents saturated parents
    /// from deadlocking their children, but callers should still avoid deep or
    /// tiny nested fan-outs when one outer batch would express the same work.
    [[nodiscard]] auto submit(std::size_t worker_count, std::function<void(std::size_t)> body)
        -> Batch;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    std::size_t threads_;
};

/// The lazily constructed process-wide pool. Sized by `decode_thread_count()`
/// on first use and reused for every query, so no query pays thread-creation
/// cost.
///
/// Like the query lease, the pool's mutable state lives in the host runtime TU
/// rather than an `inline` variable: bundled plugins statically link runtime
/// code and would otherwise each get their own pool (the RTLD_LOCAL trap).
[[nodiscard]] auto process_worker_pool() -> WorkerPool&;

/// The process-wide pool's thread count if it has already been created, or 0
/// otherwise -- never constructs it. `process_worker_pool().size()` reads the
/// same number but creates the pool as a side effect of asking; a caller that
/// only wants to know "how big would capacity accounting be" (the profiler)
/// must not spend a query's first pool-touch on a report line, or the new
/// pool's own thread-startup latency gets counted as idle time against a
/// window whose capacity was computed before the pool existed.
[[nodiscard]] auto existing_worker_pool_size() -> std::size_t;

/// Stop and join the process-wide worker pool, if it has been created.
///
/// Hosts which unload the Ibex shared library before their process exits (R's
/// namespace unload path is one example) must call this while code in the
/// library is still mapped.  Joining here ensures no worker can resume into
/// unloaded code.  A later call to process_worker_pool() creates a fresh pool,
/// which also makes this safe for an unload/reload development cycle.
void shutdown_process_worker_pool();

/// True when the calling thread belongs to a WorkerPool.
///
/// A scheduling-context query. Operators use this to avoid gratuitous nested
/// fan-out when an outer morsel batch already owns the available parallelism.
/// `WorkerPool` itself supports a necessary nested batch by cooperatively
/// executing queued work while its parent waits.
[[nodiscard]] auto on_worker_pool_thread() noexcept -> bool;

/// True on a runtime-owned thread that is NOT a pool worker.
///
/// The runtime has two species of thread and they are scheduled by different
/// rules. `WorkerPool` runs short, independent, non-blocking bodies to
/// completion and joins them; a **stage thread** is long-lived and blocks on
/// another thread's progress — `PipelinedStageOperator`'s producer parks on ring
/// backpressure until its consumer drains. A fixed-size pool cannot safely host
/// the second kind: N producers parked on backpressure, with a consumer that
/// needs a pool batch to drain them, is a deadlock, and the producer's child
/// chain would lose every fan-out to the `on_worker_pool_thread()` guard.
///
/// So the raw thread is correct, and this flag is what stops it from being
/// UNACCOUNTED. Before it existed the profiler credited a stage thread's work to
/// the calling thread's self time, concurrently with the real calling thread —
/// which made `self_ms` exceed `wall_ms` on exactly the queries that stage, and
/// inflated the measured serial fraction by at least 77ms across PDS-H SF-1.
///
/// A proper task scheduler names this distinction explicitly (Go hands off the M
/// on a blocking syscall; Tokio has a separate `spawn_blocking` pool). Until
/// then this is the minimum: the two kinds are distinguishable and countable.
[[nodiscard]] auto on_stage_thread() noexcept -> bool;

/// Marks the calling thread as a stage thread for the scope's lifetime, and
/// maintains the live/peak counts reported by `stage_thread_peak()`.
///
/// Construct it as the first statement of the thread body, so everything the
/// thread calls sees the flag.
class StageThreadScope {
   public:
    StageThreadScope() noexcept;
    ~StageThreadScope();
    StageThreadScope(const StageThreadScope&) = delete;
    auto operator=(const StageThreadScope&) -> StageThreadScope& = delete;
    StageThreadScope(StageThreadScope&&) = delete;
    auto operator=(StageThreadScope&&) -> StageThreadScope& = delete;
};

/// Most stage threads alive at once since process start.
///
/// Reported alongside the pool size because the process runs
/// `decode_thread_count()` pool threads PLUS one per staged breaker, and nothing
/// bounded — or even reported — the sum. Peak rather than live, because by the
/// time a query's profile prints, its stage threads have exited.
[[nodiscard]] auto stage_thread_peak() noexcept -> std::size_t;

/// A reading, at one instant, of how long each runtime thread has spent inside
/// some state — parked with an empty queue, parked on backpressure, or merely
/// alive.
///
/// This is how the profile accounts for time that belongs to no operator.
/// `pool_work_ms` says how long workers spent working and `pool_idle_ms` how
/// long they spent parked on ring BACKPRESSURE, but neither sees a thread parked
/// because the queue was simply EMPTY — which is most of the machine, and which
/// no operator can be charged for, since an empty pool belongs to nobody.
/// Sampling at query start and end attributes it to the query instead.
///
/// Sampled rather than accumulated because the interesting case is a thread that
/// enters the state before the query and is still in it when the query ends: it
/// never leaves, so it never gets the chance to add anything to a counter. The
/// open interval's start is what lets `idle_between` clip it to the window.
struct IdleSample {
    /// `steady_clock` reading when the sample was taken.
    std::uint64_t at_ns = 0;
    /// Per thread: `{time already closed, current interval's start or 0}`.
    std::vector<std::pair<std::uint64_t, std::uint64_t>> threads;
};

/// Total thread-time inside the sampled state between two samples, clipped to
/// the window at both ends.
///
/// Threads that appear only in `end` (the pool grew, a stage thread was spawned
/// mid-window) are counted from zero. The pairs are read without a lock held
/// across both fields, so an interval that closes between the two reads can be
/// miscounted by at most its own length, once per thread per sample —
/// irrelevant against a window of whole milliseconds, and never a systematic
/// bias in one direction.
[[nodiscard]] auto idle_between(const IdleSample& begin, const IdleSample& end)
    -> std::chrono::nanoseconds;

/// Pool threads parked with NOTHING QUEUED. Cheap (one mutex, no clock per
/// thread) and safe to call at any time, including before the pool exists.
[[nodiscard]] auto sample_pool_idle() -> IdleSample;

/// Stage threads parked on ring BACKPRESSURE — a producer that has run ahead of
/// its consumer and has nowhere to put the next chunk.
///
/// The pool's equivalent is `pool_idle_ms`, which `run_task` subtracts from
/// worker time. A stage thread has no `run_task` to subtract it from: its park
/// sits outside every profile scope, so before this it was not miscounted, it
/// was simply absent. Two thread species, two accounting paths, and the second
/// one was missing.
[[nodiscard]] auto sample_stage_park() -> IdleSample;

/// Stage threads merely EXISTING. The denominator for the other two stage
/// numbers: a stage thread's lifetime is its work plus its backpressure park, so
/// `stage_self_ms + stage_park_ms` should exhaust `stage_live_ms`, and the
/// profile prints all three so that it can be seen not to.
[[nodiscard]] auto sample_stage_live() -> IdleSample;

/// Open/close the calling thread's backpressure-park interval. No-ops off a
/// stage thread, so a caller that may run on any thread — `RingWaitScope` — can
/// call them unconditionally.
void stage_park_begin() noexcept;
void stage_park_end() noexcept;

/// Thread budget for compute, from `IBEX_CORES`. Unset or `auto` means
/// `std::thread::hardware_concurrency()`; an explicit count is used as given;
/// anything unparseable or zero falls back to 1.
///
/// This is the COMPUTE budget, and deliberately not the pool size — decode
/// wants more threads than cores and compute measurably does not, so the two
/// are separate numbers. `configure_parallel_from_env` pins
/// `ExecutionContext::parallel_threads` to this, which is what every compute
/// gate sizes itself from; `decode_thread_count()` sizes the pool.
///
/// Named `IBEX_CORES` rather than `IBEX_THREADS` because that is what it now
/// means. The old name said "threads" while setting a core count and a pool
/// size at once, which is exactly the conflation this split exists to undo.
[[nodiscard]] auto compute_thread_count() -> std::size_t;

/// Size of the process worker pool, chosen for DECODE rather than compute.
///
/// Two knobs, both for experiments rather than for users:
///   * `IBEX_DECODE_THREADS` — absolute pool size, bypassing the policy.
///   * `IBEX_DECODE_SATURATION` — the point past which extra decode threads
///     stop paying (default 8). It is a property of the box's memory system,
///     so sweep it on a new machine rather than trusting the default.
/// Decode is memory-latency bound and wants more threads than cores up to the
/// point the memory system saturates; compute does not, and clamps to
/// `ExecutionContext::parallel_threads` instead. `IBEX_DECODE_THREADS` overrides
/// with an absolute count (for A/B, and for boxes whose saturation point
/// differs). See the definition for the measurements behind the policy.
[[nodiscard]] auto decode_thread_count() -> std::size_t;

/// Whether `IBEX_STREAM_SCAN` asks for lazy sources to be streamed through
/// their scan operator rather than decoded whole. Same three-state contract and
/// same spellings as `stream_scans_from_env`'s siblings.
///
/// `configure_parallel_from_env` applies this to `ExecutionContext::
/// stream_scans`, which is the only thing the scan seams read. Nothing else may
/// call `getenv` for it: the three seams that consult the setting have to
/// agree, and a second reader is a second authority free to disagree.
[[nodiscard]] auto stream_scans_from_env() -> std::optional<bool>;

/// Whether `IBEX_JOIN_PROBE` asks for join probes to fan out across worker
/// ranges. Same three-state contract and same spellings as
/// the shared on/off spelling; applied to
/// `ExecutionContext::parallel_join_probe`.
[[nodiscard]] auto parallel_join_probe_from_env() -> std::optional<bool>;

/// Rows per SOURCE chunk from `IBEX_CHUNK_ROWS`, or 0 when unset/invalid.
///
/// Zero means "one chunk", which is what production does today. A non-zero
/// value makes every materialized source emit successive row ranges instead —
/// see `make_table_source` and Phase 0 of `plans/pipelined-execution-plan.md`.
/// It exists to EXERCISE the operators' cross-chunk paths, which no production
/// query has ever reached, and is read fresh on every call so a test can change
/// it between pipelines.
[[nodiscard]] auto source_chunk_rows_from_env() -> std::size_t;

/// Morsel row-grain override from `IBEX_MORSEL_ROWS`, or 0 when unset/invalid
/// (meaning: keep the `ExecutionContext` default).
[[nodiscard]] auto morsel_rows_from_env() -> std::size_t;

}  // namespace ibex::runtime
