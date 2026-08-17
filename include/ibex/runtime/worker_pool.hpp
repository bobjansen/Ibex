// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>

namespace ibex::runtime {

/// Process-owned worker pool for the runtime's parallel islands
/// (`plans/runtime-multithreading-plan.md`, Phase 1).
///
/// Deliberately small and boring, as the plan requires: pre-spawned threads, a
/// single mutex+condvar task queue, and a completion latch. There is no work
/// stealing, no futures/continuations, and no DAG scheduler — the parallel
/// island's coordination (a shared morsel cursor and an ordered merger) lives
/// in the island operator, not here.
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
    /// of the workers: in a parallel island it is the ordered merger's
    /// consumer, and must stay free to release completed morsels.
    ///
    /// Not reentrant: a worker body must never submit its own batch (nested
    /// islands are not a Phase 1 shape and would deadlock against a saturated
    /// pool).
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
/// The guard against nested parallelism. Anything that may run inside a pool
/// task — an operator in a parallel island, and so everything it calls — must
/// check this before submitting work of its own, because `WorkerPool::submit`
/// from a worker deadlocks (and aborts loudly rather than hanging).
[[nodiscard]] auto on_worker_pool_thread() noexcept -> bool;

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

/// Whether `IBEX_PARALLEL` asks the interpreter to enable parallel islands.
/// `nullopt` when `IBEX_PARALLEL` is unset or unrecognized, so the caller keeps
/// whatever it already chose; otherwise the requested setting.
///
/// It answers BOTH ways on purpose. Parallel islands are on by default now, so
/// a switch that could only turn them on would leave no way to turn them off —
/// which is what a user hitting a threading bug, or an A/B measuring the
/// feature, actually needs. Accepts `1`/`on`/`true`/`yes` and
/// `0`/`off`/`false`/`no`.
[[nodiscard]] auto parallel_enabled_from_env() -> std::optional<bool>;

/// Whether `IBEX_STREAM_SCAN` asks for lazy sources to be streamed through
/// their scan operator rather than decoded whole. Same three-state contract and
/// same spellings as `parallel_enabled_from_env`.
///
/// `configure_parallel_from_env` applies this to `ExecutionContext::
/// stream_scans`, which is the only thing the scan seams read. Nothing else may
/// call `getenv` for it: the three seams that consult the setting have to
/// agree, and a second reader is a second authority free to disagree.
[[nodiscard]] auto stream_scans_from_env() -> std::optional<bool>;

/// Whether `IBEX_JOIN_PROBE` asks for join probes to fan out across worker
/// ranges. Same three-state contract and same spellings as
/// `parallel_enabled_from_env`; applied to
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
