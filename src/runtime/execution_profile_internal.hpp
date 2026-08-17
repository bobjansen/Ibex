// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/operator.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::runtime {

struct ExecutionProfileEntry;

struct ExecutionProfileSnapshotRow {
    std::uint64_t node_id = 0;
    std::string label;
    std::uint64_t build_self_ns = 0;
    std::uint64_t next_self_ns = 0;
    std::uint64_t source_self_ns = 0;
    std::uint64_t span_ns = 0;
    std::uint64_t pool_next_ns = 0;
    std::uint64_t pool_source_ns = 0;
    std::uint64_t pool_work_ns = 0;
    std::uint64_t calls = 0;
    std::uint64_t chunks = 0;
    std::uint64_t rows = 0;
    std::uint64_t pool_thread_calls = 0;
    std::uint64_t pool_tasks = 0;
    std::uint64_t barriers = 0;
    std::uint64_t barrier_wait_ns = 0;
    std::uint64_t stage_self_ns = 0;
};

/// Share of the machine an operator kept busy while it ran: 0 means it was
/// handed no worker at all, 1 means it filled every one of them for its whole
/// span. Turns the binary "no worker work" observation into a number, so an
/// operator that got partial help stops looking like one that got none.
[[nodiscard]] auto profile_row_occupancy(const ExecutionProfileSnapshotRow& row,
                                         std::size_t workers) -> double;

/// Plan-level totals derived from a profile.
///
/// Everything here is computed from SELF time, never from `span_ns`. Spans are
/// inclusive and nest, so summing them across rows double-counts every parent.
/// Self times are exclusive by construction, so they add up: their total is the
/// profiled main-thread work, and the share of it that drew no worker help is
/// the *measured* serial fraction — which is what predicts the ceiling on a
/// wider machine, rather than inferring it backwards from an observed speedup.
struct ExecutionProfileSummary {
    double self_ms = 0.0;
    /// Main-thread time that was NOT parked at a barrier.
    ///
    /// This used to be "self time of operators that were handed no worker at
    /// all", a per-operator binary. That understated the residue in exactly the
    /// interesting case: an operator that fans out contributed ZERO, even
    /// though its self time still contains the serial phases between its
    /// barriers (a prefix sum, a first-occurrence merge, slot growth) and the
    /// time parked in `wait()`. Now every operator contributes
    /// `self - barrier_wait`, so a partly-parallel operator's serial residue is
    /// visible instead of rounded away.
    double serial_self_ms = 0.0;
    /// Main-thread time parked inside `Batch::wait()`, across every operator.
    ///
    /// The scheduler question in one number. Serial residue that is mostly THIS
    /// is reclaimable by a `join` that participates in the work queue instead
    /// of blocking; residue that is mostly `serial_self_ms` is not, and wants
    /// algorithmic work instead. Nothing could tell the two apart before.
    double barrier_wait_ms = 0.0;
    /// Fork-join round trips the query issued.
    std::uint64_t barriers = 0;
    /// `serial_self_ms / self_ms`.
    double serial_fraction = 0.0;
    /// Amdahl's limit at unbounded cores, `1 / serial_fraction`. Zero when
    /// nothing serial was measured, meaning "no ceiling observed".
    double amdahl_ceiling = 0.0;
    double pool_work_ms = 0.0;
    /// Time that ran on a stage thread (a `PipelinedStageOperator` producer).
    /// Excluded from `self_ms`, and reported so the exclusion is visible: this
    /// number used to be silently folded into `serial_self_ms`, which is what
    /// made `self_ms` exceed `wall_ms` on a query that stages a breaker.
    double stage_self_ms = 0.0;
    /// Whole-query occupancy: `pool_work_ms / (wall_ms * workers)`.
    double occupancy = 0.0;
};

[[nodiscard]] auto summarize_execution_profile(const std::vector<ExecutionProfileSnapshotRow>& rows,
                                               double wall_ms, std::size_t workers)
    -> ExecutionProfileSummary;

/// Query-scoped state behind the opt-in IBEX_PROFILE_OPERATORS report.
/// Kept internal so profiling adds no public API surface beyond the opaque
/// pointer carried by ExecutionContext.
class ExecutionProfileState {
   public:
    /// `worker_budget` is the thread count occupancy is measured against —
    /// how many workers this query was allowed, not how many exist.
    explicit ExecutionProfileState(std::size_t worker_budget = 1, bool report = true);
    ExecutionProfileState(const ExecutionProfileState&) = delete;
    auto operator=(const ExecutionProfileState&) -> ExecutionProfileState& = delete;
    ~ExecutionProfileState();

    [[nodiscard]] auto entry(std::uint64_t node_id, std::string label) -> ExecutionProfileEntry*;
    [[nodiscard]] auto stage(std::string_view label) -> ExecutionProfileEntry*;
    [[nodiscard]] auto snapshot() const -> std::vector<ExecutionProfileSnapshotRow>;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

enum class ProfilePhase : std::uint8_t { Build, Next, Source };

/// Nest-aware timer. Exclusive/self time subtracts child scopes on the same
/// thread; inclusive span remains available to expose critical-path latency.
class ExecutionProfileScope {
   public:
    struct Frame;

    ExecutionProfileScope(ExecutionProfileEntry* entry, ProfilePhase phase);
    ExecutionProfileScope(const ExecutionProfileScope&) = delete;
    auto operator=(const ExecutionProfileScope&) -> ExecutionProfileScope& = delete;
    ~ExecutionProfileScope();

   private:
    ExecutionProfileEntry* entry_ = nullptr;
    ProfilePhase phase_ = ProfilePhase::Next;
    std::chrono::steady_clock::time_point start_;
    std::unique_ptr<Frame> frame_;
};

/// Wrap an operator so each pull records inclusive span, exclusive work on the
/// calling thread, rows/chunks, and whether it ran inside a pool task.
[[nodiscard]] auto profile_operator(OperatorPtr op, std::shared_ptr<ExecutionProfileState> profile,
                                    const ir::Node& node) -> OperatorPtr;
[[nodiscard]] auto execution_profile_entry(const std::shared_ptr<ExecutionProfileState>& profile,
                                           const ir::Node& node) -> ExecutionProfileEntry*;

/// The worker pool attributes each submitted task to the operator scope that
/// submitted it. This is separate from operator self time: it is the summed
/// worker occupancy used to estimate available parallelism.
[[nodiscard]] auto current_execution_profile_entry() noexcept -> ExecutionProfileEntry*;
void record_execution_profile_worker(ExecutionProfileEntry* entry,
                                     std::chrono::nanoseconds elapsed) noexcept;

/// Count one fork-join round trip against the operator that issued it.
void record_execution_profile_barrier(ExecutionProfileEntry* entry) noexcept;

/// Add wall time the submitting thread spent parked in `Batch::wait()`.
///
/// Separate from `record_execution_profile_worker`, and the distinction is the
/// whole point of measuring it: worker time is work that happened, barrier wait
/// is a thread doing nothing while it happens. The summary subtracts this from
/// self time, so the reported serial fraction stops counting a barrier park as
/// serial compute.
void record_execution_profile_barrier_wait(ExecutionProfileEntry* entry,
                                           std::chrono::nanoseconds elapsed) noexcept;

[[nodiscard]] auto execution_profile_requested() noexcept -> bool;

}  // namespace ibex::runtime
