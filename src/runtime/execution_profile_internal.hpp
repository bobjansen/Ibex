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
    std::uint64_t pool_work_ns = 0;
    std::uint64_t calls = 0;
    std::uint64_t chunks = 0;
    std::uint64_t rows = 0;
    std::uint64_t pool_thread_calls = 0;
    std::uint64_t pool_tasks = 0;
};

/// Query-scoped state behind the opt-in IBEX_PROFILE_OPERATORS report.
/// Kept internal so profiling adds no public API surface beyond the opaque
/// pointer carried by ExecutionContext.
class ExecutionProfileState {
   public:
    explicit ExecutionProfileState(bool report = true);
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

[[nodiscard]] auto execution_profile_requested() noexcept -> bool;

}  // namespace ibex::runtime
