#pragma once

#include <ibex/ir/node.hpp>

#include <cstdint>
#include <vector>

namespace ibex::runtime {

/// Execution capability used by the physical operator builder. This is a
/// single vocabulary for what a future executor is permitted to do with an
/// operation.
enum class ExecutionCapability : std::uint8_t {
    ParallelMap,
    OrderedStream,
    Barrier,
    ParallelBarrier,
};

/// Classify a node's execution capability. Expression-level constraints are
/// checked by analyze_parallel_island(), not duplicated at call sites.
[[nodiscard]] auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability;

enum class ParallelEligibilityReason : std::uint8_t {
    Eligible,
    NotParallelMap,
    UnsupportedExpression,
    UnsupportedShape,
};

/// A maximal, bottom-up chain of parallel-map candidates rooted at an IR node.
/// `operators` is ordered source-to-sink; `input` is the subtree that must
/// provide the materialized input table when a parallel executor is added.
struct ParallelIslandCandidate {
    std::vector<const ir::Node*> operators;
    const ir::Node* input = nullptr;
    ParallelEligibilityReason reason = ParallelEligibilityReason::NotParallelMap;

    [[nodiscard]] auto eligible() const noexcept -> bool {
        return reason == ParallelEligibilityReason::Eligible;
    }
};

/// The sole expression-aware eligibility analysis for a parallel map island.
/// Unknown calls (including externs/plugins), generators, transforms, ranks,
/// and aggregates make the candidate serial-only through
/// ir::is_subset_evaluable_expr().
[[nodiscard]] auto analyze_parallel_island(const ir::Node& root) -> ParallelIslandCandidate;

}  // namespace ibex::runtime
