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

/// Classify a node kind's execution capability — the *most* a node of this kind
/// may be. Expression-level constraints are checked by
/// analyze_parallel_island(), not duplicated at call sites.
[[nodiscard]] auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability;

/// Classify one node, which some kinds only answer with the node in hand.
///
/// `Update` is the case that needs it: a bare update is a barrier in general —
/// it may carry a `by` grouping, a `where` guard, or a table-valued tuple
/// assignment, none of which are row-local — but an unguarded, ungrouped update
/// whose every field is scalar-only *is* a row-local map. That conditional role
/// is exactly what the runtime multithreading plan asks for, and it is the
/// difference between an island that holds a query's arithmetic and one that
/// holds only the projection around it.
///
/// Every other kind defers to the kind-based classification above.
[[nodiscard]] auto execution_capability(const ir::Node& node) -> ExecutionCapability;

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
