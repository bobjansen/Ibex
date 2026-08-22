// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

#include <cstdint>
#include <optional>
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

/// Closed construction-time vocabulary for the row-local kernel family.  This
/// is intentionally narrower than `ExecutionCapability`: it says which
/// already-ported kernel composition constructs an operator, while the latter
/// describes scheduling/barrier behaviour for every logical node.
enum class MapKernelCapability : std::uint8_t {
    FilterGather,
    MetadataMap,
    RowLocalUpdate,
    FilterProjectGather,
    FilterUpdateProjectGather,
};

/// The row-local kernel family a node can construct, or nullopt when the node
/// must remain outside this closed dispatch table.  A row-local Update is the
/// only conditional member; guarded/grouped/tuple/non-row-local forms decline.
[[nodiscard]] auto map_kernel_capability(const ir::Node& node) noexcept
    -> std::optional<MapKernelCapability>;

/// Classify a node kind's execution capability — the *most* a node of this kind
/// may be. Expression-level constraints are checked by
/// analyze_parallel_island(), not duplicated at call sites.
[[nodiscard]] auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability;

/// True when a node relabels or selects columns without touching a row.
///
/// `project_table` and `rename_table` both build their output with
/// `add_column_shared` — they copy no rows at all, and cost O(columns) rather
/// than O(rows). Two callers depend on that: `analyze_parallel_island` refuses
/// a chain made only of these (there is nothing to parallelize), and the
/// two-phase filter runs them **once over its finished output** rather than
/// per morsel, which is what lets a `filter … rename` chain keep the fast path.
[[nodiscard]] auto is_metadata_only_node(ir::NodeKind kind) noexcept -> bool;

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
    /// Every operator in the chain is metadata-only, so there is no per-row
    /// work to spread across threads — see `analyze_parallel_island`.
    NoRowWork,
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
