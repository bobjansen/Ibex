// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

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

struct ColumnKernelSignature;

/// The construction function selected for one physical map step.  The
/// physical planner stores this pointer after proving the node's capability,
/// so its executor need not redispatch on the capability for every step.
/// `source_signature` is null only for compatibility callers outside a
/// physical plan (parallel islands); a physical map pipeline passes its
/// resolved source representations so construction can select a route once.
using MapKernelFactory = std::expected<OperatorPtr, std::string> (*)(
    const ir::Node&, OperatorPtr, const ScalarRegistry*, const ExternRegistry*,
    const ExecutionContext&, const std::vector<ColumnKernelSignature>*,
    bool preserve_empty_morsels);

struct MapKernelDispatch {
    MapKernelCapability capability = MapKernelCapability::FilterGather;
    MapKernelFactory factory = nullptr;

    friend auto operator==(const MapKernelDispatch&, const MapKernelDispatch&) -> bool = default;
};

/// Storage shape selected once when a physical map pipeline is constructed.
/// Fixed-width covers numeric and temporal columns; categorical values gather
/// their flat codes while retaining the shared dictionary separately.
enum class ColumnRepresentation : std::uint8_t {
    FixedWidth,
    PackedBool,
    StringSlabs,
    CategoricalCodes,
};

enum class KernelNullPolicy : std::uint8_t { AllValid, Nullable };

struct ColumnKernelSignature {
    ColumnRepresentation representation = ColumnRepresentation::FixedWidth;
    KernelNullPolicy null_policy = KernelNullPolicy::AllValid;

    friend auto operator==(const ColumnKernelSignature&, const ColumnKernelSignature&)
        -> bool = default;
};

/// Derive the representation/null-policy part of a kernel dispatch choice.
[[nodiscard]] auto column_kernel_signature(const ColumnValue& column,
                                           const std::optional<ValidityBitmap>& validity) noexcept
    -> ColumnKernelSignature;

/// The row-local kernel family a node can construct, or nullopt when the node
/// must remain outside this closed dispatch table.  A row-local Update is the
/// only conditional member; guarded/grouped/tuple/non-row-local forms decline.
[[nodiscard]] auto map_kernel_capability(const ir::Node& node) noexcept
    -> std::optional<MapKernelCapability>;

/// Return the static factory for a proven capability, or null when a caller
/// passes a value outside the closed vocabulary.
[[nodiscard]] auto map_kernel_factory(MapKernelCapability capability) noexcept -> MapKernelFactory;

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
