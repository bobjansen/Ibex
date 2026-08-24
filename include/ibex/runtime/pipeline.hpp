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

struct MapStep;

using MapKernelFactory = std::expected<OperatorPtr, std::string> (*)(
    const MapStep&, OperatorPtr, const ScalarRegistry*, const ExternRegistry*,
    const ExecutionContext&, const std::vector<ColumnKernelSignature>*,
    bool preserve_empty_morsels);

/// One step of a physical map pipeline: the IR node it executes and the kernel
/// proven able to execute it. Node and dispatch travel together because they
/// are one decision — a step whose factory belonged to a different node was a
/// silent possibility while the planner kept them in two vectors indexed
/// alike.
struct MapStep {
    /// The step's leading node, and the one its capability was proven from.
    /// For a planner-fused shape this is the `Filter`, which runs first.
    const ir::Node* node = nullptr;
    /// Further IR nodes this step executes in the same pass, fused by the
    /// planner rather than by canonicalize. Both null for an unfused step,
    /// including one whose node is already a fused IR kind. The shapes are
    /// `Project(Filter(x))` and `Project(Update(Filter(x)))` — the same two
    /// canonicalize R5 and R6 rewrite, expressed physically.
    const ir::Node* fused_update = nullptr;
    const ir::Node* fused_project = nullptr;
    MapKernelCapability capability = MapKernelCapability::FilterGather;
    MapKernelFactory factory = nullptr;

    friend auto operator==(const MapStep&, const MapStep&) -> bool = default;
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
/// `map_step_expressions_are_subset_evaluable`, not duplicated at call sites.
[[nodiscard]] auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability;

/// True when a node relabels or selects columns without touching a row.
///
/// `project_table` and `rename_table` both build their output with
/// `add_column_shared` — they copy no rows at all, and cost O(columns) rather
/// than O(rows). Two callers depend on that: a physical pipeline made only of
/// these is serial-only (there is nothing to parallelize), and the
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

/// Whether every expression in one map step can be evaluated over a subset of
/// rows. Unknown calls (including externs/plugins), generators, transforms,
/// ranks, and aggregates cannot, which is what makes a chain serial-only.
///
/// Public because the physical planner decides a pipeline's execution mode with
/// it: there is one definition of "this step may run per morsel", not one per
/// analysis.
[[nodiscard]] auto map_step_expressions_are_subset_evaluable(const ir::Node& node) -> bool;

}  // namespace ibex::runtime
