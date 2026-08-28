// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <array>
#include <optional>
#include <string>
#include <variant>

namespace ibex::runtime {

auto map_kernel_capability(const ir::Node& node) noexcept -> std::optional<MapKernelCapability> {
    struct Entry {
        ir::NodeKind kind;
        MapKernelCapability capability;
    };
    // The unconditional members of the closed family.  This is consulted once
    // at pipeline construction and copied into physical::Plan, never per row.
    static constexpr std::array kTable{
        Entry{.kind = ir::NodeKind::Filter, .capability = MapKernelCapability::FilterGather},
        Entry{.kind = ir::NodeKind::Project, .capability = MapKernelCapability::MetadataMap},
        Entry{.kind = ir::NodeKind::Rename, .capability = MapKernelCapability::MetadataMap},
    };
    for (const auto& entry : kTable) {
        if (entry.kind == node.kind()) {
            return entry.capability;
        }
    }
    if (node.kind() == ir::NodeKind::Update) {
        const auto& update = ir::node_cast<ir::UpdateNode>(node);
        if (update.guard() == nullptr && update.group_by().empty() &&
            update.tuple_fields().empty() &&
            std::ranges::all_of(update.fields(), [](const ir::FieldSpec& field) {
                return ir::is_row_local_update_expr(field.expr);
            })) {
            return MapKernelCapability::RowLocalUpdate;
        }
    }
    return std::nullopt;
}

auto column_kernel_signature(const ColumnValue& column,
                             const std::optional<ValidityBitmap>& validity) noexcept
    -> ColumnKernelSignature {
    const auto representation = [&] {
        if (std::holds_alternative<Column<bool>>(column)) {
            return ColumnRepresentation::PackedBool;
        }
        if (std::holds_alternative<Column<std::string>>(column)) {
            return ColumnRepresentation::StringSlabs;
        }
        if (std::holds_alternative<Column<Categorical>>(column)) {
            return ColumnRepresentation::CategoricalCodes;
        }
        return ColumnRepresentation::FixedWidth;
    }();
    return {.representation = representation,
            .null_policy =
                validity.has_value() ? KernelNullPolicy::Nullable : KernelNullPolicy::AllValid};
}

auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability {
    switch (kind) {
        case ir::NodeKind::Filter:
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
            return ExecutionCapability::ParallelMap;

        case ir::NodeKind::Head:
        case ir::NodeKind::Tail:
        case ir::NodeKind::FilterHead:
        case ir::NodeKind::FilterTail:
            return ExecutionCapability::OrderedStream;

        case ir::NodeKind::Aggregate:
        case ir::NodeKind::Order:
        case ir::NodeKind::TopK:
        case ir::NodeKind::Distinct:
        case ir::NodeKind::Join:
            return ExecutionCapability::ParallelBarrier;

        default:
            return ExecutionCapability::Barrier;
    }
}

auto is_metadata_only_node(ir::NodeKind kind) noexcept -> bool {
    return kind == ir::NodeKind::Project || kind == ir::NodeKind::Rename;
}

auto map_step_expressions_are_subset_evaluable(const ir::Node& node) -> bool {
    switch (node.kind()) {
        case ir::NodeKind::Filter:
            return ir::is_subset_evaluable_expr(ir::node_cast<ir::FilterNode>(node).predicate());
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
            return true;
        default:
            return false;
    }
}

auto execution_capability(const ir::Node& node) -> ExecutionCapability {
    // A bare `update` is deliberately NOT a ParallelMap, even though it is
    // row-local and an earlier slice did admit it here.
    //
    // An update is 1:1, and `update_table` builds its output by moving the
    // input, so a passthrough column costs nothing. Running one through a
    // morsel pipeline therefore adds two whole-table copies (the per-morsel
    // gather and the merge concat) to buy parallelism over the computed column
    // alone — and `update_table` can now split that computation across threads
    // by itself, with no copies at all. Measured on 20M rows, net of
    // generation: a heavy update over six columns is 0.32s serial, 0.72s as an
    // pipeline, 0.08s split inside the operator; over two columns, 0.29s / 0.27s
    // / 0.09s. The pipeline loses on the wide table and wins nothing on the
    // narrow one.
    //
    // Filter-shaped nodes stay ParallelMap: their cardinality is
    // data-dependent, so they cannot presize an output, and the pipeline's
    // ordered merger is what resolves that.
    return execution_capability(node.kind());
}

}  // namespace ibex::runtime
