// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// materialized_node.cpp — run one node the physical plan has no streaming
// operator for: drain its inputs through the physical path (`materialize_plan`)
// and call its whole-table kernel (reshape, stats, window, resample, grouped or
// non-row-local update, materializing join, `MaterializeAll` aggregate, matmul,
// model, construct, stream, program, extern call, bare scan). Reached only from
// `build_materialized_fallback`.
//
// This used to be `run_materialized_node`, a second executor that walked the whole
// tree beneath such a node itself, so joins, scans and filters under it lost
// the physical path (2026-10-04: a join under `rbind` or a declined aggregate
// lost its streaming join and deferred probe).

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"
#include "join_internal.hpp"
#include "model_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

auto map_column_from_scalars(const std::vector<ScalarValue>& values)
    -> std::expected<ComputedColumn, std::string> {
    const auto build = [&]<class T>() -> std::expected<ComputedColumn, std::string> {
        Column<T> column;
        column.reserve(values.size());
        ValidityBitmap validity;
        validity.reserve(values.size());
        bool any_null = false;
        for (const auto& value : values) {
            if (is_null_scalar(value)) {
                column.push_back(T{});
                validity.push_back(false);
                any_null = true;
                continue;
            }
            if (const auto* typed = std::get_if<T>(&value)) {
                column.push_back(*typed);
            } else if constexpr (std::is_same_v<T, double>) {
                if (const auto* integer = std::get_if<std::int64_t>(&value)) {
                    column.push_back(static_cast<double>(*integer));
                } else {
                    return std::unexpected("mixed value types in one map field");
                }
            } else {
                return std::unexpected("mixed value types in one map field");
            }
            validity.push_back(true);
        }
        return ComputedColumn{.column = ColumnValue{std::move(column)},
                              .validity = any_null
                                              ? std::optional<ValidityBitmap>{std::move(validity)}
                                              : std::nullopt};
    };

    for (const auto& value : values) {
        if (std::holds_alternative<std::int64_t>(value)) {
            const bool has_double = std::ranges::any_of(values, [](const ScalarValue& candidate) {
                return std::holds_alternative<double>(candidate);
            });
            return has_double ? build.template operator()<double>()
                              : build.template operator()<std::int64_t>();
        }
        if (std::holds_alternative<double>(value)) {
            return build.template operator()<double>();
        }
        if (std::holds_alternative<bool>(value)) {
            return build.template operator()<bool>();
        }
        if (std::holds_alternative<std::string>(value)) {
            return build.template operator()<std::string>();
        }
        if (std::holds_alternative<Date>(value)) {
            return build.template operator()<Date>();
        }
        if (std::holds_alternative<Timestamp>(value)) {
            return build.template operator()<Timestamp>();
        }
    }
    return build.template operator()<std::int64_t>();
}

}  // namespace

// NOLINTNEXTLINE(readability-function-size)
auto run_materialized_node(const ir::Node& node, const TableRegistry& registry,
                           const ScalarRegistry* scalars, const ExternRegistry* externs,
                           const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    // Cooperative interruption boundary: a Ctrl+C during a long-running plan
    // unwinds here between operators rather than killing the process.
    if (interrupt_requested()) {
        return std::unexpected(interrupt_message());
    }
    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = ir::node_cast<ir::ScanNode>(node);
            auto it = registry.find(scan.source_name());
            if (it == registry.end()) {
                // A deferred lazy source has no registry entry; decode it here
                // with whatever bounds its join has published so far.
                if (const auto* deferred = exec.deferred_scan(scan.source_name());
                    deferred != nullptr) {
                    auto table = materialize_deferred_scan(*deferred, exec);
                    if (!table.has_value()) {
                        return std::unexpected(std::move(table.error()));
                    }
                    normalize_time_index(table.value());
                    return std::move(table.value());
                }
                return std::unexpected("unknown table: " + scan.source_name() +
                                       " (available: " + format_tables(registry) + ")");
            }
            Table output = it->second;
            normalize_time_index(output);
            return output;
        }
        case ir::NodeKind::Update: {
            const auto& update = ir::node_cast<ir::UpdateNode>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            auto child =
                materialize_plan(*update.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            if (update.guard() != nullptr) {
                return apply_guarded_update(std::move(child.value()), update, scalars, externs,
                                            exec);
            }
            if (!update.group_by().empty()) {
                const bool all_rank = std::all_of(
                    update.fields().begin(), update.fields().end(), [](const ir::FieldSpec& f) {
                        return std::holds_alternative<ir::RankExpr>(f.expr.node);
                    });
                // Pure-rank grouped update has a fast path: rank only needs
                // group keys + ordering, so it skips the gather/scatter dance.
                if (all_rank && update.tuple_fields().empty()) {
                    Table result = std::move(child.value());
                    for (const auto& field : update.fields()) {
                        const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node);
                        auto res = evaluate_rank_column(result, *rank, update.group_by(), exec);
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        if (res->validity.has_value()) {
                            result.add_column(field.alias, std::move(res->column),
                                              std::move(*res->validity));
                        } else {
                            result.add_column(field.alias, std::move(res->column));
                        }
                    }
                    return result;
                }
                if (!update.tuple_fields().empty()) {
                    return std::unexpected(
                        "update + by: tuple-bound fields are not yet supported in grouped updates");
                }
                return grouped_update_table(std::move(child.value()), update.fields(),
                                            update.group_by(), scalars, externs, exec);
            }
            auto result =
                update_table(std::move(child.value()), update.fields(), scalars, externs, exec);
            if (!result) {
                return result;
            }
            for (const auto& tspec : update.tuple_fields()) {
                auto src = materialize_plan(*tspec.source, registry, scalars, externs, exec);
                if (!src) {
                    return std::unexpected(src.error());
                }
                if (tspec.aliases.empty()) {
                    // `update = expr`: merge all columns from the source table.
                    for (const auto& entry : src->columns) {
                        if (entry.validity) {
                            result->add_column(entry.name, *entry.column, *entry.validity);
                        } else {
                            result->add_column(entry.name, *entry.column);
                        }
                    }
                } else {
                    if (src->columns.size() != tspec.aliases.size()) {
                        return std::unexpected(
                            "tuple assignment: expected " + std::to_string(tspec.aliases.size()) +
                            " column(s), got " + std::to_string(src->columns.size()));
                    }
                    for (std::size_t i = 0; i < tspec.aliases.size(); ++i) {
                        const auto& entry = src->columns[i];
                        if (entry.validity) {
                            result->add_column(tspec.aliases[i], *entry.column, *entry.validity);
                        } else {
                            result->add_column(tspec.aliases[i], *entry.column);
                        }
                    }
                }
            }
            return result;
        }
        case ir::NodeKind::Map: {
            const auto& map_node = ir::node_cast<ir::MapNode>(node);
            if (map_node.children().empty()) {
                return std::unexpected("map node missing child");
            }
            auto child =
                materialize_plan(*map_node.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            // Map is deliberately row-major and serial: field expressions may
            // perform observable I/O, so both row and declaration order are
            // language semantics rather than a parallelization opportunity.
            std::vector<std::vector<ScalarValue>> values(map_node.fields().size());
            for (auto& field_values : values) {
                field_values.reserve(child->rows());
            }
            for (std::size_t row = 0; row < child->rows(); ++row) {
                if (interrupt_requested()) {
                    return std::unexpected(interrupt_message());
                }
                for (std::size_t field = 0; field < map_node.fields().size(); ++field) {
                    auto value =
                        eval_expr(map_node.fields()[field].expr, *child, row, scalars, externs);
                    if (!value) {
                        if (const auto* ref =
                                std::get_if<ir::ColumnRef>(&map_node.fields()[field].expr.node);
                            ref != nullptr && registry.contains(ref->name)) {
                            return std::unexpected("map field '" + map_node.fields()[field].alias +
                                                   "' must evaluate to a scalar, not a table");
                        }
                        return std::unexpected("map field '" + map_node.fields()[field].alias +
                                               "': " + value.error());
                    }
                    values[field].push_back(scalar_from_expr(*value));
                }
            }
            Table out;
            for (std::size_t field = 0; field < map_node.fields().size(); ++field) {
                auto column = map_column_from_scalars(values[field]);
                if (!column) {
                    return std::unexpected("map field '" + map_node.fields()[field].alias +
                                           "': " + column.error());
                }
                if (column->validity) {
                    out.add_column(map_node.fields()[field].alias, std::move(column->column),
                                   std::move(*column->validity));
                } else {
                    out.add_column(map_node.fields()[field].alias, std::move(column->column));
                }
            }
            return out;
        }
        case ir::NodeKind::Aggregate: {
            const auto& agg = ir::node_cast<ir::AggregateNode>(node);
            if (agg.children().empty()) {
                return std::unexpected("aggregate node missing child");
            }
            // With no `by`, an empty input still yields one row (count 0,
            // everything else null). The kernels return zero rows; fix it here.
            const auto one_group_if_ungrouped =
                [&](std::expected<Table, std::string> result) -> std::expected<Table, std::string> {
                if (!result.has_value() || !agg.group_by().empty() || result->rows() != 0 ||
                    result->columns.empty()) {
                    return result;
                }
                Table row;
                for (auto& entry : global_aggregate_of_empty(result->columns, agg.aggregations())) {
                    row.add_column_shared(std::move(entry.name), std::move(entry.column),
                                          std::move(entry.validity));
                }
                return row;
            };
            // Fast path: Aggregate(Scan) — pass the registry table by const ref to skip the copy.
            const ir::Node& child_node = *agg.children().front();
            // A scan the registry does not hold (a deferred lazy source) takes
            // the ordinary input path below.
            if (child_node.kind() == ir::NodeKind::Scan) {
                const auto& scan = ir::node_cast<ir::ScanNode>(child_node);
                if (auto it = registry.find(scan.source_name()); it != registry.end()) {
                    return one_group_if_ungrouped(
                        aggregate_table(it->second, agg.group_by(), agg.aggregations(), &exec));
                }
            }
            // Only `MaterializeAll` aggregates reach here: `plan_aggregate`
            // migrates the left-join-count fusion and every streamable one.
            auto child = materialize_plan(child_node, registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return one_group_if_ungrouped(
                aggregate_table(child.value(), agg.group_by(), agg.aggregations(), &exec));
        }
        case ir::NodeKind::Resample: {
            const auto& rs = ir::node_cast<ir::ResampleNode>(node);
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child.has_value())
                return child;
            return resample_table(child.value(), rs.duration(), rs.group_by(), rs.aggregations());
        }
        case ir::NodeKind::Window: {
            const auto& win = ir::node_cast<ir::WindowNode>(node);
            const ir::Node& child_node = *node.children().front();
            // The child must be an UpdateNode produced by the `update` clause.
            if (child_node.kind() != ir::NodeKind::Update) {
                return std::unexpected(
                    "window: only 'update' is currently supported inside a window block");
            }
            const auto& update_node = ir::node_cast<ir::UpdateNode>(child_node);
            // Neither is applied by the windowed evaluators; dropping them quietly
            // returned a table with the columns or rows the user asked for missing.
            if (!update_node.tuple_fields().empty() || update_node.guard() != nullptr) {
                return std::unexpected(
                    "window: a window update does not support tuple fields or a `where` guard");
            }
            // Evaluate the source (grandchild) without the window context.
            auto source =
                materialize_plan(*child_node.children().front(), registry, scalars, externs, exec);
            if (!source.has_value()) {
                return source;
            }
            if (!source->time_index().has_value()) {
                return std::unexpected(
                    "window requires a TimeFrame — use as_timeframe() to designate a timestamp "
                    "column");
            }
            auto windowed =
                update_node.group_by().empty()
                    ? windowed_update_table(std::move(source.value()), update_node.fields(),
                                            win.duration(), scalars, externs, exec, win.aligned())
                    : grouped_windowed_update_table(std::move(source.value()), update_node.fields(),
                                                    win.duration(), update_node.group_by(), scalars,
                                                    externs, exec, win.aligned());
            if (!windowed.has_value() || !win.select_only()) {
                return windowed;
            }
            // `window` + `select`: keep only the time index, group keys, and the
            // listed fields (row-preserving). The time index leads so the result
            // stays a TimeFrame; a set dedupes fields that name a key or the index.
            std::vector<ir::ColumnRef> keep;
            robin_hood::unordered_set<std::string> seen;
            auto keep_col = [&](const std::string& name) {
                if (seen.insert(name).second) {
                    keep.push_back(ir::ColumnRef{.name = name});
                }
            };
            if (windowed->time_index().has_value()) {
                keep_col(*windowed->time_index());
            }
            for (const auto& key : update_node.group_by()) {
                keep_col(key.name);
            }
            for (const auto& field : update_node.fields()) {
                keep_col(field.alias);
            }
            // A grouped window leaves the rows group-major, and `project_table`
            // preserves that: it derives its metadata with `RowTransform::
            // Preserve`, which carries `grouped_by` through and drops the
            // ordering only if the projection removes one of its keys, and
            // `normalize_time_index` leaves a group-major ordering alone rather
            // than rewriting it to the (false) "time index ascending".
            return project_table(windowed.value(), keep);
        }
        case ir::NodeKind::AsTimeframe: {
            const auto& atf = ir::node_cast<ir::AsTimeframeNode>(node);
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child.has_value()) {
                return child;
            }
            Table& t = child.value();
            const auto* col = t.find(atf.column());
            if (col == nullptr) {
                return std::unexpected("as_timeframe: column '" + atf.column() + "' not found");
            }
            // Accept Int columns as nanosecond timestamps so CSV-loaded integer
            // time columns work without a plugin.
            if (const auto* int_col = std::get_if<Column<std::int64_t>>(col)) {
                Column<Timestamp> ts_col;
                ts_col.reserve(int_col->size());
                for (auto v : *int_col)
                    ts_col.push_back(Timestamp{v});
                auto idx_it = t.index.find(atf.column());
                if (idx_it != t.index.end()) {
                    t.replace_column(idx_it->second, ColumnValue{std::move(ts_col)});
                    col = t.find(atf.column());
                }
            }
            if (!std::holds_alternative<Column<Timestamp>>(*col) &&
                !std::holds_alternative<Column<Date>>(*col)) {
                return std::unexpected("as_timeframe: column '" + atf.column() +
                                       "' must be Timestamp, Date, or Int");
            }
            // A TimeFrame's whole contract is an ordering on its time index, and
            // a null has no position in time — it cannot be earlier or later
            // than anything. Every operator that reads the index (asof, window,
            // resample) would be asking where a row sits when the answer does
            // not exist, so the index is required to be fully valid and the
            // rejection happens here, where the TimeFrame is established.
            if (const auto* entry = t.find_entry(atf.column());
                entry != nullptr && entry->validity.has_value()) {
                for (std::size_t r = 0; r < t.rows(); ++r) {
                    if (!(*entry->validity)[r]) {
                        return std::unexpected(
                            "as_timeframe: time index '" + atf.column() + "' is null at row " +
                            std::to_string(r) +
                            "; a TimeFrame's index must have no nulls (drop or fill them first)");
                    }
                }
            }
            auto sorted = order_table(t, {{.name = atf.column(), .ascending = true}}, exec);
            if (!sorted.has_value()) {
                return sorted;
            }
            sorted->set_properties(TableProperties::time_frame(atf.column()));
            return sorted;
        }
        case ir::NodeKind::Ascribe: {
            const auto& asc = ir::node_cast<ir::AscribeNode>(node);
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child.has_value()) {
                return child;
            }
            const Table& t = child.value();
            auto type_matches = [](const ColumnValue& col, ir::ColumnType type) -> bool {
                switch (type) {
                    case ir::ColumnType::Int32:
                    case ir::ColumnType::Int64:
                        return std::holds_alternative<Column<std::int64_t>>(col);
                    case ir::ColumnType::Float32:
                    case ir::ColumnType::Float64:
                        return std::holds_alternative<Column<double>>(col);
                    case ir::ColumnType::Bool:
                        return std::holds_alternative<Column<bool>>(col);
                    case ir::ColumnType::String:
                        return std::holds_alternative<Column<std::string>>(col) ||
                               std::holds_alternative<Column<Categorical>>(col);
                    case ir::ColumnType::Date:
                        return std::holds_alternative<Column<Date>>(col);
                    case ir::ColumnType::Timestamp:
                        return std::holds_alternative<Column<Timestamp>>(col);
                    case ir::ColumnType::Decimal:
                        return std::holds_alternative<Column<Decimal>>(col);
                    case ir::ColumnType::Categorical:
                        // Unreachable here for the same reason as emitter.cpp's
                        // twin switch: no `parser::ScalarType` spells this, so a
                        // written ascription's `type` is never Categorical. A
                        // physically Categorical column is matched by the String
                        // arm above, same as before this type was split out.
                        return std::holds_alternative<Column<Categorical>>(col);
                }
                return false;
            };
            if (asc.checked()) {
                // Proven against the input schema before execution, so the
                // columns it names need not be materialized -- and must not
                // be looked for here, since demand was narrowed on that basis.
                return child;
            }
            for (const auto& field : asc.schema()) {
                const auto* col = t.find(field.name);
                if (col == nullptr) {
                    return std::unexpected("schema ascription: missing column '" + field.name +
                                           "'");
                }
                if (field.type.has_value() && !type_matches(*col, *field.type)) {
                    return std::unexpected("schema ascription: column '" + field.name +
                                           "' has the wrong type");
                }
            }
            return child;
        }
        case ir::NodeKind::Columns: {
            if (node.children().empty()) {
                return std::unexpected("columns node missing child");
            }
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child.has_value()) {
                return child;
            }
            return columns_table(child.value());
        }
        case ir::NodeKind::ExternCall: {
            const auto& ec = ir::node_cast<ir::ExternCallNode>(node);
            auto result = invoke_extern_call(ec, scalars, externs);
            if (!result.has_value()) {
                return std::unexpected(std::move(result.error()));
            }
            if (auto* table = std::get_if<Table>(&result.value())) {
                return std::move(*table);
            }
            if (externs != nullptr) {
                const auto* fn = externs->find(ec.callee());
                if (fn != nullptr && fn->kind != ExternReturnKind::Table) {
                    return std::unexpected("extern function does not return a table: " +
                                           ec.callee());
                }
            }
            return std::unexpected("extern function did not return a table: " + ec.callee());
        }
        case ir::NodeKind::Join: {
            const auto& join = ir::node_cast<ir::JoinNode>(node);
            if (join.children().size() != 2) {
                return std::unexpected("join node expects exactly two children");
            }
            auto left = materialize_plan(*join.children()[0], registry, scalars, externs, exec);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = materialize_plan(*join.children()[1], registry, scalars, externs, exec);
            if (!right) {
                return std::unexpected(right.error());
            }
            // Only joins `plan_join` does not stream reach here (predicate,
            // nulls equal, expect, take, outer kinds, wider keys).
            const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys(), pred,
                                   scalars, compute_mask, join.suffix(), join.pending_order(),
                                   join.null_match(), join.expect(), join.take(), &exec);
        }
        case ir::NodeKind::Melt: {
            const auto& mn = ir::node_cast<ir::MeltNode>(node);
            if (mn.children().empty()) {
                return std::unexpected("melt node missing child");
            }
            auto child = materialize_plan(*mn.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return melt_table(child.value(), mn.id_columns(), mn.measure_columns());
        }
        case ir::NodeKind::Dcast: {
            const auto& dn = ir::node_cast<ir::DcastNode>(node);
            if (dn.children().empty()) {
                return std::unexpected("dcast node missing child");
            }
            auto child = materialize_plan(*dn.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return dcast_table(child.value(), dn.pivot_column(), dn.value_column(), dn.row_keys());
        }
        case ir::NodeKind::Cov: {
            if (node.children().empty()) {
                return std::unexpected("cov node missing child");
            }
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return cov_table(child.value());
        }
        case ir::NodeKind::Corr: {
            if (node.children().empty()) {
                return std::unexpected("corr node missing child");
            }
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return corr_table(child.value());
        }
        case ir::NodeKind::Transpose: {
            if (node.children().empty()) {
                return std::unexpected("transpose node missing child");
            }
            auto child =
                materialize_plan(*node.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            return transpose_table(child.value());
        }
        case ir::NodeKind::Matmul: {
            if (node.children().size() != 2) {
                return std::unexpected("matmul node expects exactly two children");
            }
            auto left = materialize_plan(*node.children()[0], registry, scalars, externs, exec);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = materialize_plan(*node.children()[1], registry, scalars, externs, exec);
            if (!right) {
                return std::unexpected(right.error());
            }
            return matmul_table(left.value(), right.value());
        }
        case ir::NodeKind::Rbind: {
            if (node.children().size() < 2) {
                return std::unexpected("rbind node expects at least two children");
            }
            std::vector<Table> operands;
            operands.reserve(node.children().size());
            for (const auto& child : node.children()) {
                auto result = materialize_plan(*child, registry, scalars, externs, exec);
                if (!result) {
                    return std::unexpected(result.error());
                }
                operands.push_back(std::move(result.value()));
            }
            std::vector<const Table*> ptrs;
            ptrs.reserve(operands.size());
            for (const Table& t : operands) {
                ptrs.push_back(&t);
            }
            // When every operand is a TimeFrame on the same time-index column,
            // the result stays a TimeFrame: rbind_table k-way merges the
            // already-sorted operands so the rows interleave in time order in a
            // single pass (SPEC §9.1 keeps TimeFrames sorted). Mixed/absent
            // indices yield a plain appended DataFrame.
            std::optional<std::string> common_ti = operands.front().time_index();
            if (common_ti.has_value()) {
                for (const Table& t : operands) {
                    if (t.time_index() != common_ti) {
                        common_ti.reset();
                        break;
                    }
                }
            }
            auto result = rbind_table(ptrs, common_ti);
            if (!result) {
                return std::unexpected(std::move(result.error()));
            }
            if (common_ti.has_value()) {
                // The merge already produced sorted rows, so just stamp the
                // index and its ordering — no re-sort.
                result->set_properties(TableProperties::time_frame(*common_ti));
            }
            return result;
        }
        case ir::NodeKind::Stream: {
            const auto& sn = ir::node_cast<ir::StreamNode>(node);
            if (externs == nullptr) {
                return std::unexpected("stream node requires an extern registry");
            }
            if (sn.children().empty()) {
                return std::unexpected("stream node has no transform child");
            }

            // Resolve source and sink functions.
            const auto* source_fn = externs->find(sn.source_callee());
            if (source_fn == nullptr) {
                return std::unexpected("unknown stream source: " + sn.source_callee());
            }
            if (source_fn->kind != ExternReturnKind::Table) {
                return std::unexpected("stream source must return a table: " + sn.source_callee());
            }
            const auto* sink_fn = externs->find(sn.sink_callee());
            if (sink_fn == nullptr) {
                return std::unexpected("unknown stream sink: " + sn.sink_callee());
            }
            if (!sink_fn->first_arg_is_table) {
                return std::unexpected("stream sink must be a table-consumer extern: " +
                                       sn.sink_callee());
            }

            // Pre-evaluate scalar args (literals — no row context needed).
            // Externs take null-free ScalarValue arguments.
            auto eval_scalar_args =
                [&](const std::vector<ir::Expr>& exprs) -> std::expected<ExternArgs, std::string> {
                ExternArgs out;
                out.reserve(exprs.size());
                for (const auto& arg : exprs) {
                    auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                    if (!val)
                        return std::unexpected(val.error());
                    auto scalar = scalar_from_expr(val.value());
                    if (is_null_scalar(scalar))
                        return std::unexpected("null argument in stream extern call");
                    out.push_back(std::move(scalar));
                }
                return out;
            };
            auto source_args_res = eval_scalar_args(sn.source_args());
            if (!source_args_res)
                return std::unexpected(source_args_res.error());
            const ExternArgs source_args = std::move(*source_args_res);
            const auto sink_args_res = eval_scalar_args(sn.sink_args());
            if (!sink_args_res)
                return std::unexpected(sink_args_res.error());
            ExternArgs sink_scalar_args = *sink_args_res;

            const ir::Node& transform_ir = sn.transform_ir();

            // Append every row of `src` into `dst`, initialising dst schema on first call.
            auto append_table = [&](Table& dst,
                                    const Table& src) -> std::expected<void, std::string> {
                if (src.rows() == 0)
                    return {};
                if (dst.columns.empty()) {
                    for (const auto& entry : src.columns) {
                        dst.add_column(entry.name, make_empty_like(*entry.column));
                    }
                    dst.set_properties(src.properties());
                }
                for (std::size_t row = 0; row < src.rows(); ++row) {
                    for (std::size_t col = 0; col < src.columns.size(); ++col) {
                        if (col >= dst.columns.size()) {
                            return std::unexpected("stream: source schema changed mid-stream");
                        }
                        auto& dst_col = dst.mutable_column(col);
                        append_value(dst_col, *src.columns[col].column, row);
                        const bool null = is_null(src.columns[col], row);
                        if (null) {
                            if (!dst.columns[col].validity.has_value()) {
                                dst.columns[col].validity =
                                    ValidityBitmap(column_size(dst_col) - 1, true);
                            }
                            dst.columns[col].validity->push_back(false);
                        } else if (dst.columns[col].validity.has_value()) {
                            dst.columns[col].validity->push_back(true);
                        }
                    }
                }
                return {};
            };

            // Slice a single row out of `src` into a new one-row Table.
            auto slice_row = [&](const Table& src, std::size_t r) -> Table {
                Table out;
                for (const auto& entry : src.columns) {
                    out.add_column(entry.name, make_empty_like(*entry.column));
                    const std::size_t out_pos = out.columns.size() - 1;
                    append_value(out.mutable_column(out_pos), *entry.column, r);
                    if (is_null(entry, r)) {
                        out.columns.back().validity = ValidityBitmap{false};
                    }
                }
                // A one-row slice inherits the time index only: a single row
                // has no group boundary to read across, so claiming a grouping
                // would arm the row-order guard against a correct call.
                if (src.time_index().has_value()) {
                    out.set_properties(TableProperties::time_frame(*src.time_index()));
                }
                return out;
            };

            // Get the nanosecond timestamp of the last row (for bucket detection).
            auto get_last_ts_ns = [&](const Table& t) -> std::optional<std::int64_t> {
                if (t.rows() == 0 || !t.time_index().has_value())
                    return std::nullopt;
                const auto* col = t.find(*t.time_index());
                if (col == nullptr)
                    return std::nullopt;
                const std::size_t last = t.rows() - 1;
                return std::visit(
                    [last](const auto& c) -> std::optional<std::int64_t> {
                        using C = std::decay_t<decltype(c)>;
                        if constexpr (std::is_same_v<C, Column<Timestamp>>) {
                            return static_cast<std::int64_t>(c[last].nanos);
                        } else if constexpr (std::is_same_v<C, Column<std::int64_t>>) {
                            return c[last];
                        }
                        return std::nullopt;
                    },
                    *col);
            };

            // Run the transform over `buf` and emit the result to the sink.
            auto emit_buffer = [&](const Table& buf) -> std::expected<void, std::string> {
                if (buf.rows() == 0)
                    return {};
                TableRegistry stream_reg = registry;
                stream_reg["__stream_input__"] = buf;
                auto output = materialize_plan(transform_ir, stream_reg, scalars, externs, exec);
                if (!output)
                    return std::unexpected(output.error());
                if (output->rows() == 0)
                    return {};
                auto sr = sink_fn->table_consumer_func(*output, sink_scalar_args);
                if (!sr)
                    return std::unexpected(sr.error());
                return {};
            };

            // ── Event loop ──────────────────────────────────────────────────────
            Table buffer;
            std::int64_t open_bucket_ns = -1;
            std::int64_t bucket_open_wall_ns = -1;  // wall-clock ns when current bucket was opened
            const std::int64_t bucket_ns =
                sn.stream_kind() == ir::StreamKind::TimeBucket
                    ? static_cast<std::int64_t>(sn.bucket_duration().count())
                    : 0;

            // Returns current wall-clock time in nanoseconds.
            auto wall_now_ns = []() -> std::int64_t {
                auto now = std::chrono::system_clock::now();
                auto ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch());
                return static_cast<std::int64_t>(ns.count());
            };

            while (true) {
                auto src_result = source_fn->func(source_args);
                if (!src_result)
                    return std::unexpected(src_result.error());

                // StreamTimeout: the source had a receive timeout — no data arrived but
                // it is not done.  Run the wall-clock flush check and keep listening.
                const bool is_timeout = std::holds_alternative<StreamTimeout>(src_result.value());

                if (!is_timeout) {
                    const auto* batch = std::get_if<Table>(&src_result.value());
                    if (batch == nullptr) {
                        return std::unexpected("stream source did not return a table");
                    }
                    if (batch->rows() == 0)
                        break;  // source signalled EOF
                }

                if (sn.stream_kind() == ir::StreamKind::TimeBucket && bucket_ns > 0) {
                    if (open_bucket_ns >= 0 && buffer.rows() > 0 &&
                        wall_now_ns() - bucket_open_wall_ns >= bucket_ns) {
                        auto er = emit_buffer(buffer);
                        if (!er)
                            return std::unexpected(er.error());
                        buffer = Table{};
                        open_bucket_ns = -1;
                        bucket_open_wall_ns = -1;
                    }

                    if (!is_timeout) {
                        const auto& batch = std::get<Table>(src_result.value());
                        for (std::size_t r = 0; r < batch.rows(); ++r) {
                            const Table row_tbl = slice_row(batch, r);
                            const auto ts_opt = get_last_ts_ns(row_tbl);
                            const std::int64_t row_bucket =
                                ts_opt ? ((*ts_opt / bucket_ns) * bucket_ns) : -1;

                            if (open_bucket_ns >= 0 && row_bucket >= 0 &&
                                row_bucket > open_bucket_ns) {
                                auto er = emit_buffer(buffer);
                                if (!er)
                                    return std::unexpected(er.error());
                                buffer = Table{};
                            }
                            if (row_bucket >= 0) {
                                if (row_bucket != open_bucket_ns) {
                                    bucket_open_wall_ns = wall_now_ns();
                                }
                                open_bucket_ns = row_bucket;
                            }
                            auto app = append_table(buffer, row_tbl);
                            if (!app)
                                return std::unexpected(app.error());
                        }
                    }
                } else if (!is_timeout) {
                    const auto& batch = std::get<Table>(src_result.value());
                    auto app = append_table(buffer, batch);
                    if (!app)
                        return std::unexpected(app.error());
                    auto er = emit_buffer(buffer);
                    if (!er)
                        return std::unexpected(er.error());
                }
            }

            if (sn.stream_kind() == ir::StreamKind::TimeBucket && buffer.rows() > 0) {
                auto er = emit_buffer(buffer);
                if (!er)
                    return std::unexpected(er.error());
            }

            return Table{};
        }
        case ir::NodeKind::Construct: {
            const auto& cn = ir::node_cast<ir::ConstructNode>(node);
            // `Table(n)` form: an empty frame carrying an explicit row count.
            if (cn.row_count().has_value()) {
                auto n = evaluate_row_count_expr_impl(*cn.row_count(), scalars, externs);
                if (!n.has_value()) {
                    return std::unexpected(n.error());
                }
                Table empty;
                empty.logical_rows = *n;
                return empty;
            }
            Table result;
            for (const auto& col : cn.columns()) {
                if (col.expr_node) {
                    // Expression column: evaluate the sub-node to produce a Table,
                    // then extract the target column from it.
                    auto sub = materialize_plan(*col.expr_node, registry, scalars, externs, exec);
                    if (!sub.has_value())
                        return std::unexpected(sub.error());
                    if (sub->columns.size() == 1) {
                        // Single-column result: use it regardless of its name.
                        ColumnEntry entry = sub->columns[0];
                        entry.name = col.name;
                        result.index[col.name] = result.columns.size();
                        result.columns.push_back(std::move(entry));
                    } else if (auto it = sub->index.find(col.name); it != sub->index.end()) {
                        // Multi-column result: extract the column matching col.name.
                        ColumnEntry entry = sub->columns[it->second];
                        entry.name = col.name;
                        result.index[col.name] = result.columns.size();
                        result.columns.push_back(std::move(entry));
                    } else {
                        return std::unexpected(
                            "Table constructor: expression for column '" + col.name +
                            "' must produce a single-column result or a table containing"
                            " a column named '" +
                            col.name + "'");
                    }
                    continue;
                }
                if (col.elements.empty()) {
                    // Empty array literal: default to Int64
                    result.add_column(col.name, Column<std::int64_t>{});
                    continue;
                }
                // Literal column: build from inline values.
                ColumnValue cv = std::visit(
                    [&](const auto& first_val) -> ColumnValue {
                        using T = std::decay_t<decltype(first_val)>;
                        if constexpr (std::is_same_v<T, DecimalValue>) {
                            // One column type for the whole list: the narrowest
                            // Decimal holding every element exactly.
                            DecimalType unified = first_val.type;
                            for (const auto& lit : col.elements) {
                                if (const auto* d = std::get_if<DecimalValue>(&lit.value)) {
                                    unified = decimal::union_type(unified, d->type);
                                }
                            }
                            Column<Decimal> col_data = make_decimal_column(unified);
                            col_data.reserve(col.elements.size());
                            for (const auto& lit : col.elements) {
                                col_data.push_back(
                                    Decimal{decimal_units_for(scalar_from_literal(lit), unified)});
                            }
                            return col_data;
                        } else {
                            Column<T> col_data;
                            col_data.reserve(col.elements.size());
                            for (const auto& lit : col.elements) {
                                col_data.push_back(std::get<T>(lit.value));
                            }
                            return col_data;
                        }
                    },
                    col.elements[0].value);
                if (col.valid.empty()) {
                    result.add_column(col.name, std::move(cv));
                } else {
                    result.add_column(col.name, std::move(cv), ValidityBitmap{col.valid});
                }
            }
            // Validate that all columns have the same length.
            if (!result.columns.empty()) {
                const std::size_t n_rows =
                    std::visit([](const auto& c) { return c.size(); }, *result.columns[0].column);
                for (std::size_t i = 1; i < result.columns.size(); ++i) {
                    const std::size_t len = std::visit([](const auto& c) { return c.size(); },
                                                       *result.columns[i].column);
                    if (len != n_rows) {
                        return std::unexpected(
                            "Table constructor: all columns must have the same length ('" +
                            result.columns[i].name + "' has " + std::to_string(len) +
                            " elements, expected " + std::to_string(n_rows) + ")");
                    }
                }
            }
            return result;
        }
        case ir::NodeKind::Model: {
            const auto& mn = ir::node_cast<ir::ModelNode>(node);
            if (mn.children().empty()) {
                return std::unexpected("model node missing child");
            }
            auto child = materialize_plan(*mn.children().front(), registry, scalars, externs, exec);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto result =
                fit_model(child.value(), mn.formula(), mn.method(), mn.params(), scalars, externs);
            if (!result) {
                return std::unexpected(result.error());
            }
            // Extract the primary table before potentially moving the whole
            // result. Linear methods expose coefficients; tree models expose
            // feature importance; unsupervised models (e.g. kmeans) have neither,
            // so fall back to the per-row fitted output (e.g. cluster ids).
            Table primary =
                !result.value().coefficients.columns.empty() ? result.value().coefficients
                : !result.value().importance.columns.empty() ? result.value().importance
                                                             : result.value().fitted_values;
            if (model_out != nullptr) {
                *model_out = std::move(result.value());
            }
            return primary;
        }
        case ir::NodeKind::Program: {
            const auto& program = ir::node_cast<ir::ProgramNode>(node);
            auto preamble = execute_program_preamble(program.preamble(), scalars, externs);
            if (!preamble.has_value()) {
                return std::unexpected(std::move(preamble.error()));
            }
            return materialize_plan(program.main_node(), registry, scalars, externs, exec,
                                    model_out);
        }
        case ir::NodeKind::Filter:
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
        case ir::NodeKind::Distinct:
        case ir::NodeKind::Order:
        case ir::NodeKind::Head:
        case ir::NodeKind::Tail:
        case ir::NodeKind::TopK:
        case ir::NodeKind::FilterHead:
        case ir::NodeKind::FilterTail:
            // Built by `build_operator` every time -- migrated plans, or (Filter,
            // Project, Rename) its own branches -- and never a fallback root, so
            // `run_materialized_node` (whose only caller is the fallback) cannot see one.
            return std::unexpected(
                "run_materialized_node: this node kind is always built by the "
                "physical plan");
    }
    return std::unexpected("unknown node kind");
}

}  // namespace ibex::runtime
