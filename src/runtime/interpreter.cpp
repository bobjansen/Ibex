#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#ifdef __GLIBC__
#include <malloc.h>  // mallopt
#endif

#include "interpreter_internal.hpp"
#include "join_internal.hpp"
#include "model_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

// Process-wide allocator tuning to flatten the large-buffer page-fault cliff.
//
// Every result column is backed by std::vector<T>, so any column above glibc's
// dynamic mmap threshold (grows up to 32 MB = 4M float64 rows) is served by a
// fresh mmap and munmapped on free. The next same-size allocation re-mmaps and
// re-faults every 4 KB page on first touch — a ~5x throughput cliff once columns
// cross ~32 MB (see plans/benchmark-perf-priorities.md, P0). Serving large
// allocations from the main arena and never trimming the heap top lets freed
// buffers recycle already-faulted pages across the warmup/timed iterations.
// glibc-only; a no-op elsewhere. Opt out via IBEX_NO_MALLOC_TUNING.
void tune_allocator_once() {
#ifdef __GLIBC__
    static std::once_flag flag;
    std::call_once(flag, [] {
        if (const char* off = std::getenv("IBEX_NO_MALLOC_TUNING");
            off != nullptr && off[0] != '\0' && off[0] != '0') {
            return;
        }
        mallopt(M_MMAP_MAX, 0);         // large allocs from sbrk arena, not mmap
        mallopt(M_TRIM_THRESHOLD, -1);  // keep freed buffers resident for reuse
    });
#endif
}

}  // namespace

auto ordering_keys_present(const std::vector<ir::OrderKey>& keys,
                           const robin_hood::unordered_map<std::string, std::size_t>& index)
    -> bool {
    return std::ranges::all_of(keys, [index](const auto& key) { return index.contains(key.name); });
}

auto ordering_keys_for_table(const Table& input, const std::vector<ir::OrderKey>& keys)
    -> std::vector<ir::OrderKey> {
    if (!keys.empty()) {
        return keys;
    }
    std::vector<ir::OrderKey> resolved;
    resolved.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        resolved.push_back(ir::OrderKey{.name = entry.name, .ascending = true});
    }
    return resolved;
}

auto format_tables(const TableRegistry& registry) -> std::string {
    if (registry.empty()) {
        return "<none>";
    }
    std::vector<std::string_view> names;
    names.reserve(registry.size());
    for (const auto& entry : registry) {
        names.emplace_back(entry.first);
    }
    std::ranges::sort(names);
    std::string out;
    for (std::size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(names[i]);
    }
    return out;
}

[[noreturn]] void invariant_violation(std::string_view detail) {
    // This is triggered by a severe bug, everything in here is on a best effort basis
    (void)std::fputs("ibex internal invariant violated (runtime/interpreter): ", stderr);
    (void)std::fwrite(detail.data(), sizeof(char), detail.size(), stderr);
    (void)std::fputc('\n', stderr);
    std::abort();
}

auto project_table(const Table& input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Table, std::string> {
    Table output;
    for (const auto& col : columns) {
        const auto* entry = input.find_entry(col.name);
        if (entry == nullptr) {
            return std::unexpected("select column not found: " + col.name +
                                   " (available: " + format_columns(input) + ")");
        }
        // Share the column's shared_ptr instead of deep-copying its data. The
        // projected table is a read-only selection; under copy-on-write any
        // later mutation reseats a fresh column, so sharing is safe.
        output.add_column_shared(col.name, entry->column, entry->validity);
    }
    if (input.ordering.has_value() && ordering_keys_present(*input.ordering, output.index)) {
        output.ordering = input.ordering;
    }
    if (input.time_index.has_value()) {
        if (output.index.contains(*input.time_index)) {
            output.time_index = input.time_index;
        } else {
            output.time_index.reset();
            output.ordering.reset();
        }
    }
    normalize_time_index(output);
    return output;
}

auto rename_table(const Table& input, const std::vector<ir::RenameSpec>& renames)
    -> std::expected<Table, std::string> {
    robin_hood::unordered_map<std::string, std::string> rename_map;
    rename_map.reserve(renames.size());
    for (const auto& spec : renames) {
        const auto* entry = input.find_entry(spec.old_name);
        if (entry == nullptr) {
            return std::unexpected("rename: column not found: " + spec.old_name +
                                   " (available: " + format_columns(input) + ")");
        }
        rename_map[spec.old_name] = spec.new_name;
    }

    Table output;
    for (const auto& entry : input.columns) {
        auto it = rename_map.find(entry.name);
        const std::string& out_name = (it != rename_map.end()) ? it->second : entry.name;
        // Rename only relabels columns; share the data rather than copying it.
        output.add_column_shared(out_name, entry.column, entry.validity);
    }

    if (input.ordering.has_value()) {
        std::vector<ir::OrderKey> new_ordering;
        for (const auto& key : *input.ordering) {
            auto it = rename_map.find(key.name);
            new_ordering.push_back({
                .name = (it != rename_map.end()) ? it->second : key.name,
                .ascending = key.ascending,
            });
        }
        output.ordering = std::move(new_ordering);
    }

    if (input.time_index.has_value()) {
        auto it = rename_map.find(*input.time_index);
        output.time_index = (it != rename_map.end()) ? it->second : *input.time_index;
    }

    normalize_time_index(output);
    return output;
}

auto columns_table(const Table& input) -> std::expected<Table, std::string> {
    Table output;
    Column<std::string> names;
    names.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        names.push_back(entry.name);
    }
    output.add_column("name", std::move(names));
    return output;
}

auto expr_type_for_column(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
    }
    if (std::holds_alternative<Column<bool>>(column)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Column<Date>>(column)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Column<Timestamp>>(column)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

auto distinct_table(const Table& input) -> std::expected<Table, std::string> {
    if (input.columns.empty()) {
        Table output = input;
        output.ordering.reset();
        output.time_index.reset();
        return output;
    }
    std::size_t rows = input.rows();
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_empty_like(*entry.column));
    }
    for (auto& entry : output.columns) {
        std::visit([&](auto& col) { col.reserve(rows); }, *entry.column);
    }

    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen;
    seen.reserve(rows);

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(input.columns.size());
        for (const auto& entry : input.columns) {
            key.values.push_back(scalar_from_column(*entry.column, row));
        }
        if (!seen.insert(std::move(key)).second) {
            continue;
        }
        for (std::size_t col = 0; col < input.columns.size(); ++col) {
            append_value(output.mutable_column(col), *input.columns[col].column, row);
        }
    }
    output.ordering.reset();
    output.time_index.reset();
    return output;
}

// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    ModelResult* model_out) -> std::expected<Table, std::string> {
    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = static_cast<const ir::ScanNode&>(node);
            auto it = registry.find(scan.source_name());
            if (it == registry.end()) {
                return std::unexpected("unknown table: " + scan.source_name() +
                                       " (available: " + format_tables(registry) + ")");
            }
            Table output = it->second;
            normalize_time_index(output);
            return output;
        }
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            if (filter.children().empty()) {
                return std::unexpected("filter node missing child");
            }
            auto child = interpret_node(*filter.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return filter_table(child.value(), filter.predicate(), scalars);
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            if (project.children().empty()) {
                return std::unexpected("project node missing child");
            }
            auto child = interpret_node(*project.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return project_table(child.value(), project.columns());
        }
        case ir::NodeKind::Distinct: {
            if (node.children().empty()) {
                return std::unexpected("distinct node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return distinct_table(child.value());
        }
        case ir::NodeKind::Order: {
            const auto& order = static_cast<const ir::OrderNode&>(node);
            if (order.children().empty()) {
                return std::unexpected("order node missing child");
            }
            auto child = interpret_node(*order.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return order_table(child.value(), order.keys());
        }
        case ir::NodeKind::Head: {
            const auto& head = static_cast<const ir::HeadNode&>(node);
            if (head.children().empty()) {
                return std::unexpected("head node missing child");
            }
            auto count = evaluate_row_count_expr_impl(head.count_expr(), scalars, externs);
            if (!count) {
                return std::unexpected(count.error());
            }
            auto child = interpret_node(*head.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return head_table(child.value(), *count, head.group_by());
        }
        case ir::NodeKind::Tail: {
            const auto& tail = static_cast<const ir::TailNode&>(node);
            if (tail.children().empty()) {
                return std::unexpected("tail node missing child");
            }
            auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
            if (!count) {
                return std::unexpected(count.error());
            }
            auto child = interpret_node(*tail.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return tail_table(child.value(), *count, tail.group_by());
        }
        case ir::NodeKind::Update: {
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            if (update.children().empty()) {
                return std::unexpected("update node missing child");
            }
            auto child = interpret_node(*update.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            if (update.guard() != nullptr) {
                return apply_guarded_update(std::move(child.value()), update, scalars, externs);
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
                        auto res = evaluate_rank_column(result, *rank, update.group_by());
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
                                            update.group_by(), scalars, externs);
            }
            auto result = update_table(std::move(child.value()), update.fields(), scalars, externs);
            if (!result) {
                return result;
            }
            for (const auto& tspec : update.tuple_fields()) {
                auto src = interpret_node(*tspec.source, registry, scalars, externs);
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
        case ir::NodeKind::Rename: {
            const auto& rename = static_cast<const ir::RenameNode&>(node);
            if (rename.children().empty()) {
                return std::unexpected("rename node missing child");
            }
            auto child = interpret_node(*rename.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return rename_table(child.value(), rename.renames());
        }
        case ir::NodeKind::Aggregate: {
            const auto& agg = static_cast<const ir::AggregateNode&>(node);
            if (agg.children().empty()) {
                return std::unexpected("aggregate node missing child");
            }
            // Fast path: Aggregate(Scan) — pass the registry table by const ref to skip the copy.
            const ir::Node& child_node = *agg.children().front();
            if (child_node.kind() == ir::NodeKind::Scan) {
                const auto& scan = static_cast<const ir::ScanNode&>(child_node);
                auto it = registry.find(scan.source_name());
                if (it == registry.end()) {
                    return std::unexpected("unknown table: " + scan.source_name());
                }
                return aggregate_table(it->second, agg.group_by(), agg.aggregations());
            }
            auto child = interpret_node(child_node, registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return aggregate_table(child.value(), agg.group_by(), agg.aggregations());
        }
        case ir::NodeKind::Resample: {
            const auto& rs = static_cast<const ir::ResampleNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value())
                return child;
            return resample_table(child.value(), rs.duration(), rs.group_by(), rs.aggregations());
        }
        case ir::NodeKind::Window: {
            const auto& win = static_cast<const ir::WindowNode&>(node);
            const ir::Node& child_node = *node.children().front();
            // The child must be an UpdateNode produced by the `update` clause.
            if (child_node.kind() != ir::NodeKind::Update) {
                return std::unexpected(
                    "window: only 'update' is currently supported inside a window block");
            }
            const auto& update_node = static_cast<const ir::UpdateNode&>(child_node);
            // Evaluate the source (grandchild) without the window context.
            auto source =
                interpret_node(*child_node.children().front(), registry, scalars, externs);
            if (!source.has_value()) {
                return source;
            }
            if (!source->time_index.has_value()) {
                return std::unexpected(
                    "window requires a TimeFrame — use as_timeframe() to designate a timestamp "
                    "column");
            }
            if (!update_node.group_by().empty()) {
                return grouped_windowed_update_table(std::move(source.value()),
                                                     update_node.fields(), win.duration(),
                                                     update_node.group_by(), scalars, externs);
            }
            return windowed_update_table(std::move(source.value()), update_node.fields(),
                                         win.duration(), scalars, externs);
        }
        case ir::NodeKind::AsTimeframe: {
            const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
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
            auto sorted = order_table(t, {{.name = atf.column(), .ascending = true}});
            if (!sorted.has_value()) {
                return sorted;
            }
            sorted->time_index = atf.column();
            normalize_time_index(*sorted);
            return sorted;
        }
        case ir::NodeKind::Ascribe: {
            const auto& asc = static_cast<const ir::AscribeNode&>(node);
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
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
                }
                return false;
            };
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
            // An exact (non-wildcard) ascription forbids columns not listed.
            if (!asc.open()) {
                for (const auto& entry : t.index) {
                    const bool listed = std::any_of(
                        asc.schema().begin(), asc.schema().end(),
                        [&](const ir::SchemaField& f) { return f.name == entry.first; });
                    if (!listed) {
                        return std::unexpected("schema ascription: input has extra column '" +
                                               entry.first +
                                               "' not in the ascribed schema (add `*` to allow "
                                               "extras)");
                    }
                }
            }
            return child;
        }
        case ir::NodeKind::Columns: {
            if (node.children().empty()) {
                return std::unexpected("columns node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child.has_value()) {
                return child;
            }
            return columns_table(child.value());
        }
        case ir::NodeKind::ExternCall: {
            const auto& ec = static_cast<const ir::ExternCallNode&>(node);
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
            const auto& join = static_cast<const ir::JoinNode&>(node);
            if (join.children().size() != 2) {
                return std::unexpected("join node expects exactly two children");
            }
            auto left = interpret_node(*join.children()[0], registry, scalars, externs);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = interpret_node(*join.children()[1], registry, scalars, externs);
            if (!right) {
                return std::unexpected(right.error());
            }
            const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
            return join_table_impl(left.value(), right.value(), join.kind(), join.keys(), pred,
                                   scalars, compute_mask);
        }
        case ir::NodeKind::Melt: {
            const auto& mn = static_cast<const ir::MeltNode&>(node);
            if (mn.children().empty()) {
                return std::unexpected("melt node missing child");
            }
            auto child = interpret_node(*mn.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return melt_table(child.value(), mn.id_columns(), mn.measure_columns());
        }
        case ir::NodeKind::Dcast: {
            const auto& dn = static_cast<const ir::DcastNode&>(node);
            if (dn.children().empty()) {
                return std::unexpected("dcast node missing child");
            }
            auto child = interpret_node(*dn.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return dcast_table(child.value(), dn.pivot_column(), dn.value_column(), dn.row_keys());
        }
        case ir::NodeKind::Cov: {
            if (node.children().empty()) {
                return std::unexpected("cov node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return cov_table(child.value());
        }
        case ir::NodeKind::Corr: {
            if (node.children().empty()) {
                return std::unexpected("corr node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return corr_table(child.value());
        }
        case ir::NodeKind::Transpose: {
            if (node.children().empty()) {
                return std::unexpected("transpose node missing child");
            }
            auto child = interpret_node(*node.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            return transpose_table(child.value());
        }
        case ir::NodeKind::Matmul: {
            if (node.children().size() != 2) {
                return std::unexpected("matmul node expects exactly two children");
            }
            auto left = interpret_node(*node.children()[0], registry, scalars, externs);
            if (!left) {
                return std::unexpected(left.error());
            }
            auto right = interpret_node(*node.children()[1], registry, scalars, externs);
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
                auto result = interpret_node(*child, registry, scalars, externs);
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
            std::optional<std::string> common_ti = operands.front().time_index;
            if (common_ti.has_value()) {
                for (const Table& t : operands) {
                    if (t.time_index != common_ti) {
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
                result->time_index = common_ti;
                normalize_time_index(*result);
            }
            return result;
        }
        case ir::NodeKind::Stream: {
            const auto& sn = static_cast<const ir::StreamNode&>(node);
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
            ExternArgs source_args;
            for (const auto& arg : sn.source_args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                source_args.push_back(std::move(val.value()));
            }
            ExternArgs sink_scalar_args;
            for (const auto& arg : sn.sink_args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                if (!val)
                    return std::unexpected(val.error());
                sink_scalar_args.push_back(std::move(val.value()));
            }

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
                    dst.time_index = src.time_index;
                    dst.ordering = src.ordering;
                }
                for (std::size_t row = 0; row < src.rows(); ++row) {
                    for (std::size_t col = 0; col < src.columns.size(); ++col) {
                        if (col >= dst.columns.size()) {
                            return std::unexpected("stream: source schema changed mid-stream");
                        }
                        auto& dst_col = dst.mutable_column(col);
                        append_value(dst_col, *src.columns[col].column, row);
                        bool null = is_null(src.columns[col], row);
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
                out.time_index = src.time_index;
                return out;
            };

            // Get the nanosecond timestamp of the last row (for bucket detection).
            auto get_last_ts_ns = [&](const Table& t) -> std::optional<std::int64_t> {
                if (t.rows() == 0 || !t.time_index.has_value())
                    return std::nullopt;
                const auto* col = t.find(*t.time_index);
                if (col == nullptr)
                    return std::nullopt;
                std::size_t last = t.rows() - 1;
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
                auto output = interpret_node(transform_ir, stream_reg, scalars, externs);
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
                            Table row_tbl = slice_row(batch, r);
                            auto ts_opt = get_last_ts_ns(row_tbl);
                            std::int64_t row_bucket =
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
            const auto& cn = static_cast<const ir::ConstructNode&>(node);
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
                    auto sub = interpret_node(*col.expr_node, registry, scalars, externs);
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
                        Column<T> col_data;
                        col_data.reserve(col.elements.size());
                        for (const auto& lit : col.elements) {
                            col_data.push_back(std::get<T>(lit.value));
                        }
                        return col_data;
                    },
                    col.elements[0].value);
                result.add_column(col.name, std::move(cv));
            }
            // Validate that all columns have the same length.
            if (!result.columns.empty()) {
                std::size_t n_rows =
                    std::visit([](const auto& c) { return c.size(); }, *result.columns[0].column);
                for (std::size_t i = 1; i < result.columns.size(); ++i) {
                    std::size_t len = std::visit([](const auto& c) { return c.size(); },
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
            const auto& mn = static_cast<const ir::ModelNode&>(node);
            if (mn.children().empty()) {
                return std::unexpected("model node missing child");
            }
            auto child = interpret_node(*mn.children().front(), registry, scalars, externs);
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
            const auto& program = static_cast<const ir::ProgramNode&>(node);
            auto preamble = execute_program_preamble(program.preamble(), scalars, externs);
            if (!preamble.has_value()) {
                return std::unexpected(std::move(preamble.error()));
            }
            return interpret_node(program.main_node(), registry, scalars, externs, model_out);
        }
        case ir::NodeKind::FilterProject: {
            // Fused shape produced by canonicalize R5. Materializing fallback
            // for contexts where chunked build_operator is bypassed: evaluate
            // filter then project sequentially.
            const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
            if (fp.children().empty()) {
                return std::unexpected("filter_project node missing child");
            }
            auto child = interpret_node(*fp.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fp.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return project_table(filtered.value(), fp.columns());
        }
        case ir::NodeKind::FilterUpdateProject: {
            // Fused shape produced by canonicalize R6. Materializing fallback.
            const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
            if (fup.children().empty()) {
                return std::unexpected("filter_update_project node missing child");
            }
            auto child = interpret_node(*fup.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fup.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            auto updated =
                update_table(std::move(filtered.value()), fup.fields(), scalars, externs);
            if (!updated) {
                return std::unexpected(updated.error());
            }
            return project_table(updated.value(), fup.project_columns());
        }
        case ir::NodeKind::TopK: {
            const auto& topk = static_cast<const ir::TopKNode&>(node);
            if (topk.children().empty()) {
                return std::unexpected("topk node missing child");
            }
            auto child = interpret_node(*topk.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto sorted = order_table(child.value(), topk.keys());
            if (!sorted) {
                return std::unexpected(sorted.error());
            }
            if (topk.keep_mode() == ir::TopKNode::KeepMode::First) {
                return head_table(sorted.value(), topk.count(), topk.group_by());
            }
            return tail_table(sorted.value(), topk.count(), topk.group_by());
        }
        case ir::NodeKind::FilterHead: {
            const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
            if (fh.children().empty()) {
                return std::unexpected("filter_head node missing child");
            }
            auto child = interpret_node(*fh.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), fh.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return head_table(filtered.value(), fh.count(), {});
        }
        case ir::NodeKind::FilterTail: {
            const auto& ft = static_cast<const ir::FilterTailNode&>(node);
            if (ft.children().empty()) {
                return std::unexpected("filter_tail node missing child");
            }
            auto child = interpret_node(*ft.children().front(), registry, scalars, externs);
            if (!child) {
                return std::unexpected(child.error());
            }
            auto filtered = filter_table(child.value(), ft.predicate(), scalars);
            if (!filtered) {
                return std::unexpected(filtered.error());
            }
            return tail_table(filtered.value(), ft.count(), {});
        }
    }
    return std::unexpected("unknown node kind");
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

auto evaluate_row_count_expr(const ir::Expr& expr, const ScalarRegistry* scalars,
                             const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    return evaluate_row_count_expr_impl(expr, scalars, externs);
}

auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b, std::size_t n)
    -> std::optional<ValidityBitmap> {
    return merge_validity(a, b, n);
}

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        // Reseat the shared_ptr rather than mutating shared data (copy-on-write).
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        columns[it->second].validity.reset();
        return;
    }
    const std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{
        .name = std::move(name),
        .column = std::make_shared<ColumnValue>(std::move(column)),
        .validity = std::nullopt,
    });
    index[columns.back().name] = pos;
}

void Table::add_column(std::string name, ColumnValue column, ValidityBitmap validity) {
    if (auto it = index.find(name); it != index.end()) {
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        columns[it->second].validity = std::move(validity);
        return;
    }
    const std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{
        .name = std::move(name),
        .column = std::make_shared<ColumnValue>(std::move(column)),
        .validity = std::move(validity),
    });
    index[columns.back().name] = pos;
}

void Table::replace_column(std::size_t pos, ColumnValue column) {
    auto& entry = columns.at(pos);
    entry.column = std::make_shared<ColumnValue>(std::move(column));
}

void Table::replace_column(std::size_t pos, ColumnValue column,
                           std::optional<ValidityBitmap> validity) {
    auto& entry = columns.at(pos);
    entry.column = std::make_shared<ColumnValue>(std::move(column));
    entry.validity = std::move(validity);
}

void Table::rename_column(std::size_t pos, std::string name) {
    auto& entry = columns.at(pos);
    if (auto it = index.find(entry.name); it != index.end() && it->second == pos) {
        index.erase(it);
    }
    entry.name = std::move(name);
    index[entry.name] = pos;
}

auto Table::mutable_column(std::size_t pos) -> ColumnValue& {
    auto& column = columns.at(pos).column;
    if (column.use_count() != 1) {
        column = std::make_shared<ColumnValue>(*column);
    }
    return *column;
}

void Table::add_column_shared(std::string name, std::shared_ptr<ColumnValue> column,
                              std::optional<ValidityBitmap> validity) {
    if (auto it = index.find(name); it != index.end()) {
        columns[it->second].column = std::move(column);
        columns[it->second].validity = std::move(validity);
        return;
    }
    const std::size_t pos = columns.size();
    columns.push_back(ColumnEntry{
        .name = std::move(name),
        .column = std::move(column),
        .validity = std::move(validity),
    });
    index[columns.back().name] = pos;
}

auto Table::find_entry(const std::string& name) const -> const ColumnEntry* {
    if (auto it = index.find(name); it != index.end()) {
        return &columns[it->second];
    }
    return nullptr;
}

auto Table::find(const std::string& name) -> ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

auto Table::find(const std::string& name) const -> const ColumnValue* {
    if (auto it = index.find(name); it != index.end()) {
        return columns[it->second].column.get();
    }
    return nullptr;
}

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
               const ExternRegistry* externs, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    tune_allocator_once();
    auto op = build_operator(node, registry, scalars, externs, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    MaterializeOperator sink{std::move(op.value())};
    return sink.run();
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<std::string>& keys, const ir::Expr* predicate,
                 const ScalarRegistry* scalars) -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys, predicate, scalars, compute_mask);
}

auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string> {
    if (table.rows() != 1) {
        return std::unexpected("scalar() requires exactly one row");
    }
    const auto* col = table.find(column);
    if (col == nullptr) {
        return std::unexpected("column not found: " + column);
    }
    return scalar_from_column(*col, 0);
}

auto is_scalar_builtin(std::string_view name) -> bool {
    // The registry now also holds column-kind builtins (Generators); "scalar
    // builtin" means the row-local kind only, as before the generalization.
    const auto* fn = find_builtin(name);
    return fn != nullptr && fn->kind == ir::FnKind::Scalar;
}

auto eval_scalar_builtin(std::string_view name, const std::vector<ScalarValue>& args)
    -> std::expected<ScalarValue, std::string> {
    const auto* found = find_builtin(name);
    if (found == nullptr || found->kind != ir::FnKind::Scalar) {
        return std::unexpected("not a scalar builtin: " + std::string(name));
    }
    const auto& fn = *found;
    const auto argc = static_cast<int>(args.size());
    if (argc < fn.min_args || (fn.max_args >= 0 && argc > fn.max_args)) {
        return std::unexpected(std::string(name) + ": wrong number of arguments");
    }
    // ScalarValue and the interpreter's ExprValue are the same variant, so the
    // already-evaluated scalar args pass straight into the registry's eval.
    return fn.eval(name, args);
}

auto aggregate_series(std::string_view name, const ColumnValue& column, double param)
    -> std::expected<ScalarValue, std::string> {
    auto func = parse_aggregate_func(name);
    if (!func.has_value()) {
        return std::unexpected("not an aggregate function: " + std::string(name));
    }
    // Reduce the series via the shared aggregate kernel on a one-column table.
    Table t;
    t.add_column("__series", column);
    ir::AggSpec spec{
        .func = *func,
        .column = ir::ColumnRef{.name = "__series"},
        .alias = "__agg",
        .param = param,
    };
    auto agg = aggregate_table(t, {}, std::vector<ir::AggSpec>{std::move(spec)});
    if (!agg.has_value()) {
        return std::unexpected(agg.error());
    }
    const auto* entry = agg->find_entry("__agg");
    if (entry == nullptr || entry->column == nullptr) {
        return std::unexpected(std::string(name) + "(): produced no result");
    }
    return scalar_from_column(*entry->column, 0);
}

}  // namespace ibex::runtime
