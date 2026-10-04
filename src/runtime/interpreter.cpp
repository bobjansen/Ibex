// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/query_lease.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <algorithm>
#include <atomic>
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
#include <set>
#include <span>
#include <string>
#include <string_view>
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
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace detail {

namespace {
std::atomic<bool> query_in_flight{false};
}

auto try_claim_query_execution() noexcept -> bool {
    bool expected = false;
    // A successful claim observes all writes from the preceding query's release;
    // a failed claim only needs to observe that the slot is occupied.
    return query_in_flight.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                   std::memory_order_acquire);
}

auto release_query_execution() noexcept -> void {
    query_in_flight.store(false, std::memory_order_release);
}

}  // namespace detail

namespace {

// Process-wide allocator tuning to flatten the large-buffer page-fault cliff.
//
// Every result column is backed by std::vector<T>, so any column above glibc's
// dynamic mmap threshold (grows up to 32 MB = 4M float64 rows) is served by a
// fresh mmap and munmapped on free. The next same-size allocation re-mmaps and
// re-faults every 4 KB page on first touch — a ~5x throughput cliff once columns
// cross ~32 MB (cumsum 0.7 -> 3.3 ns/row from 4M to 8M rows). Serving large
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
        if (col.name.empty()) {
            if (const auto& time_index = input.time_index();
                time_index.has_value() && !output.index.contains(*time_index)) {
                output.add_column_from(*time_index, *input.find_entry(*time_index));
            }
            continue;
        }
        const auto* entry = input.find_entry(col.name);
        if (entry == nullptr) {
            return std::unexpected("select column not found: " + col.name +
                                   " (available: " + format_columns(input) + ")");
        }
        // Share the column's shared_ptr instead of deep-copying its data. The
        // projected table is a read-only selection; under copy-on-write any
        // later mutation reseats a fresh column, so sharing is safe.
        output.add_column_from(col.name, *entry);
    }
    // A key or the time index survives only if its column survives the
    // selection; a dropped time index also voids the ordering.
    apply_table_properties(output, TableProperties::derive(
                                       table_properties_of(input),
                                       [&](const std::string& name) -> KeyFate {
                                           return output.index.contains(name) ? KeyFate::kept(name)
                                                                              : KeyFate::dropped();
                                       },
                                       RowTransform::Preserve));
    return output;
}

namespace {

/// Whether synthesizing `[min, max]` bound conjuncts is worth it for this
/// scan: only when the estimated pruning against the source's footer range is
/// at least ~20% (uniform-distribution estimate — the only one a min/max pair
/// supports). A near-full selection would push every non-key column onto the
/// gather-decode path, slower than the dense decode it replaces; no stats
/// means no proof, so no bounds. The publisher records raw bounds always —
/// this is the consumer-side policy.
auto bounds_worth_applying(const DeferredScan& scan) -> bool {
    const auto& stats = scan.lazy->column_stats();
    const auto stat = stats.find(scan.key_column);
    if (stat == stats.end() || scan.filter == nullptr) {
        return false;
    }
    const auto& col_stat = stat->second;
    if (!col_stat.min.has_value() || !col_stat.max.has_value() || !scan.filter->min.has_value() ||
        !scan.filter->max.has_value()) {
        return false;
    }
    const auto source_min = static_cast<double>(*col_stat.min);
    const auto source_max = static_cast<double>(*col_stat.max);
    const double kept_min = std::max(static_cast<double>(*scan.filter->min), source_min);
    const double kept_max = std::min(static_cast<double>(*scan.filter->max), source_max);
    const double source_span = source_max - source_min + 1.0;
    const double kept_span = std::max(0.0, kept_max - kept_min + 1.0);
    return kept_span / source_span <= 0.8;
}

}  // namespace

auto plan_deferred_scan(const DeferredScan& scan) -> DeferredScanPlan {
    DeferredScanPlan plan;
    plan.conjuncts = scan.conjuncts;
    if (scan.filter != nullptr && scan.filter->ready) {
        if (scan.filter->has_membership()) {
            plan.dynamic = scan.filter.get();
        }
        // Bound conjuncts only when there is no membership filter (the Bloom
        // was built from exactly these keys, so bounds add nothing to it) and
        // they provably prune.
        if (plan.dynamic == nullptr && scan.filter->min.has_value() &&
            scan.filter->max.has_value() && bounds_worth_applying(scan)) {
            const auto bound = [&](ir::CompareOp op, std::int64_t value) {
                plan.conjuncts.push_back(ir::Expr{ir::CompareExpr{
                    .op = op,
                    .left = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = scan.key_column}}),
                    .right = ir::make_expr_ptr(ir::Expr{ir::Literal{.value = value}}),
                }});
            };
            bound(ir::CompareOp::Ge, *scan.filter->min);
            bound(ir::CompareOp::Le, *scan.filter->max);
        }
    }
    plan.names = scan.demand;
    if (scan.demand_all) {
        for (const auto& field : scan.lazy->schema().columns) {
            plan.names.insert(field.name);
        }
    }
    return plan;
}

auto materialize_deferred_scan(const DeferredScan& scan, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    const auto plan = plan_deferred_scan(scan);
    if (plan.conjuncts.empty() && plan.dynamic == nullptr) {
        return scan.demand_all ? scan.lazy->materialize(exec)
                               : scan.lazy->project(scan.demand, exec);
    }
    return scan.lazy->project_where(plan.names, plan.conjuncts, exec, nullptr, plan.dynamic,
                                    plan.dynamic != nullptr ? &scan.key_column : nullptr);
}

auto deferred_scan_units(const DeferredScan& scan) -> std::vector<SourceUnit> {
    return scan.lazy == nullptr ? std::vector<SourceUnit>{} : scan.lazy->scan_units();
}

auto materialize_deferred_scan_unit(const DeferredScan& scan, const DeferredScanPlan& plan,
                                    const SourceUnit& unit, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    return scan.lazy->project_where_unit(plan.names, plan.conjuncts, unit, exec, nullptr,
                                         plan.dynamic,
                                         plan.dynamic != nullptr ? &scan.key_column : nullptr);
}

auto deferred_scan_key_selection(const DeferredScan& scan, const ExecutionContext& exec)
    -> std::expected<std::optional<LazyTable::JoinKeySelection>, std::string> {
    if (scan.filter == nullptr || !scan.filter->ready) {
        return std::optional<LazyTable::JoinKeySelection>{};
    }
    // Static conjuncts only — bound conjuncts are never synthesized when
    // membership exists (see materialize_deferred_scan), and phase A only
    // runs with membership.
    return scan.lazy->join_key_selection(scan.conjuncts, exec, nullptr, *scan.filter,
                                         scan.key_column);
}

auto materialize_deferred_scan_rows(const DeferredScan& scan, const Selection& rows,
                                    const ExecutionContext& exec, const ColumnEntry& key_column)
    -> std::expected<Table, std::string> {
    std::set<std::string> names = scan.demand;
    if (scan.demand_all) {
        for (const auto& field : scan.lazy->schema().columns) {
            names.insert(field.name);
        }
    }
    names.erase(scan.key_column);
    auto rest = scan.lazy->project_rows(names, rows, exec);
    if (!rest) {
        return std::unexpected(rest.error());
    }
    Table out;
    for (const auto& field : scan.lazy->schema().columns) {
        if (field.name == scan.key_column) {
            out.add_column_from(key_column.name, key_column);
            continue;
        }
        if (const auto* entry = rest->find_entry(field.name); entry != nullptr) {
            out.add_column_from(entry->name, *entry);
        }
    }
    out.logical_rows = rows.size();
    normalize_time_index(out);
    return out;
}

auto rename_table(Table input, const std::vector<ir::RenameSpec>& renames)
    -> std::expected<Table, std::string> {
    std::vector<std::string_view> input_names;
    input_names.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        input_names.push_back(entry.name);
    }
    const ir::ColumnNameMap names(renames);
    if (auto valid = names.validate_input(input_names); !valid.has_value()) {
        return std::unexpected(valid.error() + " (available: " + format_columns(input) + ")");
    }

    // Rename never drops a column, it relabels it; rewrite each key and the
    // time index to its new name.
    const TableProperties properties = TableProperties::derive(
        table_properties_of(input),
        [&](const std::string& name) -> KeyFate {
            return KeyFate::kept(std::string(names.output_name(name)));
        },
        RowTransform::Preserve);

    // The Table value is owned here. Relabel its entries and rebuild the name
    // index in place; column buffers and validity bitmaps are untouched.
    input.index.clear();
    input.index.reserve(input.columns.size());
    for (std::size_t pos = 0; pos < input.columns.size(); ++pos) {
        auto& entry = input.columns[pos];
        entry.name = names.output_name(entry.name);
        input.index.emplace(entry.name, pos);
    }
    apply_table_properties(input, properties);
    return input;
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
    if (std::holds_alternative<Column<Decimal>>(column)) {
        return ExprType::Decimal;
    }
    return ExprType::String;
}

auto evaluate_row_count_expr(const ir::Expr& expr, const ScalarRegistry* scalars,
                             const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string> {
    return evaluate_row_count_expr_impl(expr, scalars, externs);
}

auto evaluate_scalar_expr(const ir::Expr& expr, const ScalarRegistry* scalars,
                          const ExternRegistry* externs)
    -> std::expected<ScalarValue, std::string> {
    auto value = eval_expr(expr, Table{}, 0, scalars, externs);
    if (!value) {
        return std::unexpected(value.error());
    }
    return scalar_from_expr(*value);
}

auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b, std::size_t n)
    -> std::optional<ValidityBitmap> {
    return merge_validity(a, 0, b, 0, n);
}

void Table::add_column(std::string name, ColumnValue column) {
    if (auto it = index.find(name); it != index.end()) {
        // Reseat the shared_ptr rather than mutating shared data (copy-on-write).
        columns[it->second].column = std::make_shared<ColumnValue>(std::move(column));
        columns[it->second].validity.reset();
        // New storage under an existing name is a new column, not an edit of
        // the old one: its zone (if any) is the caller's to set afresh.
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

void Table::add_column_from(std::string name, const ColumnEntry& source) {
    add_column_shared(std::move(name), source.column, source.validity);
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
               const ExternRegistry* externs, ModelResult* model_out, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    // One query at a time (Phase 0 item 6): a concurrent or re-entrant top-level
    // entry is rejected rather than serialized. This is the single chokepoint —
    // internal recursion goes through build_operator, never back through this
    // public entry — so the lease is claimed exactly once per query.
    const QueryExecutionLease lease;
    if (!lease.held()) {
        return std::unexpected(query_in_flight_message());
    }
    tune_allocator_once();
    auto op = build_operator(node, registry, scalars, externs, exec, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    MaterializeOperator sink{std::move(op.value())};
    return sink.run();
}

auto interpret(const ir::Node& node, const TableRegistry& registry, const ScalarRegistry* scalars,
               const ExternRegistry* externs, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    // The environment is applied here, not left to each caller. Before this,
    // `ExecutionContext{}` was the only spelling available to a caller with no
    // opinion, and it meant "library defaults, ignore IBEX_*" — so every tool
    // that used this overload silently opted out of the knobs. `ibex_bench`
    // did, which made the suite's `-st` pass a second identical run of the
    // parallel binary and every ibex-vs-ibex-st ratio read exactly 1.00.
    // Nothing about that was visible in the output: `parallel` defaults to
    // true, so the work WAS parallel, and only the off switch and the stats
    // counter were dead.
    //
    // A caller that wants the environment ignored has a spelling for it —
    // build an ExecutionContext and pass it to the overload above. There was
    // no spelling for the opposite, which is the wrong way round for variables
    // named IBEX_*.
    ExecutionContext exec;
    configure_parallel_from_env(exec);
    return interpret(node, registry, scalars, externs, model_out, exec);
}

auto invoke_table_consumer(const ExternRegistry& externs, const std::string& callee,
                           const Table& input, const ExternArgs& args)
    -> std::expected<void, std::string> {
    const auto* function = externs.find(callee);
    if (function == nullptr) {
        return std::unexpected("unknown table consumer: " + callee);
    }
    if (!function->first_arg_is_table || !function->table_consumer_func) {
        return std::unexpected("extern function is not a table consumer: " + callee);
    }
    auto result = function->table_consumer_func(input, args);
    if (!result.has_value()) {
        return std::unexpected(result.error());
    }
    return {};
}

auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                 const std::vector<ir::JoinKey>& keys, const ir::Expr* predicate,
                 const ScalarRegistry* scalars, const ir::JoinSuffixPolicy& suffix,
                 ir::NullMatch null_match, const ir::JoinExpect& expect, ir::MatchSelection take)
    -> std::expected<Table, std::string> {
    return join_table_impl(left, right, kind, keys, predicate, scalars, compute_mask, suffix, {},
                           null_match, expect, take);
}

auto extract_scalar(const Table& table, const std::string& column, bool zero_rows_is_null)
    -> std::expected<ScalarValue, std::string> {
    if (table.rows() == 0 && zero_rows_is_null) {
        return ScalarValue{std::monostate{}};
    }
    if (table.rows() != 1) {
        return std::unexpected("scalar() requires exactly one row");
    }
    const auto* entry = table.find_entry(column);
    if (entry == nullptr || entry->column == nullptr) {
        return std::unexpected("column not found: " + column);
    }
    // A null cell yields a null scalar (the monostate alternative), not an
    // error -- see SPEC.md §6.7.
    if (entry->validity.has_value() && !(*entry->validity)[0]) {
        return ScalarValue{std::monostate{}};
    }
    return scalar_from_column(*entry->column, 0);
}

auto materialize_deferred_scalar_bindings(std::span<const ir::DeferredScalarBinding> bindings,
                                          const TableRegistry& tables, ScalarRegistry& scalars,
                                          const ExternRegistry* externs)
    -> std::expected<void, std::string> {
    for (const auto& binding : bindings) {
        for (const auto& source : binding.sources) {
            if (source.plan == nullptr) {
                return std::unexpected("deferred scalar '" + binding.name + "': missing subplan");
            }
            auto table = interpret(*source.plan, tables, &scalars, externs);
            if (!table) {
                return std::unexpected("deferred scalar '" + binding.name + "': " + table.error());
            }
            const std::string column = source.column.value_or(
                table->columns.empty() ? std::string{} : table->columns.front().name);
            if (!source.column.has_value() && table->columns.size() != 1) {
                return std::unexpected("scalar(<table>): '" + binding.name +
                                       "' subquery must have exactly one column");
            }
            auto value = extract_scalar(*table, column, /*zero_rows_is_null=*/!source.column);
            if (!value) {
                return std::unexpected("deferred scalar '" + binding.name + "': " + value.error());
            }
            scalars[source.tmp_name] = std::move(*value);
        }
        auto result = eval_expr(binding.value, Table{}, 0, &scalars, externs);
        if (!result) {
            return std::unexpected("deferred scalar '" + binding.name + "': " + result.error());
        }
        scalars[binding.name] = scalar_from_expr(*result);
    }
    return {};
}

auto is_scalar_builtin(std::string_view name) -> bool {
    // The registry also holds column-kind builtins (Generators); "scalar
    // builtin" means the row-local kind only, as before the generalization.
    const auto* fn = find_builtin(name);
    return fn != nullptr && std::holds_alternative<ScalarExec>(fn->exec);
}

auto eval_scalar_builtin(std::string_view name, const std::vector<ScalarValue>& args)
    -> std::expected<ScalarValue, std::string> {
    const auto* found = find_builtin(name);
    const auto* scalar_exec = found != nullptr ? std::get_if<ScalarExec>(&found->exec) : nullptr;
    if (scalar_exec == nullptr || scalar_exec->eval == nullptr) {
        return std::unexpected("not a scalar builtin: " + std::string(name));
    }
    const auto argc = static_cast<int>(args.size());
    if (argc < found->min_args || (found->max_args >= 0 && argc > found->max_args)) {
        return std::unexpected(std::string(name) + ": wrong number of arguments");
    }
    // The registry evaluates ExprValue; conversion to and from ScalarValue is
    // lossless, including ScalarValue's std::monostate null alternative.
    std::vector<ExprValue> expr_args;
    expr_args.reserve(args.size());
    for (const auto& a : args) {
        expr_args.push_back(expr_from_scalar(a));
    }
    auto result = scalar_exec->eval(name, expr_args);
    if (!result) {
        return std::unexpected(result.error());
    }
    // A null result is now a valid scalar (the monostate alternative); null
    // propagation and the null-handling builtins (coalesce, is null) rely on
    // it reaching the caller rather than erroring here.
    return scalar_from_expr(*result);
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
    // An aggregate with no valid observations reduces to a null scalar (the
    // monostate alternative), consistent with the column path.
    if (entry->validity.has_value() && !(*entry->validity)[0]) {
        return ScalarValue{std::monostate{}};
    }
    return scalar_from_column(*entry->column, 0);
}

}  // namespace ibex::runtime
