#include <ibex/ir/builder.hpp>
#include <ibex/runtime/ops.hpp>
#include <ibex/runtime/table_format.hpp>

#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

namespace ibex::ops {

namespace {

// Scratch table name used when we wrap an in-memory table in a one-node IR plan.
// It is intentionally internal and never exposed to users.
constexpr const char* kSrcKey = "__ibex__";
const runtime::ScalarRegistry* g_scalars = nullptr;

auto delegate(ir::NodePtr node, const runtime::Table& src) -> runtime::Table {
    // Route all convenience ops (filter/project/order/...) through the same
    // interpreter entry point so behavior stays aligned with the query engine.
    runtime::TableRegistry reg;
    reg.emplace(kSrcKey, src);
    auto result = runtime::interpret(*node, reg, g_scalars, nullptr);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto delegate_with_registry(ir::NodePtr node, const runtime::TableRegistry& reg) -> runtime::Table {
    auto result = runtime::interpret(*node, reg, g_scalars, nullptr);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto to_col_refs(const std::vector<std::string>& names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> refs;
    refs.reserve(names.size());
    for (const auto& n : names) {
        refs.push_back(ir::ColumnRef{.name = n});
    }
    return refs;
}

}  // namespace

// ─── Core ops ─────────────────────────────────────────────────────────────────

void set_scalars(const runtime::ScalarRegistry* scalars) {
    g_scalars = scalars;
}

auto eval_row_count(const ir::Expr& expr) -> std::size_t {
    auto count = runtime::evaluate_row_count_expr(expr, g_scalars, nullptr);
    if (!count) {
        throw std::runtime_error(count.error());
    }
    return *count;
}

auto filter(const runtime::Table& t, ir::Expr pred) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto filter_node = b.filter(std::move(pred));
    filter_node->add_child(std::move(scan_node));
    return delegate(std::move(filter_node), t);
}

auto project(const runtime::Table& t, const std::vector<std::string>& col_names) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto proj_node = b.project(to_col_refs(col_names));
    proj_node->add_child(std::move(scan_node));
    return delegate(std::move(proj_node), t);
}

auto distinct(const runtime::Table& t) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto distinct_node = b.distinct();
    distinct_node->add_child(std::move(scan_node));
    return delegate(std::move(distinct_node), t);
}

auto order(const runtime::Table& t, const std::vector<ir::OrderKey>& keys) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto order_node = b.order(keys);
    order_node->add_child(std::move(scan_node));
    return delegate(std::move(order_node), t);
}

auto head(const runtime::Table& t, std::size_t count, const std::vector<std::string>& group_by)
    -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto head_node = b.head(count, to_col_refs(group_by));
    head_node->add_child(std::move(scan_node));
    return delegate(std::move(head_node), t);
}

auto tail(const runtime::Table& t, std::size_t count, const std::vector<std::string>& group_by)
    -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto tail_node = b.tail(count, to_col_refs(group_by));
    tail_node->add_child(std::move(scan_node));
    return delegate(std::move(tail_node), t);
}

auto top_k(const runtime::Table& t, const std::vector<ir::OrderKey>& keys, std::size_t count,
           const std::vector<std::string>& group_by, bool keep_first) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    const auto keep = keep_first ? ir::TopKNode::KeepMode::First : ir::TopKNode::KeepMode::Last;
    auto topk_node = b.top_k(keys, count, to_col_refs(group_by), keep);
    topk_node->add_child(std::move(scan_node));
    return delegate(std::move(topk_node), t);
}

auto aggregate(const runtime::Table& t, const std::vector<std::string>& group_by,
               const std::vector<ir::AggSpec>& aggs) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto agg_node = b.aggregate(to_col_refs(group_by), aggs);
    agg_node->add_child(std::move(scan_node));
    return delegate(std::move(agg_node), t);
}

auto resample(const runtime::Table& t, ir::Duration duration,
              const std::vector<std::string>& group_by, const std::vector<ir::AggSpec>& aggs)
    -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto resample_node = b.resample(duration, to_col_refs(group_by), aggs);
    resample_node->add_child(std::move(scan_node));
    return delegate(std::move(resample_node), t);
}

auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto upd_node = b.update(fields);
    upd_node->add_child(std::move(scan_node));
    return delegate(std::move(upd_node), t);
}

auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields,
            const std::vector<TupleSource>& tuple_sources, const std::vector<std::string>& group_by)
    -> runtime::Table {
    if (tuple_sources.empty() && group_by.empty()) {
        return update(t, fields);
    }

    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);

    runtime::TableRegistry reg;
    reg.emplace(kSrcKey, t);

    std::vector<ir::TupleFieldSpec> tuple_specs;
    tuple_specs.reserve(tuple_sources.size());

    for (std::size_t i = 0; i < tuple_sources.size(); ++i) {
        const auto source_name = "__ibex_tuple_" + std::to_string(i);
        reg.emplace(source_name, tuple_sources[i].table);
        tuple_specs.push_back(ir::TupleFieldSpec{
            .aliases = tuple_sources[i].aliases,
            .source = b.scan(source_name),
        });
    }

    auto upd_node = b.update(fields, std::move(tuple_specs), to_col_refs(group_by));
    upd_node->add_child(std::move(scan_node));
    return delegate_with_registry(std::move(upd_node), reg);
}

auto rename(const runtime::Table& t, const std::vector<ir::RenameSpec>& renames) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto rename_node = b.rename(renames);
    rename_node->add_child(std::move(scan_node));
    return delegate(std::move(rename_node), t);
}

auto as_timeframe(const runtime::Table& t, const std::string& column) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto atf_node = b.as_timeframe(column);
    atf_node->add_child(std::move(scan_node));
    return delegate(std::move(atf_node), t);
}

auto ascribe(const runtime::Table& t, std::vector<ir::SchemaField> schema, bool open)
    -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto asc_node = b.ascribe(std::move(schema), open);
    asc_node->add_child(std::move(scan_node));
    return delegate(std::move(asc_node), t);
}

auto columns(const runtime::Table& t) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto columns_node = b.columns();
    columns_node->add_child(std::move(scan_node));
    return delegate(std::move(columns_node), t);
}

auto windowed_update(const runtime::Table& t, ir::Duration duration,
                     const std::vector<ir::FieldSpec>& fields) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto upd_node = b.update(fields);
    upd_node->add_child(std::move(scan_node));
    auto win_node = b.window(duration);
    win_node->add_child(std::move(upd_node));
    return delegate(std::move(win_node), t);
}

auto melt(const runtime::Table& t, const std::vector<std::string>& id_cols,
          const std::vector<std::string>& measure_cols) -> runtime::Table {
    ir::Builder b;
    auto scan = b.scan(kSrcKey);
    auto melt_node = b.melt(id_cols, measure_cols);
    melt_node->add_child(std::move(scan));
    return delegate(std::move(melt_node), t);
}

auto dcast(const runtime::Table& t, const std::string& pivot_col, const std::string& value_col,
           const std::vector<std::string>& row_keys) -> runtime::Table {
    ir::Builder b;
    auto scan = b.scan(kSrcKey);
    auto dcast_node = b.dcast(pivot_col, value_col, row_keys);
    dcast_node->add_child(std::move(scan));
    return delegate(std::move(dcast_node), t);
}

auto cov(const runtime::Table& t) -> runtime::Table {
    ir::Builder b;
    auto scan = b.scan(kSrcKey);
    auto node = b.cov();
    node->add_child(std::move(scan));
    return delegate(std::move(node), t);
}

auto corr(const runtime::Table& t) -> runtime::Table {
    ir::Builder b;
    auto scan = b.scan(kSrcKey);
    auto node = b.corr();
    node->add_child(std::move(scan));
    return delegate(std::move(node), t);
}

auto transpose(const runtime::Table& t) -> runtime::Table {
    ir::Builder b;
    auto scan = b.scan(kSrcKey);
    auto node = b.transpose();
    node->add_child(std::move(scan));
    return delegate(std::move(node), t);
}

auto matmul(const runtime::Table& left, const runtime::Table& right) -> runtime::Table {
    constexpr const char* kRightKey = "__ibex_right__";
    ir::Builder b;
    auto left_scan = b.scan(kSrcKey);
    auto right_scan = b.scan(kRightKey);
    auto node = b.matmul();
    node->add_child(std::move(left_scan));
    node->add_child(std::move(right_scan));
    runtime::TableRegistry reg;
    reg.emplace(kSrcKey, left);
    reg.emplace(kRightKey, right);
    return delegate_with_registry(std::move(node), reg);
}

auto model_coef(const runtime::ModelResult& m) -> runtime::Table {
    return m.coefficients;
}
auto model_summary(const runtime::ModelResult& m) -> runtime::Table {
    return m.summary;
}
auto model_fitted(const runtime::ModelResult& m) -> runtime::Table {
    return m.fitted_values;
}
auto model_residuals(const runtime::ModelResult& m) -> runtime::Table {
    return m.residuals;
}
auto model_r_squared(const runtime::ModelResult& m) -> double {
    return m.r_squared;
}

auto inner_join(const runtime::Table& left, const runtime::Table& right,
                const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Inner, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto left_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto right_join(const runtime::Table& left, const runtime::Table& right,
                const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Right, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto outer_join(const runtime::Table& left, const runtime::Table& right,
                const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Outer, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto semi_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    auto result = runtime::join_tables(left, right, ir::JoinKind::Semi, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto anti_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    auto result = runtime::join_tables(left, right, ir::JoinKind::Anti, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto cross_join(const runtime::Table& left, const runtime::Table& right) -> runtime::Table {
    auto result = runtime::join_tables(left, right, ir::JoinKind::Cross, {}, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto asof_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Asof, keys, nullptr, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto join_with_predicate(const runtime::Table& left, const runtime::Table& right, ir::JoinKind kind,
                         const std::vector<std::string>& keys, const ir::Expr& predicate)
    -> runtime::Table {
    auto result = runtime::join_tables(left, right, kind, keys, &predicate, g_scalars);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

void print(const runtime::Table& t, std::ostream& out) {
    // Delegate to the shared formatter so transpiled output is byte-identical
    // to the interpreter REPL / ibex_eval path (capped at 10 rows like there).
    runtime::format_table(t, out);
}

// ─── Expression builders ──────────────────────────────────────────────────────

auto col_ref(std::string name) -> ir::Expr {
    return ir::Expr{ir::ColumnRef{.name = std::move(name)}};
}

auto int_lit(std::int64_t v) -> ir::Expr {
    return ir::Expr{ir::Literal{v}};
}

auto dbl_lit(double v) -> ir::Expr {
    return ir::Expr{ir::Literal{v}};
}

auto str_lit(std::string v) -> ir::Expr {
    return ir::Expr{ir::Literal{std::move(v)}};
}

auto date_lit(Date v) -> ir::Expr {
    return ir::Expr{ir::Literal{v}};
}

auto timestamp_lit(Timestamp v) -> ir::Expr {
    return ir::Expr{ir::Literal{v}};
}

auto binop(ir::ArithmeticOp op, ir::Expr lhs, ir::Expr rhs) -> ir::Expr {
    return ir::Expr{ir::BinaryExpr{
        .op = op,
        .left = ir::make_expr_ptr(std::move(lhs)),
        .right = ir::make_expr_ptr(std::move(rhs)),
    }};
}

auto fn_call(std::string callee, std::vector<ir::Expr> args, std::vector<NamedArgExpr> named_args)
    -> ir::Expr {
    ir::CallExpr call;
    call.callee = std::move(callee);
    call.args.reserve(args.size());
    for (auto& arg : args) {
        call.args.push_back(ir::make_expr_ptr(std::move(arg)));
    }
    call.named_args.reserve(named_args.size());
    for (auto& narg : named_args) {
        call.named_args.push_back(ir::NamedArg{.name = std::move(narg.name),
                                               .value = ir::make_expr_ptr(std::move(narg.value))});
    }
    return ir::Expr{std::move(call)};
}

auto rank_expr(std::vector<ir::OrderKey> order_keys, ir::RankMethod method,
               ir::RankNaOption na_option, bool pct) -> ir::Expr {
    ir::RankExpr rank;
    rank.order_keys = std::move(order_keys);
    rank.method = method;
    rank.na_option = na_option;
    rank.pct = pct;
    return ir::Expr{std::move(rank)};
}

// ─── Predicate builders ───────────────────────────────────────────────────────

namespace {
auto mk_expr(ir::Expr e) -> ir::ExprPtr {
    return ir::make_expr_ptr(std::move(e));
}
auto lit_expr(std::variant<std::int64_t, double, bool, std::string, Date, Timestamp> v)
    -> ir::Expr {
    return ir::Expr{.node = ir::Literal{.value = std::move(v)}};
}
}  // namespace

auto filter_col(std::string name) -> ir::Expr {
    return ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}};
}

auto filter_int(std::int64_t v) -> ir::Expr {
    return lit_expr(v);
}
auto filter_dbl(double v) -> ir::Expr {
    return lit_expr(v);
}
auto filter_bool(bool v) -> ir::Expr {
    return lit_expr(v);
}
auto filter_str(std::string v) -> ir::Expr {
    return lit_expr(std::move(v));
}
auto filter_date(Date v) -> ir::Expr {
    return lit_expr(v);
}
auto filter_timestamp(Timestamp v) -> ir::Expr {
    return lit_expr(v);
}

auto filter_arith(ir::ArithmeticOp op, ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::BinaryExpr{
                        .op = op, .left = mk_expr(std::move(l)), .right = mk_expr(std::move(r))}};
}

auto filter_call(std::string callee, std::vector<ir::Expr> args) -> ir::Expr {
    ir::CallExpr call;
    call.callee = std::move(callee);
    call.args.reserve(args.size());
    for (auto& arg : args) {
        call.args.push_back(mk_expr(std::move(arg)));
    }
    return ir::Expr{.node = std::move(call)};
}

auto filter_cmp(ir::CompareOp op, ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{
                        .op = op, .left = mk_expr(std::move(l)), .right = mk_expr(std::move(r))}};
}

auto filter_and(ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                            .left = mk_expr(std::move(l)),
                                            .right = mk_expr(std::move(r))}};
}

auto filter_or(ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::Or,
                                            .left = mk_expr(std::move(l)),
                                            .right = mk_expr(std::move(r))}};
}

auto filter_not(ir::Expr operand) -> ir::Expr {
    return ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::Not,
                                            .left = mk_expr(std::move(operand)),
                                            .right = nullptr}};
}

auto filter_is_null(ir::Expr operand) -> ir::Expr {
    return ir::Expr{.node =
                        ir::IsNullExpr{.operand = mk_expr(std::move(operand)), .negated = false}};
}

auto filter_is_not_null(ir::Expr operand) -> ir::Expr {
    return ir::Expr{.node =
                        ir::IsNullExpr{.operand = mk_expr(std::move(operand)), .negated = true}};
}

// ─── Compound builders ────────────────────────────────────────────────────────

auto make_field(std::string alias, ir::Expr expr) -> ir::FieldSpec {
    return ir::FieldSpec{.alias = std::move(alias), .expr = std::move(expr)};
}

auto make_agg(ir::AggFunc func, std::string col_name, std::string alias, double param)
    -> ir::AggSpec {
    return ir::AggSpec{.func = func,
                       .column = ir::ColumnRef{.name = std::move(col_name)},
                       .alias = std::move(alias),
                       .param = param};
}

// ─── Stream helpers ───────────────────────────────────────────────────────────

void stream_append_row(runtime::Table& dst, const runtime::Table& src, std::size_t row) {
    if (row >= src.rows())
        return;

    // Lazily initialise dst schema on first append.
    if (dst.columns.empty()) {
        for (const auto& entry : src.columns) {
            runtime::ColumnValue empty = std::visit(
                [](const auto& c) -> runtime::ColumnValue { return std::decay_t<decltype(c)>{}; },
                *entry.column);
            dst.add_column(entry.name, std::move(empty));
        }
        dst.time_index = src.time_index;
        dst.ordering = src.ordering;
    }

    for (std::size_t ci = 0; ci < src.columns.size() && ci < dst.columns.size(); ++ci) {
        const std::size_t prev_size =
            std::visit([](const auto& c) { return c.size(); }, *dst.columns[ci].column);

        std::visit(
            [&](auto& dc) {
                using D = std::decay_t<decltype(dc)>;
                const auto* sc = std::get_if<D>(&*src.columns[ci].column);
                if (sc)
                    dc.push_back((*sc)[row]);
            },
            *dst.columns[ci].column);

        const bool null = runtime::is_null(src.columns[ci], row);
        if (null) {
            if (!dst.columns[ci].validity.has_value()) {
                dst.columns[ci].validity = runtime::ValidityBitmap(prev_size, true);
            }
            dst.columns[ci].validity->push_back(false);
        } else if (dst.columns[ci].validity.has_value()) {
            dst.columns[ci].validity->push_back(true);
        }
    }
}

auto stream_get_ts_ns(const runtime::Table& t, std::size_t row) -> std::optional<std::int64_t> {
    if (!t.time_index.has_value())
        return std::nullopt;
    const auto* col = t.find(*t.time_index);
    if (col == nullptr)
        return std::nullopt;
    return std::visit(
        [row](const auto& c) -> std::optional<std::int64_t> {
            using C = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<C, Column<Timestamp>>) {
                if (row < c.size())
                    return static_cast<std::int64_t>(c[row].nanos);
            } else if constexpr (std::is_same_v<C, Column<std::int64_t>>) {
                if (row < c.size())
                    return c[row];
            }
            return std::nullopt;
        },
        *col);
}

}  // namespace ibex::ops
