#include <ibex/ir/builder.hpp>
#include <ibex/runtime/ops.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace ibex::ops {

namespace {

constexpr const char* kSrcKey = "__ibex__";

auto delegate(ir::NodePtr node, const runtime::Table& src) -> runtime::Table {
    runtime::TableRegistry reg;
    reg.emplace(kSrcKey, src);
    auto result = runtime::interpret(*node, reg, nullptr, nullptr);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto to_col_refs(const std::vector<std::string>& names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> refs;
    refs.reserve(names.size());
    for (const auto& n : names) {
        refs.push_back(ir::ColumnRef{n});
    }
    return refs;
}

auto format_value(const runtime::ColumnValue& col, std::size_t row) -> std::string {
    return std::visit(
        [row](const auto& c) -> std::string {
            using T = typename std::decay_t<decltype(c)>::value_type;
            if constexpr (std::is_same_v<T, std::string>) {
                return c[row];
            } else if constexpr (std::is_same_v<T, std::string_view>) {
                return std::string(c[row]);
            } else if constexpr (std::is_same_v<T, double>) {
                double v = c[row];
                if (std::isnan(v))
                    return "nan";
                if (std::isinf(v))
                    return v > 0 ? "inf" : "-inf";
                std::string s = fmt::format("{:g}", v);
                return s;
            } else {
                return std::to_string(c[row]);
            }
        },
        col);
}

}  // namespace

// ─── Core ops ─────────────────────────────────────────────────────────────────

auto filter(const runtime::Table& t, ir::FilterExprPtr pred) -> runtime::Table {
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

auto aggregate(const runtime::Table& t, const std::vector<std::string>& group_by,
               const std::vector<ir::AggSpec>& aggs) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto agg_node = b.aggregate(to_col_refs(group_by), aggs);
    agg_node->add_child(std::move(scan_node));
    return delegate(std::move(agg_node), t);
}

auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields) -> runtime::Table {
    ir::Builder b;
    auto scan_node = b.scan(kSrcKey);
    auto upd_node = b.update(fields);
    upd_node->add_child(std::move(scan_node));
    return delegate(std::move(upd_node), t);
}

auto inner_join(const runtime::Table& left, const runtime::Table& right,
                const std::vector<std::string>& keys) -> runtime::Table {
    auto result = runtime::join_tables(left, right, ir::JoinKind::Inner, keys);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto left_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    auto result = runtime::join_tables(left, right, ir::JoinKind::Left, keys);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

void print(const runtime::Table& t, std::ostream& out) {
    if (t.columns.empty()) {
        out << "(empty table)\n";
        return;
    }

    std::size_t rows = t.rows();

    // Collect all cell strings and compute column widths.
    std::vector<std::vector<std::string>> cells(t.columns.size());
    std::vector<std::size_t> widths(t.columns.size());

    for (std::size_t c = 0; c < t.columns.size(); ++c) {
        widths[c] = t.columns[c].name.size();
        cells[c].reserve(rows);
        for (std::size_t r = 0; r < rows; ++r) {
            auto s = format_value(*t.columns[c].column, r);
            widths[c] = std::max(widths[c], s.size());
            cells[c].push_back(std::move(s));
        }
    }

    // Header row.
    for (std::size_t c = 0; c < t.columns.size(); ++c) {
        if (c > 0)
            out << "  ";
        out << fmt::format("{:<{}}", t.columns[c].name, widths[c]);
    }
    out << "\n";

    // Separator.
    for (std::size_t c = 0; c < t.columns.size(); ++c) {
        if (c > 0)
            out << "  ";
        out << std::string(widths[c], '-');
    }
    out << "\n";

    // Data rows.
    for (std::size_t r = 0; r < rows; ++r) {
        for (std::size_t c = 0; c < t.columns.size(); ++c) {
            if (c > 0)
                out << "  ";
            out << fmt::format("{:<{}}", cells[c][r], widths[c]);
        }
        out << "\n";
    }
}

// ─── Expression builders ──────────────────────────────────────────────────────

auto col_ref(std::string name) -> ir::Expr {
    return ir::Expr{ir::ColumnRef{std::move(name)}};
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

auto binop(ir::ArithmeticOp op, ir::Expr lhs, ir::Expr rhs) -> ir::Expr {
    return ir::Expr{ir::BinaryExpr{
        op,
        std::make_shared<ir::Expr>(std::move(lhs)),
        std::make_shared<ir::Expr>(std::move(rhs)),
    }};
}

auto fn_call(std::string callee, std::vector<ir::Expr> args) -> ir::Expr {
    ir::CallExpr call;
    call.callee = std::move(callee);
    call.args.reserve(args.size());
    for (auto& arg : args) {
        call.args.push_back(std::make_shared<ir::Expr>(std::move(arg)));
    }
    return ir::Expr{std::move(call)};
}

// ─── FilterExpr builders ──────────────────────────────────────────────────────

auto filter_col(std::string name) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterColumn{std::move(name)}});
}

auto filter_int(std::int64_t v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterLiteral{std::variant<std::int64_t, double, std::string>{v}}});
}

auto filter_dbl(double v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterLiteral{std::variant<std::int64_t, double, std::string>{v}}});
}

auto filter_str(std::string v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        ir::FilterLiteral{std::variant<std::int64_t, double, std::string>{std::move(v)}}});
}

auto filter_arith(ir::ArithmeticOp op, ir::FilterExprPtr l, ir::FilterExprPtr r)
    -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterArith{op, std::move(l), std::move(r)}});
}

auto filter_cmp(ir::CompareOp op, ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterCmp{op, std::move(l), std::move(r)}});
}

auto filter_and(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterAnd{std::move(l), std::move(r)}});
}

auto filter_or(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterOr{std::move(l), std::move(r)}});
}

auto filter_not(ir::FilterExprPtr operand) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterNot{std::move(operand)}});
}

// ─── Compound builders ────────────────────────────────────────────────────────

auto make_field(std::string alias, ir::Expr expr) -> ir::FieldSpec {
    return ir::FieldSpec{std::move(alias), std::move(expr)};
}

auto make_agg(ir::AggFunc func, std::string col_name, std::string alias) -> ir::AggSpec {
    return ir::AggSpec{func, ir::ColumnRef{std::move(col_name)}, std::move(alias)};
}

}  // namespace ibex::ops
