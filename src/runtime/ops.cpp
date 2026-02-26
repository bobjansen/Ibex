#include <ibex/ir/builder.hpp>
#include <ibex/runtime/ops.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>

namespace ibex::ops {

namespace {

// Scratch table name used when we wrap an in-memory table in a one-node IR plan.
// It is intentionally internal and never exposed to users.
constexpr const char* kSrcKey = "__ibex__";

auto delegate(ir::NodePtr node, const runtime::Table& src) -> runtime::Table {
    // Route all convenience ops (filter/project/order/...) through the same
    // interpreter entry point so behavior stays aligned with the query engine.
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
        refs.push_back(ir::ColumnRef{.name = n});
    }
    return refs;
}

auto format_date(ibex::Date date) -> std::string {
    using namespace std::chrono;
    sys_days day = sys_days{days{date.days}};
    year_month_day ymd{day};
    return fmt::format("{:04}-{:02}-{:02}", static_cast<int>(ymd.year()),
                       static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()));
}

auto format_timestamp(ibex::Timestamp ts) -> std::string {
    using namespace std::chrono;
    sys_time<nanoseconds> tp{nanoseconds{ts.nanos}};
    auto day = floor<days>(tp);
    year_month_day ymd{day};
    auto tod = tp - day;
    hh_mm_ss<nanoseconds> hms{tod};
    return fmt::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:09}", static_cast<int>(ymd.year()),
                       static_cast<unsigned>(ymd.month()), static_cast<unsigned>(ymd.day()),
                       hms.hours().count(), hms.minutes().count(), hms.seconds().count(),
                       hms.subseconds().count());
}

auto format_value(const runtime::ColumnValue& col, std::size_t row) -> std::string {
    // Table printer normalization:
    // - keep temporal types human-readable,
    // - keep NaN/Inf explicit,
    // - avoid surprising std::to_string formatting for doubles.
    return std::visit(
        [row](const auto& c) -> std::string {
            using T = typename std::decay_t<decltype(c)>::value_type;
            if constexpr (std::is_same_v<T, std::string>) {
                return c[row];
            } else if constexpr (std::is_same_v<T, std::string_view>) {
                return std::string(c[row]);
            } else if constexpr (std::is_same_v<T, ibex::Date>) {
                return format_date(c[row]);
            } else if constexpr (std::is_same_v<T, ibex::Timestamp>) {
                return format_timestamp(c[row]);
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

auto inner_join(const runtime::Table& left, const runtime::Table& right,
                const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
    auto result = runtime::join_tables(left, right, ir::JoinKind::Inner, keys);
    if (!result) {
        throw std::runtime_error(result.error());
    }
    return std::move(*result);
}

auto left_join(const runtime::Table& left, const runtime::Table& right,
               const std::vector<std::string>& keys) -> runtime::Table {
    // Joins already have a dedicated runtime path; call it directly.
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
        .left = std::make_shared<ir::Expr>(std::move(lhs)),
        .right = std::make_shared<ir::Expr>(std::move(rhs)),
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
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        ir::FilterLiteral{std::variant<std::int64_t, double, std::string, Date, Timestamp>{v}}});
}

auto filter_dbl(double v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        ir::FilterLiteral{std::variant<std::int64_t, double, std::string, Date, Timestamp>{v}}});
}

auto filter_str(std::string v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterLiteral{
        std::variant<std::int64_t, double, std::string, Date, Timestamp>{std::move(v)}}});
}

auto filter_date(Date v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        ir::FilterLiteral{std::variant<std::int64_t, double, std::string, Date, Timestamp>{v}}});
}

auto filter_timestamp(Timestamp v) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{
        ir::FilterLiteral{std::variant<std::int64_t, double, std::string, Date, Timestamp>{v}}});
}

auto filter_arith(ir::ArithmeticOp op, ir::FilterExprPtr l, ir::FilterExprPtr r)
    -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterArith{.op = op, .left = std::move(l), .right = std::move(r)}});
}

auto filter_cmp(ir::CompareOp op, ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterCmp{.op = op, .left = std::move(l), .right = std::move(r)}});
}

auto filter_and(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterAnd{.left = std::move(l), .right = std::move(r)}});
}

auto filter_or(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(
        ir::FilterExpr{ir::FilterOr{.left = std::move(l), .right = std::move(r)}});
}

auto filter_not(ir::FilterExprPtr operand) -> ir::FilterExprPtr {
    return std::make_unique<ir::FilterExpr>(ir::FilterExpr{ir::FilterNot{std::move(operand)}});
}

// ─── Compound builders ────────────────────────────────────────────────────────

auto make_field(std::string alias, ir::Expr expr) -> ir::FieldSpec {
    return ir::FieldSpec{.alias = std::move(alias), .expr = std::move(expr)};
}

auto make_agg(ir::AggFunc func, std::string col_name, std::string alias) -> ir::AggSpec {
    return ir::AggSpec{.func = func,
                       .column = ir::ColumnRef{.name = std::move(col_name)},
                       .alias = std::move(alias)};
}

}  // namespace ibex::ops
