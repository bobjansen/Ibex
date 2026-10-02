// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/codegen/emitter.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/ops.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace ibex;
using namespace ibex::ops;

// Helper: emit an IR tree to a string.
static auto emit_to_string(const ir::Node& root, const codegen::Emitter::Config& cfg)
    -> std::string {
    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, root, cfg);
    return oss.str();
}

static auto emit_to_string(const ir::Node& root) -> std::string {
    return emit_to_string(root, codegen::Emitter::Config{});
}

// Check that a string contains a substring.
static auto contains(const std::string& haystack, const std::string& needle) -> bool {
    return haystack.contains(needle);
}

// Helper: create a leaf ExternCallNode representing a table data source.
static auto make_source(ir::Builder& b, std::string_view path) -> ir::NodePtr {
    return b.extern_call("read_csv", {ir::Expr{ir::Literal{std::string(path)}}});
}

// --- ExternCall ---------------------------------------------------------------

TEST_CASE("emitter: extern call node", "[codegen]") {
    ir::Builder b;
    auto root = make_source(b, "trades.csv");
    auto out = emit_to_string(*root);

    CHECK(contains(out, "#include <ibex/runtime/ops.hpp>"));
    CHECK(contains(out, "int main()"));
    CHECK(contains(out, "read_csv(\"trades.csv\")"));
    CHECK(contains(out, "ibex::ops::print("));
    CHECK(contains(out, "return 0;"));
}

TEST_CASE("lower/codegen: table extern named args are bound before emission", "[codegen]") {
    const char* src = R"(
extern fn read_csv(
    path: String,
    nulls: String = "",
    delimiter: String = ",",
    has_header: Bool = true,
    schema: String = ""
) -> DataFrame from "csv.hpp";

read_csv("employees.csv", nulls = "<empty>", schema = "id:int,name:str");
)";

    auto parsed = parser::parse(src);
    REQUIRE(parsed.has_value());
    auto lowered = parser::lower(*parsed);
    REQUIRE(lowered.has_value());

    auto out = emit_to_string(**lowered);
    CHECK(contains(out, R"(read_csv("employees.csv", "<empty>", ",", 1, "id:int,name:str"))"));
}

// --- Filter ------------------------------------------------------------------

TEST_CASE("emitter: filter node - int64 predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(
        ops::filter_cmp(ir::CompareOp::Gt, ops::filter_col("price"), ops::filter_int(100)));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "read_csv(\"data.csv\")"));
    CHECK(contains(out, "ibex::ops::filter("));
    CHECK(contains(out, "ibex::ir::CompareOp::Gt"));
    CHECK(contains(out, "std::int64_t{100}"));
}

TEST_CASE("emitter: filter node - double predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(
        ops::filter_cmp(ir::CompareOp::Le, ops::filter_col("ratio"), ops::filter_dbl(0.5)));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Le"));
    CHECK(contains(out, "0.5"));
}

TEST_CASE("emitter: filter node - string predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(
        ops::filter_cmp(ir::CompareOp::Eq, ops::filter_col("symbol"), ops::filter_str("AAPL")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Eq"));
    CHECK(contains(out, "\"AAPL\""));
}

TEST_CASE("emitter: filter node - AND compound predicate", "[codegen]") {
    ir::Builder b;
    // price > 10 && qty < 5
    auto filter = b.filter(ops::filter_and(
        ops::filter_cmp(ir::CompareOp::Gt, ops::filter_col("price"), ops::filter_int(10)),
        ops::filter_cmp(ir::CompareOp::Lt, ops::filter_col("qty"), ops::filter_int(5))));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_and("));
    CHECK(contains(out, "ibex::ir::CompareOp::Gt"));
    CHECK(contains(out, "ibex::ir::CompareOp::Lt"));
}

TEST_CASE("emitter: filter node - arithmetic in predicate", "[codegen]") {
    ir::Builder b;
    // price * 2 > 100
    auto filter = b.filter(ops::filter_cmp(
        ir::CompareOp::Gt,
        ops::filter_arith(ir::ArithmeticOp::Mul, ops::filter_col("price"), ops::filter_int(2)),
        ops::filter_int(100)));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_arith("));
    CHECK(contains(out, "ibex::ir::ArithmeticOp::Mul"));
    CHECK(contains(out, "ibex::ir::CompareOp::Gt"));
}

TEST_CASE("emitter: filter node - is null predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(ops::filter_is_null(ops::filter_col("dept_name")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_is_null("));
    CHECK(contains(out, "ibex::ops::filter_col(\"dept_name\")"));
}

TEST_CASE("emitter: filter node - is not null predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(ops::filter_is_not_null(ops::filter_col("dept_name")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_is_not_null("));
    CHECK(contains(out, "ibex::ops::filter_col(\"dept_name\")"));
}

// --- Project -----------------------------------------------------------------

TEST_CASE("emitter: project node", "[codegen]") {
    ir::Builder b;
    auto proj = b.project({ir::ColumnRef{.name = "symbol"}, ir::ColumnRef{.name = "price"}});
    proj->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*proj);
    CHECK(contains(out, "ibex::ops::project("));
    CHECK(contains(out, "\"symbol\""));
    CHECK(contains(out, "\"price\""));
}

TEST_CASE("emitter: distinct node", "[codegen]") {
    ir::Builder b;
    auto distinct = b.distinct();
    distinct->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*distinct);
    CHECK(contains(out, "ibex::ops::distinct("));
    CHECK(contains(out, "read_csv(\"trades.csv\")"));
}

TEST_CASE("emitter: order node", "[codegen]") {
    ir::Builder b;
    auto order = b.order({ir::OrderKey{.name = "symbol", .ascending = false}});
    order->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*order);
    CHECK(contains(out, "ibex::ops::order("));
    CHECK(contains(out, "OrderKey{\"symbol\", false}"));
}

// --- Aggregate ---------------------------------------------------------------

TEST_CASE("emitter: aggregate node", "[codegen]") {
    ir::Builder b;
    auto agg = b.aggregate(
        {ir::ColumnRef{.name = "symbol"}},
        {ir::AggSpec{
             .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "price"}, .alias = "total"},
         ir::AggSpec{
             .func = ir::AggFunc::Count, .column = ir::ColumnRef{.name = "price"}, .alias = "n"}});
    agg->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*agg);
    CHECK(contains(out, "ibex::ops::aggregate("));
    CHECK(contains(out, "\"symbol\""));
    CHECK(contains(out, "ibex::ir::AggFunc::Sum"));
    CHECK(contains(out, "ibex::ir::AggFunc::Count"));
    CHECK(contains(out, "\"total\""));
    CHECK(contains(out, "\"n\""));
    CHECK(contains(out, "ibex::ops::make_agg("));
}

TEST_CASE("emitter: aggregate node with parameterized function", "[codegen]") {
    ir::Builder b;
    auto agg = b.aggregate({}, {ir::AggSpec{.func = ir::AggFunc::Ewma,
                                            .column = ir::ColumnRef{.name = "price"},
                                            .alias = "ewm",
                                            .param = 0.5}});
    agg->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*agg);
    CHECK(contains(out, "ibex::ops::make_agg(ibex::ir::AggFunc::Ewma"));
    CHECK(contains(out, ", 0.5)"));
}

// --- Resample ----------------------------------------------------------------

TEST_CASE("emitter: resample node - OHLC with group-by", "[codegen]") {
    ir::Builder b;
    // 1-minute bucket (60 * 10^9 ns), group by symbol, 4 aggs
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    auto rs = b.resample(
        ir::Duration(min_ns), {ir::ColumnRef{.name = "symbol"}},
        {ir::AggSpec{
             .func = ir::AggFunc::First, .column = ir::ColumnRef{.name = "price"}, .alias = "open"},
         ir::AggSpec{
             .func = ir::AggFunc::Max, .column = ir::ColumnRef{.name = "price"}, .alias = "high"},
         ir::AggSpec{
             .func = ir::AggFunc::Min, .column = ir::ColumnRef{.name = "price"}, .alias = "low"},
         ir::AggSpec{.func = ir::AggFunc::Last,
                     .column = ir::ColumnRef{.name = "price"},
                     .alias = "close"}});
    rs->add_child(make_source(b, "ticks.csv"));

    auto out = emit_to_string(*rs);
    CHECK(contains(out, "ibex::ops::resample("));
    CHECK(contains(out, "ibex::ir::Duration(60000000000LL)"));
    CHECK(contains(out, "\"symbol\""));
    CHECK(contains(out, "ibex::ir::AggFunc::First"));
    CHECK(contains(out, "ibex::ir::AggFunc::Max"));
    CHECK(contains(out, "ibex::ir::AggFunc::Min"));
    CHECK(contains(out, "ibex::ir::AggFunc::Last"));
    CHECK(contains(out, "\"open\""));
    CHECK(contains(out, "\"close\""));
    CHECK(contains(out, "ibex::ops::make_agg("));
}

TEST_CASE("emitter: resample node - no group-by", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t hour_ns = 3600LL * 1'000'000'000LL;
    auto rs = b.resample(
        ir::Duration(hour_ns), {},
        {ir::AggSpec{
            .func = ir::AggFunc::Mean, .column = ir::ColumnRef{.name = "price"}, .alias = "avg"}});
    rs->add_child(make_source(b, "ticks.csv"));

    auto out = emit_to_string(*rs);
    CHECK(contains(out, "ibex::ops::resample("));
    CHECK(contains(out, "ibex::ir::Duration(3600000000000LL)"));
    CHECK(contains(out, "ibex::ir::AggFunc::Mean"));
    CHECK(contains(out, "\"avg\""));
}

// --- AsTimeframe -------------------------------------------------------------

TEST_CASE("emitter: as_timeframe node", "[codegen]") {
    ir::Builder b;
    auto atf = b.as_timeframe("ts");
    atf->add_child(make_source(b, "ticks.csv"));

    auto out = emit_to_string(*atf);
    CHECK(contains(out, "ibex::ops::as_timeframe("));
    CHECK(contains(out, "\"ts\""));
}

// --- Window ------------------------------------------------------------------

TEST_CASE("emitter: window node - rolling sum", "[codegen]") {
    ir::Builder b;
    // tf[window 1m, update { s = rolling_sum(price) }]
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    auto price_arg = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "price"}});
    auto upd = b.update(
        {ir::FieldSpec{.alias = "s",
                       .expr = ir::Expr{ir::CallExpr{
                           .callee = "rolling_sum", .args = {price_arg}, .named_args = {}}}}});
    upd->add_child(make_source(b, "ticks.csv"));
    auto win = b.window(ir::Duration(min_ns));
    win->add_child(std::move(upd));

    auto out = emit_to_string(*win);
    CHECK(contains(out, "ibex::ops::windowed_update("));
    CHECK(contains(out, "ibex::ir::Duration(60000000000LL)"));
    CHECK(contains(out, "\"s\""));
    CHECK(contains(out, "rolling_sum"));
    CHECK(contains(out, "ibex::ops::make_field("));
}

TEST_CASE("emitter: window node - multiple rolling ops", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t min5_ns = 300LL * 1'000'000'000LL;
    auto price_arg = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "price"}});
    auto upd = b.update({
        ir::FieldSpec{.alias = "s",
                      .expr = ir::Expr{ir::CallExpr{
                          .callee = "rolling_sum", .args = {price_arg}, .named_args = {}}}},
        ir::FieldSpec{.alias = "m",
                      .expr = ir::Expr{ir::CallExpr{
                          .callee = "rolling_mean", .args = {price_arg}, .named_args = {}}}},
    });
    upd->add_child(make_source(b, "ticks.csv"));
    auto win = b.window(ir::Duration(min5_ns));
    win->add_child(std::move(upd));

    auto out = emit_to_string(*win);
    CHECK(contains(out, "ibex::ir::Duration(300000000000LL)"));
    CHECK(contains(out, "\"s\""));
    CHECK(contains(out, "\"m\""));
    CHECK(contains(out, "rolling_mean"));
}

TEST_CASE("emitter: window node - by-clause is emitted, not rejected", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    auto price_arg = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "price"}});
    auto upd = b.update(
        {ir::FieldSpec{.alias = "m",
                       .expr = ir::Expr{ir::CallExpr{
                           .callee = "rolling_mean", .args = {price_arg}, .named_args = {}}}}},
        {}, {ir::ColumnRef{.name = "symbol"}});
    upd->add_child(make_source(b, "ticks.csv"));
    auto win = b.window(ir::Duration(min_ns));
    win->add_child(std::move(upd));

    auto out = emit_to_string(*win);
    CHECK(contains(out, "ibex::ops::windowed_update("));
    CHECK(contains(out, "\"symbol\""));
}

TEST_CASE("emitter: window node - tuple fields are still rejected", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    std::vector<ir::TupleFieldSpec> tuple_fields;
    tuple_fields.push_back(ir::TupleFieldSpec{
        .aliases = {"x", "y"},
        .source = make_source(b, "extra.csv"),
    });
    auto upd = b.update({}, std::move(tuple_fields));
    upd->add_child(make_source(b, "ticks.csv"));
    auto win = b.window(ir::Duration(min_ns));
    win->add_child(std::move(upd));

    REQUIRE_THROWS(emit_to_string(*win));
}

// --- Update ------------------------------------------------------------------

TEST_CASE("emitter: update node - simple expression", "[codegen]") {
    ir::Builder b;

    // mid = (bid + ask) / 2.0
    ir::FieldSpec field{
        .alias = "mid",
        .expr = ir::Expr{ir::BinaryExpr{
            .op = ir::ArithmeticOp::Div,
            .left = ir::make_expr_ptr(ir::Expr{ir::BinaryExpr{
                .op = ir::ArithmeticOp::Add,
                .left = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "bid"}}),
                .right = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "ask"}})}}),
            .right = ir::make_expr_ptr(ir::Expr{ir::Literal{2.0}})}}};

    auto upd = b.update({std::move(field)});
    upd->add_child(make_source(b, "trades.csv"));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::update("));
    CHECK(contains(out, "ibex::ops::make_field(\"mid\""));
    CHECK(contains(out, "ibex::ir::ArithmeticOp::Div"));
    CHECK(contains(out, "ibex::ir::ArithmeticOp::Add"));
    CHECK(contains(out, "ibex::ops::col_ref(\"bid\")"));
    CHECK(contains(out, "ibex::ops::col_ref(\"ask\")"));
    CHECK(contains(out, "ibex::ops::dbl_lit(2.0)"));
}

TEST_CASE("emitter: update node - literal types", "[codegen]") {
    ir::Builder b;

    ir::FieldSpec f1{.alias = "label", .expr = ir::Expr{ir::Literal{std::string{"hello"}}}};
    ir::FieldSpec f2{.alias = "count", .expr = ir::Expr{ir::Literal{std::int64_t{42}}}};
    ir::FieldSpec f3{.alias = "day", .expr = ir::Expr{ir::Literal{Date{1}}}};
    ir::FieldSpec f4{.alias = "ts", .expr = ir::Expr{ir::Literal{Timestamp{1000}}}};

    auto upd = b.update({std::move(f1), std::move(f2), std::move(f3), std::move(f4)});
    upd->add_child(make_source(b, "t.csv"));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::str_lit(\"hello\")"));
    CHECK(contains(out, "ibex::ops::int_lit(std::int64_t{42})"));
    CHECK(contains(out, "ibex::ops::date_lit(ibex::Date{std::int32_t{1}})"));
    CHECK(contains(out, "ibex::ops::timestamp_lit(ibex::Timestamp{std::int64_t{1000}})"));
}

TEST_CASE("emitter: malformed update node does not crash", "[codegen]") {
    ir::Builder b;
    auto upd = b.update({ir::FieldSpec{
        .alias = "x",
        .expr = ir::Expr{ir::ColumnRef{.name = "price"}},
    }});

    REQUIRE_THROWS(emit_to_string(*upd));
}

TEST_CASE("emitter: update node - tuple sources and by keys", "[codegen]") {
    ir::Builder b;
    std::vector<ir::TupleFieldSpec> tuple_fields;
    tuple_fields.push_back(ir::TupleFieldSpec{
        .aliases = {"x", "y"},
        .source = make_source(b, "extra.csv"),
    });
    auto upd = b.update({}, std::move(tuple_fields), {ir::ColumnRef{.name = "symbol"}});
    upd->add_child(make_source(b, "base.csv"));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::TupleSource{{\"x\", \"y\"},"));
    CHECK(contains(out, "{\"symbol\"}"));
}

TEST_CASE("emitter: call expression preserves named args", "[codegen]") {
    ir::Builder b;
    ir::CallExpr rep_call{
        .callee = "rep",
        .args = {ir::make_expr_ptr(ir::Expr{ir::Literal{std::int64_t{7}}})},
        .named_args = {ir::NamedArg{
            .name = "times",
            .value = ir::make_expr_ptr(ir::Expr{ir::Literal{std::int64_t{3}}}),
        }},
    };
    auto upd =
        b.update({ir::FieldSpec{.alias = "v", .expr = ir::Expr{.node = std::move(rep_call)}}});
    upd->add_child(make_source(b, "base.csv"));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::fn_call(\"rep\""));
    CHECK(contains(out, "ibex::ops::NamedArgExpr{\"times\""));
}

TEST_CASE("emitter: map node - row-wise fields", "[codegen]") {
    ir::Builder b;
    ir::FieldSpec doubled{.alias = "doubled",
                          .expr = ir::Expr{ir::BinaryExpr{
                              .op = ir::ArithmeticOp::Mul,
                              .left = ir::make_expr_ptr(ir::Expr{ir::ColumnRef{.name = "n"}}),
                              .right = ir::make_expr_ptr(ir::Expr{ir::Literal{std::int64_t{2}}})}}};
    ir::FieldSpec tag{.alias = "tag", .expr = ir::Expr{ir::ColumnRef{.name = "label"}}};

    auto map_node = b.map({std::move(doubled), std::move(tag)});
    map_node->add_child(make_source(b, "rows.csv"));

    auto out = emit_to_string(*map_node);
    CHECK(contains(out, "ibex::ops::map("));
    CHECK(contains(out, "ibex::ops::make_field(\"doubled\""));
    CHECK(contains(out, "ibex::ops::make_field(\"tag\""));
    CHECK(contains(out, "ibex::ir::ArithmeticOp::Mul"));
    CHECK(contains(out, "ibex::ops::col_ref(\"label\")"));
}

TEST_CASE("emitter: non-literal row-count argument resolves at run time", "[codegen]") {
    ir::Builder b;
    // Table(n) where `n` is a `scalar(...)` deferred `let`: emitted as a
    // run-time registry lookup rather than a hard error.
    auto construct = b.construct_rows(ir::Expr{ir::ColumnRef{.name = "n"}});

    codegen::Emitter::Config cfg;
    cfg.deferred_scalar_bindings.push_back(ir::DeferredScalarBinding{
        .name = "n", .sources = {}, .value = ir::Expr{ir::Literal{std::int64_t{3}}}});
    auto out = emit_to_string(*construct, cfg);
    CHECK(contains(out, "ibex::ops::scalar_arg("));
    CHECK(contains(out, "ibex::ops::col_ref(\"n\")"));
    CHECK_FALSE(contains(out, "non-literal argument"));
}

TEST_CASE("emitter: a bare unbound name in an extern argument is still an error", "[codegen]") {
    ir::Builder b;
    auto construct = b.construct_rows(ir::Expr{ir::ColumnRef{.name = "mystery"}});
    CHECK_THROWS(emit_to_string(*construct));
}

TEST_CASE("emitter: forward_cli_args changes the main signature", "[codegen]") {
    ir::Builder b;
    auto project = b.project({ir::ColumnRef{.name = "x"}});
    project->add_child(make_source(b, "t.csv"));

    codegen::Emitter::Config cfg;
    cfg.forward_cli_args = true;
    auto out = emit_to_string(*project, cfg);
    CHECK(contains(out, "int main(int argc, char** argv)"));
    CHECK(contains(out, "ibex::ops::forward_cli_args(argc, argv);"));

    auto plain = emit_to_string(*project);
    CHECK(contains(plain, "int main() {"));
    CHECK_FALSE(contains(plain, "forward_cli_args"));
}

// --- Chained pipeline --------------------------------------------------------

TEST_CASE("emitter: filter then project pipeline", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(filter_cmp(ir::CompareOp::Ge, filter_col("price"), filter_int(50)));
    filter->add_child(make_source(b, "trades.csv"));
    auto proj = b.project({ir::ColumnRef{.name = "symbol"}});
    proj->add_child(std::move(filter));

    auto out = emit_to_string(*proj);
    auto pos_source = out.find("read_csv(");
    auto pos_filter = out.find("ibex::ops::filter(");
    auto pos_proj = out.find("ibex::ops::project(");
    REQUIRE(pos_source != std::string::npos);
    REQUIRE(pos_filter != std::string::npos);
    REQUIRE(pos_proj != std::string::npos);
    CHECK(pos_source < pos_filter);
    CHECK(pos_filter < pos_proj);
}

// --- Join --------------------------------------------------------------------

TEST_CASE("emitter: join node - inner join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Inner, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::inner_join("));
    CHECK(contains(out, "\"id\""));
}

TEST_CASE("emitter: join node - mapped keys", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Inner, {{"left_id", "right_id"}});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "{\"left_id\", \"right_id\"}"));
}

TEST_CASE("emitter: folded join key preserves its logical output label", "[codegen]") {
    ir::Builder b;
    auto join = b.join(ir::JoinKind::Inner, {{"left_native", "right_native", true, "logical_id"}});
    join->add_child(make_source(b, "left.csv"));
    join->add_child(make_source(b, "right.csv"));

    CHECK(contains(emit_to_string(*join),
                   "{\"left_native\", \"right_native\", true, \"logical_id\"}"));
}

TEST_CASE("emitter: join node - right join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Right, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::right_join("));
}

TEST_CASE("emitter: join node - outer join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Outer, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::outer_join("));
}
TEST_CASE("emitter: join node - semi join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Semi, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::semi_join("));
}

TEST_CASE("emitter: join node - anti join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Anti, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::anti_join("));
}

TEST_CASE("emitter: join node - cross join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Cross, {});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::cross_join("));
}

TEST_CASE("emitter: join node - asof join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Asof, {"ts", "symbol"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::asof_join("));
    CHECK(contains(out, "\"ts\""));
    CHECK(contains(out, "\"symbol\""));
}

TEST_CASE("emitter: join node - non-equijoin predicate", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Inner, {},
                       filter_cmp(ir::CompareOp::Gt, filter_col("price"), filter_col("limit")));
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::join_with_predicate("));
    CHECK(contains(out, "ibex::ir::JoinKind::Inner"));
}

TEST_CASE("emitter: join node - side-qualified predicate references", "[codegen]") {
    // A side tag has to survive into the generated code: without it the
    // transpiled join would re-resolve `v` against both inputs and reject it
    // as ambiguous, where the interpreter honours the qualifier.
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Inner, {},
                       filter_cmp(ir::CompareOp::Lt, filter_col_side("v", ir::JoinSide::Left),
                                  filter_col_side("v", ir::JoinSide::Right)));
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::filter_col_side(\"v\", ibex::ir::JoinSide::Left)"));
    CHECK(contains(out, "ibex::ops::filter_col_side(\"v\", ibex::ir::JoinSide::Right)"));
}

TEST_CASE("emitter: join node - suffix policy", "[codegen]") {
    // A dropped policy does not produce wrong names, it produces a *rejected
    // program*: the transpiled join would report the collision the clause
    // exists to resolve, on input the interpreter accepts.
    ir::Builder b;

    SECTION("a clause is carried into the generated call") {
        auto join = b.join(ir::JoinKind::Inner, {{"id", "id"}}, std::nullopt,
                           ir::JoinSuffixPolicy{.present = true, .left = "_l", .right = "_r"});
        join->add_child(make_source(b, "left.csv"));
        join->add_child(make_source(b, "right.csv"));
        const auto out = emit_to_string(*join);
        CHECK(contains(out,
                       "ibex::ir::JoinSuffixPolicy{.present = true, .left = \"_l\", "
                       ".right = \"_r\"}"));
    }

    SECTION("an absent clause emits no extra argument") {
        auto join = b.join(ir::JoinKind::Inner, {{"id", "id"}});
        join->add_child(make_source(b, "left.csv"));
        join->add_child(make_source(b, "right.csv"));
        CHECK_FALSE(contains(emit_to_string(*join), "JoinSuffixPolicy"));
    }

    SECTION("an empty suffix survives as an empty string, not as absence") {
        auto join = b.join(ir::JoinKind::Inner, {{"id", "id"}}, std::nullopt,
                           ir::JoinSuffixPolicy{.present = true, .left = "", .right = "_r"});
        join->add_child(make_source(b, "left.csv"));
        join->add_child(make_source(b, "right.csv"));
        const auto out = emit_to_string(*join);
        CHECK(contains(out, ".present = true, .left = \"\", .right = \"_r\""));
    }

    SECTION("a theta join carries both the predicate and the clause") {
        auto join = b.join(ir::JoinKind::Inner, {},
                           filter_cmp(ir::CompareOp::Lt, filter_col_side("v", ir::JoinSide::Left),
                                      filter_col_side("v", ir::JoinSide::Right)),
                           ir::JoinSuffixPolicy{.present = true, .left = "", .right = "_r"});
        join->add_child(make_source(b, "left.csv"));
        join->add_child(make_source(b, "right.csv"));
        const auto out = emit_to_string(*join);
        CHECK(contains(out, "ibex::ops::join_with_predicate("));
        CHECK(contains(out, "ibex::ir::JoinSuffixPolicy{"));
    }
}

// --- Config ------------------------------------------------------------------

TEST_CASE("emitter: extern headers in config", "[codegen]") {
    ir::Builder b;
    auto root = make_source(b, "t.csv");

    codegen::Emitter::Config cfg;
    cfg.extern_headers = {"stats.hpp", "math_utils.hpp"};
    cfg.source_name = "query.ibex";

    auto out = emit_to_string(*root, cfg);
    CHECK(contains(out, "// Source: query.ibex"));
    CHECK(contains(out, "#include \"stats.hpp\""));
    CHECK(contains(out, "#include \"math_utils.hpp\""));
}

// --- String escaping ---------------------------------------------------------

TEST_CASE("emitter: escape quotes in extern call arg", "[codegen]") {
    ir::Builder b;
    auto root = b.extern_call("read_csv",
                              {ir::Expr{ir::Literal{std::string{R"(path/with "quotes".csv)"}}}});
    auto out = emit_to_string(*root);
    CHECK(contains(out, R"(path/with \"quotes\".csv)"));
}

// --- Schema ascription --------------------------------------------------------

TEST_CASE("emitter: ascription emits a validating ops::ascribe call", "[codegen]") {
    ir::Builder b;
    auto asc = b.ascribe({ir::SchemaField{.name = "a", .type = ir::ColumnType::Int64},
                          ir::SchemaField{.name = "b", .type = ir::ColumnType::Float64}},
                         /*open=*/false);
    asc->add_child(make_source(b, "iris.csv"));
    auto out = emit_to_string(*asc);
    CHECK(contains(out, "ibex::ops::ascribe("));
    CHECK(contains(out, R"(ibex::ir::SchemaField{"a", ibex::ir::ColumnType::Int64})"));
    CHECK(contains(out, "ibex::ir::ColumnType::Float64"));
    CHECK(contains(out, ", false)"));  // exact (forbids extras)
}

TEST_CASE("emitter: wildcard ascription emits open=true", "[codegen]") {
    ir::Builder b;
    auto asc = b.ascribe({ir::SchemaField{.name = "a", .type = ir::ColumnType::Int64}},
                         /*open=*/true);
    asc->add_child(make_source(b, "iris.csv"));
    auto out = emit_to_string(*asc);
    CHECK(contains(out, "ibex::ops::ascribe("));
    CHECK(contains(out, ", true)"));  // wildcard (allows extras)
}

// --- rbind --------------------------------------------------------------------

TEST_CASE("emitter: rbind emits a brace-init ops::rbind over its children", "[codegen]") {
    ir::Builder b;
    auto node = b.rbind();
    node->add_child(make_source(b, "jan.csv"));
    node->add_child(make_source(b, "feb.csv"));
    node->add_child(make_source(b, "mar.csv"));

    auto out = emit_to_string(*node);
    CHECK(contains(out, "ibex::ops::rbind({"));
    CHECK(contains(out, "read_csv(\"jan.csv\")"));
    CHECK(contains(out, "read_csv(\"feb.csv\")"));
    CHECK(contains(out, "read_csv(\"mar.csv\")"));
}

// --- like ---------------------------------------------------------------------

TEST_CASE("emitter: like in a filter predicate", "[codegen][like]") {
    ir::Builder b;
    auto filter =
        b.filter(ops::filter_call("like", {ops::filter_col("p_name"), ops::filter_str("%green%")}));
    filter->add_child(make_source(b, "part.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_call(\"like\""));
    CHECK(contains(out, "\"%green%\""));
}

TEST_CASE("emitter: negated like in a filter predicate", "[codegen][like]") {
    ir::Builder b;
    auto filter = b.filter(ops::filter_not(ops::filter_call(
        "like", {ops::filter_col("o_comment"), ops::filter_str("%special%requests%")})));
    filter->add_child(make_source(b, "orders.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_not("));
    CHECK(contains(out, "ibex::ops::filter_call(\"like\""));
}

TEST_CASE("emitter: like as an update field emits a value-position call", "[codegen][like]") {
    ir::Builder b;
    auto update = b.update(
        {{.alias = "is_green",
          .expr = ops::fn_call("like", {ops::col_ref("p_name"), ops::str_lit("%green%")})}});
    update->add_child(make_source(b, "part.csv"));

    auto out = emit_to_string(*update);
    CHECK(contains(out, "ibex::ops::fn_call(\"like\""));
    CHECK(contains(out, "\"%green%\""));
}

TEST_CASE("emitter: string length calls use the generic value-call path", "[codegen][string]") {
    ir::Builder b;
    auto update = b.update({
        {.alias = "chars", .expr = ops::fn_call("length", {ops::col_ref("name")})},
        {.alias = "bytes", .expr = ops::fn_call("byte_length", {ops::col_ref("name")})},
    });
    update->add_child(make_source(b, "part.csv"));

    const auto out = emit_to_string(*update);
    CHECK(contains(out, "ibex::ops::fn_call(\"length\""));
    CHECK(contains(out, "ibex::ops::fn_call(\"byte_length\""));
}

TEST_CASE("emitter: a boolean-valued field emits its predicate node", "[codegen]") {
    // Boolean nodes are legal in value position (`flag = !like(...)`), and the
    // interpreter builds a Bool column from them — codegen must emit the same
    // tree rather than rejecting the field.
    ir::Builder b;
    auto update = b.update({{.alias = "plain",
                             .expr = ops::filter_not(ops::fn_call(
                                 "like", {ops::col_ref("p_name"), ops::str_lit("%green%")}))}});
    update->add_child(make_source(b, "part.csv"));

    auto out = emit_to_string(*update);
    CHECK(contains(out, "ibex::ops::filter_not("));
    CHECK(contains(out, "ibex::ops::fn_call(\"like\""));
}

// --- Scripts with effects -----------------------------------------------------

namespace {

auto emit_script_to_string(const codegen::Emitter::Script& script) -> std::string {
    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, script, codegen::Emitter::Config{});
    return oss.str();
}

}  // namespace

TEST_CASE("emitter: a script runs its steps in order and a sink sees its input", "[codegen]") {
    ir::Builder b;
    auto first = make_source(b, "in.csv");
    auto result = make_source(b, "out.csv");

    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step sink;
    sink.kind = codegen::Emitter::Script::Step::Kind::Sink;
    sink.callee = "write_csv";
    sink.plan = first.get();
    sink.args.emplace_back(ir::Literal{std::string("copy.csv")});
    script.steps.push_back(std::move(sink));
    script.result = result.get();

    const auto out = emit_script_to_string(script);
    const auto read_in = out.find("read_csv(\"in.csv\")");
    const auto write = out.find("write_csv(t0, \"copy.csv\")");
    const auto read_out = out.find("read_csv(\"out.csv\")");
    REQUIRE(read_in != std::string::npos);
    REQUIRE(write != std::string::npos);
    REQUIRE(read_out != std::string::npos);
    // The sink's input is read, written, and only then is the result's source read.
    CHECK(read_in < write);
    CHECK(write < read_out);
}

TEST_CASE("emitter: a script's result reuses the table its sink consumed", "[codegen]") {
    // `write(result, ...); result;` -- the source must be read once.
    ir::Builder b;
    auto plan = make_source(b, "in.csv");
    auto again = make_source(b, "in.csv");

    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step sink;
    sink.kind = codegen::Emitter::Script::Step::Kind::Sink;
    sink.callee = "write_csv";
    sink.plan = plan.get();
    sink.args.emplace_back(ir::Literal{std::string("copy.csv")});
    sink.input_binding = "result";
    script.steps.push_back(std::move(sink));
    script.result = again.get();
    script.result_binding = "result";

    const auto out = emit_script_to_string(script);
    const auto first = out.find("read_csv(\"in.csv\")");
    REQUIRE(first != std::string::npos);
    CHECK(out.find("read_csv(\"in.csv\")", first + 1) == std::string::npos);
    CHECK(contains(out, "ibex::ops::print(t0)"));
}

TEST_CASE("emitter: a scan of a shared binding resolves to the step that built it", "[codegen]") {
    ir::Builder b;
    auto shared = make_source(b, "in.csv");
    auto result = b.scan("shared");

    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step step;
    step.kind = codegen::Emitter::Script::Step::Kind::SharedBinding;
    step.name = "shared";
    step.plan = shared.get();
    script.steps.push_back(std::move(step));
    script.result = result.get();

    const auto out = emit_script_to_string(script);
    CHECK(contains(out, "auto t0 = read_csv(\"in.csv\")"));
    CHECK(contains(out, "ibex::ops::print(t0)"));
}

TEST_CASE("emitter: a bound sink and a bound call store their results as scalars", "[codegen]") {
    ir::Builder b;
    auto input = make_source(b, "in.csv");
    auto call = b.extern_call("ping", {ir::Expr{ir::Literal{std::int64_t{7}}}});
    auto result = make_source(b, "out.csv");

    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step sink;
    sink.kind = codegen::Emitter::Script::Step::Kind::Sink;
    sink.callee = "write_csv";
    sink.plan = input.get();
    sink.args.emplace_back(ir::Literal{std::string("copy.csv")});
    sink.bind = "rows";
    script.steps.push_back(std::move(sink));
    codegen::Emitter::Script::Step ping;
    ping.kind = codegen::Emitter::Script::Step::Kind::Call;
    ping.plan = call.get();
    ping.bind = "pong";
    script.steps.push_back(std::move(ping));
    script.result = result.get();

    codegen::Emitter::Config config;
    config.runtime_scalar_names = {"rows", "pong"};
    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, script, config);
    const auto out = oss.str();

    // The registry exists, and each call's result lands in it, in step order.
    const auto registry = out.find("ibex::runtime::ScalarRegistry _ibex_scalars;");
    const auto rows = out.find(
        "_ibex_scalars[\"rows\"] = ibex::runtime::ScalarValue(write_csv(t0, "
        "\"copy.csv\"))");
    const auto pong = out.find("_ibex_scalars[\"pong\"] = ibex::runtime::ScalarValue(ping(");
    REQUIRE(registry != std::string::npos);
    REQUIRE(rows != std::string::npos);
    REQUIRE(pong != std::string::npos);
    CHECK(registry < rows);
    CHECK(rows < pong);
}

TEST_CASE("emitter: a deferred scalar step runs where it is, not before the other steps",
          "[codegen]") {
    ir::Builder b;
    auto first = make_source(b, "first.csv");
    auto second = make_source(b, "second.csv");
    auto deferred_plan = make_source(b, "scalar_source.csv");
    auto result = make_source(b, "out.csv");

    ir::DeferredScalarBinding binding;
    binding.name = "k";
    binding.sources.push_back(ir::DeferredScalarSource{
        .tmp_name = "__ibex_scalar_src_0", .plan = std::move(deferred_plan), .column = "v"});
    binding.value = ir::Expr{.node = ir::ColumnRef{.name = "__ibex_scalar_src_0", .lexical = true}};

    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step sink;
    sink.kind = codegen::Emitter::Script::Step::Kind::Sink;
    sink.callee = "write_csv";
    sink.plan = first.get();
    sink.args.emplace_back(ir::Literal{std::string("rewritten.csv")});
    script.steps.push_back(std::move(sink));
    codegen::Emitter::Script::Step step;
    step.kind = codegen::Emitter::Script::Step::Kind::DeferredScalar;
    step.deferred = &binding;
    script.steps.push_back(std::move(step));
    script.result = second.get();

    codegen::Emitter::Config config;
    config.runtime_scalar_names = {"k"};
    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, script, config);
    const auto out = oss.str();

    const auto write = out.find("write_csv(t0, \"rewritten.csv\")");
    const auto scalar_read = out.find("read_csv(\"scalar_source.csv\")");
    REQUIRE(write != std::string::npos);
    REQUIRE(scalar_read != std::string::npos);
    CHECK(write < scalar_read);
    CHECK(contains(out, "_ibex_scalars[\"k\"] = ibex::ops::eval_scalar("));
}

TEST_CASE("emitter: a resource is a variable, aliased, passed to calls and released", "[codegen]") {
    ir::Builder b;
    auto open = b.extern_call("open", {ir::Expr{ir::Literal{std::string("file:x")}}});
    auto use_a = b.extern_call("run", {ir::Expr{ir::ColumnRef{.name = "a"}},
                                       ir::Expr{ir::Literal{std::string("select 1")}}});
    auto use_b = b.extern_call("run", {ir::Expr{ir::ColumnRef{.name = "b"}},
                                       ir::Expr{ir::Literal{std::string("select 2")}}});
    auto result = make_source(b, "out.csv");

    codegen::Emitter::Script script;
    const auto call_step = [](const ir::Node* plan, std::optional<std::string> bind,
                              bool resource) {
        codegen::Emitter::Script::Step step;
        step.kind = codegen::Emitter::Script::Step::Kind::Call;
        step.plan = plan;
        step.bind = std::move(bind);
        step.bind_resource = resource;
        return step;
    };
    script.steps.push_back(call_step(open.get(), "a", true));
    codegen::Emitter::Script::Step alias;
    alias.kind = codegen::Emitter::Script::Step::Kind::ResourceAlias;
    alias.name = "b";
    alias.alias_of = "a";
    script.steps.push_back(std::move(alias));
    script.steps.push_back(call_step(use_b.get(), std::nullopt, false));
    codegen::Emitter::Script::Step release;
    release.kind = codegen::Emitter::Script::Step::Kind::ResourceUnbind;
    release.name = "b";
    script.steps.push_back(std::move(release));
    script.steps.push_back(call_step(use_a.get(), std::nullopt, false));
    script.result = result.get();

    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, script, codegen::Emitter::Config{});
    const auto out = oss.str();

    const auto opened = out.find("auto _res0_a = open(\"file:x\");");
    const auto aliased = out.find("auto _res1_b = _res0_a;");
    const auto used_b = out.find("(void)run(_res1_b, \"select 2\");");
    const auto released = out.find("_res1_b = {};");
    const auto used_a = out.find("(void)run(_res0_a, \"select 1\");");
    REQUIRE(opened != std::string::npos);
    REQUIRE(aliased != std::string::npos);
    REQUIRE(used_b != std::string::npos);
    REQUIRE(released != std::string::npos);
    REQUIRE(used_a != std::string::npos);
    CHECK(opened < aliased);
    CHECK(aliased < used_b);
    CHECK(used_b < released);
    // Releasing `b` leaves `a`'s variable alone: a name is released, not the connection.
    CHECK(released < used_a);
    CHECK_FALSE(contains(out, "_res0_a = {};"));
}

TEST_CASE("emitter: rebinding a resource name releases the old variable after the new value",
          "[codegen]") {
    ir::Builder b;
    auto first = b.extern_call("open", {ir::Expr{ir::Literal{std::string("file:1")}}});
    auto second = b.extern_call("open", {ir::Expr{ir::Literal{std::string("file:2")}}});
    auto result = make_source(b, "out.csv");

    codegen::Emitter::Script script;
    for (const ir::Node* plan : {first.get(), second.get()}) {
        codegen::Emitter::Script::Step step;
        step.kind = codegen::Emitter::Script::Step::Kind::Call;
        step.plan = plan;
        step.bind = "db";
        step.bind_resource = true;
        script.steps.push_back(std::move(step));
    }
    script.result = result.get();

    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, script, codegen::Emitter::Config{});
    const auto out = oss.str();
    const auto second_open = out.find("auto _res1_db = open(\"file:2\");");
    const auto release_first = out.find("_res0_db = {};");
    REQUIRE(second_open != std::string::npos);
    REQUIRE(release_first != std::string::npos);
    CHECK(second_open < release_first);
}

TEST_CASE("emitter: a program function is declared, takes its parameters, and is called by prefix",
          "[codegen]") {
    ir::Builder b;
    // fn pick(c: Conn, uri: String) -> Conn { open(uri); }
    auto inner_call = b.extern_call("open", {ir::Expr{ir::ColumnRef{.name = "uri"}}});
    auto fn = std::make_unique<codegen::Emitter::Script::Function>();
    fn->name = "pick";
    fn->params = {{.name = "c",
                   .cpp_type = "Conn",
                   .kind = codegen::Emitter::Script::Function::Param::Kind::Resource},
                  {.name = "uri",
                   .cpp_type = "std::string",
                   .kind = codegen::Emitter::Script::Function::Param::Kind::Scalar}};
    fn->return_type = "Conn";
    fn->body.return_call = inner_call.get();

    // The program: let db = pick(a, "file:x");
    auto opened = b.extern_call("open", {ir::Expr{ir::Literal{std::string("file:a")}}});
    auto use = b.extern_call("pick", {ir::Expr{ir::ColumnRef{.name = "a"}},
                                      ir::Expr{ir::Literal{std::string("file:x")}}});
    auto result = make_source(b, "out.csv");
    codegen::Emitter::Script script;
    codegen::Emitter::Script::Step open_step;
    open_step.kind = codegen::Emitter::Script::Step::Kind::Call;
    open_step.plan = opened.get();
    open_step.bind = "a";
    open_step.bind_resource = true;
    script.steps.push_back(std::move(open_step));
    codegen::Emitter::Script::Step pick_step;
    pick_step.kind = codegen::Emitter::Script::Step::Kind::Call;
    pick_step.plan = use.get();
    pick_step.bind = "db";
    pick_step.bind_resource = true;
    script.steps.push_back(std::move(pick_step));
    script.result = result.get();
    script.functions.push_back(std::move(fn));

    std::ostringstream oss;
    codegen::Emitter emitter;
    codegen::Emitter::Config config;
    config.runtime_scalar_names = {};
    emitter.emit(oss, script, config);
    const auto out = oss.str();

    // Declared, then defined, ahead of main; the parameters are prefixed; a scalar
    // parameter is published in the function's own scope; the function's value is
    // its call.
    const auto declaration =
        out.find("static auto _ibex_fn_pick(Conn _p_c, std::string _p_uri) -> Conn;");
    const auto definition =
        out.find("static auto _ibex_fn_pick(Conn _p_c, std::string _p_uri) -> Conn {");
    const auto main_at = out.find("int main()");
    REQUIRE(declaration != std::string::npos);
    REQUIRE(definition != std::string::npos);
    REQUIRE(main_at != std::string::npos);
    CHECK(declaration < definition);
    CHECK(definition < main_at);
    CHECK(contains(out, "ibex::ops::ScalarScope _ibex_scope;"));
    CHECK(contains(out, "_ibex_scope.set(\"uri\", ibex::runtime::ScalarValue(_p_uri));"));
    CHECK(contains(out, "return open(ibex::ops::scalar_arg(ibex::ops::col_ref(\"uri\")));"));
    // The call from the program is spelled with the prefix.
    CHECK(contains(out, "auto _res"));
    CHECK(contains(out, "_ibex_fn_pick(_res"));
}
