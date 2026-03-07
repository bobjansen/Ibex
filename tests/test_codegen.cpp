#include <ibex/codegen/emitter.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/ops.hpp>

#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <string>

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
    return haystack.find(needle) != std::string::npos;
}

// Helper: create a leaf ExternCallNode representing a table data source.
static auto make_source(ir::Builder& b, std::string_view path) -> ir::NodePtr {
    return b.extern_call("read_csv", {ir::Expr{ir::Literal{std::string(path)}}});
}

// ─── ExternCall ───────────────────────────────────────────────────────────────

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

// ─── Filter ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: filter node — int64 predicate", "[codegen]") {
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

TEST_CASE("emitter: filter node — double predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(
        ops::filter_cmp(ir::CompareOp::Le, ops::filter_col("ratio"), ops::filter_dbl(0.5)));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Le"));
    CHECK(contains(out, "0.5"));
}

TEST_CASE("emitter: filter node — string predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(
        ops::filter_cmp(ir::CompareOp::Eq, ops::filter_col("symbol"), ops::filter_str("AAPL")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Eq"));
    CHECK(contains(out, "\"AAPL\""));
}

TEST_CASE("emitter: filter node — AND compound predicate", "[codegen]") {
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

TEST_CASE("emitter: filter node — arithmetic in predicate", "[codegen]") {
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

TEST_CASE("emitter: filter node — is null predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(ops::filter_is_null(ops::filter_col("dept_name")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_is_null("));
    CHECK(contains(out, "ibex::ops::filter_col(\"dept_name\")"));
}

TEST_CASE("emitter: filter node — is not null predicate", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(ops::filter_is_not_null(ops::filter_col("dept_name")));
    filter->add_child(make_source(b, "data.csv"));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::filter_is_not_null("));
    CHECK(contains(out, "ibex::ops::filter_col(\"dept_name\")"));
}

// ─── Project ─────────────────────────────────────────────────────────────────

TEST_CASE("emitter: project node", "[codegen]") {
    ir::Builder b;
    auto proj = b.project({ir::ColumnRef{"symbol"}, ir::ColumnRef{"price"}});
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

// ─── Aggregate ───────────────────────────────────────────────────────────────

TEST_CASE("emitter: aggregate node", "[codegen]") {
    ir::Builder b;
    auto agg = b.aggregate({ir::ColumnRef{"symbol"}},
                           {ir::AggSpec{ir::AggFunc::Sum, ir::ColumnRef{"price"}, "total"},
                            ir::AggSpec{ir::AggFunc::Count, ir::ColumnRef{"price"}, "n"}});
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

// ─── Resample ────────────────────────────────────────────────────────────────

TEST_CASE("emitter: resample node — OHLC with group-by", "[codegen]") {
    ir::Builder b;
    // 1-minute bucket (60 * 10^9 ns), group by symbol, 4 aggs
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    auto rs = b.resample(ir::Duration(min_ns), {ir::ColumnRef{"symbol"}},
                         {ir::AggSpec{ir::AggFunc::First, ir::ColumnRef{"price"}, "open"},
                          ir::AggSpec{ir::AggFunc::Max, ir::ColumnRef{"price"}, "high"},
                          ir::AggSpec{ir::AggFunc::Min, ir::ColumnRef{"price"}, "low"},
                          ir::AggSpec{ir::AggFunc::Last, ir::ColumnRef{"price"}, "close"}});
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

TEST_CASE("emitter: resample node — no group-by", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t hour_ns = 3600LL * 1'000'000'000LL;
    auto rs = b.resample(ir::Duration(hour_ns), {},
                         {ir::AggSpec{ir::AggFunc::Mean, ir::ColumnRef{"price"}, "avg"}});
    rs->add_child(make_source(b, "ticks.csv"));

    auto out = emit_to_string(*rs);
    CHECK(contains(out, "ibex::ops::resample("));
    CHECK(contains(out, "ibex::ir::Duration(3600000000000LL)"));
    CHECK(contains(out, "ibex::ir::AggFunc::Mean"));
    CHECK(contains(out, "\"avg\""));
}

// ─── AsTimeframe ─────────────────────────────────────────────────────────────

TEST_CASE("emitter: as_timeframe node", "[codegen]") {
    ir::Builder b;
    auto atf = b.as_timeframe("ts");
    atf->add_child(make_source(b, "ticks.csv"));

    auto out = emit_to_string(*atf);
    CHECK(contains(out, "ibex::ops::as_timeframe("));
    CHECK(contains(out, "\"ts\""));
}

// ─── Window ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: window node — rolling sum", "[codegen]") {
    ir::Builder b;
    // tf[window 1m, update { s = rolling_sum(price) }]
    constexpr std::int64_t min_ns = 60LL * 1'000'000'000LL;
    auto price_arg = std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{.name = "price"}});
    auto upd = b.update({ir::FieldSpec{
        .alias = "s",
        .expr = ir::Expr{ir::CallExpr{.callee = "rolling_sum", .args = {price_arg}}}}});
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

TEST_CASE("emitter: window node — multiple rolling ops", "[codegen]") {
    ir::Builder b;
    constexpr std::int64_t min5_ns = 300LL * 1'000'000'000LL;
    auto price_arg = std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{.name = "price"}});
    auto upd = b.update({
        ir::FieldSpec{.alias = "s",
                      .expr = ir::Expr{ir::CallExpr{.callee = "rolling_sum", .args = {price_arg}}}},
        ir::FieldSpec{
            .alias = "m",
            .expr = ir::Expr{ir::CallExpr{.callee = "rolling_mean", .args = {price_arg}}}},
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

// ─── Update ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: update node — simple expression", "[codegen]") {
    ir::Builder b;

    // mid = (bid + ask) / 2.0
    ir::FieldSpec field{
        "mid",
        ir::Expr{ir::BinaryExpr{
            ir::ArithmeticOp::Div,
            std::make_shared<ir::Expr>(ir::Expr{ir::BinaryExpr{
                ir::ArithmeticOp::Add, std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{"bid"}}),
                std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{"ask"}})}}),
            std::make_shared<ir::Expr>(ir::Expr{ir::Literal{2.0}})}}};

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

TEST_CASE("emitter: update node — literal types", "[codegen]") {
    ir::Builder b;

    ir::FieldSpec f1{"label", ir::Expr{ir::Literal{std::string{"hello"}}}};
    ir::FieldSpec f2{"count", ir::Expr{ir::Literal{std::int64_t{42}}}};
    ir::FieldSpec f3{"day", ir::Expr{ir::Literal{Date{1}}}};
    ir::FieldSpec f4{"ts", ir::Expr{ir::Literal{Timestamp{1000}}}};

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

TEST_CASE("emitter: update node — tuple sources and by keys", "[codegen]") {
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
        .args = {std::make_shared<ir::Expr>(ir::Expr{ir::Literal{std::int64_t{7}}})},
        .named_args = {ir::NamedArg{
            .name = "times",
            .value = std::make_shared<ir::Expr>(ir::Expr{ir::Literal{std::int64_t{3}}}),
        }},
    };
    auto upd =
        b.update({ir::FieldSpec{.alias = "v", .expr = ir::Expr{.node = std::move(rep_call)}}});
    upd->add_child(make_source(b, "base.csv"));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::fn_call(\"rep\""));
    CHECK(contains(out, "ibex::ops::NamedArgExpr{\"times\""));
}

// ─── Chained pipeline ────────────────────────────────────────────────────────

TEST_CASE("emitter: filter then project pipeline", "[codegen]") {
    ir::Builder b;
    auto filter = b.filter(filter_cmp(ir::CompareOp::Ge, filter_col("price"), filter_int(50)));
    filter->add_child(make_source(b, "trades.csv"));
    auto proj = b.project({ir::ColumnRef{"symbol"}});
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

// ─── Join ────────────────────────────────────────────────────────────────────

TEST_CASE("emitter: join node — inner join", "[codegen]") {
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

TEST_CASE("emitter: join node — right join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Right, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::right_join("));
}

TEST_CASE("emitter: join node — outer join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Outer, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::outer_join("));
}
TEST_CASE("emitter: join node — semi join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Semi, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::semi_join("));
}

TEST_CASE("emitter: join node — anti join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Anti, {"id"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::anti_join("));
}

TEST_CASE("emitter: join node — cross join", "[codegen]") {
    ir::Builder b;
    auto left = make_source(b, "left.csv");
    auto right = make_source(b, "right.csv");
    auto join = b.join(ir::JoinKind::Cross, {});
    join->add_child(std::move(left));
    join->add_child(std::move(right));

    auto out = emit_to_string(*join);
    CHECK(contains(out, "ibex::ops::cross_join("));
}

TEST_CASE("emitter: join node — asof join", "[codegen]") {
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

TEST_CASE("emitter: join node — non-equijoin predicate", "[codegen]") {
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

// ─── Config ──────────────────────────────────────────────────────────────────

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

// ─── String escaping ─────────────────────────────────────────────────────────

TEST_CASE("emitter: escape quotes in extern call arg", "[codegen]") {
    ir::Builder b;
    auto root = b.extern_call("read_csv",
                              {ir::Expr{ir::Literal{std::string{R"(path/with "quotes".csv)"}}}});
    auto out = emit_to_string(*root);
    CHECK(contains(out, R"(path/with \"quotes\".csv)"));
}
