#include <ibex/codegen/emitter.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>

#include <catch2/catch_test_macros.hpp>

#include <sstream>
#include <string>

using namespace ibex;

// Helper: emit an IR tree to a string.
static auto emit_to_string(const ir::Node& root,
                            const codegen::Emitter::Config& cfg = {}) -> std::string {
    std::ostringstream oss;
    codegen::Emitter emitter;
    emitter.emit(oss, root, cfg);
    return oss.str();
}

// Check that a string contains a substring.
static auto contains(const std::string& haystack, const std::string& needle) -> bool {
    return haystack.find(needle) != std::string::npos;
}

// ─── Scan ────────────────────────────────────────────────────────────────────

TEST_CASE("emitter: scan node", "[codegen]") {
    ir::Builder b;
    auto root = b.scan("trades.csv");
    auto out = emit_to_string(*root);

    CHECK(contains(out, "#include <ibex/runtime/ops.hpp>"));
    CHECK(contains(out, "int main()"));
    CHECK(contains(out, "ibex::ops::scan(\"trades.csv\")"));
    CHECK(contains(out, "ibex::ops::print("));
    CHECK(contains(out, "return 0;"));
}

// ─── Filter ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: filter node — int64 predicate", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("data.csv");
    ir::FilterPredicate pred{ir::ColumnRef{"price"}, ir::CompareOp::Gt,
                             std::int64_t{100}};
    auto filter = b.filter(std::move(pred));
    filter->add_child(std::move(scan));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ops::scan(\"data.csv\")"));
    CHECK(contains(out, "ibex::ops::filter("));
    CHECK(contains(out, "ibex::ir::CompareOp::Gt"));
    CHECK(contains(out, "std::int64_t{100}"));
}

TEST_CASE("emitter: filter node — double predicate", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("data.csv");
    ir::FilterPredicate pred{ir::ColumnRef{"ratio"}, ir::CompareOp::Le, 0.5};
    auto filter = b.filter(std::move(pred));
    filter->add_child(std::move(scan));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Le"));
    CHECK(contains(out, "0.5"));
}

TEST_CASE("emitter: filter node — string predicate", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("data.csv");
    ir::FilterPredicate pred{ir::ColumnRef{"symbol"}, ir::CompareOp::Eq,
                             std::string{"AAPL"}};
    auto filter = b.filter(std::move(pred));
    filter->add_child(std::move(scan));

    auto out = emit_to_string(*filter);
    CHECK(contains(out, "ibex::ir::CompareOp::Eq"));
    CHECK(contains(out, "std::string{\"AAPL\"}"));
}

// ─── Project ─────────────────────────────────────────────────────────────────

TEST_CASE("emitter: project node", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("trades.csv");
    auto proj = b.project({ir::ColumnRef{"symbol"}, ir::ColumnRef{"price"}});
    proj->add_child(std::move(scan));

    auto out = emit_to_string(*proj);
    CHECK(contains(out, "ibex::ops::project("));
    CHECK(contains(out, "\"symbol\""));
    CHECK(contains(out, "\"price\""));
}

// ─── Aggregate ───────────────────────────────────────────────────────────────

TEST_CASE("emitter: aggregate node", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("trades.csv");
    auto agg = b.aggregate(
        {ir::ColumnRef{"symbol"}},
        {ir::AggSpec{ir::AggFunc::Sum, ir::ColumnRef{"price"}, "total"},
         ir::AggSpec{ir::AggFunc::Count, ir::ColumnRef{"price"}, "n"}});
    agg->add_child(std::move(scan));

    auto out = emit_to_string(*agg);
    CHECK(contains(out, "ibex::ops::aggregate("));
    CHECK(contains(out, "\"symbol\""));
    CHECK(contains(out, "ibex::ir::AggFunc::Sum"));
    CHECK(contains(out, "ibex::ir::AggFunc::Count"));
    CHECK(contains(out, "\"total\""));
    CHECK(contains(out, "\"n\""));
    CHECK(contains(out, "ibex::ops::make_agg("));
}

// ─── Update ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: update node — simple expression", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("trades.csv");

    // mid = (bid + ask) / 2.0
    ir::FieldSpec field{
        "mid",
        ir::Expr{ir::BinaryExpr{
            ir::ArithmeticOp::Div,
            std::make_shared<ir::Expr>(ir::Expr{ir::BinaryExpr{
                ir::ArithmeticOp::Add,
                std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{"bid"}}),
                std::make_shared<ir::Expr>(ir::Expr{ir::ColumnRef{"ask"}})}}),
            std::make_shared<ir::Expr>(ir::Expr{ir::Literal{2.0}})}}};

    auto upd = b.update({std::move(field)});
    upd->add_child(std::move(scan));

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
    auto scan = b.scan("t.csv");

    ir::FieldSpec f1{"label", ir::Expr{ir::Literal{std::string{"hello"}}}};
    ir::FieldSpec f2{"count", ir::Expr{ir::Literal{std::int64_t{42}}}};

    auto upd = b.update({std::move(f1), std::move(f2)});
    upd->add_child(std::move(scan));

    auto out = emit_to_string(*upd);
    CHECK(contains(out, "ibex::ops::str_lit(\"hello\")"));
    CHECK(contains(out, "ibex::ops::int_lit(std::int64_t{42})"));
}

// ─── Chained pipeline ────────────────────────────────────────────────────────

TEST_CASE("emitter: filter then project pipeline", "[codegen]") {
    ir::Builder b;
    auto scan = b.scan("trades.csv");
    ir::FilterPredicate pred{ir::ColumnRef{"price"}, ir::CompareOp::Ge,
                             std::int64_t{50}};
    auto filter = b.filter(std::move(pred));
    filter->add_child(std::move(scan));
    auto proj = b.project({ir::ColumnRef{"symbol"}});
    proj->add_child(std::move(filter));

    auto out = emit_to_string(*proj);
    // Should have scan -> filter -> project order in code
    auto pos_scan = out.find("ibex::ops::scan(");
    auto pos_filter = out.find("ibex::ops::filter(");
    auto pos_proj = out.find("ibex::ops::project(");
    REQUIRE(pos_scan != std::string::npos);
    REQUIRE(pos_filter != std::string::npos);
    REQUIRE(pos_proj != std::string::npos);
    CHECK(pos_scan < pos_filter);
    CHECK(pos_filter < pos_proj);
}

// ─── Config ──────────────────────────────────────────────────────────────────

TEST_CASE("emitter: extern headers in config", "[codegen]") {
    ir::Builder b;
    auto root = b.scan("t.csv");

    codegen::Emitter::Config cfg;
    cfg.extern_headers = {"stats.hpp", "math_utils.hpp"};
    cfg.source_name = "query.ibex";

    auto out = emit_to_string(*root, cfg);
    CHECK(contains(out, "// Source: query.ibex"));
    CHECK(contains(out, "#include \"stats.hpp\""));
    CHECK(contains(out, "#include \"math_utils.hpp\""));
}

// ─── String escaping ─────────────────────────────────────────────────────────

TEST_CASE("emitter: escape quotes in scan source", "[codegen]") {
    ir::Builder b;
    auto root = b.scan(R"(path/with "quotes".csv)");
    auto out = emit_to_string(*root);
    // The emitted string should have escaped quotes
    CHECK(contains(out, R"(path/with \"quotes\".csv)"));
}
