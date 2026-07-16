#include <ibex/ir/builder.hpp>
#include <ibex/ir/cardinality.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

TEST_CASE("cardinality uses source metadata through row-preserving operators",
          "[ir][cardinality]") {
    ir::Builder builder;
    auto scan = builder.scan("lineitem");
    auto project = builder.project({ir::ColumnRef{.name = "l_orderkey"}});
    project->add_child(std::move(scan));

    const auto estimate = ir::estimate_cardinality(*project, {{"lineitem", 6'001'215}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 6'001'215);
    CHECK_FALSE(estimate.heuristic);
}

TEST_CASE("cardinality makes filter selectivity explicit", "[ir][cardinality]") {
    ir::Builder builder;
    auto scan = builder.scan("orders");
    auto filter = builder.filter(ir::Expr{.node = ir::Literal{.value = true}});
    filter->add_child(std::move(scan));

    const auto estimate =
        ir::estimate_cardinality(*filter, {{"orders", 1'500}}, {.filter_selectivity = 0.1});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 150);
    CHECK(estimate.heuristic);
}

TEST_CASE("cardinality does not invent missing source sizes", "[ir][cardinality]") {
    ir::Builder builder;
    auto scan = builder.scan("unknown");

    const auto estimate = ir::estimate_cardinality(*scan, {});
    CHECK_FALSE(estimate.rows.has_value());
}
