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

TEST_CASE("cardinality bounds a semi join by its left input", "[ir][cardinality]") {
    // A semi join selects left rows -- each survives or does not, and none is
    // duplicated -- so the left count is a hard upper bound and the same
    // selectivity guess the filter arm uses applies.
    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Semi, {"o_orderkey"});
    join->add_child(builder.scan("orders"));
    join->add_child(builder.scan("lineitem"));

    const auto estimate = ir::estimate_cardinality(*join, {{"orders", 1'500}, {"lineitem", 6'000}},
                                                   {.filter_selectivity = 0.1});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 150);
    CHECK(estimate.heuristic);
}

TEST_CASE("cardinality gives an anti join the complement of a semi join", "[ir][cardinality]") {
    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Anti, {"o_orderkey"});
    join->add_child(builder.scan("orders"));
    join->add_child(builder.scan("lineitem"));

    const auto estimate = ir::estimate_cardinality(*join, {{"orders", 1'500}, {"lineitem", 6'000}},
                                                   {.filter_selectivity = 0.1});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 1'350);
    CHECK(estimate.heuristic);
}

TEST_CASE("cardinality multiplies a cross join exactly", "[ir][cardinality]") {
    // Exact, not a guess: the common shape is an uncorrelated scalar subquery's
    // one-row right side, where this is just the left count.
    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Cross, {});
    join->add_child(builder.scan("orders"));
    join->add_child(builder.scan("one_row"));

    const auto estimate = ir::estimate_cardinality(*join, {{"orders", 1'500}, {"one_row", 1}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 1'500);
    CHECK_FALSE(estimate.heuristic);
}

TEST_CASE("cardinality refuses to estimate an inner join", "[ir][cardinality]") {
    // An inner join fans out on duplicate keys, so its size needs distinct-key
    // counts nothing plumbs through yet. Returning nothing makes the join-order
    // cost model decline, which is the honest outcome: a cost model that
    // reorders on an invented number regresses queries to improve an average.
    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Inner, {"o_orderkey"});
    join->add_child(builder.scan("orders"));
    join->add_child(builder.scan("lineitem"));

    const auto estimate = ir::estimate_cardinality(*join, {{"orders", 1'500}, {"lineitem", 6'000}});
    CHECK_FALSE(estimate.rows.has_value());
}
