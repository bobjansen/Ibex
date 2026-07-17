#include <ibex/ir/builder.hpp>
#include <ibex/ir/cardinality.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <utility>

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
        ir::estimate_cardinality(*filter, {{"orders", 1'500}}, {}, {.filter_selectivity = 0.1});
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
                                                   {}, {.filter_selectivity = 0.1});
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
                                                   {}, {.filter_selectivity = 0.1});
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

TEST_CASE("cardinality refuses an inner join with no proven unique side", "[ir][cardinality]") {
    // Two bare scans: nothing proves either side unique on the key, so the join
    // can fan out arbitrarily and its size would need distinct-key counts we do
    // not have. Returning nothing makes the join-order cost model decline,
    // which is the honest outcome -- a cost model that reorders on an invented
    // number regresses individual queries to improve an average.
    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Inner, {"o_orderkey"});
    join->add_child(builder.scan("orders"));
    join->add_child(builder.scan("lineitem"));

    const auto estimate = ir::estimate_cardinality(*join, {{"orders", 1'500}, {"lineitem", 6'000}});
    CHECK_FALSE(estimate.rows.has_value());
}

TEST_CASE("cardinality bounds an inner join by the non-unique side", "[ir][cardinality]") {
    // `revenue = lineitem[select {r = sum(v)}, by {l_suppkey}]` is unique on
    // l_suppkey BY CONSTRUCTION -- one row per group. So each lineitem row
    // matches at most one revenue row and the join yields at most |lineitem|
    // rows. Note what is NOT needed: the distinct count of l_suppkey, or any
    // statistic at all. Knowing *that* the side is unique is the whole input.
    ir::Builder builder;
    auto revenue = builder.aggregate(
        {ir::ColumnRef{.name = "l_suppkey"}},
        {ir::AggSpec{
            .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "v"}, .alias = "r"}});
    revenue->add_child(builder.scan("lineitem_src"));
    auto join = builder.join(ir::JoinKind::Inner, {"l_suppkey"});
    join->add_child(builder.scan("lineitem"));
    join->add_child(std::move(revenue));

    const auto estimate = ir::estimate_cardinality(*join, {{"lineitem", 6'000}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 6'000);
    // The bound is proved; using it as a point estimate assumes every lineitem
    // row finds a match, which is an assumption and so flagged.
    CHECK(estimate.heuristic);
}

TEST_CASE("cardinality takes the tighter bound when both join sides are unique",
          "[ir][cardinality]") {
    // Both sides unique on k bounds the output by *either* side's rows, so the
    // smaller one is the bound that holds.
    //
    // Reaching that needs a unique side that also has a row count, which by
    // construction alone is only ever an ungrouped aggregate (one row) -- a
    // grouped one is unique but unsized. So this states the rule through a
    // source-declared unique key, which is the shape a scan-level PK would
    // arrive in if one is ever derived (see the footer note in
    // plans/decode-fusion-plan.md).
    auto unique_source = [](std::string column) {
        auto info = ir::SchemaInfo::known({ir::SchemaField{.name = column}});
        info.add_unique_key({std::move(column)});
        return info;
    };
    ir::SourceSchemas schemas;
    schemas["big"] = unique_source("k");
    schemas["small"] = unique_source("k");

    ir::Builder builder;
    auto join = builder.join(ir::JoinKind::Inner, {"k"});
    join->add_child(builder.scan("big"));
    join->add_child(builder.scan("small"));

    const auto estimate = ir::estimate_cardinality(*join, {{"big", 900}, {"small", 40}}, schemas);
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 40);
}

TEST_CASE("cardinality bounds an inner join on a subset of the join keys", "[ir][cardinality]") {
    // Uniqueness on {s_suppkey} alone bounds a join on {s_suppkey, s_nationkey}:
    // a wider key can only match fewer rows, never more. The subset direction
    // is the one that is sound.
    ir::Builder builder;
    auto revenue = builder.aggregate({ir::ColumnRef{.name = "s_suppkey"}},
                                     {ir::AggSpec{.func = ir::AggFunc::Sum,
                                                  .column = ir::ColumnRef{.name = "v"},
                                                  .alias = "s_nationkey"}});
    revenue->add_child(builder.scan("lineitem_src"));
    auto join = builder.join(ir::JoinKind::Inner, {"s_suppkey", "s_nationkey"});
    join->add_child(builder.scan("supplier"));
    join->add_child(std::move(revenue));

    const auto estimate = ir::estimate_cardinality(*join, {{"supplier", 10'000}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 10'000);
}

TEST_CASE("cardinality collapses an ungrouped aggregate to one row", "[ir][cardinality]") {
    // By construction, whatever it reads.
    ir::Builder builder;
    auto agg = builder.aggregate(
        {}, {ir::AggSpec{
                .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "v"}, .alias = "total"}});
    agg->add_child(builder.scan("lineitem"));

    const auto estimate = ir::estimate_cardinality(*agg, {{"lineitem", 6'001'215}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 1);
    CHECK_FALSE(estimate.heuristic);
}

TEST_CASE("cardinality does not size a grouped aggregate", "[ir][cardinality]") {
    // Its size is the distinct cardinality of the group keys, which needs
    // statistics we do not have. Uniqueness answers "are the keys distinct",
    // never "how many are there" -- so it cannot fill this in.
    ir::Builder builder;
    auto agg = builder.aggregate(
        {ir::ColumnRef{.name = "l_suppkey"}},
        {ir::AggSpec{
            .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "v"}, .alias = "total"}});
    agg->add_child(builder.scan("lineitem"));

    CHECK_FALSE(ir::estimate_cardinality(*agg, {{"lineitem", 6'001'215}}).rows.has_value());
}

TEST_CASE("cardinality carries a proven unique key through a filtered projection",
          "[ir][cardinality]") {
    // The proof has to survive the row-wise operators a real plan puts between
    // the aggregate and the join, or it never reaches the estimator.
    ir::Builder builder;
    auto revenue = builder.aggregate(
        {ir::ColumnRef{.name = "s_suppkey"}},
        {ir::AggSpec{
            .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "v"}, .alias = "total"}});
    revenue->add_child(builder.scan("lineitem_src"));
    auto filtered = builder.filter(ir::Expr{.node = ir::Literal{.value = true}});
    filtered->add_child(std::move(revenue));
    auto projected =
        builder.project({ir::ColumnRef{.name = "s_suppkey"}, ir::ColumnRef{.name = "total"}});
    projected->add_child(std::move(filtered));

    auto join = builder.join(ir::JoinKind::Inner, {"s_suppkey"});
    join->add_child(builder.scan("supplier"));
    join->add_child(std::move(projected));

    const auto estimate = ir::estimate_cardinality(*join, {{"supplier", 10'000}});
    REQUIRE(estimate.rows.has_value());
    CHECK(*estimate.rows == 10'000);
}

TEST_CASE("cardinality loses the unique key when a projection drops it", "[ir][cardinality]") {
    // Project away the group key and the proof goes with it: the remaining
    // column is an aggregate value, which can repeat.
    ir::Builder builder;
    auto revenue = builder.aggregate({ir::ColumnRef{.name = "s_suppkey"}},
                                     {ir::AggSpec{.func = ir::AggFunc::Sum,
                                                  .column = ir::ColumnRef{.name = "v"},
                                                  .alias = "s_suppkey_total"}});
    revenue->add_child(builder.scan("lineitem_src"));
    auto projected = builder.project({ir::ColumnRef{.name = "s_suppkey_total"}});
    projected->add_child(std::move(revenue));

    auto join = builder.join(ir::JoinKind::Inner, {"s_suppkey"});
    join->add_child(builder.scan("supplier"));
    join->add_child(std::move(projected));

    CHECK_FALSE(ir::estimate_cardinality(*join, {{"supplier", 10'000}}).rows.has_value());
}
