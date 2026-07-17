#include <ibex/ir/builder.hpp>
#include <ibex/ir/join_reorder.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

namespace {

// customer -- orders -- lineitem, but with lineitem tiny (a deliberately bad
// order to build customer-first). Distinct estimates are what a Parquet footer
// would give: dense PKs exact, a foreign key's distinct == the parent's PK.
auto misordered_stats() -> ir::SourceStats {
    ir::SourceStats stats;
    stats.schemas = {{"customer", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64}})},
                     {"orders", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64},
                                                       {"o_orderkey", ir::ColumnType::Int64}})},
                     {"lineitem", ir::SchemaInfo::known({{"o_orderkey", ir::ColumnType::Int64},
                                                         {"l_price", ir::ColumnType::Float64}})}};
    stats.rows = {{"customer", 150'000}, {"orders", 1'500'000}, {"lineitem", 1'000}};
    stats.distinct = {{"customer", {{"c_custkey", 150'000}}},
                      {"orders", {{"c_custkey", 150'000}, {"o_orderkey", 1'500'000}}},
                      {"lineitem", {{"o_orderkey", 1'000}}}};
    return stats;
}

auto scan_name(const ir::Node& node) -> std::string {
    const auto* scan = dynamic_cast<const ir::ScanNode*>(&node);
    REQUIRE(scan != nullptr);
    return scan->source_name();
}

}  // namespace

TEST_CASE("join reorder changes an aggregate's order-insensitive inner chain",
          "[ir][join_reorder]") {
    // Chain as written joins customer(150k) and orders(1.5M) first -- a 1.5M
    // intermediate -- when starting from the 1000-row lineitem builds only
    // 1000-row intermediates. The cost model should rebuild it lineitem-first.
    ir::Builder builder;
    auto first = builder.join(ir::JoinKind::Inner, {"c_custkey"});
    first->add_child(builder.scan("customer"));
    first->add_child(builder.scan("orders"));
    auto chain = builder.join(ir::JoinKind::Inner, {"o_orderkey"});
    chain->add_child(std::move(first));
    chain->add_child(builder.scan("lineitem"));
    auto aggregate = builder.aggregate(
        {}, {ir::AggSpec{.func = ir::AggFunc::Count, .column = {}, .alias = "n"}});
    aggregate->add_child(std::move(chain));

    auto out = ir::reorder_inner_joins_for_aggregates(std::move(aggregate), misordered_stats());
    const auto& outer_join = *out->children().front();
    REQUIRE(outer_join.kind() == ir::NodeKind::Join);
    // The property that matters: customer -- the big table whose join builds the
    // 1.5M intermediate -- is no longer in the first join. It is joined last,
    // and the inner join is the cheap {lineitem, orders} pair. Which of those
    // two seeds the chain is a genuine tie (both build only 1000-row
    // intermediates), so the test does not pin the tie-break.
    CHECK(scan_name(*outer_join.children()[1]) == "customer");
    const auto& inner_join = *outer_join.children()[0];
    REQUIRE(inner_join.kind() == ir::NodeKind::Join);
    const std::string a = scan_name(*inner_join.children()[0]);
    const std::string b = scan_name(*inner_join.children()[1]);
    CHECK(((a == "lineitem" && b == "orders") || (a == "orders" && b == "lineitem")));
}

TEST_CASE("join reorder hands back the plan intact when it rejects a rewrite",
          "[ir][join_reorder]") {
    // `dup` is a non-key column on two leaves, so key ownership is ambiguous and
    // the rewrite must be refused. The refusal has to happen while the plan is
    // still whole: the dismantling step moves children out as it descends, and
    // rejecting after it would leave the aggregate with no input at all. That
    // path was unreachable only because the cost model always declined here --
    // it stopped being unreachable the moment inner joins became estimable.
    ir::SourceStats stats = misordered_stats();
    // Add a non-key column `dup` shared by two leaves, making key ownership
    // ambiguous. (Distinct estimates are inherited from misordered_stats and are
    // enough to reach the cost model, which is the point -- the ambiguity check
    // must fire regardless.)
    stats.schemas["customer"] = ir::SchemaInfo::known(
        {{"c_custkey", ir::ColumnType::Int64}, {"dup", ir::ColumnType::Int64}});
    stats.schemas["orders"] = ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64},
                                                     {"o_orderkey", ir::ColumnType::Int64},
                                                     {"dup", ir::ColumnType::Int64}});

    ir::Builder builder;
    auto first = builder.join(ir::JoinKind::Inner, {"c_custkey"});
    first->add_child(builder.scan("customer"));
    first->add_child(builder.scan("orders"));
    auto chain = builder.join(ir::JoinKind::Inner, {"o_orderkey"});
    chain->add_child(std::move(first));
    chain->add_child(builder.scan("lineitem"));
    auto aggregate = builder.aggregate(
        {}, {ir::AggSpec{.func = ir::AggFunc::Count, .column = {}, .alias = "n"}});
    aggregate->add_child(std::move(chain));

    // The stats would reorder (lineitem is smallest) if the rewrite were
    // allowed, so the rejection is what is under test rather than a no-op.
    auto out = ir::reorder_inner_joins_for_aggregates(std::move(aggregate), stats);

    REQUIRE(out != nullptr);
    REQUIRE(out->children().size() == 1);
    REQUIRE(out->children().front() != nullptr);  // not gutted
    const auto& outer_join = *out->children().front();
    REQUIRE(outer_join.kind() == ir::NodeKind::Join);
    CHECK(scan_name(*outer_join.children()[1]) == "lineitem");  // untouched source order
}

TEST_CASE("join reorder keeps First aggregates in source order", "[ir][join_reorder]") {
    ir::Builder builder;
    auto root = builder.join(ir::JoinKind::Inner, {"c_custkey"});
    root->add_child(builder.scan("customer"));
    root->add_child(builder.scan("orders"));
    auto aggregate = builder.aggregate(
        {},
        {ir::AggSpec{.func = ir::AggFunc::First, .column = {.name = "o_orderkey"}, .alias = "x"}});
    aggregate->add_child(std::move(root));

    // Stats that would reorder (orders is tiny) if First did not veto it.
    ir::SourceStats stats = misordered_stats();
    stats.rows = {{"customer", 2'000}, {"orders", 1}};
    auto out = ir::reorder_inner_joins_for_aggregates(std::move(aggregate), stats);
    CHECK(scan_name(*out->children().front()->children()[0]) == "customer");
}
