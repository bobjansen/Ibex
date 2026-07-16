#include <ibex/ir/builder.hpp>
#include <ibex/ir/join_reorder.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

namespace {

auto sources() -> ir::SourceSchemas {
    return {{"customer", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64}})},
            {"orders", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64},
                                              {"o_orderkey", ir::ColumnType::Int64}})},
            {"lineitem", ir::SchemaInfo::known({{"o_orderkey", ir::ColumnType::Int64},
                                                {"l_price", ir::ColumnType::Float64}})}};
}

auto scan_name(const ir::Node& node) -> std::string {
    const auto* scan = dynamic_cast<const ir::ScanNode*>(&node);
    REQUIRE(scan != nullptr);
    return scan->source_name();
}

}  // namespace

TEST_CASE("join reorder changes an aggregate's order-insensitive inner chain",
          "[ir][join_reorder]") {
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

    auto out = ir::reorder_inner_joins_for_aggregates(
        std::move(aggregate), sources(),
        {{"customer", 150'000}, {"orders", 1'500'000}, {"lineitem", 1'000}});
    const auto& outer_join = *out->children().front();
    REQUIRE(outer_join.kind() == ir::NodeKind::Join);
    CHECK(scan_name(*outer_join.children()[1]) == "customer");
    const auto& inner_join = *outer_join.children()[0];
    REQUIRE(inner_join.kind() == ir::NodeKind::Join);
    CHECK(scan_name(*inner_join.children()[0]) == "lineitem");
    CHECK(scan_name(*inner_join.children()[1]) == "orders");
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

    auto out = ir::reorder_inner_joins_for_aggregates(std::move(aggregate), sources(),
                                                      {{"customer", 2'000}, {"orders", 1}});
    CHECK(scan_name(*out->children().front()->children()[0]) == "customer");
}
