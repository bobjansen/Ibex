#include <ibex/ir/builder.hpp>
#include <ibex/ir/join_order.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

namespace {

auto sources() -> ir::SourceSchemas {
    return {{"customer", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64}})},
            {"orders", ir::SchemaInfo::known({{"c_custkey", ir::ColumnType::Int64},
                                              {"o_orderkey", ir::ColumnType::Int64}})},
            {"lineitem", ir::SchemaInfo::known({{"o_orderkey", ir::ColumnType::Int64}})}};
}

}  // namespace

TEST_CASE("join order starts with the smallest connected relation", "[ir][join_order]") {
    ir::Builder builder;
    auto customer = builder.scan("customer");
    auto orders = builder.scan("orders");
    auto first = builder.join(ir::JoinKind::Inner, {"c_custkey"});
    first->add_child(std::move(customer));
    first->add_child(std::move(orders));
    auto lineitem = builder.scan("lineitem");
    auto root = builder.join(ir::JoinKind::Inner, {"o_orderkey"});
    root->add_child(std::move(first));
    root->add_child(std::move(lineitem));

    const auto order = ir::choose_inner_join_order(
        *root, sources(), {{"customer", 150'000}, {"orders", 1'500'000}, {"lineitem", 6'000'000}});
    REQUIRE(order.has_value());
    CHECK(*order == std::vector<std::size_t>{0, 1, 2});
}

TEST_CASE("join order rejects an uncosted source", "[ir][join_order]") {
    ir::Builder builder;
    auto left = builder.scan("customer");
    auto right = builder.scan("orders");
    auto root = builder.join(ir::JoinKind::Inner, {"c_custkey"});
    root->add_child(std::move(left));
    root->add_child(std::move(right));

    CHECK_FALSE(ir::choose_inner_join_order(*root, sources(), {{"customer", 150'000}}).has_value());
}
