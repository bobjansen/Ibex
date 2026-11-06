// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/ir/sort_key_reduction.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

/// `t(x, y)` with `x` proved null-free when `x_never_null`; `y` may be null.
auto sources(bool x_never_null) -> ir::SourceSchemas {
    ir::SourceSchemas out;
    out.emplace("t", ir::SchemaInfo::known({
                         ir::SchemaField{.name = "x",
                                         .type = ir::ColumnType::Float64,
                                         .nulls = x_never_null ? ir::Nullability::Never
                                                               : ir::Nullability::Maybe},
                         ir::SchemaField{.name = "y", .type = ir::ColumnType::Float64},
                     }));
    return out;
}

auto col(std::string name) -> ir::Expr {
    return ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}};
}

auto is_null(std::string name, bool negated = false) -> ir::Expr {
    return ir::Expr{
        .node = ir::IsNullExpr{.operand = ir::ExprPtr{col(std::move(name))}, .negated = negated}};
}

auto update(ir::NodePtr input, std::string alias, ir::Expr expr, std::uint64_t id = 2)
    -> ir::NodePtr {
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{.alias = std::move(alias), .expr = std::move(expr)});
    auto node = std::make_unique<ir::UpdateNode>(ir::NodeId{id}, std::move(fields));
    node->add_child(std::move(input));
    return node;
}

auto scan() -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{1}, "t");
}

/// The adapter's nulls-first sort: `[update { f = <flag> }][order { f desc, x desc }]`.
auto flagged_order(ir::Expr flag) -> ir::NodePtr {
    auto order = std::make_unique<ir::OrderNode>(
        ir::NodeId{3}, std::vector<ir::OrderKey>{ir::OrderKey{.name = "f", .ascending = false},
                                                 ir::OrderKey{.name = "x", .ascending = false}});
    order->add_child(update(scan(), "f", std::move(flag)));
    return order;
}

auto key_names(const ir::Node& node) -> std::vector<std::string> {
    const auto& keys = node.kind() == ir::NodeKind::TopK
                           ? ir::node_cast<ir::TopKNode>(node).keys()
                           : ir::node_cast<ir::OrderNode>(node).keys();
    std::vector<std::string> out;
    for (const auto& key : keys) {
        out.push_back(key.name);
    }
    return out;
}

}  // namespace

TEST_CASE("sort key reduction drops a null flag over a null-free column") {
    auto plan = ir::drop_constant_null_flag_sort_keys(flagged_order(is_null("x")), sources(true));
    REQUIRE(plan->kind() == ir::NodeKind::Order);
    CHECK(key_names(*plan) == std::vector<std::string>{"x"});
}

TEST_CASE("sort key reduction keeps a null flag that can be true") {
    SECTION("the operand may be null") {
        auto plan =
            ir::drop_constant_null_flag_sort_keys(flagged_order(is_null("x")), sources(false));
        CHECK(key_names(*plan) == std::vector<std::string>{"f", "x"});
    }
    SECTION("a negated flag is constant true, not false") {
        // Still constant, but this pass only claims the shape it was written for.
        auto plan =
            ir::drop_constant_null_flag_sort_keys(flagged_order(is_null("x", true)), sources(true));
        CHECK(key_names(*plan) == std::vector<std::string>{"f", "x"});
    }
    SECTION("the flag reads another, nullable column") {
        auto plan =
            ir::drop_constant_null_flag_sort_keys(flagged_order(is_null("y")), sources(true));
        CHECK(key_names(*plan) == std::vector<std::string>{"f", "x"});
    }
}

TEST_CASE("sort key reduction reads the nearest definition of the key") {
    // `f` is redefined above the null flag, as a value that can vary.
    auto order = std::make_unique<ir::OrderNode>(
        ir::NodeId{3}, std::vector<ir::OrderKey>{ir::OrderKey{.name = "f", .ascending = false},
                                                 ir::OrderKey{.name = "x", .ascending = false}});
    order->add_child(update(update(scan(), "f", is_null("x"), 2), "f", col("y"), 4));
    auto plan = ir::drop_constant_null_flag_sort_keys(std::move(order), sources(true));
    CHECK(key_names(*plan) == std::vector<std::string>{"f", "x"});
}

TEST_CASE("sort key reduction rewrites a top-k and looks through a projection") {
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{5},
        std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "f"}, ir::ColumnRef{.name = "x"}});
    project->add_child(update(scan(), "f", is_null("x")));
    auto topk = std::make_unique<ir::TopKNode>(
        ir::NodeId{6},
        std::vector<ir::OrderKey>{ir::OrderKey{.name = "f", .ascending = false},
                                  ir::OrderKey{.name = "x", .ascending = false}},
        20, std::vector<ir::ColumnRef>{}, ir::TopKNode::KeepMode::First);
    topk->add_child(std::move(project));
    auto plan = ir::drop_constant_null_flag_sort_keys(std::move(topk), sources(true));
    REQUIRE(plan->kind() == ir::NodeKind::TopK);
    CHECK(key_names(*plan) == std::vector<std::string>{"x"});
    CHECK(ir::node_cast<ir::TopKNode>(*plan).count() == 20);
}

TEST_CASE("sort key reduction never drops every key") {
    auto order = std::make_unique<ir::OrderNode>(
        ir::NodeId{3}, std::vector<ir::OrderKey>{ir::OrderKey{.name = "f", .ascending = false}});
    order->add_child(update(scan(), "f", is_null("x")));
    auto plan = ir::drop_constant_null_flag_sort_keys(std::move(order), sources(true));
    CHECK(key_names(*plan) == std::vector<std::string>{"f"});
}
