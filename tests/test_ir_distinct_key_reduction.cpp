// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/distinct_key_reduction.hpp>
#include <ibex/ir/node.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
namespace {

auto scan(std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{1}, std::move(name));
}

auto refs(std::vector<std::string> names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> out;
    out.reserve(names.size());
    for (auto& name : names) {
        out.push_back(ir::ColumnRef{.name = std::move(name), .source = {0}});
    }
    return out;
}

auto column(std::string name) -> ir::Expr {
    return ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}};
}

/// `update { alias = <expr> }`.
auto update(std::string alias, ir::Expr expr, ir::NodePtr input, ir::NodeId id = ir::NodeId{2})
    -> ir::NodePtr {
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{.alias = std::move(alias), .expr = std::move(expr)});
    auto node = std::make_unique<ir::UpdateNode>(id, std::move(fields));
    node->add_child(std::move(input));
    return node;
}

auto project(std::vector<std::string> columns, ir::NodePtr input, ir::NodeId id = ir::NodeId{3})
    -> ir::NodePtr {
    auto node = std::make_unique<ir::ProjectNode>(id, refs(std::move(columns)));
    node->add_child(std::move(input));
    return node;
}

auto distinct(ir::NodePtr input) -> ir::NodePtr {
    auto node = std::make_unique<ir::DistinctNode>(ir::NodeId{4});
    node->add_child(std::move(input));
    return node;
}

auto find_distinct(const ir::Node& plan) -> const ir::Node* {
    if (plan.kind() == ir::NodeKind::Distinct) {
        return &plan;
    }
    for (const auto& child : plan.children()) {
        if (child != nullptr) {
            if (const auto* found = find_distinct(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

/// What the dedup actually keys on: the columns of the projection under it.
auto dedup_columns(const ir::Node& plan) -> std::vector<std::string> {
    const auto* node = find_distinct(plan);
    REQUIRE(node != nullptr);
    REQUIRE(node->children().size() == 1);
    const auto& below = *node->children()[0];
    REQUIRE(below.kind() == ir::NodeKind::Project);
    std::vector<std::string> names;
    for (const auto& ref : static_cast<const ir::ProjectNode&>(below).columns()) {
        names.push_back(ref.name);
    }
    return names;
}

/// The plan's output columns, in order, as the top projection gives them.
auto output_columns(const ir::Node& plan) -> std::vector<std::string> {
    REQUIRE(plan.kind() == ir::NodeKind::Project);
    std::vector<std::string> names;
    for (const auto& ref : static_cast<const ir::ProjectNode&>(plan).columns()) {
        names.push_back(ref.name);
    }
    return names;
}

}  // namespace
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

TEST_CASE("distinct key reduction dedups on one copy of a duplicated column") {
    // PDS-H q22's shape: one column presented twice so the join has a matching
    // name, then a dedup on the pair.
    auto plan =
        distinct(project({"c_custkey", "o_custkey"},
                         project({"c_custkey", "o_custkey"},
                                 update("c_custkey", column("o_custkey"), scan("orders")))));

    plan = ir::reduce_duplicate_distinct_columns(std::move(plan));

    REQUIRE(dedup_columns(*plan) == std::vector<std::string>{"c_custkey"});
    // The duplicate is restored above the dedup, in the original order.
    REQUIRE(output_columns(*plan) == std::vector<std::string>{"c_custkey", "o_custkey"});
}

TEST_CASE("value identity follows copied columns through a distinct") {
    auto plan = distinct(project({"c_custkey", "o_custkey"},
                                 update("c_custkey", column("o_custkey"), scan("orders"))));

    CHECK(ir::columns_have_same_value(*plan, "c_custkey", "o_custkey"));
    CHECK_FALSE(ir::columns_have_same_value(*plan, "c_custkey", "missing"));
}

TEST_CASE("distinct key reduction leaves genuinely different columns alone") {
    auto plan = distinct(project({"o_custkey", "o_orderkey"}, scan("orders")));

    plan = ir::reduce_duplicate_distinct_columns(std::move(plan));

    REQUIRE(dedup_columns(*plan) == std::vector<std::string>{"o_custkey", "o_orderkey"});
    // Nothing to restore, so the dedup is still the root.
    REQUIRE(plan->kind() == ir::NodeKind::Distinct);
}

TEST_CASE("distinct key reduction does not treat a computed column as a copy") {
    // `total = price` would be a copy; `total = price + price` is a new value,
    // and deduping on one of the two would merge rows that differ.
    ir::BinaryExpr sum;
    sum.op = ir::ArithmeticOp::Add;
    sum.left = ir::ExprPtr{column("price")};
    sum.right = ir::ExprPtr{column("price")};

    auto plan = distinct(
        project({"price", "total"}, update("total", ir::Expr{.node = std::move(sum)}, scan("t"))));

    plan = ir::reduce_duplicate_distinct_columns(std::move(plan));

    REQUIRE(dedup_columns(*plan) == std::vector<std::string>{"price", "total"});
}

TEST_CASE("distinct key reduction does not follow a guarded update") {
    // `where <p> update { b = a }` writes b on some rows and leaves the old
    // value on the others, so b is not a copy of a.
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{.alias = "b", .expr = column("a")});
    auto guarded = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields));
    guarded->set_guard(column("keep"));
    guarded->add_child(scan("t"));

    auto plan = distinct(project({"a", "b"}, std::move(guarded)));

    plan = ir::reduce_duplicate_distinct_columns(std::move(plan));

    REQUIRE(dedup_columns(*plan) == std::vector<std::string>{"a", "b"});
}

TEST_CASE("distinct key reduction stops at a join rather than trusting the name") {
    // Both sides of a self-join name the same base column while pairing
    // DIFFERENT rows, so a shared origin proves nothing. The walk stops at the
    // join, which makes the two columns distinct identities.
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{7}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{ir::JoinKey{"o_custkey"}});
    join->add_child(scan("orders"));
    join->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{8}, "orders_2"));

    auto plan = distinct(project({"o_totalprice", "o_totalprice_right"}, std::move(join)));

    plan = ir::reduce_duplicate_distinct_columns(std::move(plan));

    REQUIRE(dedup_columns(*plan) == std::vector<std::string>{"o_totalprice", "o_totalprice_right"});
}
