// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/group_key_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto scan(std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(ir::NodeId{1}, std::move(name));
}

/// A source schema, optionally with `unique` proved unique.
auto source(std::vector<std::string> names, const std::string& unique = {}) -> ir::SchemaInfo {
    std::vector<ir::SchemaField> fields;
    fields.reserve(names.size());
    for (auto& name : names) {
        fields.push_back(ir::SchemaField{.name = std::move(name), .type = std::nullopt});
    }
    auto info = ir::SchemaInfo::known(std::move(fields));
    if (!unique.empty()) {
        info.add_unique_key(ir::UniqueKey{unique});
    }
    return info;
}

auto refs(std::vector<std::string> names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> out;
    out.reserve(names.size());
    for (auto& name : names) {
        out.push_back(ir::ColumnRef{.name = std::move(name), .source = {0}});
    }
    return out;
}

auto sum_of(std::string column, std::string alias) -> std::vector<ir::AggSpec> {
    return {ir::AggSpec{.func = ir::AggFunc::Sum,
                        .column = ir::ColumnRef{.name = std::move(column)},
                        .alias = std::move(alias)}};
}

auto aggregate(std::vector<std::string> keys, ir::NodePtr input) -> ir::NodePtr {
    auto node = std::make_unique<ir::AggregateNode>(ir::NodeId{9}, refs(std::move(keys)),
                                                    sum_of("v", "total"));
    node->add_child(std::move(input));
    return node;
}

/// The AggregateNode in `plan`, wherever the pass left it.
auto find_aggregate(const ir::Node& plan) -> const ir::AggregateNode* {
    if (plan.kind() == ir::NodeKind::Aggregate) {
        return static_cast<const ir::AggregateNode*>(&plan);
    }
    for (const auto& child : plan.children()) {
        if (child != nullptr) {
            if (const auto* found = find_aggregate(*child)) {
                return found;
            }
        }
    }
    return nullptr;
}

}  // namespace

TEST_CASE("group key reduction drops keys a unique key determines") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_phone", "v"}, "c_custkey"));

    auto plan = aggregate({"c_custkey", "c_name", "c_phone"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_custkey");
    // The dropped keys come back as `first()`, exact because they are constant
    // within a group.
    REQUIRE(agg->aggregations().size() == 3);
    CHECK(agg->aggregations()[1].func == ir::AggFunc::First);
    CHECK(agg->aggregations()[1].alias == "c_name");
}

TEST_CASE("group key reduction restores the original column order") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    // The aggregate emits keys then aggregates, so without a projection the
    // reduction would reorder the result for a positional reader.
    REQUIRE(plan->kind() == ir::NodeKind::Project);
    const auto& columns = static_cast<const ir::ProjectNode&>(*plan).columns();
    REQUIRE(columns.size() == 3);
    CHECK(columns[0].name == "c_custkey");
    CHECK(columns[1].name == "c_name");
    CHECK(columns[2].name == "total");
}

TEST_CASE("group key reduction carries a dependency across a join, transitively") {
    // q10's shape: c_custkey -> c_nationkey inside customer, then across the
    // join to n_name because n_nationkey is unique in nation.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "c_nationkey", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Inner, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_custkey", "c_name", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_custkey");
}

TEST_CASE("group key reduction declines without a proved unique key") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}));  // nothing proved

    auto plan = aggregate({"c_custkey", "c_name"}, scan("customer"));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction declines across a right join") {
    // An unmatched RIGHT row carries a null left key, so two such rows share a
    // key value while differing in the right table's columns.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_nationkey", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Right, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_custkey", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction does not carry a dependency backwards across a left join") {
    // Although c_custkey is unique, n_nationkey does not determine customer
    // columns after a left join: every unmatched customer has the same null
    // n_nationkey while its c_name may differ. Dropping c_name would merge all
    // those unmatched groups.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_custkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Left, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"n_nationkey", "c_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction carries a dependency forwards across a left join") {
    // The sound direction remains useful: one left key identifies at most one
    // unique right row, with unmatched keys consistently producing nulls.
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_nationkey", "v"}));
    sources.emplace("nation", source({"n_nationkey", "n_name"}, "n_nationkey"));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_nationkey"}, std::string{"n_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Left, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    auto plan = aggregate({"c_nationkey", "n_name"}, std::move(join));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    REQUIRE(agg->group_by().size() == 1);
    CHECK(agg->group_by().front().name == "c_nationkey");
}

TEST_CASE("group key reduction declines when a key has no traceable origin") {
    ir::SourceSchemas sources;
    sources.emplace("customer", source({"c_custkey", "c_name", "v"}, "c_custkey"));

    // A computed key has no base column, so nothing can be proved about it.
    std::vector<ir::FieldSpec> fields;
    fields.push_back(ir::FieldSpec{
        .alias = "derived",
        .expr = ir::Expr{
            .node = ir::BinaryExpr{
                .op = ir::ArithmeticOp::Add,
                .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "c_custkey"}}),
                .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "v"}})}}});
    auto update = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields));
    update->add_child(scan("customer"));

    auto plan = aggregate({"c_custkey", "derived"}, std::move(update));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    REQUIRE(plan->kind() == ir::NodeKind::Aggregate);
    CHECK(static_cast<const ir::AggregateNode&>(*plan).group_by().size() == 2);
}

TEST_CASE("group key reduction never reduces to no key at all") {
    // Two keys that are the same base column: one is redundant, but dropping
    // both would turn a grouped aggregate into an ungrouped one.
    ir::SourceSchemas sources;
    sources.emplace("t", source({"a", "v"}, "a"));

    std::vector<ir::RenameSpec> renames{ir::RenameSpec{.new_name = "b", .old_name = "a"}};
    auto rename = std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames));
    rename->add_child(scan("t"));

    auto plan = aggregate({"b"}, std::move(rename));
    plan = ir::reduce_functionally_dependent_group_keys(std::move(plan), sources);

    const auto* agg = find_aggregate(*plan);
    REQUIRE(agg != nullptr);
    CHECK(agg->group_by().size() == 1);
}
