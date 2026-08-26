// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_origins.hpp>
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

auto schema_of(std::vector<std::string> names) -> ir::SchemaInfo {
    std::vector<ir::SchemaField> fields;
    fields.reserve(names.size());
    for (auto& name : names) {
        fields.push_back(ir::SchemaField{.name = std::move(name), .type = std::nullopt});
    }
    return ir::SchemaInfo::known(std::move(fields));
}

auto col(std::string name) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}});
}

auto refs(std::vector<std::string> names) -> std::vector<ir::ColumnRef> {
    std::vector<ir::ColumnRef> out;
    out.reserve(names.size());
    for (auto& name : names) {
        out.push_back(ir::ColumnRef{.name = std::move(name), .source = {0}});
    }
    return out;
}

/// Assert `column` of `node`'s output came from `source`.`origin`.
void expect_origin(const ir::Node& node, const ir::SourceSchemas& sources,
                   const std::string& column, const std::string& source,
                   const std::string& origin) {
    const auto found = ir::column_origin_of(node, column, sources);
    REQUIRE(found.has_value());
    CHECK(found->source == source);
    CHECK(found->column == origin);
}

}  // namespace

TEST_CASE("column_origins seeds a scan from its source schema") {
    ir::SourceSchemas sources;
    sources.emplace("customer", schema_of({"c_custkey", "c_name"}));

    auto node = scan("customer");
    const auto origins = ir::column_origins(*node, sources);
    CHECK(origins.size() == 2);
    expect_origin(*node, sources, "c_custkey", "customer", "c_custkey");
    expect_origin(*node, sources, "c_name", "customer", "c_name");
}

TEST_CASE("column_origins follows a rename, which a name comparison cannot") {
    ir::SourceSchemas sources;
    sources.emplace("nation", schema_of({"n_nationkey", "n_name"}));

    // The PDS-H idiom: `select { c_nationkey = n_nationkey }`. The output name
    // no longer resembles the source column at all.
    std::vector<ir::RenameSpec> renames{
        ir::RenameSpec{.new_name = "c_nationkey", .old_name = "n_nationkey"}};
    auto node = std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames));
    node->add_child(scan("nation"));

    expect_origin(*node, sources, "c_nationkey", "nation", "n_nationkey");
    CHECK_FALSE(ir::column_origin_of(*node, "n_nationkey", sources).has_value());
}

TEST_CASE("column_origins applies a multi-rename simultaneously") {
    ir::SourceSchemas sources;
    sources.emplace("t", schema_of({"a", "b"}));

    // Read every mapping against the input schema: a -> b and b -> c. This
    // hand-built map violates the surface collision rule, but still pins the
    // mapping primitive's simultaneous semantics for every IR consumer.
    std::vector<ir::RenameSpec> renames{ir::RenameSpec{.new_name = "b", .old_name = "a"},
                                        ir::RenameSpec{.new_name = "c", .old_name = "b"}};
    auto node = std::make_unique<ir::RenameNode>(ir::NodeId{2}, std::move(renames));
    node->add_child(scan("t"));

    expect_origin(*node, sources, "b", "t", "a");
    expect_origin(*node, sources, "c", "t", "b");
    CHECK_FALSE(ir::column_origin_of(*node, "a", sources).has_value());
}

TEST_CASE("column_origins keeps a copy but drops a computed column") {
    ir::SourceSchemas sources;
    sources.emplace("t", schema_of({"a", "b"}));

    std::vector<ir::FieldSpec> fields;
    // A bare column reference is a copy, so it is still that base column.
    fields.push_back(
        ir::FieldSpec{.alias = "copy", .expr = ir::Expr{.node = ir::ColumnRef{.name = "a"}}});
    // `a + b` is a new value however it was computed.
    fields.push_back(ir::FieldSpec{
        .alias = "sum",
        .expr = ir::Expr{.node = ir::BinaryExpr{
                             .op = ir::ArithmeticOp::Add, .left = col("a"), .right = col("b")}}});
    auto node = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(fields));
    node->add_child(scan("t"));

    expect_origin(*node, sources, "copy", "t", "a");
    expect_origin(*node, sources, "a", "t", "a");
    CHECK_FALSE(ir::column_origin_of(*node, "sum", sources).has_value());
}

TEST_CASE("column_origins traces both sides through a join") {
    ir::SourceSchemas sources;
    sources.emplace("customer", schema_of({"c_custkey", "c_name"}));
    sources.emplace("nation", schema_of({"c_nationkey", "n_name"}));

    std::vector<ir::JoinKey> keys{
        ir::JoinKey{std::string{"c_custkey"}, std::string{"c_nationkey"}}};
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{3}, ir::JoinKind::Inner, std::move(keys));
    join->add_child(scan("customer"));
    join->add_child(scan("nation"));

    // A column keeps its origin across the join, and the two sides stay
    // distinguishable even though the plan holds both.
    expect_origin(*join, sources, "c_name", "customer", "c_name");
    expect_origin(*join, sources, "n_name", "nation", "n_name");
}

TEST_CASE("column_origins keeps a group key's origin and drops the aggregates") {
    ir::SourceSchemas sources;
    sources.emplace("t", schema_of({"k", "v"}));

    std::vector<ir::ColumnRef> group = refs({"k"});
    std::vector<ir::AggSpec> aggs{ir::AggSpec{
        .func = ir::AggFunc::Sum, .column = ir::ColumnRef{.name = "v"}, .alias = "total"}};
    auto node =
        std::make_unique<ir::AggregateNode>(ir::NodeId{2}, std::move(group), std::move(aggs));
    node->add_child(scan("t"));

    // The key column holds one of the values that grouped, so it is still that
    // base column; the aggregate is computed and is not.
    expect_origin(*node, sources, "k", "t", "k");
    CHECK_FALSE(ir::column_origin_of(*node, "total", sources).has_value());
}

TEST_CASE("column_origins reports nothing for an unknown source") {
    auto node = scan("mystery");
    CHECK(ir::column_origins(*node, {}).empty());
}
