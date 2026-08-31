// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/count_distinct_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

/// `Aggregate(count() as c, by K) <- Distinct <- Project(cols) <- Scan("t")`.
auto build_idiom(const std::vector<std::string>& group_keys, const std::vector<std::string>& cols)
    -> ir::NodePtr {
    std::vector<ir::ColumnRef> proj;
    for (const auto& c : cols) {
        proj.push_back(ir::ColumnRef{.name = c});
    }
    auto project = std::make_unique<ir::ProjectNode>(ir::NodeId{1}, std::move(proj));
    project->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{0}, "t"));

    auto distinct = std::make_unique<ir::DistinctNode>(ir::NodeId{2});
    distinct->add_child(std::move(project));

    std::vector<ir::ColumnRef> keys;
    for (const auto& k : group_keys) {
        keys.push_back(ir::ColumnRef{.name = k});
    }
    auto agg = std::make_unique<ir::AggregateNode>(
        ir::NodeId{3}, std::move(keys),
        std::vector<ir::AggSpec>{ir::AggSpec{.func = ir::AggFunc::Count, .column = {}, .alias = "c"}});
    agg->add_child(std::move(distinct));
    return agg;
}

auto sources_with(ir::Nullability v_nulls) -> ir::SourceSchemas {
    ir::SourceSchemas s;
    s["t"] = ir::SchemaInfo::known(
        {{.name = "brand", .type = ir::ColumnType::String, .nulls = ir::Nullability::Maybe},
         {.name = "size", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Maybe},
         {.name = "suppkey", .type = ir::ColumnType::Int64, .nulls = v_nulls}});
    return s;
}

}  // namespace

TEST_CASE("count_distinct_reduction: fuses distinct+count on a non-null value column",
          "[ir][count_distinct]") {
    auto tree = build_idiom({"brand", "size"}, {"brand", "size", "suppkey"});
    auto out = ir::fuse_distinct_count_to_count_distinct(std::move(tree),
                                                        sources_with(ir::Nullability::Never));

    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    const auto& agg = ir::node_cast<ir::AggregateNode>(*out);
    REQUIRE(agg.aggregations().size() == 1);
    CHECK(agg.aggregations()[0].func == ir::AggFunc::CountDistinct);
    CHECK(agg.aggregations()[0].column.name == "suppkey");
    CHECK(agg.aggregations()[0].alias == "c");
    REQUIRE(agg.group_by().size() == 2);
    CHECK(agg.group_by()[0].name == "brand");
    // The Distinct is gone; the Project is now the aggregate's direct child.
    REQUIRE(agg.children().size() == 1);
    CHECK(agg.children()[0]->kind() == ir::NodeKind::Project);
}

TEST_CASE("count_distinct_reduction: declines when the value column may be null",
          "[ir][count_distinct]") {
    auto tree = build_idiom({"brand", "size"}, {"brand", "size", "suppkey"});
    auto out = ir::fuse_distinct_count_to_count_distinct(std::move(tree),
                                                        sources_with(ir::Nullability::Maybe));
    // Unchanged: still an Aggregate over a Distinct.
    REQUIRE(out->kind() == ir::NodeKind::Aggregate);
    CHECK(ir::node_cast<ir::AggregateNode>(*out).aggregations()[0].func == ir::AggFunc::Count);
    CHECK(out->children()[0]->kind() == ir::NodeKind::Distinct);
}

TEST_CASE("count_distinct_reduction: declines when the projection has more than one extra column",
          "[ir][count_distinct]") {
    // distinct over {brand, size, suppkey, extra} but grouped only by {brand, size}
    // -> two non-key columns, not a count(distinct one_thing).
    auto tree = build_idiom({"brand", "size"}, {"brand", "size", "suppkey", "extra"});
    auto out = ir::fuse_distinct_count_to_count_distinct(std::move(tree),
                                                        sources_with(ir::Nullability::Never));
    CHECK(ir::node_cast<ir::AggregateNode>(*out).aggregations()[0].func == ir::AggFunc::Count);
    CHECK(out->children()[0]->kind() == ir::NodeKind::Distinct);
}

TEST_CASE("count_distinct_reduction: declines count(col), only bare count() qualifies",
          "[ir][count_distinct]") {
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{1}, std::vector<ir::ColumnRef>{{.name = "brand"}, {.name = "suppkey"}});
    project->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{0}, "t"));
    auto distinct = std::make_unique<ir::DistinctNode>(ir::NodeId{2});
    distinct->add_child(std::move(project));
    auto agg = std::make_unique<ir::AggregateNode>(
        ir::NodeId{3}, std::vector<ir::ColumnRef>{{.name = "brand"}},
        std::vector<ir::AggSpec>{
            ir::AggSpec{.func = ir::AggFunc::Count, .column = {.name = "suppkey"}, .alias = "c"}});
    agg->add_child(std::move(distinct));

    auto out = ir::fuse_distinct_count_to_count_distinct(std::move(agg),
                                                        sources_with(ir::Nullability::Never));
    CHECK(ir::node_cast<ir::AggregateNode>(*out).aggregations()[0].func == ir::AggFunc::Count);
    CHECK(out->children()[0]->kind() == ir::NodeKind::Distinct);
}
