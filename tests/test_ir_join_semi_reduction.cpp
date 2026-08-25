// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_semi_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace ibex;

namespace {

auto make_scan(ir::NodeId id, std::string name) -> ir::NodePtr {
    return std::make_unique<ir::ScanNode>(id, std::move(name));
}

/// `lines` is the fact table; `counts` is a grouped aggregate over it, so its
/// schema is unique on `k` by construction -- the q21 shape this pass exists
/// for, reduced to the two nodes that matter.
auto counts_by_k(ir::NodeId scan_id, ir::NodeId agg_id) -> ir::NodePtr {
    auto agg = std::make_unique<ir::AggregateNode>(
        agg_id, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "k"}},
        std::vector<ir::AggSpec>{
            ir::AggSpec{.func = ir::AggFunc::Count, .column = {}, .alias = "n"}});
    agg->add_child(make_scan(scan_id, "lines"));
    return agg;
}

auto sources() -> ir::SourceSchemas {
    ir::SourceSchemas s;
    s["lines"] = ir::SchemaInfo::known({{.name = "k"}, {.name = "v"}});
    s["other"] = ir::SchemaInfo::known({{.name = "k"}, {.name = "w"}});
    return s;
}

/// Inner join of `lines` (left) to the unique aggregate (right), with a project
/// above naming which columns the plan actually reads.
auto plan(std::vector<std::string> projected, bool swap_sides = false) -> ir::NodePtr {
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{10}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{ir::JoinKey{"k"}});
    if (swap_sides) {
        join->add_child(counts_by_k(ir::NodeId{1}, ir::NodeId{2}));
        join->add_child(make_scan(ir::NodeId{3}, "lines"));
    } else {
        join->add_child(make_scan(ir::NodeId{3}, "lines"));
        join->add_child(counts_by_k(ir::NodeId{1}, ir::NodeId{2}));
    }
    std::vector<ir::ColumnRef> cols;
    cols.reserve(projected.size());
    for (auto& name : projected) {
        cols.push_back(ir::ColumnRef{.name = std::move(name)});
    }
    auto project = std::make_unique<ir::ProjectNode>(ir::NodeId{11}, std::move(cols));
    project->add_child(std::move(join));
    return project;
}

auto join_of(const ir::Node& root) -> const ir::JoinNode& {
    REQUIRE(root.children().size() == 1);
    REQUIRE(root.children()[0]->kind() == ir::NodeKind::Join);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return static_cast<const ir::JoinNode&>(*root.children()[0]);
}

}  // namespace

TEST_CASE("An unread unique side reduces the join to a semi join", "[ir][join][semi]") {
    // Nothing above reads `n`, and the aggregate is unique on `k`, so the right
    // side contributes no column and cannot multiply rows.
    auto out = ir::reduce_inner_joins_to_semi(plan({"k", "v"}), sources());
    const auto& join = join_of(*out);
    CHECK(join.kind() == ir::JoinKind::Semi);
    CHECK(join.keys().size() == 1);
    CHECK(join.keys()[0].left == "k");
    CHECK(join.children()[0]->kind() == ir::NodeKind::Scan);
}

TEST_CASE("Reading the unique side's column keeps the inner join", "[ir][join][semi]") {
    // `n` lives only on the aggregate side, so dropping it would lose a column
    // the project asks for.
    auto out = ir::reduce_inner_joins_to_semi(plan({"k", "v", "n"}), sources());
    CHECK(join_of(*out).kind() == ir::JoinKind::Inner);
}

TEST_CASE("A shared name does not block the reduction", "[ir][join][semi]") {
    // `k` is on both sides. Above the join it resolves to the retained side and
    // is equal on surviving rows, so demanding it proves nothing about the side
    // being dropped.
    auto out = ir::reduce_inner_joins_to_semi(plan({"k"}), sources());
    CHECK(join_of(*out).kind() == ir::JoinKind::Semi);
}

TEST_CASE("The unique side may be the left one", "[ir][join][semi]") {
    // q21 as written: the aggregate is the LEFT child. Reducing keeps the fact
    // table, so the retained side moves and the keys travel with it.
    auto out = ir::reduce_inner_joins_to_semi(plan({"k", "v"}, /*swap_sides=*/true), sources());
    const auto& join = join_of(*out);
    CHECK(join.kind() == ir::JoinKind::Semi);
    REQUIRE(join.children().size() == 2);
    CHECK(join.children()[0]->kind() == ir::NodeKind::Scan);
    CHECK(join.children()[1]->kind() == ir::NodeKind::Aggregate);
}

TEST_CASE("A non-unique side keeps the inner join", "[ir][join][semi]") {
    // `other` is a bare scan: nothing proves one row per `k`, so a semi join
    // would emit fewer rows than the inner join it replaced.
    auto join = std::make_unique<ir::JoinNode>(ir::NodeId{10}, ir::JoinKind::Inner,
                                               std::vector<ir::JoinKey>{ir::JoinKey{"k"}});
    join->add_child(make_scan(ir::NodeId{3}, "lines"));
    join->add_child(make_scan(ir::NodeId{4}, "other"));
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{11}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "v"}});
    project->add_child(std::move(join));

    auto out = ir::reduce_inner_joins_to_semi(std::move(project), sources());
    CHECK(join_of(*out).kind() == ir::JoinKind::Inner);
}

TEST_CASE("A declared cardinality keeps the inner join", "[ir][join][semi]") {
    // `expect` is checked against the pairs actually emitted; a semi join emits
    // a different set, so the assertion would no longer mean what was written.
    auto join = std::make_unique<ir::JoinNode>(
        ir::NodeId{10}, ir::JoinKind::Inner, std::vector<ir::JoinKey>{ir::JoinKey{"k"}},
        std::nullopt, ir::JoinSuffixPolicy{}, ir::NullMatch::Never,
        ir::JoinExpect{.left = ir::JoinMultiplicity::Many, .right = ir::JoinMultiplicity::One});
    join->add_child(make_scan(ir::NodeId{3}, "lines"));
    join->add_child(counts_by_k(ir::NodeId{1}, ir::NodeId{2}));
    auto project = std::make_unique<ir::ProjectNode>(
        ir::NodeId{11}, std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "v"}});
    project->add_child(std::move(join));

    auto out = ir::reduce_inner_joins_to_semi(std::move(project), sources());
    CHECK(join_of(*out).kind() == ir::JoinKind::Inner);
}
