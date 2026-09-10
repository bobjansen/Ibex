// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/builder.hpp>
#include <ibex/ir/extern_sources.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/required_columns.hpp>

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <utility>
#include <vector>

using namespace ibex;

TEST_CASE("extern source hoisting coalesces repeated literal readers", "[ir][extern_sources]") {
    ir::Builder builder;
    std::vector<ir::Expr> args;
    args.push_back(ir::Expr{.node = ir::Literal{.value = std::string{"lineitem.parquet"}}});
    auto left = builder.extern_call("read_parquet", args);
    auto right = builder.extern_call("read_parquet", args);
    auto join = builder.join(ir::JoinKind::Inner, {"l_orderkey"});
    join->add_child(std::move(left));
    join->add_child(std::move(right));
    auto project = builder.project({ir::ColumnRef{.name = "l_orderkey"}});
    project->add_child(std::move(join));

    auto [plan, sources] = ir::hoist_extern_sources(std::move(project), {"read_parquet"});
    REQUIRE(sources.size() == 1);
    CHECK(sources[0].callee == "read_parquet");

    const auto& children = plan->children().front()->children();
    REQUIRE(children.size() == 2);
    const auto* lhs = dynamic_cast<const ir::ScanNode*>(children[0].get());
    const auto* rhs = dynamic_cast<const ir::ScanNode*>(children[1].get());
    REQUIRE(lhs != nullptr);
    REQUIRE(rhs != nullptr);
    CHECK(lhs->source_name() == rhs->source_name());

    auto demand = ir::required_columns(*plan);
    REQUIRE(demand.size() == 1);
    CHECK(demand.begin()->second.names == std::set<std::string>{"l_orderkey"});
}

TEST_CASE("extern source hoisting gives dynamic readers distinct sources", "[ir][extern_sources]") {
    ir::Builder builder;
    auto left =
        builder.extern_call("read_parquet", {ir::Expr{.node = ir::ColumnRef{.name = "path"}}});
    auto right =
        builder.extern_call("read_parquet", {ir::Expr{.node = ir::ColumnRef{.name = "path"}}});
    auto plan = builder.join(ir::JoinKind::Cross, {});
    plan->add_child(std::move(left));
    plan->add_child(std::move(right));

    auto [rewritten, sources] = ir::hoist_extern_sources(std::move(plan), {"read_parquet"});
    REQUIRE(sources.size() == 2);
    CHECK(sources[0].callee == "read_parquet");
    CHECK(sources[1].callee == "read_parquet");
    REQUIRE(sources[0].args.size() == 1);
    REQUIRE(sources[1].args.size() == 1);
    REQUIRE(rewritten->children().size() == 2);
    const auto& lhs = ir::node_cast<ir::ScanNode>(*rewritten->children()[0]);
    const auto& rhs = ir::node_cast<ir::ScanNode>(*rewritten->children()[1]);
    CHECK(lhs.source_name() != rhs.source_name());
}
