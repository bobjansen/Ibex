// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/null_flag_aggregates.hpp>
#include <ibex/ir/schema.hpp>

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

namespace {

/// `t(g, x)`; `x` proved null-free when `x_never_null`.
auto sources(bool x_never_null) -> ir::SourceSchemas {
    ir::SourceSchemas out;
    out.emplace(
        "t", ir::SchemaInfo::known({
                 ir::SchemaField{
                     .name = "g", .type = ir::ColumnType::Int64, .nulls = ir::Nullability::Never},
                 ir::SchemaField{
                     .name = "x",
                     .type = ir::ColumnType::Int64,
                     .nulls = x_never_null ? ir::Nullability::Never : ir::Nullability::Maybe},
             }));
    return out;
}

auto col(std::string name) -> ir::Expr {
    return ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}};
}
auto int_lit(std::int64_t v) -> ir::Expr {
    return ir::Expr{.node = ir::Literal{.value = v}};
}
auto call(std::string callee, std::vector<ir::Expr> args) -> ir::Expr {
    ir::CallExpr out{.callee = std::move(callee), .args = {}, .named_args = {}};
    for (auto& arg : args) {
        out.args.emplace_back(std::move(arg));
    }
    return ir::Expr{.node = std::move(out)};
}
auto null_flag(std::string name) -> ir::Expr {
    return call("Int64", {ir::Expr{.node = ir::IsNullExpr{.operand = ir::ExprPtr{col(name)}}}});
}
auto add(ir::Expr l, ir::Expr r) -> ir::Expr {
    return ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Add,
                                           .left = ir::ExprPtr{std::move(l)},
                                           .right = ir::ExprPtr{std::move(r)}}};
}

/// What lowering `select { n = count_distinct(x) + coalesce(max(<flag>), 0) }`
/// builds: Project(Update(Aggregate(Update(scan t)))).
/// `pruned` puts the projection column pruning adds between the aggregate and
/// the pre-aggregate update.
auto n_unique_plan(ir::Expr flag, bool grouped = true, bool pruned = false) -> ir::NodePtr {
    std::vector<ir::FieldSpec> pre_fields;
    pre_fields.push_back(ir::FieldSpec{.alias = "_agg2", .expr = std::move(flag)});
    ir::NodePtr pre = std::make_unique<ir::UpdateNode>(ir::NodeId{2}, std::move(pre_fields));
    pre->add_child(std::make_unique<ir::ScanNode>(ir::NodeId{1}, "t"));
    if (pruned) {
        auto project = std::make_unique<ir::ProjectNode>(
            ir::NodeId{6},
            std::vector<ir::ColumnRef>{ir::ColumnRef{.name = "g"}, ir::ColumnRef{.name = "x"},
                                       ir::ColumnRef{.name = "_agg2"}});
        project->add_child(std::move(pre));
        pre = std::move(project);
    }

    std::vector<ir::ColumnRef> group_by;
    if (grouped) {
        group_by.push_back(ir::ColumnRef{.name = "g"});
    }
    std::vector<ir::AggSpec> aggs;
    aggs.push_back(
        ir::AggSpec{.func = ir::AggFunc::CountDistinct, .column = {.name = "x"}, .alias = "_agg0"});
    aggs.push_back(
        ir::AggSpec{.func = ir::AggFunc::Max, .column = {.name = "_agg2"}, .alias = "_agg1"});
    auto aggregate =
        std::make_unique<ir::AggregateNode>(ir::NodeId{3}, std::move(group_by), std::move(aggs));
    aggregate->add_child(std::move(pre));

    std::vector<ir::FieldSpec> post_fields;
    post_fields.push_back(ir::FieldSpec{
        .alias = "n", .expr = add(col("_agg0"), call("coalesce", {col("_agg1"), int_lit(0)}))});
    auto post = std::make_unique<ir::UpdateNode>(ir::NodeId{4}, std::move(post_fields));
    post->add_child(std::move(aggregate));

    std::vector<ir::ColumnRef> columns{ir::ColumnRef{.name = "g"}, ir::ColumnRef{.name = "n"}};
    auto project = std::make_unique<ir::ProjectNode>(ir::NodeId{5}, std::move(columns));
    project->add_child(std::move(post));
    return project;
}

auto aggregate_of(const ir::Node& project) -> const ir::AggregateNode& {
    return ir::node_cast<ir::AggregateNode>(*project.children()[0]->children()[0]);
}

}  // namespace

TEST_CASE("null flag aggregates: max of a never-null flag folds to 0",
          "[ir][null-flag-aggregates]") {
    auto root =
        ir::fold_constant_null_flag_aggregates(n_unique_plan(null_flag("x")), sources(true));
    const auto& aggregate = aggregate_of(*root);
    REQUIRE(aggregate.aggregations().size() == 1);
    CHECK(aggregate.aggregations()[0].func == ir::AggFunc::CountDistinct);
    // The flag is no longer computed: the pre-aggregate update had only it.
    CHECK(aggregate.children()[0]->kind() == ir::NodeKind::Scan);
    // `coalesce(_agg1, 0)` reads the literal 0 instead.
    const auto& post = ir::node_cast<ir::UpdateNode>(*root->children()[0]);
    const auto n = std::ranges::find_if(
        post.fields(), [](const ir::FieldSpec& field) { return field.alias == "n"; });
    REQUIRE(n != post.fields().end());
    const auto& sum = std::get<ir::BinaryExpr>(n->expr.node);
    const auto& coalesce = std::get<ir::CallExpr>(sum.right->node);
    const auto* zero = std::get_if<ir::Literal>(&coalesce.args[0]->node);
    REQUIRE(zero != nullptr);
    CHECK(std::get<std::int64_t>(zero->value) == 0);
}

TEST_CASE("null flag aggregates: a case of flags and zeros folds too",
          "[ir][null-flag-aggregates]") {
    // The filtered `n_unique` shape: `case { p => Int64(is_null(x)), else => 0 }`.
    auto flag =
        call("__case", {ir::Expr{.node = ir::Literal{.value = true}}, null_flag("x"), int_lit(0)});
    auto root =
        ir::fold_constant_null_flag_aggregates(n_unique_plan(std::move(flag)), sources(true));
    CHECK(aggregate_of(*root).aggregations().size() == 1);
}

TEST_CASE("null flag aggregates: declined when the column may be null",
          "[ir][null-flag-aggregates]") {
    auto root =
        ir::fold_constant_null_flag_aggregates(n_unique_plan(null_flag("x")), sources(false));
    CHECK(aggregate_of(*root).aggregations().size() == 2);
}

TEST_CASE("null flag aggregates: declined for an ungrouped aggregate",
          "[ir][null-flag-aggregates]") {
    // Over no rows an ungrouped aggregate still emits a row, whose max is null.
    auto root =
        ir::fold_constant_null_flag_aggregates(n_unique_plan(null_flag("x"), false), sources(true));
    CHECK(aggregate_of(*root).aggregations().size() == 2);
}

TEST_CASE("null flag aggregates: the folded column stays, as the literal 0",
          "[ir][null-flag-aggregates]") {
    // Something above the update may still read the aggregate's column, so
    // the update assigns it the value it always held.
    auto root =
        ir::fold_constant_null_flag_aggregates(n_unique_plan(null_flag("x")), sources(true));
    const auto& post = ir::node_cast<ir::UpdateNode>(*root->children()[0]);
    const auto found = std::ranges::find_if(
        post.fields(), [](const ir::FieldSpec& field) { return field.alias == "_agg1"; });
    REQUIRE(found != post.fields().end());
    const auto* zero = std::get_if<ir::Literal>(&found->expr.node);
    REQUIRE(zero != nullptr);
    CHECK(std::get<std::int64_t>(zero->value) == 0);
}

TEST_CASE("null flag aggregates: crosses a pruning projection and drops the flag from it",
          "[ir][null-flag-aggregates]") {
    auto root = ir::fold_constant_null_flag_aggregates(n_unique_plan(null_flag("x"), true, true),
                                                       sources(true));
    const auto& aggregate = aggregate_of(*root);
    REQUIRE(aggregate.aggregations().size() == 1);
    const auto& project = ir::node_cast<ir::ProjectNode>(*aggregate.children()[0]);
    CHECK(std::ranges::none_of(project.columns(),
                               [](const ir::ColumnRef& column) { return column.name == "_agg2"; }));
    CHECK(project.children()[0]->kind() == ir::NodeKind::Scan);
}
