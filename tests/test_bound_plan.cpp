// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Binding a lowered plan's `^name` references to literals for one execution
// (runtime::bind_plan_constants). The REPL's session tests check the bound
// plans still mean what they meant; these check what is bound and what is not.

#include <ibex/core/scalar_value.hpp>
#include <ibex/ir/builder.hpp>
#include <ibex/ir/canonicalize.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

using namespace ibex;

namespace {

auto ref(std::string name, bool lexical) -> ir::ExprPtr {
    return ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = std::move(name), .lexical = lexical}});
}

auto greater(std::string column, ir::ExprPtr right) -> ir::Expr {
    return ir::Expr{.node = ir::CompareExpr{
                        .op = ir::CompareOp::Gt, .left = ref(std::move(column), false), .right = std::move(right)}};
}

auto literal_of(const ir::ExprPtr& expr) -> const ir::Literal* {
    return expr == nullptr ? nullptr : std::get_if<ir::Literal>(&expr->node);
}

auto ref_of(const ir::ExprPtr& expr) -> const ir::ColumnRef* {
    return expr == nullptr ? nullptr : std::get_if<ir::ColumnRef>(&expr->node);
}

auto right_of(const ir::Expr& expr) -> const ir::ExprPtr& {
    return std::get<ir::CompareExpr>(expr.node).right;
}

}  // namespace

TEST_CASE("bind_plan_constants binds lexical references at every bindable site",
          "[runtime][bound_plan]") {
    ir::Builder b;
    auto scan = b.scan("t");
    auto filter = b.filter(greater("x", ref("low", true)));
    filter->add_child(std::move(scan));

    std::vector<ir::FieldSpec> fields;
    fields.push_back({.alias = "y",
                      .expr = ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Add,
                                                              .left = ref("x", false),
                                                              .right = ref("step", true)}}});
    auto update = b.update(std::move(fields));
    update->set_guard(greater("x", ref("low", true)));
    update->add_child(std::move(filter));

    auto head = b.head(ir::Expr{.node = ir::ColumnRef{.name = "n", .lexical = true}});
    head->add_child(std::move(update));
    const auto head_id = head->id();

    runtime::ScalarRegistry scalars;
    scalars.insert_or_assign("low", std::int64_t{5});
    scalars.insert_or_assign("step", 0.5);
    scalars.insert_or_assign("n", std::int64_t{3});

    auto bound = runtime::bind_plan_constants(std::move(head), scalars);
    REQUIRE(bound != nullptr);
    REQUIRE(bound->kind() == ir::NodeKind::Head);
    CHECK(bound->id() == head_id);
    CHECK(ir::node_cast<ir::HeadNode>(*bound).count_literal() == std::optional<std::size_t>{3});

    REQUIRE(bound->children().size() == 1);
    const auto& bound_update = ir::node_cast<ir::UpdateNode>(*bound->children().front());
    const auto& y = std::get<ir::BinaryExpr>(bound_update.fields().front().expr.node);
    REQUIRE(literal_of(y.right) != nullptr);
    CHECK(std::get<double>(literal_of(y.right)->value) == 0.5);
    REQUIRE(bound_update.guard() != nullptr);
    REQUIRE(literal_of(right_of(*bound_update.guard())) != nullptr);

    REQUIRE(bound_update.children().size() == 1);
    const auto& bound_filter = ir::node_cast<ir::FilterNode>(*bound_update.children().front());
    const auto* low = literal_of(right_of(bound_filter.predicate()));
    REQUIRE(low != nullptr);
    CHECK(std::get<std::int64_t>(low->value) == 5);
    REQUIRE(bound_filter.children().size() == 1);
    CHECK(bound_filter.children().front()->kind() == ir::NodeKind::Scan);
}

TEST_CASE("bind_plan_constants leaves names it cannot prove are bindings",
          "[runtime][bound_plan]") {
    ir::Builder b;
    // `x > low`: a bare name may be a column of the operand, so it stays even
    // though a scalar of that name exists. `x > ^missing` names no binding.
    auto bare = b.filter(greater("x", ref("low", false)));
    bare->add_child(b.scan("t"));
    auto missing = b.filter(greater("x", ref("missing", true)));
    missing->add_child(std::move(bare));

    runtime::ScalarRegistry scalars;
    scalars.insert_or_assign("low", std::int64_t{5});
    auto bound = runtime::bind_plan_constants(std::move(missing), scalars);

    const auto* outer = ref_of(right_of(ir::node_cast<ir::FilterNode>(*bound).predicate()));
    REQUIRE(outer != nullptr);
    CHECK(outer->name == "missing");
    CHECK(outer->lexical);
    const auto& inner = ir::node_cast<ir::FilterNode>(*bound->children().front());
    const auto* bare_ref = ref_of(right_of(inner.predicate()));
    REQUIRE(bare_ref != nullptr);
    CHECK(bare_ref->name == "low");
    CHECK_FALSE(bare_ref->lexical);
}

TEST_CASE("bind_plan_constants keeps a binding's type, null and NaN", "[runtime][bound_plan]") {
    ir::Builder b;
    auto filter = b.filter(ir::Expr{.node = ir::LogicalExpr{.op = ir::LogicalOp::And,
                                                            .left = ir::make_expr_ptr(greater("x", ref("none", true))),
                                                            .right = ir::make_expr_ptr(greater("x", ref("nan", true)))}});
    filter->add_child(b.scan("t"));

    runtime::ScalarRegistry scalars;
    scalars.insert_or_assign("none", NullScalar{ScalarKind::Int});
    scalars.insert_or_assign("nan", std::nan(""));
    auto bound = runtime::bind_plan_constants(std::move(filter), scalars);

    const auto& both = std::get<ir::LogicalExpr>(ir::node_cast<ir::FilterNode>(*bound).predicate().node);
    const auto* none = literal_of(right_of(*both.left));
    REQUIRE(none != nullptr);
    REQUIRE(std::holds_alternative<NullScalar>(none->value));
    CHECK(std::get<NullScalar>(none->value).kind == ScalarKind::Int);
    const auto* nan = literal_of(right_of(*both.right));
    REQUIRE(nan != nullptr);
    CHECK(std::isnan(std::get<double>(nan->value)));
}

TEST_CASE("bind_plan_constants reaches join predicates and fused filter-limits",
          "[runtime][bound_plan]") {
    ir::Builder b;
    auto join = b.join(ir::JoinKind::Inner, {ir::JoinKey{"k", "k"}}, greater("x", ref("low", true)));
    join->add_child(b.scan("l"));
    join->add_child(b.scan("r"));
    auto fused = b.filter_head(greater("x", ref("low", true)), 2);
    fused->add_child(std::move(join));

    runtime::ScalarRegistry scalars;
    scalars.insert_or_assign("low", std::int64_t{5});
    auto bound = runtime::bind_plan_constants(std::move(fused), scalars);

    REQUIRE(bound->kind() == ir::NodeKind::FilterHead);
    const auto& head = ir::node_cast<ir::FilterHeadNode>(*bound);
    CHECK(head.count() == 2);
    CHECK(literal_of(right_of(head.predicate())) != nullptr);
    const auto& bound_join = ir::node_cast<ir::JoinNode>(*bound->children().front());
    REQUIRE(bound_join.predicate().has_value());
    CHECK(literal_of(right_of(*bound_join.predicate())) != nullptr);
    CHECK(bound_join.children().size() == 2);
}

TEST_CASE("a bound head count fuses with its order into a top-k", "[runtime][bound_plan]") {
    // The point of binding: canonicalize's R16 fuses Head(Order) only for a
    // literal count, so `[order v, head ^n]` used to run a full sort.
    ir::Builder b;
    auto order = b.order({ir::OrderKey{.name = "v"}});
    order->add_child(b.scan("t"));
    auto head = b.head(ir::Expr{.node = ir::ColumnRef{.name = "n", .lexical = true}});
    head->add_child(std::move(order));

    runtime::ScalarRegistry scalars;
    scalars.insert_or_assign("n", std::int64_t{2});
    auto plan = ir::canonicalize(runtime::bind_plan_constants(std::move(head), scalars));
    CHECK(plan->kind() == ir::NodeKind::TopK);
}
