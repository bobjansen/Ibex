// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/expr_predicates.hpp>

#include <catch2/catch_test_macros.hpp>

using namespace ibex;

TEST_CASE("function metadata classifies built-in execution shapes", "[ir][functions]") {
    const auto* rolling = ir::builtin_function_info("rolling_mean");
    REQUIRE(rolling != nullptr);
    CHECK(rolling->kind == ir::FnKind::Transform);
    CHECK(rolling->rolling);
    CHECK(ir::is_rolling_func("rolling_mean"));

    const auto* aggregate = ir::builtin_function_info("quantile");
    REQUIRE(aggregate != nullptr);
    CHECK(aggregate->kind == ir::FnKind::Aggregate);
    CHECK_FALSE(aggregate->rolling);
    CHECK(ir::is_aggregate_func("quantile"));

    CHECK(ir::fn_kind("rand_normal") == ir::FnKind::Generator);
    CHECK(ir::fn_kind("abs") == ir::FnKind::Scalar);
}

TEST_CASE("unknown functions are never assumed to be scalar", "[ir][functions]") {
    CHECK(ir::builtin_function_info("plugin_smooth") == nullptr);
    CHECK_FALSE(ir::fn_kind("plugin_smooth").has_value());
    CHECK_FALSE(ir::is_rolling_func("plugin_smooth"));
    CHECK_FALSE(ir::is_aggregate_func("plugin_smooth"));

    const ir::Expr plugin_call{
        .node = ir::CallExpr{.callee = "plugin_smooth", .args = {}, .named_args = {}}};
    CHECK_FALSE(ir::is_row_local_update_expr(plugin_call));
    CHECK_FALSE(ir::is_subset_evaluable_expr(plugin_call));

    const ir::Expr scalar_call{.node = ir::CallExpr{.callee = "abs", .args = {}, .named_args = {}}};
    CHECK(ir::is_row_local_update_expr(scalar_call));
    CHECK(ir::is_subset_evaluable_expr(scalar_call));
}

// Licenses cutting a group's rows at a window-bucket boundary. Anything that
// reads across buckets must be refused, because the cut has no halo -- a false
// positive here is a wrong answer, not a slow one.
TEST_CASE("bucket-local classification gates the aligned window split", "[ir][functions]") {
    auto col = [](std::string name) {
        return ir::ExprPtr{ir::Expr{.node = ir::ColumnRef{.name = std::move(name)}}};
    };
    auto call = [&](std::string callee, std::vector<ir::NamedArg> named = {}) {
        std::vector<ir::ExprPtr> args;
        args.push_back(col("v"));
        return ir::Expr{.node = ir::CallExpr{.callee = std::move(callee),
                                             .args = std::move(args),
                                             .named_args = std::move(named)}};
    };

    // Bounded below by the current bucket's start under `aligned`.
    CHECK(ir::is_bucket_local_window_expr(call("rolling_max")));
    CHECK(ir::is_bucket_local_window_expr(call("rolling_first")));
    // Row-local: reads its own row only.
    CHECK(ir::is_bucket_local_window_expr(call("abs")));
    CHECK(ir::is_bucket_local_window_expr(ir::Expr{.node = ir::ColumnRef{.name = "v"}}));
    // A pure function of the row's own timestamp, despite being a Transform.
    CHECK(ir::is_bucket_local_window_expr(
        ir::Expr{.node = ir::CallExpr{.callee = "window_start", .args = {}, .named_args = {}}}));

    // Reads the neighbouring row whatever bucket it sits in.
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("lag")));
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("cumsum")));
    // Reduces the whole slice, not one bucket.
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("quantile")));
    // Unclassifiable, so unsafe by default.
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("plugin_smooth")));

    // A per-call window override replaces the enclosing clause: a count window
    // knows nothing about the grid, and a different duration is a different
    // grid. Either way the boundaries we would cut at no longer bound the call.
    std::vector<ir::NamedArg> count_arg;
    count_arg.push_back(
        ir::NamedArg{.name = "__window_n",
                     .value = ir::ExprPtr{ir::Expr{.node = ir::Literal{std::int64_t{20}}}}});
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("rolling_max", std::move(count_arg))));

    std::vector<ir::NamedArg> ns_arg;
    ns_arg.push_back(
        ir::NamedArg{.name = "__window_ns",
                     .value = ir::ExprPtr{ir::Expr{.node = ir::Literal{std::int64_t{5000}}}}});
    CHECK_FALSE(ir::is_bucket_local_window_expr(call("rolling_max", std::move(ns_arg))));

    // Composition: a bucket-local call nested under arithmetic stays local, and
    // one bad leaf anywhere disqualifies the whole expression.
    auto binary = [&](ir::Expr lhs, ir::Expr rhs) {
        return ir::Expr{.node = ir::BinaryExpr{.op = ir::ArithmeticOp::Add,
                                               .left = ir::ExprPtr{std::move(lhs)},
                                               .right = ir::ExprPtr{std::move(rhs)}}};
    };
    CHECK(ir::is_bucket_local_window_expr(binary(call("rolling_max"), call("abs"))));
    CHECK_FALSE(ir::is_bucket_local_window_expr(binary(call("rolling_max"), call("lag"))));
}
