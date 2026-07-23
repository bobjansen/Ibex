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
