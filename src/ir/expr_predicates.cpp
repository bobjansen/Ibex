#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>

#include <array>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

namespace ibex::ir {

namespace {

using Info = BuiltinFunctionInfo;

// This is the IR-facing source of truth for built-in classification. It is
// intentionally small: scalar built-ins are the default, so only functions
// whose shape affects lowering/planning need an entry. `builtins()` validates
// its executable entries against this table at startup.
constexpr auto kBuiltinFunctionInfo = std::to_array<std::pair<std::string_view, Info>>({
    {"rolling_sum", {FnKind::Transform, true}},
    {"rolling_mean", {FnKind::Transform, true}},
    {"rolling_min", {FnKind::Transform, true}},
    {"rolling_max", {FnKind::Transform, true}},
    {"rolling_count", {FnKind::Transform, true}},
    {"rolling_median", {FnKind::Transform, true}},
    {"rolling_std", {FnKind::Transform, true}},
    {"rolling_ewma", {FnKind::Transform, true}},
    {"rolling_quantile", {FnKind::Transform, true}},
    {"rolling_skew", {FnKind::Transform, true}},
    {"rolling_kurtosis", {FnKind::Transform, true}},
    {"rolling_first", {FnKind::Transform, true}},
    {"rolling_last", {FnKind::Transform, true}},
    {"cumsum", {FnKind::Transform}},
    {"cumprod", {FnKind::Transform}},
    {"lag", {FnKind::Transform}},
    {"lead", {FnKind::Transform}},
    {"fill_forward", {FnKind::Transform}},
    {"fill_backward", {FnKind::Transform}},
    {"window_start", {FnKind::Transform}},
    {"window_end", {FnKind::Transform}},
    {"rand_uniform", {FnKind::Generator}},
    {"rand_normal", {FnKind::Generator}},
    {"rand_student_t", {FnKind::Generator}},
    {"rand_gamma", {FnKind::Generator}},
    {"rand_exponential", {FnKind::Generator}},
    {"rand_bernoulli", {FnKind::Generator}},
    {"rand_poisson", {FnKind::Generator}},
    {"rand_int", {FnKind::Generator}},
    {"rep", {FnKind::Generator}},
    {"sum", {FnKind::Aggregate}},
    {"mean", {FnKind::Aggregate}},
    {"min", {FnKind::Aggregate}},
    {"max", {FnKind::Aggregate}},
    {"count", {FnKind::Aggregate}},
    {"first", {FnKind::Aggregate}},
    {"last", {FnKind::Aggregate}},
    {"median", {FnKind::Aggregate}},
    {"std", {FnKind::Aggregate}},
    {"ewma", {FnKind::Aggregate}},
    {"quantile", {FnKind::Aggregate}},
    {"skew", {FnKind::Aggregate}},
    {"kurtosis", {FnKind::Aggregate}},
    {"abs", {FnKind::Scalar}},
    {"__interp", {FnKind::Scalar}},
    {"acos", {FnKind::Scalar}},
    {"asin", {FnKind::Scalar}},
    {"atan", {FnKind::Scalar}},
    {"ceil", {FnKind::Scalar}},
    {"coalesce", {FnKind::Scalar}},
    {"cos", {FnKind::Scalar}},
    {"cosh", {FnKind::Scalar}},
    {"day", {FnKind::Scalar}},
    {"exp", {FnKind::Scalar}},
    {"fill_null", {FnKind::Scalar}},
    {"floor", {FnKind::Scalar}},
    {"hour", {FnKind::Scalar}},
    {"is_nan", {FnKind::Scalar}},
    {"like", {FnKind::Scalar}},
    {"log", {FnKind::Scalar}},
    {"log10", {FnKind::Scalar}},
    {"log2", {FnKind::Scalar}},
    {"minute", {FnKind::Scalar}},
    {"month", {FnKind::Scalar}},
    {"null_if_nan", {FnKind::Scalar}},
    {"null_if_not_finite", {FnKind::Scalar}},
    {"pmax", {FnKind::Scalar}},
    {"pmin", {FnKind::Scalar}},
    {"round", {FnKind::Scalar}},
    {"second", {FnKind::Scalar}},
    {"sin", {FnKind::Scalar}},
    {"sinh", {FnKind::Scalar}},
    {"sqrt", {FnKind::Scalar}},
    {"substring", {FnKind::Scalar}},
    {"tan", {FnKind::Scalar}},
    {"tanh", {FnKind::Scalar}},
    {"trunc", {FnKind::Scalar}},
    {"year", {FnKind::Scalar}},
    {"Int", {FnKind::Scalar}},
    {"Int32", {FnKind::Scalar}},
    {"Int64", {FnKind::Scalar}},
    {"Float32", {FnKind::Scalar}},
    {"Float64", {FnKind::Scalar}},
});

}  // namespace

auto builtin_function_info(std::string_view name) -> const BuiltinFunctionInfo* {
    const auto find = [name](const auto& entries) -> const BuiltinFunctionInfo* {
        for (const auto& [candidate, info] : entries) {
            if (candidate == name) {
                return &info;
            }
        }
        return nullptr;
    };
    return find(kBuiltinFunctionInfo);
}

auto is_rolling_func(std::string_view name) -> bool {
    const auto* info = builtin_function_info(name);
    return info != nullptr && info->rolling;
}

auto is_aggregate_func(std::string_view name) -> bool {
    const auto* info = builtin_function_info(name);
    return info != nullptr && info->kind == FnKind::Aggregate;
}

auto fn_kind(std::string_view name) -> std::optional<FnKind> {
    const auto* info = builtin_function_info(name);
    return info != nullptr ? std::optional{info->kind} : std::nullopt;
}

namespace {

// Walk `expr`; a call is disqualifying when `bad(fn_kind(callee))`. A `RankExpr`
// node is always disqualifying (non-row-local). Shared by the row-local and
// subset-evaluable predicates so both classify through `fn_kind`.
template <typename BadKind>
auto no_call_of_kind(const Expr& expr, BadKind bad) -> bool {
    return std::visit(
        [&](const auto& n) -> bool {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef> || std::is_same_v<T, Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                return no_call_of_kind(*n.left, bad) && no_call_of_kind(*n.right, bad);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                // `right` is null for unary Not.
                return no_call_of_kind(*n.left, bad) &&
                       (n.right == nullptr || no_call_of_kind(*n.right, bad));
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                return no_call_of_kind(*n.operand, bad);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                const auto kind = fn_kind(n.callee);
                if (!kind.has_value() || bad(*kind)) {
                    return false;
                }
                for (const auto& arg : n.args) {
                    if (!no_call_of_kind(*arg, bad)) {
                        return false;
                    }
                }
                for (const auto& na : n.named_args) {
                    if (!no_call_of_kind(*na.value, bad)) {
                        return false;
                    }
                }
                return true;
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                return false;
            } else {
                return false;
            }
        },
        expr.node);
}

}  // namespace

auto is_row_local_update_expr(const Expr& expr) -> bool {
    return no_call_of_kind(
        expr, [](FnKind k) { return k == FnKind::Transform || k == FnKind::Generator; });
}

auto is_subset_evaluable_expr(const Expr& expr) -> bool {
    return no_call_of_kind(expr, [](FnKind k) { return k != FnKind::Scalar; });
}

auto is_group_parallel_safe_expr(const Expr& expr) -> bool {
    // `no_call_of_kind` already refuses an unknown callee and a RankExpr, which
    // is most of what this needs; Generator is the one classified kind that is
    // unsafe to run concurrently.
    return no_call_of_kind(expr, [](FnKind k) { return k == FnKind::Generator; });
}

void collect_expr_column_refs(const Expr& expr, robin_hood::unordered_set<std::string>& out) {
    std::visit(
        [&](const auto& n) {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef>) {
                out.insert(n.name);
            } else if constexpr (std::is_same_v<T, Literal>) {
                // nothing
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                collect_expr_column_refs(*n.left, out);
                collect_expr_column_refs(*n.right, out);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                collect_expr_column_refs(*n.left, out);
                if (n.right != nullptr) {  // null for unary Not
                    collect_expr_column_refs(*n.right, out);
                }
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                collect_expr_column_refs(*n.operand, out);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                for (const auto& arg : n.args) {
                    collect_expr_column_refs(*arg, out);
                }
                for (const auto& na : n.named_args) {
                    collect_expr_column_refs(*na.value, out);
                }
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                for (const auto& key : n.order_keys) {
                    out.insert(key.name);
                }
            }
        },
        expr.node);
}

}  // namespace ibex::ir
