#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>

#include <algorithm>
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
    {"rolling_sum", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_mean", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_min", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_max", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_count", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_median", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_std", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_ewma", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_quantile", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_skew", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_kurtosis", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_first", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"rolling_last", {.kind = FnKind::Transform, .rolling = true, .order_dependent = true}},
    {"cumsum", {.kind = FnKind::Transform, .order_dependent = true}},
    {"cumprod", {.kind = FnKind::Transform, .order_dependent = true}},
    {"lag", {.kind = FnKind::Transform, .order_dependent = true}},
    {"lead", {.kind = FnKind::Transform, .order_dependent = true}},
    {"fill_forward", {.kind = FnKind::Transform, .order_dependent = true}},
    {"fill_backward", {.kind = FnKind::Transform, .order_dependent = true}},
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
    {"with_timezone", {.kind = FnKind::Transform}},
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

auto is_order_dependent_func(std::string_view name) -> bool {
    const auto* info = builtin_function_info(name);
    return info != nullptr && info->order_dependent;
}

auto fn_kind(std::string_view name) -> std::optional<FnKind> {
    const auto* info = builtin_function_info(name);
    return info != nullptr ? std::optional{info->kind} : std::nullopt;
}

namespace {

// Walk `expr`; a call is disqualifying unless `ok(call)` accepts it. A
// `RankExpr` node is always disqualifying — see the header on
// `is_group_parallel_safe_expr` for why that is a default rather than a fact
// about rank. Every expression-class predicate below shares this walk so that
// the `ok` lambda is the ONLY thing they differ in; otherwise two predicates
// could disagree about the same expression for reasons unrelated to their
// policy. It also fixes the easy mistake in one place: `CallExpr` has two child
// lists, and a walk that visits `args` but not `named_args` silently accepts an
// expression whose disqualifying call is hidden in a named argument.
template <typename OkCall>
auto every_call(const Expr& expr, OkCall ok) -> bool {
    return std::visit(
        [&](const auto& n) -> bool {
            using T = std::decay_t<decltype(n)>;
            if constexpr (std::is_same_v<T, ColumnRef> || std::is_same_v<T, Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                return every_call(*n.left, ok) && every_call(*n.right, ok);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                // `right` is null for unary Not.
                return every_call(*n.left, ok) && (n.right == nullptr || every_call(*n.right, ok));
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                return every_call(*n.operand, ok);
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                if (!ok(n)) {
                    return false;
                }
                return std::ranges::all_of(n.args,
                                           [&](const auto& a) { return every_call(*a, ok); }) &&
                       std::ranges::all_of(
                           n.named_args, [&](const auto& na) { return every_call(*na.value, ok); });
                // NOLINTNEXTLINE(bugprone-branch-clone)
            } else if constexpr (std::is_same_v<T, RankExpr>) {
                return false;
            } else {
                return false;
            }
        },
        expr.node);
}

// A call is disqualifying when `bad(fn_kind(callee))`. An unknown callee — an
// extern or a plugin — is disqualifying for every predicate here: we cannot
// classify what we cannot look up.
template <typename BadKind>
auto no_call_of_kind(const Expr& expr, BadKind bad) -> bool {
    return every_call(expr, [&](const CallExpr& call) {
        const auto kind = fn_kind(call.callee);
        return kind.has_value() && !bad(*kind);
    });
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

auto find_order_dependent_call(const Expr& expr) -> std::string {
    std::string found;
    // `every_call` short-circuits on the first `false`, so returning
    // `found.empty()` stops the walk as soon as one is named.
    every_call(expr, [&](const CallExpr& call) {
        if (is_order_dependent_func(call.callee)) {
            found = call.callee;
        }
        return found.empty();
    });
    return found;
}

auto is_bucket_local_window_expr(const Expr& expr) -> bool {
    return every_call(expr, [](const CallExpr& call) {
        const auto kind = fn_kind(call.callee);

        // Unknown callee — an extern or a plugin. Unclassifiable, so unprovable;
        // refuse, as every predicate here does. Also guards the deref below.
        if (!kind.has_value()) {
            return false;
        }
        // Row-local by definition: a scalar call reads only its own row, so it
        // cannot see across a bucket boundary.
        if (*kind == FnKind::Scalar) {
            return true;
        }
        // Classified Transform, but each is a pure function of its own row's
        // timestamp and the clause's duration — as row-local as a Scalar. They
        // are not reclassified because the other predicates that read `fn_kind`
        // have their own reasons to treat them as non-row-local.
        if (call.callee == "window_start" || call.callee == "window_end") {
            return true;
        }
        // Every other kind reads neighbours or the whole slice. Of those, only
        // the rolling family is bounded by the enclosing window: under
        // `aligned` its lower bound is the current bucket's start (see
        // `apply_rolling_func`). `lag`/`diff`/`cumsum` read across buckets
        // freely, and a bare Aggregate reduces the entire slice.
        if (!is_rolling_func(call.callee)) {
            return false;
        }
        // A per-call window overrides the enclosing clause: `__window_n` is a
        // count window, which knows nothing about the grid, and `__window_ns`
        // is a different duration and so a different grid. Either way the
        // boundaries we would split on no longer bound this call.
        return std::ranges::all_of(call.named_args, [&](const auto& na) {
            return !(na.name == "__window_n" || na.name == "__window_ns");
        });
    });
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
