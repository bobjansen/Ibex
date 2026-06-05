#pragma once

#include <ibex/ir/node.hpp>

#include <string_view>
#include <unordered_set>

namespace ibex::ir {

[[nodiscard]] constexpr auto is_rolling_func(std::string_view name) -> bool {
    return name == "rolling_sum" || name == "rolling_mean" || name == "rolling_min" ||
           name == "rolling_max" || name == "rolling_count" || name == "rolling_median" ||
           name == "rolling_std" || name == "rolling_ewma" || name == "rolling_quantile" ||
           name == "rolling_skew" || name == "rolling_kurtosis";
}

[[nodiscard]] constexpr auto is_cum_func(std::string_view name) -> bool {
    return name == "cumsum" || name == "cumprod";
}

[[nodiscard]] constexpr auto is_rng_func(std::string_view name) -> bool {
    return name == "rand_uniform" || name == "rand_normal" || name == "rand_student_t" ||
           name == "rand_gamma" || name == "rand_exponential" || name == "rand_bernoulli" ||
           name == "rand_poisson" || name == "rand_int";
}

[[nodiscard]] constexpr auto is_aggregate_func(std::string_view name) -> bool {
    return name == "sum" || name == "mean" || name == "min" || name == "max" || name == "count" ||
           name == "first" || name == "last" || name == "median" || name == "std" ||
           name == "ewma" || name == "quantile" || name == "skew" || name == "kurtosis";
}

/// The kind of a built-in function — its shape and row-dependency. Single front
/// door for the function taxonomy; see `plans/function-kind-registry-plan.md`.
///
///   Scalar    — row-local: `out[i] = f(args[i])` (arithmetic, casts, math,
///               date parts, pmin/pmax, is_nan, round, ...).
///   Transform — non-row-local: reads neighbours/order (rolling_*, cumsum/
///               cumprod, lag/lead, fill_forward/fill_backward; `rank` is the
///               RankExpr node, classified at the node level).
///   Generator — produces a column from a sequence/pattern (rand_*, rep).
///   Aggregate — reduces a column or group (sum/mean/.../kurtosis).
enum class FnKind : std::uint8_t { Scalar, Transform, Generator, Aggregate };

/// Classify a built-in by name. Unknown names (extern / user functions) are
/// treated as Scalar (row-local) — the safe default for the callers.
[[nodiscard]] auto fn_kind(std::string_view name) -> FnKind;

/// True if an Expr's output at row i depends only on its inputs at row i — no
/// `Transform`/`Generator` call and no `RankExpr`. Aggregates are treated as
/// row-local here (they are routed through the aggregate machinery before this
/// check). Equivalent to "no call whose `fn_kind` is Transform or Generator".
[[nodiscard]] auto is_row_local_update_expr(const Expr& expr) -> bool;

/// True if every call in `expr` is `Scalar`-kind (no Transform, Generator, or
/// Aggregate) and there is no `RankExpr`. Such a field may be evaluated on just
/// a subset of rows (gather/scatter) — used by the guarded `where … update`.
[[nodiscard]] auto is_subset_evaluable_expr(const Expr& expr) -> bool;

/// Collects the set of column names referenced anywhere inside `expr` into `out`.
void collect_expr_column_refs(const Expr& expr, std::unordered_set<std::string>& out);

}  // namespace ibex::ir
