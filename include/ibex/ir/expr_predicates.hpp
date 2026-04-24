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

/// True if an Expr's output at row i depends only on its inputs at row i.
/// Calls that reach across rows (lag/lead, rolling_*, cumsum/cumprod,
/// fill_forward/fill_backward, rep) or sample a global sequence (rng_*) are
/// not row-local.
[[nodiscard]] auto is_row_local_update_expr(const Expr& expr) -> bool;

/// Collects the set of column names referenced anywhere inside `expr` into `out`.
void collect_expr_column_refs(const Expr& expr, std::unordered_set<std::string>& out);

}  // namespace ibex::ir
