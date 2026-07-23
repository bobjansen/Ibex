#pragma once

#include <ibex/ir/node.hpp>

#include <optional>
#include <robin_hood.h>
#include <string_view>

namespace ibex::ir {
/// The kind of a built-in function — its shape and row-dependency. Single front
/// door for the function taxonomy; see `plans/function-kind-registry-plan.md`.
///
///   Scalar    — row-local: `out[i] = f(args[i])` (arithmetic, casts, math,
///               date parts, pmin/pmax, is_nan, round, ...).
///   Transform — non-row-local: reads neighbours/order (rolling_*, cumsum/
///               cumprod, lag/lead, fill_forward/fill_backward). The
///               null-handling scalars (fill_null, null_if_*, coalesce) are
///               Scalar: the per-row evaluator carries null natively
///               (plans/exprvalue-null-arm-plan.md). `rank` is the RankExpr
///               node, classified at the node level.
///   Generator — produces a column from a sequence/pattern (rand_*, rep).
///   Aggregate — reduces a column or group (sum/mean/.../kurtosis).
enum class FnKind : std::uint8_t { Scalar, Transform, Generator, Aggregate };

/// Metadata required by IR lowering and planning. Keep classification here,
/// rather than scattering function-name lists through those passes. Runtime
/// entries still carry their executable implementation in `BuiltinFn`.
///
/// A future plugin function registry should expose this same metadata to the
/// lowering context. Unknown functions are deliberately unclassified: planning
/// must not assume that arbitrary plugin code is row-local, deterministic, or
/// safe to duplicate.
struct BuiltinFunctionInfo {
    FnKind kind;
    bool rolling = false;
};

/// Returns metadata for a built-in function, or nullptr for an unknown name.
[[nodiscard]] auto builtin_function_info(std::string_view name) -> const BuiltinFunctionInfo*;

/// True for built-in rolling transforms. This is deliberately metadata-driven:
/// lowering uses it only to parse the rolling window argument syntax.
[[nodiscard]] auto is_rolling_func(std::string_view name) -> bool;

/// True for built-in aggregate functions.
[[nodiscard]] auto is_aggregate_func(std::string_view name) -> bool;

/// Classify a known built-in by name. Unknown names have no classification.
/// The runtime builtin registry (builtins() in src/runtime/expr.cpp) checks at
/// construction that this table agrees with every entry's execution payload.
[[nodiscard]] auto fn_kind(std::string_view name) -> std::optional<FnKind>;

/// True if an Expr's output at row i depends only on its inputs at row i — no
/// `Transform`/`Generator`/unknown call and no `RankExpr`. Aggregates are
/// treated as row-local here (they are routed through the aggregate machinery
/// before this check).
[[nodiscard]] auto is_row_local_update_expr(const Expr& expr) -> bool;

/// True if every call in `expr` is `Scalar`-kind (no Transform, Generator, or
/// Aggregate) and there is no `RankExpr`. Such a field may be evaluated on just
/// a subset of rows (gather/scatter) — used by the guarded `where … update`.
[[nodiscard]] auto is_subset_evaluable_expr(const Expr& expr) -> bool;

/// Collects the set of column names referenced anywhere inside `expr` into `out`.
void collect_expr_column_refs(const Expr& expr, robin_hood::unordered_set<std::string>& out);

}  // namespace ibex::ir
