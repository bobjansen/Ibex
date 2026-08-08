// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

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

/// How a built-in's result relates to nulls in its arguments.
///
/// Consulted only for `FnKind::Scalar`: a non-row-local function's result at
/// row i does not stand in any fixed relation to its arguments at row i --
/// `lag(x)` is null in the first row of each group however present `x` is --
/// so the kind answers first and this never gets asked. Use
/// `scalar_null_behavior()`, which enforces that.
///
/// This is the single source of truth for the question, read by every pass that
/// reasons about nullability. `builtins()` cross-checks it against each entry's
/// runtime `NullPolicy` at startup, so a new null-handling built-in cannot be
/// added to one table and forgotten in the other.
enum class NullBehavior : std::uint8_t {
    /// Present exactly when every argument is present: a null argument yields a
    /// null result. What arithmetic and the math built-ins do, and the default.
    Propagates,
    /// Present when *any* argument is present. `coalesce(a, ..., z)` takes the
    /// first argument that has a value, and `fill_null(x, v)` is the two-
    /// argument spelling of the same rule.
    Absorbs,
    /// May be null however present its arguments are -- `null_if_nan(x)` turns
    /// a perfectly present NaN into a null, which is the point of it.
    Introduces,
};

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
    /// Only meaningful for `kind == Scalar`; see `NullBehavior`.
    NullBehavior null_behavior = NullBehavior::Propagates;
    bool rolling = false;
    /// True if the function's result at row i depends on rows other than i —
    /// it reads neighbours (lag/lead/rolling), accumulates along the row order
    /// (cumsum/cumprod), or propagates through it (fill_forward/fill_backward).
    /// Such a function is only meaningful when the rows are in a meaningful
    /// order, which is what `check_time_index_ordering` enforces.
    bool order_dependent = false;
};

/// Returns metadata for a built-in function, or nullptr for an unknown name.
[[nodiscard]] auto builtin_function_info(std::string_view name) -> const BuiltinFunctionInfo*;

/// True for built-in rolling transforms. This is deliberately metadata-driven:
/// lowering uses it only to parse the rolling window argument syntax.
[[nodiscard]] auto is_rolling_func(std::string_view name) -> bool;

/// True for built-in aggregate functions.
[[nodiscard]] auto is_aggregate_func(std::string_view name) -> bool;

/// True for built-ins whose result depends on the row order (see
/// BuiltinFunctionInfo::order_dependent).
[[nodiscard]] auto is_order_dependent_func(std::string_view name) -> bool;

/// Returns the name of the first order-dependent call found anywhere in
/// `expr`, or empty if there is none. Used to name the offending function in
/// the row-order diagnostic.
[[nodiscard]] auto find_order_dependent_call(const Expr& expr) -> std::string;

/// The null behaviour of a *row-local* built-in, or `nullopt` when the name is
/// not one: an unknown callee (a plugin, about which nothing is assumed) or a
/// function whose kind rules the question out. `nullopt` means "no claim",
/// which every caller must treat as the conservative answer.
[[nodiscard]] auto scalar_null_behavior(std::string_view name) -> std::optional<NullBehavior>;

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

/// True if `expr` may be evaluated for several groups concurrently.
///
/// Permits Scalar, Transform and Aggregate calls — a grouped windowed update
/// runs each group over its own slice, so a rolling or aggregate call sees only
/// that group either way. Two things are refused:
///
/// - **An unknown callee**, i.e. an extern/plugin function. They are left
///   unclassified on purpose (see `BuiltinFunctionInfo`) so planning cannot
///   assume they are deterministic or safe to duplicate, and the runtime's rule
///   is that a plugin runs on the thread of the query that called it.
/// - **`Generator`** (`rand_*`), which draws from one shared RNG stream.
///   Running groups concurrently would reorder the draws and change the answer
///   rather than just the timing — the failure a value test would catch only by
///   luck.
///
/// `RankExpr` is refused too. Per-group rank would in fact be safe, but the
/// standing rule for this work is that anything unproven defaults to unsafe.
[[nodiscard]] auto is_group_parallel_safe_expr(const Expr& expr) -> bool;

/// True if, under an *aligned* window clause, every row's value depends only on
/// rows in its own window bucket.
///
/// This is what licenses splitting one group's rows at bucket boundaries and
/// evaluating the pieces independently — the split needs no halo because no
/// call reaches across the cut. Permits only Scalar calls (row-local) and the
/// rolling family without a per-call window override (bounded below by
/// `bucket_start`). Everything else — `lag`, `diff`, `cumsum`, a bare
/// Aggregate, an extern — is refused, because it reads across buckets.
///
/// Only meaningful for `aligned`: a trailing window spans `(t - dur, t]`, which
/// straddles any boundary we might pick.
[[nodiscard]] auto is_bucket_local_window_expr(const Expr& expr) -> bool;

/// Collects the set of column names referenced anywhere inside `expr` into `out`.
void collect_expr_column_refs(const Expr& expr, robin_hood::unordered_set<std::string>& out);

}  // namespace ibex::ir
