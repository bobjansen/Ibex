#pragma once

#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace ibex::ops {

struct NamedArgExpr {
    std::string name;
    ir::Expr value;
};

struct TupleSource {
    std::vector<std::string> aliases;
    runtime::Table table;
};

void set_scalars(const runtime::ScalarRegistry* scalars);

// ─── Core table operations ────────────────────────────────────────────────────
//  These are the functions emitted by ibex_compile into the generated C++ file.

[[nodiscard]] auto filter(const runtime::Table& t, ir::FilterExprPtr pred) -> runtime::Table;

// ─── FilterExpr builders ──────────────────────────────────────────────────────
//  Convenience factories for constructing filter expression trees in emitted
//  and test code.  Each returns a heap-allocated FilterExprPtr (unique_ptr).

[[nodiscard]] auto filter_col(std::string name) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_int(std::int64_t v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_dbl(double v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_bool(bool v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_str(std::string v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_date(Date v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_timestamp(Timestamp v) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_arith(ir::ArithmeticOp op, ir::FilterExprPtr l, ir::FilterExprPtr r)
    -> ir::FilterExprPtr;
[[nodiscard]] auto filter_cmp(ir::CompareOp op, ir::FilterExprPtr l, ir::FilterExprPtr r)
    -> ir::FilterExprPtr;
[[nodiscard]] auto filter_and(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_or(ir::FilterExprPtr l, ir::FilterExprPtr r) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_not(ir::FilterExprPtr operand) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_is_null(ir::FilterExprPtr operand) -> ir::FilterExprPtr;
[[nodiscard]] auto filter_is_not_null(ir::FilterExprPtr operand) -> ir::FilterExprPtr;

[[nodiscard]] auto project(const runtime::Table& t, const std::vector<std::string>& col_names)
    -> runtime::Table;

[[nodiscard]] auto distinct(const runtime::Table& t) -> runtime::Table;

[[nodiscard]] auto order(const runtime::Table& t, const std::vector<ir::OrderKey>& keys)
    -> runtime::Table;

[[nodiscard]] auto head(const runtime::Table& t, std::size_t count,
                        const std::vector<std::string>& group_by = {}) -> runtime::Table;

[[nodiscard]] auto tail(const runtime::Table& t, std::size_t count,
                        const std::vector<std::string>& group_by = {}) -> runtime::Table;

[[nodiscard]] auto aggregate(const runtime::Table& t, const std::vector<std::string>& group_by,
                             const std::vector<ir::AggSpec>& aggs) -> runtime::Table;

[[nodiscard]] auto resample(const runtime::Table& t, ir::Duration duration,
                            const std::vector<std::string>& group_by,
                            const std::vector<ir::AggSpec>& aggs) -> runtime::Table;

[[nodiscard]] auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields)
    -> runtime::Table;

[[nodiscard]] auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields,
                          const std::vector<TupleSource>& tuple_sources,
                          const std::vector<std::string>& group_by) -> runtime::Table;

[[nodiscard]] auto rename(const runtime::Table& t, const std::vector<ir::RenameSpec>& renames)
    -> runtime::Table;

[[nodiscard]] auto as_timeframe(const runtime::Table& t, const std::string& column)
    -> runtime::Table;

[[nodiscard]] auto columns(const runtime::Table& t) -> runtime::Table;

[[nodiscard]] auto windowed_update(const runtime::Table& t, ir::Duration duration,
                                   const std::vector<ir::FieldSpec>& fields) -> runtime::Table;

[[nodiscard]] auto melt(const runtime::Table& t, const std::vector<std::string>& id_cols,
                        const std::vector<std::string>& measure_cols) -> runtime::Table;

[[nodiscard]] auto dcast(const runtime::Table& t, const std::string& pivot_col,
                         const std::string& value_col, const std::vector<std::string>& row_keys)
    -> runtime::Table;

/// Covariance matrix of all numeric columns. Drops non-numeric columns.
/// Output: leading "column" String column + N Float64 columns named after inputs.
[[nodiscard]] auto cov(const runtime::Table& t) -> runtime::Table;

/// Pearson correlation matrix of all numeric columns. Same shape as cov().
[[nodiscard]] auto corr(const runtime::Table& t) -> runtime::Table;

/// Transpose a homogeneous-type DataFrame (swap rows ↔ columns).
/// Optional String/Categorical label column names the output columns;
/// if absent output columns are named "r0", "r1", …
[[nodiscard]] auto transpose(const runtime::Table& t) -> runtime::Table;

/// Matrix multiply: left (m×k) × right (k×n) → (m×n).
/// Numeric-only columns are extracted from each operand.
[[nodiscard]] auto matmul(const runtime::Table& left, const runtime::Table& right)
    -> runtime::Table;

/// Model accessor functions — extract sub-tables from a ModelResult.
[[nodiscard]] auto model_coef(const runtime::ModelResult& m) -> runtime::Table;
[[nodiscard]] auto model_summary(const runtime::ModelResult& m) -> runtime::Table;
[[nodiscard]] auto model_fitted(const runtime::ModelResult& m) -> runtime::Table;
[[nodiscard]] auto model_residuals(const runtime::ModelResult& m) -> runtime::Table;
[[nodiscard]] auto model_r_squared(const runtime::ModelResult& m) -> double;

[[nodiscard]] auto inner_join(const runtime::Table& left, const runtime::Table& right,
                              const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto left_join(const runtime::Table& left, const runtime::Table& right,
                             const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto right_join(const runtime::Table& left, const runtime::Table& right,
                              const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto outer_join(const runtime::Table& left, const runtime::Table& right,
                              const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto semi_join(const runtime::Table& left, const runtime::Table& right,
                             const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto anti_join(const runtime::Table& left, const runtime::Table& right,
                             const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto cross_join(const runtime::Table& left, const runtime::Table& right)
    -> runtime::Table;

[[nodiscard]] auto asof_join(const runtime::Table& left, const runtime::Table& right,
                             const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto join_with_predicate(const runtime::Table& left, const runtime::Table& right,
                                       ir::JoinKind kind, const std::vector<std::string>& keys,
                                       ir::FilterExprPtr predicate) -> runtime::Table;

void print(const runtime::Table& t, std::ostream& out = std::cout);

// ─── Stream helpers ───────────────────────────────────────────────────────────
//  Used by the stream event loop generated by ibex_compile.

/// Append row `row` of `src` to `dst`.  On first call (dst is empty) the
/// column schema is initialised from `src`.
void stream_append_row(runtime::Table& dst, const runtime::Table& src, std::size_t row);

/// Return the nanosecond timestamp of row `row` in `t`, or nullopt if `t`
/// has no time index or the time-index column is not a Timestamp / Int column.
[[nodiscard]] auto stream_get_ts_ns(const runtime::Table& t, std::size_t row)
    -> std::optional<std::int64_t>;

// ─── Expression builders ──────────────────────────────────────────────────────
//  Convenience factories for constructing IR expression trees in emitted code.

[[nodiscard]] auto col_ref(std::string name) -> ir::Expr;
[[nodiscard]] auto int_lit(std::int64_t v) -> ir::Expr;
[[nodiscard]] auto dbl_lit(double v) -> ir::Expr;
[[nodiscard]] auto str_lit(std::string v) -> ir::Expr;
[[nodiscard]] auto date_lit(Date v) -> ir::Expr;
[[nodiscard]] auto timestamp_lit(Timestamp v) -> ir::Expr;
[[nodiscard]] auto binop(ir::ArithmeticOp op, ir::Expr lhs, ir::Expr rhs) -> ir::Expr;
[[nodiscard]] auto fn_call(std::string callee, std::vector<ir::Expr> args,
                           std::vector<NamedArgExpr> named_args = {}) -> ir::Expr;

// ─── Compound builders ────────────────────────────────────────────────────────

[[nodiscard]] auto make_field(std::string alias, ir::Expr expr) -> ir::FieldSpec;
[[nodiscard]] auto make_agg(ir::AggFunc func, std::string col_name, std::string alias,
                            double param = 0.0) -> ir::AggSpec;

}  // namespace ibex::ops
