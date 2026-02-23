#pragma once

#include <ibex/ir/builder.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace ibex::ops {

// ─── Core table operations ────────────────────────────────────────────────────
//  These are the functions emitted by ibex_compile into the generated C++ file.

[[nodiscard]] auto filter(const runtime::Table& t, ir::FilterPredicate pred) -> runtime::Table;

[[nodiscard]] auto project(const runtime::Table& t, const std::vector<std::string>& col_names)
    -> runtime::Table;

[[nodiscard]] auto distinct(const runtime::Table& t) -> runtime::Table;

[[nodiscard]] auto aggregate(const runtime::Table& t, const std::vector<std::string>& group_by,
                             const std::vector<ir::AggSpec>& aggs) -> runtime::Table;

[[nodiscard]] auto update(const runtime::Table& t, const std::vector<ir::FieldSpec>& fields)
    -> runtime::Table;

[[nodiscard]] auto inner_join(const runtime::Table& left, const runtime::Table& right,
                              const std::vector<std::string>& keys) -> runtime::Table;

[[nodiscard]] auto left_join(const runtime::Table& left, const runtime::Table& right,
                             const std::vector<std::string>& keys) -> runtime::Table;

void print(const runtime::Table& t, std::ostream& out = std::cout);

// ─── Expression builders ──────────────────────────────────────────────────────
//  Convenience factories for constructing IR expression trees in emitted code.

[[nodiscard]] auto col_ref(std::string name) -> ir::Expr;
[[nodiscard]] auto int_lit(std::int64_t v) -> ir::Expr;
[[nodiscard]] auto dbl_lit(double v) -> ir::Expr;
[[nodiscard]] auto str_lit(std::string v) -> ir::Expr;
[[nodiscard]] auto binop(ir::ArithmeticOp op, ir::Expr lhs, ir::Expr rhs) -> ir::Expr;
[[nodiscard]] auto fn_call(std::string callee, std::vector<ir::Expr> args) -> ir::Expr;

// ─── Compound builders ────────────────────────────────────────────────────────

[[nodiscard]] auto make_field(std::string alias, ir::Expr expr) -> ir::FieldSpec;
[[nodiscard]] auto make_agg(ir::AggFunc func, std::string col_name, std::string alias)
    -> ir::AggSpec;

}  // namespace ibex::ops
