#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/parser/ast.hpp>

#include <expected>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace ibex::parser {

struct LowerError {
    std::string message;
};

using LowerResult = std::expected<ir::NodePtr, LowerError>;

struct LowerContext {
    std::unordered_map<std::string, ir::NodePtr> bindings;
    /// Names of extern functions whose return type is DataFrame/TimeFrame.
    /// Populate before calling lower_expr so that tuple-LHS RHS expressions
    /// that call table-returning externs are lowered correctly.
    std::unordered_set<std::string> table_externs;
    /// Names of extern functions whose first argument is a DataFrame.
    /// Populate before calling lower_expr so Stream sink calls can be validated.
    std::unordered_set<std::string> sink_externs;
};

/// Lower a parsed Program into an IR node tree.
/// Returns the IR for the last expression statement.
[[nodiscard]] auto lower(const Program& program) -> LowerResult;

/// Lower a single expression with an external context.
[[nodiscard]] auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult;

}  // namespace ibex::parser
