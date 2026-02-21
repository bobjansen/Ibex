#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/parser/ast.hpp>

#include <expected>
#include <string>
#include <unordered_map>

namespace ibex::parser {

struct LowerError {
    std::string message;
};

using LowerResult = std::expected<ir::NodePtr, LowerError>;

struct LowerContext {
    std::unordered_map<std::string, ir::NodePtr> bindings;
};

/// Lower a parsed Program into an IR node tree.
/// Returns the IR for the last expression statement.
[[nodiscard]] auto lower(const Program& program) -> LowerResult;

/// Lower a single expression with an external context.
[[nodiscard]] auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult;

}  // namespace ibex::parser
