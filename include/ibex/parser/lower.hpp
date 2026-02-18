#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/parser/ast.hpp>

#include <expected>
#include <string>

namespace ibex::parser {

struct LowerError {
    std::string message;
};

using LowerResult = std::expected<ir::NodePtr, LowerError>;

/// Lower a parsed Program into an IR node tree.
/// Returns the IR for the last expression statement.
[[nodiscard]] auto lower(const Program& program) -> LowerResult;

}  // namespace ibex::parser
