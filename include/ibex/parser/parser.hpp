#pragma once

#include <ibex/ir/node.hpp>

#include <expected>
#include <string>
#include <string_view>

namespace ibex::parser {

/// Parse error with location information.
struct ParseError {
    std::string message;
    std::size_t line = 0;
    std::size_t column = 0;

    [[nodiscard]] auto format() const -> std::string;
};

/// Result type for parse operations.
using ParseResult = std::expected<ir::NodePtr, ParseError>;

/// Parse an Ibex query string into an IR node tree.
///
/// TODO: Implement lexer and recursive-descent parser.
/// Currently returns an error indicating parsing is not yet implemented.
[[nodiscard]] auto parse(std::string_view source) -> ParseResult;

}  // namespace ibex::parser
