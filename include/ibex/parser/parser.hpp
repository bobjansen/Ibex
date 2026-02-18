#pragma once

#include <ibex/parser/ast.hpp>

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
using ParseResult = std::expected<Program, ParseError>;

/// Parse an Ibex source string into a Program AST.
[[nodiscard]] auto parse(std::string_view source) -> ParseResult;

}  // namespace ibex::parser
