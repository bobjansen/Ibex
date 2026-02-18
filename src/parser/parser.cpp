#include <ibex/parser/parser.hpp>

#include <fmt/core.h>

namespace ibex::parser {

auto ParseError::format() const -> std::string {
    return fmt::format("{}:{}: {}", line, column, message);
}

auto parse(std::string_view /*source*/) -> ParseResult {
    // TODO: Implement recursive-descent parser.
    return std::unexpected(ParseError{
        .message = "parser not yet implemented",
        .line = 0,
        .column = 0,
    });
}

}  // namespace ibex::parser
