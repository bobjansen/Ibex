#include <ibex/parser/lexer.hpp>

namespace ibex::parser {

auto tokenize(std::string_view source) -> std::vector<Token> {
    // TODO: Implement full lexer.
    // For now, return a single EOF token.
    std::vector<Token> tokens;
    tokens.push_back(Token{
        .kind = TokenKind::Eof,
        .lexeme = source.substr(0, 0),
        .line = 1,
        .column = 1,
    });
    return tokens;
}

}  // namespace ibex::parser
