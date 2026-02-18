#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::parser {

/// Token types for the Ibex lexer.
enum class TokenKind : std::uint8_t {
    // Literals
    IntLiteral,
    FloatLiteral,
    StringLiteral,

    // Identifiers & keywords
    Identifier,
    KeywordSelect,
    KeywordFrom,
    KeywordWhere,
    KeywordGroup,
    KeywordBy,
    KeywordAs,

    // Operators
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,

    // Delimiters
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,

    // Special
    Eof,
    Error,
};

/// A single token with source location.
struct Token {
    TokenKind kind = TokenKind::Error;
    std::string_view lexeme;
    std::size_t line = 0;
    std::size_t column = 0;
};

/// Tokenize an Ibex source string.
///
/// TODO: Implement full lexer. Currently a stub.
[[nodiscard]] auto tokenize(std::string_view source) -> std::vector<Token>;

}  // namespace ibex::parser
