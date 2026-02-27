#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace ibex::parser {

/// Token types for the Ibex lexer.
/// See SPEC.md Section 2 (Lexical Structure) for the full specification.
enum class TokenKind : std::uint8_t {
    // Literals
    IntLiteral,
    FloatLiteral,
    StringLiteral,
    BoolLiteral,
    DurationLiteral,

    // Identifiers
    Identifier,
    QuotedIdentifier,

    // Hard keywords (SPEC.md Appendix B)
    KeywordLet,
    KeywordMut,
    KeywordExtern,
    KeywordFn,
    KeywordFrom,
    KeywordFilter,
    KeywordSelect,
    KeywordUpdate,
    KeywordDistinct,
    KeywordOrder,
    KeywordBy,
    KeywordWindow,
    KeywordResample,
    KeywordJoin,
    KeywordLeft,
    KeywordAsof,
    KeywordOn,
    KeywordIs,
    KeywordNull,
    KeywordNot,
    KeywordAsc,
    KeywordDesc,

    // Type keywords
    KeywordInt,
    KeywordInt32,
    KeywordInt64,
    KeywordFloat32,
    KeywordFloat64,
    KeywordBool,
    KeywordString,
    KeywordDate,
    KeywordTimestamp,
    KeywordColumn,
    KeywordSeries,
    KeywordDataFrame,
    KeywordTimeFrame,

    // Comparison operators
    EqEq,    // ==
    BangEq,  // !=
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=

    // Arithmetic operators
    Plus,     // +
    Minus,    // -
    Star,     // *
    Slash,    // /
    Percent,  // %

    // Logical operators
    AmpAmp,    // &&
    PipePipe,  // ||
    Bang,      // !

    // Scope escape (SPEC.md Section 6.2)
    Caret,  // ^

    // Assignment
    Eq,  // =

    // Delimiters
    LParen,     // (
    RParen,     // )
    LBracket,   // [
    RBracket,   // ]
    LBrace,     // {
    RBrace,     // }
    Comma,      // ,
    Semicolon,  // ;
    Colon,      // :
    Arrow,      // ->

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
[[nodiscard]] auto tokenize(std::string_view source) -> std::vector<Token>;

}  // namespace ibex::parser
