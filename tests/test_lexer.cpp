#include <ibex/parser/lexer.hpp>

#include <catch2/catch_test_macros.hpp>

#include <string_view>

namespace {

using namespace ibex::parser;

// Helper: find first token of a given kind.
auto first_of(const std::vector<Token>& tokens, TokenKind kind) -> const Token* {
    for (const auto& t : tokens) {
        if (t.kind == kind) {
            return &t;
        }
    }
    return nullptr;
}

// Helper: count tokens of a given kind (excluding Eof).
auto count_kind(const std::vector<Token>& tokens, TokenKind kind) -> std::size_t {
    std::size_t n = 0;
    for (const auto& t : tokens) {
        if (t.kind == kind) {
            ++n;
        }
    }
    return n;
}

}  // namespace

// ─── Empty and minimal input ─────────────────────────────────────────────────

TEST_CASE("Lexer: empty input produces only Eof", "[lexer]") {
    auto tokens = tokenize("");
    REQUIRE(tokens.size() == 1);
    REQUIRE(tokens[0].kind == TokenKind::Eof);
}

TEST_CASE("Lexer: whitespace-only input produces only Eof", "[lexer]") {
    auto tokens = tokenize("   \t\n\r\n  ");
    REQUIRE(tokens.size() == 1);
    REQUIRE(tokens[0].kind == TokenKind::Eof);
}

// ─── Integer literals ────────────────────────────────────────────────────────

TEST_CASE("Lexer: single integer literal", "[lexer]") {
    auto tokens = tokenize("42");
    REQUIRE(tokens.size() == 2);  // IntLiteral + Eof
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[0].lexeme == "42");
}

TEST_CASE("Lexer: zero", "[lexer]") {
    auto tokens = tokenize("0");
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[0].lexeme == "0");
}

TEST_CASE("Lexer: large integer", "[lexer]") {
    auto tokens = tokenize("9999999999");
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[0].lexeme == "9999999999");
}

// ─── Float literals ──────────────────────────────────────────────────────────

TEST_CASE("Lexer: float literal", "[lexer]") {
    auto tokens = tokenize("3.14");
    REQUIRE(tokens[0].kind == TokenKind::FloatLiteral);
    REQUIRE(tokens[0].lexeme == "3.14");
}

TEST_CASE("Lexer: float with scientific notation", "[lexer]") {
    auto tokens = tokenize("1.5e10");
    REQUIRE(tokens[0].kind == TokenKind::FloatLiteral);
    REQUIRE(tokens[0].lexeme == "1.5e10");
}

TEST_CASE("Lexer: float with negative exponent", "[lexer]") {
    auto tokens = tokenize("2.0E-5");
    REQUIRE(tokens[0].kind == TokenKind::FloatLiteral);
    REQUIRE(tokens[0].lexeme == "2.0E-5");
}

TEST_CASE("Lexer: float with positive exponent", "[lexer]") {
    auto tokens = tokenize("1.0e+3");
    REQUIRE(tokens[0].kind == TokenKind::FloatLiteral);
    REQUIRE(tokens[0].lexeme == "1.0e+3");
}

// ─── String literals ─────────────────────────────────────────────────────────

TEST_CASE("Lexer: simple string literal", "[lexer]") {
    auto tokens = tokenize("\"hello\"");
    REQUIRE(tokens[0].kind == TokenKind::StringLiteral);
    REQUIRE(tokens[0].lexeme == "\"hello\"");
}

TEST_CASE("Lexer: empty string literal", "[lexer]") {
    auto tokens = tokenize("\"\"");
    REQUIRE(tokens[0].kind == TokenKind::StringLiteral);
    REQUIRE(tokens[0].lexeme == "\"\"");
}

TEST_CASE("Lexer: string with escape sequence", "[lexer]") {
    auto tokens = tokenize("\"hello\\nworld\"");
    REQUIRE(tokens[0].kind == TokenKind::StringLiteral);
    REQUIRE(tokens[0].lexeme == "\"hello\\nworld\"");
}

TEST_CASE("Lexer: unterminated string is an error", "[lexer]") {
    auto tokens = tokenize("\"hello");
    REQUIRE(tokens[0].kind == TokenKind::Error);
}

// ─── Bool literals ───────────────────────────────────────────────────────────

TEST_CASE("Lexer: true literal", "[lexer]") {
    auto tokens = tokenize("true");
    REQUIRE(tokens[0].kind == TokenKind::BoolLiteral);
    REQUIRE(tokens[0].lexeme == "true");
}

TEST_CASE("Lexer: false literal", "[lexer]") {
    auto tokens = tokenize("false");
    REQUIRE(tokens[0].kind == TokenKind::BoolLiteral);
    REQUIRE(tokens[0].lexeme == "false");
}

// ─── Duration literals ──────────────────────────────────────────────────────

TEST_CASE("Lexer: duration literals for all units", "[lexer]") {
    auto check = [](const char* src, const char* expected) {
        auto tokens = tokenize(src);
        REQUIRE(tokens[0].kind == TokenKind::DurationLiteral);
        REQUIRE(tokens[0].lexeme == expected);
    };
    check("10ns", "10ns");
    check("5us", "5us");
    check("100ms", "100ms");
    check("30s", "30s");
    check("5m", "5m");
    check("1h", "1h");
    check("7d", "7d");
    check("2w", "2w");
    check("3mo", "3mo");
    check("1y", "1y");
}

TEST_CASE("Lexer: invalid suffix after integer is an error", "[lexer]") {
    auto tokens = tokenize("42xyz");
    REQUIRE(tokens[0].kind == TokenKind::Error);
    REQUIRE(tokens[0].lexeme == "42xyz");
}

// ─── Identifiers ─────────────────────────────────────────────────────────────

TEST_CASE("Lexer: simple identifier", "[lexer]") {
    auto tokens = tokenize("price");
    REQUIRE(tokens[0].kind == TokenKind::Identifier);
    REQUIRE(tokens[0].lexeme == "price");
}

TEST_CASE("Lexer: identifier with underscore", "[lexer]") {
    auto tokens = tokenize("_foo_bar");
    REQUIRE(tokens[0].kind == TokenKind::Identifier);
    REQUIRE(tokens[0].lexeme == "_foo_bar");
}

TEST_CASE("Lexer: identifier with digits", "[lexer]") {
    auto tokens = tokenize("col1");
    REQUIRE(tokens[0].kind == TokenKind::Identifier);
    REQUIRE(tokens[0].lexeme == "col1");
}

// ─── Quoted identifiers ─────────────────────────────────────────────────────

TEST_CASE("Lexer: quoted identifier with dot", "[lexer]") {
    auto tokens = tokenize("`Sepal.Length`");
    REQUIRE(tokens[0].kind == TokenKind::QuotedIdentifier);
    REQUIRE(tokens[0].lexeme == "`Sepal.Length`");
}

TEST_CASE("Lexer: unterminated quoted identifier is an error", "[lexer]") {
    auto tokens = tokenize("`unterminated");
    REQUIRE(tokens[0].kind == TokenKind::Error);
}

// ─── Keywords ────────────────────────────────────────────────────────────────

TEST_CASE("Lexer: all hard keywords", "[lexer]") {
    struct KW {
        const char* text;
        TokenKind kind;
    };
    KW keywords[] = {
        {"let", TokenKind::KeywordLet},
        {"mut", TokenKind::KeywordMut},
        {"extern", TokenKind::KeywordExtern},
        {"fn", TokenKind::KeywordFn},
        {"from", TokenKind::KeywordFrom},
        {"import", TokenKind::KeywordImport},
        {"filter", TokenKind::KeywordFilter},
        {"select", TokenKind::KeywordSelect},
        {"update", TokenKind::KeywordUpdate},
        {"distinct", TokenKind::KeywordDistinct},
        {"order", TokenKind::KeywordOrder},
        {"by", TokenKind::KeywordBy},
        {"window", TokenKind::KeywordWindow},
        {"resample", TokenKind::KeywordResample},
        {"rename", TokenKind::KeywordRename},
        {"join", TokenKind::KeywordJoin},
        {"left", TokenKind::KeywordLeft},
        {"asof", TokenKind::KeywordAsof},
        {"on", TokenKind::KeywordOn},
        {"is", TokenKind::KeywordIs},
        {"null", TokenKind::KeywordNull},
        {"not", TokenKind::KeywordNot},
        {"asc", TokenKind::KeywordAsc},
        {"desc", TokenKind::KeywordDesc},
    };
    for (const auto& kw : keywords) {
        auto tokens = tokenize(kw.text);
        INFO("keyword: " << kw.text);
        REQUIRE(tokens[0].kind == kw.kind);
        REQUIRE(tokens[0].lexeme == kw.text);
    }
}

TEST_CASE("Lexer: all type keywords", "[lexer]") {
    struct KW {
        const char* text;
        TokenKind kind;
    };
    KW keywords[] = {
        {"Int", TokenKind::KeywordInt},       {"Int32", TokenKind::KeywordInt32},
        {"Int64", TokenKind::KeywordInt64},    {"Float32", TokenKind::KeywordFloat32},
        {"Float64", TokenKind::KeywordFloat64}, {"Bool", TokenKind::KeywordBool},
        {"String", TokenKind::KeywordString}, {"Date", TokenKind::KeywordDate},
        {"Timestamp", TokenKind::KeywordTimestamp},
        {"Column", TokenKind::KeywordColumn}, {"Series", TokenKind::KeywordSeries},
        {"DataFrame", TokenKind::KeywordDataFrame},
        {"TimeFrame", TokenKind::KeywordTimeFrame},
        {"Stream", TokenKind::KeywordStream},
    };
    for (const auto& kw : keywords) {
        auto tokens = tokenize(kw.text);
        INFO("type keyword: " << kw.text);
        REQUIRE(tokens[0].kind == kw.kind);
    }
}

// ─── Comparison operators ────────────────────────────────────────────────────

TEST_CASE("Lexer: comparison operators", "[lexer]") {
    auto tokens = tokenize("== != < <= > >=");
    REQUIRE(tokens.size() == 7);  // 6 operators + Eof
    REQUIRE(tokens[0].kind == TokenKind::EqEq);
    REQUIRE(tokens[1].kind == TokenKind::BangEq);
    REQUIRE(tokens[2].kind == TokenKind::Lt);
    REQUIRE(tokens[3].kind == TokenKind::Le);
    REQUIRE(tokens[4].kind == TokenKind::Gt);
    REQUIRE(tokens[5].kind == TokenKind::Ge);
}

// ─── Arithmetic operators ────────────────────────────────────────────────────

TEST_CASE("Lexer: arithmetic operators", "[lexer]") {
    auto tokens = tokenize("+ - * / %");
    REQUIRE(tokens.size() == 6);  // 5 operators + Eof
    REQUIRE(tokens[0].kind == TokenKind::Plus);
    REQUIRE(tokens[1].kind == TokenKind::Minus);
    REQUIRE(tokens[2].kind == TokenKind::Star);
    REQUIRE(tokens[3].kind == TokenKind::Slash);
    REQUIRE(tokens[4].kind == TokenKind::Percent);
}

// ─── Logical operators ──────────────────────────────────────────────────────

TEST_CASE("Lexer: logical operators", "[lexer]") {
    auto tokens = tokenize("&& || !");
    REQUIRE(tokens.size() == 4);  // 3 operators + Eof
    REQUIRE(tokens[0].kind == TokenKind::AmpAmp);
    REQUIRE(tokens[1].kind == TokenKind::PipePipe);
    REQUIRE(tokens[2].kind == TokenKind::Bang);
}

TEST_CASE("Lexer: single ampersand is an error", "[lexer]") {
    auto tokens = tokenize("&");
    REQUIRE(tokens[0].kind == TokenKind::Error);
}

TEST_CASE("Lexer: single pipe is an error", "[lexer]") {
    auto tokens = tokenize("|");
    REQUIRE(tokens[0].kind == TokenKind::Error);
}

// ─── Assignment and arrow ────────────────────────────────────────────────────

TEST_CASE("Lexer: assignment operator", "[lexer]") {
    auto tokens = tokenize("=");
    REQUIRE(tokens[0].kind == TokenKind::Eq);
}

TEST_CASE("Lexer: arrow operator", "[lexer]") {
    auto tokens = tokenize("->");
    REQUIRE(tokens[0].kind == TokenKind::Arrow);
    REQUIRE(tokens[0].lexeme == "->");
}

// ─── Delimiters ──────────────────────────────────────────────────────────────

TEST_CASE("Lexer: all delimiters", "[lexer]") {
    auto tokens = tokenize("( ) [ ] { } , ; :");
    REQUIRE(tokens.size() == 10);  // 9 delimiters + Eof
    REQUIRE(tokens[0].kind == TokenKind::LParen);
    REQUIRE(tokens[1].kind == TokenKind::RParen);
    REQUIRE(tokens[2].kind == TokenKind::LBracket);
    REQUIRE(tokens[3].kind == TokenKind::RBracket);
    REQUIRE(tokens[4].kind == TokenKind::LBrace);
    REQUIRE(tokens[5].kind == TokenKind::RBrace);
    REQUIRE(tokens[6].kind == TokenKind::Comma);
    REQUIRE(tokens[7].kind == TokenKind::Semicolon);
    REQUIRE(tokens[8].kind == TokenKind::Colon);
}

// ─── Scope escape ────────────────────────────────────────────────────────────

TEST_CASE("Lexer: caret (scope escape)", "[lexer]") {
    auto tokens = tokenize("^x");
    REQUIRE(tokens[0].kind == TokenKind::Caret);
    REQUIRE(tokens[1].kind == TokenKind::Identifier);
}

// ─── Comments ────────────────────────────────────────────────────────────────

TEST_CASE("Lexer: line comment is skipped", "[lexer]") {
    auto tokens = tokenize("// this is a comment\n42");
    REQUIRE(tokens.size() == 2);  // IntLiteral + Eof
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[0].lexeme == "42");
}

TEST_CASE("Lexer: block comment is skipped", "[lexer]") {
    auto tokens = tokenize("/* block\ncomment */42");
    REQUIRE(tokens.size() == 2);
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[0].lexeme == "42");
}

TEST_CASE("Lexer: inline block comment", "[lexer]") {
    auto tokens = tokenize("1 /* skip */ + 2");
    REQUIRE(tokens.size() == 4);  // 1 + 2 Eof
    REQUIRE(tokens[0].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[1].kind == TokenKind::Plus);
    REQUIRE(tokens[2].kind == TokenKind::IntLiteral);
}

// ─── Line and column tracking ────────────────────────────────────────────────

TEST_CASE("Lexer: line numbers track across newlines", "[lexer]") {
    auto tokens = tokenize("a\nb\nc");
    REQUIRE(tokens[0].line == 1);
    REQUIRE(tokens[1].line == 2);
    REQUIRE(tokens[2].line == 3);
}

TEST_CASE("Lexer: column numbers within a line", "[lexer]") {
    auto tokens = tokenize("a + b");
    REQUIRE(tokens[0].column == 1);
    REQUIRE(tokens[1].column == 3);
    REQUIRE(tokens[2].column == 5);
}

TEST_CASE("Lexer: column resets after newline", "[lexer]") {
    auto tokens = tokenize("abc\nxy");
    REQUIRE(tokens[0].line == 1);
    REQUIRE(tokens[0].column == 1);
    REQUIRE(tokens[1].line == 2);
    REQUIRE(tokens[1].column == 1);
}

// ─── Complex expressions ────────────────────────────────────────────────────

TEST_CASE("Lexer: let binding statement", "[lexer]") {
    auto tokens = tokenize("let mut x: Int64 = 42;");
    REQUIRE(tokens[0].kind == TokenKind::KeywordLet);
    REQUIRE(tokens[1].kind == TokenKind::KeywordMut);
    REQUIRE(tokens[2].kind == TokenKind::Identifier);
    REQUIRE(tokens[2].lexeme == "x");
    REQUIRE(tokens[3].kind == TokenKind::Colon);
    REQUIRE(tokens[4].kind == TokenKind::KeywordInt64);
    REQUIRE(tokens[5].kind == TokenKind::Eq);
    REQUIRE(tokens[6].kind == TokenKind::IntLiteral);
    REQUIRE(tokens[7].kind == TokenKind::Semicolon);
}

TEST_CASE("Lexer: function declaration", "[lexer]") {
    auto tokens = tokenize("fn square(x: Int) -> Int { x * x; }");
    REQUIRE(tokens[0].kind == TokenKind::KeywordFn);
    REQUIRE(tokens[1].kind == TokenKind::Identifier);
    REQUIRE(tokens[1].lexeme == "square");
    REQUIRE(tokens[2].kind == TokenKind::LParen);
    REQUIRE(tokens[3].kind == TokenKind::Identifier);
    REQUIRE(tokens[4].kind == TokenKind::Colon);
    REQUIRE(tokens[5].kind == TokenKind::KeywordInt);
    REQUIRE(tokens[6].kind == TokenKind::RParen);
    REQUIRE(tokens[7].kind == TokenKind::Arrow);
    REQUIRE(tokens[8].kind == TokenKind::KeywordInt);
    REQUIRE(tokens[9].kind == TokenKind::LBrace);
}

TEST_CASE("Lexer: block expression with filter and select", "[lexer]") {
    auto tokens = tokenize("df[filter price > 10, select { price }]");
    REQUIRE(first_of(tokens, TokenKind::Identifier) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::KeywordFilter) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::KeywordSelect) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::Gt) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::LBracket) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::RBracket) != nullptr);
}

TEST_CASE("Lexer: extern declaration", "[lexer]") {
    auto tokens = tokenize("extern fn read_csv(path: String) -> DataFrame from \"csv.hpp\";");
    REQUIRE(tokens[0].kind == TokenKind::KeywordExtern);
    REQUIRE(tokens[1].kind == TokenKind::KeywordFn);
    REQUIRE(first_of(tokens, TokenKind::KeywordFrom) != nullptr);
    REQUIRE(first_of(tokens, TokenKind::StringLiteral) != nullptr);
}

TEST_CASE("Lexer: join expression", "[lexer]") {
    auto tokens = tokenize("lhs asof join rhs on {ts, symbol}");
    REQUIRE(tokens[0].kind == TokenKind::Identifier);
    REQUIRE(tokens[0].lexeme == "lhs");
    REQUIRE(tokens[1].kind == TokenKind::KeywordAsof);
    REQUIRE(tokens[2].kind == TokenKind::KeywordJoin);
    REQUIRE(tokens[3].kind == TokenKind::Identifier);
    REQUIRE(tokens[3].lexeme == "rhs");
    REQUIRE(tokens[4].kind == TokenKind::KeywordOn);
}

TEST_CASE("Lexer: multiple tokens on one line preserve column positions", "[lexer]") {
    auto tokens = tokenize("a+b*c");
    REQUIRE(tokens[0].column == 1);  // a
    REQUIRE(tokens[1].column == 2);  // +
    REQUIRE(tokens[2].column == 3);  // b
    REQUIRE(tokens[3].column == 4);  // *
    REQUIRE(tokens[4].column == 5);  // c
}

TEST_CASE("Lexer: Eof token is always last", "[lexer]") {
    auto tokens = tokenize("1 + 2");
    REQUIRE(tokens.back().kind == TokenKind::Eof);
}
