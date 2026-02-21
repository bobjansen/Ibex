#include <ibex/parser/lexer.hpp>

#include <cctype>
#include <unordered_map>

namespace ibex::parser {

auto tokenize(std::string_view source) -> std::vector<Token> {
    std::vector<Token> tokens;

    const auto add_token = [&](TokenKind kind, std::size_t start, std::size_t length,
                               std::size_t line, std::size_t column) {
        tokens.push_back(Token{
            .kind = kind,
            .lexeme = source.substr(start, length),
            .line = line,
            .column = column,
        });
    };

    const std::unordered_map<std::string_view, TokenKind> keywords = {
        {"let", TokenKind::KeywordLet},
        {"mut", TokenKind::KeywordMut},
        {"extern", TokenKind::KeywordExtern},
        {"fn", TokenKind::KeywordFn},
        {"from", TokenKind::KeywordFrom},
        {"filter", TokenKind::KeywordFilter},
        {"select", TokenKind::KeywordSelect},
        {"update", TokenKind::KeywordUpdate},
        {"by", TokenKind::KeywordBy},
        {"window", TokenKind::KeywordWindow},
        {"join", TokenKind::KeywordJoin},
        {"left", TokenKind::KeywordLeft},
        {"asof", TokenKind::KeywordAsof},
        {"on", TokenKind::KeywordOn},
        {"Int32", TokenKind::KeywordInt32},
        {"Int64", TokenKind::KeywordInt64},
        {"Float32", TokenKind::KeywordFloat32},
        {"Float64", TokenKind::KeywordFloat64},
        {"Bool", TokenKind::KeywordBool},
        {"String", TokenKind::KeywordString},
        {"Timestamp", TokenKind::KeywordTimestamp},
        {"Series", TokenKind::KeywordSeries},
        {"DataFrame", TokenKind::KeywordDataFrame},
        {"TimeFrame", TokenKind::KeywordTimeFrame},
    };

    const auto is_ident_start = [](unsigned char ch) -> bool {
        return std::isalpha(ch) != 0 || ch == '_';
    };

    const auto is_ident_cont = [](unsigned char ch) -> bool {
        return std::isalnum(ch) != 0 || ch == '_';
    };

    std::size_t i = 0;
    std::size_t line = 1;
    std::size_t column = 1;

    const auto at_end = [&]() -> bool {
        return i >= source.size();
    };
    const auto peek = [&](std::size_t offset = 0) -> char {
        if (i + offset >= source.size()) {
            return '\0';
        }
        return source[i + offset];
    };
    const auto advance = [&]() -> char {
        if (at_end()) {
            return '\0';
        }
        char ch = source[i++];
        if (ch == '\n') {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
        return ch;
    };

    const auto match = [&](char expected) -> bool {
        if (peek() != expected) {
            return false;
        }
        advance();
        return true;
    };

    const auto skip_whitespace_and_comments = [&]() {
        while (!at_end()) {
            char ch = peek();
            if (ch == ' ' || ch == '\r' || ch == '\t' || ch == '\n') {
                advance();
                continue;
            }
            if (ch == '/' && peek(1) == '/') {
                while (!at_end() && peek() != '\n') {
                    advance();
                }
                continue;
            }
            if (ch == '/' && peek(1) == '*') {
                advance();
                advance();
                while (!at_end()) {
                    if (peek() == '*' && peek(1) == '/') {
                        advance();
                        advance();
                        break;
                    }
                    advance();
                }
                continue;
            }
            break;
        }
    };

    const auto is_duration_unit = [](std::string_view unit) -> bool {
        return unit == "ns" || unit == "us" || unit == "ms" || unit == "s" || unit == "m" ||
               unit == "h" || unit == "d" || unit == "w" || unit == "mo" || unit == "y";
    };

    while (!at_end()) {
        skip_whitespace_and_comments();
        if (at_end()) {
            break;
        }

        std::size_t token_start = i;
        std::size_t token_line = line;
        std::size_t token_column = column;
        char ch = advance();

        if (is_ident_start(static_cast<unsigned char>(ch))) {
            while (is_ident_cont(static_cast<unsigned char>(peek()))) {
                advance();
            }
            std::string_view text = source.substr(token_start, i - token_start);
            if (text == "true" || text == "false") {
                add_token(TokenKind::BoolLiteral, token_start, i - token_start, token_line,
                          token_column);
                continue;
            }
            if (auto it = keywords.find(text); it != keywords.end()) {
                add_token(it->second, token_start, i - token_start, token_line, token_column);
                continue;
            }
            add_token(TokenKind::Identifier, token_start, i - token_start, token_line,
                      token_column);
            continue;
        }

        if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            while (std::isdigit(static_cast<unsigned char>(peek())) != 0) {
                advance();
            }
            bool is_float = false;
            if (peek() == '.' && std::isdigit(static_cast<unsigned char>(peek(1))) != 0) {
                is_float = true;
                advance();
                while (std::isdigit(static_cast<unsigned char>(peek())) != 0) {
                    advance();
                }
                if (peek() == 'e' || peek() == 'E') {
                    advance();
                    if (peek() == '+' || peek() == '-') {
                        advance();
                    }
                    while (std::isdigit(static_cast<unsigned char>(peek())) != 0) {
                        advance();
                    }
                }
            }

            if (!is_float) {
                std::size_t unit_start = i;
                if (is_ident_start(static_cast<unsigned char>(peek()))) {
                    while (is_ident_cont(static_cast<unsigned char>(peek()))) {
                        advance();
                    }
                }
                std::string_view unit = source.substr(unit_start, i - unit_start);
                if (!unit.empty() && is_duration_unit(unit)) {
                    add_token(TokenKind::DurationLiteral, token_start, i - token_start, token_line,
                              token_column);
                    continue;
                }
                if (!unit.empty()) {
                    add_token(TokenKind::Error, token_start, i - token_start, token_line,
                              token_column);
                    continue;
                }
            }

            add_token(is_float ? TokenKind::FloatLiteral : TokenKind::IntLiteral, token_start,
                      i - token_start, token_line, token_column);
            continue;
        }

        switch (ch) {
            case '"': {
                while (!at_end() && peek() != '"') {
                    if (peek() == '\\' && peek(1) != '\0') {
                        advance();
                    }
                    advance();
                }
                if (at_end()) {
                    add_token(TokenKind::Error, token_start, i - token_start, token_line,
                              token_column);
                    continue;
                }
                advance();
                add_token(TokenKind::StringLiteral, token_start, i - token_start, token_line,
                          token_column);
                continue;
            }
            case '+':
                add_token(TokenKind::Plus, token_start, 1, token_line, token_column);
                continue;
            case '-':
                if (match('>')) {
                    add_token(TokenKind::Arrow, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Minus, token_start, 1, token_line, token_column);
                }
                continue;
            case '*':
                add_token(TokenKind::Star, token_start, 1, token_line, token_column);
                continue;
            case '/':
                add_token(TokenKind::Slash, token_start, 1, token_line, token_column);
                continue;
            case '%':
                add_token(TokenKind::Percent, token_start, 1, token_line, token_column);
                continue;
            case '!':
                if (match('=')) {
                    add_token(TokenKind::BangEq, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Bang, token_start, 1, token_line, token_column);
                }
                continue;
            case '=':
                if (match('=')) {
                    add_token(TokenKind::EqEq, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Eq, token_start, 1, token_line, token_column);
                }
                continue;
            case '<':
                if (match('=')) {
                    add_token(TokenKind::Le, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Lt, token_start, 1, token_line, token_column);
                }
                continue;
            case '>':
                if (match('=')) {
                    add_token(TokenKind::Ge, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Gt, token_start, 1, token_line, token_column);
                }
                continue;
            case '&':
                if (match('&')) {
                    add_token(TokenKind::AmpAmp, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Error, token_start, 1, token_line, token_column);
                }
                continue;
            case '|':
                if (match('|')) {
                    add_token(TokenKind::PipePipe, token_start, 2, token_line, token_column);
                } else {
                    add_token(TokenKind::Error, token_start, 1, token_line, token_column);
                }
                continue;
            case '^':
                add_token(TokenKind::Caret, token_start, 1, token_line, token_column);
                continue;
            case '(':
                add_token(TokenKind::LParen, token_start, 1, token_line, token_column);
                continue;
            case ')':
                add_token(TokenKind::RParen, token_start, 1, token_line, token_column);
                continue;
            case '[':
                add_token(TokenKind::LBracket, token_start, 1, token_line, token_column);
                continue;
            case ']':
                add_token(TokenKind::RBracket, token_start, 1, token_line, token_column);
                continue;
            case '{':
                add_token(TokenKind::LBrace, token_start, 1, token_line, token_column);
                continue;
            case '}':
                add_token(TokenKind::RBrace, token_start, 1, token_line, token_column);
                continue;
            case ',':
                add_token(TokenKind::Comma, token_start, 1, token_line, token_column);
                continue;
            case ';':
                add_token(TokenKind::Semicolon, token_start, 1, token_line, token_column);
                continue;
            case ':':
                add_token(TokenKind::Colon, token_start, 1, token_line, token_column);
                continue;
            default:
                add_token(TokenKind::Error, token_start, 1, token_line, token_column);
                continue;
        }
    }

    tokens.push_back(Token{
        .kind = TokenKind::Eof,
        .lexeme = source.substr(source.size(), 0),
        .line = line,
        .column = column,
    });
    return tokens;
}

}  // namespace ibex::parser
