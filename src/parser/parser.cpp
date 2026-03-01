#include <ibex/parser/lexer.hpp>
#include <ibex/parser/parser.hpp>

#include <fmt/core.h>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>
#include <utility>

namespace ibex::parser {

namespace {

class Parser {
   public:
    explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

    auto parse_program() -> std::expected<Program, ParseError> {
        Program program;
        while (!is_at_end()) {
            if (peek().kind == TokenKind::Error) {
                return std::unexpected(
                    make_error(peek(), fmt::format("invalid token {}", format_token(peek()))));
            }
            auto stmt = parse_statement();
            if (!stmt.has_value()) {
                return std::unexpected(error_);
            }
            program.statements.push_back(std::move(*stmt));
        }
        return program;
    }

   private:
    auto parse_statement() -> std::optional<Stmt> {
        if (match(TokenKind::KeywordExtern)) {
            return parse_extern_decl();
        }
        if (match(TokenKind::KeywordImport)) {
            return parse_import_decl();
        }
        if (match(TokenKind::KeywordFn)) {
            return parse_fn_decl();
        }
        if (match(TokenKind::KeywordLet)) {
            return parse_let_stmt();
        }
        return parse_expr_stmt();
    }

    /// Parse `import "name";` or `import name;`.
    ///
    /// The name identifies a library file (<name>.ibex) that the REPL will
    /// locate on the import search path and process automatically.
    auto parse_import_decl() -> std::optional<Stmt> {
        const std::size_t start_line = previous().line;
        std::string name;
        if (match(TokenKind::StringLiteral)) {
            name = unescape_string(previous().lexeme);
            // Strip any ".ibex" suffix so both `import "csv"` and
            // `import "csv.ibex"` are equivalent.
            constexpr std::string_view suffix = ".ibex";
            if (name.size() > suffix.size() &&
                name.substr(name.size() - suffix.size()) == suffix) {
                name.resize(name.size() - suffix.size());
            }
        } else {
            auto ident = consume_identifier("expected module name after 'import'");
            if (!ident.has_value()) {
                return std::nullopt;
            }
            name = std::move(*ident);
        }
        if (!consume(TokenKind::Semicolon, "expected ';' after import declaration")) {
            return std::nullopt;
        }
        return ImportDecl{
            .name = std::move(name),
            .start_line = start_line,
            .end_line = previous().line,
        };
    }

    auto parse_extern_decl() -> std::optional<Stmt> {
        const std::size_t start_line = previous().line;
        if (!consume(TokenKind::KeywordFn, "expected 'fn' after 'extern'")) {
            return std::nullopt;
        }
        auto name = consume_identifier("expected extern function name");
        if (!name.has_value()) {
            return std::nullopt;
        }
        if (!consume(TokenKind::LParen, "expected '(' after extern function name")) {
            return std::nullopt;
        }
        std::vector<Param> params;
        if (!check(TokenKind::RParen)) {
            do {
                auto param_name = consume_identifier("expected parameter name");
                if (!param_name.has_value()) {
                    return std::nullopt;
                }
                if (!consume(TokenKind::Colon, "expected ':' after parameter name")) {
                    return std::nullopt;
                }
                auto param_type = parse_type();
                if (!param_type.has_value()) {
                    return std::nullopt;
                }
                params.push_back(
                    Param{.name = std::move(*param_name), .type = std::move(*param_type)});
            } while (match(TokenKind::Comma));
        }
        if (!consume(TokenKind::RParen, "expected ')' after parameter list")) {
            return std::nullopt;
        }
        if (!consume(TokenKind::Arrow, "expected '->' after extern parameter list")) {
            return std::nullopt;
        }
        auto return_type = parse_type();
        if (!return_type.has_value()) {
            return std::nullopt;
        }
        if (!consume(TokenKind::KeywordFrom, "expected 'from' before extern source")) {
            return std::nullopt;
        }
        if (!consume(TokenKind::StringLiteral, "expected string literal after 'from'")) {
            return std::nullopt;
        }
        std::string source_path = unescape_string(previous().lexeme);
        if (!consume(TokenKind::Semicolon, "expected ';' after extern declaration")) {
            return std::nullopt;
        }
        return ExternDecl{
            .name = std::move(*name),
            .params = std::move(params),
            .return_type = std::move(*return_type),
            .source_path = std::move(source_path),
            .start_line = start_line,
            .end_line = previous().line,
        };
    }

    auto parse_fn_decl() -> std::optional<Stmt> {
        const std::size_t start_line = previous().line;
        auto name = consume_identifier("expected function name");
        if (!name.has_value()) {
            return std::nullopt;
        }
        if (!consume(TokenKind::LParen, "expected '(' after function name")) {
            return std::nullopt;
        }
        std::vector<Param> params;
        if (!check(TokenKind::RParen)) {
            do {
                auto param_name = consume_identifier("expected parameter name");
                if (!param_name.has_value()) {
                    return std::nullopt;
                }
                if (!consume(TokenKind::Colon, "expected ':' after parameter name")) {
                    return std::nullopt;
                }
                auto param_type = parse_type();
                if (!param_type.has_value()) {
                    return std::nullopt;
                }
                params.push_back(
                    Param{.name = std::move(*param_name), .type = std::move(*param_type)});
            } while (match(TokenKind::Comma));
        }
        if (!consume(TokenKind::RParen, "expected ')' after parameter list")) {
            return std::nullopt;
        }
        if (!consume(TokenKind::Arrow, "expected '->' after parameter list")) {
            return std::nullopt;
        }
        auto return_type = parse_type();
        if (!return_type.has_value()) {
            return std::nullopt;
        }
        if (!consume(TokenKind::LBrace, "expected '{' to start function body")) {
            return std::nullopt;
        }
        std::vector<FnStmt> body;
        while (!check(TokenKind::RBrace) && !is_at_end()) {
            if (match(TokenKind::KeywordExtern)) {
                error_ = make_error(previous(), "extern not allowed inside function body");
                return std::nullopt;
            }
            if (match(TokenKind::KeywordFn)) {
                error_ = make_error(previous(), "nested function not supported");
                return std::nullopt;
            }
            if (match(TokenKind::KeywordLet)) {
                auto stmt = parse_let_stmt();
                if (!stmt.has_value()) {
                    return std::nullopt;
                }
                body.push_back(std::get<LetStmt>(std::move(*stmt)));
            } else {
                auto stmt = parse_expr_stmt();
                if (!stmt.has_value()) {
                    return std::nullopt;
                }
                body.push_back(std::get<ExprStmt>(std::move(*stmt)));
            }
        }
        if (!consume(TokenKind::RBrace, "expected '}' after function body")) {
            return std::nullopt;
        }
        return FunctionDecl{
            .name = std::move(*name),
            .params = std::move(params),
            .return_type = std::move(*return_type),
            .body = std::move(body),
            .start_line = start_line,
            .end_line = previous().line,
        };
    }

    auto parse_let_stmt() -> std::optional<Stmt> {
        const std::size_t start_line = previous().line;
        bool is_mut = match(TokenKind::KeywordMut);
        auto name = consume_identifier("expected identifier after 'let'");
        if (!name.has_value()) {
            return std::nullopt;
        }
        std::optional<Type> annotated_type;
        if (match(TokenKind::Colon)) {
            auto parsed_type = parse_type();
            if (!parsed_type.has_value()) {
                return std::nullopt;
            }
            annotated_type = std::move(*parsed_type);
        }
        if (!consume(TokenKind::Eq, "expected '=' after let binding")) {
            return std::nullopt;
        }
        auto value = parse_expression();
        if (!value) {
            return std::nullopt;
        }
        if (!consume(TokenKind::Semicolon, "expected ';' after let binding")) {
            return std::nullopt;
        }
        return LetStmt{
            .is_mut = is_mut,
            .name = std::move(*name),
            .type = std::move(annotated_type),
            .value = std::move(value),
            .start_line = start_line,
            .end_line = previous().line,
        };
    }

    auto parse_expr_stmt() -> std::optional<Stmt> {
        const std::size_t start_line = peek().line;
        auto expr = parse_expression();
        if (!expr) {
            return std::nullopt;
        }
        if (!check(TokenKind::Semicolon)) {
            error_ = make_error(
                peek(), fmt::format("unexpected token {} after expression", format_token(peek())));
            return std::nullopt;
        }
        if (!consume(TokenKind::Semicolon, "expected ';' after expression")) {
            return std::nullopt;
        }
        return ExprStmt{
            .expr = std::move(expr),
            .start_line = start_line,
            .end_line = previous().line,
        };
    }

    auto parse_expression() -> ExprPtr { return parse_join(); }

    auto parse_join() -> ExprPtr {
        auto expr = parse_or();
        if (!expr) {
            return nullptr;
        }
        while (true) {
            JoinKind kind;
            if (match(TokenKind::KeywordJoin)) {
                kind = JoinKind::Inner;
            } else if (match(TokenKind::KeywordLeft)) {
                if (!consume(TokenKind::KeywordJoin, "expected 'join' after 'left'")) {
                    return nullptr;
                }
                kind = JoinKind::Left;
            } else if (match(TokenKind::KeywordAsof)) {
                if (!consume(TokenKind::KeywordJoin, "expected 'join' after 'asof'")) {
                    return nullptr;
                }
                kind = JoinKind::Asof;
            } else {
                break;
            }

            auto right = parse_or();
            if (!right) {
                return nullptr;
            }
            if (!consume(TokenKind::KeywordOn, "expected 'on' after join expression")) {
                return nullptr;
            }
            auto keys = parse_join_keys();
            if (!keys.has_value()) {
                return nullptr;
            }
            auto join = std::make_unique<Expr>();
            join->node = JoinExpr{
                .kind = kind,
                .left = std::move(expr),
                .right = std::move(right),
                .keys = std::move(*keys),
            };
            expr = std::move(join);
        }
        return expr;
    }

    auto parse_or() -> ExprPtr {
        auto expr = parse_and();
        while (match(TokenKind::PipePipe)) {
            auto right = parse_and();
            if (!right) {
                return nullptr;
            }
            expr = make_binary(BinaryOp::Or, std::move(expr), std::move(right));
        }
        return expr;
    }

    auto parse_and() -> ExprPtr {
        auto expr = parse_equality();
        while (match(TokenKind::AmpAmp)) {
            auto right = parse_equality();
            if (!right) {
                return nullptr;
            }
            expr = make_binary(BinaryOp::And, std::move(expr), std::move(right));
        }
        return expr;
    }

    auto parse_equality() -> ExprPtr {
        auto expr = parse_comparison();
        while (true) {
            if (match(TokenKind::EqEq)) {
                auto right = parse_comparison();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Eq, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::BangEq)) {
                auto right = parse_comparison();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Ne, std::move(expr), std::move(right));
                continue;
            }
            break;
        }
        return expr;
    }

    auto parse_comparison() -> ExprPtr {
        auto expr = parse_term();
        while (true) {
            if (match(TokenKind::Lt)) {
                auto right = parse_term();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Lt, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Le)) {
                auto right = parse_term();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Le, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Gt)) {
                auto right = parse_term();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Gt, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Ge)) {
                auto right = parse_term();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Ge, std::move(expr), std::move(right));
                continue;
            }
            break;
        }
        // Postfix: expr is null / expr is not null
        if (match(TokenKind::KeywordIs)) {
            if (match(TokenKind::KeywordNot)) {
                if (!consume(TokenKind::KeywordNull, "expected 'null' after 'is not'"))
                    return nullptr;
                return make_unary(UnaryOp::IsNotNull, std::move(expr));
            }
            if (!consume(TokenKind::KeywordNull, "expected 'null' after 'is'"))
                return nullptr;
            return make_unary(UnaryOp::IsNull, std::move(expr));
        }
        return expr;
    }

    auto parse_term() -> ExprPtr {
        auto expr = parse_factor();
        while (true) {
            if (match(TokenKind::Plus)) {
                auto right = parse_factor();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Add, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Minus)) {
                auto right = parse_factor();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Sub, std::move(expr), std::move(right));
                continue;
            }
            break;
        }
        return expr;
    }

    auto parse_factor() -> ExprPtr {
        auto expr = parse_unary();
        while (true) {
            if (match(TokenKind::Star)) {
                auto right = parse_unary();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Mul, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Slash)) {
                auto right = parse_unary();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Div, std::move(expr), std::move(right));
                continue;
            }
            if (match(TokenKind::Percent)) {
                auto right = parse_unary();
                if (!right) {
                    return nullptr;
                }
                expr = make_binary(BinaryOp::Mod, std::move(expr), std::move(right));
                continue;
            }
            break;
        }
        return expr;
    }

    auto parse_unary() -> ExprPtr {
        if (match(TokenKind::Minus)) {
            auto expr = parse_unary();
            if (!expr) {
                return nullptr;
            }
            return make_unary(UnaryOp::Negate, std::move(expr));
        }
        if (match(TokenKind::Bang)) {
            auto expr = parse_unary();
            if (!expr) {
                return nullptr;
            }
            return make_unary(UnaryOp::Not, std::move(expr));
        }
        return parse_postfix();
    }

    auto parse_postfix() -> ExprPtr {
        auto expr = parse_primary();
        if (!expr) {
            return nullptr;
        }
        while (match(TokenKind::LBracket)) {
            auto clauses = parse_clause_list();
            if (!clauses.has_value()) {
                return nullptr;
            }
            if (!consume(TokenKind::RBracket, "expected ']' after clause list")) {
                return nullptr;
            }
            auto block = std::make_unique<Expr>();
            block->node = BlockExpr{
                .base = std::move(expr),
                .clauses = std::move(*clauses),
            };
            expr = std::move(block);
        }
        return expr;
    }

    auto parse_join_keys() -> std::optional<std::vector<std::string>> {
        std::vector<std::string> keys;
        if (match(TokenKind::LBrace)) {
            if (!check(TokenKind::RBrace)) {
                do {
                    auto name = consume_column_identifier("expected join key");
                    if (!name.has_value()) {
                        return std::nullopt;
                    }
                    keys.push_back(std::move(*name));
                } while (match(TokenKind::Comma));
            }
            if (!consume(TokenKind::RBrace, "expected '}' after join key list")) {
                return std::nullopt;
            }
        } else {
            auto name = consume_column_identifier("expected join key");
            if (!name.has_value()) {
                return std::nullopt;
            }
            keys.push_back(std::move(*name));
        }
        if (keys.empty()) {
            error_ = make_error(peek(), "expected at least one join key");
            return std::nullopt;
        }
        return keys;
    }
    auto parse_primary() -> ExprPtr {
        if (match(TokenKind::Identifier)) {
            std::string name(previous().lexeme);
            if ((name == "date" || name == "timestamp" || name == "ts") &&
                check(TokenKind::StringLiteral)) {
                advance();
                std::string literal = unescape_string(previous().lexeme);
                if (name == "date") {
                    auto value = parse_date_literal(literal);
                    if (!value.has_value()) {
                        return fail_expr(previous(), "invalid date literal");
                    }
                    return make_literal(*value);
                }
                auto value = parse_timestamp_literal(literal);
                if (!value.has_value()) {
                    return fail_expr(previous(), "invalid timestamp literal");
                }
                return make_literal(*value);
            }
            if (match(TokenKind::LParen)) {
                std::vector<ExprPtr> args;
                if (!check(TokenKind::RParen)) {
                    do {
                        auto arg = parse_expression();
                        if (!arg) {
                            return nullptr;
                        }
                        args.push_back(std::move(arg));
                    } while (match(TokenKind::Comma));
                }
                if (!consume(TokenKind::RParen, "expected ')' after argument list")) {
                    return nullptr;
                }
                auto expr = std::make_unique<Expr>();
                expr->node = CallExpr{.callee = std::move(name), .args = std::move(args)};
                return expr;
            }
            auto expr = std::make_unique<Expr>();
            expr->node = IdentifierExpr{.name = std::move(name)};
            return expr;
        }
        if (match(TokenKind::QuotedIdentifier)) {
            std::string name = unescape_quoted_identifier(previous().lexeme);
            auto expr = std::make_unique<Expr>();
            expr->node = IdentifierExpr{.name = std::move(name)};
            return expr;
        }
        if (match(TokenKind::IntLiteral)) {
            auto value = parse_int(previous().lexeme);
            if (!value.has_value()) {
                return fail_expr(previous(), "invalid integer literal");
            }
            return make_literal(*value);
        }
        if (match(TokenKind::FloatLiteral)) {
            auto value = parse_double(previous().lexeme);
            if (!value.has_value()) {
                return fail_expr(previous(), "invalid float literal");
            }
            return make_literal(*value);
        }
        if (match(TokenKind::BoolLiteral)) {
            bool value = previous().lexeme == "true";
            return make_literal(value);
        }
        if (match(TokenKind::StringLiteral)) {
            return make_literal(unescape_string(previous().lexeme));
        }
        if (match(TokenKind::DurationLiteral)) {
            return make_literal(DurationLiteral{.text = std::string(previous().lexeme)});
        }
        if (match(TokenKind::LParen)) {
            auto expr = parse_expression();
            if (!expr) {
                return nullptr;
            }
            if (!consume(TokenKind::RParen, "expected ')' after expression")) {
                return nullptr;
            }
            auto group = std::make_unique<Expr>();
            group->node = GroupExpr{.expr = std::move(expr)};
            return group;
        }
        if (match(TokenKind::LBrace)) {
            auto expr = parse_expression();
            if (!expr) {
                return nullptr;
            }
            if (!consume(TokenKind::RBrace, "expected '}' after expression")) {
                return nullptr;
            }
            auto group = std::make_unique<Expr>();
            group->node = GroupExpr{.expr = std::move(expr)};
            return group;
        }
        return fail_expr(peek(), "expected expression");
    }

    auto parse_clause_list() -> std::optional<std::vector<Clause>> {
        std::vector<Clause> clauses;
        if (!check(TokenKind::RBracket)) {
            do {
                auto clause = parse_clause();
                if (!clause.has_value()) {
                    return std::nullopt;
                }
                clauses.push_back(std::move(*clause));
            } while (match(TokenKind::Comma));
        }
        return clauses;
    }

    auto parse_clause() -> std::optional<Clause> {
        if (match(TokenKind::KeywordFilter)) {
            auto predicate = parse_expression();
            if (!predicate) {
                return std::nullopt;
            }
            return FilterClause{.predicate = std::move(predicate)};
        }
        if (match(TokenKind::KeywordSelect)) {
            auto fields = parse_field_list_or_single();
            if (!fields.has_value()) {
                return std::nullopt;
            }
            return SelectClause{.fields = std::move(*fields)};
        }
        if (match(TokenKind::KeywordDistinct)) {
            auto fields = parse_field_list_or_single();
            if (!fields.has_value()) {
                return std::nullopt;
            }
            return DistinctClause{.fields = std::move(*fields)};
        }
        if (match(TokenKind::KeywordOrder)) {
            auto keys = parse_order_keys();
            if (!keys.has_value()) {
                return std::nullopt;
            }
            return OrderClause{.keys = std::move(keys->first), .is_braced = keys->second};
        }
        if (match(TokenKind::KeywordUpdate)) {
            auto fields = parse_field_list_or_single();
            if (!fields.has_value()) {
                return std::nullopt;
            }
            return UpdateClause{.fields = std::move(*fields)};
        }
        if (match(TokenKind::KeywordBy)) {
            if (match(TokenKind::LBrace)) {
                auto fields = parse_field_list_after_open_brace();
                if (!fields.has_value()) {
                    return std::nullopt;
                }
                return ByClause{.keys = std::move(*fields), .is_braced = true};
            }
            auto ident = consume_column_identifier("expected identifier after 'by'");
            if (!ident.has_value()) {
                return std::nullopt;
            }
            Field key{.name = std::move(*ident), .expr = nullptr};
            std::vector<Field> keys;
            keys.push_back(std::move(key));
            return ByClause{.keys = std::move(keys), .is_braced = false};
        }
        if (match(TokenKind::KeywordWindow)) {
            if (!consume(TokenKind::DurationLiteral, "expected duration literal after 'window'")) {
                return std::nullopt;
            }
            return WindowClause{.duration =
                                    DurationLiteral{.text = std::string(previous().lexeme)}};
        }
        if (match(TokenKind::KeywordResample)) {
            if (!consume(TokenKind::DurationLiteral,
                         "expected duration literal after 'resample'")) {
                return std::nullopt;
            }
            return ResampleClause{.duration =
                                      DurationLiteral{.text = std::string(previous().lexeme)}};
        }
        error_ = make_error(peek(), "expected clause");
        return std::nullopt;
    }

    auto parse_order_keys() -> std::optional<std::pair<std::vector<OrderKey>, bool>> {
        std::vector<OrderKey> keys;
        bool is_braced = false;

        if (check(TokenKind::Comma) || check(TokenKind::RBracket)) {
            return std::make_pair(std::move(keys), is_braced);
        }

        if (match(TokenKind::LBrace)) {
            is_braced = true;
            if (!check(TokenKind::RBrace)) {
                do {
                    auto key = parse_order_key();
                    if (!key.has_value()) {
                        return std::nullopt;
                    }
                    keys.push_back(std::move(*key));
                } while (match(TokenKind::Comma));
            }
            if (!consume(TokenKind::RBrace, "expected '}' after order list")) {
                return std::nullopt;
            }
            return std::make_pair(std::move(keys), is_braced);
        }

        auto key = parse_order_key();
        if (!key.has_value()) {
            return std::nullopt;
        }
        keys.push_back(std::move(*key));
        return std::make_pair(std::move(keys), is_braced);
    }

    auto parse_order_key() -> std::optional<OrderKey> {
        auto name = consume_column_identifier("expected order key");
        if (!name.has_value()) {
            return std::nullopt;
        }
        bool ascending = true;
        if (match(TokenKind::KeywordAsc)) {
            ascending = true;
        } else if (match(TokenKind::KeywordDesc)) {
            ascending = false;
        }
        return OrderKey{.name = std::move(*name), .ascending = ascending};
    }

    auto parse_field_list_or_single() -> std::optional<std::vector<Field>> {
        if (check(TokenKind::LBrace)) {
            if (!consume(TokenKind::LBrace, "expected '{' to start field list")) {
                return std::nullopt;
            }
            return parse_field_list_after_open_brace();
        }
        auto field = parse_single_field();
        if (!field.has_value()) {
            return std::nullopt;
        }
        std::vector<Field> fields;
        fields.push_back(std::move(*field));
        return fields;
    }

    auto parse_field_list_after_open_brace() -> std::optional<std::vector<Field>> {
        std::vector<Field> fields;
        if (!check(TokenKind::RBrace)) {
            do {
                auto name = consume_column_identifier("expected field name");
                if (!name.has_value()) {
                    return std::nullopt;
                }
                ExprPtr expr = nullptr;
                if (match(TokenKind::Eq)) {
                    auto value = parse_expression();
                    if (!value) {
                        return std::nullopt;
                    }
                    expr = std::move(value);
                }
                fields.push_back(Field{.name = std::move(*name), .expr = std::move(expr)});
            } while (match(TokenKind::Comma));
        }
        if (!consume(TokenKind::RBrace, "expected '}' after field list")) {
            return std::nullopt;
        }
        return fields;
    }

    auto parse_single_field() -> std::optional<Field> {
        auto name = consume_column_identifier("expected field name");
        if (!name.has_value()) {
            return std::nullopt;
        }
        ExprPtr expr = nullptr;
        if (match(TokenKind::Eq)) {
            auto value = parse_expression();
            if (!value) {
                return std::nullopt;
            }
            expr = std::move(value);
        }
        return Field{.name = std::move(*name), .expr = std::move(expr)};
    }

    auto parse_type() -> std::optional<Type> {
        if (auto scalar = parse_scalar_type()) {
            return Type{.kind = Type::Kind::Scalar, .arg = *scalar};
        }
        if (match(TokenKind::KeywordColumn)) {
            if (!consume(TokenKind::Lt, "expected '<' after 'Column'")) {
                return std::nullopt;
            }
            auto arg = parse_scalar_type();
            if (!arg.has_value()) {
                error_ = make_error(peek(), "expected scalar type in Column<T>");
                return std::nullopt;
            }
            if (!consume(TokenKind::Gt, "expected '>' after Column type argument")) {
                return std::nullopt;
            }
            return Type{.kind = Type::Kind::Series, .arg = *arg};
        }
        if (match(TokenKind::KeywordSeries)) {
            if (!consume(TokenKind::Lt, "expected '<' after 'Series'")) {
                return std::nullopt;
            }
            auto arg = parse_scalar_type();
            if (!arg.has_value()) {
                error_ = make_error(peek(), "expected scalar type in Series<T>");
                return std::nullopt;
            }
            if (!consume(TokenKind::Gt, "expected '>' after Series type argument")) {
                return std::nullopt;
            }
            return Type{.kind = Type::Kind::Series, .arg = *arg};
        }
        if (match(TokenKind::KeywordDataFrame)) {
            if (match(TokenKind::Lt)) {
                auto schema = parse_schema_type();
                if (!schema.has_value()) {
                    return std::nullopt;
                }
                if (!consume(TokenKind::Gt, "expected '>' after DataFrame type argument")) {
                    return std::nullopt;
                }
                return Type{.kind = Type::Kind::DataFrame, .arg = std::move(*schema)};
            }
            return Type{.kind = Type::Kind::DataFrame, .arg = SchemaType{}};
        }
        if (match(TokenKind::KeywordTimeFrame)) {
            if (match(TokenKind::Lt)) {
                auto schema = parse_schema_type();
                if (!schema.has_value()) {
                    return std::nullopt;
                }
                if (!consume(TokenKind::Gt, "expected '>' after TimeFrame type argument")) {
                    return std::nullopt;
                }
                return Type{.kind = Type::Kind::TimeFrame, .arg = std::move(*schema)};
            }
            return Type{.kind = Type::Kind::TimeFrame, .arg = SchemaType{}};
        }
        error_ = make_error(peek(), "expected type");
        return std::nullopt;
    }

    auto parse_schema_type() -> std::optional<SchemaType> {
        if (!consume(TokenKind::LBrace, "expected '{' to start schema type")) {
            return std::nullopt;
        }
        std::vector<SchemaField> fields;
        if (!check(TokenKind::RBrace)) {
            do {
                auto name = consume_column_identifier("expected schema field name");
                if (!name.has_value()) {
                    return std::nullopt;
                }
                if (!consume(TokenKind::Colon, "expected ':' after schema field name")) {
                    return std::nullopt;
                }
                auto scalar = parse_scalar_type();
                if (!scalar.has_value()) {
                    error_ = make_error(peek(), "expected scalar type in schema field");
                    return std::nullopt;
                }
                fields.push_back(SchemaField{.name = std::move(*name), .type = *scalar});
            } while (match(TokenKind::Comma));
        }
        if (!consume(TokenKind::RBrace, "expected '}' after schema type")) {
            return std::nullopt;
        }
        return SchemaType{.fields = std::move(fields)};
    }

    auto parse_scalar_type() -> std::optional<ScalarType> {
        if (match(TokenKind::KeywordInt)) {
            return ScalarType::Int64;
        }
        if (match(TokenKind::KeywordInt32)) {
            return ScalarType::Int32;
        }
        if (match(TokenKind::KeywordInt64)) {
            return ScalarType::Int64;
        }
        if (match(TokenKind::KeywordFloat32)) {
            return ScalarType::Float32;
        }
        if (match(TokenKind::KeywordFloat64)) {
            return ScalarType::Float64;
        }
        if (match(TokenKind::KeywordBool)) {
            return ScalarType::Bool;
        }
        if (match(TokenKind::KeywordString)) {
            return ScalarType::String;
        }
        if (match(TokenKind::KeywordDate)) {
            return ScalarType::Date;
        }
        if (match(TokenKind::KeywordTimestamp)) {
            return ScalarType::Timestamp;
        }
        return std::nullopt;
    }

    auto consume(TokenKind kind, std::string_view message) -> bool {
        if (check(kind)) {
            advance();
            return true;
        }
        error_ = make_error(peek(), message);
        return false;
    }

    auto consume_identifier(std::string_view message) -> std::optional<std::string> {
        if (match(TokenKind::Identifier)) {
            return std::string(previous().lexeme);
        }
        error_ = make_error(peek(), message);
        return std::nullopt;
    }

    auto consume_column_identifier(std::string_view message) -> std::optional<std::string> {
        if (match(TokenKind::Identifier)) {
            return std::string(previous().lexeme);
        }
        if (match(TokenKind::QuotedIdentifier)) {
            return unescape_quoted_identifier(previous().lexeme);
        }
        error_ = make_error(peek(), message);
        return std::nullopt;
    }

    auto check(TokenKind kind) const -> bool {
        if (is_at_end()) {
            return kind == TokenKind::Eof;
        }
        return peek().kind == kind;
    }

    auto match(TokenKind kind) -> bool {
        if (!check(kind)) {
            return false;
        }
        advance();
        return true;
    }

    auto advance() -> const Token& {
        if (!is_at_end()) {
            current_ += 1;
        }
        return previous();
    }

    auto is_at_end() const -> bool { return peek().kind == TokenKind::Eof; }

    auto peek() const -> const Token& { return tokens_[current_]; }

    auto previous() const -> const Token& { return tokens_[current_ - 1]; }

    static auto make_error(const Token& token, std::string_view message) -> ParseError {
        return ParseError{
            .message = std::string(message),
            .line = token.line,
            .column = token.column,
        };
    }

    static auto format_token(const Token& token) -> std::string {
        if (token.kind == TokenKind::Eof || token.lexeme.empty()) {
            return "'<eof>'";
        }
        return fmt::format("'{}'", std::string(token.lexeme));
    }

    auto fail_expr(const Token& token, std::string_view message) -> ExprPtr {
        error_ = make_error(token, message);
        return nullptr;
    }

    static auto parse_int(std::string_view text) -> std::optional<std::int64_t> {
        std::int64_t value = 0;
        auto result = std::from_chars(text.begin(), text.end(), value);
        if (result.ec != std::errc()) {
            return std::nullopt;
        }
        return value;
    }

    static auto parse_double(std::string_view text) -> std::optional<double> {
        std::string tmp(text);
        char* end = nullptr;
        double value = std::strtod(tmp.c_str(), &end);
        if (end == tmp.c_str()) {
            return std::nullopt;
        }
        return value;
    }

    static auto unescape_string(std::string_view text) -> std::string {
        if (text.size() < 2) {
            return std::string(text);
        }
        std::string result;
        result.reserve(text.size() - 2);
        for (std::size_t idx = 1; idx + 1 < text.size(); ++idx) {
            char ch = text[idx];
            if (ch == '\\' && idx + 1 < text.size() - 1) {
                char next = text[idx + 1];
                switch (next) {
                    case 'n':
                        result.push_back('\n');
                        break;
                    case 'r':
                        result.push_back('\r');
                        break;
                    case 't':
                        result.push_back('\t');
                        break;
                    case '0':
                        result.push_back('\0');
                        break;
                    case '"':
                        result.push_back('"');
                        break;
                    case '\\':
                        result.push_back('\\');
                        break;
                    default:
                        result.push_back(next);
                        break;
                }
                idx += 1;
                continue;
            }
            result.push_back(ch);
        }
        return result;
    }

    static auto unescape_quoted_identifier(std::string_view text) -> std::string {
        if (text.size() < 2) {
            return std::string(text);
        }
        std::string result;
        result.reserve(text.size() - 2);
        for (std::size_t idx = 1; idx + 1 < text.size(); ++idx) {
            char ch = text[idx];
            if (ch == '\\' && idx + 1 < text.size() - 1) {
                char next = text[idx + 1];
                switch (next) {
                    case '`':
                        result.push_back('`');
                        break;
                    case '\\':
                        result.push_back('\\');
                        break;
                    default:
                        result.push_back(next);
                        break;
                }
                idx += 1;
                continue;
            }
            result.push_back(ch);
        }
        return result;
    }

    static auto parse_date_literal(std::string_view text) -> std::optional<Date> {
        if (text.size() != 10 || text[4] != '-' || text[7] != '-') {
            return std::nullopt;
        }
        auto parse_int = [](std::string_view part) -> std::optional<int> {
            int value = 0;
            auto result = std::from_chars(part.data(), part.data() + part.size(), value);
            if (result.ec != std::errc()) {
                return std::nullopt;
            }
            return value;
        };
        auto year = parse_int(text.substr(0, 4));
        auto month = parse_int(text.substr(5, 2));
        auto day = parse_int(text.substr(8, 2));
        if (!year || !month || !day) {
            return std::nullopt;
        }
        using namespace std::chrono;
        year_month_day ymd{std::chrono::year{*year},
                           std::chrono::month{static_cast<unsigned>(*month)},
                           std::chrono::day{static_cast<unsigned>(*day)}};
        if (!ymd.ok()) {
            return std::nullopt;
        }
        sys_days days_since = sys_days{ymd};
        auto days = days_since.time_since_epoch().count();
        if (days < std::numeric_limits<std::int32_t>::min() ||
            days > std::numeric_limits<std::int32_t>::max()) {
            return std::nullopt;
        }
        return Date{static_cast<std::int32_t>(days)};
    }

    static auto parse_timestamp_literal(std::string_view text) -> std::optional<Timestamp> {
        if (text.size() < 19 || text[4] != '-' || text[7] != '-' ||
            (text[10] != 'T' && text[10] != ' ') || text[13] != ':' || text[16] != ':') {
            return std::nullopt;
        }
        auto parse_int = [](std::string_view part) -> std::optional<int> {
            int value = 0;
            auto result = std::from_chars(part.data(), part.data() + part.size(), value);
            if (result.ec != std::errc()) {
                return std::nullopt;
            }
            return value;
        };
        auto year = parse_int(text.substr(0, 4));
        auto month = parse_int(text.substr(5, 2));
        auto day = parse_int(text.substr(8, 2));
        auto hour = parse_int(text.substr(11, 2));
        auto minute = parse_int(text.substr(14, 2));
        auto second = parse_int(text.substr(17, 2));
        if (!year || !month || !day || !hour || !minute || !second) {
            return std::nullopt;
        }
        if (*hour > 23 || *minute > 59 || *second > 59 || *hour < 0 || *minute < 0 || *second < 0) {
            return std::nullopt;
        }
        std::size_t pos = 19;
        std::int64_t nanos = 0;
        if (pos < text.size() && text[pos] == '.') {
            pos += 1;
            std::size_t start = pos;
            while (pos < text.size() && std::isdigit(static_cast<unsigned char>(text[pos])) != 0) {
                pos += 1;
            }
            std::size_t digits = pos - start;
            if (digits == 0 || digits > 9) {
                return std::nullopt;
            }
            std::int64_t frac = 0;
            auto result = std::from_chars(text.data() + start, text.data() + pos, frac);
            if (result.ec != std::errc()) {
                return std::nullopt;
            }
            for (std::size_t i = digits; i < 9; ++i) {
                frac *= 10;
            }
            nanos = frac;
        }
        if (pos < text.size() && text[pos] == 'Z') {
            pos += 1;
        }
        if (pos != text.size()) {
            return std::nullopt;
        }
        using namespace std::chrono;
        year_month_day ymd{std::chrono::year{*year},
                           std::chrono::month{static_cast<unsigned>(*month)},
                           std::chrono::day{static_cast<unsigned>(*day)}};
        if (!ymd.ok()) {
            return std::nullopt;
        }
        sys_days day_point{ymd};

        constexpr std::int64_t kNanosPerSecond = 1'000'000'000;
        constexpr std::int64_t kNanosPerDay = 86'400 * kNanosPerSecond;
        const auto day_count_raw = day_point.time_since_epoch().count();
        if (day_count_raw < std::numeric_limits<std::int64_t>::min() ||
            day_count_raw > std::numeric_limits<std::int64_t>::max()) {
            return std::nullopt;
        }
        const auto day_count = static_cast<std::int64_t>(day_count_raw);
        const auto time_of_day_nanos =
            ((static_cast<std::int64_t>(*hour) * 3600 + static_cast<std::int64_t>(*minute) * 60 +
              static_cast<std::int64_t>(*second)) *
             kNanosPerSecond) +
            nanos;

        auto checked_mul = [](std::int64_t lhs, std::int64_t rhs) -> std::optional<std::int64_t> {
            if (lhs == 0 || rhs == 0) {
                return std::int64_t{0};
            }
            if (lhs > 0) {
                if (rhs > 0) {
                    if (lhs > std::numeric_limits<std::int64_t>::max() / rhs) {
                        return std::nullopt;
                    }
                } else if (rhs < std::numeric_limits<std::int64_t>::min() / lhs) {
                    return std::nullopt;
                }
            } else if (rhs > 0) {
                if (lhs < std::numeric_limits<std::int64_t>::min() / rhs) {
                    return std::nullopt;
                }
            } else if (lhs != 0 && rhs < std::numeric_limits<std::int64_t>::max() / lhs) {
                return std::nullopt;
            }
            return lhs * rhs;
        };
        auto checked_add = [](std::int64_t lhs, std::int64_t rhs) -> std::optional<std::int64_t> {
            if (rhs > 0 && lhs > std::numeric_limits<std::int64_t>::max() - rhs) {
                return std::nullopt;
            }
            if (rhs < 0 && lhs < std::numeric_limits<std::int64_t>::min() - rhs) {
                return std::nullopt;
            }
            return lhs + rhs;
        };

        auto day_nanos = checked_mul(day_count, kNanosPerDay);
        if (!day_nanos.has_value()) {
            return std::nullopt;
        }
        auto total = checked_add(*day_nanos, time_of_day_nanos);
        if (!total.has_value()) {
            return std::nullopt;
        }
        return Timestamp{*total};
    }

    static auto make_literal(std::int64_t value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = value};
        return expr;
    }

    static auto make_literal(double value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = value};
        return expr;
    }

    static auto make_literal(bool value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = value};
        return expr;
    }

    static auto make_literal(std::string value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = std::move(value)};
        return expr;
    }

    static auto make_literal(DurationLiteral value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = std::move(value)};
        return expr;
    }

    static auto make_literal(Date value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = value};
        return expr;
    }

    static auto make_literal(Timestamp value) -> ExprPtr {
        auto expr = std::make_unique<Expr>();
        expr->node = LiteralExpr{.value = value};
        return expr;
    }

    static auto make_unary(UnaryOp op, ExprPtr expr) -> ExprPtr {
        auto node = std::make_unique<Expr>();
        node->node = UnaryExpr{.op = op, .expr = std::move(expr)};
        return node;
    }

    static auto make_binary(BinaryOp op, ExprPtr left, ExprPtr right) -> ExprPtr {
        auto node = std::make_unique<Expr>();
        node->node = BinaryExpr{
            .op = op,
            .left = std::move(left),
            .right = std::move(right),
        };
        return node;
    }

    std::vector<Token> tokens_;
    std::size_t current_ = 0;
    ParseError error_{};
};

}  // namespace

auto ParseError::format() const -> std::string {
    return fmt::format("{}:{}: {}", line, column, message);
}

auto parse(std::string_view source) -> ParseResult {
    Parser parser(tokenize(source));
    return parser.parse_program();
}

}  // namespace ibex::parser
