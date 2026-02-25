#include <ibex/parser/parser.hpp>

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <variant>

namespace {

using namespace ibex::parser;

const Expr& require_expr(const ExprPtr& expr) {
    REQUIRE(expr != nullptr);
    return *expr;
}

const BinaryExpr& require_binary(const Expr& expr, BinaryOp op) {
    const auto* node = std::get_if<BinaryExpr>(&expr.node);
    REQUIRE(node != nullptr);
    REQUIRE(node->op == op);
    return *node;
}

const LiteralExpr& require_literal(const Expr& expr) {
    const auto* node = std::get_if<LiteralExpr>(&expr.node);
    REQUIRE(node != nullptr);
    return *node;
}

const IdentifierExpr& require_identifier(const Expr& expr) {
    const auto* node = std::get_if<IdentifierExpr>(&expr.node);
    REQUIRE(node != nullptr);
    return *node;
}

const CallExpr& require_call(const Expr& expr) {
    const auto* node = std::get_if<CallExpr>(&expr.node);
    REQUIRE(node != nullptr);
    return *node;
}

const BlockExpr& require_block(const Expr& expr) {
    const auto* node = std::get_if<BlockExpr>(&expr.node);
    REQUIRE(node != nullptr);
    return *node;
}

}  // namespace

TEST_CASE("Parse extern declaration with schema types") {
    const char* source =
        "extern fn read_csv(path: String, schema: DataFrame<{ id: Int64, name: String }>)"
        " -> DataFrame<{ id: Int64, name: String }> from \"csv.hpp\";";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExternDecl>(stmt));
    const auto& decl = std::get<ExternDecl>(stmt);
    REQUIRE(decl.name == "read_csv");
    REQUIRE(decl.params.size() == 2);
    REQUIRE(decl.params[0].name == "path");
    REQUIRE(decl.params[0].type.kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(decl.params[0].type.arg) == ScalarType::String);
    REQUIRE(decl.params[1].name == "schema");
    REQUIRE(decl.params[1].type.kind == Type::Kind::DataFrame);
    const auto& schema = std::get<SchemaType>(decl.params[1].type.arg);
    REQUIRE(schema.fields.size() == 2);
    REQUIRE(schema.fields[0].name == "id");
    REQUIRE(schema.fields[0].type == ScalarType::Int64);
    REQUIRE(schema.fields[1].name == "name");
    REQUIRE(schema.fields[1].type == ScalarType::String);
    REQUIRE(decl.return_type.kind == Type::Kind::DataFrame);
    REQUIRE(std::get<SchemaType>(decl.return_type.arg).fields.size() == 2);
    REQUIRE(decl.source_path == "csv.hpp");
}

TEST_CASE("Parse extern declaration with inferred schema") {
    const char* source = "extern fn read_csv(path: String) -> DataFrame from \"csv.hpp\";";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExternDecl>(stmt));
    const auto& decl = std::get<ExternDecl>(stmt);
    REQUIRE(decl.name == "read_csv");
    REQUIRE(decl.return_type.kind == Type::Kind::DataFrame);
    const auto& schema = std::get<SchemaType>(decl.return_type.arg);
    REQUIRE(schema.fields.empty());
    REQUIRE(decl.source_path == "csv.hpp");
}

TEST_CASE("Parse function declaration with typed params") {
    const char* source = "fn foo(col: Column<Int>, x: Int) -> Int { x; }";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<FunctionDecl>(stmt));
    const auto& fn = std::get<FunctionDecl>(stmt);
    REQUIRE(fn.name == "foo");
    REQUIRE(fn.params.size() == 2);
    REQUIRE(fn.params[0].name == "col");
    REQUIRE(fn.params[0].type.kind == Type::Kind::Series);
    REQUIRE(std::get<ScalarType>(fn.params[0].type.arg) == ScalarType::Int64);
    REQUIRE(fn.params[1].name == "x");
    REQUIRE(fn.params[1].type.kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(fn.params[1].type.arg) == ScalarType::Int64);
    REQUIRE(fn.return_type.kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(fn.return_type.arg) == ScalarType::Int64);
    REQUIRE(fn.body.size() == 1);
    REQUIRE(std::holds_alternative<ExprStmt>(fn.body.front()));
}

TEST_CASE("Parse let binding with precedence") {
    const char* source = "let mut x: Int64 = 1 + 2 * 3;";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<LetStmt>(stmt));
    const auto& let_stmt = std::get<LetStmt>(stmt);
    REQUIRE(let_stmt.is_mut);
    REQUIRE(let_stmt.name == "x");
    REQUIRE(let_stmt.type.has_value());
    REQUIRE(let_stmt.type->kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(let_stmt.type->arg) == ScalarType::Int64);

    const auto& expr = require_expr(let_stmt.value);
    const auto& add = require_binary(expr, BinaryOp::Add);
    const auto& left_lit = require_literal(require_expr(add.left));
    REQUIRE(std::get<std::int64_t>(left_lit.value) == 1);
    const auto& mul = require_binary(require_expr(add.right), BinaryOp::Mul);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(mul.left)).value) == 2);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(mul.right)).value) == 3);
}

TEST_CASE("Parse date and timestamp literals") {
    using namespace std::chrono;

    {
        auto result = parse("date\"2024-01-02\";");
        REQUIRE(result.has_value());
        const auto& stmt = result->statements.front();
        REQUIRE(std::holds_alternative<ExprStmt>(stmt));
        const auto& expr_stmt = std::get<ExprStmt>(stmt);
        const auto& lit = require_literal(require_expr(expr_stmt.expr));
        const auto& date = std::get<ibex::Date>(lit.value);
        auto expected =
            sys_days{year{2024} / month{1} / std::chrono::day{2}}.time_since_epoch().count();
        REQUIRE(date.days == expected);
    }

    {
        auto result = parse("timestamp\"2024-01-02T03:04:05.123456789Z\";");
        REQUIRE(result.has_value());
        const auto& stmt = result->statements.front();
        REQUIRE(std::holds_alternative<ExprStmt>(stmt));
        const auto& expr_stmt = std::get<ExprStmt>(stmt);
        const auto& lit = require_literal(require_expr(expr_stmt.expr));
        const auto& ts = std::get<ibex::Timestamp>(lit.value);
        auto day_point = sys_days{year{2024} / month{1} / std::chrono::day{2}};
        auto tp = day_point + hours{3} + minutes{4} + seconds{5} + nanoseconds{123'456'789};
        REQUIRE(ts.nanos == tp.time_since_epoch().count());
    }
}

TEST_CASE("Parse timestamp literal rejects out-of-range epoch nanos") {
    // 2262-04-12 exceeds int64 nanoseconds since Unix epoch.
    auto result = parse("timestamp\"2262-04-12T00:00:00Z\";");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parse expression statement with call") {
    const char* source = "foo(1, 2 + 3);";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExprStmt>(stmt));
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& call = require_call(require_expr(expr_stmt.expr));
    REQUIRE(call.callee == "foo");
    REQUIRE(call.args.size() == 2);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(call.args[0])).value) == 1);
    const auto& add = require_binary(require_expr(call.args[1]), BinaryOp::Add);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(add.left)).value) == 2);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(add.right)).value) == 3);
}

TEST_CASE("Parse block expression with filter and select") {
    const char* source = "df[filter price > 10, select { price, total = price * 2 }];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExprStmt>(stmt));
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 2);

    REQUIRE(std::holds_alternative<FilterClause>(block.clauses[0]));
    const auto& filter = std::get<FilterClause>(block.clauses[0]);
    const auto& gt = require_binary(require_expr(filter.predicate), BinaryOp::Gt);
    REQUIRE(require_identifier(require_expr(gt.left)).name == "price");
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(gt.right)).value) == 10);

    REQUIRE(std::holds_alternative<SelectClause>(block.clauses[1]));
    const auto& select = std::get<SelectClause>(block.clauses[1]);
    REQUIRE(select.fields.size() == 2);
    REQUIRE(select.fields[0].name == "price");
    REQUIRE(select.fields[0].expr == nullptr);
    REQUIRE(select.fields[1].name == "total");
    REQUIRE(select.fields[1].expr != nullptr);
}

TEST_CASE("Parse select without braces") {
    const char* source = "df[select price];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& select = std::get<SelectClause>(block.clauses[0]);
    REQUIRE(select.fields.size() == 1);
    REQUIRE(select.fields[0].name == "price");
    REQUIRE(select.fields[0].expr == nullptr);
}

TEST_CASE("Parse distinct without braces") {
    const char* source = "df[distinct symbol];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& distinct = std::get<DistinctClause>(block.clauses[0]);
    REQUIRE(distinct.fields.size() == 1);
    REQUIRE(distinct.fields[0].name == "symbol");
    REQUIRE(distinct.fields[0].expr == nullptr);
}

TEST_CASE("Parse order clause with keys") {
    const char* source = "df[order { symbol desc, price asc }];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& order = std::get<OrderClause>(block.clauses[0]);
    REQUIRE(order.keys.size() == 2);
    REQUIRE(order.keys[0].name == "symbol");
    REQUIRE(order.keys[0].ascending == false);
    REQUIRE(order.keys[1].name == "price");
    REQUIRE(order.keys[1].ascending == true);
}

TEST_CASE("Parse order clause with no keys") {
    const char* source = "df[order];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& order = std::get<OrderClause>(block.clauses[0]);
    REQUIRE(order.keys.empty());
}

TEST_CASE("Parse select assignment without braces") {
    const char* source = "df[select total = price * 2];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& select = std::get<SelectClause>(block.clauses[0]);
    REQUIRE(select.fields.size() == 1);
    REQUIRE(select.fields[0].name == "total");
    REQUIRE(select.fields[0].expr != nullptr);
}

TEST_CASE("Parse update without braces") {
    const char* source = "df[update price = price + 1];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.fields.size() == 1);
    REQUIRE(update.fields[0].name == "price");
    REQUIRE(update.fields[0].expr != nullptr);
}

TEST_CASE("Parse quoted identifiers in column references") {
    const char* source = "df[filter `Sepal.Length` > 10, select { `Sepal.Length` }];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExprStmt>(stmt));
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 2);

    const auto& filter = std::get<FilterClause>(block.clauses[0]);
    const auto& gt = require_binary(require_expr(filter.predicate), BinaryOp::Gt);
    REQUIRE(require_identifier(require_expr(gt.left)).name == "Sepal.Length");

    const auto& select = std::get<SelectClause>(block.clauses[1]);
    REQUIRE(select.fields.size() == 1);
    REQUIRE(select.fields[0].name == "Sepal.Length");
}

TEST_CASE("Parse by clause with braced keys") {
    const char* source = "df[by { symbol, yr = year(ts) }];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    REQUIRE(std::holds_alternative<ByClause>(block.clauses[0]));
    const auto& by = std::get<ByClause>(block.clauses[0]);
    REQUIRE(by.is_braced);
    REQUIRE(by.keys.size() == 2);
    REQUIRE(by.keys[0].name == "symbol");
    REQUIRE(by.keys[0].expr == nullptr);
    REQUIRE(by.keys[1].name == "yr");
    REQUIRE(by.keys[1].expr != nullptr);
}

TEST_CASE("Parse window clause") {
    const char* source = "tf[window 5m];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    REQUIRE(std::holds_alternative<WindowClause>(block.clauses[0]));
    const auto& window = std::get<WindowClause>(block.clauses[0]);
    REQUIRE(window.duration.text == "5m");
}

TEST_CASE("Parse error for missing semicolon") {
    const char* source = "let x = 1";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("';'") != std::string::npos);
}

TEST_CASE("Parse error includes unexpected token lexeme") {
    const char* source = "1 1;";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("'1'") != std::string::npos);
}

TEST_CASE("Lexer error includes invalid numeric literal lexeme") {
    const char* source = "1dfsd1;";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("1dfsd1") != std::string::npos);
}
