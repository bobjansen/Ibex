#include <ibex/parser/parser.hpp>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
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

const JoinExpr& require_join(const Expr& expr) {
    const auto* node = std::get_if<JoinExpr>(&expr.node);
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
    REQUIRE(decl.params[0].effect == Param::Effect::Const);
    REQUIRE(decl.params[1].name == "schema");
    REQUIRE(decl.params[1].type.kind == Type::Kind::DataFrame);
    REQUIRE(decl.params[1].effect == Param::Effect::Const);
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
    REQUIRE(fn.params[0].effect == Param::Effect::Const);
    REQUIRE(fn.params[1].name == "x");
    REQUIRE(fn.params[1].type.kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(fn.params[1].type.arg) == ScalarType::Int64);
    REQUIRE(fn.params[1].effect == Param::Effect::Const);
    REQUIRE(fn.return_type.kind == Type::Kind::Scalar);
    REQUIRE(std::get<ScalarType>(fn.return_type.arg) == ScalarType::Int64);
    REQUIRE(fn.body.size() == 1);
    REQUIRE(std::holds_alternative<ExprStmt>(fn.body.front()));
}

TEST_CASE("Parse extern and function declarations with effects") {
    const char* source = R"(
extern fn read_csv(path: String) -> DataFrame effects { io_read("file"), may_fail } from "csv.hpp";
fn identity(df: DataFrame) -> DataFrame effects {} {
  df;
}
)";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 2);

    const auto& ext = std::get<ExternDecl>(result->statements[0]);
    REQUIRE(ext.effects.has_value());
    REQUIRE(ext.effects->size() == 2);
    REQUIRE(ext.effects->at(0).kind == EffectKind::IoRead);
    REQUIRE(ext.effects->at(0).resource.has_value());
    REQUIRE(*ext.effects->at(0).resource == "file");
    REQUIRE(ext.effects->at(1).kind == EffectKind::MayFail);

    const auto& fn = std::get<FunctionDecl>(result->statements[1]);
    REQUIRE(fn.effects.has_value());
    REQUIRE(fn.effects->empty());
}

TEST_CASE("Parse rejects unknown effect kind") {
    auto result = parse("fn f(x: Int) -> Int effects { unknown_effect } { x; }");
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("unknown effect kind") != std::string::npos);
}

TEST_CASE("Parse parameter effect modifiers") {
    const char* source = R"(
extern fn ext(const path: String, mutable out: DataFrame, consume payload: String)
    -> Int
    effects { io_write, may_fail }
    from "x.hpp";
fn f(const x: Int, mutable y: Int, consume z: Int) -> Int {
  x;
}
)";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 2);

    const auto& ext = std::get<ExternDecl>(result->statements[0]);
    REQUIRE(ext.params.size() == 3);
    REQUIRE(ext.params[0].effect == Param::Effect::Const);
    REQUIRE(ext.params[1].effect == Param::Effect::Mutable);
    REQUIRE(ext.params[2].effect == Param::Effect::Consume);

    const auto& fn = std::get<FunctionDecl>(result->statements[1]);
    REQUIRE(fn.params.size() == 3);
    REQUIRE(fn.params[0].effect == Param::Effect::Const);
    REQUIRE(fn.params[1].effect == Param::Effect::Mutable);
    REQUIRE(fn.params[2].effect == Param::Effect::Consume);
}

TEST_CASE("Parse allows parameter names that match modifier words") {
    auto result = parse("fn f(mutable: Int, consume: Int, const: Int) -> Int { const; }");
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);
    const auto& fn = std::get<FunctionDecl>(result->statements[0]);
    REQUIRE(fn.params.size() == 3);
    REQUIRE(fn.params[0].name == "mutable");
    REQUIRE(fn.params[0].effect == Param::Effect::Const);
    REQUIRE(fn.params[1].name == "consume");
    REQUIRE(fn.params[1].effect == Param::Effect::Const);
    REQUIRE(fn.params[2].name == "const");
    REQUIRE(fn.params[2].effect == Param::Effect::Const);
}

TEST_CASE("Parse optimization showcase example script") {
    const auto script_path =
        std::filesystem::path(IBEX_SOURCE_DIR) / "examples" / "optimization_showcase.ibex";
    std::ifstream input(script_path);
    REQUIRE(input.is_open());

    std::stringstream source;
    source << input.rdbuf();

    auto result = parse(source.str());
    REQUIRE(result.has_value());
}

TEST_CASE("Effect checking: user fn annotation must include inferred extern effects") {
    const char* source = R"(
extern fn write_csv(df: DataFrame, path: String) -> Int effects { io_write } from "csv.hpp";
fn save(df: DataFrame) -> DataFrame effects {} {
  write_csv(df, "out.csv");
  df;
}
)";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("missing: io_write") != std::string::npos);
}

TEST_CASE("Effect checking: annotation passes when inferred effects are declared") {
    const char* source = R"(
extern fn write_csv(df: DataFrame, path: String) -> Int effects { io_write } from "csv.hpp";
fn save(df: DataFrame) -> DataFrame effects { io_write } {
  write_csv(df, "out.csv");
  df;
}
)";
    auto result = parse(source);
    REQUIRE(result.has_value());
}

TEST_CASE("Effect checking: unannotated extern defaults to conservative effects") {
    const char* source = R"(
extern fn ext(x: Int) -> Int from "x.hpp";
fn f(x: Int) -> Int effects {} {
  ext(x);
  x;
}
)";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("io_read") != std::string::npos);
}

TEST_CASE("Effect checking: transitive effects through user function calls") {
    const char* source = R"(
fn inner() -> Int effects { nondet } {
  rand_int(1, 6);
  1;
}
fn wrapper() -> Int effects {} {
  inner();
  2;
}
)";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("nondet") != std::string::npos);
}

TEST_CASE("Effect checking: io resource mismatch is reported precisely") {
    const char* source = R"(
extern fn read_csv(path: String) -> DataFrame effects { io_read("file"), may_fail } from "csv.hpp";
fn load(path: String) -> DataFrame effects { io_read("s3"), may_fail } {
  read_csv(path);
}
)";
    auto result = parse(source);
    REQUIRE_FALSE(result.has_value());
    REQUIRE(result.error().message.find("io_read(\"file\")") != std::string::npos);
}

TEST_CASE("Effect checking: unscoped io annotation covers scoped inferred resources") {
    const char* source = R"(
extern fn read_csv(path: String) -> DataFrame effects { io_read("file"), may_fail } from "csv.hpp";
fn load(path: String) -> DataFrame effects { io_read, may_fail } {
  read_csv(path);
}
)";
    auto result = parse(source);
    REQUIRE(result.has_value());
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

TEST_CASE("Parse statements track source line ranges") {
    const char* source = R"(// header comment
let x = 1;

foo();
fn twice(v: Int) -> Int {
  let y = v + v;
  y;
}
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
)";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 4);

    const auto& let_stmt = std::get<LetStmt>(result->statements[0]);
    REQUIRE(let_stmt.start_line == 2);
    REQUIRE(let_stmt.end_line == 2);

    const auto& expr_stmt = std::get<ExprStmt>(result->statements[1]);
    REQUIRE(expr_stmt.start_line == 4);
    REQUIRE(expr_stmt.end_line == 4);

    const auto& fn = std::get<FunctionDecl>(result->statements[2]);
    REQUIRE(fn.start_line == 5);
    REQUIRE(fn.end_line == 8);

    const auto& ext = std::get<ExternDecl>(result->statements[3]);
    REQUIRE(ext.start_line == 9);
    REQUIRE(ext.end_line == 9);
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

TEST_CASE("Parse asof join expression with multiple keys") {
    const char* source = "lhs asof join rhs on {ts, symbol};";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<ExprStmt>(stmt));
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Asof);
    REQUIRE(join.keys == std::vector<std::string>{"ts", "symbol"});
    REQUIRE(require_identifier(require_expr(join.left)).name == "lhs");
    REQUIRE(require_identifier(require_expr(join.right)).name == "rhs");
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

TEST_CASE("Parse head clause") {
    const char* source = "df[head 10];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& head = std::get<HeadClause>(block.clauses[0]);
    REQUIRE(head.count == 10);
}

TEST_CASE("Parse tail clause") {
    const char* source = "df[tail 10];";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& tail = std::get<TailClause>(block.clauses[0]);
    REQUIRE(tail.count == 10);
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

TEST_CASE("Parse select map field") {
    const char* source = R"(
let cols = ["price", "fee"];
df[select { map c in cols => `avg_${c}` = mean(get(c)) }, by symbol];
)";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 2);

    const auto& stmt = result->statements[1];
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 2);

    const auto& select = std::get<SelectClause>(block.clauses[0]);
    REQUIRE(select.fields.empty());
    REQUIRE(select.map_fields.size() == 1);
    REQUIRE(select.map_fields[0].bindings.size() == 1);
    REQUIRE(select.map_fields[0].bindings[0].value_name == "c");
    REQUIRE_FALSE(select.map_fields[0].bindings[0].index_name.has_value());
    REQUIRE(select.map_fields[0].bindings[0].source_name == "cols");
    REQUIRE(select.map_fields[0].alias_template == "avg_${c}");
    REQUIRE(select.map_fields[0].expr != nullptr);
}

TEST_CASE("Parse update map field with indexed bindings and where clause") {
    const char* source = R"(
let cols = ["ask_price", "bid_price", "wap"];
df[update {
    map (i, a) in cols, (j, b) in cols where i > j => `${a}_${b}_imb` = (get(a) - get(b)) / (get(a) + get(b))
}];
)";

    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 2);

    const auto& stmt = result->statements[1];
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);

    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.fields.empty());
    REQUIRE(update.map_fields.size() == 1);
    REQUIRE(update.map_fields[0].bindings.size() == 2);
    REQUIRE(update.map_fields[0].bindings[0].index_name == "i");
    REQUIRE(update.map_fields[0].bindings[0].value_name == "a");
    REQUIRE(update.map_fields[0].bindings[1].index_name == "j");
    REQUIRE(update.map_fields[0].bindings[1].value_name == "b");
    REQUIRE(update.map_fields[0].where_expr != nullptr);
    REQUIRE(update.map_fields[0].alias_template == "${a}_${b}_imb");
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

// --- Unary expressions ------------------------------------------------------

TEST_CASE("Parse unary negation") {
    const char* source = "let x = -42;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<LetStmt>(stmt));
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    const auto* unary = std::get_if<UnaryExpr>(&expr.node);
    REQUIRE(unary != nullptr);
    REQUIRE(unary->op == UnaryOp::Negate);
}

TEST_CASE("Parse logical NOT") {
    const char* source = "df[filter !active];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    const auto& filter = std::get<FilterClause>(block.clauses[0]);
    const auto* unary = std::get_if<UnaryExpr>(&require_expr(filter.predicate).node);
    REQUIRE(unary != nullptr);
    REQUIRE(unary->op == UnaryOp::Not);
}

// --- Boolean literals --------------------------------------------------------

TEST_CASE("Parse boolean true literal") {
    const char* source = "let x = true;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    const auto& lit = require_literal(expr);
    REQUIRE(std::get<bool>(lit.value) == true);
}

TEST_CASE("Parse boolean false literal") {
    const char* source = "let x = false;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    const auto& lit = require_literal(expr);
    REQUIRE(std::get<bool>(lit.value) == false);
}

// --- Float literals ----------------------------------------------------------

TEST_CASE("Parse float literal in expression") {
    const char* source = "let x = 3.14;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    const auto& lit = require_literal(expr);
    REQUIRE(std::get<double>(lit.value) == Catch::Approx(3.14));
}

// --- String literal ----------------------------------------------------------

TEST_CASE("Parse string literal") {
    const char* source = R"(let x = "hello world";)";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    const auto& lit = require_literal(expr);
    REQUIRE(std::get<std::string>(lit.value) == "hello world");
}

// --- Rename clause -----------------------------------------------------------

TEST_CASE("Parse rename clause with braces") {
    const char* source = "df[rename { cost = price, amount = qty }];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    REQUIRE(std::holds_alternative<RenameClause>(block.clauses[0]));
    const auto& rename = std::get<RenameClause>(block.clauses[0]);
    REQUIRE(rename.fields.size() == 2);
    REQUIRE(rename.fields[0].name == "cost");
    REQUIRE(rename.fields[1].name == "amount");
}

TEST_CASE("Parse rename clause without braces") {
    const char* source = "df[rename cost = price];";
    auto result = parse(source);
    REQUIRE(result.has_value());

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    const auto& rename = std::get<RenameClause>(block.clauses[0]);
    REQUIRE(rename.fields.size() == 1);
    REQUIRE(rename.fields[0].name == "cost");
}

// --- Resample clause ---------------------------------------------------------

TEST_CASE("Parse resample clause") {
    const char* source = "tf[resample 1m, select { symbol, open = first(price) }, by symbol];";
    auto result = parse(source);
    REQUIRE(result.has_value());

    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 3);
    REQUIRE(std::holds_alternative<ResampleClause>(block.clauses[0]));
    const auto& resample = std::get<ResampleClause>(block.clauses[0]);
    REQUIRE(resample.duration.text == "1m");
}

// --- Import declaration -----------------------------------------------------

TEST_CASE("Parse import declaration with identifier") {
    const char* source = "import csv;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);
    REQUIRE(std::holds_alternative<ImportDecl>(result->statements.front()));
    const auto& import = std::get<ImportDecl>(result->statements.front());
    REQUIRE(import.name == "csv");
}

TEST_CASE("Parse import declaration with string") {
    const char* source = R"(import "parquet";)";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);
    REQUIRE(std::holds_alternative<ImportDecl>(result->statements.front()));
    const auto& import = std::get<ImportDecl>(result->statements.front());
    REQUIRE(import.name == "parquet");
}

// --- Multiple statements ----------------------------------------------------

TEST_CASE("Parse multiple statements in sequence") {
    const char* source = R"(
let x = 1;
let y = 2;
foo(x, y);
)";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 3);
    REQUIRE(std::holds_alternative<LetStmt>(result->statements[0]));
    REQUIRE(std::holds_alternative<LetStmt>(result->statements[1]));
    REQUIRE(std::holds_alternative<ExprStmt>(result->statements[2]));
}

// --- Nested call expressions -------------------------------------------------

TEST_CASE("Parse nested function calls") {
    const char* source = "foo(bar(1), baz(2, 3));";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& call = require_call(require_expr(expr_stmt.expr));
    REQUIRE(call.callee == "foo");
    REQUIRE(call.args.size() == 2);
    const auto& inner1 = require_call(require_expr(call.args[0]));
    REQUIRE(inner1.callee == "bar");
    const auto& inner2 = require_call(require_expr(call.args[1]));
    REQUIRE(inner2.callee == "baz");
    REQUIRE(inner2.args.size() == 2);
}

// --- Operator precedence ----------------------------------------------------

TEST_CASE("Parse comparison has lower precedence than arithmetic") {
    const char* source = "let x = a + b > c * d;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    // Top level should be Gt (comparison)
    const auto& gt = require_binary(expr, BinaryOp::Gt);
    // Left should be Add
    require_binary(require_expr(gt.left), BinaryOp::Add);
    // Right should be Mul
    require_binary(require_expr(gt.right), BinaryOp::Mul);
}

TEST_CASE("Parse AND has lower precedence than comparison") {
    const char* source = "df[filter a > 1 && b < 2];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    const auto& filter = std::get<FilterClause>(block.clauses[0]);
    // Top level should be And
    const auto& and_expr = require_binary(require_expr(filter.predicate), BinaryOp::And);
    // Children should be comparisons
    require_binary(require_expr(and_expr.left), BinaryOp::Gt);
    require_binary(require_expr(and_expr.right), BinaryOp::Lt);
}

TEST_CASE("Parse OR has lower precedence than AND") {
    const char* source = "df[filter a > 1 || b > 2 && c > 3];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    const auto& filter = std::get<FilterClause>(block.clauses[0]);
    // Top level should be Or
    const auto& or_expr = require_binary(require_expr(filter.predicate), BinaryOp::Or);
    // Left is just a > 1
    require_binary(require_expr(or_expr.left), BinaryOp::Gt);
    // Right is AND of b > 2 && c > 3
    require_binary(require_expr(or_expr.right), BinaryOp::And);
}

// --- Parenthesized expressions -----------------------------------------------

TEST_CASE("Parse parenthesized expression overrides precedence") {
    const char* source = "let x = (1 + 2) * 3;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    const auto& expr = require_expr(let_stmt.value);
    // Top level should be Mul
    const auto& mul = require_binary(expr, BinaryOp::Mul);
    REQUIRE(std::get<std::int64_t>(require_literal(require_expr(mul.right)).value) == 3);
}

// --- Let binding without type annotation -------------------------------------

TEST_CASE("Parse let binding without type annotation") {
    const char* source = "let x = 42;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& let_stmt = std::get<LetStmt>(stmt);
    REQUIRE_FALSE(let_stmt.is_mut);
    REQUIRE(let_stmt.name == "x");
    REQUIRE_FALSE(let_stmt.type.has_value());
}

// --- More error cases --------------------------------------------------------

TEST_CASE("Parse error for unmatched bracket") {
    auto result = parse("df[filter price > 10;");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("Parse empty block is valid (identity)") {
    auto result = parse("df[];");
    REQUIRE(result.has_value());
}

TEST_CASE("Parse error for missing expression in let") {
    auto result = parse("let x = ;");
    REQUIRE_FALSE(result.has_value());
}

// --- Join expressions --------------------------------------------------------

TEST_CASE("Parse inner join") {
    const char* source = "a join b on id;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Inner);
    REQUIRE(join.keys.size() == 1);
    REQUIRE(join.keys[0] == "id");
}

TEST_CASE("Parse left join") {
    const char* source = "a left join b on key;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Left);
    REQUIRE(join.keys.size() == 1);
    REQUIRE(join.keys[0] == "key");
}

TEST_CASE("Parse right join") {
    const char* source = "a right join b on key;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Right);
    REQUIRE(join.keys.size() == 1);
    REQUIRE(join.keys[0] == "key");
}

TEST_CASE("Parse outer join") {
    const char* source = "a outer join b on key;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Outer);
    REQUIRE(join.keys.size() == 1);
    REQUIRE(join.keys[0] == "key");
}
TEST_CASE("Parse semi join") {
    const char* source = "a semi join b on key;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Semi);
    REQUIRE(join.keys == std::vector<std::string>{"key"});
}

TEST_CASE("Parse anti join") {
    const char* source = "a anti join b on key;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Anti);
    REQUIRE(join.keys == std::vector<std::string>{"key"});
}

TEST_CASE("Parse cross join") {
    const char* source = "a cross join b;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& join = require_join(require_expr(expr_stmt.expr));
    REQUIRE(join.kind == JoinKind::Cross);
    REQUIRE(join.keys.empty());
}

// --- All comparison operators parse correctly --------------------------------

TEST_CASE("Parse all comparison operators") {
    auto check = [](const char* source, BinaryOp expected) {
        auto result = parse(source);
        REQUIRE(result.has_value());
        const auto& stmt = result->statements.front();
        const auto& let_stmt = std::get<LetStmt>(stmt);
        const auto& expr = require_expr(let_stmt.value);
        require_binary(expr, expected);
    };
    check("let x = a == b;", BinaryOp::Eq);
    check("let x = a != b;", BinaryOp::Ne);
    check("let x = a < b;", BinaryOp::Lt);
    check("let x = a <= b;", BinaryOp::Le);
    check("let x = a > b;", BinaryOp::Gt);
    check("let x = a >= b;", BinaryOp::Ge);
}

// --- All arithmetic operators parse correctly --------------------------------

TEST_CASE("Parse all arithmetic operators") {
    auto check = [](const char* source, BinaryOp expected) {
        auto result = parse(source);
        REQUIRE(result.has_value());
        const auto& stmt = result->statements.front();
        const auto& let_stmt = std::get<LetStmt>(stmt);
        const auto& expr = require_expr(let_stmt.value);
        require_binary(expr, expected);
    };
    check("let x = a + b;", BinaryOp::Add);
    check("let x = a - b;", BinaryOp::Sub);
    check("let x = a * b;", BinaryOp::Mul);
    check("let x = a / b;", BinaryOp::Div);
    check("let x = a % b;", BinaryOp::Mod);
}

// --- Tuple let (multi-column) assignment -------------------------------------

TEST_CASE("Parse tuple let binding with two names") {
    const char* source = "let (a, b) = some_fn();";
    auto result = parse(source);
    REQUIRE(result.has_value());
    REQUIRE(result->statements.size() == 1);
    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<TupleLetStmt>(stmt));
    const auto& tlet = std::get<TupleLetStmt>(stmt);
    REQUIRE_FALSE(tlet.is_mut);
    REQUIRE(tlet.names.size() == 2);
    REQUIRE(tlet.names[0] == "a");
    REQUIRE(tlet.names[1] == "b");
    REQUIRE(tlet.value != nullptr);
}

TEST_CASE("Parse mut tuple let binding with three names") {
    const char* source = "let mut (x, y, z) = gen();";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<TupleLetStmt>(stmt));
    const auto& tlet = std::get<TupleLetStmt>(stmt);
    REQUIRE(tlet.is_mut);
    REQUIRE(tlet.names.size() == 3);
    REQUIRE(tlet.names[0] == "x");
    REQUIRE(tlet.names[1] == "y");
    REQUIRE(tlet.names[2] == "z");
}

TEST_CASE("Parse tuple let binding with one name") {
    const char* source = "let (col) = tbl;";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    REQUIRE(std::holds_alternative<TupleLetStmt>(stmt));
    const auto& tlet = std::get<TupleLetStmt>(stmt);
    REQUIRE(tlet.names.size() == 1);
    REQUIRE(tlet.names[0] == "col");
}

TEST_CASE("Parse tuple let binding tracks source lines") {
    const char* source = "let (a, b) = fn_call();\n";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& tlet = std::get<TupleLetStmt>(stmt);
    REQUIRE(tlet.start_line == 1);
    REQUIRE(tlet.end_line == 1);
}

// --- Tuple-LHS in update/select clauses --------------------------------------

TEST_CASE("Parse tuple-LHS update with two names") {
    const char* source = "df[update { (colA, colB) = make_xy() }];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.fields.empty());
    REQUIRE(update.tuple_fields.size() == 1);
    REQUIRE(update.tuple_fields[0].names.size() == 2);
    REQUIRE(update.tuple_fields[0].names[0] == "colA");
    REQUIRE(update.tuple_fields[0].names[1] == "colB");
    REQUIRE(update.tuple_fields[0].expr != nullptr);
}

TEST_CASE("Parse tuple-LHS update mixed with regular field") {
    const char* source = "df[update { x = price + 1, (a, b) = make_ab() }];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.fields.size() == 1);
    REQUIRE(update.fields[0].name == "x");
    REQUIRE(update.tuple_fields.size() == 1);
    REQUIRE(update.tuple_fields[0].names[0] == "a");
    REQUIRE(update.tuple_fields[0].names[1] == "b");
}

TEST_CASE("Parse tuple-LHS select with three names") {
    const char* source = "df[select { (p, q, r) = gen() }];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    const auto& select = std::get<SelectClause>(block.clauses[0]);
    REQUIRE(select.fields.empty());
    REQUIRE(select.tuple_fields.size() == 1);
    REQUIRE(select.tuple_fields[0].names.size() == 3);
    REQUIRE(select.tuple_fields[0].names[0] == "p");
    REQUIRE(select.tuple_fields[0].names[1] == "q");
    REQUIRE(select.tuple_fields[0].names[2] == "r");
}

// --- update = expr (merge-all) -----------------------------------------------

TEST_CASE("Parse update = expr merge-all form") {
    const char* source = "df[update = gen_cols()];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    REQUIRE(block.clauses.size() == 1);
    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.fields.empty());
    REQUIRE(update.tuple_fields.empty());
    REQUIRE(update.merge_expr != nullptr);
}

TEST_CASE("Parse update = expr with argument") {
    const char* source = "prices[update = gen_prices(symbols)];";
    auto result = parse(source);
    REQUIRE(result.has_value());
    const auto& stmt = result->statements.front();
    const auto& expr_stmt = std::get<ExprStmt>(stmt);
    const auto& block = require_block(require_expr(expr_stmt.expr));
    const auto& update = std::get<UpdateClause>(block.clauses[0]);
    REQUIRE(update.merge_expr != nullptr);
    // RHS must be a call expression
    const auto* call = std::get_if<CallExpr>(&update.merge_expr->node);
    REQUIRE(call != nullptr);
    REQUIRE(call->callee == "gen_prices");
}
