#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace ibex::parser {

enum class ScalarType : std::uint8_t {
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    Timestamp,
};

struct SchemaField {
    std::string name;
    ScalarType type = ScalarType::Int64;
};

struct SchemaType {
    std::vector<SchemaField> fields;
};

using TypeArg = std::variant<ScalarType, SchemaType>;

struct Type {
    enum class Kind : std::uint8_t {
        Scalar,
        Series,
        DataFrame,
        TimeFrame,
    };

    Kind kind = Kind::Scalar;
    TypeArg arg = ScalarType::Int64;
};

struct Param {
    std::string name;
    Type type;
};

struct DurationLiteral {
    std::string text;
};

struct IdentifierExpr {
    std::string name;
};

struct LiteralExpr {
    std::variant<std::int64_t, double, bool, std::string, DurationLiteral> value;
};

enum class UnaryOp : std::uint8_t {
    Negate,
    Not,
};

enum class BinaryOp : std::uint8_t {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
};

struct Expr;
using ExprPtr = std::unique_ptr<Expr>;

struct Field {
    std::string name;
    ExprPtr expr;
};

struct FilterClause {
    ExprPtr predicate;
};

struct SelectClause {
    std::vector<Field> fields;
};

struct UpdateClause {
    std::vector<Field> fields;
};

struct ByClause {
    std::vector<Field> keys;
    bool is_braced = false;
};

struct WindowClause {
    DurationLiteral duration;
};

using Clause = std::variant<FilterClause, SelectClause, UpdateClause, ByClause, WindowClause>;

struct BlockExpr {
    ExprPtr base;
    std::vector<Clause> clauses;
};

struct CallExpr {
    std::string callee;
    std::vector<ExprPtr> args;
};

struct UnaryExpr {
    UnaryOp op = UnaryOp::Negate;
    ExprPtr expr;
};

struct BinaryExpr {
    BinaryOp op = BinaryOp::Add;
    ExprPtr left;
    ExprPtr right;
};

struct GroupExpr {
    ExprPtr expr;
};

struct Expr {
    std::variant<IdentifierExpr, LiteralExpr, CallExpr, UnaryExpr, BinaryExpr, GroupExpr, BlockExpr>
        node;
};

struct LetStmt {
    bool is_mut = false;
    std::string name;
    std::optional<Type> type;
    ExprPtr value;
};

struct ExprStmt {
    ExprPtr expr;
};

using FnStmt = std::variant<LetStmt, ExprStmt>;

struct FunctionDecl {
    std::string name;
    std::vector<Param> params;
    Type return_type;
    std::vector<FnStmt> body;
};

struct ExternDecl {
    std::string name;
    std::vector<Param> params;
    Type return_type;
    std::string source_path;
};

using Stmt = std::variant<ExternDecl, FunctionDecl, LetStmt, ExprStmt>;

struct Program {
    std::vector<Stmt> statements;
};

}  // namespace ibex::parser
