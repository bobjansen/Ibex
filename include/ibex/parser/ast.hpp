#pragma once

#include <ibex/core/time.hpp>

#include <cstddef>
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
    Date,
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
    std::variant<std::int64_t, double, bool, std::string, DurationLiteral, Date, Timestamp> value;
};

enum class UnaryOp : std::uint8_t {
    Negate,
    Not,
    IsNull,     // expr is null
    IsNotNull,  // expr is not null
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

enum class JoinKind : std::uint8_t {
    Inner,
    Left,
    Asof,
};

struct Expr;
using ExprPtr = std::unique_ptr<Expr>;

struct Field {
    std::string name;
    ExprPtr expr;
};

/// Tuple-LHS field assignment: `(colA, colB) = expr` inside select/update.
/// The RHS must evaluate to a DataFrame; each name is bound to the
/// corresponding column of the result.
struct TupleField {
    std::vector<std::string> names;
    ExprPtr expr;
};

struct OrderKey {
    std::string name;
    bool ascending = true;
};

struct FilterClause {
    ExprPtr predicate;
};

struct SelectClause {
    std::vector<Field> fields;
    std::vector<TupleField> tuple_fields;
};

struct DistinctClause {
    std::vector<Field> fields;
};

struct UpdateClause {
    std::vector<Field> fields;
    std::vector<TupleField> tuple_fields;
    /// Set for the `update = expr` form: all columns of the result are merged in.
    ExprPtr merge_expr;
};

struct RenameClause {
    std::vector<Field> fields;
};

struct OrderClause {
    std::vector<OrderKey> keys;
    bool is_braced = false;
};

struct ByClause {
    std::vector<Field> keys;
    bool is_braced = false;
};

struct WindowClause {
    DurationLiteral duration;
};

struct ResampleClause {
    DurationLiteral duration;
};

struct MeltClause {
    std::vector<Field> id_fields;
};

struct DcastClause {
    std::string pivot_column;
};

using Clause = std::variant<FilterClause, SelectClause, DistinctClause, UpdateClause, RenameClause,
                            OrderClause, ByClause, WindowClause, ResampleClause, MeltClause,
                            DcastClause>;

struct BlockExpr {
    ExprPtr base;
    std::vector<Clause> clauses;
};

struct NamedArg {
    std::string name;
    ExprPtr value;
};

struct CallExpr {
    std::string callee;
    std::vector<ExprPtr> args;
    std::vector<NamedArg> named_args;
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

struct JoinExpr {
    JoinKind kind = JoinKind::Inner;
    ExprPtr left;
    ExprPtr right;
    std::vector<std::string> keys;
};

/// `Stream { source = call_expr, transform = [clauses...], sink = call_expr }`
///
/// source   — an extern call returning DataFrame; called in a loop (empty = EOF).
/// transform — anonymous block clauses applied to the accumulated buffer.
///             The lowerer synthesises ScanNode("__stream_input__") as the implicit base.
/// sink_callee / sink_args — the table-consumer extern that receives each output batch;
///             the stream runtime prepends the output Table as the first argument.
struct StreamExpr {
    ExprPtr source;                  ///< source call expression (e.g. udp_recv(9001))
    std::vector<Clause> transform;   ///< anonymous block clauses
    std::string sink_callee;         ///< name of the sink extern fn
    std::vector<ExprPtr> sink_args;  ///< extra scalar args for the sink (table prepended at runtime)
};

struct Expr {
    std::variant<IdentifierExpr, LiteralExpr, CallExpr, UnaryExpr, BinaryExpr, GroupExpr, BlockExpr,
                 JoinExpr, StreamExpr>
        node;
};

struct LetStmt {
    bool is_mut = false;
    std::string name;
    std::optional<Type> type;
    ExprPtr value;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

/// Tuple destructuring assignment: `let (a, b, c) = expr;`
/// The RHS must evaluate to a DataFrame; each name is bound to the
/// corresponding column as a Series.
struct TupleLetStmt {
    bool is_mut = false;
    std::vector<std::string> names;
    ExprPtr value;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

struct ExprStmt {
    ExprPtr expr;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

using FnStmt = std::variant<LetStmt, TupleLetStmt, ExprStmt>;

struct FunctionDecl {
    std::string name;
    std::vector<Param> params;
    Type return_type;
    std::vector<FnStmt> body;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

struct ExternDecl {
    std::string name;
    std::vector<Param> params;
    Type return_type;
    std::string source_path;
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

/// `import "name";` — import all extern fn declarations from a library file
/// (<name>.ibex) found on the import search path.
struct ImportDecl {
    std::string name;  // e.g. "csv" or "parquet"
    std::size_t start_line = 0;
    std::size_t end_line = 0;
};

using Stmt = std::variant<ExternDecl, FunctionDecl, LetStmt, TupleLetStmt, ExprStmt, ImportDecl>;

struct Program {
    std::vector<Stmt> statements;
};

}  // namespace ibex::parser
