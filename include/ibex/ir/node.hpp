#pragma once

#include <ibex/core/time.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace ibex::ir {

/// Unique identifier for IR nodes.
using NodeId = std::uint64_t;

/// Duration type for window specifications (nanoseconds).
using Duration = std::chrono::nanoseconds;

/// Forward declarations
class Node;
using NodePtr = std::unique_ptr<Node>;

/// Column reference in the IR.
struct ColumnRef {
    std::string name;
    NodeId source = 0;
};

/// Expression node for computed fields.
struct Expr;
using ExprPtr = std::shared_ptr<Expr>;

struct Literal {
    std::variant<std::int64_t, double, bool, std::string, Date, Timestamp> value;
};

enum class ArithmeticOp : std::uint8_t {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
};

struct BinaryExpr {
    ArithmeticOp op = ArithmeticOp::Add;
    ExprPtr left;
    ExprPtr right;
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

struct Expr {
    std::variant<ColumnRef, Literal, BinaryExpr, CallExpr> node;
};

/// A computed field: an alias mapped to an expression (represented as
/// a small expression tree).
/// See SPEC.md Section 5.3 (select/update field semantics).
struct FieldSpec {
    std::string alias;
    Expr expr;
};

/// A tuple-assignment field: multiple aliases bound to columns of a
/// table-producing sub-plan.  `(colA, colB) = create_x_y()` in an
/// update/select clause lowers to one TupleFieldSpec whose `source`
/// is the IR sub-tree for `create_x_y()`.
struct TupleFieldSpec {
    std::vector<std::string> aliases;
    NodePtr source;
};

struct OrderKey {
    std::string name;
    bool ascending = true;
};

/// Supported comparison operators for filter predicates.
/// See SPEC.md Section 2.5 (Operators).
enum class CompareOp : std::uint8_t {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
};

/// Supported aggregation functions.
/// See SPEC.md Section 7.1 (Aggregate Functions).
enum class AggFunc : std::uint8_t {
    Sum,
    Mean,
    Min,
    Max,
    Count,
    First,
    Last,
    Median,  ///< Middle value (ignores nulls; always returns double).
    Stddev,  ///< Sample standard deviation, n-1 denominator (ignores nulls; always returns double).
    Ewma,    ///< Exponentially weighted moving average: ewma(col, alpha) (always returns double).
    Quantile,  ///< quantile(col, p): p-th quantile via linear interpolation (always returns
               ///< double).
    Skew,      ///< Sample skewness (Fisher–Pearson, n≥3; ignores nulls; always returns double).
    Kurtosis,  ///< Sample excess kurtosis (n≥4; ignores nulls; always returns double).
};

/// Join type.
enum class JoinKind : std::uint8_t {
    Inner,
    Left,
    Right,
    Outer,
    Semi,
    Anti,
    Cross,
    Asof,
};

/// Filter expression tree — unified value and boolean nodes.
/// See SPEC.md Section 5.3 (filter clause semantics).
struct FilterExpr;
using FilterExprPtr = std::unique_ptr<FilterExpr>;

/// Column reference in a filter expression (resolved at runtime as column or bound scalar).
struct FilterColumn {
    std::string name;
};
/// Literal value in a filter expression.
struct FilterLiteral {
    std::variant<std::int64_t, double, std::string, Date, Timestamp> value;
};
/// Arithmetic on two value expressions.
struct FilterArith {
    ArithmeticOp op;
    FilterExprPtr left, right;
};
/// Comparison between two value expressions — produces a bool.
struct FilterCmp {
    CompareOp op;
    FilterExprPtr left, right;
};
/// Logical AND of two boolean expressions.
struct FilterAnd {
    FilterExprPtr left, right;
};
/// Logical OR of two boolean expressions.
struct FilterOr {
    FilterExprPtr left, right;
};
/// Logical NOT of a boolean expression.
struct FilterNot {
    FilterExprPtr operand;
};
/// Null check — always produces a valid boolean (never null itself).
struct FilterIsNull {
    FilterExprPtr operand;
};
struct FilterIsNotNull {
    FilterExprPtr operand;
};

struct FilterExpr {
    std::variant<FilterColumn, FilterLiteral, FilterArith, FilterCmp, FilterAnd, FilterOr,
                 FilterNot, FilterIsNull, FilterIsNotNull>
        node;
};

/// Aggregation specification: apply function to column, store as alias.
struct AggSpec {
    AggFunc func = AggFunc::Sum;
    ColumnRef column;
    std::string alias;
    double param = 0.0;  ///< Function-specific parameter (e.g. alpha for Ewma).
};

/// Rename specification: maps an old column name to a new column name.
struct RenameSpec {
    std::string new_name;
    std::string old_name;
};

/// IR node types.
/// See SPEC.md Section 1.3 (Mapping to Relational Algebra).
enum class NodeKind : std::uint8_t {
    Scan,
    Filter,
    Project,
    Distinct,
    Order,
    Aggregate,
    Update,
    Rename,
    Window,
    Resample,
    AsTimeframe,
    ExternCall,
    Join,
    Melt,
    Dcast,
    Stream,
    Construct,  ///< Build a Table from inline literal column vectors.
    Program,    ///< Top-level program: zero or more preamble side-effect calls + one main node.
};

/// How a StreamNode triggers output emission.
/// Inferred from the transform IR — never specified by the user.
enum class StreamKind : std::uint8_t {
    /// Emit one output row for every incoming row (rolling window or plain transforms).
    PerRow,
    /// Buffer rows and emit when a time-bucket boundary is crossed (resample).
    TimeBucket,
};

/// Base IR node for the query plan.
///
/// Represents a single relational operation in the query DAG.
/// Children are owned via unique_ptr for clear ownership semantics.
class Node {
   public:
    explicit Node(NodeKind kind, NodeId id) : kind_(kind), id_(id) {}
    virtual ~Node() = default;

    Node(const Node&) = delete;
    auto operator=(const Node&) -> Node& = delete;
    Node(Node&&) = default;
    auto operator=(Node&&) -> Node& = default;

    [[nodiscard]] auto kind() const noexcept -> NodeKind { return kind_; }
    [[nodiscard]] auto id() const noexcept -> NodeId { return id_; }
    [[nodiscard]] auto children() const noexcept -> const std::vector<NodePtr>& {
        return children_;
    }

    void add_child(NodePtr child) { children_.push_back(std::move(child)); }

   private:
    NodeKind kind_;
    NodeId id_;
    std::vector<NodePtr> children_;
};

/// Scan node: reads from a named source.
class ScanNode final : public Node {
   public:
    ScanNode(NodeId id, std::string source_name)
        : Node(NodeKind::Scan, id), source_name_(std::move(source_name)) {}

    [[nodiscard]] auto source_name() const noexcept -> const std::string& { return source_name_; }

   private:
    std::string source_name_;
};

/// Filter node: applies a predicate to its child.
/// See SPEC.md Section 5.3 (filter clause).
class FilterNode final : public Node {
   public:
    FilterNode(NodeId id, FilterExprPtr predicate)
        : Node(NodeKind::Filter, id), predicate_(std::move(predicate)) {}

    [[nodiscard]] auto predicate() const noexcept -> const FilterExpr& { return *predicate_; }

   private:
    FilterExprPtr predicate_;
};

/// Project node: selects and computes a subset of columns.
/// See SPEC.md Section 5.3 (select clause).
class ProjectNode final : public Node {
   public:
    ProjectNode(NodeId id, std::vector<ColumnRef> columns)
        : Node(NodeKind::Project, id), columns_(std::move(columns)) {}

    [[nodiscard]] auto columns() const noexcept -> const std::vector<ColumnRef>& {
        return columns_;
    }

   private:
    std::vector<ColumnRef> columns_;
};

/// Distinct node: drops duplicate rows.
class DistinctNode final : public Node {
   public:
    explicit DistinctNode(NodeId id) : Node(NodeKind::Distinct, id) {}
};

/// Order node: sorts rows by one or more keys.
class OrderNode final : public Node {
   public:
    OrderNode(NodeId id, std::vector<OrderKey> keys)
        : Node(NodeKind::Order, id), keys_(std::move(keys)) {}

    [[nodiscard]] auto keys() const noexcept -> const std::vector<OrderKey>& { return keys_; }

   private:
    std::vector<OrderKey> keys_;
};

/// Aggregate node: groups and aggregates.
/// See SPEC.md Section 7 (Aggregation Rules).
class AggregateNode final : public Node {
   public:
    AggregateNode(NodeId id, std::vector<ColumnRef> group_by, std::vector<AggSpec> aggregations)
        : Node(NodeKind::Aggregate, id),
          group_by_(std::move(group_by)),
          aggregations_(std::move(aggregations)) {}

    [[nodiscard]] auto group_by() const noexcept -> const std::vector<ColumnRef>& {
        return group_by_;
    }
    [[nodiscard]] auto aggregations() const noexcept -> const std::vector<AggSpec>& {
        return aggregations_;
    }

   private:
    std::vector<ColumnRef> group_by_;
    std::vector<AggSpec> aggregations_;
};

/// Update node: adds or replaces columns while retaining all existing ones.
/// See SPEC.md Section 5.3 (update clause).
///
/// When group_by is non-empty, expressions are evaluated per group and
/// broadcast back (SPEC.md Section 7.4).
class UpdateNode final : public Node {
   public:
    UpdateNode(NodeId id, std::vector<FieldSpec> fields,
               std::vector<TupleFieldSpec> tuple_fields = {}, std::vector<ColumnRef> group_by = {})
        : Node(NodeKind::Update, id),
          fields_(std::move(fields)),
          tuple_fields_(std::move(tuple_fields)),
          group_by_(std::move(group_by)) {}

    [[nodiscard]] auto fields() const noexcept -> const std::vector<FieldSpec>& { return fields_; }
    [[nodiscard]] auto tuple_fields() const noexcept -> const std::vector<TupleFieldSpec>& {
        return tuple_fields_;
    }
    [[nodiscard]] auto group_by() const noexcept -> const std::vector<ColumnRef>& {
        return group_by_;
    }

   private:
    std::vector<FieldSpec> fields_;
    std::vector<TupleFieldSpec> tuple_fields_;
    std::vector<ColumnRef> group_by_;
};

/// Rename node: renames specified columns while keeping all others intact.
class RenameNode final : public Node {
   public:
    RenameNode(NodeId id, std::vector<RenameSpec> renames)
        : Node(NodeKind::Rename, id), renames_(std::move(renames)) {}

    [[nodiscard]] auto renames() const noexcept -> const std::vector<RenameSpec>& {
        return renames_;
    }

   private:
    std::vector<RenameSpec> renames_;
};

/// AsTimeframe node: promotes a DataFrame to a TimeFrame by designating a timestamp column.
/// See SPEC.md Section 8 (TimeFrame Extensions).
class AsTimeframeNode final : public Node {
   public:
    AsTimeframeNode(NodeId id, std::string column)
        : Node(NodeKind::AsTimeframe, id), column_(std::move(column)) {}

    [[nodiscard]] auto column() const noexcept -> const std::string& { return column_; }

   private:
    std::string column_;
};

/// ExternCall node: calls a table-returning extern C++ function.
///
/// This is produced by the lowerer when it sees a call to a declared extern fn
/// whose return type is DataFrame or TimeFrame.  At interpret time the call is
/// dispatched through the ExternRegistry; at compile time the emitter emits a
/// direct C++ function call.
class ExternCallNode final : public Node {
   public:
    ExternCallNode(NodeId id, std::string callee, std::vector<Expr> args)
        : Node(NodeKind::ExternCall, id), callee_(std::move(callee)), args_(std::move(args)) {}

    [[nodiscard]] auto callee() const noexcept -> const std::string& { return callee_; }
    [[nodiscard]] auto args() const noexcept -> const std::vector<Expr>& { return args_; }

   private:
    std::string callee_;
    std::vector<Expr> args_;
};

/// Join node: combines two tables using key equality (or as-of), or a
/// general non-equijoin predicate (theta join).
class JoinNode final : public Node {
   public:
    JoinNode(NodeId id, JoinKind kind, std::vector<std::string> keys,
             std::optional<FilterExprPtr> predicate = std::nullopt)
        : Node(NodeKind::Join, id),
          kind_(kind),
          keys_(std::move(keys)),
          predicate_(std::move(predicate)) {}

    [[nodiscard]] auto kind() const noexcept -> JoinKind { return kind_; }
    [[nodiscard]] auto keys() const noexcept -> const std::vector<std::string>& { return keys_; }
    [[nodiscard]] auto predicate() const noexcept -> const std::optional<FilterExprPtr>& {
        return predicate_;
    }

   private:
    JoinKind kind_;
    std::vector<std::string> keys_;
    std::optional<FilterExprPtr> predicate_;
};

/// ResampleNode: time-bucket aggregation on a TimeFrame.
/// Groups rows into buckets of `duration` width; applies aggregations within
/// each (bucket, group_by) partition. Output has one row per partition.
class ResampleNode final : public Node {
   public:
    ResampleNode(NodeId id, Duration duration, std::vector<ColumnRef> group_by,
                 std::vector<AggSpec> aggregations)
        : Node(NodeKind::Resample, id),
          duration_(duration),
          group_by_(std::move(group_by)),
          aggregations_(std::move(aggregations)) {}

    [[nodiscard]] auto duration() const noexcept -> Duration { return duration_; }
    [[nodiscard]] auto group_by() const noexcept -> const std::vector<ColumnRef>& {
        return group_by_;
    }
    [[nodiscard]] auto aggregations() const noexcept -> const std::vector<AggSpec>& {
        return aggregations_;
    }

   private:
    Duration duration_;
    std::vector<ColumnRef> group_by_;
    std::vector<AggSpec> aggregations_;
};

/// Window node: specifies a time-based window for rolling computations.
/// See SPEC.md Section 8 (TimeFrame Extensions).
///
/// Valid only on TimeFrame operands. The duration defines the lookback
/// range [t - duration, t] for each row at time t.
class WindowNode final : public Node {
   public:
    WindowNode(NodeId id, Duration duration) : Node(NodeKind::Window, id), duration_(duration) {}

    [[nodiscard]] auto duration() const noexcept -> Duration { return duration_; }

   private:
    Duration duration_;
};

/// Melt node: unpivots wide-format columns into long-format (variable, value) rows.
/// id_columns stay as-is; measure_columns are unpivoted.
/// If measure_columns is empty, all non-id columns are melted.
class MeltNode final : public Node {
   public:
    MeltNode(NodeId id, std::vector<std::string> id_columns,
             std::vector<std::string> measure_columns)
        : Node(NodeKind::Melt, id),
          id_columns_(std::move(id_columns)),
          measure_columns_(std::move(measure_columns)) {}

    [[nodiscard]] auto id_columns() const noexcept -> const std::vector<std::string>& {
        return id_columns_;
    }
    [[nodiscard]] auto measure_columns() const noexcept -> const std::vector<std::string>& {
        return measure_columns_;
    }

   private:
    std::vector<std::string> id_columns_;
    std::vector<std::string> measure_columns_;
};

/// Dcast node: pivots long-format data into wide-format columns.
/// pivot_column's distinct values become new column names; value_column fills cells.
/// row_keys are the grouping columns that become row identifiers.
class DcastNode final : public Node {
   public:
    DcastNode(NodeId id, std::string pivot_column, std::string value_column,
              std::vector<std::string> row_keys)
        : Node(NodeKind::Dcast, id),
          pivot_column_(std::move(pivot_column)),
          value_column_(std::move(value_column)),
          row_keys_(std::move(row_keys)) {}

    [[nodiscard]] auto pivot_column() const noexcept -> const std::string& { return pivot_column_; }
    [[nodiscard]] auto value_column() const noexcept -> const std::string& { return value_column_; }
    [[nodiscard]] auto row_keys() const noexcept -> const std::vector<std::string>& {
        return row_keys_;
    }

   private:
    std::string pivot_column_;
    std::string value_column_;
    std::vector<std::string> row_keys_;
};

/// A single named column in a Construct node.
///
/// Two mutually exclusive modes:
///   - Literal: `elements` is non-empty, `expr_node` is null.  Used for inline
///     array literals `[v, v, ...]`; all elements share the same variant type.
///   - Expression: `expr_node` is non-null, `elements` is empty.  The node is
///     evaluated at interpret/codegen time to produce a Table; the column is
///     extracted from that Table (single-column result, or column named `name`).
struct ConstructColumn {
    std::string name;
    std::vector<Literal> elements;    ///< non-empty iff expr_node is null
    std::unique_ptr<Node> expr_node;  ///< non-null iff elements is empty
};

/// Construct node: builds a Table from column definitions.
///
/// Produced by lowering `Table { col1 = expr, ... }` expressions.
/// Each column is either an inline literal vector or an arbitrary expression
/// that evaluates to a single-column Table (or a Table with a matching column name).
/// All columns must have equal length (validated at interpret time).
class ConstructNode final : public Node {
   public:
    ConstructNode(NodeId id, std::vector<ConstructColumn> columns)
        : Node(NodeKind::Construct, id), columns_(std::move(columns)) {}

    [[nodiscard]] auto columns() const noexcept -> const std::vector<ConstructColumn>& {
        return columns_;
    }

   private:
    std::vector<ConstructColumn> columns_;
};

/// Stream node: wires a source extern, an anonymous transform, and a sink extern into a
/// continuous event loop.
///
/// Children:
///   child[0] — transform IR (rooted at ScanNode("__stream_input__")); the event loop
///              substitutes the current buffer for "__stream_input__" on each iteration.
///
/// The source and sink are stored as callee names + pre-evaluated scalar argument lists
/// rather than as child nodes, because they are called with a fixed calling convention
/// by the event loop rather than being evaluated as part of the IR tree.
///
/// stream_kind is inferred by the lowerer:
///   ResampleNode anywhere in the transform → TimeBucket
///   WindowNode anywhere in the transform   → PerRow
///   Neither                                → PerRow
class StreamNode final : public Node {
   public:
    StreamNode(NodeId id, std::string source_callee, std::vector<Expr> source_args,
               std::string sink_callee, std::vector<Expr> sink_args, StreamKind kind,
               Duration bucket_duration)
        : Node(NodeKind::Stream, id),
          source_callee_(std::move(source_callee)),
          source_args_(std::move(source_args)),
          sink_callee_(std::move(sink_callee)),
          sink_args_(std::move(sink_args)),
          stream_kind_(kind),
          bucket_duration_(bucket_duration) {}

    [[nodiscard]] auto source_callee() const noexcept -> const std::string& {
        return source_callee_;
    }
    [[nodiscard]] auto source_args() const noexcept -> const std::vector<Expr>& {
        return source_args_;
    }
    [[nodiscard]] auto sink_callee() const noexcept -> const std::string& { return sink_callee_; }
    [[nodiscard]] auto sink_args() const noexcept -> const std::vector<Expr>& { return sink_args_; }
    [[nodiscard]] auto stream_kind() const noexcept -> StreamKind { return stream_kind_; }
    /// Bucket duration for TimeBucket streams (zero for PerRow streams).
    [[nodiscard]] auto bucket_duration() const noexcept -> Duration { return bucket_duration_; }
    /// The transform IR is stored as child[0].
    [[nodiscard]] auto transform_ir() const -> const Node& { return *children()[0]; }

   private:
    std::string source_callee_;
    std::vector<Expr> source_args_;
    std::string sink_callee_;
    std::vector<Expr> sink_args_;
    StreamKind stream_kind_;
    Duration bucket_duration_;  ///< set for TimeBucket; zero for PerRow
};

/// Top-level program node.
///
/// Holds zero or more preamble side-effect calls (e.g. `ws_listen(8765)`) as
/// `ExternCallNode` children emitted before the main expression, plus a single
/// main child node (the last expression in the program).
class ProgramNode final : public Node {
   public:
    ProgramNode(NodeId id, std::vector<NodePtr> preamble, NodePtr main_node)
        : Node(NodeKind::Program, id),
          preamble_(std::move(preamble)),
          main_node_(std::move(main_node)) {}

    [[nodiscard]] auto preamble() const noexcept -> const std::vector<NodePtr>& {
        return preamble_;
    }
    [[nodiscard]] auto main_node() const noexcept -> const Node& { return *main_node_; }

   private:
    std::vector<NodePtr> preamble_;
    NodePtr main_node_;
};

}  // namespace ibex::ir
