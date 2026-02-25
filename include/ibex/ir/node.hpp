#pragma once

#include <ibex/core/time.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
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
    std::variant<std::int64_t, double, std::string, Date, Timestamp> value;
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

struct CallExpr {
    std::string callee;
    std::vector<ExprPtr> args;
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
};

/// Join type.
enum class JoinKind : std::uint8_t {
    Inner,
    Left,
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

struct FilterExpr {
    std::variant<FilterColumn, FilterLiteral, FilterArith, FilterCmp, FilterAnd, FilterOr,
                 FilterNot>
        node;
};

/// Aggregation specification: apply function to column, store as alias.
struct AggSpec {
    AggFunc func = AggFunc::Sum;
    ColumnRef column;
    std::string alias;
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
    Window,
    AsTimeframe,
    ExternCall,
    Join,
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
    UpdateNode(NodeId id, std::vector<FieldSpec> fields, std::vector<ColumnRef> group_by = {})
        : Node(NodeKind::Update, id), fields_(std::move(fields)), group_by_(std::move(group_by)) {}

    [[nodiscard]] auto fields() const noexcept -> const std::vector<FieldSpec>& { return fields_; }
    [[nodiscard]] auto group_by() const noexcept -> const std::vector<ColumnRef>& {
        return group_by_;
    }

   private:
    std::vector<FieldSpec> fields_;
    std::vector<ColumnRef> group_by_;
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

/// Join node: combines two tables using key equality (or as-of).
class JoinNode final : public Node {
   public:
    JoinNode(NodeId id, JoinKind kind, std::vector<std::string> keys)
        : Node(NodeKind::Join, id), kind_(kind), keys_(std::move(keys)) {}

    [[nodiscard]] auto kind() const noexcept -> JoinKind { return kind_; }
    [[nodiscard]] auto keys() const noexcept -> const std::vector<std::string>& { return keys_; }

   private:
    JoinKind kind_;
    std::vector<std::string> keys_;
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

}  // namespace ibex::ir
