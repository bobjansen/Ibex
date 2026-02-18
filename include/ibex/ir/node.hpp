#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

namespace ibex::ir {

/// Unique identifier for IR nodes.
using NodeId = std::uint64_t;

/// Forward declarations
class Node;
using NodePtr = std::unique_ptr<Node>;

/// Column reference in the IR.
struct ColumnRef {
    std::string name;
    NodeId source = 0;
};

/// Supported comparison operators for filter predicates.
enum class CompareOp : std::uint8_t {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
};

/// Supported aggregation functions.
enum class AggFunc : std::uint8_t {
    Sum,
    Min,
    Max,
    Mean,
    Count,
};

/// Filter predicate: column <op> literal.
/// TODO: Extend to support compound predicates and expression trees.
struct FilterPredicate {
    ColumnRef column;
    CompareOp op = CompareOp::Eq;
    std::variant<std::int64_t, double, std::string> value;
};

/// Aggregation specification: apply function to column, store as alias.
struct AggSpec {
    AggFunc func = AggFunc::Sum;
    ColumnRef column;
    std::string alias;
};

/// IR node types.
enum class NodeKind : std::uint8_t {
    Scan,
    Filter,
    Project,
    Aggregate,
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

    [[nodiscard]] auto source_name() const noexcept -> const std::string& {
        return source_name_;
    }

private:
    std::string source_name_;
};

/// Filter node: applies a predicate to its child.
class FilterNode final : public Node {
public:
    FilterNode(NodeId id, FilterPredicate predicate)
        : Node(NodeKind::Filter, id), predicate_(std::move(predicate)) {}

    [[nodiscard]] auto predicate() const noexcept -> const FilterPredicate& {
        return predicate_;
    }

private:
    FilterPredicate predicate_;
};

/// Project node: selects a subset of columns.
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

/// Aggregate node: groups and aggregates.
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

}  // namespace ibex::ir
