#pragma once

#include <ibex/ir/node.hpp>

#include <atomic>

namespace ibex::ir {

/// Factory for constructing IR nodes with unique IDs.
///
/// Thread-safe ID generation via atomic counter.
/// TODO: Add plan-level validation and optimization passes.
class Builder {
public:
    Builder() = default;

    [[nodiscard]] auto scan(std::string source_name) -> NodePtr {
        return std::make_unique<ScanNode>(next_id(), std::move(source_name));
    }

    [[nodiscard]] auto filter(FilterPredicate predicate) -> NodePtr {
        return std::make_unique<FilterNode>(next_id(), std::move(predicate));
    }

    [[nodiscard]] auto project(std::vector<ColumnRef> columns) -> NodePtr {
        return std::make_unique<ProjectNode>(next_id(), std::move(columns));
    }

    [[nodiscard]] auto aggregate(
        std::vector<ColumnRef> group_by, std::vector<AggSpec> aggregations) -> NodePtr {
        return std::make_unique<AggregateNode>(
            next_id(), std::move(group_by), std::move(aggregations));
    }

    [[nodiscard]] auto update(
        std::vector<FieldSpec> fields, std::vector<ColumnRef> group_by = {}) -> NodePtr {
        return std::make_unique<UpdateNode>(
            next_id(), std::move(fields), std::move(group_by));
    }

    [[nodiscard]] auto window(Duration duration) -> NodePtr {
        return std::make_unique<WindowNode>(next_id(), duration);
    }

    [[nodiscard]] auto extern_call(std::string callee, std::vector<Expr> args) -> NodePtr {
        return std::make_unique<ExternCallNode>(next_id(), std::move(callee), std::move(args));
    }

    [[nodiscard]] auto join(JoinKind kind, std::vector<std::string> keys) -> NodePtr {
        return std::make_unique<JoinNode>(next_id(), kind, std::move(keys));
    }

private:
    [[nodiscard]] auto next_id() -> NodeId {
        return next_id_.fetch_add(1, std::memory_order_relaxed);
    }

    std::atomic<NodeId> next_id_{1};
};

}  // namespace ibex::ir
