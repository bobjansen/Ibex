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

    [[nodiscard]] auto filter(FilterExprPtr predicate) -> NodePtr {
        return std::make_unique<FilterNode>(next_id(), std::move(predicate));
    }

    [[nodiscard]] auto project(std::vector<ColumnRef> columns) -> NodePtr {
        return std::make_unique<ProjectNode>(next_id(), std::move(columns));
    }

    [[nodiscard]] auto distinct() -> NodePtr { return std::make_unique<DistinctNode>(next_id()); }

    [[nodiscard]] auto order(std::vector<OrderKey> keys) -> NodePtr {
        return std::make_unique<OrderNode>(next_id(), std::move(keys));
    }

    [[nodiscard]] auto aggregate(std::vector<ColumnRef> group_by, std::vector<AggSpec> aggregations)
        -> NodePtr {
        return std::make_unique<AggregateNode>(next_id(), std::move(group_by),
                                               std::move(aggregations));
    }

    [[nodiscard]] auto update(std::vector<FieldSpec> fields,
                              std::vector<TupleFieldSpec> tuple_fields = {},
                              std::vector<ColumnRef> group_by = {}) -> NodePtr {
        return std::make_unique<UpdateNode>(next_id(), std::move(fields), std::move(tuple_fields),
                                           std::move(group_by));
    }

    [[nodiscard]] auto rename(std::vector<RenameSpec> renames) -> NodePtr {
        return std::make_unique<RenameNode>(next_id(), std::move(renames));
    }

    [[nodiscard]] auto window(Duration duration) -> NodePtr {
        return std::make_unique<WindowNode>(next_id(), duration);
    }

    [[nodiscard]] auto resample(Duration duration, std::vector<ColumnRef> group_by,
                                std::vector<AggSpec> aggregations) -> NodePtr {
        return std::make_unique<ResampleNode>(next_id(), duration, std::move(group_by),
                                              std::move(aggregations));
    }

    [[nodiscard]] auto as_timeframe(std::string column) -> NodePtr {
        return std::make_unique<AsTimeframeNode>(next_id(), std::move(column));
    }

    [[nodiscard]] auto extern_call(std::string callee, std::vector<Expr> args) -> NodePtr {
        return std::make_unique<ExternCallNode>(next_id(), std::move(callee), std::move(args));
    }

    [[nodiscard]] auto melt(std::vector<std::string> id_columns,
                            std::vector<std::string> measure_columns) -> NodePtr {
        return std::make_unique<MeltNode>(next_id(), std::move(id_columns),
                                          std::move(measure_columns));
    }

    [[nodiscard]] auto dcast(std::string pivot_column, std::string value_column,
                             std::vector<std::string> row_keys) -> NodePtr {
        return std::make_unique<DcastNode>(next_id(), std::move(pivot_column),
                                           std::move(value_column), std::move(row_keys));
    }

    [[nodiscard]] auto join(JoinKind kind, std::vector<std::string> keys) -> NodePtr {
        return std::make_unique<JoinNode>(next_id(), kind, std::move(keys));
    }

    [[nodiscard]] auto stream(std::string source_callee, std::vector<Expr> source_args,
                              std::string sink_callee, std::vector<Expr> sink_args,
                              StreamKind kind, Duration bucket_duration) -> NodePtr {
        return std::make_unique<StreamNode>(next_id(), std::move(source_callee),
                                           std::move(source_args), std::move(sink_callee),
                                           std::move(sink_args), kind, bucket_duration);
    }

   private:
    [[nodiscard]] auto next_id() -> NodeId {
        return next_id_.fetch_add(1, std::memory_order_relaxed);
    }

    std::atomic<NodeId> next_id_{1};
};

}  // namespace ibex::ir
