// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "physical_executor_internal.hpp"

namespace ibex::runtime {

auto build_migrated_physical_operator(const physical::Plan& plan, const ir::Node& node,
                                      const TableRegistry& registry, const ScalarRegistry* scalars,
                                      const ExternRegistry* externs, const ExecutionContext& exec,
                                      ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    if (!plan.migrated) {
        return std::unexpected("physical executor: plan does not migrate its root");
    }
    if (plan.root != &node) {
        return std::unexpected("physical executor: plan root does not match execution root");
    }

    using namespace physical_executor_detail;
    if (node.kind() == ir::NodeKind::Head) {
        physical::note_map_pipeline_executed();
        return build_physical_head(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Tail) {
        physical::note_map_pipeline_executed();
        return build_physical_tail(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::TopK) {
        physical::note_map_pipeline_executed();
        return build_physical_topk(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::FilterHead || node.kind() == ir::NodeKind::FilterTail) {
        physical::note_map_pipeline_executed();
        return build_physical_filter_head_tail(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Distinct) {
        physical::note_map_pipeline_executed();
        return build_physical_distinct(plan, node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Order) {
        physical::note_map_pipeline_executed();
        return build_physical_order(node, registry, scalars, externs, exec, model_out);
    }
    if (plan.aggregate.describes) {
        physical::note_map_pipeline_executed();
        return build_physical_aggregate(plan, node, registry, scalars, externs, exec, model_out);
    }
    if (plan.join.describes) {
        physical::note_map_pipeline_executed();
        return build_physical_join(plan, node, registry, scalars, externs, exec, model_out);
    }
    if (plan.mode != physical::PipelineMode::MorselParallel || !exec.can_fan_out()) {
        physical::note_map_pipeline_executed();
    }
    return build_physical_map_step(plan, 0, registry, scalars, externs, exec, model_out);
}

auto build_operator_from_physical_plan(const physical::Plan& plan, const ir::Node& node,
                                       const TableRegistry& registry, const ScalarRegistry* scalars,
                                       const ExternRegistry* externs, const ExecutionContext& exec,
                                       ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    return build_migrated_physical_operator(plan, node, registry, scalars, externs, exec,
                                            model_out);
}

}  // namespace ibex::runtime
