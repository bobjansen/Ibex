#include <ibex/runtime/pipeline.hpp>

namespace ibex::runtime {

auto classify_node(ir::NodeKind kind) noexcept -> PipelineRole {
    switch (kind) {
        case ir::NodeKind::Scan:
        case ir::NodeKind::ExternCall:
        case ir::NodeKind::Construct:
            return PipelineRole::Source;

        case ir::NodeKind::Filter:
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
        case ir::NodeKind::Update:
        case ir::NodeKind::Columns:
        case ir::NodeKind::Head:
        case ir::NodeKind::Tail:
            return PipelineRole::Passthrough;

        case ir::NodeKind::Aggregate:
        case ir::NodeKind::Order:
        case ir::NodeKind::Distinct:
        case ir::NodeKind::Join:
        case ir::NodeKind::Window:
        case ir::NodeKind::Resample:
        case ir::NodeKind::AsTimeframe:
        case ir::NodeKind::Melt:
        case ir::NodeKind::Dcast:
        case ir::NodeKind::Stream:
        case ir::NodeKind::Cov:
        case ir::NodeKind::Corr:
        case ir::NodeKind::Transpose:
        case ir::NodeKind::Matmul:
        case ir::NodeKind::Model:
        case ir::NodeKind::Program:
            return PipelineRole::Breaker;
    }
    return PipelineRole::Breaker;
}

namespace {

void walk(const ir::Node& node, PipelinePlan& plan, PipelineSegment& current) {
    const auto role = classify_node(node.kind());

    if (role == PipelineRole::Source) {
        current.nodes.insert(current.nodes.begin(), &node);
        return;
    }

    if (role == PipelineRole::Passthrough) {
        if (!node.children().empty()) {
            walk(*node.children().front(), plan, current);
        }
        current.nodes.push_back(&node);
        return;
    }

    // Breaker: recurse into child to finish the upstream segment,
    // then close the current segment and start a new one.
    if (!node.children().empty()) {
        walk(*node.children().front(), plan, current);
    }

    // The current segment holds the upstream pipeline (source → passthroughs).
    // Close it if non-empty.
    if (!current.nodes.empty()) {
        plan.segments.push_back(std::move(current));
        current = PipelineSegment{};
    }

    // The breaker itself forms a single-node segment.
    PipelineSegment breaker_seg;
    breaker_seg.nodes.push_back(&node);
    plan.segments.push_back(std::move(breaker_seg));
}

}  // namespace

auto plan_pipelines(const ir::Node& root) -> PipelinePlan {
    PipelinePlan plan;
    PipelineSegment current;
    walk(root, plan, current);
    if (!current.nodes.empty()) {
        plan.segments.push_back(std::move(current));
    }
    return plan;
}

}  // namespace ibex::runtime
