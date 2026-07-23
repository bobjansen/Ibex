#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <algorithm>
#include <utility>

namespace ibex::runtime {

auto execution_capability(ir::NodeKind kind) noexcept -> ExecutionCapability {
    switch (kind) {
        case ir::NodeKind::Filter:
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
        case ir::NodeKind::FilterProject:
        case ir::NodeKind::FilterUpdateProject:
            return ExecutionCapability::ParallelMap;

        case ir::NodeKind::Head:
        case ir::NodeKind::Tail:
        case ir::NodeKind::FilterHead:
        case ir::NodeKind::FilterTail:
            return ExecutionCapability::OrderedStream;

        case ir::NodeKind::Aggregate:
        case ir::NodeKind::Order:
        case ir::NodeKind::TopK:
        case ir::NodeKind::Distinct:
        case ir::NodeKind::Join:
            return ExecutionCapability::ParallelBarrier;

        default:
            return ExecutionCapability::Barrier;
    }
}

namespace {

auto expressions_are_subset_evaluable(const ir::Node& node) -> bool {
    switch (node.kind()) {
        case ir::NodeKind::Filter:
            return ir::is_subset_evaluable_expr(
                static_cast<const ir::FilterNode&>(node).predicate());
        case ir::NodeKind::FilterProject:
            return ir::is_subset_evaluable_expr(
                static_cast<const ir::FilterProjectNode&>(node).predicate());
        case ir::NodeKind::FilterUpdateProject: {
            const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
            if (!ir::is_subset_evaluable_expr(fup.predicate())) {
                return false;
            }
            return std::ranges::all_of(fup.fields(), [](const ir::FieldSpec& field) {
                return ir::is_subset_evaluable_expr(field.expr);
            });
        }
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
            return true;
        default:
            return false;
    }
}

}  // namespace

auto analyze_parallel_island(const ir::Node& root) -> ParallelIslandCandidate {
    ParallelIslandCandidate candidate;
    const ir::Node* current = &root;

    while (execution_capability(current->kind()) == ExecutionCapability::ParallelMap) {
        if (!expressions_are_subset_evaluable(*current)) {
            candidate.reason = ParallelEligibilityReason::UnsupportedExpression;
            return candidate;
        }
        if (current->children().size() != 1 || current->children().front() == nullptr) {
            candidate.reason = ParallelEligibilityReason::UnsupportedShape;
            return candidate;
        }
        candidate.operators.push_back(current);
        current = current->children().front().get();
    }

    if (candidate.operators.empty()) {
        candidate.reason = ParallelEligibilityReason::NotParallelMap;
        return candidate;
    }

    std::reverse(candidate.operators.begin(), candidate.operators.end());
    candidate.input = current;
    candidate.reason = ParallelEligibilityReason::Eligible;
    return candidate;
}

}  // namespace ibex::runtime
