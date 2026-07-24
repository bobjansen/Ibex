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

/// True for an update the island may run one morsel at a time.
///
/// The field test is `is_subset_evaluable_expr` (scalar-only), deliberately
/// stricter than the `is_row_local_update_expr` that routes an update to the
/// serial `ChunkedUpdateOperator`. That looser predicate admits aggregate
/// calls: `y = x - mean(x)` passes it, and per morsel that would mean a
/// per-morsel aggregate. Evaluation happens to reject an ungrouped aggregate in
/// an update today, but eligibility must not rest on another layer's error —
/// the serial path also gets away with the looser test only because its source
/// hands over the whole table as one chunk, and an island must not inherit that
/// assumption.
auto is_row_local_update_node(const ir::UpdateNode& update) -> bool {
    if (update.guard() != nullptr || !update.group_by().empty() || !update.tuple_fields().empty()) {
        return false;
    }
    return std::ranges::all_of(update.fields(), [](const ir::FieldSpec& field) {
        return ir::is_subset_evaluable_expr(field.expr);
    });
}

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
        case ir::NodeKind::Update:
            // Already proved by execution_capability(const Node&); a node that
            // failed it never reaches here as a ParallelMap.
            return true;
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
            return true;
        default:
            return false;
    }
}

}  // namespace

auto execution_capability(const ir::Node& node) -> ExecutionCapability {
    if (node.kind() == ir::NodeKind::Update) {
        return is_row_local_update_node(static_cast<const ir::UpdateNode&>(node))
                   ? ExecutionCapability::ParallelMap
                   : ExecutionCapability::Barrier;
    }
    return execution_capability(node.kind());
}

auto analyze_parallel_island(const ir::Node& root) -> ParallelIslandCandidate {
    ParallelIslandCandidate candidate;
    const ir::Node* current = &root;

    while (execution_capability(*current) == ExecutionCapability::ParallelMap) {
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
