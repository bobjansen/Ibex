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

/// True when a node relabels or selects columns without touching a row.
///
/// `project_table` and `rename_table` both build their output with
/// `add_column_shared` — they copy no rows at all, and cost O(columns) rather
/// than O(rows). There is nothing in them to parallelize.
auto is_metadata_only_node(ir::NodeKind kind) noexcept -> bool {
    return kind == ir::NodeKind::Project || kind == ir::NodeKind::Rename;
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
        case ir::NodeKind::Project:
        case ir::NodeKind::Rename:
            return true;
        default:
            return false;
    }
}

}  // namespace

auto execution_capability(const ir::Node& node) -> ExecutionCapability {
    // A bare `update` is deliberately NOT a ParallelMap, even though it is
    // row-local and an earlier slice did admit it here.
    //
    // An update is 1:1, and `update_table` builds its output by moving the
    // input, so a passthrough column costs nothing. Running one through a
    // morsel island therefore adds two whole-table copies (the per-morsel
    // gather and the merge concat) to buy parallelism over the computed column
    // alone — and `update_table` can now split that computation across threads
    // by itself, with no copies at all. Measured on 20M rows, net of
    // generation: a heavy update over six columns is 0.32s serial, 0.72s as an
    // island, 0.08s split inside the operator; over two columns, 0.29s / 0.27s
    // / 0.09s. The island loses on the wide table and wins nothing on the
    // narrow one.
    //
    // Filter-shaped nodes stay ParallelMap: their cardinality is
    // data-dependent, so they cannot presize an output, and the island's
    // ordered merger is what resolves that.
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

    // A chain of nothing but Project/Rename has no per-row work in it, so an
    // island would gather every morsel and concatenate the results — two whole
    // table copies — in order to parallelize what is a pointer assignment per
    // column. Measured on 20M rows across six columns, a bare `select` went
    // from 0.65-0.83s serial to 1.20-1.36s as an island, and a bare `rename`
    // from 0.68-0.72s to 1.31-1.45s.
    //
    // They stay ParallelMap rather than being demoted, because a `select` or
    // `rename` sitting above a filter belongs in that filter's island: it is
    // free there, and excluding it would split the chain and materialize
    // between the two halves.
    if (std::ranges::all_of(candidate.operators, [](const ir::Node* node) {
            return is_metadata_only_node(node->kind());
        })) {
        candidate.operators.clear();
        candidate.reason = ParallelEligibilityReason::NoRowWork;
        return candidate;
    }

    std::reverse(candidate.operators.begin(), candidate.operators.end());
    candidate.input = current;
    candidate.reason = ParallelEligibilityReason::Eligible;
    return candidate;
}

}  // namespace ibex::runtime
