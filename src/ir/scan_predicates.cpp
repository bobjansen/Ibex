#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/scan_predicates.hpp>

#include <cstddef>
#include <map>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

auto append_conjuncts(const Expr& expr, std::vector<Expr>& out) -> bool {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node);
        logical != nullptr && logical->op == LogicalOp::And && logical->right != nullptr) {
        return append_conjuncts(*logical->left, out) && append_conjuncts(*logical->right, out);
    }
    if (!is_subset_evaluable_expr(expr)) {
        return false;
    }
    out.push_back(expr);
    return true;
}

auto projected_scan(const Node& node) -> const ScanNode* {
    if (node.children().size() != 1 || node.children().front() == nullptr) {
        return nullptr;
    }
    const Node* child = node.children().front().get();
    while (child->kind() == NodeKind::Project) {
        if (child->children().size() != 1 || child->children().front() == nullptr) {
            return nullptr;
        }
        child = child->children().front().get();
    }
    if (child->kind() != NodeKind::Scan) {
        return nullptr;
    }
    return static_cast<const ScanNode*>(child);
}

auto filter_predicate(const Node& node) -> const Expr* {
    switch (node.kind()) {
        case NodeKind::Filter:
            return &static_cast<const FilterNode&>(node).predicate();
        case NodeKind::FilterProject:
            return &static_cast<const FilterProjectNode&>(node).predicate();
        case NodeKind::FilterUpdateProject:
            return &static_cast<const FilterUpdateProjectNode&>(node).predicate();
        case NodeKind::FilterHead:
            return &static_cast<const FilterHeadNode&>(node).predicate();
        case NodeKind::FilterTail:
            return &static_cast<const FilterTailNode&>(node).predicate();
        default:
            return nullptr;
    }
}

void visit(const Node& node, std::map<std::string, std::size_t>& scan_counts,
           ScanPredicateMap& candidates) {
    if (node.kind() == NodeKind::Scan) {
        ++scan_counts[static_cast<const ScanNode&>(node).source_name()];
    }

    if (const auto* predicate = filter_predicate(node)) {
        if (const auto* scan = projected_scan(node)) {
            std::vector<Expr> conjuncts;
            if (append_conjuncts(*predicate, conjuncts)) {
                auto& destination = candidates[scan->source_name()];
                destination.insert(destination.end(), conjuncts.begin(), conjuncts.end());
            }
        }
    }

    for (const auto& child : node.children()) {
        if (child != nullptr) {
            visit(*child, scan_counts, candidates);
        }
    }
}

}  // namespace

auto scan_predicates(const Node& root) -> ScanPredicateMap {
    std::map<std::string, std::size_t> scan_counts;
    ScanPredicateMap candidates;
    visit(root, scan_counts, candidates);

    for (auto it = candidates.begin(); it != candidates.end();) {
        if (it->second.empty() || scan_counts[it->first] != 1) {
            it = candidates.erase(it);
        } else {
            ++it;
        }
    }
    return candidates;
}

}  // namespace ibex::ir
