#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/scan_predicates.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
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

void collect_max_id(const Node& node, std::uint64_t& max_id) {
    max_id = std::max(max_id, node.id().value);
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_max_id(*child, max_id);
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& preamble : program.preamble()) {
            if (preamble != nullptr) {
                collect_max_id(*preamble, max_id);
            }
        }
        collect_max_id(program.main_node(), max_id);
    }
}

auto take_unique_child(Node& parent) -> NodePtr {
    auto& children = parent.mutable_children();
    NodePtr child = std::move(children.front());
    children.clear();
    return child;
}

auto is_applied_scan_filter(const Node& node, const std::set<std::string>& applied_sources)
    -> bool {
    const auto* predicate = filter_predicate(node);
    const auto* scan = projected_scan(node);
    if (predicate == nullptr || scan == nullptr || !applied_sources.contains(scan->source_name())) {
        return false;
    }
    std::vector<Expr> conjuncts;
    return append_conjuncts(*predicate, conjuncts);
}

auto remove_filter(NodePtr node, std::uint64_t& next_id) -> NodePtr {
    const auto id = node->id();
    switch (node->kind()) {
        case NodeKind::Filter:
            return take_unique_child(*node);

        case NodeKind::FilterProject: {
            const auto& filter_project = static_cast<const FilterProjectNode&>(*node);
            auto columns = filter_project.columns();
            NodePtr child = take_unique_child(*node);
            auto project = std::make_unique<ProjectNode>(id, std::move(columns));
            project->add_child(std::move(child));
            return project;
        }

        case NodeKind::FilterUpdateProject: {
            const auto& filter_update_project = static_cast<const FilterUpdateProjectNode&>(*node);
            auto fields = filter_update_project.fields();
            auto columns = filter_update_project.project_columns();
            NodePtr child = take_unique_child(*node);
            auto update = std::make_unique<UpdateNode>(NodeId{next_id++}, std::move(fields));
            update->add_child(std::move(child));
            auto project = std::make_unique<ProjectNode>(id, std::move(columns));
            project->add_child(std::move(update));
            return project;
        }

        case NodeKind::FilterHead: {
            const auto count = static_cast<const FilterHeadNode&>(*node).count();
            NodePtr child = take_unique_child(*node);
            auto head = std::make_unique<HeadNode>(id, count);
            head->add_child(std::move(child));
            return head;
        }

        case NodeKind::FilterTail: {
            const auto count = static_cast<const FilterTailNode&>(*node).count();
            NodePtr child = take_unique_child(*node);
            auto tail = std::make_unique<TailNode>(id, count);
            tail->add_child(std::move(child));
            return tail;
        }

        default:
            return node;
    }
}

auto remove_applied_filters(NodePtr node, const std::set<std::string>& applied_sources,
                            std::uint64_t& next_id) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = remove_applied_filters(std::move(child), applied_sources, next_id);
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            preamble = remove_applied_filters(std::move(preamble), applied_sources, next_id);
        }
        auto& main = program.mutable_main_node();
        main = remove_applied_filters(std::move(main), applied_sources, next_id);
    }
    if (is_applied_scan_filter(*node, applied_sources)) {
        return remove_filter(std::move(node), next_id);
    }
    return node;
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

auto remove_applied_scan_filters(NodePtr root, const std::set<std::string>& applied_sources)
    -> NodePtr {
    if (root == nullptr || applied_sources.empty()) {
        return root;
    }

    // Recheck the same proof used for selection pushdown before mutating the
    // plan. This keeps the API safe if its caller overstates what it applied.
    const auto candidates = scan_predicates(*root);
    std::set<std::string> safe_sources;
    for (const auto& source : applied_sources) {
        if (candidates.contains(source)) {
            safe_sources.insert(source);
        }
    }
    if (safe_sources.empty()) {
        return root;
    }

    std::uint64_t max_id = 0;
    collect_max_id(*root, max_id);
    return remove_applied_filters(std::move(root), safe_sources, ++max_id);
}

}  // namespace ibex::ir
