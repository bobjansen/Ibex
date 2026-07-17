#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_order.hpp>
#include <ibex/ir/join_reorder.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <robin_hood.h>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

struct Edge {
    std::size_t right = 0;
    std::vector<std::string> keys;
};

auto next_id() -> std::uint64_t& {
    thread_local std::uint64_t value = 0;
    return value;
}

void max_id(const Node& node, std::uint64_t& value) {
    value = std::max(value, node.id().value);
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            max_id(*child, value);
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& preamble : program.preamble()) {
            if (preamble != nullptr) {
                max_id(*preamble, value);
            }
        }
        max_id(program.main_node(), value);
    }
}

/// Walk the same left-deep inner-join chain as `take_left_deep` WITHOUT taking
/// it apart, so every check that can reject the rewrite runs while the plan is
/// still intact. `take_left_deep` moves children out as it descends and frees
/// what it holds when it bails, which is unrecoverable: a rejection after that
/// point leaves the aggregate with no input at all.
auto scan_left_deep(const Node& node, std::vector<const Node*>& leaves, std::vector<Edge>& edges)
    -> bool {
    if (node.kind() != NodeKind::Join) {
        leaves.push_back(&node);
        return true;
    }
    const auto& join = static_cast<const JoinNode&>(node);
    if (join.kind() != JoinKind::Inner || join.predicate().has_value() || join.keys().empty() ||
        join.children().size() != 2 || join.children()[0] == nullptr ||
        join.children()[1] == nullptr || join.children()[1]->kind() == NodeKind::Join) {
        return false;
    }
    if (!scan_left_deep(*join.children()[0], leaves, edges)) {
        return false;
    }
    leaves.push_back(join.children()[1].get());
    edges.push_back(Edge{.right = leaves.size() - 1, .keys = join.keys()});
    return true;
}

auto take_left_deep(NodePtr node, std::vector<NodePtr>& leaves, std::vector<Edge>& edges) -> bool {
    if (node->kind() != NodeKind::Join) {
        leaves.push_back(std::move(node));
        return true;
    }
    auto* join = static_cast<JoinNode*>(node.get());
    if (join->kind() != JoinKind::Inner || join->predicate().has_value() || join->keys().empty() ||
        join->mutable_children().size() != 2 || join->mutable_children()[0] == nullptr ||
        join->mutable_children()[1] == nullptr ||
        join->mutable_children()[1]->kind() == NodeKind::Join) {
        return false;
    }
    NodePtr left = std::move(join->mutable_children()[0]);
    NodePtr right = std::move(join->mutable_children()[1]);
    join->mutable_children().clear();
    if (!take_left_deep(std::move(left), leaves, edges)) {
        return false;
    }
    leaves.push_back(std::move(right));
    edges.push_back(Edge{.right = leaves.size() - 1, .keys = join->keys()});
    return true;
}

auto aggregate_order_insensitive(const AggregateNode& aggregate) -> bool {
    return std::ranges::none_of(aggregate.aggregations(), [](const AggSpec& spec) {
        return spec.func == AggFunc::First || spec.func == AggFunc::Last;
    });
}

/// True if `node` computes each output row from the corresponding input row
/// alone, so the order and grouping of the rows beneath it cannot change what it
/// produces. Reordering an inner chain changes neither the column set nor the
/// multiset of rows, only their order -- which is exactly what such a node is
/// blind to.
///
/// The order-sensitivity is per-EXPRESSION, not per-node kind: `update
/// { m = rolling_mean(p, 20) }` reads its neighbours, so its values depend on
/// the order the join happens to emit. `is_row_local_update_expr` is the
/// existing front door for that question. A grouped `update ... by {...}` is
/// excluded for the same reason.
auto is_row_wise(const Node& node) -> bool {
    switch (node.kind()) {
        case NodeKind::Project:
        case NodeKind::Rename:
            return true;  // pure column plumbing
        case NodeKind::Filter:
            return is_row_local_update_expr(static_cast<const FilterNode&>(node).predicate());
        case NodeKind::Update: {
            const auto& update = static_cast<const UpdateNode&>(node);
            if (!update.group_by().empty() || !update.tuple_fields().empty()) {
                return false;
            }
            return std::ranges::all_of(update.fields(), [](const FieldSpec& field) {
                return is_row_local_update_expr(field.expr);
            });
        }
        case NodeKind::FilterProject:
            return is_row_local_update_expr(
                static_cast<const FilterProjectNode&>(node).predicate());
        case NodeKind::FilterUpdateProject: {
            const auto& fused = static_cast<const FilterUpdateProjectNode&>(node);
            if (!is_row_local_update_expr(fused.predicate())) {
                return false;
            }
            return std::ranges::all_of(fused.fields(), [](const FieldSpec& field) {
                return is_row_local_update_expr(field.expr);
            });
        }
        default:
            return false;
    }
}

/// The slot holding the aggregate's inner-join chain, which is only rarely the
/// aggregate's immediate child: the aggregate block's own `select` lowers to a
/// Project, and an equijoin's renames to an Update, so on the PDS-H suite the
/// chain sits under a Project or Update on 21 of 22 queries. Returns nullptr
/// when no chain is reachable through row-wise nodes.
auto find_join_chain(NodePtr& slot) -> NodePtr* {
    NodePtr* current = &slot;
    while (*current != nullptr) {
        if ((*current)->kind() == NodeKind::Join) {
            return current;
        }
        if (!is_row_wise(**current) || (*current)->mutable_children().size() != 1) {
            return nullptr;
        }
        current = &(*current)->mutable_children()[0];
    }
    return nullptr;
}

auto schemas_are_unambiguous(const std::vector<const Node*>& leaves, const std::vector<Edge>& edges,
                             const SourceSchemas& schemas) -> bool {
    robin_hood::unordered_set<std::string> join_keys;
    for (const auto& edge : edges) {
        join_keys.insert(edge.keys.begin(), edge.keys.end());
    }
    robin_hood::unordered_set<std::string> seen;
    for (const auto* leaf : leaves) {
        const auto schema = infer_schema(*leaf, schemas);
        if (!schema.is_known() || schema.is_open()) {
            return false;
        }
        for (const auto& field : schema.fields()) {
            if (!seen.insert(field.name).second && !join_keys.contains(field.name)) {
                return false;
            }
        }
    }
    return true;
}

auto connecting_keys(std::size_t candidate, const std::vector<std::size_t>& selected,
                     const std::vector<Edge>& edges) -> const std::vector<std::string>* {
    for (const auto& edge : edges) {
        const bool candidate_in_edge = candidate <= edge.right;
        const bool selected_in_edge = std::ranges::any_of(
            selected, [&](std::size_t relation) { return relation <= edge.right; });
        if (candidate_in_edge && selected_in_edge &&
            std::ranges::any_of(selected,
                                [&](std::size_t relation) { return relation != candidate; })) {
            return &edge.keys;
        }
    }
    return nullptr;
}

auto reorder_aggregate_child(NodePtr child, const SourceStats& stats) -> NodePtr {
    const auto order = choose_inner_join_order(*child, stats);
    if (!order.has_value()) {
        return child;
    }
    bool already_ordered = true;
    for (std::size_t i = 0; i < order->size(); ++i) {
        already_ordered = already_ordered && (*order)[i] == i;
    }
    if (already_ordered) {
        return child;
    }
    // Reject on the intact plan: past `take_left_deep` there is nothing to hand
    // back.
    std::vector<const Node*> preview;
    std::vector<Edge> preview_edges;
    if (!scan_left_deep(*child, preview, preview_edges) ||
        !schemas_are_unambiguous(preview, preview_edges, stats.schemas)) {
        return child;
    }

    std::vector<NodePtr> leaves;
    std::vector<Edge> edges;
    if (!take_left_deep(std::move(child), leaves, edges)) {
        return nullptr;  // unreachable: scan_left_deep applies the same shape checks
    }
    NodePtr result = std::move(leaves[order->front()]);
    std::vector<std::size_t> selected{order->front()};
    for (std::size_t pos = 1; pos < order->size(); ++pos) {
        const std::size_t candidate = (*order)[pos];
        const auto* keys = connecting_keys(candidate, selected, edges);
        if (keys == nullptr || leaves[candidate] == nullptr) {
            return nullptr;
        }
        auto join = std::make_unique<JoinNode>(NodeId{next_id()++}, JoinKind::Inner, *keys);
        join->add_child(std::move(result));
        join->add_child(std::move(leaves[candidate]));
        result = std::move(join);
        selected.push_back(candidate);
    }
    return result;
}

auto walk(NodePtr node, const SourceStats& stats) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), stats);
    }
    if (node->kind() == NodeKind::Aggregate && node->mutable_children().size() == 1 &&
        node->mutable_children()[0] != nullptr) {
        const auto& aggregate = static_cast<const AggregateNode&>(*node);
        if (aggregate_order_insensitive(aggregate)) {
            if (NodePtr* chain = find_join_chain(node->mutable_children()[0])) {
                NodePtr reordered = reorder_aggregate_child(std::move(*chain), stats);
                if (reordered != nullptr) {
                    *chain = std::move(reordered);
                }
            }
        }
    }
    return node;
}

}  // namespace

auto reorder_inner_joins_for_aggregates(NodePtr root, const SourceStats& stats) -> NodePtr {
    if (root != nullptr) {
        std::uint64_t highest = 0;
        max_id(*root, highest);
        next_id() = highest + 1;
    }
    return walk(std::move(root), stats);
}

}  // namespace ibex::ir
