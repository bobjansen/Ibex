#include <ibex/ir/join_order.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

struct Relation {
    const Node* node = nullptr;
    CardinalityEstimate estimate;
    SchemaInfo schema;
};

struct JoinEdge {
    std::size_t right_relation = 0;
    std::vector<std::string> keys;
};

/// Accept precisely the left-deep shape emitted by ordinary chained `join`
/// expressions. The restriction keeps key ownership unambiguous for the first
/// physical rewrite; bushy trees and predicates stay in their source order.
auto collect_left_deep(const Node& node, const SourceSchemas& schemas,
                       const SourceRowCounts& row_counts, std::vector<Relation>& relations,
                       std::vector<JoinEdge>& edges) -> bool {
    if (node.kind() != NodeKind::Join) {
        const auto estimate = estimate_cardinality(node, row_counts);
        const auto schema = infer_schema(node, schemas);
        if (!estimate.rows.has_value() || !schema.is_known()) {
            return false;
        }
        relations.push_back(Relation{.node = &node, .estimate = estimate, .schema = schema});
        return true;
    }
    const auto& join = static_cast<const JoinNode&>(node);
    if (join.kind() != JoinKind::Inner || join.predicate().has_value() || join.keys().empty() ||
        join.children().size() != 2 || join.children()[0] == nullptr ||
        join.children()[1] == nullptr || join.children()[1]->kind() == NodeKind::Join) {
        return false;
    }
    if (!collect_left_deep(*join.children()[0], schemas, row_counts, relations, edges) ||
        !collect_left_deep(*join.children()[1], schemas, row_counts, relations, edges)) {
        return false;
    }
    edges.push_back(JoinEdge{.right_relation = relations.size() - 1, .keys = join.keys()});
    return true;
}

auto has_all_keys(const SchemaInfo& schema, const std::vector<std::string>& keys) -> bool {
    return std::ranges::all_of(keys,
                               [&](const std::string& key) { return schema.find(key) != nullptr; });
}

auto group_has_all_keys(const std::vector<Relation>& relations,
                        const std::vector<std::size_t>& group, const std::vector<std::string>& keys)
    -> bool {
    return std::ranges::all_of(keys, [&](const std::string& key) {
        return std::ranges::any_of(group, [&](std::size_t relation) {
            return relations[relation].schema.find(key) != nullptr;
        });
    });
}

}  // namespace

auto choose_inner_join_order(const Node& root, const SourceSchemas& schemas,
                             const SourceRowCounts& row_counts)
    -> std::optional<std::vector<std::size_t>> {
    std::vector<Relation> relations;
    std::vector<JoinEdge> edges;
    if (!collect_left_deep(root, schemas, row_counts, relations, edges) || relations.size() < 2) {
        return std::nullopt;
    }

    const auto rows = [&](std::size_t index) { return *relations[index].estimate.rows; };
    std::vector<std::size_t> order{0};
    for (std::size_t i = 1; i < relations.size(); ++i) {
        if (rows(i) < rows(order.front())) {
            order.front() = i;
        }
    }
    std::vector<bool> selected(relations.size(), false);
    selected[order.front()] = true;

    while (order.size() < relations.size()) {
        std::optional<std::size_t> next;
        for (const auto& edge : edges) {
            std::vector<std::size_t> endpoints;
            endpoints.reserve(edge.right_relation + 1);
            for (std::size_t i = 0; i < edge.right_relation; ++i) {
                endpoints.push_back(i);
            }
            endpoints.push_back(edge.right_relation);
            for (const std::size_t candidate : endpoints) {
                if (selected[candidate] || !has_all_keys(relations[candidate].schema, edge.keys)) {
                    continue;
                }
                const bool connected = std::ranges::any_of(
                    endpoints, [&](std::size_t endpoint) { return selected[endpoint]; });
                if (!connected || !group_has_all_keys(relations, order, edge.keys)) {
                    continue;
                }
                if (!next.has_value() || rows(candidate) < rows(*next)) {
                    next = candidate;
                }
            }
        }
        if (!next.has_value()) {
            return std::nullopt;  // disconnected or key ownership is ambiguous.
        }
        selected[*next] = true;
        order.push_back(*next);
    }
    return order;
}

}  // namespace ibex::ir
