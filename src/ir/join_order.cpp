#include <ibex/ir/join_order.hpp>

#include <algorithm>
#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

struct Relation {
    const Node* node = nullptr;
    double rows = 0;
    SchemaInfo schema;
    /// Estimated distinct values for each column this relation contributes to a
    /// join key. Absent means the plan cannot be costed at all -- see
    /// `collect_left_deep`.
    std::map<std::string, double> distinct;
};

struct JoinEdge {
    std::size_t right_relation = 0;
    std::vector<std::string> keys;
};

/// Accept precisely the left-deep shape emitted by ordinary chained `join`
/// expressions. The restriction keeps key ownership unambiguous for the first
/// physical rewrite; bushy trees and predicates stay in their source order.
auto collect_left_deep(const Node& node, const SourceStats& stats, std::vector<Relation>& relations,
                       std::vector<JoinEdge>& edges) -> bool {
    if (node.kind() != NodeKind::Join) {
        const auto estimate = estimate_cardinality(node, stats.rows, stats.schemas);
        const auto schema = infer_schema(node, stats.schemas);
        if (!estimate.rows.has_value() || !schema.is_known()) {
            return false;
        }
        relations.push_back(Relation{.node = &node,
                                     .rows = static_cast<double>(*estimate.rows),
                                     .schema = schema,
                                     .distinct = {}});
        return true;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& join = static_cast<const JoinNode&>(node);
    if (join.kind() != JoinKind::Inner || join.predicate().has_value() || join.keys().empty() ||
        join.children().size() != 2 || join.children()[0] == nullptr ||
        join.children()[1] == nullptr || join.children()[1]->kind() == NodeKind::Join) {
        return false;
    }
    if (!collect_left_deep(*join.children()[0], stats, relations, edges) ||
        !collect_left_deep(*join.children()[1], stats, relations, edges)) {
        return false;
    }
    edges.push_back(JoinEdge{.right_relation = relations.size() - 1, .keys = join.keys()});
    return true;
}

auto has_all_keys(const SchemaInfo& schema, const std::vector<std::string>& keys) -> bool {
    return std::ranges::all_of(keys,
                               [&](const std::string& key) { return schema.find(key) != nullptr; });
}

/// Fill in every relation's distinct estimate for every column used as a join
/// key. Returns false if any is missing: a cost model that invents one number
/// is a cost model that reorders on a guess, and the whole point of declining is
/// that a well-written query keeps the order its author chose.
auto resolve_key_distincts(std::vector<Relation>& relations, const std::vector<JoinEdge>& edges,
                           const SourceStats& stats) -> bool {
    std::vector<std::string> key_columns;
    for (const auto& edge : edges) {
        for (const auto& key : edge.keys) {
            if (std::ranges::find(key_columns, key) == key_columns.end()) {
                key_columns.push_back(key);
            }
        }
    }
    for (auto& relation : relations) {
        for (const auto& key : key_columns) {
            if (relation.schema.find(key) == nullptr) {
                continue;  // this relation does not carry that key
            }
            const auto distinct = distinct_estimate(*relation.node, key, stats);
            if (!distinct.has_value()) {
                return false;
            }
            // A key with no distinct values only arises from an empty relation;
            // clamp so it can never divide by zero below.
            relation.distinct[key] = std::max<double>(1.0, static_cast<double>(*distinct));
        }
    }
    return true;
}

/// One relation, or several already joined together: the running left side of
/// the chain being built.
struct Group {
    std::vector<std::size_t> members;
    double rows = 0;
    std::map<std::string, double> distinct;
};

auto make_group(std::size_t index, const Relation& relation) -> Group {
    return Group{.members = {index}, .rows = relation.rows, .distinct = relation.distinct};
}

auto group_carries(const Group& group, const std::vector<Relation>& relations,
                   const std::string& key) -> bool {
    return std::ranges::any_of(group.members, [&](std::size_t member) {
        return relations[member].schema.find(key) != nullptr;
    });
}

/// Distinct values of `keys` taken together, assuming the key columns are
/// independent -- the textbook assumption, and the only one available without
/// multi-column statistics. Capped at the row count: a tuple cannot be more
/// distinct than there are rows to hold it.
auto combined_distinct(const std::map<std::string, double>& distinct,
                       const std::vector<std::string>& keys, double rows) -> double {
    double product = 1.0;
    for (const auto& key : keys) {
        const auto found = distinct.find(key);
        if (found == distinct.end()) {
            return rows;  // unknown for this key; assume it splits nothing further
        }
        product *= found->second;
    }
    return std::max(1.0, std::min(product, rows));
}

/// Estimated rows out of joining `group` to `relation` on `keys`.
///
/// The textbook equijoin estimate: `|L| * |R| / max(d_L, d_R)`. Each distinct
/// key value on the smaller-domain side pairs with `|L|/d_L` rows on one side
/// and `|R|/d_R` on the other; dividing by the LARGER domain encodes the
/// containment assumption -- that the side with fewer distinct values has its
/// values contained in the other's. For a PK-FK join this is exactly `|FK|`,
/// and for a *filtered* PK side it scales down with the fraction of keys that
/// survived, which is the case a bound on `|FK|` alone cannot see.
auto join_rows(const Group& group, const Relation& relation, const std::vector<std::string>& keys)
    -> double {
    const double group_distinct = combined_distinct(group.distinct, keys, group.rows);
    const double relation_distinct = combined_distinct(relation.distinct, keys, relation.rows);
    const double divisor = std::max(group_distinct, relation_distinct);
    return group.rows * relation.rows / std::max(1.0, divisor);
}

void absorb(Group& group, std::size_t index, const Relation& relation,
            const std::vector<std::string>& keys) {
    const double rows = join_rows(group, relation, keys);
    for (const auto& [column, distinct] : relation.distinct) {
        auto found = group.distinct.find(column);
        group.distinct[column] =
            found == group.distinct.end() ? distinct : std::min(found->second, distinct);
    }
    // Dropping rows cannot leave more distinct values than rows remain; growing
    // them (a fan-out) cannot create new values either.
    for (auto& [column, distinct] : group.distinct) {
        distinct = std::max(1.0, std::min(distinct, rows));
    }
    (void)keys;
    group.members.push_back(index);
    group.rows = rows;
}

/// The edge connecting `candidate` to a group, if the group can supply its keys.
auto connecting_edge(const Group& group, const std::vector<Relation>& relations,
                     std::size_t candidate, const std::vector<JoinEdge>& edges) -> const JoinEdge* {
    for (const auto& edge : edges) {
        if (!has_all_keys(relations[candidate].schema, edge.keys)) {
            continue;
        }
        const bool group_has_keys = std::ranges::all_of(edge.keys, [&](const std::string& key) {
            return group_carries(group, relations, key);
        });
        if (group_has_keys) {
            return &edge;
        }
    }
    return nullptr;
}

/// Greedily extend `seed` by always taking the relation that produces the
/// SMALLEST join, accumulating the size of every intermediate as the cost.
///
/// Ranking by the join's output rather than by the candidate relation's own size
/// is the whole point. "Smallest table next" walks q09 into joining 10k
/// suppliers against 6M lineitems third, because lineitem is small in no sense
/// that matters -- what matters is the 6M-row intermediate it makes, which every
/// later join then probes.
/// The cost of one candidate order. Compared lexicographically: the LARGEST
/// intermediate first, then the total. Minimizing the worst intermediate is what
/// separates a good order from one that merely looks cheap on average -- q08's
/// reordered chain builds a single 6M-row intermediate (joining the fact table
/// before a selective dimension filter applies) that a sum-of-intermediates cost
/// lets hide among smaller terms, but that dominates the actual run. The author
/// who filtered first never builds it, and minimax prefers their order.
struct OrderCost {
    double peak = 0;   ///< largest intermediate built
    double total = 0;  ///< sum of all intermediates
    auto operator<(const OrderCost& other) const -> bool {
        if (peak != other.peak) {
            return peak < other.peak;
        }
        return total < other.total;
    }
};

auto greedy_from(std::size_t seed, const std::vector<Relation>& relations,
                 const std::vector<JoinEdge>& edges)
    -> std::optional<std::pair<OrderCost, std::vector<std::size_t>>> {
    Group group = make_group(seed, relations[seed]);
    std::vector<bool> taken(relations.size(), false);
    taken[seed] = true;
    OrderCost cost;

    while (group.members.size() < relations.size()) {
        std::optional<std::size_t> best;
        const JoinEdge* best_edge = nullptr;
        double best_rows = 0;
        for (std::size_t candidate = 0; candidate < relations.size(); ++candidate) {
            if (taken[candidate]) {
                continue;
            }
            const auto* edge = connecting_edge(group, relations, candidate, edges);
            if (edge == nullptr) {
                continue;
            }
            const double rows = join_rows(group, relations[candidate], edge->keys);
            if (!best.has_value() || rows < best_rows) {
                best = candidate;
                best_edge = edge;
                best_rows = rows;
            }
        }
        if (!best.has_value()) {
            return std::nullopt;  // disconnected from this seed
        }
        absorb(group, *best, relations[*best], best_edge->keys);
        cost.peak = std::max(cost.peak, group.rows);
        cost.total += group.rows;
        taken[*best] = true;
    }
    return std::pair{cost, group.members};
}

}  // namespace

auto choose_inner_join_order(const Node& root, const SourceStats& stats)
    -> std::optional<std::vector<std::size_t>> {
    std::vector<Relation> relations;
    std::vector<JoinEdge> edges;
    if (!collect_left_deep(root, stats, relations, edges) || relations.size() < 2) {
        return std::nullopt;
    }
    // Chains beyond this many relations are left in their author's order. The
    // footer-derived estimates are reliable enough to reorder short chains (q02
    // gains 30%, deliberately bad 3-table orders are rescued to parity), but on
    // a wide snowflake the greedy can seed from a tiny unfiltered dimension and
    // defer a selective filter it cannot see the pruning of -- q08 (8 relations)
    // reorders to a plan its estimate rates cheap but that runs ~11% slower.
    // Long-chain ordering genuinely needs join-graph-aware statistics a Parquet
    // footer does not carry; until then, respect what the author wrote.
    constexpr std::size_t kMaxReorderRelations = 5;
    if (relations.size() > kMaxReorderRelations) {
        return std::nullopt;
    }
    if (!resolve_key_distincts(relations, edges, stats)) {
        return std::nullopt;
    }

    // Try every relation as the starting point, not just the smallest. The seed
    // decides which relations are even reachable next, so a greedy from one seed
    // can be forced into a join no ranking would have chosen: q09's smallest
    // relation is nation, whose only neighbour is supplier, whose only remaining
    // neighbour is the 6M-row lineitem. Costing all N seeds is N greedy walks
    // over a handful of relations -- nothing next to reading one column.
    std::optional<OrderCost> best_cost;
    std::optional<std::vector<std::size_t>> best_order;
    for (std::size_t seed = 0; seed < relations.size(); ++seed) {
        auto result = greedy_from(seed, relations, edges);
        if (!result.has_value()) {
            continue;
        }
        auto& [cost, order] = *result;
        if (!best_cost.has_value() || cost < *best_cost) {
            best_cost = cost;
            best_order = std::move(order);
        }
    }
    return best_order;
}

}  // namespace ibex::ir
