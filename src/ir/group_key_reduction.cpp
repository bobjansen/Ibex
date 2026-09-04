// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_origins.hpp>
#include <ibex/ir/group_key_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <ranges>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

/// A base column at a specific scan, as `ColumnOrigin` names it, ordered so it
/// can key a set. `scan` distinguishes two occurrences of one source (a
/// self-join); `source` is the key for `facts_for` (schema + proved uniqueness,
/// which are source-level facts).
struct SourceColumn {
    NodeId scan{};
    std::string source;
    std::string column;

    auto operator<=>(const SourceColumn&) const = default;
};

/// One equijoin edge, both sides resolved to the base columns they came from.
struct JoinEdge {
    SourceColumn left;
    SourceColumn right;
    /// An inner join preserves the dependency in both directions when the far
    /// key is unique. A left join does not: unmatched left rows all carry the
    /// same null right key while their left-side values may differ.
    bool bidirectional = false;
};

// NOLINTNEXTLINE(misc-no-recursion)
void collect_max_id(const Node& node, std::uint64_t& max) {
    max = std::max(max, node.id().value);
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_max_id(*child, max);
        }
    }
}

auto as_key(const ColumnOrigin& origin) -> SourceColumn {
    return {.scan = origin.scan, .source = origin.source, .column = origin.column};
}

/// Collect every equijoin edge below `node`, with both sides resolved through
/// `column_origins` rather than by name.
///
/// Only Inner and Left joins contribute. On either, a left row maps to at most
/// one right row when the right key is unique, so the left key determines the
/// right table's columns — an unmatched left row takes nulls, which is still a
/// function of the key. The reverse direction is valid only for Inner: a Left
/// join can produce several unmatched rows with the same null right key and
/// different left-side values. Right and Outer are excluded for the symmetric
/// reason.
void collect_join_edges(const Node& node, const SourceSchemas& sources,
                        std::vector<JoinEdge>& out) {
    if (node.kind() == NodeKind::Join && node.children().size() >= 2 &&
        node.children()[0] != nullptr && node.children()[1] != nullptr) {
        const auto& join = node_cast<JoinNode>(node);
        if (join.kind() == JoinKind::Inner || join.kind() == JoinKind::Left) {
            const ColumnOriginMap left = column_origins(*node.children()[0], sources);
            const ColumnOriginMap right = column_origins(*node.children()[1], sources);
            for (const auto& key : join.keys()) {
                const auto left_it = left.find(key.left);
                const auto right_it = right.find(key.right);
                if (left_it != left.end() && right_it != right.end()) {
                    out.push_back(JoinEdge{.left = as_key(left_it->second),
                                           .right = as_key(right_it->second),
                                           .bidirectional = join.kind() == JoinKind::Inner});
                }
            }
        }
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_join_edges(*child, sources, out);
        }
    }
}

/// A source's proved single-column unique keys, and its full column list.
struct SourceFacts {
    std::set<std::string> unique_columns;
    std::vector<std::string> all_columns;
};

auto facts_for(const std::string& source, const SourceSchemas& sources) -> SourceFacts {
    SourceFacts facts;
    const auto it = sources.find(source);
    if (it == sources.end() || !it->second.is_known()) {
        return facts;
    }
    for (const auto& field : it->second.fields()) {
        facts.all_columns.push_back(field.name);
    }
    for (const auto& key : it->second.unique_keys()) {
        // Single-column keys only. A composite key determines just as much, but
        // needs every part present in the group key to fire, and no query here
        // has one — leaving it out keeps the closure a plain reachability.
        if (key.size() == 1) {
            facts.unique_columns.insert(key.front());
        }
    }
    return facts;
}

/// Everything the seed set functionally determines.
///
/// Two rules, applied to fixpoint:
///   - a source's unique column determines every column OF THAT SOURCE;
///   - a join edge whose far side is unique carries determination ACROSS it,
///     so the near column determines every column of the far source.
///
/// The second is what makes it transitive, and q10 needs that: `c_custkey`
/// reaches `c_nationkey` by the first rule, then `n_name` by the second.
auto fd_closure(const std::set<SourceColumn>& seed, const std::vector<JoinEdge>& edges,
                const SourceSchemas& sources, const SourceColumn* assumed_unique = nullptr)
    -> std::set<SourceColumn> {
    std::set<SourceColumn> closed = seed;
    bool grew = true;
    while (grew) {
        grew = false;
        const std::set<SourceColumn> current = closed;
        // A unique column determines every column OF THAT OCCURRENCE — the same
        // `scan`, not just the same `source`, or a proof on one side of a
        // self-join would collapse the other side's group keys.
        const auto add_all_of = [&](const SourceColumn& rep) {
            for (const auto& column : facts_for(rep.source, sources).all_columns) {
                if (closed
                        .emplace(
                            SourceColumn{.scan = rep.scan, .source = rep.source, .column = column})
                        .second) {
                    grew = true;
                }
            }
        };
        for (const auto& sc : current) {
            if ((assumed_unique != nullptr && sc == *assumed_unique) ||
                facts_for(sc.source, sources).unique_columns.contains(sc.column)) {
                add_all_of(sc);
            }
        }
        for (const auto& edge : edges) {
            const bool right_unique =
                (assumed_unique != nullptr && edge.right == *assumed_unique) ||
                facts_for(edge.right.source, sources).unique_columns.contains(edge.right.column);
            const bool left_unique =
                (assumed_unique != nullptr && edge.left == *assumed_unique) ||
                facts_for(edge.left.source, sources).unique_columns.contains(edge.left.column);
            if (right_unique && closed.contains(edge.left)) {
                add_all_of(edge.right);
            }
            if (edge.bidirectional && left_unique && closed.contains(edge.right)) {
                add_all_of(edge.left);
            }
        }
    }
    return closed;
}

auto rewrite_aggregate(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    auto& agg = node_cast<AggregateNode>(*node);
    const auto& group_by = agg.group_by();
    if (group_by.size() < 2 || agg.children().empty() || agg.children().front() == nullptr) {
        return node;
    }
    const Node& input = *agg.children().front();
    const ColumnOriginMap origins = column_origins(input, sources);

    // Every key must trace to a base column, or nothing can be proved about it.
    std::vector<SourceColumn> key_origin;
    key_origin.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto it = origins.find(key.name);
        if (it == origins.end()) {
            return node;
        }
        key_origin.push_back(as_key(it->second));
    }

    std::vector<JoinEdge> edges;
    collect_join_edges(input, sources, edges);

    // Drop a key whose origin the REMAINING keys already determine. Walking
    // backwards keeps the earliest keys, so the surviving group key is the one
    // the query led with -- `c_custkey` in q10 -- which is also the one most
    // likely to be cheap to hash.
    std::vector<bool> keep(group_by.size(), true);
    for (std::size_t i = group_by.size(); i-- > 0;) {
        std::set<SourceColumn> seed;
        for (std::size_t j = 0; j < group_by.size(); ++j) {
            if (j != i && keep[j]) {
                seed.insert(key_origin[j]);
            }
        }
        if (seed.empty()) {
            continue;  // never reduce to no key at all: that is a different query
        }
        if (fd_closure(seed, edges, sources).contains(key_origin[i])) {
            keep[i] = false;
        }
    }
    if (std::ranges::all_of(keep, [](bool k) { return k; })) {
        return node;  // nothing proved; leave the plan alone
    }

    // Reduced keys, then the original aggregates, then a `first()` for each
    // dropped key -- exact because a dropped key is constant within a group.
    std::vector<ColumnRef> reduced;
    std::vector<AggSpec> aggregations = agg.aggregations();
    for (std::size_t i = 0; i < group_by.size(); ++i) {
        if (keep[i]) {
            reduced.push_back(group_by[i]);
        } else {
            aggregations.push_back(
                AggSpec{.func = AggFunc::First, .column = group_by[i], .alias = group_by[i].name});
        }
    }

    auto rebuilt =
        std::make_unique<AggregateNode>(agg.id(), std::move(reduced), std::move(aggregations));
    rebuilt->add_child(std::move(agg.mutable_children().front()));

    // The aggregate emits keys first and aggregates after, so the reduction
    // reorders the output. Restore the original order; without this a caller
    // reading columns positionally -- `write_csv`, an ascription -- would see a
    // different table for the same query.
    std::vector<ColumnRef> ordered;
    ordered.reserve(group_by.size() + agg.aggregations().size());
    for (const auto& key : group_by) {
        ordered.push_back(key);
    }
    for (const auto& spec : agg.aggregations()) {
        ordered.push_back(ColumnRef{.name = spec.alias});
    }
    auto project = std::make_unique<ProjectNode>(agg.id(), std::move(ordered));
    project->add_child(std::move(rebuilt));
    return project;
}

// NOLINTNEXTLINE(misc-no-recursion)
auto walk(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), sources);
    }
    if (node->kind() == NodeKind::Aggregate) {
        return rewrite_aggregate(std::move(node), sources);
    }
    return node;
}

/// ---------------------------------------------------------------------------
/// Late materialization of FD-determined payload columns
/// ---------------------------------------------------------------------------
///
/// `rewrite_aggregate` narrows the group KEY but recovers each dropped key with
/// `first()`, which keeps those columns referenced all the way down to the
/// scan. For q10 that is four wide `Column<std::string>` columns (`c_name`,
/// `c_address`, `c_phone`, `c_comment`, ~130 bytes/row over 1.2M customers)
/// decoded from Parquet, gathered through three joins and written into ~304K
/// aggregate slots -- so that `head 20` can discard 99.99% of them.
///
/// When a top-k sits above the aggregate, those columns can instead be fetched
/// for the surviving rows: drop them from the aggregate, and join the source
/// back on above the `head`. Once they are unreferenced in the main pipeline,
/// the existing projection pushdown stops decoding them, which is where two
/// thirds of the win comes from -- the join and aggregate savings are secondary.
///
/// Measured SF-8 (identical 20 rows, identical order): 1.06s -> 0.75s at one
/// core, 0.59s -> 0.40s at eight.
///
/// **Gated structurally, never on a cost estimate.** A payload re-fetch has the
/// same failure shape as the deferred-probe join that regressed q12: attractive
/// on paper, negative when many rows survive the aggregate. The rewrite applies
/// only when a `Head` with a literal count sits above, and the intervening
/// `order` does not sort on a lifted column. That is a pattern match, so it
/// cannot misfire on a shape it was never measured on.
///
/// v1 lifts only columns belonging to the retained key's OWN source occurrence,
/// which needs a single join keyed directly on the surviving group key. A
/// multi-hop lift (q10's `n_name`, two joins away) measured ~3% more at eight
/// cores and is deliberately left out.
struct LiftPlan {
    ColumnRef group_key;          ///< The surviving group key, in aggregate output names.
    SourceColumn key_origin;      ///< Where that key comes from.
    std::vector<AggSpec> lifted;  ///< `First` aggregates to move above the top-k.
};

/// The `Aggregate` under a chain of `Order`/`Project` nodes, or nullptr.
auto aggregate_below(Node& node, std::vector<Node*>& chain) -> AggregateNode* {
    Node* cur = &node;
    while (cur != nullptr) {
        if (cur->kind() == NodeKind::Aggregate) {
            return &node_cast<AggregateNode>(*cur);
        }
        if (cur->kind() != NodeKind::Order && cur->kind() != NodeKind::Project) {
            return nullptr;
        }
        chain.push_back(cur);
        if (cur->children().empty() || cur->children().front() == nullptr) {
            return nullptr;
        }
        cur = cur->mutable_children().front().get();
    }
    return nullptr;
}

/// Find the `ScanNode` with `id`, so a re-fetch can copy its source name and
/// ascribed schema rather than invent them.
// NOLINTNEXTLINE(misc-no-recursion)
auto find_scan(const Node& node, NodeId id) -> const ScanNode* {
    if (node.kind() == NodeKind::Scan && node.id() == id) {
        return &node_cast<ScanNode>(node);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            if (const auto* found = find_scan(*child, id); found != nullptr) {
                return found;
            }
        }
    }
    return nullptr;
}

/// Decide what a `Head` node can lift, or nullopt.
auto plan_lift(Node& head_node, const Node& root, const SourceSchemas& sources)
    -> std::optional<LiftPlan> {
    const auto& head = node_cast<HeadNode>(head_node);
    // A literal count bounds the re-fetch input; an expression could be
    // anything, and `head ..., by k` is per-group, a different query.
    if (!head.count_literal().has_value() || !head.group_by().empty()) {
        return std::nullopt;
    }
    if (head_node.children().empty() || head_node.mutable_children().front() == nullptr) {
        return std::nullopt;
    }

    std::vector<Node*> chain;
    AggregateNode* agg = aggregate_below(*head_node.mutable_children().front(), chain);
    if (agg == nullptr || agg->group_by().size() != 1 || agg->children().empty() ||
        agg->children().front() == nullptr) {
        return std::nullopt;
    }

    const ColumnOriginMap origins = column_origins(*agg->children().front(), sources);
    const auto key_it = origins.find(agg->group_by().front().name);
    if (key_it == origins.end()) {
        return std::nullopt;
    }
    const SourceColumn key_origin = as_key(key_it->second);
    // The re-fetch joins the source on this key, so it must actually identify a
    // row there. Without that the join could multiply rows.
    if (!facts_for(key_origin.source, sources).unique_columns.contains(key_origin.column)) {
        return std::nullopt;
    }
    if (find_scan(root, key_origin.scan) == nullptr) {
        return std::nullopt;
    }

    // Liftable: `first()` of a column from the SAME source occurrence as the
    // key. Same occurrence, not merely the same source, or a self-join's two
    // sides would be conflated.
    std::vector<AggSpec> lifted;
    for (const auto& spec : agg->aggregations()) {
        if (spec.func != AggFunc::First) {
            continue;
        }
        const auto it = origins.find(spec.column.name);
        if (it == origins.end()) {
            continue;
        }
        const SourceColumn from = as_key(it->second);
        if (from.scan == key_origin.scan && from.source == key_origin.source) {
            lifted.push_back(spec);
        }
    }
    if (lifted.empty()) {
        return std::nullopt;
    }

    // The top-k must not sort on anything being lifted: those columns will not
    // exist until after the join, and re-sorting cannot recover a k chosen by a
    // different ordering.
    for (const Node* link : chain) {
        if (link->kind() != NodeKind::Order) {
            continue;
        }
        for (const auto& order_key : node_cast<OrderNode>(*link).keys()) {
            const bool lifts_sort_key = std::ranges::any_of(
                lifted, [&](const AggSpec& spec) { return spec.alias == order_key.name; });
            if (lifts_sort_key) {
                return std::nullopt;
            }
        }
    }

    return LiftPlan{.group_key = agg->group_by().front(),
                    .key_origin = key_origin,
                    .lifted = std::move(lifted)};
}

/// Rewrite `head_node` into `Project(Order(Join(head, scan)))`, with the lifted
/// columns removed from everything below.
auto apply_lift(NodePtr head_node, const Node& root, const LiftPlan& plan, std::uint64_t& next)
    -> NodePtr {
    const auto is_lifted = [&](const std::string& name) {
        return std::ranges::any_of(plan.lifted,
                                   [&](const AggSpec& spec) { return spec.alias == name; });
    };

    // The output the caller currently sees, which the rewrite must reproduce
    // exactly -- `write_csv` and ascriptions read columns positionally.
    std::vector<ColumnRef> final_columns;
    {
        std::vector<Node*> chain;
        Node* below = head_node->mutable_children().front().get();
        const AggregateNode* agg = aggregate_below(*below, chain);
        const Node* topmost_project = nullptr;
        for (const Node* link : chain) {
            if (link->kind() == NodeKind::Project) {
                topmost_project = link;
                break;
            }
        }
        if (topmost_project != nullptr) {
            final_columns = node_cast<ProjectNode>(*topmost_project).columns();
        } else {
            for (const auto& key : agg->group_by()) {
                final_columns.push_back(key);
            }
            for (const auto& spec : agg->aggregations()) {
                final_columns.push_back(ColumnRef{.name = spec.alias});
            }
        }
    }

    // Strip the lifted columns from the aggregate and from every projection
    // between it and the head.
    {
        std::vector<Node*> chain;
        AggregateNode* agg = aggregate_below(*head_node->mutable_children().front(), chain);
        std::vector<AggSpec> kept;
        for (const auto& spec : agg->aggregations()) {
            if (spec.func != AggFunc::First || !is_lifted(spec.alias)) {
                kept.push_back(spec);
            }
        }
        auto narrowed =
            std::make_unique<AggregateNode>(agg->id(), agg->group_by(), std::move(kept));
        narrowed->add_child(std::move(agg->mutable_children().front()));

        // Rebuild the chain bottom-up, dropping lifted columns from projections.
        NodePtr rebuilt = std::move(narrowed);
        for (auto& it : std::views::reverse(chain)) {
            Node& link = *it;
            if (link.kind() == NodeKind::Project) {
                std::vector<ColumnRef> cols;
                for (const auto& col : node_cast<ProjectNode>(link).columns()) {
                    if (!is_lifted(col.name)) {
                        cols.push_back(col);
                    }
                }
                auto project = std::make_unique<ProjectNode>(link.id(), std::move(cols));
                project->add_child(std::move(rebuilt));
                rebuilt = std::move(project);
            } else {
                auto order =
                    std::make_unique<OrderNode>(link.id(), node_cast<OrderNode>(link).keys());
                order->add_child(std::move(rebuilt));
                rebuilt = std::move(order);
            }
        }
        head_node->mutable_children().front() = std::move(rebuilt);
    }

    // The re-fetch side: the same source, projected to the key plus the lifted
    // columns, so nothing else is decoded for it.
    const ScanNode& original = *find_scan(root, plan.key_origin.scan);
    auto scan = std::make_unique<ScanNode>(NodeId{next++}, original.source_name());
    if (original.ascribed_schema().has_value()) {
        const auto& ascribed = *original.ascribed_schema();
        std::vector<SchemaField> fields;
        for (const auto& field : ascribed.fields) {
            const bool needed = field.name == plan.key_origin.column ||
                                std::ranges::any_of(plan.lifted, [&](const AggSpec& spec) {
                                    return spec.column.name == field.name;
                                });
            if (needed) {
                fields.push_back(field);
            }
        }
        scan->set_ascribed_schema(std::move(fields), ascribed.open);
    }
    std::vector<ColumnRef> scan_columns;
    scan_columns.push_back(ColumnRef{.name = plan.key_origin.column});
    for (const auto& spec : plan.lifted) {
        scan_columns.push_back(spec.column);
    }
    auto scan_project = std::make_unique<ProjectNode>(NodeId{next++}, std::move(scan_columns));
    scan_project->add_child(std::move(scan));

    // `fold_output` keeps the key as ONE output column even though both inputs
    // spell it the same way, which a plain join rejects as a collision.
    std::vector<JoinKey> keys;
    keys.emplace_back(plan.group_key.name, plan.key_origin.column, /*fold=*/true);
    auto join = std::make_unique<JoinNode>(NodeId{next++}, JoinKind::Inner, std::move(keys));
    join->add_child(std::move(head_node));
    join->add_child(std::move(scan_project));

    // A join's row order is outside the contract (SPEC 5.6), so the ordering the
    // top-k established has to be re-established here. It is k rows.
    NodePtr top = std::move(join);
    {
        std::vector<Node*> chain;
        // Re-read the order keys from the rebuilt chain under the join's left.
        Node* left = top->mutable_children().front().get();
        aggregate_below(*left->mutable_children().front(), chain);
        for (const Node* link : chain) {
            if (link->kind() == NodeKind::Order) {
                auto order =
                    std::make_unique<OrderNode>(NodeId{next++}, node_cast<OrderNode>(*link).keys());
                order->add_child(std::move(top));
                top = std::move(order);
                break;
            }
        }
    }

    auto restore = std::make_unique<ProjectNode>(NodeId{next++}, std::move(final_columns));
    restore->add_child(std::move(top));
    return restore;
}

// NOLINTNEXTLINE(misc-no-recursion)
auto lift_walk(NodePtr node, const Node& root, const SourceSchemas& sources, std::uint64_t& next)
    -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = lift_walk(std::move(child), root, sources, next);
    }
    if (node->kind() == NodeKind::Head) {
        if (auto plan = plan_lift(*node, root, sources); plan.has_value()) {
            return apply_lift(std::move(node), root, *plan, next);
        }
    }
    return node;
}

// NOLINTNEXTLINE(misc-no-recursion)
void collect_proof_candidates(const Node& node, const SourceSchemas& sources,
                              std::map<std::string, std::set<std::string>>& out) {
    if (node.kind() == NodeKind::Aggregate) {
        const auto& aggregate = node_cast<AggregateNode>(node);
        if (aggregate.group_by().size() >= 2 && !node.children().empty() &&
            node.children().front() != nullptr) {
            const ColumnOriginMap origins = column_origins(*node.children().front(), sources);
            std::vector<SourceColumn> key_origins;
            key_origins.reserve(aggregate.group_by().size());
            for (const auto& key : aggregate.group_by()) {
                const auto it = origins.find(key.name);
                if (it == origins.end()) {
                    key_origins.clear();
                    break;
                }
                key_origins.push_back(as_key(it->second));
            }
            std::vector<JoinEdge> edges;
            collect_join_edges(*node.children().front(), sources, edges);
            for (const auto& candidate : key_origins) {
                const std::set<SourceColumn> closure =
                    fd_closure({candidate}, edges, sources, &candidate);
                if (std::ranges::all_of(key_origins, [&](const SourceColumn& key) {
                        return closure.contains(key);
                    })) {
                    out[candidate.source].insert(candidate.column);
                }
            }
        }
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_proof_candidates(*child, sources, out);
        }
    }
}

}  // namespace

auto reduce_functionally_dependent_group_keys(NodePtr root, const SourceSchemas& sources)
    -> NodePtr {
    root = walk(std::move(root), sources);
    if (root == nullptr) {
        return root;
    }
    // Second pass, deliberately after the reduction: it lifts the `first()`
    // aggregates the reduction just created (and any the user wrote that happen
    // to qualify), so it has to see the shape the reduction leaves behind.
    std::uint64_t next = 0;
    collect_max_id(*root, next);
    ++next;
    const Node& root_ref = *root;
    return lift_walk(std::move(root), root_ref, sources, next);
}

auto group_key_proof_candidates(const Node& root, const SourceSchemas& sources)
    -> std::map<std::string, std::set<std::string>> {
    std::map<std::string, std::set<std::string>> out;
    collect_proof_candidates(root, sources, out);
    return out;
}

}  // namespace ibex::ir
