// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_origins.hpp>
#include <ibex/ir/group_key_reduction.hpp>

#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

/// A base column, as `ColumnOrigin` names it, ordered so it can key a set.
using SourceColumn = std::pair<std::string, std::string>;

/// One equijoin edge, both sides resolved to the base columns they came from.
struct JoinEdge {
    SourceColumn left;
    SourceColumn right;
    /// An inner join preserves the dependency in both directions when the far
    /// key is unique. A left join does not: unmatched left rows all carry the
    /// same null right key while their left-side values may differ.
    bool bidirectional = false;
};

auto as_key(const ColumnOrigin& origin) -> SourceColumn {
    return {origin.source, origin.column};
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
        const auto& join = static_cast<const JoinNode&>(node);
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
                const SourceSchemas& sources) -> std::set<SourceColumn> {
    std::set<SourceColumn> closed = seed;
    bool grew = true;
    while (grew) {
        grew = false;
        const std::set<SourceColumn> current = closed;
        const auto add_all_of = [&](const std::string& source) {
            for (const auto& column : facts_for(source, sources).all_columns) {
                if (closed.emplace(source, column).second) {
                    grew = true;
                }
            }
        };
        for (const auto& [source, column] : current) {
            if (facts_for(source, sources).unique_columns.contains(column)) {
                add_all_of(source);
            }
        }
        for (const auto& edge : edges) {
            const bool right_unique =
                facts_for(edge.right.first, sources).unique_columns.contains(edge.right.second);
            const bool left_unique =
                facts_for(edge.left.first, sources).unique_columns.contains(edge.left.second);
            if (right_unique && closed.contains(edge.left)) {
                add_all_of(edge.right.first);
            }
            if (edge.bidirectional && left_unique && closed.contains(edge.right)) {
                add_all_of(edge.left.first);
            }
        }
    }
    return closed;
}

auto rewrite_aggregate(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    auto& agg = static_cast<AggregateNode&>(*node);
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

}  // namespace

auto reduce_functionally_dependent_group_keys(NodePtr root, const SourceSchemas& sources)
    -> NodePtr {
    return walk(std::move(root), sources);
}

}  // namespace ibex::ir
