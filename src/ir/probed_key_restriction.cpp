// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/probed_key_restriction.hpp>
#include <ibex/ir/replayable.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

/// The join keys that name one of the aggregate's group keys on the right.
///
/// Only those can be restricted: the semi join runs against the aggregate's
/// INPUT, where a group key still exists under its own name, while an
/// aggregate's OUTPUT column does not exist below it at all. A join that keys
/// on an aggregated value therefore contributes nothing here, and a join that
/// keys on both restricts by the group keys alone -- which is sound, since
/// restricting by fewer keys only keeps more groups.
auto restrictable_keys(const JoinNode& join, const AggregateNode& aggregate)
    -> std::vector<JoinKey> {
    std::vector<JoinKey> keys;
    for (const auto& key : join.keys()) {
        const bool is_group_key = std::ranges::any_of(
            aggregate.group_by(), [&](const ColumnRef& group) { return group.name == key.right; });
        if (is_group_key) {
            keys.push_back(key);
        }
    }
    return keys;
}

/// The aggregate the join's right side feeds from, or null.
///
/// Projection pushdown leaves a Project between the join and the aggregate, so
/// the aggregate is not always the direct child. Only nodes that pass every
/// row through AND leave column names alone are crossed: a Project selects but
/// does not rename, so a group key reaching the join still carries the
/// aggregate's own name for it. A Rename would break that correspondence and
/// stops the descent.
auto aggregate_below(Node& node, const std::vector<std::string>& key_names) -> AggregateNode* {
    Node* current = &node;
    while (current != nullptr) {
        if (current->kind() == NodeKind::Aggregate) {
            return &node_cast<AggregateNode>(*current);
        }
        if (current->children().size() != 1) {
            return nullptr;
        }
        switch (current->kind()) {
            case NodeKind::Project:
            case NodeKind::Ascribe:
                break;
            case NodeKind::Update: {
                // An arithmetic wrapper around the aggregate's value -- q17's
                // `0.2 * mean(..)` is one -- passes every row through and
                // leaves the group key alone. One that WRITES a key column
                // does not, and the name reaching the join would no longer be
                // the aggregate's.
                const auto& update = node_cast<UpdateNode>(*current);
                const bool writes_a_key =
                    std::ranges::any_of(update.fields(),
                                        [&](const FieldSpec& field) {
                                            return std::ranges::find(key_names, field.alias) !=
                                                   key_names.end();
                                        }) ||
                    std::ranges::any_of(update.tuple_fields(), [&](const TupleFieldSpec& tuple) {
                        return std::ranges::any_of(tuple.aliases, [&](const std::string& alias) {
                            return std::ranges::find(key_names, alias) != key_names.end();
                        });
                    });
                if (writes_a_key) {
                    return nullptr;
                }
                break;
            }
            default:
                return nullptr;
        }
        current = current->mutable_children()[0].get();
    }
    return nullptr;
}

/// True if `input` is already the restriction this pass would add.
auto already_restricted(const Node& input, const std::vector<JoinKey>& keys) -> bool {
    if (input.kind() != NodeKind::Join) {
        return false;
    }
    const auto& join = node_cast<JoinNode>(input);
    if (join.kind() != JoinKind::Semi || join.keys().size() != keys.size()) {
        return false;
    }
    return std::ranges::all_of(keys, [&](const JoinKey& key) {
        return std::ranges::any_of(join.keys(), [&](const JoinKey& present) {
            return present.left == key.right && present.right == key.left;
        });
    });
}

/// How many distinct values of `column` the probe side can contribute.
///
/// `distinct_estimate` follows row-wise operators but stops at a join, and the
/// probe side of a correlated subquery is usually a join. Descending is sound
/// as an upper bound: an equijoin's output holds no more distinct values of a
/// key than its SMALLER side, a filter only removes values, and no operator
/// here invents one. Being wrong costs a rewrite that was worth doing, or one
/// that was marginal -- never a wrong answer, since this only decides whether
/// to optimize.
auto probed_distinct(const Node& node, const std::string& column, const SourceStats& stats)
    -> std::optional<std::size_t> {
    if (auto direct = distinct_estimate(node, column, stats); direct.has_value()) {
        return direct;
    }
    std::optional<std::size_t> best;
    for (const auto& child : node.children()) {
        if (child == nullptr) {
            continue;
        }
        if (auto from_child = probed_distinct(*child, column, stats); from_child.has_value()) {
            best = best.has_value() ? std::min(*best, *from_child) : *from_child;
        }
    }
    return best;
}

/// Whether restricting pays.
///
/// The saving is groups the aggregate never builds, so the question is a count
/// of GROUPS, not of rows: the probe side must key into fewer distinct values
/// than the aggregate would otherwise group. Costing this in rows says the
/// opposite on the query it was written for -- q17 reads the same lineitem
/// table on both sides, so by rows neither side is smaller, while by distinct
/// keys the probe wants 25k of 400k groups.
///
/// An estimate neither side can supply is not a licence to guess: the rewrite
/// is declined, since the alternative is paying for a second evaluation of a
/// probe side that may dwarf what it saves.
auto restriction_pays(const Node& probe_side, const Node& aggregate_input,
                      const std::vector<JoinKey>& keys, const SourceStats& stats) -> bool {
    return std::ranges::any_of(keys, [&](const JoinKey& key) {
        const auto probed = probed_distinct(probe_side, key.left, stats);
        const auto groups = distinct_estimate(aggregate_input, key.right, stats);
        return probed.has_value() && groups.has_value() && *probed < *groups;
    });
}

void rewrite(Node& node, const SourceStats& stats, std::uint64_t& next) {
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            rewrite(*child, stats, next);
        }
    }
    if (node.kind() != NodeKind::Join) {
        return;
    }
    auto& join = node_cast<JoinNode>(node);
    // Left and Inner only. A Right or Outer join emits rows the probe side
    // never matched, and their aggregate values would then depend on groups
    // the restriction had removed.
    if (join.kind() != JoinKind::Left && join.kind() != JoinKind::Inner) {
        return;
    }
    // The restriction is a `nulls never` semi join, so it would drop a
    // null-keyed group the probe side's null key is entitled to match.
    if (join.null_match() != NullMatch::Never) {
        return;
    }
    if (join.children().size() != 2 || join.children()[1] == nullptr) {
        return;
    }
    std::vector<std::string> key_names;
    key_names.reserve(join.keys().size());
    for (const auto& key : join.keys()) {
        key_names.push_back(key.right);
    }
    AggregateNode* found = aggregate_below(*join.mutable_children()[1], key_names);
    if (found == nullptr) {
        return;
    }
    AggregateNode& aggregate = *found;
    if (aggregate.children().size() != 1 || aggregate.children()[0] == nullptr) {
        return;
    }
    const std::vector<JoinKey> keys = restrictable_keys(join, aggregate);
    if (keys.empty() || already_restricted(*aggregate.children()[0], keys)) {
        return;
    }
    const Node& probe_side = *join.children()[0];
    if (!restriction_pays(probe_side, *aggregate.children()[0], keys, stats)) {
        return;
    }
    auto probe_clone = clone_replayable_subplan(probe_side, next);
    if (probe_clone == nullptr) {
        return;
    }

    std::vector<ColumnRef> key_columns;
    std::vector<JoinKey> semi_keys;
    key_columns.reserve(keys.size());
    semi_keys.reserve(keys.size());
    for (const auto& key : keys) {
        key_columns.push_back(ColumnRef{.name = key.left});
        // The aggregate's input on the left, the probe side's own name on the
        // right -- the mirror of the join above.
        semi_keys.emplace_back(key.right, key.left);
    }
    auto key_projection = std::make_unique<ProjectNode>(NodeId{next++}, std::move(key_columns));
    key_projection->add_child(std::move(probe_clone));

    auto semi = std::make_unique<JoinNode>(NodeId{next++}, JoinKind::Semi, std::move(semi_keys));
    semi->add_child(std::move(aggregate.mutable_children()[0]));
    semi->add_child(std::move(key_projection));
    aggregate.mutable_children()[0] = std::move(semi);
}

}  // namespace

auto restrict_aggregates_to_probed_keys(NodePtr root, const SourceStats& stats) -> NodePtr {
    if (root == nullptr) {
        return root;
    }
    std::uint64_t next = max_node_id(*root) + 1;
    rewrite(*root, stats, next);
    return root;
}

}  // namespace ibex::ir
