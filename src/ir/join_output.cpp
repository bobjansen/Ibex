// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_output.hpp>

#include <algorithm>
#include <unordered_set>

namespace ibex::ir {
namespace {

auto quote(std::string_view name) -> std::string {
    return "\"" + std::string(name) + "\"";
}

/// The diagnostic for a collision with no suffix policy to resolve it.
auto unresolved_collision(std::string_view name) -> std::string {
    return "join output: column " + quote(name) +
           " exists in both inputs; add suffix { \"_left\", \"_right\" } to rename them, or "
           "project one away before the join";
}

/// The diagnostic for a suffix policy that did not separate the two sides.
auto unresolved_after_suffix(std::string_view name) -> std::string {
    return "join output: column " + quote(name) +
           " is still duplicated after applying the suffix clause; choose suffixes that do not "
           "collide with an existing column";
}

}  // namespace

auto plan_join_output(JoinKind kind, const std::vector<JoinKey>& keys,
                      std::span<const std::string_view> left_names,
                      std::span<const std::string_view> right_names, const JoinSuffixPolicy& suffix)
    -> std::expected<std::vector<JoinOutputColumn>, std::string> {
    std::vector<JoinOutputColumn> plan;
    plan.reserve(left_names.size() + right_names.size());

    const auto is_left_key = [&](std::string_view name) {
        return std::ranges::any_of(keys, [&](const JoinKey& key) { return key.left == name; });
    };
    for (std::size_t i = 0; i < left_names.size(); ++i) {
        plan.push_back(JoinOutputColumn{.side = JoinOutputSide::Left,
                                        .source_index = i,
                                        .name = std::string(left_names[i]),
                                        .is_key = is_left_key(left_names[i]),
                                        // Filled in below, once the right
                                        // side's folds are known.
                                        .folded_peer_index = std::nullopt});
    }

    // Semi and anti emit no right columns, so nothing can collide -- and
    // nothing folds either: they emit whole left rows, never a value drawn
    // from the right.
    if (kind == JoinKind::Semi || kind == JoinKind::Anti) {
        return plan;
    }

    const std::unordered_set<std::string_view> left_set(left_names.begin(), left_names.end());

    // A same-name equijoin key folds into the single left column, so it is not
    // a collision; a mapped key keeps both native columns and may well be one.
    const auto folds_into_left = [&](std::string_view name) {
        return std::ranges::any_of(
            keys, [&](const JoinKey& key) { return key.left == key.right && key.right == name; });
    };

    // Record the fold on the surviving left column while the planner still has
    // both sides in hand. This is the one place that knows a right column was
    // dropped in favour of a left one.
    for (auto& column : plan) {
        if (!folds_into_left(left_names[column.source_index])) {
            continue;
        }
        const auto peer = std::ranges::find(right_names, left_names[column.source_index]);
        if (peer != right_names.end()) {
            column.folded_peer_index =
                static_cast<std::size_t>(std::distance(right_names.begin(), peer));
        }
    }

    std::vector<std::size_t> emitted_right;
    emitted_right.reserve(right_names.size());
    for (std::size_t i = 0; i < right_names.size(); ++i) {
        if (!folds_into_left(right_names[i])) {
            emitted_right.push_back(i);
        }
    }

    // Names held by both inputs, in right-input order so the first reported
    // collision does not depend on hash iteration.
    std::unordered_set<std::string_view> collisions;
    for (const std::size_t i : emitted_right) {
        if (left_set.contains(right_names[i])) {
            if (!suffix.present) {
                return std::unexpected(unresolved_collision(right_names[i]));
            }
            collisions.insert(right_names[i]);
        }
    }

    // Suffixes apply to the collisions only, on both sides, never to every
    // column. An empty suffix leaves its side under the original name, which
    // is how a caller keeps one side pristine.
    if (!collisions.empty()) {
        for (auto& column : plan) {
            if (collisions.contains(column.name)) {
                column.name += suffix.left;
            }
        }
    }

    const auto is_right_key = [&](std::string_view name) {
        return std::ranges::any_of(keys, [&](const JoinKey& key) { return key.right == name; });
    };
    for (const std::size_t i : emitted_right) {
        std::string name(right_names[i]);
        if (collisions.contains(right_names[i])) {
            name += suffix.right;
        }
        plan.push_back(
            JoinOutputColumn{.side = JoinOutputSide::Right,
                             .source_index = i,
                             .name = std::move(name),
                             .is_key = is_right_key(right_names[i]),
                             // A right column reaching the output natively did not fold -- that
                             // is what `emitted_right` selected for -- so it has no peer.
                             .folded_peer_index = std::nullopt});
    }

    // Catches an empty or shared suffix pair, a suffixed name that lands on an
    // untouched column, and duplicate names arriving inside one input.
    std::unordered_set<std::string_view> seen;
    seen.reserve(plan.size());
    for (const auto& column : plan) {
        if (!seen.insert(column.name).second) {
            return std::unexpected(suffix.present ? unresolved_after_suffix(column.name)
                                                  : unresolved_collision(column.name));
        }
    }

    return plan;
}

}  // namespace ibex::ir
