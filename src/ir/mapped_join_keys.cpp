// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/mapped_join_keys.hpp>
#include <ibex/ir/required_columns.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "ibex/ir/node.hpp"
#include "ibex/ir/schema.hpp"

namespace ibex::ir {

namespace {

// Fresh NodeIds for the Rename nodes this pass synthesizes. Seeded at entry to
// (max id in tree + 1) so new ids never collide; same convention as
// canonicalize.cpp and join_pushdown.cpp.
auto next_id_counter() -> std::uint64_t& {
    thread_local std::uint64_t next_id = 0;
    return next_id;
}

auto fresh_id() -> NodeId {
    return NodeId{next_id_counter()++};
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
// Node kind is checked immediately before every downcast below.

void collect_max_id(const Node& node, std::uint64_t& value) {
    value = std::max(value, node.id().value);
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_max_id(*child, value);
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& entry : program.preamble()) {
            if (entry != nullptr) {
                collect_max_id(*entry, value);
            }
        }
        collect_max_id(program.main_node(), value);
    }
}

/// The kinds whose right key column can be folded into the left's without
/// changing a value anything can observe. See the header for the reasoning;
/// the short version is that Right and Outer fill the surviving key column
/// from the RIGHT row when a right row goes unmatched, so folding would change
/// it.
auto foldable_kind(JoinKind kind) -> bool {
    return kind == JoinKind::Inner || kind == JoinKind::Left || kind == JoinKind::Semi ||
           kind == JoinKind::Anti;
}

void rewrite_join(Node& node, const SourceSchemas& sources,
                  const std::map<const Node*, ColumnDemand>& demand) {
    auto& join = static_cast<JoinNode&>(node);
    if (!foldable_kind(join.kind()) || join.keys().empty() ||
        join_keys_are_same_named(join.keys())) {
        return;
    }
    if (join.children().size() != 2 || join.children()[0] == nullptr ||
        join.children()[1] == nullptr) {
        return;
    }

    // An unreached or unmodelled join is absent from the map, and absent means
    // "nothing proven" — never "nothing read".
    const auto found = demand.find(&node);
    if (found == demand.end() || found->second.all) {
        return;
    }
    const ColumnDemand& above = found->second;

    // Both absence tests below ("this name is not on that side") are only sound
    // against a schema that is Known and closed.
    const SchemaInfo left = infer_schema(*join.children()[0], sources);
    const SchemaInfo right = infer_schema(*join.children()[1], sources);
    if (!left.is_known() || left.is_open() || !right.is_known() || right.is_open()) {
        return;
    }

    std::vector<JoinKey> keys = join.keys();
    std::vector<RenameSpec> renames;
    for (auto& key : keys) {
        if (key.left == key.right) {
            continue;  // already same-named; nothing to fold
        }
        // Read above the join, so the fold would be observable. (Vacuous for
        // Semi/Anti, whose output carries no right columns at all.)
        if (above.names.contains(key.right)) {
            continue;
        }
        // The rename would duplicate a name inside the right child.
        if (right.find(key.left) != nullptr) {
            continue;
        }
        // The right key's name already exists on the left, so the join's suffix
        // policy is renaming one of them; folding would change which.
        if (left.find(key.right) != nullptr) {
            continue;
        }
        // A repeated key name on either side (`on { k = x, k = y }`) would ask
        // one rename list to produce a column twice or consume one twice.
        // Degenerate, but writable, and the join means something else than the
        // fold would.
        if (std::ranges::any_of(renames, [&](const RenameSpec& spec) {
                return spec.new_name == key.left || spec.old_name == key.right;
            })) {
            continue;
        }
        renames.push_back(RenameSpec{.new_name = key.left, .old_name = key.right});
        key.right = key.left;
    }
    if (renames.empty()) {
        return;
    }

    join.set_keys(std::move(keys));
    NodePtr right_child = std::move(join.mutable_children()[1]);
    auto renamed = std::make_unique<RenameNode>(fresh_id(), std::move(renames));
    renamed->add_child(std::move(right_child));
    join.mutable_children()[1] = std::move(renamed);
}

/// Children first. The demand map was computed on the intact tree, but nothing
/// this pass does moves a Join node — it only edits keys and wraps the right
/// child — so `&node` stays a valid key throughout.
void walk(Node& node, const SourceSchemas& sources,
          const std::map<const Node*, ColumnDemand>& demand) {
    for (auto& child : node.mutable_children()) {
        if (child != nullptr) {
            walk(*child, sources, demand);
        }
    }
    if (node.kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(node);
        for (auto& entry : program.mutable_preamble()) {
            if (entry != nullptr) {
                walk(*entry, sources, demand);
            }
        }
        if (program.mutable_main_node() != nullptr) {
            walk(*program.mutable_main_node(), sources, demand);
        }
    }
    if (node.kind() == NodeKind::Join) {
        rewrite_join(node, sources, demand);
    }
}

// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto normalize_mapped_join_keys(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    if (root == nullptr) {
        return root;
    }
    std::uint64_t max_id = 0;
    collect_max_id(*root, max_id);
    next_id_counter() = max_id + 1;

    const auto demand = join_output_demand(*root);
    walk(*root, sources, demand);
    return root;
}

}  // namespace ibex::ir
