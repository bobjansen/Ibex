// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/join_semi_reduction.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

auto fresh_id(std::uint64_t& next) -> NodeId {
    return NodeId{next++};
}

void collect_max_id(Node& n, std::uint64_t& m) {
    m = std::max(m, n.id().value);
    for (const auto& child : n.mutable_children()) {
        if (child) {
            collect_max_id(*child, m);
        }
    }
    if (n.kind() == NodeKind::Program) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        auto& prog = static_cast<ProgramNode&>(n);
        for (const auto& pre : prog.mutable_preamble()) {
            if (pre) {
                collect_max_id(*pre, m);
            }
        }
        if (prog.mutable_main_node()) {
            collect_max_id(*prog.mutable_main_node(), m);
        }
    }
}

/// Names a schema lists. Empty when the schema cannot be enumerated, which
/// every caller here treats as "prove nothing".
auto field_names(const SchemaInfo& schema) -> std::vector<std::string> {
    std::vector<std::string> names;
    if (!schema.is_known()) {
        return names;
    }
    names.reserve(schema.fields().size());
    for (const auto& field : schema.fields()) {
        names.push_back(field.name);
    }
    return names;
}

auto contains(const std::vector<std::string>& names, const std::string& name) -> bool {
    return std::find(names.begin(), names.end(), name) != names.end();
}

/// True when no column that exists on `dropped` but not on `kept` is read above
/// the join. A shared name resolves to the kept side and is equal on surviving
/// rows, so it survives the reduction untouched.
auto dropped_side_is_unread(const ColumnDemand& demand, const std::vector<std::string>& dropped,
                            const std::vector<std::string>& kept) -> bool {
    if (demand.all) {
        return false;
    }
    for (const auto& name : demand.names) {
        if (contains(dropped, name) && !contains(kept, name)) {
            return false;
        }
    }
    return true;
}

/// The join shapes whose pair count and column set this pass can reason about.
auto reducible_shape(const JoinNode& join) -> bool {
    return join.kind() == JoinKind::Inner && !join.predicate().has_value() &&
           !join.keys().empty() && join.take() == MatchSelection::All &&
           join.expect().left == JoinMultiplicity::Many &&
           join.expect().right == JoinMultiplicity::Many && join.null_match() == NullMatch::Never &&
           !join.suffix().present && join.children().size() == 2;
}

auto swapped_keys(const std::vector<JoinKey>& keys) -> std::vector<JoinKey> {
    std::vector<JoinKey> out;
    out.reserve(keys.size());
    for (const auto& key : keys) {
        out.emplace_back(key.right, key.left);
    }
    return out;
}

auto make_semi(NodePtr kept, NodePtr dropped, std::vector<JoinKey> keys, std::uint64_t& next)
    -> NodePtr {
    auto semi = std::make_unique<JoinNode>(fresh_id(next), JoinKind::Semi, std::move(keys));
    semi->add_child(std::move(kept));
    semi->add_child(std::move(dropped));
    return semi;
}

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)
// Node kind is checked immediately before every downcast below.

auto rewrite_join(NodePtr node, const SourceSchemas& sources,
                  const std::map<const Node*, ColumnDemand>& demand, std::uint64_t& next)
    -> NodePtr {
    const auto& join = static_cast<const JoinNode&>(*node);
    if (!reducible_shape(join)) {
        return node;
    }
    const auto found = demand.find(node.get());
    if (found == demand.end() || found->second.all) {
        return node;
    }

    const SchemaInfo left = infer_schema(*join.children()[0], sources);
    const SchemaInfo right = infer_schema(*join.children()[1], sources);
    // A missing-column test is only sound on a closed, Known schema: an open
    // one may carry columns this pass cannot see, and would silently drop them.
    if (!left.is_known() || left.is_open() || !right.is_known() || right.is_open()) {
        return node;
    }
    const std::vector<std::string> left_names = field_names(left);
    const std::vector<std::string> right_names = field_names(right);

    // Prefer dropping the right: it keeps the retained side, the key
    // orientation and the output column order exactly as they were.
    if (right.is_unique_within(right_join_key_names(join.keys())) &&
        dropped_side_is_unread(found->second, right_names, left_names)) {
        auto children = std::move(node->mutable_children());
        return make_semi(std::move(children[0]), std::move(children[1]), join.keys(), next);
    }
    if (left.is_unique_within(left_join_key_names(join.keys())) &&
        dropped_side_is_unread(found->second, left_names, right_names)) {
        auto children = std::move(node->mutable_children());
        return make_semi(std::move(children[1]), std::move(children[0]), swapped_keys(join.keys()),
                         next);
    }
    return node;
}

auto walk(NodePtr node, const SourceSchemas& sources,
          const std::map<const Node*, ColumnDemand>& demand, std::uint64_t& next) -> NodePtr {
    if (!node) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), sources, demand, next);
    }
    if (node->kind() == NodeKind::Program) {
        auto& prog = static_cast<ProgramNode&>(*node);
        if (prog.mutable_main_node()) {
            prog.mutable_main_node() =
                walk(std::move(prog.mutable_main_node()), sources, demand, next);
        }
        for (auto& pre : prog.mutable_preamble()) {
            pre = walk(std::move(pre), sources, demand, next);
        }
    }
    if (node->kind() == NodeKind::Join) {
        return rewrite_join(std::move(node), sources, demand, next);
    }
    return node;
}

// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto reduce_inner_joins_to_semi(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    if (!root) {
        return root;
    }
    // Computed once, against the untouched tree: `join_output_demand` keys on
    // node address, and this pass replaces the very nodes it asks about. Every
    // lookup below happens before that node is replaced, and a reduction only
    // removes columns its own entry proved unread -- so no ancestor's demand
    // can be invalidated by a descendant's rewrite.
    const std::map<const Node*, ColumnDemand> demand = join_output_demand(*root);
    std::uint64_t next = 0;
    collect_max_id(*root, next);
    ++next;
    return walk(std::move(root), sources, demand, next);
}

}  // namespace ibex::ir
