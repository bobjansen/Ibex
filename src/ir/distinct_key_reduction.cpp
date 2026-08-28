// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/distinct_key_reduction.hpp>
#include <ibex/ir/node.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast)

/// Where a value was produced: the node that made it, and the name it had
/// there. Two columns hold the same value exactly when this is equal for both.
struct ValueId {
    std::uint64_t node = 0;
    std::string name;

    auto operator==(const ValueId&) const -> bool = default;
};

void collect_max_id(const Node& node, std::uint64_t& max) {
    max = std::max(max, node.id().value);
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_max_id(*child, max);
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        collect_max_id(program.main_node(), max);
        for (const auto& statement : program.preamble()) {
            if (statement != nullptr) {
                collect_max_id(*statement, max);
            }
        }
    }
}

/// Follow `name` down from `node` to the value it carries.
///
/// Only descends through nodes that pass a column's value through untouched.
/// A projection selects and does not rename; a filter drops whole rows, which
/// cannot make two equal columns unequal; a rename is a name change; an update
/// field whose expression is a bare column reference is a copy, and a copy IS
/// the value it copied. Everything else ends the walk and becomes the identity,
/// so an unmodelled node makes columns look distinct rather than equal.
auto resolve_value(const Node& node, std::string name) -> ValueId {
    const Node* current = &node;
    while (true) {
        const Node* child = current->children().empty() ? nullptr : current->children()[0].get();
        if (child == nullptr) {
            break;
        }
        bool descend = true;
        switch (current->kind()) {
            case NodeKind::Project:
            case NodeKind::Filter:
            case NodeKind::Distinct:
                break;  // neither renames nor computes

            case NodeKind::Rename: {
                const ColumnNameMap names(static_cast<const RenameNode&>(*current).renames());
                name = names.input_name(name);
                break;
            }

            case NodeKind::Update: {
                const auto& update = static_cast<const UpdateNode&>(*current);
                // A guarded update (`where <p> update { ... }`) writes the field
                // on some rows and leaves the old value on the others, so the
                // alias is not a copy of anything. A grouped one is left alone
                // for the same reason: what it writes depends on the group.
                if (update.guard() != nullptr || !update.group_by().empty()) {
                    descend = false;
                    break;
                }
                for (const auto& tuple : update.tuple_fields()) {
                    for (const auto& alias : tuple.aliases) {
                        if (alias == name) {
                            descend = false;
                        }
                    }
                }
                for (const auto& field : update.fields()) {
                    if (field.alias != name) {
                        continue;
                    }
                    const auto* ref = as_column_ref(field.expr);
                    if (ref == nullptr) {
                        descend = false;  // computed: a new value
                    } else {
                        name = ref->name;
                    }
                    break;
                }
                break;
            }

            default:
                descend = false;
                break;
        }
        if (!descend) {
            break;
        }
        current = child;
    }
    return ValueId{.node = current->id().value, .name = std::move(name)};
}

/// Rewrite one `Distinct(Project[...])` whose projection names a value twice.
auto rewrite_distinct(NodePtr node, std::uint64_t& next) -> NodePtr {
    auto& distinct = *node;
    if (distinct.children().size() != 1 || distinct.children()[0] == nullptr ||
        distinct.children()[0]->kind() != NodeKind::Project) {
        return node;
    }
    auto& project = static_cast<ProjectNode&>(*distinct.mutable_children()[0]);
    const std::vector<ColumnRef> columns = project.columns();
    if (columns.size() < 2 || project.children().empty() || project.children()[0] == nullptr) {
        return node;
    }

    std::vector<ValueId> values;
    values.reserve(columns.size());
    for (const auto& column : columns) {
        values.push_back(resolve_value(*project.children()[0], column.name));
    }

    // Keep the first column naming each value; every later one is a copy of it.
    std::vector<ColumnRef> kept;
    std::vector<FieldSpec> restored;
    kept.reserve(columns.size());
    for (std::size_t i = 0; i < columns.size(); ++i) {
        std::size_t first = i;
        for (std::size_t j = 0; j < i; ++j) {
            if (values[j] == values[i]) {
                first = j;
                break;
            }
        }
        if (first == i) {
            kept.push_back(columns[i]);
            continue;
        }
        restored.push_back(FieldSpec{
            .alias = columns[i].name,
            .expr = Expr{.node = ColumnRef{.name = columns[first].name}},
        });
    }
    if (restored.empty()) {
        return node;
    }

    // Reuse the projection's id for the narrowed one -- it is the same
    // projection, minus the columns that were never doing anything -- and mint
    // ids for the two nodes that put the copies back.
    auto narrowed = std::make_unique<ProjectNode>(project.id(), std::move(kept));
    narrowed->add_child(std::move(project.mutable_children()[0]));
    distinct.mutable_children()[0] = std::move(narrowed);

    auto update = std::make_unique<UpdateNode>(NodeId{next++}, std::move(restored));
    update->add_child(std::move(node));

    auto restore_order = std::make_unique<ProjectNode>(NodeId{next++}, columns);
    restore_order->add_child(std::move(update));
    return restore_order;
}

auto walk(NodePtr node, std::uint64_t& next) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), next);
    }
    // A program's statements hang off the node separately from its children.
    if (node->kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(*node);
        if (program.mutable_main_node() != nullptr) {
            program.mutable_main_node() = walk(std::move(program.mutable_main_node()), next);
        }
        for (auto& statement : program.mutable_preamble()) {
            statement = walk(std::move(statement), next);
        }
    }
    if (node->kind() == NodeKind::Distinct) {
        return rewrite_distinct(std::move(node), next);
    }
    return node;
}

// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace

auto columns_have_same_value(const Node& root, std::string_view left, std::string_view right)
    -> bool {
    return resolve_value(root, std::string(left)) == resolve_value(root, std::string(right));
}

auto reduce_duplicate_distinct_columns(NodePtr root) -> NodePtr {
    if (root == nullptr) {
        return root;
    }
    std::uint64_t next = 0;
    collect_max_id(*root, next);
    ++next;
    return walk(std::move(root), next);
}

}  // namespace ibex::ir
