// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/ir/nullability.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/ir/sort_key_reduction.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

/// Whether `name`, as the sort node above `node` sees it, is an `is_null(e)`
/// flag over an `e` proved never null -- `false` on every row.
// NOLINTNEXTLINE(misc-no-recursion)
auto is_constant_null_flag(const Node& node, const std::string& name, const SourceSchemas& sources)
    -> bool {
    switch (node.kind()) {
        case NodeKind::Project:
        case NodeKind::Filter:
            // A projection keeps the name's column; a filter keeps its values.
            return !node.children().empty() && node.children().front() != nullptr &&
                   is_constant_null_flag(*node.children().front(), name, sources);
        case NodeKind::Update: {
            const auto& update = node_cast<UpdateNode>(node);
            if (node.children().empty() || node.children().front() == nullptr) {
                return false;
            }
            const auto* field = [&]() -> const FieldSpec* {
                for (const auto& f : update.fields()) {
                    if (f.alias == name) {
                        return &f;
                    }
                }
                return nullptr;
            }();
            if (field == nullptr) {
                // A tuple assignment could define it in a way this does not
                // read; anything else passes the column through unchanged.
                for (const auto& tuple : update.tuple_fields()) {
                    for (const auto& alias : tuple.aliases) {
                        if (alias == name) {
                            return false;
                        }
                    }
                }
                return is_constant_null_flag(*node.children().front(), name, sources);
            }
            // A grouped or guarded update evaluates the field on a subset or
            // per group; neither changes a row-local `is_null`, but stay with
            // the plain shape the adapter writes.
            if (!update.group_by().empty() || update.guard() != nullptr) {
                return false;
            }
            const auto* flag = std::get_if<IsNullExpr>(&field->expr.node);
            if (flag == nullptr || flag->negated || flag->operand == nullptr) {
                return false;
            }
            const SchemaInfo input = infer_schema(*node.children().front(), sources);
            return input.is_known() &&
                   expr_nullability(*flag->operand, input) == Nullability::Never;
        }
        default:
            return false;
    }
}

/// The keys of a sort over `child` that are not proved-constant null flags,
/// or nullopt when none can be dropped (or all would be).
auto surviving_keys(const std::vector<OrderKey>& keys, const Node& child,
                    const SourceSchemas& sources) -> std::optional<std::vector<OrderKey>> {
    std::vector<OrderKey> kept;
    kept.reserve(keys.size());
    for (const auto& key : keys) {
        if (!is_constant_null_flag(child, key.name, sources)) {
            kept.push_back(key);
        }
    }
    if (kept.empty() || kept.size() == keys.size()) {
        return std::nullopt;
    }
    return kept;
}

// NOLINTNEXTLINE(misc-no-recursion)
auto walk(NodePtr node, const SourceSchemas& sources) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), sources);
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = node_cast<ProgramNode>(*node);
        if (program.mutable_main_node()) {
            program.mutable_main_node() = walk(std::move(program.mutable_main_node()), sources);
        }
        for (auto& pre : program.mutable_preamble()) {
            pre = walk(std::move(pre), sources);
        }
        return node;
    }
    if ((node->kind() != NodeKind::Order && node->kind() != NodeKind::TopK) ||
        node->children().size() != 1 || node->children().front() == nullptr) {
        return node;
    }
    const Node& child = *node->children().front();
    if (node->kind() == NodeKind::Order) {
        auto kept = surviving_keys(node_cast<OrderNode>(*node).keys(), child, sources);
        if (!kept.has_value()) {
            return node;
        }
        auto order = std::make_unique<OrderNode>(node->id(), std::move(*kept));
        order->add_child(std::move(node->mutable_children().front()));
        return order;
    }
    const auto& topk = node_cast<TopKNode>(*node);
    auto kept = surviving_keys(topk.keys(), child, sources);
    if (!kept.has_value()) {
        return node;
    }
    auto rebuilt = std::make_unique<TopKNode>(node->id(), std::move(*kept), topk.count(),
                                              topk.group_by(), topk.keep_mode());
    rebuilt->add_child(std::move(node->mutable_children().front()));
    return rebuilt;
}

}  // namespace

auto drop_constant_null_flag_sort_keys(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    return walk(std::move(root), sources);
}

}  // namespace ibex::ir
