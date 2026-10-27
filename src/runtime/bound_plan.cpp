// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "bound_filter.hpp"

namespace ibex::runtime {

namespace {

// Only a `^name` reference is certain to mean the binding without a schema; a
// bare name may still be a column of the operand, so it is left for the
// operator to resolve.
constexpr auto kLexicalOnly = [](const std::string&) { return true; };

auto has_binding(const ir::Expr& expr, const ScalarRegistry& scalars) -> bool {
    bool found = false;
    bound_filter_detail::visit_bindings(expr, scalars, kLexicalOnly,
                                        [&](const ir::Expr&, const ScalarValue&) { found = true; });
    return found;
}

void bind_expr(ir::Expr& expr, const ScalarRegistry& scalars) {
    bound_filter_detail::visit_bindings(
        expr, scalars, kLexicalOnly, [](ir::Expr& target, const ScalarValue& value) {
            std::visit([&](const auto& scalar) { target.node = ir::Literal{.value = scalar}; },
                       value);
        });
}

// Replace `node` by `rebuilt`, which takes over its children.
void replace_node(ir::NodePtr& node, ir::NodePtr rebuilt) {
    rebuilt->mutable_children() = std::move(node->mutable_children());
    node = std::move(rebuilt);
}

void bind_node(ir::NodePtr& node, const ScalarRegistry& scalars) {
    if (node == nullptr) {
        return;
    }
    for (auto& child : node->mutable_children()) {
        bind_node(child, scalars);
    }
    switch (node->kind()) {
        case ir::NodeKind::Filter: {
            auto& filter = ir::node_cast<ir::FilterNode>(*node);
            if (has_binding(filter.predicate(), scalars)) {
                auto predicate = filter.take_predicate();
                bind_expr(predicate, scalars);
                replace_node(node, std::make_unique<ir::FilterNode>(node->id(), std::move(predicate)));
            }
            break;
        }
        case ir::NodeKind::FilterHead:
        case ir::NodeKind::FilterTail: {
            const bool head = node->kind() == ir::NodeKind::FilterHead;
            const auto& predicate = head ? ir::node_cast<ir::FilterHeadNode>(*node).predicate()
                                         : ir::node_cast<ir::FilterTailNode>(*node).predicate();
            const auto count = head ? ir::node_cast<ir::FilterHeadNode>(*node).count()
                                    : ir::node_cast<ir::FilterTailNode>(*node).count();
            if (has_binding(predicate, scalars)) {
                auto bound = predicate;
                bind_expr(bound, scalars);
                if (head) {
                    replace_node(node, std::make_unique<ir::FilterHeadNode>(
                                           node->id(), std::move(bound), count));
                } else {
                    replace_node(node, std::make_unique<ir::FilterTailNode>(
                                           node->id(), std::move(bound), count));
                }
            }
            break;
        }
        case ir::NodeKind::Head:
        case ir::NodeKind::Tail: {
            const bool head = node->kind() == ir::NodeKind::Head;
            const auto& count = head ? ir::node_cast<ir::HeadNode>(*node).count_expr()
                                     : ir::node_cast<ir::TailNode>(*node).count_expr();
            if (has_binding(count, scalars)) {
                auto bound = count;
                bind_expr(bound, scalars);
                if (head) {
                    replace_node(node, std::make_unique<ir::HeadNode>(
                                           node->id(), std::move(bound),
                                           ir::node_cast<ir::HeadNode>(*node).group_by()));
                } else {
                    replace_node(node, std::make_unique<ir::TailNode>(
                                           node->id(), std::move(bound),
                                           ir::node_cast<ir::TailNode>(*node).group_by()));
                }
            }
            break;
        }
        case ir::NodeKind::Update: {
            auto& update = ir::node_cast<ir::UpdateNode>(*node);
            for (auto& tuple : update.mutable_tuple_fields()) {
                bind_node(tuple.source, scalars);
            }
            const bool fields = std::ranges::any_of(update.fields(), [&](const ir::FieldSpec& f) {
                return has_binding(f.expr, scalars);
            });
            const bool guard = update.guard() != nullptr && has_binding(*update.guard(), scalars);
            if (!fields && !guard) {
                break;
            }
            auto bound_fields = update.fields();
            for (auto& field : bound_fields) {
                bind_expr(field.expr, scalars);
            }
            auto rebuilt = std::make_unique<ir::UpdateNode>(
                node->id(), std::move(bound_fields), std::move(update.mutable_tuple_fields()),
                update.group_by());
            if (const auto* old_guard = update.guard()) {
                auto bound_guard = *old_guard;
                bind_expr(bound_guard, scalars);
                rebuilt->set_guard(std::move(bound_guard));
            }
            replace_node(node, std::move(rebuilt));
            break;
        }
        case ir::NodeKind::Join: {
            auto& join = ir::node_cast<ir::JoinNode>(*node);
            if (join.predicate().has_value() && has_binding(*join.predicate(), scalars)) {
                auto predicate = *join.predicate();
                bind_expr(predicate, scalars);
                join.set_predicate(std::move(predicate));
            }
            break;
        }
        case ir::NodeKind::Program: {
            auto& program = ir::node_cast<ir::ProgramNode>(*node);
            for (auto& statement : program.mutable_preamble()) {
                bind_node(statement, scalars);
            }
            bind_node(program.mutable_main_node(), scalars);
            break;
        }
        default:
            break;
    }
}

}  // namespace

auto bind_plan_constants(ir::NodePtr plan, const ScalarRegistry& scalars) -> ir::NodePtr {
    if (!scalars.empty()) {
        bind_node(plan, scalars);
    }
    return plan;
}

}  // namespace ibex::runtime
