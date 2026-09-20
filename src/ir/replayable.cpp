// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/replayable.hpp>

#include <algorithm>
#include <variant>

namespace ibex::ir {
namespace {

auto replayable_call(const CallExpr& call) -> bool {
    // An unknown callee is an extern: unclassified on purpose, so nothing may
    // be assumed about repeating it. A generator draws from a shared RNG
    // stream, so a second evaluation gives different values by design.
    const auto kind = fn_kind(call.callee);
    if (!kind.has_value() || *kind == FnKind::Generator) {
        return false;
    }
    return std::ranges::all_of(call.args,
                               [](const ExprPtr& arg) { return is_replayable_expr(*arg); }) &&
           std::ranges::all_of(call.named_args, [](const NamedArg& named) {
               return is_replayable_expr(*named.value);
           });
}

}  // namespace

auto is_replayable_expr(const Expr& expr) -> bool {
    return std::visit(
        [](const auto& node) -> bool {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ColumnRef> || std::is_same_v<T, Literal>) {
                return true;
            } else if constexpr (std::is_same_v<T, CallExpr>) {
                return replayable_call(node);
            } else if constexpr (std::is_same_v<T, BinaryExpr> || std::is_same_v<T, CompareExpr>) {
                return is_replayable_expr(*node.left) && is_replayable_expr(*node.right);
            } else if constexpr (std::is_same_v<T, LogicalExpr>) {
                return is_replayable_expr(*node.left) &&
                       (node.right == nullptr || is_replayable_expr(*node.right));
            } else if constexpr (std::is_same_v<T, IsNullExpr>) {
                return is_replayable_expr(*node.operand);
            } else {
                // RankExpr and anything added later: unproven, so unsafe.
                return false;
            }
        },
        expr.node);
}

auto is_replayable_subplan(const Node& node) -> bool {
    switch (node.kind()) {
        // No expression of their own, and nothing but their input decides
        // what they produce.
        case NodeKind::Scan:
        case NodeKind::Project:
        case NodeKind::Rename:
        case NodeKind::Distinct:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::Ascribe:
            break;
        case NodeKind::Filter:
            if (!is_replayable_expr(node_cast<FilterNode>(node).predicate())) {
                return false;
            }
            break;
        case NodeKind::Join: {
            const auto& join = node_cast<JoinNode>(node);
            if (join.predicate().has_value() && !is_replayable_expr(*join.predicate())) {
                return false;
            }
            break;
        }
        case NodeKind::Aggregate:
            // Group keys and aggregate inputs are column references; the
            // function is an AggFunc, not a callee that could be a generator.
            break;
        case NodeKind::Update: {
            const auto& update = node_cast<UpdateNode>(node);
            if (!std::ranges::all_of(update.fields(), [](const FieldSpec& field) {
                    return is_replayable_expr(field.expr);
                })) {
                return false;
            }
            break;
        }
        case NodeKind::Construct: {
            // A literal column's values are literals, but an expression column
            // holds a whole sub-plan that `children()` does not report.
            const auto& construct = node_cast<ConstructNode>(node);
            if (construct.row_count().has_value() && !is_replayable_expr(*construct.row_count())) {
                return false;
            }
            if (!std::ranges::all_of(construct.columns(), [](const ConstructColumn& column) {
                    return column.expr_node == nullptr || is_replayable_subplan(*column.expr_node);
                })) {
                return false;
            }
            break;
        }
        default:
            // Everything else -- an extern call, a stream, a model fit, a map
            // over plugin code -- is either effectful or unclassified. A node
            // kind added later lands here too, which is the point.
            return false;
    }
    return std::ranges::all_of(node.children(), [](const NodePtr& child) {
        return child == nullptr || is_replayable_subplan(*child);
    });
}

}  // namespace ibex::ir
