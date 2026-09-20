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

namespace {

/// Copy one node's own state, without children. Returns null for a kind that
/// is not on the allow-list.
///
/// This IS the allow-list: the safety question ("may this be evaluated twice")
/// and the mechanical one ("can this be copied") are answered by one switch, so
/// they cannot drift apart. A kind added later falls to `default` and is
/// refused, which is the safe answer to both.
auto clone_node_shallow(const Node& node, std::uint64_t& next) -> NodePtr {
    const NodeId id{next++};
    switch (node.kind()) {
        case NodeKind::Scan: {
            const auto& scan = node_cast<ScanNode>(node);
            auto clone = std::make_unique<ScanNode>(id, scan.source_name());
            if (const auto& ascribed = scan.ascribed_schema(); ascribed.has_value()) {
                clone->set_ascribed_schema(ascribed->fields, ascribed->open);
            }
            return clone;
        }
        case NodeKind::Filter: {
            const auto& filter = node_cast<FilterNode>(node);
            if (!is_replayable_expr(filter.predicate())) {
                return nullptr;
            }
            return std::make_unique<FilterNode>(id, filter.predicate());
        }
        case NodeKind::Project:
            return std::make_unique<ProjectNode>(id, node_cast<ProjectNode>(node).columns());
        case NodeKind::Rename:
            return std::make_unique<RenameNode>(id, node_cast<RenameNode>(node).renames());
        case NodeKind::Distinct:
            return std::make_unique<DistinctNode>(id);
        case NodeKind::Order:
            return std::make_unique<OrderNode>(id, node_cast<OrderNode>(node).keys());
        case NodeKind::Head: {
            const auto& head = node_cast<HeadNode>(node);
            if (!is_replayable_expr(head.count_expr())) {
                return nullptr;
            }
            return std::make_unique<HeadNode>(id, head.count_expr(), head.group_by());
        }
        case NodeKind::Tail: {
            const auto& tail = node_cast<TailNode>(node);
            if (!is_replayable_expr(tail.count_expr())) {
                return nullptr;
            }
            return std::make_unique<TailNode>(id, tail.count_expr(), tail.group_by());
        }
        case NodeKind::Ascribe: {
            const auto& ascribe = node_cast<AscribeNode>(node);
            return std::make_unique<AscribeNode>(id, ascribe.schema(), ascribe.open());
        }
        case NodeKind::Join: {
            const auto& join = node_cast<JoinNode>(node);
            if (join.predicate().has_value() && !is_replayable_expr(*join.predicate())) {
                return nullptr;
            }
            auto clone = std::make_unique<JoinNode>(id, join.kind(), join.keys(), join.predicate(),
                                                    join.suffix(), join.null_match(), join.expect(),
                                                    join.take());
            clone->set_pending_order(join.pending_order());
            return clone;
        }
        case NodeKind::Aggregate: {
            // Group keys and aggregate inputs are column references; the
            // function is an AggFunc, not a callee that could be a generator.
            const auto& aggregate = node_cast<AggregateNode>(node);
            return std::make_unique<AggregateNode>(id, aggregate.group_by(),
                                                   aggregate.aggregations());
        }
        case NodeKind::Update: {
            const auto& update = node_cast<UpdateNode>(node);
            // A tuple field owns a sub-plan through a move-only handle, so the
            // node cannot be copied at all while it has any.
            if (!update.tuple_fields().empty()) {
                return nullptr;
            }
            if (!std::ranges::all_of(update.fields(), [](const FieldSpec& field) {
                    return is_replayable_expr(field.expr);
                })) {
                return nullptr;
            }
            return std::make_unique<UpdateNode>(id, update.fields(), std::vector<TupleFieldSpec>{},
                                                update.group_by());
        }
        default:
            // Everything else -- an extern call, a stream, a model fit, a map
            // over plugin code -- is either effectful or unclassified. A node
            // kind added later lands here too, which is the point.
            return nullptr;
    }
}

}  // namespace

auto clone_replayable_subplan(const Node& node, std::uint64_t& next) -> NodePtr {
    auto clone = clone_node_shallow(node, next);
    if (clone == nullptr) {
        return nullptr;
    }
    for (const auto& child : node.children()) {
        if (child == nullptr) {
            continue;
        }
        auto child_clone = clone_replayable_subplan(*child, next);
        if (child_clone == nullptr) {
            return nullptr;
        }
        clone->add_child(std::move(child_clone));
    }
    return clone;
}

}  // namespace ibex::ir
