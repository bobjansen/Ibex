// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <optional>
#include <type_traits>
#include <variant>

namespace ibex::runtime {

namespace bound_filter_detail {

// Visit only references that resolve to scalar scope for this input. In
// particular, an ordinary column reference wins over a same-named binding.
template <typename Expr, typename HasColumn, typename Visit>
void visit_bindings(Expr& expr, const ScalarRegistry& scalars, const HasColumn& has_column,
                    const Visit& visit) {
    if (const auto* ref = std::get_if<ir::ColumnRef>(&expr.node)) {
        if (ref->side == ir::JoinSide::Any && (ref->lexical || !has_column(ref->name))) {
            if (const auto it = scalars.find(ref->name); it != scalars.end()) {
                visit(expr, it->second);
            }
        }
        return;
    }
    std::visit(
        [&](auto& node) {
            using T = std::decay_t<decltype(node)>;
            const auto child = [&](auto& ptr) {
                if (ptr) {
                    visit_bindings(*ptr, scalars, has_column, visit);
                }
            };
            if constexpr (std::is_same_v<T, ir::BinaryExpr> || std::is_same_v<T, ir::CompareExpr> ||
                          std::is_same_v<T, ir::LogicalExpr>) {
                child(node.left);
                child(node.right);
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                child(node.operand);
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                for (auto& arg : node.args) {
                    child(arg);
                }
                for (auto& arg : node.named_args) {
                    child(arg.value);
                }
            }
        },
        expr.node);
}

}  // namespace bound_filter_detail

// Specialize a filter for this execution only. Never modify the reusable IR or
// retain values across executions. Literal construction preserves every scalar
// type, including typed nulls and NaNs, and leaves comparison policy untouched.
// No copy is needed for filters without a resolvable scalar reference.
template <typename HasColumn>
[[nodiscard]] auto bind_filter_constants(const ir::Expr& expr, const ScalarRegistry* scalars,
                                         const HasColumn& has_column) -> std::optional<ir::Expr> {
    if (scalars == nullptr || scalars->empty()) {
        return std::nullopt;
    }
    bool found = false;
    bound_filter_detail::visit_bindings(expr, *scalars, has_column,
                                        [&](const ir::Expr&, const ScalarValue&) { found = true; });
    if (!found) {
        return std::nullopt;
    }
    ir::Expr bound = expr;
    bound_filter_detail::visit_bindings(
        bound, *scalars, has_column, [](ir::Expr& target, const ScalarValue& value) {
            std::visit([&](const auto& scalar) { target.node = ir::Literal{.value = scalar}; },
                       value);
        });
    return bound;
}

}  // namespace ibex::runtime
