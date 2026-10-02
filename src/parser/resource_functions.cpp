// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/resource_functions.hpp>

#include <algorithm>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::parser {

auto has_resource_signature(const std::vector<Param>& params, const Type& return_type) -> bool {
    return return_type.kind == Type::Kind::Resource ||
           std::ranges::any_of(
               params, [](const Param& param) { return param.type.kind == Type::Kind::Resource; });
}

ResourceFunctions::ResourceFunctions(ExternLookup externs, FunctionLookup functions)
    : externs_(std::move(externs)), functions_(std::move(functions)) {}

auto ResourceFunctions::contains(std::string_view callee) const -> bool {
    robin_hood::unordered_set<std::string> visiting;
    return contains(callee, visiting);
}

auto ResourceFunctions::contains(std::string_view callee,
                                 robin_hood::unordered_set<std::string>& visiting) const -> bool {
    std::string name(callee);
    if (known_.contains(name)) {
        return true;
    }
    bool found = false;
    if (const auto* fn = functions_(callee); fn != nullptr) {
        // A recursive call adds nothing its enclosing visit does not find.
        if (!visiting.insert(name).second) {
            return false;
        }
        found = has_resource_signature(fn->params, fn->return_type) ||
                first_call(*fn, visiting).has_value();
    } else if (const auto* decl = externs_(callee); decl != nullptr) {
        found = has_resource_signature(decl->params, decl->return_type);
    }
    if (found) {
        known_.insert(std::move(name));
    }
    return found;
}

auto ResourceFunctions::first_call(const Expr& expr) const -> std::optional<std::string> {
    std::optional<std::string> found;
    (void)contains_call_if(expr, [&](std::string_view callee) {
        if (contains(callee)) {
            found = std::string(callee);
            return true;
        }
        return false;
    });
    return found;
}

auto ResourceFunctions::first_call(const Clause& clause) const -> std::optional<std::string> {
    std::optional<std::string> found;
    (void)clause_contains_call_if(clause, [&](std::string_view callee) {
        if (contains(callee)) {
            found = std::string(callee);
            return true;
        }
        return false;
    });
    return found;
}

auto ResourceFunctions::first_misplaced(const Expr& expr) const -> std::optional<std::string> {
    const auto* call = std::get_if<CallExpr>(&expr.node);
    if (call != nullptr && contains(call->callee)) {
        return misplaced_in_call(*call);
    }
    return misplaced_below(expr);
}

// A resource call's own arguments: a nested resource call is allowed (it can
// open the resource a parameter takes); anything else that hides one is not.
auto ResourceFunctions::misplaced_in_call(const CallExpr& call) const
    -> std::optional<std::string> {
    const auto check = [&](const ExprPtr& arg) -> std::optional<std::string> {
        if (!arg) {
            return std::nullopt;
        }
        const auto* nested = std::get_if<CallExpr>(&arg->node);
        if (nested != nullptr && contains(nested->callee)) {
            return misplaced_in_call(*nested);
        }
        return first_call(*arg);
    };
    for (const auto& arg : call.args) {
        if (auto found = check(arg)) {
            return found;
        }
    }
    for (const auto& named : call.named_args) {
        if (auto found = check(named.value)) {
            return found;
        }
    }
    return std::nullopt;
}

// The positions a statement may hold a resource call in: exactly the slots the
// REPL hoists from, and the compiler's pre-pass; everything else may not.
auto ResourceFunctions::misplaced_below(const Expr& expr) const -> std::optional<std::string> {
    const auto slot = [&](const ExprPtr& child) -> std::optional<std::string> {
        return child ? first_misplaced(*child) : std::nullopt;
    };
    const auto anywhere = [&](const ExprPtr& child) -> std::optional<std::string> {
        return child ? first_call(*child) : std::nullopt;
    };
    if (const auto* block = std::get_if<BlockExpr>(&expr.node)) {
        if (auto found = slot(block->base)) {
            return found;
        }
        for (const auto& clause : block->clauses) {
            if (auto found = first_call(clause)) {
                return found;
            }
        }
        return std::nullopt;
    }
    if (const auto* join = std::get_if<JoinExpr>(&expr.node)) {
        if (auto found = slot(join->left)) {
            return found;
        }
        if (auto found = slot(join->right)) {
            return found;
        }
        return join->predicate.has_value() ? anywhere(*join->predicate) : std::nullopt;
    }
    if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
        return slot(group->expr);
    }
    if (const auto* ascribe = std::get_if<AscribeExpr>(&expr.node)) {
        return slot(ascribe->base);
    }
    if (const auto* call = std::get_if<CallExpr>(&expr.node)) {
        for (const auto& arg : call->args) {
            if (auto found = slot(arg)) {
                return found;
            }
        }
        for (const auto& named : call->named_args) {
            if (auto found = slot(named.value)) {
                return found;
            }
        }
        return std::nullopt;
    }
    return first_call(expr);
}

auto ResourceFunctions::first_call(const FunctionDecl& fn) const -> std::optional<std::string> {
    robin_hood::unordered_set<std::string> visiting{fn.name};
    return first_call(fn, visiting);
}

auto ResourceFunctions::first_call(const FunctionDecl& fn,
                                   robin_hood::unordered_set<std::string>& visiting) const
    -> std::optional<std::string> {
    std::optional<std::string> found;
    const auto check = [&](const Expr* expr) {
        if (expr == nullptr || found.has_value()) {
            return;
        }
        (void)contains_call_if(*expr, [&](std::string_view callee) {
            if (contains(callee, visiting)) {
                found = std::string(callee);
                return true;
            }
            return false;
        });
    };
    for (const auto& param : fn.params) {
        check(param.default_value.get());
    }
    for (const auto& stmt : fn.body) {
        std::visit(
            [&](const auto& s) {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, ExprStmt>) {
                    check(s.expr.get());
                } else {
                    check(s.value.get());
                }
            },
            stmt);
    }
    return found;
}

}  // namespace ibex::parser
