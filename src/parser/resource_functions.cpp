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
