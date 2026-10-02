// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/parser/ast.hpp>

#include <functional>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::parser {

/// True when `params` or `return_type` names an `extern type` resource.
[[nodiscard]] auto has_resource_signature(const std::vector<Param>& params, const Type& return_type)
    -> bool;

/// Decides which callees are resource functions: an `extern fn` or `fn` that
/// takes or returns a resource, or a `fn` whose body (or a parameter default)
/// calls one, directly or through other functions. Calls to them run one
/// statement at a time on the statement coordinator, never inside a query.
///
/// A `fn` shadows an `extern fn` of the same name, as it does at runtime.
/// Lookups see the declarations as they are when asked; build a new instance
/// after declarations change.
class ResourceFunctions {
   public:
    using ExternLookup = std::function<const ExternDecl*(std::string_view)>;
    using FunctionLookup = std::function<const FunctionDecl*(std::string_view)>;

    ResourceFunctions(ExternLookup externs, FunctionLookup functions);

    [[nodiscard]] auto contains(std::string_view callee) const -> bool;

    /// The first resource function `expr` calls anywhere, including inside
    /// clauses and nested blocks.
    [[nodiscard]] auto first_call(const Expr& expr) const -> std::optional<std::string>;
    [[nodiscard]] auto first_call(const Clause& clause) const -> std::optional<std::string>;

    /// The first resource function `fn`'s body or parameter defaults call.
    [[nodiscard]] auto first_call(const FunctionDecl& fn) const -> std::optional<std::string>;

    /// The first resource call in a statement's value `expr` that nothing would
    /// run: a call may be the statement's value, a table operand (the base of a
    /// block, either side of a join, a group or an ascription), or an argument
    /// of another call; it may not sit inside a query clause, where it would run
    /// once per row or group. Checked before any call runs, so a misplaced call
    /// makes no plugin call at all. `kPlacementError` completes the message.
    [[nodiscard]] auto first_misplaced(const Expr& expr) const -> std::optional<std::string>;

    static constexpr std::string_view kPlacementError =
        "can be called only as a statement's value, as a table operand, or as an argument of "
        "another call, not inside a query clause";

   private:
    [[nodiscard]] auto misplaced_in_call(const CallExpr& call) const -> std::optional<std::string>;
    [[nodiscard]] auto misplaced_below(const Expr& expr) const -> std::optional<std::string>;
    [[nodiscard]] auto contains(std::string_view callee,
                                robin_hood::unordered_set<std::string>& visiting) const -> bool;
    [[nodiscard]] auto first_call(const FunctionDecl& fn,
                                  robin_hood::unordered_set<std::string>& visiting) const
        -> std::optional<std::string>;

    ExternLookup externs_;
    FunctionLookup functions_;
    // Only positive answers are cached: a negative one reached while a
    // recursive function is still being visited may not be final.
    mutable robin_hood::unordered_set<std::string> known_;
};

}  // namespace ibex::parser
