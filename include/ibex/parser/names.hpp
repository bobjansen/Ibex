// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Namespaces and name resolution.
//
// A namespace holds `fn`, `extern fn` and `extern type` declarations. The parser
// flattens `namespace a { ... }` blocks: a declaration inside one is named
// `a::f` and remembers `a` as its scope. From then on a qualified name is just
// a string (`CallExpr::callee == "a::f"`), so lowering, the effect pass, the
// registries and the plugin ABI key on it unchanged.
//
// What is left is deciding which declaration a call names. `resolve_callee` is
// the one place that does it, so the REPL and `ibex_compile` cannot disagree:
//
//   1. `a::f`  the declaration of that full name; column and lexical scope are
//              never consulted, so a column or `let` called `a` cannot shadow it.
//   2. `f` in a function declared in namespace `a::b`: `a::b::f`, then `a::f`,
//              then rule 3.
//   3. `f`     a `using` that brings in `f`, else `f` itself (the existing
//              column -> lexical -> built-in rule, applied later by lowering).
//   `::f` is rule 3 without the `using` step: the global `f`.
//
// Resolution rewrites `callee` to the full name in place; it is idempotent.

#include <ibex/parser/ast.hpp>
#include <ibex/parser/parser.hpp>

#include <expected>
#include <functional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::parser {

/// True for `a::f` and `::f`.
[[nodiscard]] auto is_qualified(std::string_view name) -> bool;

/// The namespace part of `a::b::f` (`a::b`); empty for an unqualified name.
[[nodiscard]] auto namespace_of(std::string_view name) -> std::string_view;

/// What the `using` declarations seen so far make visible. One per file or
/// script: a `using` applies from its statement to the end of its source.
struct UsingScope {
    /// `using a;` — every name declared directly in `a`.
    std::vector<std::string> namespaces;
    /// `using a::f;` — bare `f` to each qualified name brought in under it.
    /// Two that disagree are an error where `f` is used, not here.
    robin_hood::unordered_map<std::string, std::vector<std::string>> names;
};

/// The `fn` / `extern fn` names a resolution can see, by full name.
struct DeclaredNames {
    std::function<bool(std::string_view)> contains;
    /// Every declared name, for "did you mean" and for telling a namespace
    /// from a misspelling. May be empty.
    std::function<std::vector<std::string>()> list;
};

/// The full name `callee` refers to from a function declared in `scope`
/// (empty outside any namespace). An unqualified name nothing claims comes back
/// unchanged, for lowering's own rules to resolve.
[[nodiscard]] auto resolve_callee(std::string_view callee, std::string_view scope,
                                  const UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<std::string, std::string>;

/// Record `using target;` in `usings`. The target is a declared name or a
/// namespace that has at least one declaration.
[[nodiscard]] auto apply_using(std::string_view target, UsingScope& usings,
                               const DeclaredNames& declared) -> std::expected<void, std::string>;

/// Resolve every callee in `expr` as seen from `scope`.
[[nodiscard]] auto resolve_names(Expr& expr, std::string_view scope, const UsingScope& usings,
                                 const DeclaredNames& declared) -> std::expected<void, std::string>;

/// Resolve the parameter defaults and body of `fn` from its own scope.
[[nodiscard]] auto resolve_names(FunctionDecl& fn, const UsingScope& usings,
                                 const DeclaredNames& declared) -> std::expected<void, std::string>;

/// Resolve one statement. A `UsingDecl` is applied to `usings`.
[[nodiscard]] auto resolve_names(Stmt& stmt, UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<void, std::string>;

/// Resolve a whole program in statement order, `usings` taking effect where
/// they appear.
[[nodiscard]] auto resolve_names(Program& program, UsingScope& usings,
                                 const DeclaredNames& declared) -> std::expected<void, ParseError>;

/// Add the name of every `fn` and `extern fn` in `program` to `out`.
void collect_declared_names(const Program& program, robin_hood::unordered_set<std::string>& out);

/// `DeclaredNames` over a set the caller owns (and keeps alive).
[[nodiscard]] auto declared_names_of(const robin_hood::unordered_set<std::string>& names)
    -> DeclaredNames;

/// The part of resolution one source can do alone, run by `parse()`: in a
/// function declared in a namespace, a bare call to a sibling declared in the
/// same source becomes qualified (rule 2), so the effect pass already sees the
/// right callee. Siblings from other sources wait for `resolve_names`.
[[nodiscard]] auto qualify_sibling_calls(Program& program) -> std::expected<void, ParseError>;

}  // namespace ibex::parser
