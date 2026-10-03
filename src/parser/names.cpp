// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parser/ast.hpp>
#include <ibex/parser/names.hpp>
#include <ibex/parser/parser.hpp>

#include <algorithm>
#include <cstddef>
#include <expected>
#include <functional>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::parser {

namespace {

using Status = std::expected<void, std::string>;
using CalleeFn = std::function<Status(std::string&)>;

/// Calls `fn` on every callee name in an expression tree: each `CallExpr` and
/// the sink of a `Stream`. Every node kind is listed, so a new one fails to
/// compile here instead of hiding its calls from resolution.
class CalleeWalker {
   public:
    explicit CalleeWalker(const CalleeFn& fn) : fn_(&fn) {}

    auto expr(Expr* expr) -> Status {
        if (expr == nullptr) {
            return {};
        }
        return std::visit([this](auto& node) { return this->node(node); }, expr->node);
    }

    auto expr(ExprPtr& ptr) -> Status { return expr(ptr.get()); }

    auto clauses(std::vector<Clause>& clauses) -> Status {
        for (auto& clause : clauses) {
            auto status = std::visit([this](auto& c) { return this->clause(c); }, clause);
            if (!status) {
                return status;
            }
        }
        return {};
    }

   private:
    auto exprs(std::vector<ExprPtr>& list) -> Status {
        for (auto& item : list) {
            if (auto status = expr(item); !status) {
                return status;
            }
        }
        return {};
    }

    auto named(std::vector<NamedArg>& args) -> Status {
        for (auto& arg : args) {
            if (auto status = expr(arg.value); !status) {
                return status;
            }
        }
        return {};
    }

    auto fields(std::vector<Field>& list) -> Status {
        for (auto& field : list) {
            if (auto status = expr(field.expr); !status) {
                return status;
            }
        }
        return {};
    }

    auto tuple_fields(std::vector<TupleField>& list) -> Status {
        for (auto& field : list) {
            if (auto status = expr(field.expr); !status) {
                return status;
            }
        }
        return {};
    }

    auto map_fields(std::vector<MapField>& list) -> Status {
        for (auto& field : list) {
            if (auto status = expr(field.where_expr); !status) {
                return status;
            }
            if (auto status = expr(field.expr); !status) {
                return status;
            }
        }
        return {};
    }

    static auto node(IdentifierExpr& /*node*/) -> Status { return {}; }
    static auto node(LiteralExpr& /*node*/) -> Status { return {}; }
    auto node(CaseExpr& node) -> Status {
        if (auto status = expr(node.selector); !status) {
            return status;
        }
        for (auto& arm : node.arms) {
            if (auto status = expr(arm.condition); !status) {
                return status;
            }
            if (auto status = expr(arm.value); !status) {
                return status;
            }
        }
        return expr(node.else_value);
    }
    auto node(CallExpr& node) -> Status {
        if (auto status = (*fn_)(node.callee); !status) {
            return status;
        }
        if (auto status = exprs(node.args); !status) {
            return status;
        }
        return named(node.named_args);
    }
    auto node(RankExpr& node) -> Status { return named(node.named_args); }
    auto node(UnaryExpr& node) -> Status { return expr(node.expr); }
    auto node(BinaryExpr& node) -> Status {
        if (auto status = expr(node.left); !status) {
            return status;
        }
        return expr(node.right);
    }
    auto node(GroupExpr& node) -> Status { return expr(node.expr); }
    auto node(BlockExpr& node) -> Status {
        if (auto status = expr(node.base); !status) {
            return status;
        }
        return clauses(node.clauses);
    }
    auto node(JoinExpr& node) -> Status {
        if (auto status = expr(node.left); !status) {
            return status;
        }
        if (auto status = expr(node.right); !status) {
            return status;
        }
        if (node.predicate.has_value()) {
            return expr(*node.predicate);
        }
        return {};
    }
    auto node(StreamExpr& node) -> Status {
        if (auto status = expr(node.source); !status) {
            return status;
        }
        if (auto status = clauses(node.transform); !status) {
            return status;
        }
        if (auto status = (*fn_)(node.sink_callee); !status) {
            return status;
        }
        return exprs(node.sink_args);
    }
    auto node(ArrayLiteralExpr& node) -> Status { return exprs(node.elements); }
    auto node(TableExpr& node) -> Status {
        for (auto& column : node.columns) {
            if (auto status = expr(column.expr); !status) {
                return status;
            }
        }
        return expr(node.row_count);
    }
    auto node(AscribeExpr& node) -> Status { return expr(node.base); }

    auto clause(FilterClause& c) -> Status { return expr(c.predicate); }
    auto clause(SelectClause& c) -> Status {
        if (auto status = fields(c.fields); !status) {
            return status;
        }
        if (auto status = tuple_fields(c.tuple_fields); !status) {
            return status;
        }
        return map_fields(c.map_fields);
    }
    auto clause(DistinctClause& c) -> Status { return fields(c.fields); }
    auto clause(UpdateClause& c) -> Status {
        if (auto status = fields(c.fields); !status) {
            return status;
        }
        if (auto status = tuple_fields(c.tuple_fields); !status) {
            return status;
        }
        if (auto status = map_fields(c.map_fields); !status) {
            return status;
        }
        if (auto status = expr(c.merge_expr); !status) {
            return status;
        }
        return expr(c.guard);
    }
    auto clause(RenameClause& c) -> Status { return fields(c.fields); }
    static auto clause(OrderClause& /*c*/) -> Status { return {}; }
    auto clause(HeadClause& c) -> Status { return expr(c.count); }
    auto clause(TailClause& c) -> Status { return expr(c.count); }
    auto clause(ByClause& c) -> Status { return fields(c.keys); }
    static auto clause(WindowClause& /*c*/) -> Status { return {}; }
    static auto clause(ResampleClause& /*c*/) -> Status { return {}; }
    auto clause(MeltClause& c) -> Status { return fields(c.id_fields); }
    static auto clause(DcastClause& /*c*/) -> Status { return {}; }
    static auto clause(CovClause& /*c*/) -> Status { return {}; }
    static auto clause(CorrClause& /*c*/) -> Status { return {}; }
    static auto clause(TransposeClause& /*c*/) -> Status { return {}; }
    auto clause(ModelClause& c) -> Status {
        for (auto& param : c.params) {
            if (auto status = expr(param.value); !status) {
                return status;
            }
        }
        return {};
    }
    auto clause(MapClause& c) -> Status { return fields(c.fields); }

    const CalleeFn* fn_;
};

/// Every callee in a function's parameter defaults and body.
auto walk_function(FunctionDecl& fn, const CalleeFn& on_callee) -> Status {
    CalleeWalker walker(on_callee);
    for (auto& param : fn.params) {
        if (auto status = walker.expr(param.default_value); !status) {
            return status;
        }
    }
    for (auto& stmt : fn.body) {
        auto status = std::visit(
            [&](auto& s) -> Status {
                using T = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<T, ExprStmt>) {
                    return walker.expr(s.expr);
                } else {
                    return walker.expr(s.value);
                }
            },
            stmt);
        if (!status) {
            return status;
        }
    }
    return {};
}

/// `scope`'s enclosing namespace: `a` for `a::b`, empty for `a`.
auto parent_scope(std::string_view scope) -> std::string_view {
    const auto cut = scope.rfind("::");
    return cut == std::string_view::npos ? std::string_view{} : scope.substr(0, cut);
}

auto join(std::string_view scope, std::string_view name) -> std::string {
    std::string out(scope);
    out.append("::").append(name);
    return out;
}

/// Rule 2: the innermost `scope::name` that is declared, looking outward.
auto find_in_scope(std::string_view name, std::string_view scope, const DeclaredNames& declared)
    -> std::optional<std::string> {
    for (; !scope.empty(); scope = parent_scope(scope)) {
        auto candidate = join(scope, name);
        if (declared.contains(candidate)) {
            return candidate;
        }
    }
    return std::nullopt;
}

auto edit_distance(std::string_view a, std::string_view b) -> std::size_t {
    std::vector<std::size_t> row(b.size() + 1);
    for (std::size_t j = 0; j <= b.size(); ++j) {
        row[j] = j;
    }
    for (std::size_t i = 1; i <= a.size(); ++i) {
        std::size_t diagonal = row[0];
        row[0] = i;
        for (std::size_t j = 1; j <= b.size(); ++j) {
            const std::size_t above = row[j];
            row[j] =
                std::min({row[j] + 1, row[j - 1] + 1, diagonal + (a[i - 1] == b[j - 1] ? 0U : 1U)});
            diagonal = above;
        }
    }
    return row[b.size()];
}

auto declared_list(const DeclaredNames& declared) -> std::vector<std::string> {
    return declared.list ? declared.list() : std::vector<std::string>{};
}

/// Whether anything is declared directly or nested in namespace `ns`.
auto namespace_exists(std::string_view ns, const std::vector<std::string>& names) -> bool {
    const auto prefix = std::string(ns) + "::";
    return std::ranges::any_of(names, [&](const std::string& n) { return n.starts_with(prefix); });
}

/// Rule 1 failed: say why, naming the closest declaration in the same
/// namespace when there is one.
auto unknown_qualified(std::string_view name, const DeclaredNames& declared) -> std::string {
    const auto ns = namespace_of(name);
    const auto names = declared_list(declared);
    if (!namespace_exists(ns, names)) {
        return "unknown namespace '" + std::string(ns) + "' in '" + std::string(name) +
               "' (is it imported?)";
    }
    const std::string* best = nullptr;
    std::size_t best_distance = 0;
    for (const auto& candidate : names) {
        if (namespace_of(candidate) != ns) {
            continue;
        }
        const auto distance = edit_distance(name, candidate);
        if (best == nullptr || distance < best_distance) {
            best = &candidate;
            best_distance = distance;
        }
    }
    std::string message = "unknown function '" + std::string(name) + "'";
    if (best != nullptr) {
        message += "; did you mean '" + *best + "'?";
    }
    return message;
}

}  // namespace

auto is_qualified(std::string_view name) -> bool {
    return name.contains("::");
}

auto namespace_of(std::string_view name) -> std::string_view {
    if (name.starts_with("::")) {
        name.remove_prefix(2);
    }
    return parent_scope(name);
}

auto resolve_callee(std::string_view callee, std::string_view scope, const UsingScope& usings,
                    const DeclaredNames& declared) -> std::expected<std::string, std::string> {
    if (callee.starts_with("::")) {
        callee.remove_prefix(2);
        if (!is_qualified(callee)) {
            return std::string(callee);
        }
    }
    if (is_qualified(callee)) {
        if (declared.contains(callee)) {
            return std::string(callee);
        }
        return std::unexpected(unknown_qualified(callee, declared));
    }
    if (auto in_scope = find_in_scope(callee, scope, declared)) {
        return std::move(*in_scope);
    }
    std::vector<std::string> candidates;
    const auto add = [&](std::string name) {
        if (std::ranges::find(candidates, name) == candidates.end()) {
            candidates.push_back(std::move(name));
        }
    };
    if (auto it = usings.names.find(std::string(callee)); it != usings.names.end()) {
        for (const auto& target : it->second) {
            add(target);
        }
    }
    for (const auto& ns : usings.namespaces) {
        auto candidate = join(ns, callee);
        if (declared.contains(candidate)) {
            add(std::move(candidate));
        }
    }
    if (candidates.empty()) {
        return std::string(callee);
    }
    if (declared.contains(callee)) {
        candidates.insert(candidates.begin(), std::string(callee));
    }
    if (candidates.size() > 1) {
        std::string message = "'" + std::string(callee) + "' is ambiguous: it could be ";
        for (std::size_t i = 0; i < candidates.size(); ++i) {
            if (i > 0) {
                message += i + 1 == candidates.size() ? " or " : ", ";
            }
            message += "'" + candidates[i] + "'";
        }
        message += "; write the qualified name";
        return std::unexpected(std::move(message));
    }
    return std::move(candidates.front());
}

auto apply_using(std::string_view target, UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<void, std::string> {
    if (is_qualified(target) && declared.contains(target)) {
        const auto bare = target.substr(target.rfind("::") + 2);
        auto& targets = usings.names[std::string(bare)];
        if (std::ranges::find(targets, target) == targets.end()) {
            targets.emplace_back(target);
        }
        return {};
    }
    if (namespace_exists(target, declared_list(declared))) {
        if (std::ranges::find(usings.namespaces, target) == usings.namespaces.end()) {
            usings.namespaces.emplace_back(target);
        }
        return {};
    }
    return std::unexpected("using '" + std::string(target) +
                           "': no namespace or qualified function of that name is declared "
                           "(is it imported?)");
}

auto resolve_names(Expr& expr, std::string_view scope, const UsingScope& usings,
                   const DeclaredNames& declared) -> std::expected<void, std::string> {
    const CalleeFn on_callee = [&](std::string& callee) -> Status {
        auto resolved = resolve_callee(callee, scope, usings, declared);
        if (!resolved) {
            return std::unexpected(resolved.error());
        }
        callee = std::move(*resolved);
        return {};
    };
    return CalleeWalker(on_callee).expr(&expr);
}

auto resolve_names(FunctionDecl& fn, const UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<void, std::string> {
    const CalleeFn on_callee = [&](std::string& callee) -> Status {
        auto resolved = resolve_callee(callee, fn.scope, usings, declared);
        if (!resolved) {
            return std::unexpected("in function '" + fn.name + "': " + resolved.error());
        }
        callee = std::move(*resolved);
        return {};
    };
    return walk_function(fn, on_callee);
}

auto resolve_names(Stmt& stmt, UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<void, std::string> {
    return std::visit(
        [&](auto& s) -> Status {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, UsingDecl>) {
                return apply_using(s.target, usings, declared);
            } else if constexpr (std::is_same_v<T, FunctionDecl>) {
                return resolve_names(s, usings, declared);
            } else if constexpr (std::is_same_v<T, ExternDecl>) {
                for (auto& param : s.params) {
                    if (param.default_value != nullptr) {
                        if (auto status =
                                resolve_names(*param.default_value, s.scope, usings, declared);
                            !status) {
                            return status;
                        }
                    }
                }
                return {};
            } else if constexpr (std::is_same_v<T, ExprStmt>) {
                return resolve_names(*s.expr, {}, usings, declared);
            } else if constexpr (std::is_same_v<T, LetStmt> || std::is_same_v<T, TupleLetStmt>) {
                return resolve_names(*s.value, {}, usings, declared);
            } else {
                return {};
            }
        },
        stmt);
}

auto resolve_names(Program& program, UsingScope& usings, const DeclaredNames& declared)
    -> std::expected<void, ParseError> {
    for (auto& stmt : program.statements) {
        if (auto status = resolve_names(stmt, usings, declared); !status) {
            const auto line = std::visit([](const auto& s) { return s.start_line; }, stmt);
            return std::unexpected(
                ParseError{.message = status.error(), .line = line, .column = 0});
        }
    }
    return {};
}

void collect_declared_names(const Program& program, robin_hood::unordered_set<std::string>& out) {
    for (const auto& stmt : program.statements) {
        if (const auto* fn = std::get_if<FunctionDecl>(&stmt)) {
            out.insert(fn->name);
        } else if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
            out.insert(ext->name);
        }
    }
}

auto declared_names_of(const robin_hood::unordered_set<std::string>& names) -> DeclaredNames {
    return DeclaredNames{
        .contains = [&names](std::string_view name) { return names.contains(std::string(name)); },
        .list = [&names]() { return std::vector<std::string>(names.begin(), names.end()); },
    };
}

auto qualify_sibling_calls(Program& program) -> std::expected<void, ParseError> {
    robin_hood::unordered_set<std::string> names;
    collect_declared_names(program, names);
    const auto declared = declared_names_of(names);
    for (auto& stmt : program.statements) {
        auto* fn = std::get_if<FunctionDecl>(&stmt);
        if (fn == nullptr || fn->scope.empty()) {
            continue;
        }
        const CalleeFn on_callee = [&](std::string& callee) -> Status {
            if (!is_qualified(callee)) {
                if (auto found = find_in_scope(callee, fn->scope, declared)) {
                    callee = std::move(*found);
                }
            }
            return {};
        };
        if (auto walked = walk_function(*fn, on_callee); !walked) {
            return std::unexpected(
                ParseError{.message = walked.error(), .line = fn->start_line, .column = 0});
        }
    }
    return {};
}

}  // namespace ibex::parser
