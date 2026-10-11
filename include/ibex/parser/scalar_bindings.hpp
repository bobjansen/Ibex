// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Scalar `let` bindings (`let k = (base * 2) + 5;`) are not lowered into IR as
// columns: each is lowered to a value expression and evaluated at run time, in
// order, by the runtime's scalar evaluator (the one every other scalar goes
// through), and supplied to execution as a ScalarRegistry. Both the transpiler
// (ibex_compile) and the parity runner need to reconstruct those bindings from
// a parsed Program, so the logic lives here in one place.

#include <ibex/ir/model_accessors.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/parser/resource_functions.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::parser {

// A program's scalar `let` bindings. Every one in `deferred` is evaluated at run
// time (by the interpreter reference and by the emitted C++) via
// runtime::materialize_deferred_scalar_bindings: a `scalar(<table>)` subquery
// runs its subplans first, a plain expression (`let k = 7 / 2;`) has none.
// Nothing is folded here, so a scalar has the same value on every path.
struct ScalarBindingSet {
    std::vector<ir::DeferredScalarBinding> deferred;
    /// Statement index of each `deferred` binding, parallel to it. A consumer
    /// that runs the script in order runs the binding there; one that evaluates
    /// all of them up front (the single-plan path) ignores it.
    std::vector<std::size_t> deferred_positions;
    /// `let n = f(...)` where `f` is an `extern fn` returning a scalar: the call
    /// runs at its statement and its result is the scalar `n`. Not evaluated here
    /// -- `lower_script` lowers the call -- so only the names are recorded, for a
    /// consumer to know they are runtime scalars.
    std::vector<std::string> extern_calls;
};

[[nodiscard]] inline auto is_scalar_cast_name(std::string_view callee) -> bool {
    return callee == "Int64" || callee == "Int32" || callee == "Int" || callee == "Float64" ||
           callee == "Float32" || callee == "Date" || callee == "Timestamp" || callee == "Decimal";
}

[[nodiscard]] inline auto ir_literal_from_ast(const Expr& expr) -> std::optional<ir::Literal> {
    const auto* lit = std::get_if<LiteralExpr>(&expr.node);
    if (lit == nullptr) {
        return std::nullopt;
    }
    return std::visit(
        [](const auto& v) -> std::optional<ir::Literal> {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, DurationLiteral>) {
                return std::nullopt;
            } else {
                return ir::Literal{.value = v};
            }
        },
        lit->value);
}

[[nodiscard]] inline auto ast_column_name(const Expr& expr) -> std::optional<std::string> {
    if (const auto* id = std::get_if<IdentifierExpr>(&expr.node)) {
        return id->name;
    }
    if (const auto* lit = std::get_if<LiteralExpr>(&expr.node)) {
        if (const auto* s = std::get_if<std::string>(&lit->value)) {
            return *s;
        }
    }
    return std::nullopt;
}

// One layer of scalar-expression wrapper around the `scalar(<table>)` call:
// a cast `Int64(_)`, or `coalesce(_, <literal>)`.
struct DeferredWrap {
    std::string callee;                    // cast name, or "coalesce"
    std::optional<ir::Literal> extra_arg;  // the coalesce default (nullopt for a cast)
};

// Recognize a `let name = <value>` whose RHS is a `scalar(<table>)` subquery,
// wrapped in any number of scalar casts and `coalesce(_, <literal>)` layers.
// Returns nullopt when `value` is not one of those shapes (the caller then
// treats it as a plain scalar let). A recognized-but-malformed shape errors.
[[nodiscard]] inline auto try_build_deferred_scalar_binding(const std::string& name,
                                                            const Expr& value, LowerContext& ctx,
                                                            int& counter)
    -> std::optional<std::expected<ir::DeferredScalarBinding, std::string>> {
    using Ret = std::expected<ir::DeferredScalarBinding, std::string>;

    // Peel wrapper layers, outermost first, until we reach `scalar(...)`.
    std::vector<DeferredWrap> wraps;
    const Expr* cur = &value;
    const CallExpr* scalar_call = nullptr;
    while (true) {
        const auto* call = std::get_if<CallExpr>(&cur->node);
        if (call == nullptr) {
            return std::nullopt;
        }
        if (call->callee == "scalar") {
            scalar_call = call;
            break;
        }
        if (is_scalar_cast_name(call->callee) && call->args.size() == 1) {
            wraps.push_back(DeferredWrap{.callee = call->callee, .extra_arg = std::nullopt});
            cur = call->args[0].get();
            continue;
        }
        if (call->callee == "coalesce" && call->args.size() == 2) {
            auto lit = ir_literal_from_ast(*call->args[1]);
            if (!lit.has_value()) {
                return std::nullopt;  // non-literal default is not a deferred shape
            }
            wraps.push_back(DeferredWrap{.callee = "coalesce", .extra_arg = std::move(lit)});
            cur = call->args[0].get();
            continue;
        }
        return std::nullopt;
    }
    if (scalar_call->args.empty() || scalar_call->args.size() > 2) {
        return std::nullopt;
    }

    auto plan = lower_expr(*scalar_call->args[0], ctx);
    if (!plan.has_value()) {
        return Ret{std::unexpected("scalar let '" + name +
                                   "': scalar() argument is not a table: " + plan.error().message)};
    }
    std::optional<std::string> column;
    if (scalar_call->args.size() == 2) {
        column = ast_column_name(*scalar_call->args[1]);
        if (!column.has_value()) {
            return Ret{std::unexpected("scalar let '" + name +
                                       "': scalar() column must be an identifier or string")};
        }
    }

    std::string tmp = "__ibex_scalar_src_" + std::to_string(counter++);
    ir::Expr residual{.node = ir::ColumnRef{.name = tmp, .lexical = true}};
    // Rebuild the wrapper layers inside-out.
    for (auto& wrap : wraps | std::views::reverse) {
        ir::CallExpr call;
        call.callee = wrap.callee;
        call.args.emplace_back(std::move(residual));
        if (wrap.extra_arg.has_value()) {
            call.args.emplace_back(ir::Expr{.node = std::move(*wrap.extra_arg)});
        }
        residual = ir::Expr{.node = std::move(call)};
    }

    ir::DeferredScalarBinding binding;
    binding.name = name;
    binding.sources.push_back(ir::DeferredScalarSource{
        .tmp_name = std::move(tmp), .plan = std::move(plan.value()), .column = std::move(column)});
    binding.value = std::move(residual);
    return Ret{std::move(binding)};
}

// Walk a program's top-level statements and classify every scalar `let`.
// Table-valued lets are lowered (so later scalar lets and subqueries can
// reference them via the shared LowerContext) but not returned.
[[nodiscard]] inline auto collect_scalar_binding_set(const Program& program,
                                                     std::span<const Program* const> prelude = {})
    -> std::expected<ScalarBindingSet, std::string> {
    ScalarBindingSet out;
    int deferred_counter = 0;

    LowerContext lower_ctx;
    lower_ctx.table_args_as_bindings = true;
    // Externs that return a scalar, by name.
    robin_hood::unordered_set<std::string> scalar_externs;
    // Every extern, for the resource-function test below.
    robin_hood::unordered_map<std::string, const ExternDecl*> all_externs;
    const ResourceFunctions resource_functions(
        [&](std::string_view name) -> const ExternDecl* {
            const auto it = all_externs.find(std::string(name));
            return it == all_externs.end() ? nullptr : it->second;
        },
        [&](std::string_view name) -> const FunctionDecl* {
            const auto it = lower_ctx.functions.find(std::string(name));
            return it == lower_ctx.functions.end() ? nullptr : it->second;
        });

    const auto collect_declaration = [&](const Stmt& stmt) {
        if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
            all_externs.insert_or_assign(ext->name, ext);
            if (ext->return_type.kind == Type::Kind::DataFrame ||
                ext->return_type.kind == Type::Kind::TimeFrame) {
                lower_ctx.table_externs.insert(ext->name);
                lower_ctx.table_extern_decls.insert_or_assign(ext->name, ext);
            } else {
                scalar_externs.insert(ext->name);
            }
            if (!ext->params.empty() && ext->params[0].type.kind == Type::Kind::DataFrame) {
                lower_ctx.sink_externs.insert(ext->name);
            }
        } else if (const auto* fn = std::get_if<FunctionDecl>(&stmt)) {
            lower_ctx.functions.insert_or_assign(fn->name, fn);
        }
    };
    for (const auto* unit : prelude) {
        for (const auto& stmt : unit->statements) {
            collect_declaration(stmt);
        }
    }

    for (std::size_t position = 0; position < program.statements.size(); ++position) {
        const auto& stmt = program.statements[position];
        if (std::holds_alternative<ExternDecl>(stmt)) {
            collect_declaration(stmt);
            continue;
        }
        if (std::holds_alternative<FunctionDecl>(stmt)) {
            collect_declaration(stmt);
            continue;
        }

        const auto* let_stmt = std::get_if<LetStmt>(&stmt);
        if (let_stmt == nullptr) {
            continue;
        }

        // Stream lets are never scalar; let the full lowering phase validate them.
        if (std::holds_alternative<StreamExpr>(let_stmt->value->node)) {
            continue;
        }

        // `let x = f(...)` with `f` a resource `fn`: a call of the program, run
        // where it is. Its result is a table, a scalar or a resource by its
        // declared type; the call is not lowered here (a plan would inline it).
        if (const auto* fn_call = std::get_if<CallExpr>(&let_stmt->value->node);
            fn_call != nullptr && lower_ctx.functions.contains(fn_call->callee) &&
            resource_functions.contains(fn_call->callee)) {
            const auto kind = lower_ctx.functions.at(fn_call->callee)->return_type.kind;
            if (kind != Type::Kind::DataFrame && kind != Type::Kind::TimeFrame) {
                out.extern_calls.push_back(let_stmt->name);
            }
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        // `let c = coef(m)` / `let r = r_squared(m)`: read off a fitted model, in
        // order with the statements that fit it; lowered by the script, not here.
        if (const auto* accessor = std::get_if<CallExpr>(&let_stmt->value->node);
            accessor != nullptr && (ir::is_model_table_accessor(accessor->callee) ||
                                    ir::is_model_scalar_accessor(accessor->callee))) {
            if (ir::is_model_scalar_accessor(accessor->callee)) {
                out.extern_calls.push_back(let_stmt->name);
            }
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        auto table_result = lower_expr(*let_stmt->value, lower_ctx);
        if (table_result) {
            // Record the binding's schema so a later scalar(<table>) subquery
            // or table let that references it can resolve its columns.
            if (auto schema = ir::infer_schema(*table_result.value(), lower_ctx.source_schemas);
                schema.is_known()) {
                lower_ctx.source_schemas.insert_or_assign(let_stmt->name, std::move(schema));
            }
            lower_ctx.bindings[let_stmt->name] = std::move(table_result.value());
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        if (auto deferred = try_build_deferred_scalar_binding(let_stmt->name, *let_stmt->value,
                                                              lower_ctx, deferred_counter);
            deferred.has_value()) {
            if (!deferred->has_value()) {
                return std::unexpected(deferred->error());
            }
            out.deferred.push_back(std::move(deferred->value()));
            out.deferred_positions.push_back(position);
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        // `let n = f(...)` with an extern `f` (a `fn` of the same name shadows
        // it): a runtime scalar, produced by the call.
        if (const auto* call = std::get_if<CallExpr>(&let_stmt->value->node);
            call != nullptr && scalar_externs.contains(call->callee) &&
            !lower_ctx.functions.contains(call->callee)) {
            out.extern_calls.push_back(let_stmt->name);
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        // A call to a table function is a table let whose lowering failed, not
        // a scalar: report why it could not be lowered.
        if (const auto* call = std::get_if<CallExpr>(&let_stmt->value->node);
            call != nullptr && lower_ctx.functions.contains(call->callee)) {
            const auto kind = lower_ctx.functions.at(call->callee)->return_type.kind;
            if (kind == Type::Kind::DataFrame || kind == Type::Kind::TimeFrame) {
                return std::unexpected("let '" + let_stmt->name +
                                       "': " + table_result.error().message);
            }
        }

        // A plain scalar expression: a binding with no subplan, evaluated in
        // order with the others. Later table lets may filter/compute against
        // it; a bare name there must resolve as a binding, not a missing column.
        // `let x: Int64 = NULL;`: the declaration types the NULL, as a literal.
        auto value = [&]() -> std::expected<ir::Expr, LowerError> {
            const auto* null_call = std::get_if<CallExpr>(&let_stmt->value->node);
            const auto* declared =
                let_stmt->type.has_value() && let_stmt->type->kind == Type::Kind::Scalar
                    ? std::get_if<ScalarType>(&let_stmt->type->arg)
                    : nullptr;
            if (declared != nullptr && null_call != nullptr && null_call->callee == "__null" &&
                null_call->args.empty()) {
                return ir::Expr{
                    .node = ir::Literal{
                        .value = NullScalar{scalar_kind_of(*declared),
                                            let_stmt->type->decimal.value_or(DecimalType{})}}};
            }
            return lower_value_expr(*let_stmt->value, lower_ctx);
        }();
        if (!value) {
            return std::unexpected("unsupported scalar let '" + let_stmt->name +
                                   "': " + value.error().message);
        }
        ir::DeferredScalarBinding binding;
        binding.name = let_stmt->name;
        binding.value = std::move(*value);
        out.deferred.push_back(std::move(binding));
        out.deferred_positions.push_back(position);
        lower_ctx.lexical_names.insert(let_stmt->name);
    }

    return out;
}

}  // namespace ibex::parser
