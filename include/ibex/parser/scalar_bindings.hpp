// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Scalar `let` bindings (`let k = (base * 2) + 5;`) are not lowered into IR as
// columns — they are evaluated at compile time and supplied to execution as a
// ScalarRegistry. Both the transpiler (ibex_compile) and the parity runner need
// to reconstruct that registry from a parsed Program, so the logic lives here in
// one place. Values use the same std::variant as ibex::runtime::ScalarValue and
// ibex::codegen::Emitter::Config::ScalarValue (identical aliases, so the results
// are interchangeable with both without conversion).

#include <ibex/core/time.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::parser {

// Must stay identical to ibex::runtime::ScalarValue and
// ibex::codegen::Emitter::Config::ScalarValue -- see the note there. The
// leading std::monostate is the null alternative.
using ScalarValue = std::variant<std::monostate, std::int64_t, double, bool, std::string, ibex::Date,
                                 ibex::Timestamp>;

[[nodiscard]] inline auto eval_scalar_expr(
    const Expr& expr, const robin_hood::unordered_map<std::string, ScalarValue>& env)
    -> std::expected<ScalarValue, std::string> {
    if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
        if (auto it = env.find(ident->name); it != env.end()) {
            return it->second;
        }
        return std::unexpected("unknown scalar binding: " + ident->name);
    }

    if (const auto* lit = std::get_if<LiteralExpr>(&expr.node)) {
        if (const auto* v = std::get_if<std::int64_t>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<double>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<std::string>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Date>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Timestamp>(&lit->value))
            return ScalarValue{*v};
        return std::unexpected("unsupported scalar literal");
    }

    if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
        return eval_scalar_expr(*group->expr, env);
    }

    if (const auto* unary = std::get_if<UnaryExpr>(&expr.node)) {
        if (unary->op != UnaryOp::Negate) {
            return std::unexpected("unsupported unary scalar operator");
        }
        auto value = eval_scalar_expr(*unary->expr, env);
        if (!value) {
            return std::unexpected(value.error());
        }
        if (const auto* v = std::get_if<std::int64_t>(&*value))
            return ScalarValue{-(*v)};
        if (const auto* v = std::get_if<double>(&*value))
            return ScalarValue{-(*v)};
        return std::unexpected("unary negate requires numeric scalar");
    }

    if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
        auto left = eval_scalar_expr(*binary->left, env);
        if (!left) {
            return std::unexpected(left.error());
        }
        auto right = eval_scalar_expr(*binary->right, env);
        if (!right) {
            return std::unexpected(right.error());
        }

        const bool left_double = std::holds_alternative<double>(*left);
        const bool right_double = std::holds_alternative<double>(*right);
        if (left_double || right_double) {
            const double lhs = left_double ? std::get<double>(*left)
                                           : static_cast<double>(std::get<std::int64_t>(*left));
            const double rhs = right_double ? std::get<double>(*right)
                                            : static_cast<double>(std::get<std::int64_t>(*right));
            switch (binary->op) {
                case BinaryOp::Add:
                    return ScalarValue{lhs + rhs};
                case BinaryOp::Sub:
                    return ScalarValue{lhs - rhs};
                case BinaryOp::Mul:
                    return ScalarValue{lhs * rhs};
                case BinaryOp::Div:
                    return ScalarValue{lhs / rhs};
                case BinaryOp::Mod:
                    return std::unexpected("mod not supported for float scalars");
                default:
                    return std::unexpected("unsupported scalar operator");
            }
        }

        if (!std::holds_alternative<std::int64_t>(*left) ||
            !std::holds_alternative<std::int64_t>(*right)) {
            return std::unexpected("binary scalar op requires numeric operands");
        }
        const auto lhs = std::get<std::int64_t>(*left);
        const auto rhs = std::get<std::int64_t>(*right);
        switch (binary->op) {
            case BinaryOp::Add:
                return ScalarValue{lhs + rhs};
            case BinaryOp::Sub:
                return ScalarValue{lhs - rhs};
            case BinaryOp::Mul:
                return ScalarValue{lhs * rhs};
            case BinaryOp::Div:
                if (rhs == 0) {
                    return std::unexpected("division by zero in scalar let");
                }
                return ScalarValue{lhs / rhs};
            case BinaryOp::Mod:
                if (rhs == 0) {
                    return std::unexpected("modulo by zero in scalar let");
                }
                return ScalarValue{lhs % rhs};
            default:
                return std::unexpected("unsupported scalar operator");
        }
    }

    return std::unexpected("unsupported scalar expression");
}

// Scalar `let` bindings split by how they are evaluated: `compile_time` folds
// to a constant during lowering; `deferred` contains a `scalar(<table>)`
// subquery and is evaluated at run time (both by the interpreter reference and
// by the emitted C++) via runtime::materialize_deferred_scalar_bindings.
struct ScalarBindingSet {
    std::vector<std::pair<std::string, ScalarValue>> compile_time;
    std::vector<ir::DeferredScalarBinding> deferred;
};

[[nodiscard]] inline auto is_scalar_cast_name(std::string_view callee) -> bool {
    return callee == "Int64" || callee == "Int32" || callee == "Int" || callee == "Float64" ||
           callee == "Float32" || callee == "Date" || callee == "Timestamp";
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
    for (auto it = wraps.rbegin(); it != wraps.rend(); ++it) {
        ir::CallExpr call;
        call.callee = it->callee;
        call.args.emplace_back(std::move(residual));
        if (it->extra_arg.has_value()) {
            call.args.emplace_back(ir::Expr{.node = std::move(*it->extra_arg)});
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
[[nodiscard]] inline auto collect_scalar_binding_set(const Program& program)
    -> std::expected<ScalarBindingSet, std::string> {
    ScalarBindingSet out;
    robin_hood::unordered_map<std::string, ScalarValue> env;
    int deferred_counter = 0;

    LowerContext lower_ctx;

    for (const auto& stmt : program.statements) {
        if (const auto* ext = std::get_if<ExternDecl>(&stmt)) {
            if (ext->return_type.kind == Type::Kind::DataFrame ||
                ext->return_type.kind == Type::Kind::TimeFrame) {
                lower_ctx.table_externs.insert(ext->name);
                lower_ctx.table_extern_decls.insert_or_assign(ext->name, ext);
            }
            if (!ext->params.empty() && ext->params[0].type.kind == Type::Kind::DataFrame) {
                lower_ctx.sink_externs.insert(ext->name);
            }
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

        auto scalar_result = eval_scalar_expr(*let_stmt->value, env);
        if (scalar_result) {
            env[let_stmt->name] = scalar_result.value();
            out.compile_time.emplace_back(let_stmt->name, scalar_result.value());
            // Later table lets may filter/compute against this scalar; a bare
            // name there must resolve as a binding, not a missing column.
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        if (auto deferred =
                try_build_deferred_scalar_binding(let_stmt->name, *let_stmt->value, lower_ctx,
                                                  deferred_counter);
            deferred.has_value()) {
            if (!deferred->has_value()) {
                return std::unexpected(deferred->error());
            }
            out.deferred.push_back(std::move(deferred->value()));
            lower_ctx.lexical_names.insert(let_stmt->name);
            continue;
        }

        return std::unexpected("unsupported scalar let '" + let_stmt->name +
                               "': " + scalar_result.error());
    }

    return out;
}

// Backwards-compatible: the compile-time bindings only. Prefer
// collect_scalar_binding_set for paths that must also honour deferred bindings.
[[nodiscard]] inline auto collect_scalar_bindings(const Program& program)
    -> std::expected<std::vector<std::pair<std::string, ScalarValue>>, std::string> {
    auto set = collect_scalar_binding_set(program);
    if (!set) {
        return std::unexpected(set.error());
    }
    return std::move(set->compile_time);
}

}  // namespace ibex::parser
