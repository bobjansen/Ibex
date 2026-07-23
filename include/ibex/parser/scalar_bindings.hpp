#pragma once

// Scalar `let` bindings (`let k = (base * 2) + 5;`) are not lowered into IR as
// columns — they are evaluated at compile time and supplied to execution as a
// ScalarRegistry. Both the transpiler (ibex_compile) and the parity runner need
// to reconstruct that registry from a parsed Program, so the logic lives here in
// one place. Values use the same std::variant as ibex::runtime::ScalarValue and
// ibex::codegen::Emitter::Config::ScalarValue (identical aliases, so the results
// are interchangeable with both without conversion).

#include <ibex/core/time.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <cstdint>
#include <expected>
#include <robin_hood.h>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::parser {

using ScalarValue =
    std::variant<std::int64_t, double, bool, std::string, ibex::Date, ibex::Timestamp>;

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
            double lhs = left_double ? std::get<double>(*left)
                                     : static_cast<double>(std::get<std::int64_t>(*left));
            double rhs = right_double ? std::get<double>(*right)
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

// Walk a program's top-level statements and evaluate every scalar `let`,
// returning them in declaration order. Table-valued lets are lowered (so later
// scalar lets can reference them via the shared LowerContext) but not returned.
[[nodiscard]] inline auto collect_scalar_bindings(const Program& program)
    -> std::expected<std::vector<std::pair<std::string, ScalarValue>>, std::string> {
    std::vector<std::pair<std::string, ScalarValue>> ordered;
    robin_hood::unordered_map<std::string, ScalarValue> env;

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
            lower_ctx.bindings[let_stmt->name] = std::move(table_result.value());
            continue;
        }

        auto scalar_result = eval_scalar_expr(*let_stmt->value, env);
        if (!scalar_result) {
            return std::unexpected("unsupported scalar let '" + let_stmt->name +
                                   "': " + scalar_result.error());
        }
        env[let_stmt->name] = scalar_result.value();
        ordered.emplace_back(let_stmt->name, scalar_result.value());
    }

    return ordered;
}

}  // namespace ibex::parser
