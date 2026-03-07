#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>

#include <cstdint>
#include <expected>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

auto read_file(const std::string& path) -> std::string {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("unable to open file: " + path);
    }
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

auto eval_scalar_expr(const ibex::parser::Expr& expr,
                      const std::unordered_map<std::string, ibex::runtime::ScalarValue>& env)
    -> std::expected<ibex::runtime::ScalarValue, std::string> {
    using namespace ibex::parser;

    if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
        if (auto it = env.find(ident->name); it != env.end()) {
            return it->second;
        }
        return std::unexpected("unknown scalar binding: " + ident->name);
    }

    if (const auto* lit = std::get_if<LiteralExpr>(&expr.node)) {
        if (const auto* v = std::get_if<std::int64_t>(&lit->value))
            return ibex::runtime::ScalarValue{*v};
        if (const auto* v = std::get_if<double>(&lit->value))
            return ibex::runtime::ScalarValue{*v};
        if (const auto* v = std::get_if<std::string>(&lit->value))
            return ibex::runtime::ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Date>(&lit->value))
            return ibex::runtime::ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Timestamp>(&lit->value))
            return ibex::runtime::ScalarValue{*v};
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
            return ibex::runtime::ScalarValue{-(*v)};
        if (const auto* v = std::get_if<double>(&*value))
            return ibex::runtime::ScalarValue{-(*v)};
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
                    return ibex::runtime::ScalarValue{lhs + rhs};
                case BinaryOp::Sub:
                    return ibex::runtime::ScalarValue{lhs - rhs};
                case BinaryOp::Mul:
                    return ibex::runtime::ScalarValue{lhs * rhs};
                case BinaryOp::Div:
                    return ibex::runtime::ScalarValue{lhs / rhs};
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
                return ibex::runtime::ScalarValue{lhs + rhs};
            case BinaryOp::Sub:
                return ibex::runtime::ScalarValue{lhs - rhs};
            case BinaryOp::Mul:
                return ibex::runtime::ScalarValue{lhs * rhs};
            case BinaryOp::Div:
                if (rhs == 0)
                    return std::unexpected("division by zero in scalar let");
                return ibex::runtime::ScalarValue{lhs / rhs};
            case BinaryOp::Mod:
                if (rhs == 0)
                    return std::unexpected("modulo by zero in scalar let");
                return ibex::runtime::ScalarValue{lhs % rhs};
            default:
                return std::unexpected("unsupported scalar operator");
        }
    }

    return std::unexpected("unsupported scalar expression");
}

auto collect_scalar_bindings(const ibex::parser::Program& program)
    -> std::expected<ibex::runtime::ScalarRegistry, std::string> {
    ibex::runtime::ScalarRegistry scalars;

    ibex::parser::LowerContext lower_ctx;
    std::unordered_map<std::string, ibex::runtime::ScalarValue> env;

    for (const auto& stmt : program.statements) {
        if (const auto* ext = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (ext->return_type.kind == ibex::parser::Type::Kind::DataFrame ||
                ext->return_type.kind == ibex::parser::Type::Kind::TimeFrame) {
                lower_ctx.table_externs.insert(ext->name);
            }
            continue;
        }

        const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt);
        if (let_stmt == nullptr) {
            continue;
        }

        auto table_result = ibex::parser::lower_expr(*let_stmt->value, lower_ctx);
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
        scalars[let_stmt->name] = scalar_result.value();
    }

    return scalars;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc != 2) {
        std::cerr << "usage: ibex_eval <file.ibex>\n";
        return 2;
    }

    try {
        const std::string source = read_file(argv[1]);
        auto parsed = ibex::parser::parse(source);
        if (!parsed) {
            std::cerr << "parse error: " << parsed.error().message << '\n';
            return 1;
        }

        auto scalars = collect_scalar_bindings(*parsed);
        if (!scalars) {
            std::cerr << "scalar error: " << scalars.error() << '\n';
            return 1;
        }

        auto lowered = ibex::parser::lower(*parsed);
        if (!lowered) {
            std::cerr << "lower error: " << lowered.error().message << '\n';
            return 1;
        }

        ibex::runtime::TableRegistry tables;
        auto result = ibex::runtime::interpret(*lowered.value(), tables, &*scalars, nullptr);
        if (!result) {
            std::cerr << "runtime error: " << result.error() << '\n';
            return 1;
        }

        ibex::ops::print(*result);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
