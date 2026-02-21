#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/csv.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <variant>

namespace ibex::repl {

namespace {

auto build_builtin_tables() -> runtime::TableRegistry {
    runtime::TableRegistry registry;
    runtime::Table trades;
    trades.add_column("price", Column<std::int64_t>{10, 20, 30, 25});
    trades.add_column("symbol", Column<std::string>{"A", "B", "A", "C"});
    registry.emplace("trades", std::move(trades));
    return registry;
}

void print_table(const runtime::Table& table, std::size_t max_rows = 10) {
    if (table.columns.empty()) {
        fmt::print("<empty>\n");
        return;
    }
    fmt::print("rows: {}\n", table.rows());
    fmt::print("columns:");
    for (const auto& entry : table.columns) {
        fmt::print(" {}", entry.name);
    }
    fmt::print("\n");

    std::size_t rows = std::min(table.rows(), max_rows);
    for (std::size_t row = 0; row < rows; ++row) {
        fmt::print("  ");
        for (std::size_t col = 0; col < table.columns.size(); ++col) {
            const auto& entry = table.columns[col];
            std::visit([&](const auto& column) { fmt::print("{}", column[row]); }, entry.column);
            if (col + 1 < table.columns.size()) {
                fmt::print("\t");
            }
        }
        fmt::print("\n");
    }
    if (table.rows() > rows) {
        fmt::print("  ... ({} more rows)\n", table.rows() - rows);
    }
}

void print_tables(const runtime::TableRegistry& tables) {
    if (tables.empty()) {
        fmt::print("tables: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(tables.size());
    for (const auto& entry : tables) {
        names.push_back(entry.first);
    }
    std::ranges::sort(names);
    fmt::print("tables:");
    for (const auto& name : names) {
        fmt::print(" {}", name);
    }
    fmt::print("\n");
}

void print_scalars(const runtime::ScalarRegistry& scalars) {
    if (scalars.empty()) {
        fmt::print("scalars: <none>\n");
        return;
    }
    std::vector<std::string> names;
    names.reserve(scalars.size());
    for (const auto& entry : scalars) {
        names.push_back(entry.first);
    }
    std::sort(names.begin(), names.end());
    fmt::print("scalars:\n");
    for (const auto& name : names) {
        fmt::print("  {} = ", name);
        const auto& value = scalars.at(name);
        std::visit([](const auto& v) { fmt::print("{}", v); }, value);
        fmt::print("\n");
    }
}

std::string column_type_name(const runtime::ColumnValue& column) {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return "Int64";
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return "Float64";
    }
    if (std::holds_alternative<Column<std::string>>(column)) {
        return "String";
    }
    return "Unknown";
}

void print_schema(const runtime::Table& table) {
    fmt::print("columns:\n");
    for (const auto& entry : table.columns) {
        fmt::print("  {}: {}\n", entry.name, column_type_name(entry.column));
    }
}

void describe_table(const runtime::Table& table, std::size_t max_rows = 10) {
    print_schema(table);
    print_table(table, max_rows);
}

std::string_view ltrim(std::string_view text) {
    auto pos = text.find_first_not_of(" \t\r\n");
    if (pos == std::string_view::npos) {
        return {};
    }
    return text.substr(pos);
}

std::string_view rtrim(std::string_view text) {
    auto pos = text.find_last_not_of(" \t\r\n");
    if (pos == std::string_view::npos) {
        return {};
    }
    return text.substr(0, pos + 1);
}

std::string_view trim(std::string_view text) {
    return rtrim(ltrim(text));
}

std::size_t parse_optional_size(std::string_view text, std::size_t default_value) {
    text = trim(text);
    if (text.empty()) {
        return default_value;
    }
    std::size_t value = 0;
    auto result = std::from_chars(text.begin(), text.end(), value);
    if (result.ec != std::errc()) {
        return default_value;
    }
    return value;
}

using FunctionRegistry = std::unordered_map<std::string, parser::FunctionDecl>;
using EvalValue = std::variant<runtime::Table, runtime::ScalarValue>;

auto column_name_from_expr(const parser::Expr& expr) -> std::optional<std::string> {
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        return ident->name;
    }
    if (const auto* literal = std::get_if<parser::LiteralExpr>(&expr.node)) {
        if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
            return *str_value;
        }
    }
    return std::nullopt;
}

auto eval_table_expr(const parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<runtime::Table, std::string>;

auto eval_scalar_expr(const parser::Expr& expr, runtime::TableRegistry& tables,
                      runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<runtime::ScalarValue, std::string>;

auto eval_function_call(const parser::CallExpr& call, runtime::TableRegistry& tables,
                        runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<EvalValue, std::string>;

auto eval_expr_value(const parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<EvalValue, std::string> {
    if (std::holds_alternative<parser::LiteralExpr>(expr.node) ||
        std::holds_alternative<parser::BinaryExpr>(expr.node) ||
        std::holds_alternative<parser::UnaryExpr>(expr.node) ||
        std::holds_alternative<parser::GroupExpr>(expr.node)) {
        auto scalar = eval_scalar_expr(expr, tables, scalars, functions);
        if (!scalar) {
            return std::unexpected(scalar.error());
        }
        return EvalValue{std::move(scalar.value())};
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (auto it = scalars.find(ident->name); it != scalars.end()) {
            return EvalValue{it->second};
        }
    }
    if (const auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "scalar") {
            auto scalar = eval_scalar_expr(expr, tables, scalars, functions);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return EvalValue{std::move(scalar.value())};
        }
        if (functions.contains(call->callee)) {
            return eval_function_call(*call, tables, scalars, functions);
        }
    }
    auto table = eval_table_expr(expr, tables, scalars, functions);
    if (!table) {
        return std::unexpected(table.error());
    }
    return EvalValue{std::move(table.value())};
}

auto eval_scalar_expr(const parser::Expr& expr, runtime::TableRegistry& tables,
                      runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<runtime::ScalarValue, std::string> {
    if (const auto* literal = std::get_if<parser::LiteralExpr>(&expr.node)) {
        if (const auto* int_value = std::get_if<std::int64_t>(&literal->value)) {
            return runtime::ScalarValue{*int_value};
        }
        if (const auto* double_value = std::get_if<double>(&literal->value)) {
            return runtime::ScalarValue{*double_value};
        }
        if (const auto* str_value = std::get_if<std::string>(&literal->value)) {
            return runtime::ScalarValue{*str_value};
        }
        return std::unexpected("unsupported scalar literal");
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (auto it = scalars.find(ident->name); it != scalars.end()) {
            return it->second;
        }
        return std::unexpected("unknown scalar: " + ident->name);
    }
    if (const auto* group = std::get_if<parser::GroupExpr>(&expr.node)) {
        return eval_scalar_expr(*group->expr, tables, scalars, functions);
    }
    if (const auto* unary = std::get_if<parser::UnaryExpr>(&expr.node)) {
        auto value = eval_scalar_expr(*unary->expr, tables, scalars, functions);
        if (!value) {
            return std::unexpected(value.error());
        }
        if (unary->op != parser::UnaryOp::Negate) {
            return std::unexpected("unsupported unary operator in scalar expression");
        }
        if (const auto* int_value = std::get_if<std::int64_t>(&value.value())) {
            return runtime::ScalarValue{-(*int_value)};
        }
        if (const auto* double_value = std::get_if<double>(&value.value())) {
            return runtime::ScalarValue{-(*double_value)};
        }
        return std::unexpected("unsupported unary operand type");
    }
    if (const auto* binary = std::get_if<parser::BinaryExpr>(&expr.node)) {
        auto left = eval_scalar_expr(*binary->left, tables, scalars, functions);
        if (!left) {
            return std::unexpected(left.error());
        }
        auto right = eval_scalar_expr(*binary->right, tables, scalars, functions);
        if (!right) {
            return std::unexpected(right.error());
        }
        bool left_double = std::holds_alternative<double>(left.value());
        bool right_double = std::holds_alternative<double>(right.value());
        if (left_double || right_double) {
            double lhs = left_double ? std::get<double>(left.value())
                                     : static_cast<double>(std::get<std::int64_t>(left.value()));
            double rhs = right_double ? std::get<double>(right.value())
                                      : static_cast<double>(std::get<std::int64_t>(right.value()));
            switch (binary->op) {
                case parser::BinaryOp::Add:
                    return runtime::ScalarValue{lhs + rhs};
                case parser::BinaryOp::Sub:
                    return runtime::ScalarValue{lhs - rhs};
                case parser::BinaryOp::Mul:
                    return runtime::ScalarValue{lhs * rhs};
                case parser::BinaryOp::Div:
                    return runtime::ScalarValue{lhs / rhs};
                case parser::BinaryOp::Mod:
                    return std::unexpected("mod not supported for float scalars");
                default:
                    return std::unexpected("unsupported operator in scalar expression");
            }
        }
        auto lhs = std::get<std::int64_t>(left.value());
        auto rhs = std::get<std::int64_t>(right.value());
        switch (binary->op) {
            case parser::BinaryOp::Add:
                return runtime::ScalarValue{lhs + rhs};
            case parser::BinaryOp::Sub:
                return runtime::ScalarValue{lhs - rhs};
            case parser::BinaryOp::Mul:
                return runtime::ScalarValue{lhs * rhs};
            case parser::BinaryOp::Div:
                return runtime::ScalarValue{lhs / rhs};
            case parser::BinaryOp::Mod:
                return runtime::ScalarValue{lhs % rhs};
            default:
                return std::unexpected("unsupported operator in scalar expression");
        }
    }
    if (const auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "scalar") {
            if (call->args.size() != 2) {
                return std::unexpected("scalar() expects (table, column)");
            }
            auto column_name = column_name_from_expr(*call->args[1]);
            if (!column_name.has_value()) {
                return std::unexpected("scalar() column must be identifier or string");
            }
            auto table = eval_table_expr(*call->args[0], tables, scalars, functions);
            if (!table) {
                return std::unexpected(table.error());
            }
            auto scalar = runtime::extract_scalar(table.value(), *column_name);
            if (!scalar) {
                return std::unexpected(scalar.error());
            }
            return scalar.value();
        }
        if (functions.contains(call->callee)) {
            auto value = eval_function_call(*call, tables, scalars, functions);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                return *scalar;
            }
            return std::unexpected("function returned table where scalar expected");
        }
        return std::unexpected("unknown function: " + call->callee);
    }
    return std::unexpected("expected scalar expression");
}

auto eval_table_expr(const parser::Expr& expr, runtime::TableRegistry& tables,
                     runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<runtime::Table, std::string> {
    if (const auto* call = std::get_if<parser::CallExpr>(&expr.node)) {
        if (call->callee == "read_csv") {
            if (call->args.size() != 1) {
                return std::unexpected("read_csv() expects (path)");
            }
            const auto* path_lit = std::get_if<parser::LiteralExpr>(&call->args[0]->node);
            if (path_lit == nullptr ||
                !std::holds_alternative<std::string>(path_lit->value)) {
                return std::unexpected("read_csv() path must be string literal");
            }
            auto table = runtime::read_csv_simple(std::get<std::string>(path_lit->value));
            if (!table) {
                return std::unexpected(table.error());
            }
            return std::move(table.value());
        }
        if (functions.contains(call->callee)) {
            auto value = eval_function_call(*call, tables, scalars, functions);
            if (!value) {
                return std::unexpected(value.error());
            }
            if (auto* table = std::get_if<runtime::Table>(&value.value())) {
                return std::move(*table);
            }
            return std::unexpected("function returned scalar where table expected");
        }
    }
    if (const auto* ident = std::get_if<parser::IdentifierExpr>(&expr.node)) {
        if (scalars.contains(ident->name)) {
            return std::unexpected("expected table expression");
        }
    }
    parser::LowerContext context;
    auto lowered = parser::lower_expr(expr, context);
    if (!lowered) {
        return std::unexpected(lowered.error().message);
    }
    auto evaluated = runtime::interpret(*lowered.value(), tables, &scalars);
    if (!evaluated) {
        return std::unexpected(evaluated.error());
    }
    return std::move(evaluated.value());
}

auto eval_function_call(const parser::CallExpr& call, runtime::TableRegistry& tables,
                        runtime::ScalarRegistry& scalars, const FunctionRegistry& functions)
    -> std::expected<EvalValue, std::string> {
    auto it = functions.find(call.callee);
    if (it == functions.end()) {
        return std::unexpected("unknown function: " + call.callee);
    }
    const auto& fn = it->second;
    if (call.args.size() != fn.params.size()) {
        return std::unexpected("function argument count mismatch");
    }

    runtime::TableRegistry local_tables = tables;
    runtime::ScalarRegistry local_scalars = scalars;

    for (std::size_t i = 0; i < fn.params.size(); ++i) {
        const auto& param = fn.params[i];
        const auto& arg = *call.args[i];
        switch (param.type.kind) {
            case parser::Type::Kind::Scalar: {
                auto value = eval_scalar_expr(arg, tables, scalars, functions);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_scalars.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::DataFrame:
            case parser::Type::Kind::TimeFrame: {
                auto value = eval_table_expr(arg, tables, scalars, functions);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_tables.insert_or_assign(param.name, std::move(value.value()));
                break;
            }
            case parser::Type::Kind::Series:
                return std::unexpected("Column parameters not supported yet");
        }
    }

    std::optional<EvalValue> last_value;
    for (const auto& stmt : fn.body) {
        if (std::holds_alternative<parser::LetStmt>(stmt)) {
            const auto& let_stmt = std::get<parser::LetStmt>(stmt);
            bool type_is_scalar =
                let_stmt.type.has_value() && let_stmt.type->kind == parser::Type::Kind::Scalar;
            bool type_is_table =
                let_stmt.type.has_value() &&
                (let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                 let_stmt.type->kind == parser::Type::Kind::TimeFrame);
            if (let_stmt.type.has_value() && let_stmt.type->kind == parser::Type::Kind::Series) {
                return std::unexpected("Column bindings not supported yet");
            }
            if (type_is_scalar) {
                auto value = eval_scalar_expr(*let_stmt.value, local_tables, local_scalars,
                                              functions);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_scalars.insert_or_assign(let_stmt.name, std::move(value.value()));
            } else if (type_is_table) {
                auto value = eval_table_expr(*let_stmt.value, local_tables, local_scalars,
                                             functions);
                if (!value) {
                    return std::unexpected(value.error());
                }
                local_tables.insert_or_assign(let_stmt.name, std::move(value.value()));
            } else {
                auto value =
                    eval_expr_value(*let_stmt.value, local_tables, local_scalars, functions);
                if (!value) {
                    return std::unexpected(value.error());
                }
                if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                    local_scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
                } else {
                    local_tables.insert_or_assign(let_stmt.name, std::get<runtime::Table>(
                                                                  std::move(value.value())));
                }
            }
            continue;
        }
        const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
        auto value = eval_expr_value(*expr_stmt.expr, local_tables, local_scalars, functions);
        if (!value) {
            return std::unexpected(value.error());
        }
        last_value = std::move(value.value());
    }

    if (!last_value.has_value()) {
        return std::unexpected("function has no return expression");
    }

    if (fn.return_type.kind == parser::Type::Kind::Scalar) {
        if (!std::holds_alternative<runtime::ScalarValue>(*last_value)) {
            return std::unexpected("function return type mismatch (expected scalar)");
        }
        return EvalValue{std::get<runtime::ScalarValue>(std::move(*last_value))};
    }
    if (fn.return_type.kind == parser::Type::Kind::DataFrame ||
        fn.return_type.kind == parser::Type::Kind::TimeFrame) {
        if (!std::holds_alternative<runtime::Table>(*last_value)) {
            return std::unexpected("function return type mismatch (expected table)");
        }
        return EvalValue{std::get<runtime::Table>(std::move(*last_value))};
    }

    return std::unexpected("Column return types not supported yet");
}

}  // namespace

auto normalize_input(std::string_view input) -> std::string {
    std::string normalized(input);
    auto last_non_space = normalized.find_last_not_of(" \t\r\n");
    if (last_non_space != std::string::npos && normalized[last_non_space] != ';') {
        normalized.push_back(';');
    }
    return normalized;
}

void run(const ReplConfig& config, runtime::ExternRegistry& /*registry*/) {
    spdlog::info("Ibex REPL started (verbose={})", config.verbose);

    auto tables = build_builtin_tables();
    runtime::ScalarRegistry scalars;
    FunctionRegistry functions;

    std::string line;
    while (true) {
        fmt::print("{}", config.prompt);
        if (!std::getline(std::cin, line)) {
            fmt::print("\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        if (line == ":q" || line == ":quit" || line == ":exit") {
            break;
        }
        if (line == ":tables") {
            print_tables(tables);
            continue;
        }
        if (line == ":scalars") {
            print_scalars(scalars);
            continue;
        }
        if (line.starts_with(":schema")) {
            auto arg = trim(line.substr(std::string_view(":schema").size()));
            if (arg.empty()) {
                fmt::print("usage: :schema <table>\n");
                continue;
            }
            auto it = tables.find(std::string(arg));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", arg);
                continue;
            }
            print_schema(it->second);
            continue;
        }
        if (line.starts_with(":head")) {
            auto rest = trim(line.substr(std::string_view(":head").size()));
            auto space = rest.find(' ');
            std::string_view name = rest;
            std::string_view count_text;
            if (space != std::string_view::npos) {
                name = trim(rest.substr(0, space));
                count_text = trim(rest.substr(space + 1));
            }
            if (name.empty()) {
                fmt::print("usage: :head <table> [n]\n");
                continue;
            }
            auto it = tables.find(std::string(name));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", name);
                continue;
            }
            std::size_t count = parse_optional_size(count_text, 10);
            print_table(it->second, count);
            continue;
        }
        if (line.starts_with(":describe")) {
            auto rest = trim(line.substr(std::string_view(":describe").size()));
            auto space = rest.find(' ');
            std::string_view name = rest;
            std::string_view count_text;
            if (space != std::string_view::npos) {
                name = trim(rest.substr(0, space));
                count_text = trim(rest.substr(space + 1));
            }
            if (name.empty()) {
                fmt::print("usage: :describe <table> [n]\n");
                continue;
            }
            auto it = tables.find(std::string(name));
            if (it == tables.end()) {
                fmt::print("error: unknown table '{}'\n", name);
                continue;
            }
            std::size_t count = parse_optional_size(count_text, 10);
            describe_table(it->second, count);
            continue;
        }

        auto normalized = normalize_input(line);
        auto parsed = parser::parse(normalized);
        if (!parsed) {
            fmt::print("error: {}\n", parsed.error().format());
            continue;
        }

        parser::LowerContext context;
        bool had_error = false;
        for (auto& stmt : parsed->statements) {
            if (std::holds_alternative<parser::ExternDecl>(stmt)) {
                continue;
            }
            if (std::holds_alternative<parser::FunctionDecl>(stmt)) {
                auto fn = std::get<parser::FunctionDecl>(std::move(stmt));
                functions.insert_or_assign(fn.name, std::move(fn));
                continue;
            }
            if (std::holds_alternative<parser::LetStmt>(stmt)) {
                const auto& let_stmt = std::get<parser::LetStmt>(stmt);
                if (const auto* fn_call = std::get_if<parser::CallExpr>(&let_stmt.value->node);
                    fn_call != nullptr && functions.contains(fn_call->callee)) {
                    auto value = eval_function_call(*fn_call, tables, scalars, functions);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        had_error = true;
                        break;
                    }
                    bool expect_scalar = let_stmt.type.has_value() &&
                                         let_stmt.type->kind == parser::Type::Kind::Scalar;
                    bool expect_table = let_stmt.type.has_value() &&
                                        (let_stmt.type->kind == parser::Type::Kind::DataFrame ||
                                         let_stmt.type->kind == parser::Type::Kind::TimeFrame);
                    if (expect_scalar && !std::holds_alternative<runtime::ScalarValue>(*value)) {
                        fmt::print("error: expected scalar return for {}\n", let_stmt.name);
                        had_error = true;
                        break;
                    }
                    if (expect_table && !std::holds_alternative<runtime::Table>(*value)) {
                        fmt::print("error: expected table return for {}\n", let_stmt.name);
                        had_error = true;
                        break;
                    }
                    if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                        scalars.insert_or_assign(let_stmt.name, std::move(*scalar));
                    } else {
                        tables.insert_or_assign(let_stmt.name,
                                                std::get<runtime::Table>(std::move(value.value())));
                    }
                    continue;
                }
                if (const auto* scalar_call = std::get_if<parser::CallExpr>(&let_stmt.value->node);
                    scalar_call != nullptr && scalar_call->callee == "scalar") {
                    auto scalar = eval_scalar_expr(*let_stmt.value, tables, scalars, functions);
                    if (!scalar) {
                        fmt::print("error: {}\n", scalar.error());
                        had_error = true;
                        break;
                    }
                    scalars.insert_or_assign(let_stmt.name, std::move(scalar.value()));
                } else if (const auto* read_call =
                               std::get_if<parser::CallExpr>(&let_stmt.value->node);
                           read_call != nullptr && read_call->callee == "read_csv") {
                    if (read_call->args.size() != 1) {
                        fmt::print("error: read_csv() expects (path)\n");
                        had_error = true;
                        break;
                    }
                    const auto* path_lit =
                        std::get_if<parser::LiteralExpr>(&read_call->args[0]->node);
                    if (path_lit == nullptr ||
                        !std::holds_alternative<std::string>(path_lit->value)) {
                        fmt::print("error: read_csv() path must be string literal\n");
                        had_error = true;
                        break;
                    }
                    auto table = runtime::read_csv_simple(std::get<std::string>(path_lit->value));
                    if (!table) {
                        fmt::print("error: {}\n", table.error());
                        had_error = true;
                        break;
                    }
                    tables.insert_or_assign(let_stmt.name, std::move(table.value()));
                } else {
                    auto lowered = parser::lower_expr(*let_stmt.value, context);
                    if (!lowered) {
                        fmt::print("error: {}\n", lowered.error().message);
                        had_error = true;
                        break;
                    }
                    auto evaluated = runtime::interpret(*lowered.value(), tables, &scalars);
                    if (!evaluated) {
                        fmt::print("error: {}\n", evaluated.error());
                        had_error = true;
                        break;
                    }
                    tables.insert_or_assign(let_stmt.name, std::move(evaluated.value()));
                }
                continue;
            }
            if (std::holds_alternative<parser::ExprStmt>(stmt)) {
                const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
                if (const auto* fn_call = std::get_if<parser::CallExpr>(&expr_stmt.expr->node);
                    fn_call != nullptr && functions.contains(fn_call->callee)) {
                    auto value = eval_function_call(*fn_call, tables, scalars, functions);
                    if (!value) {
                        fmt::print("error: {}\n", value.error());
                        had_error = true;
                        break;
                    }
                    if (auto* scalar = std::get_if<runtime::ScalarValue>(&value.value())) {
                        std::visit([](const auto& v) { fmt::print("{}\n", v); }, *scalar);
                    } else {
                        print_table(std::get<runtime::Table>(std::move(value.value())));
                    }
                    continue;
                }
                if (const auto* scalar_call =
                        std::get_if<parser::CallExpr>(&expr_stmt.expr->node);
                    scalar_call != nullptr && scalar_call->callee == "scalar") {
                    auto scalar = eval_scalar_expr(*expr_stmt.expr, tables, scalars, functions);
                    if (!scalar) {
                        fmt::print("error: {}\n", scalar.error());
                        had_error = true;
                        break;
                    }
                    std::visit([](const auto& value) { fmt::print("{}\n", value); },
                               scalar.value());
                } else if (const auto* read_call =
                               std::get_if<parser::CallExpr>(&expr_stmt.expr->node);
                           read_call != nullptr && read_call->callee == "read_csv") {
                    if (read_call->args.size() != 1) {
                        fmt::print("error: read_csv() expects (path)\n");
                        had_error = true;
                        break;
                    }
                    const auto* path_lit =
                        std::get_if<parser::LiteralExpr>(&read_call->args[0]->node);
                    if (path_lit == nullptr ||
                        !std::holds_alternative<std::string>(path_lit->value)) {
                        fmt::print("error: read_csv() path must be string literal\n");
                        had_error = true;
                        break;
                    }
                    auto table = runtime::read_csv_simple(std::get<std::string>(path_lit->value));
                    if (!table) {
                        fmt::print("error: {}\n", table.error());
                        had_error = true;
                        break;
                    }
                    print_table(table.value());
                } else {
                    if (const auto* ident =
                            std::get_if<parser::IdentifierExpr>(&expr_stmt.expr->node)) {
                        if (auto it = scalars.find(ident->name); it != scalars.end()) {
                            std::visit([](const auto& value) { fmt::print("{}\n", value); },
                                       it->second);
                            continue;
                        }
                    }
                    auto lowered = parser::lower_expr(*expr_stmt.expr, context);
                    if (!lowered) {
                        fmt::print("error: {}\n", lowered.error().message);
                        had_error = true;
                        break;
                    }
                    auto evaluated = runtime::interpret(*lowered.value(), tables, &scalars);
                    if (!evaluated) {
                        fmt::print("error: {}\n", evaluated.error());
                        had_error = true;
                        break;
                    }
                    print_table(evaluated.value());
                }
            }
        }
        if (had_error) {
            continue;
        }
    }

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
