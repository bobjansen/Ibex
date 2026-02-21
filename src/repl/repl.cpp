#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <iostream>
#include <string>
#include <vector>

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
        for (const auto& stmt : parsed->statements) {
            if (std::holds_alternative<parser::ExternDecl>(stmt)) {
                continue;
            }
            if (std::holds_alternative<parser::LetStmt>(stmt)) {
                const auto& let_stmt = std::get<parser::LetStmt>(stmt);
                if (const auto* call = std::get_if<parser::CallExpr>(&let_stmt.value->node);
                    call != nullptr && call->callee == "scalar") {
                    if (call->args.size() != 2) {
                        fmt::print("error: scalar() expects (table, column)\n");
                        had_error = true;
                        break;
                    }
                    const auto* col_ident =
                        std::get_if<parser::IdentifierExpr>(&call->args[1]->node);
                    const auto* col_lit = std::get_if<parser::LiteralExpr>(&call->args[1]->node);
                    std::string column_name;
                    if (col_ident != nullptr) {
                        column_name = col_ident->name;
                    } else if (col_lit != nullptr &&
                               std::holds_alternative<std::string>(col_lit->value)) {
                        column_name = std::get<std::string>(col_lit->value);
                    } else {
                        fmt::print("error: scalar() column must be identifier or string\n");
                        had_error = true;
                        break;
                    }
                    auto lowered = parser::lower_expr(*call->args[0], context);
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
                    auto scalar = runtime::extract_scalar(evaluated.value(), column_name);
                    if (!scalar) {
                        fmt::print("error: {}\n", scalar.error());
                        had_error = true;
                        break;
                    }
                    scalars.insert_or_assign(let_stmt.name, std::move(scalar.value()));
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
                if (const auto* call = std::get_if<parser::CallExpr>(&expr_stmt.expr->node);
                    call != nullptr && call->callee == "scalar") {
                    if (call->args.size() != 2) {
                        fmt::print("error: scalar() expects (table, column)\n");
                        had_error = true;
                        break;
                    }
                    const auto* col_ident =
                        std::get_if<parser::IdentifierExpr>(&call->args[1]->node);
                    const auto* col_lit = std::get_if<parser::LiteralExpr>(&call->args[1]->node);
                    std::string column_name;
                    if (col_ident != nullptr) {
                        column_name = col_ident->name;
                    } else if (col_lit != nullptr &&
                               std::holds_alternative<std::string>(col_lit->value)) {
                        column_name = std::get<std::string>(col_lit->value);
                    } else {
                        fmt::print("error: scalar() column must be identifier or string\n");
                        had_error = true;
                        break;
                    }
                    auto lowered = parser::lower_expr(*call->args[0], context);
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
                    auto scalar = runtime::extract_scalar(evaluated.value(), column_name);
                    if (!scalar) {
                        fmt::print("error: {}\n", scalar.error());
                        had_error = true;
                        break;
                    }
                    std::visit([](const auto& value) { fmt::print("{}\n", value); },
                               scalar.value());
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
