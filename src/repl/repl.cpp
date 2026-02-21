#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <algorithm>
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
                auto lowered = parser::lower_expr(*let_stmt.value, context);
                if (!lowered) {
                    fmt::print("error: {}\n", lowered.error().message);
                    had_error = true;
                    break;
                }
                auto evaluated = runtime::interpret(*lowered.value(), tables);
                if (!evaluated) {
                    fmt::print("error: {}\n", evaluated.error());
                    had_error = true;
                    break;
                }
                tables.insert_or_assign(let_stmt.name, std::move(evaluated.value()));
                continue;
            }
            if (std::holds_alternative<parser::ExprStmt>(stmt)) {
                const auto& expr_stmt = std::get<parser::ExprStmt>(stmt);
                auto lowered = parser::lower_expr(*expr_stmt.expr, context);
                if (!lowered) {
                    fmt::print("error: {}\n", lowered.error().message);
                    had_error = true;
                    break;
                }
                auto evaluated = runtime::interpret(*lowered.value(), tables);
                if (!evaluated) {
                    fmt::print("error: {}\n", evaluated.error());
                    had_error = true;
                    break;
                }
                print_table(evaluated.value());
            }
        }
        if (had_error) {
            continue;
        }
    }

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
