// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Emscripten bridge: exposes an Ibex REPL session to JavaScript so the browser
// UI can evaluate queries with no local server. This is a spike — the goal is
// to prove the interpreter runs in WASM and that the result shape matches what
// `src/ui/server.cpp` already sends over HTTP, so the frontend transport can be
// swapped with minimal change.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_format.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <variant>

#include <emscripten/bind.h>

// data_gen is statically linked (see wasm/CMakeLists.txt); its shared-object
// entry point is called directly instead of via dlopen.
extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry);

namespace {

using json = nlohmann::json;
using ibex::Categorical;
using ibex::Column;
using ibex::Date;
using ibex::Timestamp;

// Rows returned per result. Matches the server's clamp so a runaway `trades;`
// does not try to marshal 50k rows through embind.
constexpr std::size_t kMaxRows = 1000;

auto column_type(const ibex::runtime::ColumnValue& value) -> std::string {
    if (std::holds_alternative<Column<std::int64_t>>(value))
        return "Int64";
    if (std::holds_alternative<Column<double>>(value))
        return "Float64";
    if (std::holds_alternative<Column<std::string>>(value))
        return "String";
    if (std::holds_alternative<Column<Categorical>>(value))
        return "Categorical";
    if (std::holds_alternative<Column<Date>>(value))
        return "Date";
    if (std::holds_alternative<Column<Timestamp>>(value))
        return "Timestamp";
    return "Bool";
}

auto cell_json(const ibex::runtime::ColumnEntry& entry, std::size_t row) -> json {
    if (ibex::runtime::is_null(entry, row))
        return nullptr;
    return std::visit(
        [row](const auto& column) -> json {
            using T = typename std::decay_t<decltype(column)>::value_type;
            if constexpr (std::same_as<T, Date>) {
                return ibex::runtime::format_date(column[row]);
            } else if constexpr (std::same_as<T, Timestamp>) {
                return ibex::runtime::format_timestamp(column[row]);
            } else if constexpr (std::same_as<T, Categorical>) {
                return std::string(column[row]);
            } else {
                return column[row];
            }
        },
        *entry.column);
}

auto table_json(const ibex::runtime::Table& table) -> json {
    const std::size_t rows = std::min(table.rows(), kMaxRows);
    json columns = json::array();
    for (const auto& entry : table.columns)
        columns.push_back({{"name", entry.name}, {"type", column_type(*entry.column)}});
    json out_rows = json::array();
    for (std::size_t row = 0; row < rows; ++row) {
        json values = json::array();
        for (const auto& entry : table.columns)
            values.push_back(cell_json(entry, row));
        out_rows.push_back(std::move(values));
    }
    return {{"kind", "table"},
            {"columns", std::move(columns)},
            {"rows", std::move(out_rows)},
            {"offset", 0},
            {"total_rows", table.rows()}};
}

auto scalar_json(const ibex::runtime::ScalarValue& value) -> json {
    return std::visit(
        [](const auto& scalar) -> json {
            using T = std::decay_t<decltype(scalar)>;
            if constexpr (std::same_as<T, Date>) {
                return ibex::runtime::format_date(scalar);
            } else if constexpr (std::same_as<T, Timestamp>) {
                return ibex::runtime::format_timestamp(scalar);
            } else {
                return scalar;
            }
        },
        value);
}

class Bridge {
   public:
    Bridge() {
        ibex_register(&registry_);
        registry_.register_library("data_gen");

        ibex::repl::ReplConfig config;
        config.persistent_history = false;
        config.plugin_search_paths = {"/ibex"};
        config.import_search_paths = {"/ibex"};
        session_ = std::make_unique<ibex::repl::ReplSession>(config, registry_);
    }

    auto environment_json() const -> json {
        json tables = json::array();
        for (const auto& table : session_->environment()) {
            json columns = json::array();
            for (const auto& [name, type] : table.columns)
                columns.push_back({{"name", name}, {"type", type}});
            tables.push_back({{"name", table.name},
                              {"rows", table.rows},
                              {"lazy", table.lazy},
                              {"columns", std::move(columns)}});
        }
        return {{"tables", std::move(tables)}};
    }

    auto execute(const std::string& source) -> std::string {
        const ibex::repl::ExecutionResult result = session_->execute(source);
        json out;
        out["ok"] = result.ok;
        out["error"] = result.error;
        out["error_line"] = result.error_line.has_value() ? json(*result.error_line) : json(nullptr);
        out["error_column"] =
            result.error_column.has_value() ? json(*result.error_column) : json(nullptr);
        json results = json::array();
        for (const auto& table : result.tables)
            results.push_back(table_json(table));
        out["results"] = std::move(results);
        if (result.scalar.has_value())
            out["scalar"] = scalar_json(*result.scalar);
        out["environment"] = environment_json();
        return out.dump();
    }

   private:
    ibex::runtime::ExternRegistry registry_;
    std::unique_ptr<ibex::repl::ReplSession> session_;
};

Bridge& bridge() {
    static Bridge instance;
    return instance;
}

auto execute(const std::string& source) -> std::string {
    return bridge().execute(source);
}

auto environment() -> std::string {
    return bridge().environment_json().dump();
}

}  // namespace

EMSCRIPTEN_BINDINGS(ibex) {
    emscripten::function("execute", &execute);
    emscripten::function("environment", &environment);
}
