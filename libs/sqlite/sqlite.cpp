// Ibex plugin entry point for sqlite.hpp.
//
// Build as a shared library alongside sqlite.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn read_sqlite(path: String, table: String) -> DataFrame from "sqlite.hpp";
//   extern fn write_sqlite(df: DataFrame, path: String, table: String) -> Int from "sqlite.hpp";

#include "sqlite.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "read_sqlite",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 2) {
                return std::unexpected("read_sqlite(path, table) expects exactly 2 arguments");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_sqlite: first argument (path) must be a String");
            }
            const auto* table = std::get_if<std::string>(&args[1]);
            if (table == nullptr) {
                return std::unexpected("read_sqlite: second argument (table) must be a String");
            }
            try {
                return ibex::runtime::ExternValue{read_sqlite(*path, *table)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry->register_scalar_table_consumer(
        "write_sqlite",
        ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 2) {
                return std::unexpected(
                    "write_sqlite(df, path, table) expects exactly 2 scalar arguments "
                    "(path and table name)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_sqlite: second argument (path) must be a String");
            }
            const auto* table_name = std::get_if<std::string>(&args[1]);
            if (table_name == nullptr) {
                return std::unexpected(
                    "write_sqlite: third argument (table name) must be a String");
            }
            try {
                std::int64_t rows = write_sqlite(table, *path, *table_name);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
