// Ibex plugin entry point for csv.hpp.
//
// Build as a shared library alongside csv.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   extern fn write_csv(df: DataFrame, path: String) -> Int from "csv.hpp";

#include "csv.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "read_csv",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1 && args.size() != 2) {
                return std::unexpected("read_csv() expects 1 or 2 arguments");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_csv() expects a string path");
            }
            try {
                if (args.size() == 2) {
                    const auto* null_spec = std::get_if<std::string>(&args[1]);
                    if (null_spec == nullptr) {
                        return std::unexpected("read_csv(path, nulls) expects a string null spec");
                    }
                    return ibex::runtime::ExternValue{read_csv(*path, *null_spec)};
                }
                return ibex::runtime::ExternValue{read_csv(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry->register_scalar_table_consumer(
        "write_csv",
        ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("write_csv(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_csv(df, path) expects a string path");
            }
            try {
                std::int64_t rows = write_csv(table, *path);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
