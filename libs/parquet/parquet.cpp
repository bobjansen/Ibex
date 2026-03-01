// Ibex plugin entry point for parquet.hpp.
//
// Build as a shared library alongside parquet.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";
//   extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";

#include "parquet.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "read_parquet",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_parquet() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_parquet() expects a string path");
            }
            try {
                return ibex::runtime::ExternValue{read_parquet(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry->register_scalar_table_consumer(
        "write_parquet",
        ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected(
                    "write_parquet(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("write_parquet(df, path) expects a string path");
            }
            try {
                std::int64_t rows = write_parquet(table, *path);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
