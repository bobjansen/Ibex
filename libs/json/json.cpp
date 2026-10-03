// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Ibex plugin entry point for json.hpp.
//
// Build as a shared library alongside json.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn json::read(path: String) -> DataFrame from "json.hpp";
//   extern fn json::write(df: DataFrame, path: String) -> Int from "json.hpp";

#include "json.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "json::read",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("json::read() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("json::read() expects a string path");
            }
            try {
                return ibex::runtime::ExternValue{ibex::ext::json::read(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    registry->register_scalar_table_consumer(
        "json::write", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected(
                    "json::write(df, path) expects exactly 1 scalar argument (path)");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("json::write(df, path) expects a string path");
            }
            try {
                std::int64_t rows = ibex::ext::json::write(table, *path);
                return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{rows}};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
