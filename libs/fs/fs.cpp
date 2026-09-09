// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Ibex plugin entry point for fs.hpp.
//
//   import "fs";
//   let files = list_files("data/csv", "*.csv");

#include "fs.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <exception>
#include <string>
#include <variant>

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "list_files",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.empty() || args.size() > 3) {
                return std::unexpected(
                    "list_files(dir[, pattern[, recursive]]) expects 1 to 3 arguments");
            }
            const auto* dir = std::get_if<std::string>(args.data());
            if (dir == nullptr) {
                return std::unexpected("list_files: dir must be a string");
            }
            std::string pattern = "*";
            if (args.size() >= 2) {
                const auto* p = std::get_if<std::string>(&args[1]);
                if (p == nullptr) {
                    return std::unexpected("list_files: pattern must be a string");
                }
                pattern = *p;
            }
            bool recursive = false;
            if (args.size() == 3) {
                if (const auto* b = std::get_if<bool>(&args[2])) {
                    recursive = *b;
                } else if (const auto* i = std::get_if<std::int64_t>(&args[2])) {
                    recursive = *i != 0;
                } else {
                    return std::unexpected("list_files: recursive must be a bool");
                }
            }
            try {
                return ibex::runtime::ExternValue{ibex::fs::list_files(*dir, pattern, recursive)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
