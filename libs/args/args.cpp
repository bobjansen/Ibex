// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Ibex plugin entry point for args.hpp.
//
//   import "args";
//   let args = parse_args("threads (t) : int = 4");

#include "args.hpp"

#include <ibex/runtime/extern_registry.hpp>

#include <exception>
#include <string>

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "parse_args",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.empty() || args.size() > 2) {
                return std::unexpected("parse_args(spec[, argv]) expects 1 or 2 arguments");
            }
            const auto* spec = std::get_if<std::string>(&args[0]);
            if (spec == nullptr) {
                return std::unexpected("parse_args() expects a string spec");
            }
            const std::string* argv = nullptr;
            if (args.size() == 2) {
                argv = std::get_if<std::string>(&args[1]);
                if (argv == nullptr) {
                    return std::unexpected("parse_args(spec, argv) expects a string argv");
                }
            }
            try {
                // An empty explicit argv means "use the IBEX_ARGS environment".
                if (argv != nullptr && !argv->empty()) {
                    return ibex::runtime::ExternValue{parse_args(*spec, *argv)};
                }
                return ibex::runtime::ExternValue{parse_args(*spec)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
