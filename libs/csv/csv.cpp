// Ibex plugin entry point for csv.hpp.
//
// Build as a shared library alongside csv.hpp and place it in a directory
// on IBEX_LIBRARY_PATH so the Ibex REPL can load it automatically when a
// script declares:
//
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

#include "csv.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table(
        "read_csv",
        [](const ibex::runtime::ExternArgs& args) -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 1) {
                return std::unexpected("read_csv() expects 1 argument");
            }
            const auto* path = std::get_if<std::string>(&args[0]);
            if (path == nullptr) {
                return std::unexpected("read_csv() expects a string path");
            }
            try {
                return ibex::runtime::ExternValue{read_csv(*path)};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });
}
