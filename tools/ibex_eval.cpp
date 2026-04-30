// Non-interactive script runner: same execution path as the REPL's `:load`
// command, so model accessors (`model_coef`, `model_fitted`), CSV plugins,
// and other REPL-bound vocabulary work in batch scripts. Replaces an older
// implementation that lowered the whole program to a single IR node and
// interpreted it once — that path didn't recognise model_* and produced
// "unknown table" errors on otherwise valid scripts.

#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

namespace {

auto read_file(const std::string& path) -> std::string {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("unable to open file: " + path);
    }
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

auto main(int argc, char** argv) -> int {
    CLI::App app{"ibex_eval — run an Ibex script non-interactively"};

    std::string script_path;
    std::string plugin_path;
    std::string import_path;
    bool verbose = false;
    app.add_option("file", script_path, ".ibex script to execute")->required();
    app.add_option("--plugin-path", plugin_path,
                   "Directory to search for plugin shared libraries (*.so). "
                   "Defaults to IBEX_LIBRARY_PATH environment variable.");
    app.add_option("--import-path", import_path,
                   "Directory to search for library stub files (*.ibex) used by "
                   "`import` declarations.  Defaults to the plugin search path.");
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");

    CLI11_PARSE(app, argc, argv);

    if (verbose) {
        spdlog::set_level(spdlog::level::debug);
    } else {
        spdlog::set_level(spdlog::level::warn);
    }

    if (plugin_path.empty()) {
        if (const char* env = std::getenv("IBEX_LIBRARY_PATH"); env != nullptr) {
            plugin_path = env;
        }
    }

    ibex::repl::ReplConfig config;
    config.verbose = verbose;
    if (!plugin_path.empty()) {
        config.plugin_search_paths.push_back(plugin_path);
    }
    if (!import_path.empty()) {
        config.import_search_paths.push_back(import_path);
    }

    try {
        const std::string source = read_file(script_path);
        ibex::runtime::ExternRegistry registry;
        return ibex::repl::execute_script(source, registry, config) ? 0 : 1;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
