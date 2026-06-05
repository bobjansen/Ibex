#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <string>
#include <vector>

auto main(int argc, char** argv) -> int {
    CLI::App app{"Ibex — interactive columnar DSL"};

    bool verbose = false;
    bool no_history = false;
    std::string plugin_path;
    std::string import_path;
    std::string history_file;
    std::vector<std::string> scripts;
    app.add_option("script", scripts,
                   "Ibex script file(s) to execute (.ibex). When given, runs the "
                   "files instead of starting the interactive REPL. Statements may "
                   "span multiple lines.")
        ->type_name("FILE");
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");
    app.add_flag("--no-history", no_history, "Disable persistent readline history");
    app.add_option("--plugin-path", plugin_path,
                   "Directory to search for plugin shared libraries (*.so). "
                   "Defaults to IBEX_LIBRARY_PATH environment variable.");
    app.add_option("--import-path", import_path,
                   "Directory to search for library stub files (*.ibex) used by "
                   "`import` declarations.  Defaults to the plugin search path.");
    app.add_option("--history-file", history_file,
                   "Read/write REPL history at this path. Defaults to IBEX_HISTORY_FILE "
                   "or ~/.ibex_history.");

    CLI11_PARSE(app, argc, argv);

    if (verbose) {
        spdlog::set_level(spdlog::level::debug);
    } else {
        spdlog::set_level(spdlog::level::info);
    }

    // Resolve plugin search path: --plugin-path flag takes precedence,
    // then fall back to IBEX_LIBRARY_PATH environment variable.
    if (plugin_path.empty()) {
        const char* env = std::getenv("IBEX_LIBRARY_PATH");
        if (env != nullptr) {
            plugin_path = env;
        }
    }

    ibex::runtime::ExternRegistry registry;

    ibex::repl::ReplConfig config;
    config.verbose = verbose;
    config.persistent_history = !no_history;
    config.history_path = history_file;
    if (!plugin_path.empty()) {
        config.plugin_search_paths.push_back(plugin_path);
    }
    if (!import_path.empty()) {
        config.import_search_paths.push_back(import_path);
    }

    // File argument(s): run each script and exit. Otherwise start the REPL.
    if (!scripts.empty()) {
        int exit_code = 0;
        for (const auto& path : scripts) {
            if (!ibex::repl::run_file(path, config, registry)) {
                exit_code = 1;
            }
        }
        return exit_code;
    }

    ibex::repl::run(config, registry);

    return 0;
}
