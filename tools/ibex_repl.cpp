#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

#include <cstdlib>
#include <string>

auto main(int argc, char** argv) -> int {
    CLI::App app{"Ibex — interactive columnar DSL"};

    bool verbose = false;
    std::string plugin_path;
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");
    app.add_option("--plugin-path", plugin_path,
                   "Directory to search for plugin shared libraries (*.so). "
                   "Defaults to IBEX_LIBRARY_PATH environment variable.");

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
    if (!plugin_path.empty()) {
        config.plugin_search_paths.push_back(plugin_path);
    }

    ibex::repl::run(config, registry);

    return 0;
}
