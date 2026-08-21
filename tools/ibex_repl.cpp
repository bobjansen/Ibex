// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#if defined(IBEX_HAS_UI)
#include <ibex/ui/server.hpp>
#endif

#if defined(IBEX_HAS_PARQUET_BACKEND)
#include <ibex/parquet/backend.hpp>
#endif

#include <CLI/CLI.hpp>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "exe_path.hpp"

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
    bool report_planner = false;
    app.add_flag("--report-planner", report_planner,
                 "Print which planner path each script took (whole-script vs statements).");
    app.add_flag("--no-history", no_history, "Disable persistent readline history");
    app.add_option("--plugin-path", plugin_path,
                   "Directory to search for plugin shared libraries (*.so). "
                   "Defaults to IBEX_LIBRARY_PATH environment variable, then to "
                   "the directory containing this executable.");
    app.add_option("--import-path", import_path,
                   "Directory to search for library stub files (*.ibex) used by "
                   "`import` declarations.  Defaults to the plugin search path.");
    app.add_option("--history-file", history_file,
                   "Read/write REPL history at this path. Defaults to IBEX_HISTORY_FILE "
                   "or ~/.ibex_history.");
#if defined(IBEX_HAS_UI)
    std::uint16_t ui_port = 8765;
    std::string ui_web_root;
    std::string ui_data_dir;
    auto* ui = app.add_subcommand("ui", "Start the local browser-based query workbench");
    ui->add_option("--port", ui_port, "Loopback port to listen on")->default_val(8765);
    ui->add_option("--web-root", ui_web_root,
                   "Directory containing the bundled UI assets (defaults beside the executable)");
    ui->add_option(
          "--data-dir", ui_data_dir,
          "Directory the browser workbench may read and write (defaults to the current directory)")
        ->type_name("DIR");
#endif

    CLI11_PARSE(app, argc, argv);

    // Resolve plugin search path: --plugin-path flag takes precedence, then
    // IBEX_LIBRARY_PATH, then the directory holding this executable (where
    // plugins are built/packaged by default).
    if (plugin_path.empty()) {
        const char* env = std::getenv("IBEX_LIBRARY_PATH");
        if (env != nullptr) {
            plugin_path = env;
        }
    }
    if (plugin_path.empty()) {
        std::error_code ec;
        auto exe_dir = ibex::tools::executable_directory();
        if (!exe_dir.empty() && std::filesystem::exists(exe_dir, ec)) {
            plugin_path = exe_dir.string();
        }
    }

    ibex::runtime::ExternRegistry registry;
#if defined(IBEX_HAS_PARQUET_BACKEND)
    ibex::parquet::register_backend(registry);
#endif

    ibex::repl::ReplConfig config;
    config.verbose = verbose;
    config.report_planner = report_planner;
    config.persistent_history = !no_history;
    config.history_path = history_file;
    if (!plugin_path.empty()) {
        config.plugin_search_paths.push_back(plugin_path);
    }
    if (!import_path.empty()) {
        config.import_search_paths.push_back(import_path);
    }

#if defined(IBEX_HAS_UI)
    if (*ui) {
        std::filesystem::path web_root = ui_web_root;
        if (web_root.empty()) {
            web_root = ibex::tools::executable_directory() / "ui";
        }
        return ibex::ui::run_server(ibex::ui::ServerConfig{.port = ui_port,
                                                           .web_root = std::move(web_root),
                                                           .data_directory = std::move(ui_data_dir),
                                                           .repl = config},
                                    registry);
    }
#endif

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
