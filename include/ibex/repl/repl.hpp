#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <cstddef>
#include <string>
#include <vector>

namespace ibex::repl {

/// Configuration for the REPL session.
struct ReplConfig {
    bool verbose = false;
    /// Print `planner: whole-script` / `planner: statements (<reason>)` to
    /// stderr per script. Deliberately NOT folded into `verbose`, which also
    /// raises the spdlog level to debug: the benchmark harness reads this line
    /// to record which engine path it measured, and must not pay for logging
    /// someone later adds to a hot path.
    bool report_planner = false;
    std::string prompt = "ibex> ";
    bool persistent_history = true;
    /// History file used when readline is available. Empty means
    /// `$IBEX_HISTORY_FILE`, then `$HOME/.ibex_history` where possible.
    std::string history_path;
    /// Maximum number of entries kept in the persistent history file.
    std::size_t history_limit = 10000;
    /// Directories searched (in order) for plugin shared libraries (*.so).
    /// When a script declares `extern fn foo(...) from "bar.hpp"`, the REPL
    /// looks for `bar.so` in each of these directories and loads it via dlopen.
    std::vector<std::string> plugin_search_paths;
    /// Directories searched (in order) for library stub files (<name>.ibex).
    /// Used by `import "name";` declarations.  When empty, the plugin_search_paths
    /// are used as a fallback so that plugins and their accompanying .ibex stubs
    /// can live in the same directory.
    std::vector<std::string> import_search_paths;
};

/// Run the interactive REPL loop.
///
/// Reads lines from stdin, parses and evaluates them.
void run(const ReplConfig& config, runtime::ExternRegistry& registry);

/// Execute a script in a fresh REPL context (useful for tests).
[[nodiscard]] auto execute_script(std::string_view source, runtime::ExternRegistry& registry)
    -> bool;

/// Execute a script with the same plugin / import search paths the
/// interactive REPL would use. Lets non-interactive callers (`ibex_eval`)
/// run scripts that declare `extern fn ... from "csv.hpp"` etc., and use
/// the REPL's full vocabulary including model accessors.
[[nodiscard]] auto execute_script(std::string_view source, runtime::ExternRegistry& registry,
                                  const ReplConfig& config) -> bool;

/// Execute an Ibex script file by path. The whole file is parsed at once, so
/// statements may span multiple physical lines (e.g. a multi-line `model { ... }`
/// clause). Honors the config's plugin / import search paths. Returns false if
/// the file cannot be read or a statement fails.
[[nodiscard]] auto run_file(const std::string& path, const ReplConfig& config,
                            runtime::ExternRegistry& registry) -> bool;

/// Normalize a single REPL input line (e.g., inject implicit semicolon).
[[nodiscard]] auto normalize_input(std::string_view input) -> std::string;

}  // namespace ibex::repl
