#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <string>
#include <vector>

namespace ibex::repl {

/// Configuration for the REPL session.
struct ReplConfig {
    bool verbose = false;
    std::string prompt = "ibex> ";
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

/// Normalize a single REPL input line (e.g., inject implicit semicolon).
[[nodiscard]] auto normalize_input(std::string_view input) -> std::string;

}  // namespace ibex::repl
