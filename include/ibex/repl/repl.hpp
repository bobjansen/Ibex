#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <string>

namespace ibex::repl {

/// Configuration for the REPL session.
struct ReplConfig {
    bool verbose = false;
    std::string prompt = "ibex> ";
};

/// Run the interactive REPL loop.
///
/// Reads lines from stdin, parses and evaluates them.
/// TODO: Implement evaluation pipeline (parse -> IR -> execute).
void run(const ReplConfig& config, runtime::ExternRegistry& registry);

/// Execute a script in a fresh REPL context (useful for tests).
[[nodiscard]] auto execute_script(std::string_view source, runtime::ExternRegistry& registry)
    -> bool;

/// Normalize a single REPL input line (e.g., inject implicit semicolon).
[[nodiscard]] auto normalize_input(std::string_view input) -> std::string;

}  // namespace ibex::repl
