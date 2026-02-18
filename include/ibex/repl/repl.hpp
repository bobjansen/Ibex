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

}  // namespace ibex::repl
