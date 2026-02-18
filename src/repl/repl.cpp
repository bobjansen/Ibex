#include <ibex/repl/repl.hpp>

#include <ibex/parser/parser.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string>

namespace ibex::repl {

void run(const ReplConfig& config, runtime::ExternRegistry& /*registry*/) {
    spdlog::info("Ibex REPL started (verbose={})", config.verbose);

    std::string line;
    while (true) {
        fmt::print("{}", config.prompt);
        if (!std::getline(std::cin, line)) {
            fmt::print("\n");
            break;
        }

        if (line.empty()) {
            continue;
        }

        if (line == "quit" || line == "exit") {
            break;
        }

        // Attempt to parse
        auto result = parser::parse(line);
        if (!result) {
            fmt::print("error: {}\n", result.error().format());
            continue;
        }

        // TODO: Execute the IR plan against the runtime.
        fmt::print("parsed OK (node id={})\n", result.value()->id());
    }

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
