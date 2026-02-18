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

        auto input = line;
        auto last_non_space = input.find_last_not_of(" \t\r\n");
        if (last_non_space != std::string::npos && input[last_non_space] != ';') {
            input.push_back(';');
        }

        // Attempt to parse
        auto result = parser::parse(input);
        if (!result) {
            fmt::print("error: {}\n", result.error().format());
            continue;
        }

        // TODO: Execute the program against the runtime.
        fmt::print("parsed OK (statements={})\n", result.value().statements.size());
    }

    spdlog::info("Ibex REPL exiting");
}

}  // namespace ibex::repl
