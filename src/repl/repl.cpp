#include <ibex/repl/repl.hpp>

#include <ibex/parser/parser.hpp>

#include <fmt/core.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <string>

namespace ibex::repl {

auto normalize_input(std::string_view input) -> std::string {
    std::string normalized(input);
    auto last_non_space = normalized.find_last_not_of(" \t\r\n");
    if (last_non_space != std::string::npos && normalized[last_non_space] != ';') {
        normalized.push_back(';');
    }
    return normalized;
}

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
        auto result = parser::parse(normalize_input(line));
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
