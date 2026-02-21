#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_functions.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <CLI/CLI.hpp>
#include <spdlog/spdlog.h>

auto main(int argc, char** argv) -> int {
    CLI::App app{"Ibex — interactive columnar DSL"};

    bool verbose = false;
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");

    CLI11_PARSE(app, argc, argv);

    if (verbose) {
        spdlog::set_level(spdlog::level::debug);
    } else {
        spdlog::set_level(spdlog::level::info);
    }

    ibex::runtime::ExternRegistry registry;
    ibex::runtime::register_read_csv(registry);

    ibex::repl::ReplConfig config{
        .verbose = verbose,
    };

    ibex::repl::run(config, registry);

    return 0;
}
