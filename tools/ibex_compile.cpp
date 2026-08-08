// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/codegen/emitter.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/parser/scalar_bindings.hpp>

#include <CLI/CLI.hpp>

#include <expected>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <robin_hood.h>
#include <string>
#include <variant>

#include "import_resolver.hpp"

int main(int argc, char* argv[]) {
    CLI::App app{"ibex compiler — transpile .ibex source to C++23"};
    app.set_version_flag("--version", "ibex_compile 0.1.0");

    std::string input_path;
    std::string output_path;
    bool no_print = false;
    bool table_entry_point = false;
    bool bench = false;
    int bench_warmup = 3;
    int bench_iters = 10;
    std::vector<std::string> import_paths;

    app.add_option("input", input_path, "Input .ibex source file")->required();
    app.add_option("-o,--output", output_path, "Output .cpp file (default: stdout)");
    app.add_flag("--no-print", no_print, "Disable ibex::ops::print() in generated code");
    app.add_flag("--table-entry-point", table_entry_point,
                 "Emit ibex_generated_execute() returning Table instead of main()");
    app.add_flag("--bench", bench,
                 "Emit a benchmark harness: data loaded once, query timed internally");
    app.add_option("--bench-warmup", bench_warmup, "Warmup iterations (default: 3)")
        ->needs("--bench");
    app.add_option("--bench-iters", bench_iters, "Timed iterations (default: 10)")
        ->needs("--bench");
    app.add_option("--import-path", import_paths,
                   "Directory to search for library stub files (*.ibex) used by imports. "
                   "Can be passed multiple times.");

    CLI11_PARSE(app, argc, argv);

    // Read source
    std::ifstream in_file(input_path);
    if (!in_file) {
        std::cerr << "ibex_compile: cannot open '" << input_path << "'\n";
        return 1;
    }
    std::string source(std::istreambuf_iterator<char>{in_file}, {});

    const auto parse_and_expand =
        [&](const std::string& src) -> std::expected<ibex::parser::Program, std::string> {
        auto parsed = ibex::parser::parse(src);
        if (!parsed) {
            return std::unexpected(
                "parse error at " + input_path + ":" + std::to_string(parsed.error().line) + ":" +
                std::to_string(parsed.error().column) + ": " + parsed.error().message);
        }
        auto expanded = ibex::tools::expand_imports(std::move(*parsed), input_path, import_paths);
        if (!expanded) {
            return std::unexpected(expanded.error());
        }
        return expanded;
    };

    auto scalar_program = parse_and_expand(source);
    if (!scalar_program) {
        std::cerr << "ibex_compile: " << scalar_program.error() << "\n";
        return 1;
    }

    auto scalar_bindings = ibex::parser::collect_scalar_bindings(*scalar_program);
    if (!scalar_bindings) {
        std::cerr << "ibex_compile: " << scalar_bindings.error() << "\n";
        return 1;
    }

    auto program = parse_and_expand(source);
    if (!program) {
        std::cerr << "ibex_compile: " << program.error() << "\n";
        return 1;
    }

    // Lower to IR
    auto ir = ibex::parser::lower(*program);
    if (!ir) {
        std::cerr << "ibex_compile: " << ir.error().message << "\n";
        return 1;
    }

    // Collect extern headers from the program (deduplicated)
    ibex::codegen::Emitter::Config config;
    config.source_name = input_path;
    config.print_result = !no_print && !bench;
    config.table_entry_point = table_entry_point;
    config.bench_mode = bench;
    config.bench_warmup = bench_warmup;
    config.bench_iters = bench_iters;
    config.scalar_bindings = std::move(*scalar_bindings);
    {
        robin_hood::unordered_set<std::string> seen_headers;
        for (const auto& stmt : program->statements) {
            if (const auto* ext = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
                if (!ext->source_path.empty()) {
                    std::string header = ext->source_path;
                    if (!std::filesystem::path(header).has_extension()) {
                        header += ".hpp";
                    }
                    if (seen_headers.insert(header).second) {
                        config.extern_headers.push_back(std::move(header));
                    }
                }
            }
        }
    }

    // Emit
    ibex::codegen::Emitter emitter;
    if (output_path.empty()) {
        emitter.emit(std::cout, **ir, config);
    } else {
        std::ofstream out_file(output_path);
        if (!out_file) {
            std::cerr << "ibex_compile: cannot write to '" << output_path << "'\n";
            return 1;
        }
        emitter.emit(out_file, **ir, config);
    }

    return 0;
}
