#include <ibex/codegen/emitter.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <CLI/CLI.hpp>

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

int main(int argc, char* argv[]) {
    CLI::App app{"ibex compiler — transpile .ibex source to C++23"};
    app.set_version_flag("--version", "ibex_compile 0.1.0");

    std::string input_path;
    std::string output_path;
    bool no_print = false;
    bool bench = false;
    int bench_warmup = 3;
    int bench_iters = 10;

    app.add_option("input", input_path, "Input .ibex source file")->required();
    app.add_option("-o,--output", output_path, "Output .cpp file (default: stdout)");
    app.add_flag("--no-print", no_print, "Disable ibex::ops::print() in generated code");
    app.add_flag("--bench", bench,
                 "Emit a benchmark harness: data loaded once, query timed internally");
    app.add_option("--bench-warmup", bench_warmup, "Warmup iterations (default: 3)")
        ->needs("--bench");
    app.add_option("--bench-iters", bench_iters, "Timed iterations (default: 10)")
        ->needs("--bench");

    CLI11_PARSE(app, argc, argv);

    // Read source
    std::ifstream in_file(input_path);
    if (!in_file) {
        std::cerr << "ibex_compile: cannot open '" << input_path << "'\n";
        return 1;
    }
    std::string source(std::istreambuf_iterator<char>{in_file}, {});

    // Parse
    auto program = ibex::parser::parse(source);
    if (!program) {
        std::cerr << "ibex_compile: parse error at " << input_path << ":" << program.error().line
                  << ":" << program.error().column << ": " << program.error().message << "\n";
        return 1;
    }

    // Lower to IR
    auto ir = ibex::parser::lower(*program);
    if (!ir) {
        std::cerr << "ibex_compile: " << ir.error().message << "\n";
        return 1;
    }

    // Collect extern headers from the program
    ibex::codegen::Emitter::Config config;
    config.source_name = input_path;
    config.print_result = !no_print && !bench;
    config.bench_mode = bench;
    config.bench_warmup = bench_warmup;
    config.bench_iters = bench_iters;
    for (const auto& stmt : program->statements) {
        if (const auto* ext = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (!ext->source_path.empty()) {
                config.extern_headers.push_back(ext->source_path);
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
