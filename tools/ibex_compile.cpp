// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/codegen/emitter.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/parser/scalar_bindings.hpp>

#include <CLI/CLI.hpp>

#include <algorithm>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>

#include "import_resolver.hpp"

namespace {

/// The first function taking or returning a resource type that `program`
/// calls, if any.
auto first_resource_call(const ibex::parser::Program& program) -> std::optional<std::string> {
    std::set<std::string, std::less<>> resource_functions;
    for (const auto& stmt : program.statements) {
        const auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt);
        if (decl == nullptr) {
            continue;
        }
        const auto is_resource = [](const ibex::parser::Type& type) {
            return type.kind == ibex::parser::Type::Kind::Resource;
        };
        if (is_resource(decl->return_type) ||
            std::ranges::any_of(decl->params, [&](const ibex::parser::Param& param) {
                return is_resource(param.type);
            })) {
            resource_functions.insert(decl->name);
        }
    }
    std::optional<std::string> found;
    const auto check = [&](const ibex::parser::Expr& expr) {
        (void)ibex::parser::contains_call_if(expr, [&](std::string_view callee) {
            if (resource_functions.contains(callee)) {
                found = std::string(callee);
                return true;
            }
            return false;
        });
    };
    for (const auto& stmt : program.statements) {
        if (const auto* let = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            check(*let->value);
        } else if (const auto* tuple = std::get_if<ibex::parser::TupleLetStmt>(&stmt)) {
            check(*tuple->value);
        } else if (const auto* expr = std::get_if<ibex::parser::ExprStmt>(&stmt)) {
            check(*expr->expr);
        } else if (const auto* fn = std::get_if<ibex::parser::FunctionDecl>(&stmt)) {
            for (const auto& body : fn->body) {
                std::visit(
                    [&](const auto& s) {
                        using T = std::decay_t<decltype(s)>;
                        if constexpr (std::is_same_v<T, ibex::parser::ExprStmt>) {
                            check(*s.expr);
                        } else {
                            check(*s.value);
                        }
                    },
                    body);
            }
        }
        if (found) {
            return found;
        }
    }
    return std::nullopt;
}

}  // namespace

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

    // Resources (`extern type`) live on the REPL's statement path only; the
    // generated C++ has no runtime for them yet.
    if (auto callee = first_resource_call(*scalar_program)) {
        std::cerr << "ibex_compile: " << *callee
                  << " takes or returns a resource (extern type), which compiled programs do not "
                     "support yet; run the script with the ibex tool instead\n";
        return 1;
    }

    auto scalar_bindings = ibex::parser::collect_scalar_binding_set(*scalar_program);
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
    config.scalar_bindings = std::move(scalar_bindings->compile_time);
    config.deferred_scalar_bindings = std::move(scalar_bindings->deferred);
    {
        robin_hood::unordered_set<std::string> seen_headers;
        for (const auto& stmt : program->statements) {
            if (const auto* ext = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
                // `parse_args` reads the process argv (via IBEX_ARGS); the
                // generated `main` must forward its own argv, the compiled
                // equivalent of `ibex script.ibex -- <args>`.
                if (ext->name == "parse_args") {
                    config.forward_cli_args = true;
                }
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
