// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/codegen/emitter.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/parser/resource_functions.hpp>
#include <ibex/parser/scalar_bindings.hpp>

#include <CLI/CLI.hpp>

#include <algorithm>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "import_resolver.hpp"

namespace {

/// What stops a program with resources from compiling, if anything: a resource
/// call where nothing would run it (inside a query clause). The same placement
/// rule the REPL applies, before any call runs. (A `fn` that uses a resource has
/// its body checked when it is lowered.)
auto resource_problem(const ibex::parser::Program& program) -> std::optional<std::string> {
    std::map<std::string, const ibex::parser::ExternDecl*, std::less<>> externs;
    std::map<std::string, const ibex::parser::FunctionDecl*, std::less<>> functions;
    for (const auto& stmt : program.statements) {
        if (const auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            externs.insert_or_assign(decl->name, decl);
        } else if (const auto* fn = std::get_if<ibex::parser::FunctionDecl>(&stmt)) {
            functions.insert_or_assign(fn->name, fn);
        }
    }
    const ibex::parser::ResourceFunctions resource_functions(
        [&](std::string_view name) -> const ibex::parser::ExternDecl* {
            auto it = externs.find(name);
            return it == externs.end() ? nullptr : it->second;
        },
        [&](std::string_view name) -> const ibex::parser::FunctionDecl* {
            auto it = functions.find(name);
            return it == functions.end() ? nullptr : it->second;
        });
    for (const auto& stmt : program.statements) {
        const ibex::parser::Expr* value = nullptr;
        if (const auto* let = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            value = let->value.get();
        } else if (const auto* tuple = std::get_if<ibex::parser::TupleLetStmt>(&stmt)) {
            value = tuple->value.get();
        } else if (const auto* expr = std::get_if<ibex::parser::ExprStmt>(&stmt)) {
            value = expr->expr.get();
        }
        if (value == nullptr) {
            continue;
        }
        if (std::holds_alternative<ibex::parser::TupleLetStmt>(stmt) &&
            resource_functions.first_call(*value).has_value()) {
            return "a resource cannot be destructured";
        }
        if (auto misplaced = resource_functions.first_misplaced(*value)) {
            return *misplaced + " " + std::string(ibex::parser::ResourceFunctions::kPlacementError);
        }
    }
    return std::nullopt;
}

using EmitterScript = ibex::codegen::Emitter::Script;

/// The steps of a lowered script in the order of their statements. The plan keeps
/// preamble calls, shared bindings and sinks in separate lists, each with its
/// statement index, so merge them back by that index (each statement has three
/// slots: the bindings its expression needs, the statement itself, and the release
/// of a name it rebinds). `scalars` carries the deferred `scalar(<table>)` lets,
/// which run at their own statement; a function body has none.
auto build_script(ibex::parser::ScriptPlan& plan, ibex::parser::ScalarBindingSet* scalars)
    -> EmitterScript {
    using Step = EmitterScript::Step;
    std::vector<std::pair<std::size_t, Step>> ordered;
    for (std::size_t i = 0; i < plan.preamble.size(); ++i) {
        Step step;
        step.kind = Step::Kind::Call;
        step.plan = plan.preamble[i].get();
        bool hoisted = false;
        if (i < plan.preamble_binds.size() && plan.preamble_binds[i].has_value()) {
            step.bind = plan.preamble_binds[i]->name;
            step.bind_resource = plan.preamble_binds[i]->resource;
            hoisted = plan.preamble_binds[i]->hoisted;
        }
        // A call nested in another's argument runs before the statement's own
        // bindings, which may need it.
        ordered.emplace_back(plan.preamble_positions.at(i) * 4 + (hoisted ? 0U : 1U),
                             std::move(step));
    }
    for (const auto& shared : plan.shared_bindings) {
        Step step;
        step.kind = Step::Kind::SharedBinding;
        step.name = shared.name;
        step.plan = shared.plan.get();
        ordered.emplace_back(shared.position * 4, std::move(step));
    }
    for (auto& sink : plan.sinks) {
        Step step;
        step.kind = Step::Kind::Sink;
        step.callee = sink.callee;
        step.plan = sink.input.get();
        step.args = std::move(sink.args);
        step.input_binding = sink.input_binding;
        step.bind = sink.bind;
        ordered.emplace_back(sink.position * 4 + 1, std::move(step));
    }
    for (const auto& resource : plan.resource_steps) {
        Step step;
        step.kind = resource.kind == ibex::parser::ResourceStep::Kind::Alias
                        ? Step::Kind::ResourceAlias
                        : Step::Kind::ResourceUnbind;
        step.name = resource.name;
        step.alias_of = resource.source;
        // A release comes after the statement that rebinds the name: its value
        // may read the old connection. Everything else orders by statement.
        ordered.emplace_back(
            resource.position * 4 +
                (resource.kind == ibex::parser::ResourceStep::Kind::Unbind ? 2U : 1U),
            std::move(step));
    }
    if (scalars != nullptr) {
        for (std::size_t i = 0; i < scalars->deferred.size(); ++i) {
            Step step;
            step.kind = Step::Kind::DeferredScalar;
            step.deferred = &scalars->deferred[i];
            ordered.emplace_back(scalars->deferred_positions.at(i) * 4 + 1, std::move(step));
        }
    }
    std::ranges::stable_sort(ordered, {}, &std::pair<std::size_t, Step>::first);
    EmitterScript script;
    script.steps.reserve(ordered.size());
    for (auto& entry : ordered) {
        script.steps.push_back(std::move(entry.second));
    }
    script.result = plan.result.get();
    script.result_binding = plan.result_binding;
    script.return_call = plan.return_call.get();
    script.return_expr = plan.return_expr.has_value() ? &*plan.return_expr : nullptr;
    return script;
}

/// The C++ type a parameter or return value of this language type has in a
/// generated function.
auto cpp_type_of(const ibex::parser::Type& type) -> std::expected<std::string, std::string> {
    using Kind = ibex::parser::Type::Kind;
    using ibex::parser::ScalarType;
    switch (type.kind) {
        case Kind::Resource:
            return type.resource;
        case Kind::DataFrame:
        case Kind::TimeFrame:
            return std::string("ibex::runtime::Table");
        case Kind::Series:
            return std::unexpected("a column parameter or result is not supported yet");
        case Kind::Scalar:
            break;
    }
    const auto* scalar = std::get_if<ScalarType>(&type.arg);
    if (scalar == nullptr) {
        return std::unexpected("this parameter or result type is not supported yet");
    }
    switch (*scalar) {
        case ScalarType::Int32:
        case ScalarType::Int64:
            return std::string("std::int64_t");
        case ScalarType::Float32:
        case ScalarType::Float64:
            return std::string("double");
        case ScalarType::Bool:
            return std::string("bool");
        case ScalarType::String:
            return std::string("std::string");
        case ScalarType::Date:
            return std::string("ibex::Date");
        case ScalarType::Timestamp:
            return std::string("ibex::Timestamp");
        case ScalarType::Decimal:
            break;
    }
    return std::unexpected("a Decimal parameter or result is not supported yet");
}

/// A lowered resource `fn` as a function of the generated program.
auto build_function(ibex::parser::FunctionPlan& plan)
    -> std::expected<std::unique_ptr<EmitterScript::Function>, std::string> {
    using Param = EmitterScript::Function::Param;
    auto fn = std::make_unique<EmitterScript::Function>();
    fn->name = plan.decl->name;
    for (const auto& param : plan.decl->params) {
        auto type = cpp_type_of(param.type);
        if (!type) {
            return std::unexpected("function '" + fn->name + "', parameter '" + param.name +
                                   "': " + type.error());
        }
        Param out;
        out.name = param.name;
        out.cpp_type = *type;
        out.kind = param.type.kind == ibex::parser::Type::Kind::Resource ? Param::Kind::Resource
                   : param.type.kind == ibex::parser::Type::Kind::Scalar ? Param::Kind::Scalar
                                                                         : Param::Kind::Table;
        fn->params.push_back(std::move(out));
    }
    auto returns = cpp_type_of(plan.decl->return_type);
    if (!returns) {
        return std::unexpected("function '" + fn->name + "': the result: " + returns.error());
    }
    fn->return_type = *returns;
    fn->body = build_script(plan.body, nullptr);
    return fn;
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

    if (auto problem = resource_problem(*scalar_program)) {
        std::cerr << "ibex_compile: " << *problem << "\n";
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

    // Lower to IR. A script whose statements have effects (a table sink such as
    // `write_csv(df, path);`, or `let n = f(...);` binding an extern's result)
    // cannot be one query plus constants: it is lowered as a whole script and
    // emitted in statement order. Everything else keeps the single-plan path.
    std::optional<ibex::parser::ScriptPlan> script_plan;
    if (auto scripted = ibex::parser::lower_script(*program);
        scripted.has_value() &&
        (!scripted->sinks.empty() ||
         std::ranges::any_of(scripted->preamble_binds,
                             [](const auto& bind) { return bind.has_value(); }) ||
         std::ranges::any_of(scripted->shared_bindings, [](const auto& binding) {
             return binding.plan != nullptr && binding.plan->kind() == ibex::ir::NodeKind::Model;
         }))) {
        script_plan = std::move(*scripted);
    }
    ibex::parser::LowerResult ir = ibex::ir::NodePtr{};
    if (!script_plan.has_value()) {
        ir = ibex::parser::lower(*program);
        if (!ir) {
            std::cerr << "ibex_compile: " << ir.error().message << "\n";
            return 1;
        }
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
    if (script_plan.has_value()) {
        // The script orders its deferred scalars itself, among its other steps.
        for (const auto& binding : scalar_bindings->deferred) {
            config.runtime_scalar_names.push_back(binding.name);
        }
    } else {
        config.deferred_scalar_bindings = std::move(scalar_bindings->deferred);
    }
    for (const auto& name : scalar_bindings->extern_calls) {
        config.runtime_scalar_names.push_back(name);
    }
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
    std::ofstream out_file;
    if (!output_path.empty()) {
        out_file.open(output_path);
        if (!out_file) {
            std::cerr << "ibex_compile: cannot write to '" << output_path << "'\n";
            return 1;
        }
    }
    std::ostream& out = output_path.empty() ? std::cout : out_file;
    if (!script_plan.has_value()) {
        emitter.emit(out, **ir, config);
        return 0;
    }

    auto script = build_script(*script_plan, &*scalar_bindings);
    for (auto& fn : script_plan->functions) {
        auto built = build_function(*fn);
        if (!built) {
            std::cerr << "ibex_compile: " << built.error() << "\n";
            return 1;
        }
        script.functions.push_back(std::move(*built));
    }
    emitter.emit(out, script, config);
    return 0;
}
