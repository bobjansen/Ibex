#include <ibex/codegen/emitter.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>

#include <CLI/CLI.hpp>

#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "import_resolver.hpp"

namespace {

using ScalarValue = ibex::codegen::Emitter::Config::ScalarValue;

auto eval_scalar_expr(const ibex::parser::Expr& expr,
                      const std::unordered_map<std::string, ScalarValue>& env)
    -> std::expected<ScalarValue, std::string> {
    using namespace ibex::parser;

    if (const auto* ident = std::get_if<IdentifierExpr>(&expr.node)) {
        if (auto it = env.find(ident->name); it != env.end()) {
            return it->second;
        }
        return std::unexpected("unknown scalar binding: " + ident->name);
    }

    if (const auto* lit = std::get_if<LiteralExpr>(&expr.node)) {
        if (const auto* v = std::get_if<std::int64_t>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<double>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<std::string>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Date>(&lit->value))
            return ScalarValue{*v};
        if (const auto* v = std::get_if<ibex::Timestamp>(&lit->value))
            return ScalarValue{*v};
        return std::unexpected("unsupported scalar literal");
    }

    if (const auto* group = std::get_if<GroupExpr>(&expr.node)) {
        return eval_scalar_expr(*group->expr, env);
    }

    if (const auto* unary = std::get_if<UnaryExpr>(&expr.node)) {
        if (unary->op != UnaryOp::Negate) {
            return std::unexpected("unsupported unary scalar operator");
        }
        auto value = eval_scalar_expr(*unary->expr, env);
        if (!value) {
            return std::unexpected(value.error());
        }
        if (const auto* v = std::get_if<std::int64_t>(&*value))
            return ScalarValue{-(*v)};
        if (const auto* v = std::get_if<double>(&*value))
            return ScalarValue{-(*v)};
        return std::unexpected("unary negate requires numeric scalar");
    }

    if (const auto* binary = std::get_if<BinaryExpr>(&expr.node)) {
        auto left = eval_scalar_expr(*binary->left, env);
        if (!left) {
            return std::unexpected(left.error());
        }
        auto right = eval_scalar_expr(*binary->right, env);
        if (!right) {
            return std::unexpected(right.error());
        }

        const bool left_double = std::holds_alternative<double>(*left);
        const bool right_double = std::holds_alternative<double>(*right);
        if (left_double || right_double) {
            double lhs = left_double ? std::get<double>(*left)
                                     : static_cast<double>(std::get<std::int64_t>(*left));
            double rhs = right_double ? std::get<double>(*right)
                                      : static_cast<double>(std::get<std::int64_t>(*right));
            switch (binary->op) {
                case BinaryOp::Add:
                    return ScalarValue{lhs + rhs};
                case BinaryOp::Sub:
                    return ScalarValue{lhs - rhs};
                case BinaryOp::Mul:
                    return ScalarValue{lhs * rhs};
                case BinaryOp::Div:
                    return ScalarValue{lhs / rhs};
                case BinaryOp::Mod:
                    return std::unexpected("mod not supported for float scalars");
                default:
                    return std::unexpected("unsupported scalar operator");
            }
        }

        if (!std::holds_alternative<std::int64_t>(*left) ||
            !std::holds_alternative<std::int64_t>(*right)) {
            return std::unexpected("binary scalar op requires numeric operands");
        }
        const auto lhs = std::get<std::int64_t>(*left);
        const auto rhs = std::get<std::int64_t>(*right);
        switch (binary->op) {
            case BinaryOp::Add:
                return ScalarValue{lhs + rhs};
            case BinaryOp::Sub:
                return ScalarValue{lhs - rhs};
            case BinaryOp::Mul:
                return ScalarValue{lhs * rhs};
            case BinaryOp::Div:
                if (rhs == 0) {
                    return std::unexpected("division by zero in scalar let");
                }
                return ScalarValue{lhs / rhs};
            case BinaryOp::Mod:
                if (rhs == 0) {
                    return std::unexpected("modulo by zero in scalar let");
                }
                return ScalarValue{lhs % rhs};
            default:
                return std::unexpected("unsupported scalar operator");
        }
    }

    return std::unexpected("unsupported scalar expression");
}

auto collect_scalar_bindings(const ibex::parser::Program& program)
    -> std::expected<std::vector<std::pair<std::string, ScalarValue>>, std::string> {
    std::vector<std::pair<std::string, ScalarValue>> ordered;
    std::unordered_map<std::string, ScalarValue> env;

    ibex::parser::LowerContext lower_ctx;

    for (const auto& stmt : program.statements) {
        if (const auto* ext = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (ext->return_type.kind == ibex::parser::Type::Kind::DataFrame ||
                ext->return_type.kind == ibex::parser::Type::Kind::TimeFrame) {
                lower_ctx.table_externs.insert(ext->name);
            }
            if (!ext->params.empty() &&
                ext->params[0].type.kind == ibex::parser::Type::Kind::DataFrame) {
                lower_ctx.sink_externs.insert(ext->name);
            }
            continue;
        }

        const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt);
        if (let_stmt == nullptr) {
            continue;
        }

        // Stream lets are never scalar; let the full lowering phase validate them.
        if (std::holds_alternative<ibex::parser::StreamExpr>(let_stmt->value->node)) {
            continue;
        }

        auto table_result = ibex::parser::lower_expr(*let_stmt->value, lower_ctx);
        if (table_result) {
            lower_ctx.bindings[let_stmt->name] = std::move(table_result.value());
            continue;
        }

        auto scalar_result = eval_scalar_expr(*let_stmt->value, env);
        if (!scalar_result) {
            return std::unexpected("unsupported scalar let '" + let_stmt->name +
                                   "': " + scalar_result.error());
        }
        env[let_stmt->name] = scalar_result.value();
        ordered.emplace_back(let_stmt->name, scalar_result.value());
    }

    return ordered;
}

}  // namespace

int main(int argc, char* argv[]) {
    CLI::App app{"ibex compiler — transpile .ibex source to C++23"};
    app.set_version_flag("--version", "ibex_compile 0.1.0");

    std::string input_path;
    std::string output_path;
    bool no_print = false;
    bool bench = false;
    int bench_warmup = 3;
    int bench_iters = 10;
    std::vector<std::string> import_paths;

    app.add_option("input", input_path, "Input .ibex source file")->required();
    app.add_option("-o,--output", output_path, "Output .cpp file (default: stdout)");
    app.add_flag("--no-print", no_print, "Disable ibex::ops::print() in generated code");
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

    auto scalar_bindings = collect_scalar_bindings(*scalar_program);
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
    config.bench_mode = bench;
    config.bench_warmup = bench_warmup;
    config.bench_iters = bench_iters;
    config.scalar_bindings = std::move(*scalar_bindings);
    {
        std::unordered_set<std::string> seen_headers;
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
