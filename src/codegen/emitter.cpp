// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/codegen/emitter.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/model_accessors.hpp>
#include <ibex/ir/node.hpp>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::codegen {

namespace {

auto escape_string(const std::string& s) -> std::string {
    std::string out;
    out.reserve(s.size());
    for (const char c : s) {
        if (c == '"')
            out += "\\\"";
        else if (c == '\\')
            out += "\\\\";
        else if (c == '\n')
            out += "\\n";
        else if (c == '\r')
            out += "\\r";
        else if (c == '\t')
            out += "\\t";
        else
            out += c;
    }
    return out;
}

/// C++ that reconstructs `v` exactly: the digits travel as text, so no
/// int128 literal (which C++ lacks) and no double is involved.
auto decimal_value_cpp(const DecimalValue& v) -> std::string {
    return "ibex::decimal::make_value(\"" + decimal::to_string(v) + "\", " +
           std::to_string(v.type.precision) + ", " + std::to_string(v.type.scale) + ")";
}

auto format_double(double v) -> std::string {
    if (std::isnan(v))
        return "std::numeric_limits<double>::quiet_NaN()";
    if (std::isinf(v))
        return v > 0 ? "std::numeric_limits<double>::infinity()"
                     : "-std::numeric_limits<double>::infinity()";
    // Round-trip precision
    std::ostringstream ss;
    ss.precision(17);
    ss << v;
    std::string s = ss.str();
    // Ensure it looks like a double literal (has . or e)
    if (!s.contains('.') && !s.contains('e')) {
        s += ".0";
    }
    return s;
}

}  // namespace

// ─── Public ──────────────────────────────────────────────────────────────────

auto Emitter::indent_code(const std::string& code, size_t spaces) -> std::string {
    if (spaces <= 0 || code.empty())
        return code;
    const std::string prefix(spaces, ' ');
    std::string result;
    result.reserve(code.size() + (prefix.size() * 16));
    bool at_start = true;
    for (const char c : code) {
        if (at_start && c != '\n') {
            result += prefix;
            at_start = false;
        }
        result += c;
        if (c == '\n')
            at_start = true;
    }
    return result;
}

void Emitter::emit(std::ostream& out, const ir::Node& root, const Config& config) {
    emit_header(out, config);
    emit_query(out, root, config);
    emit_footer(out, config);
}

void Emitter::emit(std::ostream& out, const Script& script, const Config& config) {
    if (config.bench_mode) {
        throw std::runtime_error(
            "ibex_compile: a script with effects cannot be a benchmark harness");
    }
    emit_header(out, config, &script);

    named_tables_.clear();
    resource_vars_.clear();
    model_vars_.clear();
    const std::string result_var = emit_script_steps(script);
    if (result_var.empty()) {
        // The script ends in an effect (`adbc_close(db);`): there is no table to
        // print or return.
        if (config.table_entry_point) {
            out << "    return ibex::runtime::Table{};\n";
        }
    } else if (config.table_entry_point) {
        out << "    return " << result_var << ";\n";
    } else if (config.print_result) {
        out << "    ibex::ops::print(" << result_var << ");\n";
    }
    emit_footer(out, config);
}

auto Emitter::callee_name(const std::string& callee) const -> std::string {
    // `coef(m)` and `model_coef(m)` are the same call, of the ops layer.
    if (ir::is_model_table_accessor(callee) || ir::is_model_scalar_accessor(callee)) {
        return "ibex::ops::" + (callee.starts_with("model_") ? callee : "model_" + callee);
    }
    return user_functions_.contains(callee) ? "_ibex_fn_" + callee : callee;
}

void Emitter::emit_scalar_store(const std::string& name, const std::string& value) {
    if (in_function_) {
        *out_ << "    _ibex_scope.set(\"" << escape_string(name) << "\", " << value << ");\n";
    } else {
        *out_ << "    _ibex_scalars[\"" << escape_string(name) << "\"] = " << value << ";\n";
    }
}

auto Emitter::emit_script_steps(const Script& script) -> std::string {
    auto& out = *out_;
    // The table a sink consumed, by the binding that named it: the result of a
    // script ending `write(result, ...); result;` is that same table, and must
    // not be computed (or read from its source) a second time.
    robin_hood::unordered_map<std::string, std::string> sink_inputs;
    const auto emit_call_args = [&](const std::vector<ir::Expr>& args, bool leading_comma,
                                    std::string_view first) {
        std::string text{first};
        bool need_comma = leading_comma;
        for (const auto& arg : args) {
            if (need_comma) {
                text += ", ";
            }
            need_comma = true;
            text += emit_raw_expr(arg);
        }
        return text;
    };

    // Bind `name` to a new resource variable and let go of the one it held: the
    // new value may have been computed from the old (`let db = f(db)`), so the
    // old is released only after.
    const auto bind_resource_var = [&](const std::string& name, const std::string& value) {
        const std::string variable = "_res" + std::to_string(tmp_counter_++) + "_" + name;
        out << "    auto " << variable << " = " << value << ";\n";
        if (const auto old = resource_vars_.find(name); old != resource_vars_.end()) {
            out << "    " << old->second << " = {};\n";
        }
        resource_vars_.insert_or_assign(name, variable);
    };
    // `callee(args)` run for its effect. A bound call stores its result as a
    // scalar the later steps read through the registry, or as a resource
    // variable; an unbound one drops it.
    const auto emit_effect_call = [&](const std::optional<std::string>& bind, bool resource,
                                      const std::string& callee, const std::string& args) {
        const std::string call = callee_name(callee) + "(" + args + ")";
        if (bind.has_value() && resource) {
            bind_resource_var(*bind, call);
        } else if (bind.has_value()) {
            emit_scalar_store(*bind, "ibex::runtime::ScalarValue(" + call + ")");
        } else {
            out << "    (void)" << call << ";\n";
        }
    };

    for (const auto& step : script.steps) {
        switch (step.kind) {
            case Script::Step::Kind::SharedBinding: {
                if (step.plan == nullptr) {
                    throw std::runtime_error("ibex_compile: shared binding has no plan");
                }
                last_model_.clear();
                named_tables_[step.name] = emit_node(*step.plan);
                // A binding that fits a model is also that model, by the same name.
                if (step.plan->kind() == ir::NodeKind::Model) {
                    model_vars_[step.name] = last_model_;
                } else {
                    model_vars_.erase(step.name);
                }
                break;
            }
            case Script::Step::Kind::Sink: {
                std::string input;
                if (step.input_binding.has_value()) {
                    if (const auto it = sink_inputs.find(*step.input_binding);
                        it != sink_inputs.end()) {
                        input = it->second;
                    }
                }
                if (input.empty()) {
                    if (step.plan == nullptr) {
                        throw std::runtime_error("ibex_compile: sink has no input plan");
                    }
                    input = emit_node(*step.plan);
                    if (step.input_binding.has_value()) {
                        sink_inputs[*step.input_binding] = input;
                    }
                }
                emit_effect_call(step.bind, false, step.callee,
                                 emit_call_args(step.args, /*leading_comma=*/true, input));
                break;
            }
            case Script::Step::Kind::Call: {
                if (step.plan == nullptr || step.plan->kind() != ir::NodeKind::ExternCall) {
                    throw std::runtime_error("ibex_compile: a call step needs an ExternCall node");
                }
                const auto& call = ir::node_cast<ir::ExternCallNode>(*step.plan);
                emit_effect_call(step.bind, step.bind_resource, call.callee(),
                                 emit_call_args(call.args(), /*leading_comma=*/false, ""));
                break;
            }
            case Script::Step::Kind::ResourceAlias: {
                const auto source = resource_vars_.find(step.alias_of);
                if (source == resource_vars_.end()) {
                    throw std::runtime_error("ibex_compile: '" + step.alias_of +
                                             "' is not a resource");
                }
                bind_resource_var(step.name, source->second);
                break;
            }
            case Script::Step::Kind::ResourceUnbind: {
                if (const auto held = resource_vars_.find(step.name);
                    held != resource_vars_.end()) {
                    out << "    " << held->second << " = {};\n";
                    resource_vars_.erase(held);
                }
                break;
            }
            case Script::Step::Kind::DeferredScalar: {
                if (step.deferred == nullptr) {
                    throw std::runtime_error("ibex_compile: deferred step has no binding");
                }
                emit_deferred_scalar(*step.deferred);
                break;
            }
        }
    }

    if (script.return_call != nullptr) {
        const auto& call = ir::node_cast<ir::ExternCallNode>(*script.return_call);
        out << "    return " << callee_name(call.callee()) << "("
            << emit_call_args(call.args(), /*leading_comma=*/false, "") << ");\n";
        return {};
    }
    if (script.return_expr != nullptr) {
        out << "    return " << emit_raw_expr(*script.return_expr) << ";\n";
        return {};
    }
    std::string result_var;
    if (script.result_binding.has_value()) {
        if (const auto it = sink_inputs.find(*script.result_binding); it != sink_inputs.end()) {
            result_var = it->second;
        } else if (const auto named = named_tables_.find(*script.result_binding);
                   named != named_tables_.end()) {
            result_var = named->second;
        }
    }
    if (result_var.empty() && script.result != nullptr) {
        result_var = emit_node(*script.result);
    }
    return result_var;
}

void Emitter::emit_functions(const Script& script) {
    auto& out = *out_;
    for (const auto& fn : script.functions) {
        user_functions_.insert(fn->name);
    }
    const auto signature = [&](const Script::Function& fn) {
        std::string text = "auto " + callee_name(fn.name) + "(";
        for (std::size_t i = 0; i < fn.params.size(); ++i) {
            const auto& param = fn.params[i];
            text += i == 0 ? "" : ", ";
            text += param.kind == Script::Function::Param::Kind::Table
                        ? "const ibex::runtime::Table&"
                        : param.cpp_type;
            text += " _p_" + param.name;
        }
        return text + ") -> " + fn.return_type;
    };
    // Declared first so that functions may call one another in any order.
    for (const auto& fn : script.functions) {
        out << "static " << signature(*fn) << ";\n";
    }
    if (!script.functions.empty()) {
        out << "\n";
    }
    for (const auto& fn : script.functions) {
        out << "static " << signature(*fn) << " {\n";
        emit_function(*fn);
        out << "}\n\n";
    }
}

void Emitter::emit_function(const Script::Function& fn) {
    auto& out = *out_;
    // A function sees the program's constants and its own parameters, nothing of
    // the program's tables or connections.
    const auto saved_tables = std::move(named_tables_);
    const auto saved_resources = std::move(resource_vars_);
    const auto saved_names = runtime_scalar_names_;
    const auto saved_constants = compile_time_scalars_;
    named_tables_.clear();
    resource_vars_.clear();
    in_function_ = true;

    out << "    ibex::ops::ScalarScope _ibex_scope;\n";
    for (const auto& param : fn.params) {
        const std::string variable = "_p_" + param.name;
        switch (param.kind) {
            case Script::Function::Param::Kind::Resource:
                resource_vars_[param.name] = variable;
                break;
            case Script::Function::Param::Kind::Table:
                named_tables_[param.name] = variable;
                break;
            case Script::Function::Param::Kind::Scalar:
                runtime_scalar_names_.insert(param.name);
                compile_time_scalars_.erase(param.name);
                out << "    _ibex_scope.set(\"" << escape_string(param.name)
                    << "\", ibex::runtime::ScalarValue(" << variable << "));\n";
                break;
        }
    }
    const std::string result_var = emit_script_steps(fn.body);
    if (!result_var.empty()) {
        out << "    return " << result_var << ";\n";
    }

    in_function_ = false;
    named_tables_ = saved_tables;
    resource_vars_ = saved_resources;
    runtime_scalar_names_ = saved_names;
    compile_time_scalars_ = saved_constants;
}

void Emitter::emit_header(std::ostream& out, const Config& config, const Script* script) {
    out_ = &out;
    tmp_counter_ = 0;
    cached_vars_.clear();
    compile_time_scalars_.clear();
    for (const auto& [name, value] : config.scalar_bindings) {
        compile_time_scalars_.emplace(name, value);
    }
    runtime_scalar_names_.clear();
    for (const auto& binding : config.deferred_scalar_bindings) {
        runtime_scalar_names_.insert(binding.name);
    }
    for (const auto& name : config.runtime_scalar_names) {
        runtime_scalar_names_.insert(name);
    }

    // Preamble
    if (!config.source_name.empty()) {
        out << "// Generated by ibex compiler\n";
        out << "// Source: " << config.source_name << "\n";
        out << "\n";
    } else {
        out << "// Generated by ibex compiler\n\n";
    }

    out << "#include <cstdint>\n";
    out << "#include <ctime>\n";
    if (config.bench_mode)
        out << "#include <chrono>\n#include <cstdio>\n";
    else if (!config.table_entry_point)
        out << "#include <iostream>\n";
    out << "#include <limits>\n";
    out << "#include <optional>\n";
    out << "#include <string>\n";
    out << "#include <variant>\n";
    out << "#include <vector>\n";
    out << "#include <ibex/runtime/ops.hpp>\n";
    out << "#include <ibex/runtime/extern_registry.hpp>\n";

    for (const auto& hdr : config.extern_headers) {
        out << "#include \"" << escape_string(hdr) << "\"\n";
    }

    out << "\n";
    user_functions_.clear();
    if (script != nullptr) {
        emit_functions(*script);
    }
    if (config.table_entry_point) {
        if (config.bench_mode)
            throw std::runtime_error("table entry point cannot be a benchmark harness");
        out << "ibex::runtime::Table " << config.entry_point_name << "() {\n";
    } else if (config.forward_cli_args) {
        out << "int main(int argc, char** argv) {\n";
        out << "    ibex::ops::forward_cli_args(argc, argv);\n\n";
    } else {
        out << "int main() {\n";
    }

    const bool has_scalars = !config.scalar_bindings.empty() ||
                             !config.deferred_scalar_bindings.empty() ||
                             !config.runtime_scalar_names.empty();
    if (has_scalars) {
        out << "    ibex::runtime::ScalarRegistry _ibex_scalars;\n";
        for (const auto& [name, value] : config.scalar_bindings) {
            out << "    _ibex_scalars[\"" << escape_string(name) << "\"] = ";
            std::visit(
                [&](const auto& v) {
                    using V = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<V, std::monostate>) {
                        // Null scalar bindings are not yet emitted by codegen
                        // (plans/parse-args-and-nullable-scalars-plan.md, later
                        // slice); collect_scalar_bindings never produces one.
                        out << "ibex::runtime::ScalarValue{std::monostate{}}";
                    } else if constexpr (std::is_same_v<V, std::int64_t>) {
                        out << "std::int64_t{" << v << "}";
                    } else if constexpr (std::is_same_v<V, double>) {
                        out << format_double(v);
                    } else if constexpr (std::is_same_v<V, bool>) {
                        out << (v ? "true" : "false");
                    } else if constexpr (std::is_same_v<V, std::string>) {
                        out << "\"" << escape_string(v) << "\"";
                    } else if constexpr (std::is_same_v<V, Date>) {
                        out << "ibex::Date{std::int32_t{" << v.days << "}}";
                    } else if constexpr (std::is_same_v<V, DecimalValue>) {
                        out << decimal_value_cpp(v);
                    } else {
                        static_assert(std::is_same_v<V, Timestamp>);
                        out << "ibex::Timestamp{std::int64_t{" << v.nanos << "}}";
                    }
                },
                value);
            out << ";\n";
        }
        out << "    ibex::ops::set_scalars(&_ibex_scalars);\n\n";

        // Deferred scalar `let`s: run each subplan, extract, then evaluate the
        // residual expression against the registry. Same order and semantics as
        // runtime::materialize_deferred_scalar_bindings.
        for (const auto& binding : config.deferred_scalar_bindings) {
            emit_deferred_scalar(binding);
        }
        if (!config.deferred_scalar_bindings.empty()) {
            out << "\n";
        }
    }
}

void Emitter::emit_deferred_scalar(const ir::DeferredScalarBinding& binding) {
    auto& out = *out_;
    for (const auto& source : binding.sources) {
        out << "    {\n";
        auto src_var = emit_node(*source.plan);
        emit_scalar_store(source.tmp_name, "ibex::ops::scalar_of_table(" + src_var + ", \"" +
                                               escape_string(source.column.value_or("")) + "\", " +
                                               (source.column.has_value() ? "false" : "true") +
                                               ")");
        out << "    }\n";
    }
    emit_scalar_store(binding.name, "ibex::ops::eval_scalar(" + emit_expr(binding.value) + ")");
}

void Emitter::emit_query(std::ostream& out, const ir::Node& root, const Config& config) {
    if (config.bench_mode) {
        // Phase 1: emit ExternCall (data loading) nodes into main buffer (setup).
        collect_extern_calls(root);

        // Phase 2: emit the query into a temporary buffer.
        std::ostringstream query_buf;
        auto* saved_out = out_;
        out_ = &query_buf;
        auto result_var = emit_node(root);
        out_ = saved_out;

        // Re-indent query code by 4 extra spaces so it sits inside the loop body.
        const std::string query_code = indent_code(query_buf.str(), 4);

        // Warmup loop
        out << "\n    // Warmup\n";
        out << "    for (int _bench_w = 0; _bench_w < " << config.bench_warmup
            << "; ++_bench_w) {\n";
        out << query_code;
        out << "        (void)" << result_var << ";\n";
        out << "    }\n";

        // Timed loop
        out << "\n    // Timed iterations\n";
        out << "    auto _bench_t0 = std::chrono::steady_clock::now();\n";
        out << "    for (int _bench_i = 0; _bench_i < " << config.bench_iters
            << "; ++_bench_i) {\n";
        out << query_code;
        out << "        (void)" << result_var << ";\n";
        out << "    }\n";
        out << "    auto _bench_t1 = std::chrono::steady_clock::now();\n";
        out << "    double _avg_ms = std::chrono::duration<double, std::milli>"
               "(_bench_t1 - _bench_t0).count() / "
            << config.bench_iters << ";\n";
        out << "    std::fprintf(stderr, \"avg_ms=%.3f\\n\", _avg_ms);\n";
    } else {
        auto result_var = emit_node(root);
        if (config.table_entry_point) {
            out << "    return " << result_var << ";\n";
        } else if (config.print_result) {
            out << "    ibex::ops::print(" << result_var << ");\n";
        }
    }
}

void Emitter::emit_footer(std::ostream& out, const Config& config) {
    if (!config.table_entry_point)
        out << "    return 0;\n";
    out << "}\n";

    out_ = nullptr;
}

void Emitter::collect_extern_calls(const ir::Node& node) {
    if (cached_vars_.contains(&node))
        return;
    if (node.kind() == ir::NodeKind::ExternCall) {
        const auto& ec = ir::node_cast<ir::ExternCallNode>(node);
        auto var = fresh_var();
        *out_ << "    auto " << var << " = " << ec.callee() << "(";
        bool first = true;
        for (const auto& arg : ec.args()) {
            if (!first)
                *out_ << ", ";
            first = false;
            *out_ << emit_raw_expr(arg);
        }
        *out_ << ");\n";
        cached_vars_[&node] = std::move(var);
        return;
    }
    for (const auto& child : node.children()) {
        if (!child) {
            throw std::runtime_error("ibex_compile: IR contains a null child node");
        }
        collect_extern_calls(*child);
    }
}

// ─── Private helpers ─────────────────────────────────────────────────────────

auto Emitter::fresh_var() -> std::string {
    return "t" + std::to_string(tmp_counter_++);
}

// NOLINTNEXTLINE(readability-function-size): one exhaustive dispatcher keeps IR emission local.
auto Emitter::emit_node(const ir::Node& node) -> std::string {
    const auto require_single_child = [](const ir::Node& parent,
                                         std::string_view node_name) -> const ir::Node& {
        if (parent.children().size() != 1 || !parent.children().front()) {
            throw std::runtime_error("ibex_compile: " + std::string(node_name) +
                                     " expects exactly one non-null child");
        }
        return *parent.children().front();
    };

    switch (node.kind()) {
        case ir::NodeKind::Scan: {
            const auto& scan = ir::node_cast<ir::ScanNode>(node);
            if (scan.source_name() == "__stream_input__" && !stream_scan_var_.empty()) {
                return stream_scan_var_;
            }
            if (const auto named = named_tables_.find(scan.source_name());
                named != named_tables_.end()) {
                return named->second;
            }
            throw std::runtime_error(
                "ibex_compile: ScanNode cannot be emitted — use 'extern fn' to declare data "
                "sources");
        }

        case ir::NodeKind::Filter: {
            const auto& filter = ir::node_cast<ir::FilterNode>(node);
            auto child = emit_node(require_single_child(filter, "FilterNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::filter(" << child << ", "
                  << emit_filter_expr(filter.predicate()) << ");\n";
            return var;
        }

        case ir::NodeKind::Project: {
            const auto& proj = ir::node_cast<ir::ProjectNode>(node);
            auto child = emit_node(require_single_child(proj, "ProjectNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::project(" << child << ", {";
            bool first = true;
            for (const auto& col : proj.columns()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(col.name) << '"';
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Distinct: {
            auto child = emit_node(require_single_child(node, "DistinctNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::distinct(" << child << ");\n";
            return var;
        }

        case ir::NodeKind::Order: {
            const auto& order = ir::node_cast<ir::OrderNode>(node);
            auto child = emit_node(require_single_child(order, "OrderNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::order(" << child << ", {";
            bool first = true;
            for (const auto& key : order.keys()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ir::OrderKey{\"" << escape_string(key.name) << "\", "
                      << (key.ascending ? "true" : "false") << "}";
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Head: {
            const auto& head = ir::node_cast<ir::HeadNode>(node);
            auto child = emit_node(require_single_child(head, "HeadNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::head(" << child
                  << ", ibex::ops::eval_row_count(" << emit_expr(head.count_expr()) << "), {";
            bool first = true;
            for (const auto& key : head.group_by()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(key.name) << '"';
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Tail: {
            const auto& tail = ir::node_cast<ir::TailNode>(node);
            auto child = emit_node(require_single_child(tail, "TailNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::tail(" << child
                  << ", ibex::ops::eval_row_count(" << emit_expr(tail.count_expr()) << "), {";
            bool first = true;
            for (const auto& key : tail.group_by()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(key.name) << '"';
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::TopK: {
            const auto& topk = ir::node_cast<ir::TopKNode>(node);
            auto child = emit_node(require_single_child(topk, "TopKNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::top_k(" << child << ", {";
            bool first = true;
            for (const auto& key : topk.keys()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ir::OrderKey{\"" << escape_string(key.name) << "\", "
                      << (key.ascending ? "true" : "false") << "}";
            }
            *out_ << "}, " << topk.count() << ", {";
            first = true;
            for (const auto& key : topk.group_by()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(key.name) << '"';
            }
            *out_ << "}, " << (topk.keep_mode() == ir::TopKNode::KeepMode::First ? "true" : "false")
                  << ");\n";
            return var;
        }

        case ir::NodeKind::Aggregate: {
            const auto& agg = ir::node_cast<ir::AggregateNode>(node);
            auto child = emit_node(require_single_child(agg, "AggregateNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::aggregate(" << child << ",\n";

            // group_by
            *out_ << "        {";
            bool first = true;
            for (const auto& g : agg.group_by()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(g.name) << '"';
            }
            *out_ << "},\n";

            // aggregations
            *out_ << "        {";
            first = true;
            for (const auto& a : agg.aggregations()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ops::make_agg("
                      << "ibex::ir::AggFunc::" << emit_agg_func(a.func) << ", \""
                      << escape_string(a.column.name) << "\", \"" << escape_string(a.alias)
                      << "\", " << format_double(a.param) << (a.is_count ? ", true" : "") << ")";
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Update: {
            const auto& upd = ir::node_cast<ir::UpdateNode>(node);
            auto child = emit_node(require_single_child(upd, "UpdateNode"));
            auto var = fresh_var();

            std::vector<std::pair<std::vector<std::string>, std::string>> tuple_sources;
            tuple_sources.reserve(upd.tuple_fields().size());
            for (const auto& tf : upd.tuple_fields()) {
                if (!tf.source) {
                    throw std::runtime_error("ibex_compile: UpdateNode tuple source is null");
                }
                tuple_sources.emplace_back(tf.aliases, emit_node(*tf.source));
            }

            *out_ << "    auto " << var
                  << " = ibex::ops::" << (upd.guard() != nullptr ? "update_where" : "update") << "("
                  << child << ", {\n";
            bool first = true;
            for (const auto& f : upd.fields()) {
                if (!first)
                    *out_ << ",\n";
                first = false;
                *out_ << "        ibex::ops::make_field(\"" << escape_string(f.alias) << "\", "
                      << emit_expr(f.expr) << ")";
            }
            *out_ << "\n    }, {";

            bool first_tuple = true;
            for (const auto& tf : tuple_sources) {
                if (!first_tuple)
                    *out_ << ", ";
                first_tuple = false;
                *out_ << "ibex::ops::TupleSource{{";
                bool first_alias = true;
                for (const auto& alias : tf.first) {
                    if (!first_alias)
                        *out_ << ", ";
                    first_alias = false;
                    *out_ << '"' << escape_string(alias) << '"';
                }
                *out_ << "}, " << tf.second << "}";
            }

            *out_ << "}, {";
            bool first_group = true;
            for (const auto& key : upd.group_by()) {
                if (!first_group)
                    *out_ << ", ";
                first_group = false;
                *out_ << '"' << escape_string(key.name) << '"';
            }
            *out_ << "}";
            if (upd.guard() != nullptr) {
                *out_ << ", " << emit_expr(*upd.guard());
            }
            *out_ << ");\n";
            return var;
        }

        case ir::NodeKind::Map: {
            const auto& mn = ir::node_cast<ir::MapNode>(node);
            auto child = emit_node(require_single_child(mn, "MapNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::map(" << child << ", {\n";
            bool first = true;
            for (const auto& f : mn.fields()) {
                if (!first)
                    *out_ << ",\n";
                first = false;
                *out_ << "        ibex::ops::make_field(\"" << escape_string(f.alias) << "\", "
                      << emit_expr(f.expr) << ")";
            }
            *out_ << "\n    });\n";
            return var;
        }

        case ir::NodeKind::Rename: {
            const auto& ren = ir::node_cast<ir::RenameNode>(node);
            auto child = emit_node(require_single_child(ren, "RenameNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::rename(" << child << ", {";
            bool first = true;
            for (const auto& spec : ren.renames()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ir::RenameSpec{\"" << escape_string(spec.new_name) << "\", \""
                      << escape_string(spec.old_name) << "\"}";
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Window: {
            const auto& win = ir::node_cast<ir::WindowNode>(node);
            const auto& window_child = require_single_child(win, "WindowNode");
            if (window_child.kind() != ir::NodeKind::Update) {
                throw std::runtime_error("ibex_compile: WindowNode must have an UpdateNode child");
            }
            const auto& upd = ir::node_cast<ir::UpdateNode>(window_child);
            // The interpreter rejects these too; saying so here beats a program
            // that always fails when it runs.
            if (!upd.tuple_fields().empty() || upd.guard() != nullptr) {
                throw std::runtime_error(
                    "ibex_compile: a window update does not support tuple fields or a "
                    "`where` guard");
            }
            auto source = emit_node(require_single_child(upd, "UpdateNode (window payload)"));
            auto var = fresh_var();
            // The common `window + update` keeps its short form; the `select` and
            // `aligned` forms take the full one.
            const bool full = win.select_only() || win.aligned();
            *out_ << "    auto " << var
                  << " = ibex::ops::" << (full ? "window_update" : "windowed_update") << "("
                  << source << ",\n";
            *out_ << "        ibex::ir::Duration(" << win.duration().count() << "LL),\n";
            *out_ << "        {";
            bool first = true;
            for (const auto& f : upd.fields()) {
                if (!first)
                    *out_ << ",\n         ";
                first = false;
                *out_ << "ibex::ops::make_field(\"" << escape_string(f.alias) << "\", "
                      << emit_expr(f.expr) << ")";
            }
            *out_ << "},\n        {";
            bool first_group = true;
            for (const auto& key : upd.group_by()) {
                if (!first_group)
                    *out_ << ", ";
                first_group = false;
                *out_ << '"' << escape_string(key.name) << '"';
            }
            *out_ << "}";
            if (full) {
                *out_ << ", " << (win.select_only() ? "true" : "false") << ", "
                      << (win.aligned() ? "true" : "false");
            }
            *out_ << ");\n";
            return var;
        }

        case ir::NodeKind::Resample: {
            const auto& rs = ir::node_cast<ir::ResampleNode>(node);
            auto child = emit_node(require_single_child(rs, "ResampleNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::resample(" << child << ",\n";

            // duration (nanoseconds count)
            *out_ << "        ibex::ir::Duration(" << rs.duration().count() << "LL),\n";

            // group_by
            *out_ << "        {";
            bool first = true;
            for (const auto& g : rs.group_by()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(g.name) << '"';
            }
            *out_ << "},\n";

            // aggregations
            *out_ << "        {";
            first = true;
            for (const auto& a : rs.aggregations()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ops::make_agg("
                      << "ibex::ir::AggFunc::" << emit_agg_func(a.func) << ", \""
                      << escape_string(a.column.name) << "\", \"" << escape_string(a.alias)
                      << "\", " << format_double(a.param) << (a.is_count ? ", true" : "") << ")";
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::AsTimeframe: {
            const auto& atf = ir::node_cast<ir::AsTimeframeNode>(node);
            auto child = emit_node(require_single_child(atf, "AsTimeframeNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::as_timeframe(" << child << ", \""
                  << escape_string(atf.column()) << "\");\n";
            return var;
        }

        case ir::NodeKind::Ascribe: {
            const auto& asc = ir::node_cast<ir::AscribeNode>(node);
            auto child = emit_node(require_single_child(asc, "AscribeNode"));
            auto var = fresh_var();
            auto type_name = [](ir::ColumnType t) -> std::string_view {
                switch (t) {
                    case ir::ColumnType::Int32:
                        return "Int32";
                    case ir::ColumnType::Int64:
                        return "Int64";
                    case ir::ColumnType::Float32:
                        return "Float32";
                    case ir::ColumnType::Float64:
                        return "Float64";
                    case ir::ColumnType::Bool:
                        return "Bool";
                    case ir::ColumnType::String:
                        return "String";
                    case ir::ColumnType::Date:
                        return "Date";
                    case ir::ColumnType::Timestamp:
                        return "Timestamp";
                    case ir::ColumnType::Decimal:
                        return "Decimal";
                    case ir::ColumnType::Categorical:
                        // Unreachable from parsed source: no `parser::ScalarType`
                        // spells this, so a written ascription field never carries
                        // it. Handled for switch exhaustiveness, not because emitted
                        // code can reach this arm.
                        return "String";
                }
                return "Int64";
            };
            *out_ << "    auto " << var << " = ibex::ops::ascribe(" << child << ", {";
            bool first = true;
            for (const auto& field : asc.schema()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << "ibex::ir::SchemaField{\"" << escape_string(field.name) << "\", ";
                if (field.type.has_value()) {
                    *out_ << "ibex::ir::ColumnType::" << type_name(*field.type);
                } else {
                    *out_ << "std::nullopt";
                }
                *out_ << "}";
            }
            *out_ << "}, " << (asc.open() ? "true" : "false") << ");\n";
            return var;
        }

        case ir::NodeKind::Columns: {
            auto child = emit_node(require_single_child(node, "ColumnsNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::columns(" << child << ");\n";
            return var;
        }

        case ir::NodeKind::ExternCall: {
            // In bench mode ExternCall nodes were pre-emitted; reuse the var.
            if (auto it = cached_vars_.find(&node); it != cached_vars_.end())
                return it->second;
            const auto& ec = ir::node_cast<ir::ExternCallNode>(node);
            auto var = fresh_var();
            *out_ << "    auto " << var << " = " << callee_name(ec.callee()) << "(";
            bool first = true;
            for (const auto& arg : ec.args()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << emit_raw_expr(arg);
            }
            *out_ << ");\n";
            return var;
        }

        case ir::NodeKind::Join: {
            const auto& join = ir::node_cast<ir::JoinNode>(node);
            if (join.children().size() != 2) {
                throw std::runtime_error("ibex_compile: JoinNode expects two children");
            }
            if (!join.children()[0] || !join.children()[1]) {
                throw std::runtime_error("ibex_compile: JoinNode children must be non-null");
            }
            auto left = emit_node(*join.children()[0]);
            auto right = emit_node(*join.children()[1]);
            auto var = fresh_var();
            const char* fn = nullptr;
            const char* kind = nullptr;
            switch (join.kind()) {
                case ir::JoinKind::Inner:
                    fn = "inner_join";
                    kind = "Inner";
                    break;
                case ir::JoinKind::Left:
                    fn = "left_join";
                    kind = "Left";
                    break;
                case ir::JoinKind::Right:
                    fn = "right_join";
                    kind = "Right";
                    break;
                case ir::JoinKind::Outer:
                    fn = "outer_join";
                    kind = "Outer";
                    break;
                case ir::JoinKind::Semi:
                    fn = "semi_join";
                    kind = "Semi";
                    break;
                case ir::JoinKind::Anti:
                    fn = "anti_join";
                    kind = "Anti";
                    break;
                case ir::JoinKind::Cross:
                    fn = "cross_join";
                    kind = "Cross";
                    break;
                case ir::JoinKind::Asof:
                    fn = "asof_join";
                    kind = "Asof";
                    break;
            }
            if (join.predicate().has_value()) {
                *out_ << "    auto " << var << " = ibex::ops::join_with_predicate(" << left << ", "
                      << right << ", ibex::ir::JoinKind::" << kind << ", {";
                bool first = true;
                for (const auto& key : join.keys()) {
                    if (!first)
                        *out_ << ", ";
                    first = false;
                    *out_ << "{\"" << escape_string(key.left) << "\", \""
                          << escape_string(key.right) << "\"";
                    if (key.fold_output) {
                        *out_ << ", true";
                        if (!key.output_name_override.empty()) {
                            *out_ << ", \"" << escape_string(key.output_name_override) << "\"";
                        }
                    }
                    *out_ << "}";
                }
                *out_ << "}, " << emit_filter_expr(*join.predicate())
                      << emit_join_suffix(join.suffix(), join.null_match(), join.expect(),
                                          join.take())
                      << ");\n";
                return var;
            }
            *out_ << "    auto " << var << " = ibex::ops::" << fn << "(" << left << ", " << right;
            if (join.kind() != ir::JoinKind::Cross) {
                *out_ << ", {";
                bool first = true;
                for (const auto& key : join.keys()) {
                    if (!first)
                        *out_ << ", ";
                    first = false;
                    *out_ << "{\"" << escape_string(key.left) << "\", \""
                          << escape_string(key.right) << "\"";
                    if (key.fold_output) {
                        *out_ << ", true";
                        if (!key.output_name_override.empty()) {
                            *out_ << ", \"" << escape_string(key.output_name_override) << "\"";
                        }
                    }
                    *out_ << "}";
                }
                *out_ << "}";
            }
            *out_ << emit_join_suffix(join.suffix(), join.null_match(), join.expect(), join.take())
                  << ");\n";
            return var;
        }

        case ir::NodeKind::Melt: {
            const auto& mn = ir::node_cast<ir::MeltNode>(node);
            auto child = emit_node(require_single_child(mn, "MeltNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::melt(" << child << ", {";
            bool first = true;
            for (const auto& col : mn.id_columns()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(col) << '"';
            }
            *out_ << "}, {";
            first = true;
            for (const auto& col : mn.measure_columns()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(col) << '"';
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Dcast: {
            const auto& dn = ir::node_cast<ir::DcastNode>(node);
            auto child = emit_node(require_single_child(dn, "DcastNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::dcast(" << child << ", \""
                  << escape_string(dn.pivot_column()) << "\", \""
                  << escape_string(dn.value_column()) << "\", {";
            bool first = true;
            for (const auto& key : dn.row_keys()) {
                if (!first)
                    *out_ << ", ";
                first = false;
                *out_ << '"' << escape_string(key) << '"';
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Cov: {
            auto child = emit_node(require_single_child(node, "CovNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::cov(" << child << ");\n";
            return var;
        }

        case ir::NodeKind::Corr: {
            auto child = emit_node(require_single_child(node, "CorrNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::corr(" << child << ");\n";
            return var;
        }

        case ir::NodeKind::Transpose: {
            auto child = emit_node(require_single_child(node, "TransposeNode"));
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::transpose(" << child << ");\n";
            return var;
        }

        case ir::NodeKind::Matmul: {
            if (node.children().size() != 2) {
                throw std::runtime_error("MatmulNode expects exactly two children");
            }
            auto left = emit_node(*node.children()[0]);
            auto right = emit_node(*node.children()[1]);
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::matmul(" << left << ", " << right
                  << ");\n";
            return var;
        }

        case ir::NodeKind::Rbind: {
            if (node.children().size() < 2) {
                throw std::runtime_error("RbindNode expects at least two children");
            }
            std::vector<std::string> child_vars;
            child_vars.reserve(node.children().size());
            for (const auto& child : node.children()) {
                child_vars.push_back(emit_node(*child));
            }
            auto var = fresh_var();
            *out_ << "    auto " << var << " = ibex::ops::rbind({";
            for (std::size_t i = 0; i < child_vars.size(); ++i) {
                if (i > 0) {
                    *out_ << ", ";
                }
                *out_ << child_vars[i];
            }
            *out_ << "});\n";
            return var;
        }

        case ir::NodeKind::Model: {
            const auto& mn = ir::node_cast<ir::ModelNode>(node);
            // Only the built-in methods fit in a compiled program: a plugin's
            // method lives in a plugin the program has no registry to load.
            if (mn.method() != "ols" && mn.method() != "ridge" && mn.method() != "wls") {
                throw std::runtime_error("ibex_compile: model method '" + mn.method() +
                                         "' comes from a plugin, which compiled programs do not "
                                         "support yet (built in: ols, ridge, wls)");
            }
            auto child = emit_node(require_single_child(mn, "ModelNode"));
            const std::string model_var = "_model" + std::to_string(tmp_counter_++);
            auto var = fresh_var();
            *out_ << "    ibex::runtime::ModelResult " << model_var << ";\n";
            *out_ << "    auto " << var << " = ibex::ops::fit_model(" << child
                  << ", ibex::ir::ModelFormula{\"" << escape_string(mn.formula().response)
                  << "\", {";
            bool first_term = true;
            for (const auto& term : mn.formula().terms) {
                if (!first_term)
                    *out_ << ", ";
                first_term = false;
                *out_ << "ibex::ir::ModelTerm{{";
                bool first_column = true;
                for (const auto& column : term.columns) {
                    if (!first_column)
                        *out_ << ", ";
                    first_column = false;
                    *out_ << '"' << escape_string(column) << '"';
                }
                *out_ << "}, " << (term.is_dot ? "true" : "false") << "}";
            }
            *out_ << "}, " << (mn.formula().has_intercept ? "true" : "false") << "}, \""
                  << escape_string(mn.method()) << "\", {";
            bool first_param = true;
            for (const auto& param : mn.params()) {
                if (!first_param)
                    *out_ << ", ";
                first_param = false;
                *out_ << "ibex::ir::ModelParamSpec{\"" << escape_string(param.name) << "\", "
                      << emit_expr(param.value) << "}";
            }
            *out_ << "}, " << model_var << ");\n";
            last_model_ = model_var;
            return var;
        }

        case ir::NodeKind::Construct: {
            const auto& cn = ir::node_cast<ir::ConstructNode>(node);
            auto var = fresh_var();
            *out_ << "    ibex::runtime::Table " << var << ";\n";
            if (cn.row_count().has_value()) {
                *out_ << "    " << var << ".logical_rows = static_cast<std::size_t>("
                      << emit_raw_expr(*cn.row_count()) << ");\n";
                return var;
            }
            for (const auto& col : cn.columns()) {
                if (col.expr_node) {
                    // Expression column: emit the sub-node then extract the column.
                    auto sub_var = emit_node(*col.expr_node);
                    auto cname = escape_string(col.name);
                    *out_ << "    {\n";
                    *out_ << "        auto& __sub = " << sub_var << ";\n";
                    *out_ << "        if (__sub.columns.size() == 1) {\n";
                    *out_ << "            const auto& __entry = __sub.columns[0];\n";
                    *out_ << "            " << var << ".add_column_shared(\"" << cname
                          << "\", __entry.column, __entry.validity);\n";
                    *out_ << "        } else {\n";
                    *out_ << "            auto __it = __sub.index.find(\"" << cname << "\");\n";
                    *out_ << "            if (__it == __sub.index.end())\n";
                    *out_
                        << "                throw std::runtime_error(\"Table constructor: column '"
                        << cname << "' not found in expression result\");\n";
                    *out_ << "            const auto& __entry = __sub.columns[__it->second];\n";
                    *out_ << "            " << var << ".add_column_shared(\"" << cname
                          << "\", __entry.column, __entry.validity);\n";
                    *out_ << "        }\n";
                    *out_ << "    }\n";
                    continue;
                }
                if (col.elements.empty()) {
                    *out_ << "    " << var << ".add_column(\"" << escape_string(col.name)
                          << "\", ibex::Column<std::int64_t>{});\n";
                    continue;
                }
                // A column with `null` elements is added with an explicit
                // validity bitmap; without one, add_column marks every row valid.
                auto emit_validity = [&] {
                    if (col.valid.empty()) {
                        return;
                    }
                    *out_ << ", ibex::runtime::ValidityBitmap{std::vector<bool>{";
                    bool first = true;
                    for (const bool v : col.valid) {
                        if (!first) {
                            *out_ << ", ";
                        }
                        first = false;
                        *out_ << (v ? "true" : "false");
                    }
                    *out_ << "}}";
                };
                // Determine column type from first element and emit the add_column call.
                std::visit(
                    [&](const auto& first_val) {
                        using T = std::decay_t<decltype(first_val)>;
                        std::string type_str;
                        if constexpr (std::is_same_v<T, std::int64_t>) {
                            type_str = "std::int64_t";
                        } else if constexpr (std::is_same_v<T, double>) {
                            type_str = "double";
                        } else if constexpr (std::is_same_v<T, bool>) {
                            type_str = "bool";
                        } else if constexpr (std::is_same_v<T, std::string>) {
                            type_str = "std::string";
                        } else if constexpr (std::is_same_v<T, Date>) {
                            type_str = "ibex::Date";
                        } else if constexpr (std::is_same_v<T, DecimalValue>) {
                            type_str = "ibex::Decimal";
                        } else {
                            static_assert(std::is_same_v<T, Timestamp>);
                            type_str = "ibex::Timestamp";
                        }
                        if constexpr (std::is_same_v<T, DecimalValue>) {
                            // A Decimal column carries its type as metadata, so it
                            // is built by a helper rather than a brace list.
                            DecimalType unified = first_val.type;
                            for (const auto& lit : col.elements) {
                                if (const auto* d = std::get_if<DecimalValue>(&lit.value)) {
                                    unified = decimal::union_type(unified, d->type);
                                }
                            }
                            *out_ << "    " << var << ".add_column(\"" << escape_string(col.name)
                                  << "\", ibex::runtime::decimal_column(ibex::DecimalType{"
                                  << static_cast<int>(unified.precision) << ", "
                                  << static_cast<int>(unified.scale) << "}, {";
                            bool first_elem = true;
                            for (const auto& lit : col.elements) {
                                if (!first_elem) {
                                    *out_ << ", ";
                                }
                                first_elem = false;
                                if (const auto* d = std::get_if<DecimalValue>(&lit.value)) {
                                    *out_ << decimal_value_cpp(*d);
                                } else if (const auto* i = std::get_if<std::int64_t>(&lit.value)) {
                                    *out_ << "ibex::DecimalValue{" << *i
                                          << ", ibex::decimal::kInt64Type}";
                                }
                            }
                            *out_ << "})";
                            emit_validity();
                            *out_ << ");\n";
                            return;
                        }
                        *out_ << "    " << var << ".add_column(\"" << escape_string(col.name)
                              << "\", ibex::Column<" << type_str << ">{";
                        bool first = true;
                        for (const auto& lit : col.elements) {
                            if (!first)
                                *out_ << ", ";
                            first = false;
                            std::visit(
                                [&](const auto& v) {
                                    using V = std::decay_t<decltype(v)>;
                                    if constexpr (std::is_same_v<V, std::int64_t>) {
                                        *out_ << "std::int64_t{" << std::to_string(v) << "}";
                                    } else if constexpr (std::is_same_v<V, double>) {
                                        *out_ << format_double(v);
                                    } else if constexpr (std::is_same_v<V, bool>) {
                                        *out_ << (v ? "true" : "false");
                                    } else if constexpr (std::is_same_v<V, std::string>) {
                                        *out_ << '"' << escape_string(v) << '"';
                                    } else if constexpr (std::is_same_v<V, Date>) {
                                        *out_ << "ibex::Date{std::int32_t{"
                                              << std::to_string(v.days) << "}}";
                                    } else if constexpr (std::is_same_v<V, DecimalValue>) {
                                        // Unreachable: Decimal columns returned above.
                                        *out_ << decimal_value_cpp(v);
                                    } else {
                                        static_assert(std::is_same_v<V, Timestamp>);
                                        *out_ << "ibex::Timestamp{std::int64_t{"
                                              << std::to_string(v.nanos) << "}}";
                                    }
                                },
                                lit.value);
                        }
                        *out_ << "}";
                        emit_validity();
                        *out_ << ");\n";
                    },
                    col.elements[0].value);
            }
            return var;
        }

        case ir::NodeKind::Stream: {
            const auto& sn = ir::node_cast<ir::StreamNode>(node);
            auto var = fresh_var();

            // ── Emit the transform into a temporary buffer ────────────────────
            // The transform IR starts with ScanNode("__stream_input__").
            // We substitute it with the lambda parameter "_ibex_sbuf".
            const std::string sbuf = "_ibex_sbuf";
            std::ostringstream transform_buf;
            auto* saved_out = out_;
            out_ = &transform_buf;
            stream_scan_var_ = sbuf;
            auto transform_var = emit_node(sn.transform_ir());
            stream_scan_var_.clear();
            out_ = saved_out;

            // ── Emit the "flush" lambda: transform buffer → sink ──────────────
            const std::string emit_fn = "_emit_fn_" + var;
            *out_ << "    auto " << emit_fn << " = [&](const ibex::runtime::Table& " << sbuf
                  << ") {\n";
            *out_ << "        if (" << sbuf << ".rows() == 0) return;\n";
            *out_ << indent_code(transform_buf.str(), 4);
            *out_ << "        " << sn.sink_callee() << "(" << transform_var;
            for (const auto& arg : sn.sink_args()) {
                *out_ << ", " << emit_raw_expr(arg);
            }
            *out_ << ");\n";
            *out_ << "    };\n\n";

            // ── Emit the source-normalisation lambda ──────────────────────────
            // Source may return Table (e.g. udp_recv) or ExternValue (e.g. ws_recv).
            // We normalise both to ExternValue so the event loop is uniform.
            const std::string call_src = "_call_src_" + var;
            *out_ << "    auto " << call_src << " = [&]() -> ibex::runtime::ExternValue {\n";
            *out_ << "        auto _r = " << sn.source_callee() << "(";
            {
                bool first = true;
                for (const auto& arg : sn.source_args()) {
                    if (!first)
                        *out_ << ", ";
                    first = false;
                    *out_ << emit_raw_expr(arg);
                }
            }
            *out_ << ");\n";
            *out_ << "        if constexpr (std::is_same_v<decltype(_r), "
                     "ibex::runtime::ExternValue>) {\n";
            *out_ << "            return _r;\n";
            *out_ << "        } else {\n";
            *out_ << "            return ibex::runtime::ExternValue{std::move(_r)};\n";
            *out_ << "        }\n";
            *out_ << "    };\n\n";

            const std::string buf = "_buf_" + var;
            *out_ << "    ibex::runtime::Table " << buf << ";\n";

            if (sn.stream_kind() == ir::StreamKind::TimeBucket) {
                const auto bucket_ns = static_cast<std::int64_t>(sn.bucket_duration().count());
                *out_ << "    std::int64_t _bopen_" << var << " = -1;\n";
                *out_ << "    std::int64_t _bwall_" << var << " = -1;\n";
                *out_ << "    const std::int64_t _bns_" << var << " = std::int64_t{" << bucket_ns
                      << "LL};\n\n";

                *out_ << "    while (true) {\n";
                *out_ << "        auto _src_" << var << " = " << call_src << "();\n";
                *out_ << "        const bool _tout_" << var
                      << " = std::holds_alternative<ibex::runtime::StreamTimeout>(_src_" << var
                      << ");\n";
                *out_ << "        if (!_tout_" << var << ") {\n";
                *out_ << "            if (std::get<ibex::runtime::Table>(_src_" << var
                      << ").rows() == 0) break;\n";
                *out_ << "        }\n";

                // Wall-clock flush
                *out_ << "        {\n";
                *out_ << "            struct timespec _tp_" << var << "{};\n";
                *out_ << "            clock_gettime(CLOCK_REALTIME, &_tp_" << var << ");\n";
                *out_ << "            std::int64_t _wall_" << var << " = std::int64_t(_tp_" << var
                      << ".tv_sec) * 1'000'000'000LL\n";
                *out_ << "                       + std::int64_t(_tp_" << var << ".tv_nsec);\n";
                *out_ << "            if (_bopen_" << var << " >= 0 && " << buf
                      << ".rows() > 0 &&\n";
                *out_ << "                _wall_" << var << " - _bwall_" << var << " >= _bns_"
                      << var << ") {\n";
                *out_ << "                " << emit_fn << "(" << buf << ");\n";
                *out_ << "                " << buf << " = ibex::runtime::Table{};\n";
                *out_ << "                _bopen_" << var << " = -1; _bwall_" << var << " = -1;\n";
                *out_ << "            }\n";

                // Row-by-row bucket detection
                *out_ << "            if (!_tout_" << var << ") {\n";
                *out_ << "                const auto& _batch_" << var
                      << " = std::get<ibex::runtime::Table>(_src_" << var << ");\n";
                *out_ << "                for (std::size_t _ri_" << var << " = 0; _ri_" << var
                      << " < _batch_" << var << ".rows(); ++_ri_" << var << ") {\n";
                *out_ << "                    auto _rts_" << var
                      << " = ibex::ops::stream_get_ts_ns(_batch_" << var << ", _ri_" << var
                      << ");\n";
                *out_ << "                    const std::int64_t _rbk_" << var << " = _rts_" << var
                      << " ? ((*_rts_" << var << " / _bns_" << var << ") * _bns_" << var
                      << ") : -1;\n";
                *out_ << "                    if (_bopen_" << var << " >= 0 && _rbk_" << var
                      << " >= 0 && _rbk_" << var << " > _bopen_" << var << ") {\n";
                *out_ << "                        " << emit_fn << "(" << buf << ");\n";
                *out_ << "                        " << buf << " = ibex::runtime::Table{};\n";
                *out_ << "                    }\n";
                *out_ << "                    if (_rbk_" << var << " >= 0) {\n";
                *out_ << "                        if (_rbk_" << var << " != _bopen_" << var
                      << ") _bwall_" << var << " = _wall_" << var << ";\n";
                *out_ << "                        _bopen_" << var << " = _rbk_" << var << ";\n";
                *out_ << "                    }\n";
                *out_ << "                    ibex::ops::stream_append_row(" << buf << ", _batch_"
                      << var << ", _ri_" << var << ");\n";
                *out_ << "                }\n";
                *out_ << "            }\n";
                *out_ << "        }\n";
                *out_ << "    }\n";
                *out_ << "    if (" << buf << ".rows() > 0) " << emit_fn << "(" << buf << ");\n";
            } else {
                // PerRow: append entire batch then run transform on accumulated buffer.
                *out_ << "    while (true) {\n";
                *out_ << "        auto _src_" << var << " = " << call_src << "();\n";
                *out_ << "        if (std::holds_alternative<ibex::runtime::StreamTimeout>(_src_"
                      << var << ")) continue;\n";
                *out_ << "        const auto& _batch_" << var
                      << " = std::get<ibex::runtime::Table>(_src_" << var << ");\n";
                *out_ << "        if (_batch_" << var << ".rows() == 0) break;\n";
                *out_ << "        for (std::size_t _ri_" << var << " = 0; _ri_" << var
                      << " < _batch_" << var << ".rows(); ++_ri_" << var << ") {\n";
                *out_ << "            ibex::ops::stream_append_row(" << buf << ", _batch_" << var
                      << ", _ri_" << var << ");\n";
                *out_ << "        }\n";
                *out_ << "        " << emit_fn << "(" << buf << ");\n";
                *out_ << "    }\n";
            }

            *out_ << "    ibex::runtime::Table " << var << ";\n";
            return var;
        }
        case ir::NodeKind::FilterHead: {
            // Fused shape produced by canonicalize R7. Emit as filter + head —
            // the ops layer has no fused primitive.
            const auto& fh = ir::node_cast<ir::FilterHeadNode>(node);
            auto child = emit_node(require_single_child(fh, "FilterHeadNode"));
            auto fvar = fresh_var();
            *out_ << "    auto " << fvar << " = ibex::ops::filter(" << child << ", "
                  << emit_filter_expr(fh.predicate()) << ");\n";
            auto hvar = fresh_var();
            *out_ << "    auto " << hvar << " = ibex::ops::head(" << fvar << ", " << fh.count()
                  << ", {});\n";
            return hvar;
        }
        case ir::NodeKind::FilterTail: {
            // Fused shape produced by canonicalize R8. Emit as filter + tail.
            const auto& ft = ir::node_cast<ir::FilterTailNode>(node);
            auto child = emit_node(require_single_child(ft, "FilterTailNode"));
            auto fvar = fresh_var();
            *out_ << "    auto " << fvar << " = ibex::ops::filter(" << child << ", "
                  << emit_filter_expr(ft.predicate()) << ");\n";
            auto tvar = fresh_var();
            *out_ << "    auto " << tvar << " = ibex::ops::tail(" << fvar << ", " << ft.count()
                  << ", {});\n";
            return tvar;
        }
        case ir::NodeKind::Program: {
            const auto& prog = ir::node_cast<ir::ProgramNode>(node);
            for (const auto& pnode : prog.preamble()) {
                const auto& ec = ir::node_cast<ir::ExternCallNode>(*pnode);
                *out_ << "    (void)" << ec.callee() << "(";
                bool first = true;
                for (const auto& arg : ec.args()) {
                    if (!first)
                        *out_ << ", ";
                    first = false;
                    *out_ << emit_raw_expr(arg);
                }
                *out_ << ");\n";
            }
            return emit_node(prog.main_node());
        }
    }
    throw std::runtime_error("ibex_compile: unknown IR node kind");
}

auto Emitter::emit_join_suffix(const ir::JoinSuffixPolicy& suffix, ir::NullMatch null_match,
                               const ir::JoinExpect& expect, ir::MatchSelection take)
    -> std::string {
    // These sit after `suffix` in the ops signatures, so a non-default one has
    // to spell every earlier argument out even where it is absent.
    const auto multiplicity = [](ir::JoinMultiplicity m) {
        return m == ir::JoinMultiplicity::One ? "ibex::ir::JoinMultiplicity::One"
                                              : "ibex::ir::JoinMultiplicity::Many";
    };
    std::string tail_arg;
    if (expect.asserts_anything() || take != ir::MatchSelection::All) {
        tail_arg = std::string(", ibex::ir::JoinExpect{.left = ") + multiplicity(expect.left) +
                   ", .right = " + multiplicity(expect.right) + "}";
    }
    if (take != ir::MatchSelection::All) {
        const auto* name = take == ir::MatchSelection::First  ? "First"
                           : take == ir::MatchSelection::Last ? "Last"
                                                              : "Any";
        tail_arg += std::string(", ibex::ir::MatchSelection::") + name;
    }
    const std::string null_arg = null_match == ir::NullMatch::Equal
                                     ? ", ibex::ir::NullMatch::Equal"
                                     : (tail_arg.empty() ? "" : ", ibex::ir::NullMatch::Never");
    if (!suffix.present) {
        if (!null_arg.empty()) {
            return ", ibex::ir::JoinSuffixPolicy{}" + null_arg + tail_arg;
        }
        return "";
    }
    return ", ibex::ir::JoinSuffixPolicy{.present = true, .left = \"" + escape_string(suffix.left) +
           "\", .right = \"" + escape_string(suffix.right) + "\"}" + null_arg + tail_arg;
}

auto Emitter::emit_filter_expr(const ir::Expr& expr) -> std::string {
    return std::visit(
        [](const auto& node) -> std::string {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                if (node.lexical) {
                    return "ibex::ops::lexical_ref(\"" + escape_string(node.name) + "\")";
                }
                if (node.side != ir::JoinSide::Any) {
                    return "ibex::ops::filter_col_side(\"" + escape_string(node.name) +
                           "\", ibex::ir::JoinSide::" +
                           (node.side == ir::JoinSide::Left ? "Left" : "Right") + ")";
                }
                return "ibex::ops::filter_col(\"" + escape_string(node.name) + "\")";
            } else if constexpr (std::is_same_v<T, ir::Literal>) {
                return std::visit(
                    [](const auto& v) -> std::string {
                        using V = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<V, std::int64_t>) {
                            return "ibex::ops::filter_int(std::int64_t{" + std::to_string(v) + "})";
                        } else if constexpr (std::is_same_v<V, double>) {
                            return "ibex::ops::filter_dbl(" + format_double(v) + ")";
                        } else if constexpr (std::is_same_v<V, bool>) {
                            return std::string("ibex::ops::filter_bool(") + (v ? "true" : "false") +
                                   ")";
                        } else if constexpr (std::is_same_v<V, std::string>) {
                            return "ibex::ops::filter_str(\"" + escape_string(v) + "\")";
                        } else if constexpr (std::is_same_v<V, Date>) {
                            return "ibex::ops::filter_date(ibex::Date{std::int32_t{" +
                                   std::to_string(v.days) + "}})";
                        } else if constexpr (std::is_same_v<V, DecimalValue>) {
                            return "ibex::ops::filter_decimal(" + decimal_value_cpp(v) + ")";
                        } else {
                            static_assert(std::is_same_v<V, Timestamp>);
                            return "ibex::ops::filter_timestamp(ibex::Timestamp{std::int64_t{" +
                                   std::to_string(v.nanos) + "}})";
                        }
                    },
                    node.value);
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                return "ibex::ops::filter_arith(ibex::ir::ArithmeticOp::" + emit_arith_op(node.op) +
                       ", " + emit_filter_expr(*node.left) + ", " + emit_filter_expr(*node.right) +
                       ")";
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                std::string s = "([]{ std::vector<ibex::ir::Expr> args;";
                for (const auto& arg : node.args) {
                    s += " args.push_back(" + emit_filter_expr(*arg) + ");";
                }
                s += " return ibex::ops::filter_call(\"" + escape_string(node.callee) +
                     "\", std::move(args)); })()";
                return s;
            } else if constexpr (std::is_same_v<T, ir::CompareExpr>) {
                return "ibex::ops::filter_cmp(ibex::ir::CompareOp::" + emit_compare_op(node.op) +
                       ", " + emit_filter_expr(*node.left) + ", " + emit_filter_expr(*node.right) +
                       ")";
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                if (node.op == ir::LogicalOp::And) {
                    return "ibex::ops::filter_and(" + emit_filter_expr(*node.left) + ", " +
                           emit_filter_expr(*node.right) + ")";
                }
                if (node.op == ir::LogicalOp::Or) {
                    return "ibex::ops::filter_or(" + emit_filter_expr(*node.left) + ", " +
                           emit_filter_expr(*node.right) + ")";
                }
                return "ibex::ops::filter_not(" + emit_filter_expr(*node.left) + ")";
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return node.negated
                           ? "ibex::ops::filter_is_not_null(" + emit_filter_expr(*node.operand) +
                                 ")"
                           : "ibex::ops::filter_is_null(" + emit_filter_expr(*node.operand) + ")";
            } else {
                throw std::runtime_error(
                    "ibex_compile: unsupported expression node in filter predicate");
            }
        },
        expr.node);
}

auto Emitter::emit_expr(const ir::Expr& expr) -> std::string {
    return std::visit(
        [&](const auto& node) -> std::string {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                if (node.lexical) {
                    return "ibex::ops::lexical_ref(\"" + escape_string(node.name) + "\")";
                }
                return "ibex::ops::col_ref(\"" + escape_string(node.name) + "\")";
            } else if constexpr (std::is_same_v<T, ir::Literal>) {
                return std::visit(
                    [&](const auto& v) -> std::string {
                        using V = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<V, std::int64_t>) {
                            return "ibex::ops::int_lit(std::int64_t{" + std::to_string(v) + "})";
                        } else if constexpr (std::is_same_v<V, double>) {
                            return "ibex::ops::dbl_lit(" + format_double(v) + ")";
                        } else if constexpr (std::is_same_v<V, bool>) {
                            return "ibex::ops::int_lit(std::int64_t{" + std::string(v ? "1" : "0") +
                                   "})";
                        } else if constexpr (std::is_same_v<V, std::string>) {
                            return "ibex::ops::str_lit(\"" + escape_string(v) + "\")";
                        } else if constexpr (std::is_same_v<V, Date>) {
                            return "ibex::ops::date_lit(ibex::Date{std::int32_t{" +
                                   std::to_string(v.days) + "}})";
                        } else if constexpr (std::is_same_v<V, DecimalValue>) {
                            return "ibex::ops::decimal_lit(" + decimal_value_cpp(v) + ")";
                        } else {
                            static_assert(std::is_same_v<V, Timestamp>);
                            return "ibex::ops::timestamp_lit(ibex::Timestamp{std::int64_t{" +
                                   std::to_string(v.nanos) + "}})";
                        }
                    },
                    node.value);
            } else if constexpr (std::is_same_v<T, ir::BinaryExpr>) {
                return "ibex::ops::binop(ibex::ir::ArithmeticOp::" + emit_arith_op(node.op) + ", " +
                       emit_expr(*node.left) + ", " + emit_expr(*node.right) + ")";
            } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                std::string s = "ibex::ops::fn_call(\"" + escape_string(node.callee) + "\", {";
                bool first = true;
                for (const auto& arg : node.args) {
                    if (!first)
                        s += ", ";
                    first = false;
                    s += emit_expr(*arg);
                }
                s += "})";
                if (!node.named_args.empty()) {
                    s.pop_back();  // remove trailing ')'
                    s += ", {";
                    bool first_named = true;
                    for (const auto& narg : node.named_args) {
                        if (!first_named)
                            s += ", ";
                        first_named = false;
                        s += "ibex::ops::NamedArgExpr{\"" + escape_string(narg.name) + "\", " +
                             emit_expr(*narg.value) + "}";
                    }
                    s += "})";
                }
                return s;
            } else if constexpr (std::is_same_v<T, ir::RankExpr>) {
                std::string s = "ibex::ops::rank_expr({";
                bool first = true;
                for (const auto& key : node.order_keys) {
                    if (!first)
                        s += ", ";
                    first = false;
                    s += "ibex::ir::OrderKey{\"" + escape_string(key.name) + "\", " +
                         std::string(key.ascending ? "true" : "false") + "}";
                }
                s += "}, ibex::ir::RankMethod::";
                switch (node.method) {
                    case ir::RankMethod::Average:
                        s += "Average";
                        break;
                    case ir::RankMethod::Min:
                        s += "Min";
                        break;
                    case ir::RankMethod::Max:
                        s += "Max";
                        break;
                    case ir::RankMethod::First:
                        s += "First";
                        break;
                    case ir::RankMethod::Dense:
                        s += "Dense";
                        break;
                }
                s += ", ibex::ir::RankNaOption::";
                switch (node.na_option) {
                    case ir::RankNaOption::Keep:
                        s += "Keep";
                        break;
                    case ir::RankNaOption::Top:
                        s += "Top";
                        break;
                    case ir::RankNaOption::Bottom:
                        s += "Bottom";
                        break;
                }
                s += ", ";
                s += node.pct ? "true" : "false";
                s += ')';
                return s;
            } else if constexpr (std::is_same_v<T, ir::CompareExpr>) {
                // Boolean-valued nodes are legal in value position too — the
                // interpreter evaluates `flag = x > 1` or `plain = !like(name,
                // "%green%")` into a Bool column. The ops::filter_* helpers are
                // the generic ir::Expr constructors for these nodes despite the
                // name, so a field and a predicate emit the same tree.
                return "ibex::ops::filter_cmp(ibex::ir::CompareOp::" + emit_compare_op(node.op) +
                       ", " + emit_expr(*node.left) + ", " + emit_expr(*node.right) + ")";
            } else if constexpr (std::is_same_v<T, ir::LogicalExpr>) {
                if (node.op == ir::LogicalOp::And) {
                    return "ibex::ops::filter_and(" + emit_expr(*node.left) + ", " +
                           emit_expr(*node.right) + ")";
                }
                if (node.op == ir::LogicalOp::Or) {
                    return "ibex::ops::filter_or(" + emit_expr(*node.left) + ", " +
                           emit_expr(*node.right) + ")";
                }
                return "ibex::ops::filter_not(" + emit_expr(*node.left) + ")";
            } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                return node.negated
                           ? "ibex::ops::filter_is_not_null(" + emit_expr(*node.operand) + ")"
                           : "ibex::ops::filter_is_null(" + emit_expr(*node.operand) + ")";
            }
            throw std::runtime_error("ibex_compile: unknown expression type");
        },
        expr.node);
}

auto Emitter::emit_compare_op(ir::CompareOp op) -> std::string {
    switch (op) {
        case ir::CompareOp::Eq:
            return "Eq";
        case ir::CompareOp::Ne:
            return "Ne";
        case ir::CompareOp::Lt:
            return "Lt";
        case ir::CompareOp::Le:
            return "Le";
        case ir::CompareOp::Gt:
            return "Gt";
        case ir::CompareOp::Ge:
            return "Ge";
    }
    return "Eq";
}

auto Emitter::emit_arith_op(ir::ArithmeticOp op) -> std::string {
    switch (op) {
        case ir::ArithmeticOp::Add:
            return "Add";
        case ir::ArithmeticOp::Sub:
            return "Sub";
        case ir::ArithmeticOp::Mul:
            return "Mul";
        case ir::ArithmeticOp::Div:
            return "Div";
        case ir::ArithmeticOp::Mod:
            return "Mod";
    }
    return "Add";
}

auto Emitter::emit_raw_expr(const ir::Expr& expr) -> std::string {
    return std::visit(
        [&](const auto& node) -> std::string {
            using T = std::decay_t<decltype(node)>;
            if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                // A resource a script bound is the C++ variable that holds it,
                // and a table a script step produced is its variable too (a
                // table argument of a call, bound at its statement).
                if (const auto held = resource_vars_.find(node.name);
                    held != resource_vars_.end()) {
                    return held->second;
                }
                if (const auto model = model_vars_.find(node.name); model != model_vars_.end()) {
                    return model->second;
                }
                if (const auto table = named_tables_.find(node.name);
                    table != named_tables_.end()) {
                    return table->second;
                }
                // A compile-time scalar `let` is emitted as its value. Anything
                // else — a `scalar(...)` deferred `let`, a `let` bound to a
                // computed expression, a `^` lexical binding — is resolved at
                // run time from the scalar registry the generated `main` builds.
                const auto it = compile_time_scalars_.find(node.name);
                if (it == compile_time_scalars_.end()) {
                    if (!runtime_scalar_names_.contains(node.name)) {
                        throw std::runtime_error(
                            "ibex_compile: non-literal argument in extern call: '" + node.name +
                            "'");
                    }
                    return "ibex::ops::scalar_arg(" + emit_expr(expr) + ")";
                }
                return std::visit(
                    [&](const auto& v) -> std::string {
                        using V = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<V, std::monostate>) {
                            throw std::runtime_error(
                                "ibex_compile: null scalar in extern call argument");
                        } else if constexpr (std::is_same_v<V, std::int64_t>) {
                            return std::to_string(v);
                        } else if constexpr (std::is_same_v<V, double>) {
                            return format_double(v);
                        } else if constexpr (std::is_same_v<V, bool>) {
                            return v ? "1" : "0";
                        } else if constexpr (std::is_same_v<V, std::string>) {
                            return "\"" + escape_string(v) + "\"";
                        } else if constexpr (std::is_same_v<V, Date>) {
                            return "ibex::Date{std::int32_t{" + std::to_string(v.days) + "}}";
                        } else if constexpr (std::is_same_v<V, DecimalValue>) {
                            return decimal_value_cpp(v);
                        } else {
                            static_assert(std::is_same_v<V, Timestamp>);
                            return "ibex::Timestamp{std::int64_t{" + std::to_string(v.nanos) + "}}";
                        }
                    },
                    it->second);
            }
            if constexpr (std::is_same_v<T, ir::Literal>) {
                return std::visit(
                    [&](const auto& v) -> std::string {
                        using V = std::decay_t<decltype(v)>;
                        if constexpr (std::is_same_v<V, std::int64_t>) {
                            return std::to_string(v);
                        } else if constexpr (std::is_same_v<V, double>) {
                            return format_double(v);
                        } else if constexpr (std::is_same_v<V, bool>) {
                            return v ? "1" : "0";
                        } else if constexpr (std::is_same_v<V, std::string>) {
                            return "\"" + escape_string(v) + "\"";
                        } else if constexpr (std::is_same_v<V, Date>) {
                            return "ibex::Date{std::int32_t{" + std::to_string(v.days) + "}}";
                        } else if constexpr (std::is_same_v<V, DecimalValue>) {
                            return decimal_value_cpp(v);
                        } else {
                            static_assert(std::is_same_v<V, Timestamp>);
                            return "ibex::Timestamp{std::int64_t{" + std::to_string(v.nanos) + "}}";
                        }
                    },
                    node.value);
            }
            // A computed argument (arithmetic / a nested call over bound
            // scalars): resolve it against the scalar registry at run time,
            // the same way the interpreter evaluates the argument expression.
            return "ibex::ops::scalar_arg(" + emit_expr(expr) + ")";
        },
        expr.node);
}

auto Emitter::emit_agg_func(ir::AggFunc func) -> std::string {
    switch (func) {
        case ir::AggFunc::Sum:
            return "Sum";
        case ir::AggFunc::Mean:
            return "Mean";
        case ir::AggFunc::Min:
            return "Min";
        case ir::AggFunc::Max:
            return "Max";
        case ir::AggFunc::Count:
            return "Count";
        case ir::AggFunc::CountDistinct:
            return "CountDistinct";
        case ir::AggFunc::First:
            return "First";
        case ir::AggFunc::Last:
            return "Last";
        case ir::AggFunc::Median:
            return "Median";
        case ir::AggFunc::Stddev:
            return "Stddev";
        case ir::AggFunc::Ewma:
            return "Ewma";
        case ir::AggFunc::Quantile:
            return "Quantile";
        case ir::AggFunc::Skew:
            return "Skew";
        case ir::AggFunc::Kurtosis:
            return "Kurtosis";
    }
    return "Sum";
}

}  // namespace ibex::codegen
