// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <robin_hood.h>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::codegen {

/// Emits a C++23 source file from an IR node tree.
///
/// The emitted code uses ibex::ops::* for all table operations and can be
/// compiled against the ibex runtime library.
class Emitter {
   public:
    struct Config {
        // Must stay identical to ibex::runtime::ScalarValue and
        // ibex::parser::ScalarValue. Leading std::monostate is the null
        // alternative; codegen support for emitting null scalar bindings is a
        // later slice (plans/parse-args-and-nullable-scalars-plan.md).
        using ScalarValue = std::variant<std::monostate, std::int64_t, double, bool, std::string,
                                         Date, Timestamp, DecimalValue>;

        /// Header files to #include (from extern fn declarations).
        std::vector<std::string> extern_headers;
        /// Source file name shown in the generated comment.
        std::string source_name;
        /// Scalar bindings captured from `let` statements and initialized
        /// before the emitted query executes.
        std::vector<std::pair<std::string, ScalarValue>> scalar_bindings;
        /// Scalar `let`s whose value is a `scalar(<table>)` subquery: their
        /// subplans run and the value is extracted into the registry at run
        /// time, in order, after `scalar_bindings` and before the query.
        std::vector<ir::DeferredScalarBinding> deferred_scalar_bindings;
        /// Whether to emit ibex::ops::print() for the final result.
        bool print_result = true;
        /// Emit a self-contained benchmark harness: data is loaded once
        /// outside the timing loop; the query runs bench_warmup + bench_iters
        /// times and prints "avg_ms=X.XXX\n" to stderr.
        bool bench_mode = false;
        int bench_warmup = 3;
        int bench_iters = 10;
        /// Emit a callable Table-returning entry point instead of main().
        bool table_entry_point = false;
        std::string entry_point_name = "ibex_generated_execute";
        /// The script calls `parse_args`: emit `main(int argc, char** argv)` and
        /// forward the process argv to `IBEX_ARGS` before the query runs.
        bool forward_cli_args = false;
    };

    /// Emit a complete C++ translation unit to `out`.
    ///
    /// The last IR node's result is passed to ibex::ops::print().
    void emit(std::ostream& out, const ir::Node& root, const Config& config);

    void emit(std::ostream& out, const ir::Node& root) { emit(out, root, Config{}); }

   private:
    std::ostream* out_{nullptr};
    int tmp_counter_{0};
    /// Cache of nodes already emitted (used in bench mode to avoid re-emitting
    /// ExternCall nodes inside the timing loop).
    robin_hood::unordered_map<const ir::Node*, std::string> cached_vars_;
    /// When emitting a stream transform, holds the C++ variable name that
    /// substitutes for `ScanNode("__stream_input__")` in the transform IR.
    std::string stream_scan_var_;
    /// Compile-time scalar `let` values, keyed by name. A reference to one in an
    /// extern-call argument position emits the literal value (extern calls in
    /// generated code take plain C++ arguments, not a scalar registry lookup).
    robin_hood::unordered_map<std::string, Config::ScalarValue> compile_time_scalars_;
    /// Names of `scalar(...)` deferred `let` bindings. A bare reference to one in
    /// an extern-call / row-count argument position emits a run-time scalar
    /// registry lookup (`ibex::ops::scalar_arg`); a bare reference to any other
    /// unbound name there is still a hard error.
    robin_hood::unordered_set<std::string> runtime_scalar_names_;

    auto fresh_var() -> std::string;

    /// Pre-emit all ExternCall nodes in the subtree to the current output
    /// stream and cache their variable names in cached_vars_.
    void collect_extern_calls(const ir::Node& node);

    /// Emit code for a node and all its children; returns the variable name
    /// that holds the result.
    auto emit_node(const ir::Node& node) -> std::string;

    /// Emit a boolean predicate expression as nested ibex::ops::filter_* builder
    /// calls (inline).
    static auto emit_filter_expr(const ir::Expr& expr) -> std::string;

    /// Emit the trailing `, ibex::ir::JoinSuffixPolicy{...}` argument for a
    /// join, or nothing when the join carries no clause. Dropping it would let
    /// a transpiled program reject a collision the interpreter resolves.
    static auto emit_join_suffix(const ir::JoinSuffixPolicy& suffix,
                                 ir::NullMatch null_match = ir::NullMatch::Never,
                                 const ir::JoinExpect& expect = {},
                                 ir::MatchSelection take = ir::MatchSelection::All) -> std::string;

    /// Emit an Expr expression builder call (inline).
    auto emit_expr(const ir::Expr& expr) -> std::string;

    /// Emit a raw C++ value expression for extern call arguments (literals only).
    auto emit_raw_expr(const ir::Expr& expr) -> std::string;

    static auto emit_compare_op(ir::CompareOp op) -> std::string;
    static auto emit_arith_op(ir::ArithmeticOp op) -> std::string;
    static auto emit_agg_func(ir::AggFunc func) -> std::string;

    /// Prefix every line in `code` with `spaces` additional spaces.
    static auto indent_code(const std::string& code, size_t spaces) -> std::string;
};

}  // namespace ibex::codegen
