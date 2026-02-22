#pragma once

#include <ibex/ir/node.hpp>

#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

namespace ibex::codegen {

/// Emits a C++23 source file from an IR node tree.
///
/// The emitted code uses ibex::ops::* for all table operations and can be
/// compiled against the ibex runtime library.
class Emitter {
   public:
    struct Config {
        /// Header files to #include (from extern fn declarations).
        std::vector<std::string> extern_headers;
        /// Source file name shown in the generated comment.
        std::string source_name;
        /// Whether to emit ibex::ops::print() for the final result.
        bool print_result = true;
        /// Emit a self-contained benchmark harness: data is loaded once
        /// outside the timing loop; the query runs bench_warmup + bench_iters
        /// times and prints "avg_ms=X.XXX\n" to stderr.
        bool bench_mode = false;
        int bench_warmup = 3;
        int bench_iters = 10;
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
    std::unordered_map<const ir::Node*, std::string> cached_vars_;

    auto fresh_var() -> std::string;

    /// Pre-emit all ExternCall nodes in the subtree to the current output
    /// stream and cache their variable names in cached_vars_.
    void collect_extern_calls(const ir::Node& node);

    /// Emit code for a node and all its children; returns the variable name
    /// that holds the result.
    auto emit_node(const ir::Node& node) -> std::string;

    /// Emit a FilterPredicate initialiser (inline, no trailing newline).
    static auto emit_predicate(const ir::FilterPredicate& pred) -> std::string;

    /// Emit an Expr expression builder call (inline).
    auto emit_expr(const ir::Expr& expr) -> std::string;

    /// Emit a raw C++ value expression for extern call arguments (literals only).
    static auto emit_raw_expr(const ir::Expr& expr) -> std::string;

    static auto emit_compare_op(ir::CompareOp op) -> std::string;
    static auto emit_arith_op(ir::ArithmeticOp op) -> std::string;
    static auto emit_agg_func(ir::AggFunc func) -> std::string;

    /// Prefix every line in `code` with `spaces` additional spaces.
    static auto indent_code(const std::string& code, int spaces) -> std::string;
};

}  // namespace ibex::codegen
