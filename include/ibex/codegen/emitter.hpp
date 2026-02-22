#pragma once

#include <ibex/ir/node.hpp>

#include <iostream>
#include <string>
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
    };

    /// Emit a complete C++ translation unit to `out`.
    ///
    /// The last IR node's result is passed to ibex::ops::print().
    void emit(std::ostream& out, const ir::Node& root, const Config& config = {});

   private:
    std::ostream* out_{nullptr};
    int tmp_counter_{0};

    auto fresh_var() -> std::string;

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
};

}  // namespace ibex::codegen
