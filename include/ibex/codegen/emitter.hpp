// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::codegen {

/// The C++ name generated code uses for an extern function or resource type
/// Ibex calls `name`: `ibex::ext::a::f` for the qualified `a::f`, which a
/// plugin header declares as `f` in `ibex::ext::a`; an unqualified name as is.
[[nodiscard]] auto cpp_extern_name(const std::string& name) -> std::string;

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
        /// Scalars a Script's steps set at run time (`let n = f(...)`, and the
        /// deferred `scalar(<table>)` lets it orders itself): their names, so a
        /// bare reference in an extern-call argument resolves through the
        /// registry, and the registry itself is emitted.
        std::vector<std::string> runtime_scalar_names;
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
        /// The script calls `args::parse`: emit `main(int argc, char** argv)` and
        /// forward the process argv to `IBEX_ARGS` before the query runs.
        bool forward_cli_args = false;
    };

    /// A script whose statements have effects, in the order they run.
    ///
    /// `emit(root)` takes one query plus constants, so a program that writes a
    /// file or calls an extern for its effect has no way to say "this, then
    /// that". A Script does: each step runs where its statement was, and a
    /// plan's `Scan` of a shared binding resolves to the table that binding's
    /// step produced. The plans are borrowed; the caller keeps them alive.
    struct Script {
        struct Step {
            enum class Kind : std::uint8_t {
                /// Materialize `plan` once, under `name`; later plans scan it.
                SharedBinding,
                /// Run `plan`, then `callee(<that table>, args...)`.
                Sink,
                /// `plan`, an ExternCall node, run for its effect; its result
                /// is bound to `bind` when set and discarded otherwise.
                Call,
                /// A `let n = scalar(<table>)` binding, evaluated here rather
                /// than before every other step.
                DeferredScalar,
                /// `let name = other;` where `other` is a resource: the same
                /// connection under a second name.
                ResourceAlias,
                /// `name` stops being a resource here: its variable lets go of
                /// the connection, which closes if no other name holds it.
                ResourceUnbind,
            };
            Kind kind = Kind::Call;
            std::string name;
            const ir::Node* plan = nullptr;
            /// Sink only.
            std::string callee;
            std::vector<ir::Expr> args;
            /// Sink only: the binding the source passed as the table, so a
            /// later `result` naming it reuses the table rather than rerunning.
            std::optional<std::string> input_binding;
            /// Sink and Call: the scalar the call's result is bound to.
            std::optional<std::string> bind;
            /// Call only: the bound result is a resource (a variable of the
            /// program), not a scalar in the registry.
            bool bind_resource = false;
            /// ResourceAlias: the resource `name` becomes another name for.
            std::string alias_of;
            /// DeferredScalar only; borrowed.
            const ir::DeferredScalarBinding* deferred = nullptr;
        };
        std::vector<Step> steps;
        const ir::Node* result = nullptr;
        std::optional<std::string> result_binding;
        /// A function whose value is not a table: the call that is its value, or
        /// the expression (a name, a literal). Otherwise `result` is the value.
        const ir::Node* return_call = nullptr;
        const ir::Expr* return_expr = nullptr;

        /// A program function: a `fn` that runs statements (it takes, opens or
        /// returns a connection), emitted as a C++ function ahead of `main`. Its
        /// body is a Script of its own; a call to it is an ExternCall of its name.
        struct Function;
        std::vector<std::unique_ptr<Function>> functions;
    };

    /// Emit a complete C++ translation unit to `out`.
    ///
    /// The last IR node's result is passed to ibex::ops::print().
    void emit(std::ostream& out, const ir::Node& root, const Config& config);

    /// Emit a translation unit that runs `script`'s steps in order, then
    /// produces its result. Benchmark mode is not supported for a script.
    void emit(std::ostream& out, const Script& script, const Config& config);

    void emit(std::ostream& out, const ir::Node& root) { emit(out, root, Config{}); }

   private:
    std::ostream* out_{nullptr};
    /// Names of the program's own functions, which a call spells with a prefix.
    robin_hood::unordered_set<std::string> user_functions_;
    /// Inside a function body, a scalar the script binds goes in the function's
    /// own scope rather than the program's registry.
    bool in_function_{false};
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

    /// The C++ variable holding each fitted model a Script's steps bound, by name,
    /// and the one the Model node just emitted declared.
    robin_hood::unordered_map<std::string, std::string> model_vars_;
    std::string last_model_;
    /// The C++ variable holding each resource a Script's steps bound, by the
    /// name the script knows it by. A name rebound gets a fresh variable; the
    /// old one is released.
    robin_hood::unordered_map<std::string, std::string> resource_vars_;
    /// Tables produced by a Script's shared-binding steps, by binding name.
    robin_hood::unordered_map<std::string, std::string> named_tables_;

    /// Everything before the query: includes, `main`/entry-point opening, and
    /// the scalar registry. `emit_footer` closes what this opens.
    void emit_header(std::ostream& out, const Config& config, const Script* script = nullptr);
    /// The steps of a script body, in order; the variable holding its table
    /// result, or empty when its value is not a table.
    auto emit_script_steps(const Script& script) -> std::string;
    void emit_functions(const Script& script);
    void emit_function(const Script::Function& fn);
    /// Store a scalar the script bound: in the program's registry, or in the
    /// scope of the function being emitted.
    void emit_scalar_store(const std::string& name, const std::string& value);
    /// How a call of `callee` is spelled: the program's own functions are prefixed
    /// so they cannot collide with anything the generated code names.
    [[nodiscard]] auto callee_name(const std::string& callee) const -> std::string;
    /// Run a deferred scalar's subplans, extract each, then evaluate the
    /// residual expression: the same order and semantics as
    /// runtime::materialize_deferred_scalar_bindings.
    void emit_deferred_scalar(const ir::DeferredScalarBinding& binding);
    void emit_query(std::ostream& out, const ir::Node& root, const Config& config);
    void emit_footer(std::ostream& out, const Config& config);

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

/// See Script::functions.
struct Emitter::Script::Function {
    struct Param {
        enum class Kind : std::uint8_t { Resource, Table, Scalar };
        std::string name;
        /// The C++ type, for a Resource or a Scalar; a Table is always
        /// `ibex::runtime::Table`.
        std::string cpp_type;
        Kind kind = Kind::Scalar;
    };
    std::string name;
    std::vector<Param> params;
    std::string return_type;
    Script body;
};

}  // namespace ibex::codegen
