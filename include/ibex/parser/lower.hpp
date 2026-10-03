// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/ast.hpp>

#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::parser {

struct LowerError {
    std::string message;
};

using LowerResult = std::expected<ir::NodePtr, LowerError>;

/// A table-consuming top-level effect separated from the relational DAG it
/// consumes.  Keeping this out of ProgramNode is essential for whole-script
/// planning: the input can be optimized with the rest of the script before
/// the executor schedules the sink in source order.
struct ScriptSink {
    std::string callee;
    ir::NodePtr input;
    std::vector<ir::Expr> args;
    /// Set when the source syntax passed a simple `let` binding to the sink.
    /// The batch executor uses this to avoid evaluating the final result twice.
    std::optional<std::string> input_binding;
    /// Set for `let n = sink(df, ...);`: the name the sink's scalar result is
    /// bound to. The batch executor does not bind it, so it declines such scripts.
    std::optional<std::string> bind;
    /// Index of the sink's statement in the program. Sinks and shared bindings
    /// run in `position` order, so a binding that reads a file never moves
    /// above an earlier statement that writes it.
    std::size_t position = 0;
};

/// A `let` binding whose plan the executor materializes exactly once, in
/// declaration order, before any sink or result plan runs. Chosen by the
/// lowerer when a binding is referenced from several table positions and its
/// subtree contains real computation (a join, aggregate, sort, …): cloning
/// such a subtree per reference would re-run that computation per consumer.
/// References to the binding lower to a Scan of its name; the executor must
/// place the materialized table in the registry under that name.
struct SharedBinding {
    std::string name;
    ir::NodePtr plan;
    /// Index of the `let` statement in the program: the executor materializes
    /// the binding after every sink with a smaller `position` and before every
    /// sink with a larger one.
    std::size_t position = 0;
};

/// What a preamble call's result is bound to: a scalar, which later queries read
/// through the scalar registry, or a resource (`let db = adbc::connect(...)`),
/// which is a variable of the program that only resource parameters take.
struct CallBind {
    std::string name;
    bool resource = false;
    /// A temporary the lowerer made for a call nested in another's argument:
    /// it runs before the statement's own bindings and call.
    bool hoisted = false;
};

/// A statement about a resource binding that is not a call: `let b = a;` makes
/// `b` the same connection as `a`, and rebinding `a` to something that is not a
/// resource releases it. A binding's name lives in one place, so a name that
/// stops being a resource must say so.
struct ResourceStep {
    enum class Kind : std::uint8_t { Alias, Unbind };
    Kind kind = Kind::Alias;
    /// Alias: the new name. Unbind: the name being released.
    std::string name;
    /// Alias: the resource it names.
    std::string source;
    std::size_t position = 0;
};

struct FunctionPlan;

struct ScriptPlan {
    std::vector<ir::NodePtr> preamble;
    /// Statement index of each `preamble` call, parallel to it. A consumer that
    /// runs the preamble up front (the REPL declines such scripts) can ignore
    /// it; one that runs the script in order interleaves the calls with the
    /// sinks and shared bindings by this index.
    std::vector<std::size_t> preamble_positions;
    /// Parallel to `preamble`: the name a call's scalar result is bound to
    /// (`let n = f(...);`), if any.
    std::vector<std::optional<CallBind>> preamble_binds;
    /// Resource aliases and releases, each at its statement.
    std::vector<ResourceStep> resource_steps;
    std::vector<SharedBinding> shared_bindings;
    std::vector<ScriptSink> sinks;
    ir::NodePtr result;
    /// Set when the final expression is a simple identifier.
    std::optional<std::string> result_binding;
    /// A function body whose value is not a table (a connection, a scalar): the
    /// call that is its value, or the expression (a name or a literal). Exactly
    /// one of `result`, `return_call`, `return_expr` is set for a function.
    ir::NodePtr return_call;
    std::optional<ir::Expr> return_expr;
    /// The `fn`s of the program that take, open or return a resource: they run
    /// statements, so they cannot be inlined into a plan. Each is lowered as a
    /// small script of its own, emitted as a function of the program.
    std::vector<std::unique_ptr<FunctionPlan>> functions;
};

/// A resource `fn`, lowered. `body` holds its statements and its value.
struct FunctionPlan {
    const FunctionDecl* decl = nullptr;
    ScriptPlan body;
};

using ScriptPlanResult = std::expected<ScriptPlan, LowerError>;

struct LowerContext {
    robin_hood::unordered_map<std::string, ir::NodePtr> bindings;
    robin_hood::unordered_map<std::string, std::vector<std::string>> compile_time_lists;
    /// Names of extern functions whose return type is DataFrame/TimeFrame.
    /// Populate before calling lower_expr so that tuple-LHS RHS expressions
    /// that call table-returning externs are lowered correctly.
    robin_hood::unordered_set<std::string> table_externs;
    /// Optional declarations for table-returning externs. When present, named
    /// arguments and defaults are bound before lowering to ExternCall IR.
    robin_hood::unordered_map<std::string, const ExternDecl*> table_extern_decls;
    /// Names of extern functions whose first argument is a DataFrame.
    /// Populate before calling lower_expr so Stream sink calls can be validated.
    robin_hood::unordered_set<std::string> sink_externs;
    /// All in-scope lexical binding names (scalars, columns, models, functions,
    /// compile-time lists, table bindings). Used to suppress false positives when
    /// statically validating column references in `filter`/computed expressions:
    /// a bare name there may resolve to one of these rather than a column. A
    /// superset is safe. When empty, expression-level reference checking is off.
    robin_hood::unordered_set<std::string> lexical_names;
    /// Schemas of in-scope table bindings, keyed by binding name, so a reference
    /// to a let-bound table (which lowers to a `ScanNode`) carries its schema
    /// into the current expression's static checks. Populated by the REPL from
    /// the runtime tables registry; entries are exact (closed) schemas.
    ir::SourceSchemas source_schemas;
    /// Scalar user-function declarations, keyed by name. Calls to these inside
    /// clause expressions are inlined during lowering. Populated by the REPL
    /// from its function registry; the whole-program `lower()` collects them
    /// from the program's `fn` statements.
    robin_hood::unordered_map<std::string, const FunctionDecl*> functions;
    /// Lower a table argument of an extern call (`adbc::write(db, df, ...)`'s `df`)
    /// as a binding of its own that the argument names, the way a script does.
    /// Set by a caller that only wants to know whether an expression is a table
    /// (the scalar-binding classifier); the bindings made are dropped with the
    /// lowerer.
    bool table_args_as_bindings = false;
};

/// Lower a parsed Program into an IR node tree.
/// Returns the IR for the last expression statement.
using CallPredicate = std::function<bool(std::string_view)>;

/// True when any call anywhere in `expr`, including inside clauses and
/// nested blocks, has a callee that `matches`.
[[nodiscard]] auto contains_call_if(const Expr& expr, const CallPredicate& matches) -> bool;
[[nodiscard]] auto clause_contains_call_if(const Clause& clause, const CallPredicate& matches)
    -> bool;

[[nodiscard]] auto lower(const Program& program) -> LowerResult;

/// Lower a complete script while preserving table-consuming extern calls as
/// explicit effects instead of forcing them into the relational result tree.
/// The caller schedules `preamble`, `sinks`, and `result`; the latter can be
/// passed through whole-script optimization before any source is materialized.
/// Lower a whole script into a batch-executable plan.
///
/// `reader_schemas` supplies the schemas of reader call sites, keyed by
/// `ir::extern_call_site_key`. It matters more than it looks: join filter
/// pushdown runs inside this function, because it must precede canonicalize
/// (which fuses `Filter(Join(...))` and cannot tell which side owns a column) --
/// and without reader schemas that pass is blind, pushing a conjunct only when
/// its join side happens to be a `Project` that describes itself. Passing them
/// is what lets a naturally-written join push its filters at all.
/// `prelude` holds programs whose declarations are in scope for `program` but
/// whose statements are not its own — the stub an `import` names. They are kept
/// separate rather than spliced in because a `Stmt` owns move-only expressions
/// and so cannot be copied into a combined program; the caller keeps them alive
/// for the call. Only declarations are read from them, so a prelude carrying
/// anything else contributes nothing.
[[nodiscard]] auto lower_script(const Program& program,
                                const ir::SourceSchemas& reader_schemas = {},
                                std::span<const Program* const> prelude = {}) -> ScriptPlanResult;

/// Lower a single expression with an external context.
[[nodiscard]] auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult;

}  // namespace ibex::parser
