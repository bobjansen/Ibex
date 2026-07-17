#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/parser/ast.hpp>

#include <expected>
#include <optional>
#include <robin_hood.h>
#include <string>
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
};

struct ScriptPlan {
    std::vector<ir::NodePtr> preamble;
    std::vector<SharedBinding> shared_bindings;
    std::vector<ScriptSink> sinks;
    ir::NodePtr result;
    /// Set when the final expression is a simple identifier.
    std::optional<std::string> result_binding;
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
};

/// Lower a parsed Program into an IR node tree.
/// Returns the IR for the last expression statement.
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
[[nodiscard]] auto lower_script(const Program& program,
                                const ir::SourceSchemas& reader_schemas = {}) -> ScriptPlanResult;

/// Lower a single expression with an external context.
[[nodiscard]] auto lower_expr(const Expr& expr, LowerContext& context) -> LowerResult;

}  // namespace ibex::parser
