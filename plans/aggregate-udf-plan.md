# Aggregate UDFs

## Goal

Let user-defined functions participate in aggregation, in two staged features
(see the follow-up note in `plans/udf-dataframe-plan.md`):

1. **Scalar UDFs inside aggregate / clause argument expressions** — e.g.
   `trades[select { avg_adj = mean(adjust(price)) }, by symbol]`, where `adjust`
   is an ordinary scalar `fn`.
2. **True aggregate UDFs** (`agg fn`) — multi-column user aggregates evaluated
   per group, e.g.

   ```ibex
   agg fn weighted_mean(p: Series<Float64>, w: Series<Float64>) -> Float64 {
       sum(p * w) / sum(w)
   }
   trades[select { wavg = weighted_mean(price, qty) }, by symbol]
   ```

## Current State

- **`AggSpec`** (`ir/node.hpp`) = `{ AggFunc func, ColumnRef column, alias,
  param }`: a single built-in function over a single column. No representation
  for a user function or for multiple column arguments.
- **Computed aggregate args already work** for built-ins: lowering rewrites
  `mean(price + fee)` into a pre-`update` (`_agg0 = price + fee`) followed by an
  `Aggregate` over `_agg0` (canonicalize R20 prunes the scan). So the machinery
  to materialize an intermediate column before aggregation exists.
- **Scalar UDFs are evaluated at statement level** by `call_user_function`
  (`repl.cpp`) against a `FunctionRegistry` (`unordered_map<string,
  parser::FunctionDecl>`). Bodies are evaluated over the AST.
- **Clause expressions are lowered to `ir::Expr`** and evaluated by the runtime
  interpreter against **built-ins / externs only** — user functions are not
  resolved there, so a UDF call inside a clause currently fails.
- **`aggregate_table`** (`interpreter.cpp`) partitions the input by the group
  keys and computes each `AggSpec` per group.

## The central problem: a layering boundary

UDF bodies live in the **parser/REPL** layer; clause and aggregate evaluation
live in the **runtime** layer, which has no notion of user functions. Both
features must cross that boundary. Two approaches:

- **(A) Inline at lower time.** Substitute the UDF body (parameters → argument
  expressions) into the clause IR during lowering, producing plain `ir::Expr` /
  IR the runtime already understands. *Pros:* no runtime- or codegen-layer
  change; works on every backend for free. *Cons:* only bodies that reduce to a
  single expression inline cleanly (bodies with `let`s need expression folding;
  recursion can't inline).
- **(B) Runtime callback.** Thread the function registry (or an evaluation
  callback) into the interpreter so a `CallExpr` can invoke a UDF. *Pros:*
  arbitrary bodies. *Cons:* runtime depends on AST evaluation; codegen still
  needs a separate story.

**Recommendation: (A) inlining.** Most scalar helpers and `agg fn` bodies are
single expressions; inlining keeps the runtime and codegen untouched and is the
smallest sound step. Restrict to inlinable bodies initially and error clearly
otherwise. Revisit (B) only if non-inlinable bodies become important.

## Feature 1 — scalar UDFs in clause / aggregate args

A scalar `fn f(x: T, ...) -> U { <expr> }` call inside a clause expression is
**inlined during lowering**: clone the body expression and substitute each
parameter with its (already-lowered) argument expression. The result is a pure
`ir::Expr`, so:

- `mean(adjust(price))` → `adjust` inlines to `price * 1.01` (say) →
  `mean(price * 1.01)` → existing pre-`update` materialization handles the rest.
- `filter` / `select` / `update` computed expressions get scalar UDF calls for
  free, which also advances udf-dataframe Phase 5.

Constraints (initial): the body must reduce to one expression (single return,
no `let`; reject `let`-bodies with a clear error for now). Argument arity/type
checking reuses the existing call-binding diagnostics. Result type flows through
the schema pass's expression inference (already built).

## Feature 2 — true aggregate UDFs (`agg fn`)

### Syntax & declaration
`agg fn <name>(<param>: Series<T>, ...) -> <scalar_type> { <body> }`. An `agg`
modifier marks the declaration; parameters are `Series<T>` (the group's column
slices); the body is a scalar expression over those Series using
reduction/aggregate built-ins and elementwise ops.

### IR representation
Extend the aggregate to carry user aggregates alongside built-in `AggSpec`s —
either a sibling `AggUdfSpec { string udf_name; std::vector<ColumnRef> columns;
std::string alias; }` or a variant inside the aggregate node. Key difference
from `AggSpec`: **multiple column arguments**.

### Evaluation model (reuses existing machinery)
Per group, slice each argument column to the group's rows, bind them to the
parameters as a tiny per-group table `T = { p: slice, w: slice, ... }`, and
evaluate the body as a **scalar query over `T`** — `sum(p * w) / sum(w)` is just
a scalar aggregate over a computed column, which the interpreter already does.
Collect one scalar per group into the result column.

This reuses `aggregate_table`'s partitioning and the existing scalar/aggregate
evaluation; the new work is the per-group "bind slices → evaluate body → scalar"
step. (Performance: interpreting the body per group is acceptable for v1;
vectorising/compiling it is a later optimisation.)

### Semantics to define
- **Null handling** — the body's reductions follow their existing null rules;
  decide how nulls in the Series args propagate.
- **Empty group** — a group with zero rows (can it occur? `by` groups are
  non-empty; resample buckets may be empty): define the result (null vs error).
- **Row order within a group** — does the body see the group's rows in input
  order (matters for order-dependent reductions)? Default: input order.
- **Return type** — from the `-> <scalar_type>` signature → the output column's
  type; feeds the schema pass (extend the aggregate case to type a UDF
  aggregate by its declared return type).

### Backends
- **Interpreter** — per-group evaluation as above.
- **Chunked / streaming** — an arbitrary body is **not incrementalisable**, so
  the chunked planner treats a UDF aggregate as a full-materialisation breaker
  (buffer the whole group); document the non-streaming limitation.
- **Codegen** — emit the per-group invocation; if the body is lowered to an IR
  sub-pipeline this can reuse the emitter. Later stage.

## Staging

1. **Scalar UDF inlining in clause expressions.** — **DONE.** A scalar
   user-function call in a `select`/`update` computed expression is inlined at
   lowering time (`inline_scalar_udf` in `lower.cpp`): parameters are bound to
   the lowered argument expressions via an inline-scope stack and the body is
   lowered with those substitutions; calls nest. Only single-expression,
   scalar-returning bodies inline; recursion and multi-statement bodies are
   lower-time errors. `FunctionDecl`s reach the lowerer via
   `LowerContext::functions` (REPL) / collected in `lower_program` (compile
   path). Because the existing aggregate lowering already materialises computed
   args, this gives **feature 1 in aggregate args for free**
   (`mean(adjust(price))` works — stage 2 below). Tested in test_e2e/test_repl;
   SPEC §10 updated.

   *Not yet:* `filter` predicates use a separate, restricted `FilterExpr`
   sublanguage (a `static` lowering path), so scalar UDFs aren't inlined there
   yet — deferred follow-up.
2. **Feature 1 in aggregate args** — **DONE** (folded into stage 1 via the
   existing computed-arg materialisation; `mean(adjust(price)) by sym` verified
   in test_e2e).
3. **`agg fn` syntax + declaration** (parser/AST) and the aggregate IR
   representation (`AggUdfSpec` / variant, multi-column args).
4. **Interpreter per-group evaluation** (bind slices → evaluate body → scalar).
5. **Semantics** (null / empty-group / order) + **return typing** + schema-pass
   integration.
6. **Chunked path** — UDF aggregates as a materialising breaker.
7. **Codegen** for UDF aggregates.

## Non-Goals (initial waves)

- Incremental / streaming aggregate UDFs.
- `agg fn` bodies that cannot reduce to a scalar expression (multi-statement /
  stateful) — handled later if needed.
- Window / rolling UDFs (a separate feature).
- Approach (B) runtime callback — only if inlining proves insufficient.

## Open Questions

- Declaration syntax: `agg fn name(...)` vs `fn name(...) agg` vs an attribute.
- Empty-group result: null, error, or a body-defined default?
- Inlining scalar UDFs with `let` bodies — fold the lets into the expression, or
  defer those bodies to a later stage?
- Per-group body interpretation cost — acceptable for v1; what's the
  optimisation path (vectorise, or compile the body once and apply per group)?
- Does an `agg fn` compose inside another aggregate or only at the top of a
  `select`/`by`? (Recommend: only as a top-level aggregate in `select`, like
  built-ins.)

## Related

- [[project-execution-roadmap]] — the chunked/streaming and codegen stages
  intersect the execution roadmap.
- `plans/udf-dataframe-plan.md` — scalar/table UDFs; Phase 5 (clause-expression
  integration) is advanced by Feature 1 here.
