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

   *Was deferred:* filter predicates. After the `FilterExpr` → `ir::Expr`
   unification (`plans/unify-filter-expr-plan.md` complete) filter predicates
   lower through the same `lower_expr_to_ir`, so scalar-UDF inlining works in
   filter for free. Verified: `Table{x=[1,2,3,4]}[filter over_threshold(x)]`
   with `fn over_threshold(x: Int64) -> Bool { x > 2; }` evaluates correctly.
2. **Feature 1 in aggregate args** — **DONE** (folded into stage 1 via the
   existing computed-arg materialisation; `mean(adjust(price)) by sym` verified
   in test_e2e).

3. **`agg fn` via inference + AST body inlining (Feature 2)** — chosen
   approach: no new keyword. A `fn` whose parameters are all `Series<T>` and
   whose body contains aggregate calls is an **aggregate UDF**. At lowering
   time the call is treated like a scalar UDF call *but inlined at the AST
   level* before the aggregate detector runs: `weighted_mean(price, qty)` with
   body `sum(p*w) / sum(w)` substitutes `p`/`w` and produces
   `sum(price*qty) / sum(qty)`, which the existing select-aggregate machinery
   already lowers (two `AggSpec`s + an arithmetic `update`/project of the temp
   aliases). Because the body's aggregates become plain built-ins, **no new IR
   node, no `AggUdfSpec`, no per-group interpreter path, no schema-pass
   change** are needed. Return type flows from the body's existing inference.

   Constraints: body must reduce to a single expression composed of built-in
   aggregate calls and arithmetic over them (mirrors the scalar-UDF
   single-expression-body rule). Recursion is rejected as it is for scalar
   UDFs. A Series-param fn whose body is purely scalar (no aggregates) still
   inlines via the scalar path — the rule selects the aggregate path only when
   the body actually contains an aggregate.

4. **Chunked path** — once `agg fn` lands, the existing chunked planner picks
   it up unchanged (the lowered body is plain built-in aggregates), so the
   "materialising breaker" caveat from earlier drafts no longer applies for
   inlinable bodies.
5. **Codegen** — same: the emitter sees only built-in `AggSpec`s after
   inlining, so existing codegen handles `agg fn` for free.
6. **Future (deferred)**: bodies with non-aggregate Series operations that
   aren't reducible to "arithmetic of aggregates" (e.g. an `agg fn` that
   wants to call a custom non-built-in reduction). These would need the
   richer `AggUdfSpec` + per-group interpreter path described in earlier
   drafts of this plan.

## Follow-ups (raised after stage 3 landed)

These extend the agg-UDF surface area. None are required for the basic
`weighted_mean(price, qty)` case to work, but each unblocks a natural
ergonomic that users will reach for.

### F1 — `update { agg(...) }, by` broadcasting — **DONE**

`update { wavg = sum(p*w) / sum(w) }, by sym` (and agg-UDFs that inline to
that shape) now broadcast a per-group scalar across each group's rows. The
fix was at the runtime layer rather than the IR/lowering layer: extend
`broadcast_aggregate_column` (runtime) to recognise compound aggregate
expressions (any expression that contains an aggregate call), evaluate
each aggregate sub-call against the per-group slice via `aggregate_table`
(materialising the arg as a temp column when it isn't a bare ColumnRef),
compose intermediate results via the existing column arithmetic on 1-row
columns (so int/double promotion matches the column path), and broadcast
the final scalar to the group's rows.

No new IR node, no `lower_update` change — the existing per-group slice
loop in `grouped_update_table` already handles broadcasting, this just
extends what it recognises as "broadcast-able". `e2e/[update_by]` covers
both the inline compound aggregate and the agg-UDF cases.

### F2 — mixed scalar + Series parameters — **DONE**

`fn wm_floored(p: Series<Float64>, w: Series<Float64>, floor: Float64) -> Float64 { pmax(sum(p*w)/sum(w), floor); }`
now works in `select`/`update` contexts.

Two coordinated changes landed in `lower.cpp`:

1. `aggregate_udf_body` accepts *at least one* `Series<T>` parameter plus any
   number of scalar parameters (return type still scalar, body still contains
   at least one built-in aggregate). Scalar args substitute into the body via
   the existing `inline_agg_udf_body` AST substitution path.
2. `lower_agg_expr`'s `CallExpr` arm gained a generic-scalar-call fallback:
   if the callee is not a built-in aggregate and not an aggregate UDF, lower
   each argument in agg-expression context (so nested aggregate calls reduce
   to `AggSpec` temp column refs) and emit an `ir::CallExpr`, which the
   trailing `update` evaluates as a post-aggregate scalar expression. Named
   args are forwarded through the same path.

Tested in `test_e2e` with both the inline form (`pmax(sum(p*q)/sum(q), 2.0)`)
and the UDF form (`wm_floored(price, qty, 2.0)`), each grouped by `sym`.

### F3 — multi-statement bodies (`let` first, then control flow)

Tier 1 — **`let` bodies** — **DONE.** Bodies of the shape
`let x = expr; ...; final_expr` (any number of lets followed by a single
trailing expression) now inline in both scalar and aggregate UDF positions.
A shared `inlinable_body_shape` helper recognises the shape;
`inline_scalar_udf` extends the IR-level inline scope with each let's
lowered RHS in order; `inline_agg_udf_body` folds let RHSes through the
AST-level substitution map and the final expression substitutes once at the
end. Aggregate-UDF inference looks at both let RHSes and the trailing
expression for built-in aggregate calls, and `expr_contains_builtin_aggregate`
propagates "is aggregate" through agg-UDF calls so a body like
`let total = sum(p); total;` routes through the aggregate path even though
the trailing expression itself has no aggregate call. SPEC §10 updated.

Tier 2 — **value-selecting control flow** (`if cond then a else b`,
`case`/`switch`). **Not in scope for this plan, but not ruled out as a
language feature.** Pure value-selection (the SQL `CASE WHEN` shape) is
expressible as a vectorizable built-in (e.g. `where(cond, a, b)`) rather
than as language-level control flow inside a UDF body. Whether to expose it
as a built-in, as a restricted expression form, or not at all is a separate
language design call — see the framing note below.

Tier 3 — **statement-level / effectful control flow** (loops, early return,
mutation, recursion, anything that can run arbitrary code in one branch and
terminate or diverge in another) is **out of scope by design**. See the
framing note below.

**Framing note (why "no control flow" in Ibex proper):** the load-bearing
reason is *not* vectorization — element-wise conditionals vectorize fine
(SQL `CASE WHEN`, Polars `when/then/otherwise`, NumPy `where` all do this).
The real reasons are:

1. **Pure-expression relational DSL.** Ibex is a query language, not a host
   language. Keeping bodies to expressions (plus `let`) makes them map
   cleanly to SQL/relational algebra, to pushdown into ADBC backends, and to
   the codegen path. Statement-level control flow doesn't translate.
2. **Laziness, fusion, planning.** Pure expressions reorder, fuse, and
   pushdown without semantic checks. Effectful control flow blocks all of
   that.
3. **Bounded power on purpose.** Too much expressive power is a footgun for
   users: the moment UDF bodies can loop, mutate, or call out arbitrarily,
   the planner can't reason about them, the user writes row-at-a-time code
   that defeats the columnar model, and the cost model goes opaque. The
   restricted surface is a feature.

So the spectrum is: **pure value-selection** (vectorizable, relational-friendly
— possible as a built-in) → **simple multi-way value selection**
(`case`/`switch` over a value — also vectorizable, but adds language surface)
→ **arbitrary statement-level / effectful control flow** (breaks all three
properties above — firmly out).

## Non-Goals (initial waves)

- Incremental / streaming aggregate UDFs.
- `agg fn` bodies that cannot reduce to a scalar expression (multi-statement /
  stateful) — F3 tier 1 (`let`) is in scope; tier 2 (value-selecting control
  flow) is not in this plan but may land as a `where`-style built-in later;
  tier 3 (statement-level / effectful control flow) is out of scope by design
  — Ibex is a pure-expression relational DSL.
- Window / rolling UDFs (a separate feature).
- Approach (B) runtime callback — only if inlining proves insufficient.

## Open Questions

- Empty-group result: null, error, or a body-defined default?
- Per-group body interpretation cost — acceptable for v1; what's the
  optimisation path (vectorise, or compile the body once and apply per group)?
- Does an `agg fn` compose inside another aggregate or only at the top of a
  `select`/`by`? (Recommend: only as a top-level aggregate in `select`, like
  built-ins.)
- Sequencing of F1/F2/F3: F1, F2, and F3.1 are done. F3.2 (value-selecting
  control flow) is deferred and would land as a `where`-style built-in, not
  as in-body `if`/`else`. F3.3 (statement-level / effectful control flow) is
  out of scope by design — Ibex is a pure-expression relational DSL.

## Related

- [[project-execution-roadmap]] — the chunked/streaming and codegen stages
  intersect the execution roadmap.
- `plans/udf-dataframe-plan.md` — scalar/table UDFs; Phase 5 (clause-expression
  integration) is advanced by Feature 1 here.
