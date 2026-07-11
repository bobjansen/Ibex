# Function-Kind Registry: one typed dispatch for all builtins

Status (2026-07-11): implemented. Generators, transforms, aggregates,
`coalesce`, and scalar dispatch now route through the unified `BuiltinFn` /
`FnKind` registry, and the duplicated field-evaluator ladders have been
collapsed onto the shared dispatch path. The null wrinkle was closed by
`plans/done/exprvalue-null-arm-plan.md`.

## Motivation

Ibex builtins are dispatched **ad hoc**. The scalar (row-local) builtins were
recently unified into a single `scalar_builtins()` table — one entry per
function, consulted by both the type-inference pass and the value-evaluation
pass, so they can never drift apart. That cleanup paid off immediately. But
every *other* category of function is still dispatched by a scattered chain of
name predicates — `is_rng_func`, `is_rolling_func`, `is_cum_func`,
`is_fill_func`, `is_float_clean_func`, `callee == "rep"`, `parse_aggregate_func`
— and that chain is **duplicated across four field evaluators** (`update_table`,
`windowed_update_table`, the `select` path, and `eval_value_vec`).

The cost is concrete and recurring:

- Adding **RNG-in-arithmetic** meant editing two field evaluators
  (`update_table` and `evaluate_field_column`) plus `eval_value_vec`.
- **`lag`/`lead`** is still a named branch in three places.
- The **rolling / cum / fill** dispatch is copy-pasted between the plain and
  windowed update paths.
- Whether function `F` may nest inside `G` is hand-wired per case, not derived
  from any property — which is why RNG composition needed bespoke routing logic
  (`expr_contains_rng` + "only when the top node is arithmetic").
- The same "duplicated logic drifts apart" pathology extends below the function
  layer to the **arithmetic kernels**. Integer division/modulo was reimplemented
  in *four* evaluators (`arith_into`, `eval_expr`, `apply_int_op`, and the REPL's
  `eval_scalar_expr`) that **disagreed on safety**: the vectorized path guarded
  divide-by-zero (and still missed `INT_MIN / -1`), while three others were
  unguarded and would `SIGFPE`. Resolved by routing all four through one
  `runtime::safe_idiv` / `safe_imod` (`include/ibex/runtime/safe_arith.hpp`) — a
  single source of truth, exactly the shape this plan proposes for functions. It
  is concrete evidence that the duplication is a present hazard, not just an
  aesthetic one, and a working model for the consolidation.

Each new builtin means touching N switches and re-deriving its composition
rules. `scalar_builtins` (and now `safe_arith` for the operators) proved the
fix; this plan finishes it for the other function kinds.

## Background: two strategies, four kinds

There are **two evaluation strategies**:

- **Scalar (row-local).** `out[i] = f(args[i])` — depends only on row `i`. Can
  be run per row *or* vectorised; same answer either way. Composes freely
  inside arithmetic.
- **Vectorised (whole-column).** The result cannot be computed from row `i`
  alone — it needs the whole column, neighbours, ordering, or external state.
  Produced at the column level.

But "vectorised" is not one thing. Sorting builtins by **shape and dependency**
gives four **kinds**:

| Kind          | Shape          | Depends on            | Examples |
|---------------|----------------|-----------------------|----------|
| **Scalar**    | N→N, row-local | `args[i]` only        | `abs`, casts (`Int64`/`Float64`/…), `ceil`/`floor`/`trunc`, `round`, date parts (`year`/`month`/…), `pmin`/`pmax`, `is_nan`, arithmetic/compare/logical operators, scalar `extern fn` |
| **Transform** | N→N, ordered   | neighbours / position | `rolling_*`, `cumsum`/`cumprod`, `lag`/`lead`, `rank`, `ewma`, `fill_forward`/`fill_backward` |
| **Generator** | ∅→N            | params + RNG/pattern  | `rand_*`, `rep`, future `seq`/`cycle` |
| **Aggregate** | N→1 (per group)| whole column          | `mean`/`sum`/`count`/`min`/`max`/`median`/`std`/`var`/`quantile`/`first`/`last`/`skew`/`kurtosis` |

Transform, Generator, and Aggregate all use the *vectorised* strategy, but the
evaluator must still tell them apart — you cannot put an Aggregate where a
Transform goes. This taxonomy aligns with the existing "row-local /
non-row-local" language already used in `plans/non-row-local-filter-plan.md`;
"Scalar" is row-local, the other three are non-row-local.

## Current State

What dispatches a function call today:

- **Scalar kind — unified.** `scalar_builtins()` (interpreter.cpp): a
  `unordered_map<string_view, ScalarBuiltin>` where each entry carries an
  `infer` (arg `ExprType`s → result `ExprType`) and an `eval` (arg `ExprValue`s
  → result `ExprValue`). Both `infer_expr_type` and `eval_expr` dispatch
  through it. `round` rides alongside it (its mode is a syntactic identifier).
- **Everything else — scattered predicates.** `is_rng_func`, `is_rolling_func`,
  `is_cum_func` (in `include/ibex/ir/expr_predicates.hpp`), plus local
  `is_fill_func` / `is_float_clean_func` and `parse_aggregate_func`. Each field
  evaluator runs the same `if (is_rolling_func) … else if (is_cum_func) … else
  if (is_fill_func) … else if (is_rng_func) … else if (callee=="rep") …` ladder
  before falling through to the scalar/per-row path.
- **The column-level implementations** are fine and should be reused as-is:
  `apply_rolling_func`, `eval_cumsum_cumprod_column`, `eval_lag_lead_column`,
  `eval_fill_null` (+ forward/backward), `evaluate_rank_column`, `apply_rng_func`,
  `apply_rep_func`, the aggregate kernels. The problem is **dispatch**, not the
  kernels.
- **Four+ evaluators repeat the ladder:** `update_table` (inlines
  infer→fast→per-row), `windowed_update_table` (own rolling dispatch, delegates
  the rest to `evaluate_field_column`), the `select` path, and `eval_value_vec`
  (the vectorised value/predicate evaluator, now the single value-IR evaluator
  after the `FilterExpr` unification — see `plans/done/unify-filter-expr-plan.md`).

So one kind is a registry; the other three are predicate ladders copied N times.

## Target Design

Generalise `scalar_builtins` into **one function registry** keyed by name, where
every builtin declares its **kind** plus a kind-appropriate evaluator. Dispatch
becomes a single lookup on `kind`, not a predicate ladder.

```cpp
enum class FnKind { Scalar, Transform, Generator, Aggregate };

struct BuiltinFn {
    FnKind kind;
    int    min_args, max_args;          // -1 == variadic
    // Result column/scalar type from argument types (validates arity/types).
    std::expected<ExprType, std::string> (*infer)(std::string_view,
                                                  std::span<const ExprType>);
    // One eval pointer is set per kind (the others null):
    ScalarEval    scalar_eval = nullptr;     // args ExprValue  -> ExprValue (row-local)
    ColumnEval    column_eval = nullptr;     // CallExpr, input Table -> ColumnValue (+validity)
    AggregateEval agg_eval    = nullptr;     // column/group -> ScalarValue
};

const std::unordered_map<std::string_view, BuiltinFn>& builtins();
```

- **Scalar** entries keep today's `ScalarBuiltin` shape (`scalar_eval`); the
  existing `scalar_builtins()` becomes the `kind == Scalar` slice.
- **Transform / Generator** entries carry a `column_eval` that wraps the
  existing kernel (`apply_rolling_func`, `eval_lag_lead_column`,
  `apply_rng_func`, `apply_rep_func`, …). The signature must give the kernel
  what it needs: the raw `CallExpr` (for arg names / literals), the input
  `Table`, the row count `n`, and ordering/window context where relevant.
- **Aggregate** entries carry an `agg_eval`; `parse_aggregate_func` becomes a
  registry lookup filtered to `kind == Aggregate`.

**Dispatch, written once**, used by every field/value evaluator:

```
look up callee in builtins():
  Scalar     -> evaluate per-row (eval_expr) or vectorised; composable in arithmetic
  Transform  -> column_eval(call, input, n, ctx)   -> ColumnValue
  Generator  -> column_eval(call, input, n)        -> ColumnValue
  Aggregate  -> only valid in reducing position    -> ScalarValue
  (not found) -> extern fn / error
```

`update_table`, `windowed_update_table`, `select`, and `eval_value_vec` call
this shared dispatch instead of each re-deriving the ladder.

### Composition rules fall out of `kind`

The hand-wired "can F nest inside G?" questions become a property check:

- **Scalar** may nest anywhere a value is expected (arithmetic, predicates,
  inside other scalar calls).
- **Transform / Generator** produce a column; the **vectorised** evaluator
  consumes them, so they may be nested inside arithmetic (a Generator leaf or a
  Transform leaf under a `BinaryExpr`). This is exactly the RNG-composability
  rule — `t + rand_normal(0, 1)` — but generalised, so `price - rolling_mean(price)`
  in an `update` field works by the same mechanism instead of a special case.
- **Aggregate** is reducing: valid only in `select`/`by` reducing position,
  never nested inside a row-wise expression. The evaluator rejects an Aggregate
  found in a value sub-expression (this is the existing
  `expr_contains_aggregate_call` check, re-expressed as a `kind` lookup).

The current `expr_contains_rng` + "only when arithmetic-topped" routing
generalises to: *if a field expression contains a Transform or Generator
sub-node, evaluate it vectorised via `eval_value_vec`.*

## The null / validity wrinkle

One category does not fit the clean split: the per-row `ExprValue` variant has
**no null**. So a few functions that are *row-local by shape* still cannot use
the scalar per-row path because they read or produce SQL null (validity):
`is_null` / `is_not_null` (already `IsNullExpr`), `coalesce` (unimplemented),
`null_if_nan` / `null_if_not_finite`, `fill_null`.

Options, to settle in the design:

1. **Mark them Transform** (validity-aware column ops) and always evaluate them
   vectorised. Minimal; honest about the constraint. (Recommended.)
2. **Give `ExprValue` an optional/null arm** so the scalar path can carry null,
   collapsing the wrinkle — larger change, touches every scalar kernel and the
   per-row push loop. A possible later unification, not part of this plan.

This is the same root issue noted in `project_scalar_builtin_registry`: `is_null`
/ `coalesce` were deferred precisely because the per-row model has no null.

## Migration / Staging

Incremental, each stage shippable and behaviour-preserving:

1. **Introduce `FnKind` + `BuiltinFn`; make `scalar_builtins` the `Scalar`
   slice.** Re-express the existing scalar registry under the new struct;
   `round` stays adjacent. No behaviour change. *(Smallest step; validates the
   shape.)*
2. **Fold Generators (`rand_*`, `rep`) into the registry** as `kind ==
   Generator` with `column_eval` wrapping `apply_rng_func` / `apply_rep_func`.
   Replace the `is_rng_func` / `callee=="rep"` branches in all four evaluators
   with the shared dispatch. Retire the bespoke `expr_contains_rng` routing in
   favour of the generic "contains Transform/Generator" rule. *(This is the kind
   I just hand-wired — migrating it first proves the `column_eval` path and
   deletes the most recent ad-hoc code.)*
3. **Fold Transforms** (`rolling_*`, `cumsum`/`cumprod`, `lag`/`lead`, `rank`,
   `ewma`, `fill_forward`/`fill_backward`) into the registry; delete
   `is_rolling_func` / `is_cum_func` / `is_fill_func` and the duplicated rolling
   dispatch in `windowed_update_table`. Thread window/order context through the
   `column_eval` signature.
4. **Fold Aggregates**: `parse_aggregate_func` → registry lookup filtered to
   `kind == Aggregate`; the reducing-position check becomes a `kind` lookup.
5. **Settle the null wrinkle** (option 1: mark validity-aware funcs Transform;
   land `is_null`/`coalesce` in `update`/`select` for free). Option 2 is a
   separate plan if pursued.
6. **Collapse the four field evaluators onto the shared dispatch** so the ladder
   exists exactly once. (Codegen's emitter is necessarily separate — it emits
   C++ source — but can read the same registry for arity/kind metadata.)

## Payoff

- **One place to add a builtin.** Declare name + kind + arity + eval; every
  evaluator picks it up, and its composition rules are implied by `kind`.
- Deletes the `is_*_func` predicate family and the duplicated ladders across
  `update_table` / `windowed_update_table` / `select` / `eval_value_vec`.
- Generalises RNG composability to *all* Transform/Generator nesting
  (`price - rolling_mean(price)` etc.) without per-function routing.
- "Can F nest inside G / is F valid here?" becomes a property lookup, removing a
  class of `unsupported expression` / `unknown function in expression` surprises
  driven by which switch a function happened to be added to.
- Aggregates, transforms, and scalars validate and infer types through one path.

## Non-Goals

- **Not** changing any kernel (`apply_rolling_func`, `eval_lag_lead_column`, the
  aggregate math); only how they are dispatched.
- **Not** changing evaluation semantics, null/3VL behaviour, or `filter`
  cardinality rules.
- **Not** a performance change: the vectorised hot paths (rolling, filter mask)
  must be preserved; the registry lookup is one hash on the callee, off the
  per-row inner loop.
- **Not** giving `ExprValue` a null arm (the larger option-2 unification) — out
  of scope here.

## Open Questions

- **`column_eval` signature.** Transforms need varied context (window duration,
  `by` groups, order keys); Generators need only `n`. One signature with an
  optional context struct, or a small set of typed kernels keyed by sub-kind?
  Lean: a single `column_eval(const CallExpr&, const Table& input, EvalCtx)`
  where `EvalCtx` carries optional window/order/group info.
- **Where the registry lives.** `scalar_builtins` is interpreter-local; the kind
  metadata (arity, kind) is also wanted by codegen and the schema pass. Split
  metadata (kind/arity, in `ibex::ir`) from the runtime `eval` pointers (in the
  interpreter)?
- **Sub-kinds of Transform.** Ordered (`cumsum`, `lag`) vs windowed (`rolling_*`)
  vs rank — do they need distinct context, or does one `EvalCtx` cover all?
- **Extern functions.** Scalar externs are effectively `kind == Scalar` with a
  dynamic eval. Do future vectorised/aggregate externs get `Transform` /
  `Aggregate` kinds in the same registry?

## Related

- `project_scalar_builtin_registry` (memory) — the `Scalar` slice that this
  generalises; records the `is_null`/`coalesce` null deferral.
- `plans/done/unify-filter-expr-plan.md` — unified the value **IR** and made
  `eval_value_vec` the single value evaluator; this plan unifies the **function
  dispatch** that runs on top of it. Complementary.
- `plans/non-row-local-filter-plan.md` — the row-local / non-row-local language
  rule this taxonomy formalises.
- `project_table_n_rows`, `project_rep_cycle_design` (memory) — `rep`/`cycle`/
  `seq` are Generators that should land via stage 2.
