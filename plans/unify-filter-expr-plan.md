# Unify `FilterExpr` into `ir::Expr`

## Motivation

Ibex has **two parallel expression representations**:

- `ir::Expr` = `variant<ColumnRef, Literal, BinaryExpr, CallExpr, RankExpr>` —
  value-producing expressions in `select` / `update` / counts. No booleans,
  comparisons, or logical operators.
- `ir::FilterExpr` = `variant<FilterColumn, FilterLiteral, FilterArith,
  FilterCall, FilterCmp, FilterAnd, FilterOr, FilterNot, FilterIsNull,
  FilterIsNotNull>` — predicates only.

`FilterExpr` **re-implements** arithmetic, column refs, literals, and calls that
`ir::Expr` already has, and adds the boolean / comparison / logical / null-test
nodes that `ir::Expr` lacks. The duplication is a recurring tax: every
value-expression capability stops at the filter boundary and must be written
twice or only covers one side. Already observed:

- **Scalar UDF inlining** works in `select`/`update` but not `filter`
  (aggregate-udf stage 1 deferred filter precisely because of this split).
- **Expression type inference** (`expr_type`) covers `ir::Expr` only.
- **Column-reference collection/validation** has `collect_expr_columns` *and*
  `collect_filter_columns`.
- Two interpreter paths and two codegen paths.

**Why now, before aggregate-UDF stage 3:** true multi-column aggregate UDFs will
be usable in predicates as well as `select`. If we build the richer aggregate /
UDF machinery on `ir::Expr` while `FilterExpr` still exists, we either duplicate
it into the filter sublanguage or leave filters behind again. Unifying the
expression IR first means aggregate UDFs (and everything else) are built once.

## Current State (what touches `FilterExpr`)

- **`ir::node.hpp`** — the `FilterExpr` variant + node structs; `FilterNode`
  holds a `FilterExprPtr`; `JoinNode` theta predicates use `FilterExprPtr`; the
  fused nodes (`FilterProject`, `FilterUpdateProject`, `FilterHead`,
  `FilterTail`) carry predicates.
- **Parser** — the `filter` clause lowers via `lower_filter_expr` (a `static`
  member, separate from `lower_expr_to_ir`).
- **Interpreter** — a specialised, **vectorised** predicate evaluator
  (`eval_value_vec` + boolean-mask machinery + Arrow-style validity bitmaps)
  operating on `FilterExpr`. This is a performance-critical hot path.
- **Codegen** — `ibex::ops` filter factories (`filter_col`, `filter_int`,
  `filter_cmp`, `filter_and`, …) build `FilterExpr`; the emitter uses them.
- **Schema pass** — `collect_filter_columns` (parallel to
  `collect_expr_columns`); `expr_type` does not handle predicates.

## Target Design

**One expression IR.** A predicate is simply a *boolean-typed* `ir::Expr`.
Extend `ir::Expr` with the node kinds it currently lacks:

- **Comparison** — `Eq Ne Lt Le Gt Ge` (a `CompareExpr { CompareOp, left,
  right }`), result type `Bool`.
- **Logical** — `And Or Not` (a `LogicalExpr`), result `Bool`.
- **Null test** — `IsNull` / `IsNotNull`, result `Bool` (never null itself).

Then:

- `FilterNode`, join theta predicates, and the fused nodes hold an `ir::Expr`
  (required to be boolean) instead of `FilterExprPtr`.
- **Delete `FilterExpr`** and its variant structs.
- The interpreter's fast vectorised filter path becomes an **evaluation strategy
  over (boolean) `ir::Expr`** — the same columnar mask / validity-bitmap logic,
  re-pointed at the unified nodes. *Not* a per-row virtual walk.
- Filter predicates lower through `lower_expr_to_ir` (the same path as value
  expressions), so they **inherit scalar UDF inlining, expression type
  inference, and column-reference validation for free**.
- `ibex::ops` filter factories collapse into the general expression builders
  plus the new comparison/logical builders; the emitter follows.
- The schema pass uses a single column-collection + type-inference path; predicate
  references are validated like any other.

## Migration / Risk Checklist

1. **`ir::Expr` gains boolean nodes** (comparison/logical/null). Extend
   `expr_type` to type them as `Bool`.
2. **Interpreter predicate evaluator** — re-implement the vectorised mask
   evaluation over `ir::Expr`. **Highest-risk item:** `filter` is a hot path;
   benchmark against `build-release/` to confirm no regression (the existing
   pre-sorted / columnar / validity-bitmap behaviour must be preserved).
3. **Node migration** — `FilterNode`, `JoinNode` predicate, and the four fused
   nodes switch to `ir::Expr`. Update `builder`, `clone_node`, and every
   `NodeKind` switch that reads a predicate.
4. **Parser** — `filter` clause lowers via `lower_expr_to_ir`; delete
   `lower_filter_expr`. Confirm column-or-scalar resolution in predicates is
   preserved (the `lexical_names` mechanism already covers it).
5. **Codegen** — emit predicates via the unified expression emission; remove the
   `ops::filter_*` factories (or redirect).
6. **Optimizer / canonicalize** — fusion rules (R5–R8, etc.) that move
   predicates around now operate on `ir::Expr`.
7. **Schema pass** — drop `collect_filter_columns`; the column-ref check and
   `expr_type` cover predicates uniformly.
8. **Tests** — predicate parsing, masked-eval correctness, 3VL null semantics,
   and a filter benchmark sanity check.

## Staging

1. Add comparison / logical / null-test nodes to `ir::Expr` + `expr_type` (Bool).
2. Port the interpreter's vectorised filter evaluator to `ir::Expr`; benchmark.
3. Migrate `FilterNode` / join / fused nodes + builder/clone; lower the `filter`
   clause through `lower_expr_to_ir`.
4. Codegen + `ops` factories.
5. Delete `FilterExpr`; unify the schema-pass column collection / inference.
6. Land the payoff: scalar UDF inlining + validation + type inference now reach
   filters; add tests. (This also un-defers the aggregate-udf stage-1 follow-up.)

## Payoff / What It Unblocks

- Scalar UDF inlining inside `filter` predicates — free (the deferred follow-up).
- Multi-column aggregate UDFs usable uniformly in predicates and `select` —
  built once, not twice (the reason to do this before aggregate-udf stage 3).
- A single column-reference validation, type-inference, and codegen path for all
  expressions.
- **Boolean masks as first-class column values.** Once a predicate is a
  boolean-typed `ir::Expr`, a boolean expression can be computed and *stored*
  like any other column, and a stored boolean column can be applied as a
  predicate:

  ```ibex
  let mask = trades[update { keep = price > 100 && symbol == "AAPL" }];
  mask[filter keep];   // apply the named mask
  ```

  The split representation makes this awkward today — a predicate can't be named
  or stored. This is a concrete, user-visible win, and it motivates resolving
  the "booleans in value position" question (below) toward **allowing** it.
- Less code, fewer parallel representations to keep in sync.

## Non-Goals

- Changing predicate **semantics** — SQL-style three-valued logic and the
  null-drop behaviour of `filter` stay exactly as specified (SPEC §3.5).
- Any filter **performance** regression — preserving the vectorised path is a
  hard requirement, not a nice-to-have.

## Open Questions

- **Node shape:** distinct `CompareExpr` / `LogicalExpr` / null-test nodes vs.
  folding comparisons into a generalised binary node with a `Bool` result.
  Recommend distinct nodes — clearer, and they type and evaluate cleanly.
- **Booleans in value position — leaning yes.** Once predicates are `ir::Expr`,
  a boolean expression is expressible in `select`/`update` (e.g.
  `select { flag = a > b }`, producing a `Column<Bool>`). `Column<Bool>` already
  exists, and the storable-mask use-case above is a real motivation, so the plan
  is to **allow** it and spec it (a stage-6 deliverable), rather than restrict
  booleans to predicate position. Sub-decisions: does `filter <bool col>` accept
  a bare boolean column as the predicate (it should — it's just a boolean
  `ir::Expr` that is a `ColumnRef`), and how does this interact with the
  column-or-scalar resolution already in predicates.
- **`rank` / `RankExpr`** appears only in value position today; confirm it stays
  out of predicate contexts (or define behaviour) under the unified IR.

## Related

- `plans/aggregate-udf-plan.md` — do this **before** stage 3 (multi-column
  aggregate UDFs) so that work targets the unified expression IR.
- [[project-execution-roadmap]] — codegen / fusion stages intersect here.
