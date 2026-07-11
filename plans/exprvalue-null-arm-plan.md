# ExprValue null arm: one null model for the per-row evaluator

## Motivation

The per-row value type has no null. `ExprValue` is
`variant<int64, double, bool, string, Date, Timestamp>`, so `eval_expr` cannot
represent "this cell is missing" — null lives only at the column level, in
validity bitmaps. Everything null-related in the per-row world is therefore a
workaround, and the workarounds have accumulated:

- **Null-consuming functions can't be scalar.** `fill_null`, `null_if_nan`,
  `null_if_not_finite`, and `coalesce` are row-local by shape — `out[i]`
  depends only on inputs at row `i` — but they *read or produce* validity, so
  they are classified `FnKind::Transform` and evaluated through whole-column
  kernels (`plans/done/function-kind-registry-plan.md`, stage 5, "the null
  wrinkle", option 1). That classification is honest but conservative, and the
  conservatism leaks into the planner: `is_row_local_update_expr` /
  `is_subset_evaluable_expr` treat them as non-row-local, so canonicalize
  won't reorder past them, the chunked pipeline falls back to materialized
  execution, and a guarded `where … update` evaluates them over the full
  table instead of the matching subset.
- **Undefined payloads are read, then masked.** The per-row loop evaluates on
  whatever bytes sit in null cells and relies on `collect_expr_validity` to
  mask the garbage afterwards. That helper hardcodes the assumption *output
  is null iff any input is null* (strict propagation) — which is exactly why
  null-consuming functions can't use the path: they violate the assumption.
  The pattern works today but is fragile (it silently miscomputes if a
  function is added to the per-row path without checking its null behavior)
  and it wastes the information the bitmap already has.
- **Semantics can fork.** Until July 2026, `is_nan(x)` (bare column, via a
  validity-aware column kernel) returned `false` on a null row while
  `is_nan(x * 1.0)` (per-row + post-hoc mask) returned `null` — the same
  function with two null semantics depending on argument spelling. That fork
  was closed by deleting the kernel and declaring `is_nan` null-propagating,
  but the *mechanism* that produced it (two evaluation paths with different
  null models) is still in place.
- **Codegen will need it anyway.** Fused per-row codegen (the execution-model
  roadmap) has to emit null handling into generated loops; a per-row null
  model in the interpreter is the semantic reference for that.

The language rule this plan implements uniformly:

> **Null in → null out, unless the function exists to handle null**
> (`is_null`, `coalesce`, `fill_null`, `null_if_*`).

## Current State

- `ExprValue` / `ScalarValue` — the same six-alternative variant, no null.
- Column null: `std::optional<ValidityBitmap>` per column entry.
- Per-row path: `eval_expr` reads payloads unconditionally;
  `evaluate_field` masks afterwards via `collect_expr_validity` (strict
  propagation assumed; also used by the fused numeric fast path, where the
  assumption always holds since it only compiles arithmetic).
- Boolean nodes (`CompareExpr`/`LogicalExpr`/`IsNullExpr`) never evaluate
  per-row: `field_uses_vectorized_eval` routes them to the mask machinery,
  which already implements 3VL.
- Null-consuming functions: `Transform` registry entries with column kernels
  (`eval_fill_null`/`eval_fill_forward`/`eval_fill_backward`,
  `eval_float_clean`, `eval_coalesce_column`).
- REPL scalars (`ScalarRegistry`, `let x = …`) share the variant and have no
  null; `eval_scalar_builtin` passes `ScalarValue` straight into registry
  `eval`.

## Target Design

Give the variant a null alternative and make the per-row evaluator
null-aware at the source instead of masked at the sink.

```cpp
struct Null {};  // or std::monostate with an alias
using ExprValue = std::variant<Null, std::int64_t, double, bool,
                               std::string, Date, Timestamp>;
```

- **`eval_expr` on `ColumnRef`**: consult the column's validity bitmap; a
  null cell returns `Null{}` — undefined payloads are never read.
- **Operators propagate.** Arithmetic/comparison on `Null` yields `Null`.
  (Per-row boolean *logic* stays out of scope — boolean nodes remain routed
  to the vectorized 3VL mask machinery, unchanged.)
- **Registry entries declare their null behavior:**

  ```cpp
  enum class NullPolicy : std::uint8_t {
      Propagate,  // default: any Null argument -> Null result, eval never sees Null
      Handles,    // eval receives Null arguments and decides (coalesce, fill_null, ...)
  };
  ```

  The call site in `eval_expr` implements `Propagate` once, before invoking
  `eval`; `Handles` entries get the raw arguments. This is the per-row dual
  of what the column kernels do today.
- **The per-row loop** in `evaluate_field` builds the validity bitmap inline
  (`value == Null` → payload default + validity false). On this path
  `collect_expr_validity` becomes redundant; the fused numeric fast path
  keeps it (strict propagation holds there by construction).
- **Reclassification.** `fill_null`, `null_if_nan`, `null_if_not_finite`,
  and `coalesce` become `FnKind::Scalar` with `NullPolicy::Handles`.
  `ir::fn_kind`'s Transform list shrinks back to the genuinely ordered
  functions (rolling/cum/lag/lead/fill_forward/fill_backward — LOCF/NOCB
  read neighbours, so they stay Transform). The planner predicates regain
  precision automatically: subset-evaluable guarded updates, chunked
  eligibility, and canonicalize reordering all start accepting the
  null-handling scalars again.
- **Column kernels become fast paths, not semantics.** `eval_fill_null`,
  `eval_float_clean`, and `eval_coalesce_column` may be kept as vectorized
  implementations behind the same registry entries (`column_eval` alongside
  `eval`), or deleted where the per-row path is fast enough. Either way the
  per-row path defines the semantics and the kernels must match it —
  registry-level parity tests (same expression through both paths) enforce
  that.

## Migration / Staging

Each stage shippable, suite green throughout:

1. **Add the arm, quarantined.** Extend the variant; make every existing
   `std::visit` / `get_if` site compile (most add an unreachable `Null` case
   guarded by `invariant_violation`). No producer yet — behaviour identical.
2. **Null-aware ColumnRef + propagating operators** behind the per-row loop;
   the loop consumes `Null` into validity. Debug-assert parity with
   `collect_expr_validity` on propagating expressions, then drop the
   post-hoc mask from the per-row path.
3. **`NullPolicy` on `BuiltinFn`**; implement `Propagate` centrally. Move
   `coalesce` / `fill_null` / `null_if_*` to `Scalar` + `Handles` (per-row
   eval implementations are a few lines each). Reclassify in `ir::fn_kind`.
4. **Planner reclaim + kernel cleanup.** Re-run the guarded-update /
   chunked / canonicalize paths over the reclassified functions; keep or
   delete the old column kernels per benchmark evidence.
   *Decided (July 2026): the kernels stay.* On 1M rows (half null,
   release build) kernel fill_null runs ~6.6 ms vs ~66 ms per-row — 10× —
   so the `use_column_kernel` gate keeps the bare-column form on the
   kernel and the per-row Handles eval covers the general forms.
   Guarded-update subset evaluation of fill_null verified end-to-end;
   chunked/canonicalize covered by the suite.
5. **(Optional, separate decision) REPL/ScalarValue null.** `let x =
   first(col)` where the cell is null currently cannot happen (aggregates
   never return null scalars today); if/when it can, extend `ScalarValue`
   the same way and let the REPL print `null`. Until then, convert at the
   boundary and error on `Null` escaping into a scalar binding.

## Payoff

- One null model; the two-paths-two-semantics class of bug (the `is_nan`
  fork) becomes unrepresentable.
- Null-handling functions are ordinary scalars: composable everywhere a
  scalar composes, subset-evaluable, chunkable, reorderable.
- No more computing on undefined payloads in the per-row path.
- The semantic reference for null handling in future fused codegen.

## Non-Goals

- 3VL inside per-row boolean logic — boolean nodes stay on the vectorized
  mask machinery.
- Changing any vectorized kernel's null semantics, `filter` cardinality, or
  join/order null placement.
- Performance work on the hot paths: the fused numeric and vectorized paths
  bypass `ExprValue` entirely and are untouched; the per-row loop is already
  the slow path and gains one branch per value.

## Open Questions

- `Null` as `std::monostate` vs a named tag type (named reads better in
  diagnostics; monostate avoids a definition).
- Should `Propagate` be enforced for *extern* scalar functions too (extern
  eval never sees null), or do externs need an opt-in `Handles`?
- Does `rep()` of a null-bearing pattern (array literal with null, once
  literals can be null) need `Handles` at the Generator level? Currently
  unreachable — literals cannot be null.
- Ordering with the FnKind-registry follow-ups: this plan supersedes the
  "validity-aware ⇒ Transform" workaround; any new validity-aware builtin
  added before this lands should follow the workaround, not pre-implement
  half of this.

## Related

- `plans/done/function-kind-registry-plan.md` — stage 5 ("the null wrinkle")
  chose the Transform workaround and explicitly deferred this plan as
  "option 2".
- The July 2026 `is_nan` decision: null-propagating everywhere, column
  kernel deleted — the first application of the "null in → null out unless
  the function exists to handle null" rule this plan generalises.
- `project_fnkind_registry_generators` (memory) — records the kernel
  self-validation gotcha that stage 3 of this migration must respect
  (ladders dispatch before inference; `Handles` evals validate their own
  arguments).
