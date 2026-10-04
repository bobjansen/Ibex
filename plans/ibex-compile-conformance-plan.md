---
name: ibex_compile_conformance
description: "Umbrella plan: close the divergence between ibex_compile (the C++ transpiler) and the interpreter so every language construct runs on all three execution surfaces. map { } is workstream 1."
metadata:
  node_type: memory
  type: project
---

# `ibex_compile` ↔ interpreter conformance

## Status / how to read this

Written 2026-09-07 for a **clean session**. This started as a `map`-only plan
and was broadened on review: `map` not compiling is one symptom of a systemic
gap — `ibex_compile` (the transpiler) and the interpreter have drifted in what
they can express, and nothing makes that drift visible. This is the **umbrella**;
`map` (W1) is the first concrete workstream and carries the most design detail.

**Non-goal:** this is *not* the "typed logical IR → physical pipelines →
templated kernels" successor described in `kernel-pipeline-execution-plan.md`.
It keeps the current architecture (the transpiler emits `ibex::ops::*` calls
that re-enter the runtime kernels) and makes it cover the language that already
exists.

---

## Context — the three execution surfaces

| # | Surface | Entry | Pipeline | Executes? |
|---|---|---|---|---|
| 1 | **transpile** | `build/tools/ibex_compile` (`tools/ibex_compile.cpp`) | `parse → expand_imports → parser::lower` (whole program → IR) `→ codegen::Emitter → C++23` | **No.** Emits `.cpp`; you compile that. Unrepresentable construct = hard `throw`. |
| 2 | **whole-script** | `build/tools/ibex` → `try_execute_whole_script` (`src/repl/repl.cpp:~5200`) | `parser::lower_script` (all statements → one IR `Program`) `→ ir::optimize_plan → runtime interpret()` | Yes — tree-walks IR (`src/runtime/interpreter.cpp`). |
| 3 | **statements** | `build/tools/ibex`, fallback when 2 declines | per-statement `parser::lower_expr → subset of passes → interpret()`, orchestrated by `eval_table_expr` / `eval_expr_value` in `src/repl/repl.cpp` | Yes — same tree-walker **plus a REPL-only expression layer**. |

Surfaces 2 and 3 interpret the IR; surface 1 is the only one that produces
native code and never interprets. "Runs interpreted" = surface 2 or 3. All
benchmarking runs through 2/3; surface 1 is secondary today but the intent is
for it to be a real target.

### What the transpiler actually emits

`codegen::Emitter` today emits a thin C++ program that rebuilds the plan as
`ibex::ops::*` calls (`include/ibex/runtime/ops.hpp`); the `ops` layer delegates
to the same runtime kernels the interpreter uses (`src/runtime/ops.cpp` →
`interpret()`). So surface 1 and surfaces 2/3 already **share all kernels** —
the divergence is purely in *plan construction and expression evaluation*.

**This "ops calls only, never a native loop" shape is historical, not a
constraint.** Every construct so far mapped onto an existing `ops` kernel, so
the emitter never needed to emit real control flow. `map { }` is a `for` loop
by another name — the emitter *can* just emit the loop, calling the extern
header functions (`read_csv`, `write_parquet`, all at global scope with the
headers already `#include`d) as ordinary C++. That path needs **no runtime
changes at all** for surface 1, and it is the emitter finally earning its name.
It also decouples surface 1 from the hard part (a runtime expression-level
extern evaluator) — see W1.

---

## The divergence is silent

`tests/parity/run_parity.sh` (ctest `ibex_parity_interpreter_vs_transpiled`)
runs 32 hand-picked `.ibex` cases through surfaces 3 and 1 and diffs output. It
is an **allowlist**: a construct the interpreter supports but the transpiler
does not simply has no case (and a case that fails to compile can't be added).
Nothing tells you the surfaces have diverged until a user hits it.

### Known gaps (verify + extend the matrix in-session)

| Construct | S2 whole-script | S1 transpile | Symptom |
|---|---|---|---|
| `map { }` clause | **yes** — shared `MapNode` evaluator (**W1b DONE**) | **yes** — `MapNode` → `ibex::ops::map` (**W1a DONE**) | — |
| non-literal extern-call args (`read_csv(runtime_path)`) | **yes** — materialized scalar bindings feed lazy-source construction (**W2-S2 DONE**) | **yes** — `ibex::ops::scalar_arg` (**W2-S1 DONE**) | deferred scalar sourced by the same lazy-reader graph still declines |
| top-level `fn … -> DataFrame` as the result | **yes** — table UDF is inlined (**W3 DONE**) | **yes** — same inliner (**W3 DONE**) | — |
| `model { }` clause | yes | **no** — `src/codegen/emitter.cpp:860` "model clause is not yet supported in compiled mode" | W4 |
| `window` + `select` | yes | **no** — `emitter.cpp:509` | W5 |
| `aligned` window | yes | **no** — `emitter.cpp:513` | W5 |
| windowed `update` with tuple fields | yes (partial) | **no** — `emitter.cpp:518` | W5 |
| stream constructs | S3 only (event loop) | partial (`NodeKind::Stream` case exists) | audit in-session |

`emitter.cpp` node cases: 31 of the ~35 `NodeKind`s. Missing / stubbed: `Map`
(new), plus the throw-guards above.

---

## W1a — **DONE** (2026-09-07)

`map { }` transpiles. Decision at implementation time (deviating from the
"native `for` loop" sketch below): the emitter emits a call to a new
`ibex::ops::map` kernel that re-enters the interpreter's row-wise `update`
evaluator, then keeps only the named columns. `MapNode` gained a real
`interpret()` case (not emitter-only after all — the emitted `ibex::ops::map`
re-enters it). Byte-identical to surface 3 for pure cells. W1b subsequently
extended that interpreter case to effectful extern cells and made it the shared
implementation for all surfaces.

Landed:
- `ir::NodeKind::Map` + `ir::MapNode` (one child + `std::vector<FieldSpec>`),
  `Builder::map`, `node_kind_v`.
- `src/parser/lower.cpp`: `MapClause` → `MapNode`; `map { }` must be the sole
  clause of its block. W1b enabled this lowering on every surface.
- IR visitors: `schema.cpp` (output = field list only; also `check_column_refs`),
  `cardinality.cpp` (row-count-preserving), `required_columns.cpp` (demand =
  field-expr columns), plus `infer_output_column_names` / `clone_node` in
  `lower.cpp`.
- `run_materialized_node` `case Map`: serial row-major evaluation, then construction
  of exactly the named result columns.
- `ibex::ops::map` (`ops.hpp` / `ops.cpp`); emitter `case Map`.
- Tests: `tests/test_lower.cpp` (S1/S2 build a MapNode, map+other
  clause rejected), `tests/test_codegen.cpp` (emit string-match),
  `tests/data/compile_map.ibex` + `scripts/ibex-e2e.sh` check. Parity gate
  `ibex_parity_interpreter_vs_transpiled` runs `map_rows.ibex` for real
  (marker deleted).

The native-loop emit was intentionally not pursued; the shared runtime route
keeps the three surfaces on one implementation.

## Step 0 — make divergence loud — **DONE** (`tests/parity/`)

The parity harness (`tests/parity/run_parity.sh`, ctest
`ibex_parity_interpreter_vs_transpiled`) is now a **conformance gate**:

- Every `cases/<name>.ibex` must transpile+match on `ibex_compile` **or** carry
  a `cases/<name>.unsupported` marker whose first line is a one-line reason.
- A marked case is instead checked two ways: `ibex_eval` must still run it
  (valid Ibex / interpreter regression guard) and `ibex_compile` must still
  reject it (**stale-marker detection** — a marker that starts passing fails
  the suite so it gets deleted).
- An unmarked case that does not transpile fails with instructions.
- Orphan markers (no `.ibex`) fail.

`cases/README.md` documents the convention and lists current markers.
Seeded with `cases/map_rows.ibex` + `.unsupported` (W1a). **Remaining matrix
constructs get a marked case as their workstream is picked up** — adding a
broken `model {}` / window case now, before W4/W5, just risks a flaky
`ibex_eval` sanity check. Closing a workstream = deleting its marker(s).

That initial peel has since been removed. All surfaces lower a real `MapNode`.

---

## W1 — `map { }` on every surface — DONE

Slice 1 (the `map { }` clause on surface 3) is **landed** — see
`[[project_fs_plugin_and_map_clause]]`.

**Reframe (see "What the transpiler actually emits"):** `map` is a `for` loop.
The emitter can emit the loop directly — with `read_csv` / `write_parquet` as
literal C++ calls — and that needs **zero runtime changes**. The hard thing (a
runtime expression-level extern evaluator) is only needed to make the
*interpreter* plan a `map`, which is marginal: `map`'s prefix is almost always
a trivial `list_files(...)`, so whole-script-planning it buys nothing, and S3
already runs `map` via the peel.

This was the original split. W1b was subsequently completed and replaced the
surface-specific peel with shared runtime execution.

### Superseded W1a design sketch

1. **`include/ibex/ir/node.hpp`:** `NodeKind::Map` + `class MapNode` — one child
   + `std::vector<ir::FieldSpec>` (`ir::FieldSpec` at `node.hpp:264`), mirroring
   `UpdateNode` (`node.hpp:804`). `node_kind_v` (`~1356`).
2. **`src/parser/lower.cpp`:** replace the `ClauseState::record` guard with
   `MapClause → MapNode`. Field exprs lower to nested `ir::CallExpr`
   (`read_csv`/`write_parquet` become `CallExpr`, **not**
   `ExternCallNode`/`ScanNode` — detect "inside a MapNode field").
   `clone_clause` already handles `MapClause` (`lower.cpp:~291`).
3. **IR visitors** — `grep -n 'NodeKind::Update' src/ include/`:
   `src/ir/schema.cpp` (output schema = field list; type from `infer_expr_type`),
   `src/ir/cardinality.cpp` (rows out == rows in),
   `src/ir/required_columns.cpp` (union of `ColumnRef`s). **`MapNode` is an
   optimizer barrier** — effectful, row-order-significant: audit every
   `ir::optimize_plan` pass against `NodeKind::Stream` and match it.
4. **Interpreter surfaces do NOT get a `MapNode` case.**
   - S3 keeps the Slice-1 peel (`MapClause` handled in `eval_table_expr` before
     `lower_expr` — it never produces a `MapNode`).
   - S2: `try_execute_whole_script` (`src/repl/repl.cpp:~5216`) **declines** on
     a script containing a `map` clause — one line, next to the other declines,
     with a `decline("script uses map { }")` reason. Marginal value lost.
   - So `parser::lower` only ever yields a `MapNode` from `ibex_compile`, and
     `src/runtime/interpreter.cpp` never needs to execute one. `MapNode` is an
     **emitter-only IR node** (there is precedent for surface-specific nodes).
5. **`src/codegen/emitter.cpp`: emit a native `for` loop.** New `emit_row_loop`
   / `emit_map_node`:
   ```cpp
   auto _in = <child>;
   ibex::Column<T0> _f0; ibex::Column<T1> _f1; /* ... reserve(_in.rows()) */
   for (std::size_t _r = 0; _r < _in.rows(); ++_r) {
       auto <col> = ibex::ops::cell_scalar(_in, "<col>", _r);   // per referenced input column
       _f0.push_back(<field0 expr as real C++>);
       _f1.push_back(<field1 expr as real C++>);
   }
   ibex::runtime::Table _out;
   _out.add_column("f0", std::move(_f0)); /* ... */
   ```
   The field expression is emitted as **real C++** by extending `emit_raw_expr`
   (`src/codegen/emitter.cpp:1414`): `ColumnRef` → the per-row local (not the
   current "throw unless compile-time literal"); `BinaryExpr`/`CompareExpr` →
   C++ operators; nested `CallExpr` to an extern → a literal call
   (`write_parquet(read_csv(path), target)` verbatim); `Literal` → as now.
   Column typing: `map_column_from_scalars`'s first-non-null rule
   (`src/repl/repl.cpp`) becomes a compile-time decision from `infer_expr_type`;
   nulls → a `ValidityBitmap` built in the loop.
6. **`include/ibex/runtime/ops.hpp` + `src/runtime/ops.cpp`:** a small
   `cell_scalar(const Table&, string, size_t) -> <appropriately-typed>` helper
   (or emit the `std::visit` inline). No `map_rows` kernel — the loop *is* the
   kernel now.

**Deliverable:** `ibex_compile` on a `map` script emits `.cpp` that compiles and
produces output byte-identical to S3. Delete
`tests/parity/cases/map*.unsupported`.

### W1b — runtime expression-level extern evaluator (surfaces 2 & 3) — DONE

The runtime evaluator now dispatches registered extern `ir::CallExpr`s through
`eval_extern_expr(call, table, row, scalars, externs)`:
- scalar extern → eval args, call `fn->func`.
- scalar extern, `first_arg_is_table` → arg 0 is a `CallExpr` to a
  table-returning extern; recurse to a `Table`, call `fn->table_consumer_func`.
- table extern → valid only as arg 0 of a consumer (lift the
  `first_arg_is_table` guard in `invoke_extern_call` to be position-aware).
Home: `extern_call.cpp` (has the registry), forward-declared for `expr.cpp`.
`src/runtime/interpreter.cpp` evaluates `MapNode` fields in serial row-major
order, preserving the ordering of observable effects. The S2 lowering guard
and S3 peel are gone; all three surfaces use the same node and evaluator.

**Deliverable (if done):** `let n = write_parquet(read_csv(^p), ^o);` runs on
S2/S3; S2 plans `map` prefixes; one expression evaluator instead of two.

---

## W2 — non-literal extern-call arguments

### W2-S1 — **DONE** (2026-09-07)

`emit_raw_expr` no longer hard-throws on a non-literal argument. The emitter
already builds `_ibex_scalars` (compile-time + deferred bindings) and calls
`set_scalars`, so the fix is small: emit `ibex::ops::scalar_arg(<emit_expr>)` —
a proxy (`ScalarArg` in `ops.hpp`) whose templated `operator T()` converts the
registry value to whatever scalar C++ type the call site needs (the extern
signature / the `static_cast` under a row count is the type authority). Guarded
so a **bare** reference emits a lookup only when the name is a known
`scalar(...)` deferred `let` (`runtime_scalar_names_`); any other unbound bare
name is still a compile-time error. Computed argument expressions (arithmetic /
nested calls over bound scalars) always emit the lookup.

Covers: `read_csv(scalar(manifest[...]))`, `Table(scalar(...))` row counts,
`read_parquet(dir ++ name)`.

Tests: `tests/parity/cases/construct_deferred_row_count.ibex`, `test_codegen`
(bound → `scalar_arg`, unbound → throws), `tests/data/compile_scalar_arg.ibex`
+ `ibex-e2e.sh`.

### W2-S1b — `parse_args` in compiled mode — **DONE** (2026-09-07)

`parse_args` reads the process argv from `IBEX_ARGS` (one entry per line); the
`ibex` runner sets it from everything after `--`. For a compiled binary the
natural UX is `./binary <args>` directly, so when the script declares a
`parse_args` extern the emitter emits `main(int argc, char** argv)` and, as the
first statement, `ibex::ops::forward_cli_args(argc, argv)` — joins argv[1..]
with newlines into `IBEX_ARGS` (no-op when `argc <= 1`, so an inherited
`IBEX_ARGS` still applies to an argument-less run). Zero plugin changes: the
emitted `parse_args(spec, "")` call already falls through to the env-var path.
`ibex_compile.cpp` sets `Config::forward_cli_args` when it sees the extern.

Test: `tests/data/compile_parse_args.ibex` + `ibex-e2e.sh` (transpile, compile,
run with and without argv, check row count + label). `test_codegen` for the
`main` signature switch.

### W2-S2 — **DONE** (2026-09-10)

The whole-script path now collects compile-time and deferred scalar bindings,
materializes them before source construction, and evaluates source/sink args
against that `ScalarRegistry`. Dynamic readers are hoisted into distinct lazy
sources (literal readers retain value-based coalescing), and the registry is
also passed through pushed-filter decoding and final interpretation.

The circular case remains an explicit, observable decline: a deferred scalar
whose own subplan calls a lazy reader stays on the statement path. Resolving
that graph without eager or duplicated effects requires dependency-aware source
scheduling beyond W1b. `Series<T>` extern arguments remain the separate ABI project in
`plans/extern-series-arguments-plan.md`.

---

## W3 — user functions on surfaces 1 & 2 — **DONE** (2026-09-07)

Both halves landed:
- **S2 decline removed** (`try_execute_whole_script`): `fn` declarations no
  longer force the statement path. `lower_script` registers them
  (`collect_declaration`) and inlines scalar/aggregate/table UDF calls; a shape
  it still can't lower declines with its own "did not lower" reason (→ S3).
- **Table-UDF inliner** (`Lowerer::inline_table_udf`, called from
  `lower_table_call` when the callee is a registered `fn`, not a table extern):
  lowers each argument in the caller's scope, installs `DataFrame`/`TimeFrame`
  params as IR bindings (shadowing + restore) and scalar params into a fresh
  `inline_scopes_` frame, folds body `let`s the same way, then lowers the
  body's trailing expression. `inlinable_body_shape` (lets + one trailing
  expr); direct recursion rejected via `inlining_active_`. `bindings_ == nullptr`
  (statement-path lowering) → not inlinable.

So `ibex_compile` now transpiles a script whose result routes through a
table-returning `fn`, and the whole-script planner plans function-organised
scripts. Test: `tests/parity/cases/table_udf.ibex` (transpile+match),
`tests/test_lower.cpp` (inline shape, recursion rejected).

Import stubs containing `fn` declarations no longer force a whole-script
decline; their declarations are supplied to both scalar-binding collection and
script lowering. Helpers whose bodies contain effectful `map` cells now remain
on that path through W1b's shared evaluator.

### original plan (kept for history)

1. `try_execute_whole_script` (`src/repl/repl.cpp:~5216`): the
   `decline("script declares a function")` is conservative — `lower_script`
   already handles `FunctionDecl` (`src/parser/lower.cpp:1397`
   `collect_declaration`). Remove the decline, run the parity + e2e suites,
   keep only the declines that a real failure justifies. (`import` was
   un-declined the same way in `fcb729d5`.)
2. `ibex_compile`: a top-level `fn … -> DataFrame` used as the result or bound
   with `let` currently fails ("no expression to lower" / "unsupported scalar
   let"). Table-returning UDFs must be inlined at their call site the way
   scalar UDFs are (`src/parser/lower.cpp:2939` `inline_scalar_udf`,
   `:2852` `inlinable_body_shape`) — or the trailing `ExprStmt` that is a UDF
   call must be recognised as the result plan. Audit
   `tools/ibex_compile.cpp:75` (`no expression to lower`) and the scalar-let
   path.

Unblocks: `import "fs"; csv_dir_to_parquet(…)` running on S2, and any
function-organised script compiling.

---

## W4 — `model { }` in codegen

**Status (2026-10-02): built-in methods done.** `let m = df[model { ... }]` is a
fitted model of the program: a script binding at its statement (the plan's root is
the Model node), emitted as `ibex::runtime::ModelResult _modelN;` and
`ibex::ops::fit_model(table, ModelFormula{...}, "method", {params}, _modelN)`, which
runs the interpreter's own Model evaluation, so a compiled fit is the interpreter's
fit. `m` is also the coefficients table, as in the REPL. The accessors read the model
by name: `coef` / `summary` / `fitted` / `residuals` / `importance` (a table: a call
on the name, pinned where it is written so a refit under the same name does not change
an earlier accessor) and `r_squared` (a scalar, a bound call), under either spelling,
through the ops layer's `model_*` functions. `ibex_compile` takes the script path when
a model is fitted; `lower()` and the batch executor decline it.

Refused with a message: a model method from a plugin (`lightgbm`, `kmeans`, `pca`:
the compiled program has no registry to load a plugin into -- built in are `ols`,
`ridge`, `wls`), so `predict(m, newdata)` (plugin models only) has nothing to read; a
model fitted inside a function. Parity (`effect_cases/`): `model_ols`,
`model_accessors` (a refit under the same name; both fits' accessors and scalar),
`model_wls`.

Found on the way: the interpreter's `summary(m)` reports p-values outside [0, 1]
(`1.6`, `3.8e-243` for t = 33 at 3 degrees of freedom). Compiled and interpreted
agree because they are one code path; the p-value computation itself is wrong.

---

## W5 — window / resample edge combos

**Status (2026-10-02): done, and a wrong answer fixed.** `window + select` and
`aligned` windows emit through `ibex::ops::window_update(..., select_only, aligned)`
(parity: `window_select`, `window_aligned`); the plain `window + update` keeps its
short form. A windowed update with tuple fields is not an emitter gap: the interpreter
dropped the tuple's columns silently, so it now rejects it, and so does the compiler,
with the same message (a `where` guard on a window update likewise).

The same pass found `where <predicate> update { ... }` compiled WITHOUT its guard --
every row was updated -- since `UpdateNode::guard()` was never read by the emitter. It
now emits `ibex::ops::update_where` (parity: `update_where`).

## W6 — effects and resources in compiled programs (ADBC)

Added 2026-10-02, rewritten the same day. The promise is that a script that
works in `ibex` can be productized with `ibex_compile`; for database scripts it
does not hold yet. Measured on 2026-10-02:

- `write_csv(a, "x.csv");` as a statement: "table-consuming extern calls
  require lower_script()" — `ibex_compile` calls `lower()`, which takes one
  result plan and no sinks.
- `let n = write_csv(...);`: "unsupported scalar let" — a scalar that comes
  from an extern call is neither a compile-time constant nor a deferred
  table-subplan scalar.
- Any resource (`extern type`, e.g. `AdbcConnection`): refused up front by
  `first_resource_call` (`tools/ibex_compile.cpp`). The whole-script planner
  declines scripts that call resource functions too, so resources exist only on
  the REPL's statement path.
- `adbc_read` alone transpiles, but the generated code includes `adbc.hpp`,
  which does not exist: the program fails in the C++ compiler. Every other
  bundled plugin (csv, json, args, fs, data_gen, parquet) ships a header with a
  C++ entry point per extern; ADBC was built as a plugin `.so` only.

### Principle: the effect system orders statements

Statement order is not a property of an emit mode. It is a property of the
plan, and the plan gets it from effect summaries.
The opaque-resource design already said so ("Mapping and effects", retired:
`git show 0d069262:plans/opaque-resource-lifetime-plan.md`): resource operations run in source order, and cannot be eliminated,
duplicated, commoned, speculated or parallelized; its step 2 is "propagate
resource summaries through helpers, validate query contexts/map expansions, and
preserve statement ordering in planning". The REPL and `ibex_compile` must get
ordering from that one mechanism. A compiled mode that merely runs statements
in source order would be a second implementation of the same rule.

State of that step 2, checked 2026-10-02 (rechecked after the first slice):

| Part | State |
|---|---|
| Placement validation (zero plugin calls on a misplaced call) | **Done**, REPL statement path; `ResourceFunctions` shared by REPL, planner, `ibex_compile` |
| Statement order between sinks and shared bindings | **Done** (`6fdc72bd`): `position` on `SharedBinding`/`ScriptSink`; executor runs them in order |
| Reads that must not move past a conflicting write | **Done** (`6fdc72bd`): a `let` calling an effectful extern is pinned when a non-commuting sink separates it from its first reader; `ir::is_reorderable` now has its first caller |
| ADBC externs' effects | Undeclared, so each carries all core effects, unscoped: already a barrier against every non-pure statement and against each other |
| Resource summary (acquire / use / close) | **Not needed for correctness** — see below |
| REPL `OptimizationContext` | **Not a gap.** The statement path optimizes one expression at a time; the default context is deliberate (`repl.cpp`: "every effect, so the effect-sensitive passes stay conservative"), and the only pass that reads summaries acts on `ProgramNode` preambles, which a single expression never has |
| `ScriptPlan` represents non-sink effectful statements | **Not built**: scalar-call statements go to `preamble`, which runs before everything regardless of position, and `let n = extern(...)` is "unsupported scalar let". The batch executor declines such scripts (`script has statements that must run before the plan`) |
| Whole-script planner accepts resource scripts | **Declined on purpose**; the statement path owns resources |

### W6-0 — effect-ordered planning (prerequisite; finishes step 2)

Revised after the first slice. What the first slice showed:

- Ordering needs no new analysis for resources. Undeclared externs already
  carry every effect, so `is_reorderable` pins them against each other and
  against any non-pure statement. A transitive acquire/use/close summary would
  only buy *finer* commuting (two connections never conflicting), which needs
  alias analysis first. It is an optimization; defer it until a measured script
  wants it. Placement is already enforced by `ResourceFunctions`.
- Declaring `effects { ... }` on the ADBC externs only helps once the file
  readers and writers are declared too (an undeclared extern conflicts with
  everything, scoped or not). Do that together, not for ADBC alone.
- The REPL statement path needs nothing: it executes in source order by
  construction.

What remains is the representation, and it is W6a's first step because the
emitter is its only consumer:

1. **Ordered effectful statements in `ScriptPlan`.** Scalar-call statements and
   `let n = extern(...)` get a `position` like sinks and shared bindings; the
   `preamble` list (which runs first, whatever the source order) goes away.
   Resource calls are statements of the same kind, carrying the resource
   function's name and its resource arguments by binding name.
2. **Both consumers follow the order.** The batch executor keeps declining
   scripts with such statements until it grows a resource registry (it does not
   need one for scalar statements and can take those now); the emitter takes all
   of them.
3. **Tests** that fail on the unfixed tree: a scalar statement between two
   sinks runs between them; `let n = f(...)` is visible to a later filter.

### W6a — sinks and extern scalars (no resources)

**Status (2026-10-02): W6a done.** `ibex_compile` lowers a script that has a
table sink, or a `let n = f(...)` binding an extern's result, with `lower_script`
and emits it with `Emitter::Script` (`emit(out, Script, config)`): steps in
statement order, `Scan` of a shared binding resolved to the table its step built,
a `write(x, ...); x;` result served from the sink's input, a bound call stored in
the scalar registry where it runs (`_ibex_scalars["n"] = ScalarValue(f(...))`) so
later queries and extern arguments read it, and `scalar(<table>)` lets run at
their own statement instead of before every step. Scripts with none of these keep
the single-plan path. `ScriptPlan` carries the statement position of preamble
calls (`preamble_positions`) and the name each call's result binds
(`preamble_binds`, `ScriptSink::bind`); `ScalarBindingSet` carries the position of
each deferred scalar and the names bound to extern calls.

Guards: the whole-script batch executor declines a script that binds an extern
call's result (it has no step for it, and planning it would never call the
function); `lower()` refuses one. Before this, `lower_script` accepted such a
`let` as a scalar and dropped the call.

Parity: four cases in `tests/parity/effect_cases/` run the transpiled program and
the interpreter and compare the final table; the two ordering cases fail if shared
bindings are emitted first. Not covered by a parity case: a bound call of a
non-sink extern (no bundled one returns a scalar without a resource) -- it has
lowering and emitter unit tests only. Named arguments in a `let` of an extern
call are refused for now.

### W6b — ADBC as a linkable library

**Status (2026-10-02): done.** `libs/adbc/adbc.cpp` is split in two. The client
(sessions, statements, driver quirks, discovery) is the static library
`ibex_adbc` (`adbc_client.cpp`), with a typed C++ API in `adbc_client.hpp`
(`ibex::adbc::connect/read/query/execute/write/tables/table_schema/begin/commit/
rollback/close`, and `ibex::adbc::Connection`, a value type holding the shared
session, so copies alias and the last one closes). That header names no ADBC
type. The plugin (`adbc.cpp`) is `ExternArgs` parsing over it. `adbc.hpp` is
what generated code includes: global functions named like the externs
(`adbc_read`, `adbc_connect`, ...) over the same API, throwing
`std::runtime_error` with the REPL's message, and `AdbcConnection` as an alias of
`Connection`. `scripts/ibex-build.sh` links `libibex_adbc.a`, the Arrow bridge
and the driver manager (and `-ldl`) when the generated code includes
`adbc.hpp`; the parity harness does the same.

`adbc_read` compiles and matches the interpreter (two `effect_cases`, which
substitute the SQLite driver path and skip when the build has none).
`tests/test_adbc_client.cpp` exercises the library through `adbc.hpp` without the
plugin or the REPL. Not yet: `ibex_compile` still refuses a script that uses a
resource (`adbc_connect`); that is W6c. A build with
`IBEX_ADBC_SYSTEM_DRIVER_MANAGER=ON` links the system manager, which
`ibex-build.sh` does not know to find.

Original design, kept for the record:

Independent of W6-0 and W6a. Split `libs/adbc/adbc.cpp` into `ibex_adbc`
(static library: the session, statements, quirks, discovery, all of today's
logic, with a C++ API in a new `libs/adbc/adbc.hpp`) and the plugin
(`ExternArgs` <-> C++ wrappers only). The C++ API mirrors the externs:
`AdbcConnection` is a value type holding a `shared_ptr` to the session (copies
alias, the last copy closes — the REPL's binding semantics for free);
`adbc_query(AdbcConnection&, std::string, Table)`, etc. `adbc_read` is usable
on its own after this slice. `scripts/ibex-build.sh` links `libibex_adbc.a` and
the driver manager when the generated code includes `adbc.hpp`.

### W6c — resource values in the emitter

**Status (2026-10-02): done, including functions.** A script's
connections compile: `let db = adbc_connect(...)` is a C++ variable
(`auto _res0_db = adbc_connect(...);`), `let b = a;` copies it (copies alias, the
last closes), and every call on it -- `adbc_execute`, `adbc_write` (its table
argument and every defaulted `params` become bindings of their own at the
statement), `adbc_query`, transactions, `adbc_close` -- runs where the statement
is. A plan that calls a resource function is pinned at its statement and runs once
however many readers it has, or none; a bare `adbc_query(db, "...");` runs too.
Rebinding a name to a new connection releases the old variable after the new value
is computed; rebinding it to something that is not a resource releases it right
after that statement (`ResourceStep::Unbind`), so an open transaction rolls back
where the REPL rolls it back. The placement rule is `ResourceFunctions::first_
misplaced`, moved out of the REPL and shared with `ibex_compile`; a call inside a
query clause is refused with the REPL's message before anything is emitted.

Parity (`tests/parity/effect_cases/`): `adbc_connection`, `adbc_alias_and_rebind`,
`adbc_transaction`, `adbc_release` -- the last fails ("database is locked") if the
release is not emitted. A connection opened inside another call's argument
(`adbc_execute(adbc_connect(...), "...")`) is a temporary of the statement: it runs
first and is released after (`adbc_nested_call`).

A `fn` that takes, opens or returns a resource (or calls one that does) is a C++
function of the program: `_ibex_fn_<name>`, declared ahead of `main` so functions
call one another in any order. Its body is lowered by a lowerer of its own as a
small script (`lower_resource_function`), so everything above works inside it, and
its locals are C++ locals: released at the return, in reverse order, by the
destructors -- the REPL's frame. Parameters are the C++ types of their language
types (a connection is the extern type's name, a table `const Table&`, scalars
`std::int64_t` / `double` / `std::string` / ...); scalars are also published in a
`ScalarScope`, a copy of the caller's registry plus the parameters, restored at the
return, which is how the REPL's local registries behave. The value is the last
statement: a table plan, a connection name, a literal, or a call (including a call
of another function). A call of a function from the program is a bound call or a
pinned binding like any call of a resource function. Parity: `adbc_function_table`,
`adbc_function_connection`, `adbc_function_scope` (a function's connection is
released at the return, so its open transaction cannot lock the table).

A program may now end in an effect (`adbc_close(db);`, as `examples/adbc_connection.ibex`
does): `lower_script` gives no result plan, the compiled program prints nothing for
it, and `lower()` and the REPL's batch executor decline. `scripts/ibex-e2e.sh`
builds and runs a connection-and-function program with `ibex-build.sh`.

Not done, and refused with a message: a scalar `let` in a function body (scalar
bindings are collected for the program, not per function), a value that is none of
the forms above, a Decimal or column parameter, a function reading the program's
tables or connections (the REPL gives it no connections either), and a nested call
that returns a scalar or table argument.

Original design, kept for the record:

Needs W6-0, W6a and W6b. The emitter follows the same ordered plan, extended
with resource values: `let db = adbc_connect(...)` -> `auto db =
adbc_connect(...);`; a resource call as a statement or `let` value emits a C++
call at its plan position, its table result bound to a variable later plans
`Scan`; rebinding a name (`let kept = 0;`) emits a new scope or resets the
handle so the old connection closes where the REPL closes it. A `fn` that
takes, opens or returns a resource emits as a C++ function with a statement
body (the REPL runs these on the statement path; they cannot be inlined into a
plan). The placement checks must run in the compiler too — through
`ResourceFunctions` and the W6-0 summary, not re-derived.

### W6 verification

Parity cases against SQLite (always available when ADBC is built):
connect/execute/write/params/query+filter, transactions, a `fn` taking a
connection, a returned connection, `adbc_close` through an alias. The parity
runner needs the ADBC driver path in its environment; cases skip (marker) when
ADBC is not built. A Release-build smoke in `scripts/ibex-e2e.sh`: compile an
ADBC script with `ibex-build.sh` and run the binary.

---

## Sequencing

1. ~~**Step 0** (parity conformance gate)~~ — **DONE**.
2. ~~**W1a** (`MapNode` + `ibex::ops::map` kernel emit)~~ — **DONE** (kernel
   route, not native loop; see the W1a section above). `emit_raw_expr`-as-C++
   was *not* built, so W2/W4 do not inherit it.
3. ~~**W3** (functions)~~ — **DONE**: S2 decline removed + `inline_table_udf`.
4. ~~**W2** — S1 (`ibex::ops::scalar_arg`), S1b (`parse_args` argv forwarding),
   and S2 (whole-script scalar extern args)~~ — **DONE**.
5. ~~**W1b** (runtime extern-expr evaluator + shared `map` execution)~~ — **DONE**.
6. **W4 / W5** — lower priority, independent.
7. **W6** (2026-10-02, user priority: ADBC fully supported in compiled
   programs) — W6-0 (effect-ordered planning; finishes step 2 of
   the opaque-resource plan, now retired), then W6a; W6b is independent and can
   run in parallel; W6c needs all three.

Each workstream is a landable unit and deletes its `.unsupported` markers.

## Verification

Build `cmake --build build -j6` (`[[feedback_cap_build_parallelism]]`).

- **Step 0:** parity test fails on any un-marked interpreter case that doesn't
  transpile.
- **W1a:** existing `tests/test_fs.cpp` `[map]` cases keep passing (S3
  unchanged). New codegen e2e: `ibex_compile` a `map` script → compile the
  `.cpp` → assert stdout equals `build/tools/ibex` on the same script
  (`scripts/ibex-e2e.sh`, `tests/test_codegen.cpp`). Effectful `map` (csv→csv
  round-trip) and pure `map` (arithmetic) both.
- **W1b:** `tests/test_interpreter.cpp` exercises nested table-reader/consumer
  extern expressions; `tests/test_fs.cpp` covers the effectful file round trip;
  the import planner test asserts an effectful map helper stays whole-script.
- **W2 / W3 / W4 / W5:** a parity case per construct;
  `scripts/check-object-equivalence.sh` (`[[project_object_equivalence_script]]`)
  for codegen-neutrality.
- **Regression:** full `ctest -j6` green after every phase (currently 1866,
  ~200s incl. parity). `ibex_parity_interpreter_vs_transpiled` is the
  load-bearing S1-vs-S3 check.

## Risks / decisions in-session

- **`emit_raw_expr` as real C++ is the pivot.** Today it throws on anything but
  a compile-time literal (`src/codegen/emitter.cpp:1414`). W1a turns it into a
  small C++ expression emitter (column-ref → local, operators, literal calls).
  Keep it conservative: only what a `map` field needs, error clearly on the
  rest. This is the emitter's first real codegen — do it carefully, it's the
  seam everything else (W2, W4) reuses.
- **Optimizer barrier correctness** — `MapNode` sits in the IR on every
  surface. A pass that
  reorders/drops work across it is a silent miscompile. `NodeKind::Stream` is
  the existing "opaque, ordered, effectful" node; match it everywhere.
- **`map` field returning a table** stays rejected — a rbind-the-results
  `map(table, fn)` is a *different* feature, deferred, do not conflate.

## Files (by workstream)

| WS | Primary | Also |
|---|---|---|
| Step 0 | `tests/parity/run_parity.sh`, `tests/parity/structured_runner.cpp`, `tests/parity/cases/` | `plans/README.md` |
| W1a | `include/ibex/ir/node.hpp`, `src/parser/lower.cpp`, `src/codegen/emitter.cpp` (`MapNode` case + `emit_raw_expr`-as-C++), `src/repl/repl.cpp` (S2 decline on `map`) | `src/ir/{schema,cardinality,required_columns}.cpp`, `src/ir/optimizer*`, `include/ibex/runtime/ops.hpp` + `src/runtime/ops.cpp` (`cell_scalar`), `tests/test_codegen.cpp`, `scripts/ibex-e2e.sh` |
| W1b | `src/runtime/expr.cpp`, `src/runtime/extern_call.cpp`, `src/runtime/interpreter.cpp` (`MapNode` case), `src/repl/repl.cpp` (delete peel, drop decline) | `src/runtime/runtime_internal.hpp`, `src/runtime/CMakeLists.txt`, `tests/test_extern_expr.cpp` |
| W2 | `src/codegen/emitter.cpp` (`emitter.cpp:1422/1469` lift), `src/repl/repl.cpp` (`literal_args`, `:5010`) | shares W1b for the S2 half; `plans/extern-series-arguments-plan.md` |
| W3 | `src/repl/repl.cpp` (`try_execute_whole_script`), `tools/ibex_compile.cpp`, `src/parser/lower.cpp` (UDF inlining) | parity cases |
| W4 | `src/codegen/emitter.cpp:860`, `src/runtime/ops.cpp`, `include/ibex/runtime/ops.hpp` | `tests/test_codegen.cpp` |
| W5 | `src/codegen/emitter.cpp:509/513/518` | `tests/test_codegen.cpp` |
| W6 | `src/parser/effects.cpp`, `src/ir/optimizer.cpp`, `src/parser/lower.cpp` (`ScriptPlan`), `src/repl/repl.cpp` (context, drop decline), `tools/ibex_compile.cpp`, `src/codegen/emitter.cpp`, `libs/adbc/` (split), `scripts/ibex-build.sh` | `include/ibex/parser/resource_functions.hpp`, `tests/parity/`, `scripts/ibex-e2e.sh` |

## Related

`[[project_fs_plugin_and_map_clause]]` (W1 Slice 1, landed) ·
`[[project_physical_fallback_adapter]]` · `[[project_interpreter_tu_split]]` ·
`[[project_repl_two_path_consolidation]]` · `[[project_e2e_lazy_query_harness]]` ·
`[[project_object_equivalence_script]]` ·
`plans/kernel-pipeline-execution-plan.md` (the architectural successor — this
plan is explicitly *not* that) ·
`plans/extern-series-arguments-plan.md` (the `Series<T>` extern-arg ABI, pairs
with W2) · the retired count-window plan (W5 backstory,
`git show 0d069262:plans/count-window-plan.md`)
