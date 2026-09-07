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
| `map { }` clause | via S3 peel; W1a keeps it there, S2 declines | **no** — `lower` errors "map { } runs only on the interpreter path" | W1a (S1) · W1b (S2, deferred) |
| non-literal extern-call args (`read_csv(runtime_path)`) | **no** — `src/repl/repl.cpp:5010` "whole-script execution requires literal extern arguments" | **no** — `src/codegen/emitter.cpp:1422/1469` "non-literal argument in extern call" | S1 half falls out of W1a's `emit_raw_expr`; general lift W2; S2 half W1b |
| top-level `fn … -> DataFrame` as the result | S2 declines on **any** `fn` (`src/repl/repl.cpp:~5216`) → runs on S3 | **no** — "no expression to lower" / "unsupported scalar let" | W3 |
| `model { }` clause | yes | **no** — `src/codegen/emitter.cpp:860` "model clause is not yet supported in compiled mode" | W4 |
| `window` + `select` | yes | **no** — `emitter.cpp:509` | W5 |
| `aligned` window | yes | **no** — `emitter.cpp:513` | W5 |
| windowed `update` with tuple fields | yes (partial) | **no** — `emitter.cpp:518` | W5 |
| stream constructs | S3 only (event loop) | partial (`NodeKind::Stream` case exists) | audit in-session |

`emitter.cpp` node cases: 31 of the ~35 `NodeKind`s. Missing / stubbed: `Map`
(new), plus the throw-guards above.

---

## Step 0 — make divergence loud (do this first)

Upgrade the parity harness from allowlist to **conformance gate**:

1. Every `.ibex` under `tests/parity/cases/` that surface 3 runs must also
   transpile+run on surface 1 **or** carry a sibling `<name>.unsupported`
   marker file with a one-line reason. A case that neither matches nor is
   marked fails the test.
2. Add a `tests/parity/cases/` case for *each* construct in the matrix above —
   the ones that fail get `.unsupported` markers naming their workstream. This
   turns the matrix into executable state: closing a workstream = deleting its
   markers.
3. Point `plans/README.md` and this file's matrix at the marker set as the
   source of truth.

`tests/parity/structured_runner.cpp` + `run_parity.sh` are the files.

---

## W1 — `map { }` on surface 1 (transpile), then optionally 2

Slice 1 (the `map { }` clause on surface 3) is **landed** — see
`[[project_fs_plugin_and_map_clause]]`.

**Reframe (see "What the transpiler actually emits"):** `map` is a `for` loop.
The emitter can emit the loop directly — with `read_csv` / `write_parquet` as
literal C++ calls — and that needs **zero runtime changes**. The hard thing (a
runtime expression-level extern evaluator) is only needed to make the
*interpreter* plan a `map`, which is marginal: `map`'s prefix is almost always
a trivial `list_files(...)`, so whole-script-planning it buys nothing, and S3
already runs `map` via the peel.

So W1 splits into a **required** emitter track and a **deferred** runtime track.

### W1a — `MapNode` + native-loop emit (surface 1) — REQUIRED

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

### W1b — runtime expression-level extern evaluator (surfaces 2 & 3) — DEFERRED

Only worth doing to (a) let S2 whole-script-plan a `map` prefix (low value) or
(b) retire the S3 peel and unify the two expression evaluators (cleanliness).
Shares its core with **W2**. `eval_expr` (`src/runtime/expr.cpp`) gains an
extern arm — dispatch an `ir::CallExpr` whose callee is a registered extern to
`eval_extern_expr(call, table, row, scalars, externs)`:
- scalar extern → eval args, call `fn->func`.
- scalar extern, `first_arg_is_table` → arg 0 is a `CallExpr` to a
  table-returning extern; recurse to a `Table`, call `fn->table_consumer_func`.
- table extern → valid only as arg 0 of a consumer (lift the
  `first_arg_is_table` guard in `invoke_extern_call` to be position-aware).
Home: `extern_call.cpp` (has the registry), forward-declared for `expr.cpp`.
Then `src/runtime/interpreter.cpp` gets a `case ir::NodeKind::Map` (port the
Slice-1 row loop onto the runtime `eval_expr`), the S2 decline is removed, and
the S3 peel is deleted. Not on the critical path.

**Deliverable (if done):** `let n = write_parquet(read_csv(^p), ^o);` runs on
S2/S3; S2 plans `map` prefixes; one expression evaluator instead of two.

---

## W2 — non-literal extern-call arguments (general)

W1a's `emit_raw_expr` extension already makes the *emitter* half work in a
`map` body. W2 finishes it for **top-level** `ExternCallNode` args so
`read_parquet(^path)[filter …]` / `write_parquet(t, ^out)` compile and
whole-script-plan: emit the arg as a C++ expression (or an `ibex::ops::eval_scalar`
call), lift `src/codegen/emitter.cpp:1422/1469` and the whole-script
`literal_args` guard (`src/repl/repl.cpp:5010`, `~5000`) to evaluate against the
`ScalarRegistry`/`DeferredScalarBinding` set. The S2 half shares W1b's runtime
evaluator. Coordinate with `plans/extern-series-arguments-plan.md` (`Series<T>`
extern-arg ABI).

---

## W3 — user functions on surfaces 1 & 2

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

`src/codegen/emitter.cpp:860` throws. Model fitting needs the model registry +
plugin dispatch at runtime; the `ops` layer would need `ibex::ops::model_fit` /
`model_predict` wrappers over the same `ModelOps` the interpreter calls
(`include/ibex/runtime/extern_registry.hpp` `ModelOps`). Scope: fit + the
`model_*` accessors (`model_summary`, `model_predict`, `.r_squared`).

---

## W5 — window / resample edge combos

`emitter.cpp:509/513/518`: `window + select`, `aligned` window, windowed
`update` with tuple fields. The interpreter supports these; the emitter
rejects. `plans/count-window-plan.md` records that per-construct codegen parity
has been chased one combo at a time — W5 is finishing that list. Lower priority
(narrow shapes).

---

## Sequencing

1. **Step 0** (parity conformance gate) — first; everything is measured
   against it.
2. **W1a** (`MapNode` + native-loop emit) — self-contained, no runtime
   changes, delivers `map` on `ibex_compile`. This is the emitter learning to
   emit real code; `emit_raw_expr`-as-C++ is reused by W2 and W4.
3. **W3** (functions) — independent, high user value, the S2 half is one line.
4. **W2** (top-level non-literal extern args) — emitter half falls out of
   W1a's `emit_raw_expr`; S2 half needs W1b.
5. **W1b** (runtime extern-expr evaluator) — only if S2 `map` planning or
   evaluator unification is wanted. Deprioritised by the reframe.
6. **W4 / W5** — lower priority, independent.

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
- **W1b (if done):** `tests/test_extern_expr.cpp` — runtime `eval_expr` of
  `write_csv(read_csv(^p), ^o)` against a hand-built registry (mirror
  `tests/test_fs.cpp`); the S3 peel deletion must leave `[map]` green.
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
- **`MapNode` as an emitter-only IR node.** Unusual but honest given the
  reframe: S3 peels, S2 declines, so `interpret()` never sees it. Document it
  loudly at the `MapNode` definition and in `try_execute_whole_script`.
- **Optimizer barrier correctness** — even emitter-only, `MapNode` sits in the
  IR that `ir::optimize_plan` walks (surface 1 runs the optimizer). A pass that
  reorders/drops work across it is a silent miscompile. `NodeKind::Stream` is
  the existing "opaque, ordered, effectful" node; match it everywhere.
- **W3 decline removal** needs a full parity + e2e pass before trusting it —
  the July-2026 guard may hide a real `lower_script` gap for some function
  shape.
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
| W5 | `src/codegen/emitter.cpp:509/513/518` | `tests/test_codegen.cpp`, `plans/count-window-plan.md` |

## Related

`[[project_fs_plugin_and_map_clause]]` (W1 Slice 1, landed) ·
`[[project_physical_fallback_adapter]]` · `[[project_interpreter_tu_split]]` ·
`[[project_repl_two_path_consolidation]]` · `[[project_e2e_lazy_query_harness]]` ·
`[[project_object_equivalence_script]]` ·
`plans/kernel-pipeline-execution-plan.md` (the architectural successor — this
plan is explicitly *not* that) ·
`plans/extern-series-arguments-plan.md` (the `Series<T>` extern-arg ABI, pairs
with W2) · `plans/count-window-plan.md` (W5 backstory)
