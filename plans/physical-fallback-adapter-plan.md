> **Note (2026-09-04):** the step SHAs below (`5f7afc59`, `94957719`) no longer
> resolve — the post-commit hook re-commits, so hashes recorded at authoring
> time drift. The work is in `a5183b9a` "Name the retained subtree in
> MaterializedCall", `d7f2d59f` "Route materializing breakers through one
> interpret_node fallback", `4923c02e` "Keep fallback-breaker inputs on the
> physical path" and `cb2888cd` "Accept the MaterializedCall adapter as the
> Phase 5 end state". Cite by subject line, not by hash.

# Explicit physical fallback adapter

Phase 5 item 3 of [`kernel-pipeline-execution-plan.md`](kernel-pipeline-execution-plan.md):
*"Remove obsolete `build_operator` recursion; migrate `interpret_node` to an
explicit physical fallback adapter."* Also tracked there as "Next" item 5 and
follow-up-sequence item 5 ("replace residual recursion with the explicit physical
fallback adapter, preserving mutation-tested `MaterializedCall` coverage").

## Problem

`build_operator_impl` (`src/runtime/chunked.cpp` ~4923-5405) still carries a
~430-line per-`NodeKind` `if`-chain below the physical-plan seam. The seam itself
is already clean:

```cpp
const physical::Plan plan = physical::plan_physical(node, registry, externs);
if (plan.migrated) return build_migrated_physical_operator(...);   // join, agg, order,
                                                                  // distinct, head/tail/topk,
                                                                  // map-pipeline
physical::note_materialized_call(plan.reason, node.kind());
// ... ~430 lines of per-kind if-chain ...
// tail:
auto table = interpret_node(node, registry, scalars, externs, exec, model_out);
return make_table_source(std::move(table.value()));
```

The `if`-chain splits three ways:

| Bucket | Kinds | Branch behaviour |
|---|---|---|
| **A. Materializing breakers** | `Columns, Melt, Dcast, Cov, Corr, Transpose, Matmul, Resample, Window, AsTimeframe, Model, Construct, Stream, Program`, materializing `Join`, non-row-local `Update` | recurse `build_operator(child)` → `materialize_operator` → table fn → `make_table_source` |
| **B. Genuinely-streaming sources** | `Scan`+`stream_scans` (deferred lazy scan), `ExternCall` | build a real chunked / pipelined source operator |
| **C. Near-dead map duplicates** | `Filter, Project, Rename` | build row-local map operators, recursing into `build_operator` |

Confirmed: `interpret_node` has a branch for every bucket-A kind and recurses into
itself for children, so bucket A hand-rolls child recursion that `interpret_node`
already does. The function tail *is* the adapter — bucket A only needs to reach
it.

`explain physical` prints `MaterializedCall(<reason>)` with no node identity,
contradicting the header contract (`physical_plan.hpp:36`: *"`MaterializedCall`
naming the logical subtree retained by the fallback"*).

## Steps

### Step 1 — Name the `MaterializedCall` node (observability first) — DONE `5f7afc59`

`explain_physical` (`physical_plan.cpp` ~760) emits the root `NodeKind` in the
`MaterializedCall(...)` line, e.g. `MaterializedCall(Melt: root is not a
row-local map)`. `plan.root` is always set (`plan_physical` line 509). Reuse
`node_kind_name_impl`. Update the three affected assertions in
`tests/test_physical_plan.cpp` (lines ~562, ~760, and the join-materializing
block ~1138). No execution change. Landed first, on its own commit.

### Step 2 — Collapse bucket A into the tail adapter — DONE `94957719` (pending A/B)

Deleted the 15 per-kind `if` blocks (`Columns, Melt, Dcast, Cov, Corr,
Transpose`, materializing `Join`, `Matmul, Update, Resample, Window, AsTimeframe,
Model, Construct/Stream, Program`) so they fall through to the single
`interpret_node` + `make_table_source` tail. Each was a hand-synced copy of an
`interpret_node` branch that produces the same table via the same table fn with
the same args and error strings (verified, including the grouped-update
rank/tuple dispatch and the `window` + `select_only` projection). `Model`
threads `model_out`; `Program` runs the preamble — both handled by
`interpret_node`. `build_binary_materializing_operator` had no other caller and
was removed.

Initial wholesale collapse (route everything to `interpret_node`) confirmed a
regression: `join_filter_rank` +14.7% (`regression` verdict, 15 repeats). Root
cause — a `Filter` feeding a bucket-A breaker (here the grouped-rank `update`
between a join and its output filter) lost the fused parallel scan `build_operator`
gave it; `interpret_node` re-evaluated it whole-table and serial.

### Step 3 — `build_materialized_fallback` keeps input construction on the physical path

`build_operator_impl`'s tail now calls `build_materialized_fallback`, which:

1. Resolves the node's **relational inputs** via `fallback_relational_inputs` — a
   `switch` allowlist. Most kinds: the direct children. `Window`: the *grandchild*
   (its direct child is an `update` clause `interpret_node` must own). `Stream`,
   `Construct`, `Program`, and anything unlisted: none (their children are
   template / expression nodes, not relational subtrees).
2. Builds and drains each input via `materialize_row_local` (= `build_operator` +
   `materialize_operator`) — the fused parallel path.
3. Runs `interpret_node` over the node with those inputs handed back through a
   new `ExecutionContext::pre_materialized_children` list (node ptr → table).
   `interpret_node` checks it at entry and returns the pre-built table instead
   of recursing; only direct inputs are listed, so the fallback node itself and
   everything deeper are interpreted normally.

`interpret_node` still owns every per-kind semantic; this only moves where the
inputs are built. Recoverable end state is unchanged — a kind can still be
lifted to a fully migrated breaker-over-pipeline, driven by
`physical_fallbacks_for(kind)`.

Validation: all 1,815 non-slow tests; strict GCC; Debug + Release builds. A
first attempt without the `Window` grandchild / `Stream` exclusions failed 51
tests (evaluating an `update` clause or a `__stream_input__` transform
standalone) — the allowlist is load-bearing. Release A/B over
`join,reshape,window,stats,transform,multi` vs pre-step-2 pending.

### Step 3 — Separate bucket B from the fallback bucket

`Scan`(deferred) and `ExternCall` are not materialized calls — the plan
deliberately leaves bare-source streaming to the executor (`EmptyChain`). Today
`note_materialized_call` fires for them, polluting the migration-backlog counter.
Either teach `plan_physical` to mark bare streaming sources `migrated`, or
exclude `EmptyChain` bare-source from the count. Keep the operators unchanged.

### Step 4 — Prove bucket C dead

A `Filter`/`Project`/`Rename` root over any classifiable source or breaker
becomes a migrated `MapPipeline`. Confirm the residual branches are reachable
only on `MalformedMapNode` (where they return the same structural error), then
replace with `invariant_violation` or delete.

### Step 5 — Confirm recursion is gone

After step 2 the only `build_operator` recursion left is inside migrated builders
(join / aggregate / order children) and bucket B sources. That is the
"remove obsolete `build_operator` recursion" goal met.

### Step 6 — Validate

Focused physical/interpreter tests; `explain physical` snapshot updates; full
non-slow suite (1,815); strict GCC runtime build; Debug + Release Parquet /
LightGBM plugin builds; `check-object-equivalence.sh` on the touched breakers;
Release interleaved A/B.

## Expected outcome

`chunked.cpp` drops ~400 lines. The fallback becomes one tail adapter plus a
`MaterializedCall` node that names its subtree in `explain physical`.
Mutation-tested `MaterializedCall` coverage in `tests/test_physical_plan.cpp` is
preserved and extended with the node name.

## Not in scope

Porting the 6 materializing joins (intended `MaterializedCall`); a join
build-side cost model; deferred-probe selectivity. Same exclusions as the parent
plan.
