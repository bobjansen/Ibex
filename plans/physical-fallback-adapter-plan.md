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

### Step 1 — Name the `MaterializedCall` node (observability first)

`explain_physical` (`physical_plan.cpp` ~760) emits the root `NodeKind` in the
`MaterializedCall(...)` line, e.g. `MaterializedCall(Melt: root is not a
row-local map)`. `plan.root` is always set (`plan_physical` line 509). Reuse
`node_kind_name_impl`. Update the three affected assertions in
`tests/test_physical_plan.cpp` (lines ~562, ~760, and the join-materializing
block ~1138). No execution change. Landed first, on its own commit.

### Step 2 — Collapse bucket A into the tail adapter

Delete the per-kind `if` blocks for every bucket-A kind so they fall through to
the single `interpret_node` + `make_table_source` tail. Gate each deletion on
proving the branch does nothing the tail does not:

- Pure `interpret_node`+wrap kinds (`Columns, Melt, Dcast, Cov, Corr, Transpose,
  Matmul, Resample, Construct, Stream`): trivially equivalent.
- **Risk kinds** — `Window`, non-row-local `Update`, `Model`, `Program`: the
  branch builds a child operator then calls a table fn. Must confirm
  `interpret_node`'s path matches, including per-call rolling `__window_n`
  handling, grouped-update overwrite semantics (see known-issue memories), and
  `Program` preamble execution. Verify with `check-object-equivalence.sh` on
  each and focused tests before deleting.

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
