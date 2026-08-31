# Scan-instance split cost gate

Capture the q21 fruit [[project_scan_instance_split_no_cost_gate]] and
[[project_q21_is_occupancy_bound]] point at.

## Problem

`hoist_extern_sources` coalesces repeated `read_parquet("x")` calls to one
`Scan(__ibex_source_N)`. `split_scan_instances` then renames each occurrence to
`__ibex_source_N#1`, `#2`, … so `scan_predicates` can push a *different*
predicate + column demand into each instance's decode. **No cost model — it
splits whenever a source is scanned more than once.**

### q21, measured (SF-4, from [[project_scan_instance_split_no_cost_gate]])

`lineitem` is scanned twice and **decoded twice** (~1163ms pool_work total;
`l_orderkey` on both sides):
- `#2` — 1 column (`l_orderkey`), 24M rows, no filter → `count() by l_orderkey`
  (~365ms).
- `#1` — 4 columns, `l_receiptdate > l_commitdate` pushed → semi-join probe
  (~798ms + 107ms stage).

The probe is **100% ring_wait** — it cannot start until the aggregate *and*
`#1`'s decode finish. A single shared decode of the 4-column union deletes `#2`
(~365ms pool_work, ~30–45ms wall) and lets the aggregate and probe run off one
in-memory table.

## Approach — merge cost-losing instances back, after FD reduction

`split_scan_instances` does two jobs: (1) give each occurrence a distinct
**identity** for `reduce_functionally_dependent_group_keys`, whose
`ColumnOrigin` keys on `(source_name, column)` and would otherwise conflate the
two sides of a self-join (`repl.cpp` comment at the split call); (2) give each
occurrence a distinct **name** so `scan_predicates` and `required_columns` can
be per-instance.

Job (1) is done the moment FD reduction finishes (`repl.cpp:5028`). Job (2) is
only worth its duplicate decode when the per-instance pushdown pays for it. So:

**after FD reduction, rename the instances of a cost-losing source back to the
base name.** No round-trip in spirit — the split is a scoped identity for one
pass, and the merge only touches sources where the split's *other* job (per-
instance pushdown) was not worth having. FD reduction keeps its distinct
identities; nothing downstream of it needs them.

Once merged, a source flows through the existing machinery:
- `scan_predicates` sees `scan_counts[name] > 1` → pushes nothing (its existing
  `!= 1` guard). The per-reference `Filter` nodes — which the rename-only split
  never removed — stay and run post-decode.
- `required_columns` yields one demand entry (the union).
- the eager registration decodes it **once** into `tables[name]`; every
  `Scan(name)` node slices that one shallow-handle `Table`, exactly as a
  `let x = …` self-join already does.

One executor change: the streaming-registration gate
(`repl.cpp:5142`, `if (exec.stream_scans && lazy->scan_units().size() > 1)`)
gains `&& !merged.contains(name)`, so a merged source takes the eager
`lazy->project(union)` path (one cache-backed decode) rather than two
independent per-unit streaming decodes.

### Why merge-after-FD and not skip-the-split

Skipping the split for a keep-shared source is tempting (no round-trip), but it
puts the un-split self-join back in front of FD reduction, and the exact shape
that misreduces is **underspecified** — the `repl.cpp` comment asserts a
"group key that is not actually redundant" gets dropped, but
`schemas_with_instances` copies *identical* unique-key facts to every instance,
so the protection is entirely in `ColumnOrigin` identity, and reconstructing the
failing case from the FD code is not obvious. A 15-line inverse-rename after the
pass that already works is less risk than getting that analysis wrong. If a
concrete failing case turns up during implementation and it is narrow, revisit.

## Steps

### 1. `scan_decode_cost.{hpp,cpp}` (`src/ir/`)

`w(ir::ColumnType)` — Int64/Date/Double/Bool ≈ 1, String/Categorical ≈ 4 (rough
constants, one tuning pass). `W(columns, SchemaInfo) = Σ w`. Small; shared with
the future scan-fusion gate, landed with only this caller.

### 2. `plan_scan_instance_merges(const Node& plan, const SourceStats&, const ScanInstanceSplit&) -> std::set<std::string>` (`src/ir/scan_predicates.cpp`)

Runs on the post-FD-reduction plan (instance names present). For each base
source with ≥2 live instances:

1. **Per-instance facts, from passes that already compute them.**
   `ir::required_columns(*plan)` is keyed by scan name → post-split it yields
   `lineitem#1` / `lineitem#2` demand directly, no subtree walk.
   `ir::scan_predicates(*plan)` returns the exact conjuncts that *would* push to
   each instance.
2. **Prunable guard.** If any instance's pushable conjuncts contain a
   `column <cmp> literal` comparison (`< <= > >= ==`), the split may be
   skipping row groups via footer min/max (`parquet.hpp:2443`) — a decode
   saving the merge would forfeit. Do **not** merge that source. `column <cmp>
   column` (q21's `l_receiptdate > l_commitdate`) is not prunable → merge
   allowed. A plain AST check on the conjuncts, no stats.
3. **Duplication cost** — `union` = every column any instance demands;
   `duplicated` = Σ over columns `c` of `w(c) × (instances_demanding(c) − 1)`.
   With the prunable guard already applied, no instance's pushed predicate
   prunes the decode, so every instance decodes all `rows` of its demanded
   columns either way — the only saving is the columns decoded more than once.
4. **Merge iff** the prunable guard passed **and**
   `duplicated ≥ 0.15 × W(union)` **and** `duplicated × rows ≥ 1e6` (absolute
   floor — a 25-row dimension never qualifies).

Returns the set of base names to merge.

### 3. `merge_scan_instances(NodePtr, const std::set<std::string>& bases, const ScanInstanceSplit&) -> NodePtr`

The inverse of `rename_scans`: walk the plan, rewrite `Scan(base#N)` → `Scan(base)`
for every `base` in the set. ~15 lines next to `rename_scans`.

### 4. Wire into `repl.cpp`

```cpp
rewritten = ir::reduce_functionally_dependent_group_keys(std::move(rewritten), schemas_with_instances);
rewritten = ir::reduce_duplicate_distinct_columns(std::move(rewritten));
auto merged = ir::plan_scan_instance_merges(*rewritten, source_stats, split);
if (!merged.empty()) rewritten = ir::merge_scan_instances(std::move(rewritten), merged, split);
```

`merged` threads to the streaming-registration gate. `--report-planner` gains a
`merged_scans=[…]` note.

### 5. Tests (`tests/test_ir_required_columns.cpp` — it already covers `split_scan_instances`)

- q21 shape: self-semi-join, one unfiltered side. `plan_scan_instance_merges`
  returns the base name; after `merge_scan_instances` the plan has one
  `Scan(base)` and the probe `Filter` retained.
- both-sides-narrow-demand double scan, little overlap → not merged (0.15).
- 25-row dimension ×2 → not merged (1e6 floor).
- an existing `split_scan_instances` test extended: split then merge is
  identity when the whole source is merged.
- byte-identity of q21's result: the existing e2e / `check_answers.py` suites —
  semantics do not change.

### 6. Measure

`benchmarking/tpch` interleaved A/B, HEAD vs this, SF-4 8-core, q21 + full 22
(must not regress q02/q07/q08/q09/q19). `check-object-equivalence.sh` on q21.
Per `MEASURING.md`: byte-identity + accounting closure before any number.

## Risks / open questions

- **Parallel path shares the registry `Table` — verified.** In
  `build_operator_impl` (chunked.cpp:1485) the streaming branch is gated on
  `!registry.contains(scan.source_name())`; a merged source *is* in the registry
  (the demand loop's `project` populated `tables[base]`), so it falls to
  `build_materialized_fallback` → `interpret_node`, which does
  `Table output = it->second` (shallow copy) → `make_table_source`. Two
  `Scan(base)` nodes → two shallow copies of one decoded table — the same path a
  `let x = …` self-join already takes under `IBEX_PARALLEL`. One physical decode.
- **Which PDS-H queries even have a repeated scan?** Enumerate before the A/B
  (`IBEX_PLAN_STATS` / grep the split's `instances` map per query). Known:
  q07/q08 (`nation` ×2 — excluded by the 1e6 floor), q21 (`lineitem` ×2 — the
  target). Any other that merges must be in the A/B set, not just the usual
  suspects.
- **Eager vs streaming trade.** A merged source is forced eager, giving up
  decode↔first-operator overlap. For q21 (breaker-heavy) that overlap is
  negligible and the win is measured. If a streaming-heavy query regresses in
  the A/B, the `0.15` / `1e6` thresholds are the knobs — no query-specific
  branch.
- **One decode, downstream of the registration.** `for [name, needed] : demand`
  iterates once per unique name; a merged source has one name → one
  `lazy->project(union)` → `tables[name]`. Both `Scan(name)` nodes then read
  that entry (subject to the parallel-path question above). Object-equivalence
  on q21 is the check.
- **Deferred-probe interaction.** A merged source is scanned ≥2× → never the
  "sole feed of a join's right side" `deferrable_probe_scans` needs (and only
  fires for `JoinKind::Inner`; q21 is semi) → no conflict. Confirm in step 2.
- **`plan_scan_instance_merges` sees the pre-`scan_predicates` plan** — demand
  and conjuncts are accurate (reorder + FD reduction done; predicate
  *absorption* removes Filter nodes but does not relocate them between scans).
- **Statement path** (`repl.cpp:3698`) unchanged — the win is batch-only, like
  the measurement.

## Not in scope

The `scan_predicates` / `project_where` fusion gate
([[project_scan_fusion_cost_gate_gap]]) and deferred-probe registration
selectivity ([[project_deferred_probe_no_cost_model]]) — same family, share the
`scan_decode_cost` helper, separate schedules. Real sampled selectivity — only
if the A/B shows the heuristic misclassifying. Deleting `split_scan_instances`
in favour of scan-node-identity in `ColumnOrigin` — a larger cleanup that would
subsume this, noted but not taken.
