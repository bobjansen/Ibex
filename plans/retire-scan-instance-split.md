# Retire `split_scan_instances`; share a repeated scan by default

**LANDED 2026-08-31** (`f9b0866e` FD identity, `40c6e497` delete,
`<probe-fix>` probe isolation). Fixed the q21 duplicate decode
[[project_scan_instance_split_no_cost_gate]] / [[project_q21_is_occupancy_bound]]
by removing the mechanism, keeping the one job it did that mattered
(deferred-probe occurrence identity) in a narrow purpose-named pass.

**Result (SF-4, 8-core, vs pre-change):** q21 −3.7% (noisy, ~5–10%), q03 −12.5%,
small wins q05/q07/q11, q18 +3.7% (recovered from an intermediate +83% — see
below), geomean 0.997, polars reference flat. `check_answers.py` 22/22,
1813 tests, strict GCC, ASAN/UBSAN parity all pass.

**The q18 lesson:** deleting the split outright regressed q18 83%. Its main
`lineitem` join is a *deferrable probe* (semi-join pushdown filters `orders` to
~57 rows first, so the probe decodes a few hundred lines not 24M), and
`collect_deferrable` requires the probe source's scan count to be 1 — which a
self-referenced `lineitem` (also the aggregate side) fails.
`isolate_deferrable_probe_scans` fixes it: rename **only** the scan occurrence
that is the right-child probe subtree of an eligible inner join, when its source
is scanned elsewhere. q21's join is semi → not isolated → its `lineitem` stays
shared, the win holds.

**Attempted and reverted 2026-08-31: keying deferred probes by `Scan` node id
instead of a name.** The idea was to delete `isolate_deferrable_probe_scans`
entirely — `deferrable_probe_scans` returns `{NodeId scan, source, key_column}`,
`ExecutionContext` gets a `DeferredProbeRegistry` keyed by node, and dropping
the `count == 1` guard lets a self-join's probe occurrence defer directly.
Compiled and passed the 1813 fast tests, but broke **6–10 TPC-H answers**
(q2/q3/q5/q7/q8/q9/q10/q11/q16/q21 — wrong joins, raw ints where dates belong).
Root cause not fully isolated but two contributors: (1) the demand loop still
eagerly decodes the source, and `interpret_node`'s Scan handler resolves
`registry.find(name)` before any node-keyed probe, so the probe occurrence read
the full table; (2) `interpret_wrapped_right`'s `local[name] = probe_table`
shadow, harmless with a `#1` name, collides with the source's other uses when
the name is real; (3) dropping `count == 1` made `collect_deferrable` fire on
inner-join right-side scans that were not previously eligible. The
name-scoped decode registration is doing more coordination than it looks. A
node-keyed rewrite would need `interpret_node` / `build_operator` / the demand
loop all made node-aware together — out of proportion to removing one `#1`.
`isolate_deferrable_probe_scans` stays.

Original plan below.

## What `split_scan_instances` is, and why it should go

Added **2026-07-13** with "direct filtered Parquet scans" so a source scanned
twice could still get *per-occurrence* predicate pushdown and column demand —
before it, `scan_predicates` abandoned pushdown entirely for any source with
`scan_counts[name] > 1`. It renames each `Scan(src)` to `Scan(src#1)`,
`Scan(src#2)`, … so every occurrence has a count of 1.

**A month later** `reduce_functionally_dependent_group_keys` landed, and
`64868d53` had to sequence it *after* the split and hand it
`schemas_with_instances`: `ColumnOrigin` keys a base column by
`(scan-instance name, column)`, so before the split, two occurrences of one
source share a name and a unique-key proof on one occurrence wrongly determines
the other's columns (concrete repro in `64868d53`: self-join on a non-unique
column + a row-preserving join on the table's unique key elsewhere → a group
key silently collapses to `first()`).

So the split now serves **two** purposes bolted together:
1. **occurrence identity** for FD reduction — a genuine correctness need, but
   the wrong tool: it is a *plan mutation* standing in for a *derived property*.
2. **per-occurrence pushdown** — measured as a **net loss** on the one query it
   matters for (q21: `lineitem` decoded twice, ~365ms of pure duplication, the
   semi-join probe 100% ring_wait behind it), and negligible everywhere else
   (`nation` ×2 in q07/q08 is 25 rows).

Purpose 1 belongs in `ColumnOrigin` — occurrence identity is derivable from the
`ScanNode`. Purpose 2 splits into: the common case (a filter shared by every
use of a source) which `scan_predicates` can push without any split at all by
taking the intersection, and the rare case (divergent prunable filters) which
belongs on the physical plan, not in a logical-IR name rewrite.

## Design

### 1. Occurrence identity in `ColumnOrigin` (the correctness fix)

`ColumnOrigin` gains `NodeId scan` — the id of the `ScanNode` a column's values
came from. `column_origins.cpp` sets it in the one `Scan` case
(`column_origins.cpp:148`, `scan.id()` is in hand); the top-down walk already
inherits each output column's origin from its input (Filter/Project/Rename/Join
all copy `it->second`), so `scan` rides through with no extra plumbing.

**Verified the two occurrences have distinct ids:** a `let x = read_parquet(…)`
reference lowers via `clone_node` (`lower.cpp:1878`), which rebuilds every node
through `builder_` → `next_id()` → **fresh ids per clone**;
`hoist_extern_sources` then makes one `ScanNode` per call site *preserving that
node's id* (`extern_sources.cpp:82`) while coalescing only the source *name*. So
`T ⋈ T` gives two `Scan(__ibex_source_0)` nodes with different ids. (A `let`
promoted to a shared binding lowers to `Scan("x")` and is materialized once by
the shared-binding loop — never split, already shared.)

`group_key_reduction.cpp`'s `SourceColumn` (today `std::pair<string,string>`,
"ordered so it can key a set") becomes
`{NodeId scan; std::string source; std::string column}` with an `operator<=>`
over `(scan.value, source, column)` — `scan` + `column` are the set/edge
identity (two occurrences no longer conflate); `source` stays the key for
`facts_for(source, schemas)` (schema + proved unique keys, by real table name).
`as_key`, `JoinEdge` construction, and the closure thread the extra field; the
reduction logic is unchanged. `column_origin_of` callers in
`scan_predicates.cpp` (the deferred-probe gate, `plan_join_key_origins`) read
`origin.source` only — unaffected.

Result: FD reduction distinguishes self-join occurrences from a **derived**
property, with the real (unsplit) source name available for every schema query.
`schemas_with_instances` is deleted.

### 2. Delete `split_scan_instances`

- `scan_predicates.{hpp,cpp}`: remove `ScanInstanceSplit`, `split_scan_instances`,
  `count_scans`, `rename_scans` (~60 lines). `scan_predicates()` and
  `plan_join_key_origins()` already key on the real source name and are
  unchanged.
- `repl.cpp` (both the whole-script driver ~5014 and the statement path ~3698):
  remove the `split` call, `split.plan` reassignment, `schemas_with_instances`,
  the `split.instances` fallbacks in `resolve_lazy` / `resolve_lazy_ptr`, and
  the instance-name additions to `deferrable_names`. FD reduction is then fed
  plain `schemas`.

### 3. `scan_predicates`: unchanged — no pushdown for a repeated scan

`scan_predicates` already erases a source's candidates when
`scan_counts[name] != 1` (`scan_predicates.cpp:276`). Without the split, a
source scanned twice hits that guard → no per-occurrence pushdown, the
use-site `Filter` nodes stay and run post-decode. This is exactly what q21
wants: `lineitem_raw` is bare, filtered only at one use site
(`[filter l_receiptdate > l_commitdate]`, which is `column <cmp> column` and
prunes nothing), so a residual filter costs nothing.

**An intersection push** (a conjunct every use of a source shares → push it to
the one shared decode) is a real future enhancement but needs structural `Expr`
equality, which does not exist yet. It is not built here: **no PDS-H query has
the shape** — every query is `let X_raw = read_parquet(…)` (unfiltered) with
distinct filters at each use site, so the intersection is always empty. Add it
only if the A/B or a real query shows a common-filter repeated scan losing
row-group pruning.

### 4. Execution: one decode

- `required_columns` yields one demand entry (the union of both occurrences).
- the eager registration decodes it **once** into `tables[name]`; both
  `Scan(name)` nodes read that entry, exactly as a `let x = …` self-join does
  today. Verified for the parallel path: `build_operator_impl`'s streaming
  branch is gated `!registry.contains(name)`, so a registry-backed scan falls
  to `interpret_node` → `Table output = it->second` (shallow) → one physical
  decode shared by both.

One executor change so a repeated scan does not stream two independent per-unit
decodes: the streaming gate (`repl.cpp:5142`,
`if (exec.stream_scans && lazy->scan_units().size() > 1)`) also requires the
name to be scanned once (a per-name occurrence count computed once from
`count_scan_occurrences`, not per-iteration). A repeated scan takes the eager
`lazy->project(union)` path — one cache-backed decode, with any intersected
conjunct applied via `project_where`.

### q21 consequence

`lineitem` is scanned twice, intersection empty → one shared decode of
`{orderkey, suppkey, commitdate, receiptdate}` → `#2`'s separate ~365ms
`l_orderkey` decode is gone and the probe filter runs off the same in-memory
table instead of waiting behind it. No cost model, no threshold — a repeated
scan is simply shared, and a filter common to every use of it still pushes.

### The one shape that genuinely loses

Occurrence A wants `[filter P]` where `P` is a *prunable range* and occurrence B
wants the source unfiltered. `P` cannot push into a decode B needs whole, so A
now pays a full decode + residual `P` where the split let it decode a pruned
subset. No PDS-H query has this (q21's divergent predicate is not prunable). If
the A/B surfaces one, the fix is a **cost-gated physical decode split for that
source** — the demand loop registering a second, filtered decode — decided on
the `physical::Plan`, not by mangling names in the logical IR. Do not pre-build
it.

## Steps

1. **Regression test** (`test_ir_group_key_reduction.cpp`): the `64868d53`
   repro built directly — two `Scan` nodes, **distinct `NodeId`, same source
   name**, self-joined on a non-unique column, grouped by `(left key, right
   name)`, plus a row-preserving join on the table's unique key elsewhere;
   plain `schemas`. Assert the right-name group key is **kept**. This *fails*
   today (the reduction pass conflates the two scans by name — the same bug the
   split works around in `repl.cpp`) and goes green after step 3. Red-green
   proof that the derived identity replaces the split's protection.
2. `ColumnOrigin::scan` + set it in `column_origins.cpp`; assertions in
   `test_ir_column_origins.cpp`.
3. `group_key_reduction.cpp`: `SourceColumn` gains `scan`; thread it through
   `as_key` / `JoinEdge` / the closure. `facts_for` unchanged (uses `source`).
   Step 1's test goes green here.
4. Delete `split_scan_instances` + `ScanInstanceSplit` + `count_scans` /
   `rename_scans`; delete the split plumbing in `repl.cpp` (both paths) and
   `schemas_with_instances`. Remove the now-dead `split_scan_instances` tests in
   `test_ir_required_columns.cpp`; keep the "scanned once keeps its name" and
   demand-union coverage. `scan_predicates` is unchanged — its `!= 1` guard
   already declines pushdown for a repeated scan.
5. Streaming gate: require occurrence-count 1 for per-unit streaming (one
   `count_scan_occurrences` map, not per-iteration), so a repeated scan decodes
   once eagerly instead of twice per-unit.
7. `--report-planner`: a repeated scan now reports as one source; adjust the
   note if it changes.
8. **Measure** — `benchmarking/tpch` interleaved A/B, HEAD vs this, SF-4
   8-core, the full 22. The one query this is *for* is q21; enumerate every
   other query with a repeated scan first (known: q07/q08 `nation`, q21
   `lineitem`) and make sure each is in the A/B set. `check_answers.py` at SF-1
   (exact answers) is the correctness gate for the FD change.
   `check-object-equivalence.sh` on q21.

## Risk

- **FD reduction is delicate** (`189edaec`, `21a6b352`). The change is
  mechanical — one field added to a set/edge key — and step 1's red-green test
  pins the exact correctness the split was protecting *before* it is removed.
  `check_answers.py` SF-1 (exact TPC-H answers) covers the rest of the suite.
- **Losing per-occurrence pushdown.** The intersection push (section 3) keeps a
  filter that every use of a source shares. Only a *divergent* prunable filter
  is lost, and only against an occurrence that needs the source whole — see "The
  one shape that genuinely loses". No PDS-H query has it; if the A/B finds one,
  the fix is a physical decode split, not this mechanism.
- **Statement path** gets the same treatment (it has the same split plumbing) —
  keeps the two paths consistent.
- **`deferrable_probe_scans` / `resolve_lazy` instance fallbacks** become dead
  code once names are never mangled — a repeated `Scan(name)` resolves directly
  and is never a "sole feed" deferrable probe. Delete, don't rework.

## Not in scope

The `scan_predicates` / `project_where` fusion gate
([[project_scan_fusion_cost_gate_gap]]) and deferred-probe registration
selectivity ([[project_deferred_probe_no_cost_model]]) — unrelated, and neither
depends on the split.
