# Scan-instance split cost gate

Capture the q21 fruit that [[project_scan_instance_split_no_cost_gate]] and
[[project_q21_is_occupancy_bound]] point at. Scoped small: one gate, one query
to flip, no new estimator framework.

## The problem

`split_scan_instances` (`src/ir/scan_predicates.cpp`) renames every scan of a
multiply-referenced lazy source to a per-reference instance name
(`lineitem#1`, `lineitem#2`, …) so `scan_predicates` can push a *different*
pushed predicate + column demand into each. **No cost model — it splits
whenever `count > 1`.**

The rename has a load-bearing *correctness* job: `ColumnOrigin` / FD reduction
identify a base column by scan-instance name, and a self-join whose two sides
share one name would let a uniqueness proof on one side wrongly determine the
other's columns (repl.cpp comment at the `split_scan_instances` call). **But
that job is finished by the time the physical decode is chosen** — FD reduction
(`reduce_functionally_dependent_group_keys`) and
`reduce_duplicate_distinct_columns` both run *between* the split (~repl.cpp:5014)
and `scan_predicates` (~repl.cpp:5049). After them, the instance identities can
be merged back for the physical decode with no correctness cost.

### q21, measured (SF-4, `project_scan_instance_split_no_cost_gate`)

`lineitem` is scanned twice:
- `#2` — 1 column (`l_orderkey`), 24M rows whole, no filter → `count() by l_orderkey`, pool_work ~365ms.
- `#1` — 4 columns, `l_receiptdate > l_commitdate` pushed → semi-join probe, pool_work ~798ms, +107ms stage.

So `lineitem` decodes **twice** (~1163ms total pool_work; `l_orderkey` on both
sides), and the probe (node 18) is **100% ring_wait** — it cannot start until
the aggregate (node 11) finishes *and* `#1` feeds it. A shared scan (union
projection `{orderkey, suppkey, commitdate, receiptdate}`, whole, one residual
`Filter` for the probe side) deletes `#2` entirely and lets the aggregate and
the probe pipeline off one pass.

Two costs the split pays that the naive col-count comparison misses:
1. **A pushed predicate still decodes its predicate columns densely over every
   row** (the [[project_scan_fusion_cost_gate_gap]] physics) — so `#1` decodes
   `commitdate` + `receiptdate` over all 24M rows regardless, and only the
   *remaining* demand (`orderkey`, `suppkey`) scales by survival.
2. **The split serializes two consumers** a shared scan would let pipeline,
   when they sit on opposite sides of a blocking breaker.

## Design

A pass **`merge_redundant_scan_instances(plan, row_counts, schemas)`**, run
right after `reduce_duplicate_distinct_columns` and before `scan_predicates`.
For each base source with ≥2 live instances it costs *shared* vs *split* and,
when shared wins, renames those instances back to the base name. `scan_predicates`
then sees `scan_counts[base] > 1` and declines to push (its existing
`scan_counts[name] != 1` guard) — the per-instance `Filter` nodes, which the
rename-only split never removed, stay in the plan and run post-decode. Demand
unions naturally through `required_columns`. One scan registration results.

### Cost model

Per base source `S`, `rows` from `row_counts` (exact, Parquet footer), column
type from `schemas`:

```
w(col)      = decode weight by ir::ColumnType — Int64/Date/Double/Bool ≈ 1,
              String/Categorical ≈ 4 (rough constants, one tuning pass)
W(cols)     = Σ w(col)

shared_cost = W(union of every instance's demand) × rows

split_cost  = Σ_instances [ ( W(predicate_cols_i)                      // dense, always
                            + W(remaining_demand_i) × survival_i )     // gathered
                            × rows ]
            + serialization_penalty(S)

merge S  ⇔  shared_cost ≤ split_cost × (1 − margin)      // margin ≈ 0.05
```

- `survival_i`: `estimate_cardinality` of the instance's filtered subtree ÷
  `rows` (falls back to `compound_selectivity` when the walk is inconclusive).
  An instance with no pushed predicate has `survival = 1` and
  `predicate_cols = {}`, so it contributes `W(demand_i) × rows` — exactly its
  share of the shared decode, i.e. splitting it off is never a decode win, only
  a demand-narrowing one.
- `serialization_penalty(S)`: walk from each instance's `Scan` to the nearest
  common ancestor. If the paths meet at a `Join` and at least one path crosses
  a blocking breaker (`Aggregate` / `Order` / `Distinct` / `Window`), the split
  forces that breaker to finish before the other side can feed the join. Add a
  penalty proportional to the blocked branch's estimated output — this is the
  q21 term. Keep it a single, documented heuristic; do not build a scheduler
  model.

### Why this flips q21 and little else

q21's `#2` is unfiltered (`survival = 1`, no predicate columns) → its split
contribution is `W({orderkey}) × 24M`, pure duplication of a column the shared
decode already pays for. `#1`'s predicate columns are decoded dense either way.
Plus the serialization penalty (probe blocked behind the 3M-group aggregate).
Every term points the same way.

The other multi-scan PDS-H queries — `nation` in q07/q08 (tiny, 25 rows, cost
negligible either way), self-joins where both sides carry a *selective* filter
— either cost out as "keep split" or are too small to matter. The full-suite
A/B is the check.

## Steps

1. **`scan_decode_cost.{hpp,cpp}`** (`src/ir/`) — `w(ColumnType)` weights and
   `W(columns, schema)`. Shared with the future `scan_fusion` gate; land it
   with only this caller.
2. **`merge_redundant_scan_instances`** in `scan_predicates.cpp` (it already
   owns `count_scans` / `rename_scans` — the merge is `rename_scans` in
   reverse, gated). Signature takes `row_counts` + `schemas` +
   `split.instances`. Returns the rewritten plan + the set of merged base names
   (for logging / `explain`).
3. **Wire into repl.cpp batch path** after `reduce_duplicate_distinct_columns`,
   before `scan_predicates`. The `--report-planner` line gains a
   `merged_scans=[…]` note. (Statement path at repl.cpp:3698 unchanged for now —
   the win is batch-only, like the measurement.)
4. **`serialization_penalty`** — the common-ancestor / blocking-breaker walk.
5. **Tests** (`tests/test_ir_*.cpp`): a q21-shaped fixture asserts one
   `Scan(lineitem)` after the pass and the probe `Filter` retained; a
   both-sides-selective self-join asserts the split is *kept*; a tiny-dimension
   multi-scan asserts kept (cost below noise, no churn). Byte-identity is
   covered by the existing interpreter/e2e suites — semantics do not change.
6. **Measure** — `benchmarking/tpch` interleaved A/B, HEAD vs this, SF-4 8-core,
   q21 + the full 22 (must not regress q02/q07/q08/q09/q19). Object-equivalence
   on q21. Per `MEASURING.md`: byte-identity check + accounting closure before
   any number is claimed.

## Not in scope

The `scan_predicates` / `project_where` fusion gate
([[project_scan_fusion_cost_gate_gap]]) and deferred-probe registration
selectivity ([[project_deferred_probe_no_cost_model]]) — same family, same
`scan_decode_cost` helper, separate schedules. Tuning the type weights against a
broad corpus (one rough pass here; revisit only if the A/B misclassifies).
