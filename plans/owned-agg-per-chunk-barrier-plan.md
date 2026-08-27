---
name: owned_agg_per_chunk_barrier
description: "Phase 1b from the 2026-08-26 vs-Polars-streaming analysis: the owned aggregation path re-runs a 3-barrier radix pipeline per streaming chunk. Collapse it. Targets q18, secondarily q13/q20."
metadata:
  type: project
---

# Owned aggregation: kill the per-chunk barrier pipeline

Status: **scoped, not built** (2026-08-27). Prereq context: Phase 1a
(over-partitioning) is a measured dead end —
[[project_agg_partition_count_dead_end]]. Do not touch `part_count`; it stays at
`workers`.

## The problem, measured

`ChunkedAggregateOperator::try_owned` (src/runtime/chunked.cpp) is the
partition-owned accumulate path. On TPC-H q18 (`sum(l_quantity) by l_orderkey`,
~12M lineitem rows, ~1.5M near-unique groups, SF-2) it fires on all 12 streaming
chunks. Per-chunk it runs four phases, three of them pool barriers:

1. histogram: `pool.submit(workers)` → per-range × per-partition key counts — **barrier**
2. exclusive prefix sum over `counts` → `offsets` / `part_begin` — serial, calling thread
3. scatter: `pool.submit(workers)` → `scatter_rows_[cursor[part]++] = row` — **barrier**
4. accumulate: `pool.submit(min(workers, part_count))`, atomic cursor over
   partitions, worker owns partition `p`, walks `scatter_rows_[part_begin[p]..]`,
   `key_at(row)` (random gather back into the key column), probe/insert
   `partition.index`, update slot — **barrier**

Operator profile, q18 aggregate node, `IBEX_CORES` sweep (span_ms /
barrier_wait_ms / occupancy):

| cores | span | barrier_wait | occupancy | pool_work |
|------:|-----:|-------------:|----------:|----------:|
| 1 (serial path, no owned) | 405 | 0 | – | 0 |
| 2 | 391 | 328 | 0.70 | 548 |
| 4 | 250 | 184 | 0.59 | 594 |
| 8 | 191 | 131 | 0.40 | 615 |

`pool_work` (total work) is ~flat 550–615ms across 2/4/8 — parallelism divides,
does not multiply. The whole 2c→8c deficit is **barrier_wait**: 37 barriers
(≈12 chunks × 3), 131ms of workers parked at 8 cores. Polars streaming does the
entire q18 in 105ms at 8c (vs our 201ms) and scales 1→8 at 4.2× (we manage
1.9×); its biggest wins over its own in-memory engine are exactly the
high-cardinality-groupby queries (q21/q18/q13/q22).

`perf` (task-clock, 8c): 57%+3.8%+5.3% in the try_owned lambdas (accumulate
dominant), `robin_hood::insert_move` 4.3%, `__memmove_avx` 5.3% — the last two
are **per-partition hash-table rehash growth**: each partition's map grows from
empty to ~190k entries with no reservation, and `partition.slots` /
`.keys` / `.first_rows` grow one element at a time (`slots.resize((local+1)*n_aggs_)`
per new key).

Two independent costs, addressed in two stages:

- **rehash skew** — one partition hitting a rehash cascade parks the other 7 at
  the accumulate barrier (no work-stealing: `part_count == workers`, one
  partition per worker).
- **barrier count** — 3 barriers/chunk × 12 chunks, plus the random key-column
  gather in phase 4.

## UPDATE 4 2026-08-27 — parallel finalize merge: LANDED, ~7% on q18 AND q20

Reading `~/polars` end to end (`polars-stream/src/nodes/group_by.rs`,
`polars-expr/src/hot_groups/fixed_index_table.rs`) settled how Polars wins both:
a 4096-slot **second-chance hot table** that aggregates hot rows' values in a
cache-resident array (carries q18's sorted key; near-useless for q20's
scattered composite -- a `{partkey,suppkey}` pair recurs ~250k rows apart, long
evicted), **plus** three things that help every group-by: no per-chunk barrier
(async), cold groupers reserved from a `CardinalitySketch`, and a
**parallel per-partition finalize**. Measured Polars finalize: q18 ~40ms,
q20 ~27ms -- both across 8 cores.

Ibex's `finalize_owned` was **fully serial**: a linear K-way merge of the
`part_count` partition group-lists by `first_rows`. For q18's 3M groups that is
~24M comparisons + 3M slot copies on the calling thread.

**Fix (landed):** merge-path co-ranking. `first_rows` values are globally
unique row indices, so `sum_p lower_bound(first_rows_p, v)` steps by exactly
one at each value and equals any target output rank at exactly one `v`. Binary
-search `W-1` frontier values (each eval = `part_count` lower_bounds, ~30k ops
total), then `pool.submit(W)` merges the disjoint input slices between adjacent
frontiers into disjoint output slices. Byte-identical (a stable merge over a
total order); serial fallback below 128k groups / on a pool thread / when
`part_count < 2`.

Measured (SF-2, 8c, interleaved A/B vs HEAD, two runs of 30-40):
- **q18: -9 to -11% min, -6 to -8% mean**
- **q20: -4 to -9% min, -6 to -8% mean**
- q13, q11 (owned path, small): neutral. q01/q03/q07/q09/q17 (not owned):
  within ±3% noise.
- 22/22 `check_answers` (SF-1), 1791/1791 ctest.

The `part_count` / accumulate hot loop is unchanged -- this is purely the
output merge, which is why it helps the unsorted q20 as much as q18 and has no
regression surface. Remaining q18 gap to Polars is the hot-table (q18-specific,
multi-day) and the per-chunk barrier count.

---

## UPDATE 3 2026-08-27 — Stage A (hot table) and Design B both measured; neither lands

Instrumented the owned accumulate directly (`IBEX_AGG_TIMING`). Findings on q18:

- `child_->next()` (decode) returns in **0.0ms** from the aggregate's view —
  the pipelined scan already overlaps decode fully. **Lever B (overlap decode)
  does not exist; the scan is not blocking the aggregate.**
- `process_chunk` total = ~97–109ms, of which the **accumulate submit is
  ~78–91ms**; histogram + prefix-sum + scatter together are only **~18ms**
  across all 12 chunks.
- Per-partition accumulate is **well balanced** (8 partitions, ~131k rows /
  ~32k new groups each, times within 20%). The chunk-wall-vs-max-partition gap
  is ~1.5–2ms/chunk of dispatch.
- The accumulate is **latency-bound at ~30ns/row** (gather key → hash → probe →
  gather slot → FP-add), already 8-way parallel, running at ~4.6×/8 effective.

Four attempts, all measured (interleaved A/B, `taskset`, min+mean of 24–70):

| attempt | q18 | q20 | q13 | verdict |
|---|--:|--:|--:|---|
| 1a over-partition `part_count` | +13…+48% | +7% | — | dead end ([[project_agg_partition_count_dead_end]]) |
| Stage-1 serial `reserve` of partition maps | +44% | +30% | — | dead end (serialized parallel rehash work) |
| **Stage A** 1-entry key/gid cache only | −0.4% | +1.7% | −2.5% | neutral — repeated-key probes were already L1 |
| **Stage A'** run-collapsing accumulator (hold run's key+agg in regs, seed-from-slot + write-back, bit-identical) | −1 to −10% (noisy, ~−5% typical) | **+2 to +6%** | 0% | net negative |
| **Design B** single fused per-partition whole-chunk scan (1 barrier not 3, 8× redundant hash) | −2.6% | **+12%** | 0% | net negative — q20's unsorted composite key has no run-clustering to amortize the redundant hashing |
| Stage A' **gated** on a per-chunk clustering sample (run-collapse only when mean run ≥ 2; q18 yes, q20/q13 no) | −1 to −5% | **still +3 to +6%** | 0% | net negative — q20 regressed even on the plain (unchanged-logic) branch; suspected `resolve`-lambda / code-layout cost, not pinned down |

**Conclusion: the owned aggregate is not improvable by small tweaks.** It is
balanced, correctly parallel, and latency-bound at a reasonable per-row cost.
Every restructuring either serializes parallel work, adds redundant work to the
unsorted-key path (q20), or perturbs code layout enough to cost ~2–5% somewhere.
q18's 2× gap to Polars streaming needs the **full hot/cold architecture**
(a cache-resident hot grouper that *aggregates hot keys' values in place*, so
75% of the >L2 slot writes never happen — see UPDATE 2), which is a multi-day
rewrite, not a patch to `try_owned`. All experiments reverted; nothing kept.

Recommended pivot: q10's **36ms serial hash-join build** (`node=31`, no
documented dead end) or q22.

---

## UPDATE 2 2026-08-27 — what Polars streaming actually does (read ~/polars source)

Verified against `~/polars` (v1.42), `crates/polars-stream/src/nodes/group_by.rs`
+ `physical_plan/{lower_group_by,lower_ir}.rs` +
`polars-plan/src/plans/optimizer/sortedness.rs`, and a `POLARS_VERBOSE=1` run of
q18 streaming.

- **Polars does NOT exploit sortedness on q18.** `IR::Scan => None` in
  sortedness.rs: it never infers a sort order from a Parquet scan. There is a
  `SortedGroupBy` fast path but it needs `are_keys_sorted` which for
  `scan_parquet(...).group_by(...)` is false. Verbose confirms a generic
  `group-by` node. Our SF-2 parquet files carry no `sorting_columns` metadata
  either. So "we know l_orderkey is sorted" is a **dataset-incidental**
  numpy-diff observation, not a fact the engine (ours or theirs) can rely on.

- **Polars' streaming group-by is a two-tier hot/cold partitioned hash
  aggregation:**
  1. **Hot tier**: a `DEFAULT_HOT_TABLE_SIZE = 4096`-entry recency grouper per
     pipeline. Repeated/clustered keys hit it and are aggregated in-cache; when
     it fills it *evicts* the least-recent. On q18 l_orderkey is locally
     clustered (mean run 4), so a run of 4 equal keys hits the hot table 4×
     and evicts once — the hot table does run-length compression *for free,
     from locality, without any sortedness assumption*. ~12M rows → ~3M cold
     inserts.
  2. **Cold tier**: evicted keys are hash-partitioned into `num_partitions =
     num_pipelines` buckets (**same partition count as Ibex** under the
     benchmark's `taskset` — Polars is not winning on partition count),
     pre-aggregated per partition, with a `CardinalitySketch` per partition
     sizing its final grouper (no rehash cascade — this is what a correct
     version of the failed Stage-1 `reserve` looks like).
  3. Partitioning happens **incrementally during streaming eviction**, not as a
     per-chunk barriered histogram→scatter.
  4. The group-by is a **push Sink** fed by the scan's pipelines — aggregation
     overlaps decode fully. Ibex's `ChunkedAggregateOperator::next()` pulls one
     ~1M-row chunk, processes it (3 barriers), then pulls the next: serialized
     (`pool_unqueued_ms ~1054` on q18).
  5. Finalize combines partitions **in parallel via rayon**
     (`into_par_iter().map(into_df)`).

### Revised plan — the gap is mechanism, not sortedness

Priority order (all attack the same 131ms, none depends on sorted input):

- **A — hot-table tier.** Put a small (~2–8k entry) cache-resident recency
  grouper in front of the partition maps in `try_owned`. Clustered keys
  (q18's mean-run-4 l_orderkey, any GROUP BY over a semi-sorted natural key)
  get aggregated in-cache; only evictions reach the bandwidth-bound partition
  maps. This is the port of Polars' key idea and it is locality-driven, not
  order-assuming. Biggest expected lever.
- **B — overlap decode with accumulate.** The aggregate's child is a
  pipelined scan; `PipelinedStageOperator` (double-buffer on a raw thread)
  already exists for exactly this. Check `has_multi_unit_deferred_scan`
  admission for the aggregate here — if it is not firing, that is
  `pool_unqueued_ms` of free overlap.
- **C — incremental partitioning.** Route hot-table evictions straight into
  persistent partition maps as they occur, deleting the per-chunk
  histogram→prefix-sum→scatter (3 barriers → ~0). Subsumes the original
  "Stage 2" below.
- **D (optional, Ibex-specific) — ordered-run Sum.** Extend the existing
  `owned_ordered_run_*` Count path to `Sum` (details in the superseded
  section below). This would beat Polars *specifically* on genuinely sorted
  input by skipping the hash entirely. Fragile to input order; keep as a
  bonus fast path with the existing sample-gate + `nondecreasing` fallback,
  not the main fix. A good hot table (A) captures most of its win anyway.

The measured dead ends stand: do NOT widen `part_count`
([[project_agg_partition_count_dead_end]]) and do NOT serial-`reserve` the
partition maps.

---

## (OBSOLETE) UPDATE 1 — Stages 1 (reserve) and the 1a widening are BOTH dead ends; real Stage 1 is the ordered-run Sum path

`barrier_wait_ms` was misread. It is **the calling thread's time blocked in
`Batch::wait()`** (worker_pool.cpp `wait_for_batch`), capped at `self` — i.e.
the wall of the parallel regions as seen from the calling thread, which is
*good* (work is off the calling thread), not straggler skew. q18's aggregate is
~74% parallel already (`serial_fraction 0.25`). The real inefficiency: the
parallel regions themselves run at `pool_work 615ms / barrier_wait 131ms` ≈
**4.7× on 8 cores**, because 8 threads doing random inserts into eight
~6MB (>L2) `robin_hood` maps are **DRAM-bandwidth bound** — the ceiling
[[radix_partitioned_groupby]] already names.

- **Stage-1 reserve, measured (min-of-25, interleaved, 8c): q18 +44%, q20 +30%.**
  Moving the per-partition rehash growth onto the serial calling thread
  *serialized work that was running in parallel across the 8 partitions*. Same
  failure shape as 1a. `check_answers` stayed 22/22 — it is a pure perf loss.
  Reverted, nothing kept.

**The lever that isn't bandwidth-bound: don't build a hash table.**
TPC-H `lineitem.l_orderkey` is **globally nondecreasing** (verified: 12M rows,
3M groups, mean run 4). `sum(l_quantity) by l_orderkey` over sorted input is one
sequential pass with run detection — no hash, range-parallel with a boundary
fixup. The machinery already exists for `Count`: `owned_ordered_run_mode_`,
`owned_ordered_run_keys_/_counts_`, `owned_ordered_runs_nondecreasing_`,
`finalize_owned_ordered_runs` (the `nondecreasing` branch needs no map at all).
The gate at chunked.cpp ~7274 restricts it to `plan_[0].func == Count`.

### Real Stage 1 — extend the ordered-run path to `Sum(Double)`

1. Gate (~7274): admit `Sum` as well as `Count`. Same clustered-sample check
   (`repeats*2 >= sampled-1`).
2. Add `std::vector<double> owned_ordered_run_sums_` parallel to
   `owned_ordered_run_counts_`. The per-range run-compression pass
   (already parallel, `source_ranges = workers`, 1 barrier) also accumulates
   `l_quantity` within each run into `owned_run_sums_` alongside `owned_run_lengths_`.
3. `finalize_owned_ordered_runs`: in the `nondecreasing` branch, when the agg is
   Sum, fold `owned_ordered_run_sums_` into the slot's `double_value` +
   `mark_present()` instead of `count`. In the hash-fallback branch (input not
   globally sorted — not q18) sum into `counts`-analog.
4. Determinism: run boundaries are data-derived (equal-key runs), the boundary
   merge is associative-order-fixed (ascending row), float order identical to a
   sequential scan. Assert byte-identical vs current on q18 + `check_answers`.

Expected: replaces the 12M-row × 8-partition hash build with one bandwidth-light
sequential pass (already ~1 barrier/chunk). This is where q18's gap to Polars
streaming actually is.

Scope note: helps any sorted-key `Sum`/`Count` group-by. q13's inner
`count() by o_orderkey` is on `orders` (o_orderkey is the primary key, sorted) —
also a beneficiary. q20's PairIntKey path is unsorted (`by {l_partkey,
l_suppkey}`) — unaffected, which is fine (must-not-regress).

---

## (SUPERSEDED) Stage 1 — pre-reserve the per-partition containers (cheapest)

On the first owned chunk, after `part_count` is known, for each partition:

```
const std::size_t est = std::max(rows_offered_, rows) / part_count;  // whole-query estimate
partition.index.reserve(est);
partition.keys.reserve(est);
partition.first_rows.reserve(est);
partition.slots.reserve(est * n_aggs_);
```

Rationale: every query that reaches the owned int/pair path is high-cardinality
by construction (the gate is `n_aggs_ == 1` + `Sum(Double)`/`Count`; low-card
keys take the ordered-run or serial paths — verified: q01/q04/q17 never hit
owned; only q18, q13, q20). So `distinct ≈ rows / part_count` is a good estimate
and over-reservation is bounded and rare. Cap at `rows_per_chunk / part_count *
K` if a low-card owned query ever shows up.

Also replace the per-key `partition.slots.resize((local+1)*n_aggs_)` with
`push_back` of `n_aggs_` default slots (or reserve + resize once per rehash) so a
reserved vector never reallocates.

Expected: removes `insert_move` + `memmove` (~10% task-clock) and the barrier
skew they cause. **Cheapest experiment: reserve only, interleaved A/B on q18**
(and q13/q20 for regressions). If this recovers most of the 131ms, stop here.

Risk: low. Memory: q18 reserves 8 × 1.5M × (8B key + 8B first_row + 16B slot +
robin_hood node) ≈ 8 × ~60MB = 480MB transient — comparable to what the maps
reach anyway; measure RSS.

## Stage 2 — one barrier per chunk (if Stage 1 is not enough)

Replace phases 1–4 with a single `pool.submit(part_count)` where worker `p`
scans the **whole chunk sequentially** and skips rows not in its partition:

```
pool.submit(part_count, [&](std::size_t p) {
    auto& partition = partitions[p];
    for (std::size_t row = 0; row < rows; ++row) {
        const Key key = key_at(row);
        if ((hasher(key) & part_mask) != p) continue;
        // probe/insert partition.index, update slot   (same body as phase 4)
    }
});
```

This is the proven `ChunkedDistinctOperator` Pass-2 model (chunked.cpp ~3101 /
~3202: "one worker per partition, each scanning the whole chunk"). It removes:

- `part_of_row_.resize(rows)` (~12MB/chunk on q18), `scatter_rows_.resize(rows)`
  (~12MB), `counts`, `offsets`, `part_begin` — all gone
- the histogram barrier and the scatter barrier (→ 1 barrier/chunk, not 3)
- the serial prefix sum
- the **random** key-column gather in phase 4 → **sequential** per-worker read
- the `gids[row] = local` write — `gids` is dead in owned mode (caller returns
  `std::nullopt`; `finalize_owned` / `build_output_chunk` never read it), so
  `gids_buf_.resize(rows)` can also go for this path

New cost: each row's key is read and hashed `part_count` (=8) times instead of
~2. That is 8 × 12M sequential int64 reads + hashes over a ~24MB column
(streams from L3), ≈ low tens of ms of *parallel* work — must be measured, but
the barrier saving (131ms → target ~40ms) should dominate. For PairIntKey (q20)
`key_at` reconstructs a pair per call; 8× is still cheap.

Determinism: unchanged. Worker `p` visits rows ascending and inserts keys
first-seen → `partition.first_rows` ascending within a partition, exactly as the
scatter produced today; float accumulation order within a partition is
identical; the first-occurrence merge in `finalize_owned` is untouched. Assert
byte-identical output vs the current path on q18/q13/q20 + full `check_answers`.

`part_count` stays `== workers`; `min(workers, part_count)` submit becomes
`submit(part_count)` (equal). Skew within a partition is unchanged from today —
Stage 1's reserve still matters here.

## Stage 3 — overlap chunk decode with accumulate (follow-up, not 1b)

`pool_unqueued_ms` was ~1054ms on q18: pool threads idle between chunks while
the calling thread pulls + decodes the next from the pipelined scan. A
`PipelinedStageOperator` double-buffer already exists for this shape; check
`has_multi_unit_deferred_scan` admission for the aggregate's child here. Separate
change, separate measurement.

## Verification

- interleaved A/B (`taskset -c 0-7`, `IBEX_CORES=8`, ≥12 rounds, old binary
  built via `git checkout HEAD -- src/runtime/chunked.cpp` + full `ninja`, NOT
  `git stash`) on q18 primary; q13, q20, q10, q11, q22 for regressions
- `check_answers.py` 22/22 at SF-1, `ctest` full
- watch q20 specifically — it shares `try_owned` via the PairIntKey
  instantiation and is at −16.5% today; must not regress
- box drift this sitting is ±10–20ms cross-run on q18; only interleaved A/B is
  trustworthy ([[project_bench_interleaved_methodology]])
