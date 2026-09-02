---
name: owned_agg_per_chunk_barrier
description: "High-cardinality partition-owned aggregation vs Polars streaming. q18 (single-Int64 Sum) largely closed by the async hot/cold rewrite; q20 (PairIntKey) and q21 (ordered-run Count) are the live fronts. Do NOT touch part_count."
metadata:
  type: project
---

# Owned aggregation: the per-chunk barrier pipeline

**Compacted 2026-08-27** — the UPDATE 1–6 chain and the superseded Stage 1/2/3
sketches are in git history at the pre-compaction commit's parent. `part_count`
stays `== workers` — over-partitioning is a measured dead end
([[project_agg_partition_count_dead_end]]).

## Current state

| path | shape | status |
|---|---|---|
| single-Int64 `Sum(Double)` | q18 | **async hot/cold rewrite LANDED, q18 −33%** (kill switch `IBEX_DISABLE_ASYNC_HOT_AGG=1`) |
| all owned paths' finalize | q18 + q20 | **parallel merge LANDED, −7% each** |
| `PairIntKey` (`{partkey,suppkey}`) | q20 | **async partition collector LANDED, q20 −51.7%** — scattered pairs bypass the hot table, stream compact records to final owners, then build each pre-sized map once. |
| ordered-run `Count` (sorted key) | q21 | **parallel first-occurrence merge + emit fusion LANDED** (fd9fecb1, 103f3904, this session) — q21 −11.2% SF-4. See [[project_q21_is_occupancy_bound]] |

### q18 async hot/cold rewrite (LANDED, UPDATE 5)

Both remaining Polars mechanisms, for the single-Int64 `Sum(Double)` owned path
only (not widened to PairIntKey):

1. **4096-slot two-choice second-chance hot table per streamed chunk** — Polars'
   exact `DEFAULT_HOT_TABLE_SIZE` policy: two hash-derived candidate slots,
   tag-first hit checks, random candidate selection, second-chance
   admission/eviction, a one-record recent cache for run clusters. Hot and cold
   records created at their first source row, updated in place, routed stably to
   cold partitions — first-occurrence order preserved without a sort. The hot
   table does run-length compression *for free from locality* (q18's mean-run-4
   `l_orderkey` → ~12M rows collapse to ~3M cold inserts).
2. **Async no-per-chunk-barrier sink** — `WorkerPool::TaskGroup` takes one chunk
   task at a time while the pull loop immediately requests the next; one
   end-of-stream join, then one task per cold partition builds its persistent
   map in parallel, then the parallel finalize merge emits first-seen order.
   Removes `part_of_row_`, histogram/prefix/scatter, the random key/value
   gather, and the per-chunk barrier. Decoded columns released per task as
   pre-aggregates finish — async does not retain all chunks.

Measured SF-2/8c, 20 paired rounds vs the kill-switched base: **q18 206.1 →
132.2 ms min, −33%, 20/20, p<0.001**; aggregate profile **38 → 4 barriers**.
q11/q13/q20 neutral within 0.3%. 22/22 answers, dedicated 131,074-row two-chunk
test (cross-chunk run, fixed-table churn, first-seen order, nullable sum).

### Parallel finalize merge (LANDED, UPDATE 4)

`finalize_owned` was a fully serial K-way merge of the `part_count` partition
group-lists by `first_rows` (~24M comparisons + 3M slot copies for q18's 3M
groups). **Fix:** merge-path co-ranking — `first_rows` values are globally
unique row indices, so `sum_p lower_bound(first_rows_p, v)` steps by one at each
value; binary-search `W-1` frontiers (~30k ops), then `pool.submit(W)` merges
disjoint input slices into disjoint output slices. Byte-identical (stable merge
over a total order); serial fallback below 128k groups / on a pool thread /
`part_count < 2`. **q18 −9 to −11% min, q20 −4 to −9% min** — helps the unsorted
q20 as much as q18, no regression surface. (The ordered-run `Count` path had its
own separate serial finalize — parallelized this session, fd9fecb1.)

## What Polars streaming actually does (UPDATE 2 — reference)

Verified against `~/polars` v1.42 (`polars-stream/src/nodes/group_by.rs`,
`polars-expr/src/hot_groups/fixed_index_table.rs`, `optimizer/sortedness.rs`) +
`POLARS_VERBOSE=1`.

- **Polars does NOT exploit sortedness on q18** — `IR::Scan => None` in
  sortedness.rs, `are_keys_sorted` is false for `scan_parquet(...).group_by(...)`,
  and the SF-2 files carry no `sorting_columns`. "We know `l_orderkey` is
  sorted" is a dataset-incidental numpy-diff observation, not something either
  engine relies on.
- **Two-tier hot/cold partitioned hash aggregation:** (1) a 4096-entry recency
  grouper per pipeline — clustered keys hit it and aggregate in-cache, evicting
  least-recent when full; (2) evicted keys hash-partitioned into `num_pipelines`
  buckets (**same partition count as Ibex** under `taskset` — Polars is not
  winning on partition count), each with a `CardinalitySketch` sizing its final
  grouper (no rehash cascade). (3) Partitioning is **incremental during
  eviction**, not a per-chunk barriered histogram→scatter. (4) The group-by is a
  **push Sink** fed by the scan's pipelines — aggregation overlaps decode fully.
  (5) Finalize is parallel via rayon.

## Live fronts

- **q20 / PairIntKey — LANDED.** The hot table does not help scattered composite
  keys. Instead, `OwnedPairChunk` streams compact `(key, value, first-row)`
  records to a `TaskGroup`; one final parallel owner per partition reserves its
  map from the exact record count and builds it once. SF-8, 8 cores, interleaved
  16-pair A/B: **672.7 → 301.4 ms minimum, −51.7%, 16/16, p<0.001**,
  byte-identical. This is stronger than a cardinality sketch for the current
  in-memory collector: the exact record count is already available at final
  build, without an additional sketch pass. q10/q13 were neutral and q18 was
  −5.7% in the focused regression A/B; 22/22 answers and 1830 fast tests pass.
- **q21 / ordered-run Count** — the parallel finalize + emit fusion landed this
  session (−11.2% SF-4). Remaining q21 wall is the per-chunk accumulate
  orchestration and a duplicate lineitem decode
  ([[project_scan_instance_split_no_cost_gate]]), plus the serial hash-join
  build ([[kernel-pipeline-execution-plan]] "where join time goes" — q21 hashes
  1.28M rows in 40ms serial).
- **Decode overlap (Stage 3)** — MEASURED NON-EXISTENT as a lever for the async
  path: `child_->next()` returns in 0.0ms from the aggregate's view, the
  pipelined scan already overlaps decode fully.

## Measured dead ends (do not re-attempt — see [[project_reverted_perf_dead_ends]])

- **Over-partitioning `part_count`** — q18 +13…+48%.
- **Serial `reserve` of the partition maps** on the calling thread — q18 +44%,
  q20 +30% (serializes work that was parallel across 8 partitions).
- **Small tweaks to the radix owned accumulate** (1-entry key/gid cache;
  run-collapsing accumulator; single-fused-scan Design B with 8× redundant hash;
  clustering-gated variants) — all net negative or neutral. The radix owned
  accumulate is balanced, correctly parallel, latency-bound at ~30ns/row
  (gather key → hash → probe → gather slot → FP-add), ~4.6×/8 effective. It "is
  not improvable by small tweaks" — q18's gap needed the full hot/cold
  architecture (now landed).
- **Ordered-run `Sum(Double)` extension** — designed in the old UPDATE 1, NOT
  built; the async hot table (UPDATE 5) handles q18's single-Int64 Sum instead.
  The compress-runs sample gate (`chunked.cpp:~7669`) remains `Count`-only.

## Verification protocol

Interleaved A/B (`taskset -c 0-7`, `IBEX_CORES=8`, ≥12 rounds, base built via
`git checkout <ref> -- src/runtime/chunked.cpp` + full build — NOT `git stash`);
q18 primary, q13/q20/q10/q11/q22 for regressions; `check_answers.py` 22/22 + full
`ctest`. **Watch q20** — it shares `try_owned` via the PairIntKey instantiation
and is at −16.5%; must not regress. Box drift ±10–20ms cross-run on q18 — only
interleaved A/B is trustworthy ([[project_bench_interleaved_methodology]]).
