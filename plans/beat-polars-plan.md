# Beating Polars multi-core

Status: **ongoing umbrella plan. Rebaselined 2026-09-23.** It sets the target,
says where the gap is, and ranks the workstreams. Mechanism lives in the plans
it points to: `kernel-pipeline-execution-plan.md`,
`runtime-multithreading-plan.md`, `owned-agg-per-chunk-barrier-plan.md`,
`join-perf-plan.md`, `parallelism-overview.md` and `src/runtime/PARALLELISM.md`.

The August version (SF-2, target "raise the implied parallel fraction from 44%
to 60–65%") has been met and is superseded. Its full text, including the W1
join / W1b / W3.1 decode-budget / q22 / q10 write-ups and the 2026-08-11
per-query standings, is at `git show e82f679d:plans/beat-polars-plan.md`.

The rebaseline changes the question. At 8 cores Ibex is now level with or
ahead of Polars on suite total, and it gets there on a single-core lead, not
on scaling. **The single-core lead covers the gap up to about 8 cores. Beyond
that, only the parallel fraction counts,** and the next round of measurement
goes past 8 cores.

## 1. Baseline

PDS-H **SF-8**, dev box (i7-13700 under WSL2), `taskset` to 8 cores, both
engines at the same budget, min of 5 after 1 warm-up. Run
`20260904T080101Z_b492f707-dirty_sf8` in `benchmarking/tpch/results/runs/`,
Polars streaming measured in the same sitting.

| | Ibex | Polars | Ibex/Polars |
|---|---:|---:|---:|
| 1 core, total | 15,459 ms | 26,024 ms | **0.59** |
| 8 cores, total | 4,846 ms | 5,259 ms | **0.92** |
| 8 cores, geomean | | | **1.06** (Polars ahead) |
| scaling 1→8 | 3.19× | 4.95× | |
| implied parallel fraction (Amdahl, 8c) | 78.5% | 91.2% | |
| queries won | 19/22 at 1c | 10/22 at 8c | |

**q21 carries the total.** Ibex runs it in 571 ms against Polars' 1,305 at
8 cores (and 0.44× at 1 core too). Without q21, the 8-core total is **1.08×
(Polars ahead)**, scaling is 2.89× against 4.77×, and the implied fraction is
74.7% against 90.3%. Quote the ex-q21 figure next to the total, and quote the
geomean next to both. The geomean weights the small queries Ibex loses as
heavily as the large ones it wins
(memory: `project_scaling_is_the_whole_gap`).

Per query, sorted by the 8-core ratio (ms, min of 5):

| query | ibex 8c | polars 8c | ratio 8c | ratio 1c | ibex scaling | polars scaling |
|---|---:|---:|---:|---:|---:|---:|
| q14 | 96 | 44 | **2.19** | 1.13 | 2.66 | 5.15 |
| q15 | 84 | 45 | **1.88** | 1.25 | 2.89 | 4.34 |
| q16 | 136 | 78 | **1.73** | 0.99 | 1.87 | 3.27 |
| q10 | 383 | 244 | **1.57** | 0.77 | 1.84 | 3.77 |
| q19 | 159 | 105 | **1.51** | 0.94 | 2.95 | 4.77 |
| q03 | 220 | 158 | **1.39** | 0.67 | 2.70 | 5.66 |
| q01 | 587 | 432 | **1.36** | 0.67 | 2.45 | 5.00 |
| q07 | 232 | 177 | **1.31** | 0.78 | 2.83 | 4.74 |
| q06 | 81 | 69 | 1.17 | 1.56 | 3.20 | 2.40 |
| q13 | 335 | 297 | 1.13 | 0.60 | 3.35 | 6.32 |
| q20 | 259 | 232 | 1.12 | 0.79 | 3.66 | 5.16 |
| q05 | 199 | 193 | 1.03 | 0.54 | 2.64 | 5.03 |
| q08 | 185 | 186 | 0.99 | 0.58 | 2.56 | 4.40 |
| q02 | 35 | 36 | 0.96 | 0.70 | 1.71 | 2.33 |
| q17 | 107 | 119 | 0.90 | 0.58 | 2.68 | 4.15 |
| q18 | 385 | 454 | 0.85 | 0.74 | 4.41 | 5.02 |
| q04 | 176 | 231 | 0.76 | 0.52 | 3.15 | 4.57 |
| q09 | 391 | 530 | 0.74 | 0.45 | 2.82 | 4.63 |
| q22 | 56 | 79 | 0.72 | 0.47 | 3.15 | 4.78 |
| q12 | 133 | 191 | 0.70 | 0.50 | 3.45 | 4.81 |
| q11 | 37 | 54 | 0.69 | 0.36 | 1.85 | 3.54 |
| q21 | 571 | 1305 | 0.44 | 0.44 | 5.45 | 5.48 |

Read it in two columns. **Per-core losses** (ratio at 1c above 1) are only q06,
q15 and q14. **Scaling losses** are everything else in the top half: q10, q16,
q03, q01 and q07 win or tie at one core and lose at eight.

**This baseline is stale on purpose.** It predates the breaker-map arc
(q20 −21% from two-key semi/anti streaming, q18 −7% from the `FinalOrdering`
alloc fix, the semi/anti right side no longer materialized), the reader's
literal-range scan filter and the costed probed-key restriction. It is kept
because it is the last run with Polars measured fresh in the same sitting.
W0 replaces it.

## 2. Why more cores changes the target

Extrapolating each engine's 8-core Amdahl fit (SF-8, from §1):

| cores | Ibex (projected) | Polars (projected) | ratio | Ibex fraction needed for parity | … to win by 10% |
|---:|---:|---:|---:|---:|---:|
| 8 | 4,846 | 5,259 | 0.92 | 75.4% | 79.3% |
| 12 | 4,341 | 4,270 | 1.02 | 79.0% | 82.0% |
| 16 | 4,088 | 3,775 | **1.08** | 80.6% | 83.2% |
| 16, ex-q21 | 3,698 | 2,888 | **1.28** | 81.7% | 84.2% |

Two conclusions, both with the caveat below.

- **The gap reopens as cores are added.** Ibex's 1-core lead is a fixed
  asset. Polars' better scaling compounds with every core. At 16 cores the
  current fractions put Ibex about 8% behind on total and 28% behind without
  q21.
- **The fraction needed is not far away.** To stay at parity at 16 cores
  Ibex's implied fraction must go from 78.5% to about 81%, and to 84% to win
  by 10%. That means turning roughly one more tenth of the remaining serial
  time into parallel work.

**Caveat: these are extrapolations, and Ibex has broken the Amdahl premise
before.** In August its implied fraction *climbed* with core count
(39.6% → 62.7% from 2 to 8 cores), because parallel work was being declined at
low budgets instead of executed slowly (the W3.1 story in the August version).
The same can happen the other way above 8: a threshold tuned at 8 may leave
width unused, or oversubscribe. The 12- and 16-core points must be measured,
not projected. That is W0.

## 3. Where the idle time is

Two measurements say where the remaining serial and idle time lives. Both need
re-running at the new core counts.

**The breaker map** (re-measured 2026-09-23, SF-8/8c, 18 of 22 queries with
reliable closure; `benchmarking/breaker_map.py 8`): 13,963 idle core-ms.
**Join 59%**, scan 18.6%, aggregate 18.3% (down from 33.7% before the
breaker-map arc). The top rows:

- q04 `join semi`: all self time is ring wait. It is starved by its scan
  (decode width), not slow.
- q21 `join semi`: q21's occupancy problem (memory:
  `project_q21_is_occupancy_bound`).
- q19 `join inner`: inner-join probe and output assembly, spread thinly across
  queries.
- q10 `source decode whole`: decode width.

The map ranks idle capacity, not waste. Pair it with task-clock at 1 core
against N cores to find work that parallelism multiplies (memory:
`project_task_clock_finds_multiplied_work`).

**The ceiling** (2026-08-25, SF-2/8c, `benchmarking/profile_suite.py 8`,
memory: `project_serial_fraction_is_the_ceiling`): perfectly scheduling all
existing parallel work would buy at most 16%. The rest has to come from
turning serial work into parallel work. It predates most of the breaker-map
work and was measured at SF-2, so re-run it with W0.

## 4. Workstreams

Ranked by expected effect on the fraction, highest first. W0 comes first
because it decides the ranking of the others.

### W0: Re-measure, then extend the curve past 8 cores

1. **Fresh local sweep at 1/2/4/8**, SF-8, both engines in the same sitting,
   interleaved and `taskset`-bound (`run_bench.sh --sf 8 --cores N`). This
   replaces §1.
2. **12 and 16 cores on the dev box, labelled exploratory.** The box has 16
   physical cores (8 P + 8 E) and 24 logical, but WSL2 hides the hybrid
   topology, so `taskset` picks a count, never a core type (memory:
   `project_bench_core_count_cap`). Above 8, some cores will be E-cores or SMT
   siblings. These points show the shape of the curve, not a number to
   publish.
3. **The publishable curve on AWS**, physical cores only
   (`--threads-per-core 1`). `r7i.4xlarge` gives 8 physical cores and
   `r7i.8xlarge` gives 16. The harness exists
   (`benchmarking/aws/run-thread-scaling.sh`; memory:
   `project_bench_two_tier_framework`). Refresh the AMI first (memory:
   `project_aws_baked_ami_goes_stale`).
4. **Breaker map and `profile_suite.py` at the widest local core count**,
   so the idle ranking is taken where the fight is.

Exit: §1 and §2 rewritten from measurement, with the implied fraction at
1/2/4/8/12/16 for both engines.

### W1: Constants and gates tuned at 8 cores (new; do before trusting W0's 16-core point)

Every scale-dependent threshold in the engine was calibrated on the 8-core dev
box. Audit each of these against the W0 sweep:

- `IBEX_DECODE_SATURATION` (default 8), which sizes the decode pool at
  `min(2·cores, max(cores, saturation))`. At 2–4 cores decode gets extra
  threads to fill memory stalls. From 8 cores up the pool equals the core
  count, because at 8 this box's memory system was saturated and extra threads
  cost 9%. Whether 12 or 16 cores stay saturated, or have stalls worth filling
  again, is unmeasured. Sweep it at 12 and 16, and on AWS. It describes one
  machine's memory system, not the code.
- `kNestedDecodeFanout = 4` and the `num_row_groups() < pool.size()` gate in
  `parallel_readers`. SF-8 `lineitem` has 46 row groups, so at 16 cores the
  row-group axis still fills the pool, but the dimension tables (1–2 groups)
  have more idle cores to recover.
- Row-group count as the width cap for scans (memory:
  `project_row_group_caps_parallel_width`). The lever is row-group size at
  write time, and it bites harder as cores rise.
- `parallel_min_rows`, `kParallelDecodeMinRows` and the per-operator
  `workers < 2` / partial-state budgets (`per_morsel_bytes`). More workers
  means more replicated partial state per aggregate.

Rule (memory: `feedback_no_premature_constant_tuning`): measure first, change
one constant per A/B, and only after W0 has the curve.

### W2: Join, 59% of idle at 8 cores

- **Inner-join probe and output assembly** (the breaker map's §4.4 slices,
  about 3,100 core-ms at the last full ranking). q19, q10, q03, q05 and q07
  sit here, all scaling losers. See `join-perf-plan.md` and memory
  `project_join_parallelism`.
- **q21's semi-join occupancy.** It is ranked by ring wait and occupancy, not
  pool work (memory: `project_q21_is_occupancy_bound`). q21 carries the
  suite total, so a regression here hides everywhere else.
- **Serial build pieces** that remain: the semi/anti `seen` map and the
  `right_i64_` rebuild (12 + 23 ms on q04). The serial hash build on q10
  (about 58 ms at SF-2) predates the join build/probe split. Re-time it
  before treating it as a target.

### W3: Decode width and scan starvation

q04, q10, q12, q06 and q20 show the `occ ≈ 0.5, ring_wait ≈ self` signature:
the consumer waits on a scan that cannot go wider. The levers are row-group
granularity. Split a single-column decode's row-group task when there are
fewer tasks than workers (`query-shape-conformance-plan.md`, item 2), and look
at row-group size at write time. This is the workstream most likely to grow
with core count.

### W4: The small and mid queries that decide the geomean

q14, q15, q16, q10 and q19 lose 1.5–2.2× at 8 cores.

- **q14** is at parity per core and loses on contention. It is not bandwidth
  bound: Polars runs at half the achievable bandwidth and Ibex at a quarter
  (§7). The lever is the access pattern over the ~600k surviving rows (memory:
  `project_q14_bandwidth_and_selected_gather`, which records two dead ends).
- **q15 and q06** are the only other per-core losses. Start there, before
  threading.
- **q16** scales 1.87× against 3.27×. Its composite-categorical `distinct` is
  the I3 gap in `parallelism-overview.md`.
- **q13's fused non-anchored LIKE scan** does 66% more CPU than dense decode
  plus a filter (`query-shape-conformance-plan.md`, item 1).
- **Small-query tax guard** (the old W5, standing): every workstream's A/B
  includes q02/q13/q16 at 1 and 8 cores, and any new per-query setup cost
  (pools, rings, stages) is built lazily or amortized across the process
  (memory: `project_decode_threading_small_query_tax`).

### W5: Aggregate residue, mostly done

The breaker-map arc took aggregate from 33.7% to 18.3% of idle. What is left:

- `FinalOrdering`'s fanout on q18 (536 core-ms, occupancy 0.63–0.70).
- q01's aggregate queues behind its own scan. Sized and judged a no-go for now:
  removing the overlap costs q01 44% (memory:
  `project_q01_scan_aggregate_contention`).
- High-cardinality group-by, 8× slower at 5M groups (memory:
  `project_high_cardinality_groupby_gap`; `radix-partitioned-groupby.md`).
  q20's `PairIntKey` is the PDS-H instance
  (`owned-agg-per-chunk-barrier-plan.md`).

Reopen this only if W0 moves aggregate back up the map at 16 cores.

### W6: Chunked `let` bindings (structural, unbuilt)

A `let` binding is one contiguous `Table`, so streaming a large intermediate
into it pays a serial concat in `MaterializeOperator`. The semi/anti right side
and the inner-join left side were both fixed locally by not asking for a
`Table` (memories: `project_semi_anti_right_not_materialized`,
`project_join_left_drain_not_copy`). The general fix lets a binding hold a
chunk list and makes it contiguous only when a consumer needs that. It touches
the binding/registry contract and needs its own design note, and it overlaps
the GC work in memory `project_runtime_binding_lifetime`. Size it from W0's
profile before starting.

## 5. Validation gates

- **Answers:** all 22 PDS-H answers byte-identical at 1 and 8 cores (and at
  the widest W0 count) for every change. The serial-vs-parallel diff has
  exactly three legitimate exceptions, q01, q09 and q15, which differ in the
  last ulp from the parallel float reduction. A fourth file in that diff is a
  bug, not drift.
- **The leading metric is the implied parallel fraction** from an interleaved
  sweep at 1/2/4/8 (plus 12/16 once W0 lands). A wall-time win that does not
  move the fraction must say so.
- **The 1-core total is a hard floor.** The single-core lead is the strategy
  up to 8 cores, and §2 shows it still matters at 16.
- **The 2-core point must not be a loss.** It was the weakest point on the
  curve until decode got its own thread budget (`IBEX_CORES` versus the
  decode pool; see `decode_thread_count`).
- **Per-query gates:** q02/q13/q16 (small-query tax), q06 (scan canary),
  q21 (carries the total), q14/q15/q19 (geomean losers), q20 (aggregate
  gates).
- **Method** (MEASURING.md; memories `project_bench_interleaved_methodology`,
  `project_ab_queries_cpu_burn_bias`, `feedback_rerun_reference_engine`):
  interleave A/B, never compare absolute totals across sittings, and re-run
  Polars in the same sitting. For changes that add threads or spinning, use
  `perf stat` elapsed time plus standalone min-wall, not `ab_queries`. Measure
  allocation-shaped changes cold as well as warm.
- **Pin explicit `parquet_sf<N>` paths.** The `benchmarking/data/tpch/parquet`
  symlink flips per scale factor (memory: `project_tpch_sf_symlink`).
- **`self_ms` is not serial time.** Confirm any target sized from the operator
  table with phase timers before building. Both breaker-map items diagnosed
  from reading code alone were wrong.

## 6. Measured dead ends: do not revisit without new evidence

From the August plan: probe-operator Bloom; column-axis join gather;
gid-sharded aggregate accumulate over the discovery scatter (8× read
amplification); the two-phase join `left_copy` branch; a lookahead decode
window (+52% RSS, no wall change); a static one-producer admission gate;
lowering the int-partition row gate; a footer-bytes small-query gate; naive
morsel islands around 1:1 operators; AVX2 filter left-pack; more island
coverage; one-chunk lookahead for first-chunk discovery (−1.0%, below the
floor); a work-stealing probe cursor at 2 cores (a wash).

- **Categorical code hashing in the generic group-by.** It gave wrong answers
  on q16, because chunks after a join carry different dictionaries.
- **Selected-row gather straight from Parquet pages** (q14): about 20% slower,
  because the dense path's 512 KB scratch batch is L2-resident. Do not size a
  decode change off `__memmove` percentages without asking whether the buffer
  fits in cache.
- **Moving a computed column across a join without a cost model:** −9 to −15%
  where it fits, q12 +40% where it does not. It needs a join-output
  cardinality estimate.
- **Row-encoded group-by:** q10 +26% from contention with the concurrent
  decode for memory bandwidth. The third q10 group-by dead end.
- **Worker-local low-cardinality aggregate** (breaker map item 3): built, −2%.
  q01 is limited by contention with its own scan.
- **Aggregate inside the scan pipeline** (breaker map item 5): sized and a
  no-go. Removing the overlap costs q01 44% and q15 61%.

The full list with mechanisms is in memory `project_reverted_perf_dead_ends`.

## 7. q14 and the ceiling that wasn't (measured 2026-09-02, SF-8, 8 cores)

Kept because two plausible framings of q14 were both wrong, in opposite
directions, and the corrections apply elsewhere.

q14 is at parity single-threaded (234 ms against Polars streaming at 222 ms)
and loses 2.3× multi-threaded. Its CPU time grows with cores: 279 ms at 1c,
425 at 4c, 537 at 8c. Eight independent single-threaded copies each take
632 ms instead of 285. So it contends for a shared resource.

**That resource is not streaming memory bandwidth.** Measured ceiling on the
dev box:

```
                 1 thread    4 threads    8 threads
DRAM read         15.8 GB/s   34.4 GB/s   38.6 GB/s
pread page cache  14.3 GB/s   42.5 GB/s   44.5 GB/s
```

Eight copies move 8 × 903 MB in 632 ms, which is 11.4 GB/s, a quarter of
peak. The contention is L3 capacity plus a non-streaming access pattern (the
1.6M-entry join hash probe and the 600k-row gathers).

| | wall | effective | vs floor |
|---|---|---|---|
| floor (read the 903 MB) | 20.5 ms | 44 GB/s | 1.0× |
| Polars streaming | 41.3 ms | 21.9 GB/s | 2.0× |
| Ibex | ~104 ms | 8.7 GB/s | 4.5× |

Two corrections came out of the same session:

- **A join's build side is not inherently serial.** The scan under it gets
  pool tasks. What ran serially in the q14 experiment was the `Update` above
  it, because one field shape fell off `plan_direct_field`, and that route is
  all-or-nothing per node (`4ac93b33`). Check which node is serial before
  blaming the build.
- **SF-8 `lineitem` layout:** 46 row groups of 1Mi rows, uncompressed.
  `l_partkey` and `l_extendedprice` are PLAIN (their dictionaries outgrew the
  page limit); `l_discount` and `l_shipdate` are dictionary-encoded. Chunks
  mix encodings (`l_partkey` has 1 dictionary page, 7 PLAIN data pages and 1
  RLE_DICTIONARY data page), so "is this column dictionary-encoded" is a
  per-page question.

## 8. What winning looks like

At SF-8, measured on physical cores (AWS, `--threads-per-core 1`), in the same
sitting as a fresh Polars run:

- faster than Polars on suite total **at every core count from 1 to 16**, and
  also without q21;
- geomean at or below parity at 8 cores (it is 1.06 today);
- the 1-core total no worse than today's baseline.

Re-check at SF-2 before publishing, since thresholds were calibrated there and
a ranking does not always transfer.
