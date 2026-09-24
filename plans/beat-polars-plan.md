# Beating Polars multi-core

Status: **ongoing umbrella plan. Rebaselined 2026-09-23; §1–§3 re-measured
2026-09-24 (W0 local).** It sets the target,
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
that, only the parallel fraction counts.** The first measurement past 8 cores
(§2, exploratory, dev box) does not show the gap reopening: Polars loses more
than Ibex on the E-cores and SMT siblings. But Ibex gets slower from 12 to 16
cores, and on equal physical cores the projection is still the prior until
AWS says otherwise.

## 1. Baseline (W0, measured 2026-09-24)

PDS-H **SF-8**, dev box (i7-13700 under WSL2), commit `2f57078d` (clean),
`run_bench.sh --sf 8 --cores N --polars-streaming --no-polars-in-memory
--no-duckdb`. Both engines are `taskset` to cores `0..N-1` at the same budget,
min of 5 after 1 warm-up, and Polars streaming runs in the same sitting. The
runs are `20260924T0{54822,55418,55941,60500,61019}Z_2f57078d_sf8` in
`benchmarking/tpch/results/runs/`, for N = 2, 4, 8, 12 and 16.

**The 1-core column is pooled.** Every run also times both engines at one
core (`IBEX_CORES=1`, `POLARS_MAX_THREADS=1`), so there are five 1-core
samples. The 1c row is the per-query median of the five. Ibex's 1-core total
was 16,083 / 15,725 / 15,449 / 15,539 / 15,678 ms in run order. The first run
was inflated, and the last four agree within 1.5%. Polars' was 25,229 /
25,616 / 24,751 / 24,701 / 24,695.

**12 and 16 cores are exploratory.** The box has 8 P-cores (16 threads) and 8
E-cores, and WSL2 hides which logical CPU is which. `taskset` picks a count,
not a core type, so above 8 some of the cores are SMT siblings or E-cores.
These rows show the shape of the curve, not numbers to publish. The
publishable curve is W0 item 3, on AWS.

| cores | Ibex ms | Polars ms | Ibex/Polars | geomean | Ibex scaling | Polars scaling | Ibex fraction | Polars fraction |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 15,639 | 24,857 | **0.63** | 0.72 | | | | |
| 2 | 9,293 | 13,693 | **0.68** | 0.82 | 1.68× | 1.82× | 81.2% | 89.8% |
| 4 | 6,079 | 8,314 | **0.73** | 0.87 | 2.57× | 2.99× | 81.5% | 88.7% |
| 8 | 4,863 | 5,289 | **0.92** | 1.05 | 3.22× | 4.70× | 78.8% | 90.0% |
| 12 | 4,579 | 4,945 | 0.93 | 1.05 | 3.42× | 5.03× | 77.1% | 87.4% |
| 16 | 4,700 | 4,728 | 0.99 | 1.10 | 3.33× | 5.26× | 74.6% | 86.4% |

Without q21:

| cores | Ibex ms | Polars ms | Ibex/Polars | geomean | Ibex scaling | Polars scaling | Ibex fraction | Polars fraction |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 12,451 | 18,035 | **0.69** | 0.73 | | | | |
| 2 | 8,249 | 9,993 | **0.83** | 0.86 | 1.51× | 1.80× | 67.5% | 89.2% |
| 4 | 5,373 | 6,181 | **0.87** | 0.91 | 2.32× | 2.92× | 75.8% | 87.6% |
| 8 | 4,281 | 3,932 | **1.09** | 1.09 | 2.91× | 4.59× | 75.0% | 89.4% |
| 12 | 4,010 | 3,739 | 1.07 | 1.09 | 3.11× | 4.82× | 74.0% | 86.5% |
| 16 | 4,133 | 3,642 | 1.13 | 1.14 | 3.01× | 4.95× | 71.3% | 85.1% |

Fraction is the Amdahl fraction implied by the scaling at that core count,
`(1 - 1/S) / (1 - 1/N)`. Geomean is the geometric mean of per-query ratios, and
above 1 means Polars is ahead. Quote the ex-q21 figure and the geomean next to
the total, as before (memory: `project_scaling_is_the_whole_gap`). q21 still
carries the total: 581 ms against 1,357 at 8 cores.

**What the 1-to-8 curve says:**

- **The 8-core total did not move against 2026-09-04,** but the code did get
  faster. Total 0.92, geomean 1.05 and fraction 78.8%, against 0.92, 1.06 and
  78.5% on 2026-09-04. Across those two sittings q10 (+12%), q04 (+8%) and
  q05/q07 (+6%) seemed to rise by about what q20 gained. The interleaved A/B
  (W0 item 5) does not reproduce any of those rises. At 8 cores HEAD is 3.3%
  faster by geomean, with nothing significantly slower. The flat total is
  cross-sitting drift plus the uncommitted changes in the 2026-09-04 run,
  which was marked dirty.
- **Ibex's fraction peaks at 2–4 cores and falls after that** (81 → 79%),
  while Polars holds about 89–90% from 2 to 8 cores. The August pattern, where
  the fraction climbed with cores, is gone. What remains is ordinary serial
  fraction.
- **The weakest point is 2 cores without q21**: 1.51× scaling, 67.5%. It is
  still a win (0.83), so §5's rule that 2 cores must not be a loss holds, but
  only by the 1-core lead. At 2 cores q21 is Ibex's best query (ratio 0.28)
  because Polars' q21 barely scales there.
- **The 1-core lead is 0.63 on total and 0.72 geomean.** The per-core losses
  are q06 (1.57), q15 (1.31) and q14 (1.25). q16 (1.02) and q19 (1.00) are at
  parity.

Per query, sorted by the 8-core ratio (ms, min of 5; 1c is the pooled median):

| query | ibex 1c | polars 1c | ratio 1c | ibex 8c | polars 8c | ratio 8c | ibex scal 8c | polars scal 8c | ratio 16c | ibex scal 16c | polars scal 16c |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q14 | 282 | 226 | 1.25 | 93 | 48 | **1.95** | 3.02 | 4.71 | 2.29 | 3.25 | 5.96 |
| q10 | 747 | 886 | 0.84 | 429 | 246 | **1.74** | 1.74 | 3.60 | 1.57 | 1.98 | 3.67 |
| q15 | 258 | 197 | 1.31 | 79 | 47 | **1.69** | 3.26 | 4.18 | 2.20 | 3.13 | 5.25 |
| q16 | 248 | 244 | 1.02 | 126 | 76 | **1.67** | 1.96 | 3.23 | 1.45 | 2.20 | 3.13 |
| q19 | 502 | 503 | 1.00 | 163 | 105 | **1.56** | 3.08 | 4.81 | 1.79 | 3.37 | 6.04 |
| q03 | 632 | 870 | 0.73 | 227 | 155 | **1.46** | 2.79 | 5.61 | 1.76 | 2.49 | 6.04 |
| q01 | 1,578 | 2,005 | 0.79 | 612 | 427 | **1.44** | 2.58 | 4.70 | 1.20 | 3.51 | 5.37 |
| q07 | 693 | 801 | 0.87 | 247 | 174 | **1.42** | 2.80 | 4.61 | 1.67 | 2.50 | 4.82 |
| q13 | 1,119 | 1,746 | 0.64 | 329 | 288 | 1.14 | 3.40 | 6.06 | 1.22 | 3.73 | 7.07 |
| q05 | 568 | 893 | 0.64 | 211 | 189 | 1.12 | 2.70 | 4.73 | 1.31 | 2.37 | 4.89 |
| q02 | 64 | 82 | 0.78 | 40 | 39 | 1.02 | 1.60 | 2.09 | 1.03 | 1.27 | 1.66 |
| q08 | 544 | 808 | 0.67 | 193 | 191 | 1.01 | 2.82 | 4.23 | 1.22 | 2.28 | 4.13 |
| q06 | 270 | 172 | 1.57 | 68 | 74 | 0.92 | 3.95 | 2.31 | 0.79 | 4.46 | 2.24 |
| q17 | 310 | 492 | 0.63 | 99 | 115 | 0.86 | 3.14 | 4.29 | 0.88 | 3.18 | 4.45 |
| q04 | 594 | 1,078 | 0.55 | 191 | 232 | 0.82 | 3.12 | 4.65 | 0.80 | 3.51 | 5.08 |
| q18 | 1,746 | 2,127 | 0.82 | 378 | 471 | 0.80 | 4.61 | 4.51 | 0.93 | 4.51 | 5.14 |
| q09 | 1,129 | 2,420 | 0.47 | 401 | 502 | 0.80 | 2.81 | 4.82 | 0.89 | 2.67 | 5.07 |
| q12 | 463 | 847 | 0.55 | 136 | 185 | 0.74 | 3.39 | 4.58 | 0.75 | 3.94 | 5.41 |
| q11 † | 72 | 183 | 0.39 | 38 | 53 | 0.72 | 1.89 | 3.45 | 0.66 | 1.80 | 3.01 |
| q20 | 455 | 1,106 | 0.41 | 164 | 236 | 0.70 | 2.77 | 4.69 | 0.79 | 2.63 | 5.01 |
| q22 | 178 | 350 | 0.51 | 56 | 80 | 0.69 | 3.18 | 4.34 | 0.61 | 3.68 | 4.45 |
| q21 | 3,188 | 6,822 | 0.47 | 581 | 1,357 | 0.43 | 5.48 | 5.03 | 0.52 | 5.63 | 6.28 |

† Invalid above SF-1: Ibex's q11 does not scale `FRACTION` by SF and returns
zero rows (W0 item 6).

Read it in two columns, as before. **Per-core losses** (1c ratio above 1) are
q06, q15 and q14. **Scaling losses** are q10, q16, q19, q03, q01 and q07, which
win or tie at one core and lose at eight. q10 and q16 scale less than 2× at 8
cores.

## 2. Past 8 cores (measured, exploratory)

The 2026-09-23 version of this section projected each engine's 8-core Amdahl
fit to 12 and 16 cores and concluded that the gap reopens: 1.08 on total and
1.28 without q21 at 16 cores. The measurement does not follow that projection,
for either engine:

| | Ibex projected | Ibex measured | Polars projected | Polars measured | ratio projected | ratio measured |
|---|---:|---:|---:|---:|---:|---:|
| 12c total | 4,349 | 4,579 (+5%) | 4,357 | 4,945 (+14%) | 1.00 | **0.93** |
| 16c total | 4,093 | 4,700 (+15%) | 3,891 | 4,728 (+22%) | 1.05 | **0.99** |
| 12c ex-q21 | 3,892 | 4,010 (+3%) | 3,261 | 3,739 (+15%) | 1.19 | **1.07** |
| 16c ex-q21 | 3,698 | 4,133 (+12%) | 2,925 | 3,642 (+25%) | 1.26 | **1.13** |

(Projected from each engine's 8-core fraction in this sitting: Ibex 78.8% /
75.0% ex-q21, Polars 90.0% / 89.4%.)

- **Both engines fall below Amdahl above 8 cores, and Polars falls further.**
  Polars' fraction drops from 90.0% to 86.4% and Ibex's from 78.8% to 74.6%.
  On this box, the cores added above 8 are SMT siblings and E-cores. They are
  worth less than a P-core, and the engine that parallelizes more loses more
  of its projected gain. So the gap does not reopen here as §2 feared. It
  stays about where it was at 8 cores (total 0.93–0.99, ex-q21 1.07–1.13).
  **Whether that holds on 16 physical, equal cores is exactly what this box
  cannot say**, which is why W0 item 3 (AWS, `--threads-per-core 1`) is still
  needed. There, the 2026-09-23 projection is the right prior.
- **Ibex gets slower from 12 to 16 cores (4,579 → 4,700 ms); Polars does
  not (4,945 → 4,728).** The queries that regress from 8 to 16 are the
  join-heavy ones: q08 +23%, q02 +25%, q05 +13%, q03 +12%, q07 +12%, q09 +5%
  and q20 +5%. The ones that keep improving are scan and aggregate: q01 −27%,
  q10 −12%, q12 −14%, q22 −14% and q04 −12%. Join fan-out loses time at
  high widths on heterogeneous cores, which looks like a straggler or
  oversubscription effect, while scans still gain. That is W1 input: a
  width or gate that is right at 8 is wrong at 16 for joins. It is not yet
  known whether this is E-core stragglers (a box artefact) or a real limit.
- **The fraction needed is unchanged in kind.** At 16 cores on this box Ibex
  is at parity on total and 13% behind without q21. Winning by 10% at 16
  still means lifting the fraction from about 79% to the mid-80s, turning
  roughly a quarter of the remaining serial time into parallel work.

## 3. Where the idle time is (re-measured at 16 cores, 2026-09-24)

Both tools were re-run at the widest W0 count, `taskset -c 0-15` with
`IBEX_CORES=16`, on the same build and in the same sitting as §1. Neither
script pins cores itself, so pass `taskset` explicitly. The same caveat as §2
applies: at 16 cores on this box, some of the cores are E-cores or SMT
siblings.

**The breaker map** (`benchmarking/breaker_map.py 16`, 21 of 22 queries with
reliable closure; q11 excluded): **41,090 idle core-ms**, against 13,963 at
8 cores on 2026-09-23 (18 of 22 reliable). Doubling the width roughly triples
the idle capacity.

| family | 8c (09-23) | 16c (09-24) |
|---|---:|---:|
| join | 59% | **43.6%** (17,920) |
| aggregate | 18.3% | **26.1%** (10,719) |
| scan | 18.6% | 22.3% (9,173) |
| map | | 7.0% (2,889) |

By boundary kind, 62% of the idle time is at `partial` breakers, 34% at
`pipeline` and 4% at `hard`. The largest rows:

| query | operator | idle core-ms | reading |
|---|---|---:|---|
| q21 | `join semi keys=1` | 2,810 | occupancy (memory `project_q21_is_occupancy_bound`) |
| q04 | `join semi keys=1` | 2,672 | ring wait = self; starved by its scan, as at 8c |
| q13 | `aggregate keys=1` | 2,624 | closure 172%, close to the cut-off, so treat with care |
| q13 | `scan __ibex_source_1` | 2,369 | the fused non-anchored LIKE scan |
| q21 | `Aggregate.Discovery` | 2,236 | new at the top; not in the 8c top rows |
| q19 | `join inner keys=1` | 1,892 | inner probe and output assembly |
| q01 | `update` | 1,852 | closure 166%, borderline; the 12-core point in §1 shows q01 still scaling |
| q21 | `source decode whole` | 1,702 | decode width |
| q20 | `join semi keys=2` | 1,642 | the item-7 stream, now wide enough to idle |
| q10 | `source decode whole` | 1,551 | decode width, as at 8c |
| q18 | `Aggregate.FinalOrdering` | 1,161 | W5's `FinalOrdering` fanout |

The map ranks idle capacity, not waste. Pair it with task-clock at 1 core
against N cores to find work that parallelism multiplies (memory:
`project_task_clock_finds_multiplied_work`). Three closures in the table (q01
166%, q13 172%, q22 170%) sit just inside the 175% cut-off, so their rows are
upper bounds.

**The ceiling** (`benchmarking/profile_suite.py 16`, all 22 queries closing at
99–100%): summed over the suite, serial self time is 1,729 ms, barrier wait
2,922 ms and ring wait 1,329 ms, against 5,991 ms of wall time. The pool sat
unqueued (nothing to run) for 42.2% of its capacity. Bounding wall time at
`serial + pool_work/16` = 1,729 + 3,357 = 5,086 ms puts **perfect scheduling
of the existing parallel work at 15%**. That is about the same as the 16%
measured on 2026-08-25 (SF-2, 8c). The rest still has to come from turning
serial work into parallel work. q21 alone is 532 ms of the 1,729 serial ms
(its wall is 901 ms), and the next largest are q13 (155), q10 (149), q18 (138)
and q01 (126). **Barrier wait (2,922 ms) is the largest single bucket.** That
fits §2's join regression from 8 to 16 cores: fan-outs waiting on their
slowest task, and on heterogeneous cores the slowest task is slower.

**Re-ranking W2–W5 from the 16-core view:**

- **W2 (join) stays first**, down from 59% to 44% of idle. q21, q04 and q20
  (semi) and q19, q10 and q07 (inner) lead. The 8→16 join regressions in §2 make
  the barrier and straggler side of join fan-out a new item.
- **W5 (aggregate) reopens.** Its own reopen condition was "if W0 moves
  aggregate back up the map at 16 cores", and it did: 18% → 26%. The rows are
  q21 `Aggregate.Discovery`, q13's aggregate, q18 `FinalOrdering` / `Emission`
  and q15 `Discovery`. This is at least level with W3.
- **W3 (decode width)** holds at 22% scan. q10 and q21 `decode whole`,
  q03/q05/q07 `decode selected` and q13's LIKE scan are the rows. It grows
  with cores as predicted, but not faster than aggregate.
- **W4 (small and mid queries)**: q14 and q15 get *worse* relative to Polars
  above 8 cores (2.29 and 2.20 at 16c), and they are also per-core losses.
  q16 and q10 improve relative to Polars above 8.

## 4. Workstreams

Ranked by expected effect on the fraction, highest first. W0 comes first
because it decides the ranking of the others.

### W0: Re-measure, then extend the curve past 8 cores

1. **DONE 2026-09-24: fresh local sweep at 1/2/4/8**, SF-8, both engines in
   the same sitting, `taskset`-bound (`run_bench.sh --sf 8 --cores N`). This
   is §1. The core counts ran one after another, not interleaved. Each run's
   own 1-core rows are the drift check: 4% over the sitting, all of it in the
   first run.
2. **DONE 2026-09-24: 12 and 16 cores on the dev box, labelled exploratory**
   (§2). The box has 16
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
4. **DONE 2026-09-24: breaker map and `profile_suite.py` at 16 cores** (§3).
5. **DONE 2026-09-24: interleaved A/B of the 2026-09-04 baseline against
   HEAD.** Run as `ab_queries.py` with 8 repeats, `taskset -c 0-7`, and each
   side on its own plugins (the plugin ABI changed in between). The base is
   `f06e6da3`, the rebased copy of `b492f707` with identical code.
   - **8 cores: geomean −3.3%, nothing significantly slower.** q20 −40%, q06
     −23% and q18 −9% are faster. q10 (+2.1%, p=0.95), q04, q01 and q13 are
     "unclear" and within 3%. The cross-sitting rises in §1 are dismissed.
   - **1 core: geomean −2.9%, but two real regressions.** q21 is **+6.1%**
     (1 of 8 pairs faster, p=0.039; about 170 ms on the 1-core floor) and q04
     is +3.1% (0 of 8, p=0.008). Faster: q20 −54%, q16 −7%, q13 −4%. §5 makes
     the 1-core total a hard floor, and q21 carries it, so bisect q21 at
     `IBEX_CORES=1` over `f06e6da3..HEAD`. Confirm it first with `perf stat`
     elapsed time: the decode pool still gets 2 threads at 1 core, so
     `ab_queries`' CPU-burn bias could apply.
   - **Output: only q11 differs,** and legitimately. Both sides return zero
     rows. The base printed a column-less `<empty>` table, and HEAD keeps the
     schema.
6. **New: q11 is not the same query as Polars' above SF-1.**
   `queries/q11.ibex` hardcodes `FRACTION = 0.0001`. TPC-H and the Polars
   reference (`0.0001 / settings.scale_factor`) scale it by SF. At SF-8, Ibex
   filters with a threshold 8× too high and returns zero rows, while Polars
   returns the real answer. q11's ratio in §1 (0.72 at 8c) is invalid until
   the query takes the scale factor. It is the only SF-dependent parameter
   in PDS-H.

Exit: §1 and §2 rewritten from measurement, with the implied fraction at
1/2/4/8/12/16 for both engines (met locally 2026-09-24). Items 3 (AWS) and 6
(q11) are still open, as is the q21 1-core bisect from item 5.

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
- geomean at or below parity at 8 cores (1.05 on 2026-09-24);
- the 1-core total no worse than today's baseline.

Re-check at SF-2 before publishing, since thresholds were calibrated there and
a ranking does not always transfer.
