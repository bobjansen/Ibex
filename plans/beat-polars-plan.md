# Beating Polars multi-core

Status: **ongoing umbrella plan. Rebaselined 2026-09-23; re-measured
2026-09-24 on the dev box and on 16 physical cores on AWS (W0).** It sets the
target,
says where the gap is, and ranks the workstreams. Mechanism lives in the plans
it points to: `kernel-pipeline-execution-plan.md`,
`runtime-multithreading-plan.md`, `owned-agg-per-chunk-barrier-plan.md`,
`join-perf-plan.md`, `parallelism-overview.md` and `src/runtime/PARALLELISM.md`.

The August version (SF-2, target "raise the implied parallel fraction from 44%
to 60–65%") has been met and is superseded. Its full text, including the W1
join / W1b / W3.1 decode-budget / q22 / q10 write-ups and the 2026-08-11
per-query standings, is at `git show e82f679d:plans/beat-polars-plan.md`.

**On physical cores, Ibex loses from about 6 cores upward, and the gap grows
with every core** (§1a): 1.08 at 8, 1.26 at 12, 1.36 at 16 (geomean 1.45; 1.51
without q21). The single-core lead is real (0.61) but fixed. Polars' implied
parallel fraction is about 98%, Ibex's about 87–88%, and both follow Amdahl
closely from 8 to 16 cores. **The whole problem is Ibex's serial time:** about
2.8 s of its 21.5 s single-core suite does not parallelize, against 0.4 s of
Polars' 35 s. Parity at 16 cores needs that halved.

The dev box (§1b) said otherwise: 0.92 at 8 cores, and the gap not reopening
past 8. Both conclusions came from the box. Its hybrid P/E-core topology under
WSL2 held Polars to 4.7× at 8 cores, where physical cores give it 7.4×. Do
not use the dev box for cross-engine claims at any core count above about 4.
It stays useful for A/B work on Ibex alone.

## 1. Baseline (W0, measured 2026-09-24)

### 1a. Physical cores, AWS (the baseline)

PDS-H **SF-8** on one `r7i.8xlarge` with SMT disabled (`--threads-per-core 1`):
16 physical Sapphire Rapids cores (Xeon Platinum 8488C), 256 GiB, commit
`461c0963` (clean). One box ran every core count in turn
(`run-tpch.sh --cores 2,4,8,12,16 --no-polars-in-memory`), each with its own
1-core rows, all engines `taskset` to cores `0..N-1`, min of 5 after 1
warm-up. Before timing, every core count passed the answer check against
Polars (22 of 22). Artifact:
`benchmarking/results/tpch_aws_20260924T102939.tar.gz`
(`s3://…/benchmarks/20260924T102939_461c0963/`), runs
`20260924T1{05232,10459,11554,12622,13645}Z_461c0963_sf8`. DuckDB ran too and
is in the TSVs, but is not analyzed here.

The five 1-core samples agree within 1.0% (Ibex 21,369–21,594 ms; Polars
34,350–35,901). A clean box does not drift the way WSL2 does.

| cores | Ibex ms | Polars ms | Ibex/Polars | geomean | Ibex scaling | Polars scaling | Ibex fraction | Polars fraction |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 21,487 | 35,108 | **0.61** | 0.70 | | | | |
| 2 | 11,626 | 17,432 | **0.67** | 0.78 | 1.85× | 2.01× | 91.8% | 100.7% |
| 4 | 7,137 | 9,107 | **0.78** | 0.93 | 3.01× | 3.86× | 89.0% | 98.7% |
| 8 | 5,134 | 4,767 | **1.08** | 1.20 | 4.19× | 7.36× | 87.0% | 98.8% |
| 12 | 4,323 | 3,429 | **1.26** | 1.35 | 4.97× | 10.24× | 87.1% | 98.4% |
| 16 | 3,768 | 2,780 | **1.36** | 1.45 | 5.70× | 12.63× | 88.0% | 98.2% |

Without q21:

| cores | Ibex ms | Polars ms | Ibex/Polars | geomean | Ibex scaling | Polars scaling | Ibex fraction | Polars fraction |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16,974 | 25,371 | **0.67** | 0.72 | | | | |
| 2 | 10,297 | 12,907 | **0.80** | 0.82 | 1.65× | 1.97× | 78.7% | 98.3% |
| 4 | 6,221 | 6,677 | **0.93** | 0.97 | 2.73× | 3.80× | 84.5% | 98.2% |
| 8 | 4,468 | 3,575 | **1.25** | 1.24 | 3.80× | 7.10× | 84.2% | 98.2% |
| 12 | 3,719 | 2,583 | **1.44** | 1.39 | 4.56× | 9.82× | 85.2% | 98.0% |
| 16 | 3,213 | 2,121 | **1.51** | 1.49 | 5.28× | 11.96× | 86.5% | 97.7% |

**What the physical-core curve says:**

- **Parity is at about 6 cores, and the gap widens from there.** The 1-core
  lead (0.61, 0.70 geomean) is intact. Polars' near-linear scaling erases it
  between 4 and 8 cores.
- **Both engines follow Amdahl.** Fitted at 8 cores and projected, Ibex comes in
  5% *better* than projection at 16 and Polars 7% worse. So the ratio at 16 is
  1.36, not the projected 1.53. The dev box's 12→16 Ibex regression (§2) does
  not happen here: 4,323 → 3,768 ms.
- **What parity takes.** At 16 cores Ibex needs an implied fraction of about
  **92.9%** for parity on total (93.3% without q21) and **94.2%** to win by
  10%. At 8 cores parity needs 88.9%, against 87.0% today. In time terms, Ibex's
  non-parallel share is about 2.8 s of 21.5 s and must fall to about 1.5 s.
  Polars' is about 0.4 s of 35 s.
- **The 2-core point is Ibex's worst without q21** (78.7%), as on the dev box.
  q21 is Ibex's best query at 2 cores (0.29), because Polars' q21 barely scales
  there.

Per query at 16 cores, sorted by the 16-core ratio (ms, min of 5; 1c is the
median of the five 1-core samples):

| query | ibex 1c | polars 1c | ratio 1c | ratio 8c | ibex 16c | polars 16c | ratio 16c | ibex scal 16c | polars scal 16c |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q16 | 374 | 365 | 1.02 | 2.35 | 131 | 45 | **2.89** | 2.85 | 8.04 |
| q10 | 1,161 | 1,298 | 0.89 | 2.05 | 433 | 170 | **2.55** | 2.68 | 7.65 |
| q14 | 380 | 329 | 1.16 | 1.84 | 61 | 25 | **2.40** | 6.25 | 12.96 |
| q15 | 372 | 268 | 1.39 | 1.86 | 56 | 24 | **2.27** | 6.70 | 10.99 |
| q19 | 788 | 705 | 1.12 | 1.76 | 122 | 55 | **2.23** | 6.45 | 12.88 |
| q07 | 1,024 | 1,219 | 0.84 | 1.60 | 216 | 98 | **2.19** | 4.75 | 12.39 |
| q03 | 924 | 1,234 | 0.75 | 1.56 | 201 | 95 | **2.12** | 4.61 | 13.05 |
| q05 | 841 | 1,293 | 0.65 | 1.30 | 176 | 101 | **1.74** | 4.77 | 12.76 |
| q13 | 1,367 | 2,360 | 0.58 | 1.07 | 274 | 188 | **1.46** | 5.00 | 12.56 |
| q20 | 666 | 1,548 | 0.43 | 0.83 | 157 | 111 | **1.41** | 4.24 | 13.92 |
| q01 | 1,855 | 2,809 | 0.66 | 1.84 | 290 | 207 | **1.40** | 6.41 | 13.60 |
| q02 | 81 | 104 | 0.79 | 1.28 | 33 | 24 | **1.38** | 2.47 | 4.33 |
| q08 | 728 | 1,102 | 0.66 | 1.00 | 137 | 107 | **1.29** | 5.30 | 10.33 |
| q09 | 1,641 | 3,497 | 0.47 | 0.93 | 320 | 263 | **1.22** | 5.13 | 13.32 |
| q04 | 748 | 1,356 | 0.55 | 0.96 | 128 | 106 | **1.21** | 5.84 | 12.84 |
| q11 | 97 | 272 | 0.36 | 0.85 | 37 | 35 | **1.05** | 2.62 | 7.66 |
| q06 | 369 | 218 | 1.69 | 1.36 | 36 | 37 | **0.98** | 10.28 | 5.98 |
| q18 | 2,261 | 2,999 | 0.75 | 0.89 | 210 | 214 | **0.98** | 10.79 | 13.99 |
| q17 | 397 | 628 | 0.63 | 0.75 | 58 | 61 | **0.94** | 6.89 | 10.28 |
| q12 | 663 | 1,193 | 0.56 | 0.82 | 91 | 102 | **0.89** | 7.26 | 11.67 |
| q22 | 237 | 574 | 0.41 | 0.83 | 47 | 53 | **0.89** | 5.03 | 10.81 |
| q21 | 4,513 | 9,737 | 0.46 | 0.56 | 555 | 659 | **0.84** | 8.12 | 14.78 |

**Scaling losers at 16 cores** (Ibex scaling below 5×): q02 2.47×, q11 2.62×,
q10 2.68×, q16 2.85×, q20 4.24×, q03 4.61×, q07 4.75×, q05 4.77×. Joins and
the small queries, as the dev-box breaker map said. **Per-core losses** are
unchanged: q06 1.69, q15 1.39, q14 1.16, q19 1.12, q16 1.02. **Ibex still wins
at 16 cores** on q06, q18, q22, q12, q17 and q21. q06 and q18 scale about 10×.

### 1b. Dev box sweep (i7-13700 under WSL2): superseded as a cross-engine baseline

Kept for the record and for single-engine work. Its cross-engine ratios above
about 4 cores are box artefacts (see the intro): Polars scales 4.70× at 8
cores here against 7.36× on physical cores, so every dev-box ratio at 8 and up
flatters Ibex.

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
| q11 † | 67 | 191 | 0.35 | 39 | 58 | 0.67 | 1.74 | 3.29 | 0.66 | 1.61 | 2.98 |
| q20 | 455 | 1,106 | 0.41 | 164 | 236 | 0.70 | 2.77 | 4.69 | 0.79 | 2.63 | 5.01 |
| q22 | 178 | 350 | 0.51 | 56 | 80 | 0.69 | 3.18 | 4.34 | 0.61 | 3.68 | 4.45 |
| q21 | 3,188 | 6,822 | 0.47 | 581 | 1,357 | 0.43 | 5.48 | 5.03 | 0.52 | 5.63 | 6.28 |

† Re-measured after the q11 fix (`c1b9855e`, runs
`20260924T0{70231,70811}Z_c1b9855e_sf8`, the same day and box). The original
row timed a q11 that returned zero rows at SF-8 (W0 item 6). The fix barely
moves it: Ibex was 0.72 at 8c on the broken query and is 0.67 on the correct
one. The suite totals above keep the original q11 figures, which shifts them
by less than 0.1%. Those two runs also re-measure the suite: 0.91 at 8c and
0.96 at 16c, against 0.92 and 0.99 above, so the curve holds across sittings.

Read it in two columns, as before. **Per-core losses** (1c ratio above 1) are
q06, q15 and q14. **Scaling losses** are q10, q16, q19, q03, q01 and q07, which
win or tie at one core and lose at eight. q10 and q16 scale less than 2× at 8
cores.

## 2. Past 8 cores on the dev box (measured, exploratory; SUPERSEDED by §1a)

**Read §1a instead.** This section concluded that the gap does not reopen past
8 cores and that Ibex regresses from 12 to 16 on joins. On physical cores
neither holds: the gap grows (1.08 → 1.36) and Ibex keeps scaling (12→16c:
−13%). Both were effects of the E-cores and SMT siblings, which cost Polars
more than Ibex. The text is kept as the record of what the dev box shows.


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

## 3. Where the time goes (AWS physical cores, 2026-09-24)

`run-tpch.sh --profile --cores 8,16` on the §1a box type (r7i.8xlarge, SMT off,
`de99061c`). It runs `breaker_map.py` and `profile_suite.py` under `taskset`,
with no timing suite. Artifact:
`benchmarking/results/tpch_aws_20260924T120611.tar.gz` (`results/profile/`).
The dev-box 16-core profile from the same day is summarized at the end of this
section, for contrast only.

**Serial time is the Amdahl term, and it does not shrink with cores.**
`profile_suite.py`, summed over the suite:

| | AWS 8c | AWS 16c | dev box 16c |
|---|---:|---:|---:|
| wall | 6,258 | 4,739 | 5,991 |
| serial self | **1,833** | **1,869** | 1,729 |
| barrier wait | 3,126 | 1,842 | 2,922 |
| ring wait | 1,282 | 1,014 | 1,329 |
| pool unqueued (share of capacity) | 42% | 55% | 42% |
| perfect-scheduling ceiling (`serial + pool_work/N`) | 5,088 (−19%) | 3,930 (−17%) | 5,086 (−15%) |

At 16 cores, serial self time is **39% of Ibex's wall time**. Perfectly
scheduling the parallel work that exists would buy 17%. Parity needs about
26% (§1a: 3,768 → 2,780 ms), so **the gap cannot be closed by scheduling
alone. Serial work has to become parallel.** The dev box overstated barrier
wait by about 60% (2,922 against 1,842 ms at 16 cores): on heterogeneous cores
a fan-out waits on a slower straggler.

**Serial time by query (AWS 16c, ms; barrier wait and wall beside it):**

| query | serial | barrier | wall |
|---|---:|---:|---:|
| q21 | **708** | 252 | 961 |
| q13 | 182 | 36 | 360 |
| q10 | 182 | 157 | 379 |
| q18 | 113 | 215 | 376 |
| q01 | 92 | 168 | 315 |
| q07 | 84 | 140 | 249 |
| q03 | 75 | 124 | 222 |
| q20 | 72 | 44 | 214 |
| q05 | 68 | 120 | 189 |
| q09 | 59 | 219 | 367 |
| q19 / q16 / q15 / q11 | 48 / 47 / 35 / 30 | | |
| the other eight | ≤ 23 each | | |

q21 alone is 38% of all serial time. The top five (q21, q13, q10, q18, q01)
are 68%. q21 is also Ibex's best query against Polars at every core count, so
fixing it moves the total and does nothing for the geomean.

**The breaker map (AWS 16c, 20 of 22 reliable; q11 and q15 excluded):**
43,516 idle core-ms. By family: **join 41.2%**, **scan 27.0%**, **aggregate
25.7%**, map 4.9%. These are close to the dev box's shares (43.6 / 22.3 /
26.1), so the family ranking transfers. The top rows:

| query | operator | idle core-ms |
|---|---|---:|
| q21 | `join semi keys=1` | 3,514 |
| q13 | `aggregate keys=1 aggs=1` | 2,929 |
| q21 | `Aggregate.Discovery` | 2,456 |
| q21 | `source decode whole` | 2,078 |
| q04 | `join semi keys=1` (scan-starved, as before) | 2,063 |
| q10 | `source decode whole` | 1,652 |
| q20 | `join semi keys=2` | 1,614 |
| q13 | `scan __ibex_source_1` (the LIKE scan) | 1,614 |
| q19 | `join inner keys=1` | 1,592 |
| q01 | `update` | 1,101 |

At 8 cores, four queries' closure fell outside the trusted band (q01, q11, q13,
q15), so read the 8-core map without them.

**Re-ranking W2–W5 from the physical-core view: work on the losers.** At 16
cores the losing queries account for the whole gap. Per query, Ibex minus
Polars on AWS: **losers +1,117 ms, winners −129 ms** (q21 −103), for a total
of +988. q21's lead also shrinks with cores (−526 ms at 8, −103 at 16), so
speeding it up would widen a shrinking win and close none of the gap. Polars
scales the losers 7–13× at 16 cores, so that work demonstrably parallelizes.
Order, by share of the 16-core gap:

1. **q10: +263 ms (24%).** Scales 2.68× against Polars' 7.65×. 182 ms serial,
   the serial hash build (predates the build/probe split, re-time it first),
   and `source decode whole` (W3), the sixth-largest idle row.
2. **The inner-join group: q07 +117, q03 +106, q05 +75, q19 +67 (33%).**
   Scaling 4.6–6.5× against 12–13×. One mechanism, W2's inner-join probe and
   output assembly (`join-perf-plan.md`, memory `project_join_parallelism`),
   so fix it once and measure all four.
3. **q16 +86 (8%) and q13 +86 (8%).** q16 scales 2.85×: its
   composite-categorical `distinct` (the I3 gap in `parallelism-overview.md`).
   q13: 182 ms serial, the aggregate row, and the fused non-anchored LIKE scan
   (`query-shape-conformance-plan.md` item 1).
4. **q01 +83 (7%).** `update` plus scan/aggregate contention; removing the
   overlap is a measured no-go (§6), so look elsewhere.
5. **The tail: q09, q20, q14, q15, q08, q04, q02, q11** (+234 together). q14 and
   q15 are also per-core losses (W4).
6. **q21: a watch item, not a target.** It is still the largest serial term (708
   ms, 38%) and will turn into a loser somewhere past 16 cores at this rate.
   Revisit when the target goes beyond 16 cores.
7. **W1 (constants) after these.** Physical cores show no 16-core cliff.

*Dev box, 16 cores (same day, for contrast):* 41,090 idle core-ms, split join
43.6%, scan 22.3%, aggregate 26.1%; serial 1,729 ms; barrier 2,922 ms. Its
per-query serial ranking matches AWS's top five. Its barrier bucket, and the
join regression it seemed to explain, do not transfer.

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
3. **DONE 2026-09-24: the publishable curve on AWS** (§1a). One
   `r7i.8xlarge` with SMT off, `run-tpch.sh --cores 2,4,8,12,16` (the list form
   was added for this, `994b017c`). The stale AMI was fine. Two launches failed
   first, and both were harness bugs that only a fresh clone exposes: the exit
   handler could not ship its log (`76271a6d`), and the answer check needed the
   `scale-<sf>` path (`461c0963`). About $4 in total.
4. **DONE 2026-09-24: breaker map and `profile_suite.py` at 16 cores**, on the
   dev box and then on AWS at 8 and 16 physical cores (`run-tpch.sh --profile`,
   `de99061c`). §3 is the AWS version.
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
   - **Bisected 2026-09-24.** `perf stat` pairs at 1 core put q04 at +6.1%
     (10 of 10 pairs, task-clock +6.3%) and q21 at only +2.1% (8 of 10). So the
     bisect ran on q04. The first bad commit is `7a9cdd54` (Add Decimal(p, s)):
     30 of 30 pairs, +6.1% against its parent. It did not change the loop that
     got slower. `compare_vec`'s Date/Timestamp column-vs-column branch kept a
     `switch (op)` inside its per-row loop, and that was only fast while the
     optimizer hoisted the switch out. Decimal grew `compare_vec`, which then
     got inlined into `compute_mask`. The loop went scalar and reloaded both
     column pointers every row: 81 → 277 samples. **Fixed** by routing
     Date/Timestamp through `cmp_into` like the numeric types: q04 −6.5% and
     q07 −4.0% at 1 core, geomean −1.2%, 8 cores neutral, answers
     byte-identical. q04 ends up 4.7% faster than before Decimal. q21's +2% is
     not this and was not recovered; it is too small to bisect reliably.
     Lesson: a kernel whose speed depends on the optimizer unswitching a loop
     is fragile. Write the per-op loops out, as `cmp_into` does.
   - **Output: only q11 differs,** and legitimately. Both sides return zero
     rows. The base printed a column-less `<empty>` table, and HEAD keeps the
     schema.
6. **DONE 2026-09-24 (`c1b9855e`): q11 was not the same query as Polars'
   above SF-1.**
   `queries/q11.ibex` hardcodes `FRACTION = 0.0001`. TPC-H and the Polars
   reference (`0.0001 / settings.scale_factor`) scale it by SF. At SF-8, Ibex
   filters with a threshold 8× too high and returns zero rows, while Polars
   returns the real answer. q11's ratio in §1 (0.72 at 8c) is invalid until
   the query takes the scale factor. It is the only SF-dependent parameter
   in PDS-H. Fixed: the query reads SF off the supplier count. And
   `run_bench.sh` now diffs every answer against Polars at the benchmarked SF
   before timing (`check_against_polars.py`), which is the check that would
   have caught this. The official answers are SF-1 only, and every other
   check compared Ibex with Ibex.

Exit: §1 and §2 rewritten from measurement, with the implied fraction at
1/2/4/8/12/16 for both engines. Met 2026-09-24, locally and on AWS. W0 is
closed; q21's residual +2% at 1 core (item 5) is too small to bisect.

### W1: Constants and gates tuned at 8 cores (still worth a sweep, but §1a shows no 16-core cliff)

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
  bug, not drift. Byte-identity compares Ibex with Ibex, so it cannot catch a
  query that is wrong on both sides. `run_bench.sh` therefore also diffs every
  answer against upstream Polars at the benchmarked SF before timing
  (`check_against_polars.py`; the SF-1 official answers are
  `check_answers.py`). Don't pass `--no-answer-check` for a published number.
- **Warm and fresh.** `run_bench.sh` times a warm in-process loop for both
  engines. Ibex gains far more from warmth than Polars (q21: ~38% against ~8%,
  from first-touch page faults), so a large Ibex win should also be quoted
  fresh-process (`plans/allocator-and-huge-pages.md`).
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

- **jemalloc for the Ibex executables:** +3.7% on fresh 1-core PDS-H, a wash at
  8 cores. **Huge pages for column buffers only:** −0.6% / −1.8%, with q04 and
  q18 slower. Whole-heap huge pages are the lever
  (`plans/allocator-and-huge-pages.md`, parked).

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
- geomean at or below parity at 8 cores.

Status on 2026-09-24 (§1a): the 1-core lead holds (0.61), but Ibex trails from
about 6 cores on: 1.08 at 8, 1.36 at 16 on total, and a geomean of 1.20 at 8
and 1.45 at 16. Winning at 16 needs an implied fraction of about 94%, against
88% today.
- the 1-core total no worse than today's baseline.

Re-check at SF-2 before publishing, since thresholds were calibrated there and
a ranking does not always transfer.
