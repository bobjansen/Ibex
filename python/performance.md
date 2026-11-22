# Python frontends: performance summary

Where `IbexEngine` (Polars queries run on Ibex) and the independent `ibex`
package stand against Polars. The PDS-H section is measured against Polars
2.1.0, which `IbexEngine` pins; the other sections were measured against
Polars 2.0.0 and have not been re-run. This replaces the per-change measurement
diaries (`pdsh-performance.md`, `join-performance.md`,
`polars-engine-performance.md`, `q01-performance.md`,
`grouped-mean-performance.md`); the investigations behind each number live in
git history and the plans.

**Method.** Local i7-13700, Linux, Release build, CPU frequency locked at
3.2 GHz unless noted. The reference is Polars' streaming engine with
Categorical keys, re-run in the same sitting. Eight threads are pinned with
`taskset -c 0,2,4,6,8,10,12,14` (eight physical cores), one thread to one of
those cores, and both engines get the same budget (`IBEX_CORES` and
`POLARS_MAX_THREADS`). Every result is checked against Polars before timing.
Raw samples stay on the measuring machine; they are not versioned.

## PDS-H through `IbexEngine` (scale 10)

Upstream `polars-benchmark` query functions, unchanged, on upstream-generated
Parquet. Ibex/Polars-streaming medians against Polars 2.1.0, 2026-11-22
(`b0070d41`), six interleaved rounds; all 22 answers match at both budgets.

| Query | 8 threads | 1 thread | Query | 8 threads | 1 thread |
|---|---:|---:|---|---:|---:|
| Q01 | 0.91 | 0.82 | Q12 | 0.87 | 0.86 |
| Q02 | 0.95 | 0.97 | Q13 | 0.69 | 0.67 |
| Q03 | 0.98 | 0.92 | Q14 | 1.07 | 0.95 |
| Q04 | 0.99 | 1.17 | Q15 | 0.92 | 0.96 |
| Q05 | 1.07 | 0.96 | Q16 | 1.11 | 0.97 |
| Q06 | 0.87 | 0.88 | Q17 | 0.95 | 0.97 |
| Q07 | 1.04 | 0.92 | Q18 | 0.63 | 0.55 |
| Q08 | 1.00 | 0.95 | Q19 | 0.92 | 0.97 |
| Q09 | 1.05 | 0.92 | Q20 | 0.78 | 0.75 |
| Q10 | 0.80 | 0.71 | Q21 | 1.06 | 0.88 |
| Q11 | 0.69 | 0.67 | Q22 | 0.96 | 1.05 |

Geometric mean **0.91× at 8 threads** (faster on 15/22; summed medians 6.47 s
against 7.02 s) and **0.87× at 1 thread** (20/22; 42.0 s against 49.0 s).
Polars 2.1.0 is within about 3% of 2.0.0 on most queries; Q16 got 17–23%
faster (now Ibex's largest 8-thread gap) and Q18 14% slower at 8 threads.
The other gaps at 8 threads are Q05, Q14, Q21 and Q09 (1.05–1.07×), and Q04
(1.17×) at one. Against Polars 2.0.0 on 2026-11-07 (`075948ae`) the means
were 0.93× and 0.88×.

For the native hand-written Ibex queries against Polars and DuckDB on AWS,
see the [published benchmark snapshot](../benchmarking/aws/README.md#published-benchmark-snapshot).

## h2o groupby through `IbexEngine`

The h2o db-benchmark's Polars groupby queries, unchanged, on resident 10M-row
frames: `G1_1e7_*` with K groups per id column, one dataset with 5% nulls and
one pre-sorted. Ibex/Polars-streaming ratios of each question's minimum over
five runs after a warmup, against Polars 2.1.0, 2026-11-22 (`7d41f173`); every
answer validated. Each cell is **8 threads / 1 thread**.

| Question | K=1e2 | K=1e2, 5% NA | K=1e2, sorted | K=1e1 | K=2e0 |
|---|---:|---:|---:|---:|---:|
| q1 sum v1 by id1 | 0.46 / 0.18 | 0.56 / 0.41 | 0.65 / 0.44 | 0.72 / 0.21 | 0.55 / 0.30 |
| q2 sum v1 by id1:id2 | 0.58 / 0.31 | 0.56 / 0.37 | 0.58 / 0.31 | 0.54 / 0.22 | 0.84 / 0.26 |
| q3 sum v1 mean v3 by id3 | 0.86 / 0.45 | 0.80 / 0.51 | 0.82 / 0.45 | 0.77 / 0.39 | 0.95 / 0.51 |
| q4 mean v1:v3 by id4 | 0.72 / 0.39 | 0.72 / 0.55 | 0.71 / 0.40 | 0.74 / 0.39 | 1.14 / 0.57 |
| q5 sum v1:v3 by id6 | 0.78 / 0.49 | 0.81 / 0.54 | 0.77 / 0.51 | 0.79 / 0.55 | 1.00 / 0.73 |
| q6 median v3 sd v3 by id4 id5 | 0.50 / 0.39 | 0.40 / 0.28 | 0.45 / 0.39 | 0.34 / 0.40 | 0.41 / 0.48 |
| q7 max v1 - min v2 by id3 | 1.13 / 0.57 | 1.15 / 0.69 | 1.02 / 0.56 | 0.95 / 0.56 | 1.05 / 0.73 |
| q8 largest two v3 by id6 | 0.30 / 0.44 | 0.36 / 0.54 | 0.29 / 0.44 | 0.13 / 0.26 | 0.06 / 0.18 |
| q9 regression v1 v2 by id2 id4 | 0.78 / 0.43 | 0.75 / 0.39 | 0.73 / 0.42 | 0.93 / 0.45 | 1.59 / 0.48 |
| q10 sum v3 count by id1:id6 | 1.15 / 1.05 | 1.26 / 1.23 | 1.19 / 1.27 | 1.20 / 1.13 | 1.25 / 1.11 |
| **geomean** | **0.68 / 0.43** | **0.68 / 0.51** | **0.67 / 0.48** | **0.62 / 0.40** | **0.69 / 0.47** |

Ibex is faster on 38 of the 50 eight-thread cells and 45 of the 50 one-thread
cells. The consistent loss is q10, a sum and count keyed by all six id
columns, at 1.05–1.27×; q8 (the two largest values per group) is where Ibex
gains most. Against Polars 2.0.0 on 2026-11-08 (seven questions) the
eight-thread geomeans were 0.75–1.22×.

## h2o joins through `IbexEngine`

`J1_1e7_NA_0_0`, a 10M-row `x` joined to small / medium / big tables, same
run and method as the groupby table:

| Question | 8 threads | 1 thread |
|---|---:|---:|
| q1 small inner on int | 1.45 | 0.83 |
| q2 medium inner on int | 1.35 | 0.94 |
| q3 medium outer (left) on int | 0.78 | 0.70 |
| q4 medium inner on factor | 1.29 | 0.81 |
| q5 big inner on int | 0.87 | 0.47 |
| **geomean** | **1.11** | **0.73** |

The small and medium inner joins still lose at eight threads; the left join
(q3) has won since the streaming left join landed (it was 1.43× on 11-10).
The native joins are as fast as Polars or faster; the remainder is
converting ~9M-row results to Polars, chiefly `cat.to`'s per-row category
validation. Status, mechanisms and how to measure:
[plans/polars-engine-join-gap.md](../plans/polars-engine-join-gap.md).

## Synthetic joins through `IbexEngine`

Seeded resident inputs (1M left rows), default threads (24), medians of 12
alternating repetitions; collection includes conversion to a Polars
`DataFrame`, plan preparation excluded:

| Join | Polars ms | `IbexEngine` ms |
|---|---:|---:|
| Integer inner | 5.60 | 5.57 |
| Left | 5.90 | 7.60 |
| Full | 8.17 | 18.23 |
| Semi | 1.86 | 1.36 |
| Anti | 2.24 | 1.39 |
| Many-to-many | 1.50 | 2.91 |
| Composite keys | 8.82 | 9.43 |
| Categorical keys | 2.90 | 6.75 |
| Cross | 0.63 | 1.03 |

At one thread Ibex led on the unique-key inner join (1.6×), semi/anti
(~2.5×) and many-to-many (1.1×). These predate the 11-10 conversion work
above, which targets the categorical and full-join gaps. Driver:
`python/bench_polars_joins.py`.

## Resident grouped mean (`IbexEngine` and `import ibex as pl`)

Mean price by symbol and day on resident categorical inputs (`prices_multi`
shape: 252 symbols, four days, 1,008 groups), warm collection medians,
2026-10-12 (WSL2, before the native-box move):

| Rows | Cores | `IbexEngine` | `ibex` package | Polars streaming |
|---:|---:|---:|---:|---:|
| 1M | 1 | 2.39 ms | 1.97 ms | 5.77 ms |
| 1M | 8 | 1.62 ms | 1.16 ms | 3.02 ms |
| 16M | 1 | 26.39 ms | 25.76 ms | 85.90 ms |
| 16M | 8 | 9.43 ms | 8.89 ms | 14.52 ms |

Every pair favours Ibex (p=0.00049). The page's historical 21.6× for this
workload came from an older machine, older engines and plain string keys;
it is not a claim for these packages. First use is a different cost: at 16M
rows and eight cores, preparation took 530 ms (Polars 319 ms) and the first
collection 162 ms (Polars 14.9 ms). The package pays off on repeated queries
over prepared tables. Driver: `python/bench_mean_by_symbol_day.py`; example:
[examples/ibex_python_group_mean.py](../examples/ibex_python_group_mean.py).

## PDS-H q01 through the `ibex` package

Upstream `queries.polars.q1.q` on SF-1 `lineitem` (6.0M rows) loaded before
timing, 2026-10-12 (WSL2): 97.3 ms against 96.1 ms at one core (unclear,
p=0.30) and 26.8 against 26.2 ms at eight (1.04×, p=0.021). Preparing
native input once, with repeated strings encoded, is what makes the package
competitive: the raw-import route took 39.4 ms against 14.2 ms prepared on
1M synthetic rows. Driver: `python/bench_pdsh_q01.py`.

## Rerunning

Build the complete Release tree first (`cmake --build build-release -j6`),
then, with `PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python` and the
pinning above:

- PDS-H: `.venv/bin/python python/bench_pdsh_engine.py --out <file>.json`
- h2o joins: `.venv/bin/python python/bench_join_gap.py --out <file>.json`
- synthetic joins: `.venv/bin/python python/bench_polars_joins.py --out <file>.json`
- grouped mean: `.venv/bin/python python/bench_mean_by_symbol_day.py --rows 16000000 --iters 12 --out <file>.json`
- the full Polars-engines suite: `python/bench_polars_engines.sh`, summarised
  by `python/summarize_polars_engines.py`

Use `IbexEngine(cache_size=0)` for an engine that must not reuse a prepared
plan, and never compare against a Polars number from an earlier sitting.
