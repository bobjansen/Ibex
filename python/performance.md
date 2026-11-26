# Python frontends: performance summary

Where `IbexEngine` (Polars queries run on Ibex) and the independent `ibex`
package stand against Polars. The PDS-H and h2o sections are measured against
Polars 2.1.0, which `IbexEngine` pins; the other sections were measured
against Polars 2.0.0 and have not been re-run. This replaces the per-change measurement
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
Parquet. Ibex/Polars-streaming medians against Polars 2.1.0, 2026-11-26
(`c317ea77`), six interleaved rounds; all 22 answers match at both budgets.
This run's CPU frequency was not locked; a second full run the same morning
gave the same geometric means to within 0.01.

| Query | 8 threads | 1 thread | Query | 8 threads | 1 thread |
|---|---:|---:|---|---:|---:|
| Q01 | 1.02 | 0.83 | Q12 | 0.91 | 0.89 |
| Q02 | 0.95 | 0.98 | Q13 | 0.68 | 0.66 |
| Q03 | 1.01 | 0.94 | Q14 | 1.12 | 0.97 |
| Q04 | 1.10 | 1.15 | Q15 | 0.95 | 0.96 |
| Q05 | 1.06 | 0.96 | Q16 | 1.16 | 1.00 |
| Q06 | 0.94 | 0.89 | Q17 | 0.95 | 0.99 |
| Q07 | 1.03 | 0.92 | Q18 | 0.62 | 0.53 |
| Q08 | 1.03 | 0.95 | Q19 | 0.97 | 0.98 |
| Q09 | 1.04 | 0.91 | Q20 | 0.77 | 0.71 |
| Q10 | 0.79 | 0.71 | Q21 | 1.06 | 0.88 |
| Q11 | 0.70 | 0.67 | Q22 | 0.98 | 1.08 |

Geometric mean **0.93× at 8 threads** (faster on 12/22; summed medians 5.56 s
against 5.94 s) and **0.88× at 1 thread** (19/22; 26.6 s against 31.1 s).
The largest gaps at 8 threads are Q16 (1.16×), Q14 (1.12×), Q04 (1.10×),
Q05 and Q21 (1.06×); at one thread Q04 (1.15×) and Q22 (1.08×). Ratios near
1.0 move by a few hundredths between runs: Q01 was 0.98 and Q04 1.02 in the
first run. On 2026-11-22 (`b0070d41`, locked at 3.2 GHz) the means were
0.91× and 0.87×, and against Polars 2.0.0 on 2026-11-07 (`075948ae`) 0.93×
and 0.88×.

For the native hand-written Ibex queries against Polars and DuckDB on AWS,
see the [published benchmark snapshot](../benchmarking/aws/README.md#published-benchmark-snapshot).

## h2o groupby through `IbexEngine`

The h2o db-benchmark's Polars groupby queries, unchanged, on resident 10M-row
frames: `G1_1e7_*` with K groups per id column, one dataset with 5% nulls and
one pre-sorted. Ibex/Polars-streaming ratios of each question's minimum over
five runs after a warmup, against Polars 2.1.0, 2026-11-26 (`c317ea77`, CPU
frequency not locked); every answer validated. Each cell is **8 threads / 1
thread**.

| Question | K=1e2 | K=1e2, 5% NA | K=1e2, sorted | K=1e1 | K=2e0 |
|---|---:|---:|---:|---:|---:|
| q1 sum v1 by id1 | 0.39 / 0.19 | 0.66 / 0.39 | 0.73 / 0.42 | 0.44 / 0.21 | 0.55 / 0.30 |
| q2 sum v1 by id1:id2 | 0.62 / 0.33 | 0.54 / 0.37 | 0.66 / 0.32 | 0.55 / 0.23 | 0.70 / 0.27 |
| q3 sum v1 mean v3 by id3 | 0.75 / 0.45 | 0.83 / 0.52 | 0.81 / 0.45 | 0.68 / 0.39 | 0.87 / 0.50 |
| q4 mean v1:v3 by id4 | 0.72 / 0.42 | 0.71 / 0.55 | 0.74 / 0.43 | 0.75 / 0.41 | 0.97 / 0.59 |
| q5 sum v1:v3 by id6 | 0.68 / 0.47 | 0.76 / 0.54 | 0.85 / 0.47 | 0.78 / 0.58 | 0.89 / 0.71 |
| q6 median v3 sd v3 by id4 id5 | 0.40 / 0.37 | 0.38 / 0.30 | 0.43 / 0.37 | 0.31 / 0.38 | 0.40 / 0.54 |
| q7 max v1 - min v2 by id3 | 1.08 / 0.57 | 1.14 / 0.65 | 1.10 / 0.56 | 0.89 / 0.59 | 1.03 / 0.70 |
| q8 largest two v3 by id6 | 0.33 / 0.45 | 0.32 / 0.55 | 0.38 / 0.45 | 0.15 / 0.26 | 0.08 / 0.20 |
| q9 regression v1 v2 by id2 id4 | 0.77 / 0.48 | 0.58 / 0.39 | 0.78 / 0.48 | 1.21 / 0.50 | 1.19 / 0.53 |
| q10 sum v3 count by id1:id6 | 1.06 / 1.00 | 1.14 / 1.06 | 1.12 / 1.12 | 1.13 / 0.90 | 1.16 / 1.06 |
| **geomean** | **0.63 / 0.44** | **0.66 / 0.50** | **0.72 / 0.48** | **0.59 / 0.40** | **0.65 / 0.49** |

Ibex is faster on 39 of the 50 eight-thread cells and 46 of the 50 one-thread
cells. The consistent losses are q10, a sum and count keyed by all six id
columns (1.06–1.16× at eight threads), and q7 (1.03–1.14× except at K=1e1);
q8 (the two largest values per group) is where Ibex gains most. On 2026-11-22
(`7d41f173`) the eight-thread geomeans were 0.62–0.69×; against Polars 2.0.0
on 2026-11-08 (seven questions) they were 0.75–1.22×.

## h2o joins through `IbexEngine`

`J1_1e7_NA_0_0`, a 10M-row `x` joined to small / medium / big tables, same
run and method as the groupby table:

| Question | 8 threads | 1 thread |
|---|---:|---:|
| q1 small inner on int | 1.51 | 0.90 |
| q2 medium inner on int | 1.50 | 1.01 |
| q3 medium outer (left) on int | 0.75 | 0.73 |
| q4 medium inner on factor | 1.45 | 0.93 |
| q5 big inner on int | 0.91 | 0.48 |
| **geomean** | **1.18** | **0.78** |

The small and medium inner joins still lose at eight threads, by 12–14 ms
each (q1: 35.8 ms against 23.7). Ibex's probe is cheaper than Polars', but
the result is built in two serial steps: the native join (~18 ms) gathers the
right columns and returns left row numbers, then Polars filters the original
frame's left columns (~18 ms). Most of the extra CPU is the right-column
gather and converting the result (`cat.to`'s category check, and `v2`'s f64 to
f32 cast, since Ibex has no Float32). On 2026-11-22 these were 1.45, 1.35 and
1.29×; the adapter of that day measures the same as today's, so the change is
run-to-run variation, not a regression. The left join (q3) has won since the
streaming left join landed (it was 1.43× on 11-10). Status, mechanisms and how
to measure: [plans/polars-engine-join-gap.md](../plans/polars-engine-join-gap.md).

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
