# Python frontends: performance summary

Where `IbexEngine` (Polars queries run on Ibex) and the independent `ibex`
package stand against Polars 2.0.0. This replaces the per-change measurement
diaries (`pdsh-performance.md`, `join-performance.md`,
`polars-engine-performance.md`, `q01-performance.md`,
`grouped-mean-performance.md`); the investigations behind each number live in
git history and the plans.

**Method.** Local i7-13700, Linux, Release build, CPU frequency locked at
3.2 GHz unless noted. The reference is Polars 2.0.0's streaming engine with
Categorical keys, re-run in the same sitting. Eight threads are pinned with
`taskset -c 0,2,4,6,8,10,12,14` (eight physical cores), one thread with
`taskset -c 2`, and both engines get the same budget (`IBEX_CORES` and
`POLARS_MAX_THREADS`). Every result is checked against Polars before timing.
Raw samples stay on the measuring machine; they are not versioned.

## PDS-H through `IbexEngine` (scale 10)

Upstream `polars-benchmark` query functions, unchanged, on upstream-generated
Parquet. Ibex/Polars-streaming medians, 2026-11-07 afternoon (`075948ae`);
all 22 answers match at both budgets.

| Query | 8 threads | 1 thread | Query | 8 threads | 1 thread |
|---|---:|---:|---|---:|---:|
| Q01 | 1.09 | 0.96 | Q12 | 0.93 | 0.92 |
| Q02 | 0.91 | 0.97 | Q13 | 0.70 | 0.66 |
| Q03 | 0.99 | 0.92 | Q14 | 1.03 | 0.94 |
| Q04 | 1.06 | 1.05 | Q15 | 0.96 | 0.93 |
| Q05 | 1.04 | 0.97 | Q16 | 0.85 | 0.78 |
| Q06 | 0.87 | 0.88 | Q17 | 0.89 | 1.00 |
| Q07 | 1.03 | 0.91 | Q18 | 0.65 | 0.61 |
| Q08 | 1.01 | 0.94 | Q19 | 1.05 | 1.09 |
| Q09 | 1.08 | 0.91 | Q20 | 0.77 | 0.71 |
| Q10 | 0.80 | 0.72 | Q21 | 1.04 | 0.79 |
| Q11 | 0.92 | 0.88 | Q22 | 0.90 | 1.05 |

Geometric mean **0.93× at 8 threads** (faster on 13/22; summed medians 6.75 s
against 7.16 s) and **0.88× at 1 thread** (19/22; 42.4 s against 49.0 s).
A 2026-11-10 resweep reproduced both means (0.933 / 0.880) with one change:
Q22 at one thread rose from 455 to 527 ms, in the adapter path only (native
Q22 did not move), between `075948ae` and the 11-10 head; not yet bisected.
The largest remaining gaps are Q01 and Q09 at 8 threads (memory-bound
aggregation at eight workers) and Q19/Q04 at one.

For the native hand-written Ibex queries against Polars and DuckDB on AWS,
see the [published benchmark snapshot](../benchmarking/aws/README.md#published-benchmark-snapshot).

## h2o joins through `IbexEngine`

`J1_1e7_NA_0_0`, a 10M-row `x` joined to small / medium / big tables, with
compact Int32 storage (then `IBEX_COMPACT_INT32=1`, now the default); median paired Ibex/Polars ratios, 2026-11-10:

| Question | 8 threads | 1 thread |
|---|---:|---:|
| q1 small inner on int | 1.24 | 0.83 |
| q2 medium inner on int | 1.35 | 0.91 |
| q3 medium outer (left) on int | 1.43 | 1.19 |
| q4 medium inner on factor | 1.24 | 0.75 |
| q5 big inner on int | 0.88 | 0.47 |

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
