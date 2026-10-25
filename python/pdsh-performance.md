# PDS-H through the Polars engine adapter

Measured 2026-10-25 on an Intel i7-13700, Linux, Release build, with CPU frequency
locked at 3.2 GHz. These are the unchanged upstream Polars query functions on
upstream-generated scale-10 Parquet data (59,986,052 lineitem rows and
15,000,000 orders). This compares `query.collect` using
Polars 2.0.0's in-memory executor, its streaming executor, and `IbexEngine`.
It does not substitute the native hand-written Ibex PDS-H queries.

## Results

Warm collection medians in milliseconds. The table uses the default-thread run
and the repeated pinned runs after a busy browser process had exited. Initial
pinned runs showed substantial interference and were excluded from this table;
their raw samples are retained separately below.

| Threads | Query | Polars in-memory ms | Polars streaming ms | Polars–Ibex ms |
|---|---|---:|---:|---:|
| 1 | Q1 | 5,469 | 4,049 | 4,027 |
| 1 | Q4 | 1,890 | 1,181 | 1,468 |
| 1 | Q6 | 1,756 | 1,478 | 10,326 |
| 8 | Q1 | 792 | 577 | 847 |
| 8 | Q4 | 298 | 168 | 346 |
| 8 | Q6 | 223 | 217 | 8,351 |
| Default (24) | Q1 | 487 | 317 | 673 |
| Default (24) | Q4 | 224 | 99 | 266 |
| Default (24) | Q6 | 110 | 107 | 8,311 |

Q6 is the clear performance outlier: at default threading Polars–Ibex is about
75 times slower than standard Polars and 78 times slower than streaming Polars.
Polars–Ibex collection takes approximately 8.3 seconds at both eight and
24 threads. These measurements identify a problem, not its cause; no runtime
or adapter optimization was made as part of this benchmark.

Q1's first default-thread collection took 528 / 318 / 1,211 ms for standard
Polars / streaming / Polars–Ibex, compared with warm medians of
487 / 317 / 673 ms. Preparation is separately recorded. First-use cost remains
relevant alongside warm throughput.

## Coverage

Only **Q1, Q4 and Q6** currently translate with the default optimization flags.
All three pass complete result comparisons before timing.
The other 19 queries are rejected, without falling back to Polars or rewriting
the queries. These are first blockers, not exhaustive inventories per query:

| Queries | First translation blocker |
|---|---|
| Q2, Q8, Q18, Q21 | Cached subplans (`Cache`) |
| Q3, Q5 | Unsupported comparison between operand types |
| Q7, Q17, Q19, Q20 | Joins carrying fused predicates |
| Q9, Q13 | String `contains` |
| Q10 | Floating-point grouping keys |
| Q11, Q22 | Inequality joins (`IEJoin`) |
| Q12, Q16 | List literals |
| Q14 | Conditional expression (`Ternary`) |
| Q15 | Float64 `max` aggregate |

## Measurement method

Each thread configuration runs in a separate process. One worker is pinned to
CPU 2; eight workers use CPUs 0,2,4,6,8,10,12,14; default threading uses the full
machine. `IBEX_CORES` and `POLARS_MAX_THREADS` receive matched budgets, or are
both unset for the default run. OpenBLAS is limited to one thread.

Translation/preparation is outside timing. Collection includes Parquet reading,
query execution, Arrow export and construction of the Polars result. There is
no result caching. After the initial correctness collections and two warmups,
six interleaved rounds balance each executor's position. Destruction of the
previous result is outside timing. These are warm-process, warm-file-cache
measurements, not cold-storage latency. Initial per-query collection times and
preparation times are retained separately in the JSON reports; only Q1 is the
first query in each fresh process.

Correctness compares every row, column name, dtype and row order against
standard Polars, for both other executors. Floating-point tolerance is
`rel_tol=1e-10, abs_tol=1e-8`. Reports retain every sample, the bridge hash,
upstream query source hashes, revisions, affinity and data file metadata.

A concurrent rebase briefly interrupted initial build attempts. Timing began
only after the tree was reconciled and the full Release rebuild succeeded.
The measured bridge and Python packages were copied aside to insulate the run
from subsequent edits. The snapshot's adapter sources are identical to the
recorded measurement revision.

## Reproduction

Build the complete Release tree first. Run the commands sequentially, with no
concurrent compilation or benchmark work. Replace the upstream checkout path
with its location on your machine:

```bash
cmake --build build-release -j 6
PYTHONPATH=build-release/python:python IBEX_CORES=8 POLARS_MAX_THREADS=8 \
  OPENBLAS_NUM_THREADS=1 taskset -c 0,2,4,6,8,10,12,14 \
  .venv/bin/python python/bench_pdsh_engine.py \
  --pdsh-root ../polars-benchmark \
  --data ../polars-benchmark/data/tables/scale-10.0 \
  --out results/pdsh-eight.json
```

For one worker, set both thread variables to 1 and use `taskset -c 2`. For
default threading, unset both variables and omit `taskset`. Affinity numbers
here describe this i7-13700's P-core topology and need adapting on other hosts.

## Saved measurements

- [Single worker](pdsh-performance-results/sf10-single.json)
- [Eight workers](pdsh-performance-results/sf10-eight.json)
- [Default threading](pdsh-performance-results/sf10-default.json)
- Excluded initial runs with competing load: [single](pdsh-performance-results/sf10-single-initial.json), [eight](pdsh-performance-results/sf10-eight-initial.json)

The [runner](bench_pdsh_engine.py) records all 22 query outcomes in every report.
There is no whole-suite performance score: 19 queries are unsupported.
