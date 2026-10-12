# Independent Python q01: local performance check

2026-10-12, Release build, Intel i7-13700 / WSL2. This is a development
measurement, not a replacement for the published PDS-H engine baseline.
The query is the unchanged upstream `queries.polars.q1.q` function, called
with each frontend and identical synthetic Arrow data prepared outside timing.

The initial raw-import frontend took roughly 39–42 ms on one million rows,
versus 17–18 ms for the reference. The native profile showed substantial
filter/gather and group-discovery work on plain string keys. Preparing the
native input once and encoding repeated strings enables Ibex's existing
categorical paths and avoids repeated Arrow conversion. No runtime kernels
were changed, and no query results are cached.

On the actual SF-1 lineitem data (6,001,215 rows, four groups), loaded into
memory before timing with the seven q01 columns supplied identically to both
frontends, warm collection measured **97.27 versus 96.07 ms at one core** and
**26.81 versus 26.18 ms at eight cores**. The one-core comparison is unclear
(`p = 0.301`); the eight-core paired ratio is 1.037 (`p = 0.021`), a small
remaining slowdown. These actual-data results belong alongside the synthetic
cases below; the synthetic wins do not establish an overall q01 win.

Warm end-to-end collection medians, milliseconds:

| Rows | Groups | Cores | Ibex | Reference streaming |
|---|---|---|---|---|
| 100,000 | 6 | 1 | 1.73 | 2.21 |
| 100,000 | 1,000 | 1 | 1.88 | 2.65 |
| 1,000,000 | 6 | 1 | 14.15 | 16.90 |
| 1,000,000 | 1,000 | 1 | 16.19 | 17.02 |
| 1,000,000 | 6 | 8 | 5.38 | 7.18 |
| 1,000,000 | 1,000 | 8 | 5.69 | 9.41 |

The one-core, six-group difference against streaming is statistically unclear
in the final three-way comparison (`p = 0.176`); do not claim a clear win from
those medians. The prepared-versus-raw route is compared in that same run:
14.15 versus 39.43 ms. Paired effects and Wilcoxon results for each case, all
samples, source hashes and revisions are in
[the raw report](../benchmarking/results/python_q01_20261012.json).

The in-memory reference executor was also checked at one million rows,
six groups and one core: 14.11 ms for Ibex, 20.29 ms for the reference. The
default reference thread setting inside an eight-CPU affinity mask selected
eight threads and measured 5.36 versus 7.85 ms. These checks cover input size
and group cardinality; they do not establish performance across all workloads.

## Preparation and first use still matter

The final one-core run measured input preparation at 26.21 ms for Ibex and
19.70 ms for the reference. The first collection in that process took 39.71
and 20.36 ms respectively: preparation plus first collection was **65.92 ms
versus 40.07 ms**. At eight cores, those totals were 40.17 versus 26.45 ms.
These are single fresh-process observations, not statistical estimates. Ibex's
warm-query advantage does not currently extend to first use in this test.

On actual SF-1 input, preparation plus first collection was 374.22 versus
225.43 ms at one core and 200.49 versus 95.35 ms at eight. Those figures exclude
the common file read and use only the query's seven columns, not the full
lineitem schema. The benchmark checks first-use results before warm timing.

Python lowering/validation took about 0.18–0.20 ms on the million-row cases;
the native endpoint took about 13.49 ms at one core and 4.99 ms at eight.
The endpoint includes parsing, planning, execution and Arrow export, rather
than isolating the kernel. Tiny output dtype corrections and result-container
construction are included in end-to-end collection timings.

First-use allocation/preparation and preparing only demanded input columns
remain worthwhile follow-up work. Keep first-use and repeated-query costs
visible as the API grows; do not trade semantic correctness for a benchmark.

## Verification and reproduction

The actual PDS-H q01 data passes at SF-1 for Parquet and in-memory input,
and at SF-10 for Parquet. Names, dtypes, row order and values are checked
against the reference. The synthetic one- and eight-core Ibex outputs also
have identical normalized Arrow IPC hashes.

Build the full Release tree immediately before measurements. Run one process
at a time, with twelve interleaved iterations after two warmups:

```bash
cmake --build build-release --parallel 6
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  taskset -c 2 .venv/bin/python python/bench_pdsh_q01.py \
  --pdsh-root ../polars-benchmark --rows 1000000 --groups 6 \
  --iters 12 --compare-inputs --out /tmp/q01-one-core.json
```

Use `IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0-7` for matched eight-core
runs, choosing CPUs available on your machine. Vary `--rows` and `--groups`;
`--polars-engine in-memory` selects the other reference executor. Clear
`POLARS_MAX_THREADS` to check its default inside the affinity mask.
Use `--lineitem ../polars-benchmark/data/tables/scale-1.0/lineitem.parquet`
to repeat the actual PDS-H in-memory case instead of generating synthetic data.
