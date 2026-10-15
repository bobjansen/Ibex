# h2o db-benchmark (groupby + join)

Ibex vs Polars vs DuckDB on the independent
[h2oai db-benchmark](https://h2oai.github.io/db-benchmark/) questions, as
maintained in [duckdblabs/db-benchmark](https://github.com/duckdblabs/db-benchmark)
(pinned at `3b074bc319`). Unlike the in-memory suite, none of these queries
or data shapes were written alongside Ibex. They include the
high-cardinality cases (`id3`/`id6` with 1e5–5e6 groups, q10 at ~1e7 groups)
where Ibex is known to be weak.

## What is measured

**Groupby** (`G1_<N>_<K>_<NA%>_<sorted>`), 10 questions:

| q | question | Ibex |
|---|---|---|
| q1 | sum v1 by id1 | `x[select { v1 = sum(v1) }, by id1]` |
| q2 | sum v1 by id1:id2 | `x[select { v1 = sum(v1) }, by { id1, id2 }]` |
| q3 | sum v1 mean v3 by id3 | `x[select { v1 = sum(v1), v3 = mean(v3) }, by id3]` |
| q4 | mean v1:v3 by id4 | `x[select { v1 = mean(v1), v2 = mean(v2), v3 = mean(v3) }, by id4]` |
| q5 | sum v1:v3 by id6 | `x[select { v1 = sum(v1), v2 = sum(v2), v3 = sum(v3) }, by id6]` |
| q6 | median v3 sd v3 by id4 id5 | `x[select { median_v3 = median(v3), sd_v3 = std(v3) }, by { id4, id5 }]` |
| q7 | max v1 - min v2 by id3 | `x[select { range_v1_v2 = max(v1) - min(v2) }, by id3]` |
| q8 | largest two v3 by id6 | `x[order { v3 desc }, head 2, by id6][select { id6, largest2_v3 = v3 }]` |
| q9 | regression v1 v2 by id2 id4 | `x[select { r2 = corr(v1, v2) }, by { id2, id4 }][update { r2 = r2 * r2 }]` |
| q10 | sum v3 count by id1:id6 | `x[select { v3 = sum(v3), count = count() }, by { id1, …, id6 }]` |

With NAs, q8 adds `filter is_not_null(v3)`, as upstream's solutions do. q9's
`corr` skips any row where either side is null, as the other engines' does.
The full query text is in `run.py`.

**Join** (`J1_<N>_NA_<NA%>_<sorted>` plus its small/medium/big right tables),
5 questions: small inner on int, medium inner on int, medium left on int,
medium inner on a string key, big inner on int. Colliding right columns are
renamed with `suffix { "", "_<table>" }`, so every engine produces the same
columns.

## Fairness

- **Upstream data.** `gen_data.sh` runs the upstream R generators, vendored
  unmodified in `datagen/` (MPL-2.0). Every engine reads the same CSV.
- **Upstream engine setup.** Polars and DuckDB use the schemas from the
  upstream solutions. For Polars that is Int32 ids and values, string keys cast
  to Categorical, Float32 join values, and the streaming engine. For DuckDB it
  is ENUM keys where upstream uses them and FLOAT `v3`. Ibex reads the CSV with
  its own reader and picks its own types. The narrower upstream types move
  fewer bytes than Ibex's Int64/Float64, so they favour the other engines.
- **Groupby string keys are Categorical in Ibex too.** Upstream casts `id1`,
  `id2` and `id3` to Categorical for Polars after loading, outside the timed
  section. Ibex is given the same instruction in its own syntax,
  `csv::read(path, schema = "id1:cat,id2:cat,id3:cat")`. Left to itself, the
  reader keeps a column with more distinct values than 10% of its rows as a
  String. At K=2e0 that is `id3` (5M levels), and until 2026-10-15 Ibex grouped
  it as a String while Polars grouped a prebuilt dictionary. DuckDB's upstream
  schema leaves `id3` as VARCHAR.
- **Loading is not timed.** Data is loaded into memory once per process. Each
  question is timed for 1 warmup and 5 timed iterations, and the result is
  fully materialised: an Ibex `let`, a Polars `DataFrame`, a DuckDB TEMP table.
  We report the minimum, with the median in the TSV.
- **Checked, not claimed.** Every engine's answer goes through upstream's "chk"
  sums plus the row count. Engines must agree to within 1e-5 relative, which
  is DuckDB's Float32 `v3`. Otherwise the row is `MISMATCH` and the script
  exits non-zero.
- **Same cores.** `--threads N` puts each engine under `taskset -c 0-(N-1)`
  and sets `IBEX_CORES`, `POLARS_MAX_THREADS` and DuckDB `threads` to N. The
  default of 8 is the cap for this box (see MEASURING.md). Run `--threads 1`
  to compare per-core efficiency.
- Ibex is timed by the REPL's `:timing`, which covers parse, plan and execute.
  Polars and DuckDB are timed with `time.perf_counter` around the call.

## Running

```sh
benchmarking/h2o/gen_data.sh groupby 1e7 1e2 0 0     # -> benchmarking/data/h2o/G1_1e7_1e2_0_0.csv
benchmarking/h2o/gen_data.sh join    1e7 0 0         # -> J1_1e7_NA_0_0.csv + small/medium/big
uv run --project . benchmarking/h2o/run.py G1_1e7_1e2_0_0 J1_1e7_NA_0_0 [--threads 8]
```

The standard 1e7 set is `G1_1e7_1e2_0_0`, `G1_1e7_1e1_0_0`, `G1_1e7_2e0_0_0`
(5M-group `id3`), `G1_1e7_1e2_5_0` (5% NAs), `G1_1e7_1e2_0_1` (sorted) and
`J1_1e7_NA_0_0`. The 1e8 data needs ~5 GB per engine in memory, which does not
fit on the dev box. Run it on AWS instead.

Results go to `results/h2o_<stamp>_<T>t.tsv`, one row per (data, question,
engine), with every timed sample kept.

## Results: 2026-10-15, dev box, 8 cores, 1e7 rows

Ibex `37d5b91e`, Polars 2.0.0 (streaming), DuckDB 1.5.4, all three engines in
the same sitting, with the string keys Categorical for Polars and Ibex (see
Fairness). The ratio is Ibex/other on min ms, so below 1 means Ibex is faster.
Raw data is in `results/h2o_20261015T092842_8t.tsv`. All 66 cells completed
and passed the cross-engine check. The 2026-10-14 column is the previous run
(`results/h2o_20261014T184344_8t.tsv`), with Ibex reading `id3` and the join's
`id6` as String.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars total | ibex/polars 2026-10-14 |
|---|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1.35 | 0.26 | 1.21 | 1.58 |
| G1_1e7_1e2_0_0 | 0.79 | 0.69 | 1.06 | 0.94 |
| G1_1e7_1e2_5_0 (NAs) | 0.64 | 0.68 | 1.72 | 1.24 |
| G1_1e7_1e2_0_1 (sorted) | 0.60 | 0.58 | 0.90 | 0.77 |
| G1_1e7_1e1_0_0 | 0.77 | 0.84 | 0.69 | 1.61 |
| G1_1e7_2e0_0_0 | 0.79 | 0.98 | 0.45 | 1.68 |

Engine changes since 2026-10-14:
- a null cell in the dense integer-key array, and nullable keys in parallel
  group numbering (q4 and q6 with NAs);
- owned-range parallel accumulation and parallel group numbering at high
  cardinality (q3, q5, q7);
- string group keys kept in arenas, as views;
- grouped `cov(x, y)` and `corr(x, y)` (q9);
- packed multi-column keys that hold a String or a 5M-level Categorical (q10).

- **Groupby:** Ibex leads both engines on the geomean of every groupby set:
  0.60–0.79 against Polars, 0.58–0.98 against DuckDB.
- **q10 (~1e7 groups)** is the largest remaining loss: 1.6–2.2× Polars
  (379–442 ms against 203–237 ms). With NAs it is 1314 ms (4.9×): a key
  column with nulls leaves the packed-key path for the generic one.
- **q6** (median by `id4, id5`) is 1.67× Polars at K=1e2 and **q5** (`by id6`)
  1.37× at K=2e0. DuckDB still leads q8 (per-group top-k) at K=1e1 and 2e0
  (1.9× and 3.3×), though Ibex is 0.25–0.48× Polars there.
- **Join:** the int-key inner joins q1 and q2 are 1.6–2.1× Polars; Ibex stays
  well ahead of DuckDB (0.26).

## Results: 2026-10-15, dev box, 1 core, 1e7 rows

Same build and engine versions, same sitting. Raw data is in
`results/h2o_20261015T093311_1t.tsv`. All 66 cells completed and passed the
cross-engine check. The 2026-10-14 column is `results/h2o_20261014T184755_1t.tsv`.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars total | ibex/polars 2026-10-14 |
|---|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1.17 | 0.22 | 1.08 | 1.33 |
| G1_1e7_1e2_0_0 | 0.83 | 0.89 | 1.73 | 0.75 |
| G1_1e7_1e2_5_0 (NAs) | 0.71 | 0.82 | 1.59 | 0.81 |
| G1_1e7_1e2_0_1 (sorted) | 0.87 | 0.89 | 1.73 | 0.78 |
| G1_1e7_1e1_0_0 | 0.79 | 0.92 | 1.63 | 0.78 |
| G1_1e7_2e0_0_0 | 0.88 | 0.92 | 1.14 | 1.11 |

- Ibex leads both engines on every groupby geomean at 1 core too. The totals
  stay above 1 because q10 (3.1–4.7× Polars) dominates them.
- **q9 regressed at 1 core.** Grouped `corr` made it 2–3× faster at 8 cores
  (14–28 ms) but slower at 1 core than the six-sum formula it replaced: 390
  against 198 ms at K=1e2, 220 against 152 ms at K=2e0. That is 2.6–3.8×
  Polars, and it is most of why the K=1e2 and sorted geomeans read higher
  than on 2026-10-14. Not yet investigated.
