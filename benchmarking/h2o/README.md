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
  its own reader and picks its own types and dictionary encoding. The narrower
  upstream types move fewer bytes than Ibex's Int64/Float64, so they favour
  the other engines.
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

## Results: 2026-10-14, dev box, 8 cores, 1e7 rows

Ibex `eeb73465`, Polars 2.0.0 (streaming), DuckDB 1.5.4, all three engines in
the same sitting. The ratio is Ibex/other on min ms, so below 1 means Ibex is
faster. Raw data is in `results/h2o_20261014T184344_8t.tsv`. All 66 cells
completed (none ran out of memory) and passed the cross-engine check. The
2026-10-13 column is the first run (`results/h2o_20261013T074652_8t.tsv`),
before the fixes listed below.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars 2026-10-13 |
|---|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1.58 | 0.38 | 1.67 |
| G1_1e7_1e2_0_0 | 0.94 | 0.86 | 1.48 |
| G1_1e7_1e2_5_0 (NAs) | 1.24 | 1.10 | 1.44 |
| G1_1e7_1e2_0_1 (sorted) | 0.77 | 0.74 | 1.42 |
| G1_1e7_1e1_0_0 | 1.61 | 1.64 | 1.96 (9 q) |
| G1_1e7_2e0_0_0 | 1.68 | 2.36 | 2.33 (8 q) |

Fixes since the first run:
- dense array for a narrow-span integer key (`6b13dcb5`, q4);
- parallel group numbering for materialized integer keys (`49694f56`, q6);
- partitioned flat heaps for grouped top-k (`6bbae675`, q8);
- data-sized packed keys and range emission (`c797512e`, `adc2fea6`, q10);
- in-memory tables cut to the columns a query reads (`df1a2495` and
  follow-ups).

- **Wins:** low- and medium-cardinality keys. At K=1e2, q1–q5, q7 and q8 are
  0.37–1.25× Polars. q8 (per-group top-k) went from 7–8× Polars to 0.27–0.38×
  at every K, though DuckDB still leads it at K=1e1 and 2e0 (1.8× and 3.7×).
- **q10 (~1e7 groups)** is the largest remaining loss: 3.2–6.5× Polars. Most of
  its time is spent assigning groups. At K=2e0 the `id3` key (5M levels) is
  read as a String, not Categorical.
- **High cardinality** (q3, q5, q7 at 1e6–5e6 groups) is 2.6–7× Polars.
- **q9** is 2.4–4.8×, because there is no grouped `corr` aggregate.
- **Nulls disable the q4 and q6 fast paths.** With 5% NAs, q4 is 70.7 ms
  against 10.7 ms without, and q6 432 ms against 115 ms.
- **Join** is unchanged: it loses to Polars on the int-key inner joins (q1, q2:
  2.3–2.5×) and stays well ahead of DuckDB.

## Results: 2026-10-14, dev box, 1 core, 1e7 rows

Same build and engine versions, same sitting. Raw data is in
`results/h2o_20261014T184755_1t.tsv`. All 66 cells completed and verified.
The 2026-10-13 column is `results/h2o_20261013T080556_1t.tsv`.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars total | ibex/polars 2026-10-13 |
|---|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1.33 | 0.26 | 1.17 | 1.33 |
| G1_1e7_1e2_0_0 | 0.75 | 0.81 | 1.61 | 1.23 |
| G1_1e7_1e1_0_0 | 0.78 | 0.89 | 1.57 | 1.19 |
| G1_1e7_2e0_0_0 | 1.11 | 1.25 | 1.36 | 1.50 |
| G1_1e7_1e2_5_0 (NAs) | 0.81 | 0.96 | 1.70 | 0.98 |
| G1_1e7_1e2_0_1 (sorted) | 0.78 | 0.82 | 1.64 | 1.26 |

- Per core, Ibex wins every groupby set except K=2e0. The totals stay above 1
  because q10 (3–4× Polars) dominates them.
- The 8-core geomeans are worse than the 1-core ones at K=1e1 and 2e0
  (1.61 vs 0.78, 1.68 vs 1.11). The high-cardinality questions (q3, q5, q7,
  q10) scale poorly: q5 at K=1e1 runs 226 ms at 1 core and 213 ms at 8.
