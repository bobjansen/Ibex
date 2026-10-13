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
| q9 | regression v1 v2 by id2 id4 | r² from six grouped sums (no grouped `corr` aggregate yet) |
| q10 | sum v3 count by id1:id6 | `x[select { v3 = sum(v3), count = count() }, by { id1, …, id6 }]` |

With NAs, q8 and q9 add `filter is_not_null(…)` to match what the other
engines do implicitly. The full query text is in `run.py`.

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

## Results: 2026-10-13, dev box, 8 cores, 1e7 rows

Ibex `1ef056f8`+, Polars 2.0.0 (streaming), DuckDB 1.5.4. The ratio is
Ibex/other on min ms, so below 1 means Ibex is faster. Raw data is in
`results/h2o_20261013T074652_8t.tsv`. All answers that completed passed the
cross-engine check.

| data | ibex/polars geomean | ibex/duckdb geomean | note |
|---|---:|---:|---|
| J1_1e7_NA_0_0 (join) | 1.67 | 0.38 | loses to Polars on all but the left join |
| G1_1e7_1e2_0_0 | 1.48 | 1.16 | totals 5.0× / 4.0×, dominated by q8 + q10 |
| G1_1e7_1e2_5_0 (NAs) | 1.44 | 1.40 | |
| G1_1e7_1e2_0_1 (sorted) | 1.42 | 1.32 | |
| G1_1e7_1e1_0_0 | 1.96 (9 q) | 2.51 (9 q) | q10 OOM in the main run; see re-run below |
| G1_1e7_2e0_0_0 | 2.33 (8 q) | 2.54 (8 q) | q8, q10 OOM in the main run; see re-run below |

- **Wins:** low- and medium-cardinality *string* keys. q1, q2, q3 and q7 at
  K=1e2 run 0.26–0.65× Polars and 0.18–0.38× DuckDB.
- **High cardinality loses, as expected.** q3 (by id3) goes from 0.54× Polars
  at 1e5 groups to 2.06× at 1e6 and 6.8× at 5e6. q10 (~1e7 groups) is 6×
  slower at K=1e2.
- **Integer keys lose even at 100 groups.** q4 (mean v1:v3 by id4) is 2.5×
  Polars and 4.6× DuckDB. The same shape on a categorical key wins.
- **Per-group top-k is the worst case.** q8 (`order, head 2, by id6`) does a
  global sort and runs 7–8× Polars and 12–41× DuckDB.
- q6 (median + sd) is 3.5×. q9 (r² via sums, no grouped `corr`) is 1.8×.
- **Memory:** q10 at ~1e7 groups peaks at ~5.3–5.9 GB RSS, and q8 at K=2e0
  above 6.3 GB. Polars and DuckDB stay at ~2.5 GB, including the loaded data.
  With ~6 GB free on this box, the three missing cells were OOM-killed. That
  is not a leak: RSS plateaus across iterations. Re-run with ~13 GB free
  (`results/h2o_20261013_rerun_8t.tsv`, same day, all three engines fresh):

  | data | q | ibex ms | polars ms | duckdb ms |
  |---|---|---:|---:|---:|
  | G1_1e7_1e1_0_0 | q8 | 8613 | 983 | 153 |
  | G1_1e7_1e1_0_0 | q10 | 1992 | 210 | 245 |
  | G1_1e7_2e0_0_0 | q8 | 22070 | 3143 | 191 |
  | G1_1e7_2e0_0_0 | q10 | 1182 | 212 | 246 |
