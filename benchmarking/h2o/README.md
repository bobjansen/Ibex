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

## Results: 2026-10-16, AWS r7i.4xlarge, 1 and 8 physical cores, 1e7 rows

Ibex `9c99d03b`, Polars 2.0.0 (streaming), DuckDB 1.5.4, on one clean
r7i.4xlarge (Xeon Platinum 8488C, 128 GiB), with SMT off and every engine
pinned to the same 1 or 8 physical cores. All 110 (data, question, core-count)
comparisons completed and passed the cross-engine row-count/checksum checks
(330 engine result rows). This is
the run shown on the website; the artifact is
`results/h2o_aws_20261016T074103.tar.gz`.

Loading is excluded. Each question fully materialises its result, with 1 warmup
and 5 timed iterations; these ratios use the minimum times. Below 1 means
Ibex is faster. Geometric means weight each question equally in ratio space;
totals compare the sum of question times, so longer questions carry more weight.
Both wins and losses can appear smaller in either summary than individually.

| data | cores | ibex/polars geomean | ibex/polars total | ibex/duckdb geomean | ibex/duckdb total |
|---|---:|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1 | 0.86 | 0.84 | 0.15 | 0.22 |
| G1_1e7_1e2_0_0 | 1 | 0.58 | 0.74 | 0.63 | 0.60 |
| G1_1e7_1e2_5_0 (NAs) | 1 | 0.51 | 0.66 | 0.59 | 0.58 |
| G1_1e7_1e2_0_1 (sorted) | 1 | 0.61 | 0.77 | 0.64 | 0.60 |
| G1_1e7_1e1_0_0 | 1 | 0.56 | 0.73 | 0.66 | 0.65 |
| G1_1e7_2e0_0_0 | 1 | 0.63 | 0.64 | 0.65 | 0.71 |
| J1_1e7_NA_0_0 (join) | 8 | 0.99 | 1.40 | 0.19 | 0.37 |
| G1_1e7_1e2_0_0 | 8 | 0.74 | 0.85 | 0.61 | 0.69 |
| G1_1e7_1e2_5_0 (NAs) | 8 | 0.73 | 0.89 | 0.67 | 0.76 |
| G1_1e7_1e2_0_1 (sorted) | 8 | 0.78 | 0.90 | 0.60 | 0.70 |
| G1_1e7_1e1_0_0 | 8 | 0.88 | 0.53 | 0.92 | 0.79 |
| G1_1e7_2e0_0_0 | 8 | 0.96 | 0.34 | 1.10 | 1.27 |

- **1 core:** Ibex leads both engines on every dataset by both summaries.
- **8 cores:** every groupby dataset leads Polars by both summaries. Against
  DuckDB, every groupby dataset leads except K=2e0. Join q1–q4 beat Polars,
  while the big join q5 takes 334 ms against 197 ms (1.69×), putting the
  join total at 1.40 despite a 0.99 geometric mean.
- **Wins and losses within a dataset:** at K=100, 8 cores, q1 takes
  2.505 / 7.916 / 3.266 ms (Ibex / Polars / DuckDB), while q6 takes
  162.82 / 105.69 / 111.15 ms. At K=2, q8 takes
  870.48 / 4167.26 / 211.11 ms: a substantial win against Polars and a
  substantial loss against DuckDB.
- **q10 improved:** the packed categorical-key changes bring its ratio to
  Polars to 0.83–0.94 at 1 core and 1.36–1.54 at 8 cores, from
  1.10–1.92 and 2.22–3.16 in the previous AWS run. These are ratios from
  separate cross-engine runs, rather than an interleaved causal A/B.

To reproduce the configuration, check out `9c99d03b` in a separate clone,
complete the [AWS setup](../aws/README.md#1-one-time-setup), then run:

```sh
./benchmarking/aws/run-h2o.sh --on-demand --type r7i.4xlarge \
    --threads-per-core 1 --cores 1,8 --warmup 1 --iters 5
```

The runner launches a paid EC2 instance and downloads the result archive.
See the [publication snapshot and reproduction commands](../aws/README.md#published-benchmark-snapshot)
for all three suites.

## Previous results: 2026-10-15, AWS r7i.4xlarge, 1 and 8 physical cores, 1e7 rows

Ibex `5ee22b2b`, Polars 2.0.0 (streaming), DuckDB 1.5.4, on one clean
r7i.4xlarge (Xeon Platinum 8488C) with SMT off, so 8 physical cores; every
engine pinned to the same cores. Run with `benchmarking/aws/run-h2o.sh
--on-demand`; the artifact is `results/h2o_aws_20261015T151409.tar.gz`. All
132 cells completed and passed the cross-engine check. Superseded on the
website by the 2026-10-16 run above.

| data | 8c ibex/polars geomean | 8c ibex/duckdb geomean | 8c ibex/polars total | 1c ibex/polars geomean | 1c ibex/duckdb geomean | 1c ibex/polars total |
|---|---:|---:|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1.00 | 0.19 | 1.47 | 0.88 | 0.15 | 0.88 |
| G1_1e7_1e2_0_0 | 0.85 | 0.67 | 1.12 | 0.64 | 0.69 | 0.90 |
| G1_1e7_1e2_5_0 (NAs) | 0.79 | 0.74 | 1.03 | 0.51 | 0.63 | 0.72 |
| G1_1e7_1e2_0_1 (sorted) | 0.87 | 0.64 | 1.12 | 0.65 | 0.70 | 0.89 |
| G1_1e7_1e1_0_0 | 0.94 | 0.98 | 0.66 | 0.63 | 0.70 | 0.98 |
| G1_1e7_2e0_0_0 | 1.07 | 1.24 | 0.43 | 0.66 | 0.70 | 0.77 |

- **1 core:** Ibex leads both engines on every geomean, groupby and join.
- **8 cores:** Ibex scales less than Polars from 1 to 8 cores, as on PDS-H,
  and the dev box (below) flatters Ibex above ~4 cores. Groupby leads Polars
  on the geomean of every set but K=2e0 (1.07), and DuckDB on all but K=2e0
  (1.24).
- **q10** is the largest loss: 2.2–3.2× Polars and 1.3–1.75× DuckDB at 8 cores
  (417–549 ms against 169–225 ms), 1.1–1.9× Polars at 1 core.
- **Other 8-core losses:** q5 (`by id6`) up to 2.1× Polars at K=2e0, q6 (median)
  1.2–1.8×, q7 (`by id3`) ~1.4×, and the big join q5 1.81× (397 against
  219 ms), which is what puts the join total at 1.47 while q1–q4 are
  0.77–0.91×.

## Results: 2026-10-15, dev box, 8 cores, 1e7 rows

Ibex `6f51208e`, Polars 2.0.0 (streaming), DuckDB 1.5.4, all three engines in
the same sitting, with the string keys Categorical for Polars and Ibex (see
Fairness). The ratio is Ibex/other on min ms, so below 1 means Ibex is faster.
Raw data is in `results/h2o_20261015T131020_8t.tsv`. All 66 cells completed
and passed the cross-engine check. The join row is a later run at `ce43503f`,
after the join changes below (`results/h2o_20261015T145615_8t.tsv`). The
2026-10-14 column is the previous run
(`results/h2o_20261014T184344_8t.tsv`), with Ibex reading `id3` and the join's
`id6` as String.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars total | ibex/polars 2026-10-14 |
|---|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 0.94 | 0.24 | 1.01 | 1.58 |
| G1_1e7_1e2_0_0 | 0.77 | 0.65 | 0.95 | 0.94 |
| G1_1e7_1e2_5_0 (NAs) | 0.64 | 0.60 | 0.89 | 1.24 |
| G1_1e7_1e2_0_1 (sorted) | 0.69 | 0.59 | 0.93 | 0.77 |
| G1_1e7_1e1_0_0 | 0.76 | 0.88 | 0.70 | 1.61 |
| G1_1e7_2e0_0_0 | 0.77 | 0.98 | 0.47 | 1.68 |

Engine changes since 2026-10-14:
- a null cell in the dense integer-key array, and nullable keys in parallel
  group numbering (q4 and q6 with NAs);
- owned-range parallel accumulation and parallel group numbering at high
  cardinality (q3, q5, q7);
- string group keys kept in arenas, as views;
- grouped `cov(x, y)` and `corr(x, y)` (q9);
- packed multi-column keys that hold a String, a 5M-level Categorical, or a
  nullable column (q10, including the NA data);
- join: a direct-address probe for a small, dense integer build side, and an
  output gather that reads each index range once for every column
  (`8e774bed`, `ce43503f`).

- **Groupby:** Ibex leads both engines on the geomean of every groupby set:
  0.64–0.77 against Polars, 0.59–0.98 against DuckDB. Every groupby total
  is below 1 against Polars; against DuckDB, K=2e0's is 1.35, nearly all of
  it q8.
- **q10 (~1e7 groups)** is the largest remaining loss: 1.5–2.0× Polars
  (318–473 ms against 211–255 ms), and it trails DuckDB too (1.13–1.53×).
- **Smaller losses:** q5 (`by id6`) is 1.12–1.29× Polars, q6 (median) up to
  1.13×, and q9 at K=2e0 1.32×, where it runs 16 ms against 12. DuckDB leads
  q8 (per-group top-k) at K=1e1 and 2e0 (2.1× and 3.6×), though Ibex is
  0.27–0.39× Polars there.
- **Join:** level with Polars. q1, q2 and q4 run 32–34 ms against Polars'
  32–35 ms, q3 (left join) is 0.69×, and q5 (a 1e7-row build side) 1.07×.
  Polars' own times vary by ~15% between runs on this box (q1 26–33 ms), so
  the 8-core join geomean has read 0.94–1.04 across clean runs. Ibex leads
  DuckDB (0.24).

## Results: 2026-10-15, dev box, 1 core, 1e7 rows

Same build and engine versions, same sitting. Raw data is in
`results/h2o_20261015T131421_1t.tsv`, and for the join row, run at `ce43503f`,
`results/h2o_20261015T145720_1t.tsv`. All 66 cells completed and passed the
cross-engine check. The 2026-10-14 column is `results/h2o_20261014T184755_1t.tsv`.

| data | ibex/polars geomean | ibex/duckdb geomean | ibex/polars total | ibex/polars 2026-10-14 |
|---|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 0.93 | 0.17 | 0.97 | 1.33 |
| G1_1e7_1e2_0_0 | 0.57 | 0.61 | 0.82 | 0.75 |
| G1_1e7_1e2_5_0 (NAs) | 0.50 | 0.58 | 0.71 | 0.81 |
| G1_1e7_1e2_0_1 (sorted) | 0.58 | 0.63 | 0.82 | 0.78 |
| G1_1e7_1e1_0_0 | 0.59 | 0.65 | 0.92 | 0.78 |
| G1_1e7_2e0_0_0 | 0.64 | 0.70 | 0.76 | 1.11 |

- Ibex leads both engines on every groupby set at 1 core, by geomean and by
  total.
- **q10** is 1.1–1.3× Polars (1.8× at K=2e0) and 0.60–0.87× DuckDB. One core
  now partitions its group discovery when the key is high-cardinality and
  unclustered, which keeps each partition's table near cache; it was
  3.0–4.5× Polars before.
- **q9** is 0.4–0.6× Polars. Grouped `corr` first ran it at 220–390 ms on one
  core, because `aggregate_table` grouped integer and code keys by cell only
  with two or more workers; with that gate removed it is 45–75 ms.
- **Join:** Ibex leads Polars on four of five questions (0.86–0.98×; q3 1.05×)
  and is 0.17× DuckDB.
