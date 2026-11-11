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
- **Upstream schemas, resident input adaptation.** Polars and DuckDB use the schemas from the
  upstream solutions. For Polars that is Int32 ids and values, string keys cast
  to Categorical, Float32 join values, and the streaming engine. For DuckDB it
  is ENUM keys where upstream uses them and FLOAT `v3`. Ibex reads the CSV with
  its own reader and picks its own types. The narrower upstream types move
  fewer bytes than Ibex's Int64/Float64, so they favour the other engines.
  DuckDB explicitly sets `storage_compatibility_version='latest'`, as upstream
  does. The pinned upstream Polars scripts scan memory-mapped IPC files; this
  harness instead uses resident DataFrames for an in-memory comparison with
  resident Ibex and DuckDB inputs. It does not reproduce upstream's IPC scan
  path. These choices are recorded in each result's configuration.
- **Explicit null semantics.** Upstream CSV empty fields are null in all
  readers, including categorical keys. Bounded fixtures use a named null token
  so that valid empty string keys can also be tested. Polars sums explicitly
  return null for an all-null group, matching Ibex and DuckDB; upstream's bare
  Polars sum returns zero in that case. This work is included in timing.
- **Groupby string keys are Categorical in Ibex too.** Upstream casts `id1`,
  `id2` and `id3` to Categorical for Polars after loading, outside the timed
  section. Ibex is given the same instruction in its own syntax,
  `csv::read(path, schema = "id1:cat,id2:cat,id3:cat")`. Left to itself, the
  reader keeps a column with more distinct values than 10% of its rows as a
  String. At K=2e0 that is `id3` (5M levels), and until 2026-10-15 Ibex grouped
  it as a String while Polars grouped a prebuilt dictionary. DuckDB's upstream
  schema leaves `id3` as VARCHAR.
- **Loading and cleanup are not timed.** One process per dataset and engine
  loads its inputs once and runs all questions. Each preceding answer is
  released before starting the next iteration's clock. Ibex's parent waits for
  complete answer validation before sending the next question. Each
  question is timed for 1 warmup and 5 timed iterations, and the result is
  fully materialised: an Ibex `let`, a Polars `DataFrame`, a DuckDB TEMP table.
  We report the minimum, with the median in the TSV.
- **Full bounded answers before timing.** All questions run on deterministic
  768-row fixtures at one and two cores (one core for a single-core run).
  The fixtures include nulls, quoted Unicode and empty keys, sparse negative
  integer keys, unmatched joins and duplicate build-side keys. Every output
  cell and duplicate row must agree; output order is unspecified. These are
  validation fixtures, not replacements for the upstream timed data.
- **Scale answers outside timing.** Every output column contributes to an
  order-independent fingerprint. Integers and strings are exact; nulls, NaN
  and infinities are distinct. Two 256-bit multiset accumulators include the
  complete exact row, using four seeded 64-bit native row hashes. Float sums, squares, cross-column products and two
  weighted moments are bound to that row in 64 buckets. Float32-derived
  fields use 1e-6 relative/absolute tolerance; other floats use 1e-9 relative
  and 1e-10 absolute tolerance. These fingerprints are probabilistic screens,
  not complete proofs of scale answers. Upstream `chk` sums remain diagnostic.
  Ibex exports through a pipe after timing, avoiding a scale-sized temporary
  CSV. Validation adds wall time but does not enter reported samples.
- **Failed runs stay pending.** Missing, duplicate, failed or mismatched engine
  results make the whole comparison fail. Diagnostic artifacts remain under
  `results/checks_<run-id>_*/`; accepted TSVs are replaced only after every
  gate passes. Each accepted TSV has a `.tsv.checks.json` sidecar tied to its
  contents by SHA-256 and a run identifier. Keep the sidecar with the TSV.
  Website generation requires all three engines and all questions for each
  dataset. AWS acceptance also requires every requested core count and dataset.
- **Executable provenance.** First run `provenance.py --build` below. It
  completes a Release build and records runtime source hashes, the executable,
  CSV plugin, plugin declarations and CMake configuration. The runner checks
  these before and after measurement. TSVs identify Ibex by executable SHA-256;
  sidecars retain the build checkout, source changes and harness hashes.
  Stale, replaced, Debug or unrecorded binaries cannot produce accepted results.
- **Same CPU budget.** `--threads N` pins every engine to the first N allowed
  logical CPUs and sets `IBEX_CORES`, `POLARS_MAX_THREADS` and DuckDB `threads`
  to N. Logical CPUs may be SMT siblings: eight threads on the local box use
  four physical cores. For physical-core comparisons, run under an outer
  `taskset` selecting one logical CPU per physical core, or disable SMT as the
  AWS launcher does. Run `--threads 1` to compare single-thread efficiency.
- Ibex is timed by the REPL's `:timing`, which covers parse, plan and execute.
  Polars and DuckDB are timed with `time.perf_counter` around the call.

## Running

```sh
benchmarking/h2o/gen_data.sh groupby 1e7 1e2 0 0     # -> benchmarking/data/h2o/G1_1e7_1e2_0_0.csv
benchmarking/h2o/gen_data.sh join    1e7 0 0         # -> J1_1e7_NA_0_0.csv + small/medium/big
python3 benchmarking/h2o/provenance.py --build-dir build-release --build
uv run --project . benchmarking/h2o/run.py G1_1e7_1e2_0_0 J1_1e7_NA_0_0 [--threads 8]
```

The standard 1e7 set is `G1_1e7_1e2_0_0`, `G1_1e7_1e1_0_0`, `G1_1e7_2e0_0_0`
(5M-group `id3`), `G1_1e7_1e2_5_0` (5% NAs), `G1_1e7_1e2_0_1` (sorted) and
`J1_1e7_NA_0_0`. The 1e8 data needs ~5 GB per engine in memory, which does not
fit on the dev box. Run it on AWS instead.

Results go to `results/h2o_<stamp>_<T>t.tsv`, one row per (data, question,
engine), with every timed sample kept.

The 2026-11-08 AWS run and the 2026-11-07 local run passed full-answer
validation. The AWS TSVs in the sections after them predate it. They cannot
be used as inputs to the current website generator; their original checksum
passes are not upgraded retroactively.

## Results: 2026-11-08, AWS r7i.4xlarge, 1 and 8 physical cores, 1e7 rows

Ibex `23436ea7`, Polars 2.0.0 (streaming), DuckDB 2.0.0, on one clean
r7i.4xlarge (Xeon Platinum 8488C, 128 GiB). SMT was off and every engine
was pinned to the same 1 or 8 physical cores. All 110 (data, question,
core-count) comparisons passed full answer validation (`h2o-answers-v2`)
with all three engines. The artifact is
`results/h2o_aws_20261108T134649.tar.gz`, with run ids `2b9ec8b0…` (1 core)
and `10df7b14…` (8 cores).

Loading is excluded, and each question fully materialises its result. There
was 1 warmup and 5 timed iterations, and the ratios use the minimum times.
Below 1 means Ibex is faster.

| data | cores | ibex/polars geomean | ibex/polars total | ibex/duckdb geomean | ibex/duckdb total |
|---|---:|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1 | 0.69 | 0.49 | 0.18 | 0.19 |
| G1_1e7_1e2_0_0 | 1 | 0.50 | 0.56 | 0.52 | 0.53 |
| G1_1e7_1e2_5_0 (NAs) | 1 | 0.48 | 0.54 | 0.57 | 0.55 |
| G1_1e7_1e2_0_1 (sorted) | 1 | 0.50 | 0.56 | 0.52 | 0.49 |
| G1_1e7_1e1_0_0 | 1 | 0.48 | 0.51 | 0.58 | 0.58 |
| G1_1e7_2e0_0_0 | 1 | 0.51 | 0.40 | 0.61 | 0.63 |
| J1_1e7_NA_0_0 (join) | 8 | 0.86 | 0.81 | 0.24 | 0.31 |
| G1_1e7_1e2_0_0 | 8 | 0.64 | 0.63 | 0.75 | 0.78 |
| G1_1e7_1e2_5_0 (NAs) | 8 | 0.68 | 0.63 | 0.85 | 0.80 |
| G1_1e7_1e2_0_1 (sorted) | 8 | 0.63 | 0.63 | 0.73 | 0.79 |
| G1_1e7_1e1_0_0 | 8 | 0.60 | 0.31 | 0.66 | 0.66 |
| G1_1e7_2e0_0_0 | 8 | 0.69 | 0.16 | 0.85 | 0.82 |

- **Every dataset leads both engines by both summaries, at 1 and 8 cores.**
  On 2026-10-16, Ibex trailed DuckDB at 8 cores on K=2e0 (1.10 / 1.27), and
  the join total trailed Polars (1.40).
- **1 core:** Ibex is faster than Polars on all 55 questions, and faster than
  DuckDB on 54. The exception is K=2 q8 (1212 / 1159 ms, 1.05×).
- **8 cores:** Ibex is faster than Polars on 42 questions and faster than
  DuckDB on 43. The losses against Polars:
  - q10 (six keys, ~1e7 groups) is 1.04–1.17× on every groupby dataset. On
    2026-10-16 it was 1.36–1.54×.
  - q7 is 1.18–1.31× at K=100.
  - At K=2, q9 is 1.33× (22 / 16 ms), q4 1.19× and q5 1.19×.

  The losses against DuckDB:
  - q8 (per-group top-2) is 1.18–1.50× on every groupby dataset, while it
    beats Polars 3.2–17×.
  - q4 is up to 1.53× (K=2, 17 / 11 ms).
  - At K=2, q9 is 1.41× and q2 1.29×.
- **Join:** the big join q5 now takes 157 ms against 204 ms for Polars
  (0.77×). On 2026-10-16 it took 334 against 197 ms (1.69×). q1–q4 are
  0.79–0.98× Polars at 8 cores.
- **AWS against the dev box:** these ratios are close to the 2026-11-07 local
  ones at 8 cores (0.60–0.69 against 0.56–0.65 vs Polars), but above them at
  1 core (0.48–0.51 against 0.40–0.47). The two runs differ in box, CPU and
  commit, and the commits only removed switches that were off by default.

To reproduce the configuration, check out `23436ea7` in a separate clone,
complete the [AWS setup](../aws/README.md#1-one-time-setup), then run:

```sh
./benchmarking/aws/run-h2o.sh --on-demand --type r7i.4xlarge \
    --threads-per-core 1 --cores 1,8 --warmup 1 --iters 5
```

The runner launches a paid EC2 instance and downloads the result archive.

## Results: 2026-11-07, local i7-13700, 1 and 8 threads, 1e7 rows

Ibex `5a5e6941`, Polars 2.0.0 (streaming), DuckDB 2.0.0, on the development
box (i7-13700 at a fixed 3.2 GHz). Eight threads ran on CPUs 0,2,…,14, one
logical CPU per performance core, and one thread on CPU 2; every engine was
pinned to the same CPUs. All 110 (data, question, thread-count) comparisons
passed full answer validation (`h2o-answers-v2`) with all three engines. The
results are `results/h2o_20261107e_local_{8t,1t}.tsv` and their
`.tsv.checks.json` sidecars (run ids `db31b7b2…` and `12e29c4d…`). This is a
local survey: the box and CPU differ from the AWS run above.

Loading is excluded, each question fully materialises its result, with 1
warmup and 5 timed iterations, and the ratios use the minimum times. Below 1
means Ibex is faster.

| data | threads | ibex/polars geomean | ibex/polars total | ibex/duckdb geomean | ibex/duckdb total |
|---|---:|---:|---:|---:|---:|
| J1_1e7_NA_0_0 (join) | 1 | 0.62 | 0.51 | 0.14 | 0.17 |
| G1_1e7_1e2_0_0 | 1 | 0.41 | 0.52 | 0.44 | 0.45 |
| G1_1e7_1e2_5_0 (NAs) | 1 | 0.47 | 0.53 | 0.56 | 0.52 |
| G1_1e7_1e2_0_1 (sorted) | 1 | 0.46 | 0.56 | 0.47 | 0.44 |
| G1_1e7_1e1_0_0 | 1 | 0.40 | 0.45 | 0.48 | 0.48 |
| G1_1e7_2e0_0_0 | 1 | 0.44 | 0.33 | 0.53 | 0.53 |
| J1_1e7_NA_0_0 (join) | 8 | 0.94 | 0.87 | 0.32 | 0.38 |
| G1_1e7_1e2_0_0 | 8 | 0.62 | 0.62 | 0.71 | 0.74 |
| G1_1e7_1e2_5_0 (NAs) | 8 | 0.65 | 0.63 | 0.80 | 0.78 |
| G1_1e7_1e2_0_1 (sorted) | 8 | 0.60 | 0.59 | 0.65 | 0.70 |
| G1_1e7_1e1_0_0 | 8 | 0.56 | 0.31 | 0.70 | 0.69 |
| G1_1e7_2e0_0_0 | 8 | 0.62 | 0.16 | 0.88 | 0.83 |

- **Every dataset leads both engines by both summaries, at 1 and 8 threads.**
- **1 thread:** Ibex is faster on all 55 questions against Polars and on 54
  against DuckDB. The exception is q8 on the NA data (240 / 232 ms), where
  the harness's `filter is_not_null(v3)` materialises 9.5M rows first.
  q8 elsewhere beats DuckDB 0.73–0.83× (K=2: 839 / 1013 ms; 1691 ms before
  `5d30bf00`) and Polars 2.6–6.3×.
- **8 threads:** q8 (per-group top-2) beats DuckDB at K=100 (69 / 83 ms,
  0.83×; sorted 0.90×), is level on the NA data (1.02×) and trails at
  K=10 (160 / 133 ms, 1.20×) and K=2 (267 / 176 ms, 1.52×; 743 ms before
  `9e39c37b`), while beating Polars 3.8–17×. q10 (six keys, ~1e7 groups) is
  1.00–1.18× Polars. K=2 q9 (corr by two keys, 4 groups, ~16 ms against
  Polars) read 1.67×; it is the noisiest cell, between 15 and 27 ms across
  today's runs.
- **Changes since the AWS run below** (2026-11-07): nullable Categorical keys
  stay on the dense code paths (`ae5cb95f`, `b1a3854b`; the harness has
  loaded empty fields as null keys since `da68fd4b`, which had pushed the NA
  dataset's 8-thread ratio to 1.49), `std()` beside a median folds per row
  range (`3d384612`, q6), a dense Int join key builds a direct-address table
  (`49d623e4`, join q5 311 → ~170 ms), parallel new-group null masks
  (`65068f95`), dense Int64 numbering across workers for up to 16M ids
  (`4b34bdfd`, `d9fad1ca`, q5), kept packed keys in partitioned discovery
  (`94215949`, q10), grouped top-k spread across workers (`9e39c37b`, q8)
  and, on one thread, found by key offset with prefetching (`5d30bf00`, q8).

## Previous results: 2026-10-16, AWS r7i.4xlarge, 1 and 8 physical cores, 1e7 rows

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
