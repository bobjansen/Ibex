# PDS-H SF-10 end-to-end check, 2026-11-10

`429b4e25` (Int32 branch, clean tree; runtime identical to `60ac018e`) runs
all 22 PDS-H queries at SF-10 against Polars 2 streaming and DuckDB in one
sitting. All 22 answers match Polars. Compared with the previous native run
(`a4dead37`, 2026-10-23), **every query is faster once the q21 query-text
change is accounted for** (see [q21](#q21-a-query-text-change-not-an-engine-regression)).

This compares two points about 290 commits apart, not the Int32 work alone.
It shows the branch carries no end-to-end regression and records where Ibex
stands. It does not attribute the gains to individual changes.

## Conditions

```bash
taskset -c 0,2,4,6,8,10,12,14 benchmarking/tpch/run_bench.sh \
    --sf 10 --cores 8 --warmup 1 --iters 5
```

- Native box (i7-13700), all cores locked at 3.2 GHz, the same as 10-23.
- One CPU per physical core. The outer `taskset` is required: without it,
  `run_bench.sh` pins CPUs 0-7, which are 4 physical cores plus their SMT
  siblings. A first attempt today made that mistake: every engine read 25-37%
  slower, and that archive (`20261110T181219Z_429b4e25_sf10`) is not used here.
- Data: polars-benchmark's own SF-10 tables (`pdsh_commit` 401908a, tpchgen
  2.0.2).
- Archives: `benchmarking/tpch/results/runs/20261110T183010Z_429b4e25_sf10`
  (now) and `20261023T194138Z_a4dead37-dirty_sf10` (before; the dirty file
  was only `.gitignore`).

The reference engines did not change, and they moved only -1.5% (Polars 8c),
-0.4% (Polars 1c), -0.1% (DuckDB 8c) and 0.0% (DuckDB 1c) in geomean. The box
is comparable, so the Ibex change is attributable to Ibex.

## Summary

| Geomean over 22 queries | 10-23 (`a4dead37`) | Now (`429b4e25`) |
| --- | ---: | ---: |
| Ibex now / Ibex 10-23, 8 cores | | 0.873 |
| Ibex now / Ibex 10-23, 1 core | | 0.877 |
| Ibex / Polars 2 streaming, 8 cores | 1.001 | **0.888** |
| Ibex / Polars 2 streaming, 1 core | 0.959 | **0.844** |
| Ibex / DuckDB, 8 cores | 0.840 | **0.734** |
| Ibex / DuckDB, 1 core | 0.883 | **0.774** |

Totals at 8 cores: Ibex 6.40 s, Polars 7.04 s, DuckDB 8.34 s.

## Per query

Mean of five timed runs. Ratios below 1 mean Ibex is faster.

| Query | Ibex 8c 10-23 ms | Ibex 8c now ms | Change | Ibex/Polars 8c | Ibex/DuckDB 8c | Ibex/Polars 1c | Ibex/DuckDB 1c |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| q01 | 599.4 | 563.7 | -5.9% | 1.03 | 1.24 | 0.95 | 1.22 |
| q02 | 61.0 | 50.3 | -17.5% | 0.75 | 0.46 | 0.94 | 0.47 |
| q03 | 389.6 | 336.7 | -13.6% | 0.97 | 0.74 | 0.91 | 0.75 |
| q04 | 226.9 | 170.0 | -25.1% | 1.05 | 0.80 | 1.05 | 1.09 |
| q05 | 446.5 | 405.4 | -9.2% | 1.09 | 0.90 | 0.95 | 0.94 |
| q06 | 219.2 | 192.4 | -12.3% | 0.89 | 0.82 | 0.88 | 0.90 |
| q07 | 466.8 | 412.7 | -11.6% | 1.01 | 0.89 | 0.90 | 0.89 |
| q08 | 502.0 | 446.6 | -11.0% | 1.01 | 0.81 | 0.94 | 0.91 |
| q09 | 674.4 | 596.0 | -11.6% | 1.00 | 0.72 | 0.88 | 0.68 |
| q10 | 436.8 | 356.2 | -18.5% | 0.77 | 0.74 | 0.69 | 0.70 |
| q11 | 51.4 | 44.8 | -13.0% | 0.62 | 0.56 | 0.51 | 0.48 |
| q12 | 187.3 | 162.5 | -13.2% | 0.95 | 0.75 | 0.90 | 0.86 |
| q13 | 308.2 | 288.7 | -6.3% | 0.70 | 0.54 | 0.72 | 0.59 |
| q14 | 314.0 | 276.8 | -11.8% | 1.02 | 0.79 | 0.92 | 0.84 |
| q15 | 275.4 | 242.3 | -12.0% | 0.96 | 0.76 | 0.93 | 0.90 |
| q16 | 100.0 | 88.4 | -11.6% | 0.86 | 0.78 | 0.77 | 0.80 |
| q17 | 224.9 | 189.1 | -15.9% | 0.90 | 0.48 | 0.96 | 0.56 |
| q18 | 346.7 | 314.0 | -9.4% | 0.63 | 0.58 | 0.60 | 0.54 |
| q19 | 359.0 | 266.8 | -25.7% | 0.89 | 0.68 | 0.94 | 0.86 |
| q20 | 332.3 | 292.3 | -12.0% | 0.76 | 0.90 | 0.69 | 0.92 |
| q21 | 580.5 | 625.3 | +7.7%¹ | 0.94 | 0.84 | 0.73 | 0.76 |
| q22 | 90.0 | 77.3 | -14.1% | 0.96 | 0.76 | 1.11 | 0.86 |

¹ Different query text; see below.

Ibex is still slower in five places:

- At 8 cores: q01 against DuckDB (1.24), and q05 (1.09), q04 (1.05) and
  q01 (1.03) against Polars.
- At 1 core: q01 (1.22) and q04 (1.09) against DuckDB, and q22 (1.11) and
  q04 (1.05) against Polars.

## q21: a query-text change, not an engine regression

`07275dd0` (2026-10-24) replaced `benchmarking/tpch/queries/q21.ibex`.

- **Old form:** counted lines per order as a proxy for suppliers. That
  matches the SQL only if no order has two lines from the same supplier,
  which TPC-H does not guarantee.
- **New form:** follows upstream PDS-H. It applies the nation and status
  filters first, then counts distinct suppliers for the remaining orders.
  This is exact, and it is more work.

The 10-23 run used the old text. To separate the text change from engine
changes, [ab_query.py](ab_query.py) ran both texts on the **same** HEAD
release binary at SF-10 and 8 cores. It used 10 interleaved fresh-process
rounds, each with one warmup and five timed `:run`s, and the per-round
median is the sample.

| q21 text | Median ms | Paired ratio vs old | Wilcoxon p |
| --- | ---: | ---: | ---: |
| Old (`a4dead37`) | 562.5 | | |
| New (HEAD) | 630.8 | 1.111 | 0.002 |

- **Same answer:** both texts give byte-identical output at SF-10, apart
  from the echoed script path.
- **The text change explains the whole difference:** on the old text, HEAD
  takes 562.5 ms, about 3% faster than the 580.5 ms of 10-23. That puts
  Ibex/Polars on q21 at about 0.85 at 8 cores, essentially 10-23's 0.84.
- **Caveat:** this check is across separate sittings, from two different
  measurements, and covers 8 cores only.

q21 numbers from before 2026-10-24 measure a different, cheaper workload.

## Reproducing the q21 A/B

```bash
D=benchmarking/results/local/int32/2026-11-10-q21-bisect   # any persistent dir
mkdir -p "$D"
git show a4dead37:benchmarking/tpch/queries/q21.ibex | grep -v '^\s*csv::write(' > "$D/q21_old.ibex"
grep -v '^\s*csv::write(' benchmarking/tpch/queries/q21.ibex > "$D/q21_new.ibex"
readlink -f benchmarking/data/tpch/parquet   # must be .../scale-10.0
.venv/bin/python benchmarking/int32/pdsh-sf10/ab_query.py \
    --variant old=build-release/tools/ibex:"$D/q21_old.ibex" \
    --variant new=build-release/tools/ibex:"$D/q21_new.ibex" \
    --rounds 10 --out "$D/query-shape-head.json"
```

The driver also accepts different binaries per variant
(`name=binary:script`). Its `answers_equal` field compares full stdout,
including the echoed `:run` path, so it reports `false` for different script
names. Compare result rows directly, as was done here. Raw samples and
outputs are in ignored persistent storage at
`benchmarking/results/local/int32/2026-11-10-q21-bisect/`.
