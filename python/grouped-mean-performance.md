# From a benchmark-page win to `import ibex as pl`

The benchmark page's strongest directly supported query is **mean price by
symbol and day**. At 16 million rows, its historical AWS run records 12.773 ms
for Ibex and 275.478 ms for Polars, about 21.6×. The page's largest absolute
entry is bounded rolling EWMA, whose Python API is not implemented yet.

The grouped mean translates directly into the independent package:

```python
import ibex as pl

# table is an already-resident Arrow table with symbol, day and price columns.
prices = pl.DataFrame(table).lazy()  # prepare input once

query = (
    prices
    .group_by("symbol", "day")
    .agg(avg_price=pl.col("price").mean())
)
result = query.collect()            # execute the analytical work
```

The equivalent Ibex expression is:

```ibex
prices[select { avg_price = mean(price) }, by { symbol, day }];
```

No Python loop computes the groups or means. The complete pipeline executes
in the native engine, and subsequent collections reuse the prepared input.
Results are not cached. A small standalone example is in
[examples/ibex_python_group_mean.py](../examples/ibex_python_group_mean.py).

## Does the win survive the Python frontend?

Yes, in this local warm-query measurement. Release build, Intel i7-13700 / WSL2,
12 interleaved iterations after two warmups. Inputs match the repository's
`prices_multi` generator: seed 43, 252 string symbols, four string days and
four-decimal prices, giving 1,008 groups. Input creation and preparation are
outside collection timing; parsing/planning, execution and result conversion
are included.

End-to-end warm collection medians, milliseconds:

| Rows | Cores | `ibex` Python | Polars string keys | Polars categorical keys |
|---|---|---|---|---|
| 1 million | 1 | 2.25 | 7.77 | 5.66 |
| 1 million | 8 | 1.21 | 3.26 | 2.53 |
| 16 million | 1 | 26.85 | 120.06 | 84.01 |
| 16 million | 8 | 8.51 | 20.38 | 13.38 |

At 16 million rows this is about **3.1× on one core** and **1.6× on eight
cores** against the categorical-key reference. That is the primary comparison:
the current benchmark harness prepares categorical keys for Polars as well
as Ibex. Every paired run favors Ibex; the paired Wilcoxon p-value is 0.000488
for each comparison shown. The full samples, revisions, source hashes and
first-use measurements are in
[the raw report](../benchmarking/results/python_mean_by_symbol_day_20261012.json).

The 21.6× historical result does not describe the current wrapper. Its
September 2026 run used another machine and older engine versions; the
Polars harness at that recorded commit also used plain string keys. The
current categorical-key contract and the current engine reduce that gap.
The old page is a guide to the workload, not a speed claim for this package.

All groups and means are compared by decoded key outside timers, with matching
Float64 mean output and numeric tolerance `rtol=1e-12, atol=1e-9`. Unspecified
group order is not timed as an extra sort. Ibex's one-/eight-core result hashes
match exactly at both scales.

## First use remains a different cost

At 16 million rows/eight cores, input preparation took 530 ms for Ibex versus
319 ms for categorical Polars, and first collection took 162 versus 14.9 ms.
The package currently benefits from repeated analytical work on prepared
tables; it does not win this first-use comparison. These are single observations,
recorded separately rather than hidden in warm-query medians.

## Run it

```bash
cmake --build build-release --parallel 6
PYTHONPATH=build-release/python:python IBEX_CORES=8 POLARS_MAX_THREADS=8 \
  taskset -c 0-7 .venv/bin/python python/bench_mean_by_symbol_day.py \
  --rows 16000000 --iters 12 --out /tmp/grouped-mean-eight-cores.json
```

Use `IBEX_CORES=1 POLARS_MAX_THREADS=1 taskset -c 2` for the single-core case,
choosing CPU IDs available on your machine. The script generates data directly
in memory using the existing generator's parameters and compares both string
and categorical reference inputs. It needs the repository's development
dependencies; the standalone example and core package do not require Polars.
