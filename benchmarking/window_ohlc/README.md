# Window-OHLC benchmark suite

Compares **Ibex vs Polars vs DuckDB** on a rolling open/close-per-window query —
the canonical market-data "OHLC bars per symbol" shape:

```
t[ select { price = price,
            open  = first(price),
            high  = max(price),
            low   = min(price),
            close = last(price),
            volume_sum = sum(volume),
            window_start = window_start() },
   by symbol,
   window 10s [aligned],
   order symbol ];
```

Full OHLCV, not a reduced open/close pair — a benchmark that computes only the
two cheapest aggregates invites the reading that the shape was chosen to
flatter someone. The frame is **expanding within each bucket**, so `high`,
`low` and `volume_sum` are running values, matching DuckDB's
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. In Polars that means
`cum_max`/`cum_min`/`cum_sum` over the partition; plain `.max().over()` would
compute a whole-bucket answer, which is both different and cheaper.

`run.py --verify` recomputes the OHLCV columns in all three engines and
compares them row by row against Ibex, so "identical output" is checked rather
than claimed. It exits non-zero on any mismatch.

Two window flavours are benchmarked:

| flavour   | Ibex              | meaning                                            |
|-----------|-------------------|----------------------------------------------------|
| `aligned` | `window 10s aligned` | tumbling buckets snapped to the epoch grid       |
| `sliding` | `window 10s`         | trailing window ending at each row (rolling/tick) |

## Fairness

- **Identical data.** One Parquet file per `(rows, symbols)` config, generated
  once by Ibex's `data_gen` plugin (`gen_ticks`, `seed_rng(42)` → reproducible),
  read by every engine.
- **Compute only.** Input is pre-loaded into memory (Ibex `as_timeframe`, Polars
  eager `DataFrame`, DuckDB in-memory `TABLE`); only compute + full
  materialisation is timed. Parquet read is excluded.
- **Same output.** All three produce one row per input row and are sorted by
  `symbol`. `run.py`'s sibling check confirms **0 value mismatches** vs Ibex on
  both flavours.
- **min-of-N.** One warmup iteration dropped; the reported number is the MIN
  (least noisy on a shared box), with median alongside.
- Ibex is timed via its own `:timing` (driven through a pty); Polars/DuckDB via
  `time.perf_counter`.
- `--threads` picks the comparison:
  - `auto` (default): each engine's default parallelism — DuckDB uses
    `--duckdb-threads` (8), Polars its default pool, Ibex its parallel islands.
    This is the *what-users-experience* comparison.
  - `1`: single-threaded everywhere (`POLARS_MAX_THREADS=1`, DuckDB
    `threads=1`, `IBEX_PARALLEL=0`). The apples-to-apples *engine efficiency*
    comparison.

  **Ibex is no longer single-threaded by default**, so every engine is now
  pinned in `1` mode and tagged with the mode it actually ran in. Results
  recorded before that change tagged Ibex `1t` in both modes; those `mt` rows
  are single-threaded Ibex and are not comparable with new `mt` rows.
  Keep both columns — one is not a substitute for the other. Polars fixes its
  thread pool at import, so `--threads` is a whole-process switch: run the suite
  once per mode and combine the TSVs.

**Per-core result (single-threaded):** Ibex is fastest at *every* scale and both
window types — e.g. 5M aligned Ibex 575 ms vs Polars 953 ms (1.7×) vs DuckDB
1559 ms (2.7×); 5M sliding Ibex 583 ms vs Polars 1855 ms (3.2×) vs DuckDB 10.3 s
(17.6×). The multithreaded losses are purely parallelism: Polars/DuckDB each pull
~3.3–3.9× from threads; Ibex pulls 1×.

## Running

```sh
# default: 100k rows, 3 symbols, aligned window
uv run --project <ibex-root> benchmarking/window_ohlc/run.py

# full extension grid used for the writeup
uv run --project <ibex-root> benchmarking/window_ohlc/run.py \
    --rows 100000 1000000 5000000 --symbols 3 --window aligned sliding --iters 7 \
    --threads auto --out benchmarking/window_ohlc/results/scale_mt.tsv

# per-core (single-threaded) — run separately and combine
uv run --project <ibex-root> benchmarking/window_ohlc/run.py \
    --rows 100000 1000000 5000000 --symbols 3 --window aligned sliding --iters 7 \
    --threads 1 --engines polars duckdb \
    --out benchmarking/window_ohlc/results/scale_1t.tsv

uv run --project <ibex-root> benchmarking/window_ohlc/run.py \
    --rows 1000000 --symbols 3 20 100 --window aligned --iters 7 \
    --out benchmarking/window_ohlc/results/symbols.tsv
```

Flags: `--rows`, `--symbols`, `--window {aligned,sliding}`, `--iters`,
`--engines`, `--duckdb-threads`, `--budget-s`, `--out`. Results are written as
TSV (`engine  threads  rows  symbols  window  min_ms  median_ms  status`).

`--budget-s` (default 300) caps a single execution. Exceed it and the cell stops
immediately — including on the warmup — reporting the time it did measure with
`status = over_budget` instead of `ok`. An `over_budget` row is a real lower
bound ("at least this slow"), not a finished measurement: `min_ms` and
`median_ms` are the one execution that blew the budget, so don't read the median
as a median. Pass `--budget-s 0` to disable.

Why it exists: DuckDB's 50M-row sliding case ran ~35 min per execution, and at
`--iters 5` that is six of them — nearly four hours to put a number on a cell
already three orders of magnitude off the pace, while a finished five-hour
matrix sat unshipped on the box.

## Notes / gotchas

- The `data/` dir caches generated Parquet by `(rows, symbols)`; delete it to
  regenerate. A 5M-row file is ~115 MB.
- Ibex timing goes through a pty because the REPL only prints `time:` on a TTY
  (`:timing on`). File/pipe mode stays silent.
- WSL2 / shared boxes drift; treat MIN as the signal and re-run if a row looks
  anomalous. Interleaving engines per-config (as `run.py` does) beats running
  each engine's whole sweep back-to-back.
