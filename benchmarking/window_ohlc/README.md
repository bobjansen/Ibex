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

## Four engines, and what `--verify` actually proves

Both suites run **Ibex, Polars, DuckDB and ClickHouse** (embedded, via `chdb`).
`--threads N` applies to every engine in both: a fairness invariant, not a
tuning knob.

`--verify` does not compare with a tolerance. It recomputes each engine's
trailing frame `[left, right]` under **that engine's own arithmetic and its own
ordering key**, and accepts a divergence only where the covered row sets
provably differ, or where tied timestamps make the answer ambiguous. Anything
else is a hard failure and exits non-zero. Four real defects had to be fixed
before all 12 configurations passed, three of them invisible on the old sparse
data:

- **Polars was doing different work.** The sliding query joined the rolling
  result back on `(symbol, timestamp)`, which FANS OUT on tied timestamps --
  200,018 rows from a 200,000-row input. Now a stable re-sort plus positional
  stack; `rolling(group_by=)` does not emit in input order.
- **Polars used a different window convention** -- `rolling` defaults to
  `closed="right"`, i.e. `(t-10s, t]`, against Ibex's and DuckDB's
  `[t-10s, t]`. Now `closed="both"`.
- **DuckDB quantizes the frame edge to microseconds.** Its `INTERVAL`
  arithmetic is microsecond-resolution: at `t = 10s + 1ns` it reports `n=3`,
  including a tick 10s+1ns away, where Ibex correctly reports `n=2`. ClickHouse
  cannot express a nanosecond RANGE offset at all (32-bit offset cap), so its
  query orders by microseconds. Ibex and Polars carry nanoseconds -- a real
  capability, paid for in the bandwidth of 8-byte timestamps touched on every
  frame comparison.
- **`ORDER BY timestamp` is ambiguous at ties**, which affects `aligned` too,
  since `last(price)` over a prefix depends on how peers are sequenced.

OPEN LANGUAGE QUESTION, flagged not settled: a SQL `RANGE` frame is
peer-inclusive at both ends, so a later tick sharing the current timestamp is
inside it. Ibex's trailing window is position-bounded and ends at the current
row. SPEC defines the window on TIME (argues for admitting peers); a streaming
engine cannot see the future (argues against). The clean fix for the tie
ambiguity is a tie-breaking row id in the data, added to every `ORDER BY`.

Window suite, 5M rows, matched threads, min ms -- Ibex wins 10 of 12:

| thr | sym | flavour | Ibex | Polars | DuckDB | ClickHouse |
|-----|-----|---------|------|--------|--------|------------|
| 8   | 3   | aligned | **128** | 301 | 1706 | 767 |
| 8   | 3   | sliding | 370 | **188** | 10786 | 2299 |
| 8   | 20  | aligned | **139** | 356 | 977 | 761 |
| 8   | 20  | sliding | **163** | 949 | 4324 | 1008 |
| 8   | 100 | aligned | **113** | 436 | 989 | 888 |
| 8   | 100 | sliding | **146** | 943 | 2285 | 874 |

The one loss is **3 symbols, sliding**: Ibex is ~2x Polars and does not improve
from 4 to 8 threads (374.6 -> 370.5ms). A sliding window has no bucket
boundaries to cut at, so parallelism caps at the group count -- three symbols
leave most cores idle. Aligned does not have this problem because
`split_at_bucket_bounds` cuts within a group.

## resample_run.py — finished bars

`run.py` computes a **running** bar state: one output row per input tick.
`resample_run.py` computes **finished bars**: one row per bucket per symbol. Two
different workloads, and the second is far friendlier to the competition —
Polars gets `group_by_dynamic().agg()`, DuckDB and ClickHouse a plain
`GROUP BY`, instead of the per-row window formulations the running variant
forces on them. It adds **ClickHouse** (embedded, via `chdb`) as a fourth
engine.

```
uv run --project <ibex-root> benchmarking/window_ohlc/resample_run.py --verify
uv run --project <ibex-root> benchmarking/window_ohlc/resample_run.py \
    --rows 5000000 --symbols 3 20 100 --threads 8
```

**`--threads` applies to every engine.** It is a fairness invariant, not a
tuning knob: the first version of this suite inherited `--duckdb-threads 8` for
DuckDB and ClickHouse while Polars and Ibex took the whole box, i.e. it
handicapped two engines threefold and called the output a comparison.

Findings, 5M rows, i7-13700, ~100 ticks/bar, matched threads (min ms):

| threads | symbols | Ibex | Polars | DuckDB | ClickHouse |
|---------|---------|------|--------|--------|------------|
| 4       | 3       | **29.7** | 178.0 | 129.4 | 44.5 |
| 4       | 20      | **27.7** | 328.2 | 134.6 | 47.0 |
| 4       | 100     | **28.8** | 471.5 | 144.2 | 58.7 |
| 8       | 3       | 30.7 | 131.9 | 72.5 | **27.6** |
| 8       | 20      | **28.2** | 201.7 | 78.3 | 31.1 |
| 8       | 100     | **29.0** | 261.4 | 78.0 | 42.1 |

**Ibex's resample is still SERIAL** -- flat across thread budgets, where every
other engine roughly halves from 4 to 8. It wins 5 of 6 cells anyway, on one
core against their four or eight; ClickHouse holds only 3-symbol/8-thread, by
3ms. The parallel work is therefore unspent headroom, not a fix for a deficit:
bar boundaries are contiguous row ranges, so a range split gives each worker
whole bars, no partial aggregates, no merge, and a bitwise-identical result.

Getting there took the grouped vectorised path (`resample_table`, window.cpp),
which took the same query from 58.9ms to 28.5ms single-threaded. It skips the
generic path's `_bucket` column and composite hashing, factorizes the group key
once, and accumulates into a dense per-bucket array -- no `AggSlot`, whose
move/copy/growth alone was ~15% of profile. `first`/`last` become gathers over
the ~50k output rows instead of scans over 5M input rows, so an OHLC query makes
three passes over the data rather than five (5.8ms -> 1.4ms per aggregate).

## Notes / gotchas

- **Tick density is load-bearing, and the default is wrong for bars.**
  `gen_ticks`' own `interval_ms` default is 1000.0 — one tick per SECOND across
  the whole feed. At that rate a 5M-row/100-symbol file spans 57.8 days, a
  10-second bar holds 1.1 ticks, and `resample` yields 4.76M groups from 5M
  rows: an identity operation wearing the name of an aggregation. It inverted a
  result — Ibex measured 3-7x SLOWER than everyone, and at a realistic density
  is 2x faster on the same query. `run.py` therefore pins
  `TICK_INTERVAL_MS = 1.0` and puts it in the cached filename, because a file
  generated at another density is a different benchmark, not a reusable one.
  Before trusting any bar number, check `distinct(bucket, key)` against the row
  count; near 1 means you are measuring group-by cardinality.
- The `data/` dir caches generated Parquet by `(rows, symbols, interval_ms)`;
  delete it to regenerate. A 5M-row file is ~115 MB.
- Ibex timing goes through a pty because the REPL only prints `time:` on a TTY
  (`:timing on`). File/pipe mode stays silent.
- WSL2 / shared boxes drift; treat MIN as the signal and re-run if a row looks
  anomalous. Interleaving engines per-config (as `run.py` does) beats running
  each engine's whole sweep back-to-back.
