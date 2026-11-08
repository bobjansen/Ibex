# IbexEngine join gap (handoff, 2026-11-08)

Status: open. Read this first if you are picking up the h2o join results of
the Polars-engines benchmark.

## The problem in one paragraph

`IbexEngine` (`python/ibex_polars`) runs unchanged Polars lazy queries on
Ibex. On the h2o join set (`J1_1e7_NA_0_0`: a 10M-row `x` joined to small /
medium / big tables), the joins themselves are as fast as Polars, but
IbexEngine is still 1.5–4.3× slower than Polars streaming end to end. The
time goes into turning Ibex's result into a Polars `DataFrame`: ~9M output
rows, several Categorical columns (one with 10M categories), and narrow types
Ibex widened on import. Polars streaming never pays a conversion. Native
Ibex, running its own `.ibex` version of the same joins, is level with or
faster than Polars.

## Numbers (local i7-13700, 8 threads, `taskset -c 0,2,...,14`, 3.2 GHz locked)

Minimum of 5 collects, h2o join questions, Polars 2.0.0 streaming:

| Question | Polars | IbexEngine before e9a25a5c | IbexEngine now | Ratio now | Native Ibex (.ibex) |
|---|---:|---:|---:|---:|---:|
| q1 small inner on int | 32 ms | 171 ms | 94 ms | 2.96 | 29 ms (0.96) |
| q2 medium inner on int | 34 ms | 178 ms | 111 ms | 3.29 | 33 ms (1.00) |
| q3 medium outer on int (left join) | 45 ms | 218 ms | 189 ms | 4.25 | 38 ms (0.91) |
| q4 medium inner on factor | 34 ms | 183 ms | 108 ms | 3.14 | 33 ms (0.99) |
| q5 big inner on int | 238 ms | 375 ms | 364 ms | 1.53 | 184 ms (0.86) |

Native column: `benchmarking/h2o/results/h2o_20261108b_local_8t.tsv` (Ibex
materialises its result but never exports it). The other columns were measured
with ad hoc scripts, not paired A/Bs; treat a few ms as noise.

q1 now, broken down (94 ms):

- native join + Arrow export: **21 ms** (Polars' entire query: 32 ms)
- left columns rebuilt from the left frame: **~33 ms** (row-number order
  check 4, numpy mask 13, `DataFrame.filter` of 7 columns 16)
- right columns converted from Ibex: **~35 ms** (`id4_right` `cat.to` ~16–20,
  `v2` Float64 → Float32 cast 12)

## Why the conversion costs what it does (measured facts)

- **Ibex widens on import.** Int32 → Int64 and Float32 → Float64
  (`_WIDENED_NUMERIC_TYPES` in `_adapter.py`), so every narrow output column
  needs a narrowing cast on the way out (~12 ms per 9M-row column).
- **Categorical results go through `cat.to`.** A result column that still
  carries a dictionary Ibex was given has its codes mapped back to Polars
  category ids (see `_engine._known_codes`/`_known_ids`, offset or lookup,
  2ce78b6a), then `pl.col(...).cat.to(dtype)` builds the Categorical. `cat.to`
  checks every id against the category mapping: ~16 ms for 9M rows of a
  10-category column, ~106 ms with a 4.3M- or 10M-category mapping (random
  lookups). It is the only public way: casting UInt32 to Categorical is
  refused ("use .cat.to").
- **Polars' `gather` is ~105 ms per 10M-row column**, Categorical or plain
  UInt32, even with sorted indices: per-row overhead, not cache misses. A
  `DataFrame` gather of 3 columns took 129 ms. So "gather the column back from
  the source Series by row number" is not cheaper than `cat.to`.
- **A Boolean-mask filter is cheap and parallel** (16 ms for 7 columns of 10M
  rows), so when each left row appears at most once and in order, a filter
  replaces the gather.
- **`pyarrow` `combine_chunks` copies even a single chunk** (31 ms for a 9M
  Int64 column). Use `.chunk(0)` when there is one.

## What is in the tree now

- **e9a25a5c** `_adapter.Adapter._passthrough_join`: an inner/left equality
  join at the plan's root over a resident left `DataFrameScan` binds only the
  left keys plus an Int64 row-number column (`_resident_table(...,
  row_number=...)`). `_engine._to_polars(..., left_rows=...)` /
  `_left_rows` rebuild the left columns from the original Polars frame: mask
  filter when the row numbers strictly increase, `DataFrame.gather`
  otherwise; right columns convert as before, renamed by position. Test:
  `test_root_join_takes_left_columns_from_the_left_frame`.
- **2ce78b6a** categorical ids computed inside one Polars query (offset /
  lookup), codes viewed as UInt32, integer narrowing left to the final cast.
- Plan tuple from `Adapter.compile()` is now 7 elements: `source, bindings,
  scalars, expected, result_schema, categories, left_rows`.
  (`python/bench_full_join.py`, `bench_polars_result.py` and
  `bench_categorical_export.py` still unpack an older 5-tuple; they were stale
  before this work.)

Tried and dropped: gathering only the Categorical columns by row number (q1
170 → **390 ms**, because of the `gather` cost above).

## Ideas not yet tried, roughly by expected payoff

1. **Right side too.** For q1–q4 the right table is small (10 / 10k rows): its
   columns could be rebuilt from right row numbers. But a 9M-row gather from a
   small Series still cost 16 ms, about the same as `cat.to` for a small
   mapping, so this probably only pays for high-cardinality right columns
   (q5's `id6_right`). Measure before building.
2. **Export narrow types natively.** If Ibex kept (or re-narrowed on export)
   Int32/Float32 columns, the narrowing casts disappear (~12 ms per column).
   Touches the native import widening, so it is a runtime change, not adapter
   work.
3. **Let the native join emit the left-row mask** (or a "matched" bitmap) when
   each left row appears at most once, instead of row numbers: removes the
   order check and the numpy mask build (~17 ms of q1).
4. **Skip the right-hand Categorical `cat.to`** by any means Polars allows
   (none found in the public API; check Polars 2 internals or ask upstream).
5. The general version of all this is producing Polars columns natively
   instead of Arrow plus Python-side fix-ups. Much larger.

Also open, same root cause elsewhere: h2o groupby q10 (~10M result groups,
1.27–1.63× after 2ce78b6a, remaining mostly native work plus `cat.to`
validation on K=1e1/2e0) and K=2e0 q3/q7 (millions of groups). q6 is slow for
an unrelated reason: the NaN-safe median needs aggregates over computed
expressions, which take a slow native path.

## How to reproduce and measure

Environment (repository root):

```bash
cmake --build build-release -j4          # full tree; never --target before measuring
export PYTHONSAFEPATH=1 PYTHONPATH="$PWD/build-release/python:$PWD/python"
scripts/perf-box.sh status               # cores locked at 3.2 GHz? (`! scripts/perf-box.sh on`, sudo)
```

Validated run of the join set (fixture answer checks + timings + prepare ms):

```bash
taskset -c 0,2,4,6,8,10,12,14 python/bench_polars_engines.sh --skip-pdsh \
    --threads 8 --h2o-data J1_1e7_NA_0_0 --out-dir /tmp/joincheck
.venv/bin/python python/summarize_polars_engines.py /tmp/joincheck
```

Splitting one question into native vs conversion (the pattern used above):

```python
import sys, time, polars as pl, ibex_pyarrow
from ibex_polars import IbexEngine, _engine
sys.path.insert(0, "benchmarking")
from h2o import run as h2o
t = h2o.join_tables("J1_1e7_NA_0_0")
ints = lambda *c: {k: pl.Int32 for k in c}
cat = lambda *c: pl.col(list(c)).cast(pl.Categorical)
rd = lambda p, o: pl.read_csv(p, schema_overrides=o)
x = rd(t["x"], {**ints("id1", "id2", "id3"), "v1": pl.Float32}).with_columns(cat("id4", "id5", "id6"))
s = rd(t["small"], {**ints("id1"), "v2": pl.Float32}).with_columns(cat("id4"))
m = rd(t["medium"], {**ints("id1", "id2"), "v2": pl.Float32}).with_columns(cat("id4", "id5"))
b = rd(t["big"], {**ints("id1", "id2", "id3"), "v2": pl.Float32}).with_columns(cat("id4", "id5", "id6"))
q = h2o.join_questions(False)[0][4](x.lazy(), s.lazy(), m.lazy(), b.lazy())   # q1
e = IbexEngine()
src, bindings, scalars, expected, output, categories, left_rows = e._prepare(q, pl.QueryOptFlags())
t0 = time.perf_counter(); res = ibex_pyarrow.execute_pipeline(src, tables=bindings, scalars=scalars)
t1 = time.perf_counter(); out = _engine._to_polars(res, expected, output, categories, left_rows)
print(f"native {(t1 - t0) * 1e3:.0f} ms, conversion {(time.perf_counter() - t1) * 1e3:.0f} ms")
```

Run it with `IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14`,
repeat a few times and take the minimum. Re-run Polars in the same sitting;
never compare against a cached Polars number.

Tests: `.venv/bin/python python/test_ibex_polars.py` (no pytest in the venv;
`uv run --with pytest python -m pytest -q python/test_polars_conformance.py`
for the conformance suite). Every new path should come with a test that fails
when the path is broken; check by mutating the code.

## Where this fits

- The AWS run of the Polars-engines benchmark
  (`benchmarking/aws/run-polars-engines.sh --on-demand`, paid; needs the
  commit pushed and `AWS_PROFILE=ibex-bench`) has not been launched yet. It
  will publish these join ratios as they stand unless this work lands first.
- `docs/polars-engine.html` and `python/pdsh-performance.md` describe the
  engine; `python/polars-engine-performance.md` has older join measurements
  (prepare times there predate the import improvements: prepare is now
  0.05–0.85 s per join question).
