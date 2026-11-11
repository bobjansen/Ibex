# IbexEngine join gap

Status: open, 2026-11-10. Compacted from the working diary; the full version
(every profile, prototype and paired table) is
`git show a929c528:plans/polars-engine-join-gap.md`. Raw outputs are local,
under `benchmarking/results/local/` (named in that version).

## The problem

`IbexEngine` (`python/ibex_polars`) runs unchanged Polars lazy queries on Ibex.
On the h2o join set (`J1_1e7_NA_0_0`: a 10M-row `x` joined to small / medium /
big tables) the native join is as fast as Polars streaming, or faster; the
cost is turning a ~9M-row Ibex result into a Polars `DataFrame`. Polars never
pays that conversion. PDS-H outputs are small aggregates, so the adapter is
already ahead there (gm 0.93× at 8 cores, 0.88× at 1 core).

## Where it stands (i7-13700, 3.2 GHz locked, `IBEX_COMPACT_INT32=1`)

Ibex/Polars streaming, median of paired ratios, at `a929c528`:

| | 8 threads | 1 thread |
|---|---:|---:|
| q1 small inner on int | 1.24 | 0.83 |
| q2 medium inner on int | 1.35 | 0.91 |
| q3 medium outer (left) on int | 1.43 | 1.19 |
| q4 medium inner on factor | 1.24 | 0.75 |
| q5 big inner on int | 0.88 | 0.47 |

On the morning of 2026-11-10 (`cf74c102`, compact off) the 8-thread ratios
were 2.9–3.4× (q5 1.5×). With compact off, the first two steps below still
apply (8t q1 1.29×, q3 1.86×, q5 0.99× at `cd08c604`).

## Why conversion costs what it does

- **`cat.to` validates every id**, serially per chunk (`from_cats_and_dtype`
  in polars-core): ~1.5 ns/row for a small mapping, ~10 ns/row for a
  10M-category one. Polars 2.0.0 has no Python-reachable unchecked
  constructor: Arrow and plugin imports rebuild dictionaries with
  `from_str_iter` (hashing each row's string), primitive imports ignore
  `_PL_CATEGORICAL2`, `scatter` rejects UInt32 values, `ext.to` needs the
  exact storage dtype, and casting UInt32 to Categorical is refused. The
  upstream source marks the unchecked numeric cast "remove after exposing
  to/from physical functions"; the issue tracker makes that unlikely soon.
- **Eager Polars expressions run one thread per column.** The same work in a
  streaming query is split into morsels across the pool (`cat.to` on 9M ids:
  15 → 2.6 ms, 10 categories; 103 → 17.5 ms, 10M).
- **Polars `gather` is ~105 ms per 10M-row column** (any type, even sorted
  indices); a Boolean-mask filter is cheap and parallel. So left columns are
  rebuilt by filter, never gathered back, when the row numbers allow it.
- **Ibex widens Float32 on import**, so a Float32 payload narrows on the way
  out (~5 ms per 9M-row column in the final streaming cast).
- **`pyarrow` `combine_chunks` copies even a single chunk**: use `.chunk(0)`.

## What is in the tree

- **Left columns from the left frame** (b45bc098): a root inner/left equality
  join over a resident left `DataFrameScan` binds only the left keys plus an
  Int64 row number; `_engine._left_rows` rebuilds the left columns from the
  original frame (identity when every row survives in order, a streaming
  mask filter when they strictly increase, a gather otherwise).
- **Known dictionaries** (f28bc821): categorical results carrying the
  dictionary Ibex was given map codes to Polars ids by offset or lookup.
- **Int32 pass-through** (585929e4): with compact on, Int32 inputs bind
  unchanged.
- **Parallel conversion** (ce53b9bb): left and right halves overlap on a
  helper thread; id mapping, `cat.to`, the left filter and the final cast run
  as streaming queries.
- **Native left-row mask** (cd08c604): `ibex_pyarrow.left_row_mask` checks
  strict increase and builds the Arrow bitmap in one pass on the worker pool.
- **Right payload Categoricals as ids** (a929c528): with compact on, a root
  join's resident right input binds its non-key Categoricals as compact Int32
  Polars ids; they return as ids and need only `cat.to`. The serial sentinel
  gather now keeps compact payloads compact.

## Measured dead ends (details in the diary version)

- Gathering only the Categorical columns by row number: q1 170 → 390 ms.
- Right-row prototype (removed): carry right row numbers and gather the
  original payloads in Polars. q3 −35% at 8t, but q5 +59% at both
  budgets: the random gather moves to Polars, which is slower.
- Native physical-output prototype (removed): write final buffers in C++,
  then attach via `cat.to`. q3 −45% at 8t, q5 +5% / +52% (8t / 1t): late
  random access is real work, and validation still costs ~83 ms on q5.

## Open

1. **`cat.to` validation** is now most of what remains on q3/q5. Without an
   unchecked Polars constructor the only lever is doing less of it: fewer or
   smaller Categorical columns reaching it (e.g. a payload whose right table
   is tiny could rebuild from right row numbers, measured ~16 ms per 9M rows,
   about the same as validation for a small mapping).
2. **Float32 payloads still widen** on import and narrow on export.
3. **The id path needs compact on.** Flipping compact to the default is its
   own gate (`plans/README.md`, retired native-int32 entry).
4. **Same cause elsewhere:** h2o groupby q10 (~10M result groups) and the K=2
   q3/q7 groupbys; q6 is slow for an unrelated reason (NaN-safe median over a
   computed expression takes a slow native path).
5. **1-thread harness bias:** `bench_join_gap.py` interleaves engines in one
   process; at 1 thread Polars then refaults memory after each Ibex call (q3
   Polars 259 ms alone vs 379 ms interleaved), flattering Ibex. 8 threads is
   unaffected. Report a solo run next to 1-thread ratios.

## How to measure

After a full Release build (`cmake --build build-release -j6`, never a
`--target` before measuring), separately at each budget:

```bash
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_join_gap.py --out build-release/join-gap-8t.json
# 1 thread: IBEX_CORES=1 POLARS_MAX_THREADS=1 taskset -c 2
```

It checks complete outputs and dtypes against streaming Polars (join order
unspecified), warms each route twice, alternates route order over 12 paired
iterations and reports paired Ibex/Polars ratios with Wilcoxon p-values.
`native_export` and `conversion` are diagnostic stages, not an additive
decomposition; `--reconstruction` profiles the conversion sub-stages.

- **Engine-only changes:** save the baseline first
  (`git show HEAD:python/ibex_polars/_engine.py > base-engine.py`) and pass
  `--baseline-engine base-engine.py` for a paired A/B in one process.
- **Adapter changes:** `--baseline-engine` does not cover them. Extract the
  committed packages (`git archive HEAD python/ibex python/ibex_polars`),
  put them first on `PYTHONPATH` for side A, and alternate whole harness
  processes A B A B on the same binary; compare the paired Ibex/Polars ratios.
- **Native changes:** `benchmarking/compare_ibex_git.sh` per MEASURING.md.
- Never compare against a cached Polars number; re-run it in the same sitting.

## Where this fits

- The AWS Polars-engines run (`benchmarking/aws/run-polars-engines.sh
  --on-demand`, paid; needs the commit pushed and `AWS_PROFILE=ibex-bench`)
  has not been launched; it would publish these ratios.
- `docs/polars-engine.html` describes the engine; `python/performance.md`
  summarises where it stands on PDS-H, h2o and synthetic joins.
