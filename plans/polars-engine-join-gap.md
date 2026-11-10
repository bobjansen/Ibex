# IbexEngine join gap (handoff, 2026-11-08)

Raw experiment output is retained locally under `benchmarking/results/local/`;
the scripts and summarized findings are versioned here.

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
  The three historical conversion/join scripts now consume this tuple.
  `bench_categorical_export.py` deliberately disables root reconstruction for
  both converters, since the historical converter requires complete Arrow payloads.

Tried and dropped: gathering only the Categorical columns by row number (q1
170 → **390 ms**, because of the `gather` cost above).

## Current implementation sequence

1. **Identity reconstruction implemented and validated.** When the left
   row numbers strictly increase, cover the whole source, and have endpoints
   0 and height-1, return the original frame. This avoids mask allocation and
   filtering without changing ordering, multiplicity, or category mappings.
   The 10M-row q3 paired A/B at eight threads measured 202.4 → 171.8 ms
   (median paired candidate/base 0.855, p=0.00098). Full outputs and dtypes
   match Polars. Raw samples (`join-gap-identity-8t.json`, local raw output).
   q1 and q5 control ratios were 1.034 (p=0.233) and 1.014 (p=0.339):
   unresolved, not evidence of equivalence or a regression.
   Control samples (`join-gap-identity-controls-8t.json`, local raw output).
   The direct identity/order test is mutation-checked; all 90 engine tests pass.
2. **Isolation completed; native output experiment now recorded below.** Both
   lookup and `cat.to` cost materially on q3; validation dominates q5's 10M
   category mapping. Benchmark Arrow `take` versus the current Polars gather
   for exact UInt32 IDs with validity preserved, then verify full end-to-end
   improvement. Independently compare right-row reconstruction against full
   categorical conversion, with nullable q3 and high-cardinality q5 gates.
   Do not remove validation without a supported construction path and proven
   mapping/lifetime invariants. q5 also merits a separate dictionary-recognition
   experiment: its known-code extraction costs ~11 ms.
3. **Native ordered selection metadata remains a bounded opportunity.**
   q1/q5 spend ~18–20 ms checking order and building masks; q3 spends only
   ~5 ms checking order after identity reuse. Avoiding that work still leaves
   ~16–17 ms of filtering for subsets. A mask needs an explicit source-order
   and uniqueness guarantee; repeated/reordered results must retain row indices.
4. **Fused narrow export is separate.** Narrowing on export still performs a
   conversion unless fused with materialization. Preserving narrow types
   throughout execution is a larger runtime project.

The proposed adapter-only Float32 cast change was checked and dropped: for
these joins the lowerer's output schema already says Float64, so Arrow does
not narrow it; the final Polars cast already produces Float32. Moving it to
that cast cannot save work.

For Python-only engine changes, save the baseline engine before editing and run:

```bash
git show HEAD:python/ibex_polars/_engine.py > build-release/join-gap-base-engine.py
cmake --build build-release -j6
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_join_gap.py \
    --baseline-engine build-release/join-gap-base-engine.py --out build-release/join-gap-ab.json
```

This uses the same native bridge and adapter on both sides, loading the saved
Python engine in a separate module namespace. It is valid for engine-only
changes, not adapter/native changes. The saved module hash is in the report.
Native hot-path edits still require `compare_ibex_git.sh` as in MEASURING.md.

## Reconstruction profile after identity reuse (8 threads)

Full 10M-row inputs, 12 alternating iterations and two warmups; q1/q3/q5
outputs passed complete equality checks against Polars. Reassembled and
cached-left diagnostic outputs additionally match production conversion in
exact row order. Raw samples and provenance (`join-gap-reconstruction-8t.json`, local raw output).

| Diagnostic stage (ms) | q1 | q3 | q5 |
|---|---:|---:|---:|
| Full conversion | 80.65 | 130.11 | 195.24 |
| Left reconstruction | 38.05 | 5.36 | 40.25 |
| Order check | 4.94 | 5.42 | 5.71 |
| Mask construction | 13.42 | identity: skipped | 14.24 |
| Left filter | 15.52 | identity: skipped | 17.49 |
| Right conversion | 34.11 | 106.57 | 144.61 |
| Final assembly | 0.07 | 0.06 | 0.07 |

These are independent warmed stages, not an additive decomposition. Cache
conditions and column parallelism matter: q3's full conversion measured 130 ms
here versus 118 ms in the adjacent whole-harness conversion samples. A
counterfactual reusing a precomputed left frame measures right conversion plus
assembly, but excludes producing that frame; it is not an achieved speedup.

All row-index arrays have one chunk and no nulls; extracting the Arrow array
and its NumPy view takes <0.02 ms each. Chunk combination is not the bottleneck
in these cases. q1/q5 are ordered unique subsets; q3 is identity.

Right-column conversion includes ID remapping, categorical validation, and
numeric casts. Every measured categorical hits the known-dictionary path but
uses **lookup**, not a constant offset. q1 `id4_right` takes 29 ms. q3's two
categoricals take 71–73 ms each in isolation (parallel together), and its
nullable Int32 takes 30 ms. Each q3 right column has 1,001,588 nulls; this is
an observation, not proof that nulls cause the difference. q5 `id6_right`
takes 120 ms, versus 29–34 ms for its other categoricals. Float32 export is
only ~5 ms per column in this run.

Reproduce after a complete Release rebuild:

```bash
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_join_gap.py --questions q1 q3 q5 \
    --reconstruction --out build-release/join-reconstruction.json
```

## Category isolation results (2026-11-08)

The real-output profile now times `_known_codes`, `_known_ids` alone,
`cat.to` on already-remapped IDs, and the current combined expression.
The split and combined paths produce identical categoricals, including order,
IDs and nulls. Full join outputs continue to pass Polars equality checks.
Times below are medians of 12 alternating runs after two warmups at 8 threads.
Independent stages reuse inputs; they are diagnostic and need not add exactly.

| Real right column | Dictionary entries | Nulls | Known codes ms | Lookup ms | `cat.to` ms | Combined expression ms |
|---|---:|---:|---:|---:|---:|---:|
| q1 id4_right | 10 | 0 | 0.13 | 13.76 | 14.54 | 28.20 |
| q3 id4_right | 10 | 1,001,588 | 0.13 | 37.50 | 32.72 | 70.02 |
| q3 id5_right | 10,000 | 1,001,588 | 0.14 | 39.45 | 35.18 | 72.66 |
| q5 id4_right | 10 | 0 | 0.13 | 13.47 | 14.54 | 28.19 |
| q5 id5_right | 10,000 | 0 | 0.14 | 14.01 | 18.73 | 32.58 |
| q5 id6_right | 10,000,000 | 0 | 10.94 | 24.43 | 82.17 | 104.04 |

Real-output samples (`join-gap-category-isolation-8t.json`, local raw output).
`cat.to` here includes the public categorical construction/validation operation;
it is not an internal function profile. Known-code extraction includes dictionary
recognition and wrapping code buffers, not only dictionary equality.

A separate fixed-seed sweep uses one million rows, independent named category
mappings, reversed dictionary-to-ID lookup, three cardinalities and null
fractions 0/10/50%. It validates against gathering the original categorical
labels. Named mappings avoid contamination by unrelated registered categories.

| Categories | Null fraction | Lookup ms (8t) | `cat.to` ms (8t) | Combined ms (8t) |
|---|---:|---:|---:|---:|
| 10 | 0% | 1.31 | 1.67 | 2.92 |
| 10 | 10% | 3.98 | 3.41 | 7.12 |
| 10 | 50% | 6.46 | 5.77 | 12.14 |
| 10,000 | 0% | 1.33 | 2.04 | 3.43 |
| 10,000 | 10% | 3.72 | 3.59 | 7.27 |
| 10,000 | 50% | 6.49 | 5.89 | 12.28 |
| 1,000,000 | 0% | 2.76 | 9.89 | 12.70 |
| 1,000,000 | 10% | 4.96 | 12.89 | 18.57 |
| 1,000,000 | 50% | 7.23 | 10.20 | 17.66 |

The 1-thread control (`join-gap-category-sweep-1t.json`, local raw output) closely
matches the 8-thread sweep (`join-gap-category-sweep-8t.json`, local raw output):
these single-column expressions gain little from the larger pool. Multiple
columns can still execute in parallel. The sweep supports null handling as
an independent cost in both stages, while high mapping cardinality especially
penalizes `cat.to`. It does not isolate CPU branch prediction or cache misses,
and does not imply a monotonic null penalty: at 1M categories, 50% nulls costs
less validation time than 10%, with fewer nonnull IDs to validate.

Reproduce the real profile with the reconstruction command above. Run the
controlled sweep separately at each thread budget (full Release rebuild first):

```bash
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_category_isolation.py --out build-release/category-sweep-8t.json
# For the control: IBEX_CORES=1 POLARS_MAX_THREADS=1 taskset -c 0.
```

## Right-row prototype: evidence for a broader boundary redesign

`python/prototype_right_rows.py` is benchmark-only, not enabled in the public
engine. It carries right keys plus a row index natively, then gathers original
Polars payload columns with nullable indices. Existing adapter eligibility and
layout checks are reused. Preparation still visits the original right payload;
the prototype has a single-query strong cache, not production cache semantics.

At eight threads, 12 paired samples on full inputs:

| Query | Current ms | Prototype ms | Median paired prototype/current |
|---|---:|---:|---:|
| q1 | 94.8 | 81.8 | 0.868 |
| q3 | 166.9 | 109.2 | 0.654 |
| q5 | 375.4 | 595.4 | 1.587 |

Raw samples (`join-gap-right-rows-8t.json`, local raw output). All comparisons have
p=0.00049; full outputs and dtypes matched streaming Polars. Focused tests pass
at 1 and 8 threads for nulls, duplicates, empty/chunked right inputs, suffixes,
composite keys and explicit order. Reversing reconstruction output makes the
correctness tests fail. Expression gather handles all-null indices, where the
DataFrame indexing helper failed for an empty right table.

The one-thread q3 control (`join-gap-right-rows-1t.json`, local raw output) shows
no resolved improvement: current 497.9 ms, prototype 496.4 ms, paired ratio
1.021, p=0.970. The eight-thread benefit therefore does not establish less
work; more favorable parallel execution may explain part of it.

q5 is a decisive regression: native/export drops to 113 ms, but reconstruction
rises to 482 ms. Moving payload work to Polars gather is not a general solution.
Run with `bench_join_gap.py --right-rows`; do not combine with reconstruction
profiling or a saved baseline engine. The baseline is the current public engine.

The architectural direction to investigate is preserving foreign payload
representations and category ownership across execution, then materializing
selected payloads once into final physical buffers. Join-key normalization
must remain separate from opaque payload storage. Narrow numeric payloads
should not require widening/narrowing; categorical payloads should preserve
mapping identity and valid source codes rather than round-trip through a new
dictionary and per-row revalidation. A native bridge needs a supported way to
retain/construct these Polars columns, with explicit ownership and lifetime
rules; feasibility is not yet established. Measure that output boundary in
isolation before committing to a general runtime redesign. Random payload
access on q5 remains real work even after conversion passes are removed.

## Native physical-output experiment

See [build/test instructions](../python/experiments/README.md). The standalone
C++ Release kernel writes original-width payload values and validity directly
into final buffers. Categorical codes remain original Polars IDs; there is no
dictionary reconstruction or ID remapping. Null selection indices produce null
payloads. Inputs and mapping owners remain alive through conversion.

The supported categorical attachment still calls `cat.to`. Polars' checked
2.0.0 plugin FFI transports logical Arrow columns; a supported direct mapping
handle attachment was not established. No internal Rust ABI layout is accessed
or mutated. This experiment therefore tests native physical materialization
plus the remaining public attachment cost, not validation-free categoricals.

Full-input paired results at eight threads (12 samples, two warmups):

| Query | Current ms | Native-output prototype ms | Paired ratio | p | Physical gather ms | Categorical attach ms |
|---|---:|---:|---:|---:|---:|---:|
| q3 | 179.9 | 98.3 | 0.555 | 0.00049 | 18.1 | 35.9 |
| q5 | 381.4 | 401.1 | 1.046 | 0.00928 | 152.5 | 82.9 |

Raw samples (`join-gap-native-output-8t.json`, local raw output). Standalone gather
and attachment measurements are not additive decompositions. The first kernel
used runtime-sized copies; specializing 4/8-byte copies reduced physical gather
from ~26 to ~18 ms on q3 and ~168 to ~153 ms on q5. These are diagnostic
successive runs, not a paired claim about copy specialization.

One-thread control, same full inputs and paired protocol:

| Query | Current ms | Native-output prototype ms | Paired ratio | Physical gather ms | Categorical attach ms |
|---|---:|---:|---:|---:|---:|
| q3 | 499.6 | 397.3 | 0.811 | 126.6 | 68.9 |
| q5 | 867.2 | 1338.1 | 1.524 | 915.1 | 59.4 |

One-thread raw samples (`join-gap-native-output-1t.json`, local raw output); both
comparisons p=0.00049. q3 improves at both budgets, while q5 regresses at both.
The standalone gather times confirm that late random access remains substantial
work even when codes and widths are preserved. These kernel measurements do not
establish an optimum for a native materializer; they measure this implementation.

Checks: complete benchmark outputs match streaming Polars, native tests pass at
1/8 threads for empty/all-null selections, bitmap boundaries, named/default
categories, Float32 bit patterns, source-owner lifetime and invalid indices.
A separately compiled corrupt-gather/bounds-check mutant makes all three tests
fail. Default engine/runtime/plugin behavior is unchanged by this experiment.

q3 supports the small-right-table direction. q5 remains a resolved regression:
the random gather itself is expensive, and validation still costs ~83 ms.
Do not enable this as a general output path. A next architectural experiment
must either preserve categorical ownership through a supported native interface
or fuse payload materialization into the join to avoid a separate random gather;
this prototype establishes neither capability. Keep the public engine unchanged.

## q5 fixed-width control: original, all Int32, all Int64

`python/bench_join_widths.py` runs the current public engine and streaming
Polars in one process, alternating all routes over 12 paired iterations after
two warmups. It checks complete outputs for each representation. Encoding and
input preparation are outside collection timers. Categorical labels become
shared physical IDs; Float32 payloads become exact Int32 IEEE bit patterns,
then widen by sign extension for the Int64 variant. No float values are rounded
or truncated. This is a storage-only join control, not a numeric-arithmetic
benchmark. All variants produce 9M rows from the full q5 inputs.

At eight threads:

| Representation | Ibex collect ms | Polars ms | Native + export ms | Conversion ms | Paired Ibex/Polars |
|---|---:|---:|---:|---:|---:|
| original | 377.90 | 254.25 | 187.26 | 190.21 | 1.496 |
| int32 | 263.31 | 246.97 | 212.89 | 42.86 | 1.074 |
| int64 | 211.00 | 297.87 | 208.62 | 0.27 | 0.710 |

Eight-thread samples (`join-gap-widths-8t.json`, local raw output). Every engine
comparison has p=0.00049. Polars' paired Int64/Int32 ratio is 1.236 (p=0.00049),
while Ibex's is 0.797 (p=0.00049). Numeric-mode native/export times are unresolved
between widths (ratio 0.976, p=0.052); both execute/export Int64 columns.

Numeric Int32 and Int64 use the same adapter path, without left reconstruction.
The original categorical input enables left reconstruction, so original versus
numeric differences include that plan change as well as categorical conversion.
Output physical buffers are 468 MB for Int32 and 936 MB for Int64. Input/output
byte estimates exclude separately owned categorical mapping storage.

One-thread control (same protocol, all full-output checks passed):

| Representation | Ibex collect ms | Polars ms | Native + export ms | Conversion ms | Paired Ibex/Polars |
|---|---:|---:|---:|---:|---:|
| original | 814.91 | 1514.40 | 544.76 | 242.02 | 0.536 |
| int32 | 970.81 | 1475.26 | 680.01 | 199.00 | 0.644 |
| int64 | 640.71 | 1712.93 | 677.63 | 0.22 | 0.376 |

One-thread samples (`join-gap-widths-1t.json`, local raw output). All engine
comparisons p=0.00049. Paired Int64/Int32 ratios: Polars 1.178, Ibex 0.695
(both p=0.00049). Native/export remains unresolved between numeric widths
(ratio 1.019, p=0.470). The conversion stages use immutable prepared results
and differ in cache conditions from collect, so medians need not add exactly.
The lossless encoding was separately round-trip checked with categorical nulls,
Float32 signed zero, NaN, infinity and negative values.

The result supports a native Int32 payload experiment: Polars benefits from
narrower buffers, whereas Ibex currently widens and then narrows them (43 ms
standalone narrowing/conversion here). This does not predict the speed of a
native Int32 build, isolate key width from payload width, or remove the original
categorical mapping/validation cost. Test payload width first with key handling
held fixed before considering a global integer-width change.

Reproduce after a complete Release rebuild, separately with budgets 1 and 8:

```bash
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_join_widths.py --out build-release/join-widths-8t.json
```

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
    --threads 8 --h2o-data J1_1e7_NA_0_0 --out-dir build-release/joincheck
.venv/bin/python python/summarize_polars_engines.py build-release/joincheck
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
use this only for diagnosis. For acceptance, use the paired harness below;
never compare against a cached Polars number.

### Reproducible paired join measurements

The table above is historical, unpaired evidence. Use `python/bench_join_gap.py`
for new measurements. After rebuilding the complete Release tree, run separately:

```bash
PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=1 POLARS_MAX_THREADS=1 taskset -c 0 \
.venv/bin/python python/bench_join_gap.py --out build-release/join-gap-1t.json

PYTHONSAFEPATH=1 PYTHONPATH=build-release/python:python \
IBEX_CORES=8 POLARS_MAX_THREADS=8 taskset -c 0,2,4,6,8,10,12,14 \
.venv/bin/python python/bench_join_gap.py --out build-release/join-gap-8t.json
```

The harness validates complete outputs and dtypes against streaming Polars,
ignoring unspecified join order. It warms each route twice, alternates route
order for 12 paired iterations, and records raw samples, medians, median paired
Ibex/Polars ratios and unadjusted Wilcoxon p-values. These compare engines, not
before/after revisions; runtime changes still require the git A/B gate in
MEASURING.md. Provenance includes binary and adapter hashes, Git status, thread
budget and CPU affinity. Input CSV loading is excluded.

`prepare` uses a fresh engine each time (warm process); `ibex_cached` uses the
public collect path with a prepared query. `native_export` includes Arrow
export; `conversion` reuses an immutable result. The latter two are diagnostic
measurements with different cache conditions, not an additive decomposition.
`--limit 10000 --iters 6` runs a smoke check explicitly marked nonrepresentative
in its JSON. Never use truncated inputs for published ratios.

Tests: `.venv/bin/python python/test_ibex_polars.py` (no pytest in the venv;
`uv run --with pytest python -m pytest -q python/test_polars_conformance.py`
for the conformance suite). Every new path should come with a test that fails
when the path is broken; check by mutating the code.

### Paired remeasurement (2026-11-08, 8 threads)

Full 10M-row inputs; complete outputs and dtypes passed against streaming
Polars. Twelve alternating pairs, two warmups. Times are medians in ms;
ratios are medians of paired ratios, not ratios of medians.

| Query | Cached Ibex | Polars streaming | Prepare | Native + export | Conversion | Paired ratio |
|---|---:|---:|---:|---:|---:|---:|
| q1 | 101.7 | 36.4 | 40.5 | 21.9 | 76.6 | 2.83 |
| q2 | 119.2 | 39.1 | 45.6 | 25.7 | 92.8 | 2.99 |
| q3 | 189.3 | 52.0 | 45.6 | 51.3 | 138.1 | 3.46 |
| q4 | 117.6 | 40.5 | 84.6 | 24.5 | 88.2 | 2.89 |
| q5 | 394.8 | 276.8 | 867.0 | 193.4 | 200.0 | 1.41 |

Raw samples and provenance: 8-thread JSON (`join-gap-20261108-8t.json`, local raw output).
Split timings are standalone diagnostics and need not sum to cached collect.

One-thread control, same protocol and complete-output checks:

| Query | Cached Ibex ms | Polars ms | Paired ratio | Wilcoxon p |
|---|---:|---:|---:|---:|
| q1 | 173.2 | 187.8 | 0.91 | 0.0923 |
| q2 | 246.4 | 203.7 | 1.21 | 0.0049 |
| q3 | 520.7 | 361.6 | 1.44 | 0.0005 |
| q4 | 236.3 | 232.9 | 1.01 | 0.5693 |
| q5 | 862.2 | 1565.2 | 0.55 | 0.0005 |

q1 and q4 are unresolved, not evidence of a speedup or equivalence.
Raw samples and provenance: 1-thread JSON (`join-gap-20261108-1t.json`, local raw output).

### Compact Int32 pass-through (2026-11-10)

With `IBEX_COMPACT_INT32=1`, the adapter binds resident Int32 columns
unchanged. The native Arrow importer adopts the buffer, lowering still types
the column as Int64, and export returns physical Int32, so the final strict
Polars cast has nothing to narrow. Float32 and the smaller integer types still
widen. The h2o `id1`-`id3` keys are Int32.

Each setting ran once, in its own process, with the same code. Compact off
reproduces the HEAD path. Medians in ms (compact off → on); the ratio is the
median paired IbexEngine/Polars-streaming ratio within each run:

| Query | 8t cached | 8t conversion | 8t ratio | 1t cached | 1t ratio |
|---|---:|---:|---:|---:|---:|
| q1 | 106.7 → 101.5 | 79.7 → 77.0 | 2.91 → 2.87 | 190.9 → 179.9 | 1.05 → 0.97 |
| q2 | 114.7 → 106.6 | 98.8 → 85.7 | 2.93 → 2.74 | 262.8 → 230.5 | 1.14 → 1.09 |
| q3 | 169.5 → 148.6 | 124.1 → 95.8 | 3.41 → 2.95 | 511.3 → 500.6 | 1.42 → 1.32 |
| q4 | 121.3 → 103.5 | 92.0 → 75.3 | 3.42 → 2.51 | 229.4 → 175.7 | 1.00 → 0.89 |
| q5 | 392.3 → 362.4 | 202.7 → 188.0 | 1.50 → 1.37 | 904.9 → 780.4 | 0.56 → 0.50 |

- **Preparation:** about 10 ms faster wherever the widening copy is removed
  (q4's stays 85-89 ms).
- **Remaining gap:** the categorical `cat.to` conversion and left-column
  reconstruction above, plus Float32 widening.
- **Status:** compact ingestion is still opt-in. These are sequential runs,
  not an interleaved A/B.
- **Raw output:** `benchmarking/results/local/int32/2026-11-10-h2o-engine/`.

The 2026-11-10 baseline at `5d33f14e` matches the 11-08 paired remeasurement
above at eight threads. Every stage is within noise; q4's higher median comes
from slow repeats, and its fastest runs are unchanged.

One-thread q1/q2 native execution plus export is about 10 ms slower than on
11-08 (q1 fastest run 51.6 → 61.7 ms). That comparison spans two sittings and
two CPUs (0 then 2), and has not been A/B'd.

### Parallel conversion (2026-11-10)

`cat.to` validates every id serially per chunk (`from_cats_and_dtype` in
polars-core), so an eager call runs one thread per column. Polars 2.0.0 has
no Python-reachable unchecked constructor: Arrow and plugin imports rebuild
dictionaries with `from_str_iter`, primitive imports ignore
`_PL_CATEGORICAL2`, `scatter` rejects UInt32 values, `ext.to` needs the exact
storage dtype. Upstream marks the unchecked numeric cast "remove after
exposing to/from physical functions"; an unchecked `from_physical` is the
thing to ask for. Standalone, 9M ids at 8 threads: eager `cat.to` 15.1 ms
(10 categories) / 102.9 ms (10M); the same expression in a streaming query
2.6 / 17.5 ms.

`_engine._to_polars` now (1) rebuilds the left columns on a helper thread
while the right columns convert, when the Polars pool has more than one
thread; (2) maps ids and runs `cat.to` in a streaming query; (3) runs the
left mask filter and the final strict cast in streaming queries. Each step
was a paired `--baseline-engine` A/B against the previous one (12 samples):

| 8 threads | `7f411695` | + overlap | + streaming `cat.to` | + streaming filter/cast | vs Polars |
|---|---:|---:|---:|---:|---:|
| q1 | 98.2 | 66.6 | 62.7 (p 0.34) | 61.1 | 1.82 |
| q2 | 126.0 | 84.5 | 68.7 | 64.4 | 1.84 |
| q3 | 180.2 | 169.8 (p 0.064) | 107.9 | 81.2 | 1.77 |
| q4 | 117.4 | 81.6 | 66.8 | 66.6 | 1.88 |
| q5 | 394.2 | 361.9 | 269.7 | 250.8 | 1.02 |

Unmarked steps have p ≤ 0.016. Columns chain medians from separate runs.
One thread is neutral: every paired ratio for steps 1+2 and step 3 is
0.94–1.02, p ≥ 0.18. Raw output:
`benchmarking/results/local/int32/2026-11-10-conversion/`.

q1/q2/q4's critical path is now the serial numpy order check and mask build
(~18 ms); the native join emitting the mask is the next step.

## Where this fits

- The AWS run of the Polars-engines benchmark
  (`benchmarking/aws/run-polars-engines.sh --on-demand`, paid; needs the
  commit pushed and `AWS_PROFILE=ibex-bench`) has not been launched yet. It
  will publish these join ratios as they stand unless this work lands first.
- `docs/polars-engine.html` and `python/pdsh-performance.md` describe the
  engine; `python/polars-engine-performance.md` has older join measurements
  (prepare times there predate the import improvements: prepare is now
  0.05–0.85 s per join question).
