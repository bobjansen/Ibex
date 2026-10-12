# Resident analytics through the Polars Engine

The prototype runs unchanged upstream q01/q06 Polars queries and preserves a
measured Ibex strength on resident tables: grouped mean by symbol and day.
This supports continuing the Engine experiment, with a bounded compatibility
scope. It does not establish a general advantage over Polars.

```python
import polars as pl
from ibex_polars import IbexEngine

engine = IbexEngine()
query = trades.lazy().group_by("symbol", "day").agg(avg_price=pl.mean("price"))
result = query.collect(engine=engine)
```

Reuse the Engine and immutable query. `engine.prepare(query)` optionally moves
plan translation/input adaptation out of collection; every collection still
executes the complete query. There is no result cache and no Polars fallback.

## Local Release results, 2026-10-12

All three routes start with the same resident categorical Polars table.
The workload follows `prices_multi`: seed 43, 252 symbols, four day strings,
rounded Float64 prices, 1,008 groups. Initial generation/encoding is excluded;
Ibex preparation, first collection and warm collection are reported separately.
The baseline is Polars 2.0.0's streaming engine with categorical keys.

| Rows | Cores | Ibex Engine | Independent Ibex API | Polars | Polars / Engine |
|---:|---:|---:|---:|---:|---:|
| 1M | 1 | 2.39 ms | 1.97 ms | 5.77 ms | 2.41x |
| 1M | 8 | 1.62 ms | 1.16 ms | 3.02 ms | 1.87x |
| 16M | 1 | 26.39 ms | 25.76 ms | 85.90 ms | 3.26x |
| 16M | 8 | 9.43 ms | 8.89 ms | 14.52 ms | 1.54x |

These are medians of twelve matched iterations, balanced over all six execution
orders after two warmups. Each process has matched CPU affinity and Ibex budget;
the one-core reference sets `POLARS_MAX_THREADS=1`, and the eight-core reference
uses its default within affinity, which selects eight threads. Engine output
conversion to a real Polars DataFrame is included. Independent output remains
Arrow-backed, so its boundary is cheaper.

The Engine beats Polars in all twelve pairs at each size/core combination
(two-sided signed-rank p=0.000488). Compared with the independent API, Engine
overhead is measurable at 1M rows; the 16M comparisons are inconclusive at this
sample count (p=0.151 at one core, p=0.077 at eight). Output names, types and
values match Polars within `1e-12` relative / `1e-9` absolute tolerance. Ibex
results match byte-for-byte after canonical ordering across core counts.

## Preparation still matters

| 16M resident rows | Engine preparation | Engine first collection | Polars first collection |
|---|---:|---:|---:|
| 1 core | 148.28 ms | 44.47 ms | 84.74 ms |
| 8 cores | 152.45 ms | 26.73 ms | 20.02 ms |

No disk input is involved. Preparation adapts resident buffers/dictionaries to
Ibex, validates them, and retains native ownership. Numeric payloads and
categorical codes can share buffers; fragmented data and incompatible layouts
can require copies. These inputs have one chunk per column. Validation and
boundary work still cost time even when payloads are borrowed.

Adding preparation and first collection makes the initial Engine query slower
in both cases. A simple cold-plus-warm model crosses over at about three total
queries on one core and thirty-three on eight; those counts are local estimates,
not a promise for other inputs. First-collection order is Engine, independent,
Polars: native initialization is charged to Engine, so the independent route's
first collection is not a fresh-process cold comparison.

The integration exposed a native aggregate read calling mutable `.data()` on
borrowed storage, causing concurrent detachment in the 16M parallel case.
Const reads fix the crash and preserve external buffers. The new native
regression fails when that fix is reverted. The existing owned-input `multi`
suite was compared before/after using twelve interleaved repeats; the effects
remain unclear (p=0.622, 0.151, 0.470, 0.077 across its four queries). This
focused sample is not proof that there is no performance regression.

## Reproduce and next work

```bash
cmake --build build-release -j 6
PYTHONPATH=build-release/python:python IBEX_CORES=8 taskset -c 0-7 \
  .venv/bin/python python/bench_polars_engine.py --rows 16000000 \
  --out /tmp/polars-engine.json
```

[Raw samples, source hashes and native A/B results](../benchmarking/results/python_polars_engine_20261012.json)
record the Release build at `97de43bd` plus the uncommitted Engine/native fix.
The upstream checkout used for conformance is
`401908a307f133a2dffbe3f5e0e0fb2b50ea310b`: q01/q06 pass on SF-1 resident
and SF-10 Parquet inputs, with one native execution per query.

Prefer the Engine experiment next: users retain real Polars query construction,
schema descriptors and result containers, while Ibex's grouped-mean advantage
survives the boundary. Further wins should come from exposing existing native
strengths and improving preparation/native execution. Priority follow-ups are
joins/windows selected from benchmark evidence, reusable sources across
different query objects, first-use allocations and a maintainable visitor
version policy. The current private visitor pin remains the principal
integration risk; full API compatibility and standalone packaging remain open.

## Categorical output follow-up

After commit `d6c79ef3`, the output boundary was changed to preserve native
Arrow dictionaries for columns declared Categorical. Previously the lowerer's
logical string schema caused those columns to be decoded into strings before
Polars encoded them again. String output still receives the declared String
type, and named category mappings retain their dtype correction.

| Warm full output boundary | Before, 1 core | After, 1 core | Before, 8 cores | After, 8 cores |
|---|---:|---:|---:|---:|
| Grouped mean: 1,008 rows, two keys + mean | 0.142 ms | 0.071 ms | 0.419 ms | 0.185 ms |
| Projection: 1M rows, two categorical keys + price | 32.71 ms | 16.37 ms | 20.90 ms | 9.25 ms |

These measurements isolate the complete Python output boundary; native query
execution, Arrow export and input preparation are excluded. They reuse the
same immutable native outputs across thirty matched pairs, alternating order.
Every categorical pair improves in both configurations (signed-rank p=0.00000173
using the repository's normal approximation for thirty pairs). Numeric/String
control cases show no statistically distinguishable changes in this sample.
Numeric payloads remain shared, but categorical import can still validate/remap
codes; dictionary preservation does not imply a zero-copy categorical result.

Checks cover nulls, sliced/chunked arrays, different dictionary orders, empty
outputs, named category mappings and native projection/grouping. Both new
regression tests fail when the previous string round trip is restored. The
Engine suite now has fifteen tests; all four Python CTest suites pass.

```bash
cmake --build build-release -j 6
PYTHONPATH=build-release/python:python IBEX_CORES=8 taskset -c 0-7 \
  .venv/bin/python python/bench_polars_result.py --iters 30 \
  --out /tmp/categorical-output.json
```

[Matched samples and source hashes](../benchmarking/results/python_categorical_output_20261012.json)
retain the old conversion function as a baseline against the revised boundary.
