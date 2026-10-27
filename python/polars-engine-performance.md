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

## Categorical results keep Polars' category ids

On the h2o join set a Categorical column of 10M distinct labels cost about
1.1 s to hand back to Polars, against 15 ms for Polars itself. The output
boundary above avoids decoding rows, but a dictionary still had to be interned
label by label (about 85 ns each) because Ibex's dictionary has no relation to
Polars' category ids.

Resident Categorical inputs now enter Ibex as one dictionary chunk whose
entries remember the physical id Polars gave each label
(`python/ibex_polars/_categories.py`). A result chunk whose dictionary equals a
recorded one maps its codes to ids with a gather, and every Categorical column
of the result is converted in one Polars query. Any other dictionary (a Parquet
column, labels built inside a query) takes the previous label path. Columns
whose physical type is not UInt32 and all-null columns are not recorded.
Ibex copies the dictionary on export, so recognition is by length and then
Arrow equality (13 ms for 10M labels), not by buffer identity.

Building the unified input also replaces Polars' per-chunk Arrow export and
Ibex's merge of those chunks, so `prepare()` drops too.

h2o, 1e7 rows, 8 cores, fresh `IbexEngine` per question, three interleaved
rounds of `run_h2o_adapter.py`, best of four iterations, every result compared
with Polars 2.0 streaming. Ratio is candidate time over previous time.

| dataset | question | before | after | ratio | `prepare()` before → after |
|---|---|---|---|---|---|
| J1 joins | q1 small inner | 1190 ms | 300 ms | 0.25 | 28.9 → 25.0 s |
| J1 joins | q2 medium inner | 1232 ms | 327 ms | 0.27 | 28.4 → 24.6 s |
| J1 joins | q3 medium left | 1349 ms | 406 ms | 0.30 | 27.0 → 23.3 s |
| J1 joins | q4 medium on factor | 1186 ms | 309 ms | 0.26 | 27.0 → 23.1 s |
| J1 joins | q5 big inner | 3004 ms | 713 ms | 0.24 | 55.6 → 47.0 s |
| G1 2e0 | q3 sum+mean by id3 | 529 ms | 216 ms | 0.41 | 2.5 → 0.4 s |
| G1 2e0 | q7 max-min by id3 | 536 ms | 211 ms | 0.39 | 2.6 → 0.4 s |
| G1 2e0 | q10 six keys | 880 ms | 508 ms | 0.58 | 3.4 → 0.7 s |
| G1 1e2 | geometric mean of 7 | | | 0.94 | |

Joins are still 8 to 10 times slower than Polars. The join is about 30 ms
natively; what remains is the export (a Polars `cat.to` check of the ids and
the gather) and `prepare()`. `G1 2e0 q2` reads 1.5 times slower in the rounds,
but each process lands on either 6.3-6.6 ms or 9.2-9.5 ms for both versions
(six processes each), so that is per-process variance, not this change.

Two regression tests (`test_categorical_results_reuse_the_ids_polars_assigned`
and `test_categorical_ids_are_not_taken_from_a_look_alike_dictionary`) cover
the fast path, nulls, multi-chunk input, equal-length dictionaries with
different labels, and the fallback; the first fails on all five of its cases
with the fast path disabled.

## Join validation no longer decodes the resident data

`prepare()` for a join over resident data cost 25 s for the 10M-row h2o join
set, against 40 ms for Polars to run the join. Visitor 15.2 does not expose a
join's `validation`, so `Adapter._check_join_validation` serialized the whole
plan to JSON and decoded it to read that one enum. A resident DataFrame
serializes as its Arrow IPC bytes written as a JSON array of numbers, so the
plan was several hundred megabytes of text: 6 s to produce and 17 s to decode
into Python integers.

The plan text is now scanned instead of decoded: the join-validation enum
names are matched, and their number must equal the number of `Join` nodes,
otherwise the query is refused rather than left unchecked. Data cannot spell
a match (it is only ever numbers), and a column that happens to be called
`validation` has a dtype name for its value, which is not matched.

| `prepare()`, h2o J1 1e7 rows | before | after |
|---|---|---|
| q1-q4 | 23.1-25.0 s | 7.8-8.6 s |
| q5 | 47.0 s | 16.1 s |

What remains is mostly `serialize_json` itself (6.2 s of 8.4 s), which is
Polars'; removing it needs the visitor to report the validation. A substring
search in place of the regex was measured and is no faster. Queries over
Parquet scans carry no data in the plan and are unaffected; the 22 PDS-H
queries still pass the full-result comparison with Polars (SF 10).

`test_join_validation_is_read_without_decoding_the_resident_data` fails on the
previous implementation ("plan decoded") and passes now. It also checks a
column named `validation`, values that spell the pattern, and that a
`validate="m:1"` join is still refused.
