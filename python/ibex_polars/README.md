# Ibex Engine for Polars (experimental)

Use real Polars expressions and lazy frames, then execute the complete supported
pipeline in Ibex and receive a real Polars DataFrame:

```python
import polars as pl
from ibex_polars import IbexEngine

engine = IbexEngine()
query = trades.lazy().group_by("symbol", "day").agg(avg_price=pl.mean("price"))
result = query.collect(engine=engine)
```

The source-tree prototype uses the same native extension as the independent
API; follow the [build instructions](../README.md). Install `polars==2.0.0`
and put `build-release/python:python` on `PYTHONPATH`. There is no standalone
wheel yet. Imports and upstream q01/q06 query bodies remain unchanged; select
the Engine at collection. `pl.Config.set_engine_affinity(engine)` can select
it as the default for lazy collection.

Polars constructs and optimizes the logical plan. A version-pinned adapter
translates that plan to the shared Ibex evaluator. Filters, arithmetic,
aggregation and sorting execute in Ibex. Polars supplies source column handles
and converts the output into its public container/dtypes. Unsupported plans
raise before native binding/execution; there is no automatic fallback.

Column `count()` counts non-null values, while `pl.len()` counts all rows.
Both return UInt32 and return zero for empty input. NaN and false Boolean
values count as present. Computed row-expression counts are also supported.

Column-expression `.len()` includes nulls and returns UInt32; for supported
row expressions its result equals the frame row count. Polars exposes this
through the same visitor aggregate as `.count()`, with `include_nulls=True`.

`pl.repeat(value, pl.len())` and `pl.repeat(value, row_expression.len())`
fill the current frame with a supported scalar literal, including typed nulls.
These execute through native scalar broadcasting. Optimizer-generated repeats
and their shared length columns are supported. Explicit literal lengths,
non-scalar values, and lengths that might change the frame height remain
unsupported. Untyped Null and other unsupported literal dtypes remain rejected.

First/last reductions preserve the selected edge row's null by default;
`ignore_nulls=True` selects the first/last present value instead. The default
uses native `first_null`/`last_null`; the null-skipping variant uses native
`first`/`last`. Empty global reductions return one null row; empty grouped
reductions return no groups. Null-skipping Date reductions remain unsupported.
The new native reducers currently materialize their input.

`set_sorted(...)` is accepted as a pass-through hint: it preserves row order
and adds no sorting operation. Its declared sortedness is not yet forwarded to
native planning metadata; Ibex uses its existing ordering proofs or general
algorithms. Other `MapFunction` variants remain unsupported.

`unique(subset=None, keep="any", maintain_order=False)` is supported with
column-name subset keys and `keep="any"`, `"first"`, `"last"`, or `"none"`.
It retains all input columns and selects whole rows, including null payloads.
Null keys compare equal; repeated NaNs form one duplicate group, and signed
zeros compare equal. `keep="any"` chooses the first representative; output is
stable even when ordering is not requested. Subset/last/none and floating-key
cases currently use materialized native grouping. Empty subsets remain unsupported.

Row `slice(offset, length)`, `head`, `tail`, uniform `gather_every` projections
and reverse projections use a temporary native `seq()` column and positional
filters. Negative bounds resolve against the current stage's height; reverse
slices order the selected positions descending. Slices folded into sorting,
distinct, and scalar grouping are supported. Temporary columns are removed.
Mixed per-column row selections and nondefault scan-limit policies remain
unsupported. Polars' own LazyFrame indexing restrictions still apply before
translation; the independent API additionally supports full Python slicing.

Equality joins support `inner`, `left`, `right`, `full`, `semi`, and `anti`;
`cross` joins are supported too. Keys must be scalar column names (including
composite keys). Null matching, suffixes, key coalescing and requested ordering
are adapted to Polars semantics; NaNs match NaNs and signed zeros compare equal.
Explicit ordering uses native row positions and a final sort. Semi/anti support
`none`, `left`, and `left_right` ordering. Both inputs and all later operations
execute within one native pipeline call, with separate prepared input bindings.

As-of/non-equi joins, computed keys, and nondefault cardinality validation are
unsupported. The pinned visitor omits the validation option, so preparation
serializes the original plan metadata once to reject unsupported assertions.
This includes resident input serialization and can be a substantial one-time
cost; prepared collections cache the plan/bindings. Join benchmarks report
preparation separately from hot execution/export timings. Polars scheduling
hints are not forwarded to native scheduling.

## Scope and preparation

Supported operations include resident DataFrame scans, single local Parquet
files, filters, projections, simultaneous row-local column updates, numeric
sums/means, first/last reductions, non-null column counts, row counts,
frame-sized scalar repeats, distinct rows, row slices, equality/cross joins,
column-key grouping and sorting. Supported resident
types are Int8, Int16, Int32, Int64, UInt8, UInt16, Float32, Float64, Boolean,
String, Date and Categorical. Resident Datetime and Duration columns are also
supported as described below. Numeric
coercion to Float64 and null predicates are supported. Windows, UDFs,
computed grouping/sort keys, floating-point grouping keys, aggregate broadcasting other than row counts, expression top-k
operations, remote/multiple files, Enum, other numeric widths and nondefault scan
policies are outside this slice. An already-sliced resident source is valid.
Background, batch, async and sink execution are not implemented.
Struct and list columns are intentionally outside the project scope.

Narrow integers are Python-boundary types, not new native runtime variants.
Resident inputs widen once to Int64 during preparation; Parquet decoding
already uses Int64 storage. Arithmetic wraps modulo `2**bits` whenever the
Polars planner declares a narrow integer result, using its signed or unsigned
range. Wrapping happens before later comparisons, casts, divisions and
reductions, and results recover the declared Polars dtype at export. Sums of
Int8, Int16, UInt8 and UInt16 promote to Int64 and do not wrap at the input
width; Int32 sums retain Int32 wrapping. Other mixed expressions also follow
the planner's result dtype. Safe widening casts and explicit integral casts
with `wrap_numerical=True` are supported. Checked or null-producing casts that
could overflow their destination range remain unsupported. UInt32 and UInt64
inputs remain unsupported, for both resident tables and Parquet scans.

Float32 inputs similarly widen once to Float64 for execution, including
Parquet input. All arithmetic and reductions use Float64 intermediates; only
declared Float32 output columns are downcast at the public result boundary.
There is no emulation of intermediate Float32 rounding. This is an intentional
compatibility difference: cancellation, intermediate overflow/underflow, and
predicates can differ from Polars even when the final dtype is Float32. For
example, a Float32 input `x = 2**24` followed by `y = x + 1` and `y - x` yields
`1.0` here and `0.0` with Polars' intermediate Float32 rounding. Floating-point
group keys and explicit narrowing casts from Float64 to Float32 remain
unsupported. Float32-to-Float64 widening casts are supported.

Resident Datetime and Duration columns use Int64 nanosecond ticks internally,
with their logical units and timestamp timezones retained in the plan schema
and restored at export. Conversion to nanoseconds is checked; values outside
the signed Int64 nanosecond range are explicitly unsupported (approximately
1677–2262 for timestamps, or ±292 years for durations). Nulls and nanosecond
column precision are preserved without Python datetime conversion.

The temporal subset covers projection/aliases, null checks, comparisons,
filters, sorting, grouping/row counts, min/max, and duration sums. Casts to finer
units are supported. Precision-losing unit casts, timezone changes, temporal
arithmetic/means, calendar operations and dynamic/rolling groups remain
unsupported. Some mixed-unit comparisons insert a precision-losing cast in
Polars and therefore also reject. Duration sums use native Int64 arithmetic
and must remain within its nanosecond range.

Millisecond/microsecond timestamp literals and exact duration literals are
supported. Nanosecond timestamp literals reject explicitly because the pinned
Polars visitor converts them to microsecond-resolution Python datetimes.
Temporal Parquet scans are also explicitly unsupported for now: the native
reader needs checked temporal rescaling and duration/zone handling first.
There is no fallback to a Polars or PyArrow data read.

Reuse the same Engine and lazy query for repeated analytics. Its bounded cache
(32 plans by default) retains translated plans and prepared resident inputs,
never results. Every collection executes again. Cache entries distinguish
optimization flags, are removed when the query is released, and are evicted
when the limit is reached. Separate lazy query objects currently prepare
inputs separately. `IbexEngine(cache_size=0)` disables retention.

`engine.prepare(query)` performs plan translation and input preparation without
executing the analytical query. It is optional; the first collection does it
automatically. Polars projection pushdown limits preparation to demanded
columns. Supported numeric Arrow buffers and categorical codes share input
ownership; string layouts, fragmented arrays and other layouts can require
copying. Categorical dictionary normalization changes the small dictionary,
while UInt32 codes are viewed as Int32 without copying the row-sized buffer.
Plain strings may be encoded once with Ibex's existing heuristic. Preparing
an in-memory table means adapting resident column buffers to Ibex, not reading
data from disk. Input buffers must remain immutable.

Parquet plans retain paths, not loaded table contents. Every collection rereads
the file using Ibex; cached schemas assume the file's schema remains stable.
Schema changes require a new query/Engine. Polars may read file metadata during
planning. `engine.explain(query)` shows the generated Ibex source and prepares
inputs; Polars' own `query.explain(engine=engine)` shows its optimized plan.
Floating-point comparisons and sorting translate Polars' NaN total ordering.
Native evaluation currently holds the GIL; concurrent use is serialized during
preparation/execution.

The implementation depends on private plan visitor APIs and requires exactly
Polars 2.0.0 and visitor version `(15, 2)`. Other versions fail explicitly. This
pin is an integration risk to resolve before distributing a general backend;
it is not a claim of full Polars compatibility.

Categorical result columns retain their Arrow dictionaries when passed to
Polars. This avoids decoding them into strings and recreating categories.
Polars may still validate/remap codes, and named category mappings receive
the required dtype correction. Columns declared as String retain String output.

## Validation and measurements

The [upstream compatibility harness](../conformance/README.md) runs pinned,
unmodified Polars tests and distinguishes supported queries, explicit
rejections, mismatches, and tests that never exercise Ibex. It is the growing
compatibility target; existing engine tests also cover ownership and execution
boundaries that upstream tests do not check.

```bash
PYTHONPATH=build-release/python:python .venv/bin/python python/test_ibex_polars.py
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  .venv/bin/python python/check_pdsh_engine.py \
  --pdsh-root ../polars-benchmark \
  --lineitem ../polars-benchmark/data/tables/scale-1.0/lineitem.parquet --in-memory
```

The runner invokes original upstream q01 and q06 without replacing imports or
expressions. It compares column types, values and ordered output against Polars
and verifies one native execution per query. Omit `--in-memory` for Parquet.
The focused suite checks null/empty behavior, simultaneous updates, category
types, caching, ownership, demand projection and failures without fallback.
CTest registers `ibex_polars_engine` when the pinned dependency is available.

See [the Engine comparison](../polars-engine-performance.md) for resident
grouped-mean timings, including preparation and first collection.

Matched join timings, preparation costs, raw samples and reproduction commands
are in the [join performance comparison](../join-performance.md).
