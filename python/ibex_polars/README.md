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

## Scope and preparation

Supported operations include resident DataFrame scans, single local Parquet
files, filters, projections, simultaneous row-local column updates, numeric
sums/means, row counts, column-key grouping and sorting. Supported resident
types are Int64, Float64, Boolean, String, Date and Categorical. Numeric
coercion to Float64 and null predicates are supported. Joins, windows, UDFs,
computed grouping/sort keys, floating-point grouping keys, aggregate broadcasting, explicit slice/top-k
nodes, remote/multiple files, Enum, other numeric widths and nondefault scan
policies are outside this slice. An already-sliced resident source is valid.
Background, batch, async and sink execution are not implemented.

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
