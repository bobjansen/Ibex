# `import ibex as pl`: independent lazy frontend (experimental)

This package is less developed than the
[Polars engine](../ibex_polars/README.md), which runs real Polars code and is
the recommended way to use Ibex from Python.

`ibex` is an independent frontend with an initial subset of the Polars 2.0
API. Polars is not a runtime dependency. Query construction creates immutable
expressions and pipeline steps; `collect()` sends the complete pipeline to the
shared Ibex evaluator once. The current subset runs the unchanged upstream
PDS-H q06 and q01 query bodies. It ships in the same `ibex-polars` wheel; see
[the Python README](../README.md#install) to install it.

## Usage

```python
from datetime import date
import ibex as pl

query = (
    pl.scan_parquet("lineitem.parquet")
    .filter(pl.col("l_shipdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
    .filter(pl.col("l_discount").is_between(0.05, 0.07))
    .filter(pl.col("l_quantity") < 24)
    .with_columns((pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue"))
    .select(pl.sum("revenue"))
)
print(query.explain())
result = query.collect()
print(result.item())
arrow_table = result.to_arrow()
```

`explain()` shows the generated source with typed scalar placeholders; it
does not read the file or execute a query. `collect()` validates input schema
using metadata and executes through `ibex_pyarrow.execute_pipeline`. Literal
values and file paths cross as typed bindings, never interpolated source.
Each collection reads its source again; results are not cached. Parquet input
decoding uses Ibex's lazy reader, not a Polars/PyArrow data read. The native
endpoint currently holds the GIL during evaluation. A standalone
`parquet::write(...)` as the pipeline result returns the number of rows written
as a Python integer.

Python table expressions use the script executor's source planning, including
streaming Parquet units and deferred join probes. Typed scalar bindings remain
available inside scan predicates. Prefix statements still execute in order;
this does not merge statements across a materialized binding. Embedders can
opt in through `ReplSession::ExecuteOptions::plan_table_expressions`; ordinary
session execution keeps its existing behavior.

For in-memory frames, `.lazy()` prepares and retains a native input once.
Int64/Float64 Arrow payloads retain shared ownership; the current importer
copies other layouts. Repeated strings are dictionary encoded using Ibex's
existing promotion threshold, and large-UTF8 offsets are normalized once.
Subsequent collections reuse the input representation and execute the query
again. This is input preparation, not a result cache. Treat input buffers as
immutable for the lifetime of the frame. Preparation and first use cost extra on
the first collection.

## Initial compatibility scope

| Area | Supported |
|---|---|
| Input | A single local Parquet file, or `DataFrame(dict_or_arrow_table).lazy()` |
| Expressions | `col`, `lit`, aliases, numeric arithmetic, comparisons, Boolean `&`, `\|`, `~`, `is_between`, numeric `sum`/`mean`, `first`/`last`, non-null column `count`, row count `len` |
| Pipelines | `filter`, `select`, one expression per `with_columns`, column-key `group_by(...).agg(...)`, column-key `sort`, `unique`, `slice`, `head`, `tail`, `gather_every`, Python row slicing, equality/cross `join`, `collect`, source-only `explain` |
| Results | Ibex `DataFrame`/`Series`, columns, shape, item, column lookup, `to_list`, `to_dict`, `to_arrow`, `to_pandas` |
| Types | Int64, Float64, Bool, strings, Date; sums preserve Int64/Float64 and return zero for empty/all-null input; mean is Float64; row-count output is UInt32 |

For an in-memory grouped query:

```python
summary = (
    pl.DataFrame({"symbol": ["A", "A", "B"], "qty": [1, 2, 3]})
    .lazy()
    .group_by("symbol")
    .agg(total=pl.sum("qty"), average=pl.mean("qty"), rows=pl.len())
    .sort("symbol")
    .collect()
)
```

Result `schema` and Series `dtype` currently expose Arrow types. These are
not Polars dtype descriptors. Result containers implement only the operations
above, not the full Polars frame/series API. Columns with unsupported types
are rejected when used; in-memory inputs reject unsupported types at `.lazy()`.
Backticks, newlines, NUL and `${` in names are rejected by this renderer.

Selectors, computed group/sort keys, nested/list expressions, multi-expression
updates, reduction broadcasting, Python UDFs, literal-only projections and
`collect(engine=...)` are not implemented. No operation falls back to Polars.
Unsupported methods/options fail; implemented APIs reject unsupported
expression shapes before native execution. Grouping supports scalar reductions
and preserves first-occurrence group order when requested. Sorting supports
per-key directions and null positions, with stable ties. Full Polars
compatibility is not yet claimed. Floating-point comparisons follow Polars:
NaN equals NaN and compares greater than every other number, including positive
infinity. Null comparisons still return null. Sorting also places NaN above
other numeric values, with null placement controlled by `nulls_last`.

`col("x").count()` counts non-null values and returns UInt32, including zero
for empty/all-null inputs. NaN and false Boolean values are present values and
are counted. `len()` counts all rows. Computed row expressions also support
`.count()`; both forms execute in the same native pipeline.

Column-expression `.len()` includes nulls and returns UInt32; for supported
row expressions its result equals the frame row count. Polars exposes this
through the same visitor aggregate as `.count()`, with `include_nulls=True`.

First/last reductions preserve the selected edge row's null by default;
`ignore_nulls=True` selects the first/last present value instead. The default
uses native `first_null`/`last_null`; the null-skipping variant uses native
`first`/`last`. Empty global reductions return one null row; empty grouped
reductions return no groups. Null-skipping Date reductions remain unsupported.
The new native reducers currently materialize their input.

`unique(subset=None, keep="any", maintain_order=False)` removes duplicate rows
or selects representatives by column-name subset keys while retaining all
columns. The `first`, `last`, and `none` modes are also supported; `any` chooses
the first row. Results retain selected rows in input order. Nulls and NaNs
compare equal within duplicate groups, and signed zeros compare equal.
Empty subsets remain unsupported.

Row selection supports `.slice(offset, length=None)`, `.head(n)`, `.tail(n)`,
`.gather_every(n, offset=0)`, and Python indexing such as `[2:10:3]` or `[::-1]`.
A temporary native `seq()` column supplies zero-based positions; bounds resolve
after preceding operations, and the helper column is removed from results.
Python indexing uses exclusive stops and supports negative bounds and steps.
The Polars-style `.slice` uses offset/length semantics and clips the requested
interval at the frame boundaries. Zero steps and negative lengths are errors.
This initial lowering materializes positions rather than skipping input chunks.

`join(other, on=..., how=...)` supports inner, left, right, full, semi, anti,
and cross joins. Use `left_on`/`right_on` for differently named column keys;
composite keys, suffixes, null matching and key coalescing are supported.
`maintain_order` accepts the Polars order policies; semi/anti support left-side
ordering only. Nondefault `validate`, computed keys, as-of and non-equi joins
remain unsupported. Every joined pipeline executes in one native call.

## Conformance runners

`check_pdsh_q06.py` and `check_pdsh_q01.py` run the original upstream
`polars-benchmark` query functions twice, through real Polars and through
this package, changing only the query module's `pl` binding. They need
Polars 2.1, a [polars-benchmark](https://github.com/pola-rs/polars-benchmark)
checkout and its generated data:

```bash
python python/check_pdsh_q06.py \
  --pdsh-root ../polars-benchmark \
  --lineitem ../polars-benchmark/data/tables/scale-1.0/lineitem.parquet
```

`check_pdsh_q01.py` takes the same arguments plus `--in-memory`. Both compare
output names, dtypes and values with Polars and check exactly one native
invocation; they measure correctness, not speed. Native tests run through
CTest as `ibex_python_lazy`.
