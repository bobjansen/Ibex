# Independent lazy Python frontend (experimental)

For unchanged real Polars lazy queries using `.collect(engine=IbexEngine())`,
see the [Polars Engine prototype](ibex_polars/README.md) and
[Engine comparison](polars-engine-performance.md).

The `ibex` module under this directory is an independent frontend with an
initial subset of the Polars 2.0 API. Polars is not a runtime dependency.
Query construction creates immutable expressions and pipeline steps;
`collect()` sends the complete pipeline to the shared Ibex evaluator once.
The current subset runs the unchanged upstream q06 and q01 query bodies.

Build the native extension using a Python installation with development
headers and PyArrow installed:

```bash
cmake -S . -B build-release -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release \
  -DIBEX_BUILD_PYTHON_BRIDGE=ON -DIBEX_BUILD_PARQUET=ON \
  -DIBEX_PYTHON_EXECUTABLE="$PWD/.venv/bin/python"
cmake --build build-release --target ibex_pyarrow --parallel 4
PYTHONPATH=build-release/python:python .venv/bin/python python/test_ibex_lazy.py
```

If CMake has cached headers from a different interpreter, also set
`Python3_INCLUDE_DIR` to the selected interpreter's include directory.
This is a source-tree prototype; standalone wheels and distribution naming
are follow-up work.

With `build-release/python:python` on `PYTHONPATH`:

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
endpoint currently holds the GIL during evaluation.

For in-memory frames, `.lazy()` prepares and retains a native input once.
Int64/Float64 Arrow payloads retain shared ownership; the current importer
copies other layouts. Repeated strings are dictionary encoded using Ibex's
existing promotion threshold, and large-UTF8 offsets are normalized once.
Subsequent collections reuse the input representation and execute the query
again. This is input preparation, not a result cache. Treat input buffers as
immutable for the lifetime of the frame. Preparation and first-use costs are
reported separately in the performance note below.

## Initial compatibility scope

| Area | Supported |
|---|---|
| Input | A single local Parquet file, or `DataFrame(dict_or_arrow_table).lazy()` |
| Expressions | `col`, `lit`, aliases, numeric arithmetic, comparisons, Boolean `&`, `\|`, `~`, `is_between`, numeric `sum`/`mean`, `first`/`last`, non-null column `count`, row count `len` |
| Pipelines | `filter`, `select`, one expression per `with_columns`, column-key `group_by(...).agg(...)`, column-key `sort`, `collect`, source-only `explain` |
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

Joins, selectors, computed group/sort keys, nested/list expressions, multi-expression
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

First/last reductions preserve the selected edge row's null by default;
`ignore_nulls=True` selects the first/last present value instead. The default
uses native `first_null`/`last_null`; the null-skipping variant uses native
`first`/`last`. Empty global reductions return one null row; empty grouped
reductions return no groups. Null-skipping Date reductions remain unsupported.
The new native reducers currently materialize their input.

## Check the actual upstream PDS-H query

The optional conformance runner needs Polars 2.0 and the upstream benchmark's
Python dependencies, checkout and generated data. It invokes the original
`queries.polars.q6.q` function twice, using real Polars and Ibex. Only the
query module's `pl` binding and explicit lazy input change; upstream files
and the reference engine remain intact.

```bash
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  .venv/bin/python python/check_pdsh_q06.py \
  --pdsh-root ../polars-benchmark \
  --lineitem ../polars-benchmark/data/tables/scale-1.0/lineitem.parquet
```

The runner checks output names, shape, Arrow dtypes and revenue within a
relative tolerance of `1e-12`, plus exactly one native invocation. It measures
correctness, not performance. Native tests run through CTest as
`ibex_python_lazy`; they also check operation with Polars imports blocked.

Check q01 over Parquet or in-memory input:

```bash
PYTHONPATH=build-release/python:python IBEX_CORES=1 POLARS_MAX_THREADS=1 \
  .venv/bin/python python/check_pdsh_q01.py \
  --pdsh-root ../polars-benchmark \
  --lineitem ../polars-benchmark/data/tables/scale-1.0/lineitem.parquet \
  --in-memory
```

Omit `--in-memory` to use lazy Parquet scans. The runner compares all output
columns and types, including UInt32 counts and Float64 means. See
[q01 performance measurements](q01-performance.md) for the release-build
in-memory comparison, input preparation and first-use limitations.

For a workload where the benchmark page shows a strong Ibex win, see
[grouped mean by symbol and day](grouped-mean-performance.md): the direct
`import ibex as pl` translation, a runnable example, and fresh resident-table
measurements against both string and categorical reference keys.
