# Ibex

A statically typed DSL for columnar DataFrame and time-series manipulation,
with a fast parallel interpreter and transpilation to C++23.

See the [website](https://bobjansen.github.io/Ibex/#get-started) for more
information.

Ibex is licensed under the GNU Affero General Public License, version 3 only
(`AGPL-3.0-only`). See [LICENSE](LICENSE) and [LICENSING.md](LICENSING.md),
including the separate MIT attribution for Poorman-derived Ibex test material.
Small pull requests are welcome under the contribution terms in
[CONTRIBUTING.md](CONTRIBUTING.md).

## Start the browser UI from a release

The release archive is self-contained: keep the `bin/`, `bin/ui/`, and
`plugins/` directories together after extracting it. The commands below work
for a new user without Node.js or a source checkout.

On Linux or macOS:

```bash
cd ibex-<version>
IBEX_LIBRARY_PATH="$PWD/plugins" ./bin/ibex ui --data-dir .
```

On Windows PowerShell:

```powershell
cd ibex-<version>
$env:IBEX_LIBRARY_PATH = "$PWD/plugins"
.\bin\ibex.exe ui --data-dir .
```

Add `--demo` to either command to seed synthetic `trades`, `prices`, and
`samples` tables into every session (via the bundled `data_gen` plugin), so you
can run queries immediately without supplying any data.

Open the printed `http://127.0.0.1:8765` address in a browser. The UI assets
are bundled at `bin/ui`; Node.js is only needed when building Ibex from source.
The `--data-dir` directory is the sandbox the UI can read and write. Press
Ctrl+C in the terminal to stop the server.

Notable language features:
  - compact bracket syntax for filtering, selecting, updating, grouping, ordering, joining, and reshaping tables
  - excellent columnar execution, with parallelism for eligible large operations
  - static types and runtime-checked DataFrame<{...}> schemas at data boundaries
  - TimeFrames for rolling, aligned, and resampled time-series analysis
  - typed functions, native extensions, and C++23 transpilation for integration and deployment
  - null-aware expressions, joins, and reshape operations

```
import "csv";

// Declare the columns this example relies on; the CSV may contain others.
let prices = as_timeframe(
    read_csv("prices.csv") as DataFrame<{
        ts: Timestamp,
        symbol: String,
        price: Float64,
        volume: Int64
    }>,
    "ts"
);

// Filter ticks, then resample them into one-minute OHLC bars per symbol.
let ohlc = prices[
    filter price > 1.0,
    resample 1m,
    select { open = first(price), high = max(price), low = min(price), close = last(price) },
    by symbol,
];

// Add derived columns (all existing columns are preserved)
let annotated = prices[update { price_k = price / 1000.0 }];

// Join two tables
let enriched = prices join ohlc on symbol;
```

## Language at a glance

### Inline table construction

Use `Table { ... }` when a small lookup table, test fixture, or derived table is
clearer inline than in a separate file. Each field defines one output column.
The value can be either an array literal or a table expression to extract a
column from:

```
// Small in-memory data, useful for examples and joins
let prices = Table {
    ts     = [ts "2026-01-02T09:30:00Z", ts "2026-01-02T09:31:00Z", ts "2026-01-02T09:32:00Z"],
    symbol = ["AAPL", "GOOG", "MSFT"],
    price  = [150.0, 140.0, 300.0],
    volume = [1000, 2000, 1500]
};

// Build a new table from columns produced by existing pipelines
let quote_view = Table {
    ts       = prices[select { ts }],
    symbol   = prices[select { symbol }],       // single-column result
    price    = prices[select { price }],
    volume   = prices[select { volume }],
    notional = prices[select { notional = price * volume }]
};

// A multi-column expression is matched by the field name
let copied = Table {
    ts     = prices,
    symbol = prices,
    price  = prices,
    volume = prices
};

// Mix literals and expression-backed columns freely
let enriched = Table {
    ts     = prices[select { ts }],
    symbol = prices[select { symbol }],
    tier   = ["mega", "search", "software"],
    price  = prices[select { price }],
    volume = prices[select { volume }]
};

// Inline tables are ordinary DataFrames
let tf = as_timeframe(
    Table { ts = [1000, 2000, 3000], price = [10, 20, 30] },
    "ts"
);
```

### Aggregation

```
// Mean price per symbol
prices[select { mean_price = mean(price) }, by symbol];
```

### Update (add / replace columns)

```
// Add derived columns — all existing columns are preserved
prices[update { price_doubled = price * 2.0 }];
```

### Distinct

```
// Unique symbols
prices[distinct symbol];

// Unique (symbol, price) pairs
prices[distinct { symbol, price }];
```

### Order

```
// Order by a single key (ascending by default)
prices[order symbol];

// Order by multiple keys with explicit directions
prices[order { symbol asc, price desc }];

// Order by all columns (schema order)
prices[order];
```

### Head / Tail

```
// First 10 rows in the current order
prices[head 10];

// First n rows using a scalar binding
let n = 10;
prices[head n];

// Top 3 rows per species after sorting
prices[order { price desc }, head 3, by symbol];

// Last 10 rows in the current order
prices[tail 10];

// Bottom 3 rows per species after sorting
prices[order { price desc }, tail 3, by symbol];
```

### Rank

```
// Dense descending rank within each symbol
scores[
    update { dense_rank = rank(score, method = dense, ascending = false) },
    by symbol
];

// Multi-key ranking with an explicit tie-break
scores[
    update { order_rank = rank(order { score desc, ts asc }, method = first) },
    by symbol
];
```

### Filter With Lag / Lead

```
// Numbers that appear at least three times consecutively
let n = 1;
logs[order id][
    filter num == lag(num, n) && num == lag(num, n + 1)
][
    distinct { ConsecutiveNums = num }
];
```

### Scalar extraction

```
let total = scalar(prices[select { total = sum(price) }], total);
```

### Joins

```
let enriched = prices join ohlc on symbol;
let with_meta = prices left join metadata on symbol;
```

### Rename

```
// Rename columns — schema updated, data unchanged
prices[rename { price -> close, volume -> shares }]
```

### Grouped update (window-function equivalent)

`update + by` evaluates each expression per group and **broadcasts the result
back to every row in the group** — analogous to a SQL window function with
`PARTITION BY` and no `ORDER BY` frame:

```
// Attach group mean to every row (no row reduction)
prices[update { group_mean = mean(price) }, by symbol]
```

### Tuple assignment

When an extern function returns multiple columns, destructure them with a
parenthesised tuple on the left-hand side of an assignment:

```
extern fn compute_greeks(p: Float64) -> DataFrame from "greeks.hpp";

trades[update { (delta, gamma) = compute_greeks(price) }]
```

### Function table contracts

User-defined functions can already require a minimum input table schema with
`DataFrame<{ ... }>` parameter types. Declared columns must exist with the
right types; extra columns are allowed:

```
fn top_two_salaries(df: DataFrame<{ salary: Int64 }>) -> DataFrame effects {} {
    df[distinct { salary }, order { salary desc }, head 2];
}
```

This is the shape to build reusable dataframe helpers on. See
`examples/function_table_contracts.ibex` for a runnable example.

### Null handling

Ibex uses SQL-style **three-valued logic (3VL)**. Each column carries an
Arrow-style validity bitmap; null propagates through arithmetic, comparisons,
and value functions (*null in → null out*), except the functions built to
handle null (`coalesce`, `fill_null`, `null_if_nan`/`null_if_not_finite`).
Use `is null` / `is not null` to test for nulls explicitly:

```
import "csv";

let emp  = read_csv("employees.csv");
let dept = read_csv("departments.csv");

// Left join — unmatched rows get null dept_name
let enriched = emp left join dept on dept_id;

// Filter using IS NULL / IS NOT NULL
enriched[filter { dept_name is null }]       // employees with no department
enriched[filter { dept_name is not null }]   // employees with a known department

// Arithmetic null propagation — bonus is null when budget is null
enriched[select { name, bonus = salary + budget }]

// 3VL OR: true OR null = true; null OR false = null
enriched[filter { dept_name is not null || salary > 80000 }]
```

### TimeFrame and rolling windows

A `TimeFrame` is a `DataFrame` with a designated `Timestamp` index, always
sorted in ascending order. Rolling window operations are **time-based** (not
row-count based) and use duration literals:

```
import "csv";

let prices = read_csv("prices.csv");
let tf = as_timeframe(prices, timestamp);

// 5-minute rolling mean
tf[window 5m, update { ma5 = rolling_mean(price) }]

// Lag / lead (positional shifts, no window clause needed)
tf[update { prev_price = lag(price, 1) }]

// Resample to 1-minute OHLC bars
tf[resample 1m, select {
    open  = first(price),
    high  = max(price),
    low   = min(price),
    close = last(price),
}]

// As-of join two TimeFrames on time index
let tf2 = as_timeframe(read_csv("quotes.csv"), timestamp);
tf asof join tf2 on timestamp
```

### Reshape: melt and dcast

`melt` unpivots a wide DataFrame to long format; `dcast` pivots it back:

```
// Wide → long (unpivot): symbol is the id column; open/high/low/close become rows
let long = ohlc[melt symbol]
// Columns: symbol | variable | value

// Restrict which measure columns to unpivot
ohlc[melt symbol, select { open, close }]

// Multiple id columns
ohlc[melt { symbol, date }]

// Long → wide (pivot): variable column becomes new column names
long[dcast variable, select value, by symbol]
// Columns: symbol | open | high | low | close
```

### Matrix Operations

Ibex treats any all-numeric DataFrame as a column-major matrix and provides
four operations for linear-algebra style computation. Non-numeric columns are
silently dropped for `cov`, `corr`, and `matmul`; `transpose` requires
homogeneous column types.

```
// Covariance matrix of numeric columns
let numeric = prices[select { open, high, low, close }]
numeric[cov]
// Result: column: String, open: Float64, high: Float64, low: Float64, close: Float64

// Pearson correlation matrix (diagonal = 1.0)
numeric[corr]

// Transpose — one optional String/Categorical column names the output columns;
// if absent, output columns are r0, r1, …
let labeled = prices[select { symbol, open, close }]
labeled[transpose]
// Result columns: column: String, AAPL: Float64, MSFT: Float64, ...

// Matrix multiply: (m×k) × (k×n) → (m×n)
// Column names of the right-hand table become the output column names
let weights = Table { w = [0.6, 0.4] }
matmul(prices[select { open, close }], weights)
```

### Model Specification

Ibex provides an R-style formula syntax for fitting regression models directly
inside a query bracket. The `model` clause builds a design matrix automatically
— numeric columns pass through, categorical (String) columns are dummy-encoded
using treatment coding, and interaction/crossing operators work as in R.

```
// OLS regression: close ~ open + volume
let m = prices[model { close ~ open + volume }];

// Accessor functions extract detailed results
let coefs = model_coef(m);        // coefficient table
let stats = model_summary(m);     // std errors, t-stats, p-values
let yhat  = model_fitted(m);      // fitted values
let e     = model_residuals(m);   // residuals
let r2    = model_r_squared(m);   // R² as a scalar table

// Dot notation: regress on all other columns
prices[model { close ~ . }]

// No intercept
prices[model { close ~ open - 1 }]

// Interaction (open:volume) and crossing (open * volume ≡ open + volume + open:volume)
prices[model { close ~ open * volume }]

// Filter + model in one bracket
prices[filter volume > 1000000, model { close ~ open + high + low }]

// Ridge regression with L2 penalty
prices[model { close ~ open + volume, method = ridge, lambda = 0.1 }]

// Weighted least squares
prices[model { close ~ open, method = wls, weights = w }]
```

Built-in methods: `ols` (default), `ridge` (requires `lambda`), `wls` (requires
`weights`). The `method =` parameter duck-types any function with signature
`(X: Table, y: Table) -> Table`, so user-defined estimators work too.

### Scalar functions

```
// Math
df[update { log_ret = log(price / lag(price, 1)) }]
df[update { vol = sqrt(variance) }]
df[update { notional = abs(pnl) }]

// Date / time extraction
df[update { yr = year(date), mo = month(date), dy = day(date) }]
df[update { hr = hour(timestamp) }]
```

### rep()

Create constant columns or repeat values across all rows:

```
// Boolean mask column
df[update { is_live = rep(true) }]

// Constant tag
df[update { source = rep("backtest") }]

// Repeat column values (R-style)
df[update { rep2 = rep(price, each=2) }]
```

### Writing output

```
import "csv";
import "json";
import "parquet";

let rows_written = write_csv(result, "output.csv");
write_json(result, "output.json");
write_parquet(result, "output.parquet");
```

### Vectorized RNG

Generate columns of random draws in a single pass — one independent value per
row, no row-by-row overhead:

```
// Gaussian noise column
df[update { noise = rand_normal(0.0, 1.0) }]

// Uniform weights, biased coin, die roll
df[update {
    w    = rand_uniform(0.0, 1.0),
    flip = rand_bernoulli(0.7),
    die  = rand_int(1, 6),
}]
```

All eight distributions are supported:

| Function | Distribution | Output |
|---|---|---|
| `rand_uniform(low, high)` | Uniform[low, high) | Float64 |
| `rand_normal(mean, stddev)` | Normal | Float64 |
| `rand_student_t(df)` | Student-t | Float64 |
| `rand_gamma(shape, scale)` | Gamma | Float64 |
| `rand_exponential(lambda)` | Exponential | Float64 |
| `rand_bernoulli(p)` | Bernoulli → 0 or 1 | Int64 |
| `rand_poisson(lambda)` | Poisson | Int64 |
| `rand_int(lo, hi)` | Uniform integer [lo, hi] | Int64 |

Ibex uses a thread-local `xoshiro256++` scheme:

- **4-wide path** (`xoshiro256++ x4`): four independent streams in SoA layout
  (`s[word][lane]`), seeded from one base seed plus fixed offsets. Used by
  `rand_uniform`, `rand_normal` (Marsaglia polar method), `rand_exponential`,
  `rand_bernoulli`, and `rand_int`. Portable — identical output with or without
  AVX2; AVX2 auto-vectorizes the state update loops for extra throughput.
- **Scalar path** (`xoshiro256++`): single stream, satisfies
  `UniformRandomBitGenerator`. Used by `rand_student_t`, `rand_gamma`, and
  `rand_poisson` (fed through `std::distributions`).

This keeps parallel queries lock-free and reproducible when reseeded.

## R and dplyr

The in-repo `ibex` package includes a lazy dplyr backend for in-memory R and
nanoarrow tables. Supported verbs remain an immutable Ibex plan until a
terminal operation executes it:

```r
library(dplyr)
library(ibex)

query <- ibex_tbl(trades, fallback = "error") |>
  filter(price > 10) |>
  mutate(notional = price * size) |>
  group_by(symbol) |>
  summarise(total = sum(notional), .groups = "drop") |>
  arrange(desc(total))

show_query(query)
result <- collect(query)
```

Captured R scalars cross as typed bindings, not interpolated source.
Unsupported expressions use an explicit `"warn"`, `"error"`, or `"collect"`
fallback policy; after a permitted fallback, execution stays in local dplyr.
Arbitrary R closures never run as Ibex worker kernels. The package compatibility
matrix documents the exact native verb, type, null, grouping, and ordering
contract.

See [`examples/ibex_dplyr.R`](examples/ibex_dplyr.R) for a complete runnable
example with scalar capture, lazy translation, grouped aggregation, and
collection back into R.

## IPython and Jupyter

Ibex now has a Python bridge that returns `pyarrow.Table` objects and an
IPython extension for notebook-style workflows. The intended split is simple:
keep joins, filters, aggregations, and reshaping in Ibex; use Python for plots,
notebooks, and downstream ML.

Build the bridge first:

```bash
cmake --build build-release --parallel --target ibex_pyarrow
uv sync
```

Then start IPython from the repository root and load the extension:

```python
%load_ext ibex_ipython
```

Inline cell example with a pandas table binding and a Python scalar binding:

```python
import pandas as pd

trades = pd.DataFrame({
    "symbol": ["AAPL", "AAPL", "MSFT"],
    "qty": [10, 15, 7],
    "px": [101.2, 101.5, 299.8],
})
offset = 10
```

```python
%%ibex --bind trades=trades --bind offset=offset --as pandas --out grouped
trades[
    select { total_qty = sum(qty + offset), avg_px = mean(px) },
    by symbol,
    order symbol
];
```

The result is stored in `grouped` as a pandas `DataFrame`. Use `--as pyarrow`
to keep the result as a `pyarrow.Table`, or `%ibexfile` to run a checked-in
`.ibex` script directly:

```python
%ibexfile --as pyarrow --out result python/plot_ibex_pyarrow_demo.ibex
```

Notebook cells are stateful by default. A `%%ibex` cell can define a table once
and later cells can reuse it without rerunning the load:

```python
%%ibex --quiet
import "csv";
let train = read_csv("../../kaggle/data/train.csv", "<empty>");

%%ibex --as pandas --out bucket_summary
train[select { rows = count() }, by seconds_in_bucket, order seconds_in_bucket];
```

Reset the hidden Ibex session with:

```python
%ibexreset
```

Supported magic options:

- `--bind ibex_name=python_var` to pass pandas/pyarrow tables or Python scalar values into Ibex
- supported scalar bindings currently include `int`, `float`, `bool`, `str`, `datetime.date`, and `datetime.datetime`
- Python `datetime.date` maps to Ibex `Date`, and `datetime.datetime` maps to Ibex `Timestamp`
- `--as pyarrow|pandas` to control the returned result type
- `--out var_name` to choose the output variable name
- `--quiet` to suppress immediate display
- `%ibexreset` to clear persisted Ibex notebook bindings

This is the fastest way to get Ibex into a notebook today without inventing a
separate plotting system or a full standalone kernel.

## Benchmark

**Reproducible and auditable by design.** The interactive results (Ibex vs
Polars, DuckDB, ClickHouse, DataFusion, pandas, data.table and dplyr, 1M–50M
rows, with peak-memory) live on the
[benchmarks page](https://bobjansen.github.io/Ibex/benchmarks.html), and the
exact code every engine runs for every query is on the
[methodology page](https://bobjansen.github.io/Ibex/methodology.html) —
extracted from the harness source, so what's shown matches what ran.

Every competing engine is a stock install (PyPI / CRAN) and the whole suite is
one script, so anyone can re-run it end-to-end:

```bash
benchmarking/run_scale_suite.sh --warmup 1 --iters 3   # -> benchmarking/results/scales.csv
# the published numbers come from a clean AWS r7i.2xlarge box:
./benchmarking/aws/run.sh --on-demand
```

Know a faster way to write one of these queries? **Open a PR against
[`benchmarking/`](benchmarking/) and the numbers get re-run and updated.**
Improvements to any engine's queries are welcome — the aim is an accurate
comparison.

## How to run the benchmarks

`ibex+parse` includes text parsing and IR lowering; the overhead is negligible
so runs are repeated for both.

For scalability runs across dataset sizes (1M, 2M, 4M, 8M, 16M, 32M, 64M rows):

```bash
benchmarking/run_scale_suite.sh --warmup 1 --iters 3
```

For the default Ibex-vs-Polars comparison path, use:

```bash
benchmarking/run_scale_ibex_vs_polars.sh --warmup 1 --iters 3
```

Use this as the primary scale comparison when checking for performance drift.
Run the full `run_scale_suite.sh` matrix when you need the broader framework
picture for release notes or README refreshes.

Results are written per size under `benchmarking/results/scales/<rows>/` and
combined into:
- `benchmarking/results/scales.tsv`
- `benchmarking/results/scales.csv`

Both include a `dataset_rows` column.

To skip only specific frameworks:

```bash
benchmarking/run_scale_suite.sh --skip-pandas --skip-dplyr
```

To guard Ibex against regressions relative to the 4M-row README snapshot:

```bash
benchmarking/run_scale_regression.sh
```

This reruns the Ibex-only benchmark suite on 4,000,000 rows and fails if any
query slows down by more than the configured tolerance versus the `ibex`
column in `README.md`. Use `--allowed-regression-pct <X>` to relax or tighten
the threshold.

To analyze where ibex is faster/slower than polars and data.table:

```bash
./build-release/tools/ibex --plugin-path build-release/tools
ibex> :load benchmarking/analyze_scales.ibex
```

## Building

Requirements: CMake 3.26+ and a C++23 compiler such as Clang 17+, GCC 13+,
AppleClang, or MSVC 2022. Ninja is recommended on Linux and macOS; CMake's
Visual Studio generator works on Windows.

```bash
# Debug with Clang or GCC (with sanitizers and Parquet)
cmake -B build -G Ninja \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug \
  -DIBEX_ENABLE_SANITIZERS=ON \
  -DIBEX_BUILD_PARQUET=ON \
  -DIBEX_BUILD_ADBC=OFF \
  -DIBEX_BUILD_PYTHON_BRIDGE=OFF
cmake --build build
ctest --test-dir build --output-on-failure

# Release with Clang or GCC and Parquet
cmake -B build-release -G Ninja \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Release \
  -DIBEX_BUILD_PARQUET=ON \
  -DIBEX_BUILD_ADBC=OFF \
  -DIBEX_BUILD_PYTHON_BRIDGE=OFF
cmake --build build-release

# Windows, from a Developer PowerShell
cmake -B build-release -DCMAKE_BUILD_TYPE=Release -DIBEX_BUILD_PARQUET=ON -DIBEX_BUILD_ADBC=OFF -DIBEX_BUILD_PYTHON_BRIDGE=OFF
cmake --build build-release --config Release
```

The bundled Parquet plugin fetches and builds Apache Arrow automatically. The
ADBC plugin is optional and additionally requires an ADBC C/C++ driver manager
(`adbc.h` and `libadbc_driver_manager`), so the standard build leaves it off.
To enable it with conda-forge:

```bash
mamba create -n ibex-adbc -c conda-forge adbc-driver-manager
mamba activate ibex-adbc
cmake -B build-release -DIBEX_BUILD_ADBC=ON -DCMAKE_PREFIX_PATH="$CONDA_PREFIX"
```

On supported Debian/Ubuntu releases, Apache Arrow's APT repository provides
the equivalent `libadbc-driver-manager-dev` package. The ADBC driver manager
only supplies the API: install an appropriate ADBC database driver separately.
The experimental Python bridge is independent of the native plugins and needs
a working Python interpreter plus development headers. The commands above turn
it off to keep a native build from failing on systems without those headers. To
enable it, pass `-DIBEX_BUILD_PYTHON_BRIDGE=ON` and, if needed,
`-DIBEX_PYTHON_EXECUTABLE=/path/to/python`.

### Build Options

| Option                      | Default | Description                        |
|-----------------------------|---------|------------------------------------|
| `IBEX_WARNINGS_AS_ERRORS`   | `OFF`   | Treat compiler warnings as errors  |
| `IBEX_ENABLE_LTO`           | `OFF`   | Link-time optimization (Release)   |
| `IBEX_ENABLE_SANITIZERS`    | `OFF`   | ASan + UBSan (Debug only)          |
| `IBEX_BUILD_TESTS`          | `ON`    | Build Catch2 test suite            |
| `IBEX_BUILD_TOOLS`          | `ON`    | Build REPL binary                  |
| `IBEX_BUILD_EXAMPLES`       | `ON`    | Build example programs             |
| `IBEX_BUILD_PARQUET`        | `ON`    | Build the bundled Parquet plugin   |
| `IBEX_BUILD_ADBC`           | `OFF`   | Build the ADBC plugin (requires the system driver manager) |
| `IBEX_BUILD_PYTHON_BRIDGE`  | `ON`    | Build the experimental pyarrow Python bridge |
| `IBEX_USE_CCACHE`           | `ON`    | Use ccache if found, to speed up rebuilds |

## Architecture

```
ibex/
├── include/ibex/          Public headers
│   ├── core/              Column<T>, DataFrame<Schema>
│   ├── ir/                Typed IR nodes (Scan, Filter, Project, Aggregate)
│   ├── parser/            Lexer, recursive-descent parser
│   ├── runtime/           Extern function registry, execution engine
│   └── repl/              Interactive REPL session
├── src/                   Implementation files (mirrors include/)
├── libs/                  Bundled plugin sources (csv.hpp, csv.cpp -> csv.so)
├── scripts/               Helper shell scripts (build, run, plugin-build)
├── tests/                 Catch2 unit tests
├── tools/                 CLI binaries (REPL, compiler, benchmark)
├── examples/              Usage examples
└── cmake/                 Build system modules
```

### Module Boundaries

| Module    | Responsibility                               | Dependencies        |
|-----------|----------------------------------------------|----------------------|
| `core`    | Columnar storage (`Column<T>`, `DataFrame`)  | None                 |
| `ir`      | Typed intermediate representation nodes       | `core`               |
| `parser`  | Source text → IR tree                         | `ir`                 |
| `runtime` | Extern function registry, execution           | `core`               |
| `repl`    | Interactive read-eval-print loop              | `parser`, `runtime`  |

## Design Goals

- **Static typing**: Schema-level type safety for columns and DataFrames
- **Relational IR**: Clean separation between parsing and execution via a typed IR layer
- **C++ interop**: Register external C++ functions for use within Ibex queries
- **Zero-copy where possible**: `std::span`-based access to columnar data
- **Modern C++23**: Concepts, `std::expected`, `std::variant`, RAII, no raw `new`/`delete`

## Running the REPL

```bash
# With the bundled CSV plugin
IBEX_LIBRARY_PATH=./build-release/tools ./build-release/tools/ibex

# Or pass the plugin directory explicitly
./build-release/tools/ibex --plugin-path ./build-release/tools
```

### Local browser UI

Start the local workbench with:

```bash
./build-release/tools/ibex ui --data-dir ./data
```

It prints a loopback URL (by default `http://127.0.0.1:8765`). The UI keeps one
REPL session per browser session, shows tables and schemas in an Environment
pane, supports `Ctrl/Cmd+Enter` to run the editor, and pages result rows so a
query never sends an unbounded result set to the browser. The bundled UI is
included in release archives; Node.js/npm is only required when building it
from source. On Linux, the UI is
confined with Landlock to `--data-dir`; when that option is omitted, its data
access is confined to the directory from which it was launched. Configured
plugin directories remain readable (but not writable) so bundled imports such
as `import "parquet";` continue to work. Providing `--data-dir` also makes it
the UI process's working directory, so query paths may be relative to that
directory.

### REPL Commands

```
:help                    Show REPL commands
:tables                  List available tables
:scalars                 List scalar bindings and values
:functions               List user and extern function signatures
:imports                 List imported libraries and extern origins
:schema <table>          Show column names and types
:head <table> [n]        Show first n rows (default 10)
:peek <expr>             Evaluate and compactly display an expression
:describe <table> [n]    Schema + first n rows
:doc <name>              Show docs/signature for a binding or built-in
?name                    Shorthand for :doc <name>
:source <fn>             Show source for a user-defined function
:load <file>             Load and execute an .ibex script
:comments [on|off]       Toggle/force printing script comments during :load
:timing [on|off]         Toggle/force command timing output
:time <command>          Time exactly one command
```

Tab completion is enabled when Ibex is built with `readline` available on the
system (e.g. `libreadline-dev` on Debian/Ubuntu). It completes REPL commands,
`:load` file paths, table/scalar/function names, and column names inside
`table[...]` expressions.
Readline builds also persist command history to `~/.ibex_history` by default.
Use `IBEX_HISTORY_FILE`, `--history-file <path>`, or `--no-history` to override
that behavior.

## Plugins

Ibex data-source functions (e.g. `read_csv`, `read_json`, `read_parquet`) are
**I/O backends** — host-linked implementations or shared plugins registered
with the runtime when a script imports their library declarations.

When the REPL encounters:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
```

it looks for `csv.so` in the plugin search path and calls its
`ibex_register(ExternRegistry*)` entry point to register the function.

Parquet is linked directly into the standard CLI, REPL, and Python hosts as a
first-party backend. `import "parquet"` remains the portable activation
boundary; a thin `parquet.so` compatibility shim is also built for embedding
hosts that use dynamic plugins.

**Bundled backends:**

| Backend | Functions | Format |
|--------|-----------|--------|
| `csv`  | `read_csv`, `write_csv` | RFC 4180 CSV with type inference |
| `json` | `read_json`, `write_json` | JSON array-of-objects, JSON-Lines, single object |
| `parquet` | `read_parquet`, `write_parquet` | Apache Parquet, including HTTPS and `s3://` reads |
| `adbc` | `read_adbc` | Optional ADBC/Arrow driver-manager source plugin |
| `kafka` | `kafka_recv`, `kafka_recv_avro`, `kafka_send` | Optional Kafka streaming plugin for JSON and Schema-Registry-backed Avro |
| `udp`  | `udp_recv`, `udp_send` | JSON-over-UDP streaming |
| `websocket` | `ws_recv`, `ws_connect`, `ws_send`, `ws_listen` | JSON-over-WebSocket streaming: server source/sink plus client mode for external feeds |

Parquet reads can target public HTTPS URLs, presigned URLs, and S3-compatible
object storage. HTTPS URLs require no client cloud setup. S3 credentials come
from the standard AWS SDK chain, and URI query parameters can set options such
as `region` or `endpoint_override`:

```
import "parquet";
let public_prices = read_parquet("https://data.example.com/prices.parquet");
let prices = read_parquet("s3://market-data/prices.parquet?region=us-east-1");
```

Use `import` to load a plugin without explicit `extern fn` declarations:

```ibex
import "json";
let df = read_json("data.json");
write_json(df, "output.json");
```

`csv.so` also supports optional null-spec, delimiter, header, and schema
arguments. Use `import "csv"` for the normal case:

```ibex
import "csv";
let df = read_csv("examples/data/null_metrics.txt", "<empty>,NA");
```

The equivalent explicit declaration is:

```ibex
extern fn read_csv(
    path: String,
    nulls: String = "",
    delimiter: String = ",",
    has_header: Bool = true
) -> DataFrame from "csv.hpp";

let df = read_csv("examples/data/null_metrics.txt", nulls = "<empty>,NA");
```

`<empty>` marks empty fields as null; additional comma-separated tokens are
also treated as null.

`json.so` reads JSON arrays of objects, JSON-Lines (one object per line), or a
single JSON object. Type inference follows the same priority as CSV: Int64,
Float64, Bool, String. Missing keys and JSON `null` values produce null
bitmaps.

`adbc.so` is optional and built with `-DIBEX_BUILD_ADBC=ON` when an installed
ADBC driver manager is available. It exposes:

```ibex
import "adbc";
let df = read_adbc("adbc_driver_sqlite", "", "select 1 as x");
```

The 4th optional argument is a `;` or newline-separated `key=value` string.
Prefix keys with `db.`, `conn.`, or `stmt.` to target database, connection, or
statement options, and use `entrypoint=...` to override the driver entrypoint
symbol.

`kafka.so` is optional and built with `-DIBEX_BUILD_KAFKA=ON` when
`librdkafka` development files are available. It exposes live Kafka streaming
sources and a JSON sink:

```ibex
import "kafka";

let tick = kafka_recv(
    "localhost:9092",
    "ticks",
    "ibex-demo",
    "ts:timestamp,symbol:str,price:f64,size:i64",
    "poll_timeout_ms=100;consumer.auto.offset.reset=latest;consumer.session.timeout.ms=6000"
);
```

For Redpanda or another Schema Registry-compatible broker, the plugin also
exposes an Avro receive path:

```ibex
import "kafka";

let tick = kafka_recv_avro(
    "localhost:19092",
    "ticks_avro",
    "ibex-demo-avro",
    "ts:timestamp,symbol:str,venue:str,price:f64,size:i64",
    "http://localhost:18081",
    "poll_timeout_ms=100;consumer.auto.offset.reset=latest;consumer.session.timeout.ms=6000"
);
```

The receive schema is explicit and required for both JSON and Avro. Use
`name:type` entries separated by commas, with types from `int`, `f64`, `bool`,
`str`, `cat`, `date`, and `timestamp`. Consumer options use
`poll_timeout_ms=...` plus any `consumer.<key>=<value>` Kafka config.
Producer options use `flush_timeout_ms=...` plus `producer.<key>=<value>`.
`kafka_recv_avro(...)` additionally requires a Schema Registry base URL.

For a streaming pipeline, wrap `kafka_recv(...)` inside `Stream { ... }`. Each
Kafka JSON message becomes a one-row table. Idle polls return `StreamTimeout`,
so the stream stays live rather than terminating on temporary inactivity.
`poll_timeout_ms` is only the consumer wait interval between polls; it is not
an overall runtime limit for the stream.

The current Avro path is intentionally narrow:
- flat records only
- writer schema fetched by schema ID from Schema Registry
- no schema references yet
- no null-valued unions yet

### Kafka demo with Redpanda

The repo includes a small Docker Compose demo under
[`demo/kafka/`](./demo/kafka/) that starts a single-node Redpanda broker, its
Schema Registry, and synthetic tick producers for both JSON and Avro topics.
The JSON producer emits messages of the form:

```json
{"ts":1713474000000000000,"symbol":"AAPL","venue":"XNAS","price":172.53,"size":900}
```

Start the demo stack with:

```bash
demo/kafka/demo-kafka.sh
```

Then, in another terminal, start either the JSON dashboard streams:

```bash
demo/kafka/run-kafka-dashboard.sh
```

or the Avro dashboard streams:

```bash
demo/kafka/run-kafka-avro-dashboard.sh
```

The JSON dashboard consumes two live websocket feeds:
- `ws://127.0.0.1:8765` for grouped trade summaries
- `ws://127.0.0.1:8766` for 5-second OHLC bars

The Avro dashboard uses:
- `ws://127.0.0.1:8775` for grouped trade summaries from `ticks_avro`
- `ws://127.0.0.1:8776` for 5-second OHLC bars from `ticks_avro`

Open either dashboard at
[`demo/kafka/ws_dashboard.html`](./demo/kafka/ws_dashboard.html) or
[`demo/kafka/ws_dashboard_avro.html`](./demo/kafka/ws_dashboard_avro.html), or
watch the same feeds in the terminal with:

```bash
python3 demo/kafka/ws_client.py
python3 demo/kafka/ohlc_ws_client.py
python3 demo/kafka/ws_client.py --port 8775
python3 demo/kafka/ohlc_ws_client.py --port 8776
```

The demo uses `consumer.auto.offset.reset=latest` so it behaves like a live
feed instead of replaying historical ticks, and `consumer.session.timeout.ms=6000`
so restarting the Ibex process does not leave the next run waiting through a
long consumer-group rebalance.

To tail producer activity directly, use:

```bash
docker compose -f demo/kafka/docker-compose.yml logs -f tick-producer
```

### Writing your own plugin

1. Create a header (`my_source.hpp`) that implements your function returning
   `ibex::runtime::Table`.

2. Create a registration file (`my_source.cpp`):

```cpp
#include "my_source.hpp"
#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("my_source", [](const ibex::runtime::ExternArgs& args) {
        // ...
    });
}
```

3. Compile it with the helper script:

```bash
scripts/ibex-plugin-build.sh my_source.cpp
# Produces: my_source.so next to my_source.cpp
```

4. Use it from Ibex:

```
extern fn my_source(path: String) -> DataFrame from "my_source.hpp";
let df = my_source("data/file.bin");
```

### Helper scripts

| Script | Description |
|--------|-------------|
| `scripts/ibex-plugin-build.sh <src.cpp> [-o out.so]` | Compile a plugin `.cpp` into a loadable `.so` |
| `scripts/ibex-build.sh <file.ibex> [-o output]` | Transpile an `.ibex` file and produce a binary |
| `scripts/ibex-run.sh <file.ibex> [-- args...]` | Transpile, compile, and run an `.ibex` file |

All scripts respect `IBEX_ROOT`, `BUILD_DIR`, and `CXX` environment overrides.

### Performance: jemalloc

The ibex runtime allocates and frees large column buffers (often 32–256 MB) on
every query. With the default glibc allocator, each such free returns the
physical pages to the OS via `munmap`; the next allocation re-faults them,
adding 30–100 ms of page-fault overhead per operation at multi-million-row
scale.

`ibex-build.sh` and `ibex-run.sh` automatically detect and link jemalloc
(`libjemalloc.so.2`) when it is installed. jemalloc pools freed extents and
reuses their physical pages, reducing that overhead to near zero after the first
call.  `ibex-run.sh` additionally sets
`MALLOC_CONF=dirty_decay_ms:30000,muzzy_decay_ms:30000` at run time, which
tells jemalloc to retain pages for 30 seconds before returning them to the OS —
long enough for any interactive or pipeline session to benefit, while still
releasing idle memory eventually.

**Installing jemalloc (Debian / Ubuntu):**

```bash
sudo apt install libjemalloc-dev
```

**Manual transpiler pipelines:** if you invoke `ibex_compile` and `clang++`
directly rather than through the helper scripts, add jemalloc to your link
command and set the environment variable before running:

```bash
clang++ -std=c++23 ... my_program.cpp libibex_runtime.a ... \
    /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 -o my_program

MALLOC_CONF=dirty_decay_ms:30000,muzzy_decay_ms:30000 ./my_program
```

If jemalloc is unavailable the binary still runs correctly with the default
allocator; only the warm-iteration performance degrades.

## Editor Support

### VS Code

Syntax highlighting for `.ibex` files is included in `editors/vscode/`.

**Install:**

```bash
# Linux / WSL — copy to the Windows VS Code extensions directory
cp -r editors/vscode /mnt/c/Users/<username>/.vscode/extensions/ibex-language-0.1.0

# macOS / Linux native VS Code
cp -r editors/vscode ~/.vscode/extensions/ibex-language-0.1.0
```

Fully restart VS Code after copying. `.ibex` files will be highlighted automatically.

**Highlights:** keywords (`filter`, `select`, `by`, …), clause operators, type names, built-in functions (`mean`, `rolling_sum`, …), duration literals (`1m`, `5s`), backtick-quoted column names, strings, and comments.
