# Ibex

A statically typed DSL for columnar DataFrame manipulation, with a fast
interpreter and transpiliable to C++23.

See the [website](https://bobjansen.github.io/Ibex/#get-started) for more
information.

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

let prices = read_csv("prices.csv");

// Filter, then group-by aggregation
let ohlc = prices[
    filter price > 1.0,
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

Build a `DataFrame` directly from column vectors. Each column may be an inline
array literal **or** any expression that produces a table:

```
// Columns from array literals
let t = Table {
    symbol = ["AAPL", "GOOG", "MSFT"],
    price  = [150.0, 140.0, 300.0],
    volume = [1000, 2000, 1500],
};

// Columns from existing table expressions
let summary = Table {
    symbol = prices[select { symbol }],
    high   = prices[select { high = max(price) }, by symbol],
    low    = prices[select { low  = min(price) }, by symbol],
};

// Mix literals and expressions freely
let ref = Table {
    label = ["a", "b", "c"],
    value = some_df[select { value }],
};

// Promote to TimeFrame via as_timeframe
let tf = as_timeframe(
    Table { ts = [1000, 2000, 3000], price = [10, 20, 30] },
    "ts"
);
```

All columns must have the same row count. For array literals, all elements must
share the same type (`Int64`, `Float64`, `Bool`, `String`, `Date`, `Timestamp`).
For expression columns, the expression must produce a single-column table or a
table containing a column named after the definition.

### Load and filter

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

let iris = read_csv("data/iris.csv");

// Filter rows, select columns
iris[filter `Sepal.Length` > 5.0, select { Species, `Sepal.Length` }];
```

### Aggregation

```
// Mean sepal length per species
iris[select { mean_sl = mean(`Sepal.Length`) }, by Species];
```

### Update (add / replace columns)

```
// Add derived columns — all existing columns are preserved
iris[update { sl_doubled = `Sepal.Length` * 2.0 }];
```

### Distinct

```
// Unique species values
iris[distinct `Species`];

// Unique (Species, Sepal.Length) pairs
iris[distinct { `Species`, `Sepal.Length` }];
```

### Order

```
// Order by a single key (ascending by default)
iris[order `Species`];

// Order by multiple keys with explicit directions
iris[order { `Species` asc, `Sepal.Length` desc }];

// Order by all columns (schema order)
iris[order];
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
iris[rename { `Sepal.Length` -> sepal_length, `Sepal.Width` -> sepal_width }]
```

### Grouped update (window-function equivalent)

`update + by` evaluates each expression per group and **broadcasts the result
back to every row in the group** — analogous to a SQL window function with
`PARTITION BY` and no `ORDER BY` frame:

```
// Attach group mean to every row (no row reduction)
iris[update { group_mean = mean(`Sepal.Length`) }, by Species]
```

### Tuple assignment

When an extern function returns multiple columns, destructure them with a
parenthesised tuple on the left-hand side of an assignment:

```
extern fn compute_greeks(p: Float64) -> DataFrame from "greeks.hpp";

trades[update { (delta, gamma) = compute_greeks(price) }]
```

### Null handling

Ibex uses SQL-style **three-valued logic (3VL)**. Each column carries an
Arrow-style validity bitmap; null propagates through arithmetic and
comparisons. Use `is null` / `is not null` to test for nulls explicitly:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

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
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";

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
extern fn write_csv(df: DataFrame, path: String) -> Int from "csv.hpp";
extern fn write_json(df: DataFrame, path: String) -> Int from "json.hpp";
extern fn write_parquet(df: DataFrame, path: String) -> Int from "parquet.hpp";

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

## Benchmark

4 M rows, release build (`-O2`), 15 iterations, 2 warmup, WSL2 / clang++.

| query                        |      ibex |   polars |    pandas | data.table |     dplyr |
| ---------------------------- | --------: | -------: | --------: | ---------: | --------: |
| mean_by_symbol               |   27.4 ms |  23.3 ms |  174.2 ms |    21.4 ms |   44.0 ms |
| ohlc_by_symbol               |   33.0 ms |  25.8 ms |  189.8 ms |    23.1 ms |   50.3 ms |
| update_price_x2              |   3.32 ms |  2.87 ms |   2.92 ms |    13.4 ms |   5.07 ms |
| cumsum_price                 |   3.19 ms |  12.2 ms |   11.0 ms |    13.6 ms |   8.67 ms |
| cumprod_price                |   3.96 ms |  12.4 ms |   11.1 ms |   328.9 ms |  339.3 ms |
| rand_uniform                 |   3.64 ms |  7.57 ms |   9.08 ms |    25.7 ms |   25.5 ms |
| rand_normal                  |   25.1 ms |  29.7 ms |   31.1 ms |    83.7 ms |   74.7 ms |
| rand_int                     |   3.91 ms |  7.45 ms |   9.23 ms |    59.3 ms |   63.3 ms |
| rand_bernoulli               |   2.74 ms |  28.8 ms |   30.5 ms |    56.5 ms |   56.1 ms |
| fill_null                    |   4.46 ms |  2.81 ms |   6.71 ms |    6.80 ms |   12.9 ms |
| fill_forward                 |   3.73 ms |  8.41 ms |   7.24 ms |    14.2 ms |   10.5 ms |
| fill_backward                |   7.91 ms |  8.21 ms |   7.61 ms |    5.33 ms |   10.9 ms |
| null_left_join               |   54.9 ms |  29.1 ms |  216.0 ms |   158.9 ms |  169.7 ms |
| null_semi_join               |   34.4 ms |  19.6 ms |  188.9 ms |    39.1 ms |   85.9 ms |
| null_anti_join               |   34.5 ms |  18.4 ms |  100.0 ms |    63.1 ms |   97.0 ms |
| null_cross_join_small        |   1.72 ms | 0.460 ms |   3.74 ms |    15.1 ms |   51.5 ms |
| filter_simple                |   19.1 ms |  7.55 ms |   23.9 ms |    30.7 ms |   34.4 ms |
| filter_and                   |   12.0 ms |  5.21 ms |   16.7 ms |    27.5 ms |   36.6 ms |
| filter_arith                 |   20.3 ms |  8.24 ms |   35.8 ms |    33.1 ms |   28.9 ms |
| filter_or                    |   12.3 ms |  5.22 ms |   14.0 ms |    26.6 ms |   30.4 ms |
| count_by_symbol_day          |   7.42 ms |  51.1 ms |  318.4 ms |    22.9 ms |   91.8 ms |
| mean_by_symbol_day           |   9.19 ms |  55.1 ms |  317.8 ms |    22.3 ms |  109.5 ms |
| ohlc_by_symbol_day           |   14.4 ms |  55.2 ms |  336.0 ms |    26.1 ms |  125.6 ms |
| sum_by_user                  |  134.3 ms |  45.3 ms |  272.3 ms |    43.3 ms |  309.4 ms |
| filter_events                |   23.4 ms |  6.88 ms |   40.0 ms |    30.5 ms |   29.3 ms |
| melt_wide_to_long            |  335.8 ms |  41.1 ms |  522.7 ms |   184.2 ms |  281.5 ms |
| dcast_long_to_wide           | 1017.5 ms | 650.3 ms | 5111.5 ms |  1376.6 ms | 2007.1 ms |
| dcast_long_to_wide_int_pivot |  744.4 ms |        - |         - |          - |         - |
| dcast_long_to_wide_cat_pivot |  686.5 ms |        - |         - |          - |         - |

## Speedup over ibex (geometric mean across available queries)

- polars: ibex is 1.2× faster than polars  (over 27 queries)
- pandas: ibex is 3.5× faster than pandas  (over 27 queries)
- data.table: ibex is 2.6× faster than data.table  (over 27 queries)
- dplyr: ibex is 4.4× faster than dplyr  (over 27 queries)

Compiled code speed is comparable to interpreted in these benchmarks and
parsing overhead is negligble.

`ibex+parse` includes text parsing and IR lowering; the overhead is negligible.
See [`benchmarking/`](benchmarking/) for methodology and reproduction instructions.

For scalability runs across dataset sizes (1M, 2M, 4M, 8M, 16M, 32M, 64M rows):

```bash
benchmarking/run_scale_suite.sh --warmup 1 --iters 3
```

Results are written per size under `benchmarking/results/scales/<rows>/` and
combined into:
- `benchmarking/results/scales.tsv`
- `benchmarking/results/scales.csv`

Both include a `dataset_rows` column.

To skip only specific frameworks:

```bash
benchmarking/run_scale_suite.sh --skip-pandas --skip-dplyr
```

To analyze where ibex is faster/slower than polars and data.table:

```bash
./build-release/tools/ibex --plugin-path build-release/tools
ibex> :load benchmarking/analyze_scales.ibex
```

## Quant Example Benchmark (Compute-Only)

`examples/quant.ibex` has equivalent pandas/polars scripts:
- `examples/quant_pandas.py`
- `examples/quant_polars.py`

For fair comparison, use the compute-only harness:

```bash
uv run --project benchmarking benchmarking/bench_quant.py \
  --min-seconds 3 --scale 50
```

This runs:
- ibex (compute-only generated script, calibrated repeats)
- polars multi-threaded (`polars-mt`)
- polars single-threaded (`polars-st`, `POLARS_MAX_THREADS=1`)
- pandas

Recent run on this repo:

```
framework    iters    total_s     avg_ms    vs_ibex
--------------------------------------------------------
ibex            21      2.881     137.17      1.00x
polars-mt       29      3.054     105.31      0.77x
polars-st       26      3.070     118.06      0.86x
pandas          10      3.005     300.48      2.19x
```

At smaller scale (`--scale 10`), ibex and single-threaded polars are near parity:

```
framework    iters    total_s     avg_ms    vs_ibex
--------------------------------------------------------
ibex           132      3.614      27.38      1.00x
polars-mt       40      3.040      75.99      2.78x
polars-st      112      3.006      26.84      0.98x
pandas          35      3.034      86.70      3.17x
```

## TimeFrame Benchmark

Rolling-window operations on 1 M rows (1-second uniform spacing).
Ibex release build (`-O2 -march=native`), Clang 20, WSL2.

| Benchmark              |    Ibex |  Polars 1.38.1 | data.table 1.17.0 |
|------------------------|--------:|---------------:|------------------:|
| as_timeframe (sort)    | 0.28 ms |        4.78 ms |            6.2 ms |
| tf_lag1                | 0.97 ms |        4.84 ms |           11.0 ms |
| tf_rolling_count_1m    | 1.12 ms |       16.9 ms  |           12.2 ms |
| tf_rolling_sum_1m      | 1.43 ms |       19.0 ms  |           10.9 ms |
| tf_rolling_mean_5m     | 1.65 ms |       19.7 ms  |            9.6 ms |
| resample 1m OHLC       | 24.7 ms |       14.6 ms  |           20.0 ms |

Notes:
- Ibex uses variable-width time-based rolling windows (two-pointer O(n)); Polars
  uses the same semantics (`rolling_sum_by`); data.table uses fixed-width row
  windows (`frollsum`/`frollmean`, n=60/300 rows — equivalent for uniform 1s data).
- Polars and data.table run multi-threaded on all cores; Ibex is single-threaded.
- Sort fast-path: ibex detects already-sorted input in O(n) without extracting keys
  into a temporary buffer; Polars detects via a pre-set `is_sorted` flag (O(1)).
- Rolling fast-path: ibex accesses the Timestamp column directly via pointer cast,
  avoiding an 8 MB copy; result column allocation is the only dynamic allocation
  per call.
- Resample: ibex floors timestamps into int64 bucket keys and delegates to the
  standard single-threaded aggregation path. Polars uses `group_by_dynamic`
  (parallel); data.table uses integer-key `by=` (parallel). The multi-threaded
  advantage inverts the result here — ibex is 1.7× behind Polars and 1.2× behind
  data.table on this query.

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
├── libraries/             Bundled plugin sources (csv.hpp, csv.cpp → csv.so)
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

## Building

Requirements: Clang 17+, CMake 3.26+, Ninja (recommended).

```bash
# Debug (with sanitizers)
cmake -B build -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug \
  -DIBEX_ENABLE_SANITIZERS=ON
cmake --build build
ctest --test-dir build --output-on-failure

# Release
cmake -B build-release -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-release
```

### Build Options

| Option                      | Default | Description                        |
|-----------------------------|---------|------------------------------------|
| `IBEX_WARNINGS_AS_ERRORS`   | `OFF`   | Treat compiler warnings as errors  |
| `IBEX_ENABLE_LTO`           | `OFF`   | Link-time optimization (Release)   |
| `IBEX_ENABLE_SANITIZERS`    | `OFF`   | ASan + UBSan (Debug only)          |
| `IBEX_BUILD_TESTS`          | `ON`    | Build Catch2 test suite            |
| `IBEX_BUILD_TOOLS`          | `ON`    | Build REPL binary                  |
| `IBEX_BUILD_EXAMPLES`       | `ON`    | Build example programs             |

## Running the REPL

```bash
# With the bundled CSV plugin
IBEX_LIBRARY_PATH=./build-release/tools ./build-release/tools/ibex

# Or pass the plugin directory explicitly
./build-release/tools/ibex --plugin-path ./build-release/tools
```

### REPL Commands

```
:tables                  List available tables
:scalars                 List scalar bindings and values
:schema <table>          Show column names and types
:head <table> [n]        Show first n rows (default 10)
:describe <table> [n]    Schema + first n rows
:load <file>             Load and execute an .ibex script
:comments [on|off]       Toggle/force printing script comments during :load
:timing [on|off]         Toggle/force command timing output
:time <command>          Time exactly one command
```

Tab completion for `:` commands is enabled when Ibex is built with `readline`
available on the system (e.g. `libreadline-dev` on Debian/Ubuntu).

## Plugins

Ibex data-source functions (e.g. `read_csv`, `read_json`, `read_parquet`) are
**plugins** — shared libraries loaded at runtime when a script declares an
`extern fn`.

When the REPL encounters:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
```

it looks for `csv.so` in the plugin search path and calls its
`ibex_register(ExternRegistry*)` entry point to register the function.

**Bundled plugins:**

| Plugin | Functions | Format |
|--------|-----------|--------|
| `csv`  | `read_csv`, `write_csv` | RFC 4180 CSV with type inference |
| `json` | `read_json`, `write_json` | JSON array-of-objects, JSON-Lines, single object |
| `parquet` | `read_parquet`, `write_parquet` | Apache Parquet via Arrow |
| `udp`  | `udp_recv`, `udp_send` | JSON-over-UDP streaming |

Use `import` to load a plugin without explicit `extern fn` declarations:

```ibex
import "json";
let df = read_json("data.json");
write_json(df, "output.json");
```

`csv.so` also supports an optional null-spec argument:

```ibex
extern fn read_csv(path: String, nulls: String) -> DataFrame from "csv.hpp";
let df = read_csv("examples/data/null_metrics.txt", "<empty>,NA");
```

`<empty>` marks empty fields as null; additional comma-separated tokens are
also treated as null.

`json.so` reads JSON arrays of objects, JSON-Lines (one object per line), or a
single JSON object. Type inference follows the same priority as CSV: Int64,
Float64, Bool, String. Missing keys and JSON `null` values produce null
bitmaps.

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

## Roadmap

- [ ] Query optimizer (predicate pushdown, projection pruning)
- [ ] Python FFI bindings (pybind11) and C API
- [ ] R bindings (Rcpp)
- [ ] Arrow C Data Interface export (zero-copy interop)
- [ ] REPL tab completion and history
