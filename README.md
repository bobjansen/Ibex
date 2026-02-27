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

## Benchmark

Aggregation benchmarks on 4 M rows (`prices.csv`, 252 symbols).
Release build (`-O2`), 5 iterations, 1 warmup, WSL2 / clang++.

```
query               |     ibex |  polars |   pandas | data.table |    dplyr
--------------------+----------+---------+----------+------------+---------
mean by symbol      |  28.4 ms | 40.1 ms | 180.7 ms |    36.0 ms |  70.6 ms
OHLC by symbol      |  34.9 ms | 48.0 ms | 248.7 ms |    34.6 ms |  56.4 ms
update price×2      |  3.27 ms | 3.33 ms |  5.01 ms |    18.8 ms |  7.20 ms
count by symbol×day |  12.6 ms | 66.2 ms | 328.3 ms |    43.8 ms | 101.6 ms
mean by symbol×day  |  14.0 ms | 76.8 ms | 367.4 ms |    32.2 ms | 155.0 ms
OHLC by symbol×day  |  20.6 ms | 73.9 ms | 400.0 ms |    30.2 ms | 160.8 ms
filter simple       |  19.5 ms | 8.40 ms |  30.7 ms |    29.6 ms |  32.0 ms
filter AND          |  10.5 ms | 5.48 ms |  23.1 ms |    30.0 ms |  46.4 ms
filter arith        |  21.1 ms | 10.9 ms |  47.8 ms |    35.8 ms |  42.4 ms
filter OR           |  11.1 ms | 7.33 ms |  16.3 ms |    26.2 ms |  35.4 ms
```

## Speedup over ibex (geometric mean across available queries)

- polars: ibex is 1.3× faster than polars  (over 10 queries)
- pandas: ibex is 5.0× faster than pandas  (over 10 queries)
- data.table: ibex is 2.1× faster than data.table  (over 10 queries)
- dplyr: ibex is 3.5× faster than dplyr  (over 10 queries)

Compiled code speed is comparable to interpreted in these benchmarks and
parsing overhead is negligble.

`ibex+parse` includes text parsing and IR lowering; the overhead is negligible.
See [`benchmarking/`](benchmarking/) for methodology and reproduction instructions.

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

Ibex data-source functions (e.g. `read_csv`, `read_parquet`) are **plugins** —
shared libraries loaded at runtime when a script declares an `extern fn`.

When the REPL encounters:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
```

it looks for `csv.so` in the plugin search path and calls its
`ibex_register(ExternRegistry*)` entry point to register the function.

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

- [ ] Time-indexed DataFrame support
- [ ] Query optimizer (predicate pushdown, projection pruning)
- [ ] REPL tab completion and history
