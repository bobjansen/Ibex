# Ibex

A statically typed DSL for columnar DataFrame manipulation, transpiling to C++23.

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

## Benchmark

Aggregation benchmarks on 4 M rows (`prices.csv`, 252 symbols).
Release build (`-O2`), 5 iterations, 1 warmup, WSL2 / clang++.

```
query               |     ibex |   polars |   pandas | data.table |    dplyr
--------------------+----------+----------+----------+------------+---------
mean by symbol      |  50.1 ms |  31.8 ms | 180.9 ms |    26.2 ms |  60.0 ms
OHLC by symbol      |  54.9 ms |  29.6 ms | 198.0 ms |    23.8 ms |  52.2 ms
update price×2      |  3.10 ms |  3.15 ms |  4.53 ms |    16.2 ms |  6.80 ms
count by symbol×day |  90.3 ms |  51.5 ms | 333.6 ms |    28.0 ms |  92.0 ms
mean by symbol×day  |  89.5 ms |  53.5 ms | 329.0 ms |    31.4 ms | 131.8 ms
OHLC by symbol×day  |  98.0 ms |  55.2 ms | 343.9 ms |    33.6 ms | 126.6 ms
filter simple       |  53.9 ms |  7.62 ms |  23.7 ms |    30.2 ms |  25.4 ms
filter AND          |  22.3 ms |  4.34 ms |  16.8 ms |    29.2 ms |  35.8 ms
filter arith        |  50.2 ms |  7.65 ms |  37.0 ms |    35.4 ms |  31.4 ms
filter OR           |  23.0 ms |  4.81 ms |  14.1 ms |    26.4 ms |  30.8 ms
```

Compiled code speed is comparable to interpreted in these benchmarks and parsing overhead is negligble.

**ibex vs. others (geometric mean):** 3.1× faster than pandas, on par with dplyr,
2.3× slower than polars, 3.5× slower than data.table.

`ibex+parse` includes text parsing and IR lowering; the overhead is negligible.
See [`benchmarking/`](benchmarking/) for methodology and reproduction instructions.

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
IBEX_LIBRARY_PATH=./build-release/libraries ./build-release/tools/ibex

# Or pass the plugin directory explicitly
./build-release/tools/ibex --plugin-path ./build-release/libraries
```

### REPL Commands

```
:tables                  List available tables
:scalars                 List scalar bindings and values
:schema <table>          Show column names and types
:head <table> [n]        Show first n rows (default 10)
:describe <table> [n]    Schema + first n rows
:load <file>             Load and execute an .ibex script
```

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

## Roadmap

- [ ] Time-indexed DataFrame support
- [ ] Query optimizer (predicate pushdown, projection pruning)
- [ ] REPL tab completion and history
