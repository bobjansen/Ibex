# Ibex

A statically typed DSL for columnar DataFrame manipulation, transpiling to C++23.

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
cmake -B build -G Ninja \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DCMAKE_BUILD_TYPE=Debug \
  -DIBEX_ENABLE_SANITIZERS=ON

cmake --build build
ctest --test-dir build --output-on-failure
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
# Without plugins (built-in tables only)
./build/tools/ibex

# With a plugin directory (required for extern fn libraries like read_csv)
./build/tools/ibex --plugin-path ./build/libraries

# Or via environment variable
IBEX_LIBRARY_PATH=./build/libraries ./build/tools/ibex
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

### Built-in Functions

```
scalar(df, col)               Extract a scalar from a single-row DataFrame
```

I/O functions such as `read_csv` are provided as plugins (see below).

## Plugins

Ibex data-source functions (e.g. `read_csv`, `read_parquet`) are **plugins** —
shared libraries loaded at runtime when a script declares an `extern fn`.

See `INSTALL.md` for build and plugin setup.

### How it works

When the REPL encounters:

```
extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
```

it looks for `csv.so` in the plugin search path and calls its
`ibex_register(ExternRegistry*)` entry point to register the function.

### Using the bundled CSV plugin

```bash
# The plugin is built automatically with the project:
ls build/libraries/csv.so

# Load it by pointing the REPL at the libraries build directory:
IBEX_LIBRARY_PATH=./build/libraries ./build/tools/ibex
```

### Building the Parquet plugin

The Parquet plugin is built **standalone** and does not affect the Ibex build.

```bash
# Build Ibex first (needed for the runtime libs)
cmake --build build --parallel

# Build the parquet plugin
./scripts/ibex-parquet-build.sh

# Run the REPL with plugin path
IBEX_LIBRARY_PATH=./libraries ./build/tools/ibex
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

## Roadmap

- [ ] Time-indexed DataFrame support
- [ ] Query optimizer (predicate pushdown, projection pruning)
- [ ] REPL tab completion and history
