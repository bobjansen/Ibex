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
├── tests/                 Catch2 unit tests
├── tools/                 CLI binaries (REPL entry point)
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
./build/tools/ibex --verbose
```

### REPL Commands

```
:tables                  List available tables
:scalars                 List scalar bindings and values
:schema <table>          Show column names and types
:head <table> [n]        Show first n rows (default 10)
:describe <table> [n]    Schema + first n rows
```

## Roadmap

- [ ] Lexer implementation (tokenize Ibex source)
- [ ] Recursive-descent parser (source → IR)
- [ ] DataFrame column storage wiring
- [ ] IR interpreter / evaluation engine
- [ ] Time-indexed DataFrame support
- [ ] Query optimizer (predicate pushdown, projection pruning)
- [ ] CSV / Parquet IO adapters
- [ ] Extern function type checking at registration
- [ ] REPL tab completion and history
