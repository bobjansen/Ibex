# Ibex Project Notes

## What is Ibex
Statically typed DSL for columnar DataFrame/TimeFrame manipulation. Transpiles to C++23.
Language spec: `SPEC.md`. Uses `data.table`-inspired bracket syntax with named clauses.

## Build
- Clang 20, CMake 3.31+, Ninja
- Debug: `cmake -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug`
- Release: `cmake -B build-release -S . -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -DIBEX_ENABLE_MARCH_NATIVE=ON`
- `cmake --build build --parallel && ctest --test-dir build --output-on-failure`
- **Always benchmark against `build-release/`, not `build/` (debug is ~4× slower)**
- Fix build warnings as they pop up
- LTO (`-DIBEX_ENABLE_LTO=ON`) gives negligible benefit — hot paths are within single TUs
- Parquet plugin is built standalone: `scripts/ibex-parquet-build.sh` (after Ibex build).
- End-to-end checks: `scripts/ibex-e2e.sh` (REPL + transpile + plugins).
- Git hooks: `scripts/install-hooks.sh` (enables clang-format pre-commit check).
- Format tool: `scripts/clang-format.sh` (uses newest available clang-format).
- Workflow: run tests after any parser/lexer/AST changes before marking work done.
- Workflow: add a usage example for new syntax in an `.ibex` file.
- Workflow: rebuild plugins after public header/runtime changes (use `scripts/ibex-plugin-build.sh`).

## Architecture
- `include/ibex/` — public headers (all under `ibex` namespace)
- `src/{core,ir,parser,runtime,repl}/` — each has own CMakeLists.txt + static library
- Targets: `Ibex::core`, `Ibex::ir`, `Ibex::parser`, `Ibex::runtime`, `Ibex::repl`
- Dependencies: fmt, spdlog, CLI11, Catch2 v3 (all via FetchContent)

## Key Design Decisions
- Language keywords: `filter` (not `where`), `select`, `update`, `by`, `window`
- IR nodes: Scan, Filter, Project, Aggregate, Update, Window
- No SQL keywords in surface syntax, no pipes, no macros
- Column resolution: column scope → lexical scope → built-in scope
- `select` and `update` are mutually exclusive in a block
- `by` requires `select` or `update`
- `window` requires TimeFrame operand

## Benchmarking Notes
- For mutating benchmarks (e.g., `data.table` updates), exclude input-copy cost from timing by preparing copies outside the timed section.
- Built-ins should remain minimal; prefer `extern fn` hooks for functionality implemented in C++
- Workflow: when loading string columns (CSV/parquet), auto-detect categorical encoding where possible.

## Recent REPL Features
- `:schema`, `:head`, `:describe`, `:scalars`, `:tables`, `:load <file>`
- `scalar(table, column)`
- `read_csv("path")` is provided via `extern fn` and registered in C++

## Recent Language Features
- `fn name(params) -> Type { ... }` with required types
- `Int` alias for `Int64`
- `Column<T>` alias for `Series<T>`
- `let` supports local type inference when the annotation is omitted
- Computed fields in `select` are supported (lowered via update + project)
