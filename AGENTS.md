# Ibex Project Notes

## What is Ibex
Statically typed DSL for columnar DataFrame/TimeFrame manipulation. Transpiles to C++23.
Language spec: `SPEC.md`. Uses `data.table`-inspired bracket syntax with named clauses.

## Build
- Clang 20, CMake 3.31+, Ninja
- `cmake -B build -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug`
- `cmake --build build --parallel && ctest --test-dir build --output-on-failure`
- Workflow: run tests after any parser/lexer/AST changes before marking work done.

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
- Built-ins should remain minimal; prefer `extern fn` hooks for functionality implemented in C++

## Recent REPL Features
- `:schema`, `:head`, `:describe`, `:scalars`, `:tables`, `:load <file>`
- `scalar(table, column)`
- `read_csv("path")` is provided via `extern fn` and registered in C++

## Recent Language Features
- `fn name(params) -> Type { ... }` with required types
- `Int` alias for `Int64`
- `Column<T>` alias for `Series<T>`
- Computed fields in `select` are supported (lowered via update + project)
