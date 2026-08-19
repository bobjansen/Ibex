# Ibex Project Notes

## What is Ibex
Statically typed DSL for columnar DataFrame/TimeFrame manipulation. Transpiles to C++23.
Language spec: `SPEC.md`. Uses `data.table`-inspired bracket syntax with named clauses.

## Build
- Clang 20, CMake 3.31+, Ninja. Debug/Release: `cmake -B build{-release} -G Ninja -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE={Debug,Release}`
- **Always benchmark `build-release/`, not debug (4× slower)**
- Fast loop: `cmake --build build --target ibex_tests`, then `ctest -LE slow` (skips 5-min parity); full `ctest` before parser/lexer/IR/codegen done
- ccache auto-enabled if on PATH; install once (`apt install ccache`). Clean dirs rebuild faster
- LTO negligible (hot paths inline). Parquet: separate rebuild via scripts/ibex-parquet-build.sh
- E2E: scripts/ibex-e2e.sh. Hooks: scripts/install-hooks.sh. Format: scripts/clang-format.sh
- Fix warnings as they pop up. CMake re-run on CMakeLists changes is real dependency, not bug
- **Parser/lexer/AST:** run full `ctest` (no `-LE slow`) before done
- **New syntax:** add .ibex example + update SPEC.md + docs/index.html (must stay in sync, no local paths)
- **Public header/runtime changes:** rebuild plugins via scripts/ibex-plugin-build.sh
- **I/O plugins:** use `import "csv/json/parquet"` in docs unless demonstrating internals

## Architecture
- `include/ibex/` — public headers (all under `ibex` namespace)
- `src/{core,ir,parser,runtime,repl}/` — each has own CMakeLists.txt + static library
- Targets: `Ibex::core`, `Ibex::ir`, `Ibex::parser`, `Ibex::runtime`, `Ibex::repl`
- Dependencies: fmt, CLI11, Catch2 v3 (all via FetchContent)

## Key Design Decisions
- Language keywords: `filter` (not `where`), `select`, `update`, `by`, `window`
- IR nodes: Scan, Filter, Project, Aggregate, Update, Window
- No SQL keywords in surface syntax, no pipes, no user-defined macros
- Built-in compile-time `map` expansion exists inside braced `select` and `update` blocks
- Column resolution: column scope → lexical scope → built-in scope
- `select` and `update` are mutually exclusive in a block
- `by` requires `select` or `update`
- `window` requires TimeFrame operand

## Benchmarking Notes
- **[Read MEASURING.md](MEASURING.md) before perf work** — covers: IBEX_PROFILE_OPERATORS, two-dimensional sweeps, mutation testing, byte-identity checks, A/B with Wilcoxon, post-commit hook waits
- **Hot paths:** always measure before/after with `benchmarking/compare_ibex_git.sh`. Don't infer perf from output
- **Polars comparison:** use both `POLARS_MAX_THREADS=1` (apples-to-apples) and default (headroom). Keep README baseline in sync
- **Mutating benches:** prepare copies outside timed section. Built-ins minimal; prefer `extern fn`
- **String columns:** auto-detect categorical encoding. Run user-requested bench commands directly, report failures plainly

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
