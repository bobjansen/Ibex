# Ibex Project Notes

## What is Ibex
Statically typed DSL for columnar DataFrame/TimeFrame manipulation. Transpiles to C++23.
Language spec: `SPEC.md`. Uses `data.table`-inspired bracket syntax with named clauses.

## Build
- Clang 20, CMake 3.31+, Ninja
- Debug: `cmake -B build -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug`
- Release: `cmake -B build-release -S . -G Ninja -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release -DIBEX_ENABLE_MARCH_NATIVE=ON`
- `cmake --build build --parallel && ctest --test-dir build --output-on-failure`
- **Always benchmark against `build-release/`, not `build/` (debug is ~4× slower)**
- Fast inner loop: build just the test binary (`cmake --build build --target ibex_tests --parallel`)
  rather than the full `--build build`, which also relinks tools/examples/plugins you likely
  didn't touch. Then run `ctest --test-dir build --output-on-failure -LE slow` to skip
  `ibex_parity_interpreter_vs_transpiled` (~5 min, compiles+runs every parity case from
  scratch). Run plain `ctest --test-dir build --output-on-failure` (no `-LE slow`) before
  declaring parser/lexer/IR/codegen work done — that test is the interpreter-vs-transpiled
  behavior check and small `-LE slow` loops won't catch what it catches.
- ccache is wired in automatically (`IBEX_USE_CCACHE`, default `ON`) when `ccache` is on
  `PATH` — install it once (`apt install ccache` / `brew install ccache`) and clean/fresh
  build dirs (new worktrees, branch switches, `-DCMAKE_BUILD_TYPE` changes) reuse object
  files instead of recompiling from scratch.
- If `cmake --build` keeps re-running the CMake configure step, that's Ninja noticing a
  changed `CMakeLists.txt` somewhere in the tree (e.g. after adding a new source file) — it's
  a real dependency, not a bug, but it only happens on the *first* build after such an edit,
  not every build.
- Fix build warnings as they pop up
- LTO (`-DIBEX_ENABLE_LTO=ON`) gives negligible benefit — hot paths are within single TUs
- Parquet plugin is built as part of the normal CMake build; `scripts/ibex-parquet-build.sh` rebuilds just that target.
- End-to-end checks: `scripts/ibex-e2e.sh` (REPL + transpile + plugins).
- Git hooks: `scripts/install-hooks.sh` (enables clang-format pre-commit check).
- Format tool: `scripts/clang-format.sh` (uses newest available clang-format).
- Workflow: run tests after any parser/lexer/AST changes before marking work done.
- Workflow: add a usage example for new syntax in an `.ibex` file.
- Workflow: rebuild plugins after public header/runtime changes (use `scripts/ibex-plugin-build.sh`).
- Workflow: **when language semantics change** (new built-in functions, syntax, type system additions, or behaviour changes), always update **both** `SPEC.md` (the authoritative language specification) and `docs/index.html` (the public-facing website). These two documents must stay in sync with the implementation. Do not use local paths in the documentation.
- Workflow: for bundled I/O plugins, prefer `import` declarations over explicit `extern fn ... from "*.hpp"` declarations in docs, examples, and user-facing snippets. Use `import "csv"`, `import "json"`, and `import "parquet"` unless the point of the example is plugin internals, parser coverage, or custom extern interop.

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
- When a user requests a benchmark command, run that command directly with any
  requested resource caps and wait for it to finish. Do not add backgrounding,
  ad-hoc wrappers, or substitute workflows merely to work around agent-tool
  timing; report a genuine tool or system failure plainly if one occurs.
- **Always check performance after changes on hot execution paths**, including
  refactors intended to be behavior- or serial-only. Build and measure the
  affected release-path workload against a pre-change baseline with
  `benchmarking/compare_ibex_git.sh` before declaring the work complete; do not
  infer performance neutrality from unchanged output.
- For mutating benchmarks (e.g., `data.table` updates), exclude input-copy cost from timing by preparing copies outside the timed section.
- Built-ins should remain minimal; prefer `extern fn` hooks for functionality implemented in C++
- Workflow: when loading string columns (CSV/parquet), auto-detect categorical encoding where possible.
- Workflow: for routine performance checks, use Polars as the primary comparison target. Prefer `benchmarking/run_scale_ibex_vs_polars.sh` before the full multi-framework suite, and treat `README.md` benchmark snapshots as the published baseline that should stay in sync with current results.
- Workflow: when comparing against Polars, run **both** single-threaded (`POLARS_MAX_THREADS=1`) and default multi-threaded. Ibex's execution is currently single-threaded, so the ST number is the apples-to-apples comparison; the MT number shows the headroom left by not parallelizing yet. Report both.

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
