# Runtime Multithreading Plan

## Summary

Add multithreading to Ibex through the existing runtime execution path, starting
with kernel-level parallelism inside chunk-local operations. Keep the pull-based
operator pipeline intact so chunked sources, generated C++ ops, and REPL
execution continue to share the same behavior.

Default behavior should remain single-threaded for compatibility and benchmark
baselines. Users can opt in with `IBEX_THREADS`, and embedders/tests can set the
same policy through a small C++ runtime options API.

## Public Configuration

- Add a runtime execution config, likely in a new
  `include/ibex/runtime/execution.hpp` or next to the interpreter API:
  `RuntimeOptions { std::size_t threads = 1; }`.
- Provide `set_runtime_options(...)`, `runtime_options()`, and
  `parse_runtime_options_from_env()` style helpers.
- Support `IBEX_THREADS=1`, positive integers, and `IBEX_THREADS=auto`.
  Invalid values should fail clearly where practical, not silently oversubscribe.
- Keep `IBEX_THREADS=1` as the default until multithreaded behavior is mature
  enough to become the default.

## Implementation Approach

- Add a small reusable runtime parallel helper:
  `parallel_for(begin, end, grain, fn)` and, only where needed,
  `parallel_reduce(...)`.
- Implement with standard C++23 facilities or a minimal internal worker pool;
  do not introduce a third-party scheduler dependency.
- Avoid worker overhead on small inputs with an internal grain-size threshold.
- Do not parallelize `Operator::next()` or pipeline stage boundaries in v1.
  Ordered buffering, backpressure, and cross-stage error propagation can wait.

## First Parallel Targets

- Parallelize low-risk row-local kernels first:
  - filter mask evaluation and selected-row gather in `filter_table_impl`;
  - row-local `update_table` paths used by `ChunkedUpdateOperator`;
  - fused `FilterProject` and `FilterUpdateProject` through the shared kernels;
  - validity bitmap propagation by partitioning disjoint row ranges.
- Preserve table copy-on-write invariants:
  - inputs are read-only;
  - workers write only to disjoint output ranges or thread-local buffers;
  - `Table::add_column`, `replace_column`, and `mutable_column` remain
    single-threaded orchestration points.
- Keep order-sensitive or global operators single-threaded initially:
  `order`, `top_k`, `head/tail`, joins, window, rank, model, stream, and
  `as_timeframe`.
- Consider aggregate parallelism only after row-local kernels are stable:
  use per-thread local group state and deterministic merges at chunk EOF.

## RNG Semantics

- Each worker uses thread-local RNG, matching the current thread-local design.
- With a fixed seed and fixed thread count, random columns should be stable.
- Changing the thread count may change RNG columns. Document this in `SPEC.md`,
  `README.md`, and `docs/index.html` when the implementation lands.

## Generated C++

- No language syntax change is required.
- Existing `ibex::ops::*` functions delegate through `runtime::interpret`, so
  generated C++ should pick up runtime multithreading automatically.
- Optionally expose `ibex::ops::set_runtime_options(...)` if hand-written or
  generated C++ needs direct runtime control.

## Test Plan

- Unit-test `IBEX_THREADS` parsing: unset, `1`, positive integer, `auto`, `0`,
  negative, and nonnumeric values.
- Verify explicit runtime options override environment-derived defaults in the
  current process.
- Compare `IBEX_THREADS=1` and `IBEX_THREADS=2` or `4` outputs for filter,
  project, row-local update, null propagation, categorical columns, strings,
  bool columns, and fused filter/update/project paths.
- Add RNG tests for fixed seed plus fixed thread count stability.
- Run the existing interpreter, parser/lower, join, and parity tests with
  `IBEX_THREADS=1`, then repeat relevant runtime/parity tests with multiple
  threads.
- Benchmark release builds only. Use
  `benchmarking/run_scale_ibex_vs_polars.sh` with `IBEX_THREADS=1` and
  `IBEX_THREADS=auto`, reporting both Polars single-threaded and default
  multi-threaded results.

## Assumptions

- v1 targets runtime kernels, not a full pipeline scheduler.
- Single-threaded execution remains the default.
- The control surface is both `IBEX_THREADS` and a C++ runtime options API.
- RNG reproducibility is guaranteed for fixed seed and fixed thread count, not
  across different thread counts.
