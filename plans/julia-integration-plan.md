# Julia Integration Plan

Goal: make Ibex usable from Julia notebooks and scripts while preserving Ibex's
compiled columnar execution model and avoiding copies where Arrow-compatible
memory can be shared.

## User-facing shape

```julia
using Ibex

bars = ibex"""
ticks[
  select {
    open  = first(price),
    high  = max(price),
    low   = min(price),
    close = last(price)
  },
  by { symbol, bucket = floor(ts, 1m) }
]
"""
```

The Julia package should start as an authoring and orchestration layer:

- `ibex"""..."""` returns a query object with useful syntax errors and source
  spans.
- `Ibex.query(input, query)` accepts `Tables.jl`-compatible inputs where
  practical.
- Results implement the `Tables.jl` interface so they can be consumed by
  `DataFrames.jl`, plotting libraries, and Arrow writers.
- Pluto support is worth pursuing: embedded syntax highlighting, rich diagnostics,
  and inline query/result display would make the DSL feel native in notebooks.

## Arrow / zero-copy interop

Ibex already has Arrow table work, so the Julia path should prefer Arrow-shaped
interop rather than bespoke row conversion.

Practical tiers:

1. **Arrow IPC / mmap path.** Ibex writes Arrow IPC output and Julia reads it via
   `Arrow.Table`. This is the easiest robust near-zero-copy path because Arrow.jl
   can expose mmap-backed columns to Julia and DataFrames.jl can consume them
   without immediately copying when `copycols=false` is appropriate.
2. **Ibex-owned table wrapper.** `Ibex.Table` holds Ibex/Arrow buffers and
   exposes Julia `AbstractVector` column views plus the `Tables.jl` interface.
   This avoids a file hop and lets `DataFrame(result; copycols=false)` work, but
   lifetime, immutability, null representation, strings, and categorical columns
   need explicit rules.
3. **Arrow C Data Interface.** Ideal long-term ABI for true in-process zero-copy
   exchange. Caveat: Arrow.jl has historically not exposed full C Data Interface
   support, so Ibex.jl should not block on this. If Julia-side CDI support becomes
   available, use it as the preferred direct bridge.

Input from arbitrary `DataFrame`s should be treated as potentially copying unless
the columns are already Arrow-compatible and immutable. The first implementation
should optimize result export and Arrow-backed inputs before promising zero-copy
for all Julia table sources.

## Benchmarking

Include `DataFrames.jl` in the benchmark suite as the Julia ecosystem baseline,
not as the primary performance target. The main published comparison should still
center on Polars single-threaded and default multi-threaded runs:

- Polars single-threaded: apples-to-apples with Ibex's current single-threaded
  engine.
- Polars default multi-threaded: shows parallel headroom.
- DataFrames.jl: answers the natural Julia user question and frames Ibex.jl as a
  compiled-kernel option for Julia workflows.

Report DataFrames.jl carefully: it is valuable and idiomatic for Julia table
workflows, but it is not expected to be the ceiling for raw columnar query-engine
throughput.

## Initial work items

| Piece | Notes |
|-------|-------|
| `Ibex.jl` package skeleton | Julia package wrapping the C API / runtime entry point |
| String macro | `ibex"""..."""` query object, diagnostics first, macro-time work later |
| `Tables.jl` result interface | Column access, schema, row iteration fallback |
| Arrow-backed result path | Prefer zero-copy column views; document lifetime rules |
| Pluto integration | Syntax highlighting and inline display |
| DataFrames.jl benchmark | Add to scale suite as a Julia ecosystem baseline |
