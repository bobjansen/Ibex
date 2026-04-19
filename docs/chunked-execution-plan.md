# Chunked Execution Roadmap

## Current Status

Ibex runs every table query through a pull-based chunk pipeline. `build_operator()`
has an explicit branch for every `ir::NodeKind` that can appear at a table
position; the final fall-through to `interpret_node()` is defensive and only
reachable for node kinds that are not yet classified.

The substrate (`Operator`, `TableSourceOperator`, `MaterializeOperator`,
`chunk_to_table` / `table_to_chunk`, `plan_pipelines()`) is stable. What the
roadmap tracks now is removing the remaining materialization boundaries and
finding fusion opportunities that actually move the benchmark.

## Coverage Today

### Native chunked path (no whole-table materialization)

| Node Kind   | Handling |
|-------------|----------|
| `Scan`      | `TableSourceOperator` |
| `ExternCall`| `TableSourceOperator` from `chunked_table_func` when registered |
| `Filter`    | `ChunkedFilterOperator` — per-chunk mask + gather |
| `Project`   | `ChunkedProjectOperator` — per-chunk column selection |
| `Rename`    | `ChunkedRenameOperator` — per-chunk metadata edit |
| `Distinct`  | `ChunkedDistinctOperator` — streaming hash set; numeric fast path (robin_hood), string fast path, single-column fast path, multi-column key fallback |
| `Update`    | `ChunkedUpdateOperator` — row-local field expressions, no tuple_fields, no group_by |
| `Aggregate` | `ChunkedAggregateOperator` — streaming for the supported subset (Count, Sum, Min, Max, Mean on numeric) |
| `Head`      | `ChunkedHeadOperator` — global and grouped, short-circuits child on reach; `count == 0` early-exit |
| `Tail`      | Streaming only when paired with `Order` (`ChunkedOrderedLimitOperator`); otherwise materializing |
| `Join`      | `ChunkedInnerJoinOperator`, `ChunkedSemiAntiJoinOperator` for the supported shapes |
| `Construct` | Materialize once via `interpret_node`, wrap as source |
| `Stream`    | Materialize once via `interpret_node`, wrap as source |
| `Program`   | Evaluate preamble, then delegate to child's operator |

### Fused chunked operators

These replace a multi-node subtree with a single operator so intermediate
chunks aren't materialized.

| Pattern | Operator | Payoff |
|---------|----------|--------|
| `Project(Filter(x))` | `ChunkedFilterProjectOperator` | Per-chunk gather touches only projected columns |
| `Project(Update(Filter(x)))` (row-local update, empty tuple_fields/group_by) | `ChunkedFilterUpdateProjectOperator` | Gather set = columns read by update ∪ projected originals not produced by update; skips columns the select drops |
| `Head(Filter(x))` (empty group_by) | `ChunkedFilterHeadOperator` | Pushes `row_limit` into the filter gather so the per-chunk compaction stops at n surviving rows; child's `next()` stops once n reached |
| `Head(Order(x))`, `Tail(Order(x))` | `ChunkedOrderedLimitOperator` | Partial sort / bounded heap instead of full sort + slice |

### Chunk-aware but materializing

These run inside the operator pipeline but still drain their child into a
full `Table` before processing.

| Node Kind | Reason still materializing |
|-----------|----------------------------|
| `Order` | Full sort requires all rows |
| `Tail` (non-`Order` child) | Needs last n rows; no streaming ring-buffer operator yet |
| `Update` (non-row-local, or with tuple_fields or group_by) | Cross-row expressions (lag, rolling, cum, rng, fill, rep) or whole-table tuple sources |
| `Columns` | Produces a one-row schema table; trivial but not streamed |
| `Melt`, `Dcast` | Reshape requires full input |
| `Cov`, `Corr` | Whole-table statistics |
| `Transpose` | Shape transform on full table |
| `Matmul` | Binary whole-table operation |
| `Resample`, `Window` | Time-window operations materialize to sort + bucket |
| `AsTimeframe` | Currently always sorts by timestamp, even when input is already sorted |
| `Model` | Fit consumes full input |

## Immediate Priorities

### Streaming `AsTimeframe` on pre-sorted input

`AsTimeframe` today always sorts by the timestamp column and marks the result
as a TimeFrame. When the source is already sorted (extremely common for
ingested time-series), the sort is wasted work and the operator can be a
metadata-only passthrough that stamps `time_index` on each chunk.

Mechanism mirrors the pre-sort early-exit already used by `order_table`.
This unblocks streaming for any TimeFrame query (currently every TF query
materializes).

### `Tail(Filter(x))` pushdown

Analogue of `Head(Filter(x))`: keep a ring buffer of the last n matches per
chunk so we don't materialize all matches only to discard all but the last n.

### Streaming `Order` for bounded cases

Full ordering remains materializing, but two important shapes already stream
via `ChunkedOrderedLimitOperator` (`Head(Order)` and `Tail(Order)`). The
remaining win here is when the input is already sorted on the order keys —
detect and passthrough.

## Later Phases

### External streaming sources

`ExternRegistry::register_chunked_table(...)` is the runtime entrypoint. The
source contract is not yet documented as a first-class API. Before we chase
ADBC or similar adapters, write down:

- stable schema across chunks
- ownership and lifetime rules
- categorical dictionary expectations
- EOF and error signaling

and add tests that drive a multi-chunk extern source through native operators.

Once stable, an `adbc` plugin reads Arrow `RecordBatch` objects as Ibex
chunks with zero-copy where layouts match — path to PostgreSQL, DuckDB,
Snowflake, BigQuery.

### Remaining fusion opportunities

These are tentative; only pursue once a benchmark identifies them as
actually slow.

- `Scan → Filter → Rename → Project` (listed in the original plan; cheap
  to implement once we see it shows up)
- Row-limit pushdown through `Project` into `Filter` when `Head` sits on
  top of a fused `Filter→Project`

### Materialization hardening

`MaterializeOperator` assumes identical schema and shared categorical
dictionaries across chunks, and has no multi-chunk validity bitmap support.
These remain tracked limitations to remove deliberately as real workloads
expose them.

## Validation Plan

Every change leaves behind tests:

- operator unit tests for chunk boundaries and EOF behavior
- interpreter tests running the same query through single-chunk and
  multi-chunk sources and comparing results
- pipeline-planner tests for representative IR shapes
- `tools/ibex_fusion_bench` for perf regression gates on fused shapes

Benchmark gates compare against `build-release/` only, since debug runs
~4× slower.

## Success Criteria

- every `ir::NodeKind` has an explicit `build_operator()` strategy — **done**
- the chunked path is the normal execution substrate for table queries — **done**
- materialization points are intentional, named, and measurable — **done**
- external readers can stream chunks into native operators — partial
  (`read_csv` streams; source contract not yet documented)
- passthrough pipelines can execute without intermediate chunk wrappers — **done** for the shapes above; ongoing for AsTimeframe and Tail(Filter)
