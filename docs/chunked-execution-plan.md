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
| `Order`     | `ChunkedOrderOperator` — buffer + validate sortedness; re-emit chunks if sorted, fall back to `order_table` on concat otherwise |
| `AsTimeframe` | `ChunkedAsTimeframeOperator` — buffer + validate; re-emit chunks with `time_index` stamped if sorted, fall back to concat + `order_table` (SPEC §9.1) |
| `Update`    | `ChunkedUpdateOperator` — row-local field expressions, no tuple_fields, no group_by |
| `Aggregate` | `ChunkedAggregateOperator` — streaming for the supported subset (Count, Sum, Min, Max, Mean on numeric); fast paths for single categorical key and single string key (transparent `string_view` lookup, SSO-friendly) bypass the generic `ScalarValue` variant key |
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
| `Tail(Filter(x))` (empty group_by) | `ChunkedFilterTailOperator` | Rolling buffer of the last n matches as chunks arrive — peak memory O(n) instead of O(all matches) |
| `Filter(Order(x))` | Rewritten to `Order(Filter(x))` at operator build time | Sort runs on the filtered (smaller) row set. SPEC §9: filter preserves ordering, so observably identical |
| `Project(Order(x))` (keys preserved) | Rewritten to `Order(Project(x))` | Sort carries only projected columns through the comparator and gather |
| `Project(Filter(Order(x)))` (keys preserved) | Rewritten to `Order(FilterProject(x))` | Combined Order-delay: both row and column reductions applied before the sort |
| `Order(Rename(x))` | Rewritten to `Rename(Order(x))` with keys remapped `new→old` | Rename is a metadata bijection; pushing Order under it exposes the sort to anything beneath (source passthrough on the pre-rename name, further Order-delay across Filter/Project under the rename) |
| `Head(Order(x))`, `Tail(Order(x))` | `ChunkedOrderedLimitOperator` | Partial sort / bounded heap instead of full sort + slice |
| `Head(Project/Rename…(x))`, `Tail(Project/Rename…(x))` (empty group_by) | Rewritten to `Project/Rename…(Head/Tail(x))` | Row-limit reaches the fused `FilterHead`/`FilterTail` beneath any Project/Rename chain; only n rows flow through the projection/rename instead of the full filtered set |

### Chunk-aware but materializing

These run inside the operator pipeline but still drain their child into a
full `Table` before processing.

| Node Kind | Reason still materializing |
|-----------|----------------------------|
| `Order` (unsorted input) | Buffer all chunks, fall back to `order_table` on concat |
| `AsTimeframe` (unsorted input) | Buffer all chunks, fall back to concat + `order_table` (SPEC §9.1 requires sorting if unsorted) |
| `Tail` (non-`Order`, non-`Filter` child) | Needs last n rows; falls back to full materialization |
| `Update` (non-row-local, or with tuple_fields or group_by) | Cross-row expressions (lag, rolling, cum, rng, fill, rep) or whole-table tuple sources |
| `Columns` | Produces a one-row schema table; trivial but not streamed |
| `Melt`, `Dcast` | Reshape requires full input |
| `Cov`, `Corr` | Whole-table statistics |
| `Transpose` | Shape transform on full table |
| `Matmul` | Binary whole-table operation |
| `Resample`, `Window` | Time-window operations materialize to sort + bucket |
| `Model` | Fit consumes full input |

## Immediate Priorities

### Order pushed later in the pipeline

`Order` and the unsorted path of `AsTimeframe` still materialize (the spec
forces it for `AsTimeframe` and global ordering inherently requires all
rows). The structural win — sinking `Order` past `Filter` and `Project` so
the sort runs on the smallest table we can produce — is now implemented as
operator-level pattern rewrites in `build_operator`. Measured wins on a 2M
× 16 table: `Filter(Order)` 28%, `Project(Order)` 73%, `Project(Filter(Order))`
85% (see `tools/ibex_fusion_bench`).

Order-delay past `Rename` now rewrites `Order(Rename(x))` to
`Rename(Order(x))` with keys remapped `new→old`, so the sort runs against
the pre-rename schema and composes with the Filter/Project Order-delay
rewrites beneath it. Remaining opportunity: pulling `Order` past
`Aggregate`/`Distinct` boundaries (which drop ordering, so the rewrite
would need to re-establish it via the aggregate's own sort when present).

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
- Row-limit pushdown through `Project`/`Rename` chains — **done**; Head/Tail
  descend past metadata-only wrappers at build time so `ChunkedFilterHeadOperator`/
  `ChunkedFilterTailOperator` still fires underneath

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
- passthrough pipelines can execute without intermediate chunk wrappers — **done** for the shapes above (including Order and AsTimeframe on sorted input, and Tail(Filter))
