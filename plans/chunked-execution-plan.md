# Chunked Execution Roadmap

## Current Status

Ibex runs every table query through a pull-based chunk pipeline. `build_operator()`
(in `src/runtime/interpreter.cpp`) has an explicit branch for every `ir::NodeKind`
that can appear at a table position; the final fall-through to `interpret_node()`
is defensive and only reachable for node kinds that are not yet classified (and
`Scan`, which the caller handles as a source).

The substrate (`Operator`/`TableSourceOperator`/`MaterializeOperator` in
`include/ibex/runtime/operator.hpp`, `chunk_to_table`/`table_to_chunk`,
`plan_pipelines()` in `src/runtime/pipeline.cpp`) is stable. What the roadmap
tracks now is removing the remaining materialization boundaries and finding
fusion opportunities that actually move the benchmark.

Since this plan was last revised, the canonicalizer grew from 8 rules to 21,
a fused `TopK` node landed (heap-select for `Head/Tail(Order)`), grouped
`update + by` became functional, and `read_parquet` gained a row-group/batch
streaming source (`ChunkedParquetSourceOperator`, Phase 4 of
`plans/bigger-than-ram-plan.md`). Those changes are folded into the tables
below.

## Coverage Today

### Native chunked path (no whole-table materialization)

| Node Kind   | Handling |
|-------------|----------|
| `Scan`      | `TableSourceOperator` (handled by caller) |
| `ExternCall`| `TableSourceOperator` from `chunked_table_func` when registered — covers `read_csv`, Kafka/Avro sources, `read_adbc`, and `read_parquet` (`ChunkedParquetSourceOperator`, 65536-row Arrow batches via `set_batch_size`) |
| `Filter`    | `ChunkedFilterOperator` — per-chunk mask + gather |
| `Project`   | `ChunkedProjectOperator` — per-chunk column selection |
| `Rename`    | `ChunkedRenameOperator` — per-chunk metadata edit |
| `Distinct`  | `ChunkedDistinctOperator` — streaming hash set; numeric fast path (robin_hood), string fast path, single-column fast path, multi-column key fallback |
| `Order`     | `ChunkedOrderOperator` — buffer + validate sortedness; re-emit chunks if sorted, fall back to `order_table` on concat otherwise |
| `AsTimeframe` | `ChunkedAsTimeframeOperator` — buffer + validate; re-emit chunks with `time_index` stamped if sorted, fall back to concat + `order_table` (SPEC §9.1) |
| `Update`    | `ChunkedUpdateOperator` — row-local field expressions, no tuple_fields, no group_by |
| `Aggregate` | `ChunkedSortedAggregateOperator` when the input chunks are sorted on the group keys (streams group-at-a-time, bounded memory, output emitted in key order); otherwise falls back to `ChunkedAggregateOperator` — hash-based, streaming for Count/Sum/Min/Max/Mean/Std/Skew/Kurtosis/First/Last (numeric; String/Categorical First/Last only on the hash operator) with fast paths for single categorical key and single string key (transparent `string_view` lookup, SSO-friendly) bypassing the generic `ScalarValue` variant key |
| `TopK`      | `ChunkedOrderedLimitOperator` — bounded heap-select (O(n log k)) for `Head/Tail(Order)`, global and grouped, `KeepMode::First`/`Last` |
| `Head`      | `ChunkedHeadOperator` — global and grouped, short-circuits child on reach; `count == 0` early-exit |
| `Tail`      | Materializing via `tail_table` (the streaming shapes — `Tail(Order)` and `Tail(Filter)` — are now rewritten to `TopK` / `FilterTail` upstream) |
| `Join`      | `ChunkedInnerJoinOperator`, `ChunkedSemiAntiJoinOperator` for single-key, no-predicate Inner/Semi/Anti shapes (right side materialized into a hash table) |
| `Construct` | Materialize once via `interpret_node`, wrap as source |
| `Stream`    | Materialize once via `interpret_node`, wrap as source |
| `Program`   | Evaluate preamble, then delegate to child's operator |

### IR canonicalization pass

`src/ir/canonicalize.cpp` runs as part of the default `PassManager` before
the interpreter walks the tree. Algebraic identities are applied bottom-up
to fixpoint so `build_operator` only sees canonical shapes — no pattern
walkers or peek-ahead for node permutations. Current rules (numbers are the
in-source labels; they reflect authoring order, not application order):

| Rule | Rewrite | Notes |
|------|---------|-------|
| R1  | `Filter(Order(x))` → `Order(Filter(x))` | Filter preserves order (SPEC §9); sort runs on the smaller row set |
| R2  | `Project(Order(x))` → `Order(Project(x))` | Only when all Order keys are preserved by the projection |
| R3  | `Order(Rename(x))` → `Rename(Order(x))` | Order keys remapped new→old; exposes the sort to rules beneath |
| R4  | `Head/Tail(Project\|Rename(x))` → `Project\|Rename(Head/Tail(x))` | group_by remapped through Rename; row-limit reaches fused limit ops beneath metadata wrappers |
| R5  | `Project(Filter(x))` → `FilterProject(x)` | Fused IR node; dispatched by `NodeKind::FilterProject` |
| R6  | `Project(Update(Filter(x)))` → `FilterUpdateProject(x)` | Only when Update is row-local (no tuple_fields, no group_by, no cross-row callees) |
| R7  | `Head(Filter(x))` → `FilterHead(x)` | Fused IR node; empty group_by only |
| R8  | `Tail(Filter(x))` → `FilterTail(x)` | Fused IR node; empty group_by only |
| R9  | `Rename(Rename(x))` → single `Rename` | Compose the two mappings |
| R10 | `Rename(x)` → `x` | Drop a Rename whose entries are empty or all identity |
| R11 | `Filter(Rename(x))` → `Rename(Filter'(x))` | Predicate column refs remapped new→old |
| R12 | `Filter(Update(x))` → `Update(Filter(x))` | Only when Update is row-local and the predicate reads none of Update's output columns |
| R13/R14 | `Head(Head(x))` / `Tail(Tail(x))` → single node | Tighter of the two bounds |
| R15 | Drop Order keys pinned to constants | When an immediate Filter child fixes a key to a constant, that key can't affect the sort |
| R16 | `Head(Order(x))` → `TopK(…, First)`, `Tail(Order(x))` → `TopK(…, Last)` | Enables heap-select (O(n log k)) instead of full sort + truncate; codegen emits a single op |
| R17 | Simplify a Filter predicate | Boolean identity/absorption, double-negation, literal folding; `true` drops the Filter, `false` becomes `Head(0, x)` |
| R18 | `Filter(Aggregate(x))` → `Aggregate(Filter(x))` | Only when the predicate references group_by columns only (HAVING-style predicates on aggregate aliases stay put) |
| R19 | `Filter(p1, Filter(p2, x))` → `Filter(p1 AND p2, x)` | Merge adjacent filters so downstream rules see one combined predicate |
| R20 | `Aggregate(gb, aggs, x)` → `Aggregate(gb, aggs, Project(needed, x))` | Prune unused columns before the breaker; skipped when `x` already projects |
| R21 | Collapse a redundant `Project` below `Project`/`FilterProject` | Outer node already restricts columns; inner Project's list is a superset, so dropping it is sound |

New identities go here as data, not as branches in `build_operator`.

### Fused chunked operators

These replace a multi-node subtree with a single operator so intermediate
chunks aren't materialized.

| Pattern (after canonicalization) | Operator | Payoff |
|----------------------------------|----------|--------|
| `FilterProject` (R5) | `ChunkedFilterProjectOperator` | Per-chunk gather touches only projected columns |
| `FilterUpdateProject` (R6) | `ChunkedFilterUpdateProjectOperator` | Gather set = columns read by update ∪ projected originals not produced by update |
| `FilterHead` (R7) | `ChunkedFilterHeadOperator` | Pushes `row_limit` into the filter gather; child stops once n reached |
| `FilterTail` (R8) | `ChunkedFilterTailOperator` | Rolling buffer of the last n matches — peak memory O(n), not O(all matches) |
| `TopK` (R16) | `ChunkedOrderedLimitOperator` | Partial heap-select instead of full sort + slice; grouped and global |

### Chunk-aware but materializing

These run inside the operator pipeline but still drain their child into a
full `Table` before processing.

| Node Kind | Reason still materializing |
|-----------|----------------------------|
| `Order` (unsorted input) | Buffer all chunks, fall back to `order_table` on concat |
| `AsTimeframe` (unsorted input) | Buffer all chunks, fall back to concat + `order_table` (SPEC §9.1 requires sorting if unsorted) |
| `Tail` (non-`Order`, non-`Filter` child) | Needs last n rows; falls back to `tail_table` on full materialization |
| `Update` (`+ by`, or tuple_fields, or non-row-local) | Grouped update (`grouped_update_table`), rank-only grouped update, and grouped/global windowed update all run on a fully materialized table |
| `Join` (multi-key, predicated, or non-Inner/Semi/Anti) | Falls back to `join_table_impl` on both sides materialized |
| `Columns` | Produces a one-row schema table; trivial but not streamed |
| `Melt`, `Dcast` | Reshape requires full input |
| `Cov`, `Corr` | Whole-table statistics |
| `Transpose` | Shape transform on full table |
| `Matmul` | Binary whole-table operation |
| `Resample`, `Window` | Time-window operations materialize to sort + bucket |
| `Model` | Fit consumes full input |

## Recently Landed (was "Immediate Priorities")

**Streaming sort-based Aggregate — done.** `ChunkedSortedAggregateOperator`
detects (from the first chunk's `ordering`) when the input is sorted on the
group keys, then keeps accumulators for only the current group, emits groups in
key order, and advertises that ordering downstream. Peak in-flight memory is
O(one group + one output chunk) instead of O(all groups), with no hashing. When
the input isn't group-sorted it transparently falls back to the hash
`ChunkedAggregateOperator` by replaying the first chunk through a prepend shim.
Measured on a 2M-row table (`tools/ibex_fusion_bench`): ~1000 groups streams in
3.5ms vs 39ms hashed (**11×**); high cardinality (one group per row, ~2M
groups) is 69ms vs 1069ms (**15×**) — the hash path's cost is building a
2M-entry table the stream never materializes. (Note: surfacing this exposed a
pre-existing canonicalize infinite loop — R20 ↔ R2 oscillating on
`Aggregate(Order(x))` when no `count(*)` is present; R20 now peeks past leading
`Order` nodes, and `rewrite_root` has a safety iteration cap.)

**Order pushed later in the pipeline — done.** Sinking `Order` past `Filter`,
`Project`, and `Rename` is implemented as canonicalize R1/R2/R3 so the sort
runs on the smallest table we can produce. Measured wins on a 2M × 16 table
(`tools/ibex_fusion_bench`): `Filter(Order)` 28%, `Project(Order)` 73%,
`Project(Filter(Order))` 85%.

**Head/Tail(Order) → TopK — done (R16).** What was previously a full sort
followed by a slice is now a bounded heap-select, grouped or global, via
`ChunkedOrderedLimitOperator`. This subsumed the old "Tail streams only when
paired with Order" special case. Measured on the 2M × 16 table
(`tools/ibex_fusion_bench`), against the `wide_order_unsorted` full-sort
baseline of ~169ms: `order_head_10` 4.1ms (~41×), `order_head_1000` 7.0ms,
`order_tail_10` 4.7ms, `order_tail_1000` 7.6ms. Grouped top-3 over 100 groups
(`grouped_order_head_3`, including a row-local group-key update) is ~60ms.

**Predicate-level algebra — done (R17–R21).** Adjacent filters merge (R19),
predicates fold and dead filters drop (R17), filters push through Rename (R11)
and through the Aggregate breaker on group keys (R18), and column pruning runs
both above the Aggregate (R20) and across stacked Projects (R21).

**Grouped update — done.** `update + by` now works (`grouped_update_table`),
along with rank-only grouped updates and grouped windowed updates. These still
materialize but are no longer rejected.

**Streaming aggregate set widened to the central moments — done.** `std`,
`skew`, and `kurtosis` now stream (both the hash and the sorted operators) via
online central moments (Welford/Pébay; `m3`/`m4` added to `AggSlot`, shared
`agg_update_*`/`agg_finalize_*` helpers). `std`'s M2 update is bit-identical to
the materializing Welford; `skew`/`kurtosis` match the two-pass materializing
results to floating-point rounding (parity test: same query streamed vs forced
materializing via an added `median`). Null thresholds match the materializing
path (`std` needs ≥2, `skew` ≥3, `kurtosis` ≥4 non-null observations). At the
time this landed, `first`/`last` still materialized for type-preserving output;
they now stream too (see the later section below). Still materializing:
`median`/`quantile` (need all values) and `ewma` (row-order coupled).
Benchmarked via `ibex_fusion_bench` (`agg_moments_stream`/
`agg_moments_hash`) with a polars companion (`tools/bench_polars_agg.py`) on the
same shape (2M rows, 1000 groups): streaming std+skew+kurtosis ≈ 28ms vs the
hash path ≈ 66ms (no hashing of every row). polars is faster, and threading is
*not* the reason: single-threaded polars does ≈ 6ms with its sorted-group fast
path (`.sort()` sets a sortedness flag) and ≈ 34ms via hashing — its 24-thread
run (~7ms) barely beats its own single-threaded sorted path. So the gap is
per-core efficiency, ≈ 4.6× on the sorted path and ≈ 1.9× on hash. Two concrete
opportunities: (1) ibex runs the Pébay moment update once *per moment agg* even
when several read the same column — a shared per-column moment accumulator would
fold std+skew+kurtosis into one pass; (2) the per-row work isn't vectorized.
(Polars exploiting sortedness ~6× single-threaded also validates this whole
sorted-aggregate direction.)

**Chunked `read_parquet` — done (`bigger-than-ram-plan.md` Phase 4, partial).**
`read_parquet` previously went through `parquet::arrow::FileReader::ReadTable`
— the whole file decoded into one Arrow table before Ibex saw a row — and was
registered only via `register_table`. `ChunkedParquetSourceOperator` now
streams 65536-row Arrow batches via `FileReader::GetRecordBatchReader`/
`set_batch_size` (independent of the file's own row-group sizing) and
registers alongside the whole-file path via `register_chunked_table`,
matching the `read_adbc` precedent. Purely additive — no behavior change on
paths that don't reach it. Local A/B at 8M rows: ~6.4× lower peak RSS, ~1.7×
faster. Still open: column pruning, row-group-statistics pushdown, and
directory/Hive-partitioned datasets — tracked in `bigger-than-ram-plan.md`,
not here.

**Streaming `first`/`last` — done.** Both chunked aggregate operators now
handle First/Last instead of forcing the whole node onto the materializing
path. Numeric columns stream on both operators via the existing
`int_value`/`double_value` `AggSlot` fields (same slots Sum/Min/Max already
use — First keeps the first value seen per group, Last keeps overwriting).
String/Categorical only stream on the hash `ChunkedAggregateOperator`, via the
`first_value`/`last_value` `ScalarValue` fields (mirroring the materializing
accumulator's fast path); output columns are built with `make_empty_like`-style
type preservation so a Categorical input stays Categorical, not a plain
string. `ChunkedSortedAggregateOperator` has no group-at-a-time string
accumulator, so it detects a non-numeric First/Last column up front (before
committing to the sorted strategy) and routes the whole node to the hash
operator instead — the same "replay the first chunk" fallback mechanism
already used for unsorted input. Benchmarked via `ibex_fusion_bench`
(`agg_firstlast_stream_*`/`agg_firstlast_hash_*`, 2M rows): moderate
cardinality (1000 groups) streams in ~2.5ms vs ~44ms hashed (**~18×**); high
cardinality (2M groups) is ~85ms vs ~900ms (**~10.5×**) — same shape as the
moments win, since the payoff is skipping the hash-table build entirely on
group-sorted input. Surfaced and fixed a latent, unrelated bug in the
*materializing* path while writing the streamed-vs-materializing parity test:
`aggregate.cpp`'s row-wise fallback (used whenever a query mixes `first`/
`last` with a non-fast-path aggregate like `median`, or has nullable agg
inputs) only ever wrote `first_value`/`last_value`, but finalize reads
`int_value`/`double_value` for numeric columns — so `first(numeric_col)`
alongside e.g. `median(x)` in the same query silently returned 0. Fixed by
making the row-wise accumulator populate both, matching what the
already-correct numeric fast path (`update_state_numeric`) does.

## Next Steps

Ordered roughly by expected payoff-to-effort. Confirm each against
`tools/ibex_fusion_bench` (or a new case) before and after.

### 1. Finish widening the streaming Aggregate function set

Count/Sum/Min/Max/Mean/Std/Skew/Kurtosis/First/Last now stream (see Recently
Landed). The remaining materializing aggregates:

- `median`/`quantile` — need all values per group (O(group) state), so not a
  bounded-memory stream; leave on the materializing path.
- `ewma` — depends on within-group row order; streamable only with care, low
  priority.

### 2. Document and harden the external chunked-source contract

`ExternRegistry::register_chunked_table(...)` is the runtime entrypoint.
Four independent sources now drive multi-chunk data through the native
operators: `read_csv`, the Kafka/Avro examples (`examples/kafka_ticks.ibex`,
`kafka_ohlc.ibex`), the `adbc` plugin (`AdbcSourceOperator`, pulling one
Arrow `RecordBatch` at a time — the reference implementation for a
well-behaved chunked source, zero-copy where layouts match, path to
PostgreSQL/DuckDB/Snowflake/BigQuery), and now `read_parquet`
(`ChunkedParquetSourceOperator`, streaming 65536-row Arrow batches via
`FileReader::GetRecordBatchReader`/`set_batch_size`, independent of the
file's own row-group sizing). The contract is still not written down as a
first-class API. Specify:

- stable schema across chunks
- ownership and lifetime rules
- categorical dictionary expectations (shared dictionary vs per-chunk)
- EOF and error signaling

and add tests that drive a synthetic multi-chunk extern source through native
operators (not just the Kafka/Parquet/ADBC demos).

What's still open on the bigger-than-RAM side — out-of-core sort/join for
inputs too big for RAM, column pruning + row-group-statistics pushdown and
directory/Hive-partitioned Parquet datasets (row-group *streaming* itself is
now done), and chunked ADBC/Parquet write sinks — is tracked separately in
`plans/bigger-than-ram-plan.md`, which builds directly on this plan's
coverage table.

### 3. Materialization hardening

`MaterializeOperator` assumes identical schema and shared categorical
dictionaries across chunks, and has no multi-chunk validity-bitmap support.
These become correctness bugs the moment a source emits chunks with
independent dictionaries or differing validity — likely first triggered by the
extern-source work in (2). Remove the assumptions deliberately as real
workloads expose them.

### 4. Remaining fusion opportunities (only if a benchmark flags them)

- `Scan → Filter → Rename → Project` end-to-end fusion (cheap once it shows up
  in a profile; R5/R11 already cover most of it).
- A streaming `Tail` for sources that expose a row count or are seekable, so
  the non-`Order`/non-`Filter` `Tail` path can avoid full materialization.

## Validation Plan

Every change leaves behind tests:

- operator unit tests for chunk boundaries and EOF behavior (`tests/test_operator.cpp`)
- interpreter tests running the same query through single-chunk and
  multi-chunk sources and comparing results
- canonicalizer tests asserting the rewritten IR shape for each rule
- `tools/ibex_fusion_bench` for perf regression gates on fused shapes

**Fusion invariants gate.** `ibex_fusion_bench --check` asserts *ratios*
between a fused case and its un-fused baseline (e.g. `order_head_10` must be
≥8× faster than `wide_order_unsorted`; `agg_sorted_stream_*` must be ≥4× faster
than their `agg_sorted_hash_*` counterparts). Ratios are intrinsic to the
algorithm (full sort O(n log n) vs TopK heap-select O(n log k); no hash-table
build for a sorted aggregate), so they cancel runner
speed and gate cleanly where absolute timings cannot. A failure means a fusion
*stopped firing* (a canonicalize rule regressed, or an operator fell back to
materialization) — a correctness regression in operator selection, not a perf
wobble. Margins sit far below the observed ratio (~30–45× in Release) so noise
can't trip them. The check only holds with optimization on — in Debug the
per-element overhead swamps the algorithmic gap — so CI runs it in the
Release `clang-werror` leg, not the Debug `build-and-test` leg. New fused
shapes should add a guard to the `fusion_guards()` table. This is distinct from
`perf-stats.yml`, which reports absolute A/B numbers and never gates.

Benchmark gates compare against `build-release/` only, since debug runs
~4× slower.

## Success Criteria

- every `ir::NodeKind` has an explicit `build_operator()` strategy — **done**
- the chunked path is the normal execution substrate for table queries — **done**
- materialization points are intentional, named, and measurable — **done**
- passthrough pipelines execute without intermediate chunk wrappers — **done**
  for the shapes above (Filter/Project/Rename/Distinct/Update, Order &
  AsTimeframe on sorted input, Head/Tail via TopK/FilterHead/FilterTail, and
  single-key joins)
- external readers can stream chunks into native operators — **partial**
  (`read_csv`, the Kafka/Avro sources, `read_adbc`, and `read_parquet` all
  stream; the source contract is not yet documented — see Next Steps §2)
