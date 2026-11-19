# Runtime execution contracts

The one-place statement of the contracts every operator, source, kernel, and
sink in this directory is bound by. Written 2026-08-22 as Phase 2's opening
deliverable of the kernel-pipeline migration (plan retired 2026-10-05:
`git show fbb4f75a:plans/kernel-pipeline-execution-plan.md`)
— extracting the kernel APIs (`ChunkView`, selection, validity, output
writer) is stating these contracts in code, so they get stated in prose
first. Each section names the header that owns the normative doc comment;
this file is the map and the cross-file invariants, not a second authority.

## 0. The execution architecture these contracts serve

```
parser AST → logical IR → physical plan → pipelines → morsel executor → Table / sink
```

- **Lowering is one-way.** After semantic analysis no runtime component
  inspects `parser::ast`.
- **The logical IR is purely logical**: relational meaning, expression and null
  semantics, the schema / ordering / uniqueness / nullability / origin proofs,
  and the optimizer's rewrites. It does not encode execution accidents (a fused
  node kind, "is this join streamable", a key representation, whether a source
  is worker-safe).
- **The physical plan (`runtime::physical`, `physical_plan.hpp`) owns algorithm
  selection and execution capability.** Its nodes are data, and each declares
  rather than implies: schema and `TableProperties` transfer, streaming or
  blocking, partitioning and output-order rule, state ownership, fan-out
  policy, and a decline reason. `explain_physical` renders it (the REPL's
  `:explain <expr>`). A plan that cannot say why it materialized is not
  acceptable.
- **A pipeline** runs from a partitionable source or a breaker's output to the
  next breaker. A breaker owns its build state and starts the next pipeline
  when its build completes. The executor runs numbered morsels on the one
  process pool; no query DAG scheduler, no work stealing, no futures (measured
  unnecessary: `PARALLELISM.md`, "Measured and dropped").
- **Kernels** are a library of statically compiled specializations behind a few
  interfaces (`kernel_types.hpp`: `ColumnView`, `ChunkView`, `Selection`,
  output writers), dispatched once at pipeline construction, never per row,
  and shared across operators (the same gather / validity / scatter / hash
  kernels serve filter output, join assembly, sort and aggregate emission).
- **Breakers follow state ownership, not universal streaming.** Hash join: one
  build, immutable once built (`JoinHashIndex`), probed by source-order
  chunks — on the consumer, or on the scan workers that produced them. Hash
  aggregate: partitions own key maps and slots, with a data-derived partition
  count. Order and distinct are explicit barriers.
- **`MaterializedCall` is the accepted end state for the rest**, not a
  way-station: honest, profiled, named in `explain physical`, its inputs built
  through `materialize_plan` (`build_materialized_fallback`,
  `run_materialized_node`). A fallback kind becomes a physical breaker only
  when a profile shows it costing wall time.
- **No query JIT.** This is Umbra's staged design (AST → relational algebra →
  pipeline-oriented morsel execution) with a precompiled templated-kernel
  backend in place of generated code — a deliberate trade for low
  implementation and query-startup cost (Kersten, Leis, Neumann, *Tidy Tuples
  and Flying Start*, VLDB Journal 2021). One freedom follows from it: Umbra
  must fix a join's build side at compile time; Ibex resolves orientation at
  run time, after the build barrier, with both row counts in hand.

## 1. `Chunk` — the unit of flow

Owner: `include/ibex/runtime/operator.hpp`.

* A `Chunk` is a horizontal slice: `vector<ColumnEntry>` with optional
  per-row validity. Column count, names, order, and variant alternatives are
  **stable across every chunk of one stream** — `MaterializeOperator`
  enforces this on concat and reports a schema mismatch otherwise.
* Table-level metadata (`ordering`, `time_index`, `grouped_by`) travels on
  the chunk through `properties()`/`set_properties`, but is only *fully*
  meaningful on a materialized table. Only `TableSourceOperator` (which
  wraps one) and `make_morsel_chunk` (a `RowTransform::Subset`, under which
  all three properties survive) stamp it wholesale. Never assemble a
  `TableProperties` field by field — `set_properties` normalizes on the way
  in precisely because half-stated claims were a real bug class.
* A column-less chunk (`Table(n)` scaffolds) carries its row count in
  `logical_rows`; `rows()` consults it only when `columns` is empty.
* **The empty carrier**: an empty input (with or without columns) emits
  exactly one zero-row chunk carrying schema and metadata, so a materializing
  sink can reconstruct the planned output shape. Every source honors this;
  an operator that drops rows must keep emitting it (the all-unmatched join
  bug recorded in the plans was exactly a lost carrier).
* Categorical columns across chunks of one stream **share one backing
  dictionary**; codes are copied verbatim, never re-interned. This is what
  `append_column_values` and `gather_range` rely on. A streamed scan decodes
  each unit over that unit's own dictionary, so `PipelinedScanOperator`
  remaps onto a shared one as it emits (`normalize_categorical_dictionaries`).
  The one sanctioned exception is a `ScanWorkerSink` consumer that names
  `untranslated_columns`: it receives the remap and owns the translation
  before anything reads those codes as shared ones. An extern chunked source
  may build each chunk over its own dictionary (an Arrow stream does, batch
  by batch): the runtime wraps it in `CategoricalUnifyingOperator`, and
  `MaterializeOperator` unifies a mismatch it sees itself, both through
  `CategoricalDictionaryUnifier` (`operator.hpp`). A null row's code is never
  read; a valid row's code outside its own dictionary is an error.
* Validity is `optional<ValidityBitmap>`; `nullopt` means all rows valid.
  Concat widens lazily: the result stays `nullopt` until some chunk carries
  nulls, backfilling the accumulated prefix as valid
  (`materialize_append_validity`).

## 2. `sequence` and `row_offset` — the source index space

Owner: `operator.hpp` (fields), `morsel.hpp` (construction).

* `sequence` is the chunk's position in its source's emission order;
  `row_offset` is the absolute row of its first row in the original,
  un-partitioned table. Both are zero on the single-chunk paths and become
  load-bearing only when a source partitions (or workers produce ranges out
  of order and an ordered merger reassembles).
* **Chunk-preserving operators copy both through unchanged**, including on
  the empty carrier. Row-count-changing operators (filter, join, aggregate)
  reset them: downstream order comes from emission order, not positions in a
  table that no longer exists.
* The morsel form of the same idea is `TableRangeMorsel`
  (`const Table* + [begin_row, end_row) + sequence`): zero-copy by design so
  range-aware kernels can address the shared immutable input by absolute
  index. The pointed-to table must outlive every morsel over it and must not
  be mutated while any is live — guaranteed by the one-query-at-a-time lease
  (`query_lease.hpp`), not by the type.
* `make_morsel_chunk` is the single construction point: the serial
  `PartitionedTableSource` and the morsel pipeline's workers must produce
  byte-identical chunks for the same range, so neither builds its own.
* Source-global *selections* (survivor masks, deferred-scan row indices,
  dynamic-filter keep lists) are expressed in the **source's** row space,
  never a chunk-local one, so a filter decided before decode and a gather
  after it agree on what a row index means. Range-aware kernels receive
  64-row-aligned boundaries (`for_row_ranges`) when bit-packed destinations
  are involved.

## 3. `Operator::next()` — the pull protocol

Owner: `operator.hpp`.

* `next() -> expected<optional<Chunk>, string>`: a chunk, `nullopt` at EOF
  (exactly once, no reuse after), or an error. Operators are non-copyable,
  single-consumer, and not thread-safe; concurrency lives *inside* an
  operator (fan-out over ranges/morsels with an ordered merge), never
  between pulls.
* Errors are **deterministically selected**: the lowest sequence / lowest
  worker id wins, and an interrupt outranks a recorded data error. Every
  parallel path reconstructs row order positionally (sequence, range order,
  prefix-summed offset) — never by completion order — so serial and parallel
  outputs agree in rows, order and non-float values (floats within
  tolerance, §7), which is what lets every threshold in the engine move
  without a correctness argument.
* Per-chunk interruption boundaries: the pipeline executors (morsel
  pipeline, streamed scan, stage) and the streaming join and semi/anti
  operators check `interrupt_requested()` between chunks; intra-operator
  fan-outs do not (documented gap, I13 in `PARALLELISM.md`).
* An operator normally degrades to serial under `on_worker_pool_thread()` —
  "outermost wins" — to avoid needless nested fan-out. A necessary nested
  `WorkerPool::submit` is safe: workers cooperatively execute queued tasks
  while waiting for their child batch, so saturation cannot deadlock it.
* **Work done on a scan worker is a hint, not a second protocol.** A consumer
  may hook the workers of the streamed scan directly beneath it
  (`ScanWorkerSink`, `scan_worker_sink_internal.hpp`) or append a step to
  every worker chain (`try_attach_worker_step`). Either way the chunks still
  arrive through `next()` in unit order, and a sink that stored nothing
  leaves the chunk to its ordinary path.

## 4. Materialization — the sink

Owner: `operator.hpp` (`MaterializeOperator`, `append_column_values`).

* The sink drains incrementally: peak memory is `result + 1 chunk`, each
  chunk released before the next pull. `materialize_operator` first switches
  off a streaming pipeline's output batching (`unbatch_pipeline_output`): a
  drain takes every chunk, so batching would only copy each row once more.
* Per-column capacity grows **geometrically**. Reserving exactly
  `size + chunk` per chunk is the recorded trap: the concat goes quadratic
  in chunk count (93% of runtime in `memmove` on a 20M-row island at 64k
  grain).
* First chunk donates its columns and properties; the result's
  `logical_rows` accumulates for column-less frames.
* Schema agreement is checked (count/name/type) and validity is widened on
  demand. A chunk over another dictionary than the rows so far moves those
  rows, once per column, onto a dictionary the sink owns and is translated
  onto it (§1); while the chunks share one, codes are copied verbatim.

## 5. `TableProperties` — derived claims

Owner: `table_properties.hpp`.

* `ordering`, `time_index` and `grouped_by` are **derived claims about row
  layout**, transferred by `RowTransform` (`Preserve`, `Subset`, `Reorder`,
  `Recombine`) together with each key's `KeyFate`, never asserted by the
  operator that wishes them true. Every caller must name its transform; there
  is no default. `normalized()` enforces the TimeFrame invariant at the
  single mutator. (`logical_rows` is not a property: it lives on the `Table`
  and the `Chunk`, §1.)
* `grouped_by` is the hazard flag that stops an unpartitioned
  order-dependent call (`lag`, `rolling_*`) from reading across a group
  boundary; slicing a group-major table into morsels does not make those
  boundaries go away, which is why `make_morsel_chunk` keeps it.
* `KeyFate` distinguishes `Dropped` from `Overwritten` because they are
  identical for an ordering key and opposite for a grouping key: an
  overwritten key's rows have not moved, so the boundary a windowed call
  would read across is still there.

## 6. Sources and demand

* `TableSourceOperator` wraps one materialized `Table` as a single chunk and
  is the only source that stamps full table metadata.
* `make_table_source` is the single call site for "this table is now a
  source"; `IBEX_CHUNK_ROWS` (test-only) swaps in `ChunkedTableSource` at a
  fixed grain so every cross-chunk operator path can be exercised.
* Lazy/deferred sources (`LazyTable`, deferred scans, chunked extern
  registrations via `register_chunked_table`) own their own decode timing.
  A deferred **probe** scan's decode belongs to the join above it — its
  filter slot is published from the build side first — which is why bare-scan
  streaming declines it (the `deferred->filter == nullptr` tests in
  `runtime_entry.cpp` and `pipeline_executor.cpp` draw the same line).
* A streamed scan on either side of a join may likewise be filtered by the
  other side's keys once that side is built (`publish_build_filter_column`,
  `streamed_left_scan_of`): inner and semi joins only — a left join keeps the
  rows such a filter would drop. A filter the unit's footer statistics prove
  rejects nothing is dropped rather than evaluated.
* Parsed Parquet footers are cached per process and validated against the
  file (`fstat`) on every open; a stale entry is re-read, never trusted.
* Column demand is computed once per plan (`ir::required_columns` /
  `scan_predicates`) against the whole-script DAG; a column is decoded only
  if it participates in the result. Predicate-only columns are never
  materialized.
* The extern chunked-source **contract** (stable schema, ownership/lifetime,
  EOF/error signalling) is the one source contract still documented only by
  its implementations (`csv::read`, ADBC, Parquet); writing it down is
  outstanding. Dictionaries are the exception: the runtime unifies them (§1).

## 7. Determinism bar (the contract behind every contract)

The same rows, in the same order, with the same non-float values, between
serial and parallel execution at any core count, any grain, any threshold.
Devices: positional order reconstruction, data-derived partition counts,
first-occurrence group ids assigned after the parallel phase, lowest-sequence
error selection, interrupt over data error. Floats may differ in their last
bits: the order of floating-point operations (a parallel sum, a per-unit
partial) is not part of the contract, and no device exists to fix it.
Answer checks compare floats with a tolerance (`benchmarking/ab_queries.py`,
`outputs_match`). A different row, row order, key, count, integer, string or
decimal is a bug.

## 8. Compact Int32 storage

Normative comments: `include/ibex/core/column.hpp` (`from_compact_int32`,
`visit_integer_storage`, `widened_int64`, `IntegerView`). Plan retired
2026-11-10:
`git show ed7fd554:plans/native-int32-storage.md`; measurements and
diagnoses are in `benchmarking/int32/*/README.md`.

* **Always on, at ingestion only.** Signed Int32 from Parquet (sharded,
  serial and selected-row decoders) and Arrow (adopted C Data buffers keep
  the producer's buffer, slice and owner) keeps Int32 backing. Nothing
  narrows an Int64 by value; UInt32 stays Int64; floats are unchanged. It was
  opt-in through `IBEX_COMPACT_INT32=1` until 2026-11-12; there is no switch.
* **The column is logically Int64.** There is no Int32 `ColumnValue`
  alternative and no Int32 arithmetic: readers return Int64 before any
  arithmetic, and every result (arithmetic, aggregate, scalar) is Int64.
* **No implicit wide copy.** A compact column has no Int64 pointer: const
  `data()`, `span()` and `buffer_data()` throw. An operation that needs
  contiguous Int64 calls `widened_int64()` and owns the copy; nothing is
  cached on, or attached to, the source. Mutable Int64 access promotes the
  destination under copy-on-write. Restoring a column-owned wide cache is
  not the fix for a slow kernel.
* **Dispatch width once, outside row loops.** Kernels take the typed span
  from `visit_integer_storage()` (or a physical `ColumnView` pointer) at
  entry; a per-row width branch or iterator bookkeeping inside a selected-row
  loop is what cost the early filter regressions. `IntegerView` is a
  random-access, not contiguous, logical reader for code that is not hot.
* **Payloads keep their width.** Gathers (selection, batched and serial
  sentinel), filters, projections and concatenation write Int32 output for
  Int32 input; Arrow export emits physical Int32, rebasing sliced validity.
* **`reserve(n)` / `capacity()`** count logical element slots in the current
  backing, not bytes; promotion keeps the reservation.
* **ABI.** The compact tag reuses the otherwise invalid `SIZE_MAX` external
  offset; inline accessors changed, so plugins must be rebuilt against
  matching headers before they can receive compact columns.
* **Code placement.** Identical code moves 1–3% across builds (e.g.
  `filter_arith`); a local alignment hint that fixed one loop regressed
  categorical join and grouped top-K, and was rejected. Judge a placement
  effect by the paired gate with an unchanged replica control.
