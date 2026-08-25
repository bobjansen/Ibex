# Runtime execution contracts

The one-place statement of the contracts every operator, source, kernel, and
sink in this directory is bound by. Written 2026-08-22 as Phase 2's opening
deliverable of [plans/kernel-pipeline-execution-plan.md](../../plans/kernel-pipeline-execution-plan.md)
— extracting the kernel APIs (`ChunkView`, selection, validity, output
writer) is stating these contracts in code, so they get stated in prose
first. Each section names the header that owns the normative doc comment;
this file is the map and the cross-file invariants, not a second authority.

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
  `append_column_values` and `gather_range` rely on. *Known hardening debt*
  (Phase 2): extern chunked sources with independent per-chunk dictionaries
  violate this silently.
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
  `PartitionedTableSource` and the parallel island's workers must produce
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
  outputs are byte-identical, which is what lets every threshold in the
  engine move without a correctness argument.
* Per-chunk interruption boundaries: long *pipelines* check
  `interrupt_requested()` between chunks; intra-operator fan-outs
  historically do not (documented gap, `parallelism-overview.md` I13).
* An operator normally degrades to serial under `on_worker_pool_thread()` —
  "outermost wins" — to avoid needless nested fan-out. A necessary nested
  `WorkerPool::submit` is safe: workers cooperatively execute queued tasks
  while waiting for their child batch, so saturation cannot deadlock it.

## 4. Materialization — the sink

Owner: `operator.hpp` (`MaterializeOperator`, `append_column_values`).

* The sink drains incrementally: peak memory is `result + 1 chunk`, each
  chunk released before the next pull.
* Per-column capacity grows **geometrically**. Reserving exactly
  `size + chunk` per chunk is the recorded trap: the concat goes quadratic
  in chunk count (93% of runtime in `memmove` on a 20M-row island at 64k
  grain).
* First chunk donates its columns and properties; the result's
  `logical_rows` accumulates for column-less frames.
* *Known hardening debt* (Phase 2 validity work): schema agreement is
  checked (count/name/type), dictionary sharing is assumed, and validity is
  widened on demand — a hostile source can violate the dictionary
  assumption silently.

## 5. `TableProperties` — derived claims

Owner: `table_properties.hpp`.

* `ordering`, `time_index`, `grouped_by`, `logical_rows` are **derived
  claims about row layout**, transfered by `RowTransform` (`Preserve`,
  `Subset`, and the fate-tracked key forms), never asserted by the operator
  that wishes them true. `normalized()` enforces the TimeFrame invariant at
  the single mutator.
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
  streaming declines it (`chunked.cpp`'s Scan branch draws the same line).
* Column demand is computed once per plan (`ir::required_columns` /
  `scan_predicates`) against the whole-script DAG; a column is decoded only
  if it participates in the result. Predicate-only columns are never
  materialized (`decode-fusion`, stage 1).
* The extern chunked-source **contract** (stable schema, ownership/lifetime,
  dictionary expectations, EOF/error signalling) is the one source contract
  still documented only by its implementations (`read_csv`, ADBC, Parquet);
  writing it down is Phase 2 contract work alongside the kernel APIs
  (tracked from the removed chunked-execution-plan).

## 7. Determinism bar (the contract behind every contract)

Byte-identical output between serial and parallel execution at any core
count, any grain, any threshold. Devices: positional order reconstruction,
data-derived partition counts, first-occurrence group ids assigned after the
parallel phase, lowest-sequence error selection, interrupt over data error.
The three legitimate ulp exceptions (PDS-H q01/q09/q15, parallel float
reduction order — itself thread-count-independent) are enumerated in
`beat-polars-plan.md` §5. Anything else that differs is a bug.
