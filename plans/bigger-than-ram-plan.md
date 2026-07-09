# Bigger-Than-RAM Execution Roadmap

## Motivation

Large datasets blow past RAM in two shapes: large archives at rest
(many-GB-to-TB Parquet, often partitioned by date or another key) and ad hoc
joins/sorts over history that's larger than the box. The chunked execution
substrate
(`plans/chunked-execution-plan.md`) already solved the *streaming compute*
half of this — scan/filter/project/aggregate-on-sorted-input run in bounded
memory today. What's missing is the *out-of-core* half: operators that are
still correct only because they assume the working set fits in RAM, and a
storage layer (Parquet, ADBC) that still reads/writes whole tables at once
instead of streaming row groups/batches.

This plan is scoped to closing that gap. It does not revisit anything
`chunked-execution-plan.md` already tracks (in-memory fusion, canonicalize
rules, streaming aggregate function coverage) — it assumes that substrate and
adds the disk-spill and pushdown-read layer on top of it.

## Current Gaps (verified against the tree, 2026-07-09)

| Area | State today | Why it breaks on bigger-than-RAM |
|---|---|---|
| `Order` (unsorted input) | Buffers every chunk, calls `order_table` on the full concatenated table | Peak memory = whole input; no way to sort data larger than RAM |
| `AsTimeframe` (unsorted input) | Same buffer-then-`order_table` fallback (SPEC §9.1) | Same failure mode, same root cause |
| `Tail` (non-`Order`/`Filter` child) | Falls back to `tail_table` on full materialization | Same |
| `Join` (the common general case) | `ChunkedInnerJoinOperator`/`ChunkedSemiAntiJoinOperator` materialize the **build side** into an in-memory hash table; multi-key/predicated joins materialize **both sides** via `join_table_impl` | Fine while the smaller side fits; wrong assumption for large-fact-to-large-fact joins (e.g. two multi-day history tables) |
| `read_parquet` | `parquet::arrow::FileReader::ReadTable` — whole file decoded into one Arrow table, then converted; registered only via `register_table` (no `register_chunked_table`) | Can't stream row groups; no column projection or row-group-statistics pushdown; a single large Parquet file can't be processed in bounded memory at all |
| Parquet dataset scanning | Single-file path only, no glob/directory, no Hive partitioning | Real archives are directories of many partitioned files — there's no way to point Ibex at one logical dataset |
| `read_adbc` | Already streams: `AdbcSourceOperator::next()` pulls one `ArrowArray` batch at a time and is registered via `register_chunked_table` — this is the reference implementation for a well-behaved chunked source | No gap on the *read* side |
| ADBC / Parquet write | `write_parquet` and the ADBC plugin both go through one-shot `MaterializeOperator` → single Arrow table → single write call | A `source → transform → sink` pipeline still holds the entire result in memory before it can write anything out |
| Memory budgeting | No concept anywhere in the runtime of a memory ceiling, spill directory, or disk-backed intermediate. `grep spill` only turns up the categorical grouping "dense array vs hash map" *representation* switch (`src/runtime/chunked.cpp`), which is an in-memory algorithmic choice, not disk spill | Every "materializing" fallback above has no escape valve — it either fits or it OOMs |

The throughline: every breaker that isn't yet chunk-native in
`chunked-execution-plan.md`'s "Chunk-aware but materializing" table is also a
bigger-than-RAM risk. That plan tracks *whether an operator streams at all*;
this plan tracks *what streaming operators do once their own state (a sort
run, a hash build side, a Parquet row group) is itself too big for RAM*.

## Phases

Phase 1 is a hard prerequisite for everything else — sort, join, and
Parquet/ADBC I/O all need the same spill primitive, so build it once. Phases
2–5 are independent of each other after that and can land in any order;
suggested sequence follows expected workload payoff.

| Phase | Deliverable | Depends on |
|---|---|---|
| 1 | Spill infrastructure: memory budget tracking + disk-backed temp chunks | chunked-execution substrate (done) |
| 2 | External (disk-backed) sort — unblocks `Order`, `AsTimeframe`, `Tail` on unsorted input | 1 |
| 3 | Out-of-core join (grace hash join) | 1 |
| 4 | Chunked, pushdown-aware Parquet: row-group streaming, column pruning, stats pushdown, directory/Hive datasets | 1 (for the case where a single row group still doesn't fit; independent otherwise) |
| 5 | ADBC + Parquet chunked sinks (write path) | none of the above strictly, but pointless without 4 for the Parquet half |
| 6 | Planner-driven adaptive operator selection (spill only when the budget is actually crossed) | 1–3 |
| 7 (stretch) | Parallel spill I/O | 1 + `runtime-multithreading-plan.md` |

### Phase 1 — Spill infrastructure

This is the piece that doesn't exist at all today and everything else is
built on it.

- `include/ibex/runtime/spill.hpp` / `src/runtime/spill.cpp`: a `SpillFile`
  RAII wrapper around a temp file (create, append chunks, read back, delete
  on destruction or process exit), plus a spill-directory resolver
  (`IBEX_SPILL_DIR`, default `std::filesystem::temp_directory_path()`).
- Spill format: reuse the existing `Table`/`Chunk` column layout, serialized
  with the Arrow C Data interop already in `src/interop/arrow_c_data.cpp`
  (Arrow IPC framing) rather than inventing a bespoke binary format — it's
  already a dependency, already handles validity bitmaps, categoricals, and
  strings, and gives spill files a debuggable, tool-readable format for free.
  Flag this as the one open design question worth confirming before writing
  code: bespoke binary would be marginally faster to (de)serialize but is a
  new format to maintain and test independently.
- A byte-budget tracker (`IBEX_MAX_MEMORY`, mirroring the `IBEX_THREADS` knob
  design in `runtime-multithreading-plan.md`): unset/0 means unbounded — the
  default, so existing benchmarks and RSS baselines in
  `plans/benchmark-perf-priorities.md` are completely unaffected until a user
  opts in. When set, operators accumulate in memory and spill only once the
  running estimate crosses the budget — no cardinality estimation or
  plan-time statistics required, matching how the existing dense-vs-hash
  grouping switch already makes a similar in-memory/alternate-representation
  call reactively rather than predictively.
- Startup sweep of stale spill files from a previous crashed process (same
  category of hygiene as the `.cache/clangd/` `.gitignore` lesson already in
  memory — leftover files in a shared temp dir are an easy thing to forget).

### Phase 2 — External sort

Replaces the "buffer all chunks, call `order_table`" fallback shared by
`Order` and `AsTimeframe` (`chunked-execution-plan.md`'s materializing table,
rows 1–2) with a real external merge sort:

1. Accumulate chunks in memory, sort each run in place once the budget is
   hit, spill the sorted run to disk via Phase 1's `SpillFile`.
2. At EOF, k-way merge the spilled runs (plus any final in-memory run)
   against `order_table`'s existing comparator, streaming output chunks —
   no different from the current sorted-input fast path from the caller's
   perspective.
3. `Tail` on a non-`Order`/non-`Filter` child (currently `tail_table` on full
   materialization) becomes free once external sort exists for the
   descending-key case, closing chunked-execution-plan's "Remaining fusion
   opportunities" item on streaming `Tail`.
4. This also unblocks the *sorted* streaming `Aggregate`
   (`ChunkedSortedAggregateOperator`) for inputs that arrive unsorted but
   would benefit from it — today that operator only activates when the
   *source* happens to already be ordered. Feeding it through external sort
   first is a legitimate strategy the planner (Phase 6) can pick when the
   input is large and the group cardinality is high enough that the hash
   path's table wouldn't fit either.

### Phase 3 — Out-of-core join

`ChunkedInnerJoinOperator`/`ChunkedSemiAntiJoinOperator` (single-key,
no-predicate Inner/Semi/Anti) build a full in-memory hash table from the
right side. Add a grace hash join fallback:

1. Partition both sides into N spill buckets by `hash(key) % N`, writing each
   bucket to a `SpillFile`.
2. Join bucket pairs one at a time in memory (each bucket pair is
   individually small even when the full tables aren't), streaming matched
   output.
3. Keep today's fully-in-memory build as the fast path when the build side
   stays under the Phase 1 budget — this is the common case for star-schema
   joins against a small dimension table (e.g. a symbol reference table) and
   should not regress.
4. Multi-key/predicated joins (`join_table_impl`, currently materializing
   both sides unconditionally) are explicitly out of scope for this phase —
   the same partitioning trick generalizes, but ship single-key first and
   revisit once it's validated. Note this scoping decision if a multi-key
   large join surfaces before then.

### Phase 4 — Chunked, pushdown-aware Parquet

Data at rest lives in two shapes: files (typically Parquet, often partitioned
by date and/or another key) and queryable stores reached over ADBC
(Postgres, DuckDB, Snowflake, BigQuery, ...). The ADBC read side is already
chunk-native and largely out of scope here (see the gap table above); this
phase is specifically about bringing the file/Parquet side up to the same
bar. Pushdown differs by source, though: ADBC can push a `Filter`/`Project`
into the remote SQL statement itself (the query engine on the other end does
the row-group-equivalent work), whereas Parquet pushdown has to be
implemented locally against row-group metadata since there's no remote
engine to delegate to. Worth flagging as a design difference, not a gap —
pushing predicates into the SQL text sent to `read_adbc` is a planner
concern (tier-2 pushdown in `project_execution_roadmap` memory), not
something this phase needs to solve for ADBC.

- **Row-group streaming**: convert `read_parquet` from
  `FileReader::ReadTable` to `FileReader::GetRecordBatchReader` (or
  row-group-at-a-time `ReadRowGroup`), registering via
  `register_chunked_table` the same way `read_adbc` already does — reuse
  `ibex::interop::import_table_from_arrow` per batch rather than per file.
- **Column projection pushdown**: thread the set of columns actually
  referenced by the query down into the Parquet reader so unread columns are
  never decoded. This is the same problem R20 (column pruning above
  `Aggregate`) already solves inside the IR — the planner already computes
  "columns needed by this subtree" for that rule; extend it to reach the
  `ExternCall`/source boundary instead of stopping above it.
- **Row-group statistics pushdown**: Parquet stores per-row-group min/max
  stats. When a `Filter` predicate on a column that's sorted or
  partition-correlated (canonically `date`/`timestamp`) can
  prove no row in a group matches, skip the group without decoding it. This
  is the single biggest win for date-ranged queries over multi-year
  archives and should be prioritized within this phase.
- **Directory / Hive-partitioned datasets**: `read_parquet("path/to/data/")`
  over a directory, with partition columns parsed from
  `date=2026-07-01/region=us/part-0.parquet`-style paths and synthesized
  as columns without being physically stored in the file. A `Filter` on a
  partition column should prune whole files before opening them — the
  cheapest possible pushdown, one level above row-group stats.
- `write_parquet` should get the equivalent streaming treatment
  (`parquet::arrow` already supports an incremental `RowGroupWriter` —
  confirm the current writer isn't already doing this via one intermediate
  `arrow::Table` before assuming it needs a rewrite) so it pairs with Phase 5.

### Phase 5 — ADBC + Parquet chunked sinks

`read_adbc` is already the reference chunked source. Nothing streams *out*
today — both the ADBC plugin and `write_parquet` route through
`MaterializeOperator` first. Add:

- `write_adbc`/`to_adbc`: a chunked sink operator that binds each Ibex chunk
  as an Arrow `RecordBatch` and drives `AdbcStatementBind` +
  `AdbcStatementExecuteQuery` (or the bulk-ingestion path if the driver
  supports it) per batch, so the statement handle is reused across the whole
  stream rather than one round trip per row.
- A streaming Parquet writer using the incremental `RowGroupWriter` API from
  Phase 4, so `source → transform → sink` pipelines (Parquet-to-Parquet
  downsampling raw data into daily aggregates, for example) never hold the
  full result in memory on either end.
- This is also the concrete first step toward the "sinks are first-class"
  north star already recorded for the future SQL backend
  (`project_execution_roadmap` memory) — the sink-operator contract designed
  here is the one the `SqlBackend` will eventually reuse for
  `INSERT INTO ... SELECT` targets.

### Phase 6 — Planner-driven adaptive operator selection

Once spillable variants of sort/join exist, `build_operator()` /
`plan_pipelines()` need to *choose* between the in-memory fast path and the
spillable one. The design constraint from Phase 1 makes this simple: no
cardinality estimation or plan-time statistics are needed. Every spillable
operator starts in-memory and switches to its disk-backed mode reactively,
the moment its own accumulated state crosses the Phase 1 budget — exactly
the same reactive pattern the existing hash-vs-sorted `Aggregate` selection
and the dense-vs-hash categorical grouping switch already use. This keeps
the change invisible to the fits-in-RAM common case (which stays exactly as
fast as it is today, unbudgeted) and only pays the spill cost when the data
genuinely doesn't fit.

### Phase 7 (stretch) — Parallel spill I/O

Only after both Phase 1 and `runtime-multithreading-plan.md` have landed:
parallelize spill-partition writes (join) and k-way merge reads (sort) using
the worker pool that plan introduces. Sequence deliberately after — do not
couple spill correctness work to multithreading work; get single-threaded
spill correct and tested first.

## Sharp Edges

- **Two unrelated things are both called "spill."** The existing
  dense-array-vs-hash-map switch for categorical group-by
  (`src/runtime/chunked.cpp`, `tests/test_interpreter.cpp:4339`) is an
  in-memory representation choice with no disk I/O involved. This plan's
  spill is disk-backed. Don't conflate them when grepping or naming new
  symbols — consider naming Phase 1's primitive something unambiguous like
  `SpillFile`/`disk_spill` rather than bare `spill_*` to avoid collisions
  with the existing usage.
- **Determinism under spill.** A spilled execution path must produce
  byte-identical output (including row order where the operator promises
  one) to the unbudgeted in-memory path. Add a parity test that forces
  spilling via a deliberately tiny `IBEX_MAX_MEMORY` and diffs against the
  unbudgeted run — the same technique already used for the streaming
  std/skew/kurtosis parity test in `chunked-execution-plan.md`.
  Extend `tools/ibex_fusion_bench` (or a bigger-than-RAM counterpart) with a
  budgeted-vs-unbudgeted ratio check rather than just a pass/fail parity
  test, following the existing fusion-invariants-gate pattern.
- **Rolling/TimeFrame windows are a separate memory axis.** The
  `project_execution_roadmap` memory already flags that rolling-window state
  (`RollingState` carried across chunks) needs its own redesign for
  chunking; that's driven by *window width*, not *total row count*, so it's
  orthogonal to this plan's row-count-driven spilling and stays out of
  scope here.
- **Measuring success.** Reuse the `bench-1brc.sh` RSS-tracking methodology
  (`avg_maxrss_kb`/`max_maxrss_kb`) already established for Phase 1 of
  chunked execution, but on a workload sized to force spilling — e.g. sort
  or join a synthetic dataset sized to exceed the benchmark box's RAM
  (or run under a tight `IBEX_MAX_MEMORY` on a smaller box) and confirm peak
  RSS stays bounded independent of input size, the same claim already
  validated for chunked scan + streaming aggregate.
- **Crash hygiene.** Spill files must not survive a crashed process
  indefinitely in a shared temp directory — RAII cleanup on the happy path,
  plus the Phase 1 startup sweep, matching the caution already called out
  for other filesystem-hygiene issues in project memory.

## Relationship to Other Plans

- **`plans/chunked-execution-plan.md`** is the prerequisite substrate: every
  phase here targets a row in that plan's "Chunk-aware but materializing"
  table and turns "materializes, but bounded operator count" into
  "materializes only up to a memory budget, then spills." That plan's own
  Next Steps §2 ("harden the external chunked-source contract") and this
  plan's Phase 4 are the same work from two angles — do them together when
  Parquet chunking starts.
- **`project_execution_roadmap` memory** (chunking → adapters → fused
  codegen → SQL backend) placed ADBC adapters *after* chunking phase 1,
  which is now done — this plan is the next concrete step in that sequence,
  filling in the out-of-core half the memory's phase list only gestured at
  ("adapters slot in after phase 1... one driver covers Postgres, BigQuery,
  Snowflake, DuckDB, SQLite"). Phase 5 here is also the first real exercise
  of that memory's "sinks are first-class" requirement for the eventual SQL
  backend.
- **`plans/runtime-multithreading-plan.md`** intersects only at Phase 7
  (stretch). Everything else in this plan is orthogonal to thread count —
  do not block spill correctness work on multithreading landing first.
- **`plans/function-kind-registry-plan.md`** is unrelated in mechanism but
  shares a design principle worth keeping consistent: both prefer a single
  reactive dispatch point (budget-crossed → spill; expression shape →
  FnKind) over scattered conditionals, so operator-selection code added in
  Phase 6 should live in `build_operator()`/`plan_pipelines()`, not as a new
  ad hoc branch elsewhere.
