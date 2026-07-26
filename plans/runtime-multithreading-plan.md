# Runtime Multithreading Plan

## Summary

Add multithreading as a query-execution capability, not as ad-hoc loops inside
individual kernels. Ibex already has vectorized `Chunk` operators; use them as
the data unit for a small morsel-driven executor. A query is split into
parallel, chunk-preserving pipelines separated by materializing or coordinating
barriers. Start with one ordered parallel island for row-local work, retain
existing serial implementations behind barriers, and expand only after each
operator family has an explicit correctness contract.

This takes the useful parts of DuckDB's model — partitionable sources, pipeline
breakers, worker-local state plus sparingly used query-global state — without
turning Ibex into a general database scheduler. Polars provides the rollout
model: execute supported streaming operations in batches and fall back to the
existing materializing path for unsupported operations.

Default behavior remains single-threaded until the parallel path is mature.
Users opt in with `IBEX_THREADS`; embedding callers may supply the same policy
through the runtime API.

## Execution Model

### Parallel island

A **parallel island** is a maximal run of consecutive, chunk-preserving,
row-local physical operators that executes concurrently over independent
morsels, bounded on both ends by a barrier (or by the query's source/sink). It
is the unit this plan parallelizes.

- **A contiguous operator segment, not one operator.** An island is a chain of
  `Passthrough` nodes in `classify_node` terms — e.g. `filter → project` or a
  fused `filter-update-project` — run as a single parallel stage.
- **Row-local / chunk-preserving.** Every operator computes each output row from
  its own input row(s) within the same morsel: no operator reads neighbouring
  rows, depends on global order, or aggregates across morsels. This is what
  makes the morsels independent — a worker can process morsel *k* without seeing
  any other morsel.
- **Barrier-bounded.** An island starts at a partitionable source (a
  materialized table; later parquet row groups/ranges) and ends at the next
  barrier — `order`, `join`, `group_by`, `distinct`, window, rank, or model. A
  query is therefore a sequence of islands separated by barriers; Phase 1
  parallelizes only the leading row-local island and leaves everything
  downstream serial.
- **Ordered.** Morsels are processed out of order but the island's output order
  is defined: the ordered merger releases results strictly by morsel `sequence`,
  so island output is byte-for-byte identical to serial execution — hence
  "ordered parallel island."
- **A unit of properties.** Per Phase 0 item 3, an island carries one derived
  `TableProperties` (`ordering`, `time_index`), computed once from its input and
  operator sequence — metadata is a property of the island, not of whichever
  morsel completes first.

Concisely: an ordered parallel island is a barrier-to-barrier segment of
row-local, chunk-preserving operators that runs one independent task per morsel,
with an ordered merger reassembling results in `sequence` order and a single set
of island-level output properties.

### Query and task context

Introduce a query-scoped `ExecutionContext`, created at the `interpret()`
boundary and passed explicitly through operator construction and expression
evaluation. It owns:

- the process worker-pool handle and the query's thread budget;
- query-local cancellation, deterministic error selection (lowest morsel
  sequence, with lower-sequence work drained before return), and worker
  completion; external interruption is observed through
  the existing process-wide interrupt flag under a one-query-at-a-time
  invariant (see Phase 0 item 6);
- the master RNG seed and deterministic stream derivation;
- immutable scalar/extern registries and query-scoped resources;
- deferred-scan state, replacing worker-invisible thread-local lookup; and
- limits and diagnostics needed by the executor (queued morsels, grain, trace
  counters).

Use a separate worker/task context for mutable worker-local state:

```cpp
struct TaskContext {
    ExecutionContext& query;
    std::uint32_t worker_id;
    RngStream rng;         // forward-looking: unused until Phase 2 (RNG is
                           // excluded from Phase 1 eligibility)
    ScratchArena scratch;  // temporary task-local work only; escaping output
                           // needs query/merger-owned storage
};
```

The process may own a reusable worker pool, but it must not own mutable query
state such as a seed, error, or deferred scan registry. Avoid nested pool use:
work submitted by a query stays within that query's thread budget.

### Worker pool

Use a small, bespoke Ibex pool rather than an off-the-shelf scheduler. There is
no good standard C++23 fit: `std::execution` (senders/receivers, P2300) is
C++26 and its reference implementation is a heavy, template-heavy dependency;
`std::for_each(par, …)` is TBB-backed on libstdc++, was long unimplemented on
libc++, and gives no grain, ordering, or backpressure control; `std::async`/
`jthread` are primitives, not a pool. General task frameworks (Taskflow, Folly,
TBB flow graph) supply a DAG scheduler, which is an explicit non-goal here. The
executor's coordination — a partitioned source dispensing numbered morsels, an
ordered merger releasing by `sequence`, and a bounded in-flight queue for
backpressure — is simpler to own than to bend a general pool around, and it is
where the tunable levers (`IBEX_THREADS`, grain-size threshold, morsel size,
Phase-1 allocator ownership) must live.

The one-query-at-a-time invariant (Phase 0 item 6) removes the hard parts of a
general pool — no cross-query fairness, priorities, or preemption. Keep v1
deliberately small and boring:

- **Process-owned and pre-spawned.** Construct the pool lazily in a process
  runtime singleton, spawn `N` `std::jthread` workers once, and reuse them
  across queries, so no query pays thread-creation cost. `ExecutionContext`
  holds a non-owning handle plus a lease for its duration; the pool owns no
  mutable query state. Its singleton destructor joins workers during normal
  process teardown.
- **Pull-based central dispensing.** Workers pull the next row-range morsel from
  the partitioned source via a single shared cursor (an atomic counter over
  stable source ranges), rather than per-worker deques with work-stealing.
  Phase-1 filter/project morsels are near-uniform, so imbalance is low; add
  stealing only if profiling shows a real skew.
- **Minimal coordination.** A morsel queue guarded by a mutex + condition
  variable (adequate for v1; revisit only under measured contention) plus a
  join latch for query completion. **No futures, continuations, or DAG** — the
  ordered merger and the join latch are the only synchronization points.

**Thread count vs. core count.** Default the pool to hardware concurrency for
the compute-bound row-local island (`IBEX_THREADS=auto`), where oversubscription
only adds context-switch and cache-thrash overhead. But size it independently
of core count where that helps: an I/O- or decode-heavy stage (Phase 3 parquet/
CSV scans, and any future path where workers block on reads) can sensibly run
**more threads than cores**, since blocked workers free a core for others.
Treat pool size as a tunable per workload class, not a hardwired
`std::thread::hardware_concurrency()`; `IBEX_THREADS` sets the compute budget,
and I/O-bound stages may layer additional blocking-friendly concurrency on top
rather than being capped at the core count.

### Morsels and ordered output

Make a parallel work unit an explicitly numbered morsel, not a concurrent call
to the current mutable `Operator::next()` API:

```cpp
struct Morsel {
    std::uint64_t sequence;     // input order for deterministic emission
    std::uint64_t row_offset;   // stable source position for diagnostics/RNG
    Chunk chunk;
};
```

Partitionable sources (initially a materialized table; later parquet row
groups/ranges) hand morsels to workers. Workers own their input morsel and
produce a distinct result morsel. An ordered merger releases completed results
only in `sequence` order, with a bounded in-flight queue for backpressure.

Do **not** protect `Operator::next()` with a mutex and call it from workers:
that serializes source production while retaining all of the unsafe operator
state. The physical source must expose partitions deliberately.

### Operator categories

**Where the eligibility decision lives (Option B).** The seam is the existing
operator builder, not a separate planner object. The former `plan_pipelines()` /
`PipelinePlan` test-only segmenter was retired because the execution path is
`interpret()` → `build_operator()` → `MaterializeOperator::run()`
(`src/runtime/interpreter.cpp:1433`), which recurses the IR directly. Rather
than promote that disconnected segmenter onto the hot path, `build_operator()`
— which already owns operator fusion
(`FilterProject`, `FilterUpdateProject`, join-shape selection) — remains
authoritative and gains one eligibility pass it consults at each `Passthrough`
chain to choose a parallel-island operator or the existing serial chain. This
decision fork, and the prerequisite work of establishing that single seam before
any parallel code lands, is [execution-plan-seam-plan.md](execution-plan-seam-plan.md);
Option B is the chosen branch.

Logical planning and optimization remain responsible for plan shape, predicate
and projection pushdown, join ordering, and fusion. `build_operator()` plus that
single eligibility pass is the one owner of whether a chosen physical operator
sequence is eligible for parallel execution, where an ordered merger is required,
and where to retain the existing serial fallback. There must be one owner of this
decision, one eligibility vocabulary, and one fallback path — and it is this
seam, not `plan_pipelines`.

The builder's eligibility pass uses one execution-capability vocabulary:

| Category | Initial behavior | Examples |
|---|---|---|
| Parallel map | One independent task per morsel, ordered merge | row-local filter, project; fused filter/update/project |
| Ordered/stateful stream | Preserve order; initially serial unless a boundary algorithm is supplied | head/tail, lag/lead, fill, cumulative functions |
| Barrier | Materialize or synchronize before continuing; serial first | windows, rank, model |
| Parallel barrier (later) | local worker state, deterministic finalize/merge, then next stage | group-by, join build/probe, sort |

Note that standalone `Update` is a barrier today, not a parallel map — see the
Phase 1 eligibility note below. Pipelines stop at a barrier. This makes every phase transition obvious and
keeps control flow out of individual operators. A first implementation needs
only the first two rows plus the existing serial fallback.

## Public Configuration

- Add an execution configuration, likely in
  `include/ibex/runtime/execution.hpp` or beside the interpreter API. Model
  `serial`, a fixed positive count, and `auto` as an explicit thread policy,
  rather than encoding `auto` as a sentinel `std::size_t`; resolve that policy
  once into the query's fixed thread budget.
- Support `IBEX_THREADS=1`, positive integers, and `IBEX_THREADS=auto`.
  Invalid values fail clearly where practical; never silently oversubscribe.
- Add an explicit `RuntimeOptions` overload to `interpret()` for embedding
  callers. The existing `interpret()` entry point resolves `IBEX_THREADS` at
  its call boundary, preserving source compatibility without mutable
  process-global query state. Threading explicit options through generated
  `ibex::ops::*` APIs is deferred; see *Generated C++ and Plugins*.
- Keep `IBEX_THREADS=1` as the default until output equivalence, cancellation,
  and plugin boundaries have been exercised under parallel execution.

## Phase 0 — Correctness Prerequisites

**STATUS (2026-07-24): complete to its natural boundary.** Every Phase-0 piece
that is observable and testable at `IBEX_THREADS=1` has landed: item 1 (serial
parity gate), item 2 (multi-chunk nulls), item 3 (island `TableProperties` +
empty-output contract), item 4's building blocks (`TableRangeMorsel` /
`PartitionedTableSource` / gather helpers + round-trip tests), item 5
(`ExecutionContext` threading, TLS deferred-scans removed), and item 6's
one-query-at-a-time lease. The three residuals — item 4 wiring, item 6
worker failure/cancellation determinism, and item 7 (extern eligibility pass)
— all share one root cause: there is no executor / eligibility pass /
`classify_node` island-role layer yet. That layer *is* the Phase 0→1 boundary,
so those residuals are Phase 1's foundation, not independently completable
Phase-0 work. Item 5's LazyTable Synchronization Contract is written (design)
but unimplemented; Phase 3 lifts the interim ineligibility gate.

**Sequencing risk — complete and bake this phase before adding any parallel
execution code.** Phase 0 is not setup that can be folded into the first
parallel operator PR. In particular, item 3 replaces an execution-wide
thread-local mechanism with explicit context propagation through operator
construction and expression evaluation (`build_operator`, every relevant
operator constructor, `ColumnEvalCtx`, and their call sites). It is invasive,
has no standalone user-visible speedup, and is the prerequisite for every
operator category above — including the supposedly simple parallel-map path.

Treat Phase 0 as a multi-PR single-threaded refactor: land coherent slices,
retain `IBEX_THREADS=1`, run the normal interpreter/parity/e2e suite after each
slice, and let the completed context path bake before introducing a worker pool,
parallel task submission, or a parallel operator. Do not overlap it with a
performance claim; its deliverable is preserved behavior and an execution
ownership boundary that makes later parallel changes reviewable.

1. **Interpreter/transpiled serial parity (first gate).** Before adding any
   parallel execution to either path, establish a differential parity suite
   that runs the same `.ibex` program through `runtime::interpret()` and through
   the transpiled `ibex::ops::*` output. Compare schema, table metadata
   (including ordering, time index, and logical row count), row order, values,
   categorical representation, and validity bitmaps. This suite is the
   baseline: Ibex must have one serial semantics independent of whether a query
   is interpreted or transpiled. Do not use generated-code benchmarks as
   evidence for whole-query fusion or shared-`ExecutionContext` behavior until
   this gate is green and generated code has such an execution entry point.
2. **Multi-chunk nulls.** Teach `MaterializeOperator` to concatenate validity
   bitmaps. It currently rejects validity after the first chunk, which blocks
   safe splitting of ordinary nullable data.
3. **Island-level metadata and empty-output contract.** Partitioning must not
   turn table metadata into a property of whichever morsel happens to complete
   first. Define a planner-derived `TableProperties` / output-properties object
   for each parallel island, holding at least `ordering` and `time_index`:

   ```cpp
   struct TableProperties {
       std::optional<std::vector<ir::OrderKey>> ordering;
       std::optional<std::string> time_index;
   };
   ```

   Derive those properties once from the island's input and physical operator
   sequence, using shared helpers extracted from the existing serial metadata
   rules rather than a second parallel-only copy. A row-local filter preserves
   both properties; project preserves them only when all required columns
   survive; rename rewrites their column names; and update / fused update uses
   the existing rules to preserve or invalidate them when it overwrites a key
   or the time-index column. Any operation without a proof of preservation
   clears the affected property. Note the serial rules are not a single helper
   today: project/filter/rename use a *presence*-based check in
   `interpreter.cpp` (does the key/time-index column still exist?), while
   `update_table` (`update.cpp:1687`, `drop_ordering`) uses an *overwrite*-based
   check (did a field alias rewrite a key column's values in place?). The shared
   helper must carry both — a fused filter-update that overwrites a sort key
   leaves the column present, so a presence-only extraction would wrongly
   preserve `ordering`.

   Worker results carry only sequence, rows, and schema -- never `ordering` or
   `time_index`. The ordered merger validates each result against the island
   output schema, concatenates results by sequence, sets `logical_rows` to the
   total output row count for column-less output, and attaches
   `TableProperties` once to the final materialized `Table` before re-entering
   the serial pipeline. The partitioned source emits one zero-row morsel for a
   zero-row input; all-filtered and other empty result morsels retain their
   schema and are not discarded while determining the final result. This keeps
   all-empty filter results, empty input, and zero-column frames semantically
   identical to serial execution.
4. **Partitioned materialized source and stable identity.** Add a
   `TableRangeMorsel` / `PartitionedTableSource` that enumerates stable,
   contiguous row ranges over one immutable materialized `Table`:

   ```cpp
   struct TableRangeMorsel {
       const Table* input;  // owned by the query until all tasks join
       std::size_t begin_row;
       std::size_t end_row;
       std::uint64_t sequence;
   };
   ```

   Do not reuse `TableSourceOperator`: it emits its complete table as one
   chunk, hence supplies only one task. Do not introduce general column
   slicing/views in this milestone either. Phase-1 kernels instead read
   absolute row indices from the shared immutable input and produce task-owned
   output chunks. Carry `sequence` and source `row_offset` through
   chunk-preserving operators, including empty schema-carrier chunks.

   **Phase-0 status (done):** `Chunk` carries `sequence` + `row_offset` (default
   0); `include/ibex/runtime/morsel.hpp` holds `TableRangeMorsel`, the
   `gather_range`/`gather_validity` materializing helpers, and
   `PartitionedTableSource`, all covered by byte-identical round-trip tests
   through `MaterializeOperator` (`tests/test_operator.cpp`).

   **Deliberately deferred to Phase 1 — NOT an open issue.** Two pieces are
   intentionally left out here and are *not* loose ends: (a) wiring
   `PartitionedTableSource` into `build_operator`, and (b) propagating
   `sequence`/`row_offset` through every chunk-preserving operator. Both are
   untestable until an island can actually be partitioned, which requires the
   `classify_node`/island-eligibility layer that does not yet exist — adding
   either now would be speculative dead code no test can exercise, and Phase 0
   is explicitly a behavior-preserving refactor at `IBEX_THREADS=1`. They land
   with the worker pool, ordered merger, and range-aware zero-copy kernels in
   Phase 1, where they first become reachable and verifiable.
5. **Context instead of worker TLS (highest-risk gate).** Replace
   execution-scoped thread-local deferred scans with `ExecutionContext`
   propagation through `interpret`, `build_operator`, every applicable operator
   constructor, and expression evaluation (`ColumnEvalCtx` and its consumers).
   This is a large, benefit-free refactor in isolation, but it establishes the
   only safe ownership path for query state once work moves to another thread.
   For Phase 0, mark any query reading a lazy/deferred source ineligible for a
   parallel island (see [Eligibility](#lazytable-synchronization-contract)). That
   ineligibility is an **interim gate, not the solution** — it is free here
   because nothing runs parallel yet, but lazy/deferred sources are the parquet
   scan path (essentially all TPC-H/PDS-H), i.e. the large memory-bound scans
   parallelism most benefits, so a later phase is obligated to lift it under the
   *LazyTable Synchronization Contract* below.
6. **Failure/cancellation.** A worker error records its morsel-sequenced
   failure, suppresses unnecessary higher-sequence work, drains/joins the
   required submitted tasks, and reports a normal `std::expected` error. The
   interrupt path follows the same mechanism.

   **Concurrency model: one query at a time, parallelized across workers.** The
   goal is to execute a *single* query on multiple worker threads, not to run
   multiple queries concurrently. The runtime supports at most one in-flight
   `interpret()`/executor invocation per process at a time; a second concurrent
   top-level entry is rejected with a stable `std::expected` error (for example,
   `"runtime already executing a query"`) rather than serialized. The REPL
   naturally blocks while its synchronous call is running, so it never creates
   a competing entry. A re-entrant runtime entry from an extern/plugin is also
   rejected: it cannot wait for the pool lease it is executing under. Because
   there is never more than one live query, the existing process-wide interrupt
   flag (`include/ibex/runtime/interrupt.hpp:14`, `detail::interrupt_flag`) can
   stay as the external-interrupt source: it means "cancel the one running query
   and all of its workers," so there is no cross-query interference to design.
   This does not remove query-local cancellation ownership: `ExecutionContext`
   owns a separate error-limit/stop state for worker failures and executor
   shutdown. Document this single-concurrent-query guarantee and its rejection
   error as released runtime behavior.

   **Error selection must be deterministic, not first-to-publish.** With one
   query but many worker threads, "publish exactly one error" must not mean
   "publish whichever worker happened to fault first" — that varies with
   scheduling, thread count, and morsel grain, and would make a failing query
   report different errors run to run. When two or more morsels fault in the
   same run, the reported error is defined as the one from the **lowest morsel
   `sequence`** (equivalently, the error the serial single-threaded execution
   would have hit first), not the first error published in wall-clock time.
   Concretely, workers pull morsels in increasing sequence from the central
   cursor. On a fault at sequence `s`, they atomically lower the context's
   `error_limit` to `min(error_limit, s)`. Workers must finish every already
   claimed morsel whose sequence is at most `error_limit`; a worker may abandon
   (or decline to claim) only a morsel above that limit. Since the cursor is
   monotonic, all sequences below a claimed `s` have already been claimed, so
   this drain rule ensures they can still reveal an earlier serial error. A
   lower-sequence fault lowers the limit again; higher-sequence faults are
   retained only for diagnostics and cannot replace the selected error. The
   executor returns only after all morsels through the final `error_limit` have
   completed, selecting the minimum fault sequence. This is deliberately not a
   blanket immediate-stop rule: prompt cancellation applies to work above the
   current error limit, while correctness requires lower-sequence work to
   drain. Computation errors must never call `request_interrupt()` or mutate the
   process-wide flag. An external interrupt (observed through the process-wide
   flag) is a distinct sentinel and wins over a data error, matching serial
   semantics where an interrupt unwinds immediately. A parity test must assert
   that a program with two deterministically faulting rows reports the same
   error under 1, 2, and N threads.
7. **Extern policy.** Until the registry has explicit purity/thread-safety
   metadata enforced by the executor, any row expression containing an extern,
   plugin source, or table consumer remains serial. Never infer thread safety
   from parser effect annotations alone. Eligibility overall is owned by
   `build_operator()`'s single eligibility pass (it works on node roles), but the
   extern/plugin scan is an expression-tree analysis, not a node-role check;
   specify which pass walks the expression trees of the candidate `Passthrough`
   nodes and reports the result up to that single eligibility decision, so the two
   do not each grow their own copy of the check.

## LazyTable Synchronization Contract

`LazyTable` (`include/ibex/runtime/lazy_table.hpp`) is the shared source object
behind every parquet/deferred scan. Before it can be touched by more than one
worker, it needs a documented ownership contract — not blanket "make it
thread-safe." The hazard surface is narrower than the whole object, and scoping
the fix to the actual shared mutable state is what keeps late materialization
intact.

**Hazard surface — two boundaries, not one.** There are two distinct concurrency
concerns, and `cache_` is only the first.

The **first** is `cache_` (`robin_hood::unordered_map<std::string, ColumnEntry>`),
a memo of **whole-file** columns — the only *directly* writable shared data
member. The plain-data members are set once at construction and never
reassigned (`schema_`, `rows_`, `stats_`), so they are safe to read
concurrently. It is a mistake — one an earlier draft of this plan made — to
think the selective paths avoid `cache_`:

- `project` / `materialize` decode whole columns straight into `cache_`.
- `project_where` and `join_key_selection` decode their **predicate and
  dynamic-key** columns whole-file through `decode_whole_columns`, which
  **writes** `cache_` (and reuses any already-cached column).
- `project_rows` — and `join_key_selection`'s fused branch, which calls it —
  **reads** `cache_`, gathering the selection from any column previously cached
  whole-file.

What the selective paths actually bypass is **caching their gathered output**: a
selection-scoped column must never masquerade as a whole-file cache entry (that
would corrupt a later whole-column read). That is a correctness invariant about
what gets *written into* `cache_`, not isolation *from* it. So the real
safe/unsafe boundary is **per operation, not per path**: every selective call
splits into (a) a whole-file predicate/key decode that reads or writes shared
`cache_`, and (b) a selection-scoped decode + gather that lands only in
caller-owned output. Even a single `project_where` does both.

The **second** boundary is the callbacks. `decode_` (`ColumnDecodeFn`) and
`key_filter_scan_` (`KeyFilterScanFn`) are `std::function`s supplied by the
plugin backing the source. The stored function object is never reassigned — but
that is **not** the same as being safe to call concurrently. The callable closes
over the source's reader state (an Arrow/Parquet `FileReader`, its column
readers, decode buffers), which is generally mutable and typically *not* safe
for concurrent calls; a single Arrow `FileReader` is not. Freezing `cache_` does
nothing for this: contract half (b) — the per-worker selection-scoped decode —
*is* a concurrent invocation of `decode_`, and the fused `key_filter_scan_`
decodes during its scan the same way. So the source must guarantee one of:

- the callbacks are safe to call concurrently (internally synchronized, or
  genuinely per-call stateless); or
- each worker is handed its own decoder/reader instance — the source exposes a
  per-worker factory or clone, not one shared closure.

This is a **source/plugin registration requirement**, aligned with the
`thread_safe` plugin metadata (see [Generated C++ and
Plugins](#generated-c-and-plugins)). A source that makes neither guarantee is
ineligible for concurrent decode even with `cache_` frozen: its `decode_` calls
must be serialized, which in practice means hoisting the whole decode into the
single-threaded build phase (fine for predicate/key columns; it forfeits
parallel late materialization of that source's projected output). For the
Parquet source the realistic path is per-worker readers (open-per-morsel or a
reader pool), since one `FileReader` cannot be shared across concurrent calls.

**Interim gate (Phase 0).** Until the contract below is implemented, a query
that reads any lazy/deferred source is ineligible for a parallel island. This
is a gate the executor's single eligibility pass records, not a property of
`LazyTable`. It costs nothing in Phase 0 (no parallel execution exists) and must
not be treated as the answer: excluding lazy sources permanently would restrict
parallelism to fully-materialized in-memory tables — the small cases — while the
large memory-bound scans that parallelize best stay serial, inverting the
payoff. Lifting the gate is a Phase 3/4 obligation, not optional.

**The contract — immutable-after-build.** `LazyTable` has two lifecycle phases
per query:

1. A **build/decode phase** that is single-threaded or externally synchronized.
   Any write to `cache_` happens here, before execution fan-out.
2. A **read-only execution phase** in which `cache_` is never mutated.
   Concurrent readers need no *mutual exclusion*, but they still need a
   happens-before edge from the build-phase writes: the fan-out point (task
   submission) must publish the frozen `cache_` — release on the dispatching
   thread, acquire on each worker — exactly as the dynamic filter is published
   below. "Read-only" removes the lock, not the obligation to make the writes
   visible; without the edge a worker can observe a stale or mid-rehash
   `robin_hood` map. "Read-only" is also stronger than "no writes": readers take
   **raw pointers** into cache-held buffers (`membership_pass_rate` aliases a key
   column's `data()`/validity through `KeyColumn`, and those pointers outlive the
   lookup), so the execution phase must forbid **eviction or replacement** of
   entries, not merely concurrent mutation. A rehash is harmless (buffers sit
   behind `shared_ptr` and do not move), but any later LRU or
   re-`insert_or_assign` on `cache_` mid-execution would dangle those pointers.

Concretely:

- **Whole-file `cache_` writes hoist into the build phase.** Because the
  selective paths write `cache_` for their predicate columns, "build" must
  pre-decode not just whole projections but **every predicate column any deferred
  scan references** — the driver already knows these (`DeferredScan` carries
  `conjuncts`, `demand`, and `key_column`). The **dynamic key column is
  deliberately excluded** from this list; pre-caching it disables an existing
  optimization, so it gets its own decision below. After fan-out `cache_` is
  read-only, so concurrent `project_rows` / `project_where` reads are safe. This
  matches the Phase 1 morsel model (workers read absolute row indices from shared
  immutable input) and keeps synchronization off the hot path. Fallback where
  pre-decode is impossible: per-slot synchronized fills (a `std::once_flag`/future
  per column name) so concurrent demand for one column decodes exactly once.
  Duplicate uncoordinated decodes of the same column are forbidden either way.
- **The dynamic key column — pre-cache vs. fused key-scan is an explicit
  choice, not a default.** `project_where` and `join_key_selection` take the
  fused `key_filter_scan_` path *only when the key is not already cached*
  (`!cache_.contains(key)`, lazy_table.cpp:183 and :446): the source evaluates
  the join-key filter inside its own decoder and never materializes the key
  column whole-file — the decode-fusion Stage 4 win (q17 −54%, q08 −51%). So the
  "pre-cache every predicate column" rule above must **not** be extended to the
  key by reflex: doing so silently forces the whole-key fallback. Pick one:
  - **(a) Accept the whole-key fallback.** Pre-cache the key like a predicate
    column; the fused path is skipped, the key is decoded whole-file and filtered
    in memory. Simplest for concurrency (workers only read a frozen cache), but
    it regresses precisely the queries the pushdown work optimized — so it ships
    only with a benchmark quantifying the loss.
  - **(b) Preserve fused scans (recommended).** Do *not* pre-cache the key. Run
    key selection (fused `key_filter_scan_`, or its whole-key fallback) as the
    serial **phase A** of the two-phase deferred probe, before fan-out. Phase A
    is once-per-scan, not per-morsel, so it needs no per-worker decoder; only
    phase B (projected-column `project_rows` over the surviving selection) fans
    out. This also falls out naturally from phasing: deferred-probe joins do not
    parallelize until Phase 4, so the key-selection stays serial until then
    regardless. Should phase A itself ever be parallelized, it inherits boundary
    2's per-worker-decoder requirement.
- **Selective paths run per-worker only on an already-populated cache.** With
  predicate columns pre-cached, a phase-B worker running `project_where` /
  `project_rows` on its own morsel only (a) reads immutable `cache_` and
  (b) decodes its own selection into its own output via
  `decode_(…, &selection)` — sharing nothing writable. That decode is the
  parallel late-materialization work and must stay lazy; do not pre-materialize
  the projected output. Two preconditions are load-bearing here: (1) a worker
  that hits a **not-yet-cached** predicate column would perform a `cache_`
  write, which must not happen concurrently (the dynamic key is decoded in serial
  phase A, not in a phase-B worker — see the key-column decision above); and
  (2) the per-worker
  `decode_(…, &selection)` calls are concurrent, so `decode_` must be
  concurrently callable or each worker must own its decoder (the second boundary
  above). Eligibility must guarantee **both** — pre-cache *and* concurrent-safe
  decode — or fall back to serial.

**The freeze is per query, and the object outlives the query.** A `let`-bound
`LazyTable` (a `LazyTablePtr` in the source registry) persists across statements,
so `cache_` accumulates and the build/execution split is a *per-query cycle*:
each query's build phase reopens `cache_` for writes, then re-freezes it for that
query's fan-out. This thaw is only safe under the one-query-at-a-time invariant
(Phase 0 item 6): the executor must guarantee every worker of query N has
**joined** before query N+1's build phase writes `cache_`. Tie the thaw to that
join barrier — otherwise a straggler reader from the previous query races the
next query's build. "Immutable-after-build" is therefore a per-execution
property re-established each query, not a write-once-forever one.

**Respect the late-materialization tension.** Late materialization decodes
*during* execution — the two-phase deferred probe (`join_key_selection` +
`project_rows`) is itself the parallel work. The pre-cache requirement therefore
covers exactly the **whole-file predicate columns** that flow through `cache_`
(plus the dynamic key only under key-decision option (a)), and nothing more: the
selection-scoped decode of projected output (`decode_(remaining, &selected)` in
`project_where`, `decode_(missing, &selected)` in `project_rows`) stays lazy and
per-worker. "Pre-decode everything" is wrong in *both* directions — hoisting the
projected output would defeat late materialization, while leaving a predicate
decode lazy leaves a concurrent `cache_` write. Hoist the whole-file half, keep
the selection-scoped half lazy.

**Dynamic scan-filter publication.** The join build side publishes a
Bloom/IN-list/min-max filter into the probe-side scan's `DynamicScanFilter`
(`project_where`'s `dynamic`/`dynamic_key`, `join_key_selection`). This is a
build→probe handoff. The one-query-at-a-time pipeline already orders it: the
join build completes (hash table built *and* filter published) before any
probe-side scan worker launches, so it is a structural happens-before rather
than a lock. Make that explicit, and sharpen what `ready` means. Today `ready`
is a plain bool and `!ready` is a *sound no-filter fallback* — the header
contract says a scan materialized before the join decides simply decodes
unfiltered (interpreter.hpp:276–280), which is safe only because publication and
consumption are the same thread. Under a parallel build→probe dependency,
redefine `ready` as the **atomic publication of the build's decision**, not of a
filter's mere presence: the build stores `ready = true` *even when it decides to
produce no filter* (empty payload), with release semantics, after the payload
(`min`/`max`/`in_list`/`bloom`) is fully written; the probe reads `ready` with
acquire. The payload is **immutable after that release store** — no lazy
finalization, no in-place edits by a probe. With the dependency asserted in the
executor, every probe worker observes `ready == true` carrying a definite
decision (a filter, or deliberately none), so the "`!ready` ⇒ decode unfiltered"
fallback is **prohibited** on the parallel path: a probe that sees `!ready` there
is an ordering bug, not a tolerable race. The serial interpreter keeps the old
fallback; the prohibition attaches only once the parallel build→probe dependency
exists.

**Tests.** A parity test asserting a query over a lazy/deferred source returns
the identical answer under 1, 2, and N threads; one exercising whole-column
`cache_` reuse across two concurrent references to the same source, proving the
column decodes once and both readers see it; a filter-publication test that a
probe fanned out under N threads always observes a published decision (`ready`
true, filter *or* deliberately none) and never the `!ready` fallback, including
the build-decides-no-filter case; — whichever key-decision is chosen — a test
pinning it: under (a) that the key is pre-cached and answers match, or under (b)
that the fused `key_filter_scan_` path is still taken (the key stays out of
`cache_`) and answers match; and a **callback-contract test that actively
detects a violation** rather than trusting the prose — a fake `ColumnDecodeFn`
(and `KeyFilterScanFn`) that, on entry, sets an atomic in-flight flag and fails
the test if it is already set, then clears it on exit. For a source declaring
its callbacks serialized, drive a fan-out and assert the flag never trips (the
executor really did serialize). For a source declaring per-worker readers, hand
each worker a distinct decoder instance from the factory and assert both that no
single instance is ever re-entered concurrently *and* that the instances handed
out are distinct — so a regression that quietly shares one reader across workers
fails loudly instead of racing silently.

## Phase 1 — First Parallel Island

**STATUS (2026-07-24): slices 1a and 1b have landed.** The executor exists and
scales; eligibility is still the narrow set below.

- **Slice 1a — serial island.** `ExecutionContext.parallel` makes
  `build_operator()` consult `analyze_parallel_island()` at its seam and run an
  eligible row-local chain over `PartitionedTableSource` morsels, serially.
  `SerialIslandOrderValidator` + `preserve_empty_morsels` enforce a 1:1
  input→output morsel contract with stamped identity, and the lazy/deferred
  source gate is applied at the seam.
- **Slice 1b — worker pool and ordered merger.** `WorkerPool`
  (`include/ibex/runtime/worker_pool.hpp`) is the process-owned, pre-spawned,
  mutex+condvar pool described below. `ParallelIslandOperator`
  (`src/runtime/chunked.cpp`) dispenses morsels from one atomic cursor, runs a
  per-worker copy of the map chain, and merges results through a bounded ring
  indexed by `sequence`. Errors are recorded by lowest sequence, and no morsel
  below a reported failure is abandoned, so the error a query reports does not
  depend on thread timing. `island_worker_count()` is the grain-size serial
  threshold; below it the 1a serial chain still runs. Knobs: `IBEX_PARALLEL`,
  `IBEX_THREADS`, `IBEX_MORSEL_ROWS`. Islands stay **off by default**.

**Two findings from 1b's measurements, both worth carrying forward:**

1. **The multi-chunk concat was quadratic.** `MaterializeOperator` reserved
   exactly `size + chunk` per chunk, and `reserve` allocates precisely what it
   is asked for — so every chunk reallocated and copied the whole accumulated
   column. Invisible at one chunk; at 306 morsels it was 93% of runtime in
   `memmove` and made the island 15× *slower* than the serial path. Fixed by
   growing the reservation geometrically. This was a pre-existing defect on
   every chunked path, not an island-specific one.
2. **A computed `select` did not reach the island — fixed by slice 1c below.**
   `filter … , select { y = f(x) }` lowers to `Project(Update(Filter(x)))`, and
   `Update` was a barrier, so the island captured only the top `Project` while
   the work stayed serial. `canonicalize` R6 fuses that shape into the eligible
   `FilterUpdateProject`, but only where it runs — and `lower_script()`
   optimizes its *result* plan while leaving `shared_bindings[].plan`
   un-optimized, so a `let`-bound query in a script keeps the unfused shape.
   **That gap is not island-specific and is worth its own look: those binding
   plans skip every optimizer pass, not just fusion.**

### Slice 1c — row-local `Update` eligibility

`execution_capability()` gained a node-aware overload. A bare `Update` is still
a barrier in general, but an unguarded, ungrouped, tuple-free update whose every
field is scalar-only is classified `ParallelMap` — the conditional
classification this section already called for. The field test is
`is_subset_evaluable_expr`, deliberately stricter than the
`is_row_local_update_expr` that routes an update to the serial
`ChunkedUpdateOperator`: the looser one admits aggregates, which per morsel
would become per-morsel aggregates. `ChunkedUpdateOperator` now propagates
morsel identity, without which the island's own validator rejects its output.

Measured on the same 20M-row table, with the heavy expression in an `update`
(whole-script wall, ~0.5s of it serial generation and aggregation):

| threads | serial | 1 | 2 | 4 | 8 | 16 |
|---|---|---|---|---|---|---|
| wall (s) | 2.37 | 3.15 | 1.92 | 1.25 | 1.09 | 1.01 |

Before this slice the same query was ~2.6s at *every* thread count, because the
`Update` holding the arithmetic was a barrier.

**Acceptance measurement (local, 24-core WSL2, release, 20M rows).** The
compute-heavy row-local workload the acceptance criterion asks for, expressed as
a heavy filter predicate (six `exp`/`tanh` terms) so the work lands inside an
eligible node. Whole-script wall time, so it includes ~0.5s of serial generation
and aggregation that no island touches:

| threads | 1 | 2 | 4 | 8 | 16 | 24 | serial (no island) |
|---|---|---|---|---|---|---|---|
| wall (s) | 3.08 | 1.89 | 1.36 | 1.08 | 0.96 | 0.98 | 2.64 |
| CPU | 99% | 169% | 256% | 360% | 516% | 591% | 99% |

Net of the serial fraction the island itself is roughly 2.6s → 0.5s at 8
threads. That is the executor working. The bandwidth-bound cases behave exactly
as the Amdahl section predicts: a `y = x + q` island is ~1.0s at any thread
count, and still *slower* than the 0.52s serial path, because the Phase-0
materializing gather and the serial merge concat cost more than the map saves.
**Range-aware zero-copy kernels are therefore the next lever, not more
threads** — until a morsel stops being a copy, low-arithmetic-intensity islands
cannot win.

### Phase 2 slice 2a/2b: range-aware filter (landed 2026-07-25)

`RowRange {begin, count}` threads through the filter's evaluators and
`ColResult` carries an `offset`. The offset enters at exactly one place — the
borrowed `ir::ColumnRef` leaf in `eval_value_vec` — so everything computed from
it is dense and only kernels taking a *borrowed* operand need it. A `Filter` or
`FilterProject` island head is now absorbed into `RangeFilterMorselSource`,
which evaluates the predicate over the input's rows directly; the morsel is
never materialized.

**The prediction above was wrong, and the measurement is worth keeping.**
Removing the gather moved a 20-filter/5M-row island from 1.48s to 1.38s
(min-of-6, interleaved) against 0.94s serial — about 7%. The gather was *not*
the island's dominant cost, and removing it alone does not make a
bandwidth-bound filter island beat serial.

The discriminating experiment: with a selective predicate, where the output and
therefore the merge concat is small, the island does win — 0.14s vs 0.16s
serial (0.17s gathering), and 0.14–0.16s vs 0.18–0.21s for `filter …, select`.

**So the remaining cost is the merge copy, not the gather.** A filter island
materializes its output per morsel and `MaterializeOperator` then copies all of
it again, so the island does twice the output copying the serial path does.
That is why the win tracks output size rather than input size. Removing it
needs the final output presized and workers writing into disjoint slices —
which for a filter means a two-phase pass (compute all masks, popcount for
exact per-morsel sizes, presize, then gather into slices), because a filter's
cardinality is not known up front. **That two-phase filter island is the next
lever, and it is a bigger structural change than 2a/2b were.**

### The two-phase filter — DONE, and it is the largest island win so far

`TwoPhaseFilterOperator` (chunked.cpp) replaces the ordered merger for an island
that is *exactly one* range-native `Filter` / `FilterProject`:

- **Phase A** (parallel) — every morsel evaluates the predicate and packs its
  surviving rows into keep words. Only the counts matter afterwards.
- **prefix sum** (serial, O(morsels)) — each morsel learns the row, and for
  string columns the byte, where its output begins. The output is allocated
  once, at exactly the final size.
- **Phase B** (parallel) — every morsel gathers its rows straight into that
  output at its own offset. Disjoint slices, no locking, nothing copied twice.

One chunk is emitted, and `MaterializeOperator` *moves* its first chunk rather
than concatenating it, so the merge copy is gone rather than merely cheaper.
Ordering is structural (a morsel's rows land at its prefix-sum offset), so there
is no ring and no merger.

Interleaved, min-of-5, 20M rows / 6 columns / 8 threads, net of generation. The
"merger" column is the same binary with the gate forced off, so this isolates
the two-phase pass rather than comparing builds:

| shape | serial | ordered merger | two-phase |
|---|---:|---:|---:|
| 4 filters, 93% kept | 1686 | 2489 | **265** |
| 4 filters, 6.7% kept | 133 | 133 | **~0** |
| 4 scalar-call filters | 297 | 277 | **32** |

Every round beat every round on all three shapes. The bulk-output filter — the
one shape where parallel had been *losing* to serial, and the reason islands
were still off by default — is now 6.4x faster than serial and 9.4x faster than
the merger it replaces.

**Bit-packed columns: handled, not excluded.** Disjoint output *rows* are only
disjoint *memory* for columns with at least one addressable unit per row. A
`Column<bool>` or a validity bitmap stores 64 rows per word, so two morsels
meeting mid-word read-modify-write the same word.

Note that a 64-row-aligned morsel grain does **not** fix this, and it is worth
recording why, because it is the obvious idea: the destination offset is the
prefix sum of *popcounts*, not of morsel sizes. With a 64-row grain, a morsel
keeping 37 rows leaves the next one starting at output row 37. Input alignment
buys nothing; the only case where it would is 100% selectivity.

What works instead (`SharedBitWords` in filter.cpp): a gather writes a
*contiguous* run of output bits, so it can meet a neighbour only at the two ends
of that run — every word strictly between them is exclusively owned. So the
first and last word are OR-ed in with a relaxed `std::atomic_ref::fetch_or` and
everything between is a plain store. Two atomics per morsel per bit-packed
column (~600 for a 20M-row island) rather than one per word.

Soundness rests on the writes being **monotonic**: the destination is
zero-filled before any gather runs, and these writes only ever *set* bits, so OR
is commutative and associative and the interleaving cannot matter. That is why
the validity gather now skips its false bits instead of assigning them — the old
`set(j, v)` had to clear, which `fetch_or` cannot express. Rewriting it to
accumulate a word at a time also made the *serial* path faster: 2412ms -> 2081ms
on a bulk nullable filter, unchanged on a selective one (which gathers 14x less
validity — the mechanism check).

`filter_gather_is_thread_safe` survives as a per-column-kind allowlist, so a new
variant alternative defaults to "not safe" and costs a fallback rather than a
silent race.

Measured on the same shapes with a nullable column present (20M rows, 7 cols,
interleaved min-of-4, net of generation) — "narrow" is the pre-widening binary,
which refuses these columns and so runs the merger:

| shape | serial | narrow (merger) | widened |
|---|---:|---:|---:|
| 4 filters, 93% kept | 1972 | 3133 | **321** |
| 4 filters, 6.7% kept | 197 | 153 | **~0** |

**Testing the race needed deliberate sizing.** The 1000-row cases pass whether
or not the shared words are atomic — too few boundaries, too short a window.
The regression test uses 50k rows at grain 37 (coprime with 64, so essentially
every boundary lands mid-word) on 8 threads, and fails 5 runs out of 5 when the
`fetch_or` is replaced by a plain `|=`.

**The refactor that made it expressible.** `filter_table_impl` was split into
`compute_filter_selection` / `build_filter_output_layout` /
`count_selected_chars` / `presize_filter_output` / `gather_selection_into`
(declared in interpreter_internal.hpp), and the serial path was routed through
them first, so the parallel filter shares one gather with the serial one instead
of growing a second that can drift.

**Observability:** `ParallelIslandStats::two_phase_filters`, for the same reason
as `range_heads` — both paths produce byte-identical output, so a silent fall
back to the merger (a narrowed gate, a newly nullable column) would cost the
merge copy again with every test green. Mutation-verified three ways: forcing
the gate open, collapsing the row prefix sum, and collapsing the byte prefix sum
each fail the suite. Notably, forcing the gate open failed **only** the counter
assertion — the bit-packing race did not manifest at 1000 rows, which is exactly
why the counter is load-bearing.

Not yet range-aware: `evaluate_field` (the per-row registry path shared with
`update`), whole-column builtins, and `eval_scalar_over_columns` all evaluate
whole-table and slice. Each is documented at `slice_column` in filter.cpp.

**These ARE reachable from an island, and the first version of this slice shipped
a 10x regression because of it.** Island eligibility is
`is_subset_evaluable_expr`, which admits Scalar calls — and every call, `abs(x)`
included, routes to one of those fallbacks. A head absorbing `abs(a) > 50` re-ran
`abs` over the whole 20M-row input once per morsel: 305 full-table passes,
measured at 3.7s against 0.38s serial. (An earlier draft of this section claimed
the opposite; it was wrong.)

`is_range_native_expr` (filter.cpp, declared in interpreter_internal.hpp) is the
gate. `range_filter_head` requires it, so a scalar-call predicate stays on
`GatherMorselSource`, where the morsel is materialized once and the predicate
then runs over morsel-sized data. **It is a claim about what the evaluators do,
so it must be widened in step with them and never ahead of them** — ahead
reintroduces the blowup, behind only costs a gather. e2e tests assert
`range_heads == 0` for `abs(price) > 350` and for a scalar call buried in one arm
of an `&&`.

The invariant is enforced, not just documented — the original defect passed every
correctness test, so a comment was demonstrably not enough. Two aborts catch the
two distinct failures:

- `require_range_evaluable`, at the `filter_table_range` /
  `filter_project_table_range` boundary: a caller passed a partial range without
  consulting the gate.
- `slice_computed`: the gate admitted a predicate an evaluator branch still
  cannot evaluate by range — the two have drifted apart.

Because a partial range can no longer reach a whole-table branch, the slicing
fallback itself is gone (`slice_column` deleted); `slice_computed` now only
unwraps the whole-range case. Both aborts were mutation-verified: widening the
gate to admit `CallExpr` trips the first, removing the check from
`range_filter_head` trips the second.

Range-threading `evaluate_field` is what would both remove that restriction and
unlock the 1:1 `Project`/`Update` shapes, where the output cardinality *is* known
and the merge copy can be removed without a two-phase pass.

Build one bounded, ordered parallel pipeline:

```text
materialized Table
  -> PartitionedTableSource (stable TableRangeMorsels)
  -> parallel filter / project / fused filter-update-project
  -> ordered merge
  -> existing serial operator pipeline
```

Eligibility is intentionally narrow:

- no extern/plugin calls;
- no generators or RNG in the first landing;
- no transforms that read neighbouring rows or depend on global order;
- no group-by, tuple fields, windows, ranks, joins, sort, distinct, models, or
  streaming event sources; and
- every output column/validity bitmap is either task-owned or written into a
  disjoint preallocated range.

Scope the first landing to `ParallelMap` paths — `Filter`, `Project`, `Rename`,
`FilterProject`, `FilterUpdateProject`. Standalone `NodeKind::Update` is a barrier,
although `build_operator()` already routes eligible row-local updates through
`ChunkedUpdateOperator`. Making bare update part of the parallel island still
is not free: the eligibility pass must make its role conditional on the
row-local eligibility proof, and `ChunkedUpdateOperator` needs the same
range-aware task form. Keep the first landing to `FilterUpdateProject`
(recommended), or make that conditional-classification/range-update work an
explicit prerequisite rather than an implication of the existing chunked path.

For these paths, add range-aware shared kernels rather than constructing sliced
input `Chunk`s: `filter/project/filter-update-project(input, [begin, end), ...)`
reads the immutable source by absolute row index and returns a task-owned
result. The merger appends results strictly by morsel `sequence`. The executor
keeps the source table alive until every task has joined, including error and
cancellation paths.

**Allocator and output ownership — chosen strategy (1b).** Each task owns the
chunk it produces and the merger's consumer moves it straight into the
downstream concat. Nothing escapes into task-local scratch, so no arena
ownership is transferred and `ScratchArena` is unused. This is the simplest
strategy the paragraph below allows; the presized query-owned buffer pool is the
next option if allocation shows up in the measurements. It has not yet been
stressed across selectivities and allocator configurations — that measurement is
still owed before any allocator claim.

**Allocator and output ownership.** Parallel task-owned filter output means
many concurrent, variable-sized allocations — a known risk given the
`project_bench_alloc_cliff` allocator sensitivity. Treat allocator behavior as
a Phase-1 design variable, not a benchmark afterthought. Establish and measure
one explicit strategy before making throughput claims: a query-owned/presized
buffer pool for escaping output, task-local arenas whose allocated blocks are
transferred to the ordered merger, or another allocator with equivalent
ownership and contention guarantees. `TaskContext::ScratchArena` is for
temporary intermediate work and must be reset per task; an output `Chunk` may
not retain pointers into it unless the backing allocation's lifetime is
transferred to the query/merger. Measure allocation count, allocator wait or
contention where available, and peak RSS alongside island/merge time, across
worker counts, morsel sizes, selectivities, and the supported allocator
configurations.

**Handoff into the serial pipeline.** The island terminates at
`-> existing serial operator pipeline`, but the merged output lives outside the
`Operator::next()` world; re-entering it means presenting the merged result as a
materialized `Table` behind a `TableSourceOperator`. There are two explicit
handoff paths. A 1:1 project/update has a known output cardinality, so workers
may write disjoint preallocated final-column ranges; that final storage is the
materialized `Table` handed downstream without a second copy. A filter, and any
fused path containing a filter, has data-dependent per-morsel cardinality. Its
workers produce task-owned output chunks, and the ordered merger must serially
concatenate them into the final table in sequence order. It cannot promise
disjoint preallocated ranges or a zero-copy merge. This serial concat is still
the only handoff materialization — do not feed those chunks into a second
`MaterializeOperator` concat — but it is part of the Amdahl serial fraction and
must be reported separately for the headline filter workloads.

The existing copy-on-write table discipline remains valid: input columns are
read-only and table metadata mutation (`add_column`, `replace_column`,
`mutable_column`) stays at orchestration boundaries.

Benchmark this island before widening eligibility. Small inputs stay serial via
a grain-size threshold; do not pay task overhead to parallelize cache-resident
work.

### Phase 1 expectation and Amdahl boundary

Phase 1 is deliberately **not** a whole-query parallelization milestone. Its
speedup is bounded by the fraction of a query spent in the eligible row-local
island, plus the serial ordered merger and any downstream barrier. A query that
reaches `order`, `join`, `group_by`, rank, or a window early may show little or
no end-to-end improvement even when the parallel map itself scales correctly.
That is expected Amdahl behavior, not by itself a reason to abandon the design.

The success workload for this phase is the materialized-trades filter suite:
`filter_simple`, `filter_and`, `filter_arith`, and `filter_or` in
`benchmarking/bench_ibex_compiled.sh`, at large row counts. These are intended
to be almost entirely scan/filter/gather work and should be reported separately
from barrier-heavy mixed queries. Add a matching `filter_update_project`
microbenchmark (filter `price * qty`, compute a row-local `notional`, then
select a narrow result) so the fused filter/update/project path has a named
success case too.

Use `filter_group_sort`, `update_group_filter`, `group_rank_filter`, and join
benchmarks as boundary checks, not Phase 1 throughput targets. Record the time
in the parallel island, ordered merge, and first serial barrier alongside total
wall time; otherwise a flat end-to-end result cannot distinguish a broken
parallel map from a correctly bounded speedup.

**The filter-only cases may be bounded by memory bandwidth rather than core
count — verify before assuming linear scaling.** A single prior measurement (an
r7i.2xlarge and a local i7-13700, at ~50% selectivity; recorded in the
`project_filter_gather_simd` note) found the output gather to be the majority of
filter time (~64% there) and memory-bound (~17 GB/s single-thread), with the
whole column streaming from DRAM. That figure is machine- and
selectivity-specific and predates this work, so treat it as a hypothesis, not a
constant. If it holds, a workload dominated by streaming reads and a selective
gather cannot be sped up past the machine's memory bandwidth by adding cores;
the same note's own estimate for a threaded gather is roughly 2–4× on large N.
So a filter-only plateau below full core count is a plausible correct outcome,
not necessarily a broken parallel map — which is exactly why the plan reports
island/merge/barrier timing rather than end-to-end speedup alone. Keep
`filter_update_project` as the fused-path correctness and throughput workload,
but do not promise near-linear scaling from one `price * qty` expression: it
remains low arithmetic intensity. Also note the ordered merger's final
concatenation of variable-length filter outputs is itself serial and
memory-bound, and on very selective filters may eat a meaningful share of the
win.

**Phase 1 needs one positive acceptance criterion, not only decomposed
timing.** Because both filter-only and `filter_update_project` are plausibly
bandwidth-bound, judging Phase 1 solely by end-to-end speedup on those risks a
flat result that cannot distinguish "executor works, workload is bandwidth-
bound" from "parallel map is broken." The decomposed island/merge/barrier timing
partly addresses this, but it can only show the map *scaled in isolation*, never
that the executor delivers a real win. Therefore promote the deliberately
compute-heavy row-local benchmark into Phase 1 itself as its acceptance gate,
rather than deferring it as future work: a row-local expression with enough
arithmetic intensity to be CPU-bound (e.g. a multi-term transcendental/price
computation over the trades table) so the island demonstrably scales with core
count before eligibility is widened. Phase 1 is complete when that workload
shows near-linear island scaling *and* the bandwidth-bound filter cases match
serial output byte-for-byte — the CPU-bound case proves the executor, the
filter cases bound the honest end-to-end expectation.

## Phase 2 — Deterministic RNG and Generators

The `RngStream` work makes independent streams possible but is not sufficient:
current expression evaluation still reaches thread-local engines and
`seed_rng()` only reseeds its calling thread.

- Put the master seed in `ExecutionContext` and thread an explicit `RngStream`
  through `ColumnEvalCtx` and all RNG generator paths.
- Make a fixed **logical RNG tile**, not an executor morsel, the unit of random
  work. Partition stable absolute source row ranges into a documented tile size
  (large enough that the number of streams is modest). The tile boundaries are
  independent of worker count, scheduling, and the executor's tunable morsel
  grain. A worker may process one or many tiles, and a task may split or combine
  tiles for non-RNG work, but it must use the same logical tiles for generator
  evaluation. The serial interpreter and transpiled path use this exact scheme.
- `ExecutionContext` owns a stream manager which creates one stream for each
  `(generator-expression identity, logical tile sequence)`, rather than one
  stream per worker or scheduled morsel. Expression identity must be stable in
  the canonical program so adding or reordering independent generators does not
  perturb an existing generator's results. Do not hand out values from a shared
  cursor: request arrival order would make values scheduling-dependent and add
  contention.
- For each generator expression, derive tile streams in sequence using
  xoshiro256++ `long_jump()` (each jump advances `2^192` steps, already used in
  `zorro.hpp` to space lanes). This gives non-overlapping tile subsequences as
  long as every tile consumes fewer than `2^192` draws. The implementation must
  make that draw bound explicit and fail rather than overlap if it could be
  exceeded. Seed-mixing (`derive_rng_seed`) is not the primary split mechanism:
  it produces statistically independent-looking streams but does not provide the
  same non-overlap guarantee.
- The user-visible contract is: a fixed seed produces the same values for the
  same logical input rows and program across thread counts, completion orders,
  and executor morsel sizes. The logical tile size is part of that contract;
  changing it requires a deliberate compatibility decision and documentation,
  rather than being an incidental performance-tuning change.
- Extend the host `RngBridge` or keep plugins serial; plugins must not silently
  fall back to unrelated thread-local RNG state.

Update `SPEC.md`, `README.md`, and `docs/index.html` when the user-visible RNG
guarantee changes.

## Phase 3 — Sources and I/O

- Make parquet/CSV scans issue stable morsels (row groups or row ranges) with
  bounded read/decode concurrency.
- Keep projection/predicate pushdown in the source; parallel decoding must not
  defeat late materialization or dynamic filter pushdown.
- Implement the [LazyTable Synchronization Contract](#lazytable-synchronization-contract)
  and lift the Phase 0 lazy/deferred-source ineligibility gate: immutable-after-build
  `cache_` (coordinated pre-fan-out decode preferred, per-slot synchronized fills
  otherwise; never duplicate uncoordinated decodes), with the selective paths
  left lazy and per-worker.
- Publish dynamic Bloom/IN-list filters with acquire/release synchronization and
  an explicit build-before-probe dependency asserted in the executor, per that
  contract. Do not rely on the current same-thread timing.

## Phase 4 — Parallel Barriers

Add one operator family at a time using worker-local state and deterministic
finalization:

1. Ungrouped and low-cardinality aggregate: local partials, fixed-order merge.
2. Group-by/distinct: local group state followed by partitioned or ordered
   deterministic merging; preserve Ibex's observed first-group order.
3. Hash join: independently build local structures, finalize a read-only global
   build table, then parallel probe morsels. Preserve join output-order rules.
4. Sort/top-k: local sorted runs plus deterministic merge.
5. Window/rank/fill/cumulative operations: only after defining partition-boundary
   handoff/halo state; these are not generic parallel maps.

For floating reductions, test and document the merge policy. A deterministic
fixed merge order avoids scheduler-dependent answers, but may not be bitwise
identical to the existing serial accumulation order.

## Generated C++ and Plugins

- A plugin/extern callback must never call `runtime::interpret()` (or any
  future executor entry point). Plugins provide sources and functions to the
  host query; nested query execution is unsupported. The host runtime rejects a
  nested call that reaches its entry point, but the policy does not rely on
  coordinating static runtime copies linked into plugin DSOs. This keeps
  worker-pool ownership, cancellation, deferred scans, and the one-query lease
  with the embedding runtime.

- Phase 1 applies to `runtime::interpret()` only. Generated C++ emits direct,
  separate `ibex::ops::*` calls; it does **not** delegate to `interpret()` and
  therefore remains serial in this phase. Its differential-parity suite remains
  a serial semantic gate, not coverage for the parallel executor.
- Generated C++ is **not** a whole-query execution path: separately emitted
  operations do not share an `ExecutionContext` and cannot form a fused parallel
  island such as `FilterUpdateProject`. Do not claim generated-code benchmarks
  as Phase-1 parallel coverage.
- Defer generated-code runtime options and parallel execution until their API
  contract is needed. At that point choose either options-carrying
  `ibex::ops::*` overloads (which must define an execution-context lifetime) or
  a generated query entry point; do not add a mutable global "current options"
  setting now.
- Add registration metadata for plugins before parallel invocation, at minimum
  `thread_safe` and a precise execution kind. A pure scalar extern may become
  eligible later; unknown and stateful/I/O functions remain serial.

## Test Plan

- **Serial parity gate:** run the same programs through `runtime::interpret()`
  and transpiled `ibex::ops::*` output before enabling parallel execution.
  Compare schema, metadata, row order, values, categorical representation, and
  validity bitmaps. Cover filters, projects, updates, fused
  filter/update/project, empty and zero-column frames, categoricals, strings,
  booleans, and nulls.
- Configuration: unset, `1`, positive integer, `auto`, `0`, negative, and
  nonnumeric `IBEX_THREADS`; explicit options override the environment.
- Ordered parallel island: compare 1/2/N-thread outputs for filters, projects,
  row-local updates, fused filter/update/project, empty chunks, categoricals,
  strings, booleans, and nullable columns. Assert byte-for-byte row order and
  validity equality where applicable.
- Partitioned materialized source: vary row-range size and worker count;
  exercise empty ranges, all-filtered ranges, strings/categoricals/nulls, and
  source-table lifetime during task cancellation. The result must equal the
  existing whole-table execution path in row order and validity.
- Island metadata and empty output: vary morsel size, worker count, and
  completion order for an ordered TimeFrame through filter, projection that
  retains/drops/renames ordering and time-index columns, and updates that
  overwrite those columns. Cover zero-row input, every morsel filtered out,
  and nonzero zero-column frames. Compare `ordering`, `time_index`, and
  `logical_rows` to serial output, not just rendered rows.
- Allocator/output ownership: stress variable-cardinality filters across worker
  counts, morsel sizes, selectivities, and supported allocator configurations.
  Record allocation count, allocator contention where available, island/merge
  time, and peak RSS. Verify that task scratch storage is reset and no completed
  output retains task-arena memory without transferring its ownership.
- Scheduling: vary morsel size, worker count, and completion order; output and
  reported errors must not vary.
- Cancellation: inject a task error and an interrupt; assert no work escapes
  the query and all workers are joined before return. Add a determinism case:
  a program with two (or more) deterministically faulting morsels must report
  the lowest-`sequence` error identically under 1, 2, and N threads, regardless
  of which worker faults first in wall-clock time; an interrupt must win over a
  concurrent data error. Assert the one-query-at-a-time invariant: a second
  concurrent executor entry is rejected with the documented error rather than
  sharing the process-wide interrupt flag with the first. Assert that a
  re-entrant executor call from an extern/plugin is rejected too.
- RNG: repeat fixed-seed executions across worker counts, completion orders,
  and executor morsel sizes; values must remain identical because logical RNG
  tiles are fixed. Test multiple generator expressions, nested generators, tile
  boundaries, and the plugin bridge. Run the same cases through interpreter and
  transpiled execution to preserve parity.
- Lazy/deferred scans: race-focused tests for cache fills, filter publication,
  source sharing, and build-before-probe ordering before enabling them.
- **Race-detection gate:** add and maintain a ThreadSanitizer build (Clang
  `-fsanitize=thread`) for the runtime tests that exercise concurrent lazy
  scans. Run cache-fill, dynamic-filter publication, source-sharing, and
  build-before-probe cases in a repeat/stress harness with varied worker counts
  and morsel sizes. A normal `ctest` pass is functional coverage only; it is
  not evidence that these shared-state paths are race-free.
- Run the existing interpreter, parser/lower, join, parity, and e2e tests with
  `IBEX_THREADS=1`, then relevant subsets with multiple thread counts.
- Once both execution routes accept runtime options, run the parity suite with
  `IBEX_THREADS=1` and multiple thread counts as well. Each route must first
  match its own serial result, and the routes must continue to match each
  other.
- Benchmark release builds only with
  `benchmarking/run_scale_ibex_vs_polars.sh`, reporting Ibex 1-thread and
  auto-thread results alongside Polars single-threaded and default results.
- Phase 1 performance acceptance: run the large-row materialized-trades
  `filter_simple`, `filter_and`, `filter_arith`, `filter_or`, new
  `filter_update_project`, and the new compute-heavy row-local workloads
  independently. Report island/merge/barrier timing as well as end-to-end
  speedup; do not judge Phase 1 by barrier-heavy group-by, join, sort, rank, or
  window queries. The gate is near-linear island scaling on the CPU-bound
  workload plus byte-for-byte serial-equivalent output on the bandwidth-bound
  filter cases — not end-to-end speedup on the filter cases alone. Confirm each
  boundary-check name (`filter_group_sort`, `update_group_filter`,
  `group_rank_filter`) is actually runnable through the compiled-Ibex path being
  timed, not only through the cross-engine harnesses that currently define them
  (`print_table.py`, `gen_website.py`, `bench_datafusion.py`).

## Non-goals for the First Landing

- Replacing the whole chunked runtime with a general DAG scheduler.
- Concurrent `Operator::next()` calls on existing operators.
- Parallelizing every operator, or making plugin code implicitly thread-safe.
- Changing source/output ordering or weakening seeded-RNG reproducibility.
- Enabling multithreading by default before the narrow parallel island is
  correctness-tested and benchmarked.
