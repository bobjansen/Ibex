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
operator builder, not a separate planner object. `plan_pipelines()` /
`PipelinePlan` (`include/ibex/runtime/pipeline.hpp`) is analysis-only and its
sole caller is `tests/test_operator.cpp`; the execution path is
`interpret()` → `build_operator()` → `MaterializeOperator::run()`
(`src/runtime/interpreter.cpp:1433`), which recurses the IR directly and never
consults `PipelinePlan`. Rather than promote that test-only segmenter onto the
hot path, `build_operator()` — which already owns operator fusion
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

The builder today reaches for `classify_node`'s three roles — `Source`,
`Passthrough`, and `Breaker` (`src/runtime/pipeline.cpp`). This plan adds a
new execution-capability vocabulary of four categories and maps it onto those
roles; it is not a classification `build_operator()` performs today:

| Category | Maps from today's role | Initial behavior | Examples |
|---|---|---|---|
| Parallel map | `Passthrough` | One independent task per morsel, ordered merge | row-local filter, project; fused filter/update/project |
| Ordered/stateful stream | some `Breaker`s | Preserve order; initially serial unless a boundary algorithm is supplied | head/tail, lag/lead, fill, cumulative functions |
| Barrier | most `Breaker`s | Materialize or synchronize before continuing; serial first | order, distinct, general join, windows, rank, model |
| Parallel barrier (later) | `Breaker` | local worker state, deterministic finalize/merge, then next stage | group-by, join build/probe, sort |

Note that standalone `Update` is a `Breaker` today, not `Passthrough` — see the
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
5. **Context instead of worker TLS (highest-risk gate).** Replace
   execution-scoped thread-local deferred scans with `ExecutionContext`
   propagation through `interpret`, `build_operator`, every applicable operator
   constructor, and expression evaluation (`ColumnEvalCtx` and its consumers).
   This is a large, benefit-free refactor in isolation, but it establishes the
   only safe ownership path for query state once work moves to another thread.
   Guard `LazyTable`'s decoded-column cache and dynamic scan-filter publication,
   or mark lazy/deferred sources ineligible until their synchronization contract
   exists.
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

## Phase 1 — First Parallel Island

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

Scope the first landing to paths that are already `Passthrough` in
`classify_node` — `Filter`, `Project`, `Rename`, `FilterProject`,
`FilterUpdateProject`. Standalone `NodeKind::Update` is currently a `Breaker`,
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
- Give `LazyTable` a documented synchronization/cache ownership strategy before
  sharing it among tasks. Per-query decode futures or synchronized cache fills
  are candidates; duplicate uncoordinated decodes are not.
- Publish dynamic Bloom/IN-list filters with synchronization and an explicit
  build-before-probe dependency. Do not rely on the current same-thread timing.

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
