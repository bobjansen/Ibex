# How multi-core execution works

Companion to [CONTRACTS.md](CONTRACTS.md). That file states the data-and-flow
contracts (`Chunk`, `next()`, materialization, `TableProperties`, determinism);
this one states the **parallelism model** — the mental picture, the three layers,
and which part of the engine owns which decision. Kept current with the code;
[plans/parallelism-overview.md](../../plans/parallelism-overview.md) is the
companion *plan* — the catalogue of where this model is still inconsistent and
the order to fix it.

## The model in one paragraph

Ibex is a **single-process, shared-memory** engine running **one query at a
time**. There is no `Exchange` operator, no partitioned re-distribution between
operators, no plan-level degree-of-parallelism. Parallelism is **operator-internal**:
each operator decides for itself whether to fan its own row range out over a
shared worker pool, and rejoins before it returns. Two coarser layers sit on top
— **pipeline overlap** between a scan and its consumer, and a **morsel pipeline**
over a maximal chain of row-local operators. Scheduling is **dynamic
self-scheduling** off a shared atomic cursor: no work stealing, no deques, no
priorities, no DAG. Every parallel path produces **byte-identical output** to its
serial counterpart at any core count.

## The one invariant: one query at a time

`query_lease.hpp`. The runtime executes at most one top-level `interpret()` per
process; a second concurrent entry (or a re-entrant call from a plugin) is
rejected with a stable error, not serialized.

This is what lets everything else stay simple. The pool has no cross-query
fairness, admission, or preemption to arbitrate. Inputs are immutable for a
fan-out's lifetime — a `TableRangeMorsel` can hold a bare `const Table*` because
nothing else in the process can mutate it. The process-wide interrupt flag means
"cancel the one running query", so there is no cross-query interference to
design.

## The substrate: `WorkerPool`

`include/ibex/runtime/worker_pool.hpp`, `src/runtime/worker_pool.cpp`.

A deliberately boring **fork–join** pool: pre-spawned threads, one
mutex+condvar FIFO task queue, a completion latch. No futures, no continuations,
no DAG scheduler, no work stealing.

- **`submit(n, body)`** runs `body(worker_id)` for `id ∈ [0, n)` and returns a
  `Batch`; `Batch::wait()` is the barrier and rethrows the first exception *by
  worker id* (deterministic error selection).
- **`TaskGroup`** takes tasks one at a time with a single end-of-stream join —
  the shape a streaming sink wants when its input arrives one chunk at a time
  (used by the async hot-table aggregate). Adding a task is not a fork/join
  round trip.

Three properties are load-bearing:

- **The calling thread is not a worker.** In a morsel pipeline it is the ordered
  merger's consumer and must stay free.
- **Nested `submit` is safe.** A pool worker blocked in `wait_for_batch`
  *cooperatively runs queued tasks* while it waits, so a saturated set of
  parents forking child batches cannot deadlock. Therefore the
  `on_worker_pool_thread()` check at the ~29 fan-out sites is a **performance
  gate, not a correctness gate**: it declines to re-divide a pool an outer
  fan-out already saturated, because that only adds dispatch and merge overhead.
  ("Outermost wins" — but by choice, not because the inner level would break.)
- **Long-lived blocking threads do not belong in the pool.** A
  `PipelinedStageOperator`'s producer is long-lived and parks on its consumer's
  ring backpressure — a fixed pool cannot host occupants that block on another
  thread's progress. Those get a raw `std::thread`, marked with
  `StageThreadScope` so the profiler attributes their work. This is the *only*
  non-pool thread species in the runtime.

### Two thread budgets, on purpose

| Budget | Source | Governs |
|---|---|---|
| **Compute** | `IBEX_CORES` → `compute_thread_count()` → `ExecutionContext::compute_budget()` | every compute fan-out |
| **Pool size** | `decode_thread_count()` (`IBEX_DECODE_THREADS` / `IBEX_DECODE_SATURATION`, default saturation 8) | threads actually spawned |

Decode is memory-latency bound and profits from **oversubscription** until the
memory system saturates; compute is not and measurably regresses under it
(q01 +4.6%, q17 +3.2% when a compute path accidentally used the pool size).
**Every compute fan-out sizes itself from `compute_budget()`**; only
`scan_pipeline_worker_count` draws on the whole pool, deliberately.
`IBEX_THREADS` (which used to conflate the two) is read only to warn once.

## The three layers of parallelism

### Layer A — pipeline overlap (inter-operator)

Producer–consumer pipelining with bounded buffers, to overlap I/O + decode with
compute across a **pipeline breaker** (join build, aggregate, sort).

- **`PipelinedScanOperator`** — the streaming scan. The source publishes units
  (Parquet row groups); N workers each run a private copy of the row-local
  chain over one unit into a **bounded ring indexed by unit sequence**; the
  consumer releases them in order.
- **`PipelinedStageOperator`** — a two-slot double buffer running its child on a
  raw thread, so a breaker's probe input is produced concurrently with the
  breaker consuming it. Admitted only when the child can publish more than one
  unit (`has_multi_unit_deferred_scan`).

`IBEX_STREAM_SCAN=0` opts out. On by default (measured ~6–8% faster on PDS-H).

### Layer B — the morsel pipeline (was "parallel island")

A maximal bottom-up chain of **row-local, chunk-preserving** operators (filter /
project / rename / row-local update / their fused forms) run as **one
independent task per morsel**, with an **order-preserving merge** that makes the
output byte-identical to the serial chain regardless of completion order.

`MorselPipelineOperator` (`chunked.cpp`) is the executor. Key rules:

1. **Materialize the input subtree first, on the calling thread.** A
   deferred/lazy source decodes exactly once, serially, before any worker
   exists — morsel sources take `const Table&`, so the signature enforces it.
2. **Grain** (`morsel_grain`): aim for ~4 morsels per thread, clamped to
   `[4096, 65536]`. Derived, not tuned — a 1000× sweep found no grain in that
   band that loses to serial.
3. **Whether to morselize at all** is a two-dimensional gate on rows *and*
   cells (`parallel_min_rows` + `parallel_min_cells`), because a morsel's cost
   is dominated by copying rows out, which scales with table *width*. A refusal
   runs the chain as **one whole-table chunk**, never a serial sweep of morsels
   (conflating those measured 100ms vs 36ms).
4. **`TwoPhaseFilterOperator`** specialization — when the chain is exactly one
   range-native filter (+ a metadata-only project/rename tail): phase A counts
   survivors per morsel, a serial prefix sum turns counts into offsets, phase B
   re-runs the morsels writing disjoint output slices. No merger, no ring, one
   chunk emitted and *moved* into the result. This is the largest morsel-pipeline
   win and why every island shape now beats serial.

The naming moved from "island" to "morsel pipeline" when the abstraction
dissolved into a pipeline *mode* (`2b960a21`); older prose still says "island".

### Layer C — intra-operator fan-out

The bulk of the wins. Every site is fork–join inside one operator call, with a
private gate. Summarized by parallel axis:

| Operator | Axis | Determinism device |
|---|---|---|
| Fused bounds scan | row ranges, block-aligned to 4096 | per-range survivor lists concatenated in range order |
| Group discovery (`try_discover_partitioned`) | radix/hash partitioning by key hash | partition-local ids, then a serial first-occurrence merge assigns global ids in row order |
| Grouped / ungrouped aggregation | partial pre-aggregation into per-morsel private slots, then merge | morsel count derived from **rows**, merged in ascending morsel order → machine-independent float reduction |
| Owned aggregation (single-Int64 / PairIntKey) | partition-owned key maps + slots | merge-path co-ranking on globally-unique `first_rows` (parallel finalize) |
| Hash join probe | probe rows; build side is a shared read-only index, never threaded | per-worker `(li, ri)` pairs concatenated in range order |
| Semi/anti predicate | probe rows against a frozen set | ascending concat |
| Distinct | packed-key partitions | workers record a keep-flag at a row, never a position |
| Sort — gather | (column × row range) tasks, 64-row-aligned | permutation already fixed; disjoint writes |
| Sort — per-group slice sort / rank | one group per task | groups own disjoint row spans |
| Parquet decode | (column × row group) tasks, one `FileReader` per worker | tasks write disjoint output ranges |

Recurring idioms: **private-state-then-merge** wherever a reduction exists;
**disjoint-slice writes** wherever one does not; **count → prefix-sum →
scatter** for anything variable-width.

## Who owns which decision

The answer depends on the operator category. Map chains and the promoted
Distinct, Join, and Aggregate policies have a clean plan/executor split;
several remaining breaker internals do not.

### The stable parts

| Concern | Owner |
|---|---|
| Plan shape, pushdown, join order | logical IR / optimizer — never an execution accident |
| Compute budget + the on/off switches (`stream_scans`, `parallel_join_probe`, derived `can_fan_out`) | **`ExecutionContext`**, one authority per setting, applied once by `configure_parallel_from_env` — never a `getenv` at a use site |
| Running bodies, joining, deterministic error order | `WorkerPool` — no scheduling policy, no eligibility opinion |

### Parallelism ownership, by operator category

| Category | Whether parallel + how | Represented in `physical::Plan`? | `explain physical` shows it? |
|---|---|---|---|
| **Map chains** (Filter/Project/Rename/row-local Update, fused) | **The physical planner owns it end to end.** `plan.mode` (`Serial`/`MorselParallel`), `parallel_begin`/`parallel_end` (which steps run over morsels), and per-step `MapStep` (capability + kernel factory + column signature). | **Yes, fully.** | Yes. |
| **Join** | The plan owns the **structure** — `JoinPlan` carries build side + runtime-resolved orientation (`49188c71`) — and **both** fan-out phases: `build_partitions` reads `par_.build`, `probe_parallel_workers` reads `par_.probe` (slices 4–5). What stays in the operator is the kill switch / nesting / per-chunk floor. Output assembly is inside `ChunkedInnerJoinOperator`. | **Yes** (structure + both phases, both authoritative). | Structure + both phases. |
| **Aggregate** | The plan owns the adaptive strategy, positional `AggregateColumnMapping`, four typed hash-fallback nodes (discovery → accumulation → final ordering → emission), and both current fan-out policies (`partition`, `finalize`). A serial coordinator invokes every node: discovery transfers one bounded chunk of group IDs/column bindings to accumulation, or marks a one-pass owned kernel explicitly fused; final ordering and emission are separate. Each node has an independent execution-profile row. | **Yes for shape and current policy; policy is not yet one-per-structural-node.** | Strategy + structural chain + both policy phases. |
| **Distinct** | The plan owns the `dedup` policy (floor, ceiling, packed-key strategy, optional estimate); the builder resolves it and the operator reads it. Nesting, the first concrete chunk's row count, and the derived partition count remain runtime decisions. | **Yes, authoritative.** | Yes. |
| **Order** | The plan describes one `sort` phase. The actual radix-sort/gather fan-out reads shared `ExecutionContext` knobs in `sort.cpp`, so this phase is descriptive rather than authoritative. | **Yes, descriptive.** | Yes. |
| **TopK / Head / Tail / FilterHead / FilterTail** | Plan-built serial breaker operators. They have no current fan-out point; TopK deliberately uses a bounded streaming heap rather than a full sort. | **Yes; no parallel policy.** | Serial-by-design reason. |
| **Remaining Layer C fan-out inside operators** (sort gather, decode, semi/anti predicate, and data-dependent aggregate specialization gates) | The operator owns decisions not yet promoted. Migrated Distinct, Join, and Aggregate paths instead read their resolved plan policy and retain only runtime/data-dependent admission checks. | Mixed. | Only promoted phases. |

**There is not yet one owner for every breaker's parallelism.** The physical
plan owns map chains and the promoted Distinct, Join, and Aggregate policies;
other breaker internals remain unrepresented and tunable only at their use
sites. Closing the remaining gap — replacing aggregate's coarser
partition/finalize policy pair with policy attached to each structural node,
then scheduling those nodes independently — is
`kernel-pipeline-execution-plan.md` Phase 4.

### The split enforced by migrated parallel paths

**The plan says whether parallel execution is *permitted*; the operator handles
facts available only at runtime** (actual rows/cells, morsel count,
`on_worker_pool_thread()`, and data-dependent specialization gates).
`plan.mode == MorselParallel` is a capability, and a serial execution of that
plan (`can_fan_out()` false) must still be correct — a q19 crash under
`IBEX_CORES=1` came from an executor that checked only `plan.mode`. Breakers not
yet promoted still combine these halves at their use sites.

## Worked example: `t[distinct { g, v }]`

1. **Logical IR** — a `Distinct` node over a `Scan`. The optimizer decides
   column demand (`g`, `v`), nothing about execution.
2. **`plan_physical`** — records a migrated `Breaker(Distinct)` with one
   `dedup` phase: packed-key strategy, 32768-row floor, worker ceiling, and any
   available row estimate. `explain physical` renders that unresolved policy.
3. **`build_physical_distinct`** — resolves the policy against the
   `ExecutionContext` and worker pool, then passes it to
   `ChunkedDistinctOperator`.
4. **`ChunkedDistinctOperator`, first chunk** — reads the resolved permission
   and cap, applies the facts only it knows (nesting and actual rows), and pins
   a derived partition count for later chunks. Each worker scans the chunk for
   its partition. Workers record keep-flags by row, so output is rebuilt in
   input order rather than completion order.

The externally visible compute knob is `IBEX_CORES`; the 32768-row floor and
packed-key strategy are named once by `distinct_dedup_parallelism` and carried
by the physical plan.

## The determinism contract

Byte-identical output between serial and parallel at any core count, any grain,
any threshold. This is the best property the codebase has — it makes every
threshold a free parameter, movable without a correctness argument, and makes
`diff <(IBEX_CORES=1 …) <(IBEX_CORES=8 …)` the primary verification method.

The devices:
1. Row order reconstructed **positionally** — sequence, range order, or a
   prefix-summed offset — never by completion order.
2. Float reduction order fixed by a **data-derived** partition (the aggregate
   derives morsel count from row count, never thread count).
3. Group ids assigned by **first occurrence in row order**, after the parallel
   phase, so ids do not vary with thread count.
4. Errors selected by **lowest sequence / lowest worker id**; an interrupt
   outranks a data error.

The only legitimate exceptions: PDS-H q01/q09/q15 differ by ≤1 ulp from parallel
float reduction order (itself thread-count-independent), enumerated in
`beat-polars-plan.md` §5. Anything else that differs is a bug. The former
two-Int64-key owned-aggregate divergence no longer reproduces; the
"two-key grouped aggregate is deterministic across thread counts" regression
test now guards the serial and parallel paths.

## Configuration surface

| Variable | Reads | Meaning |
|---|---|---|
| `IBEX_CORES` | `compute_thread_count()` | compute budget (`auto` = `hardware_concurrency`; **parallel is on whenever this is ≥ 2**, which is the default on any multicore box) |
| `IBEX_DECODE_THREADS` | `decode_thread_count()` | absolute pool-size override |
| `IBEX_DECODE_SATURATION` | `decode_thread_count()` | memory-system saturation point (default 8) |
| `IBEX_MORSEL_ROWS` | `morsel_rows_from_env()` | morsel grain override; also lowers `parallel_min_rows` to it |
| `IBEX_STREAM_SCAN` | `stream_scans_from_env()` | Layer A streaming scan on/off (default on) |
| `IBEX_JOIN_PROBE` | `parallel_join_probe_from_env()` | parallel join probe on/off (default on) |
| `IBEX_CHUNK_ROWS` | `source_chunk_rows_from_env()` | force multi-chunk sources (test-only) |
| `IBEX_PARALLEL_STATS` | `process_pipeline_stats()` | print `ParallelPipelineStats` at exit — the only way to tell "ran parallel" from "silently fell back to serial" |
| `IBEX_PROFILE_OPERATORS` | `execution_profile_requested()` | per-operator phase profile (barrier wait, ring wait, occupancy) |
| `IBEX_THREADS` | — | **ignored**, warns once (split into `IBEX_CORES` + `IBEX_DECODE_THREADS`) |
| ~~`IBEX_PARALLEL`~~ | removed | serial is `IBEX_CORES=1` — a budget of one *is* parallelism off, with no second spelling that can disagree |

The decoder and the operators **must never read the environment directly** —
`ExecutionContext` is the single authority, so there is no second copy free to
disagree.

## Where the model is still muddy

**The structural one:** breaker parallelism has no owner above the operator (see
"Who owns which decision"). A breaker's fan-out decision, its partition count,
and its worker cap are private to `chunked.cpp`, invisible to `explain
physical`, and un-A/B-able except through `IBEX_CORES`. Every other item below is
a symptom of that — the private thresholds and worker caps exist because there
is no plan-level place to put them. Fixing the altitude (decomposing breakers
into planned phases, `kernel-pipeline` Phase 4) is what makes the rest
tractable; fixing the symptoms first just moves nine constants into one header.

**The symptoms** (`plans/parallelism-overview.md` Part 2 is the live
catalogue): type-exclusion rules with no shared "is this type parallel-capable
in role X" predicate (I2/I3); nine private row thresholds beside the two
`ExecutionContext` knobs, none sweepable without recompiling (I6);
`parallel_min_cells` consulted by 2 of ~30 sites (I7); worker-count caps that
differ arbitrarily (I9); cancellation reaching Layers A/B but not Layer C (I13).

---

# Target: parallelism as a plan decision

Status: **spec, not built** (2026-08-27). This is the end state the breaker
decomposition (`kernel-pipeline-execution-plan.md` Phase 4) builds toward. It is
written before the code so it can be checked once rather than argued during
every slice. Nothing here changes a determinism device or a kernel — it moves
*who decides* and *where the tunable lives*, not *how the work is done*.

## The principle

**Every fan-out point in the engine is described by the physical plan.** The
planner computes the policy; `explain physical` prints it; a test asserts it; a
benchmark gates it. The operator becomes an executor of that policy plus two
genuinely-runtime checks it alone can make.

Map chains already meet this (`plan.mode`, `parallel_begin/end`, per-step
`MapStep`). The target extends the same treatment to every breaker.

## What the planner decides vs. what the operator decides

The split is the same one `JoinPlan` already made deliberately (it names the two
inputs and leaves *which side is hashed* to the build phase, because that
depends on measured row counts a plan cannot know):

| Decided by the **planner**, at plan time | Decided by the **operator**, at run time |
|---|---|
| Is this fan-out permitted by the query's budget? (`exec.can_fan_out()`) | Am I running nested under another fan-out? (`on_worker_pool_thread()`) |
| The worker **cap** — `min(compute_budget, pool_size, this-breaker's-own-max)`, computed **once, here**, not open-coded per operator | Did this input clear the row floor? (`rows >= floor`, checked on the first chunk of a streaming source) |
| The row **floor** — a named constant per breaker category, in `physical_plan.hpp`, not in `chunked.cpp` | The partition count, when the plan left it "derive": `min(cap, …)` from the actual first-chunk row count |
| The partition **strategy** (packed-key / radix-hash / owned) | — |
| A row **estimate** when one is available (footer stats for a bare scan; child's exact count for `Distinct(Distinct(…))`), and the fan-out **prediction** that follows from it | — |

`on_worker_pool_thread()` cannot move to the plan: the plan is built once, but
the same operator can be constructed and pulled from a nested context (a join's
build side). It stays a runtime check. So does "did this chunk clear the floor",
because a streaming source's row count is not known until it arrives.

Everything else that is currently inside a breaker's `next()` — the `32768`
hardcoded twice in `ChunkedDistinctOperator`, the `min(budget, pool, 64)`
repeated at ~10 sites with three different caps — moves to the planner.

## The descriptor

```cpp
// physical_plan.hpp
struct RowEstimate {
    std::size_t rows = 0;
    enum class Source { None, Footer, ChildExact, Guess } source = Source::None;
};

enum class FanOutDecline : std::uint8_t {
    None,          // may fan out
    SingleCore,    // exec.can_fan_out() is false
    BelowFloor,    // a confident estimate is under the row floor
    // (OnPoolThread and the first-chunk floor check are runtime, not here)
};

struct BreakerParallelism {
    FanOutDecline decline = FanOutDecline::None;
    std::size_t   worker_cap = 1;        // the ONE place this is computed
    std::size_t   partition_count = 0;   // 0 = operator derives from first-chunk rows, ≤ cap
    std::size_t   row_floor = 0;         // the plan value; default from a named constant
    PartitionStrategy strategy = PartitionStrategy::PackedKey;
    RowEstimate   estimate{};
};

// One breaker = one or more named phases, each with its own fan-out point.
// Distinct/Order/TopK have one; Join has two (hash-build / probe); Aggregate
// has structural discovery / accumulation / final-ordering / emission nodes
// plus the partition + finalize policies used by today's fused kernels.
struct BreakerPhase {
    std::string_view    name;
    BreakerParallelism  parallelism;
};

// Hangs off physical::Plan for a breaker root; JoinPlan/AggregatePlan gain a
// `std::vector<BreakerPhase> phases` beside their existing structural fields
// rather than a second parallel struct.
```

## `explain physical` output

`explain_physical(plan)` renders the **unresolved** capability (a `Plan` off
`plan_physical` has no `ExecutionContext`); the builder resolves a local copy at
construction, so a future `explain physical <query>` command with an exec in
hand would show `cap≤N` / `serial (…)` in place of `cap=unresolved`.

Distinct, off `plan_physical` (unresolved):

```
Breaker(Distinct)
  dedup: parallel-capable  cap=unresolved  floor 32768  partitions=derived  packed-key
         no row estimate -> decided on first chunk
```

Distinct with a footer estimate (bare `Distinct(Scan)`) — the estimate line:

```
Breaker(Distinct)
  dedup: parallel-capable  cap=unresolved  floor 32768  partitions=derived  packed-key
         estimate 3200000 rows (footer)
```

Distinct, resolved single-core:

```
Breaker(Distinct)
  dedup: serial (single core)
```

Order (done): one descriptive phase, no per-breaker ceiling, floor inherited:

```
Breaker(Order)
  sort: parallel-capable  cap=unresolved  floor (shared parallel_min_rows)  partitions=derived  row-range (sort + gather)
        no row estimate -> decided on first chunk
```

Head / Tail / TopK — a serial single-operator breaker:

```
Breaker(Head)
  serial (single-operator breaker, no fan-out point)
```

Join — the strategy line, then one line per fan-out phase. Both floors
(`build_partitions`'s `1U << 17U`, `probe_parallel_workers`'s `1U << 14U`) and
the shared `min(budget, pool, 64)` cap live on the plan now;
`resolved_join_parallelism` fills a `physical::JoinParallelism` at build time and
the operator reads it (slices 4–5).

```
Breaker(Join)
  Join(StreamingProbe branch=SingleKeyInner) keys=1
  hash-build: parallel-capable  cap=unresolved  floor 131072  partitions=derived  head-table (partition by key hash)
  probe:      parallel-capable  cap=unresolved  floor 16384  partitions=derived  range (contiguous probe-row slices)
```

(Distinct/Order do not print their key list — `DistinctNode`/`OrderNode` do not
carry it as an accessible field; it lives in the child columns / the order
keys, which `explain_breaker` does not walk.)

## Migration invariants

Every slice:
- **Byte-identical** serial and parallel output — `diff <(IBEX_CORES=1) <(IBEX_CORES=8)` on all 22 PDS-H answers, plus the full 1800-test suite.
- **`ParallelPipelineStats` counters unchanged** for every PDS-H query (the fan-out happened the same number of times).
- **The `BreakerParallelism` for a query is deterministic** — same across runs, same across thread counts (it's a function of `ExecutionContext` + footer stats, both stable).
- **Observability before authority.** For each breaker: land the planner computing `BreakerParallelism` + `explain physical` printing it, with the operator keeping its existing logic and a *debug assert* that its runtime decision matches the plan's prediction where the plan could predict. Only once that has run clean (full suite, both core counts) does the second slice delete the operator's copy and have it read the plan.

## Non-goals

- **Not a scheduler.** The plan still does not schedule anything; `WorkerPool`
  still just runs batches. This describes decisions, it does not sequence them.
- **Not per-morsel.** One `BreakerParallelism` per operator instance, decided
  once.
- **Not removing the runtime checks.** `on_worker_pool_thread()` and the
  first-chunk floor check are the operator's, permanently.
- **Not the `chunked.cpp` split.** That is Phase 5, and it is deliberately
  *after* this — the plan says decomposition makes the split easier.
- **Not a row-count estimator project.** The estimate is opportunistic (footer
  stats, exact child counts). `partition_count = 0 / derive` is the honest
  default and preserves today's behavior exactly.

## Sequence and blockers

1. **`BreakerParallelism` + Distinct** — **DONE.** One phase, no eligibility
   gate, self-contained fan-out. Landed as two slices: observability (the
   planner describes the `dedup` phase, `explain physical` prints it, the
   operator aborts on disagreement) then authority (the operator reads
   `dedup_plan_.decline` / `.worker_cap`; `kMinRows` / the open-coded
   `min(budget, pool, 64)` / `check_dedup_plan` deleted). Byte-identical.
2. **Order** — **DONE (descriptive).** One fan-out point, and it already lives
   in `sort.cpp` (`radix_sort`, `gather_rows_parallel`) gated on the *shared*
   `parallel_min_rows` / `parallel_min_cells` knobs, not a private constant —
   `ChunkedOrderOperator` just buffers and calls `order_table`. So the plan
   carries one `sort` phase (`PartitionStrategy::RowRange`, `row_floor` left 0 →
   `resolve` fills it from `parallel_min_rows`), `explain physical` renders
   `Breaker(Order)` instead of the old `MapPipeline` mislabel, and nothing in
   the operator changed. The move also fixed `explain physical` for every
   migrated breaker (Head/streaming-join/streaming-aggregate all used to print
   `MapPipeline\n  source: TableScan()`). `resolve_breaker_parallelism` now
   makes the shared knob the default and a non-zero `row_floor` (distinct's
   32768) a visible per-operator override.

   **TopK is not in this bucket.** `ChunkedOrderedLimitOperator` is a serial
   streaming bounded-heap operator — O(n·log k), one pass, `push_heap` /
   `pop_heap` capped at `count`, with a `heap.front()` skip fast path. It does
   **not** sort and has **no fan-out point**, so it needs no `BreakerPhase` —
   the plan records "serial, by design", like `Head`. Parallelizing it (per-range
   local heaps + merge) is barely worth it for small k, and Ibex already wins
   top-k ~12×. Do not fold it into the Order port.
3. **Join** — **DONE (slices 3–5).** Slice 3 (descriptive): a streaming join's
   plan carries two phases, `hash-build` (`PartitionStrategy::HeadTable`, floor
   `1U << 17U`) and `probe` (`PartitionStrategy::Range`, floor `1U << 14U`), and
   `explain physical` prints the strategy line plus both. Slices 4–5 (authority):
   `resolved_join_parallelism` resolves *both* phases where the
   `ExecutionContext` is in hand (every join construction site shares it, as one
   `physical::JoinParallelism`). `ChunkedInnerJoinOperator::build_partitions`
   reads `par_.build` and `JoinProbe::probe_parallel_workers` reads `par_.probe`,
   neither re-deriving the floor or the `min(budget, pool, 64)` cap. The checks
   that stay in the operator are the ones only it can make: a kill switch
   (`IBEX_JOIN_BUILD_SERIAL` / the `parallel_join_probe` toggle), nesting
   (`on_worker_pool_thread`), and whether this side/chunk cleared the floor. Both
   fan-outs now have a `ParallelPipelineStats` counter (`parallel_hash_builds`,
   `parallel_probes`) so a gate that silently stopped matching is a red test, not
   a slow query.

   **Not a speed slice.** The hash build's parallel fill landed earlier
   (`2458bfda` + `36e93fbd`, measured q21 −8.5%); slices 3–5 moved the *decision*
   onto the plan, byte-identical throughout. The `probe_parallel_workers`
   `on_worker_pool_thread()` veto was measured to fire 0/52 on PDS-H, so folding
   it changed nothing.
4. **Aggregate** — `AggregatePlan` gains `partition` + `finalize` phases (the
   two fan-out points `ChunkedAggregateOperator` has today; discovery and
   accumulate are one `pool.submit`, so they are one phase until that region is
   actually decomposed in Phase 5). **Determinism blocker cleared (2026-08-27):**
   the `try_owned` vs serial re-association divergence recorded below does not
   reproduce on the current tree — the serial probe path, the owned path, and a
   strict-row-order reference all agree bit-for-bit at every thread count
   (verified two ways: unit-test `==` comparison + an interleaved-A/B byte hash,
   1c vs 8c vs base). The Aug-25→27 aggregate commits (`04d56853` parallel
   sorted-key first-occurrence merge, `d1cfcaa0` async hot table, `f675397e`,
   `d5928ee2`) reconciled it. Removing `try_owned`'s schedule gate outright was
   also tried and reverted — correctness stayed byte-identical but 1-core q20/q18
   regressed +25%/+40%. The guard test now exists: `tests/test_interpreter.cpp`
   "two-key grouped aggregate is deterministic across thread counts". Slice 1
   (observability) LANDED: `aggregate_{partition,finalize}_parallelism`,
   `plan_physical` fills the phases, and `explain physical` prints them —
   byte-identical, full suite + q01/q10/q13/q18/q20/q21 at 1c/8c.
   Slice 2 (partition authority) LANDED: `try_owned` and `try_discover_
   partitioned` read `par_.partition.{decline,worker_cap}` for fan-out
   permission and the worker cap; the open-coded `!can_fan_out()` +
   `min(budget, pool, 64)` are gone from both. The floors stay in the operator
   — the radix `kDefaultPartitionMinRows` beside its constant, `try_owned`'s
   lower `kPairOwnedMinRows` as the operator-resolved "owned specialization
   worth it" gate (like the join's build orientation). Byte-identical vs base
   on q01/q10/q13/q18/q20/q21, full suite.

   Slice 3 (finalize authority + the async-hot partition gate slice 2 missed)
   LANDED: `finalize_owned`'s co-ranking merge, the ordered-run merge, the
   first-occurrence seed pass, and the async-hot cold build read
   `par_.finalize.{decline,worker_cap}` for their ceiling and permission; the
   data-derived cap terms (`part_count`, `total/4096`, `run_count/8192`) and the
   three strategy floors (`1U<<17`, `1U<<16`, `parallel_min_rows`) stay in the
   operator. `try_async_hot_int_sum` (the q18 path — a fourth `partition`
   strategy slice 2 did not touch) reads `par_.partition` too now. New
   `ParallelPipelineStats` counters `parallel_aggregate_partitions` /
   `parallel_aggregate_finalizes` so a silently-stopped gate is a red test.
   Byte-identical vs the slice-1 base on q01/q10/q13/q18/q20/q21, full suite
   1814/1814. **The Aggregate step of "parallelism as a plan decision" is
   complete.** The executor now has an explicit physical-plan seam; mutation
   tests alter mapped positions, phase order, and worker ceilings and prove the
   changed plan is consumed or rejected rather than reconstructed locally.
