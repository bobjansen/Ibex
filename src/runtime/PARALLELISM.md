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

The honest answer is **it depends on the operator category**, and only one
category has a single clean owner today.

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
| **Join** | The plan owns the **structure** — `JoinPlan` carries build side + runtime-resolved orientation (`49188c71`) — and now **describes** the two fan-out phases (`hash-build`, `probe`) with their floors and strategies (slice 3). The gates that act on those floors, plus output assembly, are still **inside `ChunkedInnerJoinOperator`**. | Partial (structure + phase description). | Structure + phase floors/strategies. |
| **Aggregate** | The plan owns `AggregatePlan` (which construction path). **Discovery, per-partition slots, the owned-aggregate hot table, and the finalize merge are inside `ChunkedAggregateOperator`.** | Partial. | Which path, not the phases. |
| **Distinct / Order / TopK / Head / Tail** | **Nothing in the plan.** `execution_capability(Distinct)` returns `ParallelBarrier`, but that value is **never read to make a decision** — it names what a future executor *could* do. `build_physical_distinct` just constructs `ChunkedDistinctOperator`, which owns the entire decision internally: the `can_fan_out()` / `on_worker_pool_thread()` guard, a private `kMinRows` (hardcoded, and twice — accumulate and finalize), the partition count from `compute_budget()`, the two-pass "one worker per partition scans the whole chunk" model. | **No.** | **No.** |
| **Layer C fan-out inside any operator** (group discovery, sort gather, decode, semi/anti predicate) | Always the operator's, each with its own private row threshold and its own `min(budget, pool, cap)` worker count. | No. | No. |

**So: there is no single owner of "distinct parallelism" — and the same is true
of every barrier operator's parallelism.** For map chains the physical plan is
that owner; for breakers it owns construction (and for join/aggregate,
structure), and the parallel-execution decisions live inside the operator,
unrepresented, un-inspectable, and tunable only by editing the operator. Closing
that gap — decomposing the breakers into planned phases — is
`kernel-pipeline-execution-plan.md` Phase 4, which is why that plan distinguishes
"construction ownership done" (backlog 116→6) from "decomposition not started".

### The one rule that already holds everywhere

**The plan says whether parallel execution is *permitted*; the operator says
whether it is *desirable* here** (`exec.can_fan_out()`, row/cell floors, morsel
count, `on_worker_pool_thread()`). `plan.mode == MorselParallel` is a
capability, and a serial execution of that plan (`can_fan_out()` false) must
still be correct — a q19 crash under `IBEX_CORES=1` came from an executor that
checked only `plan.mode`. For breakers, both halves currently live in the
operator.

## Worked example: `t[distinct { g, v }]`

1. **Logical IR** — a `Distinct` node over a `Scan`. The optimizer decides
   column demand (`g`, `v`), nothing about execution.
2. **`plan_physical`** — the root is `Distinct`, not a map chain, so
   `plan.migrated == false`, `reason == NotMapChain`. `plan.mode` is irrelevant
   (it's for map chains). No `DistinctPlan` field exists. `explain physical`
   prints `MaterializedCall(Distinct)` — or, since `49ca33c1`, records it as a
   plan-built breaker — and says nothing about how it will run.
3. **`build_physical_distinct`** — constructs `ChunkedDistinctOperator(child,
   exec)`. Passes the `ExecutionContext` in; makes no parallelism decision.
4. **`ChunkedDistinctOperator`, first chunk** — decides *everything*: if
   `!exec.can_fan_out() || on_worker_pool_thread() || rows < 32768` it stays
   serial and pins `dedup_part_count_ = 1` for all later chunks; otherwise it
   derives `part_count` from `compute_budget()`, hash-partitions by packed key,
   and runs one worker per partition (each scanning the whole chunk, skipping
   rows not in its partition — the proven Pass-2 model). Determinism device:
   workers record a keep-flag at a row, never a position, so the output is
   rebuilt by scanning flags in row order.

The only externally visible knob is `IBEX_CORES` (via `can_fan_out()` and
`compute_budget()`). The `32768` floor and the partition strategy are editable
only in `chunked.cpp`.

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
`beat-polars-plan.md` §5. Anything else that differs is a bug. **Known standing
violation:** the two-Int64-key owned aggregate (`try_owned_pair`) and the serial
path re-associate differently and disagree bit-for-bit at ≥65536 rows, with no
test covering it — `kernel-pipeline-execution-plan.md` "The determinism
constraint is already broken".

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
// Distinct/Order/TopK have one; a decomposed Aggregate has three
// (discovery / accumulate / finalize); a Join has two (hash-build / probe).
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

Join (slice 3, descriptive) — the strategy line, then one line per fan-out
phase. The floors are the private constants `chunked.cpp` already applies
(`build_partitions`'s `1U << 17U`, `probe_parallel_workers`'s `1U << 14U`); the
operator still owns them, `build_physical_join` does not read the phases yet.

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
3. **Join** — **slice 3 landed (descriptive).** A streaming join's plan carries
   two phases, `hash-build` (`PartitionStrategy::HeadTable`, floor `1U << 17U`)
   and `probe` (`PartitionStrategy::Range`, floor `1U << 14U`), and `explain
   physical` prints the strategy line plus both. `build_physical_join` does not
   read them yet — the floors and the `min(budget, pool, 64)` cap still live in
   `ChunkedInnerJoinOperator::build_partitions` / `probe_parallel_workers`.
   Follow-up slices move the authority the way distinct's did (assert-then-read),
   then make `build_join_hash_index`'s threading a scheduled-`HashBuild`
   decision. This is where the measured cost is (q21's 40 ms serial hash build)
   — decomposition here is a speed lever, not deferred speed.
4. **Aggregate** — `AggregatePlan` gains discovery / accumulate / finalize
   phases. **Blocked-first:** `try_owned_pair` and the serial path re-associate
   differently and disagree bit-for-bit at ≥65536 rows, with no test. That
   divergence must be reconciled and an exact-equality grouped-path test written
   *before* the decomposition — otherwise it inherits a determinism bug it will
   be blamed for. See `kernel-pipeline-execution-plan.md`.
