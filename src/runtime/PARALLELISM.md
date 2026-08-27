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
| **Join** | The plan owns the **structure** — `JoinPlan` carries build side + runtime-resolved orientation (`49188c71`). The **probe fan-out, hash-build threading, and output assembly are inside `ChunkedInnerJoinOperator`** with private gates. | Partial (structure only). | Structure only. |
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
