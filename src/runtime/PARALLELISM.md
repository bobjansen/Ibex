# How multi-core execution works

Companion to [CONTRACTS.md](CONTRACTS.md). That file states the data-and-flow
contracts (`Chunk`, `next()`, materialization, `TableProperties`, determinism);
this one states the **parallelism model** — the mental picture, the three layers,
and which part of the engine owns which decision. Kept current with the code.
Where the model is still inconsistent is catalogued below ("Where the model is
still muddy"); the plan that tracked it, with its measurement history, was
retired 2026-10-05 (`git show e2bd45bc:plans/parallelism-overview.md`).

## The model in one paragraph

Ibex is a **single-process, shared-memory** engine running **one query at a
time**. There is no `Exchange` operator, no partitioned re-distribution between
operators, and no fixed parallel lanes between operators. Parallelism is
**operator-internal**:
the physical plan permits fan-out and sets worker caps for migrated operators;
operators apply their runtime shape and size gates to work on a shared pool.
Most fan-outs join within one call; streaming task groups can span chunks and
join at end-of-stream. Two coarser layers sit on top
— **pipeline overlap** between a scan and its consumer (which may hand part of
its own work to the scan's workers), and a **morsel pipeline** over a maximal
chain of row-local operators. Scheduling is **dynamic
self-scheduling** off shared cursors, with elastic producers retiring when
bounded output buffers fill. There is one shared task queue, no per-worker
work-stealing queues, no priorities, and no DAG scheduler. Every parallel path
produces the **same rows, in the same order, with the same non-float values** as
its serial counterpart at any core count; float results may differ in their last bits (see the determinism
contract).

## The one invariant: one query at a time

`query_lease.hpp`. The runtime executes at most one top-level `interpret()` per
process; a second concurrent entry (or a re-entrant call from a plugin) is
rejected with a stable error, not serialized.

This is what lets everything else stay simple. The pool has no cross-query
fairness, admission, or preemption to arbitrate. Operators must still keep
captured inputs alive and safe for concurrent reads: the lease does not
synchronize dictionary growth or decoder state. A `TableRangeMorsel` holds a `const Table*` to the finished input
owned by its executor until its workers join. The process-wide interrupt flag
means "cancel the one running query", so there is no cross-query interference to
design.

## The substrate: `WorkerPool`

`include/ibex/runtime/worker_pool.hpp`, `src/runtime/worker_pool.cpp`.

A plain **fork–join** pool: pre-spawned threads, one
mutex+condvar FIFO task queue, a completion latch. No futures, no continuations,
no DAG scheduler, no work stealing.

- **`submit(n, body)`** runs `body(worker_id)` for `id ∈ [0, n)` and returns a
  `Batch`; `Batch::wait()` is the barrier and rethrows the first exception _by
  worker id_ (deterministic error selection).
- **`TaskGroup`** takes tasks one at a time with a single end-of-stream join —
  the shape a streaming sink wants when its input arrives one chunk at a time
  (used by async aggregate sinks and elastic pipeline producers). Adding a task
  is not a fork/join round trip.

Three properties the rest of the design depends on:

- **`submit` does not count the caller as one of its workers.** In a morsel
  pipeline the caller consumes the ordered merger. Cooperative waits and the
  scan's explicit consumer-side unit helper can nevertheless execute work on
  a calling thread; this is separate from the submitted worker count.
- **Nested `submit` is safe.** A pool worker blocked in `wait_for_batch`
  _cooperatively runs eligible queued tasks_ while it waits, so a saturated
  set of parents forking child batches cannot deadlock. Therefore, the
  `on_worker_pool_thread()` check at the ~70 call sites is a **performance
  gate, not a correctness gate**: it declines to re-divide a pool an outer
  fan-out already saturated, because that only adds dispatch and merge overhead.
  The helping gate admits only a task with a newer submit generation than the
  current task, avoiding same-generation siblings or older pipeline producers
  that could block on a ring the waiter must drain. Stage-thread batch waits
  additionally restrict helping to that exact batch. Ordinary calling-thread
  batch waits park without running pool tasks.
- **Long-lived blocking threads do not belong in the pool.** A
  `PipelinedStageOperator`'s producer is long-lived and parks on its consumer's
  ring backpressure — a fixed pool cannot host occupants that block on another
  thread's progress. Those get a raw `std::thread`, marked with
  `StageThreadScope` so the profiler attributes their work. This is the _only_
  non-pool thread species in the runtime.
  Reserving one spare worker per pipeline alone cannot ensure progress across
  several overlapping pipelines; elastic producers and restricted helping are
  part of the progress protocol.

### Two thread budgets, on purpose

| Budget        | Source                                                                                           | Governs                  |
| ------------- | ------------------------------------------------------------------------------------------------ | ------------------------ |
| **Compute**   | `IBEX_CORES` → `compute_thread_count()` → `ExecutionContext::compute_budget()`                   | every compute fan-out    |
| **Pool size** | `decode_thread_count()` (`IBEX_DECODE_THREADS` / `IBEX_DECODE_SATURATION`, default saturation 8) | threads actually spawned |

Decode is memory-latency bound and profits from **oversubscription** until the
memory system saturates; compute is not and measurably regresses under it
(q01 +4.6%, q17 +3.2% when a compute path accidentally used the pool size).
**Every compute fan-out sizes itself from `compute_budget()`**; only
`scan_pipeline_worker_count` draws on the whole pool.
The compute budget defaults to the CPUs the process may run on
(`available_cpu_count()`: the
affinity mask on Linux, so `taskset`, cpusets and container CPU sets count;
cgroup CPU quotas are not seen). At its first construction the pool uses the
compute count `C`: one thread at `C=1`, otherwise
`min(2*C, max(C, saturation))`, unless `IBEX_DECODE_THREADS` overrides it.
The pool is reused, so changing a later query's budget does not resize it.
Plugins share the host's pool: the CSV reader's
parallel eager read sizes itself from `compute_thread_count()` and the pool
size, never from its own thread count.
`IBEX_THREADS` (which used to conflate the two) is no longer read.

## The three layers of parallelism

### Layer A — pipeline overlap (inter-operator)

Producer–consumer pipelining with bounded buffers, to overlap I/O + decode with
compute across a **pipeline breaker** (join build, aggregate, sort).

- **`PipelinedScanOperator`** — the streaming scan. The source publishes units
  (Parquet row groups); N workers each run a private copy of the row-local
  chain over one unit into a **bounded ring indexed by unit sequence**; the
  consumer releases them in order. The workers are **elastic**: one claims a
  unit only when that unit's ring slot is already free, and otherwise leaves,
  returning its pool thread; the consumer submits workers again, up to N, as
  slots become available (`ElasticRingProducers::run_elastic`, `top_up`).
  `top_up` batches restarts with half-window hysteresis rather than submitting
  one task per consumed unit. So no scan worker ever
  parks holding a pool thread. Before (2026-11-15), workers parked in
  `acquire` whenever the consumer was the bottleneck, and the consumer's own
  fan-outs (an aggregate's discovery and accumulation batches) found about two
  threads: streamed `sum + count by l_orderkey` over SF-10 lineitem went
  0.45 -> 0.34 s at 8 cores. A source longer than three rounds of the pool
  still leaves one pool thread spare for downstream batches
  (`scan_pipeline_worker_count`; once a deadlock guard against every thread
  parking, which elastic workers rule out; kept because dropping it measured
  worse, q13 +7%, q15 +11%), and the consumer decodes units in
  its place whenever it would otherwise wait (`run_consumer_unit`, under a
  serial context); the ring carries one extra round so the workers are not
  starved of slots behind it. Before this, q06 at 8 cores kept 7 of 8 cores
  busy. Each unit decodes
  over its own Categorical dictionary; the consumer remaps onto a shared one
  as it emits. Row-local updates directly above or below the run join the
  worker chains when the run streams (`Plan::stream_only_updates`,
  `stream_only_bottom_updates`) and are composed serially when it does not.
- **`PipelinedStageOperator`** — a two-slot double buffer running its child on a
  raw thread, so a breaker's probe input is produced concurrently with the
  breaker consuming it. Admitted only when the child can publish more than one
  unit (`has_multi_unit_deferred_scan`).

`IBEX_STREAM_SCAN=0` opts out. On by default (measured ~6–8% faster on PDS-H).

**Consumer work on the scan workers.** A breaker directly above a streamed scan
can move its per-chunk work onto the worker that decoded the unit, while the
unit is still in that core's cache. Output still arrives in unit order, so
none of this changes an answer. Two mechanisms:

- **`ScanWorkerSink`** (`scan_worker_sink_internal.hpp`) — a hook the consumer
  offers (`ScanWorkerSinkOffer`) to the one pipeline built beneath it, admitted
  only when its input is a row-local chain directly over a lazy scan.
  `on_worker_chunk` runs on the worker, `on_emit` on the consumer; both are
  hints. Two users, both on by default: the grouped aggregate's dense
  accumulation (`DenseAggregateScanSink` — per-unit partials merged by the
  aggregate; the q01 shape) and the swapped semi/anti join's probe of its
  streamed right side (`SemiAntiScanSink`; the q04 shape, offered only when
  the left source is the smaller).
- **Attached worker steps and unit pipelines** (`try_attach_worker_step`,
  `build_unit_pipeline`) — a join probe appended to every worker chain of a
  streaming left input, or a deferred probe scan run per unit (key scan, probe,
  selected decode and assembly on one worker). **Opt-in**:
  `IBEX_STREAM_PROBES=1` (`ExecutionContext::stream_probes`), off by default.
  A unit pipeline batches its output to ~1M rows for the per-chunk breakers
  above it; a drain switches that off (`unbatch_pipeline_output`).

### Layer B — the morsel pipeline (was "parallel island")

A maximal bottom-up chain of **row-local, chunk-preserving** operators (filter /
project / rename / row-local update / their fused forms), processed as
**morsels through elastic worker tasks**, with an **order-preserving merge**.
A task can claim several morsels before retiring; a morsel is not necessarily
one pool submission. This makes the output identical in rows and order to the
serial chain regardless of completion order. A row-local chain does no cross-row float reduction, so this one is exact
in floats too.

`MorselPipelineOperator` (`pipeline_executor.cpp`) is the executor. Key rules:

1. **Finish materializing the input subtree before starting this run's
   morsel workers.** The calling thread drives materialization, but that
   subtree can itself use parallel scan/decode or operator workers. The
   morsel sources receive a finished `const Table&`; they do not concurrently
   invoke a shared lazy source or its cache. A row-local run directly over an
   eligible deferred scan instead uses Layer A's `PipelinedScanOperator`,
   where units decode concurrently and run their local chain before emission.
   `const Table&` enforces the finished-table boundary, not serial decoding.
2. **Grain** (`morsel_grain`): aim for ~4 morsels per thread, clamped to
   `[4096, 65536]`. Derived, not tuned — a 1000× sweep found no grain in that
   band that loses to serial. **Workers** (`morsel_worker_count`):
   `min(compute budget, pool, morsels)`, less one when that would take the
   whole pool on a run longer than three rounds, preserving capacity for
   downstream batches (the same measured rule as the streamed scan). Both
   executors use `ElasticRingProducers`: a worker retires when no ring slot is
   claimable, rather than parking while holding a pool thread.
3. **Whether to morselize at all** is a two-dimensional gate on rows _and_
   cells (`parallel_min_rows` + `parallel_min_cells`), because a morsel's cost
   is dominated by copying rows out, which scales with table _width_. A refusal
   runs the chain as **one whole-table chunk**, never a serial sweep of morsels
   (conflating those measured 100ms vs 36ms).
4. **`TwoPhaseFilterOperator`** specialization — when the chain is exactly one
   range-native filter (+ a metadata-only project/rename tail): phase A counts
   survivors per morsel, a serial prefix sum turns counts into offsets, phase B
   re-runs the morsels writing disjoint output slices. No merger, no ring, one
   chunk emitted and _moved_ into the result. This is the largest morsel-pipeline
   win and why every island shape now beats serial.

The naming moved from "island" to "morsel pipeline" when the abstraction
dissolved into a pipeline _mode_ (`2b960a21`); older prose still says "island".

### Layer C — intra-operator fan-out

Most compute paths fork and join inside one operator call. Async hot-table and
pair-sum paths submit work as chunks arrive and join once at end-of-stream;
their finalization can use additional batches. Plan-owned phase permissions
coexist with the runtime admission gates discussed below. Summarized by axis:

| Operator                                      | Axis                                                               | Determinism device                                                                                         |
| --------------------------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Fused bounds scan                             | row ranges, block-aligned to 4096                                  | per-range survivor lists concatenated in range order                                                       |
| Group discovery (`try_discover_partitioned`)  | radix/hash partitioning by key hash                                | partition-local ids; each row range then numbers the groups whose first row it holds (count, prefix, number), so global ids follow row order |
| Sorted group discovery (`try_discover_sorted`, Int64 / Int64 pair) | contiguous row ranges, each checked for order; no hashing | per-range run counts joined at range boundaries, a prefix gives each range its first id                     |
| Grouped / ungrouped aggregation               | partial pre-aggregation into per-morsel private slots, then merge  | morsel count derived from **rows**, merged in ascending morsel order → stable reduction order for this path |
| Sorted-chunk accumulation (`try_accumulate_ascending`) | row ranges cut where an ascending gid changes | a group's rows fall in one range, accumulated on one thread in row order: bit-identical to serial, no merge |
| Owned aggregation (single-Int64 / PairIntKey) | partition-owned key maps + slots; a PairIntKey first chunk of 2M+ rows takes the radix path instead (I15) | merge-path co-ranking on globally-unique `first_rows` (parallel finalize)                                  |
| Hash join build                               | head table partitioned by key hash, one worker per partition       | partitions are disjoint; no merge                                                                          |
| Hash join probe                               | probe rows against the built, read-only index                      | per-worker `(li, ri)` pairs concatenated in range order                                                    |
| Semi/anti join                                | dense right build; probe rows against the frozen set               | keep-bit words per row range                                                                               |
| Distinct                                      | packed-key partitions                                              | workers record a keep-flag at a row, never a position                                                      |
| Sort — radix                                  | fixed contiguous row chunks, same chunks to count and to scatter   | prefix-summed bucket offsets; stable                                                                       |
| Sort — gather                                 | (column × row range) tasks, 64-row-aligned                         | permutation already fixed; disjoint writes                                                                 |
| Sort — per-group slice sort / rank            | one group per task                                                 | groups own disjoint row spans                                                                              |
| Parquet decode                                | (column × row group) tasks, one `FileReader` per worker            | tasks write disjoint output ranges                                                                         |
| Aggregate emission                            | (output column × 64-group range) tasks                             | pre-sized destination, groups written by index                                                             |
| Row-local update fields                       | row ranges of one expression                                       | disjoint output slices                                                                                     |
| CSV eager read                                | byte ranges split at record boundaries                             | ranges concatenated in file order                                                                          |

The table lists the families, not every site. Top-k (grouped and ungrouped),
rank setup, melt, dcast, resample, `fill_forward`/`fill_backward`, as-of join,
global and grouped cov/corr, count_distinct shards and median/quantile collect
all fan out the same way, each with its own gate.

Recurring idioms: **private-state-then-merge** wherever a reduction exists;
**disjoint-slice writes** wherever one does not; **count → prefix-sum →
scatter** for anything variable-width.

### Categorical discovery and state transitions

The dense-cell limit is an indexing choice, not a limit on parallel grouping.
Codes are meaningful within the source's shared or normalized dictionary; that
identity must remain stable across chunks. Dictionary sizes can grow without
changing an existing code's value.

| Input/state | Discovery route |
| --- | --- |
| Bounded Cartesian space (up to 4M cells), with or without nulls | Dense cell index; slot-combinable aggregates can use private morsel partials. |
| First chunk already sparse, two non-null Categorical keys | Existing packed-u64 pair path. |
| First chunk already sparse, three or more keys | Data-sized packed path if the layout fits; generic fallback otherwise. |
| Non-null dictionaries grow beyond the dense limit | With multiple admitted discovery workers, adopt existing gids into the pair path (two keys) or fixed-width code-tuple partitions (three to eight). One core and wider keys retain the serial code index. |
| Sparse nullable Categorical pair, initially or after growth | Fixed 32-bit code fields, with `-1` reserved for null. Small chunks use a serial code index; admitted larger chunks can partition, including locality partitioning at one core. |
| A non-null packed pair gains a bitmap | Copy its code pairs by gid into the nullable path, reset the old partition ownership, and seed the new lookup. |
| Dense nullable keys wider than a pair outgrow the cell limit | Generic grouping, which may partition but hashes categorical text. |
| A data-sized packed layout can no longer represent a later chunk | Migrate to generic grouping. |

Adoption changes the lookup, preserving existing gids and aggregate slots. Once
partitioned discovery starts, even a small trailing chunk must consult the same
partitions. Nullable-code mode also survives bitmap-free chunks; a null never
aliases code zero. Both the code reader and generic `GroupKeyStore` check
validity before reading a null payload.

Composite row keys, boxed keys and the new fixed-code tuples use XXH3 over field
words. Where live rows, migrated keys and `GroupKeyStore` share an index, their
hashes must agree. Boxed `KeyHash` includes its own null-mask term and is not
interchangeable with `hash_key_row` for seeding that index. Specialized hashes
remain elsewhere; this is not one universal hash for every runtime container.

Reproduction instructions and benchmark harnesses live in
[the composite-key benchmark directory](../../benchmarking/composite-hash/README.md).
Cross-chunk correctness fixtures are in `tests/test_chunked_sources.cpp`.
Generated measurements and profiling output stay outside version control.

### Aggregate Discovery admission policy

`AggregateDiscoveryPolicy` collects Discovery's row thresholds and grains. An
optional override in `ExecutionContext::aggregate_discovery_policy` is copied
into the resolved `AggregateParallelism`, so an operator keeps one policy for its
lifetime. There are no new environment switches. Defaults are unchanged:

| Policy field | Default rows | Role |
| --- | ---: | --- |
| `radix_min_rows` | 262144 | General integer/string/pair partition admission |
| `packed_min_rows` | 32768 | Packed, generic and fixed-code partition admission |
| `int_owned_min_rows` | 65536 | Owned integer and async hot-sum admission |
| `pair_owned_min_rows` | 65536 | Owned pair and async pair-sum admission |
| `sorted_min_rows` | 262144 | Sorted Discovery admission, per chunk |
| `sorted_range_rows` | 65536 | Sorted Discovery range grain |
| `key_fill_range_groups` | 65536 | Key-copy group-range grain |
| `async_hot_job_rows` | 1048576 | Async integer hot-sum job grain |
| `pair_owned_max_first_chunk_rows` | 2097152 | Pair-owned first-chunk upper cutoff |

The default plan floor comes from `admission_floor()`, the minimum of the five
strategy admission floors. An explicit context override recomputes that floor
**before** resolving row estimates; lowering an operator strategy's threshold
cannot leave it vetoed by the old default plan floor. Leaving the override unset
preserves explicitly supplied physical-plan floors. Worker ceilings remain owned
by their phase descriptors. Zero floors, grains and upper cutoffs are rejected
when building the aggregate.

Shape checks, one-core cardinality sampling, cumulative rows offered, and
existing-group adoption still decide which permitted path can run. Packed keys
that cannot be reconstructed from stored groups still cannot begin partitioning
after a serial prefix. Once a path owns partitioned state, a small trailing chunk
must continue using it. Accumulation's sorted/dense thresholds are separate;
Discovery overrides do not retune that phase.

For example, an embedding can lower only `packed_min_rows` by emplacing the
optional policy and changing that field. All other strategies keep their defaults.
The [Discovery sweep](../../benchmarking/discovery-policy/README.md) exercises
whole-table and streamed inputs across rows, cardinalities and core counts;
generated samples remain in ignored storage.

### Remaining strategy thresholds

`ExecutionContext::aggregate_phase_policy` optionally supplies an
`AggregatePhasePolicy`, copied into the resolved aggregate alongside Discovery's
policy. Accumulation admission is the minimum of two private-slot morsels,
sorted rows, dense-GID rows and owned-range rows. FinalOrdering admission is the
minimum of the merge-group and ordered-run floors. Overrides refresh these
floors before row-estimate resolution; unset overrides preserve explicit plan
floors. All entries must be positive.

| Aggregate phase policy field | Default | Unit |
| --- | ---: | --- |
| `accumulation_morsel_rows` | 65536 | Input rows per private partial |
| `sorted_min_rows` / `sorted_range_rows` | 262144 / 65536 | Input rows |
| `dense_gid_min_rows` | 1048576 | Input rows |
| `owned_range_min_rows` / `owned_range_min_groups` | 1048576 / 65536 | Input rows / groups |
| `owned_range_morsel_rows` / `owned_range_window_rows` | 65536 / 4194304 | Input rows |
| `slot_init_min_bytes` | 4194304 | Fresh slot bytes |
| `merge_min_groups` / `merge_groups_per_worker` | 131072 / 4096 | Groups |
| `ordered_min_runs` / `ordered_runs_per_worker` | 65536 / 8192 | Ordered runs |

Kernel defaults are retained. FinalOrdering's default **plan** floor is now
65536, allowing its ordered-run strategy below the former 131072-row veto.
Distinct groups and compressed runs never exceed input rows, so the minimum is
a safe estimate-based floor. The first-value seeding pass retains the shared
`parallel_min_rows` gate as well as its phase permission. Emission already uses
its resolved phase floor and the shared output-group threshold.

Reduction grains depend on the input and policy, never on the worker budget.
Explicitly changing a private-partial grain may change floating-point rounding
between policies. Owned ranges preserve each group's input order. Dense-GID
admission also selects the existing two-rows-per-cell density heuristic; a
lowered threshold can therefore change the dense/hash choice at one core.
Memory budgets, key capabilities and safety alignments remain algorithm checks.

`ExecutionContext::join_threshold_policy` optionally supplies
`JoinThresholdPolicy`: build 131072 rows, probe 16384 rows, concatenation 65536
output matches, and semi/anti right-side passes 262144 rows. Build/probe floors
are refreshed before resolution. Streaming and materialized probes, and the
semi/anti left filter, use the same probe floor. The semi/anti right passes keep
their separate floor and byte budget. An unset override preserves explicit
join-plan floors. `ExecutionContext::radix_sort_min_rows` defaults to 65536 and
selects LSD versus MSD radix sorting across whole-table, streamed and ranking
calls; it does not replace the shared sort fan-out or output-cell gates. Query
execution rejects zero join/sort thresholds. No environment switches were added.

### Remaining row-range grain policy

`ExecutionContext::operator_grain_policy` contains the remaining executor grains:

| Field | Default | Role |
| --- | ---: | --- |
| `filter_bounds_rows` | 65536 | Minimum rows per fused-bounds morsel |
| `update_bucket_rows` | 32768 | Minimum rows per grouped-update bucketing worker |
| `window_split_rows` | 32768 | Minimum useful grouped-window piece |
| `decimal_partial_rows` | 65536 | Minimum rows per Decimal sum/mean partial |
| `map_min_rows` / `map_max_rows` | 4096 / 65536 | Automatic map grain band |
| `map_morsels_per_worker` | 4 | Automatic map work-piece target |

All grains must be positive, and the map band must be ordered. Query execution
and the direct filter/update/decimal entry points reject invalid policies.
The existing `parallel_min_rows` admission gate still applies to filter and
update kernels: grain controls work division after admission. A large window
grain is bounded by the input size before splitting. `parallel_grain` remains
the explicit map-grain override; automatic grain uses sequential division to
avoid overflow with a large configured morsel target. No environment switches
were added.

Decimal sum/mean now receive the execution context instead of choosing workers
from the pool alone. Their own two-partial threshold and existing scratch-cell
budget remain; workers are also bounded by the query's compute budget. A
one-core query runs the Decimal loop inline and does not create the pool.
Other Decimal aggregates retain their existing algorithms.

The [operator-grain sweep](../../benchmarking/discovery-policy/README.md#remaining-i6-executors)
uses prepared tables, paired timings, output checks and process CPU. These are
materialized row-range kernels; vary table size and group cardinality rather
than treating a synthetic chunk size as their grain. Whole-query filter plans
may instead use the map executor, so the fused-bounds sweep calls that kernel
directly and records its actual pool tasks.

### Worker ceilings and work decomposition (I9)

A compute budget, physical pool size, phase ceiling and useful work count are
separate limits. `compute_budget()` returns the requested per-query compute
budget; an explicit value is not silently clipped to machine affinity. The
pool is sized independently for decode, and may be larger. The phase resolver
computes `min(budget, pool size, breaker_max_workers)`; zero for the last term
means no additional phase ceiling. `WorkerPool::submit` knows only pool size,
not a query's context, so the operator must enforce its own limit.

| Path or limit | Current rule | Meaning |
| --- | --- | --- |
| Distinct; join build/probe; aggregate Discovery, Accumulation, FinalOrdering | Default phase ceiling 64 | Named capability policy in `physical_plan.cpp` |
| Order and aggregate Emission | No extra phase ceiling | Budget/pool, then available tasks |
| Materialized join probe; semi/anti right passes | Local ceiling 64 | Not yet carried by the same phase descriptors |
| Semi/anti left filter | Budget/pool and row ranges | The two sides already have different ceilings |
| Generic row ranges; lazy membership ranges; fused filter bounds | At most 64 ranges/morsels | Work decomposition also limits useful concurrency |
| Private aggregate partials | At most 64 partials | Data-derived arithmetic cuts and bounded scratch; not interchangeable with workers |
| Aggregate slot initialization | At most 16 workers within Accumulation's cap | First-touch/page-fault work has its own scaling limit |
| Aggregate count-distinct | 16 hash shards | Available independent state, not a generic worker policy |
| Aggregate radix discovery | At most 256 partitions, several per worker | Byte-sized partition IDs/cache locality; partitions can exceed workers |
| Semi intersection / Decimal partials | Additional private-state limits | Respectively a byte budget and a scratch-cell budget |
| Grouped updates/windows, sorts, gathers, reshape and map runs | Budget/pool plus groups, tasks or grain | No uniform additional 64-worker ceiling |
| Elastic map/scan producers | Explicit active-worker bound; sometimes reserve one pool worker | The reserve is a measured overlap choice; decode has its own budget |

The old I9 description that every remaining path was limited only by budget
and pool was incomplete. Do not normalize these numbers by changing partition
counts, reduction cuts or fixed shard arrays: those have different units and
can affect memory use or floating-point order. No measurements here establish a compute scaling cutoff at 64; this machine
has fewer hardware threads, so a large thread-count sweep would mainly test
oversubscription.

There is also an enforcement gap below those ceilings. Async integer and pair
sums use Discovery's resolved worker cap for their partition count, but their
`TaskGroup` submits independent jobs without an active-worker limit. A larger
pool can therefore execute more Discovery work concurrently than the phase
cap permits. Elastic pipeline producers use the same generic task-group API
but enforce `running_ < workers_` themselves. The
[worker-cap probe](../../benchmarking/worker-caps/README.md) reproduces the
aggregate gap with prepared inputs and attributed worker time, including the
ordinary pool policy. Fixing async admission is higher priority than making
all compile-time ceilings equal. Budgeted draining lanes/admission must retain
job boundaries and deterministic merges without blocking pool workers.

## Who owns which decision

The answer depends on the operator category. Map chains and the promoted
Distinct, Join, and Aggregate policies have a clean plan/executor split;
several remaining breaker internals do not.

### The stable parts

| Concern                                                                                             | Owner                                                                                                                             |
| --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Plan shape, pushdown, join order                                                                    | logical IR / optimizer — never an execution accident                                                                              |
| Compute budget + the on/off switches (`stream_scans`, `parallel_join_probe`, derived `can_fan_out`) | **`ExecutionContext`**, one authority per setting, applied once by `configure_parallel_from_env` — never a `getenv` at a use site |
| Running bodies, joining, deterministic batch errors                                                  | `WorkerPool` — dispatch and wait eligibility, with no operator admission decisions                                                                       |

### Parallelism ownership, by operator category

| Category                                                                                                                                     | Whether parallel + how                                                                                                                                                                                                                                                                                                                                                                                                                                              | Represented in `physical::Plan`?                       | `explain physical` shows it?                     |
| -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------ |
| **Map chains** (Filter/Project/Rename/row-local Update, fused)                                                                               | **The physical planner owns it end to end.** `plan.mode` (`Serial`/`MorselParallel`), `parallel_begin`/`parallel_end` (which steps run over morsels), and per-step `MapStep` (capability + kernel factory + column signature).                                                                                                                                                                                                                                      | **Yes, fully.**                                        | Yes.                                             |
| **Join** (streaming inner, one key or two Int64 keys; streaming left, one key)                                                              | The plan owns the **structure** — `JoinPlan` names the `JoinBranch` and leaves orientation to run time (`49188c71`); typed `HashBuild` / `HashProbe` nodes carry the edge — and **both** fan-out phases: `build_partitions` reads `par_.build`, `probe_parallel_workers` reads `par_.probe`. What stays in the operator is the kill switch / nesting / per-chunk floor. Output assembly is inside `ChunkedInnerJoinOperator`, which the left join shares. Semi/anti streams through its own operator with private gates; every other join shape materializes both sides and names its `JoinDeclineReason`. | **Yes** (structure + both phases, both authoritative). | Structure + both phases.                         |
| **Aggregate**                                                                                                                                | The plan owns the adaptive strategy, positional `AggregateColumnMapping`, and four typed hash-fallback nodes (discovery → accumulation → final ordering → emission). Each node carries and authoritatively supplies its own fan-out policy. A serial coordinator invokes every node: discovery transfers one bounded chunk of group IDs/column bindings to accumulation, or marks a one-pass kernel explicitly fused; final ordering and emission are separate. | **Yes, structural and authoritative.**                 | Strategy + typed chain + all four node policies. |
| **Distinct**                                                                                                                                 | The plan owns the `dedup` policy (floor, ceiling, packed-key strategy, optional estimate); the builder resolves it and the operator reads it. Nesting, the first concrete chunk's row count, and the derived partition count remain runtime decisions.                                                                                                                                                                                                              | **Yes, authoritative.**                                | Yes.                                             |
| **Order**                                                                                                                                    | The plan describes one `sort` phase. The actual radix-sort/gather fan-out reads shared `ExecutionContext` knobs in `sort.cpp`, so this phase is descriptive rather than authoritative.                                                                                                                                                                                                                                                                              | **Yes, descriptive.**                                  | Yes.                                             |
| **TopK / Head / Tail / FilterHead / FilterTail**                                                                                             | Plan-built serial breaker operators. They have no current fan-out point; TopK deliberately uses a bounded streaming heap rather than a full sort.                                                                                                                                                                                                                                                                                                                   | **Yes; no parallel policy.**                           | Serial-by-design reason.                         |
| **Remaining Layer C fan-out inside operators** (sort, decode, semi/anti build and predicate, the materialized fallbacks, and data-dependent aggregate specialization gates) | The operator owns decisions not yet promoted. Migrated Distinct, Join, and Aggregate paths instead read their resolved plan policy and retain only runtime/data-dependent admission checks.                                                                                                                                                                                                                                                                         | Mixed.                                                 | Only promoted phases.                            |

**There is not yet one owner for every breaker's parallelism.** The physical
plan owns map chains and the promoted Distinct, Join, and Aggregate policies;
other breaker internals remain unrepresented and tunable only at their use
sites. Aggregate's former coarse `partition` / `finalize` records are gone;
its remaining coordinator is deliberately serial while the work inside each
typed node may fan out under that node's policy.

### The split enforced by migrated parallel paths

**The plan says whether parallel execution is _permitted_; the operator handles
facts available only at runtime** (actual rows/cells, morsel count,
`on_worker_pool_thread()`, and data-dependent specialization gates).
`plan.mode == MorselParallel` is a capability, and a serial execution of that
plan (`can_fan_out()` false) must still be correct. Breakers not yet promoted
still combine these halves at their use sites.

## Worked example: `t[distinct { g, v }]`

1. **Logical IR** — a `Distinct` node over a `Scan`. The optimizer decides
   column demand (`g`, `v`), nothing about execution.
2. **`plan_physical`** — records a migrated `Breaker(Distinct)` with one
   `dedup` phase: packed-key strategy, 32768-row floor, worker ceiling, and any
   available row estimate. `:explain t[distinct { g, v }]` renders that
   unresolved policy.
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

The same rows, in the same order, with the same non-float values, between serial
and parallel at any core count, any grain, any threshold (`CONTRACTS.md` §7).
Floats may differ in their last bits: the order of floating-point operations
(a parallel sum, a per-unit partial) is not part of the contract. Some paths
retain a fixed reduction order as an implementation choice. This keeps every threshold a free parameter, movable without
a correctness argument. Verification is `IBEX_CORES=1` against `IBEX_CORES=8`
with exact comparison of everything except floats, which match within tolerance
(`benchmarking/ab_queries.py`, `outputs_match`).

The devices:

1. Row order reconstructed **positionally** — sequence, range order, or a
   prefix-summed offset — never by completion order.
2. Where cheap, float reduction order follows a **data-derived** partition (the
   aggregate derives morsel count from row count, never thread count). This
   is best effort, retired as a guarantee: new parallel paths need neither
   this device nor an exception.
3. Group ids assigned by **first occurrence in row order**, after the parallel
   phase, so ids do not vary with thread count.
4. Errors selected by **lowest sequence or lowest worker id**; an interrupt
   outranks a data error.

A different row, row order, key, count, integer, string or decimal is a bug;
a last-bit float difference is not. (The old q01/q09/q15 exception list is
gone with the byte-identical bar.) The former two-Int64-key owned-aggregate
divergence no longer reproduces; the "two-key grouped aggregate is
deterministic across thread counts" regression test still guards the serial and
parallel paths.

**Rejected: weakening first-occurrence group order** (2026-08-17). Declaring
group-by output order unspecified would drop the serial first-occurrence merge
in `try_discover_partitioned` and let grouped aggregation stream its output. The
gain was ~26 ms of ~172 ms on q18 at 3M groups, the worst case. Against it: it
gives up this contract, it weakens the serial-vs-parallel comparison above, and
it breaks scripts (Ibex follows `data.table`, where `by=` keeps first
appearance). Do instead: elide the merge at _plan_ level when the consumer
provably ignores order (it feeds an `order`, a scalar aggregate or a join) — a
required-ordering property propagated down the plan. Not built.

## Configuration surface

| Variable                 | Reads                            | Meaning                                                                                                                              |
| ------------------------ | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `IBEX_CORES`             | `compute_thread_count()`         | compute budget (unset or `auto` = `available_cpu_count()`, the affinity mask; **parallel is on whenever this is ≥ 2**, which is the default on any multicore box) |
| `IBEX_DECODE_THREADS`    | `decode_thread_count()`          | absolute pool-size override                                                                                                          |
| `IBEX_DECODE_SATURATION` | `decode_thread_count()`          | memory-system saturation point (default 8)                                                                                           |
| `IBEX_MORSEL_ROWS`       | `morsel_rows_from_env()`         | morsel grain override; also lowers `parallel_min_rows` to it                                                                         |
| `IBEX_STREAM_SCAN`       | `stream_scans_from_env()`        | Layer A streaming scan on/off (default on)                                                                                           |
| `IBEX_JOIN_PROBE`        | `parallel_join_probe_from_env()` | parallel join probe on/off (default on)                                                                                              |
| `IBEX_STREAM_PROBES`     | `stream_probes_from_env()`       | join probes on the workers of a streaming pipeline (default **off**)                                                                 |
| `IBEX_CHUNK_ROWS`        | `source_chunk_rows_from_env()`   | force multi-chunk sources (test-only)                                                                                                |
| `IBEX_PARALLEL_STATS`    | `process_pipeline_stats()`       | print fan-out counters at exit; operator profiling also exposes worker tasks and time                      |
| `IBEX_PROFILE_OPERATORS` | `execution_profile_requested()`  | per-operator phase profile (barrier wait, ring wait, occupancy)                                                                      |
| ~~`IBEX_THREADS`~~       | removed                          | no longer read (split into `IBEX_CORES` + `IBEX_DECODE_THREADS`)                                                                     |
| ~~`IBEX_PARALLEL`~~      | removed                          | `IBEX_CORES=1` disables compute fan-out; scan decode still follows the pool budget                           |

`IBEX_CORES=1` normally also constructs a one-thread pool, but an explicit
`IBEX_DECODE_THREADS` override or a pool already created for a larger budget can
still admit parallel scan decoding. For a fresh-process serial compute/decode
measurement set both `IBEX_CORES=1` and `IBEX_DECODE_THREADS=1`; changing the
compute context alone does not resize an existing pool.

The decoder and the operators **must never read the environment directly** for
any of these — `ExecutionContext` is the single authority, so there is no second
copy free to disagree. The I/O plugins keep a few experiment-only switches of
their own that do not touch the budget (`IBEX_CSV_SERIAL`,
`IBEX_CSV_PARALLEL_MIN_BYTES`, `IBEX_CSV_PARALLEL_RANGE_BYTES`,
`IBEX_PARQUET_STRING_CHUNK_BYTES`).

## Where the model is still muddy

**The structural owner now exists for migrated breakers.** Distinct, streaming
Join, and streaming Aggregate take their fan-out policies from explicit
physical nodes and expose those decisions through `explain physical`.
Aggregate's four phases, the streaming joins, migrated-plan dispatch, and the
generic map/morsel executor each live in their own file (`aggregate_chunked.cpp`,
`join_chunked.cpp`, `runtime_entry.cpp`, `pipeline_executor.cpp`); `chunked.cpp`
is gone. Remaining
operator-private decisions belong to breaker families that have not completed
that migration; do not generalize their local thresholds into a second policy
system.

**I6 is resolved for parallel admission thresholds and row grains.** The
remaining filter/update/decimal kernels and automatic map grain now read the
context policy above. Kernel-local constants still describe hash/dense
algorithms, SIMD blocks, scratch budgets, buffer compaction and output batching;
those are not competing parallel-admission authorities.

**The remaining limits**, catalogued from 2026-08 onward (the I-numbers are
cited in code; resolved entries are in the retired plan). Some are deliberate
capability limits, others are inconsistent policy or incomplete migrations.

- **I2** — String/Categorical are excluded from two fan-out decisions, each
  for its own reason and in a different role: `stageable_conjunct_columns`
  declines to stage a variable-width predicate column through a selection, and
  `aggs_are_slot_combinable` admits only Int/Double aggregate inputs to
  private-slot partials. (Update fields are no longer on this list:
  `evaluate_field_maybe_parallel` splits Bool, Categorical and interpolated
  String results through their direct routes.) These are capability limits of
  two unrelated kernels, not one missing predicate.
- **I3** — categorical discovery still has fallback limits. Dense cells are
  limited to 4M, but exceeding that limit does not itself mean serial discovery:
  sparse non-null keys can use the pair/packed paths, and sparse nullable pairs
  use compact code tuples. Non-null dictionary growth can adopt existing groups
  into code partitions for two through eight keys when multiple discovery
  workers are admitted. The remaining gaps are non-null growth beyond eight
  keys and nullable dense-to-sparse growth beyond two keys. The former retains
  the serial code index; the latter migrates to generic grouping. A data-sized
  packed layout can also outgrow its reserved fields and fall back to generic
  grouping. See the categorical transition rules above; generic grouping may
  itself partition, but hashes categorical text rather than compact codes.
- **I7** — copy admission needs a policy for each path, rather than one shared
  cell floor everywhere. The old two-site inventory was stale:
  `runtime_internal.hpp::gather_columns_batched` already applies the width gate
  to join and reshape payload gathers, as do the materialized map pipeline and
  sort gather. Their units differ: the pipeline currently counts materialized
  **input** columns, even when a fused projection returns fewer columns; the
  projection probe confirms that unused source columns can change admission
  for the same two-column result. Sort
  and payload gathers count **output** rows times the columns actually copied
  in that batch. Join index concatenation copies two index arrays, independent
  of payload width, and retains its own output-row floor.
  Aggregate Emission uses output groups, its resolved worker cap and a column
  by range task grid. It does not read the shared cell floor. That omission is
  not itself a defect: the fixed-width [I7 sweep](../../benchmarking/width-policy/README.md)
  finds profitable narrow emission that a pipeline-sized cell floor would
  suppress. Whole-filter results also show a latency/CPU trade-off. Remaining
  work is to account for projected/copied width correctly and calibrate each
  path across rows, width, types and selectivity, validating whole-query CPU and
  latency before changing defaults. Do not infer one universal crossover from
  the count of `parallel_min_cells` readers.
- **I9** — worker ceilings need consistent meaning and enforcement. The
  inventory above separates phase ceilings, useful work, memory limits and
  reduction cuts; the former claim that all other sites were budget/pool-only
  was stale. More seriously, async aggregate task groups can exceed their
  resolved Discovery cap because partition count is bounded but active jobs
  are not. The worker-cap probe confirms this on the ordinary compute/decode
  pool split. Enforce async admission first; do not turn 64 partials or 16
  hash shards into a universal thread ceiling.
- **I10** — plan admission and runtime strategy admission must remain coherent.
  Discovery's default phase floor is now derived from its policy rather than a
  second constant; explicit context overrides update it before row-estimate
  resolution. This closes the default-policy drift between 32768-row packed,
  65536-row owned and 262144-row radix strategies. Accumulation now derives
  its floor from its strategy policy; FinalOrdering derives its floor from both the merge-group and ordered-run strategies. The
  latter removes the old 131072-row veto on a 65536-run kernel. Runtime gates
  still check observed work sizes and capabilities. Emission uses group count
  as its runtime unit; declining on a smaller input-row estimate is safe because groups
  cannot exceed input rows. Explicit physical-plan floors remain intentional
  admission restrictions when no context policy override is supplied.
- **I12** — two thread species: the long-lived stage thread of
  `PipelinedStageOperator` (parks on consumer backpressure; hosting it on the
  pool would deadlock) is named and counted (`on_stage_thread()`,
  `stage_thread_peak()`), but the pool API does not express a separate
  admission rule for blocking stage bodies.
- **I13** — cancellation reaches Layers A/B; Layer C has no uniform
  cancellation protocol inside its compute fan-outs. Consistent with the per-node/chunk/statement contract; the gap
  grows as operators get bigger parallel sections.
- **I14** — two grain philosophies: `morsel_grain` from thread count, aggregate
  morsel count from row count (for float reproducibility, no longer a
  guarantee). Reusing `morsel_grain` inside a reduction would make float
  results vary with thread count, now tolerated but avoidable.
- **I15** — one-chunk inputs. An in-memory table or a one-row-group file
  reaches an aggregate as ONE chunk, where a streamed source arrives one per
  row group; so did every REPL statement until 2026-11-15 (`52f30e38`), which
  decoded its lazy sources whole. Paths tuned on streamed chunks then lose
  their parallelism or their cache fit: the async hot Int64 sum ran one job per
  chunk (now cut by `async_hot_job_rows`, `ca3fa3ea`); the async and owned
  PairIntKey paths ran it as one job or kept per-worker tables far beyond
  cache (a first chunk at `pair_owned_max_first_chunk_rows` = 2M rows goes to
  the radix path, `82b10685`); radix discovery's table reserve, extrapolated
  from a partition's first rows, over-sized tables ~7.5x when keys repeat far
  apart (now capped by a key sample, `9f6fe94e`). It cuts the other way too:
  streamed, a small last row group started the radix path late and seeded
  every group so far on one thread (`31c1befe`), per-chunk accumulation of a
  sorted key never fanned out (`36ee3947`), and the async pair sum's final
  tables were reserved per record (`70e33583`). The rule: measure a fan-out on
  one whole-table chunk AND on streamed chunks, and compare 8 cores (and 2)
  against 1 for work multiplication. REPL statements now stream like scripts.

**Not inconsistencies** (recorded so they are not "fixed"):

- prefix sums in `TwoPhaseFilterOperator` / `try_discover_partitioned` are
  serial on purpose: O(morsels), not O(rows);
- a declined morsel pipeline runs as one whole-table chunk, not a serial morsel sweep
  (fix for a measured 3× regression);
- `parallel_threads` does not cap the scan pipeline.

**Measured and dropped:**

- **A task scheduler / work stealing** (2026-08-21): `pool_idle_ms` and
  `stage_park_ms` were ~0 on all 22 queries while ~67% of the pool sat with
  nothing queued, so stealing had nothing to steal. Revisit only if occupancy
  rises far enough for queues to build.
- **Multiple producers per staged breaker.** Decoding two sibling scans
  concurrently was worth ~15% synthetically and ~8% on q10 alone (~3%
  in-suite), but every widening measured worse: semi/anti overlap q04 +19%,
  `build_binary_materializing_operator` (since removed) q09 +57% (both sides
  materialize, so overlapping them contends for bandwidth instead of filling
  idle cores). The semi/anti scan sink is the narrow form that survived: it
  drains the smaller left first and only then starts the right's pipeline.
  Removed 2026-08-21; recover from `38d6b307` / `190235b2` / `a3b39c6d` only
  with a cost-aware gate (skip when both sides are large).

---

# Target: parallelism as a plan decision

Status: **migration record for Distinct, Order, streaming Join and streaming
Aggregate** (authority landed 2026-08-29). This section preserves the original
target and completed sequence. The current ownership table and remaining limits
above take precedence where runtime specialization gates remain (I10).
The migration moved policy ownership and tunables onto the plan while
preserving the kernels and determinism devices.

## The principle

The target is for **every fan-out point to be described by the physical plan**. The
planner computes the policy; `explain_physical` prints it (`:explain <expr>` in
the REPL); a test asserts it; a
benchmark gates it. The operator becomes an executor of that policy plus two
runtime-only checks it alone can make.

Map chains already meet this (`plan.mode`, `parallel_begin/end`, per-step
`MapStep`). The target extends the same treatment to every breaker.

## What the planner decides vs. what the operator decides

The split is the same one `JoinPlan` already made deliberately (it names the two
inputs and leaves _which side is hashed_ to the build phase, because that
depends on measured row counts a plan cannot know):

| Decided by the **planner**, at plan time                                                                                                                                      | Decided by the **operator**, at run time                                                                 |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Is this fan-out permitted by the query's budget? (`exec.can_fan_out()`)                                                                                                       | Am I running nested under another fan-out? (`on_worker_pool_thread()`)                                   |
| The worker **cap** — `min(compute_budget, pool_size, breaker_max_workers)`, computed **once**, in `resolve_breaker_parallelism`, not open-coded per operator                  | Did the observed input clear the path's gate? (per-chunk or cumulative rows, groups, cardinality, and shape)  |
| The row **floor** — a named constant per breaker category, in `physical_plan.cpp`, not in the operator                                                                        | The partition count — derived from policy and workload; locality partitioning at one core may use more partitions than workers               |
| The partition **strategy** (`PartitionStrategy`: packed-key, radix-hash, owned, row-range, head-table, range, morsel, column-range)                                           | —                                                                                                        |
| A row **estimate** when one is available (a registered table's exact count, footer stats for a bare scan, a child's exact count), and the fan-out **prediction** that follows from it | —                                                                                                        |

`on_worker_pool_thread()` cannot move to the plan: the plan is built once, but
the same operator can be constructed and pulled from a nested context (a join's
build side). It stays a runtime check. So do data-dependent admission checks:
chunks, cumulative rows offered, group counts and dictionary growth become known during execution. Aggregate discovery
can start partitioning after an earlier serial chunk, adopting its existing gids;
once partitioned, later small chunks must keep consulting those partitions.

The migrations centralized phase permission and worker ceilings, including
Distinct's named floor and the shared join caps. Aggregate Discovery now carries its strategy thresholds through a shared
runtime policy; its data-dependent admission checks remain in the operator.
Aggregate Accumulation and FinalOrdering now also carry strategy policies;
remaining algorithm constraints stay local; row-grain policies are context-owned (I10).

## The descriptor

```cpp
// physical_plan.hpp
struct RowEstimate {
    std::size_t rows = 0;
    enum class Source : std::uint8_t { None, Footer, TableExact, ChildExact } source = Source::None;
    auto confident() const noexcept -> bool;  // source != None; the planner never guesses
};

enum class FanOutDecline : std::uint8_t {
    None,          // may fan out
    SingleCore,    // exec.can_fan_out() is false
    BelowFloor,    // a confident estimate is under the row floor
    // (OnPoolThread and the first-chunk floor check are runtime, not here)
};

struct BreakerParallelism {
    // Capability half: set by plan_physical, which has no ExecutionContext.
    std::size_t row_floor = 0;            // 0 = inherit exec.parallel_min_rows
    std::size_t breaker_max_workers = 0;  // 0 = no per-breaker ceiling
    PartitionStrategy strategy = PartitionStrategy::PackedKey;
    RowEstimate estimate{};

    // Resolved half: filled by resolve_breaker_parallelism at build time.
    FanOutDecline decline = FanOutDecline::None;
    std::size_t worker_cap = 0;           // 0 = not resolved yet
};

// Untyped breakers (Distinct, Order) carry named phases in
// Plan::breaker_phases. Join and Aggregate carry the same descriptor directly
// on their typed HashBuild/HashProbe and
// Discovery/Accumulation/FinalOrdering/Emission nodes
// (Plan::streaming_join, Plan::hash_aggregate), resolved together as
// JoinParallelism / AggregateParallelism.
struct BreakerPhase {
    std::string_view    name;
    BreakerParallelism  parallelism;
};
```

## `explain physical` output

`explain_physical(plan)` renders the **unresolved** capability (a `Plan` off
`plan_physical` has no `ExecutionContext`), which is what the REPL's
`:explain <expr>` prints under the heading "Physical plan (capability; runtime
fan-out may differ)". The builder resolves a local copy at construction; a
resolved phase renders `cap<=N` or `serial (…)` in place of `cap=unresolved`.

Distinct over a registered table (the estimate is the table's exact count):

```
Breaker(Distinct)
  dedup: parallel-capable  cap=unresolved  floor 32768  partitions=derived  packed-key
         input estimate 4 rows (table)
```

Without an estimate the second line reads `no row estimate -> decided on first
chunk`; a footer estimate reads `input estimate N rows (footer)`.

Distinct, resolved:

```
  dedup: serial (single core)
  dedup: serial (estimate 4 < floor 32768)
  dedup: parallel-capable  cap<=8  floor 32768  partitions=derived  packed-key
```

Order: one descriptive phase, no per-breaker ceiling, floor inherited:

```
Breaker(Order)
  sort: parallel-capable  cap=unresolved  floor (shared parallel_min_rows)  partitions=derived  row-range (sort + gather)
         no row estimate -> decided on first chunk
```

Head / Tail / TopK / FilterHead / FilterTail — a serial single-operator breaker:

```
Breaker(Head)
  serial (single-operator breaker, no fan-out point)
```

Join — the strategy line, the typed edge, the resolved key columns, then one
line per fan-out node. Both floors (`kJoinBuildRowFloor` = `1U << 17U`,
`kJoinProbeRowFloor` = `1U << 14U`) and the shared `min(budget, pool, 64)` cap
live on the plan; `resolved_join_parallelism` fills a
`physical::JoinParallelism` at build time and the operator reads it. A streaming
left join prints the same with `branch=SingleKeyLeft`.

```
Breaker(Join)
  Join(StreamingProbe branch=SingleKeyInner keys=1 left=Scan right=Scan orientation=runtime)
  edge: HashBuild.RuntimeOrientedBuildOutput -> HashProbe.build_input
  columns: resolved  left[0]=right[0]  output=3
  HashBuild: parallel-capable  cap=unresolved  floor 131072  partitions=derived  head-table (partition by key hash)
         no row estimate -> decided on first chunk
  HashProbe: parallel-capable  cap=unresolved  floor 16384  partitions=derived  range (contiguous probe-row slices)
         no row estimate -> decided on first chunk
```

Aggregate — the strategy, the typed chain and its edges, then the four nodes:

```
Breaker(Aggregate)
  Aggregate(StreamingSorted)
  hash-fallback: Discovery -> Accumulation -> FinalOrdering -> Emission
    edge: InputChunks -> DiscoveredGroups -> AccumulatedGroups -> OrderedGroups -> OutputChunks
  Discovery: parallel-capable  cap=unresolved  floor 32768  partitions=derived  radix-hash
         input estimate 4 rows (table)
  Accumulation: parallel-capable  cap=unresolved  floor 131072  partitions=derived  morsel (deterministic row ranges)
         input estimate 4 rows (table)
  FinalOrdering: parallel-capable  cap=unresolved  floor 131072  partitions=derived  owned
         input estimate 4 rows (table)
  Emission: parallel-capable  cap=unresolved  floor (shared parallel_min_rows)  partitions=derived  column-range (output column x group range)
         input estimate 4 rows (table)
```

(`StreamingSorted` is the adaptive strategy's name: group-at-a-time when the
chunks arrive sorted on the keys, the hash fallback otherwise.)

A map chain and a fallback, for contrast:

```
MapPipeline
  Filter+Project(fused)
  source: TableScan(t)
  mode: morsel-parallel(steps 0..1 of 1)
  source signature: fixed-width/all-valid fixed-width/all-valid

MaterializedCall(Melt: root is not a row-local map)
```

(Distinct/Order do not print their key list — `DistinctNode`/`OrderNode` do not
carry it as an accessible field; it lives in the child columns / the order
keys, which `explain_breaker` does not walk.)

## Migration invariants

Every slice:

- **Same answers** serial and parallel — rows and order exact, floats within tolerance (`CONTRACTS.md` §7) — comparing `IBEX_CORES=1` with `IBEX_CORES=8` on all 22 PDS-H answers, plus the full test suite.
- **`ParallelPipelineStats` counters unchanged by a policy-only migration** for
  the same query and execution budget; comparing different core counts need
  not produce the same fan-out counters.
- **Policy is deterministic for fixed inputs and context.** Unresolved
  capability metadata does not depend on core count. Resolved permission and
  worker caps depend on `ExecutionContext`, so can differ across core counts.
- **Observability before authority.** For each breaker: land the planner computing `BreakerParallelism` + `explain physical` printing it, with the operator keeping its existing logic and a _debug assert_ that its runtime decision matches the plan's prediction where the plan could predict. Only once that has run clean (full suite, both core counts) does the second slice delete the operator's copy and have it read the plan.

## Non-goals

- **Not a scheduler.** The plan still does not schedule anything; `WorkerPool`
  still runs submitted batches and task groups. This describes admission
  decisions; it does not sequence the work.
- **Not per-morsel.** One `BreakerParallelism` per operator instance, decided
  once.
- **Not removing the runtime checks.** `on_worker_pool_thread()` and the
  data-dependent admission and transition checks remain the operator's.
- **Not ownership of the `chunked.cpp` split.** This contract enabled it; the
  split finished 2026-08-31 (`runtime_entry.cpp` plus one file per family).
- **Not a row-count estimator project.** The estimate is opportunistic (exact
  table counts, footer stats, exact child counts). The partition count is
  always derived by the operator, never planned.

## Sequence and blockers

1. **`BreakerParallelism` + Distinct** — **DONE.** One phase, no eligibility
   gate, self-contained fan-out. Landed as two slices: observability (the
   planner describes the `dedup` phase, `explain physical` prints it, the
   operator aborts on disagreement) then authority (the operator reads
   `dedup_plan_.decline` / `.worker_cap`; `kMinRows` / the open-coded
   `min(budget, pool, 64)` / `check_dedup_plan` deleted). Byte-identical.
2. **Order** — **DONE (descriptive).** One fan-out point, and it already lives
   in `sort.cpp` (`radix_sort`, `gather_rows_parallel`) gated on the _shared_
   `parallel_min_rows` / `parallel_min_cells` knobs, not a private constant —
   `ChunkedOrderOperator` just buffers and calls `order_table`. So the plan
   carries one `sort` phase (`PartitionStrategy::RowRange`, `row_floor` left 0 →
   `resolve` fills it from `parallel_min_rows`), `explain physical` renders
   `Breaker(Order)` instead of the old `MapPipeline` mislabel, and nothing in
   the operator changed. The move also fixed `explain physical` for every
   migrated breaker (Head/streaming-join/streaming-aggregate all used to print
   `MapPipeline\n  source: TableScan()`). `resolve_breaker_parallelism` now
   makes the shared knob the default and a non-zero `row_floor` (distinct's 32768) a visible per-operator override.

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
   `resolved_join_parallelism` resolves _both_ phases where the
   `ExecutionContext` is in hand (every join construction site shares it, as one
   `physical::JoinParallelism`). `ChunkedInnerJoinOperator::build_partitions`
   reads `par_.build` and `JoinProbe::probe_parallel_workers` reads `par_.probe`,
   neither re-deriving the floor nor the `min(budget, pool, 64)` cap. The checks
   that stay in the operator are the ones only it can make: the
   `parallel_join_probe` toggle (probe only), nesting
   (`on_worker_pool_thread`), and whether this side/chunk cleared the floor. Both
   fan-outs now have a `ParallelPipelineStats` counter (`parallel_hash_builds`,
   `parallel_probes`) so a gate that silently stopped matching is a red test, not
   a slow query.

   The streaming single-key **left** join (2026-11-13, `35baed71`) reuses the
   inner join's operator and both resolved phases (`JoinBranch::SingleKeyLeft`):
   the right side is always the indexed one, and it takes neither a deferred
   probe nor a streamed-left key filter.

   **Not a speed slice.** The hash build's parallel fill landed earlier
   (`2458bfda` + `36e93fbd`, measured q21 −8.5%); slices 3–5 moved the _decision_
   onto the plan, byte-identical throughout. The `probe_parallel_workers`
   `on_worker_pool_thread()` veto was measured to fire 0/52 on PDS-H, so folding
   it changed nothing.

4. **Aggregate** — four typed structural nodes now own the hash fallback's
   scheduling policy. **Historical determinism blocker cleared (2026-08-27):**
   at that checkpoint, the `try_owned` vs serial re-association divergence no
   longer reproduced in the tested cases: serial, owned, and strict-row-order
   reference results agreed bit-for-bit (unit-test `==` comparison plus an
   interleaved-A/B byte hash, 1c vs 8c vs base). The current general contract
   permits last-bit float differences; those migration checks are not a promise
   that every aggregate uses the same reduction order. The Aug-25→27 aggregate commits (`04d56853` parallel
   sorted-key first-occurrence merge, `d1cfcaa0` async hot table, `f675397e`,
   `d5928ee2`) reconciled it. Removing `try_owned`'s schedule gate outright was
   also tried and reverted — correctness stayed byte-identical but 1-core q20/q18
   regressed +25%/+40%. The guard test now exists: `tests/test_interpreter.cpp`
   "two-key grouped aggregate is deterministic across thread counts". The
   observability and authority migration removed the open-coded
   `!can_fan_out()` / `min(budget, pool, 64)` decisions while retaining
   data-derived caps (`part_count`, `total/4096`, `run_count/8192`) and
   strategy-specific admission floors next to their kernels. Existing
   `ParallelPipelineStats` partition/finalize counters remain compatibility
   telemetry; structural execution-profile rows are the per-node proof. The
   executor mutation seam verifies mapped positions, typed edges, and worker
   ceilings are consumed rather than reconstructed locally.

   Slice 4 (structural-node authority) LANDED 2026-08-29: the coarse records
   are removed. Discovery owns radix, owned, and async-hot group discovery;
   Accumulation owns deterministic private-state morsels (including fused
   categorical/global kernels) and slot initialization; FinalOrdering owns
   ordered merges and first-occurrence seeding; Emission owns independent
   output-column tasks. `explain physical` renders
   all four policies on their typed nodes. Execution-profile mutation tests
   prove a one-worker node ceiling suppresses that node's pool work without
   reconstructing defaults, while preserving byte-identical output.
