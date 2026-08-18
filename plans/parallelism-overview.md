# Multi-core execution: how it actually works

Status: descriptive, updated 2026-08-18 in the worktree after `6f0a03e`. This is
not a plan — it is the map of the machinery the plans keep adding to
([runtime-multithreading-plan.md](runtime-multithreading-plan.md),
[pipelined-execution-plan.md](pipelined-execution-plan.md),
[beat-polars-plan.md](beat-polars-plan.md)). Part 1 describes the design in the
vocabulary of the parallel-database literature; Part 2 lists where the
implementation diverges from itself.

Line references are `file:line` at the commit above and will drift; the names
are the stable handle.

---

## Part 1 — The design

### 1.1 Classification in one paragraph

Ibex is a **single-process, shared-everything, shared-memory** engine using
**morsel-driven parallelism** (Leis et al., *Morsel-Driven Parallelism*, SIGMOD
2014) in an **exchange-free** form: there is no Volcano-style `Exchange`
operator, no partitioned re-distribution between operators, and no plan-level
DOP. Parallelism is instead **operator-internal** — each operator decides for
itself whether to fan its own row range out over a shared worker pool and
rejoins before it returns. On top of that sit two coarser layers: a
**bulk-synchronous parallel-map island** over a materialized table, and
**pipeline (inter-operator) parallelism** between a scan and its consumer.
Scheduling is **dynamic self-scheduling** off a shared atomic cursor (no work
stealing, no deques, no priorities). The engine is **NUMA-oblivious**: no
thread affinity, no local-memory allocation policy, no NUMA-aware partitioning.

The invariant everything rests on is **one query at a time**
(`query_lease.hpp`): the pool has no cross-query fairness, admission, or
preemption to arbitrate, and inputs are immutable for a fan-out's lifetime.

### 1.2 The substrate: `WorkerPool`

`include/ibex/runtime/worker_pool.hpp`, `src/runtime/worker_pool.cpp`.

A deliberately boring **fork–join / bulk-synchronous** pool: pre-spawned
threads, one mutex+condvar FIFO task queue, a completion latch (`Batch`). No
futures, no continuations, no DAG scheduler, no work stealing. `submit(n, body)`
runs `body(worker_id)` for `id ∈ [0, n)` and returns immediately; `Batch::wait()`
is the barrier and rethrows the first exception by worker id (deterministic
error selection).

Three properties are load-bearing:

* **The calling thread is not a worker.** In an island it is the ordered
  merger's consumer and must stay free.
* **Non-reentrant.** `submit` from a pool thread *aborts* rather than deadlocks
  against a saturated pool. Every fan-out site therefore guards on
  `on_worker_pool_thread()`. This is the engine's entire nested-parallelism
  policy: **outermost wins, inner levels degrade to serial.**
* **Process-owned, lazily built, host-shutdownable.** `process_worker_pool()`
  keeps mutable state in the host runtime TU rather than an `inline` variable —
  bundled plugins statically link runtime code and would otherwise each get
  their own pool under `RTLD_LOCAL`. `shutdown_process_worker_pool()` exists for
  hosts that unload the library (R).

#### Two thread budgets, on purpose

| Number | Source | Governs |
|---|---|---|
| **Compute budget** | `IBEX_CORES` → `compute_thread_count()` → `ExecutionContext::parallel_threads` | every compute fan-out |
| **Pool size** | `decode_thread_count()` (`IBEX_DECODE_THREADS`, `IBEX_DECODE_SATURATION`, default saturation 8) | thread count actually spawned; the scan pipeline is the only consumer sized against it |

Decode is memory-latency bound and profits from **oversubscription** until the
memory system saturates; compute is not and measurably regresses under it. The
policy is `min(cores * 2, max(cores, saturation))`, backed by the measured table
in `worker_pool.cpp:282`. `IBEX_THREADS` (which used to conflate the two) is
read only to emit a one-time warning.

### 1.3 Layer A — pipeline (inter-operator) parallelism

Classic **producer–consumer pipelining with bounded buffers**, used to overlap
I/O+decode with compute across a **pipeline breaker** (join build, aggregate,
sort).

* `PipelinedScanOperator` (`chunked.cpp:10810`) — the streaming scan. The source
  publishes **source units** (Parquet row groups); N workers each run a private
  copy of the row-local operator chain over one unit and deposit results into a
  **bounded ring indexed by unit sequence**; the consumer releases them in
  order. `scan_pipeline_worker_count` (`chunked.cpp:11254`) is the one place
  sized against the *pool* rather than the compute budget, and it reserves one
  thread when the unit count exceeds `3W` so a downstream breaker's own batch
  cannot deadlock behind ring backpressure.
* `PipelinedStageOperator` (`chunked.cpp:11102`) — a two-slot double buffer that
  runs its child on a **raw `std::thread`**, not a pool thread, so a breaker's
  probe input can be produced concurrently with the breaker consuming it.
  Admitted only when the child can publish more than one unit
  (`has_multi_unit_deferred_scan`).

`IBEX_STREAM_SCAN=0` opts out.

### 1.4 Layer B — parallel-map islands (morsel-driven, order-preserving)

The Phase-1 executor. `pipeline.hpp` classifies each IR node's
**execution capability** (`ParallelMap`, `OrderedStream`, `Barrier`,
`ParallelBarrier`); `analyze_parallel_island()` finds a maximal bottom-up chain
of row-local maps (filter / project / rename / row-local update / their fused
forms) and the subtree that feeds it.

`build_parallel_island` (`chunked.cpp:10339`):

1. **Materialize the input subtree first, on the calling thread.** This is the
   load-bearing invariant: a deferred/lazy source decodes exactly once,
   serially, before any worker exists, so `LazyTable::cache_` and plugin decode
   closures are never touched concurrently. Morsel sources take `const Table&`,
   so the signature enforces it.
2. Choose a **grain** (`island_grain`, `chunked.cpp:9273`): aim for 4 morsels
   per thread, clamped to `[4096, 65536]`.
3. Decide **whether to morselize at all** (`island_is_worth_morselizing`) — a
   two-dimensional gate on rows *and* cells (rows × columns), because an
   island's cost is dominated by copying rows out and so scales with table
   width. A refusal must run the chain as **one whole-table chunk**, not as a
   serial sweep of morsels; conflating "no workers" with "no morsels" measured
   100ms vs 36ms.
4. Build one **private operator chain per worker** and run
   `ParallelIslandOperator` (`chunked.cpp:9627`): workers pull numbered morsels
   off a shared cursor, and results land in a **bounded ring (window = 2×W)**
   indexed by `sequence`. `next()` is an **order-preserving merge** — output is
   byte-identical to the serial chain regardless of completion order. A worker
   that runs a full window ahead of the consumer blocks (backpressure) instead
   of buffering the island.
5. Errors are **deterministic**: a failing morsel records its error under the
   lock keeping the *lowest* sequence; workers abandon only morsels above it.
   Interrupt outranks a recorded data error.

Two specializations:

* **Late-materialized head** — `RangeFilterMorselSource` absorbs a
  range-native filter into the source, so the morsel is never gathered before
  being filtered.
* **`TwoPhaseFilterOperator`** (`chunked.cpp:9993`) — when the island is a
  range-native filter plus metadata-only tail, it skips the merger entirely:
  **phase A** counts survivors (and string bytes) per morsel, a serial
  **exclusive prefix sum** turns counts into offsets, the output is presized
  once, and **phase B** re-runs the morsels writing disjoint slices. This is the
  standard count → prefix-sum → scatter shape, and it is the only place strings
  are gathered in parallel.

A serial island still runs the same morsels through `PartitionedTableSource`,
and `SerialIslandOrderValidator` asserts morsel identity — the two executors are
required to be byte-identical, which is what lets the threshold move without
changing an answer.

`ParallelIslandStats` (`IBEX_PARALLEL_STATS=1`) exists because an island and the
serial chain are indistinguishable from the outside: without counters, an A/B
showing no difference cannot separate "parallelism didn't pay" from "no island
formed".

### 1.5 Layer C — intra-operator parallelism

The bulk of the wins. Every site is fork–join inside one operator call, with a
private gate. Summarized by parallel axis:

| Operator | Axis | Scheduling | Determinism device |
|---|---|---|---|
| Fused bounds scan (`filter.cpp:3273`) | row ranges, block-aligned to 4096 | shared cursor | per-range survivor lists concatenated in range order |
| Filter gather (two-phase only) | morsel → disjoint output slice | static | prefix sum |
| Group discovery (`chunked.cpp:6325` `try_discover_partitioned`) | **radix/hash partitioning** of rows by key hash, power-of-two mask | 3 passes: per-range histogram → exclusive prefix sum → scatter; then whole partitions per worker | partition-local ids, then a serial **first-occurrence merge** assigns global ids in row order |
| Grouped aggregation (`chunked.cpp:7186`, `:7550`) | **partial pre-aggregation into per-morsel private slot arrays**, then merge | shared cursor | morsel count derived from **rows**, merged in ascending morsel order → float reduction order is machine-independent |
| Ungrouped aggregation (`chunked.cpp:7860`) | same | shared cursor | same |
| Aggregate slot-array fill (`chunked.cpp:6620`) | byte ranges of a fresh allocation | static | none needed (memset) — threaded for **page-fault** parallelism, worth ~1.3× |
| Aggregate output emit (`chunked.cpp:8170`) | one output **column** per task | shared cursor | disjoint columns |
| Hash join probe (`chunked.cpp:4640`, `join.cpp:1167`) | probe rows; build side is a **shared read-only index**, never threaded, never locked | static ranges | per-worker `(li, ri)` pairs concatenated in range order; the concat itself re-threads above 64k |
| Semi/anti predicate (`chunked.cpp:3604`) | probe rows against a frozen set | static ranges | ascending concat |
| Distinct (`chunked.cpp:3020`) | packed-key partitions | static | workers record a **keep flag at a row**, never a position; output rebuilt by scanning flags in row order |
| Sort — gather (`sort.cpp:284`) | **(column × row range)** tasks, ranges aligned to 64 rows | shared cursor | permutation is already fixed; writes are disjoint |
| Sort — per-group slice sort / rank sweep (`chunked.cpp:1189`, `:1376`) | one **group** per task | shared cursor | groups own disjoint row spans |
| Grouped windowed update (`update.cpp:1633`) | one group per task | shared cursor | expression screened by `is_group_parallel_safe_expr` |
| Update bucketing (`update.cpp:1587`, `:1920`) | row ranges; per-worker private histogram, then prefix-sum scatter | static | global ids handed out by walking workers in row order, so numbering is **independent of thread count** |
| Row-local field eval (`update.cpp:2605`) | morsels writing into a presized destination | shared cursor | element-wise; no reduction |
| Generic gather (`runtime_internal.hpp:119` `for_row_ranges`) | row ranges into a presized output | shared cursor | disjoint slices |
| Parquet decode (`parquet.hpp:1866`) | `(column × row group)` tasks, one `FileReader` per worker | shared cursor (a string column can cost 10× an int one, so static assignment strands workers) | tasks write disjoint output ranges |
| Parquet fused key scan (`parquet.hpp:2219`) | one row group per worker | shared cursor | **contiguous-prefix replay**: the abandon rule is evaluated only over the completed prefix under a mutex, so the decision does not depend on completion order |

Recurring idioms worth naming:

* **Private-state-then-merge** (partial aggregation) wherever a reduction
  exists; **disjoint-slice writes** wherever one does not.
* **Count → prefix sum → scatter** for anything variable-width.
* **Per-worker output vectors, concatenated afterwards**, rather than
  count-then-fill, wherever counting would repeat an expensive probe.
* The merge cost gate: fan-out is admitted only when
  `morsels × groups ≤ rows / 4` — partial aggregation pays only while the merge
  (O(morsels × groups)) stays small against the scan it replaces (O(rows)).

### 1.6 Determinism contract

Every parallel path in the engine is required to produce **byte-identical**
output to its serial counterpart. The devices are listed above; the general
rules are:

1. Row order is reconstructed positionally (sequence, range order, or a
   prefix-summed offset) — never by completion order.
2. Floating-point reduction order is fixed by a **data-derived** partition where
   a reduction exists (the aggregate derives morsel count from rows, not
   threads).
3. Group ids are assigned by first occurrence in row order, after the parallel
   phase, so ids do not vary with thread count.
4. Errors are selected by lowest sequence / lowest worker id, and an interrupt
   outranks a data error.

### 1.7 Configuration surface

| Variable | Reads | Meaning |
|---|---|---|
| `IBEX_CORES` | `compute_thread_count()` | compute budget (`auto` = `hardware_concurrency`) |
| `IBEX_DECODE_THREADS` | `decode_thread_count()` | absolute pool size override |
| `IBEX_DECODE_SATURATION` | `decode_thread_count()` | memory-system saturation point (default 8) |
| `IBEX_PARALLEL` | `parallel_enabled_from_env()` | islands on/off (answers both ways) |
| `IBEX_MORSEL_ROWS` | `morsel_rows_from_env()` | island grain override; also lowers `parallel_min_rows` |
| `IBEX_CHUNK_ROWS` | `source_chunk_rows_from_env()` | forces multi-chunk sources (test-only) |
| `IBEX_STREAM_SCAN` | `stream_scans_from_env()` | streaming scan on/off |
| `IBEX_JOIN_PROBE` | `parallel_join_probe_from_env()` | parallel join probe on/off |
| `IBEX_PARALLEL_STATS` | `process_island_stats()` | island/probe/stage counters at exit |
| `IBEX_PROFILE_OPERATORS` | `execution_profile_requested()` | per-operator phase profile |
| `IBEX_THREADS` | — | **ignored**, warns once |

The principle is that the decoder and the operators must never read the
environment directly — the `ExecutionContext` is the single authority, so there
is no second copy free to disagree. As of I8's fix all three on/off switches obey
it: `configure_parallel_from_env` reads them once through the shared `env_flag`
parser and lands them in `parallel`, `stream_scans`, and `parallel_join_probe`,
which is all any seam consults. An unset variable leaves the caller's choice
alone, so building an `ExecutionContext` by hand remains the spelling for
"ignore the environment" (contrast the `interpret()` overload that takes none).

---

## Part 2 — Inconsistencies

Grouped by what they threaten. Most are *undocumented divergence* rather than
bugs; the risk is that each new operator copies whichever neighbour it happened
to be written next to, and the spread widens. Severity is about the cost of
leaving it, not about correctness today.

### Column-type divergence

**I1 — Three different answers for "can this column type be gathered in
parallel?"** — **RESOLVED**. Before:

| Path | `bool` | `std::string` | `Categorical` | fixed-width |
|---|---|---|---|---|
| `gather_column` (`runtime_internal.hpp`) | **serial** (bitmap words are shared between ranges) | **serial** (cumulative offsets) | parallel | parallel |
| `gather_rows_parallel` (`sort.cpp`) | **parallel** — ranges aligned to 64 rows so no word is shared | **one indivisible task** (still parallel *with* other columns) | parallel | parallel |
| `filter_gather_is_thread_safe` (`filter.cpp`) | **parallel** — via `or_bits_into_word`'s shared-word rule | **parallel** — one offset per row, disjoint byte slabs, after a prefix pass | parallel | parallel |

Three solutions to the same two hazards (bit-packed words, cumulative offsets),
each with a good local justification, none aware of the others.

`gather_column` was a second, divergent implementation of the kernel
`gather_rows` already had. It is now written in terms of it —
`make_gather_column` sizes the output, `gather_range_into` fills a range — so
the per-type rules are stated once, in that kernel, and `bool` gathers in
parallel everywhere rather than in one of the three places. The alignment rule
moved into `for_row_ranges`, which now hands out 64-row-aligned boundaries; that
is what makes a bit-packed destination safe, and `tests/test_gather_kernel.cpp`
asserts the boundaries directly rather than hoping a race surfaces.

The kernel also moved *down* a layer, from `interpreter_internal.hpp` to
`runtime_internal.hpp`, because the duplicate lived in the lower one and the
include direction is upward. `gather_rows` stayed put: it alone needs
`ir::OrderKey` and `TableProperties`.

`TwoPhaseFilterOperator` remains separate **by design**, and the kernel now says
so: it writes into a caller-presized output at a prefix-summed offset, which is
a genuinely different operation and is what lets it split strings this cannot.
That is one documented exception, not a third answer.

**I2 — String and Categorical columns are silently excluded from three
different fan-out decisions, for three different reasons** (medium)

* `stageable_conjunct_columns` (`lazy_table.cpp:1107`) rejects String/Categorical
  *predicate* columns — width, not selectivity. Documented, measured (q10 +9.7%).
* `evaluate_field_maybe_parallel` (`update.cpp:2605`) accepts only `Int`/`Double`
  results — a Categorical result would need per-piece dictionary merging.
* The parallel partial aggregation (`chunked.cpp:7196`) accepts only
  `Int`/`Double` agg kinds, because boxed `First`/`Last` values live outside the
  slot array.

Each is reasonable; together they mean a string-heavy query loses parallelism in
three places with no single place to look for why. There is no shared predicate
answering "is this column type parallel-capable in role X".

**I3 — Multi-key Categorical group discovery has no partitioned path**
(medium). `try_discover_partitioned` is instantiated for `std::string`,
`std::int64_t`, `PairIntKey`, and the packed key
(`chunked.cpp:5797/5886/6022/6085/7085`), but the multi-Categorical path
(`chunked.cpp:7010`) grouping through `multi_cat_find_or_insert` is serial. The
gap is not marked anywhere.

### Serial-path vs multi-core-path divergence

**I4 — The whole-table operators and the chunked operators have very different
parallelism, and the difference is not written down** (high)

`src/runtime/{filter,aggregate,join,sort,update}.cpp` implement whole-table
operators; `chunked.cpp` reimplements most of them for the streaming path.

**Correction (verified 2026-08-17):** an earlier draft of this entry said the
whole-table operators are what the `ops`/codegen layer, plugins and the REPL
`:load` path reach, so that "the same query can be several times more parallel
depending on which entry point reached it". That is **wrong**. `interpret()`
routes unconditionally through `build_operator` -> the chunked operators ->
`MaterializeOperator`, and `ops.hpp` is a thin `Table -> Table` wrapper over
`interpret()`. Every entry point gets the chunked engine. The whole-table
functions are reached only as `interpret_node` fallbacks for node kinds the
chunked builder declines (complex aggregates such as Median), and as delegation
targets from inside chunked operators.

That matters for priority: this is a tax on future work and a drift hazard, not
a user-facing performance gap. Note also that `ops.hpp` already demonstrates the
fix — a whole-table SIGNATURE over a chunked IMPLEMENTATION.

Three different relationships hide under one name:

* **Delegating** — the chunked operator buffers and calls the whole-table
  function: `order_table`, `filter_table` (+ `_range`/`_limit`), `update_table`,
  `join_table_impl`. Not duplication; the whole-table function is the kernel.
* **Fully reimplemented** — aggregate, distinct, and inner join. The sort was
  listed here in an earlier inventory, but that is stale: `ChunkedOrderOperator`
  concatenates an unsorted stream and calls `order_table`, while
  `radix_sort_u64_asc` lives once in `sort.cpp` and is shared by rank. There is
  no second order/radix implementation to collapse. `aggregate_table` appears
  in `chunked.cpp` only in a comment. This is the real duplication.
* **Escape hatch** — the chunked aggregate declines Median/quantile/EWMA and the
  whole node runs via `interpret_node` -> `aggregate_table`. This is what lets
  the streaming engine be incomplete, and it is why the aggregate is the LAST
  operator that can be collapsed, not the first.

Parallel coverage in the reimplemented set differs sharply:

| Operator | whole-table | chunked |
|---|---|---|
| Aggregate | per-group reduce only (`aggregate.cpp:1157`) | partitioned discovery + partial pre-aggregation + parallel emit + parallel slot fill |
| Filter | bounds scan parallel; gather now parallel (I5) | two-phase parallel gather |
| Join | parallel probe (`join.cpp:1167`) | parallel probe + parallel concat + swapped-probe replay |
| ~~Distinct~~ | ~~serial~~ | **collapsed — one implementation** |

A fix applied to one side routinely does not exist on the other. I15 is the
worked example: it had to be written into both engines, and doing only
`join.cpp` would have "fixed" a benchmark regression without touching the code
the benchmark runs.

*Convergence:* keep the whole-table **signature** and delete the second
**implementation**, per the split `ops.hpp` already uses — `filter_table(t, e)`
becomes a `TableSource` run through the chunked operator and materialized.
Sequencing constraint: an operator can only be collapsed once its chunked
version is COMPLETE, because collapsing removes the decline-and-fall-back path.
So distinct / inner join first, and the aggregate only after the chunked one
covers Median, quantile, and EWMA. The former "sort's radix path" step is already
converged, as above.

The duplication tax on the task-scheduler work is avoidable without doing any
of this first: port the chunked operators to the new primitive and leave the
whole-table fallbacks on the old one. They are fallbacks, so lagging costs
correctness nothing and measurable performance almost nothing.

#### Second correction: how narrow the fallback actually is

The entry above already corrected one wrong claim (that the entry point picks
the engine). Doing the first collapse turned up a second thing this entry got
wrong, in the same direction — the whole-table path is even less reachable than
"an `interpret_node` fallback" suggests.

**The whole-table functions run only for a subtree beneath a declined node
within a single statement.** A `let` materializes, so in

```
let d = t[distinct { g, v }];
d[select { m = median(v) }];      // aggregate declines...
```

the aggregate's child is a `Scan`, not a `Distinct`, and no whole-table
`distinct` ever runs. Written as one statement it does:

```
t[distinct { g, v }][select { m = median(v) }];
```

This was not deduced, it was measured: a mutation making `distinct_table` return
an error outright did **not** fire on the first shape. Which means a test written
the natural way — bind, then aggregate — verifies nothing at all while appearing
to pass. Any future collapse needs its coverage written as a single statement,
and mutation-checked, or it is testing the operator it already had.

It also lowers I4's remaining urgency another notch. The duplicate
implementations are not just off the hot path; they are off almost every path.
This is cleanup and a drift hazard, not latent performance.

#### Progress

**Distinct — COLLAPSED** (`74f6e32`). `interpreter.cpp`'s serial dedup loop
boxed a `Key` per row; `ChunkedDistinctOperator` has single-column and
packed-key fast paths. `distinct_table` keeps the whole-table signature and
delegates: `make_table_source` -> the operator -> `materialize_operator`.

Metadata was checkable rather than assumed: `distinct` is a `RowTransform::
Subset` keeping every column, and `Subset` "derives exactly like `Preserve`", so
the deleted `distinct_properties` was the identity the operator already relies on
by passing properties through. Added the first coverage this path has ever had.

**Inner join — COLLAPSED** (worktree after `6f0a03e`). The constrained
single-key, predicate-free, `nulls never`, unconstrained inner join now keeps
the whole-table signature but delegates to `ChunkedInnerJoinOperator`; joins
with predicates, multiple keys, `nulls equal`, `expect`, or `take` remain in
`join_table_impl`, which is the implementation of those richer semantics.

The fallback test uses the same single-statement shape as distinct:
`(lhs join rhs on k)[select { m = median(v) }]`. It caught an independent
chunked bug: an all-unmatched join emitted no morsel, so a materializing sink
lost the planned output schema. The operator now retains an empty schema carrier
until EOF. This is required for the adapter to be equivalent to
`join_table_impl`, not merely a test convenience — and since it changes the
PRODUCTION operator rather than only the adapter, it was checked against all 22
PDS-H outputs, which are byte-identical.

The semantic gate is `is_streamable_inner_join`, called by both
`build_operator` and `interpret_node`. It was written out twice at first, once
per file, character-identical — the I4 failure mode in miniature. Collapsing the
implementation while leaving a six-clause predicate duplicated across two files
would have swapped one drift hazard for a subtler one: a clause added to one
copy routes a join the operator cannot handle, and nothing would say so.

Release check, against `6f0a03e`, used **three interleaved base/target repeats**
with two warmups and five timed iterations, `IBEX_CORES=2`, and `taskset -c 2`.
The direct joins were neutral-to-faster: `inner_join_symbol` **-0.27%**
(8.786 → 8.762 ms) and `inner_join_user` **-2.45%** (12.259 → 11.959 ms).
The derived join workloads were `join_filter_rank` **-2.91%** (27.292 →
26.497 ms) and `join_update_group` **+2.43%** (14.591 → 14.945 ms), the latter
within the run-level variation. `IBEX_THREADS` is obsolete and was not used in
this corrected run.

**Aggregate — NOT a collapse, and the entry above was wrong to imply it is.**
`aggregate_table` is not near-dead fallback code the way `distinct_table` and the
whole-table join were: a median aggregate is itself the *declining* node, so it
runs at top level for every median/quantile/EWMA query. Collapsing it means
implementing those three in a streaming operator — a median needs every value of
a group retained, which the operator's fixed-size slots deliberately avoid, and
EWMA's row-order coupling fights the partitioned discovery that makes the chunked
aggregate fast. And it would not even remove the duplication: a node mixing
`median(x)` with `sum(y)` needs both in ONE pass, so `aggregate_table` keeps its
sum/mean/min/max code regardless.

The escape hatch is therefore design, not debt, and I4 is **complete at 2 of 3**.
The goal was zero DUPLICATED implementations, not zero whole-table ones.

**Aggregate measurement (2026-08-18).** Per `MEASURING.md`, this was a small
synthetic profile before any suite run: 1M `Double` rows, `median(v)`, about
9,800 groups, `IBEX_PROFILE_OPERATORS=1`, and `IBEX_CORES={1,2,4,8}` pinned to
the matching CPU sets. Two key shapes were required. One key measured
35.4 / 34.4 / 38.4 / 29.5 ms; two keys measured 52.1 / 56.9 / 47.8 / 44.8 ms.
Thus eight cores buy only 1.20x and 1.16x respectively, not a scaling result
worth extrapolating from a suite geomean. An independent `IBEX_PARALLEL=0`
versus enabled run at eight cores was 49.5 versus 46.9 ms on the two-key case.
The profile agrees: only 12.7 ms of pool work, one barrier, and roughly 1,100%
profile occupancy across eight workers. The next aggregate change must target
the serial group/value-collection path; it should not be a generic scheduler or
thread-count adjustment.

### What looking for the third collapse actually found

Chasing "which grouping implementation should survive" turned up a hash
pathology instead (`6215c88`). `median(v) by {a, b}` over 1M rows: **517ms at
9800 groups, 20ms at 5000**, same row count. Not the aggregate, not the group
count — the key hash.

`hash_key_row` combines with boost's `hash_combine` over `std::hash<int64_t>`
(the identity on libstdc++) and never finalizes. `hash_combine` does not diffuse
into the low bits, and `KeyRowIndex` masks the low bits to pick a slot, then
probes linearly. For two small integer keys the result is nearly `b + (a << 6)`,
a linear function of the key, so probing degenerates into one cluster. One
`fmix64` fixes it: **12.9x**, and the 6000→9800 ramp (29/102/258/411/523ms)
flattens to 31/31/33/43ms.

It hid because it needed **both** multiple keys and a particular table size. A
single key was always fine — dense integers map to consecutive slots, the best
case rather than the worst, 100k groups in 67ms. Two keys at 5000 groups in an
8192-slot table were fine; the same load factor at 9800 in a 16384-slot table was
not. Different mask width, different aliasing. Any sweep that varied only one of
those two dimensions would have concluded there was no bug.

Two lessons worth keeping:

* **Three copies of one hash, and the drift bit immediately.** Adding the
  finalizer to `hash_key_row` and `hash_key_value` left `chunked.cpp`'s
  `mix_one` behind, and a test failed within one run with a duplicated group —
  the exact failure the header comment predicts in as many words. The same
  combine also feeds `multi_cat_find_or_insert`, the chunked aggregate's
  open-addressed index over small dense categorical codes, which is the ideal
  input for the pathology; it is finalized too. This is the I4 thesis in one
  incident: the hazard is not that two implementations exist, it is that a
  constant is written out more than once.
* **Two hypotheses were measured and discarded first** — per-group heap
  indirection (flattening the slot array changed nothing) and group count itself
  (a single key handles 100k groups in 67ms). Both were plausible and both were
  wrong, and each cost one build.

**I5 — `gather_column`'s `exec` argument is optional, so half the callers gather
serially by omission** — **RESOLVED**. `lazy_table.cpp` (5 sites) and the chunked
join passed `exec_`; `filter.cpp` and `update.cpp` (2 sites) did not. Nothing in
the signature said which was intended, and the serial ones carried no comment
saying they were deliberate. A defaulted `nullptr` is the wrong shape for a
decision this consequential.

`exec` is now required, so every site states its choice:

* the filter's staged compaction gather now passes the query's context — it had
  one in scope all along and was serial purely by omission;
* the two per-group slice gathers in `update.cpp` pass `nullptr` **with a
  reason**: each is one task of a per-group fan-out, so a nested split would
  only oversubscribe, and a single group's slice is far below the row floor;
* `gather_column_with_nulls` and the join's `gather_entry` grew the parameter
  too, which is what put the join's output-assembly gather on the parallel path.
  Its sentinel-carrying branch stays serial — it branches per row and writes a
  validity bit beside each value, which is a different kernel.

### Gate and threshold divergence

**I6 — Nine private row thresholds coexist with the two `ExecutionContext`
knobs** (high)

`parallel_min_rows` (65536) and `parallel_min_cells` (512k) are described as
"the same knob the rest of the engine gates on". In practice:

| Constant | Value | Site |
|---|---|---|
| `parallel_min_rows` | 65536 | islands, `for_row_ranges`, filter bounds, sort, update bucketing, lazy keep-rows |
| `kMinProbeRows` | 16384 | join probe (both impls) |
| `kMinParallelPredicateRows` | 262144 | semi/anti predicate |
| `kMinRows` | 32768 | distinct packed |
| `kMinRowsPerMorsel` | 65536 | filter bounds, aggregate (×3) |
| `kDefaultPartitionMinRows` | 262144 | group discovery, int/string keys |
| `kPackedPartitionMinRows` | 32768 | group discovery, packed key |
| `kMinRowsPerWorker` / `kMinSplitRows` | 32768 | update bucketing |
| `kParallelDecodeMinRows` | 65536 | parquet readers |
| `kMinTailBytes` | 4 MiB | aggregate slot fill |
| `kMinParallelConcatRows` | 65536 | join probe concat |

Each was measured, and per-operator break-evens genuinely differ — that is not
the problem. The problem is that a test or a benchmark cannot move them: setting
`parallel_min_rows = 0` (which the tests do, to reach the worker path on small
tables) turns on *some* fan-outs and not others, and there is no way to sweep the
rest without recompiling. *Convergence:* keep per-operator break-evens, but
express them as `ExecutionContext`-scaled multiples rather than free constants.

**I7 — `parallel_min_cells` is consulted by exactly two of ~30 sites** (medium).
Only `island_is_worth_morselizing` (`chunked.cpp:10292`) and
`gather_rows_parallel` (`sort.cpp:294`) test width. The stated finding behind the
knob — "131,072 rows won at 6 columns and *lost* at 2, on the same predicate;
every row threshold puts those on the same side" — applies verbatim to the
filter gather, the join concat, and the aggregate emit, none of which check it.

**I8 — Two authorities on whether a query is parallel** — **RESOLVED**. The
decoder's `parallel_readers` explicitly documented that it must never read the
environment, because that would be a second authority free to disagree with the
`ExecutionContext`. But `parallel_join_probe_enabled()` (`IBEX_JOIN_PROBE`) was
called from three probe gates and `stream_scans_enabled()` (`IBEX_STREAM_SCAN`)
from three scan seams — six `getenv`s behind two settings.

Both are now `ExecutionContext` fields (`parallel_join_probe`, `stream_scans`)
applied by `configure_parallel_from_env`, as `parallel` already was, and all
three env readers share one `env_flag` parser so their accepted spellings cannot
drift apart. The behavioural change is deliberate and follows the precedent
documented at `interpreter.cpp`'s no-context `interpret()` overload: a caller
that builds its own `ExecutionContext` now ignores these two variables, which is
the only spelling for "ignore the environment". Every production entry point
reaches `configure_parallel_from_env`, so nothing user-facing changes.

**I9 — Worker-count caps differ arbitrarily** (low). `min(budget, pool, 64)` in
the join probe, group discovery, and distinct; `min(..., 16)` in the aggregate
slot fill; `min(budget, pool)` with no cap in sort, semi/anti, and the aggregate
emit; `kMaxMorsels = 64` for morsel counts in filter and aggregate. The 64 and
the 16 are undocumented.

**I10 — The aggregate output emit gates group count against a rows
threshold** (low). `chunked.cpp:8176` reads
`n_groups_ >= exec_->parallel_min_rows` — the work there really is per group, so
comparing to a row threshold is a unit mismatch that happens to be
conservative. Either it wants its own constant or the knob wants a name that is
not about rows.

**I11 — Some gates construct the pool before deciding they do not want it**
(low). `chunked.cpp:3034` and `:3617` bind `process_worker_pool()` before testing
`exec_->parallel`, and `island_worker_count` calls `pool.size()`
unconditionally. The pool spawns its threads eagerly in the constructor
(`worker_pool.cpp:129`), so `IBEX_PARALLEL=0` still pays for N threads —
even though `configure_parallel_from_env` goes out of its way *not* to construct
a pool just to size a profile.

### Structural

**I12 — The runtime has two species of thread and only one was accounted for**
— **PARTLY RESOLVED**, and reframed. The original entry called the raw
`std::thread` in `PipelinedStageOperator` an exception to the nesting policy. It
is better understood as a *second kind of thread* that the runtime had no
vocabulary for.

`WorkerPool` runs short, independent, non-blocking bodies to completion and joins
them. A **stage thread** is the opposite of all three: long-lived, and it parks
on its consumer's ring backpressure before each child pull. Hosting that on the
pool would be the actual bug — the producer's child chain (usually a scan
pipeline, the most parallel thing in the query) would lose every fan-out to the
`on_worker_pool_thread()` guard, and a fixed pool cannot safely hold occupants
that block on another thread's progress: N producers parked on backpressure with
a consumer that needs a pool batch to drain them is a deadlock.
`scan_pipeline_worker_count`'s reserved thread already reasons about that hazard
from the other side.

So the raw thread stays. What was wrong is that nothing could tell it apart from
the calling thread. Now `on_stage_thread()` names it, `StageThreadScope` marks it
and keeps a live count plus a monotonic peak, and the profile header reports
`pool_threads` and `stage_threads_peak` — the process runs
`decode_thread_count()` plus one per staged breaker, and previously nothing
bounded or reported the sum.

**It had already corrupted a measurement.** `ExecutionProfileScope` split on a
binary `on_worker_pool_thread()`, so a stage thread fell into the `else` and its
work was charged to main-thread self time *concurrently with the real main
thread*. That made `self_ms` exceed `wall_ms` on exactly the queries with
`pipelined_stages > 0`, and inflated `serial_self_ms` — the number the scheduler
decision is read from — by 79ms across PDS-H SF-1 (884.0 -> 805.0 once
`stage_self_ns` was split out). Fixed.

**What remains** is the design question: the two species should be explicit in
the primitive, not one on the books and one beside it. Go hands off the M on a
blocking syscall; Tokio keeps a separate `spawn_blocking` pool.

This was filed as an INPUT to the scheduler track. With that track dropped, it
stands on its own — and it shrank, because the accounting gap that motivated
half of it is closed: a stage thread's lifetime, its work, and both kinds of
park it can take are now measured, and they balance to 99.9%. What is left is
ergonomics rather than a blind spot. (`stream_buffered.hpp` also detaches a
thread, for the kafka/ws sources; left alone, as it is not part of a profiled
query plan.)

**I13 — Cancellation reaches islands and scan pipelines, not intra-operator
fan-outs** (medium). `interrupt_requested()` appears in
`ParallelIslandOperator`, `PipelinedScanOperator`, and `PipelinedStageOperator`
only. A long parallel gather, probe, group discovery, or partial aggregation is
not interruptible — consistent with the documented "per node/chunk/statement,
NOT mid-operator" contract, but the gap grows every time an operator gets a
bigger parallel section.

**I14 — Two island-grain philosophies, both correct, neither cross-referenced**
(low). `island_grain` derives the grain from the **thread count** (4 morsels per
thread), so island morsel boundaries vary with the machine — fine, because
islands do only element-wise work. The aggregate deliberately derives its morsel
count from the **row count** so the float reduction order does not vary with the
machine. `evaluate_field_maybe_parallel` reuses `island_grain` and is safe only
because its results are element-wise. Nothing warns the next author that reusing
`island_grain` inside a reduction would make results machine-dependent.

**I15 — The join's output assembly fans out once per COLUMN; the sort fans out
once for the whole table** (medium). Found while resolving I1/I5, and measured
rather than assumed.

`gather_entry` (`join.cpp`) is called in a loop over output columns, so with
`exec` threaded through it now submits and waits one batch **per column**.
`gather_rows_parallel` (`sort.cpp`) avoids exactly this: it builds
(column x range) tasks and submits **one** batch, precisely because a column
count is a poor divisor and per-column barriers do not amortize.

Measured on the local bench (9 interleaved repeats, 8 cores, base = the commit
before I1/I5): `inner_join_user` +1.84% and `inner_join_symbol` +4.95%, both
verdict `noise` and both well inside the ±13% per-query floor — so this is not a
regression. But it is not a win either, on a 1M-row output that clears the
65536-row gate and therefore *is* fanning out. The per-column barrier is
plausibly eating the parallel gather it buys.

**RESOLVED.** `gather_columns_batched` (`runtime_internal.hpp`) now holds the
sort's task shape as a shared primitive: it builds (column x range) tasks across
*all* the columns and submits **one** batch, with 64-aligned ranges, and takes a
`gather_whole(j)` callback for the tasks that cannot be split — a string, or a
job the caller marked `indivisible`. Those still run concurrently with the other
columns, which is the point.

It had **two** instances, which is I4 in miniature: `join_table_impl` in
`join.cpp` *and* `ChunkedInnerJoinOperator`'s own `gather_with_validity` loop in
`chunked.cpp`. The second is the one the `inner_join_*` benchmarks actually
exercise, so fixing only the first would have left the measured symptom
untouched — worth remembering as evidence for how much the duplicate engines
cost. Both now call the one shared helper rather than growing a copy each.

Two things fell out. `gather_entry` lost its `ExecutionContext` parameter
entirely: with the fan-out decided once, one level up, every caller passed
`nullptr`, and a parameter that is always null implies a choice that no longer
exists. And the chunked join's `has_right_nulls` — computed and then dropped on
the floor while `gather_column_with_nulls` rescanned inside every column's
gather — is now the `indivisible` flag, scanned once per side.

Measured (same harness, 9 interleaved repeats, 8 cores): geomean 1.021x, total
-1.96%, 34/34 `noise`. `inner_join_symbol` went +4.95% -> 0.00% and
`inner_join_user` +1.84% -> -2.33% against their respective baselines, so the
per-column barrier was indeed costing what it looked like. Sorts drifted the
other way this run (+1.8% to +3.7%, having been -0.6% to -2.7% last run), which
is the ±13% per-query floor doing its thing and not a signal.

This is the same lever as [[project_join_parallelism]]'s "assemble_output is the
other half"; the build side remains unthreaded on purpose.

### Not inconsistencies (recorded so they are not "fixed")

* Build side of a hash join is never threaded — measured at 1.5% of q10 against
  the probe's ~15%.
* The prefix sums in `TwoPhaseFilterOperator` and `try_discover_partitioned` are
  serial on purpose: O(morsels), not O(rows).
* A refused island runs as one whole-table chunk rather than a serial morsel
  sweep. This looks like a missing code path; it is the fix for a measured 3×
  regression.
* `parallel_threads` deliberately does **not** cap the scan pipeline; that is
  the one consumer the extra decode threads were added for.

---

## Suggested order of attack

This list was originally sequenced against one larger track, a real task
scheduler, with roughly half of Part 2 — **I6, I9, I10, I11, I12, I14**, and
parts of **I7** and **I13** — deferred to it on the grounds that a scheduler
where submitting is cheap and joining does not park a thread would rewrite the
physics behind those numbers.

**That track is now dropped** (item 7), so those items are no longer waiting on
anything. They are still low priority, but for the ordinary reason — they are
small — rather than because a rewrite is coming. (A second track — weakening
first-occurrence group ordering — was considered and REJECTED; see below.)

1. ~~**I8** — move `IBEX_JOIN_PROBE` / `IBEX_STREAM_SCAN` into
   `ExecutionContext`.~~ **Done.** Small, and it restores the "single authority"
   property the decoder already relies on — which is also what gives a scheduler
   A/B a context-level switch to bisect with instead of a `getenv`.
2. ~~**I1 + I5** — one gather kernel family, `exec` non-optional.~~ **Done.**
   Removed the largest single source of type-dependent divergence, and wanted
   doing *before* the scheduler: a scheduler multiplies the number of
   concurrently live gather sites, so shipping it on top of three disagreeing
   rules is how a heisenbug gets in. Measured neutral-to-positive (geomean
   1.012x over join/sort/filter/bool, 34/34 verdicts `noise`), and it surfaced
   I15.
3. ~~**I15** — give the join's output assembly the sort's single-batch
   (column x range) task shape.~~ **Done**, in both engines, via a shared
   `gather_columns_batched`. It needed doing twice, which is itself an argument
   for the next item.
4. ~~**Instrument the barriers.**~~ **Done.** Barrier count, barrier wait, ring
   wait, stage self-time, worker backpressure park.
5. ~~**Run the counters across PDS-H and price the scheduler.**~~ **Done**, and
   it answered the question in the negative — see the measured section above.
6. ~~**Close the accounting.**~~ **Done.** Pool threads parked with an empty
   queue (`6350918`) and stage producers (`8f3b6d8`). Closure is 99.6% on the
   pool and 99.9% on stage threads, so no further instrumentation is warranted
   and none is planned. The stage work found and fixed a double-count that an
   over-100% closure exposed.
7. ~~**The task scheduler.**~~ **DROPPED.** Demoted four times and now refused
   outright, on measurement rather than inference. `pool_idle_ms` and
   `stage_park_ms` are both 0.0 across all 22 queries while 68.6% of the pool
   sits with nothing queued: no thread is ever blocked behind another's work, so
   there is nothing for stealing to steal. A scheduler redistributes
   parallelism; it does not manufacture it. Revisit only if a future change
   raises occupancy far enough that queues actually build.
8. **I4** — collapse the duplicated implementations, keeping the whole-table
   signatures. **Promoted** from after the scheduler, since there is no longer a
   scheduler to come after — and it is the gate on item 9, because the aggregate
   cannot be parallelized further while four grouping implementations disagree
   about which one runs. Order within it is set by completeness:
   ~~distinct~~ (done, `74f6e32`) / ~~inner join~~ (done, worktree after
   `6f0a03e`) / ~~sort's radix path~~ (already converged; no duplicate), then
   the aggregate once the chunked one covers Median, quantile, and EWMA.
9. **Create parallel work, targeted at the five queries holding half the idle
   machine.** q13, q21, q18, q10, q20 account for 3113 of 6486ms of unqueued
   pool time — 48% of the waste in 22.7% of the queries. Attack per operator,
   biggest first: the aggregate (q13/q18/q10, gated on item 8), then join
   `assemble_output` (q21/q20/q09), then intra-operator fan-out for the 1:1
   operators. This replaces the scheduler as the structural lever.
10. **I2 + I3** — the type exclusions and the missing multi-Categorical
    partitioned discovery, once there is a shared predicate to hang them on.
11. **Elide the first-occurrence merge when nothing downstream reads the order**
    — see below. Small, and last, because it is worth less than it looks.

### Open structural question

Every ring in the engine runs dry and none ever runs full — producers park on a
child's ring for 42% of their lifetime and on their own output ring for 0.0ms.
So ring *depth* is irrelevant and producer *count* is the constraint. Worth
settling whether a staged breaker should have several producers before tuning
any single operator inside one.

---

## Rejected: weakening first-occurrence group ordering

Considered and turned down on 2026-08-17. Recorded because the argument for it
is genuinely tempting from inside the engine, and someone will make it again.

**The proposal.** A group-by currently emits groups in first-occurrence order.
The partitioned discovery path (`try_discover_partitioned`) therefore ends with
a SERIAL first-occurrence merge that assigns global ids in row order. Declare
the output order unspecified — at least for the hash case — and that merge
disappears, the partitioned path can emit partition-by-partition, and grouped
aggregation can stream its output instead of materializing every group first.

**Why not.** The prize is far smaller than the framing suggests. Measured on
q18 at 3M groups, which is the worst case because the merge is O(groups) and
not O(rows) (see `fill_slots_parallel`'s comment):

    80ms  parallel discovery probe
    44ms  slot fill
    26ms  serial first-occurrence merge   <- the prize
    12ms  key-array growth
    10ms  accumulate

That is ~15% of one operator at extreme cardinality, and proportionally less
everywhere else. Against it:

* **It spends the determinism contract**, which is the best property this
  codebase has. Byte-identical serial/parallel output is what makes every
  threshold in Part 1 a free parameter — movable at any time without a
  correctness argument. Trading that for 26ms is a bad trade.
* **It blunts the main verification method.** `diff <(IBEX_PARALLEL=1 ...)
  <(IBEX_PARALLEL=0 ...)` is how I8, I1/I5 and I15 were each checked. Under
  unspecified group order that degrades to "sort both, then diff", which still
  catches a wrong value but no longer catches a row misassociated with its key.
* **It is a breaking change to user scripts**, and Hyrum's Law applies with
  force to a default that has held since day one. Ibex is `data.table`-inspired,
  where `by=` preserves first appearance and `keyby=` sorts, so users arrive
  expecting exactly this. The repo has already been bitten from the other
  direction — see the note about `data.table`'s `merge()` sorting the join key.

**What to do instead (item 9).** The win is available with no language change:
elide the merge at PLAN level when the consumer provably does not read the
order. A group-by feeding an `order`, a scalar aggregate, or a join has dead
output ordering, and that instance can emit partition-by-partition. Mechanically
this is a *required-ordering* property propagating DOWN the plan, mirroring the
`ordering` property that already propagates up through `TableProperties`.
Strictly better than the language change: no user decision, nothing breaks, the
test methodology survives, and it captures the cost exactly where it is free.

---

## Measured: where the main thread's time actually goes

PDS-H SF-1, `IBEX_CORES=8`, `build-release`, quiet box, two runs per query with
only the second parsed.

**This table was wrong twice before it was right.** Both errors inflated
"serial", and both were misattributed idle: stage-thread work (79ms, fixed by
`stage_self_ns`) and ring parks (374ms, fixed by `RingWaitScope`). The first
version of this section reported 805ms serial and concluded that the residue was
mostly serial algorithms. It is not.

| | ms | share of self |
|---|---|---|
| wall | 1286.9 | |
| main-thread self | 1243.7 | |
| — genuinely serial work | **442.8** | 35.6% |
| — parked at a barrier | **414.7** | 33.3% |
| — parked on a pipeline ring | **386.2** | 31.1% |
| pool worker work | 3053.2 | |
| pool worker backpressure park | **0.0** | |
| barriers issued | 249 | |

**64% of the main thread's non-worker time is idle, not serial.**

**And the machine is 70% empty.** Pool capacity is `8 x 1286.9` = 10295 thread-ms;
3053 is used. Occupancy is **29.7%**, and that figure is now trustworthy —
`pool_idle_ms` measures 0.0 on all 22 queries, so no worker ever parked on
backpressure and `pool_work_ms` was never inflated. Total real work
(3053 + 443) / 1286.9 wall = **2.72x achieved on 8 cores**.

The 70% is pool threads sitting in the pool's own `work.wait()` with **nothing
queued**. That is a different thing from a parked worker.

### The empty pool, measured rather than inferred

The paragraph above derived the 70% by subtraction: capacity minus what the
counters saw. Given that this profiler had already misreported the serial figure
three times, a number nothing measures directly was not a number to plan on. It
is now measured, by `pool_idle_between` over a per-thread park ledger sampled at
query start and end. Sampled rather than accumulated for one specific reason: a
thread parked before the query and still parked when it ends never wakes inside
the window, so it never gets a chance to add to any counter — the fully-idle
thread, the one that matters most, is exactly the one a counter cannot see.

The report now prints `pool_unqueued_ms` next to `pool_capacity_ms`, so the
accounting closes or visibly fails to:

| | ms | share of pool capacity |
|---|---|---|
| pool capacity (`wall x pool_threads`) | 9853.6 | 100% |
| — worker work (`pool_work_ms`) | 2997.9 | 30.4% |
| — worker backpressure park (`pool_idle_ms`) | 0.0 | 0.0% |
| — **parked with nothing queued** (`pool_unqueued_ms`) | **6815.2** | **69.2%** |
| unaccounted | 40.5 | 0.4% |

**Closure: 99.6%.** The inferred 70% was right, and there is no fourth bucket.
The residual 0.4% is sampling skew — a park that closes between reading a
thread's two ledger fields — and it lands on both sides across queries (a few
rows read slightly over 100%), so it is noise, not a missing term.

This is the first number in this document that was confirmed rather than
corrected. It also makes the per-query spread visible, which the subtraction
could not: **q02 is 97.1% empty, q16 85.1%, q22 89.5%** — the small queries are
running almost entirely on one thread — while q06, the most parallel query in the
suite, is still 38.8% empty. Nothing here is anywhere near saturating the pool.

### The other thread species: stage producers

The pool is one of two kinds of runtime thread, and the same question applies to
the other. A `PipelinedStageOperator`'s producer is long-lived and blocks on
another thread's progress, so it cannot live in the pool — and having its own
thread meant having its own accounting hole. `stage_self_ms` measured its work;
nothing measured its idle.

It parks on **two different rings**, and the distinction turns out to be the
whole story:

| | ms | |
|---|---|---|
| stage thread lifetime (`stage_live_ms`) | 184.4 | 100% |
| — producing (`stage_self_ms`) | 107.1 | 58.1% |
| — parked on its CHILD's ring (`stage_ring_wait_ms`) | 77.1 | 41.8% |
| — parked on its OWN output ring (`stage_park_ms`) | **0.0** | **0.0%** |

**Closure: 99.9%.**

The two need opposite treatment, which is why conflating them showed up
immediately. A park on the child's ring happens *inside* the producer's profile
scope, so it is already inside `stage_self_ms` and has to be **subtracted**, like
every other park. A park on its own output ring happens between pulls, outside
every scope, so it has to be **added** from the ledger. The first version of this
measurement added both, and the stage accounting closed at **144%** with
`stage_self_ms` equal to `stage_live_ms` to one decimal — an over-100% closure
is what caught it, which is the argument for printing the denominator at all.

**`stage_park_ms` is 0.0 on all 22 queries.** A producer never once filled its
ring and waited for the consumer to drain it. Combined with `pool_idle_ms == 0.0`
this is the same result from a third angle: *nothing in this engine is ever
blocked by a downstream consumer being too slow.* Every ring in the system runs
dry, never full. The producers are the bottleneck, everywhere, without exception.

And it revises the producer's own numbers: what was reported as 178ms of producer
work is really **107ms of work and 77ms of waiting on its child**. 42% of stage
thread lifetime is inherited idle, passed up from a scan that cannot fill the ring
fast enough.

### What the zero tells us

`pool_idle_ms == 0.0` everywhere is the useful negative result: workers never run
a full window ahead of their consumer, so the bottleneck direction is
unambiguous — **consumers wait on producers, never the reverse.** The large
`ring_wait_ms` said the same thing; this confirms it from the other side.

Which reframes the whole scheduler question. **The problem is not that threads
block badly; it is that there is not enough queued work to fill them.** A
scheduler redistributes parallelism, it does not manufacture it. So the lever is
*more and finer parallel work* — parallelizing serial blocks like q04's
semi-join, finer grain, more operators running concurrently — rather than better
scheduling of the work that already exists. That is an argument for demoting the
scheduler relative to the concrete serial blocks.

### Idle is not automatically reclaimable

The three kinds want different things, and conflating them is what produced two
wrong conclusions in a row:

* **Barrier park (419.6ms)** — the caller waiting on a batch it submitted. A
  work-participating `join` makes it a W+1th worker, shortening each batch by
  ~1/9 at W=8, so the recoverable slice is **~47ms, 3.6% of wall**. The park is
  large; the recovery is bounded by the work still having to be done.
* **Ring park (373.6ms)** — the consumer waiting on a producer it does not
  control. This mostly means **the producer side is the bottleneck**, and is often
  correct rather than wasteful: q06 is 98.7% idle because a scan-filter-sum
  query's main thread genuinely has nothing to do. A scheduler only reclaims it
  where there is other ready work, and a sequential plan often has none.
* **Serial work (443.5ms)** — the only part that needs algorithmic attention, and
  now half what it appeared to be.

### The open question this leaves

Not the accounting any more — all four kinds of park are measured, and the
producer-side fix turned out to change nothing. What is unmeasured is **pool
threads with no task queued at all**, which is 70% of the machine. Instrumenting
that means timing the pool's own `work.wait()` and attributing it to the query
rather than to an operator, since an empty pool belongs to no operator.

That number would say whether the 70% is unavoidable (the plan genuinely has no
parallel work available at that moment) or addressable (work exists but is not
being queued). Until it is measured, "the scheduler is worth X" remains
unanswerable — but the 2.72x against a 8x machine, with zero worker
backpressure, already points at supply of work rather than its scheduling.

### q04, the query that exposed both errors

Reported as 100% serial. It is not, and Polars gets 1.94x on it where Ibex gets
1.55x (SF-1 recorded suite: polars 39.9ms / polars-st 77.5ms, ibex 53.5ms /
ibex-st 82.8ms — note Ibex is also 6.8% slower single-threaded here, one of the
few PDS-H queries where it is).

Warm per-node profile:

| node | build_self | next_self | ring_wait | pool_work |
|---|---|---|---|---|
| `join semi keys=1` | 16.7 | 28.4 | 28.3 | **0.0** |
| `scan __ibex_source_1` | 0.0 | 20.3 | 20.2 | 113.3 |

So q04 is decode-bound — 125ms of parallel decode, with the main thread parked on
the ring — plus **one genuinely serial block: the semi-join's build, 16.7ms of a
63.9ms wall (26%), with zero pool work and zero barriers.**

That block is `init_int_swapped` (`chunked.cpp:3476`), a serial loop over
3,793,296 right-side keys probing a 57,218-entry map:

    for (std::size_t i = 0; i < rcol.size(); ++i) {
        if (rnull(i)) continue;
        if (auto it = seen.find(rcol[i]); it != seen.end()) it->second = char{1};
    }

Two gates decline, for different reasons. The left-side parallel predicate path
(`select_rows`) needs `kMinParallelPredicateRows` = 262144 and q04's left is
57,218 rows — a correct decline. The right-side intersection pass has **no
parallel path at all**; it was never written. The documented position that "the
build side is never threaded, 1.5% of q10 against the probe's 15%" was measured
on q10, where the build is small; after the build-side swap that made q04 -39%,
q04's build IS the query.

It parallelizes cleanly and deterministically: the pass only ever sets a flag,
never inserts, so `seen` is immutable throughout. Store a dense index instead of
a flag, give each worker a `vector<char>` over 57k slots (~57KB), and OR them —
byte-identical, no atomics. Sizing: 16.7ms of 63.9ms wall.
