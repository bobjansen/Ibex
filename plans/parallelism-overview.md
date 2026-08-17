# Multi-core execution: how it actually works

Status: descriptive, written 2026-08-17 against `main` @ `5d8b442`. This is not
a plan — it is the map of the machinery the plans keep adding to
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
operators; `chunked.cpp` reimplements most of them for the streaming path. The
chunked ones run in production; the whole-table ones are reached through
`interpret()` fallbacks, the `ops` layer (codegen), plugins, and the REPL's
`:load`. Their parallel coverage differs sharply:

| Operator | whole-table | chunked |
|---|---|---|
| Aggregate | per-group reduce only (`aggregate.cpp:1157`) | partitioned discovery + partial pre-aggregation + parallel emit + parallel slot fill |
| Filter | bounds scan parallel; **gather serial** (`filter.cpp:3354` passes no `exec`) | two-phase parallel gather |
| Join | parallel probe (`join.cpp:1167`) | parallel probe + parallel concat + swapped-probe replay |
| Distinct | serial | packed-key partitioned |

So the same query can be several times more parallel depending on which entry
point reached it — and a fix applied to one side routinely does not exist on the
other (memory already records this trap for the fused scan and the REPL `:load`
path). *Convergence:* at minimum a table in this document kept current; better,
delete the duplicated whole-table paths as the chunked ones reach coverage.

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

**I12 — `PipelinedStageOperator` runs on a raw `std::thread`, outside the pool's
accounting** (medium). `chunked.cpp:11151`. Consequences: (a) the thread is not
counted against either budget, so peak thread count exceeds
`decode_thread_count()` by one per staged breaker; (b) `on_worker_pool_thread()`
is **false** on it, so operators running under it happily submit their own pool
batches — which is intended (that is what the reserved-thread rule in
`scan_pipeline_worker_count` accommodates) but means the "outermost wins"
nesting policy has one silent exception. Nothing states the exception.

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

Sequenced against the two larger tracks (a real task scheduler, and the decision
on first-occurrence group ordering). Roughly half of Part 2 — **I6, I9, I10,
I11, I12, I14**, and parts of **I7** and **I13** — encodes the cost of a
fork–join round trip and a barrier, so a task scheduler where submitting is
cheap and joining does not park a thread rewrites the physics behind those
numbers. Leave them for the scheduler rather than harmonizing an answer to a
question that is about to change. The items below are the ones it does not
touch.

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
4. **I4** — decide, per operator, whether the whole-table implementation is
   still needed, and delete or align it. A decision, not cleanup: the scheduler
   is exactly the kind of change one otherwise implements twice.
5. **I2 + I3** — the type exclusions and the missing multi-Categorical
   partitioned discovery, once there is a shared predicate to hang them on.
