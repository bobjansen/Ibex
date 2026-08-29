# Runtime Multithreading Plan

**Compacted 2026-08-27** from ~1650 lines — the Phase-1 two-phase-filter
development diary and the completed-phase detail moved to git history at the
pre-compaction commit's parent. `parallelism-overview.md` is the current map of
what actually shipped; this file is the phase roadmap and the two design
sections that are still load-bearing (the LazyTable Synchronization Contract,
Phase 2 RNG). The separate pipelined-execution plan was removed as superseded;
its remaining scheduler work is owned by `kernel-pipeline-execution-plan.md`.

> **Nomenclature drift:** this plan predates the config rename. `IBEX_THREADS` →
> **`IBEX_CORES`** (compute budget); `IBEX_PARALLEL` was **removed** — serial is
> `IBEX_CORES=1`. Decode pool size is `IBEX_DECODE_THREADS` / `_SATURATION`. See
> `parallelism-overview.md` §1.7.

## Summary

Multithreading as a query-execution capability, not ad-hoc loops inside kernels.
The `Chunk` operators are the data unit for a morsel-driven executor. A query is
a sequence of **morsel-parallel map pipelines** (maximal runs of row-local,
chunk-preserving operators) separated by **barriers** (order / join / group-by /
distinct / window / rank / model). Start with one ordered parallel map pipeline
for row-local work, keep serial implementations behind barriers, expand only after each
operator family has an explicit correctness contract. DuckDB's model
(partitionable sources, pipeline breakers, worker-local + sparing query-global
state) without a general DAG scheduler.

**Determinism contract:** every parallel path produces byte-identical output to
its serial counterpart. Row order reconstructed positionally (sequence / range
order / prefix-summed offset), never by completion order; FP reduction order
fixed by a **data-derived** partition (morsel count from row count, never thread
count); group ids by first occurrence after the parallel phase; errors by lowest
morsel sequence, interrupt outranks a data error.

## Execution model (as built)

- **`WorkerPool`** (`include/ibex/runtime/worker_pool.hpp`) — process-owned,
  pre-spawned, mutex+condvar FIFO, completion latch (`Batch`). No futures / DAG /
  work-stealing. Non-reentrant: `submit` from a pool thread aborts, so every
  fan-out guards on `on_worker_pool_thread()` — "outermost wins, inner degrades
  to serial".
- **Two thread budgets:** compute (`IBEX_CORES`, every compute fan-out) and pool
  size (`decode_thread_count()`, sized for decode's oversubscription, the scan
  pipeline is its only consumer). See `parallelism-overview.md` §1.2.
- **`ExecutionContext`** — query-scoped, created at `interpret()`, threaded
  explicitly through operator construction + expression eval. Owns the pool
  handle + thread budget, query-local cancellation / deterministic error
  selection, RNG seed, immutable registries, deferred-scan state. One query at a
  time (`query_lease.hpp`) — no cross-query fairness / preemption to arbitrate.
- **Morsels** — `{sequence, row_offset, chunk}`. Partitionable sources hand them
  to workers; an ordered merger releases results in `sequence` order with a
  bounded in-flight ring for backpressure. Never mutex-wrap `Operator::next()`.
- **Eligibility** lives in `build_operator()` (which already owns fusion), which
  consults one `execution_capability()` pass per `Passthrough` chain:
  `ParallelMap` / `OrderedStream` / `Barrier` / `ParallelBarrier`. One owner, one
  vocabulary, one fallback path.

## Public configuration

`IBEX_CORES` = 1 / positive int / `auto`. Invalid values fail; never silently
oversubscribe. Embedding callers pass a policy through a `RuntimeOptions`
overload; a hand-built `ExecutionContext` ignores the environment (the spelling
for "ignore env"). Generated `ibex::ops::*` stays serial and is deferred.

## Phase 0 — Correctness prerequisites — COMPLETE

Every piece observable at `IBEX_CORES=1` landed: serial/transpiled parity gate;
multi-chunk validity concat in `MaterializeOperator`; island-level
`TableProperties` (`ordering` + `time_index` derived once per island, with the
shared helper carrying *both* the presence-based and overwrite-based serial
rules) + empty-output contract; `TableRangeMorsel` / `PartitionedTableSource` /
gather helpers; `ExecutionContext` threading (worker TLS deferred-scans removed);
one-query-at-a-time lease + deterministic error selection (lowest morsel
sequence, lower-sequence work drained before return; interrupt is a distinct
sentinel that wins). Item 5's LazyTable Synchronization Contract is written
(below) but unimplemented — it's Phase 3b's foundation.

**Extern policy:** any row expression containing an extern / plugin source /
table consumer stays serial until the registry carries enforced purity /
thread-safety metadata. Never infer thread safety from parser effect
annotations.

## LazyTable Synchronization Contract (design — unimplemented)

`LazyTable` (`include/ibex/runtime/lazy_table.hpp`) is the shared source behind
every parquet/deferred scan. Before more than one worker touches it, it needs a
documented ownership contract — **not** blanket "make it thread-safe". The
hazard surface is narrower than the object.

**Interim gate (Phase 0) — LIFTED 2026-08-02, worth ~nothing.** The gate made
any query reading a lazy source map-pipeline-ineligible. Removed once
established that no worker can reach a `LazyTable`: the morsel pipeline
materializes its input subtree to an owned `Table` on the building thread, and
every morsel source takes that finished table by `const Table&`. Measured: 2 of
22 PDS-H queries changed eligibility, q19 gained a map pipeline (no time
change), **only 5 of 22 form one at all** (q18/q19/q21×2/q22). The gate was never what kept
PDS-H serial — whole-script mode eagerly projects non-probe scans, and scan
conjuncts get pushed into the decoder (removing the `Filter` a map pipeline builds
from). What's left above the scans is joins/group-by/sort — barriers. **The
PDS-H multithreading gap is Phase 4, not Phase 3b.** A slice that streams a
source's morsels straight into workers reintroduces the hazards and must
re-establish eligibility first.

**Hazard surface — two boundaries:**

1. **`cache_`** (`robin_hood::unordered_map<string, ColumnEntry>`), a memo of
   *whole-file* columns — the only directly writable shared member (`schema_` /
   `rows_` / `stats_` are set once at construction). The selective paths do
   *not* avoid it: `project_where` / `join_key_selection` decode their predicate
   and dynamic-key columns whole-file through `decode_whole_columns` (which
   writes `cache_`), and `project_rows` reads it. What the selective paths
   bypass is *caching their gathered output* (a selection-scoped column must
   never masquerade as a whole-file entry). So the boundary is **per operation,
   not per path**: every selective call = (a) a whole-file predicate/key decode
   touching shared `cache_` + (b) a selection-scoped decode landing only in
   caller-owned output.
2. **The callbacks** — `decode_` (`ColumnDecodeFn`) and `key_filter_scan_`
   (`KeyFilterScanFn`) are plugin-supplied `std::function`s. The object is never
   reassigned but that is not "safe to call concurrently" — it closes over
   mutable reader state (an Arrow `FileReader`, its column readers, buffers); a
   single `FileReader` is not concurrent-safe. Contract half (b) *is* a
   concurrent `decode_` invocation. The source must guarantee either
   internally-synchronized/stateless callbacks, **or** a per-worker
   decoder/reader factory (the Parquet path: per-worker readers — Phase 3a
   landed the reader-product factory + idle pool).

**The contract — immutable-after-build.** Two per-query lifecycle phases:
1. **Build/decode** — single-threaded or externally synchronized. Every `cache_`
   write happens here, before fan-out.
2. **Read-only execution** — `cache_` never mutated. Concurrent readers need no
   mutual exclusion but *do* need a happens-before edge: the fan-out point
   publishes the frozen `cache_` (release on dispatch, acquire on each worker).
   Stronger than "no writes": readers take **raw pointers** into cache buffers
   (`membership_pass_rate` aliases a key column's `data()`), so execution must
   forbid **eviction or replacement** of entries, not just mutation. (A rehash
   is harmless — buffers sit behind `shared_ptr`.)

Concretely:
- **Whole-file `cache_` writes hoist into build.** Build must pre-decode every
  predicate column any deferred scan references (the driver knows these —
  `DeferredScan` carries `conjuncts` / `demand` / `key_column`). Fallback:
  per-slot `std::once_flag`/future so concurrent demand decodes exactly once.
  Duplicate uncoordinated decodes are forbidden.
- **The dynamic key column is an explicit choice, not pre-cached by reflex.**
  `project_where` / `join_key_selection` take the fused `key_filter_scan_` path
  *only when the key is not already cached* — the decode-fusion Stage 4 win (q17
  −54%, q08 −51%). Pre-caching the key silently forces the whole-key fallback.
  Options: **(a)** accept the whole-key fallback (simplest, ships only with a
  benchmark quantifying the loss); **(b, recommended)** run key selection as
  serial **phase A** of the two-phase deferred probe (once-per-scan, needs no
  per-worker decoder), fan out only **phase B** (projected `project_rows` over
  the survivor selection). Deferred-probe joins don't parallelize until Phase 4
  anyway.
- **Selective paths run per-worker only on an already-populated cache.** With
  predicate columns pre-cached, a phase-B worker reads immutable `cache_` and
  decodes its own selection into its own output — sharing nothing writable.
  Eligibility must guarantee *both* pre-cache *and* concurrent-safe decode, or
  fall back to serial.

**The freeze is per query; the object outlives it.** A `let`-bound `LazyTable`
persists across statements, so each query's build phase reopens `cache_` then
re-freezes it. Safe only under one-query-at-a-time: every worker of query N must
have **joined** before query N+1's build writes `cache_`. Tie the thaw to that
join barrier.

**Dynamic scan-filter publication.** The join build publishes a
Bloom/IN/min-max filter into the probe scan's `DynamicScanFilter`. Redefine
`ready` as the **atomic publication of the build's decision** (release), not of
a filter's mere presence: the build stores `ready = true` *even when it produces
no filter* (empty payload), after the payload is fully written; the probe reads
with acquire; the payload is immutable after that store. On the parallel path
the "`!ready` ⇒ decode unfiltered" fallback is **prohibited** — a probe seeing
`!ready` there is an ordering bug. The serial interpreter keeps the old
fallback.

**Tests.** Parity under 1/2/N threads over a lazy source; whole-column `cache_`
reuse across two concurrent references (decodes once, both see it); filter-
publication test that a fanned-out probe always observes a published decision,
never the `!ready` fallback, including build-decides-no-filter; a key-decision
test pinning whichever of (a)/(b) is chosen; a **callback-contract test that
actively detects re-entry** — a fake `ColumnDecodeFn` that sets an atomic
in-flight flag on entry and fails if already set. ThreadSanitizer build for the
concurrent-lazy-scan tests.

## Phase 1 — First morsel-parallel map pipeline — LANDED, ON by default

`ExecutionContext::parallel` defaults true. What shipped, in order:
- **Morsel pipeline + worker pool + ordered merger** — `MorselPipelineOperator`
  dispenses morsels off one atomic cursor, per-worker map chain, bounded ring
  merge by `sequence`. Tests assert serial and parallel byte-identity.
- **Row-local `Update` eligibility** — an unguarded, ungrouped, tuple-free
  update whose every field is `is_subset_evaluable_expr` (stricter than
  `is_row_local_update_expr` — the looser one admits aggregates, which per
  morsel become per-morsel aggregates) is `ParallelMap`.
- **Range-aware filter** (`RangeFilterMorselSource`) — a `Filter`/`FilterProject`
  head evaluates the predicate over the input's rows directly; the morsel is
  never materialized. `is_range_native_expr` is the gate — a scalar-call
  predicate (e.g. `abs(x) > 50`) stays on `GatherMorselSource` or it re-runs the
  call over the whole input per morsel (a shipped 10× regression before the
  gate). Two `abort`s enforce the invariant (`require_range_evaluable`,
  `slice_computed`).
- **`TwoPhaseFilterOperator`** — for an island that is *exactly one* range-native
  filter (+ metadata-only tail): phase A counts survivors per morsel (parallel)
  → serial O(morsels) prefix sum → phase B gathers into disjoint output slices
  (parallel). One chunk emitted, `MaterializeOperator` *moves* it — the merge
  copy is gone, not merely cheaper. **Bit-packed columns** (`Column<bool>`,
  validity): a gather writes a contiguous bit run, so it meets a neighbour only
  at the two end words — `std::atomic_ref::fetch_or` on those, plain stores
  between; soundness rests on writes being monotonic (zero-filled first, only
  ever set bits). `filter_gather_is_thread_safe` is a per-column-kind allowlist
  (new variant defaults to unsafe). Race test: 50k rows, grain 37 (coprime with
  64), 8 threads — fails 5/5 with plain `|=`.
- **Metadata-only tail** — Project/Rename copy no rows, so the two-phase filter
  applies them once to the finished output via `project_table`/`rename_table`
  (the same functions the serial path calls).

**Every island shape now beats serial** (20M rows / 6 cols / 8 threads): 4 bulk
filters 4.9×, +rename 5.4×, fused FUP 2.0×, +nullable 5.0×, selective ~∞. The
two knobs are **derived, not tuned**: `parallel_grain` = `clamp(rows / (threads
* 4), 4096, 65536)` (the divisor keeps ≥2 morsels/thread; the upper clamp is
load-bearing — uncapped gives 625k at 20M/8 and measures worse); `parallel_min_cells`
(512k) joins `parallel_min_rows` because island cost scales with *cells* (131k
rows won at 6 cols, lost at 2). **A refused island runs as one whole-table
chunk, not a serial morsel sweep** — conflating those measured +64ms vs a 36ms
baseline.

**window_ohlc — the benchmark that motivated multithreading — does not move.**
Every node in that query is a barrier (Scan / AsTimeframe / Aggregate / Window /
Order), so no island forms. Closing that gap needs a *grouped windowed
aggregate* to parallelize (partition by group key, not row range) — Phase 4
work, not a variation on islands.

**Still not range-aware:** `evaluate_field` (the per-row registry path shared
with `update`), whole-column builtins, `eval_scalar_over_columns` — all evaluate
whole-table and slice. Range-threading `evaluate_field` is what unlocks the 1:1
`Project`/`Update` shapes (output cardinality known, merge copy removable
without a two-phase pass).

## Phase 2 — Deterministic RNG and generators (design — not started)

`RngStream` exists but expression eval still reaches thread-local engines and
`seed_rng()` only reseeds its caller.

- Master seed in `ExecutionContext`, explicit `RngStream` through `ColumnEvalCtx`
  and all generator paths.
- A fixed **logical RNG tile** (not an executor morsel) is the unit of random
  work — stable absolute source row ranges, tile size independent of worker
  count / scheduling / morsel grain. Serial interpreter and transpiled path use
  the exact same scheme.
- `ExecutionContext` owns a stream manager: one stream per `(generator-expression
  identity, logical tile sequence)`. Expression identity stable in the canonical
  program so adding/reordering independent generators doesn't perturb an
  existing one. Never hand values off a shared cursor (arrival-order dependent).
- Derive tile streams with xoshiro256++ `long_jump()` (2^192 steps/jump, already
  used in `zorro.hpp`). Fail rather than overlap if a tile could exceed 2^192
  draws. Seed-mixing (`derive_rng_seed`) is not the split mechanism — it looks
  independent but gives no non-overlap guarantee.
- **Contract:** a fixed seed → same values for the same logical input rows and
  program across thread counts, completion orders, morsel sizes. Tile size is
  part of the contract (changing it is a documented compat decision). Update
  `SPEC.md` / `README.md` / `docs/index.html` when the guarantee changes.
- Extend the host `RngBridge` or keep plugins serial — never a silent
  thread-local fallback (see `project_data_gen_plugin_rng_bridge` in memory).

## Phase 3a — First-party Parquet + Arrow-compatible storage — COMPLETE

Primitive/temporal/Bool/UTF-8/Categorical columns can retain an immutable
external owner + base buffers, detaching only mutated storage (COW). `adopt_table_from_arrow`
keeps payload+validity zero-copy including sliced offsets; the copying importer
remains. Parquet is owned by `Ibex::parquet` (CLI/REPL/Python register directly,
`import "parquet"` skips DSO load); the compat plugin is a thin delegate. Lazy
sources acquire reader **products** through a runtime factory — each owns an
independent mutable `FileReader` while sharing the immutable input/schema/footer;
successful products return to a source-local idle pool (sequential stages skip
setup; simultaneous acquisitions get distinct products). R uses
`nanoarrow_pointer_export()` for a self-contained lease. Dependency direction:
`Ibex::runtime ← Ibex::parquet ← host`; runtime defines the contracts without
Arrow C++ headers.

## Phase 3b — Parallel sources and I/O

**First slice landed 2026-08-16.** `LazyTable` streams row groups in bounded
ordered units; units decode concurrently with independent reader products; a
direct source or maximal row-local chain publishes completed units downstream
without waiting for a decode window. First bounded join-probe output handoff
landed. Pushdown / cancellation / backpressure / dictionary unification
preserved. The historical SF-1/SF-4 measurements are in git; the unresolved
two-core admission problem and general breaker work are now tracked by
`kernel-pipeline-execution-plan.md`.

**Still open:** CSV, TSAN coverage, generalized source partitioning (SF-1 has
too few row groups — must partition columns and row ranges, not just row
groups), progress-aware admission. Implement the LazyTable Synchronization
Contract and lift any remaining gate. Bound reader count independently from
morsel count (local files vs remote objects vs many-column scans differ).
**Acceptance:** identical results/metadata under 1/2/N threads; active-reader +
source-morsel counters; TSAN for cache publication / reader isolation /
cancellation / foreign-buffer lifetimes; decode-only *and* end-to-end speedup
reported separately.

## Phase 4 — Parallel barriers

The PDS-H gap lives here. Order, by measured recoverable time (re-derive at a
larger scale before ranking again — the threading share of a gap grows with row
count while Ibex's threading gain does not; the old 32M breakdown is stale):

1. **Ungrouped / low-cardinality aggregate — LANDED** (`fc8d10e`). 1M/6 aggs
   13.4→2.3ms. Two rules for the rest: **(1) partition on ROW COUNT ALONE** — a
   float reduction's value depends where the range is cut, so a pool-size
   partition makes `sum`/`std` differ between a 4- and 24-core box; **(2)
   morsels few and large, do NOT reuse `island_grain`** — a reduction's per-row
   cost is constant so equal ranges finish together; the 4-per-thread grain
   measured slower than serial.
2. **Categorical group-by — LANDED** (`956a4a4`). A Categorical code is a dense
   dictionary index, so a worker accumulates into a private array indexed by
   code — no hash table, per-worker state bounded by dictionary size. First-
   occurrence order preserved (morsels are ascending row ranges recording codes
   in seen order). **Open:** multi-key categorical, the string/int/generic hash
   paths, `distinct`.
3. **Hash join — RETIRED, not deferred.** The gap was a Categorical probe key
   hashed as *text* per row (robin_hood string probe + `__memcmp_avx2` +
   `_Hash_bytes` ~57% of the join profile). Resolving each code once:
   inner_join_user 351.8→50.2ms, 3 of 4 join benchmarks went from losses to
   1.4–6.2× wins over threaded Polars. Threading the probe would have
   parallelized the hashing instead of deleting it. **If a profile shows string
   hashing under a grouped/joined operator, check whether the key is Categorical
   before reaching for threads** — this pattern was fixed in *five* places
   (`7e6576e`, `bdb5f30` ×2, `ff42bd9`, join probe); each resolves through the
   dictionary *text*, since two codes can carry the same string.
4. **Sort / top-k — rank consumer done, rest untouched.** `ff42bd9` gave the
   radix fast path its own tie scan; `463fe8a` split the per-group sweep across
   workers (groups share no rows — no merge); `35f1d4f` replaced "sort globally
   then counting-sort by group" with "bucket by group then sort each run in
   place" (with a small-run fallback found by prototyping against the structure
   it replaces — 1M groups of 8 rows lost). rank_by_symbol 584.7→135.3ms.
   `order_*_topk` / `sort_*` not threaded (Ibex already wins top-k ~12×).
5. **Window / rank / fill / cumulative** — only after defining partition-boundary
   halo state. Has a serial half worth doing alone: the per-core deficits vs
   Polars (st/st < 1) are where_update_window 0.26, corr_price_vol 0.27,
   tf_lag1 0.29, fill_null 0.57 — expression eval + rolling/lag/fill kernels,
   roughly scale-invariant.

**Aggregate slot diet (landed alongside):** `AggSlotCore` is POD, 32 bytes
(`ScalarValue` split out — `alloc_group()` resizes per new group, so 100k groups
grew the array ~17× as *string* moves; **triviality, not size**). An aggregate
declares per-group scratch in the plan (`SlotPlan::scratch_doubles`, group-major,
`double` so trivially copyable) — Skew/Kurtosis are the first consumers. NOT
covered: variable-length per-group state (median) — needs histogram + prefix-sum
offsets into one flat buffer.

**Method notes:** measure a restructure in a standalone harness against the
structure it replaces before writing it (`35f1d4f` found its own losing shape);
doing strictly less work is not the same as being faster (removing rank's two
full-width passes measured *slower*, 432→524ms, while dropping RSS — reverted,
suspected aliasing).

## Generated C++ and plugins

A plugin/extern callback must never call `runtime::interpret()` — the host
rejects a nested entry (not by coordinating static runtime copies in DSOs).
Generated C++ emits separate `ibex::ops::*` calls, does not share an
`ExecutionContext`, cannot form a fused island — stays serial, and its parity
suite is a serial semantic gate, not parallel coverage. Plugin registration
needs `thread_safe` + a precise execution kind before parallel invocation;
unknown / stateful / I/O functions stay serial.

## Test plan (essentials)

- Serial/transpiled parity gate (schema, metadata, row order, values,
  categorical representation, validity) before any parallel execution.
- 1/2/N-thread output equality for every island shape (filters, projects,
  row-local updates, fused FUP, empty chunks, categoricals, strings, bools,
  nulls) — byte-for-byte row order + validity.
- Island metadata: vary morsel size / worker count / completion order for an
  ordered TimeFrame through filter/project/rename/update that
  retain/drop/rename/overwrite ordering + time-index columns; compare
  `ordering` / `time_index` / `logical_rows`, not just rendered rows.
- Cancellation determinism: two deterministically faulting morsels report the
  lowest-`sequence` error identically under 1/2/N threads; interrupt wins;
  second concurrent executor entry rejected; re-entrant plugin call rejected.
- RNG: fixed-seed identical across worker counts / completion orders / morsel
  sizes; multiple + nested generators; tile boundaries; plugin bridge; through
  both interpreter and transpiled paths.
- Lazy/deferred scans: race-focused (cache fills, filter publication, source
  sharing, build-before-probe) under a **ThreadSanitizer** build in a
  repeat/stress harness. A normal `ctest` pass is functional coverage only.
- Release benchmarking only; report 1-thread and auto-thread alongside Polars
  single + default.

## Non-goals for the first landing

Replacing the chunked runtime with a general DAG scheduler; concurrent
`Operator::next()` on existing operators; parallelizing every operator or making
plugin code implicitly thread-safe; changing source/output ordering or weakening
seeded-RNG reproducibility.
