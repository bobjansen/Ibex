# Pipelined Execution — scoping

Status: **scoping only, nothing implemented.** Written 2026-08-14 from the
thread sweep below. Read `plans/runtime-multithreading-plan.md` and
`plans/chunked-execution-plan.md` first; this proposes the thing both of them
stop short of, and it supersedes neither.

## The measurement that motivates it

PDS-H SF-2, one box, `taskset -c 0-(N-1)`, the same core budget handed to both
engines, four archived runs in `benchmarking/tpch/results/runs/`:

| cores | Ibex total | speedup | efficiency | Polars total | speedup | efficiency | Polars/Ibex |
|---|---|---|---|---|---|---|---|
| 1 | 3779 ms | 1.00× | 100% | 8164 ms | 1.00× | 100% | **2.16** |
| 2 | 2982 | 1.28× | 64% | 4141 | 1.87× | 93% | 1.39 |
| 4 | 2575 | 1.49× | 37% | 2671 | 2.73× | 68% | 1.04 |
| 8 | 2309 | 1.66× | 21% | 2000 | 3.44× | 43% | 0.87 |

**Ibex is 2.16× faster than Polars on one core and 1.15× slower on eight.**

The shape matters more than the endpoints. Ibex's *first* doubling returns
1.28×, against Polars' 1.87×. It is short from the start and short by roughly
the same proportion at every step (marginal +0.28 / +0.21 / +0.17 against
+0.87 / +0.86 / +0.71). That rules out contention and memory bandwidth, which
would show a near-ideal first doubling degrading later.

Amdahl fits the whole curve from one number. Solving the 2-core point gives a
parallel fraction of **≈44%**; feeding that forward predicts 1.62× at 8 cores
against 1.66× observed. Ibex behaves like a program that is **56% serial**, at
every thread count.

Per query the implied parallel fraction has a **median of 41%, and 14 of 22
queries are below 50%**. The top of the range is fine — q06 at 93% scales
3.29×, beating Polars' own 2.09× on that query, and q01 at 82% reaches 2.71×.
Those are the queries that are essentially one row-local pass. Everything with
a join or a group-by is in the bottom half.

This also explains why five separate operator-level threading attempts on
2026-08-14 each did what they claimed to their operator and moved the query by
0–3%: on a query that is 80% serial, doubling one operator's parallel phase is
worth almost nothing. Four were reverted for being unmeasurable and one for
regressing.

## Diagnosis: materialize-then-fan-out

The executor is a pull-based chunk pipeline already — `Operator::next()` returns
a `Chunk`, and `Chunk` even carries `sequence` and `row_offset` so an ordered
merger can reassemble morsels produced out of order. The substrate anticipated
this work.

What is missing is that **nothing ever produces more than one chunk, and no two
chunks are ever in flight at once.** Three places enforce that:

1. **Every production source is drained to one table.** The REPL's batch path
   resolves a reader through `lazy_table_func`, calls `LazyTable::project`, and
   wraps the result in `TableSourceOperator`, which emits exactly one chunk.
   Verified: `chunks=1` on every operator of every PDS-H query.
2. **A chunked source is materialized even when one exists.**
   `ChunkedParquetSourceOperator` streams 65536-row Arrow batches
   (`plans/bigger-than-ram-plan.md` Phase 4), and `src/runtime/chunked.cpp`
   around the `chunked_table_func` branch immediately calls
   `materialize_operator()` on it.
3. **Parallel islands materialize before fanning out**, by a documented
   load-bearing invariant: the island's input subtree is executed to a `Table`
   on the calling thread, and every morsel source below takes that finished
   table by reference. That is what makes a `LazyTable` safe inside an island.
   Islands also only cover row-local chains, which is why only 5 of 22 queries
   form one at all.

So each operator runs to completion, on one thread, before the next begins.
Whatever parallelism exists is strictly *intra*-operator with a serial merge
between, and the serial phases of every operator add up with nothing to overlap
them. That is the 56%.

Polars reaches 3.44× because morsels flow through the whole pipeline
concurrently: one morsel joining while another decodes and a third aggregates.

A consequence worth stating because it is a correctness risk, not just a
performance one: **the cross-chunk paths in the operators have never run.**
`KeyPartition::stored`, `partitioned_active_`, `cat_dictionary_id_`, the
distinct operator's `packed_part_count_` pinning, the pair path's dense-array
rebuild — all of it is written for multi-chunk input and all of it is dead
today. One latent bug in that machinery was already found and fixed on
2026-08-14 (`4ba4b75`) purely by reading. There will be others.

## The constraint that shapes the design

The single-chunk path is not an accident or laziness — it is where the biggest
wins of the last months live, and they are worth more in absolute terms than
the threading gap:

- projection pushdown (`plans/parquet-*`)
- dynamic filter pushdown, geomean ≈ −12%
- decode fusion incl. late materialization, geomean −14%
- null-free stats fast paths, geomean −27%
- the fused key-filter scan, q17 −28%

Every one of those works by giving `LazyTable` the *whole* query's demand —
columns, conjuncts, join keys — and letting it decode once, minimally. A naive
"stream row groups through the pipeline instead" throws all of it away, and the
arithmetic is not close: −12/−14/−27% against a threading ceiling that, even if
perfectly achieved, is worth ~1.6× on 8 cores.

**So the design constraint is: pipelining must be built on top of the pushdown
machinery, not instead of it.** The natural shape is that a scan still plans its
decode globally (all pushdowns intact) but *yields* the result in row-group or
batch units rather than as one table.

## Phases

Deliberately ordered so each is separately measurable and the risky one is last.

### Phase 0 — make multi-chunk real, and prove it correct — **DONE (`f9db6a0`)**

No performance goal. Get more than one chunk flowing and find out what breaks.

- A test-only or env-gated switch that makes a source emit N chunks for a
  materialized table (`PartitionedTableSource` already does exactly this, and is
  currently used only by islands and tests).
- Run the full PDS-H answer check and the 1574-test suite with it on, at several
  grains including pathological ones (1 row, 1 chunk, prime-sized).
- Extend the parity comparator (`plans/serial-parity-comparator-plan.md`) to
  assert chunked-vs-single-chunk equality structurally, not by diffing stdout.

Exit criterion: every query is byte-identical at every grain. Expect real bugs
here — this machinery has never executed.

### Phase 1 — a streaming scan that keeps its pushdowns — **DONE, on by default**

- Give `LazyTable` a way to yield its planned decode in units (row group, or
  Arrow batch) instead of one table, with projection, conjuncts, dynamic key
  filters and late materialization already applied.
- Route the batch path to it, keeping `TableSourceOperator` for anything that
  cannot.
- Measure with pushdowns on. The bar is *no regression*: this phase buys memory
  and cache locality, not parallelism.

Trap: `LazyTable::cache_` is not thread-safe and the deferred-probe path
explicitly declines when a key column is already cached. Streaming must not
quietly disable the fused scans — check by diffing plan shapes (profiler
`op="..."` counts), which is how a silent decline was caught on 2026-08-14.

**What landed.** `SourceUnit` is a source-global row range a reader can decode
alone; `LazySourceReader::decode_units()` reports them (Parquet: one per row
group) and `decode` / `key_filter_scan` / `string_filter_scan` all take one.
`LazyTable::project_where_unit` is `project_where` restricted to a unit, with
every pushdown applied to that unit rather than declined — the fused scans
included, because both already plan over the whole file and answer in
source-global indices, so restricting them is a filter on their group list.
`DeferredScanSourceOperator` drives it. **Streaming is the default**;
`IBEX_STREAM_SCAN=0` opts out, and answers both ways for the same reason
`IBEX_PARALLEL` does — a switch that could only turn it on leaves no way to
turn it off.

Two things had to be got right and are worth remembering. Selections stay
source-global at every boundary, which is what lets the unit path reuse the
whole-source filtering code instead of growing a second index space. And a unit
decode never touches `cache_`: a unit holds a *fragment* of a column, and a
fragment in the cache is indistinguishable from a whole one to every later
reader — including the fused scans, which decline when their key column is
cached.

**The bar was not met, and the way it was missed is the useful result.**
PDS-H SF-1, interleaved, min-of-5, answers identical and plan shapes identical
(no pushdown silently declined):

| | geomean vs materialized |
|---|---|
| 1 core | **0.943** |
| 8 cores, `taskset -c 0-7` | **1.084** |

Streaming is **5.7% faster on one core and 8.4% slower on eight**. Since the
same binary does the same work in both, the regression is not extra work — it
is *lost parallelism*, and the single-core number says the phase delivered
exactly the cache-locality win it promised. q01's decode confirms it directly:
pool work drops 234ms → 125ms while wall rises 123ms → 208ms, and occupancy
falls 0.44 → 0.14.

The cause is the diagnosis above, one level down. A unit is ~1M rows, which is
plenty to parallelize (`parallel_min_rows` is 65536, so no gate is being
missed) — but six units run one after another with a serial phase between each,
so the pool sees six short bursts instead of one long one and nothing overlaps
them. Materialize-then-fan-out, at unit granularity.

**This is Phase 2's case, made quantitatively.** Concurrent units are not a
refinement of Phase 1; they are the thing that turns the 0.943 into the
8-core number. Until then the switch stays off.

Per-query, the 8-core split is wide (q12 −29%, q08 −9%; q01 +69%, q20 +42%,
q19 +36%). q21 is the one query that loses single-threaded too (+13.6%) and is
the place to start if Phase 2 lands and something still regresses.

### Phase 2 — concurrent chunks — **first slice landed**

The actual win. Multiple chunks in flight through the operator chain.

**What landed: concurrent units inside the scan.** The scan decodes a WINDOW of
units on worker threads instead of one after another, and the window after the
one being served is already decoding. Ordering is untouched — workers claim
units from a shared cursor and write only their own slot; chunks are served in
unit order with `sequence` / `row_offset` assigned on the calling thread.

Decoding a unit on a worker is safe because `LazyTable::acquire_reader` hands
each concurrent acquisition its own reader product (that is what the reader pool
was built for), `project_where_unit` never writes `cache_`, and every inner
parallel path checks `on_worker_pool_thread()` and runs serial inside a task.
The middle one is now load-bearing rather than merely tidy: routing any part of
the unit path back through `project()`, which does cache, turns it into a race.

PDS-H SF-1, 8 cores, interleaved, geomean against the materialized path:

| | geomean |
|---|---|
| Phase 1 (units, serial) | 1.084 |
| Phase 2 (units, concurrent) | **0.922** |

Streaming is now **7.8% faster** than materializing, where Phase 1 was 8.4%
slower. q12 −44%, q06 −31%, q04 −23%, q19 −21%, q14 −19%, q01 −17%, q15 −16%.

**Two fixes mattered more than the concurrency itself**, and both were found by
following the profile rather than by reasoning about the design:

* *Per-row dictionary interning.* The chunk-to-chunk categorical remap interned
  **per row** instead of per dictionary entry. On q01 that was 114ms of a 160ms
  scan — the entire regression — and it was invisible from the query: TPC-H's
  `l_returnflag`/`l_linestatus` are plain `string` in the Arrow schema and only
  become Categorical because the writer dictionary-encoded them. Interning each
  dictionary entry once and gathering codes took q01 from +90% to −18%.
* *A concat that only existed because chunks did.* The semi/anti join's swapped
  path called `MaterializeOperator` on its left, which was free when the left
  was one chunk and a full copy once it was six. It buffers the chunks as a list
  now — it needs the left twice, but never glued. q21's semi join went 113ms ->
  87ms, and the query from +40% to +10%.

The second is the shape to expect more of: **operators that were written against
a one-chunk world hide a concat.** They are correct either way, so only a
profile finds them.

**q21, run down.** The residual was in a profiled statement after all — an
earlier single-sample reading said otherwise and was wrong. Statement 1 (+16%)
carried the whole of it, and inside that statement the cost was not any operator
but the **sink**: `MaterializeOperator` spent 33ms appending where the
single-chunk path spent 0, because one chunk is *moved* into the result table
and six must be concatenated. Appending was a `push_back` per row even for a
flat numeric column; `append_column_values` now bulk-copies, which took
statement 1 from +16.4% to +8.9%. What is left is the memmove itself —
`li_F` is 2.9M rows of four POD columns, so ~58MB of copying that the
single-chunk path never does.

That cost is inherent while a `let` binding is one contiguous `Table`: streaming
a large intermediate into a binding must glue it back together. It is the same
lesson as `plans/runtime-multithreading-plan.md`'s "the MERGE CONCAT is the real
island cost", and it says the wins track *output* size — which is exactly the
observed split, since every query that gains reduces its rows sharply (q06,
q12) and every one that loses binds a large intermediate.

**q20, run down — and the general lesson.** Its high-cardinality group-by cost
50ms -> 79ms under streaming, and the cause was not the aggregate's merge but
its **gate**: `pool_tasks` went 32 -> 0, i.e. partitioned group discovery
declined outright. The gate asks "are there enough rows here to be worth
partitioning", and it asked it of the CURRENT CALL. q20's aggregate sees 909k
rows over 543k groups — comfortably qualifying — but as six chunks that is
~151k per call, under the 262144 threshold, so it declined on every one.

**Chunking divides every per-chunk row gate by the number of chunks.** That is
the systemic consequence of this phase and it will keep biting; q20 is simply
where it bit first. The engine's other row gates use `parallel_min_rows`
(65536) and still clear it at these sizes, which is why only this one showed.

Fixed by counting the rows the *operator* has been offered rather than the rows
in this call. The threshold itself is unchanged — lowering it is a measured
dead end, because the break-even is set by group cardinality, not row count
(`plans/` history: q13 +9.3%). Starting part-way through a stream then means
groups already exist in the serial index, which the partitioned path neither
reads nor writes, so they are seeded into the partitions keeping their existing
ids. Only the packed path cannot do this — its key is built from a row and is
not invertible — so it declines to start late, which is the previous behaviour.

q20 +23% -> +10%, q21 +9% -> +5%, suite geomean unchanged at 0.921. What is
left on q20 is that its first chunk is still discovered serially and its groups
then seeded; closing that needs the aggregate to defer its first chunk until
the decision is made.

**q22 +7%** and the rest of **q21** are the concat above. The scan itself is
now faster than materializing on every query measured.

### Turning it on by default

Flipping it found a **data-loss bug** that every measurement up to that point
had missed: the window loop asked `batch_.has_value()` to decide whether a
window was in flight, and a ONE-UNIT window is decoded inline and never
submits. So a serial query dropped every unit after the first, and a parallel
one dropped any trailing single-unit window. The PDS-H suite never saw it —
lineitem's 6 units fit one window of 8 exactly — and only the e2e's
`IBEX_PARALLEL=0` leg, which asserts a row total, caught it. The e2e streaming
check now runs serially on purpose rather than by luck.

The lesson: **the shapes that break streaming are window remainders and serial
execution**, and neither is exercised by the benchmark suite.

### Memory: not the free win it looks like

Streaming was expected to bound peak memory. It does — but only below the
window. Peak RSS scales with the window, which is the thread budget:

| PDS-H SF-1 | materialized | streamed w1 | streamed w8 |
|---|---|---|---|
| q04 | 193M | 127M | 268M |
| q19 | 137M | 57M | 170M |

So `IBEX_THREADS` now bounds peak decode memory as well as parallelism, and at
the default window several queries use 20-47% MORE than materializing. Much of
that excess is not live data: it is glibc growing a free list per worker arena,
since decoding moved onto the pool. `MALLOC_ARENA_MAX=1` takes q13's +33% down
to +2% and q04's +45% to +20%.

A lookahead window — dispatching window k+1 before serving window k — was built
and **reverted as a measured dead end**: +52% peak RSS on a 25-row-group scan
(161MB -> 244MB) for zero wall-clock change, because the consumer is a blocking
operator that eats chunks faster than they decode. There is no consumer work to
overlap with until the rest of the pipeline runs concurrently.

### What the sweep says about the rest of the phase — read this first

Repeating the thread sweep was this plan's own gate ("the number that must move
is the implied parallel fraction, not the wall time"). Run at SF-2 on the same
harness, materialized vs streamed:

| | 1c | 2c | 4c | 8c | speedup | implied parallel fraction |
|---|---|---|---|---|---|---|
| materialized | 4990 ms | 4153 | 3568 | 3236 | 1.54× | 34% |
| streamed | 4456 ms | 3967 | 3365 | 3033 | 1.47× | **22%** |

Streaming is faster at every core count (−11% at 1c, −6% at 8c) and the
parallel fraction went **DOWN**. It did not make more of the program parallel;
it made the serial part cheaper. **The premise at the top of this document —
that pipelining is what raises the parallel fraction — is not what Phase 2's
first slice delivered**, and nothing about the rest of the phase should be
justified by it without new evidence.

(Caveat: this harness times whole processes, so startup and plugin load are
counted as serial and deflate the fraction against the `run_bench.sh` numbers
in the table at the top. The materialized-vs-streamed comparison is same-harness
and sound; the absolute 34%/22% are not comparable to the 44% above.)

### Where the serial time actually is

Calling-thread ms summed over real operator nodes across the suite, 8 cores,
with the worker help each drew (`pool_work / self`):

| operator | self ms | pool ms | worker help |
|---|---|---|---|
| scan | 479 | 2257 | 4.7× |
| aggregate | 473 | 813 | 1.7× |
| **join inner** | **422** | 92 | **0.2×** |
| **join semi** | **326** | 0 | **none** |
| **join anti** | **33** | 0 | **none** |
| update | 57 | 104 | 1.8× |
| distinct | 40 | 160 | 4.0× |

**Joins are 42% of calling-thread operator time and draw essentially no worker
help at all**; semi and anti draw literally none. The scan, which this phase
spent its effort on, is now the best-parallelized operator in the engine. The
same ranking holds with streaming off (joins 54% there), so it is a property of
the join operators, not of streaming.

So the next step is **not** the general scheduler below. It is the joins, in
this order:

1. **Semi/anti join** — 359ms, zero worker help, and the simplest shape: build
   a key set, then filter. The filter pass is row-local over a hash set, and
   under streaming its left input already arrives as chunks.
2. **Inner join** — 422ms at 0.2×. `plans/runtime-multithreading-plan.md` notes
   a parallel probe was built and shelved (`git stash` "parallel inner-join
   probe") as unmeasurable, with `assemble_output` named as the other half.
   It was unmeasurable because the scan dominated then; it does not now.

The scheduler remains the design for running the whole chain concurrently, but
it is a large change and this measurement does not argue for it yet.

The rest of this phase — a scheduler that runs the whole operator chain
concurrently rather than just the scan — is unstarted, and the table below is
still the design.

Requires deciding, per operator, which of three it is:

| class | operators | behaviour |
|---|---|---|
| streaming | filter, project, rename, update (row-local) | chunk in, chunk out; trivially concurrent |
| blocking | aggregate, sort, distinct, join build side | must see all input before emitting |
| semi-blocking | join probe, top-k | blocked on build, streaming on probe |

The classic morsel-driven answer is that a *pipeline* runs from a source to the
next blocking operator, and pipelines are scheduled with the blocking operator
as a barrier. That is a scheduler, and it replaces `build_operator`'s
straight-line chain — this is the large part of the work and should not be
started before Phases 0 and 1 have landed and held.

Ordering is the contract to preserve: `Chunk::sequence` / `row_offset` exist for
exactly this, and `MaterializeOperator`'s in-order concat is the existing
ordered merger. Everything Ibex reports in first-occurrence order (group-by
output, distinct, `head`) depends on it.

### Phase 3 — retire the island special case

If Phase 2 lands, parallel islands become a special case of the general
scheduler and the materialize-before-fan-out invariant can go. Not before.

## Validation

- **Answers**: all 22 PDS-H answers byte-identical at every phase, at every
  grain. Non-negotiable; this is the gate that catches the dead cross-chunk
  paths waking up.
- **Suite**: `ctest` (1574) plus `scripts/ibex-e2e.sh` at each phase.
- **Performance**: `run_bench.sh` archives + `compare_runs.py`, which reports a
  reference engine's drift so a box-condition change cannot be read as a result.
  Repeat the 1/2/4/8 sweep at each phase; the number that must move is the
  **implied parallel fraction**, not the wall time.
- **Interleaved A/B with a control query** for anything narrower, per the
  methodology that caught three false positives on 2026-08-14.

## Non-goals

- **Not out-of-core.** `plans/bigger-than-ram-plan.md` owns spilling. Streaming
  helps peak RSS as a side effect; that is not the goal here and must not be
  used to justify a regression on the timed path.
- **Not a rewrite of the operators.** They are already chunk-shaped. The work is
  in what feeds them and what schedules them.
- **Not more intra-operator threading.** That is the thing this measurement says
  has run out of road. `plans/runtime-multithreading-plan.md` Phase 4 remains
  open for specific gaps, but it is not the answer to the curve above.

## Open questions to settle before Phase 2

1. Where does the scheduler live — inside `build_operator`'s seam (the
   `execution-plan-seam-plan.md` Option B position), or above it?
2. What is the chunk grain, and is it per-source or global? `IBEX_MORSEL_ROWS`
   exists for islands; a pipeline may want a different answer.
3. How does a blocking operator's *build* phase get parallelised, or does it
   stay serial? The measurement says `build_index` is only 1.5% of q10, so
   possibly it stays serial forever, which would simplify the scheduler a lot.
4. Does the REPL's statement-at-a-time model need to change, or can a pipeline
   stay inside one statement? Everything above assumes the latter.

## Honest assessment of size

Phase 0 is days and will surface bugs. Phase 1 is a substantial change to the
most performance-sensitive code in the tree, with a hard no-regression bar
against four separate pushdown mechanisms. Phase 2 is a scheduler and is the
kind of change that touches the contract every operator is written against.

The upside is bounded and known: the curve says a perfect result is ~3.4× on 8
cores where we get 1.66×, i.e. roughly halving total PDS-H time and turning a
0.87 ratio into ~1.5 in Ibex's favour. That is the largest single number
available anywhere in the tree, and it is not reachable in pieces — which is the
argument for doing it, and equally the argument for scoping it properly first.
