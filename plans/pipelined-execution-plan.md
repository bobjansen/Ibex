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

### Phase 0 — make multi-chunk real, and prove it correct

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

### Phase 1 — a streaming scan that keeps its pushdowns

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

### Phase 2 — concurrent chunks

The actual win. Multiple chunks in flight through the operator chain.

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
