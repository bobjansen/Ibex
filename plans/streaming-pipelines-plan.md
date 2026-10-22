# Streaming pipelines: one unit through the whole pipeline on one core

**Status: PROPOSED (2026-10-20).** Umbrella: `beat-both-plan.md` (the 8-core
gap). Read `src/runtime/PARALLELISM.md` and `src/runtime/CONTRACTS.md` first;
this plan changes *where* work runs, not the kernels or the determinism devices.

## Why

AWS counters (r7i.8xlarge, SF-10, 2026-10-17/18, `run-tpch.sh --counters`) on
q01/q05/q09/q21: no engine multiplies instructions; Ibex's 8-core loss is ~70%
idle cores and ~30% slower cycles. The extra cycles are L2-miss stalls, and the
cost per L2 line grows from 1 to 8 cores **for Ibex only** (q05 9.3 → 12.7
stall cycles per line, q09 9.8 → 13.3; Polars and DuckDB flat). Traffic per
row is not higher except on q01. Same data, more expensive misses: the shape of
data written on one core and read on another, and of one fork/join barrier per
operator per chunk.

Today a streamed unit (up to 1M rows) is decoded and run through the row-local
chain on a scan-pipeline worker, then handed to the calling thread, where every
following operator fans the whole chunk out over all workers and joins again.
Deferred-probe joins do not stream at all: key scan of the whole relation,
probe, selected decode of the whole relation, one output table (q09: one
2,610,723-row chunk through three joins).

q09, SF-8, 8 cores, local (`IBEX_PROFILE_OPERATORS`): wall 485 ms, pool work
3066 ms (383 ms perfectly packed), occupancy 0.79, 45 barriers with 384 ms of
main-thread barrier wait. `source decode selected` alone is 1655 ms of pool
work, `dynamic key scan` 679 ms; every join emits one chunk.

Where the extra core-time goes (`benchmarking/tpch/cpu_growth.py`, SF-8,
local WSL, `cpu-clock` samples, warm, 1 core vs 8 one-per-core; confirm on
native Linux with `cycles`): libzstd, ~2/3 of all CPU, is flat (q05 ×0.98,
q09 ×1.05). The growth is in Ibex's own code (q05 ×1.23, q09 ×1.42), by
family on q09: join probe ×1.50 (+1029 samples), join build plus its serial
inlined form ×1.56 (+667), aggregate ×1.71, gather/concat ×1.76, update ×3.5,
Ibex-side Parquet decode ×1.19. The growing families are the ones that touch
data another core wrote (probe, gather, aggregate) or run a parallel variant
with an extra pass (partitioned build, swapped probe's scan-and-replay).
Running a whole unit on one worker uses the serial variants and keeps the data
on that core, so this is the share a streaming pipeline can remove; zstd's
share it cannot.

## Target

Morsel-driven pipelines (Umbra, DuckDB, Polars streaming; `CONTRACTS.md` §0
already names Umbra as the model): a pipeline runs from a source unit to the
next breaker, and **one worker takes one unit through every operator of the
pipeline**, in row-group-sized slices, ending in a per-worker sink. Breakers:
hash build, aggregate merge, order, distinct, window, materialized fallbacks.

## What exists

- Per-worker private row-local chains over a decode unit
  (`build_scan_pipeline_worker`, `PipelinedScanOperator`).
- A per-worker aggregate on the producing core: `ScanWorkerSink` /
  `DenseAggregateScanSink` (dense categorical only; q01 −30% at 8c on AWS).
- A probe at the head of a morsel chain with the maps above it fused
  (`35e6108a`, `3172e533`), opt-in `IBEX_PROBE_MORSELS`: Layer B, over a
  materialized (drained) probe side; measured a loss on its own (q09 +6.8%) and it deadlocks
  q09 at SF-8 (below).
- Immutable `JoinHashIndex` after `run_build`; build-before-probe is an explicit
  call.
- Ordered rings and unit `sequence` numbers: merging per-unit results in unit
  order is what keeps row order identical.

## The `IBEX_PROBE_MORSELS` deadlock (diagnosed 2026-10-20, not fixed)

SF-8 q09, 8 cores, hangs at 0% CPU (SF-1 completes). Stacks: all 8 pool
workers park in `OrderedChunkRing::acquire` (the probe-morsel ring is full);
a `PipelinedStageOperator` thread is inside `JoinProbe::probe_unique_ranges`,
waiting on a pool batch it submitted; the stage thread feeding it waits for that
consumer to take its chunk. The workers would have to run the stage thread's
queued probe tasks to break the cycle, but `cooperative_ring_wait` only assists
when the waiting worker has nested work of its own
(`this_thread_has_outstanding_nested_work`, `3485c684`, added after measured
spin and nesting failures). Loosening that gate is the wrong fix: the shape
breaks the rule that long-lived blocking occupants do not belong in the pool,
and it morselizes a materialized table (a §5 dead end of `beat-both-plan.md`).
**Decision:** do not fix; slice 1 supersedes the path, and slice 5 deletes it.

## Slices

Each slice: the same answers 22/22 at 1 and 8 cores, rows and order exact,
floats within tolerance (`src/runtime/CONTRACTS.md` §7), then AWS. A slice
lands on an interleaved A/B on the native box at 8 P-cores (signed-rank per
query; `MEASURING.md`): a net gain in total 8-core wall, and no query with a
significant regression. An enabling step that is wall-neutral but cuts idle or
CPI (`perf_counters.py`) may land only when the next slice depends on it, and
says so.

1. **Probe steps in the scan-pipeline worker chain.** A streaming inner probe
   (single-key and pair-int, against an already-built index) becomes a step of
   the worker chain, so decode → filter → probe → maps above run on one core per
   unit, emitting per-unit chunks in unit order (the same probe-order-major
   rows the deferred path emits today, concatenated). Includes the deferred
   probe per unit: key decode, probe, selected decode of that unit only.
   **Central design question:** a chain of probes needs every build side
   complete before the probe pipeline starts. q09's `orders` is today a
   deferred probe *filtered by* lineitem's surviving keys; chaining it means
   building `orders` in full (as DuckDB does). That is a planner decision
   (`feedback`: the engine owns join order and sides), so slice 1 starts with
   chains whose later build sides are already complete tables, and states the
   rule it uses. Without the downstream probes in the chain, per-unit chunks
   only multiply per-chunk fork/joins downstream (the q01 lesson), so the first
   measured shape must include at least two chained probes: q05 or q09.
   **First cut built 2026-10-20, opt-in `IBEX_STREAM_PROBES=1`
   (`ExecutionContext::stream_probes`).** What exists:
   - `LazyTable::join_key_selection` / `project_rows` take a unit; a unit with
     no fused answer falls back to its key and conjunct columns without the
     membership filter (`unit_key_selection_fallback`; the probe is exact).
   - `PipelinedScanOperator` runs either a scan decode or a `UnitProducer`
     per unit (`build_unit_pipeline`), accepts worker steps before it starts
     (`try_attach_worker_step`), and with no pool worker to spare runs every
     unit on the calling thread, so chunks never depend on core count.
   - Single-key deferred probes take `Mode::Streamed`
     (`try_stream_two_phase` → `run_streamed_two_phase_unit`); a join that
     builds on its right side attaches its probe to a streaming left
     (`build_hash_probe_operator`). Attached steps get the worker's context, so
     they never fan out from inside a pipeline (the deadlock above).
   - Output is batched at the ordered emit to 1M rows: per-unit chunks made
     q16's `distinct` (per-chunk floor 32768) run serially, +95%.

   Measured (local WSL, SF-8, 8 cores, warm, 3 interleaved rounds): answers
   22/22 pass, 1c == 8c under streaming for every query (q08 differs from the
   non-streamed answer in the last bits: per-unit sums; q09 keeps its known
   exception). Engages on 12 queries; q03 −5.5%, q11 −4%, q17 −3%, q18 −2%,
   q09 +3%, total −0.6%, geomean −1.3%. Small, because the chains still
   break: two-key deferred joins (q05 `supplier`, q09 `lineitem`) drain the
   stream into a table (`resolve_deferred_probe_pair`), and the 1M-row
   batching hands the breakers above DRAM-sized chunks again.

   **Two-key joins and moderate right sides (2026-10-20, same flag).** A
   two-key deferred probe whose left drained (q09 `lineitem`) runs per unit:
   the left is indexed once and each unit decodes with the scan's pushdowns
   and probes in scan order (`run_streamed_swapped_pair_unit`,
   `emit_swapped_pair(.., share_left)` so the left is not copied per unit). A
   join whose left is an unstarted streaming pipeline and whose right is at
   most 1M rows indexes the right without draining the left and attaches its
   probe (`stream_left_into_right_build`; q05 `supplier`, single-key joins
   too). `ParallelPipelineStats::streamed_probes` counts both kinds; q05 now
   streams 4 probes, q09 3. q09's chain still ends at `orders` (12M rows, over
   the 1M right-build limit), whose deferred probe drains the stream to filter
   `orders` by lineitem's keys: the build-in-full question above, unanswered.

   Measured (same setup, a slower sitting; ratios only): 22/22 answers, 1c ==
   8c under streaming except q09's known exception; q03 −4%, q05 −3%, q11 −3%,
   q10 −2%, q08/q18 +1%, total −1.0%, geomean −1.2%. On WSL the wall barely
   moves because zstd, ~2/3 of the CPU, is untouched; the expected gain is in
   1→8-core CPI and idle, which needs native Linux counters
   (`cpu_growth.py`, `perf_counters.py --engines ibex,ibex:stream
   --variant-env ibex:stream=IBEX_STREAM_PROBES=1`) and then AWS.

   Owed before the flag can default on: tests that fail without it (a
   multi-unit Parquet deferred probe, single- and two-key, asserting
   `streamed_probes` and 1c == 8c), and the REPL statement / `ibex_compile`
   paths checked (`feedback`: the three paths are equals).
2. **Generic per-worker aggregate sink.** Hash, owned and partitioned paths as
   per-unit partials merged in unit order (first-occurrence group order holds).
   The largest slice (`aggregate_chunked.cpp`, 9.5k lines).
3. **Hash build as a sink.** Build input streams into per-worker partitions
   instead of materialize-then-partition (q09, q18, q21).
   **Measured first (2026-10-21, native, 8 P-cores, `perf` 20 ms busy-core
   timelines):** the partitioned build is ~4% of cycles on q03/q09, in 20-50 ms
   windows already 5-7 cores busy, and q18/q21 have no inner build worth
   speaking of -- per-worker partitioning can buy ~5% at most. The serial
   time was the "materialize" half: q09's drain of the streamed probe output
   ran ~60 ms on one core, mostly the unit pipeline's 1M-row batching copy on
   its stage thread. Fixed by unbatching drained pipelines (`8854cd63`:
   streamed q07 -10.5%, q09 -7.2%). Copying the drain over the pool instead
   gained nothing and lost the append's overlap with the producer (q19, q14
   regressed). q21's largest hole (~180 ms at one core, 8c) was not a join
   either: two serial zero-fills of aggregate scratch the parallel passes
   overwrite (`gids_buf_` for a 60M-row chunk, the run-compressed key/count
   arrays); not zero-filling them was q21 -15% (`7dcadc5b`). A hunt for more
   (2026-10-21: page faults by thread over all 22 queries, both modes) found
   none worth it. The main thread's large faulters are the drain's
   chunk-by-chunk append (overlapped with the producer; see above) and
   decode. The join build's `chain_next` fill and the two-phase probe's
   `li`/`ri` zero-fills (~20 MB a query) were made unwritten, proven safe by
   poisoning, and measured twice: ±2-3% per query that flipped sign between
   runs, with a significant regression each time (q20). Not landed.
   **What chunk-taking consumers could save (2026-10-22, 8 P-cores, temporary
   trace in `materialize_operator`, 6 reps).** The work left after the last
   chunk arrives is <0.3 ms on every drain of every query. The upper bound is
   instead what appending inline costs the producer: drain time minus the
   producer's time to EOF when its chunks are only moved into a vector. That
   is ≤10 ms on every drain except three: q13's 14.8M-row `orders` drain into
   the materialized left join (+65-72 ms of ~580; producer 310 ms, append
   ~180), q22's 1.5M-row `customer` drain in `build_materialized_map_run`,
   which re-morselizes it (+29-33 ms of ~160: the 33 ms append outlasts the
   45 ms producer), and q09's streamed 3.3M-row drain in `initialize` (+13
   ms). About 150 ms in all, out of ~11 s total. Collecting first and
   concatenating afterwards was slower on every query, so the inline append is
   mostly hidden.
4. **Semi/anti joins as probe steps** (q04, q21, q22).
5. **Retire Layer B probe morsels** (`try_take_join_probe`,
   `build_probe_morsel_pipeline`, `IBEX_PROBE_MORSELS`), and rewrite
   `PARALLELISM.md`'s layer model.

## Constraints from history (each was measured)

- Work runs on the core that produced the data: the async aggregate task queue
  cost q01 +27% by re-reading chunks cache-cold on another core.
- Never materialize a stream in order to split it: start the pipeline at the
  producer's units and take its chunks. Probe morsels drained the streamed
  probe side and re-split it (q09 +6.8%). A table a breaker produced anyway
  (sort, aggregate output, a materialized join side) is morselized when the
  work above it pays for the split (`is_worth_morselizing`); keeping a long
  pipeline after a breaker serial is the worse loss. The split's copy is not
  what costs: `make_morsel_chunk` copies each range element by element, and a
  bulk `memcpy` copy measured null over all 22 queries (2026-10-22, same
  binary, 9 rounds, 8 cores: totals -0.14% / +0.12%). Each worker copies its
  own range while it is cache-hot, in parallel.
- A ring costs when its consumer drains synchronously (Ascribe as pipelineable,
  +13–30%). The only ring left is at the pipeline's sink.
- Keep scan/aggregate overlap: removing it cost q01 +44%, q15 +61%.
- Row-group slices do not pay for the streamed joins (2026-10-20, native
  i7-13700, SF-10, 3.2 GHz locked, 8-core medians): running the two-phase and
  two-key unit pipelines over row groups instead of 1M-row units (q09: 555
  pieces instead of 95, output still batched to 1M rows) was slower on every
  query (q03 405 → 422 ms, q05 448 → 465, q09 723 → 780, q10 517 → 528;
  1 core +4–8%) and raised q09's L3 misses (16.1M → 20.1M). Per-piece setup
  (key scan, scan plan, reader, probe) costs more than the cache gains.
  q09's two-key build side is 43.6 MB (left 13.9 MB, `chain_next` 3.5 MB,
  `pair_heads` 26.2 MB at 41% load), but halving `pair_heads` (95% load cap:
  524,288 slots, 13.1 MB) cut q09's streamed L3 misses only 7% and moved no
  wall time, so the source of the streamed path's extra misses (1 core:
  ~15M against ~4M whole-source) is still unidentified.
- Overlapping a join's two sides regressed twice (`32889afd`, `27cb4a27`):
  builds run before their probe pipeline, not beside it.

## Risks

- Determinism at the merges (slices 2–3): solvable the way the dense sink does
  it, merging in unit order.
- q21 is partly a shape gap (canonical self-join at line granularity); this
  helps its occupancy, not its shape.
- Build sides that are large and today filtered by the probe side (q09
  `orders`) may cost more to build in full than the pipelining saves; slice 1
  measures this before generalizing.
