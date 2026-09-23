# Plans Index

Status of every plan in this directory, grouped by lifecycle.

**2026-09-23:** the breaker-map arc closed and the *Landed* section was
emptied. Each of its plans was deleted once its rules had moved into code
comments, SPEC.md or a parent plan (see Complete below).
`query-shape-conformance` was compacted to its open leftovers.

**Re-verified against the source tree and git history on 2026-09-04**, at the
point engine work paused. That pass added the four plans written since the
previous one (`cooperative-pipeline-waits`, `physical-fallback-adapter`,
`retire-scan-instance-split`, `row-encoded-groupby`), corrected three entries
that contradicted the tree (`late-materialize-fd-payload` had landed,
`per-occurrence-scan-selections` was fixed rather than proposed,
`null-key-semantics` was complete rather than unverified), and added the
*Landed* section below for plans whose work is done but whose file is still
worth reading. Statuses here are checked against the tree, not copied from each
plan's own header -- all three corrections above were headers that had drifted.

Statuses were initially re-verified against the source tree on **2026-08-27**
(the kernel-pipeline entry was re-verified again on 2026-08-29); that pass compacted
the four largest plans — `parallelism-overview`, `query-shape-conformance`,
`runtime-multithreading`, `kernel-pipeline-execution` — plus
`owned-agg-per-chunk-barrier`, moving their measurement diaries to git history
at the pre-compaction commit's parent). Long-term-but-not-yet-actionable ideas
now live in `../roadmap/`, not here. Completed plans are removed from the tree
rather than archived (see the Complete section for how to find them in git
history).

## Active — open work items

| Plan | Status | What's actually left |
|---|---|---|
| [beat-polars-plan.md](beat-polars-plan.md) | **Ongoing umbrella, rebaselined 2026-09-23** on the last same-sitting SF-8 run (2026-09-04): at 8 cores Ibex is 0.92× Polars on total but 1.08× without q21 and 1.06× on geomean; 0.59× at 1 core; scaling 3.19× vs 4.95× (implied fraction 78.5% vs 91.2%). The August 44%→60–65% target is met. | The single-core lead covers the gap only to ~8 cores; projected to 16 cores Ibex is ~8% behind (28% ex-q21), so the fraction must reach ~81–84%. W0 re-measure 1/2/4/8 plus 12/16 (dev box exploratory, AWS physical cores publishable); W1 audit constants tuned at 8 cores (`IBEX_DECODE_SATURATION`, nested fan-out, row-group width); W2 join (59% of idle); W3 decode width; W4 geomean losers (q14/q15/q16/q10/q19); W5 aggregate residue; W6 chunked `let` bindings. |
| [kernel-pipeline-execution-plan.md](kernel-pipeline-execution-plan.md) | **Phase 2 complete** except `KernelContext` (deliberately unbuilt); Phase 3 handoff/island/raw-thread work complete, with accounting and DOP/memory budgets deferred; Phase 4 construction ownership **and fan-out authority** done (backlog 116→6 breakers, plan describes 97% of real-work nodes). Streaming inner joins have typed `HashBuild`/`HashProbe` nodes and positional `JoinColumnMapping`. Streaming aggregates have positional `AggregateColumnMapping`, authoritative partition/finalize policy, and a typed Discovery → Accumulation → FinalOrdering → Emission hash-fallback chain. The serial coordinator invokes all four nodes through a bounded discovery transfer or explicit fused marker, with independent profile rows. Executor-seam mutations prove mappings, policies, and structural edges are consumed or rejected. Known closed schemas bind during planning; lazy/open schemas bind once at execution. Semi/anti retains its separate streaming operator. Architectural successor: typed logical IR, physical pipelines, morsel executor, templated kernel library; not a JIT. | Next: attach aggregate fan-out policy to each structural node, admit it phase by phase, then split `chunked.cpp` by ownership. |
| [benchmark-perf-priorities.md](benchmark-perf-priorities.md) | Living reference | P0–P2 resolved/landed; rolling min/max optimized. Open: suite trimming (pin sqlite + data.table frollapply cells, duckdb at 3 scales); P4 `tanh` deferred pending accuracy-vs-speed call; P3 ohlc scatter-bound (negative result recorded — don't re-attempt naive fusion); re-check rolling_mean on AWS after the July 2026 regression fix |
| [benchmark-coverage-plan.md](benchmark-coverage-plan.md) | ~95% done | #9 ClickHouse EWMA (needs arrayFold workaround); #10 DataFusion `fill_forward/backward` + `tf_asof_join` |
| [count-window-plan.md](count-window-plan.md) | Implemented (interpreter + codegen) | Per-call count/duration windows work (`__window_n`/`__window_ns` in lower.cpp + window.cpp), and the compiled path (`ibex_compile`) is at parity. Open: `window N rows` block syntax and tuple-field `update` inside `window` (interpreter doesn't support that combo either, so codegen correctly still rejects it). The old monotonic-deque follow-up for `rolling_min`/`rolling_max` is done. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Row-local morsel-parallel pipelines are **ON by default**; Phase 3a is complete; Phase 3b's first source slice landed; Phase 4 items 1–2 landed, item 3 RETIRED (the join gap was Categorical probe keys hashed as *text*, not threading), item 4 part-done. **Nomenclature: `IBEX_THREADS` → `IBEX_CORES`; `IBEX_PARALLEL` removed (serial is `IBEX_CORES=1`).** | The PDS-H multithreading gap is **parallel barriers**, not sources. Next: group-by string/int/generic hash paths + `distinct`; the LazyTable Synchronization Contract (written, unimplemented — Phase 3b's foundation); Phase 2 deterministic RNG (designed, not started). Re-measure at a larger scale before ranking — the threading share of a gap grows with row count. |
| [join-perf-plan.md](join-perf-plan.md) | Items 1–3 done (2026-07-14; q09 −23%, q13 −30%) | Join/group-by performance findings; see the file's Results section. beat-polars points here for join mechanism |
| [owned-agg-per-chunk-barrier-plan.md](owned-agg-per-chunk-barrier-plan.md) | High-cardinality partition-owned aggregation vs Polars streaming. q18 (single-Int64 `Sum`) largely closed by the async hot/cold rewrite (**−33%**); the parallel finalize merge landed for all owned paths (**−7%**); q21's ordered-run `Count` finalize + emit fusion landed (**−11.2% SF-4**). | q20/`PairIntKey` (the hot table doesn't help a scattered composite key — a per-partition `CardinalitySketch` is the candidate); q21's remaining wall is the per-chunk accumulate orchestration + a duplicate lineitem decode + a 40ms serial hash-join build. **Do NOT touch `part_count` or serial-`reserve` the maps** — both measured dead ends. |
| [grouped-chunkview-update-plan.md](grouped-chunkview-update-plan.md) | Mostly complete — `update …, by k` runs off an immutable `GroupedRowPlan` (CSR) instead of gather → per-group `Table` → scatter. Sub-plan of kernel-pipeline Phase 2. | Remaining materialized shapes: `rank`, variable-width ordered state, `window`-clause `lag`/`lead`. |
| [per-occurrence-scan-selections-plan.md](per-occurrence-scan-selections-plan.md) | **Phases 1–3 LANDED** (`78a09fad`, `bf783ef3`, `f2b298db`). Restored filter pushdown for a source scanned more than once: each occurrence is renamed `source#fN` so `scan_predicates` keeps its predicate, `decode_demanded_lazy_sources` decodes the union of their output columns ONCE and gathers per occurrence, and the instances stay EAGER. Gated structurally on a fusable `like`. `ibex-e2e.sh` is green again. | **Phase 4 — narrow the `!= 1` gate generally.** Its price was +11.3% on q21, since the eager selection ran serial; with that fanned out (`f06e6da3`) widening the gate measures −0.4% geomean, byte-identical on 22, nothing regressed. The blocker is gone, so what is left is a risk judgement about the plan-shape change, not a cost one. |
| [query-shape-conformance-plan.md](query-shape-conformance-plan.md) | **Compacted 2026-09-23.** The investigation is closed: excluding q21 the suite is at parity (0.995×), the scan-fusion cost gate is closed, and q21 is a known single-query gap (a self-join rewrite, not pushdown). The file now holds only the leftovers and the do-not-repeat list. | q13's fused non-anchored LIKE scan (66% more CPU than dense decode + filter); row-group task granularity in `direct_decode_table` (q15); a row-count-ratio deferred-probe gate (low priority, re-survey first). |

## Reference — descriptive, not a work item

| Document | What it is |
|---|---|
| [joins.md](joins.md) | The join contract: Ibex's join model and the gaps found building the dplyr backend; mapped vs shared-name keys |
| [parallelism-overview.md](parallelism-overview.md) | **Start here before adding a new fan-out.** How multi-core execution works today in parallel-database vocabulary: the `WorkerPool` substrate + two thread budgets, the three parallelism layers, the determinism contract, the full config surface (Part 1, kept verbatim). Part 2 is the inconsistency list (I1–I15, several RESOLVED) + the standing findings: the task scheduler is DROPPED on measurement (pool is ~70% idle with nothing queued), the "70% idle, not serial" accounting, and "Rejected: weakening first-occurrence group ordering". |
| [phase3-dop-budget-analysis.md](phase3-dop-budget-analysis.md) | Analysis only, nothing built (2026-08-24). Argues the DOP half of kernel-pipeline Phase 3 item 2 is a precondition for unjustified work and the memory half has no consumer — reopen only when a multi-producer change needs it. |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [radix-partitioned-groupby.md](radix-partitioned-groupby.md) | Noted, not built. High-cardinality group-by is memory-bound; radix partitioning remains a q18/q20 mechanism. Q10 no longer reaches the generic mixed-key ceiling (2026-08-27: FD reduction + discovery-time `First` gathering handle that shape). **But the `First` gathering was itself the measured q10 cost** — late-materialize-fd-payload (retired, see Complete) LANDED (`568c4974`, q10 −32.8%) and lifts that payload above the top-k. |
| [exists-subquery-plan.md](exists-subquery-plan.md) | Proposal: `exists(table_expr)` as a boolean subquery term — semi/anti/mark joins and the residual-predicate case |
| [in-subquery-plan.md](in-subquery-plan.md) | Proposal: `x in (table_expr)` / `not in` as semi / null-aware anti join — the subquery family, not a scalar like `like()` |
| [extern-series-arguments-plan.md](extern-series-arguments-plan.md) | Proposal: `Series<T>` as a first-class extern argument, starting with CSV null tokens |
| [ibex-compile-conformance-plan.md](ibex-compile-conformance-plan.md) | **Umbrella.** Close the drift between `ibex_compile` (the C++ transpiler) and the interpreter — the surfaces have diverged (map, non-literal extern args, table-returning `fn`, `model {}`, window combos) and the parity harness is an allowlist that hides it. Step 0 upgrades parity to a conformance gate. Key reframe: the emitter emits only `ibex::ops::*` plan-reconstruction, never a native loop — but that's historical, not required. `map` is a `for` loop; **W1a** = `MapNode` + the emitter emitting an actual loop (with `read_csv`/`write_parquet` as literal C++), zero runtime changes. W1b (runtime expression-level extern evaluator, for the interpreter half) is deferred as low-value. Then W2 non-literal extern args, W3 user functions on whole-script/transpile, W4 `model {}`, W5 window combos. `ibex_compile` and the interpreter share all runtime kernels — the gap is plan-construction + expression-eval only. |
| [project_http_plugin_plan.md](project_http_plugin_plan.md) | HTTP plugin MVP: simple registration API, blocking server, no decorators yet |

## Moved to `../roadmap/`

Long-term intentions we want but that are not yet actionable work:
`julia-integration-plan.md` (Ibex.jl package, `ibex"""..."""` macro, Tables.jl
interop) and `short-mode-plan.md` (prefix-abbreviated golf mode). Relocated
2026-08-27 so `plans/` holds only things that can be picked up now.

## Complete — removed from the tree

Completed plans are no longer kept in `plans/` (removed 2026-08-22 for
focus; the last set lived under `plans/done/`). They are fully recoverable
from git history — `git log --diff-filter=D --name-only -- plans/done/`
lists them, and the removal commit's parent still has every file. Citations
in active plans to `plans/done/...` paths refer to that history.

- **parallel-chunkview-output-plan.md** — removed 2026-09-02, all five delivery
  items landed (it had been mislabelled "proposed" while its whole protocol was
  already in the tree). Its two durable rules moved into the code they govern:
  the categorical dictionary/ownership and determinism contract now heads
  `DirectCategoricalPlan` in `src/runtime/kernel_update.hpp`, and the
  all-or-nothing consequence — one field `plan_direct_field` cannot name
  serialises every *other* field in that update node, so rank new families by
  what shares their node rather than by how hot the expression is — heads
  `update_row_local_chunk` in `kernel_update.cpp`, next to the
  "do not widen `is_chunk_predicate_native`/`is_range_native_expr`" rule on
  `try_plan_direct_like_int_field`.

- **Retired 2026-09-23**, all landed or deliberately reverted. Their rules
  moved to where they apply; read the files at `git show 81392162:plans/<name>`.
  - **breaker-map-plan.md** — every PDS-H pipeline breaker ranked by idle
    core-ms; all items done or sized and dropped. Regenerate the map with
    `benchmarking/breaker_map.py`.
  - **retire-scan-instance-split.md** — a repeated scan is decoded once and
    shared, and FD identity moved to `ColumnOrigin::scan`. Why the `source#k`
    rename still can't go is on `isolate_deferrable_probe_scans`
    (`include/ibex/ir/scan_predicates.hpp`).
  - **physical-fallback-adapter-plan.md** — one `build_materialized_fallback`
    → `interpret_node` seam. The allowlist and pre-build rationale are on
    `fallback_relational_inputs` in `runtime_entry.cpp`. Its two leftover
    cleanups are now kernel-pipeline Phase 5 item 3.
  - **cooperative-pipeline-waits-plan.md** — work-conserving ring waits make
    nested fan-out safe. Both gates are explained where they live
    (`g_submit_gen` in `worker_pool.cpp`, `cooperative_ring_wait` in
    `pipeline_executor.cpp`), and so is the step-D survey that left the other
    serial fallbacks alone.
  - **null-key-semantics-plan.md** — the decided semantics are now SPEC.md §3.5
    "Null Keys", and `tests/test_null_keys.cpp` plus
    `tests/data/null_keys_check.ibex` cover them.
  - **late-materialize-fd-payload-plan.md** — q10 −32.8% (`568c4974`).
  - **row-encoded-groupby-plan.md** — reverted, q10 +26% from decode bandwidth
    contention. The third q10 group-by dead end; do not try a fourth without
    reading it.

## Cross-plan dependency notes

- **function-kind-registry** is complete and is now the dispatch foundation for
  non-row-local-filter follow-ups: "contains a Transform/Generator → evaluate
  vectorised" is the rule to reuse for rolling/rank in filter.
- **exprvalue-null-arm** completes the null/validity follow-up left by
  function-kind-registry: row-local null handlers are scalar again, while
  genuinely ordered functions (`lag`, `rolling_*`, `fill_forward/backward`) stay
  Transform.
- **count-window** can still benefit from function-kind metadata if codegen
  stops delegating rolling calls through `interpret()`.
- The extern chunked-source contract (formerly chunked-execution §2,
  removed from the tree 2026-08-22) is the gateway to the
  ADBC/pushdown stages in the execution roadmap (memory:
  project_execution_roadmap); it is now tracked in kernel-pipeline Phase 0/2
  and bigger-than-ram Phase 4.
- **pipelined-execution** now supplies the first source-to-breaker and
  join-output overlap on top of the chunked substrate. It deliberately retains
  whole-query `LazyTable` pushdowns; its next work is progress-aware admission
  and general scheduling, not another static scan gate.
- **runtime-multithreading** is the answer to the remaining polars
  multi-thread gaps in **benchmark-perf-priorities** (single-thread ibex
  already wins 37/41 vs polars-st). Its execution-plan seam and first parallel
  islands are complete. Phase 3a now deliberately precedes parallel I/O:
  promote Parquet to a first-party backend and make Ibex storage adopt
  Arrow-compatible buffers, so Python/R and source morsels share ownership
  rather than marshal. First-party Parquet and the independent reader-product
  factory and R's nanoarrow export-lease ownership protocol have landed, which
  completes Phase 3a. Phase 3b now implements the LazyTable synchronization
  contract and parallel decode; Phase 4 follows with aggregate/join/sort barriers.
- **bigger-than-ram** built directly on the removed **chunked-execution**:
  every "materializing" row in that plan's coverage table (unsorted `Order`/
  `AsTimeframe`, non-streaming `Tail`, general `Join`) is a target phase
  here — that breaker list is now kernel-pipeline Phase 4/5's — and the
  extern-source contract hardening and this plan's Phase 4 (chunked Parquet)
  are the same work from two angles. Its Phase 7 (parallel spill I/O) is explicitly sequenced after
  **runtime-multithreading**, not coupled to it.
