# Plans Index

Status of every plan in this directory, grouped by lifecycle. Statuses were
initially re-verified against the source tree on **2026-08-27** (the
kernel-pipeline entry was re-verified again on 2026-08-29); that pass compacted
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
| [beat-polars-plan.md](beat-polars-plan.md) | **Umbrella plan** for the multi-core push (proposed; §8 keeps the baseline record and dead ends). 2026-08-27 update corrected two stale q10 diagnoses (carried group fields optimized during discovery, ~−10.5%; the "36ms serial join build" was inclusive attribution). | Target: implied parallel fraction 44% → 60–65%. Workstreams W1 parallel inner join, W2 aggregate residue, W3 scheduler slice 2, W4 chunked `let` bindings, W5 small-query-tax guard. Points into pipelined-execution + runtime-multithreading + kernel-pipeline for mechanism. |
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
| [null-key-semantics-plan.md](null-key-semantics-plan.md) | **Unverified** — the file carries no status markers | Bug report: operators that build keys from column values mishandle nulls (group-by/distinct/order/join). Verify against the tree before acting |
| [query-shape-conformance-plan.md](query-shape-conformance-plan.md) | **Substantially resolved.** `eb5231c`'s realignment cost +13-16% suite-wide; the total excluding q21 is now at parity (0.995×). Mechanisms 1/2/4/5 done, moot, or closed; Mechanism 3 reverted + low-priority; q22 closed 2026-08-27. | The **scan-fusion cost gate is CLOSED** (re-measured 2026-09-02: fusing wins or is neutral on 21 of 22 at SF-8 — the q04 1.76 / q01 1.24 / q03 1.17 regressions that motivated it are gone or inverted, q04 to 0.77 and q03 to 0.39. Only q13 loses, at 1.10, for a mechanism the model does not contain: its fused `string_filter_scan` does 66% more CPU work than a dense decode + filter. The narrower successor is that one LIKE path, not a gate; note the cross-join expression pushdown reverted 2026-09-02 needs a *different* estimate — cardinality, where this gate keys on column type); Mechanism 4's two `direct_decode_table` task-granularity fixes; Mechanism 1 (footer schemas before lowering, structural); q21 is a known single-query gap (self-join blowup, not filter-pushdown; the only query in the 22 with that shape) — closed as a work item. |

## Reference — descriptive, not a work item

| Document | What it is |
|---|---|
| [joins.md](joins.md) | The join contract: Ibex's join model and the gaps found building the dplyr backend; mapped vs shared-name keys |
| [parallelism-overview.md](parallelism-overview.md) | **Start here before adding a new fan-out.** How multi-core execution works today in parallel-database vocabulary: the `WorkerPool` substrate + two thread budgets, the three parallelism layers, the determinism contract, the full config surface (Part 1, kept verbatim). Part 2 is the inconsistency list (I1–I15, several RESOLVED) + the standing findings: the task scheduler is DROPPED on measurement (pool is ~70% idle with nothing queued), the "70% idle, not serial" accounting, and "Rejected: weakening first-occurrence group ordering". |
| [breaker-map-plan.md](breaker-map-plan.md) | **Where the cores go.** Every pipeline breaker in PDS-H ranked by *idle core-ms* (`self_ms x workers - pool_work_ms`, an identity that closes against `pool_unqueued_ms`) rather than by elapsed time, so a breaker that is internally parallel but starves behind its producer is visible. SF-8/8c: **48.7% of the machine is idle**, ceiling 1.95x. join 44.5% / aggregate 32% / scan 17%; by boundary kind, 63% is `partial` (mechanism can remove it). Top three: semi/anti *build* (4.4k core-ms) -- **DONE**, it was a whole-Table materialization of the right side that nothing needed (q04 -28..-38%, q22 -51..-55%); `Aggregate.Emission` (1.8k) -- **DONE**, fanned out per COLUMN so a 2-column output got 2 workers (q18 -5..-12%); `Aggregate.Discovery` (1.8k) -- **BUILT, only -2%**: the phase label covered four different algorithms and only q01's was actually serial; q01 turns out to be limited by pool contention with its own scan, not by the aggregate. Inner-join left materialization **DONE** (q12 -27%, suite -2.7%): the join copied a 12M-row left side just to learn it was bigger than the 248k right, then chose the orientation the small-right fast path would have chosen for free. Next: aggregate-inside-the-scan-pipeline, then `Aggregate.FinalOrdering` (1.5k). Regenerate with `benchmarking/breaker_map.py`. |
| [phase3-dop-budget-analysis.md](phase3-dop-budget-analysis.md) | Analysis only, nothing built (2026-08-24). Argues the DOP half of kernel-pipeline Phase 3 item 2 is a precondition for unjustified work and the memory half has no consumer — reopen only when a multi-producer change needs it. |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [late-materialize-fd-payload-plan.md](late-materialize-fd-payload-plan.md) | **BUILT + MEASURED 2026-09-03 (uncommitted): q10 -32.8%**, 10/10 paired wins interleaved; fires on q10 ONLY (all 22 plans checked), 1844/1844 tests, 22/22 answers. q10's group-key reduction narrows the key to `c_custkey` but keeps the six FD-determined columns as `First()` aggregates, which pins them to the scan: 4 wide strings decoded from 189MB of Parquet, gathered through 3 joins and 304K agg slots, for a 20-row output. **3.1x the operator work; -34% at 1 core, -39% at 8 (identical top-20)**, and two thirds of it is decode, not the joins. Proposal: re-fetch the payload above the top-k, gated structurally on a `Head` (not on a cost estimate). Also records the REFUTED row-group-count theory and q10's separate parallel-scaling half (joins 2.0x, `Aggregate.Discovery` 1.73x). |
| [radix-partitioned-groupby.md](radix-partitioned-groupby.md) | Noted, not built. High-cardinality group-by is memory-bound; radix partitioning remains a q18/q20 mechanism. Q10 no longer reaches the generic mixed-key ceiling (2026-08-27: FD reduction + discovery-time `First` gathering handle that shape). **But the `First` gathering is itself now the measured q10 cost** — see [late-materialize-fd-payload-plan.md](late-materialize-fd-payload-plan.md): keeping the determined columns as aggregates pins them to the scan, worth 3.1x the operator work. |
| [exists-subquery-plan.md](exists-subquery-plan.md) | Proposal: `exists(table_expr)` as a boolean subquery term — semi/anti/mark joins and the residual-predicate case |
| [in-subquery-plan.md](in-subquery-plan.md) | Proposal: `x in (table_expr)` / `not in` as semi / null-aware anti join — the subquery family, not a scalar like `like()` |
| [extern-series-arguments-plan.md](extern-series-arguments-plan.md) | Proposal: `Series<T>` as a first-class extern argument, starting with CSV null tokens |
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
