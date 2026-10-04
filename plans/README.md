# Plans Index

Status of every plan in this directory, grouped by lifecycle.

**2026-10-04:** eight finished or overtaken plans retired (ADBC, opaque
resources, namespaces, parse-args/nullable scalars, count windows, join perf,
benchmark coverage, benchmark perf priorities), plus the exists plan; see
Complete. Their open leftovers are listed there.

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
| [beat-both-plan.md](beat-both-plan.md) | **Ongoing umbrella, created 2026-10-04** by merging beat-polars and beat-duckdb. References: Polars **streaming** and DuckDB (never Polars in-memory). Ibex leads both on 1 core and loses from ~6–8 cores up; the gap is serial time that stays flat with cores (39% of wall at 16). The benchmark write-up waits for milestone 1. | Milestone 1: total and geomean ≤ 1.0 against both at 8 cores on AWS SF-10 (1 core still ahead, 2 cores not a loss). Baseline §1.0 pending (AWS run `20261004T100951_1bfeceb5`), then re-rank §3 by ms lost against the faster reference; provisional order q10 → deferred-probe joins (q03/q05/q07/q09) → q01 → q12/q14/q15/q19/q04 → q16/q13 → scale-cliff sweep → canonical-plan audit. |
| [kernel-pipeline-execution-plan.md](kernel-pipeline-execution-plan.md) | **Phase 2 complete** except `KernelContext` (deliberately unbuilt); Phase 3 handoff/island/raw-thread work complete, with accounting and DOP/memory budgets deferred; Phase 4 construction ownership **and fan-out authority** done (backlog 116→6 breakers, plan describes 97% of real-work nodes). Streaming inner joins have typed `HashBuild`/`HashProbe` nodes and positional `JoinColumnMapping`. Streaming aggregates have positional `AggregateColumnMapping`, authoritative partition/finalize policy, and a typed Discovery → Accumulation → FinalOrdering → Emission hash-fallback chain. The serial coordinator invokes all four nodes through a bounded discovery transfer or explicit fused marker, with independent profile rows. Executor-seam mutations prove mappings, policies, and structural edges are consumed or rejected. Known closed schemas bind during planning; lazy/open schemas bind once at execution. Semi/anti retains its separate streaming operator. Architectural successor: typed logical IR, physical pipelines, morsel executor, templated kernel library; not a JIT. | Next: attach aggregate fan-out policy to each structural node, admit it phase by phase, then split `chunked.cpp` by ownership. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Row-local morsel-parallel pipelines are **ON by default**; Phase 3a is complete; Phase 3b's first source slice landed; Phase 4 items 1–2 landed, item 3 RETIRED (the join gap was Categorical probe keys hashed as *text*, not threading), item 4 part-done. **Nomenclature: `IBEX_THREADS` → `IBEX_CORES`; `IBEX_PARALLEL` removed (serial is `IBEX_CORES=1`).** | The PDS-H multithreading gap is **parallel barriers**, not sources. Next: group-by string/int/generic hash paths + `distinct`; the LazyTable Synchronization Contract (written, unimplemented — Phase 3b's foundation); Phase 2 deterministic RNG (designed, not started). Re-measure at a larger scale before ranking — the threading share of a gap grows with row count. |
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
| [allocator-and-huge-pages.md](allocator-and-huge-pages.md) | Analysis only, parked (2026-09-24). Ibex gains ~38% from a warm process on q21 against Polars' ~8% (first-touch page faults), so q21 is ~0.43 warm but ~0.63 fresh. jemalloc: no (+3.7% at 1 core). Whole-heap huge pages via `GLIBC_TUNABLES=glibc.malloc.hugetlb=1`: −8.0% / −4.9% fresh at 1 / 8 cores, neutral warm. Column-buffer-only `madvise` tried and reverted (−0.6% / −1.8%). Reopen if single command-line runs matter. |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [radix-partitioned-groupby.md](radix-partitioned-groupby.md) | Noted, not built. High-cardinality group-by is memory-bound; radix partitioning remains a q18/q20 mechanism. Q10 no longer reaches the generic mixed-key ceiling (2026-08-27: FD reduction + discovery-time `First` gathering handle that shape). **But the `First` gathering was itself the measured q10 cost** — late-materialize-fd-payload (retired, see Complete) LANDED (`568c4974`, q10 −32.8%) and lifts that payload above the top-k. |
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

- **beat-polars-plan.md** and **beat-duckdb-plan.md** — merged 2026-10-04 into
  `beat-both-plan.md` (one target: both references at 8 cores). Full text at
  `git show 7a32d537:plans/<name>`, including the dev-box sweeps, the W0
  measurement log and the q14 bandwidth study.

- **Retired 2026-10-04.** Read any of them at `git show 0d069262:plans/<name>`.
  - **adbc-plan.md** — ADBC library work is done: import types, `adbc::write` /
    `execute`, bound parameters, reusable connections, discovery, DuckDB and
    macOS CI, SPEC + `docs/io.html`. Left: bump the driver manager and wheels
    to ADBC 25 when released (apache/arrow-adbc#4695, then try dropping the
    `SELECT 1` in `adbc_begin`); drop the DuckDB ingest quirk once
    duckdb/duckdb#26425 ships. Not started by decision: SQL Server (needs
    filter pushdown into the query SQL first), then Flight SQL; a scalar
    argument that calls a connection function (bind it with `let`).
  - **opaque-resource-lifetime-plan.md** — typed opaque resources: top-level,
    as `fn` parameters/returns, and in `ibex_compile` (W6, 2026-10-02). Its
    design rules — close is not `consume`; a busy connection errors rather than
    buffering, waiting or opening a second one; resource operations run in
    source order and are never eliminated, duplicated, commoned or
    parallelized — are in the retired file's "Mapping and effects".
  - **namespaces-plan.md** — implemented 2026-10-03; SPEC.md §11.7. Aliases
    (`using c = csv;`) and namespaced built-ins were left unproposed.
  - **parse-args-and-nullable-scalars-plan.md** — null scalars (SPEC.md §6.7)
    and `parse_args` (`libs/args/`) complete 2026-09-07. Left: codegen does not
    emit a null scalar binding (noted at both sites in `emitter.{hpp,cpp}`).
  - **count-window-plan.md** — per-call count/duration windows
    (`rolling_mean(px, 20)`, `rolling_mean(px, 60s)`) in the interpreter and
    `ibex_compile`. Left: `window N rows` block syntax, and tuple-field
    `update` inside `window` (the interpreter rejects it too).
  - **join-perf-plan.md** — items 1–3 landed (q09 −23%, q13 −30%); item 4 was
    overtaken by beat-polars (now `beat-both-plan.md`). Measured and rejected, do not re-run: a native
    Parquet encoding decoder, a decode arena, mmap; and do not "just link
    jemalloc" (`tune_allocator_once` already tunes glibc).
  - **benchmark-coverage-plan.md** — ~95% done. Left: ClickHouse EWMA (needs an
    `arrayFold` workaround); DataFusion `fill_forward` / `fill_backward` and
    `tf_asof_join`.
  - **benchmark-perf-priorities.md** — P0 (the allocation cliff) resolved, see
    `tune_allocator_once`; suite trimming built into `run_scale_suite.sh`.
    Negative result: fusing `ohlc_by_symbol`'s four reducers into one pass
    does not help — with ~252 groups it is bound by the scatter writes, not
    bandwidth; the levers are group-id locality or SIMD/threaded scatter. Left:
    P4 `tanh` (an accuracy-vs-speed call).

- **exists-subquery-plan.md** — retired 2026-10-04. Tier 1 (a whole `exists`
  conjunct → semi / anti join, `cdff12fa`) and Tier 2 (anywhere else → mark
  join, `ef1594cd`) are built; the semantics are SPEC.md §5.8 and the lowering
  rules head `lower_exists` / `mark_exists` in `src/parser/lower.cpp`. Tier 3 —
  a non-equality capture (q21's `l_suppkey <> outer(l_suppkey)`) as equijoin +
  residual filter + dedup on a row identity — is unbuilt; its design is at
  `git show 2d064b83:plans/exists-subquery-plan.md`. No query needs it (q21
  ships as a hand-written rewrite).

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
    → `interpret_node` seam. Its input allowlist was replaced 2026-10-04 by
    `materialize_input` (`interpreter.cpp`): every input of a fallback node
    goes back through `build_operator`. Removing `interpret_node` entirely is
    in progress.
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
  multi-thread gaps (single-thread ibex already won 37/41 vs polars-st in the
  retired benchmark-perf-priorities survey). Its execution-plan seam and first parallel
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
