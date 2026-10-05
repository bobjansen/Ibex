# Plans Index

Status of every plan in this directory, grouped by lifecycle.

**2026-10-05:** retired the radix group-by note, the owned aggregate,
runtime multithreading, the Phase 3 DOP-budget analysis, the kernel-pipeline
migration (finished), the compile-conformance umbrella (finished), the
query-shape conformance leftovers (moved into beat-both), the per-occurrence
scan selections (built; widening deferred), and the three
reference documents (`joins.md`, `parallelism-overview.md`,
`allocator-and-huge-pages.md`), whose rules moved to SPEC.md, `MEASURING.md`,
`src/runtime/PARALLELISM.md` and the code; the rows below re-checked against
the tree. `plans/` holds plans; documentation lives in
SPEC.md, `docs/`, `src/**/*.md` and code comments.

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
| [beat-both-plan.md](beat-both-plan.md) | **Ongoing umbrella, created 2026-10-04** by merging beat-polars and beat-duckdb. References: Polars **streaming** and DuckDB (never Polars in-memory). Baseline §1.0 (AWS `20261004T100951_1bfeceb5`, 8 cores): Ibex/Polars 0.94 total, geomean 1.00 against both; Ibex leads on 1–4 cores and loses at 16. The benchmark write-up waits for milestone 1. | Milestone 1: total and geomean ≤ 1.0 against both at 8 cores on AWS SF-10 (1 core still ahead, 2 cores not a loss). Landed since the baseline: consumer helps the scan (`49da7a27`, lineitem scan family −4 to −7% at 8 cores) and the left-scan join filter (`c4084d23`, q12 −35%); AWS 2026-10-05 (§1.0b) meets milestone 1 at 8 cores (0.91 / 0.99 total, 0.95 / 0.96 geomean; without q21 0.98 / 1.00) confirmed by two more passes on a second box (ratios identical to the second decimal); against DuckDB without q21 it is a tie (0.996), so ~300 ms more is wanted for a robust claim. Top 8-core losers now q19, q01, q03, q10, q14; milestone 2 (16 cores) unchanged at 1.08 / 1.21. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [grouped-chunkview-update-plan.md](grouped-chunkview-update-plan.md) | Mostly complete — `update …, by k` runs off an immutable `GroupedRowPlan` (CSR) instead of gather → per-group `Table` → scatter. Began as a sub-plan of the (retired) kernel-pipeline plan's Phase 2. | Remaining materialized shapes: `rank`, variable-width ordered state, `window`-clause `lag`/`lead`. |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [in-subquery-plan.md](in-subquery-plan.md) | Proposal: `x in (table_expr)` / `!(x in …)` as semi / null-aware anti join — a sibling of `exists`; readability, not new queries |
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

- **Retired 2026-10-05**, overtaken or done. Read any of them at
  `git show 587bc2e4:plans/<name>`.
  - **radix-partitioned-groupby.md** — a July note that high-cardinality
    group-by is memory-bound and wants radix partitioning. Overtaken: the
    partition-owned aggregate (one partition per worker) is that partitioning,
    and q18's group-by has since been rebuilt (async hot/cold −33%, Int64 sums
    −85%, the HAVING prefilter −32%). Its numbers (q18 246 ms) are from before
    all of it.
  - **owned-agg-per-chunk-barrier-plan.md** — partition-owned aggregation
    against Polars streaming: the async hot/cold table (q18 −33%), the
    parallel finalize merge (−7%), the PairIntKey collector (q20 −51.7%), the
    ordered-run `Count` finalize (q21 −11.2% SF-4). Its dead ends (more
    partitions than workers; a serial `reserve` of the partition maps; small
    accumulate tweaks) are in `beat-both-plan.md` §5. Left: q21's per-chunk
    accumulate orchestration and a duplicate lineitem decode, both under
    beat-both; the "40 ms serial hash build" it cites is unconfirmed.
  - **runtime-multithreading-plan.md** — the phase roadmap for multi-core
    execution: morsel pipelines on by default, first-party Parquet, parallel
    sources, parallel ungrouped/Categorical aggregates, rank sweeps. The design
    and determinism contract are in `src/runtime/PARALLELISM.md`. Left: Phase 2
    deterministic RNG and generators (designed, not started; the design is in
    the retired file); parallel CSV sources; TSAN coverage for reader
    isolation and cancellation; bounding reader count separately from morsel
    count. Its "LazyTable Synchronization Contract" was never written down as
    code, but workers have decoded `LazyTable` units concurrently since
    2026-08-16, each with its own reader; read that section as history.

- **parallelism-overview.md** — retired 2026-10-05; read it at
  `git show 47918401:plans/parallelism-overview.md`. The catalogue of where the
  multi-core model diverges from itself (I1–I15) and the findings that closed
  or refused each track. Where it lives now: the open inconsistencies, the
  "not inconsistencies" list, the dropped scheduler and the reverted
  multi-producer overlap (with its recovery commits) in
  `src/runtime/PARALLELISM.md` ("Where the model is still muddy"); the
  rejected weakening of first-occurrence group order under its determinism
  contract; the two-phase probe's `Precomputed` trap on `try_two_phase_probe`;
  the attribution mistakes (serial operator vs expression off a fast path,
  `__memmove` in L2, the I/O floor) and the placement-changes-row-count rule in
  `MEASURING.md` §10. Left, none scheduled: eliding the first-occurrence merge
  when the consumer ignores order (a required-ordering property propagated
  down the plan), and a shared "is this type parallel-capable in role X"
  predicate (I2).
- **allocator-and-huge-pages.md** — retired 2026-10-05; read it at
  `git show 47918401:plans/allocator-and-huge-pages.md`. A parked analysis of why a
  fresh process is slow (first-touch page faults; q21 ~0.43 against Polars
  warm, ~0.63 fresh). jemalloc no; whole-heap huge pages −8.0% / −4.9% fresh
  at 1 / 8 cores and neutral warm; column-buffer-only huge pages reverted. The
  findings and the reopen condition (one-shot command-line runs become a
  target, or a warm process shows page faults) are on `tune_allocator_once`
  in `src/runtime/interpreter.cpp`; the warm/fresh quoting rule is in
  `MEASURING.md`.

- **per-occurrence-scan-selections-plan.md** — retired 2026-10-05; read it at
  `git show c5e9e2b1:plans/per-occurrence-scan-selections-plan.md`. A source scanned
  more than once had lost all filter pushdown (`afe55f25` made occurrences
  share a name). Phases 1–3 (`78a09fad`, `bf783ef3`, `f2b298db`): occurrence
  identity (`scan_predicates_by_occurrence`), the `selection_for` seam, and
  `isolate_filtered_scan_instances` with one shared decode per source,
  gated on a fusable `like` over a filter-only column; the e2e check that
  caught it is green. On the way, the eager selection was morselized (q04
  −5.5%). The gate's rule and why it stays narrow are on the gate
  (`scan_predicates.cpp`); the timer lesson is in `MEASURING.md` §6. Left:
  - Phase 4, widening the gate to any predicate: measured −0.4% geomean,
    byte-identical on 22 (2026-09-04). **Decided 2026-10-05: not now** —
    split instances must stay eager, so widening would take every
    repeatedly-scanned filtered source off the streaming and deferred-probe
    paths that beat-both relies on. Reopen with a query where a
    per-occurrence non-`like` predicate is a measured win;
  - a strictly safe interim, unbuilt: push the conjuncts common to every
    occurrence (each occurrence's own filter still runs above).

- **query-shape-conformance-plan.md** — retired 2026-10-05; read it at
  `git show 051dd48f:plans/query-shape-conformance-plan.md` (the full diary is at
  `git show 81392162:plans/query-shape-conformance-plan.md`). Why the
  canonical query rewrite (`482eb583`) cost 13–16% and how the suite got back
  to parity without q21. Its open items moved to `beat-both-plan.md` §3 (q13's
  fused non-anchored LIKE in item 6, the row-group task split in item 8), its
  do-not-repeat list and the q21 shape argument to §5. Its third item, a
  row-count-ratio deferred-probe gate, is built (`build_side_worth_deferring`:
  the build estimate under half the probe's rows, then key-domain coverage).
  Left: its note that the old "filter-into-join pushdown cannot see schemas at
  lowering" mechanism looks resolved (the REPL re-runs
  `push_filters_into_joins` with footer schemas); check that before reopening
  it.

- **ibex-compile-conformance-plan.md** — retired 2026-10-05; read it at
  `git show 707662b7:plans/ibex-compile-conformance-plan.md`. Closed the drift
  between `ibex_compile` and the interpreter: the parity suite became a
  conformance gate (every case transpiles and matches, or carries an
  `.unsupported` marker; none do), then `map { }` on every surface (W1),
  non-literal extern arguments and `parse_args` (W2), user functions (W3),
  `model { }` with the built-in methods (W4), window combinations (W5, which
  also fixed `where … update` compiling without its guard), and effects and
  ADBC resources in compiled programs, functions included (W6, 2026-10-02).
  Its two rules are on `Emitter` (`include/ibex/codegen/emitter.hpp`: one
  set of kernels, so divergence can only be in plan construction) and
  `ScriptPlan` (`include/ibex/parser/lower.hpp`: statement order comes from
  the plan's effect summaries). Left, each refused with a message today:
  - in compiled programs: plugin model methods (`lightgbm`, `kmeans`, `pca`,
    hence `predict`), a model fitted inside a function;
  - in compiled functions: a scalar `let` in the body, Decimal or column
    parameters, reading the program's tables or connections, a nested call
    returning a scalar or table argument;
  - on the whole-script path: a deferred scalar sourced by the same lazy-reader
    graph still declines (W2-S2);
  - deferred: `effects { }` declarations on the ADBC externs (only together
    with the file readers and writers), and a resource acquire/use/close
    summary for finer reordering (needs alias analysis first);
  - unverified: stream constructs in compiled mode were flagged for an audit
    that the plan never closed.

- **kernel-pipeline-execution-plan.md** — retired 2026-10-05; read it at
  `git show 2b96e6d6:plans/kernel-pipeline-execution-plan.md`. The migration from
  `build_operator` branches to logical IR → physical plan → pipelines →
  morsel executor, drained 2026-08-31: an inspectable physical plan
  (`explain physical`), row-local kernels (`kernel_types.hpp`), one ordered
  handoff (`OrderedChunkRing`), islands as a pipeline mode, breakers built from
  the plan with plan-owned fan-out (`HashBuild`/`HashProbe`; the four
  aggregate phases), `chunked.cpp` split into `runtime_entry.cpp` plus one
  file per family, and the `MaterializedCall` fallback as the accepted end
  state. Where its rules live now: the architecture in
  `src/runtime/CONTRACTS.md` §0; the planner-relays-the-builder method note on
  `plan_physical`; the two "join cost model" decisions beside each other
  (`build_side_worth_deferring`, `ChunkedInnerJoinOperator::initialize`).
  Its "q21 serial hash build" target was met by the partitioned build
  (q21 −8.5%); the 40 ms it was sized from was a wall-span self time. Left:
  - two small cleanups: the `Filter`/`Project`/`Rename` branches in
    `build_operator_impl` (`runtime_entry.cpp`) are probably reachable only on
    `MalformedMapNode` (confirm, then delete or make them
    `invariant_violation`); bare streaming sources (a deferred `Scan`, a
    chunked `ExternCall`) inflate the `note_materialized_call` backlog;
  - `IBEX_PROBE_MORSELS=1` (opt-in probe morsels) still stalls SF-4 q09: fix
    it or delete the opt-in;
  - deferred, with their reopen conditions: `KernelContext` (when kernels
    share scratch or a cancellation owner), per-pipeline scheduling accounting
    (a multi-producer change, or queues forming), splitting the test binary by
    layer (when the link hurts the inner loop), migrating a fallback kind
    (when one profiles hot), DOP/memory budgets (see the DOP-analysis entry).

- **joins.md** — retired 2026-10-05; read it at `git show fa7aad97:plans/joins.md`.
  The join contract, built 2026-08: asymmetric keys, the canonical output
  planner, row order outside the contract, `left()`/`right()`, collisions and
  `suffix`, early key checks, order-aware build-side choice, `nulls equal`,
  `expect`/`take`, schema nullability, mapped-key normalization. Where its
  rules live now: the semantics in SPEC.md §5.6 (key-type strictness added
  there on retirement); the rules beside the code — `mapped_join_keys.hpp`
  (what cannot be folded, and the equivalence-class alternative),
  `ir/nullability.{hpp,cpp}`, `check_joins` in `ir/schema.hpp`, the
  build-side ratio and its measurements in `src/runtime/join.cpp`, the
  `nulls equal` decline in `physical_plan.cpp`; the dplyr mapping at the top
  of the join helpers in `r/ibex/R/dplyr-backend.R`; its benchmarking method
  in `MEASURING.md`. Left, none scheduled:
  - time-domain joins beyond `asof join` with a tolerance: direction, tie
    behaviour, interval/overlap joins (as compound inequalities over the
    theta path), all keeping the time-index ordering;
  - the equivalence-class key model (only when a query needs a mapped key
    under a Right/Outer join, or a right key read above a Left join);
  - letting `expect n:1` bias the build side (needs a threshold and a paired
    benchmark, like the pending-order guard);
  - the R adapter: dplyr's left row order, grouping across a mutating join,
    vctrs key coercion.

- **phase3-dop-budget-analysis.md** — retired 2026-10-05; read it at
  `git show 82ab01d1:plans/phase3-dop-budget-analysis.md`. An analysis of
  kernel-pipeline Phase 3 item 2 (DOP and memory budgets). Done: 2a, the named
  compute budget (`ExecutionContext::compute_budget()`), and the removal of
  `IBEX_PARALLEL` (serial is `IBEX_CORES=1`; every core count gives the same
  bits). Parked: 2b, child DOP budgets — reopen with a multi-producer breaker,
  or when a third ad-hoc branch budget appears (consumer-helps' serial context
  and `scan_pipeline_needs_spare` are two today). Rejected as scoped: 2c, a
  memory budget, until something consumes it (binding GC is the likely first).
  Its main premise is obsolete: nested `submit` from a pool thread used to be
  an invariant violation and is now legal and bounded (cooperative waits run
  strictly nested work; capped at the pool size). Its occupancy numbers are
  SF-1, August; re-run `profile_suite.py` at SF-10 before reopening 2b.

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
    → `interpret_node` seam. On 2026-10-04 `interpret_node` stopped being a
    second executor: it became `run_materialized_node`
    (`materialized_node.cpp`), whose every input goes back through
    `build_operator` (`materialize_plan`), with its input allowlist and the
    cases for always-migrated kinds deleted.
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
  project_execution_roadmap); `src/runtime/CONTRACTS.md` §6 states it, and
  bigger-than-ram Phase 4 is where it is extended.
- **pipelined-execution** now supplies the first source-to-breaker and
  join-output overlap on top of the chunked substrate. It deliberately retains
  whole-query `LazyTable` pushdowns; its next work is progress-aware admission
  and general scheduling, not another static scan gate.
- **runtime-multithreading** (retired 2026-10-05) delivered the multi-core
  runtime: morsel pipelines, first-party Parquet with Arrow-compatible
  buffers, parallel sources and the first parallel barriers. The remaining
  multi-core gap is tracked by **beat-both**; the design lives in
  `src/runtime/PARALLELISM.md`.
- **bigger-than-ram** built directly on the removed **chunked-execution**:
  every "materializing" row in that plan's coverage table (unsorted `Order`/
  `AsTimeframe`, non-streaming `Tail`, general `Join`) is a target phase
  here — the streaming breakers that exist now are listed in
  `src/runtime/PARALLELISM.md` — and the
  extern-source contract hardening and this plan's Phase 4 (chunked Parquet)
  are the same work from two angles. Its Phase 7 (parallel spill I/O) was sequenced after
  **runtime-multithreading**, which has since landed.
