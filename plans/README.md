# Plans Index

Status of every plan in this directory, grouped by lifecycle. Statuses verified
against the source tree on 2026-08-16 (rows added since note their own date).
Completed plans are removed from the tree rather than archived (see the
Complete section for how to find them in git history).

## Active — open work items

| Plan | Status | What's actually left |
|---|---|---|
| [beat-polars-plan.md](beat-polars-plan.md) | **Umbrella plan** for the multi-core push. Absorbed the former `pds.md` baseline record (2026-08-22): standings, lever provenance, and two dead ends live in its §8 | Target: implied parallel fraction 44% → 60–65% ⇒ faster than Polars at every pinned core count. Workstreams by serial-ms attacked: W1 parallel inner join (probe+assemble+phase A, ~435 ms at 0.2× help), W2 aggregate residue (defer-first-chunk, fat-slot diet, cardinality gate), W3 scheduler slice 2 (progress-aware admission, 2-core cliff, q18/q22), W4 chunked `let` bindings (kill the sink concat), W5 small-query-tax guard. Points into pipelined-execution + runtime-multithreading for mechanism |
| [kernel-pipeline-execution-plan.md](kernel-pipeline-execution-plan.md) | **Phase 1 landed (2026-08-22).** Proposed architectural successor for the execution engine: typed logical IR, physical pipelines, a morsel executor, and a broad precompiled templated kernel library. Explicitly Umbra-like in plan/pipeline structure but **not** a JIT project. Phase 1 adds `physical::plan_physical`/`explain_physical` beside `build_operator` (serial executor; the island remains the parallel mode) with `MaterializedCall` fallback reasons and path-fired counters. Phase 0 is resolved by disposition in the plan (comparator already existed as `table_compare`; debug dump landed inside Phase 1; contract docs become Phase 2's first deliverable). |
| [benchmark-perf-priorities.md](benchmark-perf-priorities.md) | Living reference | P0–P2 resolved/landed; rolling min/max optimized. Open: suite trimming (pin sqlite + data.table frollapply cells, duckdb at 3 scales); P4 `tanh` deferred pending accuracy-vs-speed call; P3 ohlc scatter-bound (negative result recorded — don't re-attempt naive fusion); re-check rolling_mean on AWS after the July 2026 regression fix |
| [benchmark-coverage-plan.md](benchmark-coverage-plan.md) | ~95% done | #9 ClickHouse EWMA (needs arrayFold workaround); #10 DataFusion `fill_forward/backward` + `tf_asof_join` |
| [count-window-plan.md](count-window-plan.md) | Implemented (interpreter + codegen) | Per-call count/duration windows work (`__window_n`/`__window_ns` in lower.cpp + window.cpp), and the compiled path (`ibex_compile`) is at parity. Open: `window N rows` block syntax and tuple-field `update` inside `window` (interpreter doesn't support that combo either, so codegen correctly still rejects it). The old monotonic-deque follow-up for `rolling_min`/`rolling_max` is done. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Phases 1–3a, the first Phase 3b source slice, and Phase 4 items 1–2 landed; item 3 retired, item 4 part-done | Lazy scans now have bounded concurrent decode and ordered source→breaker overlap (see pipelined execution); general pipeline scheduling remains open. **Item 3 (hash join) is RETIRED**: the gap was Categorical probe keys hashed as text. Next: profile the string/int/generic group-by and `distinct` paths before adding parallelism; re-measure at target scale because fixed-core per-core wins do not preserve the headline as data grows. |
| [join-perf-plan.md](join-perf-plan.md) | Items 1–3 done (2026-07-14; q09 −23%, q13 −30%) | Join/group-by performance findings; see the file's Results section. beat-polars points here for join mechanism |
| [null-key-semantics-plan.md](null-key-semantics-plan.md) | **Unverified** — the file carries no status markers | Bug report: operators that build keys from column values mishandle nulls (group-by/distinct/order/join). Verify against the tree before acting |
| [query-shape-conformance-plan.md](query-shape-conformance-plan.md) | Down from +13-16% suite-wide to one query | `eb5231c`'s query-shape realignment cost +13-16% total at SF-2/8c vs the old hand-fused queries. Mechanism 2 (`projected_scan` sees through checked `Ascribe`) and Ascribe-as-Scan-metadata (root-causing the whole "teach pass X to see through Ascribe" pattern) are landed; a full re-measurement (2026-08-20) found the other three originally-suspected mechanisms moot or resolved as side effects — q06/q13/q15/q14 all at or better than the old baseline, and the 21 queries excluding q21 are at parity (0.995x) with the pre-realignment hand-fused suite. The regression is now **entirely concentrated in q21** (2.33x, a genuine planner gap: needs a filter pushed back through a self-join and two aggregates), plus a smaller unstarted q22 gap (1.32x, an unrelated rewrite-shape question) and an unconfirmed q09 lead. See the plan's final section for the current, narrow scope. |

## Reference — descriptive, not a work item

| Document | What it is |
|---|---|
| [joins.md](joins.md) | The join contract: Ibex's join model and the gaps found building the dplyr backend; mapped vs shared-name keys |
| [parallelism-overview.md](parallelism-overview.md) | How multi-core execution works today, in the vocabulary of the parallel-database literature: the `WorkerPool` substrate and its two thread budgets, the three parallelism layers (pipeline / parallel-map island / intra-operator), the determinism contract, and the full config surface. Part 2 is a numbered list of the places the implementation diverges from itself — type-dependent gather rules, whole-table vs chunked operator coverage, eleven private row thresholds beside the two `ExecutionContext` knobs — with a suggested order of attack. Start here before adding a new fan-out |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [pipelined-execution-plan.md](pipelined-execution-plan.md) | **Phase 2 partially implemented.** Multi-chunk correctness, lazy row-group streaming, concurrent scan decoding, ordered source→map overlap, and a bounded streamed join-probe handoff landed. SF-1 improves 2.5–4.6% at 2–8 cores; SF-4 improves 0.6–3.7% at 4–8. The two-core SF-4 crossover (+6.9%) and q18 are explicit follow-ups. A one-producer admission gate was measured worse and withdrawn. Open: progress-aware admission/backpressure and general breaker scheduling |
| [julia-integration-plan.md](julia-integration-plan.md) | Ibex.jl package, `ibex"""..."""` macro, Arrow/Tables.jl interop, DataFrames.jl benchmark baseline |
| [radix-partitioned-groupby.md](radix-partitioned-groupby.md) | Noted, not built. High-cardinality group-by is memory-bound; radix partitioning is the proposed fix — the remaining wall on q18/q10 after the boxed-Key fast paths |
| [exists-subquery-plan.md](exists-subquery-plan.md) | Proposal: `exists(table_expr)` as a boolean subquery term — semi/anti/mark joins and the residual-predicate case |
| [in-subquery-plan.md](in-subquery-plan.md) | Proposal: `x in (table_expr)` / `not in` as semi / null-aware anti join — the subquery family, not a scalar like `like()` |
| [extern-series-arguments-plan.md](extern-series-arguments-plan.md) | Proposal: `Series<T>` as a first-class extern argument, starting with CSV null tokens |
| [project_http_plugin_plan.md](project_http_plugin_plan.md) | HTTP plugin MVP: simple registration API, blocking server, no decorators yet |
| [short-mode-plan.md](short-mode-plan.md) | Prefix-abbreviated golf mode (`t[s{s,p}]`) behind an explicit mode gate + formatter round-trip. Design sketch only |

## Complete — removed from the tree

Completed plans are no longer kept in `plans/` (removed 2026-08-22 for
focus; the last set lived under `plans/done/`). They are fully recoverable
from git history — `git log --diff-filter=D --name-only -- plans/done/`
lists them, and the removal commit's parent still has every file. Citations
in active plans to `plans/done/...` paths refer to that history.

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
