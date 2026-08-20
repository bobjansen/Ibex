# Plans Index

Status of every plan in this directory, grouped by lifecycle. Statuses verified
against the source tree on 2026-08-16. Files stay in place unless a plan is
complete enough to move under `plans/done/`.

## Active — open work items

| Plan | Status | What's actually left |
|---|---|---|
| [beat-polars-plan.md](beat-polars-plan.md) | **Umbrella plan** for the multi-core push | Target: implied parallel fraction 44% → 60–65% ⇒ faster than Polars at every pinned core count. Workstreams by serial-ms attacked: W1 parallel inner join (probe+assemble+phase A, ~435 ms at 0.2× help), W2 aggregate residue (defer-first-chunk, fat-slot diet, cardinality gate), W3 scheduler slice 2 (progress-aware admission, 2-core cliff, q18/q22), W4 chunked `let` bindings (kill the sink concat), W5 small-query-tax guard. Points into pipelined-execution + runtime-multithreading for mechanism |
| [chunked-execution-plan.md](chunked-execution-plan.md) | Living roadmap | Streaming aggregate coverage is broad enough for now (`first`/`last` landed). Open: document + harden the external chunked-source contract (→ ADBC path); `MaterializeOperator` schema/dictionary/validity hardening; materializing breakers such as unsorted `Order`/`AsTimeframe`, general `Join`, reshape/stat/model nodes |
| [benchmark-perf-priorities.md](benchmark-perf-priorities.md) | Living reference | P0–P2 resolved/landed; rolling min/max optimized. Open: suite trimming (pin sqlite + data.table frollapply cells, duckdb at 3 scales); P4 `tanh` deferred pending accuracy-vs-speed call; P3 ohlc scatter-bound (negative result recorded — don't re-attempt naive fusion); re-check rolling_mean on AWS after the July 2026 regression fix |
| [benchmark-coverage-plan.md](benchmark-coverage-plan.md) | ~95% done | #9 ClickHouse EWMA (needs arrayFold workaround); #10 DataFusion `fill_forward/backward` + `tf_asof_join` |
| [count-window-plan.md](count-window-plan.md) | Implemented (interpreter + codegen) | Per-call count/duration windows work (`__window_n`/`__window_ns` in lower.cpp + window.cpp), and the compiled path (`ibex_compile`) is at parity. Open: `window N rows` block syntax and tuple-field `update` inside `window` (interpreter doesn't support that combo either, so codegen correctly still rejects it). The old monotonic-deque follow-up for `rolling_min`/`rolling_max` is done. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [multiway-join-chain-perf-plan.md](multiway-join-chain-perf-plan.md) | Investigation complete | Probe-order-preserving joins landed (82c391f): q05 -7%, q18 -4.2%, q09 -3.9%, q07 -3.4%. All three follow-ups resolved 2026-07-18: q05 stage profile (residual = lineitem decode 67ms + raw hash-probe throughput 59ms, NOT inter-stage materialization); q11's `german_supply` confirmed materialized once (shared-binding gate, lower.cpp:1347); DuckDB EXPLAIN ANALYZE shows its 2.7x q05 win is dynamic filter pushdown (`l_orderkey IN BF` in the lineitem scan → 3.3% of rows emitted). Probe-side Bloom tried + reverted (−3.4% isolated, wash end-to-end). Successor: [dynamic-filter-pushdown-plan.md](done/dynamic-filter-pushdown-plan.md) |
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Phases 1–3a, the first Phase 3b source slice, and Phase 4 items 1–2 landed; item 3 retired, item 4 part-done | Lazy scans now have bounded concurrent decode and ordered source→breaker overlap (see pipelined execution); general pipeline scheduling remains open. **Item 3 (hash join) is RETIRED**: the gap was Categorical probe keys hashed as text. Next: profile the string/int/generic group-by and `distinct` paths before adding parallelism; re-measure at target scale because fixed-core per-core wins do not preserve the headline as data grows. |
| [query-shape-conformance-plan.md](query-shape-conformance-plan.md) | Down from +13-16% suite-wide to one query | `eb5231c`'s query-shape realignment cost +13-16% total at SF-2/8c vs the old hand-fused queries. Mechanism 2 (`projected_scan` sees through checked `Ascribe`) and Ascribe-as-Scan-metadata (root-causing the whole "teach pass X to see through Ascribe" pattern) are landed; a full re-measurement (2026-08-20) found the other three originally-suspected mechanisms moot or resolved as side effects — q06/q13/q15/q14 all at or better than the old baseline, and the 21 queries excluding q21 are at parity (0.995x) with the pre-realignment hand-fused suite. The regression is now **entirely concentrated in q21** (2.33x, a genuine planner gap: needs a filter pushed back through a self-join and two aggregates), plus a smaller unstarted q22 gap (1.32x, an unrelated rewrite-shape question) and an unconfirmed q09 lead. See the plan's final section for the current, narrow scope. |

## Reference — descriptive, not a work item

| Document | What it is |
|---|---|
| [parallelism-overview.md](parallelism-overview.md) | How multi-core execution works today, in the vocabulary of the parallel-database literature: the `WorkerPool` substrate and its two thread budgets, the three parallelism layers (pipeline / parallel-map island / intra-operator), the determinism contract, and the full config surface. Part 2 is a numbered list of the places the implementation diverges from itself — type-dependent gather rules, whole-table vs chunked operator coverage, eleven private row thresholds beside the two `ExecutionContext` knobs — with a suggested order of attack. Start here before adding a new fan-out |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [pipelined-execution-plan.md](pipelined-execution-plan.md) | **Phase 2 partially implemented.** Multi-chunk correctness, lazy row-group streaming, concurrent scan decoding, ordered source→map overlap, and a bounded streamed join-probe handoff landed. SF-1 improves 2.5–4.6% at 2–8 cores; SF-4 improves 0.6–3.7% at 4–8. The two-core SF-4 crossover (+6.9%) and q18 are explicit follow-ups. A one-producer admission gate was measured worse and withdrawn. Open: progress-aware admission/backpressure and general breaker scheduling |
| [serial-parity-comparator-plan.md](serial-parity-comparator-plan.md) | **Prereq for runtime-multithreading, before its Phase 0.** The master plan's parity gate needs schema/metadata/validity/categorical-backing comparison; the current `run_parity.sh` only `diff`s stdout and can see none of that. Build an in-process structured `Table`-vs-`Table` comparator + case matrix. |
| [julia-integration-plan.md](julia-integration-plan.md) | Ibex.jl package, `ibex"""..."""` macro, Arrow/Tables.jl interop, DataFrames.jl benchmark baseline |
| [short-mode-plan.md](short-mode-plan.md) | Prefix-abbreviated golf mode (`t[s{s,p}]`) behind an explicit mode gate + formatter round-trip. Design sketch only |
| [ascribe-as-scan-metadata-plan.md](ascribe-as-scan-metadata-plan.md) | Fuse a proven `Ascribe(Scan)` into a single `Scan` carrying the ascribed schema, instead of teaching every consumer (interpreter, `scan_predicates`, and now the chunked engine, five failed same-day attempts) to see through a checked `Ascribe` left standing in the tree. Root-cause fix for [[project_ascribe_pipeline_barrier]]'s whole failure class. Design sketch only, bigger than a same-day change |

## Complete — kept for reference

| Plan | Outcome | Residual items |
|---|---|---|
| [builtin-replica-control-hardening-plan.md](done/builtin-replica-control-hardening-plan.md) | `BuiltinFn` payloads are validated at registry construction; perf comparison builds canonicalize temporary paths, balance run positions, classify replica binary identity, and keep same-source replica measurements diagnostic | Controlled matched layout seeds remain deferred until the supported toolchain has a deliberate linker/compiler perturbation; one replica is not used as a statistical floor |
| [schema-propagation-plan.md](done/schema-propagation-plan.md) | Stages 1–9 all done: `infer_schema`, `as` ascription, expression inference, static column-ref checks, exact/wildcard schemas, reader return schemas, let-binding schemas, time index | Follow-ups: named schema aliases (`type X = {...}`); time index from declared `TimeFrame<S>` sources. Static fn-arg contracts wait on whole-program schema flow |
| [unify-filter-expr-plan.md](done/unify-filter-expr-plan.md) | `FilterExpr` deleted; predicates are boolean `ir::Expr`; vectorised filter path preserved (benchmarked, no regression) | Spec/allow booleans in value position (storable masks) — leaning yes, not yet spec'd |
| [aggregate-udf-plan.md](done/aggregate-udf-plan.md) | Scalar UDF inlining (select/update/filter/agg args), `agg fn` via AST inlining, F1 grouped broadcast, F2 mixed scalar+Series params, F3.1 `let` bodies — all done with zero new IR nodes | Deferred by design: value-selection as a `where()` builtin (tier 2); statement-level control flow out of scope (see project_no_control_flow) |
| [udf-dataframe-plan.md](done/udf-dataframe-plan.md) | Superseded/delivered: phases 1–5 landed via this plan + aggregate-udf (clause integration) + schema-propagation (contracts, reader schemas) | Fn arg/return `DataFrame<Schema>` contracts remain runtime-checked in the REPL (by design until whole-program schema flow) |
| [kafka-schema-registry-plan.md](done/kafka-schema-registry-plan.md) | Avro v1 via Redpanda Schema Registry done end to end; Protobuf shelved | Two checklist items: re-run + document the e2e Avro demo; prune Protobuf-forward wording from docs |
| [canonicalize-followups.md](done/canonicalize-followups.md) | Mostly absorbed: items 1–3 landed as canonicalize rules R9–R15 (see chunked-execution-plan rule table) | Item 4: extract rules into a `try_rule` table (`rewrite_root` is still a hand-rolled loop); item 5: rule-composition tests beyond R3∘R1 |
| [function-kind-registry-plan.md](done/function-kind-registry-plan.md) | Builtins now dispatch through one `FnKind` registry: generators, transforms, aggregates, `coalesce`, and the shared field-evaluator path all landed | Residual design-only items: expose arity/kind metadata to codegen/schema if needed; future extern vector/aggregate kinds |
| [exprvalue-null-arm-plan.md](done/exprvalue-null-arm-plan.md) | Per-row `ExprValue` now has real null semantics; `NullPolicy` handles propagation centrally; `coalesce`/`fill_null`/`null_if_*` are scalar `Handles`; null aggregate scalars cross collapse/broadcast boundaries safely; language docs updated | `ScalarValue` still deliberately has no null arm, so REPL scalar bindings of null remain an explicit error until null scalars need to be bindable |
| [dynamic-filter-pushdown-plan.md](done/dynamic-filter-pushdown-plan.md) | Build-side Bloom/IN-list/min-max filters reach deferred probe scans; the fused decoder and row-group skipping complete the plan | q10 remains aggregate-dominated; static-conjunct decoder fusion belongs to decode fusion |
| [execution-plan-seam-plan.md](done/execution-plan-seam-plan.md) | `build_operator()` is the parallel-island seam; the test-only `PipelinePlan` was retired | Subsequent pipeline scheduling is tracked by pipelined execution |
| [dplyr-backend-plan.md](done/dplyr-backend-plan.md) | Delivered/superseded by the Ibex R backend work | Remaining DataFrame contracts are deliberately runtime-checked |
| [string-like-filter-plan.md](done/string-like-filter-plan.md) | String-like filter work completed and archived | None recorded |
| [correlated-subquery-q02-plan.md](done/correlated-subquery-q02-plan.md) | `outer(column)` and `scalar(table_expr)` parse, lower, decorrelate, interpret, and code-generate; Q2 is in the executable benchmark corpus | The plan deliberately did not add a general correlated-apply runtime node |
| [join-predicate-pushdown-plan.md](done/join-predicate-pushdown-plan.md) | Schema-aware `push_filters_into_joins` ships for supported inner/outer-side cases; q19's rewritten form dropped 530ms → 203ms locally | `let`-split predicates remain a separately scoped lowering limitation |
| [parquet-filtering-scan-plan.md](done/parquet-filtering-scan-plan.md) | Direct filtered Parquet decode and late materialization shipped; the companion observations preserve profiles and rejected decoder experiments | Further decoder work belongs to decode fusion / runtime parallel-source work |
| [filtered-scan-and-groupby-plan.md](done/filtered-scan-and-groupby-plan.md) | Investigation closed with the `select_bounds` reservation improvement and a measured scan-side boundary | The remaining group-by work is covered by the runtime multithreading and radix-partition notes |

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
- **chunked-execution**'s extern-source contract (§2) is the gateway to the
  ADBC/pushdown stages in the execution roadmap (memory:
  project_execution_roadmap).
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
- **bigger-than-ram** builds directly on **chunked-execution**: every
  "materializing" row in that plan's coverage table (unsorted `Order`/
  `AsTimeframe`, non-streaming `Tail`, general `Join`) is a target phase
  here, and its Next Steps §2 (harden the external chunked-source contract)
  and this plan's Phase 4 (chunked Parquet) are the same work from two
  angles. Its Phase 7 (parallel spill I/O) is explicitly sequenced after
  **runtime-multithreading**, not coupled to it.
