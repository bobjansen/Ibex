# Plans Index

Status of every plan in this directory, grouped by lifecycle. Statuses verified
against the source tree on 2026-07-12. Files stay in place unless a plan is
complete enough to move under `plans/done/`.

## Active — open work items

| Plan | Status | What's actually left |
|---|---|---|
| [join-predicate-pushdown-plan.md](join-predicate-pushdown-plan.md) | Not started | q19 is the worst PDS-H query (520ms vs polars-st 58ms) because it joins 6M rows then filters to ~120. Pushing the lineitem-only conjuncts below the join collapses join+filter from 951ms to 37ms (measured by hand). Blocker to decide first: `canonicalize` is schema-free (0 Join rules), so the rule cannot tell which side a column belongs to — needs a schema-aware pass. Stage 1 is Inner joins only; `Asof` must be refused |
| [chunked-execution-plan.md](chunked-execution-plan.md) | Living roadmap | Streaming aggregate coverage is broad enough for now (`first`/`last` landed). Open: document + harden the external chunked-source contract (→ ADBC path); `MaterializeOperator` schema/dictionary/validity hardening; materializing breakers such as unsorted `Order`/`AsTimeframe`, general `Join`, reshape/stat/model nodes |
| [benchmark-perf-priorities.md](benchmark-perf-priorities.md) | Living reference | P0–P2 resolved/landed; rolling min/max optimized. Open: suite trimming (pin sqlite + data.table frollapply cells, duckdb at 3 scales); P4 `tanh` deferred pending accuracy-vs-speed call; P3 ohlc scatter-bound (negative result recorded — don't re-attempt naive fusion); re-check rolling_mean on AWS after the July 2026 regression fix |
| [benchmark-coverage-plan.md](benchmark-coverage-plan.md) | ~95% done | #9 ClickHouse EWMA (needs arrayFold workaround); #10 DataFusion `fill_forward/backward` + `tf_asof_join` |
| [count-window-plan.md](count-window-plan.md) | Implemented (interpreter + codegen) | Per-call count/duration windows work (`__window_n`/`__window_ns` in lower.cpp + window.cpp), and the compiled path (`ibex_compile`) is at parity. Open: `window N rows` block syntax and tuple-field `update` inside `window` (interpreter doesn't support that combo either, so codegen correctly still rejects it). The old monotonic-deque follow-up for `rolling_min`/`rolling_max` is done. |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |
| [multiway-join-chain-perf-plan.md](multiway-join-chain-perf-plan.md) | Investigation complete | Probe-order-preserving joins landed (82c391f): q05 -7%, q18 -4.2%, q09 -3.9%, q07 -3.4%. All three follow-ups resolved 2026-07-18: q05 stage profile (residual = lineitem decode 67ms + raw hash-probe throughput 59ms, NOT inter-stage materialization); q11's `german_supply` confirmed materialized once (shared-binding gate, lower.cpp:1347); DuckDB EXPLAIN ANALYZE shows its 2.7x q05 win is dynamic filter pushdown (`l_orderkey IN BF` in the lineitem scan → 3.3% of rows emitted). Probe-side Bloom tried + reverted (−3.4% isolated, wash end-to-end). Successor: [dynamic-filter-pushdown-plan.md](dynamic-filter-pushdown-plan.md) |
| [dynamic-filter-pushdown-plan.md](dynamic-filter-pushdown-plan.md) | COMPLETE (stage 4 uncommitted) | Sideways information passing: a join's build keys become {Bloom, exact IN-list, min/max} pushed into the probe side's parquet scan. Stage 2 (Bloom + IN-list + sampled escape hatch): **suite geomean 0.895** — q08 −47%, q17 −45%, q09 −40%; lineitem prunes to 0.9-16% of rows, q18 to 399 rows. Stage 4 (fused `KeyFilterScanFn` in the parquet decoder, row-group skipping, in-decoder escape hatch): further **geomean 0.984**, q17 −16%, q08 −8%, q05 −5%; cumulative ≈ −12%. 22/22 answers identical throughout; hit-heavy neutral. GOTCHAS: the Bloom must front even an exact IN-list (bare binary_search cost q17 +80%); LazyTable grew a member — stale plugin .so segfaults (ABI rebuild rule). Residual: q10 +5% (agg-dominated, conjunct-carrying scans stay on the stage-2 path — folding static conjuncts into the decoder is decode-fusion Stage 4). Key constraint: filtering must gate materialization, not the hash lookup |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Kernel-level parallelism (`IBEX_THREADS`, `parallel_for`), single-threaded default. No code yet. |
| [julia-integration-plan.md](julia-integration-plan.md) | Ibex.jl package, `ibex"""..."""` macro, Arrow/Tables.jl interop, DataFrames.jl benchmark baseline |
| [short-mode-plan.md](short-mode-plan.md) | Prefix-abbreviated golf mode (`t[s{s,p}]`) behind an explicit mode gate + formatter round-trip. Design sketch only |

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
- **runtime-multithreading** is the answer to the remaining polars
  multi-thread gaps in **benchmark-perf-priorities** (single-thread ibex
  already wins 37/41 vs polars-st).
- **bigger-than-ram** builds directly on **chunked-execution**: every
  "materializing" row in that plan's coverage table (unsorted `Order`/
  `AsTimeframe`, non-streaming `Tail`, general `Join`) is a target phase
  here, and its Next Steps §2 (harden the external chunked-source contract)
  and this plan's Phase 4 (chunked Parquet) are the same work from two
  angles. Its Phase 7 (parallel spill I/O) is explicitly sequenced after
  **runtime-multithreading**, not coupled to it.
