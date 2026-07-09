# Plans Index

Status of every plan in this directory, grouped by lifecycle. Statuses verified
against the source tree on 2026-07-08. Files stay in place (memory notes and
cross-plan links reference these paths) — this index is the organization layer.

## Active — open work items

| Plan | Status | What's actually left |
|---|---|---|
| [chunked-execution-plan.md](chunked-execution-plan.md) | Living roadmap | `first`/`last` streaming aggregates; document + harden the external chunked-source contract (→ ADBC path); `MaterializeOperator` schema/dictionary/validity hardening |
| [benchmark-perf-priorities.md](benchmark-perf-priorities.md) | Living reference | P0–P2 resolved/landed. Open: suite trimming (pin sqlite + data.table frollapply cells, duckdb at 3 scales); P4 `tanh` deferred pending accuracy-vs-speed call; P3 ohlc scatter-bound (negative result recorded — don't re-attempt naive fusion) |
| [benchmark-coverage-plan.md](benchmark-coverage-plan.md) | ~95% done | #9 ClickHouse EWMA (needs arrayFold workaround); #10 DataFusion `fill_forward/backward` + `tf_asof_join` |
| [function-kind-registry-plan.md](function-kind-registry-plan.md) | Stage 1 of 6 done | Scalar slice unified. Next: fold Generators (`rand_*`, `rep`), then Transforms, then Aggregates into one `FnKind` registry; settle the null/validity wrinkle; collapse the four field-evaluator ladders |
| [count-window-plan.md](count-window-plan.md) | Implemented (interpreter + codegen) | Per-call count/duration windows work (`__window_n`/`__window_ns` in lower.cpp + window.cpp), and the compiled path (`ibex_compile`) is at parity — it was already generic via `ops::fn_call`/named-args passthrough; the one real gap was `window ..., update {...}, by ...` (grouped rolling), which the emitter rejected outright — `ops::windowed_update` now threads `group_by` through like plain `update` does. Verified via `tests/parity/run_parity.sh` (new `window_by`/`rolling_percall_window` cases) + new `test_codegen.cpp` cases. Open: `window N rows` block syntax, monotonic-deque rolling min/max, tuple-field `update` inside `window` (interpreter doesn't support that combo either, so codegen correctly still rejects it). **File untracked — commit it** |
| [non-row-local-filter-plan.md](non-row-local-filter-plan.md) | Stage 1 shipped | `lag`/`lead`/`is_null` in filter work. Remaining: `rank(...)` in filter/select with `by`, explicit `order {}` context, rolling functions in filter (`price > rolling_mean(price)`) |
| [bigger-than-ram-plan.md](bigger-than-ram-plan.md) | Phase 4 bullet 1 of 4 done | Out-of-core execution. Done: chunked/streaming `read_parquet` (branch `chunked-parquet-read`; ~6.5× lower peak RSS, ~1.7× faster, verified local + AWS). Next: column projection pushdown, row-group stats pushdown, directory/Hive datasets (rest of Phase 4), then Phase 1 spill infrastructure (prerequisite for Phases 2–3, 6–7: external sort, out-of-core join, adaptive spill selection) |

## Proposed — no implementation yet

| Plan | Notes |
|---|---|
| [runtime-multithreading-plan.md](runtime-multithreading-plan.md) | Kernel-level parallelism (`IBEX_THREADS`, `parallel_for`), single-threaded default. No code yet. |
| [julia-integration-plan.md](julia-integration-plan.md) | Ibex.jl package, `ibex"""..."""` macro, Arrow/Tables.jl interop, DataFrames.jl benchmark baseline |
| [short-mode-plan.md](short-mode-plan.md) | Prefix-abbreviated golf mode (`t[s{s,p}]`) behind an explicit mode gate + formatter round-trip. Design sketch only |

## Complete — kept for reference

| Plan | Outcome | Residual items |
|---|---|---|
| [schema-propagation-plan.md](schema-propagation-plan.md) | Stages 1–9 all done: `infer_schema`, `as` ascription, expression inference, static column-ref checks, exact/wildcard schemas, reader return schemas, let-binding schemas, time index | Follow-ups: named schema aliases (`type X = {...}`); time index from declared `TimeFrame<S>` sources. Static fn-arg contracts wait on whole-program schema flow |
| [unify-filter-expr-plan.md](unify-filter-expr-plan.md) | `FilterExpr` deleted; predicates are boolean `ir::Expr`; vectorised filter path preserved (benchmarked, no regression) | Spec/allow booleans in value position (storable masks) — leaning yes, not yet spec'd |
| [aggregate-udf-plan.md](aggregate-udf-plan.md) | Scalar UDF inlining (select/update/filter/agg args), `agg fn` via AST inlining, F1 grouped broadcast, F2 mixed scalar+Series params, F3.1 `let` bodies — all done with zero new IR nodes | Deferred by design: value-selection as a `where()` builtin (tier 2); statement-level control flow out of scope (see project_no_control_flow) |
| [udf-dataframe-plan.md](udf-dataframe-plan.md) | Superseded/delivered: phases 1–5 landed via this plan + aggregate-udf (clause integration) + schema-propagation (contracts, reader schemas) | Fn arg/return `DataFrame<Schema>` contracts remain runtime-checked in the REPL (by design until whole-program schema flow) |
| [kafka-schema-registry-plan.md](kafka-schema-registry-plan.md) | Avro v1 via Redpanda Schema Registry done end to end; Protobuf shelved | Two checklist items: re-run + document the e2e Avro demo; prune Protobuf-forward wording from docs |
| [canonicalize-followups.md](canonicalize-followups.md) | Mostly absorbed: items 1–3 landed as canonicalize rules R9–R15 (see chunked-execution-plan rule table) | Item 4: extract rules into a `try_rule` table (`rewrite_root` is still a hand-rolled loop); item 5: rule-composition tests beyond R3∘R1 |

## Cross-plan dependency notes

- **function-kind-registry** builds on the unified expression IR from
  **unify-filter-expr** and generalises the `scalar_builtins()` pattern; its
  Generator stage should absorb `rep`/`cycle`/`seq` (see memory
  project_rep_cycle_design).
- **non-row-local-filter**'s remaining items (rolling/rank in filter) get much
  easier once **function-kind-registry** lands — "contains a Transform →
  evaluate vectorised" is exactly the dispatch rule it introduces.
- **count-window**'s codegen TODO and the emitter generally can read the
  function-kind registry for arity/kind metadata.
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
