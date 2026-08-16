# Dynamic filter pushdown into parquet scans (sideways information passing)

Status: COMPLETE. Stages 1-2 committed (a2e5527, 2026-07-19); stage 3
subsumed by measurement (fires free on chained author orders); stage 4
(fused key scan in the parquet decoder) implemented 2026-07-19.
Parent investigation: `multiway-join-chain-perf-plan.md` (stage profile,
probe-side Bloom dead end, DuckDB EXPLAIN ANALYZE comparison — all measured
2026-07-18, same box, SF-1, single-threaded, `build-release/`).

## Goal

Close the q05/q07-class gap against duckdb-st by filtering a join's probe-side
parquet scan with information derived from the join's build side, *before*
the probe columns are materialized.

## Evidence this is the right lever (don't re-derive)

- q05 residual ~180ms decomposes as: lineitem scan 67ms + `s1⋈lineitem` probe
  59ms + 2-key supplier join 17ms + orders filter/s1 ~21ms. DuckDB-st runs
  the whole query in **68ms**.
- DuckDB's q05 profile shows the mechanism: the lineitem `TABLE_SCAN` carries
  `l_orderkey IN BF (o_orderkey)` (Bloom from the join build) + min/max
  bounds, and emits **196,484 of 6,001,215 rows (3.3%)**; scan 0.04s, the
  join above it 0.01s. Its customer scan gets an exact
  `c_nationkey IN (8,9,12,18,21)` from the region⋈nation build (94,724 rows
  out); orders gets `o_custkey` min/max; nation gets `n_regionkey=2`.
- **Probe-side filtering after decode is worthless — measured.** A
  register-blocked Bloom checked in front of `heads.find` in
  `ChunkedInnerJoinOperator::emit_swapped` bought −3.4% on the target stage
  (~2ms) and was reverted: a robin_hood flat-map *miss* already terminates on
  one info-byte cache line. The same filter applied **during the scan** saves
  most of the 67ms materialization and shrinks the probe stream itself. Same
  filter, different altitude — that's the whole plan.

## Where the code stands today

- **Two-phase driver**: `execute_script`'s evaluate lambda (repl.cpp ~4440—
  4510) runs IR passes, computes `ir::scan_predicates` + `required_columns`,
  then decodes EVERY lazy source into a `TableRegistry` (`project` /
  `project_where`), and only then calls `runtime::interpret`. By the time any
  join runs, the probe side is fully materialized. Deferral must break this
  for eligible scans.
- **Join operator**: `build_operator`'s Join case (chunked.cpp ~5658) builds
  the right child operator, materializes it, and constructs
  `ChunkedInnerJoinOperator(left_op, right_table, keys)`. Swapped mode
  (right large, left smaller) materializes left in `initialize()`, builds the
  chained hash index on it, then probes right in scan order — output is
  already probe-ordered (82c391f), so a scan-level `Selection` (ascending by
  contract) composes cleanly.
- **Selective decode already exists**: `LazyTable::project_where`
  (lazy_table.hpp) decodes predicate columns first, computes a `Selection`,
  then decodes the remaining columns through it. `ColumnDecodeFn` takes
  `(names, const Selection*)` — the runtime computes selections, the plugin
  only obeys them. **A key-set filter therefore needs NO plugin/ABI change.**
- **Footer stats already exist**: `LazyTable::column_stats()` has int
  min/max/null_count per column; the fused-bounds scan-filter work (q06) and
  the join-reorder cost model already consume them.
- **`JoinBloomFilter` design exists** (reverted, recorded in the parent
  plan): register-blocked, 16 bits/key, two bits in one word, splitmix64
  finalizer. Reuse it verbatim.

## Design

New IR marker + deferred decode, single-hop first:

1. **IR pass** (whole-script driver only, after `push_filters_into_joins` /
   `split_scan_instances`): find `Join(L, Scan(f))` where the join is the
   streamable-inner shape (Inner, single key, no predicate), `f` is a lazy
   source, and `f`'s scan feeds ONLY this join (post-split instance identity
   gives this — same soundness argument as `scan_predicates`). Mark the scan
   deferred: record `{source, key column, static conjuncts, demanded
   columns}` on the join (a `JoinNode` annotation or a `DynamicScan` node —
   pick whichever keeps `infer_schema`/`required_columns` untouched;
   annotation is less invasive).
2. **Driver**: skip up-front decode for deferred sources; pass their
   `LazyTablePtr` through to `interpret` (new optional registry parameter —
   internal API, default empty keeps every other caller unchanged).
3. **Operator**: `build_operator` constructs `ChunkedInnerJoinOperator` with
   the lazy handle instead of a materialized right table. `initialize()`:
   - materialize left, build the hash index (unchanged);
   - if left turned out to be the LARGER side (build-on-right today): fall
     back — `project_where(demand, static conjuncts)` with no dynamic
     filter, proceed exactly as now. Graceful, no planner-side sizing needed.
   - else derive from the build keys: `{min, max}` always; a
     `JoinBloomFilter` when key count > ~1K; an exact sorted IN-list when
     tiny (≤ ~1K, the nation/region case);
   - decode the key column (bounded by static conjuncts + row-group min/max
     skipping), evaluate the dynamic filter into a `Selection`, decode the
     remaining demanded columns through that `Selection`, and run the
     existing swapped probe over the reduced table.
4. **Adaptive escape hatch** (the Bloom experiment's lesson, hit-heavy joins
   regressed +15-20% when the filter can't reject): sample the first ~64K
   decoded key values; if the pass rate is high (>~75%), abandon selective
   decode — decode the remaining columns in full and probe as today. Cost of
   a wrong guess: one key-column decode, which the join needed anyway.

### Filter kinds, in order of implementation

| kind | source | cost | wins |
|---|---|---|---|
| min/max bounds | build-key min/max | free; reuses fused-bounds + row-group stats skipping | weak on q05 (DuckDB's own bounds were near-full-range) but free and occasionally decisive |
| Bloom | `JoinBloomFilter` over build keys | ~1ms build for 227K keys; one L2 load/probe key | the q05/q07 fact-scan case |
| exact IN-list | sorted build keys when ≤ ~1K | trivial | dimension chains (`n_regionkey = 2`, `c_nationkey IN (...)`) |

## Stages

1. **Mechanics with min/max only. — DONE (2026-07-18).** What shipped:
   - `ir::deferrable_probe_scans` (scan_predicates.cpp): eligibility = scan
     occurs once, feeds an inner/single-key/no-predicate join's RIGHT side
     through Project/Rename/**Update** nodes only. Update matters: a
     renaming `select { o_orderkey = l_orderkey }` lowers to
     `Project(Update(Scan))`, NOT Rename — every field must pass
     `is_subset_evaluable_expr`, and a key aliased to a computed expr is
     ineligible. 7 unit tests in test_ir_required_columns.cpp.
   - `DeferredScan`/`DynamicScanFilter`/`ScopedDeferredScans` +
     `materialize_deferred_scan` (interpreter.hpp/.cpp): execution-scoped
     thread-local registry (interrupt-flag pattern — threading a param
     through build_operator's 26 call sites was the rejected alternative).
     `interpret_node`'s Scan case falls back to the deferred registry, so
     ANY consumption path stays correct; slot-not-ready simply means no
     bounds (sound, just unreduced).
   - `ChunkedInnerJoinOperator` deferred-probe mode (chunked.cpp): the right
     subtree is NOT built at operator-construction time (the old
     fall-through interpreted it during build_operator, before bounds could
     exist); the join stores the IR node + interpret context, materializes
     left, publishes bounds, then interprets the right subtree.
   - **Selectivity gate**: bounds are withheld unless the build range prunes
     ≥20% of the source's footer range (uniform estimate) — a near-full
     selection would make project_where gather-decode the non-key columns,
     slower than the dense decode it replaces. No stats → no bounds.
   - Driver wiring in `execute_script`'s evaluate (repl.cpp): analysis after
     `remove_applied_scan_filters`, skip pre-decode for deferred names,
     `ScopedDeferredScans` guard around interpret. Whole-script path only.
   Results: deferral fires on q03(2 scans)/q05(4)/q07(5)/q09(4)/q18(1);
   bounds withheld everywhere on SF-1 (uniform keys span the full domain —
   as expected, and matching DuckDB's own near-useless min/max on q05).
   22/22 answers byte-identical + check_answers.py OK; 1197/1197 tests
   (clang release) + 1191/1191 (g++ strict debug); interleaved 22-query A/B
   geomean 0.969 first run, re-measured hot spots put the honest effect at
   parity-to-slightly-better (~1-3%, likely from the probe scan now decoding
   cache-warm right before the probe). Acceptance met.
2. **Bloom + exact IN-list selection, adaptive escape hatch. — DONE
   (2026-07-19).** What shipped:
   - `JoinBloomFilter` (interpreter.hpp): register-blocked, 16 bits/key
     (4 keys/word, pow2 words), two bits in one 64-bit word, splitmix64
     finalizer — the reverted probe-side design, now at scan altitude.
   - `DynamicScanFilter` gained `in_list` + `bloom` and an inline
     `passes()`. **The Bloom is ALWAYS the probe fast path, even when an
     exact IN-list exists**: `passes` = bloom reject → false, else confirm
     against the list. First cut used `std::binary_search` alone for small
     lists and q17 regressed **+80%** (88% of the whole run in the ~8
     mispredicting branches per probe key × 6M rows); bloom-first flipped
     q17 to **−45%**. The list's job is only to cancel Bloom false
     positives (exactness for dimension chains).
   - Publisher (`publish_build_filter`, chunked.cpp): Bloom over all valid
     build keys always; additionally sort+unique into an exact list when
     the build side is ≤4096 rows and ≤1024 distinct. Membership is
     published UNgated (unlike min/max): set-membership selectivity can't
     be estimated from ranges, so the decision moves to the scan.
   - `project_where` (lazy_table.cpp): optional `(DynamicScanFilter*, key
     name)`; the key joins the predicate decode; membership ANDs into the
     static selection, or builds the selection alone when there are no
     conjuncts. **Escape hatch**: strided (not prefix — key-sorted files
     lie) ~64K sample of the candidate rows; sampled pass rate > 0.75 →
     the filter is abandoned and decode stays dense. Null-keyed rows are
     dropped (single inner join + null keys never match). Selection build
     is ONE filter pass with a sample-based reserve — count-then-fill
     doubled the Bloom cost (~15ms extra on lineitem, measured).
   Results (same-session interleaved A/B vs the stage-1 binary, 7 reps,
   mins, SF-1): **geomean 0.895**. q08 0.53, q17 0.55, q09 0.60, q03 0.80,
   q05 0.83, q07 0.84, q18 0.89, q02 0.94; the rest parity except **q10
   +5% — real, accepted**: q10 is aggregate-dominated (string group-by),
   its joins were already cheap after the static `l_returnflag` conjunct,
   so the ~12ms membership pass isn't recouped; stage 4 (filter inside the
   decoder) is the structural fix. Hit-heavy stress: parity (459 vs 465ms
   mins ≈ the Bloom build + sample for a filter the scan then abandons).
   Pruning observed: lineitem 6M → 51K on q03 (0.9%), 173K on q07 (2.9% —
   DuckDB territory), 365K q09, 976K q05, 133K q10, **399 rows on q18 via
   a 57-key exact IN-list**; orders 1.5M → 135-295K on q03/07/09/10.
   Correctness: 22/22 outputs byte-identical to stage 1, check_answers.py
   all OK, 1207/1207 clang release tests (8 new `[deferred_scan]` runtime
   tests in test_lazy_table.cpp), g++ strict build clean, 1201/1201.
3. **Apply at every eligible join in a chain, not just the fact scan.** The
   dimension edges (region→nation, nation→customer) use the same single-hop
   mechanism; a reduced dimension build then strengthens the fact-scan
   filter transitively — DuckDB's 3.3% vs our expected ~15% on lineitem
   comes precisely from this compounding. **Measured 2026-07-19: this
   already fires per join with zero extra work whenever the author's order
   is a chain** — q03's lineitem Bloom is built from the already-joined
   customer⋈orders result (6M → 51K = the compounded selectivity), and
   q03/q07/q09/q10 all prune orders AND lineitem in the same run. What
   does NOT compound is q05's author order (region applied last, so the
   lineitem Bloom sees date-filtered orders only → 16% not 3.3%) — a
   join-ORDER question, out of scope here; recorded for the reorder cost
   model.
4. **Fuse the filter into the decoder. — DONE (2026-07-19).** What shipped:
   - `KeyFilterScanFn` — an OPTIONAL second callback on `LazyTable`
     (alongside `ColumnDecodeFn`): `(key_name, DynamicScanFilter) ->
     expected<optional<Selection>>`. The inner nullopt means "no fused
     answer" (unsupported column type, or the filter stopped rejecting) and
     the caller falls back to the stage-2 decode-then-filter path, so
     correctness never depends on the fused path existing. This IS a plugin
     interface addition — LazyTable grew a member, so every plugin needs a
     rebuild (the Table(n) ABI gotcha; a stale parquet.so segfaulted
     instantly in cache_ lookup during verification).
   - `filtered_key_selection_impl` (parquet.hpp): per row group, skip
     unread when the chunk's footer range is disjoint from the build keys'
     raw bounds; otherwise ReadBatch the key pages and evaluate
     `filter.passes` as values leave the decoder, emitting passing row
     indices. Null keys fail. INT32/INT64 physical only (sign-extension
     identical to the ordinary decode); nested/unsigned-32 decline. An
     in-decoder escape hatch mirrors the runtime's: past 256K scanned rows
     at >0.75 running pass rate, give up (bounded waste: a couple of
     groups' key decode, and the fallback re-decodes the key which the
     join demanded anyway).
   - `project_where` takes the fused path only when membership is present,
     conjuncts are empty, AND the key is not already cached whole-file (a
     cached key means the in-memory pass is cheaper than re-reading pages
     — the q17 double-instance case).
   - Publisher/consumer split: `publish_build_filter` now records raw
     min/max always (facts); `materialize_deferred_scan` owns the ≥20%
     pruning gate for synthesized conjuncts and skips bounds entirely when
     membership exists (policy). The fused scan uses the raw bounds for
     row-group skipping, which has no gather downside.
   Results (interleaved A/B vs the stage-2 binary, 7 reps, mins, SF-1):
   **geomean 0.984** — q17 0.84, q08 0.92, q05 0.95, q02/q19 0.97, rest
   parity (q12/q22 re-measured at 9 reps: noise); hit-heavy stress 436 vs
   435ms. The fused pass shows up as ~13% of q05 (page decode + Bloom,
   fused) replacing the ~19% it superseded (whole-column key decode +
   alloc + separate Bloom pass). Cumulative vs the pre-plan baseline:
   **geomean ≈ 0.88**. Correctness: 22/22 byte-identical, check_answers
   OK, 1211/1211 clang release (4 new fused-path tests), g++ strict clean
   1205/1205. Conjunct-carrying scans (q03/q07/q10 lineitem) still use
   the stage-2 path — folding static conjuncts into the decoder is
   decode-fusion Stage 4 (see project_decode_fusion), not this plan.

## Expected outcome

q05: scan 67 → ~20-25ms (key col + ~910K selected rows × 3 cols), probe 59 →
~15-20ms (Bloom selection pass + ~1M-row probe): **~180 → ~100-110ms**.
Remaining gap to DuckDB's 68ms is decode throughput (multithreading plan) and
stage-3 compounding. q07/q09/q18 should move in the same direction; flat-join
queries must not move at all.

**Measured after stage 2**: q05 isolated s2 stage −40ms (whole-process mins
188 → 148); whole q05 −17%, in line once the un-compounded 16% pass rate
(see stage 3) is accounted for. The residual per-scan cost stage 4 would
attack: the key column still decodes whole-file, and the Bloom pass itself
is ~2.5ns/key (~15ms over lineitem).

## Guards, traps, dead ends (mostly already paid for — don't re-pay)

- **Do not filter at the probe operator.** Measured −3.4% and reverted; see
  parent plan. The filter must gate *materialization*, not the hash lookup.
- **Two join implementations**: `join_table_impl`/`build_indices_from_right_scan`
  (join.cpp) and `ChunkedInnerJoinOperator` (chunked.cpp). The whole-script
  batch path — the bench default and the only path where the driver can
  defer decode — uses the chunked operator; target ONLY it. The REPL
  statement path pre-decodes per statement and runs no canonicalize; it
  cannot benefit and must not be touched.
- **LazyTable cache poisoning**: `project_where` deliberately bypasses
  `cache_`; a dynamically-selected column must never be cached as a
  whole-file column. The deferred path inherits this for free if it goes
  through `project_where` — do not add a cache "optimization" there.
- **Source-scanned-once proof**: a deferred scan consumed by anything else
  (self-join, second reference) is unsound to reduce. `split_scan_instances`
  runs first and gives per-instance identity; require it.
- **Ordering contract**: `Selection` is ascending → probe-scan order is
  preserved → composes with 82c391f. Any future non-ascending selection
  breaks downstream join locality silently.
- **Validity**: selective decode must carry null bitmaps through unchanged —
  the q14 lesson (only validity-PRESERVING kernels may be spliced) applies to
  the selection pass too.
- **Measurement**: same-session interleaved A/B only; pin
  `parquet_sf1` paths; whole-query harness can't see decoder changes —
  isolate the scan statement (parent plan's staged harness, fed via STDIN —
  file-arg scripts get dead-code-eliminated by the batch planner).
- **Perf-loop scripts via stdin** (see `project_whole_script_bench_default`
  memory): repeated unused `let`s are DCE'd on the file-arg path.

## Explicitly out of scope

- Join reordering changes (the ≤5-relation guard stays; stage 3 records
  findings for the cost model rather than acting on them).
- Multi-key joins (q05's supplier edge) — the composite-int-key path is a
  separate follow-up once single-key lands.
- Plugin ABI changes — none were needed for stages 1-3. Stage 4 added the
  optional `KeyFilterScanFn` to `LazyTable` (member layout change → every
  plugin .so must be rebuilt against the new headers; sources that don't
  supply one are untouched behaviorally).
