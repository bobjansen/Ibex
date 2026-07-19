# Multi-way join chain performance: findings and plan

Status: one lever landed (probe-order-preserving joins, commit 82c391f), the
rest is open. Date: 2026-07-18.
Baseline: PDS-H/TPC-H SF-1, single-threaded, `build-release/`, `:run` (whole-script
batch planner).

## Why this is next

The general benchmark suite (`benchmarking/results/scales_aws_*`) shows Ibex
dominating single-threaded competitors: geomean 0.12x duckdb-st (~8x faster),
0.45x polars-st, 0.25x datafusion-st, at 50M rows. PDS-H tells a different
story: geomean ~1.16x duckdb-st, ~1.18x polars-st at SF-1 — close to parity,
occasionally losing outright. The two benchmarks are not measuring the same
thing: the suite's join cases (`inner_join_symbol`, `inner_join_user`,
`null_left_join`, etc.) are all a **single join directly on a freshly-scanned
column**. PDS-H chains 4-8 joins per query. That structural difference is
where this investigation started.

## What's confirmed and landed

**The mechanism**: `join_table_impl`'s `build_indices_from_right_scan`
(join.cpp) and `ChunkedInnerJoinOperator`'s swapped mode (chunked.cpp) — two
independent implementations of the same small-build-side/large-probe-side
join — both reassembled matched output rows grouped by build-key instead of
the probe side's scan order, even though the probe side is already visited in
order once. This silently permutes a join's output away from whatever order
its input had (parquet scan order, or an upstream join's own probe order),
which then hurts cache locality on any downstream join that probes that
output. `SPEC.md`'s ordering-constraints section is explicit that "any join
drop[s] ordering unless the implementation can prove a specific order" — nothing
was actually guaranteed by the old grouping, it was purely incidental, and
four join tests had pinned it as if it were a contract.

**Isolated proof**: staging `q05`'s join chain into named intermediates showed
`s2 = s1 join lineitem` (probing 6M rows against a 227K-row hash table
built from `s1 = customer join orders`) cost ~65-73ms, while joining the
identical row count/schema from a **freshly-scanned** table cost ~51-61ms.
Sorting `s1` by the next join's key before probing recovered the fast-path
speed — confirming the cause was `s1`'s row order (permuted into
customer-key groups), not row count or column width. A/B was run with both
orderings of the operations (to rule out warm-cache position bias) before
trusting it.

**Fix**: both helpers now emit output rows in probe-scan order (cheaper too —
it drops a full reassembly pass). Landed in 82c391f.

**End-to-end effect, same-session A/B across all 22 queries** (stash/rebuild/
measure, restore/rebuild/measure, so both readings share the same box load):

| query | before (ms) | after (ms) | delta |
|---|---:|---:|---:|
| q05 | 196.2 | 182.4 | **-7.0%** |
| q18 | 417.1 | 399.6 | **-4.2%** |
| q09 | 289.0 | 277.8 | **-3.9%** |
| q07 | 126.3 | 122.1 | **-3.4%** |
| q02, q13, q20 | — | — | +1.7 to +4.9%, but re-measured with 6 more runs each — noise (WSL2 timing drift; see `benchmark_interleaved_methodology` memory) |
| geomean, all 22 | | | -0.4% (a wash) |

The gain concentrates exactly where the mechanism predicts: queries with 4+
chained joins (q05, q07, q09, q18). Queries with 1-2 joins are flat, because
there's no chain to benefit from, and the whole-script batch planner's
existing optimizations (shared-binding materialization, predicate pushdown)
already absorb part of what this fixes elsewhere.

## What this did NOT fix — the residual gap

q05, q07, and q11 remain the worst PDS-H queries relative to duckdb-st/
polars-st even after this landed (roughly 2-2.5x slower on q05/q07 alone).
The row-order penalty this session fixed was maybe 5-10% of their cost, not
the majority of it.

### q05 and q07 already have hand-tuned join order — this is not a reordering problem

`src/ir/join_order.cpp:265` caps automatic cost-based join reordering at 5
relations (`kMaxReorderRelations`); beyond that it returns `std::nullopt` and
falls back to "respect what the author wrote" (the code comment documents
*why*: on a wide snowflake the greedy walk can seed from a small unfiltered
dimension and defer a selective filter it can't see the pruning of — q08 at 8
relations reorders to a plan that rates cheap but runs ~11% slower).

q05 has 6 relations (customer, orders, lineitem, supplier, nation, region);
q07 has 6 (customer, cust_nation, orders, lineitem, supplier, supp_nation).
Both exceed the guard. But both query files' own comments show the *author*
already hand-arranged the join order to be selective-first — q07's header
comment: "q07 has six relations, beyond the planner's conservative
automatic-reorder limit, so source order matters: this applies the selective
two-nation edge before orders and lineitem instead of building a large
fact-table intermediate." q05 similarly hand-picks a two-column supplier-
locality join before nation/region.

**Implication**: don't spend time re-deriving "is the join order bad" for
these two queries — it was deliberately pre-optimized around the guard already
existing. The residual cost is in raw per-stage execution: materialization
between join stages 3-5 relations deep, not which order they run in. That's a
different, harder problem — see "Next steps" below.

### q11 is a different shape — double-run suspicion RESOLVED (2026-07-18)

q11 only has 3 relations (partsupp, supplier, nation) — well within the
reorder guard, so if it's slow the cause is unrelated to join ordering. The
suspicion was that `german_supply` (the 3-way join, bound once, referenced
twice — outer group-by + uncorrelated `scalar(...)` subquery) might be
silently re-run twice. **Checked in code: it is materialized once.** The
whole-script path (`lower_script`, lower.cpp:1267) sets
`share_repeated_bindings_`; `count_table_refs` descends into call arguments
so the reference inside `scalar(german_supply[...])` counts as a second
table-position ref (lower.cpp:423-426, 441-442); q11's binding passes every
condition of the sharing gate at lower.cpp:1347 (single non-`mut` let, ref
count 2, joins ⇒ `contains_expensive_node`). It lands in `shared_bindings_`,
the executor materializes it once, and the second reference lowers to
`Scan(name)` against the cached table. If q11 needs attention it needs a
fresh hypothesis — most likely the aggregate over the 800K-row join output,
not the joins.

## q05 stage profile — DONE (2026-07-18, post-fix)

Staged harness (q05 rewritten as named intermediates `s1..s5`, fed to the
REPL over **stdin** with `:timing on` first), mins over 7 runs, SF-1:

| stage | min (ms) | share |
|---|---:|---:|
| lineitem parquet scan + 4-col select | 66.8 | ~40% |
| `s2 = s1 join lineitem on o_orderkey` (6M probe, 227K build, 910K out) | 58.7 | ~35% |
| `s3 = s2 join supplier on {l_suppkey, c_nationkey}` (2-key, 910K→36K) | 16.7 | ~10% |
| orders date filter | 11.0 | ~7% |
| `s1 = customer join orders` | 9.9 | ~6% |
| s4/s5/aggregate/order | <1 each | — |

**Harness trap**: passing the script as a file argument runs the whole-script
batch planner, which dead-code-eliminates unused intermediates — a 15×-join
perf loop silently measured ONE join. Pipe via stdin to get per-statement
execution.

**Inside s2** (perf record over 15 repeated joins, statement path): the plan's
prior "prime suspect: per-stage materialization" is **disconfirmed**. The cost
is the raw hash probe itself:

- `ChunkedInnerJoinOperator::next()` = 60% of join time, and `perf annotate`
  shows one tight cluster: the robin_hood `find` inner loop (hash mix +
  info-byte compare + bucket load stalls). Phase-1 probing 6M keys against a
  227K-entry map (~4-7MB, exceeds L2) at ~10ns/probe ≈ ~35ms.
- `gather_column` output assembly = 18% (~11ms) — modest, already
  probe-ordered.
- `li`/`ri` are built as `std::vector(total, 0)` → explicit zero-fill
  (`_M_fill_insert` ~4% despite NoInitAllocator; construct empty +
  `resize(total)` to get the no-init path), `hits` has no reserve, and
  `emit_swapped` deep-copies all build-side columns before assembly. A few
  ms total; cheap cleanups, not the story.
- Only 15% of the 6M probes match (910K hits) — 85% are misses paying a
  full cache-missing lookup each.

**Implications, in value order**: (a) the lineitem *decode* is the single
biggest stage — the multithreading roadmap item attacks it directly (profile
shows Arrow `DictionaryConverter<long,int>` decode, i.e. dict-encoded int64
columns); (b) the probe loop wants either batched probing with software
prefetch, or a small build-side Bloom filter (227K keys ≈ 512KB, L2-resident)
to reject the 85% misses before touching the big map — the latter is also the
natural seed of DuckDB-style dynamic filter pushdown (step 3 below);
(c) s3's 2-key path is second-tier (17ms) — check it reuses the composite
int-key packing before optimizing anything there.

### Bloom prefilter on the swapped probe — TRIED, marginal (2026-07-18)

Implemented in `ChunkedInnerJoinOperator::emit_swapped` (int keys): a
register-blocked Bloom filter (16 bits/key, two bits in one word, splitmix64
finalizer) checked before `heads.find`. Same-session interleaved A/B, mins:

- **Always-on**: s2 (15% hit rate) 55.7→52.7ms, only **−5.4%** — and a
  100%-hit stress join (unfiltered orders⋈lineitem, 1.5M-key build → 4MB
  filter) regressed **+15-20%**. Not landable naked.
- **Adaptive gate** (probe the first 64K rows against the map directly; build
  and use the Bloom only if sampled hit rate < 50%): hit-heavy and s1 back to
  neutral, s2 57.3→55.3ms (**−3.4%**). Correct: 1190/1190 tests,
  q05/q07/q09/q18 CSVs byte-identical.
- **Whole-query**: a wash (q05 flat, q07/q09 +4%, q18 −5% — all inside this
  box's noise band).

**Why the model overestimated the win**: a robin_hood flat-map *miss* usually
terminates on the info-byte load of a single cache line — one L3 miss, not a
chain walk. The Bloom converts that L3 miss into an L2 hit but adds a hash +
branch per probe row; net a few ns saved on 85% of rows, ~2-3ms of a 55ms
stage. The Bloom idea's real payoff is NOT at the probe — it's pushing the
filter into the lineitem *scan* (semi-join reduction during decode, before 4
columns × 6M rows are materialized), i.e. step 3's dynamic filter pushdown,
which attacks the 67ms decode stage as well.

Verdict: not worth the ~70 lines for ~1% end-to-end; **REVERTED** (numbers
kept here). The `JoinBloomFilter` design (register-blocked, 16 bits/key, two
bits in one word, splitmix64) is reusable if scan-side pushdown wants it.

### DuckDB EXPLAIN ANALYZE comparison — DONE (2026-07-18): it's dynamic filter pushdown

DuckDB 1.5.4, `SET threads=1`, same SF-1 parquet files, warm mins:
**q05 68ms** (Ibex ~182ms, 2.7x), **q07 78ms** (Ibex ~122ms, 1.6x). The
q05 profile tree shows exactly where the gap lives — not smarter joins,
smaller inputs:

- **lineitem TABLE_SCAN carries `l_orderkey IN BF (o_orderkey)`** (a Bloom
  filter built from the orders-side hash join build) plus derived min/max
  bounds — the scan emits **196,484 of 6,001,215 rows (3.3%)**. The scan
  costs 0.04s and the join above it 0.01s. Ibex decodes all 6M rows × 4
  columns (67ms) and probes all 6M (59ms).
- The BF is doubly effective because its build side was already reduced by
  the *other* dynamic filters: customer's scan gets an exact
  `c_nationkey IN (8,9,12,18,21)` (the ASIA nations, computed from the
  region⋈nation build) → 94,724 rows; orders gets `o_custkey` min/max
  bounds; nation gets `n_regionkey=2`. The dimension restrictions flow into
  the fact scans without any exotic join order.
- q07 same story: `o_orderkey IN BF` on orders (145.7K rows out),
  `c_nationkey IN BF` on customer (12K rows out).

**Conclusion**: the residual q05/q07 gap is one feature — sideways
information passing: after building a join's hash table on the small side,
derive {min, max, IN-list-or-Bloom} from the build keys and push it into the
probe side's parquet scan. This composes with existing pieces: fused bounds
in scan filters (q06 work) gives the min/max plumbing, decode-fusion Stage 4
(conjuncts into the decoder) is the natural evaluation point, and the failed
probe-side Bloom above explains *why* it must sit in the scan: filtering
after decode saves ~2ms; filtering during decode saves most of 67ms of
materialization plus most of 59ms of probing. Rough headroom: q05 ~180 →
~90-100ms. Worth scoping as its own plan.

## Next steps, ranked

1. ~~**Profile q05's actual join stage costs post-fix.**~~ DONE — see the
   stage profile section above. Residual is scan decode + probe-loop
   throughput, NOT inter-stage materialization.
2. ~~**Check q11's double-reference materialization.**~~ DONE — computed
   once via shared-binding materialization; see the q11 section above.
3. ~~**Compare Ibex's chosen q05/q07 execution plan against DuckDB's.**~~
   DONE — see the EXPLAIN ANALYZE section above. The prediction was exactly
   right: DuckDB wins via a Bloom filter propagated from the join build side
   into the `lineitem` scan (plus exact IN-lists/min-max on the dimension
   scans). Next action: scope **dynamic filter pushdown into parquet scans**
   as its own plan.
4. **Do not** re-attempt "always sort the intermediate before the next join"
   as a general strategy — this was tried and explicitly rejected mid-session:
   sorting a 910K-row intermediate cost 20.85ms, more than the entire next
   join stage it would have sped up. The only sound fix is what's landed:
   never permute away probe order in the first place, not correcting for it
   after the fact.

## Dead ends / traps (don't re-derive)

- The historical PDS-H tsv baseline (`benchmarking/tpch/results/*.tsv`) is
  **not** safe to diff against fresh measurements taken in a different
  session — box load varies enough (this session's first same-tsv comparison
  overstated q07's win by ~10x, 219ms→121ms vs the real same-session
  126ms→122ms) that only same-session, same-load A/Bs are trustworthy. See
  `benchmark_interleaved_methodology` memory.
- The suite's join microbenchmarks (`inner_join_*`, `null_*_join`,
  `tf_asof_join*`) are not representative of PDS-H's join cost shape — they
  are all single joins against a freshly-scanned column, never a chain. Don't
  use suite join numbers as a proxy for chain behavior.
- `kMaxReorderRelations = 5` is a deliberate, documented guard (q08 regression
  at 8 relations), not an oversight — don't "fix" it by just raising the
  limit without the join-graph-aware statistics the comment says are missing.
