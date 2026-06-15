# Benchmark Performance Priorities

Source: AWS run `20260531T124900` / commit `a215ec3`, r7i.2xlarge, scales 1M→16M,
44 queries, 9 frameworks (`benchmarking/results/scales_aws_20260531T124900.csv`).

Standing: ibex is the fastest framework in 103 of the cells measured and lands
within 2× of the best in ~70% of them (median rank 2). The work below is about
the cells where it *doesn't*, plus trimming the suite so reruns are cheap.

## P0 — Allocation / page-fault cliff — RESOLVED (was a benchmark-env bug)

**Symptom.** The cheapest O(n) ops scaled *worst* (1M→16M exponent ~1.4–1.6 for
cumsum/update/rand_uniform/fill_*/tf_lag1 vs the expected 1.0), as a clean step:

```
cumsum_price [ibex]: 1M 0.7ns  2M 0.6ns  4M 0.7ns  | 8M 3.3ns  16M 3.5ns
                                    ~5x cliff at 4M->8M ^
```

**Root cause (confirmed three ways).** The AWS box was **not using jemalloc**.
`benchmarking/aws/bootstrap.sh` installed its deps inline and never called
`install-deps.sh`, so `libjemalloc-dev` was absent, `find_library()` failed, and
`ibex_bench` linked **glibc malloc**. glibc mmaps every allocation above its
32 MB `DEFAULT_MMAP_THRESHOLD_MAX` (= 4M float64 rows) and munmaps on free, so
each warm iteration re-faults every page of a >32 MB column. Local builds *do*
link jemalloc (`malloc_conf="dirty_decay_ms:-1"` in `tools/ibex_bench.cpp`),
which retains freed pages → flat at every scale, which is why it never reproduced
locally. The 4M→8M step lands exactly on the glibc threshold — a fingerprint
jemalloc lacks. Not an ibex inefficiency.

Evidence: forcing local jemalloc to drop retention
(`MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0,retain:false`) reproduced 3.3 ns/row;
a local glibc rebuild reproduced the cliff (1.2→3.9 ns/row) and `mallopt` flattened
it (→1.3 ns/row); AWS run `20260531T142434` (glibc + mallopt) gave 2–2.8× on every
cheap op.

**Fix (landed).**
- `mallopt(M_MMAP_MAX=0, M_TRIM_THRESHOLD=-1)` once at first `interpret()`, glibc
  only, opt-out `IBEX_NO_MALLOC_TUNING` (commit 8646231). Protects the glibc-only
  user tools (repl/eval/compile) that don't link jemalloc → ~1.4 ns/row.
- `libjemalloc-dev` added to `bootstrap.sh`; CMake now logs found/missing
  (commit 133d8ca). Bench now measures the intended allocator → ~0.9 ns/row.
- Accepted caveat: `M_TRIM_THRESHOLD=-1` never returns memory to the OS, so a
  long-running glibc REPL session retains RSS. Acceptable for ibex's batch-y
  workloads; opt-out flag exists. (Future option: link jemalloc into all tools
  and drop the glibc tuning.)

**Result.** jemalloc ~0.9 ns/row > mallopt-on-glibc ~1.4 ns/row > glibc ~3.5 ns/row.
Cliff closed. Confirm on the next AWS run (bench will now show the jemalloc numbers).

## P1 — `tf_rolling_ewma_1m`: O(n) rewrite — DONE

Was **18.5× slower than polars** (1618ms vs 88ms at 16M) — pure algorithm, not
allocation (the P0 fix moved it only 1.14×), the single worst ratio in the suite.

**DONE** (commit ba985af). The windowed `rolling_ewma` was O(n·w) — it restarted
the recurrence and rescanned the whole window per row. Replaced with a one-pass
O(n) sliding form: `result[i] = α·R_i + (1−α)·β^(i−lo)·col[lo]`, R maintained by
add-right / drop-left, `beta_pow` caching β^k. Algebraically identical (Approx,
both window tests pass), **~15× faster** locally under jemalloc (1543ms → 104ms at
16M), now on par with polars `ewm_mean` (~87ms) vs ~18× behind before. Full suite
green (884 cases).

> **Re-scope after P0.** The allocator fix (run `20260531T142434`, glibc+mallopt
> at 16M) revealed that several "algorithmic" losses were mostly *allocation*:
> `melt_wide_to_long` 1.9× faster, `tf_asof_join` 1.8× faster, just from mallopt.
> `tf_rolling_ewma_1m` moved only 1.14× — it was the one genuinely algorithm-bound
> target (now fixed above). asof/reshape drop in priority; re-measure under
> jemalloc before any code work.
>
> Bench-semantics caveat: ibex's `rolling_ewma` is *time-windowed* (restart per
> window), polars' `ewm_mean(adjust=False)` is *full-series* (O(n), different
> math). Now both are O(n); the cell is a fair speed comparison but not an
> identical-result one — worth a footnote on the web page.

## Validation run `20260531T192851` (81f9836, full 1M–16M) — current standing

Grouped-asof and ClickHouse-materialisation both confirmed @16M:
- **`tf_asof_join_by_symbol` 1040 → 405 ms (2.57×)** — ibex 405 vs data.table 224 /
  polars 258 (~1.7× behind, was 4.5×); beats polars-st (501) and duckdb (616).
- **ClickHouse now materialises**: filter_simple 19→81, filter_arith 27→118,
  melt 21→64, update 5→15; aggregations unchanged (mean_by_symbol 50→52). The
  filter cells were the FORMAT-Null artifact — ibex (63) now *beats* ClickHouse on
  filter_simple.

**The headline framing (same-core).** Ibex is single-threaded. Versus
single-threaded **polars-st @16M, ibex is faster in 37/41 queries.** The 3–4×
"filter gaps" were purely parallelism (ibex beats polars-st on every filter; the
gap is only to 8-core polars). Genuine single-thread algorithmic gaps remaining,
in order:
- **`tf_resample_1m_ohlc` — DONE (commit af823e1).** Was 3.5× vs polars-st (373 vs
  107). resample ran the generic row-wise `aggregate_table` (64M branchy slot
  updates); added a vectorized fast path that reduces each contiguous bucket slice
  with tight auto-vectorising loops (first/last/min/max/sum/mean/count, bucket-only,
  numeric, non-null; generic fallback otherwise). **~3.7× faster** (273→74ms at 16M
  locally), projecting to ~parity with polars-st. Bonus: while here, found the int64
  single-key `aggregate_table` path lacks the sorted-run shortcut the categorical/
  string paths have and over-reserves `rows`/`rows*n_aggs` — a latent cleanup, but
  the dedicated resample path sidesteps it.
- `tf_asof_join` (time-only) 2.2× vs polars-st (114 vs 53) — some headroom left
  in the merge/materialise.
- `tf_rolling_ewma_1m` 1.4×, `fill_null` 1.4× — minor.
Everything else: ibex is fastest single-thread. Multi-thread parallelism is a
separate roadmap item ([[project_execution_roadmap]]), not an algorithmic fix.

Website refreshed (`docs/benchmarks.html`) with a threads/same-core note.

## Validation run `20260531T165259` (021971f, full 1M–16M, on-demand)

All three landed steps confirmed on AWS @16M:
- **P1 ewma**: 1347 → **107 ms** (12.6×), now 1.4× vs polars (was 18.5×).
- **P2 asof (time-only)**: 262 → **114 ms** (2.3×), ~1.7–2× vs polars-st (was 6.3×).
- **P0**: cheap ops flat at ~1–1.6 ns/row, no cliff (jemalloc active).

New genuine targets at 16M (lazy-eval clickhouse cells excluded — ibex is on par
with the real materializing engines on melt/update/fill/filter):
- **`tf_asof_join_by_symbol` — DONE (commit 91c7be4)**. Was 4.5× vs data.table
  (ibex 1040 vs 232; polars 275). Added a single-key factorization fast path
  (hash native key values into a small dictionary, bucket right rows by code,
  per-bucket two-pointer merge — no per-row Key/ScalarValue heap alloc, one cursor
  per group). **~2.3× faster** (914→398ms at 16M locally), projecting to ~460ms on
  AWS — from 4.5× to ~2× behind data.table. Further headroom: hash Categorical by
  code instead of string. Generic multi-key path retained as fallback (now
  covered by a new 2-eq-key test).
- `tf_resample_1m_ohlc` 3.9× vs polars; `filter_events` 3.6× vs polars
  (filter+gather); `dcast` ~2–3×, `ohlc_by_symbol` ~2.6× — moderate.

## P2 — `tf_asof_join` (time-only) — DONE

Re-measured on the jemalloc run `20260531T155759` @16M: ibex **262 ms vs
polars-st 41 / polars 54 = ~5–6×**. data.table 237 ≈ ibex; duckdb 521 slower.
Allocation accounted for ~1.9× of the old glibc number (495→262); the remaining
~6× is genuine join logic (out rows 1.6M). This is the real next target.

## P3 — `ohlc_by_symbol` (emerged under jemalloc)

jemalloc @16M: ibex **172 ms vs clickhouse 62 / datafusion 72 / polars 73 =
~2.6×** (252 groups, 4 aggregates each). Slowest of the columnar engines except
polars-st. Genuine grouped-aggregation gap, moderate.

## Resolved / non-issues under jemalloc

- **melt_wide_to_long — NOT a gap.** ibex 448 ms vs polars 402 / polars-st 466 /
  data.table 1081 (all materializing 64M rows). The clickhouse (20 ms) and
  datafusion (90 ms) cells are lazy/streamed, not real materialization — the
  earlier "47× / 22×" was apples-to-oranges. ibex is competitive. Web-page note:
  prefer polars as the melt reference, not clickhouse.
- **dcast_long_to_wide** — ibex 2948 ms (jemalloc nearly halved the glibc 5460),
  vs datafusion 1219 / duckdb 2267 = ~2.4×, beats polars/data.table. Mid-pack;
  low priority constant-factor work.

## Confirmed strengths (protect against regressions)

- `order_head/tail_topk`: **0.11×** vs polars (~9× faster) — the fused TopK win.
- `tf_rolling_median_1m` 0.50×, rolling_sum/mean ~0.70×, rolling_std 0.86×.
- `rand_bernoulli` 0.30×, `rand_normal` 0.57×, cumprod, count_by_symbol_day,
  cross_join.
- Scalar math builtins (`update {v = f(col)}`): abs/sqrt/log/exp/round/floor/ceil
  and now the full transcendental set (sin/cos/tan/asin/acos/atan/sinh/cosh/tanh/
  log2/log10) all on the libmvec AVX2 path. At 4M `sin` 3.4 vs polars 7.3, `cos`
  3.9 vs 8.1 — ibex faster. The update fast path (`try_fast_update_numeric_expr`)
  is shared by plain update, windowed update, and select-of-expression.

## P4 — `tanh`: the one sub-parity scalar cell

At 4M ibex `tanh` is **9.7 vs polars 6.3 ms (~1.5×)** — the only scalar-math op
where ibex trails polars after the transcendental SIMD work. It already uses
libmvec `_ZGVdN4v_tanh`; polars is faster because its kernel is a cheaper
rational/poly approximation. An `1 - 2/(1+exp(2x))` form over the fast SIMD `exp`
kernel would likely beat polars but is *less* accurate than `std::tanh` (the
codebase distinguishes Approx from exact). Deferred pending a decision on whether
a benchmarked op may trade accuracy for speed. Low priority (single cell, 1.5×).

---

## Suite trimming for cheaper reruns

The full run is ~56 min and the end goal is a single static web page showing all
frameworks. Slow, stable, never-competitive frameworks don't change between ibex
iterations, so their numbers can be **pinned** (stored once, rendered on the page)
rather than recomputed every run. Competitiveness summary:

| framework | qcov | maxN | wins | within-2×% | medRank | tot ms (proxy) |
|---|---|---|---|---|---|---|
| sqlite | 18 | 4M | 0 | **0%** | 8 | 150742 |
| data.table | 34 | 16M | 4 | 13% | 5 | 78259 |
| duckdb | 30 | 16M | **0** | 7% | 5 | 55411 |
| polars-st | 40 | 16M | 15 | 24% | 4 | 54340 |
| ibex | 44 | 16M | 103 | 70% | 2 | 50530 |
| polars | 40 | 16M | 49 | 57% | 2 | 26228 |
| datafusion | 21 | 16M | 10 | 48% | 3 | 11153 |
| clickhouse | 21 | 16M | 35 | 70% | 2 | 6000 |

**Pin (reuse stored numbers, do not rerun):**
- **sqlite** — never within 2× of best, 0 wins, already capped at 4M, and the
  single largest wall-clock contributor. Keep as a recognizable baseline floor on
  the page, but its numbers won't move; pin them.
- **data.table `tf_rolling_std_1m` / `tf_rolling_median_1m`** — `frollapply` is
  O(n·window): 17.1s (median, 1M) and 13.0s (std, 2M) are the two biggest single
  cells in the entire run and dominate wall clock. Keep the rest of data.table
  (it wins 4 cells), but pin these two queries.

**Reduce scale coverage (rerun, fewer points):**
- **duckdb** — 0 wins, only 7% within 2×, median rank 5. A required reference
  (people expect it) but not informative at every scale. Run at 1M / 4M / 16M
  only.

**Always rerun in full (cheap and/or the real competition):**
- ibex (subject), polars + polars-st (primary multi/single-thread rivals),
  clickhouse (6s total, wins 35), datafusion (11s total, wins 10).

Net effect: dropping sqlite reruns + the two data.table frollapply cells removes
the largest wall-clock items (sqlite ~150s proxy, the two data.table cells ~30s
of measured time plus their warmup iterations) without losing any column from the
final page.

> Note: the wall-clock proxy column is the per-iteration `avg_ms` summed across
> all cells; true wall clock also multiplies by warmup+measured iteration counts,
> which makes the slow `frollapply`/sqlite cells dominate even more than the table
> suggests.
