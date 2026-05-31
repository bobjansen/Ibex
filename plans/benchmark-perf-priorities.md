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

## P1 — `tf_rolling_ewma_1m` rewrite (worst single ratio)

Linear scaling (exp 1.03) but **18.5× slower than polars** (1618ms vs 88ms at
16M). Pure algorithm, not allocation — the P0 fix won't touch it. Single worst
ratio in the suite. Investigate the interpreter/codegen path for ewma; expect a
per-element recurrence that isn't vectorized or is re-deriving weights per row.

> **Re-scope after P0.** The allocator fix (run `20260531T142434`, glibc+mallopt
> at 16M) revealed that several "algorithmic" losses were mostly *allocation*:
> `melt_wide_to_long` 1.9× faster, `tf_asof_join` 1.8× faster, just from mallopt.
> `tf_rolling_ewma_1m` moved only 1.14× — it is the one genuinely algorithm-bound
> target. So ewma is the real P1; asof/reshape drop in priority and should be
> re-measured under jemalloc before any code work.

## P2 — `tf_asof_join` (re-measure under jemalloc first)

Was 495ms vs polars-st 45ms = 11× under glibc, but ~1.8× of that was allocation
(now ~281ms with mallopt). Re-measure under jemalloc; if a real gap remains it's
the join logic (exp was 1.25). Lower priority than ewma.

## P3 — Reshape constant factor (lower priority)

`melt_wide_to_long` and `dcast_long_to_wide` were allocation-heavy (melt 1.9×
from mallopt alone). Re-measure under jemalloc; `dcast` barely moved (1.01×) so
that one is genuine constant-factor work. Defer until P1 lands.

## Confirmed strengths (protect against regressions)

- `order_head/tail_topk`: **0.11×** vs polars (~9× faster) — the fused TopK win.
- `tf_rolling_median_1m` 0.50×, rolling_sum/mean ~0.70×, rolling_std 0.86×.
- `rand_bernoulli` 0.30×, `rand_normal` 0.57×, cumprod, count_by_symbol_day,
  cross_join.

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
