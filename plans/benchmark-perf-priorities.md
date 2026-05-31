# Benchmark Performance Priorities

Source: AWS run `20260531T124900` / commit `a215ec3`, r7i.2xlarge, scales 1M→16M,
44 queries, 9 frameworks (`benchmarking/results/scales_aws_20260531T124900.csv`).

Standing: ibex is the fastest framework in 103 of the cells measured and lands
within 2× of the best in ~70% of them (median rank 2). The work below is about
the cells where it *doesn't*, plus trimming the suite so reruns are cheap.

## P0 — Allocation / page-fault cliff (systemic, highest ROI)

**Symptom.** The cheapest O(n) ops scale *worst*. Fitting an exponent to
1M→16M (16× data) gives:

| query | scaling exp | should be |
|---|---|---|
| cumsum_price | 1.60 | 1.0 |
| update_price_x2 | 1.58 | 1.0 |
| rand_uniform | 1.53 | 1.0 |
| fill_backward | 1.51 | 1.0 |
| tf_lag1 | 1.48 | 1.0 |
| filter_simple | 1.42 | 1.0 |

Per-row throughput is a **step function, not gradual cache decay**:

```
cumsum_price [ibex]: 1M 0.7ns  2M 0.6ns  4M 0.7ns  | 8M 3.3ns  16M 3.5ns
                                    ~5x cliff at 4M->8M ^
polars (contrast):   1M 5.2ns  2M 4.6ns  4M 4.3ns    8M 4.3ns  16M 4.2ns  (flat)
```

ibex runs at ~0.7 ns/row up to 4M, jumps ~5–10× to ~3.5 ns/row, then is flat
again. A float64 column crosses **~32 MB at 4M rows** — exactly glibc's
`DEFAULT_MMAP_THRESHOLD_MAX`. Above it, freed buffers are returned to the OS and
each result column is a fresh `mmap`, so every first-touch write takes a
zero-fill minor page fault (~3 ns/row). Ops that allocate multiple/wider buffers
(e.g. `fill_backward`) hit the cliff one scale earlier (2M→4M). Polars is flat
because Arrow pools its buffers. Same mechanism the interpreter rolling notes
already document, now visible suite-wide.

**Why it matters.** ibex is *fastest of all frameworks* below the cliff; above
it the lead collapses to "merely competitive" on ~15 queries. Fixing this is one
change that lifts a large fraction of the suite at the scales that matter most.

**Approach (cheap → real fix).**
1. `mallopt(M_TRIM_THRESHOLD / M_MMAP_THRESHOLD, …)` so freed large buffers stay
   resident and get recycled (faulted once). Quick experiment to confirm the
   diagnosis end-to-end.
2. Column-buffer arena / free-list pool keyed by size class (the real fix,
   mirrors Arrow's memory pool). Reuse result-column allocations across ops.
3. Optional: huge pages / explicit pre-fault for the largest columns.

**Validation.** Re-run the cheap-op subset (cumsum, update, rand_uniform,
filter_simple, fill_*, tf_lag1, tf_rolling_sum) at 8M/16M; target flat ns/row
matching the sub-4M figures.

## P1 — `tf_rolling_ewma_1m` rewrite (worst single ratio)

Linear scaling (exp 1.03) but **18.5× slower than polars** (1618ms vs 88ms at
16M). Pure algorithm, not allocation — the P0 fix won't touch it. Single worst
ratio in the suite. Investigate the interpreter/codegen path for ewma; expect a
per-element recurrence that isn't vectorized or is re-deriving weights per row.

## P2 — `tf_asof_join` (slow *and* superlinear)

495ms vs polars-st 45ms = **11×**, and exp 1.25 so it degrades on top of being
slow. Both an algorithmic and a scaling problem — worth profiling after P0 to
separate the allocation component from the join logic.

## P3 — Reshape constant factor (lower priority)

`melt_wide_to_long` 47× vs clickhouse, `dcast_long_to_wide` 5× vs clickhouse,
both ~linear (exp ~1.05). Inherently heavy ops; constant-factor slow rather than
broken. Defer until P0–P2 land.

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
