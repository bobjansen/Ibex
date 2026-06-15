# Benchmark Coverage Plan

Two orthogonal dimensions of work: (A) competitor harness gaps for a one-time stable
reference run, and (B) ibex functionality that has no benchmark at all yet.

SQLite is excluded — it was never meant to compete at this scale and filling its gaps
provides no useful signal.

---

## A. Competitor harness gaps

Queries ibex_bench runs that are missing from one or more competitor harnesses.
Goal: fill these once so we have a stable frozen baseline for all engines.

### Missing from ALL competitors

| Query | Note |
|---|---|
| `dcast_long_to_wide_int_pivot` | ibex_bench runs it; print_table has a label; no harness has it. pandas `pivot_table`, DuckDB `PIVOT`, R `dcast`, ClickHouse `groupArray`+`arrayJoin` |
| `dcast_long_to_wide_cat_pivot` | same — categorical pivot column variant |

### Per-engine gaps

**pandas / polars / polars-st** (`bench_python.py`):
- `dcast_long_to_wide_int_pivot`, `dcast_long_to_wide_cat_pivot`

**DuckDB** (`bench_duckdb.py`):
- `cumprod_price` — `EXP(SUM(LN(price)) OVER (...))` pattern
- `rand_normal`, `rand_int`, `rand_bernoulli` — `randnorm()` / `floor(random()*N)` / `random() < 0.3`
- `tf_rolling_ewma_1m` — recursive CTE or custom UDAF; may omit if too complex
- `dcast_long_to_wide_int_pivot`, `dcast_long_to_wide_cat_pivot`

**DataFusion** (`bench_datafusion.py`):
- `cumprod_price`
- `rand_normal`, `rand_int`, `rand_bernoulli`
- `fill_forward`, `fill_backward` — `LAST_VALUE(x IGNORE NULLS)` window
- `lag_by_symbol` — `ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts)` for stable ordering
- `tf_asof_join`, `tf_asof_join_by_symbol`
- `tf_rolling_ewma_1m`
- `dcast_long_to_wide_int_pivot`, `dcast_long_to_wide_cat_pivot`

**ClickHouse** (`bench_clickhouse.py`):
- `cumprod_price`
- `rand_normal`, `rand_int`, `rand_bernoulli` — `randNormal()`, `randBinomial()` exist
- `lag_by_symbol` — `lagInFrame()` window function
- `tf_asof_join` (global; only `tf_asof_join_by_symbol` is wired)
- `tf_rolling_ewma_1m` — `exponentialMovingAverage()` exists
- `dcast_long_to_wide_int_pivot`, `dcast_long_to_wide_cat_pivot`

**data.table / dplyr** (`bench_r.R`):
- `tf_rolling_ewma_1m` — `TTR::EMA()` for data.table; `slider::slide_dbl()` for dplyr
- `dcast_long_to_wide_int_pivot`, `dcast_long_to_wide_cat_pivot`

---

## B. Ibex functionality not yet benchmarked

### Scalar operations
- **`log_return`** — `prices[update { lr = log(price / lag(price, 1)) }, by symbol]`  
  `log` + `lag` inside `update by`; natural financial compute kernel.
- **`abs_deviation`** — `prices[update { d = abs(price - mean(price)) }, by symbol]`  
  Mixed scalar/aggregate in update.
- **`price_rounded`** — `prices[update { pr = round(price, nearest) }]`
- **`pmin_clip`** — `prices[update { clipped = pmin(price, 500.0) }]`  
  Winsorisation; exercises the vectorised `pmin` path.
- **`where_update_clip`** — `prices[where price > 900.0, update { price = 900.0 }]`  
  Guarded update (keeps cardinality). The ibex equivalent of CASE WHEN in SQL.
  Not benchmarked at all despite being a core language feature.

### Aggregates
- **`first_last_by_symbol`** — `prices[select { open = first(price), close = last(price) }, by symbol]`  
  `first`/`last` standalone (ohlc bundles them with min/max, obscuring cost).
- **`corr_price_vol`** — `trades[corr price, volume]`  
  `CorrNode` implemented but has zero benchmark coverage.
- **`mean_by_symbol_hour`** — `prices[select { avg = mean(price) }, by { symbol, hour(ts) }]`  
  ~3 024 groups; exercises date-part extraction as a group key.

### Vertical concat
- **`rbind_two`** — `rbind(prices_a, prices_b)` (two 2M-row halves)  
  `RbindNode` recently shipped; zero benchmark coverage.

---

## C. Pipelines — new suite

The existing `filter_group_sort` is a three-operator chain, all on a single table.
The goal here is deeper chains, more operator variety, cross-table paths, and
multi-stage aggregations that reflect real analytic workloads.

### Tier 1 — multi-operator single-table chains

These extend `filter_group_sort` in operator count or variety.

**`update_group_filter`**
```
prices
  [update { lr = log(price / lag(price, 1)) }, by symbol]
  [filter lr > 0.0]
  [select { pos_days = count(lr) }, by symbol]
```
*update by → filter on derived column → re-aggregate*. Tests that the interpreter
doesn't re-sort by symbol on the second groupby. Four operators, three distinct
node types.

**`group_rank_filter`**
```
prices
  [update { rk = rank({ price desc }) }, by symbol]
  [filter rk <= 10]
  [select { avg_top10 = mean(price) }, by symbol]
```
*rank within group → filter top-N per group → aggregate survivors*. Common
pattern in quant screening. Window function feeding a filter feeding an aggregation.

**`normalize_by_group`**
```
prices
  [update { z = (price - mean(price)) / std(price) }, by symbol]
  [update { clipped = pmin(pmax(z, -3.0), 3.0) }]
  [select { mean_z = mean(clipped), sd_z = std(clipped) }, by symbol]
```
*z-score normalisation → clip → verify*. Three update stages; the first mixes
scalar and aggregate in a by-group update. Tests that intermediate columns
materialize correctly between stages.

**`rolling_filter_aggregate`**
```
prices (as TimeFrame)
  [update { rm = rolling_mean(price, 5m) }]
  [filter price > rm]
  [select { n_above = count(price), avg_above = mean(price) }, by symbol]
```
*rolling stat → filter on rolling predicate → grouped aggregate*. TimeFrame path
feeding back into a DataFrame operation.

### Tier 2 — join-anchored pipelines

These add a join stage, which forces ibex to materialise an intermediate table
before continuing. Tests that the planner doesn't drop columns needed downstream.

**`join_update_group`**
```
events
  [join users on user_id]
  [update { revenue = amount * user_tier_multiplier }]
  [select { total_rev = sum(revenue) }, by { symbol, user_segment }]
```
*join → scalar update on joined column → aggregate on combined key*.
Mirrors a classic DW pattern: enrich events with dimension data, then roll up.
Requires `users.csv` (already in the bench data).

**`join_filter_rank`**
```
events
  [join users on user_id]
  [filter user_segment == "premium"]
  [update { rk = rank({ amount desc }) }, by symbol]
  [filter rk <= 5]
```
*join → string filter → rank within group → top-N filter*. Tests string
predicate post-join and a subsequent window function.

### Tier 3 — multi-stage aggregation (funnel pattern)

Two-level rollups: aggregate at fine granularity, then aggregate the aggregates.
In SQL this requires a subquery or CTE; in ibex it chains naturally.

**`symbol_day_to_symbol`**
```
prices
  [select { daily_mean = mean(price), daily_vol = std(price) }, by { symbol, day(ts) }]
  [select { mean_of_means = mean(daily_mean), mean_vol = mean(daily_vol) }, by symbol]
```
*symbol×day aggregation → re-aggregate by symbol*. Tests that output columns
from the first `select` are visible as inputs to the second. In SQL this is a
mandatory CTE/subquery; ibex chaining should make it look flat.

**`rolling_resample_aggregate`** (TimeFrame)
```
prices (as TimeFrame)
  [update { rm5 = rolling_mean(price, 5m), rv5 = rolling_std(price, 5m) }]
  [resample 1h ohlc]
  [select { mean_range = mean(high - low) }, by symbol]
```
*two rolling updates → resample to hourly bars → aggregate bar ranges by symbol*.
Three TimeFrame operators feeding a final DataFrame aggregation. Exercises the
TimeFrame→DataFrame handoff at the end of a long chain.

**`log_return_momentum`**
```
prices
  [update { lr = log(price / lag(price, 1)) }, by symbol]
  [update { mom = rolling_mean(lr, 5m) }, by symbol]   -- TimeFrame context
  [select { mean_mom = mean(mom), sharpe = mean(mom) / std(mom) }, by symbol]
```
*log return → rolling mean of returns (momentum) → Sharpe-like ratio per symbol*.
Tests that `log`, `lag`, and `rolling_mean` all compose correctly in a chained
`update by` context and that derived columns survive into the final aggregation.

### Competitor translations for pipeline suite

Each pipeline query should map naturally to:
- **pandas/polars**: chained method calls (`.assign().query().groupby().agg()`)
- **DuckDB/DataFusion/ClickHouse**: CTEs (`WITH t1 AS (...), t2 AS (...)`)
- **data.table/dplyr**: operator chains

The point is not syntactic parity but identical semantics and identical output
so ibex results are verifiable.

---

## Priority order

1. ~~`dcast_long_to_wide_int_pivot` / `cat_pivot` in all competitor harnesses~~ **DONE** (2026-06-14, commit ecd6c0a)
2. ~~`rand_normal/int/bernoulli` + `cumprod_price` for DuckDB/DataFusion/ClickHouse~~ **DONE** (2026-06-14, commit f7b1b29)
3. ~~**Pipeline suite Tier 1**: `update_group_filter`, `group_rank_filter`, `normalize_by_group`~~ **DONE** (2026-06-13, commit eba837d; surfaced + fixed the update+by mixed per-row/aggregate gap)
4. ~~**Pipeline suite Tier 2**: `join_update_group`, `join_filter_rank`~~ **DONE** (2026-06-14). Added `events.symbol` + `users.user_segment`/`user_tier_multiplier` to gen_data.py (regenerate with `--force`); join is parenthesised as the pipeline base in ibex. Mirrored across all harnesses (SQL via CTE). 756 / 1260 rows, consistent cross-engine. Existing inner_join_user/events verifications still pass with the wider schema.
5. **Pipeline suite Tier 3**: ~~`symbol_day_to_symbol`~~ **DONE** (2026-06-14, commit f7b1b29). ~~`log_return_momentum`~~ **DONE** (2026-06-15, commit e14edd0). Added `prices_ts.csv` (symbol, ts:int64-ns, price) to gen_data.py — purely additive, prices.csv baseline untouched; `as_timeframe` promotes the Int ts → Timestamp so no schema hint/plugin needed. ibex_bench auto-derives the path beside `--csv`. Mirrored with identical semantics across all 7 engines; cross-checked byte-identical per-symbol output. Two gotchas: the syntax is `[window 5m, update { mom = rolling_mean(lr) }]` (duration in a `window` clause, not a fn arg); and the first return per symbol is null (lag→null), which **poisons the time-windowed rolling_mean → inf** (known grouped-rolling/null gap), so the query `coalesce(..., 0.0)`s it. ibex @16M: 2.74 s — grouped time-windowed rolling is a future perf target.
6. ~~`where_update_clip` + `pmin_clip` (scalar feature showcase)~~ **DONE** (2026-06-13, commit eba837d)
7. ~~`rbind_two` (recently shipped, zero coverage)~~ **DONE** (2026-06-13, commit eba837d)
8. ~~`corr_price_vol` (CorrNode gap)~~ **DONE** (2026-06-13, commit eba837d)
9. `tf_rolling_ewma_1m` — **R DONE** (2026-06-14; data.table + dplyr via `TTR::EMA(n=1, ratio=0.1)`, matches the pandas/polars full-series `ewm(alpha=0.1, adjust=False)`). ClickHouse still TODO: `exponentialMovingAverage` is a time-windowed aggregate, not a per-row full-series column — needs an arrayFold/recursive workaround; deferred.
10. DataFusion: `lag_by_symbol`, `fill_forward/backward`, `tf_asof_join` — `lag_by_symbol` blocked (no stable row order without a ts/row-id column, same as the existing omission); `fill_*`/`asof` still open.
11. ~~**Scalar math suite** (`abs/sqrt/log/exp/round/floor/ceil` + the transcendental
    set `sin/cos/tanh`)~~ **DONE** (2026-06-15). ibex_bench scalar suite + all
    competitor harnesses (pandas, polars eager/lazy, DuckDB, DataFusion,
    ClickHouse, data.table, dplyr). Paired with vectorising every kernel via
    libmvec AVX2 — see [[benchmark-perf-priorities]] P4 for the lone `tanh` gap.

Remaining: #9 ClickHouse EWMA, #10 DataFusion fill/asof.
