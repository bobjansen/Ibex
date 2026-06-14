#!/usr/bin/env python3
"""Read results TSV files and print a markdown comparison table.

Usage:
  python3 print_table.py results/ibex.tsv results/python.tsv results/r.tsv
  python3 print_table.py results/*.tsv
"""
import csv, pathlib, sys
from collections import defaultdict

FRAMEWORK_ORDER = [
    "ibex",
    "ibex+parse",
    "ibex-compiled",
    "polars",
    "polars-st",
    "duckdb",
    "duckdb-st",
    "datafusion",
    "datafusion-st",
    "clickhouse",
    "clickhouse-st",
    "sqlite",
    "pandas",
    "data.table",
    "dplyr",
]

QUERY_ORDER = [
    # group-by / aggregation
    "mean_by_symbol",
    "ohlc_by_symbol",
    "median_by_symbol",
    "quantile_by_symbol",
    "std_by_symbol",
    "count_by_symbol_day",
    "mean_by_symbol_day",
    "ohlc_by_symbol_day",
    "sum_by_user",
    "distinct_symbol",
    # update / column ops
    "update_price_x2",
    "cumsum_price",
    "cumprod_price",
    "rand_uniform",
    "rand_normal",
    "rand_int",
    "rand_bernoulli",
    # scalar row-wise math builtins
    "abs_price",
    "sqrt_price",
    "log_price",
    "exp_price",
    "round_price",
    "floor_price",
    "ceil_price",
    # grouped window functions
    "rank_by_symbol",
    "lag_by_symbol",
    "cumsum_by_symbol",
    # filters
    "filter_simple",
    "filter_and",
    "filter_arith",
    "filter_or",
    "filter_events",
    # sort / top-k
    "sort_price",
    "sort_price_desc",
    "sort_symbol",
    "sort_symbol_price",
    "sort_symbol_price_desc",
    "order_head_topk",
    "order_head_topk_by_symbol",
    "order_tail_topk",
    "order_tail_topk_by_symbol",
    # multi-stage pipeline
    "filter_group_sort",
    "update_group_filter",
    "group_rank_filter",
    "normalize_by_group",
    "symbol_day_to_symbol",
    # transforms / single-pass language features
    "pmin_clip",
    "where_update_clip",
    "rbind_two",
    # statistics
    "corr_price_vol",
    # joins
    "null_left_join",
    "null_semi_join",
    "null_anti_join",
    "inner_join_symbol",
    "inner_join_user",
    "null_cross_join_small",
    "join_update_group",
    "join_filter_rank",
    # reshape
    "melt_wide_to_long",
    "dcast_long_to_wide",
    "dcast_long_to_wide_int_pivot",
    "dcast_long_to_wide_cat_pivot",
    # fill / null propagation
    "fill_null",
    "fill_forward",
    "fill_backward",
    # time-series (cross-engine: ibex, pandas, polars, duckdb, data.table, dplyr)
    "as_timeframe",
    "tf_lag1",
    "tf_rolling_count_1m",
    "tf_rolling_sum_1m",
    "tf_rolling_mean_5m",
    "tf_rolling_median_1m",
    "tf_rolling_std_1m",
    "tf_rolling_ewma_1m",
    "tf_resample_1m_ohlc",
    "tf_asof_join",
    "tf_asof_join_by_symbol",
]

QUERY_LABEL = {
    "mean_by_symbol": "mean by symbol",
    "ohlc_by_symbol": "OHLC by symbol",
    "update_price_x2": "update price×2",
    "cumsum_price": "cumsum price",
    "cumprod_price": "cumprod price",
    "rand_uniform": "rand uniform",
    "rand_normal": "rand normal",
    "rand_int": "rand int",
    "rand_bernoulli": "rand bernoulli",
    "abs_price": "abs",
    "sqrt_price": "sqrt",
    "log_price": "log",
    "exp_price": "exp",
    "round_price": "round (nearest)",
    "floor_price": "floor",
    "ceil_price": "ceil",
    "count_by_symbol_day": "count by symbol×day",
    "mean_by_symbol_day": "mean by symbol×day",
    "ohlc_by_symbol_day": "OHLC by symbol×day",
    "filter_simple": "filter simple",
    "filter_and": "filter AND",
    "filter_arith": "filter arith",
    "filter_or": "filter OR",
    "sum_by_user": "sum by user (100K str)",
    "filter_events": "filter events (str)",
    "null_left_join": "left join (50% null)",
    "null_semi_join": "semi join",
    "null_anti_join": "anti join",
    "inner_join_symbol": "inner join (126 keys)",
    "inner_join_user": "inner join (100K keys)",
    "join_update_group": "join→revenue→rollup",
    "join_filter_rank": "join→filter→rank top-5",
    "null_cross_join_small": "cross join (2k×64)",
    "distinct_symbol": "distinct symbol",
    "median_by_symbol": "median by symbol",
    "quantile_by_symbol": "p90 by symbol",
    "std_by_symbol": "stddev by symbol",
    "rank_by_symbol": "dense rank by symbol",
    "lag_by_symbol": "lag by symbol",
    "cumsum_by_symbol": "cumsum by symbol",
    "filter_group_sort": "filter→group→top-10",
    "update_group_filter": "update→filter→regroup",
    "group_rank_filter": "rank→top-N→aggregate",
    "normalize_by_group": "z-score→clip→aggregate",
    "symbol_day_to_symbol": "symbol×day→symbol rollup",
    "pmin_clip": "pmin clip (winsorise)",
    "where_update_clip": "guarded update (CASE WHEN)",
    "rbind_two": "rbind (vertical concat)",
    "corr_price_vol": "correlation matrix",
    "sort_price": "sort (full)",
    "sort_price_desc": "sort desc (full)",
    "sort_symbol": "sort symbol (full)",
    "sort_symbol_price": "sort symbol,price (full)",
    "sort_symbol_price_desc": "sort symbol,price desc (full)",
    "order_head_topk": "top-k head",
    "order_head_topk_by_symbol": "top-k head by symbol",
    "order_tail_topk": "top-k tail",
    "order_tail_topk_by_symbol": "top-k tail by symbol",
    "melt_wide_to_long": "melt wide→long",
    "dcast_long_to_wide": "dcast long→wide",
    "dcast_long_to_wide_int_pivot": "dcast long→wide (int pivot)",
    "dcast_long_to_wide_cat_pivot": "dcast long→wide (cat pivot)",
    "as_timeframe": "as timeframe",
    "fill_null": "fill null (const)",
    "fill_forward": "fill forward (LOCF)",
    "fill_backward": "fill backward (NOCB)",
    "tf_lag1": "tf lag-1",
    "tf_rolling_count_1m": "tf rolling count 1m",
    "tf_rolling_sum_1m": "tf rolling sum 1m",
    "tf_rolling_mean_5m": "tf rolling mean 5m",
    "tf_rolling_median_1m": "tf rolling median 1m",
    "tf_rolling_std_1m": "tf rolling std 1m",
    "tf_rolling_ewma_1m": "tf rolling EWMA 1m",
    "tf_resample_1m_ohlc": "tf resample 1m OHLC",
    "tf_asof_join": "tf as-of join (10% sampled)",
    "tf_asof_join_by_symbol": "tf as-of join by symbol (10% sampled)",
}


def load(paths):
    # avg_data[query][framework] = avg_ms (float)
    # stat_data[query][framework][stat] = float  (min/max/stddev/p95/p99)
    avg_data = defaultdict(dict)
    stat_data = defaultdict(lambda: defaultdict(dict))
    for p in paths:
        with open(p, newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                # Skip rows from older TSV formats that lack the framework
                # column (e.g. legacy ibex_v2.tsv / regression sweeps), or
                # transient header-only files.
                if "framework" not in row or row["framework"] is None:
                    continue
                fw = row["framework"]
                query = row["query"]
                try:
                    avg_ms = float(row["avg_ms"])
                except ValueError:
                    continue
                avg_data[query][fw] = avg_ms
                for stat in ("min_ms", "max_ms", "stddev_ms", "p95_ms", "p99_ms"):
                    if stat in row:
                        try:
                            stat_data[query][fw][stat] = float(row[stat])
                        except (ValueError, TypeError):
                            pass
    return avg_data, stat_data


def fmt(v):
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v/1000:.2f} s"
    if v >= 10:
        return f"{v:.1f} ms"
    return f"{v:.2f} ms"


def fmt_stat(v):
    """Compact format for secondary stats (stddev, p99)."""
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v/1000:.2f}s"
    if v >= 10:
        return f"{v:.1f}"
    return f"{v:.2f}"


def print_table(avg_data, stat_data):
    import math

    # Which queries have any data?
    present_queries = [
        q
        for q in QUERY_ORDER
        if any(fw in avg_data.get(q, {}) for fw in FRAMEWORK_ORDER)
    ]
    if not present_queries:
        print("No results found.", file=sys.stderr)
        return

    # Which frameworks are present?
    present = [
        fw
        for fw in FRAMEWORK_ORDER
        if any(fw in avg_data.get(q, {}) for q in present_queries)
    ]

    col_w = max(len(QUERY_LABEL.get(q, q)) for q in present_queries)
    fw_w = {fw: max(len(fw), 8) for fw in present}

    def print_divider():
        print("-" * col_w + "-+-" + "-+-".join("-" * fw_w[fw] for fw in present))

    # ── Average time table ────────────────────────────────────────────────────
    print()
    print("## Results — avg execution time per query (lower is better)\n")
    print(f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present))
    print_divider()

    for q in present_queries:
        row = avg_data.get(q, {})
        cells = {fw: fmt(row.get(fw)) for fw in present}
        label = QUERY_LABEL.get(q, q)
        print(
            f"{label:<{col_w}} | "
            + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present)
        )

    print()

    # ── p99 tail-latency table ────────────────────────────────────────────────
    # Only print if we have p99 data for at least one cell
    has_p99 = any(
        stat_data.get(q, {}).get(fw, {}).get("p99_ms") is not None
        for q in present_queries
        for fw in present
    )
    if has_p99:
        print("## Tail latency — p99 per query  (ms, lower is better)\n")
        print(
            f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present)
        )
        print_divider()
        for q in present_queries:
            cells = {}
            for fw in present:
                p99 = stat_data.get(q, {}).get(fw, {}).get("p99_ms")
                cells[fw] = fmt(p99)
            label = QUERY_LABEL.get(q, q)
            print(
                f"{label:<{col_w}} | "
                + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present)
            )
        print()

    # ── Noise table (stddev) ─────────────────────────────────────────────────
    has_stddev = any(
        stat_data.get(q, {}).get(fw, {}).get("stddev_ms") is not None
        for q in present_queries
        for fw in present
    )
    if has_stddev:
        print("## Run-to-run noise — stddev per query  (ms, lower is better)\n")
        print(
            f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present)
        )
        print_divider()
        for q in present_queries:
            cells = {}
            for fw in present:
                sd = stat_data.get(q, {}).get(fw, {}).get("stddev_ms")
                cells[fw] = fmt(sd)
            label = QUERY_LABEL.get(q, q)
            print(
                f"{label:<{col_w}} | "
                + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present)
            )
        print()

    # ── Speedup summary ───────────────────────────────────────────────────────
    print("## Speedup over ibex (geometric mean across available queries)\n")
    for fw in present:
        if fw in ("ibex", "ibex+parse"):
            continue
        ratios = []
        for q in present_queries:
            ibex_v = avg_data.get(q, {}).get("ibex")
            fw_v = avg_data.get(q, {}).get(fw)
            if ibex_v and fw_v and fw_v > 0:
                ratios.append(ibex_v / fw_v)
        if ratios:
            gm = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
            if gm > 1:
                print(
                    f"  {fw:<14}  {fw} is {gm:.1f}× faster than ibex  (over {len(ratios)} queries)"
                )
            else:
                print(
                    f"  {fw:<14}  ibex is {1/gm:.1f}× faster than {fw}  (over {len(ratios)} queries)"
                )
    print()


def main():
    paths = sys.argv[1:]
    if not paths:
        # default: all TSVs in results/
        paths = sorted(pathlib.Path("results").glob("*.tsv"))
    if not paths:
        sys.exit("Usage: print_table.py results/*.tsv")
    avg_data, stat_data = load(paths)
    print_table(avg_data, stat_data)


if __name__ == "__main__":
    main()
