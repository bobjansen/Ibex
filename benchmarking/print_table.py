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
    "pandas",
    "data.table",
    "dplyr",
]

QUERY_ORDER = [
    # group-by / aggregation
    "mean_by_symbol",
    "ohlc_by_symbol",
    "count_by_symbol_day",
    "mean_by_symbol_day",
    "ohlc_by_symbol_day",
    "sum_by_user",
    # update / column ops
    "update_price_x2",
    "cumsum_price",
    "cumprod_price",
    "rand_uniform",
    "rand_normal",
    # filters
    "filter_simple",
    "filter_and",
    "filter_arith",
    "filter_or",
    "filter_events",
    # joins
    "null_left_join",
    "null_semi_join",
    "null_anti_join",
    # reshape
    "melt_wide_to_long",
    "dcast_long_to_wide",
    # fill / null propagation
    "fill_null",
    "fill_forward",
    "fill_backward",
    # time-series (ibex only)
    "tf_lag1",
    "tf_rolling_count_1m",
    "tf_rolling_mean_5m",
    "tf_rolling_sum_1m",
    "tf_resample_1m_ohlc",
]

QUERY_LABEL = {
    "mean_by_symbol":       "mean by symbol",
    "ohlc_by_symbol":       "OHLC by symbol",
    "update_price_x2":      "update price×2",
    "cumsum_price":         "cumsum price",
    "cumprod_price":        "cumprod price",
    "rand_uniform":         "rand uniform",
    "rand_normal":          "rand normal",
    "count_by_symbol_day":  "count by symbol×day",
    "mean_by_symbol_day":   "mean by symbol×day",
    "ohlc_by_symbol_day":   "OHLC by symbol×day",
    "filter_simple":        "filter simple",
    "filter_and":           "filter AND",
    "filter_arith":         "filter arith",
    "filter_or":            "filter OR",
    "sum_by_user":          "sum by user (100K str)",
    "filter_events":        "filter events (str)",
    "null_left_join":       "left join (50% null)",
    "null_semi_join":       "semi join",
    "null_anti_join":       "anti join",
    "melt_wide_to_long":    "melt wide→long",
    "dcast_long_to_wide":   "dcast long→wide",
    "fill_null":            "fill null (const)",
    "fill_forward":         "fill forward (LOCF)",
    "fill_backward":        "fill backward (NOCB)",
    "tf_lag1":              "tf lag-1",
    "tf_rolling_count_1m":  "tf rolling count 1m",
    "tf_rolling_mean_5m":   "tf rolling mean 5m",
    "tf_rolling_sum_1m":    "tf rolling sum 1m",
    "tf_resample_1m_ohlc":  "tf resample 1m OHLC",
}


def load(paths):
    # avg_data[query][framework] = avg_ms (float)
    # stat_data[query][framework][stat] = float  (min/max/stddev/p95/p99)
    avg_data = defaultdict(dict)
    stat_data = defaultdict(lambda: defaultdict(dict))
    for p in paths:
        with open(p, newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                fw    = row["framework"]
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
    present_queries = [q for q in QUERY_ORDER if any(
        fw in avg_data.get(q, {}) for fw in FRAMEWORK_ORDER)]
    if not present_queries:
        print("No results found.", file=sys.stderr)
        return

    # Which frameworks are present?
    present = [fw for fw in FRAMEWORK_ORDER
               if any(fw in avg_data.get(q, {}) for q in present_queries)]

    col_w = max(len(QUERY_LABEL.get(q, q)) for q in present_queries)
    fw_w  = {fw: max(len(fw), 8) for fw in present}

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
        print(f"{label:<{col_w}} | " + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present))

    print()

    # ── p99 tail-latency table ────────────────────────────────────────────────
    # Only print if we have p99 data for at least one cell
    has_p99 = any(
        stat_data.get(q, {}).get(fw, {}).get("p99_ms") is not None
        for q in present_queries for fw in present
    )
    if has_p99:
        print("## Tail latency — p99 per query  (ms, lower is better)\n")
        print(f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present))
        print_divider()
        for q in present_queries:
            cells = {}
            for fw in present:
                p99 = stat_data.get(q, {}).get(fw, {}).get("p99_ms")
                cells[fw] = fmt(p99)
            label = QUERY_LABEL.get(q, q)
            print(f"{label:<{col_w}} | " + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present))
        print()

    # ── Noise table (stddev) ─────────────────────────────────────────────────
    has_stddev = any(
        stat_data.get(q, {}).get(fw, {}).get("stddev_ms") is not None
        for q in present_queries for fw in present
    )
    if has_stddev:
        print("## Run-to-run noise — stddev per query  (ms, lower is better)\n")
        print(f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present))
        print_divider()
        for q in present_queries:
            cells = {}
            for fw in present:
                sd = stat_data.get(q, {}).get(fw, {}).get("stddev_ms")
                cells[fw] = fmt(sd)
            label = QUERY_LABEL.get(q, q)
            print(f"{label:<{col_w}} | " + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present))
        print()

    # ── Speedup summary ───────────────────────────────────────────────────────
    print("## Speedup over ibex (geometric mean across available queries)\n")
    for fw in present:
        if fw in ("ibex", "ibex+parse"):
            continue
        ratios = []
        for q in present_queries:
            ibex_v = avg_data.get(q, {}).get("ibex")
            fw_v   = avg_data.get(q, {}).get(fw)
            if ibex_v and fw_v and fw_v > 0:
                ratios.append(ibex_v / fw_v)
        if ratios:
            gm = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
            if gm > 1:
                print(f"  {fw:<14}  {fw} is {gm:.1f}× faster than ibex  (over {len(ratios)} queries)")
            else:
                print(f"  {fw:<14}  ibex is {1/gm:.1f}× faster than {fw}  (over {len(ratios)} queries)")
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
