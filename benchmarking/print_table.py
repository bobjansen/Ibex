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
    "mean_by_symbol",
    "ohlc_by_symbol",
    "update_price_x2",
    "count_by_symbol_day",
    "mean_by_symbol_day",
    "ohlc_by_symbol_day",
]

QUERY_LABEL = {
    "mean_by_symbol":      "mean by symbol",
    "ohlc_by_symbol":      "OHLC by symbol",
    "update_price_x2":     "update price×2",
    "count_by_symbol_day": "count by symbol×day",
    "mean_by_symbol_day":  "mean by symbol×day",
    "ohlc_by_symbol_day":  "OHLC by symbol×day",
}


def load(paths):
    # data[query][framework] = avg_ms (float)
    data = defaultdict(dict)
    for p in paths:
        with open(p, newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                fw    = row["framework"]
                query = row["query"]
                try:
                    avg_ms = float(row["avg_ms"])
                except ValueError:
                    continue
                data[query][fw] = avg_ms
    return data


def fmt(v):
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v/1000:.2f} s"
    if v >= 10:
        return f"{v:.1f} ms"
    return f"{v:.2f} ms"




def print_table(data):
    # Which frameworks are present?
    present = [fw for fw in FRAMEWORK_ORDER
               if any(fw in data.get(q, {}) for q in QUERY_ORDER)]
    if not present:
        print("No results found.", file=sys.stderr)
        return

    # Column widths
    col_w = max(len(QUERY_LABEL[q]) for q in QUERY_ORDER if q in data)
    fw_w  = {fw: max(len(fw), 8) for fw in present}

    def row_str(label, cells, sep=" | "):
        parts = [f"{label:<{col_w}}"]
        for fw in present:
            parts.append(f"{cells[fw]:>{fw_w[fw]}}")
        return sep.join(parts)

    header = row_str("query", {fw: fw for fw in present})
    divider = row_str("", {fw: "-" * fw_w[fw] for fw in present}, sep="-+-")
    divider = "-" * len(header.split(" | ")[0]) + "-+-" + "-+-".join(
        "-" * fw_w[fw] for fw in present)

    print()
    print("## Results — avg execution time per query (lower is better)\n")
    print(f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present))
    print("-" * col_w + "-+-" + "-+-".join("-" * fw_w[fw] for fw in present))

    for q in QUERY_ORDER:
        if q not in data and q not in {q2 for fw_d in data.values() for q2 in [q]}:
            continue
        row = data.get(q, {})
        cells = {fw: fmt(row.get(fw)) for fw in present}
        label = QUERY_LABEL.get(q, q)
        print(f"{label:<{col_w}} | " + " | ".join(f"{cells[fw]:>{fw_w[fw]}}" for fw in present))

    print()

    # Summary: geometric mean speedup over ibex
    print("## Speedup over ibex (geometric mean across available queries)\n")
    import math
    for fw in present:
        if fw in ("ibex", "ibex+parse"):
            continue
        ratios = []
        for q in QUERY_ORDER:
            ibex_v = data.get(q, {}).get("ibex")
            fw_v   = data.get(q, {}).get(fw)
            if ibex_v and fw_v and fw_v > 0:
                ratios.append(ibex_v / fw_v)  # > 1 means ibex is slower
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
    data = load(paths)
    print_table(data)


if __name__ == "__main__":
    main()
