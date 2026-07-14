#!/usr/bin/env python3
"""Print a markdown comparison table from benchmarking/tpch/results/*.tsv.

Usage:
  python3 print_table.py [results/*.tsv]
"""
import csv
import math
import pathlib
import sys
from collections import defaultdict

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
FRAMEWORK_ORDER = ["ibex", "polars", "polars-st"]
QUERY_ORDER = ["q01", "q03", "q05", "q06", "q09", "q10", "q13", "q19"]


def load(paths):
    avg_data = defaultdict(dict)
    for p in paths:
        with open(p, newline="") as f:
            for row in csv.DictReader(f, delimiter="\t"):
                fw, query = row.get("framework"), row.get("query")
                if not fw or not query:
                    continue
                try:
                    avg_data[query][fw] = float(row["avg_ms"])
                except (ValueError, KeyError):
                    continue
    return avg_data


def fmt(v):
    if v is None:
        return "—"
    if v >= 1000:
        return f"{v / 1000:.2f} s"
    return f"{v:.1f} ms"


def main():
    paths = [pathlib.Path(p) for p in sys.argv[1:]] or sorted((SCRIPT_DIR / "results").glob("*.tsv"))
    if not paths:
        sys.exit("no results found — run bench_ibex.py / bench_polars.py first")
    avg_data = load(paths)

    present_queries = [q for q in QUERY_ORDER if q in avg_data]
    present_fw = [fw for fw in FRAMEWORK_ORDER if any(fw in avg_data[q] for q in present_queries)]
    if not present_fw:
        sys.exit("no recognized framework columns in results")

    col_w = 6
    fw_w = {fw: max(len(fw), 9) for fw in present_fw}

    print(f"{'query':<{col_w}} | " + " | ".join(f"{fw:>{fw_w[fw]}}" for fw in present_fw))
    print("-" * col_w + "-+-" + "-+-".join("-" * fw_w[fw] for fw in present_fw))
    for q in present_queries:
        cells = [fmt(avg_data[q].get(fw)) for fw in present_fw]
        print(f"{q:<{col_w}} | " + " | ".join(f"{c:>{fw_w[fw]}}" for c, fw in zip(cells, present_fw)))

    print()
    for fw in present_fw:
        if fw == "ibex":
            continue
        ratios = [
            avg_data[q]["ibex"] / avg_data[q][fw]
            for q in present_queries
            if "ibex" in avg_data[q] and fw in avg_data[q] and avg_data[q][fw] > 0
        ]
        if ratios:
            gm = math.exp(sum(math.log(r) for r in ratios) / len(ratios))
            print(f"{fw} is {gm:.1f}x faster than ibex (geomean over {len(ratios)} queries)")


if __name__ == "__main__":
    main()
