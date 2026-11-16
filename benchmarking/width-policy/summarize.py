#!/usr/bin/env python3
"""Summarize balanced serial/parallel pairs from ibex_bench_width."""
import argparse
import csv
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ab_queries import wilcoxon_p


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("samples", nargs="+", type=Path)
    args = parser.parse_args()
    print("mode rows width out_rows cores serial_ms parallel_ms delta% p whole_delta% cpu_ratio tasks cell_floor pipelines verdict")
    for path in args.samples:
        cases = {}
        for row in csv.DictReader(path.open()):
            key = tuple(row[k] for k in ("mode", "rows", "width", "output_rows", "cores"))
            pair = cases.setdefault(key, {}).setdefault(int(row["repeat"]), {})
            if row["side"] in pair:
                raise ValueError("Duplicate side in pair")
            pair[row["side"]] = row
        if not cases:
            raise ValueError(f"No samples: {path}")
        for key, pairs in cases.items():
            if any(set(p) != {"serial", "parallel"} for p in pairs.values()):
                raise ValueError("Incomplete serial/parallel pair")
            before = [float(p["serial"]["ms"]) for p in pairs.values()]
            after = [float(p["parallel"]["ms"]) for p in pairs.values()]
            ratio = statistics.median(b / a for a, b in zip(before, after))
            pvalue = wilcoxon_p([b - a for a, b in zip(before, after)])
            whole = statistics.median(float(p["parallel"]["total_ms"]) / float(p["serial"]["total_ms"]) for p in pairs.values())
            cpu = statistics.median(float(p["parallel"]["cpu_ms"]) / float(p["serial"]["cpu_ms"]) for p in pairs.values())
            tasks = sorted({int(p["parallel"]["tasks"]) for p in pairs.values()})
            floor = sorted({int(p["parallel"].get("cell_floor", 0)) for p in pairs.values()})
            pipelines = sorted({int(p["parallel"].get("pipelines", 0)) for p in pairs.values()})
            verdict = "same" if abs(ratio - 1) < .02 else "unclear" if pvalue >= .05 else "faster" if ratio < 1 else "slower"
            print(*key, f"{statistics.median(before):.3f}", f"{statistics.median(after):.3f}",
                  f"{100*(ratio-1):+.1f}", f"{pvalue:.4f}", f"{100*(whole-1):+.1f}", f"{cpu:.2f}", tasks, floor, pipelines, verdict)
    print("Per-case tests are exploratory; confirm selected effects independently before tuning policy.")
    print("For emission, cpu_ratio covers the whole query; ms isolates Emission. pool_ms in the raw data is summed worker wall time, not process CPU.")


if __name__ == "__main__":
    main()
