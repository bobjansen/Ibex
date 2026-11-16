#!/usr/bin/env python3
"""Run the prepared-input Discovery policy sweep and summarize balanced pairs."""
import argparse
import csv
import io
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "benchmarking"))
from ab_queries import wilcoxon_p


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "build-release/tools/ibex_bench_discovery_policy")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).with_name("results"))
    args, tool_args = parser.parse_known_args()
    if tool_args[:1] == ["--"]:
        tool_args = tool_args[1:]
    command = [str(args.binary), *tool_args]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(command, text=True, capture_output=True)
    (args.output_dir / "samples.csv").write_text(result.stdout)
    (args.output_dir / "stderr.log").write_text(result.stderr)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    (args.output_dir / "command.json").write_text(json.dumps({"argv": command, "revision": revision}, indent=2) + "\n")
    if result.returncode:
        print(result.stderr, file=sys.stderr)
        return result.returncode
    cases = {}
    rows = csv.DictReader(io.StringIO(result.stdout))
    axes = ("rows", "groups", "cores", "operation") if "operation" in (rows.fieldnames or []) else ("rows", "cardinality", "chunk_rows", "cores", "shape", "aggregate")
    core_axis = axes.index("cores")
    for row in rows:
        key = tuple(row[k] for k in axes)
        sides = cases.setdefault(key, {}).setdefault(int(row["repeat"]), {})
        if row["side"] in sides:
            raise ValueError("Duplicate pair side")
        sides[row["side"]] = row
    if not cases:
        raise ValueError("No measurement rows")
    summaries = []
    for key, pairs in cases.items():
        if any(set(p) != {"default", "target"} for p in pairs.values()):
            raise ValueError("Incomplete pair")
        before = [float(p["default"]["ms"]) for p in pairs.values()]
        after = [float(p["target"]["ms"]) for p in pairs.values()]
        cpu = statistics.median(float(p["target"]["cpu_ms"]) for p in pairs.values())
        cpu_before = statistics.median(float(p["default"]["cpu_ms"]) for p in pairs.values())
        ratio = statistics.median(b / a for a, b in zip(before, after))
        pvalue = wilcoxon_p([b - a for a, b in zip(before, after)])
        verdict = "same" if abs(ratio - 1) < 0.02 else "unclear" if pvalue >= 0.05 else "faster" if ratio < 1 else "slower"
        summaries.append({"case": key, "default_ms": statistics.median(before), "target_ms": statistics.median(after),
                          "paired_ratio": ratio, "p": pvalue, "wins": sum(b < a for a, b in zip(before, after)),
                          "target_cpu_ms": cpu, "default_cpu_ms": cpu_before, "verdict": verdict,
                          "tasks": sorted({int(p["target"].get("tasks", 0)) for p in pairs.values()}),
                          "halo_pieces": sorted({int(p["target"].get("halo_pieces", 0)) for p in pairs.values()}),
                          "fields": sorted({int(p["target"].get("fields", 0)) for p in pairs.values()}),
                          "finalizes": sorted({int(p["target"].get("finalizes", 0)) for p in pairs.values()}),
                          "fanouts": sorted({int(p["target"].get("fanouts", 0)) for p in pairs.values()})})
    (args.output_dir / "summary.json").write_text(json.dumps(summaries, indent=2) + "\n")
    print(*axes, "default_ms target_ms delta% p verdict cpu_work_x fanouts tasks halo_pieces")
    for s in summaries:
        key = s["case"]
        one_core = next((x for x in summaries if x["case"][:core_axis] == key[:core_axis] and x["case"][core_axis] == "1" and x["case"][core_axis + 1:] == key[core_axis + 1:]), None)
        work = s["target_cpu_ms"] / one_core["target_cpu_ms"] if one_core and one_core["target_cpu_ms"] else math.nan
        print(*key, f'{s["default_ms"]:.3f}', f'{s["target_ms"]:.3f}',
              f'{100 * (s["paired_ratio"] - 1):+.1f}', f'{s["p"]:.4f}', s["verdict"], f'{work:.2f}', s["fanouts"], s["tasks"], s["halo_pieces"])
    print("All runs passed the harness output checks. CPU ratios screen work inflation; output conservation alone does not count intermediate work.")
    print("Per-case p-values are exploratory; sweep families require independent confirmation before changing defaults.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
