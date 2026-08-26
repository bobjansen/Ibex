#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Diff two archived PDS-H runs.

`run_bench.sh` writes results under fixed names, so a rerun overwrites the
numbers you wanted to compare against. Each run is therefore also archived under
`results/runs/<utc>_<commit>_sf<N>/`, and this reads two of those.

What it reports, and why each column is there:

  * per-query delta for one framework (default `ibex`) — the thing you changed;
  * the same delta for the REFERENCE framework (default `polars`), which did not
    change between the runs. A reference that moved is the measurement telling
    you the two runs are not comparable, and it is the only warning you get:
    Ibex's own numbers cannot distinguish "we got slower" from "the box was
    busier". Today that distinction was missing and cost a wrong conclusion.

Usage:
    compare_runs.py <old-run-dir> <new-run-dir> [--framework ibex] [--reference polars]
    compare_runs.py --list
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path

RUNS = Path(__file__).resolve().parent / "results" / "runs"


def load_framework(run: Path, framework: str) -> dict[str, float]:
    """Every `avg_ms` in `run` belonging to `framework`, keyed by query."""
    out: dict[str, float] = {}
    for tsv in sorted(run.glob("*.tsv")):
        with tsv.open() as handle:
            for row in csv.DictReader(handle, delimiter="\t"):
                if row.get("framework") == framework:
                    out[row["query"]] = float(row["avg_ms"])
    return out


def manifest(run: Path) -> dict:
    path = run / "manifest.json"
    return json.loads(path.read_text()) if path.exists() else {}


def geomean(values: list[float]) -> float:
    return math.exp(sum(math.log(v) for v in values) / len(values))


def describe(run: Path) -> str:
    info = manifest(run)
    bits = [info.get("commit", "?")[:8]]
    if info.get("dirty"):
        bits.append("DIRTY")
    bits.append(f"sf{info.get('scale_factor', '?')}")
    bits.append(f"{info.get('cores', '?')} cores")
    if info.get("label"):
        bits.append(info["label"])
    return f"{run.name}  [{' '.join(bits)}]"


def compare(old: Path, new: Path, framework: str, reference: str) -> int:
    before, after = load_framework(old, framework), load_framework(new, framework)
    shared = sorted(set(before) & set(after), key=lambda q: int(q[1:]))
    if not shared:
        print(f"no queries in common for framework '{framework}'", file=sys.stderr)
        return 1

    for label, run in (("old", old), ("new", new)):
        print(f"{label}: {describe(run)}")

    old_info, new_info = manifest(old), manifest(new)
    for field in ("scale_factor", "cores"):
        if old_info.get(field) != new_info.get(field):
            print(f"  WARNING: {field} differs ({old_info.get(field)} vs "
                  f"{new_info.get(field)}) — these runs are not comparable")
    if old_info.get("dirty") or new_info.get("dirty"):
        print("  WARNING: a run was made from a dirty tree")

    print(f"\n{'query':6} {'old ms':>9} {'new ms':>9} {'delta':>8}")
    ratios = []
    for query in shared:
        ratio = after[query] / before[query]
        ratios.append(ratio)
        flag = " *" if abs(ratio - 1) > 0.05 else ""
        print(f"{query:6} {before[query]:9.2f} {after[query]:9.2f} "
              f"{(ratio - 1) * 100:+7.1f}%{flag}")
    print(f"\n{framework} geomean {geomean(ratios):.4f} over {len(ratios)} queries")

    # The control. `reference` is an engine neither run changed, so any movement
    # in it is the box, not the code — and it bounds how much of the primary
    # delta above you are entitled to believe.
    ref_before, ref_after = load_framework(old, reference), load_framework(new, reference)
    ref_shared = sorted(set(ref_before) & set(ref_after))
    if ref_shared:
        drift = geomean([ref_after[q] / ref_before[q] for q in ref_shared])
        print(f"{reference} geomean {drift:.4f}  <- reference: should be ~1.000")
        if abs(drift - 1) > 0.03:
            print(f"  WARNING: {reference} moved {(drift - 1) * 100:+.1f}% between runs. "
                  f"It did not change, so the box did — treat the {framework} delta above "
                  f"as unproven.")
    else:
        print(f"(no '{reference}' rows to check drift against)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("old", nargs="?")
    parser.add_argument("new", nargs="?")
    parser.add_argument("--framework", default="ibex")
    parser.add_argument("--reference", default="pdsh-polars",
                        help="engine that did NOT change, used to detect box drift")
    parser.add_argument("--list", action="store_true", help="list archived runs")
    args = parser.parse_args()

    if args.list or not (args.old and args.new):
        if not RUNS.exists():
            print("no archived runs yet", file=sys.stderr)
            return 1
        for run in sorted(RUNS.iterdir()):
            if run.is_dir():
                print(describe(run))
        return 0 if args.list else 1

    return compare(Path(args.old), Path(args.new), args.framework, args.reference)


if __name__ == "__main__":
    raise SystemExit(main())
