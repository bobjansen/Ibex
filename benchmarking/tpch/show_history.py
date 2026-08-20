#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Print a time series out of results/history.tsv.

`history.tsv` is append-only (see append_history.py) — this is the tool that
reads it back as a trend instead of a pair of snapshots. Point use: "did the
gain from commit X actually stick, or did a later run only look unchanged
because of a labeling accident" — filter to one framework+query+scale and
read the sequence.

Usage:
  python3 show_history.py --framework ibex --query q14 --sf 1
  python3 show_history.py --framework ibex --sf 1              # every query, latest run only
  python3 show_history.py --framework ibex --query q14 --sf 1 --all-runs
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
HISTORY = SCRIPT_DIR / "results" / "history.tsv"


def load_rows() -> list[dict]:
    if not HISTORY.exists():
        sys.exit(f"{HISTORY.relative_to(SCRIPT_DIR)} does not exist yet — run "
                 "append_history.py --backfill, or run_bench.sh, first")
    with HISTORY.open(newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def fmt_ms(v: str) -> str:
    try:
        return f"{float(v):.1f}"
    except ValueError:
        return v


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--framework", required=True, help="e.g. ibex, ibex-st, polars, pdsh-polars")
    parser.add_argument("--query", help="e.g. q14 — omit to show every query's LATEST run only")
    parser.add_argument("--sf", type=int, help="scale factor filter")
    parser.add_argument("--cores", help="cores filter (e.g. 8, or 'all:24') — a matrix-sweep run "
                                        "archives several core counts under the same sf, so pass "
                                        "this too if the trend looks like it's jumping around")
    parser.add_argument("--all-runs", action="store_true",
                        help="with --query omitted, still show every run (verbose) instead of just the latest")
    args = parser.parse_args()

    rows = [r for r in load_rows() if r["framework"] == args.framework]
    if args.sf is not None:
        rows = [r for r in rows if r.get("scale_factor") == str(args.sf)]
    if args.cores is not None:
        rows = [r for r in rows if r.get("cores") == args.cores]
    if args.query:
        rows = [r for r in rows if r["query"] == args.query]
    if not rows:
        sys.exit(f"no rows for framework={args.framework!r} query={args.query!r} sf={args.sf!r}")

    rows.sort(key=lambda r: r["run_utc"])

    if args.query:
        # One query's trend: every run, in order, with the delta from the row before.
        if args.cores is None and len({r.get("cores") for r in rows}) > 1:
            print("NOTE: multiple core counts in this trend — pass --cores to isolate one, "
                 "or deltas below mix core-count changes with everything else\n", file=sys.stderr)
        print(f"{'run_utc':<21} {'commit':<10} {'cores':<6} {'avg_ms':>9} {'delta':>8}  label")
        prev = None
        for r in rows:
            commit = r["commit"] + ("+dirty" if r.get("dirty") == "1" else "")
            avg = fmt_ms(r["avg_ms"])
            delta = ""
            try:
                cur = float(r["avg_ms"])
                if prev is not None:
                    delta = f"{(cur / prev - 1) * 100:+.1f}%"
                prev = cur
            except ValueError:
                pass
            print(f"{r['run_utc']:<21} {commit:<10} {r.get('cores', ''):<6} {avg:>9} {delta:>8}  "
                 f"{r.get('label', '')}")
        return 0

    # No --query: one row per query, either every run or just the latest.
    by_query: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_query[r["query"]].append(r)
    for q in by_query:
        by_query[q].sort(key=lambda r: r["run_utc"])

    queries = sorted(by_query, key=lambda q: int(q[1:]) if q[1:].isdigit() else q)
    if args.all_runs:
        for q in queries:
            print(f"== {q} ==")
            print(f"  {'run_utc':<21} {'commit':<10} {'avg_ms':>9}  label")
            for r in by_query[q]:
                commit = r["commit"] + ("+dirty" if r.get("dirty") == "1" else "")
                print(f"  {r['run_utc']:<21} {commit:<10} {fmt_ms(r['avg_ms']):>9}  {r.get('label', '')}")
        return 0

    print(f"{'query':<6} {'run_utc':<21} {'commit':<10} {'avg_ms':>9}  label")
    for q in queries:
        r = by_query[q][-1]
        commit = r["commit"] + ("+dirty" if r.get("dirty") == "1" else "")
        print(f"{q:<6} {r['run_utc']:<21} {commit:<10} {fmt_ms(r['avg_ms']):>9}  {r.get('label', '')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
