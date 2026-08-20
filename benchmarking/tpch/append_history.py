#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Append one archived run's per-query numbers to results/history.tsv.

`results/*.tsv` (the "latest" files print_table.py and the website generator
read) get overwritten every run, and `results/runs/<utc>_<commit>_sf<N>/` — the
archive `run_bench.sh` already writes — only lets you compare two runs you
pick by hand (`compare_runs.py old new`). Neither gives you an actual time
series: "how has q14 moved over the last two weeks of commits" needs reading
every archived run's tsvs and manifest, which nothing does automatically.

This appends one row per (framework, query) into a single long-format
`results/history.tsv` that is NEVER overwritten, only grown — read it with
`show_history.py`, a spreadsheet, or `awk`. Idempotent by `run_utc`: re-running
this against the same archived run is a no-op, so wiring it into
`run_bench.sh` on every archive is safe even if a run is re-archived.

Usage:
  python3 append_history.py results/runs/<utc>_<commit>_sf<N>   # one run
  python3 append_history.py --backfill                          # every archived run
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS = SCRIPT_DIR / "results"
RUNS = RESULTS / "runs"
HISTORY = RESULTS / "history.tsv"

FIELDS = [
    "run_dir", "run_utc", "commit", "dirty", "branch", "label", "scale_factor",
    "cores", "framework", "query", "avg_ms", "min_ms", "max_ms", "stddev_ms",
    "p95_ms", "p99_ms", "mode",
]


def load_manifest(run: Path) -> dict:
    path = run / "manifest.json"
    if not path.exists():
        raise SystemExit(f"{run}: no manifest.json — not an archived run directory")
    return json.loads(path.read_text())


def run_rows(run: Path) -> list[dict]:
    """One row per (framework, query) tsv line in `run`, tagged with its manifest."""
    info = load_manifest(run)
    # The archive directory name, not `generated_utc`, is the identity key: a
    # core-count/scale-factor matrix sweep (`sf-cores-matrix`, see
    # project_bench_core_count_cap) archives several directories in the same
    # second, so `generated_utc` alone collides and silently drops every run
    # but the first one processed — the directory name is unique by
    # construction (run_bench.sh always suffixes it with sf/cores/label).
    common = {
        "run_dir": run.name,
        "run_utc": info.get("generated_utc", run.name.split("_")[0]),
        "commit": (info.get("commit") or "unknown")[:8],
        "dirty": "1" if info.get("dirty") else "0",
        "branch": info.get("branch", ""),
        "label": info.get("label", ""),
        "scale_factor": info.get("scale_factor", ""),
        "cores": info.get("cores", ""),
    }
    rows = []
    for tsv in sorted(run.glob("*.tsv")):
        with tsv.open(newline="") as f:
            for line in csv.DictReader(f, delimiter="\t"):
                fw, query = line.get("framework"), line.get("query")
                if not fw or not query:
                    continue
                row = dict(common)
                row["framework"] = fw
                row["query"] = query
                for field in ("avg_ms", "min_ms", "max_ms", "stddev_ms", "p95_ms", "p99_ms"):
                    row[field] = line.get(field, "")
                row["mode"] = line.get("mode", "")
                rows.append(row)
    return rows


def existing_run_dirs() -> set[str]:
    if not HISTORY.exists():
        return set()
    with HISTORY.open(newline="") as f:
        return {row["run_dir"] for row in csv.DictReader(f, delimiter="\t") if "run_dir" in row}


def append(rows: list[dict]) -> None:
    if not rows:
        return
    new_file = not HISTORY.exists()
    with HISTORY.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", nargs="?", help="one results/runs/<utc>_<commit>_sf<N> directory")
    parser.add_argument("--backfill", action="store_true",
                        help="append every archived run under results/runs/ not already recorded")
    args = parser.parse_args()

    if not args.backfill and not args.run:
        parser.error("pass a run directory or --backfill")

    seen = existing_run_dirs()
    targets = sorted(p for p in RUNS.iterdir() if p.is_dir()) if args.backfill else [Path(args.run)]

    appended_runs = 0
    appended_rows = 0
    for run in targets:
        if run.name in seen:
            continue
        rows = run_rows(run)
        append(rows)
        seen.add(run.name)
        appended_runs += 1
        appended_rows += len(rows)

    print(f"appended {appended_rows} rows from {appended_runs} run(s) to {HISTORY.relative_to(SCRIPT_DIR)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
