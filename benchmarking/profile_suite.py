#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Sum the operator profile across the PDS-H suite, one row per query.

This is the tool the accounting tables in `plans/parallelism-overview.md` came
from. `IBEX_PROFILE_OPERATORS=1` prints one `operator profile:` line per
STATEMENT, so a query's numbers are the sum over its lines; doing that by eye
across 22 queries is how arithmetic errors get into a plan.

Two properties are deliberate and matter more than they look:

* **Each query runs twice and only the second is parsed.** The first pays cold
  page cache for its Parquet files. Parsing it makes every I/O-heavy query look
  serial, because the time lands in decode before any worker starts.
* **Closure is printed, not assumed.** `pool_work + pool_idle + pool_unqueued`
  should exhaust `pool_capacity`, and `stage_self + stage_ring_wait +
  stage_park` should exhaust `stage_live`. A column that stops closing means a
  bucket is unmeasured — which has happened five times in this profiler, always
  making "serial" look bigger than it was. Read the closure column before
  believing any other column.

Usage:
    python benchmarking/profile_suite.py [cores]      # default 8

Run it on a quiet box, and NOT within a few minutes of a commit that touched
`src/runtime/` — the post-commit hook starts a background benchmark that will
inflate everything here. `IBEX_SKIP_PERF=1 git commit ...` avoids that.
"""

import pathlib
import re
import subprocess
import sys

import bench_env

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVAL = ROOT / "build-release/tools/ibex_eval"
PLUGINS = ROOT / "build-release/tools"
QUERIES = sorted((ROOT / "benchmarking/tpch/queries").glob("q??.ibex"))

LINE = re.compile(r"operator profile: (.*)")
FIELDS = (
    "wall_ms", "self_ms", "serial_self_ms", "barrier_wait_ms", "ring_wait_ms",
    "pool_work_ms", "pool_idle_ms", "pool_unqueued_ms", "pool_capacity_ms",
    "stage_self_ms", "stage_park_ms", "stage_ring_wait_ms", "stage_live_ms",
)


def parse(stderr: str) -> dict[str, float]:
    totals = {f: 0.0 for f in FIELDS}
    for match in LINE.finditer(stderr):
        fields = dict(kv.split("=", 1) for kv in match.group(1).split() if "=" in kv)
        for f in FIELDS:
            totals[f] += float(fields.get(f, 0.0))
    return totals


def run(query: pathlib.Path, cores: str) -> dict[str, float]:
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(pathlib.Path.home()),
        "IBEX_PROFILE_OPERATORS": "1",
        "IBEX_CORES": cores,
    }
    cmd = [str(EVAL), "--plugin-path", str(PLUGINS), str(query)]
    proc = None
    for _ in range(2):  # first warms the page cache; only the second counts
        proc = subprocess.run(
            cmd, cwd=ROOT, env=env, capture_output=True, text=True, timeout=900
        )
    assert proc is not None
    if proc.returncode != 0:
        print(f"  {query.stem}: FAILED rc={proc.returncode}", file=sys.stderr)
        print(proc.stderr[-800:], file=sys.stderr)
        return {}
    return parse(proc.stderr)


def report(name: str, t: dict[str, float]) -> None:
    cap = t["pool_capacity_ms"]
    accounted = t["pool_work_ms"] + t["pool_idle_ms"] + t["pool_unqueued_ms"]
    closure = 100.0 * accounted / cap if cap > 0 else 0.0
    unq = 100.0 * t["pool_unqueued_ms"] / cap if cap > 0 else 0.0
    live = t["stage_live_ms"]
    stage = t["stage_self_ms"] + t["stage_park_ms"] + t["stage_ring_wait_ms"]
    scl = 100.0 * stage / live if live > 0 else 0.0
    print(
        f"{name:6}{t['wall_ms']:9.1f}{t['serial_self_ms']:10.1f}"
        f"{t['barrier_wait_ms']:10.1f}{t['ring_wait_ms']:9.1f}"
        f"{t['pool_work_ms']:10.1f}{t['pool_unqueued_ms']:10.1f}"
        f"{cap:10.1f}{closure:8.1f}%{unq:6.1f}%{scl:8.1f}%"
    )


def main() -> int:
    cores = sys.argv[1] if len(sys.argv) > 1 else "8"
    if not EVAL.exists():
        print(f"missing {EVAL} — build the release target first", file=sys.stderr)
        return 1
    print(bench_env.scale_factor_line())
    print(f"# PDS-H, IBEX_CORES={cores}, build-release, second run of two")
    print(
        f"{'query':6}{'wall':>9}{'serial':>10}{'barrier':>10}{'ring':>9}"
        f"{'pool_work':>10}{'unqueued':>10}{'capacity':>10}"
        f"{'closure':>9}{'unq':>7}{'stage':>9}"
    )
    grand = {f: 0.0 for f in FIELDS}
    for query in QUERIES:
        totals = run(query, cores)
        if not totals:
            continue
        for f in FIELDS:
            grand[f] += totals[f]
        report(query.stem, totals)
    report("TOTAL", grand)
    return 0


if __name__ == "__main__":
    sys.exit(main())
