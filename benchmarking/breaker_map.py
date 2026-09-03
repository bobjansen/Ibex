#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Where do the cores go? A per-operator map of idle capacity across PDS-H.

`profile_suite.py` sums the operator profile to one row per query -- it answers
"how much capacity did this query waste". This tool answers the next question:
*which operator wasted it*, and what kind of boundary that operator is.

The decomposition that makes this work is an identity, not an estimate. Each
`profile node=` row reports `self_ms` (exclusive time on the calling thread --
the row's own work, children excluded) and `pool_work_ms` (work its own fan-out
ran on pool threads). During one operator's exclusive window the pool offers
`self_ms * workers` core-milliseconds, so

    idle_core_ms = self_ms * workers - pool_work_ms

is the capacity that operator left on the floor. Because the self times
partition the wall clock, these sum to the query's `pool_unqueued_ms` -- the
closure is printed per query and a drift means the attribution is wrong, in
exactly the spirit of profile_suite.py's own closure column.

Ranking by this rather than by elapsed time is the point: a breaker that is
internally parallel but starves behind its producer shows up here, and a serial
operator that runs for 46ms while eight threads wait costs the same as a
parallel one that runs for 370.

Usage:
    python benchmarking/breaker_map.py [cores]        # default 8
    python benchmarking/breaker_map.py 8 --queries q01,q21
"""

import collections
import pathlib
import re
import subprocess
import sys

import bench_env

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVAL = ROOT / "build-release/tools/ibex_eval"
PLUGINS = ROOT / "build-release/tools"
QUERY_DIR = ROOT / "benchmarking/tpch/queries"

SUMMARY = re.compile(r"operator profile: (.*)")
ROW = re.compile(r'profile node=(\d+) op="([^"]*)" (.*)')

# The static half of the map: which breaker family an operator label belongs to,
# and whether that boundary can disappear at all. `streamable` is a claim about
# the FAMILY, not about today's implementation -- it is what the map is for.
#   pipeline : row-local, no boundary; idle here is starvation, not a barrier
#   partial  : a boundary that private-state-then-merge can shrink to one merge
#   hard     : a boundary that must see every row (order, top-k, materialized)
FAMILIES = (
    ("scan", "scan", "pipeline"),
    ("source ", "scan", "pipeline"),
    ("filter", "map", "pipeline"),
    ("update", "map", "pipeline"),
    ("project", "map", "pipeline"),
    ("rename", "map", "pipeline"),
    ("morsel", "map", "pipeline"),
    ("Aggregate.Discovery", "aggregate", "partial"),
    ("Aggregate.Accumulation", "aggregate", "partial"),
    ("Aggregate.FinalOrdering", "aggregate", "hard"),
    ("Aggregate.Emission", "aggregate", "pipeline"),
    ("aggregate", "aggregate", "partial"),
    ("Join.", "join", "partial"),
    ("join", "join", "partial"),
    ("semi", "join", "partial"),
    ("anti", "join", "partial"),
    ("distinct", "distinct", "partial"),
    ("order", "order", "hard"),
    ("sort", "order", "hard"),
    ("topk", "topk", "hard"),
    ("head", "topk", "hard"),
    ("tail", "topk", "hard"),
    ("window", "window", "hard"),
    ("rank", "window", "hard"),
)


def classify(label: str) -> tuple[str, str]:
    for prefix, family, boundary in FAMILIES:
        if label.startswith(prefix):
            return family, boundary
    return "other", "unknown"


def run(query: pathlib.Path, cores: str) -> str:
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
        return ""
    return proc.stderr


def fields(text: str) -> dict[str, float]:
    out = {}
    for kv in text.split():
        if "=" in kv:
            key, value = kv.split("=", 1)
            try:
                out[key] = float(value)
            except ValueError:
                pass
    return out


def parse(stderr: str) -> tuple[dict[str, float], list[dict]]:
    """The LAST statement's summary plus every operator row in the run.

    A query file is several statements (the `let` binds, then the query, then
    write_csv); the profile prints one summary per statement. Only statements
    that did work carry rows, and rows are attributed to the summary that
    follows them, so rows are collected across the whole run and the summary
    taken from the widest window.
    """
    summaries = [fields(m.group(1)) for m in SUMMARY.finditer(stderr)]
    summary = max(summaries, key=lambda s: s.get("wall_ms", 0.0), default={})
    rows = []
    for match in ROW.finditer(stderr):
        row = fields(match.group(3))
        row["node"] = int(match.group(1))
        row["label"] = match.group(2)
        rows.append(row)
    return summary, rows


def attribute(summary: dict, rows: list[dict]) -> list[dict]:
    workers = summary.get("workers", 1.0) or 1.0
    out = []
    for row in rows:
        self_ms = row.get("build_self_ms", 0.0) + row.get("next_self_ms", 0.0) + row.get(
            "source_self_ms", 0.0
        )
        pool = row.get("pool_work_ms", 0.0)
        family, boundary = classify(row["label"])
        out.append(
            {
                "label": row["label"],
                "node": row["node"],
                "family": family,
                "boundary": boundary,
                "self_ms": self_ms,
                "pool_work_ms": pool,
                "idle_core_ms": self_ms * workers - pool,
                "barrier_wait_ms": row.get("barrier_wait_ms", 0.0),
                "ring_wait_ms": row.get("ring_wait_ms", 0.0),
                "barriers": row.get("barriers", 0.0),
                "rows": row.get("rows", 0.0),
                "chunks": row.get("chunks", 0.0),
            }
        )
    return sorted(out, key=lambda r: -r["idle_core_ms"])


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    only = None
    for arg in sys.argv[1:]:
        if arg.startswith("--queries="):
            only = set(arg.split("=", 1)[1].split(","))
    cores = args[0] if args else "8"
    if not EVAL.exists():
        print(f"missing {EVAL} -- build the release target first", file=sys.stderr)
        return 1
    queries = sorted(QUERY_DIR.glob("q??.ibex"))
    if only:
        queries = [q for q in queries if q.stem in only]

    print(bench_env.scale_factor_line())
    print(f"# PDS-H, IBEX_CORES={cores}, build-release, second run of two")
    print("# idle_core_ms = self_ms * workers - pool_work_ms (capacity this operator left idle)")

    per_family = collections.defaultdict(float)
    per_label = collections.defaultdict(float)
    per_boundary = collections.defaultdict(float)
    grand_idle = 0.0
    grand_wall = 0.0
    print(
        f"\n{'query':6}{'wall':>8}{'workers':>8}{'idle':>10}{'unqueued':>10}{'closure':>9}"
        f"   top operators by idle core-ms"
    )
    details: dict[str, list[dict]] = {}
    for query in queries:
        stderr = run(query, cores)
        if not stderr:
            continue
        summary, rows = parse(stderr)
        if not rows:
            print(f"{query.stem:6}   (no operator rows)")
            continue
        attributed = attribute(summary, rows)
        details[query.stem] = attributed
        idle = sum(r["idle_core_ms"] for r in attributed)
        unqueued = summary.get("pool_unqueued_ms", 0.0)
        closure = 100.0 * idle / unqueued if unqueued > 0 else 0.0
        grand_idle += idle
        grand_wall += summary.get("wall_ms", 0.0)
        for row in attributed:
            per_family[row["family"]] += row["idle_core_ms"]
            per_boundary[row["boundary"]] += row["idle_core_ms"]
            per_label[row["label"].split(" ")[0]] += row["idle_core_ms"]
        top = ", ".join(
            f"{r['label'][:28]} {r['idle_core_ms']:.0f}" for r in attributed[:3]
            if r["idle_core_ms"] > 1.0
        )
        print(
            f"{query.stem:6}{summary.get('wall_ms', 0.0):8.1f}"
            f"{summary.get('workers', 0.0):8.0f}{idle:10.1f}{unqueued:10.1f}"
            f"{closure:8.1f}%   {top}"
        )

    print(f"\n{'TOTAL':6}{grand_wall:8.1f}{'':8}{grand_idle:10.1f}")

    print("\n# idle core-ms by breaker family")
    for family, idle in sorted(per_family.items(), key=lambda kv: -kv[1]):
        share = 100.0 * idle / grand_idle if grand_idle else 0.0
        print(f"  {family:14}{idle:12.1f}{share:8.1f}%")
    print("\n# idle core-ms by boundary kind (can it disappear?)")
    for boundary, idle in sorted(per_boundary.items(), key=lambda kv: -kv[1]):
        share = 100.0 * idle / grand_idle if grand_idle else 0.0
        print(f"  {boundary:14}{idle:12.1f}{share:8.1f}%")
    print("\n# idle core-ms by operator label")
    for label, idle in sorted(per_label.items(), key=lambda kv: -kv[1])[:20]:
        share = 100.0 * idle / grand_idle if grand_idle else 0.0
        print(f"  {label:24}{idle:12.1f}{share:8.1f}%")

    print("\n# per-query operator detail (idle core-ms, > 1ms)")
    for name, rows in details.items():
        print(f"\n  {name}")
        print(
            f"    {'operator':32}{'family':10}{'bound':9}{'self':>9}{'pool':>10}"
            f"{'idle':>9}{'barrier':>9}{'ring':>9}{'rows':>12}"
        )
        for row in rows:
            if row["idle_core_ms"] <= 1.0 and row["pool_work_ms"] <= 1.0:
                continue
            print(
                f"    {row['label'][:31]:32}{row['family']:10}{row['boundary']:9}"
                f"{row['self_ms']:9.1f}{row['pool_work_ms']:10.1f}"
                f"{row['idle_core_ms']:9.1f}{row['barrier_wait_ms']:9.1f}"
                f"{row['ring_wait_ms']:9.1f}{row['rows']:12.0f}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
