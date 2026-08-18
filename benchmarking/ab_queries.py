#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Interleaved A/B of two `ibex_eval` binaries over a set of .ibex queries.

Answers one question: did this change make these queries faster, slower, or
neither, and did it change any output.

`compare_ibex_git.sh` is the tool for comparing two git STATES — it builds each
side in its own temporary worktree and drives the `ibex_bench` suite. This one
compares two BINARIES over .ibex query files, which is what you want when the
thing you care about is a PDS-H query rather than a micro benchmark, and when
you already have both builds.

Build the base side without disturbing your tree:

    cp build-release/tools/ibex_eval /tmp/eval_target
    git stash -q
    CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval
    cp build-release/tools/ibex_eval /tmp/eval_base
    git stash pop -q
    CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval

That last rebuild is not optional: after `git stash pop` the binary sitting in
`build-release/` is still the BASE build, and forgetting it means measuring the
base against itself while believing otherwise.

    python3 benchmarking/ab_queries.py --base /tmp/eval_base --target /tmp/eval_target

Plugins come from the CURRENT `build-release/tools` for both sides. That is
correct only while the change does not alter the plugin ABI (anything in
`Table`/`Chunk` layout, `LazyTable` members, or `ColumnDecodeFn`). If it does,
each side needs its own plugin build and this script is the wrong tool.

Notes on reading the output:

* The per-query noise floor on a typical dev box is around **±13%**, not ±2%.
  Deltas inside `--noise-pct` are printed as `noise` and should be reported as
  such rather than as small wins.
* `geomean` over per-query ratios is the headline, not the sum of times — a sum
  is dominated by whichever query happens to be longest.
* Byte-identity is checked by default and is not a performance question: the
  engine's contract is that output does not depend on how it was computed.
"""

import argparse
import math
import pathlib
import statistics
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_QUERY_DIR = ROOT / "benchmarking/tpch/queries"
PERF_LOG = ROOT / "build-release/post_commit_perf.log"


def warn_if_busy() -> None:
    """The post-commit hook runs a scale regression in the background."""
    if PERF_LOG.exists() and time.time() - PERF_LOG.stat().st_mtime < 180:
        print(
            "WARNING: build-release/post_commit_perf.log was written in the last "
            "3 minutes.\n         The post-commit perf hook is probably still "
            "running and will inflate\n         these numbers. Wait for it, or "
            "commit with IBEX_SKIP_PERF=1.\n",
            file=sys.stderr,
        )


def run_once(binary: pathlib.Path, query: pathlib.Path, args: argparse.Namespace
             ) -> tuple[float, str]:
    """One timed run. Returns (milliseconds, stdout)."""
    cmd: list[str] = []
    if args.taskset:
        cmd += ["taskset", "-c", args.taskset]
    cmd += [str(binary), "--plugin-path", str(args.plugin_path), str(query)]
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(pathlib.Path.home()),
        "IBEX_CORES": str(args.cores),
    }
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True,
                          text=True, timeout=args.timeout)
    elapsed = (time.perf_counter() - start) * 1000.0
    if proc.returncode != 0:
        raise RuntimeError(
            f"{binary.name} failed on {query.name} (rc={proc.returncode}):\n"
            f"{proc.stderr[-600:]}"
        )
    return elapsed, proc.stdout


def compare(query: pathlib.Path, args: argparse.Namespace) -> dict | None:
    # Warm both sides: first touch pays cold page cache on the Parquet files.
    for _ in range(args.warmup):
        run_once(args.base, query, args)
        run_once(args.target, query, args)

    base_ms: list[float] = []
    target_ms: list[float] = []
    base_out = target_out = ""
    for i in range(args.repeats):
        # Alternate which side goes first, so a monotonic drift over the repeat
        # (thermal, background load) does not land entirely on one side.
        if i % 2 == 0:
            b, base_out = run_once(args.base, query, args)
            t, target_out = run_once(args.target, query, args)
        else:
            t, target_out = run_once(args.target, query, args)
            b, base_out = run_once(args.base, query, args)
        base_ms.append(b)
        target_ms.append(t)

    base = statistics.median(base_ms)
    target = statistics.median(target_ms)
    delta = 100.0 * (target - base) / base if base > 0 else 0.0
    identical = base_out == target_out
    if abs(delta) < args.noise_pct:
        verdict = "noise"
    elif delta < 0:
        verdict = "FASTER"
    else:
        verdict = "SLOWER"
    if not identical:
        verdict = "DIFFERS"
    return {
        "name": query.stem, "base": base, "target": target, "delta": delta,
        "ratio": (target / base) if base > 0 else 1.0,
        "identical": identical, "verdict": verdict,
        "base_spread": (max(base_ms) - min(base_ms)) / base * 100.0 if base else 0.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", type=pathlib.Path, required=True,
                    help="baseline ibex_eval binary")
    ap.add_argument("--target", type=pathlib.Path, required=True,
                    help="candidate ibex_eval binary")
    ap.add_argument("--queries", nargs="*", default=None,
                    help="query names (q01 q06) or .ibex paths; default all PDS-H")
    ap.add_argument("--query-dir", type=pathlib.Path, default=DEFAULT_QUERY_DIR)
    ap.add_argument("--plugin-path", type=pathlib.Path,
                    default=ROOT / "build-release/tools")
    ap.add_argument("--cores", default="8", help="IBEX_CORES (default 8)")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--taskset", default=None, help="pin with taskset -c CPUSET")
    ap.add_argument("--noise-pct", type=float, default=13.0,
                    help="deltas smaller than this are reported as noise "
                         "(default 13, the measured per-query floor)")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--no-verify", action="store_true",
                    help="skip the byte-identity check (do not use for an "
                         "operator change)")
    args = ap.parse_args()

    for binary in (args.base, args.target):
        if not binary.exists():
            print(f"missing binary: {binary}", file=sys.stderr)
            return 1
    if args.base.resolve() == args.target.resolve():
        print("NOTE: base and target are the same file — this measures the "
              "harness's own noise, which is a useful thing to do.\n",
              file=sys.stderr)

    if args.queries:
        queries = [
            pathlib.Path(q) if q.endswith(".ibex") else args.query_dir / f"{q}.ibex"
            for q in args.queries
        ]
    else:
        queries = sorted(args.query_dir.glob("q??.ibex"))
    if not queries:
        print(f"no queries found in {args.query_dir}", file=sys.stderr)
        return 1

    warn_if_busy()
    print(f"# base={args.base}  target={args.target}")
    print(f"# IBEX_CORES={args.cores} repeats={args.repeats} "
          f"warmup={args.warmup} noise={args.noise_pct}%"
          + (f" taskset={args.taskset}" if args.taskset else ""))
    print(f"{'query':8}{'base_ms':>10}{'target_ms':>11}{'delta':>9}"
          f"{'spread':>9}  {'verdict':<8}{'output':>10}")

    rows = []
    for query in queries:
        if not query.exists():
            print(f"{query.stem:8}{'MISSING':>10}", file=sys.stderr)
            continue
        try:
            row = compare(query, args)
        except RuntimeError as exc:
            print(f"{query.stem:8}  FAILED: {exc}", file=sys.stderr)
            continue
        rows.append(row)
        out = "same" if row["identical"] else "DIFFERS"
        print(f"{row['name']:8}{row['base']:10.1f}{row['target']:11.1f}"
              f"{row['delta']:8.1f}%{row['base_spread']:8.1f}%  "
              f"{row['verdict']:<8}{out:>10}")

    if not rows:
        print("no results", file=sys.stderr)
        return 1

    geo = math.exp(sum(math.log(r["ratio"]) for r in rows) / len(rows))
    base_total = sum(r["base"] for r in rows)
    target_total = sum(r["target"] for r in rows)
    differing = [r["name"] for r in rows if not r["identical"]]
    print(f"{'TOTAL':8}{base_total:10.1f}{target_total:11.1f}"
          f"{100 * (target_total - base_total) / base_total:8.1f}%")
    print(f"# geomean of per-query ratios: {geo:.4f} "
          f"({100 * (geo - 1):+.1f}%)")

    if args.no_verify:
        print("# byte-identity NOT checked (--no-verify)")
    elif differing:
        print(f"# OUTPUT CHANGED on {len(differing)} quer"
              f"{'y' if len(differing) == 1 else 'ies'}: {' '.join(differing)}")
        print("# This is a correctness result, not a perf one. Stop and explain "
              "it before reading any timing above.")
        return 2
    else:
        print(f"# byte-identical on all {len(rows)} queries")

    inside = sum(1 for r in rows if r["verdict"] == "noise")
    if inside == len(rows):
        print(f"# every query is inside the {args.noise_pct}% noise floor — "
              "report this as a wash, not a win")
    return 0


if __name__ == "__main__":
    sys.exit(main())
