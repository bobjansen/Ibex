#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Does running on N cores do more WORK than running on one?

Every other harness here measures wall time. Wall time cannot see parallelism
that multiplies work instead of dividing it: N threads doing kN work still
finish sooner than one thread doing k, so the query gets faster and every
health metric agrees it is healthy. `IBEX_PROFILE_OPERATORS` reported q17 at
85% pool occupancy, 2ms of serial self-time and a 22.79x Amdahl ceiling while
it burned 4.6x the CPU re-decoding Parquet row groups (fixed in 7fae1146).

The measurement that sees it is total CPU at one core against N:

    work_multiplier = cpu(N cores) / cpu(1 core)

1.0 means CPU was conserved. Above 1.0 there are TWO possible causes and they
need very different responses, so this number is a SCREEN, not a verdict:

  (a) REDUNDANT WORK — the parallel path really does more. q17 sharded a
      Parquet row group by row offset and each shard's `Skip` decoded and
      discarded, so shard k re-decoded every shard before it (`7fae1146`,
      q17 -33.6%). Fixable, and the fix is usually large.

  (b) CONTENTION STALLS — the same work, slowed down. `task-clock` counts a
      stalled core as busy, so threads competing for L3 and memory bandwidth
      inflate CPU without doing anything extra. q06 reads 2.27x here, and a
      row counter on its filter proves it processes IDENTICAL rows at 1 and 8
      cores (12 calls, 11,997,996 rows, both). Its decode threads and its
      filter simply contend.

**Always confirm which one before acting.** The cheap test is to count the work
directly — rows, calls, bytes — at 1 core and at N. If the counts match, it is
(b) and there is no redundant work to delete.

Note that a plain streaming read does NOT inflate on this box (400MB summed by
1 thread vs 8: 13.8 -> 52.7 GB/s, task-clock flat at ~160ms), so (b) needs
genuine concurrent contention, not merely touching a lot of memory.

CPU comes from `getrusage(RUSAGE_CHILDREN)` — user+sys of the child process —
so this needs no `perf` and runs anywhere. It agrees with
`perf stat -e task-clock` to within a millisecond or two.

Read `work_x` first and `speedup` second. A query can look fine on speedup and
still be wasting most of the machine.

Usage:
    python benchmarking/work_multiplication.py [--cores N] [--repeats N]
                                               [--queries q17 q06 ...]
Run it on a quiet box, and not within a few minutes of a commit touching
src/runtime/ (the post-commit hook starts a background benchmark).
"""

import argparse
import os
import pathlib
import resource
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
EVAL = ROOT / "build-release/tools/ibex_eval"
PLUGINS = ROOT / "build-release/tools"
QUERY_DIR = ROOT / "benchmarking/tpch/queries"

# Above this, the parallel path is doing enough extra work to be worth a look.
# Not a pass/fail line: some multiplication is inherent (thread start-up,
# per-worker buffers). 1.78x was the SUITE figure when this tool was written.
FLAG = 1.5


def run_once(query: pathlib.Path, cores: int, taskset: str | None) -> tuple[float, float]:
    """One run. Returns (cpu_ms, wall_ms). CPU is the child's user+sys."""
    cmd: list[str] = []
    if taskset:
        cmd += ["taskset", "-c", taskset]
    cmd += [str(EVAL), "--plugin-path", str(PLUGINS), str(query)]
    env = {"PATH": "/usr/bin:/bin", "HOME": str(pathlib.Path.home()),
           "IBEX_CORES": str(cores)}
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)
    wall = (time.perf_counter() - start) * 1000.0
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if proc.returncode != 0:
        raise RuntimeError(f"{query.name} failed at {cores} core(s):\n{proc.stderr[-600:]}")
    cpu = ((after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)) * 1000.0
    return cpu, wall


def measure(query: pathlib.Path, cores: int, repeats: int, taskset: str | None):
    # Minimum, not mean: noise is one-sided in both CPU and wall.
    cpu = wall = None
    for _ in range(repeats):
        c, w = run_once(query, cores, taskset)
        cpu = c if cpu is None else min(cpu, c)
        wall = w if wall is None else min(wall, w)
    return cpu, wall


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cores", type=int, default=8)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--taskset", default="0-7")
    ap.add_argument("--queries", nargs="*", default=None)
    args = ap.parse_args()

    if not EVAL.exists():
        print(f"error: {EVAL} not found — build the Release tree first", file=sys.stderr)
        return 1
    names = args.queries or sorted(p.stem for p in QUERY_DIR.glob("q??.ibex"))
    queries = [QUERY_DIR / f"{n}.ibex" for n in names]

    print(f"# IBEX_CORES=1 vs {args.cores}, repeats={args.repeats} (min), "
          f"taskset={args.taskset}")
    print(f"# work_x = cpu({args.cores}c)/cpu(1c). 1.00 = work divided; "
          f"above that is work the serial path never does.")
    print(f"{'query':>6} {'cpu 1c':>9} {'cpu Nc':>9} {'work_x':>7} "
          f"{'wall 1c':>9} {'wall Nc':>9} {'speedup':>8} {'cap':>6}")
    print("-" * 74)

    rows = []
    for q in queries:
        measure(q, args.cores, 1, args.taskset)  # warm the page cache
        c1, w1 = measure(q, 1, args.repeats, args.taskset)
        cn, wn = measure(q, args.cores, args.repeats, args.taskset)
        work = cn / c1 if c1 else float("nan")
        speed = w1 / wn if wn else float("nan")
        rows.append((work, q.stem, c1, cn, w1, wn, speed))
        flag = "  <<" if work > FLAG else ""
        print(f"{q.stem:>6} {c1:8.0f}ms {cn:8.0f}ms {work:6.2f}x "
              f"{w1:8.0f}ms {wn:8.0f}ms {speed:7.2f}x {args.cores/work:5.2f}x{flag}")

    print("-" * 74)
    c1s = sum(r[2] for r in rows); cns = sum(r[3] for r in rows)
    w1s = sum(r[4] for r in rows); wns = sum(r[5] for r in rows)
    suite_work = cns / c1s
    print(f"{'SUITE':>6} {c1s:8.0f}ms {cns:8.0f}ms {suite_work:6.2f}x "
          f"{w1s:8.0f}ms {wns:8.0f}ms {w1s/wns:7.2f}x {args.cores/suite_work:5.2f}x")
    print()
    print(f"The suite burns {suite_work:.2f}x the CPU on {args.cores} cores.")
    print(f"That is a SCREEN, not a verdict: it is redundant work only where a")
    print(f"direct count of rows/calls/bytes differs between 1 core and "
          f"{args.cores}. Where the counts match it is contention, and there is")
    print(f"nothing to delete. Confirm before acting -- see MEASURING.md 1b.")
    worst = sorted(rows, reverse=True)[:5]
    if worst and worst[0][0] > FLAG:
        print(f"\nworst multipliers (look here first):")
        for work, name, c1, cn, _w1, _wn, speed in worst:
            print(f"  {name}  {work:.2f}x work ({c1:.0f} -> {cn:.0f}ms cpu), "
                  f"speedup {speed:.2f}x")
    return 0


if __name__ == "__main__":
    sys.exit(main())
