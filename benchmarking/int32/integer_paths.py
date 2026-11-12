#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Int64-heavy queries over the runtime's integer read paths, at one core.

Each query drives one family of loops that read Int64 columns: filters, casts,
math, lag/cumsum/fills, grouped shifts, sorts and top-k, semi/anti/left/outer
joins, wide/pair/mixed/packed group keys, corr and melt. A per-element reader
there (IntegerView's width test, see 6bdbc65b) once cost a column-vs-column
filter 10x and several of the others 4-11%; this is the probe that found them.

  integer_paths.py gen               # seeded tables -> benchmarking/data/int32_paths
  integer_paths.py run --ibex A/tools/ibex --ibex B/tools/ibex --rounds 2

`run` alternates the given REPL binaries (each with its own plugin directory,
the binary's), one warmup and --iters timed collections per query, and prints
the minimum per query and binary. Answers are not checked; this measures speed
only. Every run is pinned to one CPU with IBEX_CORES=1 and, where systemd-run
is available, capped at --memory-cap so a broken build cannot exhaust the box.
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "benchmarking" / "data" / "int32_paths"
ROWS = 5_000_000

QUERIES = [
    ("filter_scalar", "t[filter a > 500000]"),
    ("filter_colcol", "t[filter a > b]"),
    ("cast_float", "t[update { d = Float64(a) }]"),
    ("abs_col", "t[update { y = abs(a) }]"),
    ("arith", "t[update { z = a * 3 + b }]"),
    ("pmax", "t[update { m = pmax(a, b) }]"),
    ("lag", "t[update { p = lag(a, 1) }]"),
    ("cumsum", "t[update { c = cumsum(a) }]"),
    ("fill_forward", "t[update { f = fill_forward(n) }]"),
    ("fill_backward", "t[update { f = fill_backward(n) }]"),
    ("grouped_lag", "t[update { p = lag(a, 1) }, by g]"),
    ("sort_int", "t[order { a }]"),
    ("topk_int", "t[order { a desc }, head 10]"),
    ("topk_int_by_g", "t[order { a desc }, head 2, by g]"),
    ("semi_join", "t semi join r on k"),
    ("anti_join", "t anti join r on k"),
    ("left_join", "t left join r on k"),
    ("outer_join", "t outer join r on k"),
    ("sum_by_wide_key", "t[select { s = sum(v) }, by k]"),
    ("sum_by_wide_pair", "t[select { s = sum(v) }, by { k, a }]"),
    ("sum_by_str_int", "t[select { x = sum(v) }, by { s, g }]"),
    ("sum_by_three_ints", "t[select { x = sum(v) }, by { g, h, v }]"),
    ("corr_int", "t[select { r = corr(a, b) }, by g]"),
    ("melt_ints", "t[melt { g, h, k, v, n, s }]"),
]
TIME_RE = re.compile(r"^time:\s*([\d.]+)\s*(us|ms|s)\s*$")
SCALE = {"us": 1e-3, "ms": 1.0, "s": 1e3}


def generate(out: Path) -> None:
    import numpy as np

    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(20261112)
    a = rng.integers(0, 1_000_000, ROWS)
    b = rng.integers(0, 1_000_000, ROWS)
    g = rng.integers(0, 1_000, ROWS)
    h = rng.integers(0, 100, ROWS)
    k = rng.integers(0, 5_000_000, ROWS)
    v = rng.integers(0, 100, ROWS)
    n = rng.integers(-1000, 1000, ROWS).astype(object)
    n[rng.random(ROWS) < 0.1] = ""  # 10% null, for the fills
    s = np.char.add("u", rng.integers(0, 1_000, ROWS).astype(str))
    with open(out / "t.csv", "w") as f:
        f.write("a,b,g,h,k,v,n,s\n")
        for start in range(0, ROWS, 500_000):
            part = slice(start, start + 500_000)
            f.write("\n".join(",".join(map(str, row)) for row in zip(
                a[part], b[part], g[part], h[part], k[part], v[part], n[part], s[part])) + "\n")
    right = rng.permutation(5_000_000)[:2_500_000]  # unique keys, half of t's domain
    with open(out / "r.csv", "w") as f:
        f.write("k,w\n")
        f.write("\n".join(f"{x},{y}" for x, y in zip(right, rng.integers(0, 1_000, right.size))))
        f.write("\n")
    print(f"wrote {out}")


def time_build(binary: Path, data: Path, queries, iters: int, cpu: int,
               memory_cap: str) -> dict[str, float | None]:
    script = ['import "csv";',
              f'let t = csv::read("{data}/t.csv", nulls = "<empty>");',
              f'let r = csv::read("{data}/r.csv", nulls = "<empty>");']
    for name, query in queries:
        script.append(f'print("@@{name}");')
        for i in range(1 + iters):  # the first collection is a warmup
            script += [":timing off", "let ans = t[head 0];",
                       ":timing on" if i else ":timing off", f"let ans = {query};", ":timing off"]
    cmd = ["taskset", "-c", str(cpu), str(binary), "--plugin-path", str(binary.parent),
           "--no-history"]
    if memory_cap and shutil.which("systemd-run"):
        cmd = ["systemd-run", "--user", "--scope", "-q", "-p", f"MemoryMax={memory_cap}",
               "-p", "MemorySwapMax=0", *cmd]
    env = dict(os.environ, IBEX_CORES="1")
    proc = subprocess.run(cmd, input="\n".join(script) + "\n", capture_output=True, text=True,
                          env=env, timeout=3600)
    times: dict[str, list[float]] = {}
    current = None
    for line in proc.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:].strip()
            times[current] = []
        elif current and (m := TIME_RE.match(line.strip())):
            times[current].append(float(m.group(1)) * SCALE[m.group(2)])
    if proc.returncode:
        print(f"{binary}: exit {proc.returncode}: {proc.stderr[-500:]}", file=sys.stderr)
    return {name: min(times[name]) if len(times.get(name, [])) == iters else None
            for name, _ in queries}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    gen = sub.add_parser("gen", help="write the seeded input tables")
    gen.add_argument("--data", type=Path, default=DATA)
    run = sub.add_parser("run", help="time the queries per binary")
    run.add_argument("--ibex", type=Path, action="append",
                     help="REPL binary; repeat to compare (default build-release/tools/ibex)")
    run.add_argument("--data", type=Path, default=DATA)
    run.add_argument("--rounds", type=int, default=1, help="alternations of the binaries")
    run.add_argument("--iters", type=int, default=5, help="timed collections per query")
    run.add_argument("--only", help="comma-separated query names")
    run.add_argument("--cpu", type=int, default=2, help="the one CPU to pin to (a P-core)")
    run.add_argument("--memory-cap", default="16G", help="systemd-run MemoryMax ('' disables)")
    args = ap.parse_args()
    if args.cmd == "gen":
        generate(args.data)
        return
    binaries = args.ibex or [ROOT / "build-release" / "tools" / "ibex"]
    queries = QUERIES
    if args.only:
        wanted = set(args.only.split(","))
        unknown = wanted - {name for name, _ in QUERIES}
        if unknown:
            ap.error(f"unknown queries: {', '.join(sorted(unknown))}")
        queries = [q for q in QUERIES if q[0] in wanted]
    if not (args.data / "t.csv").exists():
        ap.error(f"{args.data}/t.csv missing; run `{Path(__file__).name} gen` first")
    best: dict[tuple[int, str], float] = {}
    for _ in range(args.rounds):
        for i, binary in enumerate(binaries):
            for name, ms in time_build(binary, args.data.resolve(), queries, args.iters, args.cpu,
                                       args.memory_cap).items():
                if ms is not None:
                    best[(i, name)] = min(ms, best.get((i, name), ms))
    head = "query".ljust(20) + "".join(f"{f'build {i}':>12}" for i in range(len(binaries)))
    if len(binaries) == 2:
        head += f"{'1 vs 0':>10}"
    print(head)
    for name, _ in queries:
        cells = [best.get((i, name)) for i in range(len(binaries))]
        row = name.ljust(20) + "".join(f"{c:12.2f}" if c is not None else f"{'FAILED':>12}"
                                      for c in cells)
        if len(binaries) == 2 and None not in cells:
            row += f"{(cells[1] / cells[0] - 1) * 100:+9.1f}%"
        print(row)
    for i, binary in enumerate(binaries):
        print(f"build {i}: {binary}")


if __name__ == "__main__":
    main()
