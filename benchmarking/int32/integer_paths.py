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

`gen` writes each table three times, with the same values: CSV, and Parquet
with the integer columns as Int64 (`parquet64`) and as Int32 (`parquet`, which
loads as compact Int32 storage, src/runtime/CONTRACTS.md section 8). The two
Parquet inputs differ only in width, so `run --input parquet64,parquet` (the
default) is the compact-vs-wide A/B; with one binary it prints that ratio.

`run` alternates the given REPL binaries (each with its own plugin directory,
the binary's), one warmup and --iters timed collections per query, and prints
the minimum per query and binary. Answers are not checked; this measures speed
only. Every run is pinned to one CPU with IBEX_CORES=1 and, where systemd-run
is available, capped at --memory-cap so a broken build cannot exhaust the box.
The Parquet input needs a build with the Parquet plugin; the perfcmp cache
slots have none, so use Release trees there.
"""
import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import numpy as np

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


def generate(out: Path, rows: int = ROWS) -> None:
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(20261112)
    a = rng.integers(0, 1_000_000, rows)
    b = rng.integers(0, 1_000_000, rows)
    g = rng.integers(0, 1_000, rows)
    h = rng.integers(0, 100, rows)
    k = rng.integers(0, rows, rows)
    v = rng.integers(0, 100, rows)
    n_values = rng.integers(-1000, 1000, rows)
    n_null = rng.random(rows) < 0.1  # 10% null, for the fills
    n = n_values.astype(object)
    n[n_null] = ""
    s_ids = rng.integers(0, 1_000, rows)
    s = np.char.add("u", s_ids.astype(str))
    with open(out / "t.csv", "w") as f:
        f.write("a,b,g,h,k,v,n,s\n")
        for start in range(0, rows, 500_000):
            part = slice(start, start + 500_000)
            f.write("\n".join(",".join(map(str, row)) for row in zip(
                a[part], b[part], g[part], h[part], k[part], v[part], n[part], s[part])) + "\n")
    right = rng.permutation(rows)[:rows // 2]  # unique keys, half of t's domain
    w = rng.integers(0, 1_000, right.size)
    with open(out / "r.csv", "w") as f:
        f.write("k,w\n")
        f.write("\n".join(f"{x},{y}" for x, y in zip(right, w)))
        f.write("\n")
    write_parquet(out, a, b, g, h, k, v, n_values, n_null, s, right, w)
    print(f"wrote {out}")


def write_parquet(out: Path, a, b, g, h, k, v, n_values, n_null, s, right, w) -> None:
    """The CSV tables again as Parquet, integer columns as Int32 and as Int64."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    def i32(values, mask=None):
        return pa.array(values.astype(np.int32), type=pa.int32(), mask=mask)

    t = pa.table({"a": i32(a), "b": i32(b), "g": i32(g), "h": i32(h), "k": i32(k),
                  "v": i32(v), "n": i32(n_values, n_null), "s": pa.array(s)})
    r = pa.table({"k": i32(right), "w": i32(w)})
    for name, table in (("t", t), ("r", r)):
        pq.write_table(table, out / f"{name}.parquet")
        wide = pa.schema([pa.field(f.name, pa.int64() if f.type == pa.int32() else f.type)
                          for f in table.schema])
        pq.write_table(table.cast(wide), out / f"{name}64.parquet")


def time_build(binary: Path, data: Path, source: str, queries, iters: int, cpu: int,
               memory_cap: str) -> dict[str, float | None]:
    if source == "csv":
        script = ['import "csv";',
                  f'let t = csv::read("{data}/t.csv", nulls = "<empty>");',
                  f'let r = csv::read("{data}/r.csv", nulls = "<empty>");']
    else:
        stem = "64" if source == "parquet64" else ""
        # The projection binds the tables in memory, as csv::read does. A bare
        # lazy read would have each filter re-decode the file (selected-row
        # decode, ~200 ms of the 230 ms filter_scalar took that way).
        script = ['import "parquet";',
                  f'let t = parquet::read("{data}/t{stem}.parquet")[select {{ a, b, g, h, k, v, n, s }}];',
                  f'let r = parquet::read("{data}/r{stem}.parquet")[select {{ k, w }}];']
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
    gen.add_argument("--rows", type=int, default=ROWS,
                     help="rows of t (r gets half as many unique keys); non-default sizes "
                          "belong in another --data directory")
    run = sub.add_parser("run", help="time the queries per binary")
    run.add_argument("--ibex", type=Path, action="append",
                     help="REPL binary; repeat to compare (default build-release/tools/ibex)")
    run.add_argument("--data", type=Path, default=DATA)
    run.add_argument("--rounds", type=int, default=1, help="alternations of the binaries")
    run.add_argument("--iters", type=int, default=5, help="timed collections per query")
    run.add_argument("--only", help="comma-separated query names")
    run.add_argument("--input", default="parquet64,parquet",
                     help="comma-separated: csv, parquet64 (Int64), parquet (compact Int32)")
    run.add_argument("--cpu", type=int, default=2, help="the one CPU to pin to (a P-core)")
    run.add_argument("--memory-cap", default="16G", help="systemd-run MemoryMax ('' disables)")
    args = ap.parse_args()
    if args.cmd == "gen":
        generate(args.data, args.rows)
        return
    binaries = args.ibex or [ROOT / "build-release" / "tools" / "ibex"]
    queries = QUERIES
    if args.only:
        wanted = set(args.only.split(","))
        unknown = wanted - {name for name, _ in QUERIES}
        if unknown:
            ap.error(f"unknown queries: {', '.join(sorted(unknown))}")
        queries = [q for q in QUERIES if q[0] in wanted]
    sources = args.input.split(",")
    files = {"csv": "t.csv", "parquet": "t.parquet", "parquet64": "t64.parquet"}
    if set(sources) - set(files):
        ap.error(f"--input takes {', '.join(files)}, not {args.input}")
    for source in sources:
        if not (args.data / files[source]).exists():
            ap.error(f"{args.data}/{files[source]} missing; run `{Path(__file__).name} gen` first")
    best: dict[tuple[int, str, str], float] = {}
    for _ in range(args.rounds):
        for i, binary in enumerate(binaries):
            for source in sources:
                for name, ms in time_build(binary, args.data.resolve(), source, queries,
                                           args.iters, args.cpu, args.memory_cap).items():
                    if ms is not None:
                        key = (i, source, name)
                        best[key] = min(ms, best.get(key, ms))
    if len(binaries) == 1 and len(sources) == 2:
        print("query".ljust(20) + "".join(f"{x:>11}" for x in sources) + f"{'2 vs 1':>10}")
        for name, _ in queries:
            cells = [best.get((0, x, name)) for x in sources]
            row = name.ljust(20) + "".join(
                f"{c:11.2f}" if c is not None else f"{'FAILED':>11}" for c in cells)
            if None not in cells:
                row += f"{(cells[1] / cells[0] - 1) * 100:+9.1f}%"
            print(row)
        print(f"build 0: {binaries[0]}")
        return
    head = "query".ljust(20) + f"{'input':>10}" + "".join(
        f"{f'build {i}':>12}" for i in range(len(binaries)))
    if len(binaries) == 2:
        head += f"{'1 vs 0':>10}"
    print(head)
    for name, _ in queries:
        for source in sources:
            cells = [best.get((i, source, name)) for i in range(len(binaries))]
            row = name.ljust(20) + f"{source:>10}" + "".join(
                f"{c:12.2f}" if c is not None else f"{'FAILED':>12}" for c in cells)
            if len(binaries) == 2 and None not in cells:
                row += f"{(cells[1] / cells[0] - 1) * 100:+9.1f}%"
            print(row)
    for i, binary in enumerate(binaries):
        print(f"build {i}: {binary}")


if __name__ == "__main__":
    main()
