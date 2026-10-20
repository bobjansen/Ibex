#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Which code's CPU time grows when Ibex runs a PDS-H query on more cores.

`perf_counters.py` says *whether* a query spends more core-time at N cores
than at 1 (inflation); this says *where*. Each query runs warm in one REPL
process (`:run` repeated), under `perf record` at each core count, and the
samples are bucketed by binary (libzstd, libparquet, libc, ...) and, inside
the `ibex` binary, by function family (join probe, join build, Parquet
decode, gather, aggregate, ...). Equal work spread over more cores keeps each
bucket flat; a bucket that grows is code that gets slower per unit of work
(memory contention, cross-core data) or does more of it (a parallel variant
with an extra pass).

Read the families, not single functions: a fan-out runs its body as a task
(`std::function` handler), a different symbol from the serial loop it
replaces, so per-symbol counts at 1 and N cores do not line up. Some serial
paths also inline into a caller the rules cannot name (`other`), so compare
`join build` and `other` together when one shrinks as the other grows.

Samples `cycles` when the PMU exposes it (native Linux) and `cpu-clock`
otherwise (WSL, most VMs); both measure core-time per bucket.

Usage:
    python3 benchmarking/tpch/cpu_growth.py --queries q05,q09 --cores 1,8
    # one hardware thread per physical core where siblings are adjacent
    python3 benchmarking/tpch/cpu_growth.py --queries q09 --cores 1,8 --stride 2
"""
import argparse
import collections
import os
import pathlib
import re
import subprocess
import sys
import tempfile

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent

FAMILIES = [
    ("join probe", r"probe_|emit_swapped|two_phase_probe|SwappedHit|assemble_output|JoinProbe"),
    ("join build", r"fill_partitioned_heads|build_join|try_emplace|choose_and_build"),
    ("parquet decode", r"parquet|filtered_key|sharded_|direct_decode|DirectDecode|decode"),
    ("gather/concat", r"gather|append_column|concat|Materialize|copy_|scatter"),
    ("aggregate", r"Aggregate|aggregate"),
    ("update/expr", r"[Uu]pdate|evaluat|expr|kernel"),
    ("filter/selection", r"[Ff]ilter|like|Selection"),
    ("pool/sync", r"WorkerPool|worker_pool|run_task|wait_for|Batch|cooperative|Ring|ring_"),
]


def sample_event() -> str:
    probe = subprocess.run(["perf", "stat", "-e", "cycles", "-x,", "--", "true"],
                           capture_output=True, text=True)
    ok = probe.returncode == 0 and "<not supported>" not in probe.stderr
    return "cycles" if ok else "cpu-clock"


def record(binary: pathlib.Path, plugins: pathlib.Path, query: pathlib.Path, cores: int,
           cpus: str, runs: int, event: str, out: pathlib.Path, tmp: pathlib.Path) -> list[str]:
    # The query's own csv::write sink is dropped, as bench_ibex.py does.
    body = tmp / query.name
    body.write_text("".join(line for line in query.read_text().splitlines(keepends=True)
                            if "csv::write(result" not in line))
    script = ":timing on\n" + f":run {body}\n" * runs + ":quit\n"
    env = os.environ | {"IBEX_CORES": str(cores)}
    r = subprocess.run(["taskset", "-c", cpus, "perf", "record", "-q", "-e", event, "-F", "1999",
                        "-o", str(out), "--", str(binary), "--plugin-path", str(plugins)],
                       input=script, capture_output=True, text=True, env=env, cwd=IBEX_ROOT)
    if r.returncode != 0:
        sys.exit(f"perf record failed ({query.stem}, {cores}c):\n{r.stderr[-2000:]}")
    return re.findall(r"time: ([0-9.]+ m?s)", r.stdout)


def report(data: pathlib.Path, sort: str, dso: str | None = None) -> collections.Counter:
    cmd = ["perf", "report", "-i", str(data), "-n", "--no-children", "--sort", sort, "--stdio"]
    if dso:
        cmd += ["--dsos", dso]
    out = subprocess.run(cmd, capture_output=True, text=True).stdout
    counts = collections.Counter()
    for line in out.splitlines():
        m = re.match(r"\s+[0-9.]+%\s+(\d+)\s+(.*)", line)
        if m:
            counts[m.group(2).strip()] += int(m.group(1))
    return counts


def ibex_families(data: pathlib.Path, binary: pathlib.Path) -> collections.Counter:
    by_addr = collections.Counter()
    for sym, n in report(data, "sym", binary.name).items():
        m = re.search(r"0x([0-9a-f]+)", sym)
        by_addr[m.group(1) if m else sym] += n
    addrs = [a for a in by_addr if re.fullmatch(r"[0-9a-f]+", a)]
    names = {}
    if addrs:
        lines = subprocess.run(["addr2line", "-f", "-C", "-e", str(binary)] +
                               ["0x" + a for a in addrs], capture_output=True, text=True
                               ).stdout.splitlines()
        names = {a: lines[2 * i] for i, a in enumerate(addrs)}
    families = collections.Counter()
    for addr, n in by_addr.items():
        name = names.get(addr, addr)
        families[next((f for f, rx in FAMILIES if re.search(rx, name)), "other")] += n
    return families


def table(title: str, per_core: dict[int, collections.Counter]) -> None:
    cores = sorted(per_core)
    lo, hi = per_core[cores[0]], per_core[cores[-1]]
    total_lo = sum(lo.values())
    print(f"\n{title}")
    print(f"  {'bucket':22s}" + "".join(f"{str(c) + 'c':>9s}" for c in cores) +
          f"{'ratio':>8s}{'growth % of ' + str(cores[0]) + 'c total':>26s}")
    for key in sorted(set(lo) | set(hi), key=lambda k: -(hi[k] - lo[k])):
        cells = "".join(f"{per_core[c][key]:9d}" for c in cores)
        ratio = hi[key] / lo[key] if lo[key] else float("inf")
        print(f"  {key[:22]:22s}{cells}{ratio:8.2f}{100.0 * (hi[key] - lo[key]) / total_lo:+26.1f}")
    print(f"  {'total':22s}" + "".join(f"{sum(per_core[c].values()):9d}" for c in cores) +
          f"{sum(hi.values()) / total_lo:8.2f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--queries", default="q05,q09", help="comma list, e.g. q05,q09")
    ap.add_argument("--cores", default="1,8", help="comma list of core counts")
    ap.add_argument("--stride", type=int, default=1,
                    help="taskset every STRIDE-th logical CPU (2 = one thread per core "
                         "where hyperthread siblings are adjacent; check `lscpu -e`)")
    ap.add_argument("--runs", type=int, default=7, help=":run repetitions per process")
    ap.add_argument("--binary", default=str(IBEX_ROOT / "build-release/tools/ibex"))
    ap.add_argument("--keep", help="directory to keep the perf.data files in")
    args = ap.parse_args()

    binary = pathlib.Path(args.binary)
    plugins = binary.parent
    event = sample_event()
    cores = sorted({int(c) for c in args.cores.split(",")})
    dataset = (IBEX_ROOT / "benchmarking/data/tpch/parquet").resolve()
    print(f"# event: {event}   dataset: {dataset}   binary: {binary}")
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(args.keep or td)
        tmp.mkdir(parents=True, exist_ok=True)
        for q in [q.strip() for q in args.queries.split(",") if q.strip()]:
            query = IBEX_ROOT / "benchmarking/tpch/queries" / f"{q}.ibex"
            dsos, fams = {}, {}
            for c in cores:
                cpus = ",".join(str(i * args.stride) for i in range(c))
                data = tmp / f"{q}.{c}c.data"
                times = record(binary, plugins, query, c, cpus, args.runs, event, data, tmp)
                print(f"# {q} {c}c on cpus {cpus}: {' '.join(times)}")
                dsos[c] = collections.Counter({re.sub(r"\.so[.0-9]*$", "", k): v
                                               for k, v in report(data, "dso").items()})
                fams[c] = ibex_families(data, binary)
            table(f"{q}: samples by binary", dsos)
            table(f"{q}: samples inside {binary.name}, by function family", fams)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
