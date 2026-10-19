#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Hardware/software counters per PDS-H query, engine and core count.

For questions of the form "why does engine X scale worse than engine Y on
query Q": idle cores, extra instructions, or the same instructions running
slower (cache, DRAM, TLB, frequency). The timing harnesses are the ones the
suite uses (`bench_ibex.py`, `bench_pdsh.py`), wrapped in `perf stat`.

**Counts cover the timed iterations only.** Each cell runs twice under
`perf stat`: warmup + 1 iteration, then warmup + 1 + K. Startup, data load
and warmup are identical in both, so `(long - short) / K` is the cost of one
warm iteration, for every engine alike. Startup jitter does not cancel (two
identical short runs differed by 150 ms of task-clock locally), so each cell
repeats the pair `--pairs` times, alternating the order, and keeps the median
delta; `spread` in `counters.tsv` is (max - min) / median of those deltas. `perf stat --control` cannot do this:
each harness runs its iterations inside one opaque process (the Ibex REPL,
upstream's query module).

Every requested event is probed first; unknown or unsupported ones are
dropped and listed in `events.tsv`, so one preset works on any box (a VM on
part of a socket exposes fewer counters than a full-socket or metal one).
Many events on one run multiplex; `pct_running` in `counters.tsv` says how
much of the run each was counted, and `--split-events` gives every event
group (a preset, the loose raw events, or an `a+b+c` token) its own run pair
instead.

Engines spin briefly before parking, so "CPUs utilized" means busy or
spinning, and spin shows up as instructions too.

Usage (polars/duckdb need the project env, hence `uv run`):

    uv run --project . benchmarking/tpch/perf_counters.py --sf 8 \\
        --engines ibex,polars --queries q01,q09 --cores 1,8 --events basic,memory

    # a variant of one engine, e.g. an allocator setting, as its own engine
    ... --engines ibex,ibex:arena1 --variant-env ibex:arena1=GLIBC_TUNABLES=glibc.malloc.arena_max=1

Output (--out, default benchmarking/tpch/results/counters): `counters.tsv`
(long format), `timings.tsv`, `events.tsv`, `meta.txt`, `raw/`, and
`report.txt` from perf_counters_report.py.
"""
import argparse
import collections
import csv
import datetime
import os
import pathlib
import shutil
import statistics
import subprocess
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(SCRIPT_DIR.parent))
import bench_env  # noqa: E402

# Presets expand in place; any other token is taken as a raw perf event. The
# micro-architectural names differ between Intel generations, so several
# spellings are listed and the probe keeps whichever this CPU knows.
PRESETS = {
    "basic": ["task-clock", "context-switches", "cpu-migrations", "page-faults",
              "cycles", "instructions"],
    "memory": ["cache-references", "cache-misses", "LLC-loads", "LLC-load-misses",
               "L1-dcache-load-misses", "dTLB-load-misses", "dTLB-store-misses",
               "branch-misses"],
    "stalls": ["cycle_activity.stalls_total", "cycle_activity.stalls_l1d_miss",
               "cycle_activity.stalls_l2_miss", "cycle_activity.stalls_l3_miss",
               "memory_activity.stalls_l1d_miss", "memory_activity.stalls_l2_miss",
               "memory_activity.stalls_l3_miss", "exe_activity.bound_on_loads",
               "exe_activity.bound_on_stores"],
    "topdown": ["topdown-retiring", "topdown-bad-spec", "topdown-fe-bound",
                "topdown-be-bound", "topdown-mem-bound", "topdown-fetch-lat",
                "topdown-br-mispredict", "topdown-heavy-ops"],
}
# Needed by the report for every derived column; always counted.
REQUIRED = ["task-clock", "cycles", "instructions"]

ENGINES = ("ibex", "polars", "duckdb")


def expand_events(spec: str) -> list[list[str]]:
    """Comma list of presets / raw events -> groups (one per preset, loose raw
    events together, `a+b+c` its own group), REQUIRED first and no duplicates
    across groups."""
    groups: list[list[str]] = []
    raw: list[str] = []
    for token in filter(None, (t.strip() for t in spec.split(","))):
        if token in PRESETS:
            groups.append(list(PRESETS[token]))
        elif "+" in token:
            groups.append([e for e in token.split("+") if e])
        else:
            raw.append(token)
    if raw:
        groups.append(raw)
    seen = set(REQUIRED)
    out = [list(REQUIRED)]
    # The rest of `basic` is software events: no PMU counter, so it rides in
    # the first group instead of costing a run pair of its own.
    if any(g == list(PRESETS["basic"]) for g in groups):
        out[0] += [e for e in PRESETS["basic"] if e not in seen]
        seen.update(out[0])
    for g in groups:
        g = [e for e in g if e not in seen]
        seen.update(g)
        if g:
            out.append(g)
    return out


def parse_perf_csv(path: pathlib.Path) -> dict[str, tuple[float | None, float, str]]:
    """perf stat -x, output -> {event: (value or None, pct_running, unit)}."""
    out = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        f = line.split(",")
        if len(f) < 3:
            continue
        value, unit, event = f[0], f[1], f[2]
        # Unprivileged runs print `cycles:u`; hybrid CPUs `cpu_core/cycles/`,
        # plus a `cpu_atom/` line for the efficiency cores (none in --cpus).
        event = event.split(":")[0]
        if event.startswith("cpu_atom/"):
            continue
        if event.startswith("cpu_core/") and event.endswith("/"):
            event = event[len("cpu_core/"):-1]
        pct = float(f[4]) if len(f) > 4 and f[4] not in ("", "<not counted>") else 0.0
        try:
            v = float(value)
        except ValueError:  # <not counted> / <not supported>
            out[event] = (None, 0.0, unit)
            continue
        # task-clock is msec in older perf, nanoseconds with an empty unit in
        # newer; store msec.
        if event == "task-clock" and unit != "msec":
            v, unit = v / 1e6, "msec"
        out[event] = (v, pct, unit)
    return out


def probe(events: list[str], tmp: pathlib.Path) -> dict[str, str]:
    """event -> 'ok' | 'zero' | 'unknown' | 'unsupported', one perf run per event.

    'zero' is counted anyway (a cross-core event can be 0 on one thread) but
    warned about: hypervisors accept some events and then read them as 0."""
    status = {}
    out = tmp / "probe.csv"
    for ev in events:
        r = subprocess.run(["perf", "stat", "-x,", "-e", ev, "-o", str(out), "--",
                            sys.executable, "-c", "sum(range(200000))"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            status[ev] = "unknown"
            continue
        values = parse_perf_csv(out)
        counted = [v for v, _, _ in values.values() if v is not None]
        status[ev] = ("unsupported" if not counted
                      else "zero" if all(v == 0 for v in counted) else "ok")
    return status


def engine_command(base: str, query: str, cores: int, iters: int, args,
                   tsv: pathlib.Path) -> tuple[list[str], dict[str, str]]:
    env = {}
    if base == "ibex":
        env["IBEX_CORES"] = str(cores)
        cmd = [sys.executable, str(SCRIPT_DIR / "bench_ibex.py"), "--warmup", str(args.warmup),
               "--iters", str(iters), "--out", str(tsv), query]
    else:
        cmd = [sys.executable, str(SCRIPT_DIR / "bench_pdsh.py"), "--engine", base,
               "--pdsh-root", str(args.pdsh_root), "--sf", args.sf, "--warmup", str(args.warmup),
               "--iters", str(iters), "--out", str(tsv)]
        if base == "polars":
            env["POLARS_MAX_THREADS"] = str(cores)
            cmd += ["--polars-engine", args.polars_engine]
        else:
            cmd += ["--threads", str(cores)]
        cmd.append(query)
    return ["taskset", "-c", ",".join(map(str, args.cpus[:cores]))] + cmd, env


def read_samples(tsv: pathlib.Path) -> list[float]:
    with tsv.open() as f:
        row = next(csv.DictReader(f, delimiter="\t"))
    return [float(x) for x in row["samples_ms"].split(",")]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="8")
    ap.add_argument("--pdsh-root", type=pathlib.Path,
                    default=pathlib.Path(os.environ.get("PDSH_ROOT", pathlib.Path.home() / "polars-benchmark")))
    ap.add_argument("--engines", default="ibex,polars",
                    help="comma list of ibex/polars/duckdb; `base:name` is a variant of "
                         "base (give it env with --variant-env)")
    ap.add_argument("--variant-env", action="append", default=[], metavar="ENGINE=K=V",
                    help="extra environment for one engine name, e.g. "
                         "ibex:arena1=GLIBC_TUNABLES=glibc.malloc.arena_max=1 (repeatable)")
    ap.add_argument("--queries", default="q01,q09,q21")
    ap.add_argument("--cores", default="1,8", help="comma list; the smallest is the scaling base")
    ap.add_argument("--cpus", default=None,
                    help="CPU list N cores are taken from, first N entries (default 0,1,2,...). "
                         "On SMT or hybrid boxes give one thread per performance core, "
                         "e.g. 0,2,4,6,8,10,12,14")
    ap.add_argument("--events", default="basic,memory,stalls",
                    help=f"presets ({', '.join(PRESETS)}) and/or raw perf events")
    ap.add_argument("--split-events", action="store_true",
                    help="one run pair per event group instead of multiplexing them all")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--iters", type=int, default=10, help="K: timed iterations in the delta")
    ap.add_argument("--pairs", type=int, default=3,
                    help="short/long pairs per cell; the median delta is kept")
    ap.add_argument("--polars-engine", default="streaming", choices=("streaming", "in-memory"))
    ap.add_argument("--out", type=pathlib.Path, default=SCRIPT_DIR / "results" / "counters")
    args = ap.parse_args()

    if shutil.which("perf") is None:
        ap.error("perf not found")
    engines = [e.strip() for e in args.engines.split(",") if e.strip()]
    for e in engines:
        if e.split(":")[0] not in ENGINES:
            ap.error(f"unknown engine {e!r}; bases are {', '.join(ENGINES)}")
    variant_env: dict[str, dict[str, str]] = {}
    for spec in args.variant_env:
        name, _, kv = spec.partition("=")
        k, _, v = kv.partition("=")
        if name not in engines or not k:
            ap.error(f"--variant-env {spec!r}: want ENGINE=KEY=VALUE for an engine in --engines")
        variant_env.setdefault(name, {})[k] = v
    if any(e.split(":")[0] != "ibex" for e in engines):
        try:
            import polars, duckdb  # noqa: F401,E401
        except ImportError:
            ap.error("polars/duckdb not importable: run under `uv run --project <ibex root>`")
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    cores_list = sorted({int(c) for c in args.cores.split(",")})
    available = len(os.sched_getaffinity(0))
    if cores_list[0] < 1 or cores_list[-1] > available:
        ap.error(f"--cores must be within 1..{available}")
    args.cpus = ([int(c) for c in args.cpus.split(",")] if args.cpus
                 else list(range(cores_list[-1])))
    if len(args.cpus) < cores_list[-1]:
        ap.error(f"--cpus lists {len(args.cpus)} CPUs, --cores needs {cores_list[-1]}")

    # bench_ibex.py reads the `parquet` symlink; point it at this scale, as
    # run_bench.sh does, so every engine reads the same tables.
    tables = bench_env.pdsh_scale_dir(args.pdsh_root, args.sf)
    if not tables.is_dir():
        ap.error(f"missing {tables}; run `make data-tables SCALE_FACTOR={float(args.sf)}` in {args.pdsh_root}")
    link = IBEX_ROOT / "benchmarking/data/tpch/parquet"
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink() or not link.exists():
        link.unlink(missing_ok=True)
        link.symlink_to(tables)
    elif link.resolve() != tables.resolve():
        ap.error(f"{link} is not a symlink and does not point at {tables}")

    out = args.out
    raw = out / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    groups = expand_events(args.events)
    status = probe([e for g in groups for e in g], raw)
    with (out / "events.tsv").open("w") as f:
        f.write("event\tstatus\n")
        for ev, st in status.items():
            f.write(f"{ev}\t{st}\n")
    zero = [e for e, s in status.items() if s == "zero"]
    if zero:
        print(f"# WARNING: read 0 on the probe (blanked by a hypervisor?): {', '.join(zero)}",
              file=sys.stderr)
    groups = [[e for e in g if status[e] in ("ok", "zero")] for g in groups]
    groups = [g for g in groups if g]
    dropped = [e for e, s in status.items() if s not in ("ok", "zero")]
    print(f"# events: {sum(map(len, groups))} counted, {len(dropped)} dropped"
          + (f" ({', '.join(dropped)})" if dropped else ""), file=sys.stderr)
    if not args.split_events:
        groups = [[e for g in groups for e in g]]

    with (out / "meta.txt").open("w") as f:
        f.write(f"date={datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")
        f.write(f"argv={' '.join(sys.argv)}\n")
        f.write(f"ibex_commit={subprocess.run(['git', '-C', str(IBEX_ROOT), 'rev-parse', 'HEAD'], capture_output=True, text=True).stdout.strip()}\n")
        f.write(f"tables={tables.resolve()}\n")
        f.write(f"perf={subprocess.run(['perf', '--version'], capture_output=True, text=True).stdout.strip()}\n")
        f.write(f"kernel={os.uname().release}\n")
        try:
            f.write(f"perf_event_paranoid={pathlib.Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip()}\n")
        except OSError:
            pass
        f.write(f"variant_env={variant_env}\n")
        f.write(subprocess.run(["lscpu"], capture_output=True, text=True).stdout)

    counters_path, timings_path = out / "counters.tsv", out / "timings.tsv"
    with counters_path.open("w") as cf, timings_path.open("w") as tf:
        cf.write("engine\tquery\tcores\tevent\tunit\tper_iter\tspread\tpct_running\n")
        tf.write("engine\tquery\tcores\tn\tmean_ms\tmin_ms\tsamples_ms\n")
        # Engines innermost, so the cells being compared run back to back.
        for cores in cores_list:
            for q in queries:
                for engine in engines:
                    base = engine.split(":")[0]
                    tag = f"{engine.replace(':', '-')}_{q}_{cores}c"
                    samples: list[float] = []
                    for gi, group in enumerate(groups):
                        deltas = collections.defaultdict(list)  # event -> [long - short]
                        pcts = collections.defaultdict(list)
                        units = {}
                        failed = False
                        for p in range(args.pairs):
                            kinds = [("short", 1), ("long", 1 + args.iters)]
                            if p % 2:
                                kinds.reverse()
                            runs = {}
                            for kind, iters in kinds:
                                stem = raw / f"{tag}_g{gi}_p{p}_{kind}"
                                tsv = stem.with_suffix(".tsv")
                                cmd, env = engine_command(base, q, cores, iters, args, tsv)
                                env = os.environ | env | variant_env.get(engine, {})
                                print(f"=== {tag} group {gi} pair {p} {kind} ===", file=sys.stderr)
                                r = subprocess.run(["perf", "stat", "-x,", "-e", ",".join(group),
                                                    "-o", f"{stem}.perf.csv", "--"] + cmd,
                                                   env=env, stdout=subprocess.DEVNULL)
                                if r.returncode != 0:
                                    print(f"  FAILED exit {r.returncode}; cell skipped",
                                          file=sys.stderr)
                                    failed = True
                                    break
                                runs[kind] = parse_perf_csv(pathlib.Path(f"{stem}.perf.csv"))
                                if kind == "long" and gi == 0:
                                    samples += read_samples(tsv)[1:]  # the K the delta covers
                            if failed:
                                break
                            for ev in group:
                                sv, ps, unit = runs["short"].get(ev, (None, 0.0, ""))
                                lv, pl, _ = runs["long"].get(ev, (None, 0.0, ""))
                                if sv is not None and lv is not None:
                                    deltas[ev].append(lv - sv)
                                    pcts[ev].append(min(ps, pl))
                                    units[ev] = unit
                        if failed:
                            break
                        for ev, ds in deltas.items():
                            med = statistics.median(ds)
                            spread = (max(ds) - min(ds)) / abs(med) if med else float("nan")
                            cf.write(f"{engine}\t{q}\t{cores}\t{ev}\t{units[ev]}\t"
                                     f"{med / args.iters:.4f}\t{spread:.3f}\t{min(pcts[ev]):.1f}\n")
                        cf.flush()
                    if samples:
                        tf.write(f"{engine}\t{q}\t{cores}\t{len(samples)}\t"
                                 f"{sum(samples) / len(samples):.3f}\t{min(samples):.3f}\t"
                                 f"{','.join(f'{x:.3f}' for x in samples)}\n")
                        tf.flush()

    report = subprocess.run([sys.executable, str(SCRIPT_DIR / "perf_counters_report.py"), str(out)],
                            capture_output=True, text=True)
    (out / "report.txt").write_text(report.stdout + report.stderr)
    print(report.stdout, end="")
    return report.returncode


if __name__ == "__main__":
    raise SystemExit(main())
