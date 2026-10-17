#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Report for a perf_counters.py output directory (stdlib only).

    python3 benchmarking/tpch/perf_counters_report.py <dir>

Three tables, all per warm iteration:

1. **Cells**: wall ms, CPUs busy (task-clock / wall), instructions, cycles,
   IPC, GHz, and every other counted event: stall/`*_activity` events as % of
   cycles, `topdown-*` as % of the level-1 slots, the rest per 1000
   instructions.
2. **Scaling**, against the smallest core count of the same engine and query.
   The identity it uses: wall = task-clock / CPUs, so
       ideal / speedup = idle x inflation
       idle      = (N / N0) / (CPUs / CPUs0)       cores not kept busy
       inflation = task-clock / task-clock0         same answer, more core-time
   and inflation splits into instr x CPI x (1 / freq). Instructions flat with
   CPI up means the same work runs slower per core (memory, contention);
   instructions up means work multiplied (or spinning).
3. **Event growth**: each non-basic event's per-iteration count at the
   largest core count over the smallest.
"""
import collections
import csv
import math
import pathlib
import sys

BASIC = {"task-clock", "cycles", "instructions", "context-switches", "cpu-migrations",
         "page-faults"}
TOPDOWN_L1 = ("topdown-retiring", "topdown-bad-spec", "topdown-fe-bound", "topdown-be-bound")


def load(d: pathlib.Path):
    counters = collections.defaultdict(dict)  # (engine, q, cores) -> {event: per_iter}
    pct = collections.defaultdict(dict)
    spread = collections.defaultdict(dict)
    with (d / "counters.tsv").open() as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (r["engine"], r["query"], int(r["cores"]))
            counters[key][r["event"]] = float(r["per_iter"])
            pct[key][r["event"]] = float(r["pct_running"])
            spread[key][r["event"]] = float(r["spread"])
    wall = {}
    with (d / "timings.tsv").open() as f:
        for r in csv.DictReader(f, delimiter="\t"):
            wall[(r["engine"], r["query"], int(r["cores"]))] = float(r["mean_ms"])
    return counters, pct, spread, wall


def fmt(x, spec):
    return "-" if x is None or (isinstance(x, float) and math.isnan(x)) else format(x, spec)


def derived(c: dict, ms: float):
    tc = c.get("task-clock")          # perf reports msec
    ins, cyc = c.get("instructions"), c.get("cycles")
    return {
        "ms": ms,
        "tc": tc,
        "cpus": tc / ms if tc and ms else None,
        "ins": ins,
        "cyc": cyc,
        "ipc": ins / cyc if ins and cyc else None,
        "ghz": cyc / (tc * 1e6) if cyc and tc else None,
    }


def event_cell(ev: str, c: dict):
    v = c.get(ev)
    if v is None:
        return None
    if ev.startswith(("cycle_activity.", "memory_activity.", "exe_activity.")):
        return 100.0 * v / c["cycles"] if c.get("cycles") else None
    if ev.startswith("topdown-"):
        total = sum(c.get(t, 0.0) for t in TOPDOWN_L1)
        return 100.0 * v / total if total else None
    return 1000.0 * v / c["instructions"] if c.get("instructions") else None


def event_label(ev: str) -> str:
    if ev.startswith(("cycle_activity.", "memory_activity.", "exe_activity.")):
        return f"{ev} %cyc"
    if ev.startswith("topdown-"):
        return f"{ev} %slots"
    return f"{ev} /kI"


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    d = pathlib.Path(sys.argv[1])
    counters, pct, spread, wall = load(d)
    keys = sorted(k for k in counters if k in wall)
    if not keys:
        print("no complete cells", file=sys.stderr)
        return 1
    extra = sorted({e for k in keys for e in counters[k]} - BASIC)

    print("## 1. Cells (per warm iteration)\n")
    head = ["engine", "query", "cores", "ms", "CPUs", "instr G", "cycles G", "IPC", "GHz",
            "faults", "ctxsw", "tc spread"]
    print("\t".join(head + [event_label(e) for e in extra]))
    low_pct = set()
    noisy: list[str] = []
    implausible: list[str] = []
    for k in keys:
        c, x = counters[k], derived(counters[k], wall[k])
        row = [k[0], k[1], str(k[2]), fmt(x["ms"], ".1f"), fmt(x["cpus"], ".2f"),
               fmt(x["ins"] and x["ins"] / 1e9, ".2f"), fmt(x["cyc"] and x["cyc"] / 1e9, ".2f"),
               fmt(x["ipc"], ".2f"), fmt(x["ghz"], ".2f"),
               fmt(c.get("page-faults"), ".0f"), fmt(c.get("context-switches"), ".0f"),
               fmt(spread[k].get("task-clock"), ".2f")]
        row += [fmt(event_cell(e, c), ".2f") for e in extra]
        print("\t".join(row))
        low_pct.update(e for e, p in pct[k].items() if 0 < p < 50)
        if spread[k].get("task-clock", 0.0) > 0.10:
            noisy.append(f"{k[0]} {k[1]} {k[2]}c")
        if k[2] == 1 and x["cpus"] is not None and not 0.9 <= x["cpus"] <= 1.1:
            implausible.append(f"{k[0]} {k[1]} 1c CPUs={x['cpus']:.2f}")
    if low_pct:
        print(f"\n# multiplexed under 50% of the run (scaled estimates, noisier): "
              f"{', '.join(sorted(low_pct))}. --split-events avoids it.")
    if noisy:
        print(f"# task-clock deltas spread over 10% across pairs (raise --iters/--pairs): "
              f"{', '.join(noisy)}")
    if implausible:
        print(f"# 1-core task-clock does not match wall (delta swamped by startup noise?): "
              f"{', '.join(implausible)}")

    print("\n## 2. Scaling vs the smallest core count (ideal/speedup = idle x inflation)\n")
    print("\t".join(["engine", "query", "cores", "speedup", "ideal", "idle x", "inflation x",
                     "instr x", "CPI x", "1/freq x"]))
    groups = collections.defaultdict(list)
    for k in keys:
        groups[(k[0], k[1])].append(k)
    for (engine, q), ks in groups.items():
        ks.sort(key=lambda k: k[2])
        b = derived(counters[ks[0]], wall[ks[0]])
        for k in ks[1:]:
            x = derived(counters[k], wall[k])
            n_rel = k[2] / ks[0][2]
            idle = n_rel / (x["cpus"] / b["cpus"]) if x["cpus"] and b["cpus"] else None
            infl = x["tc"] / b["tc"] if x["tc"] and b["tc"] else None
            instr = x["ins"] / b["ins"] if x["ins"] and b["ins"] else None
            cpi = (b["ipc"] / x["ipc"]) if x["ipc"] and b["ipc"] else None
            freq = (b["ghz"] / x["ghz"]) if x["ghz"] and b["ghz"] else None
            print("\t".join([engine, q, f"{ks[0][2]}->{k[2]}", fmt(b["ms"] / x["ms"], ".2f"),
                             fmt(n_rel, ".0f"), fmt(idle, ".3f"), fmt(infl, ".3f"),
                             fmt(instr, ".3f"), fmt(cpi, ".3f"), fmt(freq, ".3f")]))

    if extra:
        print("\n## 3. Event growth, largest / smallest core count (per iteration)\n")
        print("\t".join(["engine", "query", "cores"] + extra))
        for (engine, q), ks in groups.items():
            if len(ks) < 2:
                continue
            lo, hi = counters[ks[0]], counters[ks[-1]]
            ratios = [fmt(hi[e] / lo[e] if lo.get(e) and e in hi else None, ".2f") for e in extra]
            print("\t".join([engine, q, f"{ks[0][2]}->{ks[-1][2]}"] + ratios))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
