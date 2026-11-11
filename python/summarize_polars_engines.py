#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Summarise a bench_polars_engines.sh run as Markdown tables.

Reads <dir>/h2o_*_<N>t.tsv (benchmarking/h2o/run.py, engines polars and
polars-ibex) and <dir>/pdsh_<N>t.json (bench_pdsh_engine.py --skip-in-memory).
h2o uses each question's minimum, as the h2o harness does; PDS-H uses medians,
as python/performance.md does. Ratios are IbexEngine over Polars
streaming, so below 1 means IbexEngine is faster.

IbexEngine prepares each query once before its timed collects: it translates
the plan and, for resident frames, imports them. Polars has no such step, so
the tables report prepare time beside the timings rather than inside them.

Usage: python/summarize_polars_engines.py python/polars-engines-results/<stamp>
"""
import csv
import json
import math
import re
import sys
from pathlib import Path

H2O_LABELS = {"J1_1e7_NA_0_0": "join", "G1_1e7_1e2_0_0": "K=1e2", "G1_1e7_1e2_5_0": "K=1e2, 5% NA",
              "G1_1e7_1e2_0_1": "K=1e2, sorted", "G1_1e7_1e1_0_0": "K=1e1", "G1_1e7_2e0_0_0": "K=2e0"}


def geomean(values):
    return math.exp(sum(math.log(v) for v in values) / len(values))


def fmt(ms):
    return f"{ms:,.1f}"


def fmt_prepare(ms):
    return "—" if ms is None else fmt(ms)


def prepare_total(values):
    values = [v for v in values if v is not None]
    return f"{sum(values) / 1000:,.1f} s" if values else "—"


def threads_of(path):
    return int(re.search(r"_(\d+)t\.", path.name).group(1))


def h2o_prepare(path):
    """{(data, question): IbexEngine prepare ms} from the run's checks sidecar."""
    sidecar = path.with_name(path.name + ".checks.json")
    prepared = {}
    if sidecar.exists():
        for result in json.loads(sidecar.read_text()).get("results", []):
            for row in result["rows"]:
                if row.get("engine") == "polars-ibex" and row.get("prepare_ms") is not None:
                    prepared[(result["data"], row["question"])] = row["prepare_ms"]
    return prepared


def h2o(path):
    """{data: [(question, desc, polars ms, ibex ms, prepare ms)]}, or raises on any unvalidated row."""
    prepared = h2o_prepare(path)
    rows = {}
    with path.open() as f:
        for r in csv.DictReader(f, delimiter="\t"):
            if r["status"] != "ok":
                raise SystemExit(f"{path}: {r['data']} {r['question']} {r['engine']}: {r['status']}")
            rows.setdefault(r["data"], {}).setdefault(r["question"], {"desc": r["desc"]})[r["engine"]] = float(r["min_ms"])
    return {data: [(q, v["desc"], v["polars"], v["polars-ibex"], prepared.get((data, q)))
                   for q, v in qs.items()]
            for data, qs in rows.items()}


def pdsh(path):
    report = json.loads(path.read_text())
    out = []
    for name, entry in report["queries"].items():
        if entry.get("status") != "passed":
            out.append((name, None, None, entry.get("status", "missing"), None))
            continue
        med = entry["median_ms"]
        out.append((name, med["polars_streaming"], med["polars_ibex"], "passed", entry.get("prepare_ms")))
    return report, out


def summary_line(pairs):
    ratios = [i / p for p, i in pairs]
    return (f"{geomean(ratios):.2f}×", f"{sum(i for _, i in pairs) / sum(p for p, _ in pairs):.2f}×",
            f"{sum(r < 1 for r in ratios)}/{len(ratios)}")


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    root = Path(sys.argv[1])
    h2o_runs = sorted(root.glob("h2o_*t.tsv"), key=threads_of, reverse=True)
    pdsh_runs = sorted(root.glob("pdsh_*t.json"), key=threads_of, reverse=True)
    if not h2o_runs and not pdsh_runs:
        raise SystemExit(f"no h2o_<N>t.tsv or pdsh_<N>t.json in {root}")

    print("IbexEngine / Polars streaming; below 1 means IbexEngine is faster.\n")
    print("| Suite | Threads | Geomean | Total | IbexEngine faster | IbexEngine prepare, total |")
    print("|---|---:|---:|---:|---:|---:|")
    h2o_data = {}
    for p in h2o_runs:
        h2o_data.setdefault(threads_of(p), {}).update(h2o(p))
    pdsh_data = {threads_of(p): pdsh(p) for p in pdsh_runs}
    for n, sets in h2o_data.items():
        for data in H2O_LABELS:
            if data in sets:
                g, t, w = summary_line([(p, i) for _, _, p, i, _ in sets[data]])
                print(f"| h2o {H2O_LABELS[data]} | {n} | {g} | {t} | {w} | "
                      f"{prepare_total(r[4] for r in sets[data])} |")
    for n, (_, queries) in pdsh_data.items():
        passed = [(p, i) for _, p, i, s, _ in queries if s == "passed"]
        g, t, w = summary_line(passed)
        print(f"| PDS-H | {n} | {g} | {t} | {w} | {prepare_total(q[4] for q in queries)} |")

    for n, sets in h2o_data.items():
        for data in H2O_LABELS:
            if data not in sets:
                continue
            print(f"\n### h2o {data} ({H2O_LABELS[data]}), {n} threads, min ms\n")
            print("| Question | Polars streaming | IbexEngine | Ratio | IbexEngine prepare |\n"
                  "|---|---:|---:|---:|---:|")
            for q, desc, p, i, prep in sets[data]:
                print(f"| {q} {desc} | {fmt(p)} | {fmt(i)} | {i / p:.2f}× | {fmt_prepare(prep)} |")
    for n, (report, queries) in pdsh_data.items():
        print(f"\n### PDS-H, {n} threads, median ms "
              f"(scale {report.get('scale_factor')}, Polars {report.get('polars_version')}, "
              f"Ibex `{report.get('git_head', '')[:8]}`)\n")
        print("| Query | Polars streaming | IbexEngine | Ratio | IbexEngine prepare |\n"
              "|---|---:|---:|---:|---:|")
        for name, p, i, s, prep in queries:
            print(f"| {name.upper()} | {fmt(p)} | {fmt(i)} | {i / p:.2f}× | {fmt_prepare(prep)} |"
                  if s == "passed" else f"| {name.upper()} | — | — | {s} | {fmt_prepare(prep)} |")


if __name__ == "__main__":
    main()
