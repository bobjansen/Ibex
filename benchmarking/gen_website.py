#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Generate a self-contained benchmark website from result CSV(s).

Usage:
  python3 gen_website.py results/<latest>.csv \\
      [--pdsh results/tpch_aws_<ts>.tar.gz] [--h2o results/h2o_aws_<ts>.tar.gz]
  # -> writes docs/benchmarks.html (page on the Ibex docs site)

The page has three sections: Ibex's own in-memory suite (the CSV), PDS-H
(`--pdsh`: a run-tpch.sh artifact, or its results/history.tsv) and the h2o
db-benchmark (`--h2o`: a run-h2o.sh artifact, or h2o/run.py TSVs; later TSVs
override earlier ones per data set and core count). A section with no input
is left off the page.

Each AWS run now produces a complete, self-contained CSV (see
benchmarking/aws/bootstrap.sh), so the page is generated from that one run — no
backfilling from older results. Multiple CSVs are still merged in order (later
files override earlier per (scale, query, framework) cell) if you ever need to
splice runs, but the default workflow passes a single latest CSV.

The output is a single HTML file (data embedded as JSON, vanilla JS) — open it
directly or publish it (e.g. GitHub Pages). Re-run after each AWS run to refresh.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import html
import json
import re
import sys
import tarfile
from collections import defaultdict
from pathlib import Path

from print_table import FRAMEWORK_ORDER, QUERY_LABEL, QUERY_ORDER
from extract_bench_code import extract_all, SOURCES

GITHUB = "https://github.com/bobjansen/Ibex/blob/main"

BENCHMARK_NAV = """  <nav class="nav">
    <span class="nav-logo">Ibex</span>
    <div class="nav-links">
      <a class="nav-link" href="./index.html">Home</a>
      <a class="nav-link" href="./getting-started.html">Getting started</a>
      <a class="nav-link active" href="./benchmarks.html">Benchmarks</a>
      <a class="nav-link" href="./comparison.html">Comparison</a>
      <a class="nav-link" href="./reference.html">Reference</a>
      <a class="nav-link" href="./io.html">I/O</a>
      <a class="nav-link" href="./functions.html">Function reference</a>
      <a class="nav-link" href="./cheatsheet.html">Cheat sheet</a>
      <a class="nav-link" href="https://github.com/bobjansen/Ibex" target="_blank" rel="noopener">GitHub &#8599;</a>
    </div>
  </nav>"""

# Engines shown on the code page, in display order: (engine_key, label, lang).
# polars-st and ibex+parse run byte-identical code to polars / ibex (only the
# thread count / parse-timing differs), so they're annotated, not duplicated.
CODE_ENGINES: list[tuple[str, str, str]] = [
    ("ibex", "Ibex", "ibex"),
    ("polars", "Polars", "python"),
    ("duckdb", "DuckDB", "sql"),
    ("datafusion", "DataFusion", "sql"),
    ("clickhouse", "ClickHouse", "sql"),
    ("pandas", "pandas", "python"),
    ("data.table", "data.table", "r"),
    ("dplyr", "dplyr", "r"),
]

# Curated category grouping (order matters; queries not listed fall to "Other").
CATEGORIES: list[tuple[str, list[str]]] = [
    ("Group-by / aggregation", [
        "mean_by_symbol", "ohlc_by_symbol", "median_by_symbol",
        "quantile_by_symbol", "std_by_symbol", "count_by_symbol_day",
        "mean_by_symbol_day", "ohlc_by_symbol_day", "sum_by_user",
        "distinct_symbol"]),
    ("Column ops / update", [
        "update_price_x2", "cumsum_price", "cumprod_price", "rand_uniform",
        "rand_normal", "rand_int", "rand_bernoulli"]),
    ("Scalar math builtins", [
        "abs_price", "sqrt_price", "log_price", "exp_price",
        "round_price", "floor_price", "ceil_price"]),
    ("Grouped window functions", [
        "rank_by_symbol", "lag_by_symbol", "cumsum_by_symbol"]),
    ("Filters", [
        "filter_simple", "filter_and", "filter_arith", "filter_or",
        "filter_events"]),
    ("Sort / top-k", [
        "sort_price", "sort_price_desc", "sort_symbol",
        "sort_symbol_price", "sort_symbol_price_desc",
        "order_head_topk", "order_head_topk_by_symbol",
        "order_tail_topk", "order_tail_topk_by_symbol"]),
    ("Multi-stage pipeline", [
        "filter_group_sort", "update_group_filter", "group_rank_filter",
        "normalize_by_group", "symbol_day_to_symbol"]),
    ("Transforms", [
        "pmin_clip", "where_update_clip", "where_update_expr", "where_update_multi",
        "where_update_window", "where_update_nullable", "corr_price_vol"]),
    ("Joins", [
        "null_left_join", "null_semi_join", "null_anti_join",
        "inner_join_symbol", "inner_join_user",
        "null_cross_join_small", "join_update_group", "join_filter_rank"]),
    ("Reshape", [
        "melt_wide_to_long", "dcast_long_to_wide",
        "dcast_long_to_wide_int_pivot", "dcast_long_to_wide_cat_pivot"]),
    ("Fill / null propagation", ["fill_null", "fill_forward", "fill_backward"]),
    ("Time-series (TimeFrame)", [
        "as_timeframe", "tf_lag1", "tf_rolling_count_1m", "tf_rolling_sum_1m",
        "tf_rolling_mean_5m", "tf_rolling_median_1m", "tf_rolling_std_1m",
        "tf_rolling_ewma_1m", "tf_resample_1m_ohlc", "tf_asof_join",
        "tf_asof_join_by_symbol"]),
]

# Frameworks we don't surface on the page (near-duplicate of ibex).
SKIP_FRAMEWORKS = {"ibex+parse", "ibex-compiled"}


def load(paths: list[Path]):
    # cells[scale][query][framework] = avg_ms ; later paths override.
    # mem[scale][query][framework]   = peak_rss_mb (absolute peak RSS, MiB).
    # peak_rss_mb is optional: older CSVs predate the column, and a <=0 value
    # means the harness couldn't measure it (non-Linux) — both are left absent.
    cells: dict[int, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(dict))
    mem: dict[int, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(dict))
    for p in paths:
        with p.open(newline="") as f:
            for r in csv.DictReader(f):
                try:
                    n = int(r["dataset_rows"])
                    ms = float(r["avg_ms"])
                except (KeyError, ValueError):
                    continue
                fw = r["framework"]
                if fw in SKIP_FRAMEWORKS:
                    continue
                cells[n][r["query"]][fw] = ms
                try:
                    mb = float(r.get("peak_rss_mb", ""))
                except (TypeError, ValueError):
                    mb = 0.0
                if mb > 0:
                    mem[n][r["query"]][fw] = mb
    return cells, mem


def commit_from_names(paths: list[Path]) -> str:
    for p in reversed(paths):
        m = re.search(r"_([0-9a-f]{8})\b", p.name)
        if m:
            return m.group(1)
    return "unknown"


def build_payload(cells, mem, paths, commit=None):
    scales = sorted(cells)
    # queries present anywhere, ordered by curated category then QUERY_ORDER.
    present = {q for n in cells for q in cells[n]}
    ordered_queries: list[tuple[str, str, str]] = []  # (id, label, category)
    seen: set[str] = set()
    for cat, qs in CATEGORIES:
        for q in qs:
            if q in present:
                ordered_queries.append((q, QUERY_LABEL.get(q, q), cat))
                seen.add(q)
    # any present query not in the curated list, appended under "Other".
    for q in QUERY_ORDER:
        if q in present and q not in seen:
            ordered_queries.append((q, QUERY_LABEL.get(q, q), "Other"))
            seen.add(q)

    present_fw = {fw for n in cells for q in cells[n] for fw in cells[n][q]}
    frameworks = [fw for fw in FRAMEWORK_ORDER if fw in present_fw]
    frameworks += sorted(fw for fw in present_fw if fw not in frameworks)

    data = {
        str(n): {q: cells[n][q] for q in cells[n]} for n in scales
    }
    mem_data = {
        str(n): {q: mem[n][q] for q in mem[n]} for n in scales if n in mem
    }
    return {
        "scales": scales,
        "frameworks": frameworks,
        "queries": [{"id": q, "label": lbl, "category": cat}
                    for q, lbl, cat in ordered_queries],
        "data": data,
        "mem": mem_data,
        "meta": {
            "generated": _dt.datetime.now(_dt.timezone.utc)
                .strftime("%Y-%m-%d %H:%M UTC"),
            "commit": commit or commit_from_names(paths),
            "sources": [p.name for p in paths],
        },
    }


# ── PDS-H and h2o: independent suites, read from their own artifacts ────────

# PDS-H frameworks shown, in order: Ibex and the upstream Polars-streaming and
# DuckDB implementations. The -st rows are one-thread runs inside each core
# count; the core switch already covers that axis, so they are not shown.
PDSH_ENGINES = [("ibex", "Ibex"), ("pdsh-polars-stream", "Polars"), ("pdsh-duckdb", "DuckDB")]


def _tar_member_texts(path: Path, pattern: str) -> list[tuple[str, str]]:
    """(name, text) of every member of a .tar.gz whose name matches `pattern`."""
    out = []
    with tarfile.open(path, "r:gz") as tar:
        for m in tar.getmembers():
            if m.isfile() and re.search(pattern, m.name):
                f = tar.extractfile(m)
                if f is not None:
                    out.append((m.name, f.read().decode("utf-8")))
    return sorted(out)


def load_pdsh(path: Path) -> dict:
    """PDS-H payload from a run-tpch.sh artifact or its results/history.tsv."""
    if path.name.endswith(".tar.gz"):
        texts = _tar_member_texts(path, r"results/history\.tsv$")
        versions = dict(_tar_member_texts(path, r"results/versions\.txt$"))
        manifests = _tar_member_texts(path, r"results/runs/[^/]+/manifest\.json$")
        if not texts:
            raise ValueError(f"{path}: no results/history.tsv")
        history = texts[0][1]
    else:
        history, versions, manifests = path.read_text(encoding="utf-8"), {}, []
    rows = list(csv.DictReader(history.splitlines(), delimiter="\t"))
    wanted = dict(PDSH_ENGINES)
    tables: dict[str, dict] = {}
    commit, sf = "unknown", None
    for r in rows:
        if r.get("framework") not in wanted:
            continue
        try:
            ms = float(r["avg_ms"])
        except (KeyError, ValueError):
            continue
        cores = r["cores"]
        table = tables.setdefault(cores, {"rows": [], "cells": {}})
        q = r["query"]
        if q not in table["cells"]:
            table["rows"].append({"id": q, "label": q})
            table["cells"][q] = {}
        table["cells"][q][r["framework"]] = ms
        commit, sf = r.get("commit", commit), r.get("scale_factor", sf)
    for t in tables.values():
        t["rows"].sort(key=lambda row: row["id"])
    cpu = ""
    for _, text in manifests:
        try:
            cpu = json.loads(text).get("cpu", "") or cpu
        except ValueError:
            pass
    version_text = next(iter(versions.values()), "")
    found = dict(line.split("=", 1) for line in version_text.splitlines() if "=" in line)
    cores = sorted(tables, key=int)
    return {
        "engines": [{"id": k, "label": v} for k, v in PDSH_ENGINES],
        "selectors": [{"key": "cores", "label": "Cores",
                       "options": [{"value": c, "label": c} for c in cores],
                       "default": cores[-1] if cores else None}],
        "tables": tables,
        "meta": {"commit": commit[:8], "scale_factor": sf, "cpu": cpu,
                 "polars": found.get("polars", ""), "duckdb": found.get("duckdb", ""),
                 "source": path.name},
    }


H2O_ENGINES = [("ibex", "Ibex"), ("polars", "Polars"), ("duckdb", "DuckDB")]
H2O_DATA_LABEL = {
    "J1_1e7_NA_0_0": "join",
    "G1_1e7_1e2_0_0": "groupby K=1e2",
    "G1_1e7_1e2_5_0": "groupby K=1e2, 5% NA",
    "G1_1e7_1e2_0_1": "groupby K=1e2, sorted",
    "G1_1e7_1e1_0_0": "groupby K=1e1",
    "G1_1e7_2e0_0_0": "groupby K=2e0",
}


def load_h2o(paths: list[Path]) -> dict:
    """h2o payload from run-h2o.sh artifacts or h2o/run.py TSVs, in order."""
    texts: list[tuple[str, str]] = []
    cpu = ""
    for p in paths:
        if p.name.endswith(".tar.gz"):
            texts += _tar_member_texts(p, r"results/h2o_[^/]*\.tsv$")
            for _, v in _tar_member_texts(p, r"results/versions\.txt$"):
                m = re.search(r"Model name:\s*(.+)", v)
                cpu = m.group(1).strip() if m else cpu
        else:
            texts.append((p.name, p.read_text(encoding="utf-8")))
    # cells[(data, threads)][question][engine] = min ms; a later file wins per
    # (data, threads), so a re-run of one set replaces it whole.
    tables: dict[str, dict] = {}
    versions: dict[str, set] = defaultdict(set)
    for _, text in texts:
        fresh: dict[str, dict] = {}
        for r in csv.DictReader(text.splitlines(), delimiter="\t"):
            if r.get("status") != "ok":
                continue
            key = f"{r['data']}|{r['threads']}"
            table = fresh.setdefault(key, {"rows": [], "cells": {}})
            q = r["question"]
            if q not in table["cells"]:
                table["rows"].append({"id": q, "label": f"{q} {r['desc']}"})
                table["cells"][q] = {}
            table["cells"][q][r["engine"]] = float(r["min_ms"])
            versions[r["engine"]].add(r["version"])
        tables.update(fresh)
    for t in tables.values():
        t["rows"].sort(key=lambda row: int(row["id"][1:]))
    data_sets = sorted({k.split("|")[0] for k in tables},
                       key=lambda d: list(H2O_DATA_LABEL).index(d) if d in H2O_DATA_LABEL else 99)
    threads = sorted({k.split("|")[1] for k in tables}, key=int)
    return {
        "engines": [{"id": k, "label": v} for k, v in H2O_ENGINES],
        "selectors": [
            {"key": "data", "label": "Data",
             "options": [{"value": d, "label": H2O_DATA_LABEL.get(d, d)} for d in data_sets],
             "default": data_sets[1] if len(data_sets) > 1 else (data_sets or [None])[0]},
            {"key": "threads", "label": "Cores",
             "options": [{"value": t, "label": t} for t in threads],
             "default": threads[-1] if threads else None},
        ],
        "tables": tables,
        "meta": {"versions": {k: sorted(v) for k, v in versions.items()}, "cpu": cpu,
                 "sources": [p.name for p in paths]},
    }


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ibex | Benchmarks vs Polars, DuckDB, ClickHouse, DataFusion, data.table</title>
  <meta name="description" content="Ibex benchmarked against Polars, DuckDB, ClickHouse, DataFusion and data.table across columnar DataFrame queries from 1M to __MAXM__M rows." />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="./styles.css" />
  <style>
    .bench-controls { display: flex; flex-wrap: wrap; gap: 1.2rem; align-items: center;
      margin: 0 0 1.1rem; }
    .bench-controls .grp { display: flex; gap: 0.35rem; align-items: center; }
    .bench-controls .lbl { color: var(--muted); font-size: 0.85rem; font-weight: 600;
      margin-right: 0.2rem; }
    button.opt { font-family: inherit; font-size: 0.85rem; cursor: pointer;
      padding: 0.28rem 0.7rem; border-radius: 7px; border: 1px solid var(--line);
      background: #fff; color: var(--ink); }
    button.opt:hover { border-color: var(--accent); }
    button.opt.on { background: var(--accent); border-color: var(--accent); color: #fff;
      font-weight: 600; }
    .columns-grp { position: relative; }
    .column-picker { position: relative; }
    .column-picker summary { min-width: 8.5rem; padding: 0.28rem 2rem 0.28rem 0.7rem;
      border: 1px solid var(--line); border-radius: 7px; background: #fff;
      font-size: 0.85rem; cursor: pointer; list-style: none; }
    .column-picker summary::-webkit-details-marker { display: none; }
    .column-picker summary::after { content: "\\25be"; position: absolute; right: 0.7rem;
      color: var(--muted); }
    .column-picker[open] summary { border-color: var(--accent); }
    .column-menu { position: absolute; top: calc(100% + 0.35rem); right: 0; z-index: 5;
      min-width: 13rem; max-height: 20rem; overflow-y: auto; padding: 0.55rem;
      border: 1px solid var(--line); border-radius: 9px; background: var(--paper);
      box-shadow: 0 8px 24px rgba(22,32,31,.14); }
    .column-actions { display: flex; gap: 0.35rem; padding-bottom: 0.4rem;
      margin-bottom: 0.25rem; border-bottom: 1px solid var(--line); }
    .column-actions button { border: 0; background: transparent; color: var(--accent);
      font-family: inherit; font-size: 0.78rem; font-weight: 600; cursor: pointer;
      padding: 0.2rem 0.35rem; }
    .column-option { display: flex; align-items: center; gap: 0.5rem; padding: 0.3rem 0.35rem;
      border-radius: 5px; font-size: 0.85rem; cursor: pointer; }
    .column-option:hover { background: rgba(10,127,121,.07); }
    .column-option input { accent-color: var(--accent); }
    .tablewrap { width: min(1400px, calc(100vw - 2.4rem)); margin-left: 50%;
      transform: translateX(-50%); overflow-x: auto; border: 1px solid var(--line);
      border-radius: 12px; background: var(--paper); }
    table.bench { margin: 0; }
    .bench thead th { position: sticky; top: 0; background: var(--paper); z-index: 2; }
    .bench th.qcol, .bench td.qcol { position: sticky; left: 0; background: var(--paper);
      z-index: 1; text-align: left; }
    .bench thead th.qcol { z-index: 3; }
    .bench th.ibex { color: var(--accent); }
    .bench td.ibex-col { box-shadow: inset 2px 0 0 rgba(10,127,121,.5),
      inset -2px 0 0 rgba(10,127,121,.5); }
    .bench tr.cat td { background: rgba(10,127,121,0.07); color: var(--accent);
      font-weight: 700; text-align: left; letter-spacing: 0.02em;
      border-bottom: 1px solid var(--line); }
    .bench td.cell { color: #16201f; border-radius: 3px; }
    .bench td.best { font-weight: 700; outline: 1.5px solid var(--accent);
      outline-offset: -2px; }
    .bench td.na { color: var(--muted); }
    .bench tr.sum td { font-weight: 600; background: var(--paper); }
    .bench tr.sum td.qcol { color: var(--accent); }
    .headline { display: flex; flex-wrap: wrap; gap: 0.6rem; margin-top: 1.1rem; }
    .headline a { display: block; padding: 0.7rem 0.95rem; border: 1px solid var(--line);
      border-radius: 10px; background: var(--paper); color: var(--ink); text-decoration: none;
      font-size: 0.9rem; line-height: 1.35; }
    .headline a:hover { border-color: var(--accent); }
    .headline strong { color: var(--accent); }
  </style>
</head>
<body>
<main class="page">

__BENCHMARK_NAV__

  <section class="hero">
    <img class="hero-logo" src="./Ibex.png" alt="Ibex logo" />
    <p class="eyebrow">Benchmarks</p>
    <h1>How fast is Ibex?</h1>
    <p class="lead">
      __LEAD_SUITES__ Each section names the machine and the commit it was
      measured on. Lower is better.
    </p>
    <div class="headline" id="headline"></div>
  </section>

  <section class="section" id="inmemory">
    <p class="section-label">Ibex's own suite</p>
    <h2>In-memory DataFrame queries</h2>
    <p class="section-sub">
      Ibex against <strong>Polars</strong>, <strong>DuckDB</strong>,
      <strong>ClickHouse</strong>, <strong>DataFusion</strong> and
      <strong>data.table</strong> on the same columnar queries, from 1M to __MAXM__M
      rows. Times are the mean of timed iterations.
    </p>
    <div class="stats" id="stats"></div>
    <p class="section-sub">
      Pick a row count and a metric. Each row is colour-scaled green&rarr;red
      against the best engine in that row &mdash; fastest for time, lightest for
      memory &mdash; and the best cell is outlined. Hover any cell for the exact
      time and peak memory.
    </p>
    <p class="bench-note">
      <strong>What this measures.</strong> Each engine loads the data once into
      its own in-memory format, untimed, and every query is timed from there
      until its full result is materialised. DuckDB hands its result over as an
      Arrow table: measured against writing it into a DuckDB table, that is the
      faster of the two overall. Ibex's CSV reader dictionary-encodes string
      columns automatically, so a group-by on <code>symbol</code> runs on
      integer codes. Polars is given the same encoding (its string columns are
      cast to <code>Categorical</code>); DuckDB, DataFusion, ClickHouse and
      data.table group on plain strings, as they do by default. Tables are read
      once and queried many times, so that is a real advantage, and part of the
      distance to the SQL engines; Polars is the closest like-for-like
      comparison on this page.
    </p>

    <div class="bench-controls">
      <div class="grp"><span class="lbl">Rows</span><span id="scales"></span></div>
      <div class="grp"><span class="lbl">Show</span>
        <button class="opt" id="m-ms">Time (ms)</button>
        <button class="opt" id="m-ratio">&times; fastest</button>
        <button class="opt" id="m-mem">Memory (MB)</button>
        <button class="opt" id="m-memratio">&times; least mem</button>
      </div>
      <div class="grp columns-grp"><span class="lbl">Columns</span>
        <details class="column-picker" id="column-picker">
          <summary id="column-summary">All columns</summary>
          <div class="column-menu" id="column-menu"></div>
        </details>
      </div>
    </div>

    <div class="tablewrap"><table class="bench" id="tbl"></table></div>

    <p class="bench-note" style="margin-top:1rem">
      <strong>Threads.</strong> Every engine gets the same box: 8 vCPU, which
      is <strong>4 physical cores</strong> with 2 hardware threads each &mdash;
      EC2 counts hyperthreads as vCPUs, so these are 4-core numbers, not 8. Ibex
      parallelises some operators by default, so <code>ibex</code> is not a
      single-core number; <code>ibex-st</code> is the same build run with
      <code>IBEX_CORES=1</code>, which makes <code>ibex</code> vs
      <code>ibex-st</code> Ibex's own threading gain. <code>polars-st</code>,
      <code>duckdb-st</code>, <code>datafusion-st</code> and
      <code>clickhouse-st</code> are the equivalent single-thread runs of their
      engines, so <code>ibex-st</code> against those is the per-core comparison.
    </p>
    <p class="bench-note">
      <strong>Memory.</strong> The memory metrics show absolute peak resident set
      size (RSS) during a query's timed iterations &mdash; the footprint to run
      the op, including the already-resident input table &mdash; measured per
      process from the kernel's high-water mark. Multi-threaded engines trade
      memory for the parallel speed-ups noted above.
    </p>
    <p class="bench-note">
      <strong>Caveats.</strong> Every engine now materialises its full result.
      <code>tf rolling EWMA</code> uses the bounded window definition in Ibex,
      pandas and Polars. ClickHouse statistical/OHLC cells <em>cannot compute
      correctly under the shared semantics</em> and are excluded from claims.
      Each page is generated from a single run; <code>SQLite</code> and the
      data.table <code>rolling median/std</code> cells are omitted (they dominate
      wall-clock and add no competitive signal).
    </p>
    <p class="bench-note">
      <strong>The code behind these numbers.</strong> Every query's exact code, in
      every engine, is on the <a href="./methodology.html">Methodology &amp;
      code</a> page. Directly extracted from the harness source.
    </p>
    <p class="bench-note" id="meta"></p>
  </section>

  <section class="section" id="h2o">
    <p class="section-label">Independent benchmark</p>
    <h2>h2o db-benchmark</h2>
    <p class="section-sub">
      The h2oai db-benchmark questions, as maintained by DuckDB Labs
      (<a href="https://github.com/duckdblabs/db-benchmark" target="_blank" rel="noopener">duckdblabs/db-benchmark</a>,
      pinned at <code>3b074bc319</code>): ten group-by questions on 10M rows at three
      key cardinalities (K=100, 10 and 2, the last with 5M groups in <code>id3</code>),
      with 5% nulls and pre-sorted variants, and five joins. None of these queries
      or data shapes were written alongside Ibex. Pick a data set and a core count.
    </p>
    <p class="bench-note">
      <strong>What this measures.</strong> The data comes from the upstream R
      generators, unmodified, and every engine reads the same CSV. Polars and
      DuckDB use the schemas of the upstream solutions: Polars runs its streaming
      engine with Int32 ids and string keys cast to <code>Categorical</code>;
      DuckDB uses <code>ENUM</code> keys where upstream does and a FLOAT
      <code>v3</code>. Ibex declares the same string keys
      <code>Categorical</code> and otherwise picks its own types (Int64 and
      Float64, which move more bytes than the upstream Int32/Float32). Loading is
      not timed; each question runs 1 warmup and 5 timed iterations, fully
      materialising its result, and the table shows the fastest. Every answer is
      checked across engines: the row count and upstream's checksum columns
      must agree.
    </p>
    <div class="bench-controls" id="h2o-controls"></div>
    <div class="tablewrap"><table class="bench" id="h2o-tbl"></table></div>
    <p class="bench-note" style="margin-top:1rem">
      The summary rows compare Ibex with each engine over the questions all three
      completed: the geometric mean of the per-question ratios, and the ratio of
      the totals. Below 1 means Ibex is faster. The harness is
      <a href="__GITHUB__/benchmarking/h2o/run.py" target="_blank" rel="noopener">benchmarking/h2o/run.py</a>.
    </p>
    <p class="bench-note" id="h2o-meta"></p>
  </section>

  <section class="section" id="pdsh">
    <p class="section-label">Independent benchmark</p>
    <h2>PDS-H (TPC-H)</h2>
    <p class="section-sub">
      Polars' TPC-H-derived benchmark
      (<a href="https://github.com/pola-rs/polars-benchmark" target="_blank" rel="noopener">pola-rs/polars-benchmark</a>):
      22 decision-support queries over a star schema, read from Parquet. Polars
      (streaming engine) and DuckDB run upstream's own query code; Ibex runs its
      translations of the same queries. Pick a core count.
    </p>
    <p class="bench-note">
      <strong>What this measures.</strong> The Parquet files come from upstream's
      generator, never from Ibex, and each query reads its inputs from them, as
      upstream PDS-H does. Every Ibex answer is diffed against Polars at the
      benchmarked scale before anything is timed. Times are the mean of 5 timed
      iterations after 1 warmup.
    </p>
    <div class="bench-controls" id="pdsh-controls"></div>
    <div class="tablewrap"><table class="bench" id="pdsh-tbl"></table></div>
    <p class="bench-note" style="margin-top:1rem">
      The summary rows compare Ibex with each engine: the geometric mean of the
      per-query ratios, and the ratio of the totals. Below 1 means Ibex is
      faster. The queries are in
      <a href="__GITHUB__/benchmarking/tpch/queries" target="_blank" rel="noopener">benchmarking/tpch/queries</a>.
    </p>
    <p class="bench-note" id="pdsh-meta"></p>
  </section>

</main>

<script>
const PAYLOAD = __PAYLOAD__;
const H2O = __H2O__;
const PDSH = __PDSH__;
const state = { scale: null, metric: "ms", columns: new Set() };

function fmt(ms) {
  if (ms < 1) return ms.toFixed(3) + " ms";
  if (ms < 10) return ms.toFixed(2) + " ms";
  if (ms < 1000) return ms.toFixed(1) + " ms";
  return (ms / 1000).toFixed(2) + " s";
}
function fmtMem(mb) {
  if (mb < 10) return mb.toFixed(1) + " MB";
  if (mb < 1024) return mb.toFixed(0) + " MB";
  return (mb / 1024).toFixed(2) + " GB";
}
function heat(ratio) {
  // ratio >= 1; 1x = pale green, >=8x = pale red, log-scaled (light theme).
  const t = Math.min(1, Math.log(ratio) / Math.log(8));
  const hue = 142 - 130 * t;            // 142 green -> 12 red
  const light = 90 - 8 * t;             // stays light for dark ink text
  return `hsl(${hue.toFixed(0)}, 62%, ${light.toFixed(0)}%)`;
}

function render() {
  const P = PAYLOAD;
  const useMem = state.metric === "mem" || state.metric === "memratio";
  const useRatio = state.metric === "ratio" || state.metric === "memratio";
  const src = (useMem ? (P.mem || {}) : P.data)[state.scale] || {};
  const tsrc = P.data[state.scale] || {};                  // times for tooltips
  const msrc = (P.mem || {})[state.scale] || {};           // memory for tooltips
  const unit = useMem ? "MB" : "ms";
  const best_lbl = useMem ? "lightest" : "fastest";
  const fws = P.frameworks.filter(fw => state.columns.has(fw));
  let h = "<thead><tr><th class='qcol'>query</th>";
  for (const fw of fws)
    h += `<th class='${fw === "ibex" ? "ibex" : ""}'>${fw}</th>`;
  h += "</tr></thead><tbody>";

  if (!fws.length) {
    document.getElementById("tbl").innerHTML = h
      + "<tr><td class='qcol'>Select at least one column to compare.</td></tr></tbody>";
    return;
  }

  let lastCat = null;
  for (const q of P.queries) {
    const cell = src[q.id] || {};
    const vals = fws.map(fw => cell[fw]).filter(v => v != null);
    if (!vals.length) continue;
    if (q.category !== lastCat) {
      h += `<tr class='cat'><td class='qcol'>${q.category}</td>`
         + `<td colspan='${fws.length}'></td></tr>`;
      lastCat = q.category;
    }
    const tcell = tsrc[q.id] || {}, mcell = msrc[q.id] || {};
    const best = Math.min(...vals);
    h += `<tr><td class='qcol'>${q.label}</td>`;
    for (const fw of fws) {
      const v = cell[fw];
      const ic = fw === "ibex" ? " ibex-col" : "";
      if (v == null) { h += `<td class='na${ic}'>&ndash;</td>`; continue; }
      const ratio = v / best, isBest = v === best;
      const txt = useRatio
        ? (isBest ? "1.0\\u00d7" : ratio.toFixed(1) + "\\u00d7")
        : (useMem ? fmtMem(v) : fmt(v));
      const tms = tcell[fw], tmb = mcell[fw];
      const tip = `${fw}: ` + (tms != null ? `${tms.toFixed(3)} ms` : "\\u2013")
                + (tmb != null ? `, ${tmb.toFixed(1)} MB peak` : "")
                + ` (${ratio.toFixed(2)}\\u00d7 ${best_lbl})`;
      h += `<td class='cell${isBest ? " best" : ""}${ic}' style='background:${heat(ratio)}' `
         + `title='${tip}'>${txt}</td>`;
    }
    h += "</tr>";
  }
  document.getElementById("tbl").innerHTML = h + "</tbody>";
}

function setMetric(m) {
  state.metric = m;
  for (const [id, key] of [["m-ms","ms"],["m-ratio","ratio"],
                           ["m-mem","mem"],["m-memratio","memratio"]]) {
    const el = document.getElementById(id);
    if (el) el.classList.toggle("on", m === key);
  }
  render();
}
function setScale(s) {
  state.scale = s;
  document.querySelectorAll("#scales button").forEach(b =>
    b.classList.toggle("on", b.dataset.s === s));
  render();
}

function setColumns(columns) {
  state.columns = new Set(columns);
  document.querySelectorAll("#column-menu input[type=checkbox]").forEach(input => {
    input.checked = state.columns.has(input.value);
  });
  const count = state.columns.size;
  document.getElementById("column-summary").textContent =
    count === PAYLOAD.frameworks.length ? "All columns" : `${count} of ${PAYLOAD.frameworks.length}`;
  render();
}

function initColumnPicker() {
  const menu = document.getElementById("column-menu");
  menu.innerHTML = "<div class='column-actions'>"
    + "<button type='button' data-columns='all'>Select all</button>"
    + "<button type='button' data-columns='none'>Clear</button></div>"
    + PAYLOAD.frameworks.map(fw => `<label class='column-option'>`
      + `<input type='checkbox' value='${fw}'>${fw}</label>`).join("");
  menu.addEventListener("change", event => {
    if (!event.target.matches("input[type=checkbox]")) return;
    setColumns(Array.from(menu.querySelectorAll("input:checked"), input => input.value));
  });
  menu.addEventListener("click", event => {
    const action = event.target.dataset.columns;
    if (action === "all") setColumns(PAYLOAD.frameworks);
    if (action === "none") setColumns([]);
  });
  setColumns(PAYLOAD.frameworks);
}

// ── PDS-H and h2o: one heat table per suite, keyed by its selectors ────────
function geomean(xs) {
  return Math.exp(xs.reduce((a, x) => a + Math.log(x), 0) / xs.length);
}
function suiteTable(suite, sel) {
  return suite.tables[suite.selectors.map(s => sel[s.key]).join("|")];
}
// Ibex over each engine across rows every engine completed:
// { engine: {geo, total, n} }.
function suiteSummary(suite, tables) {
  const out = {};
  for (const e of suite.engines) {
    if (e.id === "ibex") continue;
    const ratios = [];
    let ti = 0, te = 0;
    for (const t of tables) {
      for (const row of t.rows) {
        const c = t.cells[row.id] || {};
        if (!suite.engines.every(x => c[x.id] != null)) continue;
        ratios.push(c.ibex / c[e.id]);
        ti += c.ibex; te += c[e.id];
      }
    }
    if (ratios.length) out[e.id] = { geo: geomean(ratios), total: ti / te, n: ratios.length };
  }
  return out;
}
function renderSuite(id, suite, state) {
  const el = document.getElementById(id + "-tbl");
  const table = suiteTable(suite, state.sel);
  const engines = suite.engines;
  let h = "<thead><tr><th class='qcol'>query</th>" + engines.map(e =>
    `<th class='${e.id === "ibex" ? "ibex" : ""}'>${e.label}</th>`).join("") + "</tr></thead><tbody>";
  if (!table) {
    el.innerHTML = h + "<tr><td class='qcol'>No results for this combination.</td></tr></tbody>";
    return;
  }
  for (const row of table.rows) {
    const cell = table.cells[row.id] || {};
    const vals = engines.map(e => cell[e.id]).filter(v => v != null);
    if (!vals.length) continue;
    const best = Math.min(...vals);
    h += `<tr><td class='qcol'>${row.label}</td>`;
    for (const e of engines) {
      const v = cell[e.id];
      const ic = e.id === "ibex" ? " ibex-col" : "";
      if (v == null) { h += `<td class='na${ic}'>–</td>`; continue; }
      const ratio = v / best, isBest = v === best;
      const txt = state.metric === "ratio" ? (isBest ? "1.0×" : ratio.toFixed(1) + "×") : fmt(v);
      h += `<td class='cell${isBest ? " best" : ""}${ic}' style='background:${heat(ratio)}' `
         + `title='${e.label}: ${v.toFixed(3)} ms (${ratio.toFixed(2)}× fastest)'>${txt}</td>`;
    }
    h += "</tr>";
  }
  const sum = suiteSummary(suite, [table]);
  for (const [key, label] of [["geo", "Ibex ÷ engine, geomean"], ["total", "Ibex ÷ engine, total"]]) {
    h += `<tr class='sum'><td class='qcol'>${label}</td>`;
    for (const e of engines) {
      const s = sum[e.id];
      h += `<td class='${e.id === "ibex" ? "ibex-col" : ""}'>`
         + (e.id === "ibex" ? "" : s ? s[key].toFixed(2) : "–") + "</td>";
    }
    h += "</tr>";
  }
  el.innerHTML = h + "</tbody>";
}
function initSuite(id, suite, metaHtml) {
  const section = document.getElementById(id);
  if (!suite) { section.remove(); return; }
  const state = { sel: {}, metric: "ms" };
  const controls = document.getElementById(id + "-controls");
  let html = "";
  for (const s of suite.selectors) {
    state.sel[s.key] = s.default;
    html += `<div class='grp'><span class='lbl'>${s.label}</span>` + s.options.map(o =>
      `<button class='opt' data-k='${s.key}' data-v='${o.value}'>${o.label}</button>`).join("")
      + "</div>";
  }
  html += "<div class='grp'><span class='lbl'>Show</span>"
    + "<button class='opt' data-m='ms'>Time (ms)</button>"
    + "<button class='opt' data-m='ratio'>× fastest</button></div>";
  controls.innerHTML = html;
  const sync = () => {
    controls.querySelectorAll("button").forEach(b => b.classList.toggle("on",
      b.dataset.m ? b.dataset.m === state.metric : state.sel[b.dataset.k] === b.dataset.v));
    renderSuite(id, suite, state);
  };
  controls.addEventListener("click", ev => {
    const b = ev.target.closest("button");
    if (!b) return;
    if (b.dataset.m) state.metric = b.dataset.m; else state.sel[b.dataset.k] = b.dataset.v;
    sync();
  });
  document.getElementById(id + "-meta").innerHTML = metaHtml;
  sync();
}
function chip(href, html) { return `<a href='${href}'>${html}</a>`; }
function fx(v) { return v.toFixed(2) + "×"; }

(function initIndependent() {
  const head = [];
  if (H2O) {
    const cores = H2O.selectors.find(s => s.key === "threads").default;
    const pick = kind => Object.entries(H2O.tables)
      .filter(([k]) => k.endsWith("|" + cores) && k.startsWith(kind)).map(([, t]) => t);
    for (const [kind, name] of [["G1", "groupby"], ["J1", "join"]]) {
      const s = suiteSummary(H2O, pick(kind));
      if (s.polars) head.push(chip("#h2o", `h2o ${name} · ${cores} cores<br>Ibex <strong>${fx(s.polars.geo)}</strong> Polars`
        + (s.duckdb ? `, <strong>${fx(s.duckdb.geo)}</strong> DuckDB` : "") + " (geomean)"));
    }
    const v = H2O.meta.versions;
    const ver = Object.entries(v).map(([e, xs]) => `${e} <code>${xs.join(", ")}</code>`).join(", ");
    initSuite("h2o", H2O, (H2O.meta.cpu ? `${H2O.meta.cpu} · ` : "")
      + `versions: ${ver} · sources: ${H2O.meta.sources.join(", ")}`);
  } else initSuite("h2o", null, "");
  if (PDSH) {
    const cores = PDSH.selectors[0].default;
    const s = suiteSummary(PDSH, [PDSH.tables[cores]]);
    if (s["pdsh-polars-stream"]) head.push(chip("#pdsh", `PDS-H SF-${PDSH.meta.scale_factor} · ${cores} cores<br>`
      + `Ibex total <strong>${fx(s["pdsh-polars-stream"].total)}</strong> Polars`
      + (s["pdsh-duckdb"] ? `, <strong>${fx(s["pdsh-duckdb"].total)}</strong> DuckDB` : "")));
    const m = PDSH.meta;
    initSuite("pdsh", PDSH, `SF-${m.scale_factor}` + (m.cpu ? ` · ${m.cpu}` : "")
      + ` · Ibex commit <code>${m.commit}</code> · Polars <code>${m.polars}</code>, DuckDB <code>${m.duckdb}</code>`
      + ` · source: ${m.source}`);
  } else initSuite("pdsh", null, "");
  head.unshift(chip("#inmemory", `In-memory suite<br><strong>${PAYLOAD.queries.length}</strong> queries, `
    + `<strong>${PAYLOAD.frameworks.length}</strong> engines`));
  document.getElementById("headline").innerHTML = head.join("");
})();

(function init() {
  const P = PAYLOAD, big = P.scales[P.scales.length - 1];
  document.getElementById("stats").innerHTML =
    `<span class='stat'><strong>${P.frameworks.length}</strong> engines</span>`
    + `<span class='stat'><strong>${P.queries.length}</strong> queries</span>`
    + `<span class='stat'>1M&ndash;<strong>${(big/1e6)|0}M</strong> rows</span>`
    + `<span class='stat'>r7i.2xlarge &middot; 8 vCPU Sapphire Rapids</span>`;
  document.getElementById("scales").innerHTML = P.scales.map(n =>
    `<button class='opt' data-s='${n}'>${(n/1e6)}M</button>`).join("");
  document.querySelectorAll("#scales button").forEach(b =>
    b.onclick = () => setScale(b.dataset.s));
  initColumnPicker();
  document.getElementById("m-ms").onclick = () => setMetric("ms");
  document.getElementById("m-ratio").onclick = () => setMetric("ratio");
  // Memory views only make sense when at least one cell carries peak RSS.
  const hasMem = Object.values(P.mem || {}).some(s => Object.keys(s).length);
  if (hasMem) {
    document.getElementById("m-mem").onclick = () => setMetric("mem");
    document.getElementById("m-memratio").onclick = () => setMetric("memratio");
  } else {
    document.getElementById("m-mem").remove();
    document.getElementById("m-memratio").remove();
  }
  const commitBit = (P.meta.commit && P.meta.commit !== "unknown")
    ? `&middot; commit <code>${P.meta.commit}</code> ` : "";
  document.getElementById("meta").innerHTML =
    `Generated ${P.meta.generated} ${commitBit}&middot; sources: ${P.meta.sources.join(", ")}`;
  setScale(String(big));
  setMetric("ms");
})();
</script>
</body>
</html>
"""


CODE_PAGE_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ibex | Benchmark methodology &amp; per-engine code</title>
  <meta name="description" content="How the Ibex benchmark works: reproduce it in one command, and read the exact code every engine runs for every query — auto-extracted from the harness source." />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=JetBrains+Mono:wght@400;600&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="./styles.css" />
  <style>
    .codewrap { display: grid; grid-template-columns: 220px minmax(0, 1fr);
      gap: 1.4rem; align-items: start; }
    .qrail { border: 1px solid var(--line); border-radius: 12px; background: var(--paper);
      max-height: 70vh; overflow-y: auto; padding: 0.5rem; position: sticky; top: 1rem; }
    .qrail .cat { color: var(--accent); font-weight: 700; font-size: 0.72rem;
      text-transform: uppercase; letter-spacing: 0.04em; margin: 0.7rem 0.4rem 0.2rem; }
    .qrail button { display: block; width: 100%; text-align: left; border: none;
      background: none; font-family: inherit; font-size: 0.86rem; color: var(--ink);
      padding: 0.32rem 0.5rem; border-radius: 7px; cursor: pointer; }
    .qrail button:hover { background: rgba(10,127,121,0.08); }
    .qrail button.on { background: var(--accent); color: #fff; font-weight: 600; }
    .codepane h3 { margin: 0 0 0.2rem; font-size: 1.25rem; }
    .codepane .qmeta { color: var(--muted); font-size: 0.85rem; margin: 0 0 1.1rem; }
    .engblock { margin: 0 0 1.1rem; }
    .engblock .ehead { display: flex; align-items: baseline; gap: 0.6rem;
      margin: 0 0 0.3rem; }
    .engblock .ename { font-weight: 700; font-size: 0.95rem; }
    .engblock.ibex .ename { color: var(--accent); }
    .engblock .esrc { font-size: 0.76rem; color: var(--muted); }
    .engblock .esrc a { color: var(--muted); }
    .engblock pre { margin: 0; padding: 0.8rem 1rem; border-radius: 10px;
      border: 1px solid var(--line); background: #0f1b1a; color: #d7e6e3;
      overflow-x: auto; font-family: "JetBrains Mono", monospace; font-size: 0.83rem;
      line-height: 1.5; white-space: pre; }
    .engblock.ibex pre { border-color: var(--accent);
      box-shadow: inset 3px 0 0 var(--accent); }
    .engblock .na { color: var(--muted); font-style: italic; font-size: 0.86rem;
      padding: 0.3rem 0; }
    .repro code, .repro pre { font-family: "JetBrains Mono", monospace; }
    .repro pre { background: #0f1b1a; color: #d7e6e3; padding: 0.9rem 1.1rem;
      border-radius: 10px; overflow-x: auto; font-size: 0.84rem; line-height: 1.6; }
    .callout { border-left: 3px solid var(--accent); background: rgba(10,127,121,0.06);
      padding: 0.8rem 1.1rem; border-radius: 0 10px 10px 0; margin: 1.2rem 0; }
    @media (max-width: 760px) { .codewrap { grid-template-columns: 1fr; }
      .qrail { position: static; max-height: none; } }
  </style>
</head>
<body>
<main class="page">

__BENCHMARK_NAV__

  <section class="hero">
    <p class="eyebrow">Methodology &amp; code</p>
    <h1>How the benchmark works</h1>
    <p class="lead">
      Everything behind the benchmark numbers is here: how to run the full suite
      yourself, and the <strong>exact code each engine runs</strong> for every
      query. The code is extracted directly from the harness source.
    </p>
  </section>

  <section class="section repro">
    <p class="section-label">Reproduce it</p>
    <h2>Run it yourself</h2>
    <p class="section-sub">
      Every engine is a stock install &mdash; <code>polars</code>,
      <code>duckdb</code>, <code>datafusion</code>, <code>chdb</code> (ClickHouse)
      and <code>pandas</code> from PyPI; <code>data.table</code> and
      <code>dplyr</code> from CRAN. The runner is one script; it generates the
      synthetic data, runs all engines and writes a single CSV.
    </p>
    <pre># clone, build Ibex in release, then run the whole suite locally:
benchmarking/run_scale_suite.sh --warmup 1 --iters 3
# -&gt; benchmarking/results/scales.csv

# render these pages from that CSV:
python3 benchmarking/gen_website.py benchmarking/results/scales.csv</pre>
    <p class="section-sub">
      The published numbers come from clean cloud boxes for isolation &mdash;
      AWS <strong>r7i.2xlarge</strong> (8 vCPU Sapphire Rapids = 4 physical
      cores, 64&nbsp;GB), <strong>one instance per engine</strong> so no engine
      competes with another for memory bandwidth or threads. One command
      end-to-end:
    </p>
    <pre>./benchmarking/aws/run-per-engine.sh --on-demand   # one box per engine, 1M&ndash;50M, uploads, shuts down</pre>
    <div class="callout">
      <strong>Know a faster way to write one of these queries?</strong> Open a PR
      against
      <a id="repo-link" href="#" target="_blank" rel="noopener">the benchmark harness</a>
      and the numbers get re-run and updated. Improvements to any engine's queries
      are welcome, the aim is an accurate comparison.
    </div>
  </section>

  <section class="section">
    <p class="section-label">Transparency</p>
    <h2>Exactly what each engine runs</h2>
    <p class="section-sub">
      Pick a query. Each engine's code is verbatim from the file linked beside it;
      rolling-window frames are shown fully resolved (e.g. the
      <code>RANGE BETWEEN INTERVAL</code> vs <code>ROWS</code> clause) so the
      time-window comparison is auditable. <code>polars-st</code> runs identical
      code to Polars with <code>POLARS_MAX_THREADS=1</code>, and
      <code>ibex-st</code> identical code to Ibex with
      <code>IBEX_CORES=1</code>; <code>ibex+parse</code>
      is the same Ibex query timed with parsing included.
    </p>
    <div class="codewrap">
      <div class="qrail" id="qrail"></div>
      <div class="codepane" id="codepane"></div>
    </div>
  </section>

</main>

<script>
const CODE = __CODE_PAYLOAD__;
let current = null;

function srcLink(eng) {
  const path = CODE.sources[eng] || "";
  return path ? `${CODE.github}/${path}` : null;
}

function selectQuery(q) {
  current = q;
  document.querySelectorAll("#qrail button").forEach(b =>
    b.classList.toggle("on", b.dataset.q === q));
  const meta = CODE.queries.find(x => x.id === q);
  const code = CODE.code[q] || {};
  const pane = document.getElementById("codepane");
  pane.innerHTML = "";
  const h3 = document.createElement("h3");
  h3.textContent = meta ? meta.label : q;
  const qm = document.createElement("p");
  qm.className = "qmeta";
  qm.textContent = `${meta ? meta.category : ""} \\u00b7 query id: ${q}`;
  pane.append(h3, qm);
  for (const eng of CODE.engines) {
    const block = document.createElement("div");
    block.className = "engblock" + (eng.key === "ibex" ? " ibex" : "");
    const head = document.createElement("div");
    head.className = "ehead";
    const name = document.createElement("span");
    name.className = "ename";
    name.textContent = eng.label;
    head.appendChild(name);
    const link = srcLink(eng.key);
    if (link) {
      const s = document.createElement("span");
      s.className = "esrc";
      s.innerHTML = `<a href="${link}" target="_blank" rel="noopener">source &#8599;</a>`;
      head.appendChild(s);
    }
    block.appendChild(head);
    if (code[eng.key] != null) {
      const pre = document.createElement("pre");
      pre.textContent = code[eng.key];
      block.appendChild(pre);
    } else {
      const na = document.createElement("div");
      na.className = "na";
      na.textContent = "not benchmarked for this engine (capability gap or excluded at scale)";
      block.appendChild(na);
    }
    pane.appendChild(block);
  }
}

(function init() {
  const repo = document.getElementById("repo-link");
  if (repo) repo.href = CODE.github + "/benchmarking";
  const rail = document.getElementById("qrail");
  let lastCat = null;
  for (const q of CODE.queries) {
    if (q.category !== lastCat) {
      const c = document.createElement("div");
      c.className = "cat";
      c.textContent = q.category;
      rail.appendChild(c);
      lastCat = q.category;
    }
    const b = document.createElement("button");
    b.dataset.q = q.id;
    b.textContent = q.label;
    b.onclick = () => selectQuery(q.id);
    rail.appendChild(b);
  }
  if (CODE.queries.length) selectQuery(CODE.queries[0].id);
})();
</script>
</body>
</html>
"""


def build_code_payload(cells):
    """{queries:[{id,label,category}], engines:[{key,label,lang}], code:{q:{eng:src}},
    sources:{eng:path}} — only queries that were actually benchmarked, in the same
    category order as the results table."""
    by_q = extract_all()
    present = {q for n in cells for q in cells[n]}
    ordered: list[dict] = []
    seen: set[str] = set()
    for cat, qs in CATEGORIES:
        for q in qs:
            if q in present and q in by_q:
                ordered.append({"id": q, "label": QUERY_LABEL.get(q, q), "category": cat})
                seen.add(q)
    for q in QUERY_ORDER:
        if q in present and q in by_q and q not in seen:
            ordered.append({"id": q, "label": QUERY_LABEL.get(q, q), "category": "Other"})
            seen.add(q)

    keys = [k for k, _, _ in CODE_ENGINES]
    code = {o["id"]: {k: by_q[o["id"]][k] for k in keys if k in by_q[o["id"]]}
            for o in ordered}
    return {
        "queries": ordered,
        "engines": [{"key": k, "label": lbl, "lang": lang} for k, lbl, lang in CODE_ENGINES],
        "code": code,
        "sources": {k: SOURCES.get(k, "") for k in keys},
        "github": GITHUB,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv", nargs="+", type=Path, help="result CSV(s), merged in order")
    ap.add_argument("-o", "--out", type=Path,
                    default=Path(__file__).resolve().parent.parent / "docs" / "benchmarks.html",
                    help="output HTML (default: docs/benchmarks.html)")
    ap.add_argument("--commit", default=None,
                    help="commit hash of the latest run (local CSV names omit it)")
    ap.add_argument("--pdsh", type=Path, default=None,
                    help="PDS-H section: a run-tpch.sh artifact (.tar.gz) or its history.tsv")
    ap.add_argument("--h2o", type=Path, nargs="+", default=None,
                    help="h2o section: run-h2o.sh artifact(s) or h2o/run.py TSVs, merged in order")
    args = ap.parse_args(argv)

    for p in args.csv + ([args.pdsh] if args.pdsh else []) + (args.h2o or []):
        if not p.exists():
            print(f"error: {p} not found", file=sys.stderr)
            return 1

    cells, mem = load(args.csv)
    if not cells:
        print("error: no data loaded", file=sys.stderr)
        return 1
    payload = build_payload(cells, mem, args.csv, commit=args.commit)

    max_rows_m = f"{max(payload['scales']) // 1_000_000}" if payload["scales"] else "16"
    pdsh = load_pdsh(args.pdsh) if args.pdsh else None
    h2o = load_h2o(args.h2o) if args.h2o else None
    suites = ["Ibex's own in-memory DataFrame suite"]
    if h2o:
        suites.append("the independent h2o db-benchmark")
    if pdsh:
        suites.append("PDS-H, Polars' TPC-H-derived benchmark")
    count = {1: "One benchmark", 2: "Two benchmarks", 3: "Three benchmarks"}[len(suites)]
    listed = suites[0] if len(suites) == 1 else ", ".join(suites[:-1]) + " and " + suites[-1]
    lead = f"{count}, with every engine pinned to the same cores: {listed}."
    out_html = (HTML_TEMPLATE
                .replace("__BENCHMARK_NAV__", BENCHMARK_NAV)
                .replace("__PAYLOAD__", json.dumps(payload, separators=(",", ":")))
                .replace("__H2O__", json.dumps(h2o, separators=(",", ":")))
                .replace("__PDSH__", json.dumps(pdsh, separators=(",", ":")))
                .replace("__GITHUB__", GITHUB)
                .replace("__LEAD_SUITES__", html.escape(lead, quote=False))
                .replace("__MAXM__", max_rows_m))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(out_html, encoding="utf-8")
    print(f"wrote {args.out}  ({len(payload['queries'])} queries, "
          f"{len(payload['frameworks'])} engines, scales {payload['scales']})"
          + (f"; PDS-H cores {sorted(pdsh['tables'], key=int)}" if pdsh else "")
          + (f"; h2o {len(h2o['tables'])} (data, cores) tables" if h2o else ""))

    # Companion methodology + per-engine code page (auto-extracted from sources).
    code_payload = build_code_payload(cells)
    code_html = (CODE_PAGE_TEMPLATE
                 .replace("__BENCHMARK_NAV__", BENCHMARK_NAV)
                 .replace("__CODE_PAYLOAD__",
                          json.dumps(code_payload, separators=(",", ":"))))
    code_out = args.out.parent / "methodology.html"
    code_out.write_text(code_html, encoding="utf-8")
    print(f"wrote {code_out}  ({len(code_payload['queries'])} queries x "
          f"{len(code_payload['engines'])} engines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
