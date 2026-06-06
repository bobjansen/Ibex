#!/usr/bin/env python3
"""Generate a self-contained benchmark website from result CSV(s).

Usage:
  python3 gen_website.py results/<latest>.csv
  # -> writes docs/benchmarks.html (page on the Ibex docs site)

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
from collections import defaultdict
from pathlib import Path

from print_table import FRAMEWORK_ORDER, QUERY_LABEL, QUERY_ORDER
from extract_bench_code import extract_all, SOURCES

GITHUB = "https://github.com/bobjansen/Ibex/blob/main"

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
        "mean_by_symbol", "ohlc_by_symbol", "count_by_symbol_day",
        "mean_by_symbol_day", "ohlc_by_symbol_day", "sum_by_user",
        "distinct_symbol"]),
    ("Column ops / update", [
        "update_price_x2", "cumsum_price", "cumprod_price", "rand_uniform",
        "rand_normal", "rand_int", "rand_bernoulli"]),
    ("Filters", [
        "filter_simple", "filter_and", "filter_arith", "filter_or",
        "filter_events"]),
    ("Sort / top-k", [
        "order_head_topk", "order_head_topk_by_symbol",
        "order_tail_topk", "order_tail_topk_by_symbol"]),
    ("Joins", [
        "null_left_join", "null_semi_join", "null_anti_join",
        "null_cross_join_small"]),
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


HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ibex | Benchmarks vs Polars, DuckDB, ClickHouse, DataFusion, data.table</title>
  <meta name="description" content="Ibex benchmarked against Polars, DuckDB, ClickHouse, DataFusion and data.table across columnar DataFrame queries from 1M to 16M rows." />
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
    .tablewrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 12px;
      background: var(--paper); }
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
  </style>
</head>
<body>
<main class="page">

  <nav class="nav">
    <span class="nav-logo">Ibex</span>
    <div class="nav-links">
      <a class="nav-link" href="./index.html">Docs</a>
      <a class="nav-link" href="./io.html">I/O</a>
      <a class="nav-link" href="./cheatsheet.html">Cheat sheet</a>
      <a class="nav-link" href="./functions.html">Function reference</a>
      <a class="nav-link" href="./comparison.html">Comparison</a>
      <a class="nav-link active" href="./benchmarks.html">Benchmarks</a>
      <a class="nav-link" href="https://github.com/bobjansen/Ibex" target="_blank" rel="noopener">GitHub &#8599;</a>
    </div>
  </nav>

  <section class="hero">
    <img class="hero-logo" src="./Ibex.png" alt="Ibex logo" />
    <p class="eyebrow">Benchmarks</p>
    <h1>How fast is Ibex?</h1>
    <p class="lead">
      Ibex against <strong>Polars</strong>, <strong>DuckDB</strong>,
      <strong>ClickHouse</strong>, <strong>DataFusion</strong> and
      <strong>data.table</strong> on the same columnar queries, from 1M to 16M
      rows. Times are the mean of timed iterations; lower is better.
    </p>
    <div class="stats" id="stats"></div>
  </section>

  <section class="section">
    <p class="section-label">Results</p>
    <h2>Per-query timings</h2>
    <p class="section-sub">
      Pick a row count and a metric. Each row is colour-scaled green&rarr;red
      against the best engine in that row &mdash; fastest for time, lightest for
      memory &mdash; and the best cell is outlined. Hover any cell for the exact
      time and peak memory.
    </p>

    <div class="bench-controls">
      <div class="grp"><span class="lbl">Rows</span><span id="scales"></span></div>
      <div class="grp"><span class="lbl">Show</span>
        <button class="opt" id="m-ms">Time (ms)</button>
        <button class="opt" id="m-ratio">&times; fastest</button>
        <button class="opt" id="m-mem">Memory (MB)</button>
        <button class="opt" id="m-memratio">&times; least mem</button>
      </div>
    </div>

    <div class="tablewrap"><table class="bench" id="tbl"></table></div>

    <p class="bench-note" style="margin-top:1rem">
      <strong>Threads.</strong> Ibex runs on a single core. Polars, DuckDB,
      DataFusion and ClickHouse use all 8 — so they win the parallelisable cells
      on core count, not per-core work. For a same-core comparison read Ibex
      against <code>polars-st</code> (Polars pinned to one thread); Ibex is faster
      on the large majority of queries there.
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
      <code>tf rolling EWMA</code> is time-windowed in Ibex versus full-series in
      Polars (both O(n), different maths). Each page is generated from a single
      run; <code>SQLite</code> and the data.table <code>rolling median/std</code>
      cells are omitted (they dominate wall-clock and add no competitive signal).
    </p>
    <p class="bench-note">
      <strong>The code behind these numbers.</strong> Every query's exact code, in
      every engine, is on the <a href="./methodology.html">Methodology &amp;
      code</a> page. Directly extracted from the harness source.
    </p>
    <p class="bench-note" id="meta"></p>
  </section>

</main>

<script>
const PAYLOAD = __PAYLOAD__;
const state = { scale: null, metric: "ms" };

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
  const fws = P.frameworks;
  let h = "<thead><tr><th class='qcol'>query</th>";
  for (const fw of fws)
    h += `<th class='${fw === "ibex" ? "ibex" : ""}'>${fw}</th>`;
  h += "</tr></thead><tbody>";

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

  <nav class="nav">
    <span class="nav-logo">Ibex</span>
    <div class="nav-links">
      <a class="nav-link" href="./index.html">Docs</a>
      <a class="nav-link" href="./io.html">I/O</a>
      <a class="nav-link" href="./cheatsheet.html">Cheat sheet</a>
      <a class="nav-link" href="./functions.html">Function reference</a>
      <a class="nav-link" href="./comparison.html">Comparison</a>
      <a class="nav-link active" href="./benchmarks.html">Benchmarks</a>
      <a class="nav-link" href="https://github.com/bobjansen/Ibex" target="_blank" rel="noopener">GitHub &#8599;</a>
    </div>
  </nav>

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
      The published numbers come from a clean cloud box for isolation &mdash; an
      AWS <strong>r7i.2xlarge</strong> (8 vCPU Sapphire Rapids, 64&nbsp;GB),
      one command end-to-end:
    </p>
    <pre>./benchmarking/aws/run.sh --on-demand   # provisions, runs 1M&ndash;50M, uploads, shuts down</pre>
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
      code to Polars with <code>POLARS_MAX_THREADS=1</code>; <code>ibex+parse</code>
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
    args = ap.parse_args(argv)

    for p in args.csv:
        if not p.exists():
            print(f"error: {p} not found", file=sys.stderr)
            return 1

    cells, mem = load(args.csv)
    if not cells:
        print("error: no data loaded", file=sys.stderr)
        return 1
    payload = build_payload(cells, mem, args.csv, commit=args.commit)

    out_html = HTML_TEMPLATE.replace("__PAYLOAD__", json.dumps(payload, separators=(",", ":")))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(out_html, encoding="utf-8")
    print(f"wrote {args.out}  ({len(payload['queries'])} queries, "
          f"{len(payload['frameworks'])} engines, scales {payload['scales']})")

    # Companion methodology + per-engine code page (auto-extracted from sources).
    code_payload = build_code_payload(cells)
    code_html = CODE_PAGE_TEMPLATE.replace(
        "__CODE_PAYLOAD__", json.dumps(code_payload, separators=(",", ":")))
    code_out = args.out.parent / "methodology.html"
    code_out.write_text(code_html, encoding="utf-8")
    print(f"wrote {code_out}  ({len(code_payload['queries'])} queries x "
          f"{len(code_payload['engines'])} engines)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
