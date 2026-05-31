#!/usr/bin/env python3
"""Generate a self-contained benchmark website from result CSV(s).

Usage:
  python3 gen_website.py results/<old-full>.csv results/<latest>.csv
  # -> writes docs/benchmarks.html (page on the Ibex docs site)

Multiple CSVs are merged in order; later files override earlier ones per
(scale, query, framework) cell. Feed an older full run first to backfill pinned
cells (sqlite, the data.table frollapply rows) the trimmed default run skips,
then the latest run to take the current ibex/competitor numbers.

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

# Curated category grouping (order matters; queries not listed fall to "Other").
CATEGORIES: list[tuple[str, list[str]]] = [
    ("Group-by / aggregation", [
        "mean_by_symbol", "ohlc_by_symbol", "count_by_symbol_day",
        "mean_by_symbol_day", "ohlc_by_symbol_day", "sum_by_user"]),
    ("Column ops / update", [
        "update_price_x2", "cumsum_price", "cumprod_price", "rand_uniform",
        "rand_normal", "rand_int", "rand_bernoulli"]),
    ("Filters", [
        "filter_simple", "filter_and", "filter_arith", "filter_or",
        "filter_events"]),
    ("Joins", [
        "null_left_join", "null_semi_join", "null_anti_join",
        "null_cross_join_small"]),
    ("Reshape", ["melt_wide_to_long", "dcast_long_to_wide"]),
    ("Fill / null propagation", ["fill_null", "fill_forward", "fill_backward"]),
    ("Time-series (TimeFrame)", [
        "tf_lag1", "tf_rolling_count_1m", "tf_rolling_sum_1m",
        "tf_rolling_mean_5m", "tf_rolling_median_1m", "tf_rolling_std_1m",
        "tf_rolling_ewma_1m", "tf_resample_1m_ohlc", "tf_asof_join",
        "tf_asof_join_by_symbol"]),
]

# Frameworks we don't surface on the page (near-duplicate of ibex).
SKIP_FRAMEWORKS = {"ibex+parse", "ibex-compiled"}


def load(paths: list[Path]):
    # cells[scale][query][framework] = avg_ms ; later paths override.
    cells: dict[int, dict[str, dict[str, float]]] = defaultdict(
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
    return cells


def commit_from_names(paths: list[Path]) -> str:
    for p in reversed(paths):
        m = re.search(r"_([0-9a-f]{8})\b", p.name)
        if m:
            return m.group(1)
    return "unknown"


def build_payload(cells, paths, commit=None):
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
    return {
        "scales": scales,
        "frameworks": frameworks,
        "queries": [{"id": q, "label": lbl, "category": cat}
                    for q, lbl, cat in ordered_queries],
        "data": data,
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
  <title>Ibex | Benchmarks vs Polars, DuckDB, ClickHouse, DataFusion, data.table, SQLite</title>
  <meta name="description" content="Ibex benchmarked against Polars, DuckDB, ClickHouse, DataFusion, data.table and SQLite across columnar DataFrame queries from 1M to 16M rows." />
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
      <strong>ClickHouse</strong>, <strong>DataFusion</strong>,
      <strong>data.table</strong> and <strong>SQLite</strong> on the same
      columnar queries, from 1M to 16M rows. Times are the mean of timed
      iterations; lower is better.
    </p>
    <div class="stats" id="stats"></div>
  </section>

  <section class="section">
    <p class="section-label">Results</p>
    <h2>Per-query timings</h2>
    <p class="section-sub">
      Pick a row count and a metric. Each row is colour-scaled green&rarr;red by
      slowdown versus the fastest engine in that row, and the fastest cell is
      outlined. Hover any cell for the exact time.
    </p>

    <div class="bench-controls">
      <div class="grp"><span class="lbl">Rows</span><span id="scales"></span></div>
      <div class="grp"><span class="lbl">Show</span>
        <button class="opt" id="m-ms">Time (ms)</button>
        <button class="opt" id="m-ratio">&times; fastest</button>
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
      <strong>Caveats.</strong> Every engine now materialises its full result.
      <code>tf rolling EWMA</code> is time-windowed in Ibex versus full-series in
      Polars (both O(n), different maths). <code>SQLite</code> and the data.table
      <code>rolling median/std</code> cells are pinned from an earlier run (they
      are stable and dominate wall-clock).
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
function heat(ratio) {
  // ratio >= 1; 1x = pale green, >=8x = pale red, log-scaled (light theme).
  const t = Math.min(1, Math.log(ratio) / Math.log(8));
  const hue = 142 - 130 * t;            // 142 green -> 12 red
  const light = 90 - 8 * t;             // stays light for dark ink text
  return `hsl(${hue.toFixed(0)}, 62%, ${light.toFixed(0)}%)`;
}

function render() {
  const P = PAYLOAD, rows = P.data[state.scale] || {}, fws = P.frameworks;
  let h = "<thead><tr><th class='qcol'>query</th>";
  for (const fw of fws)
    h += `<th class='${fw === "ibex" ? "ibex" : ""}'>${fw}</th>`;
  h += "</tr></thead><tbody>";

  let lastCat = null;
  for (const q of P.queries) {
    const cell = rows[q.id] || {};
    const vals = fws.map(fw => cell[fw]).filter(v => v != null);
    if (!vals.length) continue;
    if (q.category !== lastCat) {
      h += `<tr class='cat'><td class='qcol'>${q.category}</td>`
         + `<td colspan='${fws.length}'></td></tr>`;
      lastCat = q.category;
    }
    const best = Math.min(...vals);
    h += `<tr><td class='qcol'>${q.label}</td>`;
    for (const fw of fws) {
      const v = cell[fw];
      const ic = fw === "ibex" ? " ibex-col" : "";
      if (v == null) { h += `<td class='na${ic}'>&ndash;</td>`; continue; }
      const ratio = v / best, isBest = v === best;
      const txt = state.metric === "ratio"
        ? (isBest ? "1.0\\u00d7" : ratio.toFixed(1) + "\\u00d7") : fmt(v);
      h += `<td class='cell${isBest ? " best" : ""}${ic}' style='background:${heat(ratio)}' `
         + `title='${fw}: ${v.toFixed(3)} ms (${ratio.toFixed(2)}\\u00d7 fastest)'>${txt}</td>`;
    }
    h += "</tr>";
  }
  document.getElementById("tbl").innerHTML = h + "</tbody>";
}

function setMetric(m) {
  state.metric = m;
  document.getElementById("m-ms").classList.toggle("on", m === "ms");
  document.getElementById("m-ratio").classList.toggle("on", m === "ratio");
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

    cells = load(args.csv)
    if not cells:
        print("error: no data loaded", file=sys.stderr)
        return 1
    payload = build_payload(cells, args.csv, commit=args.commit)

    out_html = HTML_TEMPLATE.replace("__PAYLOAD__", json.dumps(payload, separators=(",", ":")))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(out_html, encoding="utf-8")
    print(f"wrote {args.out}  ({len(payload['queries'])} queries, "
          f"{len(payload['frameworks'])} engines, scales {payload['scales']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
