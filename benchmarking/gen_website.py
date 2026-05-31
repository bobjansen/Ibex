#!/usr/bin/env python3
"""Generate a self-contained benchmark website from result CSV(s).

Usage:
  python3 gen_website.py results/scales_aws_*.csv [more.csv ...] -o site/index.html

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


def build_payload(cells, paths):
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
            "commit": commit_from_names(paths),
            "sources": [p.name for p in paths],
        },
    }


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ibex Benchmarks</title>
<style>
  :root {
    --bg: #0f1419; --panel: #161b22; --line: #2a313c; --fg: #e6edf3;
    --muted: #8b949e; --ibex: #4ea1ff; --accent: #ffd166;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--fg);
    font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  .wrap { max-width: 1200px; margin: 0 auto; padding: 28px 20px 80px; }
  h1 { margin: 0 0 4px; font-size: 26px; }
  h1 .ibex { color: var(--ibex); }
  .sub { color: var(--muted); margin: 0 0 20px; }
  .controls { display: flex; flex-wrap: wrap; gap: 18px; align-items: center;
    margin: 18px 0 22px; padding: 14px 16px; background: var(--panel);
    border: 1px solid var(--line); border-radius: 10px; }
  .controls .grp { display: flex; gap: 6px; align-items: center; }
  .controls label { color: var(--muted); margin-right: 4px; font-size: 13px; }
  button.opt { background: #21262d; color: var(--fg); border: 1px solid var(--line);
    padding: 5px 11px; border-radius: 6px; cursor: pointer; font-size: 13px; }
  button.opt.on { background: var(--ibex); color: #06121f; border-color: var(--ibex);
    font-weight: 600; }
  table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
  th, td { padding: 6px 9px; text-align: right; white-space: nowrap; }
  th.q, td.q { text-align: left; position: sticky; left: 0; background: var(--bg);
    z-index: 1; }
  thead th { position: sticky; top: 0; background: var(--panel); color: var(--muted);
    border-bottom: 1px solid var(--line); font-weight: 600; z-index: 2; }
  thead th.q { z-index: 3; background: var(--panel); }
  th.ibex, td.ibex-col { box-shadow: inset 2px 0 0 var(--ibex), inset -2px 0 0 var(--ibex); }
  th.ibex { color: var(--ibex); }
  tr.cat td { background: var(--panel); color: var(--accent); font-weight: 600;
    text-align: left; letter-spacing: .02em; border-top: 1px solid var(--line); }
  td.cell { color: #06121f; border-radius: 4px; }
  td.best { font-weight: 700; outline: 1px solid rgba(255,255,255,.5); }
  td.na { color: var(--muted); }
  .tablewrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; }
  .note { color: var(--muted); font-size: 12.5px; margin-top: 22px; }
  .note b { color: var(--fg); }
  a { color: var(--ibex); }
</style>
</head>
<body>
<div class="wrap">
  <h1><span class="ibex">Ibex</span> performance benchmarks</h1>
  <p class="sub" id="sub"></p>

  <div class="controls">
    <div class="grp"><label>Rows</label><span id="scales"></span></div>
    <div class="grp"><label>Show</label>
      <button class="opt" id="m-ms">Time (ms)</button>
      <button class="opt" id="m-ratio">× fastest</button>
    </div>
  </div>

  <div class="tablewrap"><table id="tbl"></table></div>

  <p class="note">
    Lower is better; each row is colour-scaled green→red by slowdown vs the
    fastest engine in that row, and the fastest cell is outlined.
    <b>Caveats:</b> some engines evaluate lazily and a few cells aren't
    apples-to-apples — clickhouse/datafusion <code>melt</code> stream rather than
    materialise; <code>tf_rolling_ewma</code> is time-windowed in ibex vs
    full-series in polars. <code>sqlite</code> and the data.table
    <code>rolling&nbsp;median/std</code> cells are pinned from an earlier run.
  </p>
  <p class="note" id="meta"></p>
</div>

<script>
const PAYLOAD = __PAYLOAD__;

const state = { scale: null, metric: "ms" };

function fmt(ms) {
  if (ms < 1) return ms.toFixed(3);
  if (ms < 10) return ms.toFixed(2);
  if (ms < 1000) return ms.toFixed(1);
  return (ms / 1000).toFixed(2) + "s";
}
function heat(ratio) {
  // ratio >= 1; 1 = green, >=8x = red, log-scaled. Light bg, dark text.
  const t = Math.min(1, Math.log(ratio) / Math.log(8));
  const hue = 140 * (1 - t);            // 140 green -> 0 red
  const light = 82 - 12 * t;            // keep dark text readable
  return `hsl(${hue.toFixed(0)}, 72%, ${light.toFixed(0)}%)`;
}

function render() {
  const P = PAYLOAD, sc = state.scale;
  const rows = P.data[sc] || {};
  const fws = P.frameworks;
  const tbl = document.getElementById("tbl");

  let h = "<thead><tr><th class='q'>query</th>";
  for (const fw of fws) {
    const cls = fw === "ibex" ? "ibex" : "";
    h += `<th class='${cls}'>${fw}</th>`;
  }
  h += "</tr></thead><tbody>";

  let lastCat = null;
  for (const q of P.queries) {
    const cell = rows[q.id] || {};
    const vals = fws.map(fw => cell[fw]).filter(v => v != null);
    if (!vals.length) continue;          // skip rows with no data at this scale
    if (q.category !== lastCat) {
      h += `<tr class='cat'><td colspan='${fws.length + 1}'>${q.category}</td></tr>`;
      lastCat = q.category;
    }
    const best = Math.min(...vals);
    h += `<tr><td class='q'>${q.label}</td>`;
    for (const fw of fws) {
      const v = cell[fw];
      const ibexCol = fw === "ibex" ? " ibex-col" : "";
      if (v == null) { h += `<td class='na${ibexCol}'>–</td>`; continue; }
      const ratio = v / best;
      const isBest = v === best;
      const bg = heat(ratio);
      const txt = state.metric === "ratio"
        ? (isBest ? "1.0×" : ratio.toFixed(1) + "×")
        : fmt(v);
      h += `<td class='cell${isBest ? " best" : ""}${ibexCol}' `
         + `style='background:${bg}' title='${fw}: ${v.toFixed(3)} ms (${ratio.toFixed(2)}× fastest)'>`
         + `${txt}</td>`;
    }
    h += "</tr>";
  }
  h += "</tbody>";
  tbl.innerHTML = h;
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

function init() {
  const P = PAYLOAD;
  const big = P.scales[P.scales.length - 1];
  document.getElementById("sub").textContent =
    `${P.frameworks.length} engines · ${P.queries.length} queries · `
    + `1M–${(big/1e6)|0}M rows · r7i.2xlarge (8 vCPU Sapphire Rapids)`;
  const sc = document.getElementById("scales");
  sc.innerHTML = P.scales.map(n =>
    `<button class='opt' data-s='${n}'>${(n/1e6)}M</button>`).join("");
  sc.querySelectorAll("button").forEach(b =>
    b.onclick = () => setScale(b.dataset.s));
  document.getElementById("m-ms").onclick = () => setMetric("ms");
  document.getElementById("m-ratio").onclick = () => setMetric("ratio");
  document.getElementById("meta").innerHTML =
    `Generated ${P.meta.generated} · commit <code>${P.meta.commit}</code> · `
    + `sources: ${P.meta.sources.join(", ")}`;
  setScale(String(big));
  setMetric("ms");
}
init();
</script>
</body>
</html>
"""


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv", nargs="+", type=Path, help="result CSV(s), merged in order")
    ap.add_argument("-o", "--out", type=Path, default=Path("site/index.html"))
    args = ap.parse_args(argv)

    for p in args.csv:
        if not p.exists():
            print(f"error: {p} not found", file=sys.stderr)
            return 1

    cells = load(args.csv)
    if not cells:
        print("error: no data loaded", file=sys.stderr)
        return 1
    payload = build_payload(cells, args.csv)

    out_html = HTML_TEMPLATE.replace("__PAYLOAD__", json.dumps(payload, separators=(",", ":")))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(out_html, encoding="utf-8")
    print(f"wrote {args.out}  ({len(payload['queries'])} queries, "
          f"{len(payload['frameworks'])} engines, scales {payload['scales']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
