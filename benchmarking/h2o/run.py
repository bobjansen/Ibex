#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""h2o db-benchmark (groupby + join): Ibex vs Polars vs DuckDB.

The queries, data and per-engine configuration follow the maintained fork of
the h2oai db-benchmark (duckdblabs/db-benchmark @ 3b074bc319):

  * data comes from the upstream R generators, vendored unmodified in
    datagen/ (see gen_data.sh);
  * Polars uses the upstream solution's schema (Int32 ids/values, string keys
    cast to Categorical) and the streaming engine (`collect(engine="streaming")`);
  * DuckDB uses the upstream solution's schema (ENUM for id1/id2 in groupby,
    ENUM id4/id5 in join, FLOAT v3) and materialises into a TEMP table;
  * Ibex reads the same CSV with its own reader (types and dictionary encoding
    are its own choice, as everywhere else in the benchmarks).

Data is loaded once per process and not timed; each question is timed
`--warmup` + `--iters` times and reported as min and median. Every result is
checked: row count and the upstream "chk" sums must agree across engines, or
the row is flagged MISMATCH and the script exits non-zero.

Groupby q9 (r^2 of v1, v2) uses Ibex's grouped `corr(x, y)`, which, like the
other engines' corr, skips any row where either side is null.

Usage:
  uv run --project <ibex-root> benchmarking/h2o/run.py G1_1e7_1e2_0_0 J1_1e7_NA_0_0 \\
      [--threads 8] [--engines ibex polars duckdb] [--questions q1 q2]

The data name selects the task (G1_* groupby, J1_* join); generate it first
with gen_data.sh.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import math
import os
import pathlib
import re
import statistics
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = pathlib.Path(os.environ.get("IBEX_ROOT", HERE.parent.parent))
BUILD_DIR = pathlib.Path(os.environ.get("BUILD_DIR", IBEX_ROOT / "build-release"))
IBEX_BIN = BUILD_DIR / "tools" / "ibex"
DATA_DIR = pathlib.Path(os.environ.get("H2O_DATA_DIR", HERE.parent / "data" / "h2o"))
RESULTS_DIR = HERE / "results"

REL_TOL = 1e-5   # DuckDB's upstream schema stores v3 as FLOAT (32-bit)
ABS_TOL = 1e-6

# ── questions ────────────────────────────────────────────────────────────────
#
# Each question: (id, upstream description, chk columns, ibex, polars, duckdb).
# The chk columns are summed over the answer, exactly as upstream does; the
# answer's row count is always checked too. `nas` is True for data generated
# with NAs, where upstream's q8 drops null v3 explicitly.

def groupby_questions(nas: bool):
    import polars as pl
    nn = lambda *cols: ("filter " + " && ".join(f"is_not_null({c})" for c in cols) + ", ") if nas else ""
    return [
        ("q1", "sum v1 by id1", ["v1"],
         "x[select { v1 = sum(v1) }, by id1]",
         lambda x: x.group_by("id1").agg(pl.sum("v1")),
         "SELECT id1, sum(v1) AS v1 FROM x GROUP BY id1"),
        ("q2", "sum v1 by id1:id2", ["v1"],
         "x[select { v1 = sum(v1) }, by { id1, id2 }]",
         lambda x: x.group_by(["id1", "id2"]).agg(pl.sum("v1")),
         "SELECT id1, id2, sum(v1) AS v1 FROM x GROUP BY id1, id2"),
        ("q3", "sum v1 mean v3 by id3", ["v1", "v3"],
         "x[select { v1 = sum(v1), v3 = mean(v3) }, by id3]",
         lambda x: x.group_by("id3").agg(pl.sum("v1"), pl.mean("v3")),
         "SELECT id3, sum(v1) AS v1, avg(v3) AS v3 FROM x GROUP BY id3"),
        ("q4", "mean v1:v3 by id4", ["v1", "v2", "v3"],
         "x[select { v1 = mean(v1), v2 = mean(v2), v3 = mean(v3) }, by id4]",
         lambda x: x.group_by("id4").agg(pl.mean("v1"), pl.mean("v2"), pl.mean("v3")),
         "SELECT id4, avg(v1) AS v1, avg(v2) AS v2, avg(v3) AS v3 FROM x GROUP BY id4"),
        ("q5", "sum v1:v3 by id6", ["v1", "v2", "v3"],
         "x[select { v1 = sum(v1), v2 = sum(v2), v3 = sum(v3) }, by id6]",
         lambda x: x.group_by("id6").agg(pl.sum("v1"), pl.sum("v2"), pl.sum("v3")),
         "SELECT id6, sum(v1) AS v1, sum(v2) AS v2, sum(v3) AS v3 FROM x GROUP BY id6"),
        ("q6", "median v3 sd v3 by id4 id5", ["median_v3", "sd_v3"],
         "x[select { median_v3 = median(v3), sd_v3 = std(v3) }, by { id4, id5 }]",
         lambda x: x.group_by(["id4", "id5"]).agg(pl.median("v3").alias("median_v3"),
                                                  pl.std("v3").alias("sd_v3")),
         "SELECT id4, id5, quantile_cont(v3, 0.5) AS median_v3, stddev(v3) AS sd_v3 "
         "FROM x GROUP BY id4, id5"),
        ("q7", "max v1 - min v2 by id3", ["range_v1_v2"],
         "x[select { range_v1_v2 = max(v1) - min(v2) }, by id3]",
         lambda x: x.group_by("id3").agg((pl.max("v1") - pl.min("v2")).alias("range_v1_v2")),
         "SELECT id3, max(v1) - min(v2) AS range_v1_v2 FROM x GROUP BY id3"),
        ("q8", "largest two v3 by id6", ["largest2_v3"],
         f"x[{nn('v3')}order {{ v3 desc }}, head 2, by id6][select {{ id6, largest2_v3 = v3 }}]",
         lambda x: (x.drop_nulls("v3").group_by("id6")
                     .agg(pl.col("v3").top_k(2).alias("largest2_v3")).explode("largest2_v3")),
         "SELECT id6, unnest(max(v3, 2)) AS largest2_v3 FROM x WHERE v3 IS NOT NULL GROUP BY id6"),
        ("q9", "regression v1 v2 by id2 id4", ["r2"],
         "x[select { r2 = corr(v1, v2) }, by { id2, id4 }][update { r2 = r2 * r2 }]",
         lambda x: x.group_by(["id2", "id4"]).agg((pl.corr("v1", "v2") ** 2).alias("r2")),
         "SELECT id2, id4, pow(corr(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4"),
        ("q10", "sum v3 count by id1:id6", ["v3", "count"],
         "x[select { v3 = sum(v3), count = count() }, by { id1, id2, id3, id4, id5, id6 }]",
         lambda x: x.group_by(["id1", "id2", "id3", "id4", "id5", "id6"])
                    .agg(pl.sum("v3"), pl.len().alias("count")),
         "SELECT id1, id2, id3, id4, id5, id6, sum(v3) AS v3, count(*) AS count "
         "FROM x GROUP BY id1, id2, id3, id4, id5, id6"),
    ]


def join_questions(nas: bool):
    # Upstream DuckDB keeps every left column and renames the colliding right
    # ones; Polars suffixes them "_right". Ibex has to name a suffix for any
    # collision (SPEC.md §5.6), which yields the same column set.
    chk = ["v1", "v2"]
    return [
        ("q1", "small inner on int", chk,
         'x join small on id1 suffix { "", "_small" }',
         lambda x, s, m, b: x.join(s, on="id1"),
         "SELECT x.*, small.id4 AS small_id4, v2 FROM x JOIN small USING (id1)"),
        ("q2", "medium inner on int", chk,
         'x join medium on id2 suffix { "", "_medium" }',
         lambda x, s, m, b: x.join(m, on="id2"),
         "SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 "
         "FROM x JOIN medium USING (id2)"),
        ("q3", "medium outer on int", chk,
         'x left join medium on id2 suffix { "", "_medium" }',
         lambda x, s, m, b: x.join(m, how="left", on="id2"),
         "SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 AS medium_id5, v2 "
         "FROM x LEFT JOIN medium USING (id2)"),
        ("q4", "medium inner on factor", chk,
         'x join medium on id5 suffix { "", "_medium" }',
         lambda x, s, m, b: x.join(m, on="id5"),
         "SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 AS medium_id4, v2 "
         "FROM x JOIN medium USING (id5)"),
        ("q5", "big inner on int", chk,
         'x join big on id3 suffix { "", "_big" }',
         lambda x, s, m, b: x.join(b, on="id3"),
         "SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 AS big_id4, big.id5 AS big_id5, "
         "big.id6 AS big_id6, v2 FROM x JOIN big USING (id3)"),
    ]


def parse_data_name(name: str) -> tuple[str, bool]:
    m = re.fullmatch(r"(G1|J1)_(\w+?)_(\w+?)_(\d+)_([01])", name)
    if not m:
        raise SystemExit(f"error: {name!r} is not an h2o data name (G1_1e7_1e2_0_0, J1_1e7_NA_0_0)")
    return ("groupby" if m.group(1) == "G1" else "join"), int(m.group(4)) > 0


def join_tables(name: str) -> dict[str, pathlib.Path]:
    """J1_1e7_NA_0_0 -> x + the small/medium/big right-hand tables (upstream naming)."""
    parts = name.split("_")
    n = float(parts[1])
    def sci(v: float) -> str:
        mant, exp = f"{v:.0e}".split("e")
        return f"{mant}e{int(exp)}"
    out = {"x": DATA_DIR / f"{name}.csv"}
    for label, div in (("small", 1e6), ("medium", 1e3), ("big", 1e0)):
        out[label] = DATA_DIR / (name.replace("NA", sci(n / div), 1) + ".csv")
    return out


def questions_for(task: str, nas: bool):
    return groupby_questions(nas) if task == "groupby" else join_questions(nas)

# ── Ibex ─────────────────────────────────────────────────────────────────────

TIME_RE = re.compile(r"^time:\s*([\d.]+)\s*(us|ms|s)\s*$")


def ibex_run(data: str, task: str, nas: bool, wanted: list[str], warmup: int, iters: int,
             threads: int, timeout_s: float) -> list[dict]:
    """One REPL process per question: data is read once (untimed), the query is
    re-run warmup+iters times under `:timing on`, then the chk sums are written
    to CSV with timing off. A process per question keeps a failing or
    runaway question from taking the rest down with it."""
    tables = {"x": DATA_DIR / f"{data}.csv"} if task == "groupby" else join_tables(data)
    loads = ['import "csv";'] + [f'let {k} = csv::read("{p}");' for k, p in tables.items()]
    results = []
    with tempfile.TemporaryDirectory(prefix="ibex_h2o_") as tmp:
        for qid, desc, chk_cols, ibex_q, _, _ in questions_for(task, nas):
            if wanted and qid not in wanted:
                continue
            chk_path = pathlib.Path(tmp) / f"{qid}.csv"
            sums = ", ".join(f"c{i} = Float64(sum({c}))" for i, c in enumerate(chk_cols))
            script = loads + [":timing on"] + [f"let ans = {ibex_q};"] * (warmup + iters) + [
                ":timing off",
                f'csv::write(ans[select {{ {sums}, n = count() }}], "{chk_path}");',
                ":quit",
            ]
            print(f"  ibex {qid} ({desc})", file=sys.stderr, flush=True)
            env = dict(os.environ, IBEX_CORES=str(threads))
            try:
                proc = subprocess.run(
                    taskset(threads) + [str(IBEX_BIN), "--plugin-path", str(IBEX_BIN.parent),
                                        "--no-history"],
                    input="\n".join(script) + "\n", capture_output=True, text=True,
                    cwd=IBEX_ROOT, env=env, timeout=timeout_s)
                out, err = proc.stdout, proc.stderr
            except subprocess.TimeoutExpired:
                results.append(failed("ibex", qid, desc, f"timeout after {timeout_s:.0f}s"))
                continue
            times = [to_ms(float(m.group(1)), m.group(2))
                     for m in map(TIME_RE.match, out.splitlines()) if m]
            if len(times) != warmup + iters or not chk_path.exists():
                tail = (err.strip() or out.strip()).splitlines()[-5:]
                results.append(failed("ibex", qid, desc,
                                      f"{len(times)} time lines, expected {warmup + iters}: "
                                      + " | ".join(tail)))
                continue
            with open(chk_path) as f:
                row = next(csv.DictReader(f))
            chk = [float(row[f"c{i}"]) if row[f"c{i}"] not in ("", "null") else math.nan
                   for i in range(len(chk_cols))]
            results.append(ok("ibex", qid, desc, times[warmup:], int(row["n"]), chk))
    return results


def to_ms(v: float, unit: str) -> float:
    return {"us": v / 1000.0, "ms": v, "s": v * 1000.0}[unit]

# ── Polars / DuckDB (run in a child process: Polars fixes its pool at import) ─

def polars_worker(data: str, task: str, nas: bool, wanted, warmup, iters) -> list[dict]:
    import polars as pl
    if task == "groupby":
        df = (pl.read_csv(DATA_DIR / f"{data}.csv",
                          schema_overrides={"id4": pl.Int32, "id5": pl.Int32, "id6": pl.Int32,
                                            "v1": pl.Int32, "v2": pl.Int32, "v3": pl.Float64})
                .with_columns(pl.col(["id1", "id2", "id3"]).cast(pl.Categorical)))
        frames = (df.lazy(),)
    else:
        t = join_tables(data)
        ints = lambda *c: {k: pl.Int32 for k in c}
        cat = lambda *c: pl.col(list(c)).cast(pl.Categorical)
        x = pl.read_csv(t["x"], schema_overrides={**ints("id1", "id2", "id3"), "v1": pl.Float32}
                        ).with_columns(cat("id4", "id5", "id6"))
        s = pl.read_csv(t["small"], schema_overrides={**ints("id1"), "v2": pl.Float32}
                        ).with_columns(cat("id4"))
        m = pl.read_csv(t["medium"], schema_overrides={**ints("id1", "id2"), "v2": pl.Float32}
                        ).with_columns(cat("id4", "id5"))
        b = pl.read_csv(t["big"], schema_overrides={**ints("id1", "id2", "id3"), "v2": pl.Float32}
                        ).with_columns(cat("id4", "id5", "id6"))
        frames = (x.lazy(), s.lazy(), m.lazy(), b.lazy())
    results = []
    for qid, desc, chk_cols, _, pq, _ in questions_for(task, nas):
        if wanted and qid not in wanted:
            continue
        times, ans = [], None
        for _ in range(warmup + iters):
            ans = None
            t0 = time.perf_counter()
            ans = pq(*frames).collect(engine="streaming")
            times.append((time.perf_counter() - t0) * 1000.0)
        chk = [float(ans[c].cast(pl.Float64).sum()) for c in chk_cols]
        results.append(ok("polars", qid, desc, times[warmup:], ans.height, chk))
    return results


def duckdb_worker(data: str, task: str, nas: bool, wanted, warmup, iters, threads) -> list[dict]:
    import duckdb
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute("SET enable_progress_bar = false")
    if task == "groupby":
        src = DATA_DIR / f"{data}.csv"
        con.execute("CREATE TABLE y(id1 VARCHAR, id2 VARCHAR, id3 VARCHAR, id4 INT, id5 INT, id6 INT, "
                    "v1 INT, v2 INT, v3 FLOAT)")
        con.execute(f"COPY y FROM '{src}' (AUTO_DETECT TRUE)")
        con.execute("CREATE TYPE id1ENUM AS ENUM (SELECT id1 FROM y WHERE id1 IS NOT NULL)")
        con.execute("CREATE TYPE id2ENUM AS ENUM (SELECT id2 FROM y WHERE id2 IS NOT NULL)")
        con.execute("CREATE TABLE x(id1 id1ENUM, id2 id2ENUM, id3 VARCHAR, id4 INT, id5 INT, id6 INT, "
                    "v1 INT, v2 INT, v3 FLOAT)")
        con.execute("INSERT INTO x (SELECT * FROM y)")
        con.execute("DROP TABLE y")
    else:
        t = join_tables(data)
        for k in ("x", "small", "medium", "big"):
            con.execute(f"CREATE TABLE {k}_csv AS SELECT * FROM read_csv_auto('{t[k]}')")
        con.execute("CREATE TYPE id4ENUM AS ENUM (SELECT id4 FROM (SELECT id4 FROM x_csv UNION ALL "
                    "SELECT id4 FROM small_csv UNION ALL SELECT id4 FROM medium_csv UNION ALL "
                    "SELECT id4 FROM big_csv) WHERE id4 IS NOT NULL)")
        con.execute("CREATE TYPE id5ENUM AS ENUM (SELECT id5 FROM (SELECT id5 FROM x_csv UNION ALL "
                    "SELECT id5 FROM medium_csv UNION ALL SELECT id5 FROM big_csv) WHERE id5 IS NOT NULL)")
        con.execute("CREATE TABLE small(id1 INT64, id4 id4ENUM, v2 DOUBLE)")
        con.execute("CREATE TABLE medium(id1 INT64, id2 INT64, id4 id4ENUM, id5 id5ENUM, v2 DOUBLE)")
        con.execute("CREATE TABLE big(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, "
                    "id6 VARCHAR, v2 DOUBLE)")
        con.execute("CREATE TABLE x(id1 INT64, id2 INT64, id3 INT64, id4 id4ENUM, id5 id5ENUM, "
                    "id6 VARCHAR, v1 DOUBLE)")
        for k in ("x", "small", "medium", "big"):
            con.execute(f"INSERT INTO {k} (SELECT * FROM {k}_csv)")
            con.execute(f"DROP TABLE {k}_csv")
    results = []
    for qid, desc, chk_cols, _, _, sql in questions_for(task, nas):
        if wanted and qid not in wanted:
            continue
        times = []
        for _ in range(warmup + iters):
            con.execute("DROP TABLE IF EXISTS ans")
            t0 = time.perf_counter()
            con.execute(f"CREATE TEMP TABLE ans AS {sql}")
            times.append((time.perf_counter() - t0) * 1000.0)
        sums = ", ".join(f"CAST(sum({c}) AS DOUBLE)" for c in chk_cols)
        row = con.execute(f"SELECT {sums}, count(*) FROM ans").fetchone()
        chk = [math.nan if v is None else float(v) for v in row[:-1]]
        results.append(ok("duckdb", qid, desc, times[warmup:], int(row[-1]), chk))
    return results


def run_worker(engine: str, args) -> list[dict]:
    env = dict(os.environ, POLARS_MAX_THREADS=str(args.threads))
    cmd = taskset(args.threads) + [sys.executable, str(pathlib.Path(__file__).resolve()),
                                   "--worker", engine, args.data_one, "--threads", str(args.threads),
                                   "--warmup", str(args.warmup), "--iters", str(args.iters)]
    if args.questions:
        cmd += ["--questions", *args.questions]
    print(f"  {engine} (all questions, one process)", file=sys.stderr, flush=True)
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=args.timeout)
    except subprocess.TimeoutExpired:
        return [failed(engine, "*", "", f"timeout after {args.timeout:.0f}s")]
    if proc.returncode != 0:
        return [failed(engine, "*", "", proc.stderr.strip().splitlines()[-1] if proc.stderr.strip()
                       else f"exit {proc.returncode}")]
    return json.loads(proc.stdout)

# ── result records, verification, report ─────────────────────────────────────

def ok(engine, qid, desc, times, rows, chk) -> dict:
    return {"engine": engine, "question": qid, "desc": desc, "times": times,
            "min_ms": min(times), "median_ms": statistics.median(times),
            "out_rows": rows, "chk": chk, "error": ""}


def failed(engine, qid, desc, why) -> dict:
    return {"engine": engine, "question": qid, "desc": desc, "times": [], "min_ms": math.nan,
            "median_ms": math.nan, "out_rows": -1, "chk": [], "error": why}


def close(a: float, b: float) -> bool:
    if math.isnan(a) or math.isnan(b):
        return math.isnan(a) and math.isnan(b)
    return abs(a - b) <= max(ABS_TOL, REL_TOL * max(abs(a), abs(b)))


def verify(rows: list[dict]) -> str:
    good = [r for r in rows if not r["error"]]
    if len(good) < 2:
        return "unchecked"
    ref = good[0]
    for r in good[1:]:
        if r["out_rows"] != ref["out_rows"] or len(r["chk"]) != len(ref["chk"]) \
                or not all(close(a, b) for a, b in zip(r["chk"], ref["chk"])):
            return (f"MISMATCH {ref['engine']}={ref['out_rows']}/{ref['chk']} "
                    f"{r['engine']}={r['out_rows']}/{r['chk']}")
    return "ok"


def taskset(threads: int) -> list[str]:
    return ["taskset", "-c", f"0-{threads - 1}"]


def tool_versions() -> dict[str, str]:
    import duckdb
    import polars
    sha = subprocess.run(["git", "-C", str(IBEX_ROOT), "rev-parse", "--short", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(IBEX_ROOT), "status", "--porcelain", "--untracked-files=no"],
                           capture_output=True, text=True).stdout.strip()
    return {"ibex": sha + ("-dirty" if dirty else ""), "polars": polars.__version__,
            "duckdb": duckdb.__version__}


def report(data: str, by_q: dict[str, list[dict]], engines: list[str]) -> bool:
    fmt = lambda v: "—" if math.isnan(v) else (f"{v:.0f}" if v >= 100 else f"{v:.1f}")
    ratio = lambda a, b: "—" if math.isnan(a) or math.isnan(b) else f"{a / b:.2f}"
    others = [e for e in engines if e != "ibex"]
    head = ["q", "question"] + [f"{e} ms" for e in engines] + \
           ([f"ibex/{e}" for e in others] if "ibex" in engines else []) + ["check"]
    lines = [head]
    all_ok = True
    for qid, rows in by_q.items():
        per = {r["engine"]: r for r in rows}
        mins = {e: per[e]["min_ms"] if e in per else math.nan for e in engines}
        status = verify(rows)
        errs = [f"{r['engine']}: {r['error']}" for r in rows if r["error"]]
        if errs:
            status = "FAILED " + "; ".join(errs)
        all_ok &= status == "ok"
        desc = next((r["desc"] for r in rows if r["desc"]), "")
        line = [qid, desc] + [fmt(mins[e]) for e in engines]
        if "ibex" in engines:
            line += [ratio(mins["ibex"], mins[e]) for e in others]
        lines.append(line + [status])
    print(f"\n{data}  (min ms; ratio < 1 means Ibex is faster)")
    widths = [max(len(str(l[i])) for l in lines) for i in range(len(head) - 1)]
    for l in lines:
        print("  ".join(str(c).ljust(w) for c, w in zip(l, widths)) + "  " + l[-1])
    if "ibex" in engines:
        for e in others:
            pairs = [(per["ibex"]["min_ms"], per[e]["min_ms"]) for per in
                     ({r["engine"]: r for r in rows} for rows in by_q.values())
                     if "ibex" in per and e in per and not per["ibex"]["error"] and not per[e]["error"]]
            if pairs:
                gm = math.exp(statistics.mean(math.log(a / b) for a, b in pairs))
                tot = sum(a for a, _ in pairs) / sum(b for _, b in pairs)
                print(f"ibex/{e}: geomean {gm:.2f}, total {tot:.2f} over {len(pairs)} questions")
    return all_ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data", nargs="+", help="h2o data names, e.g. G1_1e7_1e2_0_0 J1_1e7_NA_0_0")
    ap.add_argument("--engines", nargs="+", default=["ibex", "polars", "duckdb"])
    ap.add_argument("--questions", nargs="+", default=[])
    ap.add_argument("--threads", type=int, default=8,
                    help="cores per engine (taskset + IBEX_CORES/POLARS_MAX_THREADS/duckdb threads)")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--iters", type=int, default=5)
    ap.add_argument("--timeout", type=float, default=1800.0, help="per engine process, seconds")
    ap.add_argument("--out", default=None, help="TSV path (default results/h2o_<stamp>_<T>t.tsv)")
    ap.add_argument("--worker", choices=["polars", "duckdb"], help=argparse.SUPPRESS)
    args = ap.parse_args()

    if args.worker:
        task, nas = parse_data_name(args.data[0])
        if args.worker == "polars":
            res = polars_worker(args.data[0], task, nas, args.questions, args.warmup, args.iters)
        else:
            res = duckdb_worker(args.data[0], task, nas, args.questions, args.warmup, args.iters,
                                args.threads)
        json.dump(res, sys.stdout)
        return 0

    if "ibex" in args.engines and not IBEX_BIN.exists():
        print(f"error: {IBEX_BIN} not found — build build-release first", file=sys.stderr)
        return 1
    versions = tool_versions()
    stamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
    out = pathlib.Path(args.out) if args.out else RESULTS_DIR / f"h2o_{stamp}_{args.threads}t.tsv"
    out.parent.mkdir(parents=True, exist_ok=True)
    all_ok = True
    records = []
    for data in args.data:
        task, nas = parse_data_name(data)
        missing = [p for p in ([DATA_DIR / f"{data}.csv"] if task == "groupby"
                               else join_tables(data).values()) if not p.exists()]
        if missing:
            print(f"error: missing {', '.join(map(str, missing))} — run gen_data.sh", file=sys.stderr)
            return 1
        print(f"=== {data} ({task}, {args.threads} threads) ===", file=sys.stderr)
        args.data_one = data
        rows: list[dict] = []
        for engine in args.engines:
            if engine == "ibex":
                rows += ibex_run(data, task, nas, args.questions, args.warmup, args.iters,
                                 args.threads, args.timeout)
            else:
                rows += run_worker(engine, args)
        order = [q[0] for q in questions_for(task, nas) if not args.questions or q[0] in args.questions]
        by_q = {q: [r for r in rows if r["question"] in (q, "*")] for q in order}
        all_ok &= report(data, by_q, args.engines)
        for q, rs in by_q.items():
            status = verify(rs)
            for r in rs:
                records.append([data, task, q, r["desc"], r["engine"], versions.get(r["engine"], ""),
                                args.threads, args.warmup, len(r["times"]), f"{r['min_ms']:.3f}",
                                f"{r['median_ms']:.3f}", r["out_rows"],
                                ",".join(f"{c:.10g}" for c in r["chk"]),
                                r["error"] or status, ",".join(f"{t:.3f}" for t in r["times"])])
    with open(out, "w") as f:
        f.write("data\ttask\tquestion\tdesc\tengine\tversion\tthreads\twarmup\titers\tmin_ms\tmedian_ms\t"
                "out_rows\tchk\tstatus\tsamples_ms\n")
        for rec in records:
            f.write("\t".join(map(str, rec)) + "\n")
    print(f"\nresults written to {out}", file=sys.stderr)
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
