#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Diff every PDS-H answer against upstream Polars at the benchmarked scale.

`check_answers.py` compares against the official TPC-H answers, which exist
only at SF-1. The timing harnesses (`bench_ibex.py`, `bench_pdsh.py`) throw
both engines' output away. So nothing compared Ibex against Polars at the scale
actually being timed, and q11 hardcoded FRACTION = 0.0001 instead of
0.0001 / SF for two months: right at SF-1, 10 rows instead of 1,850 at SF-2,
and none instead of 7,508 at SF-8, while its timings were published next to
Polars computing the real answer. Checks that compare Ibex with Ibex
(`ab_queries.py`, the parity tests) cannot see that class of bug, because both
sides run the same query text.

This script runs each Ibex query once through `ibex_eval` (which writes
`benchmarking/data/tpch/out/qNN.csv`) and collects the matching upstream
Polars query from a polars-benchmark checkout over the same Parquet. Then it
compares them row by row, in order. `run_bench.sh` calls it before timing
anything, because a timing comparison between two different answers means
nothing.

Rows are compared in order, except among TIES. Each query's ORDER BY keys and
LIMIT are listed below (`ORDERING`, as the upstream Polars queries state them).
Consecutive rows whose keys are equal within the numeric tolerance below are a
tie group, and the order inside a tie group is unspecified: q11 sorts only by
`value desc`, and at SF-10 two parts share a value, which the two engines emit
in different orders. Within a group the rows must match as a set. A group cut
by the LIMIT may legitimately hold different rows (which of the tied rows make
the cut is unspecified too), so there only the keys must match.

Columns are aligned by name when both sides have the same set in a different
order (q10). Otherwise they are compared by position, since the two
implementations name a few columns differently (q18). Either case is reported
as a note and does not fail the check. Numbers match within max(0.01, 1e-9 * |value|),
because upstream Polars rounds some money columns to 2 decimals and float
summation order differs between engines. Dates compare as days since the epoch,
which is what Ibex's `write_csv` emits.

Usage (from the repo root, inside the project's uv environment):
  uv run --project . benchmarking/tpch/check_against_polars.py --sf 8 [--pdsh-root DIR] [q01 q11 ...]

The `benchmarking/data/tpch/parquet` symlink must already point at
`<pdsh-root>/data/tables/scale-<sf>` (run_bench.sh sets it), the tables
polars-benchmark generated itself. The script refuses to run otherwise, rather
than silently comparing two scales or two datasets.
"""

import argparse
import csv
import datetime
import importlib
import math
import os
import pathlib
import subprocess
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = IBEX_ROOT / "benchmarking/data/tpch"
PARQUET_LINK = DATA_ROOT / "parquet"
OUT_DIR = DATA_ROOT / "out"
QUERIES_DIR = SCRIPT_DIR / "queries"
IBEX_EVAL = IBEX_ROOT / "build-release/tools/ibex_eval"
PLUGIN_DIR = IBEX_ROOT / "build-release/tools"

sys.path.insert(0, str(SCRIPT_DIR.parent))
import bench_env  # noqa: E402

EPOCH = datetime.date(1970, 1, 1)
ABS_TOL = 0.01
REL_TOL = 1e-9
MAX_REPORTED = 5

# ORDER BY keys and LIMIT of each upstream Polars query, by query number. The
# names are Polars' (q18 sorts by its `o_orderdat`). Queries not listed return
# one row, or none are sorted (q06, q14, q17, q19).
ORDERING: dict[int, tuple[list[str], int | None]] = {
    1: (["l_returnflag", "l_linestatus"], None),
    2: (["s_acctbal", "n_name", "s_name", "p_partkey"], 100),
    3: (["revenue", "o_orderdate"], 10),
    4: (["o_orderpriority"], None),
    5: (["revenue"], None),
    7: (["supp_nation", "cust_nation", "l_year"], None),
    8: (["o_year"], None),
    9: (["nation", "o_year"], None),
    10: (["revenue"], 20),
    11: (["value"], None),
    12: (["l_shipmode"], None),
    13: (["custdist", "c_count"], None),
    15: (["s_suppkey"], None),
    16: (["supplier_cnt", "p_brand", "p_type", "p_size"], None),
    18: (["o_totalprice", "o_orderdat"], 100),
    20: (["s_name"], None),
    21: (["numwait", "s_name"], 100),
    22: (["cntrycode"], None),
}


def run_ibex(stem: str) -> list[list[str]] | str:
    """Run one query; return its CSV rows (header first), or an error string."""
    out = OUT_DIR / f"{stem}.csv"
    out.unlink(missing_ok=True)  # never read a previous run's answer
    proc = subprocess.run(
        [str(IBEX_EVAL), "--plugin-path", str(PLUGIN_DIR), str(QUERIES_DIR / f"{stem}.ibex")],
        cwd=IBEX_ROOT, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return f"ibex_eval failed (exit {proc.returncode}): {proc.stderr.strip()[:500]}"
    if not out.exists():
        return f"ibex_eval wrote no {out.name}"
    with open(out, newline="") as f:
        return list(csv.reader(f))


def run_polars(number: int) -> list[list[object]]:
    """Collect one upstream query; return rows (header first) as Python values."""
    module = importlib.import_module(f"queries.polars.q{number}")
    frame = module.q().collect(engine="streaming")
    return [frame.columns] + [list(row) for row in frame.iter_rows()]


def normalize(value: object) -> object:
    """Map a Polars value onto what Ibex's CSV would say."""
    if isinstance(value, datetime.datetime):
        value = value.date()
    if isinstance(value, datetime.date):
        return (value - EPOCH).days
    return value


def cells_match(ibex: str, polars: object) -> bool:
    polars = normalize(polars)
    if isinstance(polars, (int, float)) and not isinstance(polars, bool):
        try:
            got = float(ibex)
        except ValueError:
            return False
        want = float(polars)
        if math.isnan(want) or math.isnan(got):
            return math.isnan(want) and math.isnan(got)
        return abs(got - want) <= max(ABS_TOL, REL_TOL * abs(want))
    if polars is None:
        return ibex == ""
    return ibex.rstrip() == str(polars).rstrip()


def values_match(a: object, b: object) -> bool:
    """Two Polars values equal under the same tolerance `cells_match` applies."""
    return cells_match("" if a is None else str(normalize(a)), b)


def tie_groups(rows: list[list[object]], keys: list[int]) -> list[tuple[int, int]]:
    """[start, end) runs of consecutive rows whose sort keys are equal."""
    groups = []
    start = 0
    for i in range(1, len(rows) + 1):
        same = i < len(rows) and all(
            values_match(rows[i][k], rows[start][k]) for k in keys)
        if not same:
            groups.append((start, i))
            start = i
    return groups


def rows_match(got: list[str], want: list[object],
               columns: list[int] | None = None) -> bool:
    indices = columns if columns is not None else range(len(want))
    return all(cells_match(got[j], want[j]) for j in indices)


def compare(stem: str, ibex: list[list[str]], polars: list[list[object]]) -> tuple[list[str], list[str]]:
    """Return (errors, notes)."""
    errors: list[str] = []
    notes: list[str] = []
    ibex_header, ibex_rows = ibex[0] if ibex else [], ibex[1:]
    polars_header, polars_rows = polars[0], polars[1:]
    if len(ibex_header) != len(polars_header):
        return [f"column count: ibex {len(ibex_header)} {ibex_header}, "
                f"polars {len(polars_header)} {polars_header}"], notes
    polars_names = list(polars_header)
    if ibex_header != list(polars_header) and sorted(ibex_header) == sorted(polars_header):
        # Same columns, different order (q10): align Polars to Ibex's order.
        order = [list(polars_header).index(name) for name in ibex_header]
        polars_rows = [[row[k] for k in order] for row in polars_rows]
        polars_names = list(ibex_header)
        notes.append("column order differs (aligned by name)")
    elif ibex_header != list(polars_header):
        renamed = [f"{a}~{b}" for a, b in zip(ibex_header, polars_header) if a != b]
        notes.append("column names differ (compared by position): " + ", ".join(renamed))
    if len(ibex_rows) != len(polars_rows):
        return [f"row count: ibex {len(ibex_rows)}, polars {len(polars_rows)}"], notes

    def report_row(i: int) -> None:
        for j, (g, w) in enumerate(zip(ibex_rows[i], polars_rows[i])):
            if not cells_match(g, w):
                errors.append(f"row {i} column {ibex_header[j]!r}: ibex {g!r}, polars {normalize(w)!r}")

    key_names, limit = ORDERING.get(int(stem[1:]), ([], None))
    missing = [name for name in key_names if name not in polars_names]
    if missing:
        return [f"sort key {missing} not in polars columns {polars_names}"], notes
    keys = [polars_names.index(name) for name in key_names]
    if keys:
        groups = tie_groups(polars_rows, keys)
    else:
        groups = [(i, i + 1) for i in range(len(polars_rows))]
    reordered = 0
    for start, end in groups:
        if all(rows_match(ibex_rows[i], polars_rows[i]) for i in range(start, end)):
            continue
        if end - start == 1:
            report_row(start)
        elif limit is not None and end == len(polars_rows) == limit:
            # The LIMIT cut this tie group: any of the tied rows may make it.
            bad = [i for i in range(start, end)
                   if not rows_match(ibex_rows[i], polars_rows[start], keys)]
            if bad:
                report_row(bad[0])
            else:
                notes.append(f"rows {start}-{end - 1} tie at the LIMIT; "
                             "compared by sort key only")
        else:
            unused = list(range(start, end))
            for i in range(start, end):
                match = next((k for k in unused
                              if rows_match(ibex_rows[i], polars_rows[k])), None)
                if match is None:
                    errors.append(
                        f"row {i} (tied on {key_names} with rows {start}-{end - 1}): "
                        f"ibex {ibex_rows[i]} matches no polars row in the tie")
                    break
                unused.remove(match)
            else:
                reordered += end - start
        if len(errors) >= MAX_REPORTED:
            return errors[:MAX_REPORTED], notes
    if reordered:
        notes.append(f"{reordered} tied rows in a different order (compared as sets)")
    return errors[:MAX_REPORTED], notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sf", required=True, help="scale factor, as passed to run_bench.sh")
    parser.add_argument("--pdsh-root", type=pathlib.Path,
                        default=bench_env.default_pdsh_root())
    parser.add_argument("queries", nargs="*", help="query stems (default: every qNN.ibex)")
    args = parser.parse_args()

    tables = bench_env.pdsh_scale_dir(args.pdsh_root, args.sf)
    if not bench_env.parquet_link_points_at(tables):
        actual_link = os.readlink(PARQUET_LINK) if PARQUET_LINK.is_symlink() else None
        print(f"error: {PARQUET_LINK} -> {actual_link}, expected {tables}", file=sys.stderr)
        return 2
    if not IBEX_EVAL.exists():
        print(f"error: {IBEX_EVAL} not found; build build-release first", file=sys.stderr)
        return 2
    if not (args.pdsh_root / "queries/polars").is_dir():
        print(f"error: no polars-benchmark checkout at {args.pdsh_root}", file=sys.stderr)
        return 2

    # polars-benchmark reads its settings from the environment at import time.
    os.environ.update({
        "SCALE_FACTOR": str(args.sf),
        "PATH_TABLES": str(bench_env.pdsh_tables_root(args.pdsh_root)),
        "RUN_IO_TYPE": "parquet",
    })
    sys.path.insert(0, str(args.pdsh_root))
    os.chdir(args.pdsh_root)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    stems = args.queries or sorted(p.stem for p in QUERIES_DIR.glob("q??.ibex"))
    failed = []
    for stem in stems:
        ibex = run_ibex(stem)
        if isinstance(ibex, str):
            errors, notes = [ibex], []
        else:
            errors, notes = compare(stem, ibex, run_polars(int(stem[1:])))
        rows = "" if isinstance(ibex, str) else f" ({len(ibex) - 1} rows)"
        print(f"{stem}: {'FAIL' if errors else 'OK'}{rows}")
        for line in errors + [f"note: {n}" for n in notes]:
            print(f"    {line}")
        if errors:
            failed.append(stem)

    if failed:
        print(f"# SF-{args.sf}: {len(failed)} of {len(stems)} answers differ from Polars: {' '.join(failed)}")
        return 1
    print(f"# SF-{args.sf}: all {len(stems)} answers match Polars")
    return 0


if __name__ == "__main__":
    # Exit codes: 0 all match, 1 an answer differs, 2 a setup error, 3 the check
    # itself crashed. An uncaught exception would also exit 1 and read as a
    # disagreement, which it is not (the missing scale-<sf> path on a fresh box).
    try:
        raise SystemExit(main())
    except Exception:  # noqa: BLE001 -- report any crash as a crash
        import traceback
        traceback.print_exc()
        raise SystemExit(3)
