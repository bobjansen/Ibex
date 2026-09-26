#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Run the implemented PDS-H queries through ibex_eval and diff their output
against the official TPC-H SF-1 qualification answers (tpch-dbgen/answers/).

Those answer files are valid only at scale factor 1 (gen_data.sh fetches
them). The data is polars-benchmark's own SF-1 tables
(`make data-tables SCALE_FACTOR=1.0` in the checkout), the same generator the
timed runs use.

Usage:
  uv run benchmarking/tpch/check_answers.py [--pdsh-root DIR] [q1 q3 q5 q6 q10 q19 ...]
  (defaults to all implemented queries if none are given)
"""
import argparse
import contextlib
import csv
import datetime
import pathlib
import re
import subprocess
import sys

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
ANSWERS_DIR = IBEX_ROOT / "benchmarking/data/tpch/dbgen/answers"
OUT_DIR = IBEX_ROOT / "benchmarking/data/tpch/out"
IBEX_EVAL = IBEX_ROOT / "build-release/tools/ibex_eval"
PLUGIN_DIR = IBEX_ROOT / "build-release/tools"

sys.path.insert(0, str(SCRIPT_DIR.parent))
import bench_env  # noqa: E402

PARQUET_LINK = bench_env.PARQUET_LINK


@contextlib.contextmanager
def scale_factor_1(pdsh_root: pathlib.Path):
    """Point the parquet symlink at SF-1 for the check, then put it back.

    The official answers are SF-1 only, so this script used to be run by
    flipping the symlink by hand. Twice now the flip was never undone and a
    LATER benchmark silently measured a different dataset -- once producing a
    1.9x "regression" that was purely the scale factor, and that got as far as
    being written down as a machine-contamination finding before it was caught.
    Owning the flip here means the trap cannot be left armed.
    """
    sf1 = bench_env.pdsh_scale_dir(pdsh_root, 1)
    previous = PARQUET_LINK.readlink() if PARQUET_LINK.is_symlink() else None
    flip = not bench_env.parquet_link_points_at(sf1)
    if flip:
        print(f"# repointing {PARQUET_LINK.name} -> {sf1} "
              f"(was {previous}); will restore on exit")
        bench_env.point_parquet_link(sf1)
    try:
        yield
    finally:
        if flip:
            PARQUET_LINK.unlink()
            if previous is not None:
                PARQUET_LINK.symlink_to(previous)
            print(f"# restored {PARQUET_LINK.name} -> {previous}")

# query number -> (ibex query file stem, revenue-scale columns get a looser
# tolerance since the official answers are rounded to 2 decimal places)
IMPLEMENTED = {
    "q1": "q01",
    "q2": "q02",
    "q3": "q03",
    "q4": "q04",
    "q5": "q05",
    "q6": "q06",
    "q7": "q07",
    "q8": "q08",
    "q9": "q09",
    "q13": "q13",
    "q14": "q14",
    "q15": "q15",
    "q16": "q16",
    "q17": "q17",
    "q11": "q11",
    "q12": "q12",
    "q10": "q10",
    "q18": "q18",
    "q19": "q19",
    "q20": "q20",
    "q21": "q21",
    "q22": "q22",
}

FLOAT_ABS_TOL = 0.02  # official answers are rounded to 2 decimal places

# Per-query overrides of FLOAT_ABS_TOL. Only widen one with proof that the row
# set is right and the gap is arithmetic, not a wrong answer.
#
# q17: we report avg_yearly = 348406.054, the answer file says 348406.02, a gap
# of 0.034. TPC-H's reference arithmetic is DECIMAL; the Parquet is Float64. The
# gap is provably not a row-set difference: the query selects 587 rows summing
# to 2438842.38 where the answer file implies 2438842.14 -- a difference of 0.24,
# while the CHEAPEST qualifying row is 989.02, so no row could be added or
# dropped to explain it. Polars, over the same Parquet, computes
# 348406.05428571434 against our 348406.05428571376: the two engines agree to
# 1e-9 and both differ from the decimal reference.
TOLERANCES = {"q17": 0.05}

# Per-query fixups for the answer file's column headers, applied to the expected
# rows so the query can name its columns naturally. dbgen pads each header to the
# column's display width and truncates it to fit, and names an unaliased
# expression `colN`. q18's date column is only as wide as "1994-04-07", so
# "o_orderdate" arrives as "o_orderdat", and its unaliased sum(l_quantity) as
# "col6".
COLUMN_ALIASES = {
    "q18": {"o_orderdat": "o_orderdate", "col6": "sum_quantity"},
}

EPOCH = datetime.date(1970, 1, 1)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def read_answer(qnum: str) -> list[dict[str, str]]:
    # Fields are right-padded with spaces to a fixed column width for display.
    # Only rstrip: a leading space can be genuine string content (e.g. dbgen
    # comment text that starts mid-sentence), not padding, so stripping it
    # would corrupt the expected value.
    path = ANSWERS_DIR / f"{qnum}.out"
    aliases = COLUMN_ALIASES.get(qnum, {})
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        return [
            {aliases.get(k.strip(), k.strip()): v.rstrip() for k, v in row.items() if k}
            for row in reader
        ]


def read_ibex_output(stem: str) -> list[dict[str, str]]:
    path = OUT_DIR / f"{stem}.csv"
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def values_match(expected: str, actual: str, tol: float) -> bool:
    # Date columns: the official answer renders "YYYY-MM-DD"; write_csv emits
    # Ibex's internal Date representation (signed days since 1970-01-01).
    if DATE_RE.match(expected):
        expected_days = (datetime.date.fromisoformat(expected) - EPOCH).days
        try:
            return expected_days == int(actual)
        except ValueError:
            return False
    try:
        return abs(float(expected) - float(actual)) <= tol
    except ValueError:
        # The official .out format right-pads every field to a fixed column
        # width for display, which is indistinguishable from a genuine
        # trailing space in the source text (dbgen comment fields do contain
        # real trailing spaces) -- rstrip both sides since that padding can't
        # be reconstructed reliably. Leading whitespace is never padding in
        # this format (numeric columns are the only left-padded ones, and
        # those are compared as numbers above) so it's left intact.
        return expected.rstrip().strip('"') == actual.rstrip().strip('"')


def check_query(qnum: str, stem: str) -> list[str]:
    errors = []
    script = SCRIPT_DIR / "queries" / f"{stem}.ibex"
    if not script.exists():
        return [f"missing query file: {script}"]

    proc = subprocess.run(
        [str(IBEX_EVAL), "--plugin-path", str(PLUGIN_DIR), str(script)],
        cwd=IBEX_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return [f"ibex_eval failed (exit {proc.returncode}): {proc.stderr.strip()}"]

    tol = TOLERANCES.get(qnum, FLOAT_ABS_TOL)
    expected_rows = read_answer(qnum)
    actual_rows = read_ibex_output(stem)

    if len(expected_rows) != len(actual_rows):
        return [f"row count mismatch: expected {len(expected_rows)}, got {len(actual_rows)}"]

    expected_cols = set(expected_rows[0].keys()) if expected_rows else set()
    actual_cols = set(actual_rows[0].keys()) if actual_rows else set()
    if expected_cols != actual_cols:
        return [f"column mismatch: expected {sorted(expected_cols)}, got {sorted(actual_cols)}"]

    for i, (exp, act) in enumerate(zip(expected_rows, actual_rows)):
        for col in expected_cols:
            if not values_match(exp[col], act[col], tol):
                errors.append(f"row {i} column '{col}': expected {exp[col]!r}, got {act[col]!r}")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdsh-root", type=pathlib.Path, default=bench_env.default_pdsh_root())
    parser.add_argument("queries", nargs="*", default=list(IMPLEMENTED.keys()))
    args = parser.parse_args()

    sf1 = bench_env.pdsh_scale_dir(args.pdsh_root, 1)
    if not sf1.is_dir():
        print(f"error: {sf1} not found — run `make data-tables SCALE_FACTOR=1.0` in {args.pdsh_root}",
              file=sys.stderr)
        return 1

    if not IBEX_EVAL.exists():
        print(f"error: {IBEX_EVAL} not found — run cmake --build build-release first", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with scale_factor_1(args.pdsh_root):
        return run_checks(args.queries)


def run_checks(queries: list[str]) -> int:
    all_ok = True
    for qnum in queries:
        stem = IMPLEMENTED.get(qnum)
        if stem is None:
            print(f"{qnum}: SKIP (not implemented)")
            continue
        errors = check_query(qnum, stem)
        if errors:
            all_ok = False
            print(f"{qnum}: FAIL")
            for e in errors[:10]:
                print(f"    {e}")
            if len(errors) > 10:
                print(f"    ... and {len(errors) - 10} more")
        else:
            print(f"{qnum}: OK")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
