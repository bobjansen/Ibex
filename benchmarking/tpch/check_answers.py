#!/usr/bin/env python3
"""Run the implemented PDS-H queries through ibex_eval and diff their output
against the official TPC-H SF-1 qualification answers (tpch-dbgen/answers/).

Those answer files are valid only at scale factor 1 (see gen_data.sh), which
is what benchmarking/tpch/queries/*.ibex are written against.

Usage:
  uv run benchmarking/tpch/check_answers.py [q1 q3 q5 q6 q10 q19 ...]
  (defaults to all implemented queries if none are given)
"""
import argparse
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

# query number -> (ibex query file stem, revenue-scale columns get a looser
# tolerance since the official answers are rounded to 2 decimal places)
IMPLEMENTED = {
    "q1": "q01",
    "q2": "q02",
    "q3": "q03",
    "q5": "q05",
    "q6": "q06",
    "q9": "q09",
    "q13": "q13",
    "q16": "q16",
    "q10": "q10",
    "q19": "q19",
}

FLOAT_ABS_TOL = 0.02  # official answers are rounded to 2 decimal places
EPOCH = datetime.date(1970, 1, 1)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def read_answer(qnum: str) -> list[dict[str, str]]:
    # Fields are right-padded with spaces to a fixed column width for display.
    # Only rstrip: a leading space can be genuine string content (e.g. dbgen
    # comment text that starts mid-sentence), not padding, so stripping it
    # would corrupt the expected value.
    path = ANSWERS_DIR / f"{qnum}.out"
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="|")
        return [{k.strip(): v.rstrip() for k, v in row.items() if k} for row in reader]


def read_ibex_output(stem: str) -> list[dict[str, str]]:
    path = OUT_DIR / f"{stem}.csv"
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        return [dict(row) for row in reader]


def values_match(expected: str, actual: str) -> bool:
    # Date columns: the official answer renders "YYYY-MM-DD"; write_csv emits
    # Ibex's internal Date representation (signed days since 1970-01-01).
    if DATE_RE.match(expected):
        expected_days = (datetime.date.fromisoformat(expected) - EPOCH).days
        try:
            return expected_days == int(actual)
        except ValueError:
            return False
    try:
        return abs(float(expected) - float(actual)) <= FLOAT_ABS_TOL
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
            if not values_match(exp[col], act[col]):
                errors.append(f"row {i} column '{col}': expected {exp[col]!r}, got {act[col]!r}")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("queries", nargs="*", default=list(IMPLEMENTED.keys()))
    args = parser.parse_args()

    if not IBEX_EVAL.exists():
        print(f"error: {IBEX_EVAL} not found — run cmake --build build-release first", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for qnum in args.queries:
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
