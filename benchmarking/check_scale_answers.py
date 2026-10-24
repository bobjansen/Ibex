#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Fail closed on invalid benchmark answers.

The default TSV mode checks full-scale row counts and numeric sums. These are
screens, not proofs: the scale runner additionally requires an untimed bounded
pass using --full-answers, which compares every cell, column, and required sort
key. --report-only is an explicit diagnostic opt-out; runners must not use it.
"""
import argparse
import csv
import math
import pathlib
import sys

REL_TOL = 1e-6
FULL_REL_TOL = 1e-9
FULL_ABS_TOL = 1e-10
# These workloads deliberately have no cross-engine deterministic answer.
STOCHASTIC = {"rand_uniform", "rand_normal", "rand_int", "rand_bernoulli"}
# Operations without a Polars counterpart, plus separate parse-overhead rows.
IBEX_ONLY = {"as_timeframe", "abs_int_price", "sqrt_coalesce_price"}
DUCKDB_UNSUPPORTED = frozenset()  # all deterministic queries have stock SQL counterparts
ROW_PRESERVING = {"rank_by_symbol", "lag_by_symbol", "cumsum_by_symbol", "pmin_clip",
                  "abs_price", "sqrt_price", "log_price", "exp_price", "round_price",
                  "floor_price", "ceil_price", "sin_price", "cos_price", "tanh_price"}
SORT_KEYS = {
    "sort_price": ["price"], "sort_price_desc": ["price"],
    "sort_symbol": ["symbol"], "sort_symbol_price": ["symbol", "price"],
    "sort_symbol_price_desc": ["symbol", "price"],
    "order_head_topk": ["price"], "order_tail_topk": ["price"],
    "filter_group_sort": ["avg"], "tf_resample_1m_ohlc": ["ts"],
}


def load(path):
    """Reject malformed/empty/duplicate records rather than overwriting them."""
    result = {}
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not {"query", "rows", "numeric_sum"} <= set(reader.fieldnames or []):
            raise ValueError(f"{path}: missing digest columns")
        for row in reader:
            query = row["query"]
            if not query or query in result:
                raise ValueError(f"{path}: empty or duplicate query {query!r}")
            rows, total = int(row["rows"]), float(row["numeric_sum"])
            if rows < 0 or math.isnan(total):
                raise ValueError(f"{path}: invalid digest for {query}")
            result[query] = rows, total
    if not result:
        raise ValueError(f"{path}: no answers")
    return result


def digest_errors(left, right, required=()):
    shared = (set(left) & set(right)) - STOCHASTIC
    errors = []
    if not shared:
        errors.append("no shared queries")
    for query in sorted((set(required) - STOCHASTIC) - shared):
        errors.append(f"{query}: required answer missing")
    for query in sorted(shared):
        lrows, lsum = left[query]
        rrows, rsum = right[query]
        if lrows != rrows:
            errors.append(f"{query}: rows {lrows} vs {rrows}")
        elif not math.isclose(lsum, rsum, rel_tol=REL_TOL, abs_tol=REL_TOL):
            errors.append(f"{query}: numeric sum {lsum!r} vs {rsum!r}")
    return errors


def decode_cell(cell):
    if cell == "n:":
        return None
    prefix, sep, value = cell.partition(":")
    if not sep:
        raise ValueError(f"untyped answer cell {cell!r}")
    if prefix == "s":
        return value
    if prefix == "i":
        return int(value)
    if prefix == "f":
        return float(value)
    if prefix == "b" and value in ("0", "1"):
        return bool(int(value))
    raise ValueError(f"invalid answer cell {cell!r}")


def read_answer(path):
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header or len(set(header)) != len(header):
            raise ValueError(f"{path}: absent or duplicate columns")
        rows = []
        for row in reader:
            if len(row) != len(header):
                raise ValueError(f"{path}: row width does not match header")
            rows.append([decode_cell(value) for value in row])
    return header, rows


def cells_match(left, right):
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, int) and isinstance(right, int):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if isinstance(left, float) and math.isnan(left):
            return isinstance(right, float) and math.isnan(right)
        return math.isclose(left, right, rel_tol=FULL_REL_TOL, abs_tol=FULL_ABS_TOL)
    return type(left) is type(right) and left == right


def cell_key(value):
    # Total ordering for unordered answers, keeping null, NaN and empty String
    # distinct and retaining exact Int64 values above 2**53.
    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        return (1, value)
    if isinstance(value, (int, float)):
        return (2, 0) if isinstance(value, float) and math.isnan(value) else (3, value)
    return (4, value)


def compare_answer(query, left, right):
    lh, lr = left
    rh, rr = right
    if set(lh) != set(rh):
        return [f"{query}: columns {lh} vs {rh}"]
    order = [rh.index(name) for name in lh]
    rr = [[row[i] for i in order] for row in rr]
    if len(lr) != len(rr):
        return [f"{query}: rows {len(lr)} vs {len(rr)}"]
    # Check requested order separately from membership: ties may reorder, but
    # skipping a sort must fail even though the row multiset is unchanged.
    keys = SORT_KEYS.get(query)
    if keys:
        indices = [lh.index(name) for name in keys]
        for a, b in zip(lr, rr):
            if not all(cells_match(a[i], b[i]) for i in indices):
                return [f"{query}: sort-key sequence differs ({keys})"]
    if query in {"order_head_topk_by_symbol", "order_tail_topk_by_symbol"}:
        group, price = lh.index("symbol"), lh.index("price")
        def grouped(rows):
            out = {}
            for row in rows:
                out.setdefault(row[group], []).append(row[price])
            return out
        lg, rg = grouped(lr), grouped(rr)
        if lg.keys() != rg.keys() or any(
            len(lg[g]) != len(rg[g]) or not all(cells_match(a, b) for a, b in zip(lg[g], rg[g]))
            for g in lg):
            return [f"{query}: per-group sort-key sequence differs"]
    if query.startswith("tf_") and query != "tf_resample_1m_ohlc":
        ordered = True
    else:
        ordered = query.startswith(("where_update_", "fill_")) or query in {
            "select_one_column", "update_price_x2", "cumsum_price", "cumprod_price"} or query in ROW_PRESERVING
    if not ordered:
        key = lambda row: tuple(cell_key(v) for v in row)
        lr, rr = sorted(lr, key=key), sorted(rr, key=key)
    for i, (a, b) in enumerate(zip(lr, rr)):
        for j, (av, bv) in enumerate(zip(a, b)):
            if not cells_match(av, bv):
                return [f"{query}: row {i} column {lh[j]!r}: {av!r} vs {bv!r}"]
    return []


def compare_directories(left, right, required=(), excluded=()):
    a = {p.stem: p for p in pathlib.Path(left).glob("*.csv")}
    b = {p.stem: p for p in pathlib.Path(right).glob("*.csv")}
    ignored = STOCHASTIC | IBEX_ONLY | {q for q in a | b if q.startswith("parse_")}
    expected = (set(a) | set(b) | set(required)) - ignored - set(excluded)
    if not expected:
        return ["no deterministic answers to compare"]
    errors = []
    for query in sorted(expected):
        if query not in a or query not in b:
            errors.append(f"{query}: required full answer missing")
            continue
        errors.extend(compare_answer(query, read_answer(a[query]), read_answer(b[query])))
    return errors


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("ibex")
    ap.add_argument("others", nargs="+")
    ap.add_argument("--full-answers", action="store_true", help="compare typed answer directories")
    ap.add_argument("--require", default="", help="comma-separated required query names")
    ap.add_argument("--report-only", action="store_true", help="diagnostic only: do not gate on mismatches")
    ap.add_argument("--strict", action="store_true", help=argparse.SUPPRESS)  # compatible; now default
    args = ap.parse_args(argv)
    required = set(filter(None, args.require.split(",")))
    bad = 0
    try:
        left = None if args.full_answers else load(args.ibex)
        for path in args.others:
            errors = (compare_directories(args.ibex, path, required) if args.full_answers else
                      digest_errors(left, load(path), required))
            print(f"== {path}: {len(errors)} mismatched")
            for error in errors:
                print("  MISMATCH " + error)
            bad += len(errors)
    except (ValueError, KeyError, OSError, OverflowError, csv.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2  # malformed/missing artifacts never become a diagnostic success
    return 1 if bad and not args.report_only else 0


if __name__ == "__main__":
    raise SystemExit(main())
