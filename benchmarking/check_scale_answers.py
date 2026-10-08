#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare in-memory suite answers across engines.

A timing comparison between two different answers means nothing, so the scale
suite records one digest per query from each harness (IBEX_DIGEST_OUT): the
row count and the sum of every numeric column, nulls and NaN skipped. Neither
row order nor column names enter it. This compares Ibex's digests against each
other engine's.

    check_scale_answers.py IBEX_DIGEST OTHER_DIGEST [OTHER_DIGEST ...] [--strict]

Rows must match exactly; sums within a relative 1e-6 (float sums depend on the
order they were added in). A mismatch is a lead to investigate, not a verdict:
an engine may legitimately produce extra columns or a different type for the
same answer. Exits 1 on a mismatch only with --strict.
"""
import argparse
import csv
import math
import pathlib
import sys

REL_TOL = 1e-6


def load(path):
    with open(path, newline="") as f:
        return {
            row["query"]: (int(row["rows"]), float(row["numeric_sum"]))
            for row in csv.DictReader(f, delimiter="\t")
        }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("ibex")
    ap.add_argument("others", nargs="+")
    ap.add_argument("--strict", action="store_true")
    args = ap.parse_args()

    ibex = load(args.ibex)
    mismatches = 0
    for other_path in args.others:
        other = load(other_path)
        label = pathlib.Path(other_path).stem
        shared = sorted(set(ibex) & set(other))
        bad = []
        for query in shared:
            (ir, isum), (orows, osum) = ibex[query], other[query]
            scale = max(1.0, abs(isum), abs(osum))
            if ir != orows:
                bad.append(f"{query}: rows {ir} (ibex) vs {orows}")
            elif not math.isclose(isum, osum, rel_tol=REL_TOL, abs_tol=REL_TOL * scale):
                bad.append(f"{query}: numeric sum {isum!r} (ibex) vs {osum!r}")
        only_ibex = sorted(set(ibex) - set(other))
        print(f"== ibex vs {label}: {len(shared)} shared queries, {len(bad)} mismatched, "
              f"{len(only_ibex)} only in ibex, {len(set(other) - set(ibex))} only in {label}")
        for line in bad:
            print(f"  MISMATCH {line}")
        mismatches += len(bad)
    if args.strict and mismatches:
        sys.exit(1)


if __name__ == "__main__":
    main()
