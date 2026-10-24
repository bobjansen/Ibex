#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Gate each completed scale using this run's timed query coverage and digests."""
import argparse
import csv
import json
import math
import pathlib

from check_scale_answers import STOCHASTIC, digest_errors, load


def measured(path, framework):
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not {"framework", "query", "avg_ms", "rows"} <= set(reader.fieldnames or []):
            raise ValueError(f"{path}: invalid timing file")
        result, cut, counts = set(), set(), {}
        for row in reader:
            if row["framework"] != framework:
                continue
            query, elapsed = row["query"], float(row["avg_ms"])
            if not math.isfinite(elapsed) or query in result | cut:
                raise ValueError(f"{path}: invalid or duplicate timing for {query}")
            counts[query] = int(row["rows"])
            if counts[query] < 0:
                raise ValueError(f"{path}: negative result row count for {query}")
            (result if elapsed >= 0 else cut).add(query)
    if not result:
        raise ValueError(f"{path}: no measured {framework} queries")
    return result - STOCHASTIC, cut - STOCHASTIC, counts


def check(root, ibex_variants, polars_variants, contracts=None, without_reshape=False, without_timeframe=False):
    digests = {}
    queries = {}
    if contracts is not None:
        report = json.loads(pathlib.Path(contracts).read_text())
        if report.get("status") != "passed" or not report.get("checks"):
            raise ValueError("complete-answer preflight did not pass")
        coverages = [set(c["query_names"]) for c in report["checks"]]
        if any(c != coverages[0] for c in coverages) or any(c.get("errors") for c in report["checks"]):
            raise ValueError("inconsistent or failed preflight coverage")
        supported = coverages[0]
        if not supported:
            raise ValueError("preflight has no checked query contracts")
    else:
        supported = None
    if supported is not None:
        if without_reshape:
            supported = {q for q in supported if not q.startswith(("melt_", "dcast_"))}
        if without_timeframe:
            supported = {q for q in supported if not q.startswith("tf_")}
    for variant in ibex_variants + polars_variants:
        stem = variant.replace("-", "_")
        file = "python.tsv" if variant == "polars" else stem + ".tsv"
        digests[variant] = load(root / (stem + "_digest.tsv"))
        queries[variant], cut, counts = measured(root / file, variant)
        if supported is not None:
            absent = supported - queries[variant] - cut
            if absent:
                raise ValueError(f"{variant}: expected timed queries missing: {sorted(absent)}")
        required = queries[variant] if supported is None else queries[variant] & supported
        missing = required - set(digests[variant])
        if missing:
            raise ValueError(f"{variant}: measured queries missing from digest: {sorted(missing)}")
        for query in required:
            if digests[variant][query][0] != counts[query]:
                raise ValueError(f"{variant}/{query}: digest rows disagree with timing rows")
    errors = []
    for ibex in ibex_variants:
        for polars in polars_variants:
            required = queries[ibex] & queries[polars]
            if supported is not None:
                unchecked = required - supported
                if unchecked:
                    raise ValueError(f"{ibex}/{polars}: measured queries lack full-answer contracts: {sorted(unchecked)}")
                required &= supported
            if not required:
                errors.append(f"{ibex}/{polars}: no shared measured queries")
                continue
            # Do not compare stale/untimed or deliberately cut cells.
            left = {q: digests[ibex][q] for q in required}
            right = {q: digests[polars][q] for q in required}
            bad = digest_errors(left, right, required)
            errors.extend(f"{ibex}/{polars}: {e}" for e in bad)
            print(f"{ibex}/{polars}: {len(required)} measured queries, {len(bad)} mismatches")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=pathlib.Path)
    parser.add_argument("--ibex", default="ibex")
    parser.add_argument("--polars", default="polars")
    parser.add_argument("--contracts", type=pathlib.Path, help="successful full-answer preflight report")
    parser.add_argument("--without-reshape", action="store_true")
    parser.add_argument("--without-timeframe", action="store_true")
    args = parser.parse_args()
    try:
        errors = check(args.directory, args.ibex.split(","), args.polars.split(","), args.contracts,
                       args.without_reshape, args.without_timeframe)
    except (OSError, ValueError, KeyError) as exc:
        print(f"answer validation error: {exc}")
        return 2
    for error in errors:
        print("MISMATCH " + error)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
