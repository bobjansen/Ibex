#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Paired check of the old native-table sink and Arrow result delivery.

This compares result boundaries, not two ClickHouse engine revisions.
Inputs are resident; full result equality is checked outside timing.
"""
import argparse
import gc
import json
import statistics
import time

import polars as pl
from chdb.session import Session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--pairs", type=int, default=10)
    args = parser.parse_args()
    if args.pairs < 2 or args.pairs % 2:
        parser.error("--pairs must be an even number >= 2")
    queries = {
        "mean_by_symbol": "SELECT symbol, avg(price) AS avg_price FROM prices GROUP BY symbol",
        "update_price_x2": "SELECT *, price * 2 AS price_x2 FROM prices",
        "order_head_topk": "SELECT * FROM prices ORDER BY price DESC LIMIT 100",
    }
    report = {"threads": args.threads, "pairs": args.pairs, "queries": {}}
    with Session() as session:
        session.query(f"SET max_threads={args.threads}")
        path = args.csv.replace("'", "''")
        session.query(f"CREATE TABLE prices ENGINE=Memory AS SELECT * FROM file('{path}', CSVWithNames)")
        for name, sql in queries.items():
            def native():
                session.query(f"CREATE OR REPLACE TABLE native_sink ENGINE=Memory AS {sql}")
            def arrow():
                return session.query(sql, "ArrowTable")
            native()
            expected = pl.from_arrow(arrow())
            actual = pl.from_arrow(session.query("SELECT * FROM native_sink", "ArrowTable"))
            if name == "order_head_topk":
                # Boundary ties have no secondary ordering key.
                assert actual["price"].to_list() == expected["price"].to_list()
            else:
                from polars.testing import assert_frame_equal
                assert_frame_equal(actual.sort(actual.columns), expected.sort(expected.columns),
                                   check_exact=False, rel_tol=1e-9, abs_tol=1e-10)
            row_count = actual.height
            del actual, expected
            gc.collect()
            for _ in range(2):
                native()
                result = arrow()
                del result
            samples = {"native_table": [], "arrow_table": []}
            for pair in range(args.pairs):
                order = [("native_table", native), ("arrow_table", arrow)]
                if pair % 2:
                    order.reverse()
                for mode, query in order:
                    start = time.perf_counter()
                    result = query()
                    samples[mode].append(1000 * (time.perf_counter() - start))
                    del result
            report["queries"][name] = {
                "rows": row_count,
                "samples_ms": samples,
                "median_ms": {mode: statistics.median(values) for mode, values in samples.items()},
            }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
