# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Untimed regressions for query semantics and benchmark generality.

Run: .venv/bin/python -m unittest discover -s benchmarking -p test_review_regressions.py
Requires the Release evaluator and Parquet plugin.
"""
import datetime
import os
import pathlib
import subprocess
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parents[1]
EVAL = ROOT / "build-release/tools/ibex_eval"


class ReviewRegressions(unittest.TestCase):
    def evaluate(self, script, directory, **env):
        path = directory / "query.ibex"
        path.write_text(script)
        result = subprocess.run(
            [str(EVAL), "--plugin-path", str(EVAL.parent), str(path)],
            cwd=ROOT, env=os.environ | {"IBEX_CORES": "1"} | env,
            capture_output=True, text=True, check=True)
        return result

    def test_q21_repeated_suppliers(self):
        date = datetime.date(1995, 1, 1)
        # Order 10: two late lines from A, an on-time line from B -> A counts twice.
        # Order 20: only C supplies it, despite two lines -> no contribution.
        # Order 30: A and B are both late -> no contribution.
        # Order 40: only B is late, A is on time -> B counts once.
        keys = [10, 10, 10, 20, 20, 30, 30, 40, 40]
        suppliers = [1, 1, 2, 3, 3, 1, 2, 1, 2]
        late = [1, 1, 0, 1, 0, 1, 1, 0, 1]
        tables = {
            "supplier": pa.table({"s_suppkey": [1, 2, 3],
                                  "s_name": ["A", "B", "C"], "s_nationkey": [1, 1, 1]}),
            "nation": pa.table({"n_nationkey": [1], "n_name": ["SAUDI ARABIA"]}),
            "orders": pa.table({"o_orderkey": [10, 20, 30, 40],
                                "o_orderstatus": ["F"] * 4}),
            "lineitem": pa.table({"l_orderkey": keys, "l_suppkey": suppliers,
                                  "l_commitdate": [date] * len(keys),
                                  "l_receiptdate": [date + datetime.timedelta(days=d) for d in late]}),
        }
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            for name, table in tables.items():
                pq.write_table(table, directory / (name + ".parquet"))
            script = (ROOT / "benchmarking/tpch/queries/q21.ibex").read_text()
            script = script.replace("benchmarking/data/tpch/parquet/", str(directory) + "/")
            output = directory / "answer.csv"
            script = script.replace("benchmarking/data/tpch/out/q21.csv", str(output))
            self.evaluate(script, directory)
            with duckdb.connect() as connection:
                for name, table in tables.items():
                    connection.register(name, table)
                expected = connection.sql("""
                    SELECT s_name, count(*) AS numwait
                    FROM supplier, lineitem l1, orders, nation
                    WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
                      AND o_orderstatus = 'F' AND s_nationkey = n_nationkey
                      AND n_name = 'SAUDI ARABIA' AND l1.l_receiptdate > l1.l_commitdate
                      AND EXISTS (SELECT 1 FROM lineitem l2
                          WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey)
                      AND NOT EXISTS (SELECT 1 FROM lineitem l3
                          WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey
                            AND l3.l_receiptdate > l3.l_commitdate)
                    GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100
                """).fetchall()
                self.assertEqual(expected, [("A", 2), ("B", 1)])
                actual = connection.sql("SELECT * FROM read_csv_auto(?)", params=[str(output)]).fetchall()
                self.assertEqual(actual, expected)

    def test_polars_rolling_irregular_and_repeated_timestamps(self):
        import contextlib
        import io
        import statistics
        from unittest.mock import patch
        import bench_python as bp
        import polars as pl
        from polars.testing import assert_frame_equal

        seconds = [0, 1, 1, 3, 60, 61, 301]
        timestamps = bp.pd.to_datetime(seconds, unit="s")
        outputs = []

        def evaluate(fn, warmup, iters):
            result = fn().collect()
            outputs.append(result)
            return (0., 0., 0., 0., 0., 0., result)

        def skip(framework, name):
            return not name.startswith("tf_rolling_") or "ewma" in name

        with (
            patch.object(bp.pd, "to_datetime", return_value=timestamps),
            patch.object(bp, "timer", evaluate),
            patch.object(bp, "should_skip", skip),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            rows = bp.bench_polars_tf(len(seconds), 0, 1)
        names = [row[1] for row in rows if not skip("polars", row[1])]
        prices = [100. + i for i in range(len(seconds))]
        operations = {
            "tf_rolling_count_1m": ("c", 60, len),
            "tf_rolling_sum_1m": ("s", 60, sum),
            "tf_rolling_mean_5m": ("m", 300, statistics.mean),
            "tf_rolling_min_1m": ("mn", 60, min),
            "tf_rolling_max_1m": ("mx", 60, max),
            "tf_rolling_median_1m": ("med", 60, statistics.median),
            "tf_rolling_std_1m": ("s", 60,
                                  lambda xs: statistics.stdev(xs) if len(xs) > 1 else 0.0),
        }
        self.assertEqual(len(outputs), len(operations))
        for name, result in zip(names, outputs):
            column, duration, operation = operations[name]
            expected = [operation([p for t, p in zip(seconds, prices)
                                   if now - duration < t <= now]) for now in seconds]
            with self.subTest(query=name):
                reference = pl.DataFrame({"ts": timestamps, "price": prices,
                                          column: expected})
                assert_frame_equal(result, reference, check_dtypes=False, check_exact=False)

    def test_long_repeated_parquet_strings_keep_dictionary(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            for codec in ("NONE", "zstd"):
                with self.subTest(codec=codec):
                    labels = [str(i).zfill(8) + "x" * 2040 for i in range(100)]
                    path = directory / "labels.parquet"
                    pq.write_table(pa.table({"label": [labels[i % 100] for i in range(10000)]}),
                                   path, compression=codec, use_dictionary=True)
                    result = self.evaluate(
                        'import "parquet"; let t = parquet::read("' + str(path) + '"); '
                        't[select {n=count()}, by label][select {n=count()}];',
                        directory, IBEX_DICT_ENTRY_DEBUG="1")
                    self.assertIn("-> categorical", result.stderr)
                    self.assertNotIn("-> dense", result.stderr)

    def test_compressed_unique_parquet_strings_are_not_a_small_dictionary(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            path = directory / "unique.parquet"
            # Highly compressible bytes do not imply a small entry count.
            labels = [str(i).zfill(8) + "x" * 24 for i in range(20000)]
            pq.write_table(pa.table({"label": labels}), path,
                           compression="zstd", use_dictionary=True)
            result = self.evaluate(
                'import "parquet"; let t = parquet::read("' + str(path) + '"); '
                't[select {n=count()}, by label][select {n=count()}];',
                directory, IBEX_DICT_ENTRY_DEBUG="1")
            self.assertIn("-> dense", result.stderr)
            self.assertNotIn("-> categorical", result.stderr)


if __name__ == "__main__":
    unittest.main()
