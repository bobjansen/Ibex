"""DuckDB 2 workload semantics, checked against independent small oracles."""
import contextlib
import io
import math
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import polars as pl
import bench_duckdb as bench
import bench_python as python_bench
import check_scale_answers as answers


class DuckDBContract(unittest.TestCase):
    def test_digest_integer_accumulation_cannot_wrap(self):
        import os
        for dtype, values in ((pl.UInt32, [3_000_000_000, 3_000_000_000]),
                              (pl.Int64, [2**63-1, 2**63-1])):
            with (tempfile.TemporaryDirectory() as tmp,
                  patch.dict(os.environ, {"IBEX_DIGEST_OUT": tmp + "/answers.tsv"}),
                  patch.object(python_bench, "DIGESTS", [])):
                python_bench.record_digest("polars", "q", pl.DataFrame({"rank": pl.Series(values, dtype=dtype)}))
                self.assertEqual(python_bench.DIGESTS[0][2], float(sum(values)))

    def test_rounding_ties_match_nearest_instead_of_bankers(self):
        values = [.5, 1.5, 2.5, -.5, -1.5, -2.5, math.nextafter(.5, 0.), math.nextafter(-.5, 0.)]
        expected = [1, 2, 3, -1, -2, -3, 0, 0]
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "prices.csv"
            pl.DataFrame({"symbol": ["A"]*len(values), "price": values}).write_csv(path)
            captured = {}
            with (patch.object(bench, "should_skip", side_effect=lambda fw, name: name != "round_price"),
                  patch.object(bench, "record_answer", side_effect=lambda name, result: captured.update({name: result})),
                  contextlib.redirect_stderr(io.StringIO())):
                bench.bench_duckdb_core(str(path), None, None, 0, 1, duckdb.connect())
            self.assertEqual(captured["round_price"]["v"].to_pylist(), expected)
            with (patch.object(python_bench, "should_skip", side_effect=lambda fw, name: name != "round_price"),
                  patch.object(python_bench, "record_digest", side_effect=lambda fw, name, result: captured.update({name: result})),
                  contextlib.redirect_stderr(io.StringIO())):
                python_bench.bench_polars(str(path), None, None, 0, 1)
            self.assertEqual(captured["round_price"]["v"].to_list(), expected)

    def test_validation_does_not_load_a_competing_dataframe_engine(self):
        root = pathlib.Path(__file__).resolve().parent
        code = f'''
import os, sys, tempfile
sys.path.insert(0, {str(root)!r})
import bench_duckdb as bench
import pyarrow as pa
table = pa.table({{"x": [1, 2]}})
before = set(sys.modules)
with tempfile.TemporaryDirectory() as tmp:
    os.environ["IBEX_DIGEST_OUT"] = tmp + "/answers.tsv"
    bench.record_answer("q", table)
after = set(sys.modules) - before
assert not any(name.split(".")[0] in {{"polars", "pandas", "bench_python"}} for name in after)
'''
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_polars_only_fixture_setup_does_not_load_pandas(self):
        root = pathlib.Path(__file__).resolve().parent
        code = f'''
import sys
sys.path.insert(0, {str(root)!r})
import bench_python as bench
bench.bench_polars_tf(5, 0, 1)
bench.bench_polars_asof(10, 0, 1)
assert "pandas" not in sys.modules
'''
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_parallel_ohlc_uses_first_and_last_input_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "prices.csv"
            n = 300000
            pl.DataFrame({"symbol": ["A", "B", "C"] * (n//3),
                          "price": pl.int_range(0, n, eager=True).cast(pl.Float64)}).write_csv(path)
            captured = {}
            con = duckdb.connect(config={"threads": 4})
            with (patch.object(bench, "should_skip", side_effect=lambda fw, name: name != "ohlc_by_symbol"),
                  patch.object(bench, "record_answer", side_effect=lambda name, result: captured.update({name: result})),
                  contextlib.redirect_stderr(io.StringIO())):
                bench.bench_duckdb_core(str(path), None, None, 0, 1, con)
            result = {row["symbol"]: row for row in captured["ohlc_by_symbol"].to_pylist()}
            for i, symbol in enumerate(("A", "B", "C")):
                self.assertEqual(result[symbol]["open"], float(i))
                self.assertEqual(result[symbol]["last"], float(n-3+i))

    def test_arrow_checks_preserve_ns_nulls_and_large_integers(self):
        import os
        import pyarrow as pa
        import arrow_answers as arrow
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            table = pa.table({"ts": pa.array([1, 1000000001], type=pa.int64()).cast(pa.timestamp("ns")),
                              "key": ['α,"quoted"', None], "value": [2**53+1, 2**53+2]})
            with (patch.dict(os.environ, {"IBEX_ANSWERS_DIR": tmp, "IBEX_DIGEST_OUT": str(root/'digest.tsv')}),
                  patch.object(arrow, "DIGESTS", [])):
                arrow.record("q", table)
                arrow.write()
                self.assertEqual(arrow.DIGESTS[0][2], float(2**54+3))
            self.assertEqual(answers.read_answer(root/'q.csv')[1],
                             [[1, 'α,"quoted"', 2**53+1], [1000000001, None, 2**53+2]])

    def test_native_polars_ewma_handles_gaps_duplicates_and_nulls(self):
        ns = [0, 1_000_000_000, 59_500_000_000, 60_000_000_000, 60_000_000_000, 61_000_000_001]
        prices = [None, 17., 2., 11., 3., 13.]
        frame = pl.DataFrame({"ts": ns, "price": prices}).with_columns(pl.col("ts").cast(pl.Datetime("ns")))
        for engine in ("streaming", "in-memory"):
            actual = python_bench.polars_windowed_ewma(frame.lazy()).collect(engine=engine)
            self.assertEqual(actual.height, len(ns))
            self.assertTrue(actual.select("ts", "price").equals(frame))
            expected = []
            for t in ns:
                values = [p for when, p in zip(ns, prices) if t-60*10**9 < when <= t and p is not None]
                value = None
                for p in values:
                    value = p if value is None else value + 0.1*(p-value)
                expected.append(value)
            for a, b in zip(actual["e"], expected):
                if b is None:
                    self.assertIsNone(a)
                else:
                    self.assertAlmostEqual(a, b, places=10)

    def test_polars_prefix_ewma_handles_empty_null_only_and_restarted_windows(self):
        fixtures = [[], [None] * 160,
                    [None] * 70 + [17., None, -2.] + [None] * 75 + [5., 8.] + [None] * 70]
        for prices in fixtures:
            ns = [i * 10**9 + (i // 31) * 250 * 10**9 for i in range(len(prices))]
            frame = pl.DataFrame({"ts": pl.Series(ns, dtype=pl.Datetime("ns")),
                                  "price": pl.Series(prices, dtype=pl.Float64),
                                  "tag": list(range(len(prices)))})
            expected = []
            for t in ns:
                value = None
                for when, price in zip(ns, prices):
                    if t - 60 * 10**9 < when <= t and price is not None:
                        value = price if value is None else .9 * value + .1 * price
                expected.append(value)
            for engine in ("streaming", "in-memory"):
                with self.subTest(rows=len(prices), engine=engine):
                    actual = python_bench.polars_windowed_ewma(frame.lazy()).collect(engine=engine)
                    self.assertTrue(actual.select(frame.columns).equals(frame))
                    self.assertEqual(actual.columns, frame.columns + ["e"])
                    self.assertEqual(actual.height, len(expected))
                    for got, want in zip(actual["e"], expected):
                        if want is None:
                            self.assertIsNone(got)
                        else:
                            self.assertAlmostEqual(got, want, places=10)

    def test_polars_prefix_ewma_recovers_after_valid_nan_expires(self):
        nan = float("nan")
        prices = [nan, 10., None, 20., 30., nan, None, 40., 50., 60.,
                  None, None, 70., nan, nan, 80., 90., 100.]
        ns = [i * 30 * 10**9 for i in range(len(prices))]
        frame = pl.DataFrame({"ts": pl.Series(ns, dtype=pl.Datetime("ns")), "price": prices})
        for engine in ("streaming", "in-memory"):
            actual = python_bench.polars_windowed_ewma(frame.lazy()).collect(engine=engine)
            self.assertTrue(actual.select("ts", "price").equals(frame))
            for t, got in zip(ns, actual["e"]):
                expected = None
                for when, price in zip(ns, prices):
                    if t - 60 * 10**9 < when <= t and price is not None:
                        expected = price if expected is None else .9 * expected + .1 * price
                if expected is None:
                    self.assertIsNone(got)
                elif math.isnan(expected):
                    self.assertIsNotNone(got)
                    self.assertTrue(math.isnan(got))
                else:
                    self.assertIsNotNone(got)
                    self.assertAlmostEqual(got, expected, places=10)

    def test_irregular_repeated_timestamps_and_full_window_outputs(self):
        ns = [0, 1_000_000_000, 59_500_000_000, 60_000_000_000,
              60_000_000_000, 60_500_000_000, 61_000_000_001]
        prices = [17., 2., 11., 3., 13., 5., 7.]
        frame = pl.DataFrame({"ts": ns, "price": prices}).with_columns(pl.col("ts").cast(pl.Datetime("ns")))
        con = duckdb.connect()
        con.register("fixture", frame.to_arrow())
        con.execute("CREATE TABLE tf_data AS SELECT * FROM fixture")
        for name, sql in bench.timeframe_queries():
            if name in {"tf_lag1", "tf_resample_1m_ohlc"}:
                continue
            result = con.sql(sql).to_arrow_table().to_pylist()
            self.assertEqual(set(result[0]), {"ts", "price", {
                "tf_rolling_count_1m": "c", "tf_rolling_sum_1m": "s", "tf_rolling_mean_5m": "m",
                "tf_rolling_min_1m": "mn", "tf_rolling_max_1m": "mx", "tf_rolling_median_1m": "med",
                "tf_rolling_std_1m": "s", "tf_rolling_ewma_1m": "e"}[name]})
            column = next(c for c in result[0] if c not in {"ts", "price"})
            for i, row in enumerate(result):
                duration = 300 if name == "tf_rolling_mean_5m" else 60
                values = [p for t, p in zip(ns, prices) if ns[i]-duration*10**9 < t <= ns[i]]
                ordered = sorted(values)
                mean = sum(values)/len(values)
                ewm = values[0]
                for value in values[1:]:
                    ewm += 0.1 * (value - ewm)
                expected = {"tf_rolling_count_1m": len(values), "tf_rolling_sum_1m": sum(values),
                            "tf_rolling_mean_5m": mean, "tf_rolling_min_1m": min(values),
                            "tf_rolling_max_1m": max(values),
                            "tf_rolling_median_1m": (ordered[(len(values)-1)//2]+ordered[len(values)//2])/2,
                            "tf_rolling_ewma_1m": ewm,
                            "tf_rolling_std_1m": math.sqrt(sum((x-mean)**2 for x in values)/(len(values)-1))
                                if len(values)>1 else 0.}[name]
                self.assertAlmostEqual(row[column], expected, places=9)

    def test_reshape_integer_division_does_not_round_group_keys(self):
        con = duckdb.connect()
        captured = {}
        with (patch.object(bench, "record_answer", side_effect=lambda name, result: captured.update({name: result})),
              contextlib.redirect_stderr(io.StringIO())):
            bench.bench_duckdb_reshape(0, 1, 720, con)
        rows = captured["melt_wide_to_long"].to_pylist()
        self.assertEqual(con.sql("SHOW TABLES").fetchall(), [])
        keys = {(r["symbol"], r["day"]) for r in rows}
        self.assertIn(("S0000", 400), keys)
        self.assertNotIn(("S0001", 400), keys)
        self.assertEqual(len(keys), 720)

    def test_product_window_handles_negative_and_zero_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "prices.csv"
            path.write_text("symbol,price\nA,2.0\nA,-3.0\nA,0.0\nA,4.0\n")
            captured = {}
            with (patch.object(bench, "should_skip", side_effect=lambda fw, name: name != "cumprod_price"),
                  patch.object(bench, "record_answer", side_effect=lambda name, result: captured.update({name: result})),
                  contextlib.redirect_stderr(io.StringIO())):
                bench.bench_duckdb_core(str(path), None, None, 0, 1, duckdb.connect())
            self.assertEqual(captured["cumprod_price"]["cp"].to_pylist(), [2., -6., 0., 0.])

    def test_grouped_updates_must_preserve_row_order(self):
        rows = [["B", 2., 1], ["A", 1., 1]]
        self.assertTrue(answers.compare_answer("rank_by_symbol", (["symbol", "price", "rk"], rows),
                                               (["symbol", "price", "rk"], list(reversed(rows)))))


if __name__ == "__main__":
    unittest.main()
