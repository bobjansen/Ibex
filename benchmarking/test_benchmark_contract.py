# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Small, untimed regression checks for the in-memory benchmark contract.

Run: uv run python -m unittest discover -s benchmarking -p test_benchmark_contract.py
"""
import contextlib
import io
import pathlib
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal

import bench_python as bp
import bench_duckdb as bd
import bench_clickhouse as bc
import bench_datafusion as bf


class BenchmarkContract(unittest.TestCase):
    def test_irregular_window_strict_left_oracle(self):
        """A duration window must use each row's timestamp, not row spacing."""
        # Times are intentionally irregular: 0s, 1s, 3s, 6s.  For a 2-second
        # (t-duration, t] window the expected sums are 10, 30, 30, 40.
        timestamps = [0, 1_000_000_000, 3_000_000_000, 6_000_000_000]
        values = [10.0, 20.0, 30.0, 40.0]
        expected = [10.0, 30.0, 30.0, 40.0]
        frame = pl.DataFrame({
            "ts": pl.from_epoch(pl.Series(timestamps), time_unit="ns"),
            "price": values,
        })
        result = frame.sort("ts").rolling(
            index_column="ts", period="2s", closed="right"
        ).agg(pl.col("price").sum().alias("s"))
        self.assertEqual(result["s"].to_list(), expected)

        # Pandas' time-based rolling has the same strict-left/closed-right
        # interval when closed="right" is specified explicitly.
        pandas = frame.to_pandas().set_index("ts")
        actual = pandas["price"].rolling("2s", closed="right").sum().to_list()
        self.assertEqual(actual, expected)

    def test_irregular_window_sql_oracle(self):
        """SQL engines can express the same interval with an explicit join."""
        import duckdb
        import pyarrow as pa
        from datafusion import SessionContext
        from chdb.session import Session

        values = [(0, 10.0), (1, 20.0), (3, 30.0), (6, 40.0)]
        expected = [10.0, 30.0, 30.0, 40.0]
        query = (
            "SELECT a.ts, SUM(b.price) AS s FROM tf a JOIN tf b "
            "ON b.ts > a.ts - 2 AND b.ts <= a.ts "
            "GROUP BY a.ts ORDER BY a.ts"
        )
        with duckdb.connect() as connection:
            connection.execute("CREATE TABLE tf(ts BIGINT, price DOUBLE)")
            connection.executemany("INSERT INTO tf VALUES (?, ?)", values)
            self.assertEqual(connection.sql(query).to_arrow_reader().read_all()
                             .column("s").to_pylist(), expected)

        context = SessionContext()
        context.register_record_batches(
            "tf", [pa.table({"ts": [x[0] for x in values],
                             "price": [x[1] for x in values]}).to_batches()])
        batches = context.sql(query).collect()
        self.assertEqual([x.as_py() for b in batches for x in b.column("s")], expected)

        with Session() as session:
            session.query("CREATE TABLE tf(ts Int64, price Float64) ENGINE=Memory")
            session.query("INSERT INTO tf VALUES " + ",".join(f"({t},{p})" for t, p in values))
            self.assertEqual(session.query(query, "ArrowTable").column("s").to_pylist(), expected)

    def test_regular_grid_window_endpoint_oracle(self):
        import duckdb
        from datafusion import SessionContext
        from chdb.session import Session
        n = 65
        expected = [min(i + 1, 60) for i in range(n)]
        connection = duckdb.connect()
        connection.execute(f"CREATE TABLE tf AS SELECT TIMESTAMP '1970-01-01' + i * INTERVAL 1 SECOND AS ts, i::DOUBLE AS price FROM range({n}) t(i)")
        values = [x[0] for x in connection.sql("SELECT count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 59 SECONDS PRECEDING AND CURRENT ROW) FROM tf ORDER BY ts").fetchall()]
        self.assertEqual(values, expected)
        with Session() as session:
            session.query(f"CREATE TABLE tf ENGINE=Memory AS SELECT toDateTime64(number, 0) ts, toFloat64(number) price FROM numbers({n})")
            values = session.query("SELECT count(*) OVER (ORDER BY ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) FROM tf", "ArrowTable").column(0).to_pylist()
            self.assertEqual(values, expected)
        frame = pl.DataFrame({"ts": pl.datetime_range(pl.datetime(1970, 1, 1), pl.datetime(1970, 1, 1, 0, 1, 4), interval="1s", eager=True), "price": list(map(float, range(n)))})
        context = SessionContext()
        context.register_record_batches("tf", [frame.to_arrow().to_batches()])
        batches = context.sql("SELECT count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '59 seconds' PRECEDING AND CURRENT ROW) AS c FROM tf").collect()
        self.assertEqual([x.as_py() for batch in batches for x in batch.column("c")], expected)

    def test_bounded_ewma_matches_ibex_definition(self):
        # Ibex restarts the recursion at the left edge of each trailing window.
        values = [10.0, 20.0, 30.0, 40.0]
        expected = [10.0, 15.0, 25.0, 35.0]
        frame = pl.DataFrame({"price": values})
        polars = frame.lazy().with_columns(
            pl.col("price").rolling_map(
                lambda series: bp._windowed_ewma(series, alpha=0.5),
                window_size=2, min_samples=1).alias("e")).collect()
        self.assertEqual(polars["e"].to_list(), expected)
        pandas = frame.to_pandas()
        self.assertEqual(
            pandas["price"].rolling(2, min_periods=1).apply(
                lambda series: bp._windowed_ewma(series, alpha=0.5), raw=True).to_list(),
            expected)

    def test_debug_build_is_rejected(self):
        import os
        import subprocess
        with tempfile.TemporaryDirectory() as directory:
            pathlib.Path(directory, "CMakeCache.txt").write_text("CMAKE_BUILD_TYPE:STRING=Debug\n")
            wrapper = pathlib.Path(__file__).with_name("bench_ibex.sh")
            result = subprocess.run(["bash", str(wrapper)], env=dict(os.environ, BUILD_DIR=directory),
                                    capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must be configured as Release", result.stderr)

    def test_native_sink_is_disposed_between_iterations(self):
        from chdb.session import Session
        with patch.dict("os.environ", {"CHDB_RESULT_MODE": "native"}), Session() as session:
            def query():
                exists = int(session.query("EXISTS TABLE _bench_sink", "TabSeparated").data().strip())
                self.assertEqual(exists, 0)
                return bc._materialize(session, "SELECT 1 AS i")
            result = bc.timer(query, 2, 3)[-1]
            self.assertEqual(result.num_rows, 1)
            bc.release_result(result)

    def test_core_results_across_columnar_engines(self):
        import duckdb
        import pyarrow as pa
        from chdb.session import Session
        from datafusion import SessionContext, SessionConfig
        selected = {"mean_by_symbol", "update_price_x2", "distinct_symbol",
                    "sort_price", "order_head_topk", "order_tail_topk", "filter_simple"}
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            prices = pl.DataFrame({"symbol": ["A", "B", "C"] * 240,
                                   "price": np.arange(720, dtype=float) + .25})
            prices.write_csv(root / "prices.csv")
            prices.with_columns(pl.lit(3).alias("qty")).write_csv(root / "trades.csv")
            def capture(module, function, *extra):
                outputs = []
                def evaluate(fn, warmup, iters):
                    result = fn()
                    if isinstance(result, pl.LazyFrame):
                        result = result.collect()
                    converted = result.to_arrow_table() if isinstance(result, bc.NativeResult) else result
                    frame = converted if isinstance(converted, pl.DataFrame) else pl.from_arrow(
                        pa.Table.from_batches(converted) if isinstance(converted, list) else converted)
                    outputs.append(frame)
                    return (0., 0., 0., 0., 0., 0., result)
                with patch.object(module, "timer", evaluate), patch.object(
                        module, "should_skip", lambda fw, name: name not in selected), contextlib.redirect_stderr(io.StringIO()):
                    rows = function(str(root / "prices.csv"), None, str(root / "trades.csv"), 0, 1, *extra)
                return dict(zip([r[1] for r in rows if r[1] in selected], outputs))
            reference = capture(bp, bp.bench_polars)
            # Exercise Ibex's actual interpreter and CSV result values, not
            # merely the row-count sink used for timing.
            import subprocess
            executable = pathlib.Path(__file__).resolve().parents[1] / "build-release/tools/ibex_eval"
            if not executable.exists():
                self.skipTest("build-release/tools/ibex_eval is required for cross-engine verification")
            queries = {
                "mean_by_symbol": "prices[select {avg_price = mean(price)}, by symbol]",
                "update_price_x2": "prices[update {price_x2 = price * 2}]",
                "distinct_symbol": "prices[distinct { symbol }]",
                "sort_price": "prices[order price]",
                "order_head_topk": "prices[order price desc, head 100]",
                "order_tail_topk": "prices[order price desc, tail 100]",
                "filter_simple": "trades[filter price > 500.0]",
            }
            script = ['import "csv";',
                      f'let prices = read_csv("{root / "prices.csv"}");',
                      f'let trades = read_csv("{root / "trades.csv"}");']
            for name, query in queries.items():
                script.append(f'write_csv({query}, "{root / (name + ".csv")}");')
            (root / "check.ibex").write_text("\n".join(script))
            subprocess.run([str(executable), "--plugin-path", str(executable.parent),
                            str(root / "check.ibex")], check=True, capture_output=True, text=True)
            ibex_results = {name: pl.read_csv(root / (name + ".csv")) for name in queries}
            with duckdb.connect() as connection, Session() as session:
                connection.execute("SET threads=2")
                session.query("SET max_threads=2")
                comparisons = [
                    ibex_results,
                    capture(bd, bd.bench_duckdb_core, connection),
                    capture(bc, bc.bench_clickhouse_core, session),
                    capture(bf, bf.bench_datafusion_core,
                            SessionContext(SessionConfig().with_target_partitions(2))),
                ]
            for index, outputs in enumerate(comparisons):
                self.assertEqual(outputs.keys(), reference.keys())
                for name, result in outputs.items():
                    with self.subTest(engine=index, query=name):
                        expected = reference[name]
                        self.assertEqual(set(result.columns), set(expected.columns))
                        if name in ("sort_price", "order_head_topk", "order_tail_topk"):
                            self.assertEqual(result["price"].to_list(), expected["price"].to_list())
                        assert_frame_equal(expected.sort(expected.columns),
                                           result.select(expected.columns).sort(expected.columns),
                                           check_dtypes=False, check_exact=False,
                                           rel_tol=1e-9, abs_tol=1e-10)

    def test_previous_result_released_before_next_call(self):
        import weakref
        class Result:
            pass
        for module in (bp, bd, bc, bf):
            previous = []
            def query():
                self.assertTrue(all(r() is None for r in previous))
                result = Result()
                previous.append(weakref.ref(result))
                return result
            with self.subTest(module=module.__name__):
                result = module.timer(query, 2, 3)[-1]
                del result

    def test_polars_resident_optimizer_preserves_core_results(self):
        # Compare actual harness expressions with and without optimization,
        # including tied prices, multiple groups and temporal boundaries.
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            n = 720
            prices = pl.DataFrame({
                "symbol": ["A", "B", "C"] * (n // 3),
                "price": (np.arange(n) % 113).astype(float) + 1,
            })
            prices.write_csv(root / "prices.csv")
            prices.with_columns(pl.Series("day", ["Mon", "Tue"] * (n // 2))).write_csv(root / "multi.csv")
            prices.with_columns(pl.Series("qty", np.arange(n) % 13 + 1)).write_csv(root / "trades.csv")
            prices.with_columns(pl.Series("ts", np.arange(n) * 1_000_000_000)).write_csv(root / "prices_ts.csv")

            def capture():
                outputs = []
                def evaluate(fn, warmup, iters):
                    result = fn()
                    self.assertIsInstance(result, pl.LazyFrame)
                    result = result.collect()
                    outputs.append(result)
                    return (0., 0., 0., 0., 0., 0., result)
                with contextlib.ExitStack() as stack:
                    stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
                    stack.enter_context(patch.object(bp, "timer", evaluate))
                    stack.enter_context(patch.object(bp, "should_skip", lambda fw, name: name.startswith("rand_")))
                    rows = bp.bench_polars(str(root / "prices.csv"), str(root / "multi.csv"),
                                           str(root / "trades.csv"), 0, 1)
                    rows += bp.bench_polars_tf(n, 0, 1)
                    rows += bp.bench_polars_fill(n, 0, 1)
                    rows += bp.bench_polars_asof(n, 0, 1)
                names = [row[1] for row in rows if not row[1].startswith("rand_")]
                return dict(zip(names, outputs))
            # Do not monkeypatch DataFrame.lazy: Polars eager methods themselves
            # use it internally. Disable optimizer flags for the reference instead.
            original_collect = pl.LazyFrame.collect
            with patch.object(pl.LazyFrame, "collect",
                              lambda frame, *args, **kwargs: original_collect(
                                  frame, *args, **dict(kwargs, optimizations=pl.QueryOptFlags.none()))):
                reference = capture()
            optimized = capture()
            self.assertEqual(reference.keys(), optimized.keys())
            for name in reference:
                with self.subTest(query=name):
                    # Group output order and tied sort order are unspecified here.
                    a, b = reference[name], optimized[name]
                    if name in ("order_head_topk", "order_tail_topk"):
                        # No secondary ordering key is specified: boundary ties
                        # may select different symbols, but not different prices.
                        self.assertEqual(a["price"].to_list(), b["price"].to_list())
                        self.assertEqual(b.join(prices.unique(), on=prices.columns, how="anti").height, 0)
                        continue
                    assert_frame_equal(a.sort(a.columns), b.sort(b.columns),
                                       check_exact=False, rel_tol=1e-9, abs_tol=1e-10)

    def test_arrow_outputs_are_complete(self):
        import duckdb
        from chdb.session import Session
        sql = "SELECT number AS i, 'text' AS s FROM numbers(257)"
        with Session() as session:
            result = bc._materialize(session, sql)
            table = result.to_arrow_table() if isinstance(result, bc.NativeResult) else result
            self.assertEqual(table.num_rows, 257)
            self.assertEqual(table.column("i").to_pylist(), list(range(257)))
            bc.release_result(result)
        with duckdb.connect() as connection:
            table = connection.sql("SELECT range AS i, 'text' AS s FROM range(257)").to_arrow_table()
            self.assertEqual(table.num_rows, 257)
            self.assertEqual(table.column("i").to_pylist(), list(range(257)))


if __name__ == "__main__":
    unittest.main()
