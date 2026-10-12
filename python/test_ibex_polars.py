# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine conformance, strict failures, caching and ownership tests."""

from datetime import date
import gc
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import weakref

import polars as pl
from polars.testing import assert_frame_equal
import pyarrow as pa
import pyarrow.parquet as pq

import ibex_pyarrow
from ibex_polars import IbexEngine
from ibex_polars._adapter import Adapter
from ibex_polars._engine import _to_polars


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = IbexEngine()

    def equal(self, query, *, options=None):
        flags = options if options is not None else pl.QueryOptFlags()
        expected = query.collect(engine="streaming", optimizations=flags)
        actual = query.collect(engine=self.engine, optimizations=flags)
        assert_frame_equal(actual, expected, rel_tol=1e-12, abs_tol=1e-9)
        return actual

    def test_grouped_mean_counts_null_keys_and_output_types(self):
        query = pl.DataFrame({"g": ["b", None, "a", "b", None], "h": [1, 1, 2, 1, 1],
                              "x": [2, None, 7, 4, None]}).lazy().group_by("g", "h").agg(
            total=pl.sum("x"), average=pl.mean("x"), n=pl.len()
        ).sort("g", "h")
        self.equal(query)

    def test_q06_shape_dates_nulls_and_boundaries(self):
        query = (
            pl.DataFrame({"day": [date(1994, 1, 1), date(1995, 1, 1), None, date(1994, 6, 1)],
                          "discount": [0.05, 0.06, 0.07, 0.07], "price": [100.0, 999.0, 999.0, 200.0],
                          "qty": [1, 1, 1, 23]}).lazy()
            .filter(pl.col("day").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
            .filter(pl.col("discount").is_between(0.05, 0.07), pl.col("qty") < 24)
            .with_columns((pl.col("price") * pl.col("discount")).alias("revenue"))
            .select(pl.sum("revenue"))
        )
        self.assertEqual(self.equal(query).item(), 19.0)

    def test_empty_and_all_null_reductions(self):
        for values in ([], [None, None]):
            frame = pl.DataFrame({"g": pl.Series([], dtype=pl.String) if not values else ["a", "a"],
                                  "x": pl.Series(values, dtype=pl.Float64)})
            self.equal(frame.lazy().select(total=pl.sum("x"), mean=pl.mean("x"), n=pl.len()))
            self.equal(frame.lazy().group_by("g").agg(total=pl.sum("x"), mean=pl.mean("x"), n=pl.len()))

    def test_simultaneous_updates_and_numeric_coercion(self):
        frame = pl.DataFrame({"x": [1, 2], "y": [10, 20], "price": [0.5, 1.0]}).lazy()
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            self.equal(frame.with_columns(x=pl.col("y"), y=pl.col("x") + 1), options=flags)
            self.equal(frame.select(v=pl.col("x") * pl.col("price")), options=flags)

    def test_sort_directions_null_placement_and_stable_ties(self):
        frame = pl.DataFrame({"g": [None, "a", "a", "b", None], "h": [1, 2, 1, 0, 2],
                              "_ibex_sort_null_0_0": [10, 11, 12, 13, 14]}).lazy()
        self.equal(frame.sort(["g", "h"], descending=[True, False], nulls_last=[False, True], maintain_order=True))
        self.equal(frame.sort("g", nulls_last=True))

    def test_categorical_input_codes_are_retained(self):
        frame = pl.DataFrame({"g": ["a", "b"] * 500, "day": ["Mon", "Tue"] * 500,
                              "x": [float(i) for i in range(1000)]}).with_columns(
            pl.col("g", "day").cast(pl.Categorical))
        query = frame.lazy().group_by("g", "day").agg(avg=pl.mean("x"), n=pl.len()).sort("g")
        self.engine.prepare(query)
        _, tables, *_ = self.engine._prepare(query, pl.QueryOptFlags())
        before = ibex_pyarrow.execute_pipeline("input;", tables={"input": tables["_ibex_input"]})
        result = self.equal(query)
        self.assertEqual(result.schema["g"], frame.schema["g"])
        after = ibex_pyarrow.execute_pipeline("input;", tables={"input": tables["_ibex_input"]})
        for name in ("g", "day", "x"):
            self.assertEqual(before[name].chunk(0).buffers()[1].address,
                             after[name].chunk(0).buffers()[1].address)
        self.assertEqual(after["x"].chunk(0).buffers()[1].address,
                         frame.to_arrow()["x"].chunk(0).buffers()[1].address)

    def test_nan_comparisons_and_sort_order(self):
        frame = pl.DataFrame({"x": [float("nan"), None, 1., 2., float("inf"), -float("inf")],
                              "y": [float("nan"), 1., None, float("nan"), float("inf"), -float("inf")]}).lazy()
        for operator in ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"):
            self.equal(frame.select(getattr(pl.col("x"), operator)(pl.col("y")).alias("test")))
        for descending in (False, True):
            for last in (False, True):
                self.equal(frame.sort("x", descending=descending, nulls_last=last, maintain_order=True))

    def test_categorical_output_preserves_arrow_dictionary(self):
        # Exercise sliced/chunked codes, nulls and a dictionary whose order
        # differs from Polars' global category mapping. No native query is
        # needed to isolate the output boundary's representation contract.
        chunks = [pa.DictionaryArray.from_arrays(pa.array([0, 2, None, 1], type=pa.int32()),
                                                pa.array(["z", "a", "b"])).slice(1),
                  pa.DictionaryArray.from_arrays(pa.array([1, None, 0], type=pa.int32()),
                                                pa.array(["b", "z", "a"]))]
        table = pa.table({"g": pa.chunked_array(chunks), "x": [1., 2., 3., 4., 5., 6.]})
        output = {"g": pa.large_string(), "x": pa.float64()}
        for dtype in (pl.Categorical(), pl.Categorical(pl.Categories("ibex_output_test"))):
            expected = pl.Schema({"g": dtype, "x": pl.Float64})
            with patch.object(pl, "from_arrow", wraps=pl.from_arrow) as convert:
                result = _to_polars(table, expected, output)
                passed = convert.call_args.args[0]
                self.assertTrue(pa.types.is_dictionary(passed["g"].type))
                for before, after in zip(table["g"].chunks, passed["g"].chunks):
                    self.assertEqual(before.buffers()[1].address, after.buffers()[1].address)
            self.assertEqual(result.schema, expected)
            self.assertEqual(result["g"].to_list(), ["b", None, "a", "z", None, "b"])
            self.assertEqual(result["x"].to_list(), table["x"].to_pylist())
        empty = table.slice(0, 0)
        result = _to_polars(empty, expected, output)
        self.assertEqual(result.schema, expected)
        self.assertEqual(result.height, 0)
        string_schema = pl.Schema({"g": pl.String, "x": pl.Float64})
        self.assertEqual(_to_polars(table, string_schema, output).schema, string_schema)

    def test_categorical_output_from_native_projection_and_grouping(self):
        for dtype in (pl.Categorical, pl.Categorical(pl.Categories("ibex_pipeline_test"))):
            frame = pl.DataFrame({"g": ["z", None, "a", "z"], "x": [1., 2., 3., 4.]}).with_columns(
                pl.col("g").cast(dtype))
            query = frame.lazy()
            reference = query.collect(engine="streaming")
            with patch.object(pl, "from_arrow", wraps=pl.from_arrow) as convert:
                result = query.collect(engine=self.engine)
                self.assertTrue(pa.types.is_dictionary(convert.call_args.args[0]["g"].type))
            assert_frame_equal(result, reference)
            self.equal(frame.lazy().group_by("g").agg(mean=pl.mean("x")).sort("g"))

    def test_prepare_projects_demanded_columns_without_query_collection(self):
        query = pl.DataFrame({"x": [1, 2], "unused": [[1], [2]]}).lazy().select(pl.sum("x"))
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            with patch.object(pl.LazyFrame, "collect", side_effect=AssertionError("reference query executed")):
                self.engine.prepare(query)
            execute.assert_not_called()
            with patch.object(pl.LazyFrame, "collect", side_effect=AssertionError("reference query executed")):
                self.assertEqual(self.engine.collect(query, optimizations=pl.QueryOptFlags()).item(), 3)
            source, tables, *_ = self.engine._prepare(query, pl.QueryOptFlags())
            native = ibex_pyarrow.execute_pipeline("input;", tables={"input": tables["_ibex_input"]})
            self.assertEqual(native.column_names, ["x"])
        self.assertEqual(query.collect(engine=self.engine).item(), 3)

    def test_repeated_collection_reuses_input_but_executes_each_time(self):
        query = pl.DataFrame({"x": [1, 2]}).lazy().select(pl.sum("x"))
        with patch.object(ibex_pyarrow, "bind_table", wraps=ibex_pyarrow.bind_table) as bind:
            with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
                for _ in range(3):
                    self.assertEqual(query.collect(engine=self.engine).item(), 3)
                self.assertEqual(bind.call_count, 1)
                self.assertEqual(execute.call_count, 3)

    def test_cache_flags_eviction_and_weak_lifetime(self):
        query = pl.DataFrame({"x": [1, 2]}).lazy().select(pl.sum("x"))
        self.engine.prepare(query)
        self.engine.prepare(query, optimizations=pl.QueryOptFlags.none())
        self.assertEqual(len(self.engine._cache), 2)
        owner = weakref.ref(query)
        del query
        gc.collect()
        self.assertIsNone(owner())
        self.assertEqual(len(self.engine._cache), 0)
        engine = IbexEngine(cache_size=1)
        a = pl.DataFrame({"x": [1]}).lazy()
        b = pl.DataFrame({"x": [2]}).lazy()
        engine.prepare(a).prepare(b)
        self.assertEqual(len(engine._cache), 1)

    def test_execute_result_and_affinity(self):
        query = pl.DataFrame({"x": [1, 2]}).lazy().select(pl.sum("x"))
        executed = query.execute(engine=self.engine)
        self.assertEqual(executed.n_rows_total, 1)
        self.assertEqual(executed.lazy().collect(engine="streaming").item(), 3)
        try:
            pl.Config.set_engine_affinity(self.engine)
            self.assertEqual(query.collect().item(), 3)
        finally:
            pl.Config.set_engine_affinity(None)

    def test_version_and_unsupported_plans_fail_without_native_execution(self):
        base = pl.DataFrame({"x": [1.0, 2.0]}).lazy()
        with patch.object(ibex_pyarrow, "bind_table") as bind, patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            for query in (base.select(pl.col("x").median()), base.join(base, on="x"),
                          base.with_columns(pl.col("x").mean().alias("average")),
                          base.group_by("x").agg(pl.len()).head(1)):
                with self.assertRaises(NotImplementedError):
                    query.collect(engine=self.engine)
            with patch.object(pl, "__version__", "9.0.0"):
                with self.assertRaises(NotImplementedError):
                    base.collect(engine=self.engine)
            with patch("ibex_polars._adapter.VISITOR_VERSION", (-1, -1)):
                with self.assertRaises(NotImplementedError):
                    base.collect(engine=self.engine)
            with self.assertRaises(NotImplementedError):
                base.collect(engine=self.engine, background=True)
            with self.assertRaises(NotImplementedError):
                self.engine.collect(base, optimizations=pl.QueryOptFlags(), post_opt_callback=lambda *a: None)
            bind.assert_not_called()
            execute.assert_not_called()

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
    def test_parquet_plan_cache_rereads_data_and_rejects_scan_options(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input.parquet"
            pq.write_table(pa.table({"x": [1.0, 2.0]}), path)
            query = pl.scan_parquet(path).select(pl.sum("x"))
            self.assertEqual(query.collect(engine=self.engine).item(), 3.0)
            pq.write_table(pa.table({"x": [3.0, 4.0]}), path)
            self.assertEqual(query.collect(engine=self.engine).item(), 7.0)
            with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
                with self.assertRaises(NotImplementedError):
                    pl.scan_parquet(path, n_rows=1).collect(engine=self.engine)
                execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
