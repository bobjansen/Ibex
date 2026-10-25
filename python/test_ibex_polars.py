# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine conformance, strict failures, caching and ownership tests."""

from datetime import date, datetime, timedelta
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

    def test_string_predicates_normalize_arrow_string_width(self):
        frame = pl.DataFrame({"s": ["ASIA", "EUROPE", None, ""]})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "strings.parquet"
            # A utf8 Parquet schema and large_utf8 resident boundary represent
            # the same logical string type.
            pq.write_table(pa.table({"s": pa.array(frame["s"].to_list(), type=pa.string())}), path)
            for source in (frame.lazy(), pl.scan_parquet(path)):
                self.equal(source.filter(pl.col("s") == "ASIA"))
                self.equal(source.select(matched=pl.col("s") != "EUROPE"))

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

    def test_int32_uses_int64_storage_with_wrapping_at_expression_boundaries(self):
        frame = pl.DataFrame({
            "x": pl.Series([2**31 - 1, -(2**31), -1, 0, None], dtype=pl.Int32),
            "y": pl.Series([2, -1, -(2**31), 2**31 - 1, 1], dtype=pl.Int32),
            "wide": [1, 2, 3, 4, 5],
        }).lazy()
        queries = [
            frame,
            frame.select(add=pl.col("x") + 1, sub=pl.col("x") - 1, mul=pl.col("x") * 2),
            frame.select(nested=(pl.col("x") + pl.col("y")) * pl.col("y")),
            frame.select(negative=-pl.col("x")),
            frame.select(test=(pl.col("x") + 1) > 0, ratio=(pl.col("x") + 1) / 2),
            frame.filter((pl.col("x") + 1) > 0),
            frame.select(wide=pl.col("x") + pl.col("wide"),
                         converted=(pl.col("x") + 1).cast(pl.Int64)),
            frame.with_columns(x=pl.col("x") + 1).with_columns(x=pl.col("x").cast(pl.Int64))
                 .select(x=pl.col("x") + 1),
            frame.sort("x"),
        ]
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            for query in queries:
                with self.subTest(query=query.explain(), flags=flags):
                    self.equal(query, options=flags)

        self.engine.prepare(frame)
        _, tables, *_ = self.engine._prepare(frame, pl.QueryOptFlags())
        native = ibex_pyarrow.execute_pipeline("input;", tables={"input": tables["_ibex_input"]})
        self.assertEqual(native.schema.field("x").type, pa.int64())

    def test_int32_grouping_reductions_and_empty_inputs(self):
        for values in ([2**31 - 1, 1, None], [-(2**31), -1, None], [], [None, None]):
            frame = pl.DataFrame({"x": pl.Series(values, dtype=pl.Int32),
                                  "g": pl.Series(["a"] * len(values), dtype=pl.String)}).lazy()
            for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                self.equal(frame.select(total=pl.col("x").sum(), avg=pl.col("x").mean()), options=flags)
                self.equal(frame.select(ratio=pl.col("x").sum() / 2), options=flags)
                self.equal(frame.group_by("g", maintain_order=True).agg(
                    total=pl.col("x").sum(), avg=pl.col("x").mean()), options=flags)
                self.equal(frame.group_by("x", maintain_order=True).agg(pl.len()), options=flags)

    def test_int32_explicit_wrapping_cast(self):
        frame = pl.LazyFrame({"x": [2**63 - 1, -(2**63), 2**31, -(2**31) - 1, None]})
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            self.equal(frame.select(pl.col("x").cast(pl.Int32, wrap_numerical=True)), options=flags)
        # Checked and null-producing narrowing casts are separate semantics.
        with self.assertRaises(NotImplementedError):
            frame.select(pl.col("x").cast(pl.Int32)).collect(engine=self.engine)

    def test_small_integer_arithmetic_wraps_and_reductions_promote(self):
        for dtype, bits, signed in ((pl.Int8, 8, True), (pl.Int16, 16, True),
                                    (pl.UInt8, 8, False), (pl.UInt16, 16, False)):
            high = 2**(bits - int(signed)) - 1
            low = -(2**(bits - 1)) if signed else 0
            frame = pl.LazyFrame({"x": pl.Series([low, high, 1, 0, None], dtype=dtype),
                                  "y": pl.Series([high, high, high, 1, 1], dtype=dtype),
                                  "g": ["a"] * 5})
            queries = [frame, frame.select(add=pl.col("x") + 1, sub=pl.col("x") - 1,
                                           mul=pl.col("x") * pl.col("y")),
                       frame.select(nested=(pl.col("x") + 1) * pl.col("y")),
                       frame.select(test=(pl.col("x") + 1) > 0, ratio=(pl.col("x") + 1) / 2),
                       frame.filter((pl.col("x") + 1) > 0), frame.sort("x"),
                       frame.select(wide=(pl.col("x") + 1).cast(pl.Int64)),
                       frame.group_by("x", maintain_order=True).agg(pl.len()),
                       frame.group_by("g").agg(total=pl.col("x").sum(), avg=pl.col("x").mean())]
            if signed:
                queries.append(frame.select(negative=-pl.col("x")))
            for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                for query in queries:
                    with self.subTest(dtype=dtype, query=query.explain(), flags=flags):
                        self.equal(query, options=flags)
                promoted = pl.LazyFrame({"x": pl.Series([high, high, None], dtype=dtype)})
                result = self.equal(promoted.select(pl.col("x").sum()), options=flags)
                self.assertEqual(result.item(), high * 2)
                self.assertEqual(result.schema, {"x": pl.Int64})
            for values in ([], [None, None]):
                empty = pl.LazyFrame({"x": pl.Series(values, dtype=dtype)})
                self.equal(empty)
                self.equal(empty.select(total=pl.col("x").sum(), avg=pl.col("x").mean()))

    def test_small_integer_wrapping_and_safe_widening_casts(self):
        frame = pl.LazyFrame({"x": [-(2**63), -65537, -129, -1, 0, 128, 65536, 2**63 - 1, None]})
        for dtype in (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16):
            for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                self.equal(frame.select(pl.col("x").cast(dtype, wrap_numerical=True)), options=flags)
            with self.assertRaises(NotImplementedError):
                frame.select(pl.col("x").cast(dtype)).collect(engine=self.engine)
        for source, target in ((pl.Int8, pl.Int16), (pl.UInt8, pl.Int16),
                               (pl.UInt16, pl.Int32), (pl.Int16, pl.Float64)):
            query = pl.LazyFrame({"x": pl.Series([0, 1, None], dtype=source)}).select(pl.col("x").cast(target))
            self.equal(query)
        for dtype in (pl.UInt8, pl.UInt16):
            with self.assertRaises(NotImplementedError):
                pl.LazyFrame({"x": pl.Series([1], dtype=dtype)}).select(-pl.col("x")).collect(engine=self.engine)

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
    def test_small_integer_parquet_and_wide_unsigned_rejections(self):
        with tempfile.TemporaryDirectory() as directory:
            for dtype, arrow_type in ((pl.Int8, pa.int8()), (pl.Int16, pa.int16()),
                                      (pl.UInt8, pa.uint8()), (pl.UInt16, pa.uint16())):
                path = Path(directory) / f"{dtype}.parquet"
                pq.write_table(pa.table({"x": pa.array([0, 1, None], type=arrow_type)}), path)
                frame = pl.scan_parquet(path)
                self.equal(frame)
                self.equal(frame.select(value=pl.col("x") - 1))
                self.equal(frame.select(total=pl.col("x").sum()))
            for dtype in (pl.UInt32, pl.UInt64):
                data = pl.DataFrame({"x": pl.Series([0, 1, None], dtype=dtype)})
                path = Path(directory) / f"{dtype}.parquet"
                data.write_parquet(path)
                for query in (data.lazy(), pl.scan_parquet(path)):
                    with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
                        with self.assertRaises(NotImplementedError):
                            query.collect(engine=self.engine)
                        execute.assert_not_called()

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
    def test_int32_parquet_predicate_and_output_types(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "int32.parquet"
            pq.write_table(pa.table({"x": pa.array([2**31 - 1, 1, None], type=pa.int32()),
                                     "y": [10, 20, 30]}), path)
            frame = pl.scan_parquet(path)
            self.equal(frame.select("x"))
            self.equal(frame.filter((pl.col("x") + 1) < 0).select("y"))
            self.equal(frame.select(total=pl.col("x").sum()))

    def test_float32_uses_float64_storage_and_preserves_output_types(self):
        frame = pl.DataFrame({"x": pl.Series([1.5, -2.0, None, float("nan"), float("inf")], dtype=pl.Float32),
                              "wide": [1.0, 2.0, 3.0, 4.0, 5.0]}).lazy()
        queries = [frame, frame.select(value=pl.col("x") * 2.0),
                   frame.select(test=pl.col("x") > 0), frame.filter(pl.col("x") > 0),
                   frame.select(value=pl.col("x") + pl.col("wide")),
                   frame.select(pl.col("x").cast(pl.Float64)), frame.sort("x")]
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            for query in queries:
                self.equal(query, options=flags)
        self.engine.prepare(frame)
        _, tables, *_ = self.engine._prepare(frame, pl.QueryOptFlags())
        native = ibex_pyarrow.execute_pipeline("input;", tables={"input": tables["_ibex_input"]})
        self.assertEqual(native.schema.field("x").type, pa.float64())
        with self.assertRaisesRegex(NotImplementedError, "floating-point grouping keys"):
            frame.group_by("x").agg(pl.len()).collect(engine=self.engine)

    def test_float32_rounds_only_at_export(self):
        frame = pl.DataFrame({"x": pl.Series([float(2**24), None], dtype=pl.Float32)}).lazy()
        staged = frame.with_columns(y=pl.col("x") + 1)
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            # Polars rounds y to Float32 inside the pipeline; our agreed mode
            # retains the Float64 intermediate and rounds only public outputs.
            query = staged.select(delta=pl.col("y") - pl.col("x"))
            self.assertEqual(query.collect(optimizations=flags)["delta"].to_list(), [0.0, None])
            result = query.collect(engine=self.engine, optimizations=flags)
            self.assertEqual(result["delta"].to_list(), [1.0, None])
            self.assertEqual(result.schema, {"delta": pl.Float32})
            exported = staged.select("y").collect(engine=self.engine, optimizations=flags)
            self.assertEqual(exported["y"].to_list(), [float(2**24), None])
            selected = staged.filter(pl.col("y") > pl.col("x")).collect(engine=self.engine, optimizations=flags)
            self.assertEqual(selected.height, 1)
            large = pl.LazyFrame({"x": pl.Series([float(2**127)], dtype=pl.Float32)})
            overflow = large.with_columns(y=pl.col("x") * 4).select(value=pl.col("y") / 4)
            self.assertEqual(overflow.collect(optimizations=flags).item(), float("inf"))
            self.assertEqual(overflow.collect(engine=self.engine, optimizations=flags).item(), float(2**127))

    def test_float32_reductions_and_empty_inputs(self):
        for values in ([1.0, 2.0, None], [], [None, None]):
            frame = pl.DataFrame({"x": pl.Series(values, dtype=pl.Float32),
                                  "g": pl.Series(["a"] * len(values), dtype=pl.String)}).lazy()
            self.equal(frame.select(total=pl.col("x").sum(), avg=pl.col("x").mean()))
            self.equal(frame.group_by("g", maintain_order=True).agg(
                total=pl.col("x").sum(), avg=pl.col("x").mean()))
        cancellation = pl.LazyFrame({"x": pl.Series([float(2**24), 1.0, -float(2**24)], dtype=pl.Float32)})
        result = cancellation.select(pl.col("x").sum()).collect(engine=self.engine)
        self.assertEqual(result.item(), 1.0)
        self.assertEqual(result.schema, {"x": pl.Float32})

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
    def test_float32_parquet(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "float32.parquet"
            pq.write_table(pa.table({"x": pa.array([1.0, 2.0, None], type=pa.float32())}), path)
            frame = pl.scan_parquet(path)
            self.equal(frame)
            self.equal(frame.select(value=pl.col("x") * 2.0))
            self.equal(frame.select(total=pl.col("x").sum(), avg=pl.col("x").mean()))

    def test_select_reads_original_columns_before_assigning_aliases(self):
        frame = pl.DataFrame({"x": [1, 2], "y": [10, 20],
                              "_ibex_select_0_0": [100, 200]}).lazy()
        projections = [
            [pl.col("y").alias("x"), pl.col("x").alias("y")],
            [(pl.col("y") + 1).alias("x"), (pl.col("x") + 1).alias("y")],
            [pl.col("y").alias("x"), (pl.col("x") + pl.col("_ibex_select_0_0")).alias("y")],
            [pl.col("y").alias("_ibex_select_0_0"), pl.col("_ibex_select_0_0").alias("y")],
            [pl.sum("y").alias("x"), pl.sum("x").alias("y")],
        ]
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            for expressions in projections:
                with self.subTest(expressions=expressions, flags=flags):
                    self.equal(frame.select(expressions), options=flags)

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

    def test_nan_high_comparison_matrix_resident_and_parquet(self):
        values = [-float("inf"), -1., -0., 0., 1., float("inf"), float("nan"), None]
        frame = pl.DataFrame({"id": range(64), "x": [x for x in values for _ in values],
                              "y": values * len(values)})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nan-groups.parquet"
            # Includes all-NaN/all-null row groups and mixed NaN/finite groups.
            pq.write_table(frame.to_arrow(), path, row_group_size=8, write_statistics=True)
            for source in (frame.lazy(), pl.scan_parquet(path)):
                for name in ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"):
                    for right in (pl.col("y"), pl.lit(0.), pl.lit(float("nan")),
                                  pl.lit(float("inf")), pl.lit(None, dtype=pl.Float64)):
                        for left, rhs in ((pl.col("x"), right), (right, pl.col("x"))):
                            predicate = getattr(left, name)(rhs)
                            with self.subTest(source=type(source), comparison=name, expression=predicate):
                                self.equal(source.select(predicate.alias("result")))
                                self.equal(source.filter(predicate).select("id"))
                                self.equal(source.filter(predicate | pl.col("x").is_null()).select("id"))
                # Exercise conjunctive scan filtering, not only projected masks.
                query = source.filter((pl.col("x") >= 0.) & (pl.col("x") <= 1.)).select("id")
                self.equal(query)
                native = self.engine.explain(query)
                self.assertIn("__ibex_nan_high_ge", native)
                self.assertNotIn("case {", native)

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
        for dtype in (pl.Categorical(), pl.Categorical(pl.Categories("ibex_output_test")),
                      pl.Categorical(pl.Categories("ibex_output_u8", physical=pl.UInt8)),
                      pl.Categorical(pl.Categories("ibex_output_u16", physical=pl.UInt16))):
            expected = pl.Schema({"g": dtype, "x": pl.Float64})
            with patch.object(pl, "from_arrow", wraps=pl.from_arrow) as convert:
                result = _to_polars(table, expected, output)
                # Never send a row-sized dictionary array through Polars'
                # dictionary importer: only labels and integer codes.
                self.assertFalse(any(pa.types.is_dictionary(call.args[0].type)
                                     for call in convert.call_args_list))
            self.assertEqual(result.schema, expected)
            self.assertEqual(result["g"].to_list(), ["b", None, "a", "z", None, "b"])
            self.assertEqual(result["x"].to_list(), table["x"].to_pylist())
        empty = table.slice(0, 0)
        result = _to_polars(empty, expected, output)
        self.assertEqual(result.schema, expected)
        self.assertEqual(result.height, 0)
        string_schema = pl.Schema({"g": pl.String, "x": pl.Float64})
        self.assertEqual(_to_polars(table, string_schema, output).schema, string_schema)

    def test_categorical_output_empty_chunks_and_null_dictionary(self):
        dtype = pl.Categorical(pl.Categories("ibex_null_dictionary"))
        expected = pl.Schema({"g": dtype})
        arrays = [pa.chunked_array([], type=pa.dictionary(pa.int32(), pa.string())),
                  pa.chunked_array([pa.DictionaryArray.from_arrays(
                      pa.array([0, None, 1], type=pa.int32()), pa.array([None, "a"]))])]
        for array, values in zip(arrays, ([], [None, None, "a"])):
            result = _to_polars(pa.table({"g": array}), expected, {"g": pa.large_string()})
            self.assertEqual(result.schema, expected)
            self.assertEqual(result["g"].to_list(), values)

    def test_categorical_output_from_native_projection_and_grouping(self):
        for dtype in (pl.Categorical, pl.Categorical(pl.Categories("ibex_pipeline_test"))):
            frame = pl.DataFrame({"g": ["z", None, "a", "z"], "x": [1., 2., 3., 4.]}).with_columns(
                pl.col("g").cast(dtype))
            query = frame.lazy()
            reference = query.collect(engine="streaming")
            with patch.object(pl, "from_arrow", wraps=pl.from_arrow) as convert:
                result = query.collect(engine=self.engine)
                self.assertFalse(any(isinstance(call.args[0], pa.Table) and
                                     pa.types.is_dictionary(call.args[0]["g"].type)
                                     for call in convert.call_args_list))
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

    def test_non_null_count_preserves_zero_and_uint32_across_types(self):
        for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
                      pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Categorical,
                      pl.Date, pl.Datetime("ns", "Europe/Amsterdam"), pl.Duration("us")):
            values = [None, 1, 2, None, None]
            if dtype in (pl.String, pl.Categorical):
                values = [None, "a", "b", None, None]
            elif dtype == pl.Boolean:
                values = [None, False, True, None, None]
            elif dtype in (pl.Float32, pl.Float64):
                values = [None, float("nan"), float("inf"), None, None]
            frame = pl.DataFrame({"g": ["a", "a", "a", None, None],
                                  "x": pl.Series(values, dtype=dtype)}).lazy()
            expressions = [pl.col("x").count().alias("present"),
                           pl.col("x").len().alias("length"), pl.len().alias("rows")]
            with self.subTest(dtype=dtype):
                for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                    result = self.equal(frame.select(expressions), options=flags)
                    self.assertEqual(result.to_dict(as_series=False), {"present": [2], "length": [5], "rows": [5]})
                    self.assertEqual(result.schema["present"], pl.UInt32)
                    self.equal(frame.group_by("g", maintain_order=True).agg(expressions), options=flags)
                    empty = frame.filter(pl.lit(False))
                    result = self.equal(empty.select(expressions), options=flags)
                    self.assertEqual(result.to_dict(as_series=False), {"present": [0], "length": [0], "rows": [0]})
                    self.equal(empty.group_by("g").agg(expressions), options=flags)

    def test_non_null_count_computed_inputs_and_shared_native_execution(self):
        import ibex
        frame = pl.LazyFrame({"g": ["a", "a", "b"], "x": [None, 2, 3]})
        expressions = [(pl.col("x") + 1).count().alias("computed"),
                       (pl.col("x") > 2).count().alias("booleans"),
                       pl.col("x").count().alias("x"),
                       (pl.col("x") + 1).len().alias("length")]
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            self.equal(frame.select(expressions), options=flags)
            self.equal(frame.group_by("g", maintain_order=True).agg(expressions), options=flags)
            self.equal(frame.filter(pl.lit(False)).select(expressions), options=flags)
        query = frame.group_by("g", maintain_order=True).agg(expressions)
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            query.collect(engine=self.engine)
            self.assertEqual(execute.call_count, 1)
        result = ibex.DataFrame({"x": [None, 2, 3]}).lazy().select(
            ibex.col("x").count().alias("present"),
            (ibex.col("x") + 1).count().alias("computed"),
            ibex.col("x").len().alias("length"), ibex.len().alias("rows")).collect()
        self.assertEqual(result.to_dict(as_series=False), {"present": [2], "computed": [2], "length": [3], "rows": [3]})
        self.assertEqual(result.schema["present"], pa.uint32())
        with self.assertRaises(NotImplementedError):
            ibex.col("x").count().count()

    def test_repeat_scalar_and_typed_null_fill_the_current_frame(self):
        cases = [(pl.Int8, -7), (pl.Int16, 1234), (pl.Int32, 70000), (pl.Int64, -(2**63)),
                 (pl.UInt8, 255), (pl.UInt16, 65535), (pl.Float32, 1.25),
                 (pl.Float64, float("nan")), (pl.Float64, float("inf")),
                 (pl.Boolean, False), (pl.Boolean, True),
                 (pl.String, '${untrusted}`"\\\ntext'), (pl.Date, date(2020, 1, 2)),
                 (pl.Datetime("us"), datetime(2020, 1, 2, 3, 4, 5)),
                 (pl.Duration("ns"), timedelta(microseconds=123))]
        cases += [(dtype, None) for dtype in (
            pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
            pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Categorical,
            pl.Date, pl.Datetime("us", "Europe/Amsterdam"), pl.Duration("ns"))]
        for dtype, value in cases:
            for empty in (False, True):
                source = pl.LazyFrame({"x": pl.Series([None, 1, 2], dtype=pl.Int64)})
                if empty:
                    source = source.filter(pl.lit(False))
                with self.subTest(dtype=dtype, value=value, empty=empty):
                    for length in (pl.len(), pl.col("x").len(), (pl.col("x") + 1).len()):
                        repeated = pl.repeat(pl.lit(value, dtype=dtype), length).alias("r")
                        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                            self.equal(source.select(repeated), options=flags)
                            self.equal(source.with_columns(repeated), options=flags)
        # Length must follow the current filtered frame, not the bound source.
        frame = pl.LazyFrame({"x": [None, 1, 2, 3]}).filter(pl.col("x") > 1)
        self.equal(frame.select(pl.repeat("ok", pl.col("x").len()).alias("r")))
        self.equal(frame.select(pl.repeat(pl.lit(date(2020, 1, 2)), pl.len()).alias("x"),
                               (pl.col("x") + 1).alias("r")))

    def test_repeat_translation_handles_optimizer_generated_constants(self):
        for dtype in (pl.Int8, pl.Int16, pl.UInt8, pl.UInt16):
            frame = pl.LazyFrame({"x": pl.Series([None, 0, 1], dtype=dtype)})
            query = frame.select(pl.col("x").eq_missing(2**32).alias("no"),
                                 pl.col("x").ne_missing(2**32).alias("yes"))
            self.assertIn("repeat", query.explain())
            with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
                result = self.equal(query)
                self.assertEqual(execute.call_count, 1)
            self.assertEqual(result.to_dict(as_series=False),
                             {"no": [False, False, False], "yes": [True, True, True]})
            self.equal(query.filter(pl.lit(False)))
            self.equal(frame.filter(pl.lit(False)).select(
                pl.col("x").eq_missing(2**32).alias("no"),
                pl.col("x").ne_missing(2**32).alias("yes")))

    def test_repeat_rejects_lengths_that_could_change_height_before_execution(self):
        source = pl.LazyFrame({"x": [None, 1, 2]})
        for length in (pl.lit(2), pl.lit(3), pl.col("x").count(), pl.len() - 1):
            query = source.select(pl.repeat(7, length).alias("r"))
            with self.subTest(length=length), patch.object(ibex_pyarrow, "execute_pipeline") as execute:
                with self.assertRaises(NotImplementedError):
                    query.collect(engine=IbexEngine(), optimizations=pl.QueryOptFlags.none())
                execute.assert_not_called()
        repeated = pl.repeat(7, pl.col("n").first())
        changed_height = (source.with_columns(pl.len().alias("n"))
                          .filter(pl.col("x") > 1).select(repeated.alias("r")))
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaises(NotImplementedError):
                changed_height.collect(engine=IbexEngine(), optimizations=pl.QueryOptFlags.none())
            execute.assert_not_called()

    def test_zero_column_scan_preserves_height_for_row_counts(self):
        for size in (0, 3):
            frame = pl.LazyFrame({"_ibex_height": pl.Series(range(size), dtype=pl.Int64)})
            self.equal(frame.select(pl.len()))
            self.equal(frame.select(pl.repeat(True, pl.len()).alias("_ibex_height")))

    def test_set_sorted_passes_through_without_reordering_or_extra_sort(self):
        for descending in (False, True):
            for nulls_last in (False, True):
                data = pl.DataFrame({"k": [None, 2, 1, 1], "x": [0, 1, 2, 3]}).sort(
                    "k", descending=descending, nulls_last=nulls_last)
                base = data.lazy()
                hinted = base.set_sorted("k", descending=descending, nulls_last=nulls_last)
                for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                    with self.subTest(descending=descending, nulls_last=nulls_last, flags=flags):
                        self.equal(hinted, options=flags)
                        self.assertEqual(self.engine.explain(hinted, optimizations=flags),
                                         self.engine.explain(base, optimizations=flags))
                        query = hinted.group_by("k", maintain_order=True).agg(
                            first=pl.col("x").first(), last=pl.col("x").last(), n=pl.col("x").count())
                        self.equal(query, options=flags)
        data = pl.DataFrame({"a": [1, 1, 2], "b": [3, 2, 1], "x": [None, 7, 8]})
        self.equal(data.lazy().set_sorted("a", "b", descending=[False, True]))
        empty = data.head(0).lazy().set_sorted("a")
        self.equal(empty)
        self.equal(empty.group_by("a").agg(pl.col("x").first()))
        query = data.lazy().sort("a").set_sorted("a").filter(pl.col("a") > 1).select("a", "x")
        self.equal(query)

    def test_set_sorted_does_not_accept_other_map_functions(self):
        query = pl.LazyFrame({"a": [1, 2], "b": [3, 4]}).unpivot()
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaisesRegex(NotImplementedError, "MapFunction"):
                query.collect(engine=IbexEngine())
            execute.assert_not_called()

    def test_unique_whole_rows_across_supported_types(self):
        for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
                      pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Categorical,
                      pl.Date, pl.Datetime("ns", "Europe/Amsterdam"), pl.Duration("us")):
            values = [None, 1, 1, None, 2, 2]
            if dtype in (pl.String, pl.Categorical):
                values = [None, "a", "a", None, "b", "b"]
            elif dtype == pl.Boolean:
                values = [None, False, False, None, True, True]
            source = pl.LazyFrame({"x": pl.Series(values, dtype=dtype)})
            for keep in ("any", "first", "last", "none"):
                with self.subTest(dtype=dtype, keep=keep):
                    for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                        self.equal(source.unique(keep=keep, maintain_order=True), options=flags)
                        self.equal(source.filter(pl.lit(False)).unique(keep=keep, maintain_order=True), options=flags)

    def test_unique_float_keys_merge_nan_and_signed_zero(self):
        for dtype in (pl.Float32, pl.Float64):
            source = pl.LazyFrame({"k": pl.Series([float("nan"), float("nan"), 0.0, -0.0,
                                                   1.0, 1.0, None, None, float("inf"), -float("inf")], dtype=dtype)})
            for keep in ("any", "first", "last", "none"):
                self.equal(source.unique(keep=keep, maintain_order=True))
            data = pl.LazyFrame({"k": source.collect()["k"], "id": list(range(10))})
            for keep in ("first", "last", "none"):
                self.equal(data.unique(subset="k", keep=keep, maintain_order=True))

    def test_unique_subset_selects_whole_rows_and_preserves_selected_row_order(self):
        data = {"g": ["a", "b", "a", None, None, "c"],
                "v": [None, 20, 30, None, 50, 60], "_ibex_unique_0_row": list(range(6))}
        source = pl.LazyFrame(data)
        for keep in ("first", "last", "none"):
            for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
                self.equal(source.unique(subset="g", keep=keep, maintain_order=True), options=flags)
                query = source.unique(subset="g", keep=keep, maintain_order=False)
                assert_frame_equal(query.collect(engine=self.engine, optimizations=flags),
                                   query.collect(optimizations=flags), check_row_order=False)
        # Polars keep=any permits any representative; Ibex chooses the first.
        result = source.unique(subset="g", keep="any", maintain_order=True).collect(engine=self.engine)
        expected = source.unique(subset="g", keep="first", maintain_order=True).collect()
        assert_frame_equal(result, expected)
        keys = pl.LazyFrame({"a": [None, None, 1, 1, 1], "b": ["x", "x", "y", "y", "z"],
                             "v": [None, 1, 2, 3, 4]})
        self.equal(keys.unique(subset=["b", "a"], keep="last", maintain_order=True))
        self.equal(source.unique(subset="g", keep="last", maintain_order=True)
                   .filter(pl.col("v") > 25).select("g", "v"))

    def test_unique_preserves_row_positions_across_large_inputs(self):
        size = 150_003
        source = pl.LazyFrame({"k": [i % 23 for i in range(size)], "id": list(range(size))})
        for keep in ("first", "last", "none"):
            self.equal(source.unique(subset="k", keep=keep, maintain_order=True))

    def test_unique_shared_api_and_single_native_execution(self):
        import ibex
        data = {"g": [1, 1, 2], "v": [None, 3, 4]}
        query = pl.LazyFrame(data).unique(subset="g", keep="last", maintain_order=True)
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            self.equal(query)
            self.assertEqual(execute.call_count, 1)
        result = ibex.DataFrame(data).lazy().unique(subset="g", keep="last", maintain_order=True).collect()
        assert_frame_equal(pl.from_arrow(result.to_arrow()), query.collect())
        self.assertIn("distinct {", ibex.DataFrame({"x": [1, 1, 2]}).lazy().unique().explain())
        with self.assertRaises(NotImplementedError):
            ibex.DataFrame(data).lazy().unique(subset=[])
        self.equal(pl.LazyFrame(data).unique(keep="first", maintain_order=True).head(1))


if __name__ == "__main__":
    unittest.main()
