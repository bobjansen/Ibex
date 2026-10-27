# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Shared first/last compatibility requirements, including both null policies."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal
import pyarrow as pa
import pyarrow.parquet as pq

import ibex
import ibex_pyarrow
from ibex_polars import IbexEngine


def selectors(dtype=None):
    return [getattr(pl.col("x"), name)(ignore_nulls=ignore).alias(f"{name}_{ignore}")
            for name in ("first", "last") for ignore in ((False,) if dtype == pl.Date else (False, True))]


class FirstLastTests(unittest.TestCase):
    def equal(self, query):
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            assert_frame_equal(query.collect(engine=IbexEngine(), optimizations=flags),
                               query.collect(optimizations=flags), rel_tol=1e-12)

    def test_null_edges_and_row_order_across_supported_types(self):
        types = [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
                 pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Categorical,
                 pl.Date, pl.Datetime("ns", "Europe/Amsterdam"), pl.Duration("us")]
        for dtype in types:
            values = [None, 1, None, 2, None, None, 3, 4]
            if dtype in (pl.String, pl.Categorical):
                values = [None if value is None else f"v{value}" for value in values]
            elif dtype == pl.Boolean:
                values = [None if value is None else bool(value % 2) for value in values]
            elif dtype in (pl.Float32, pl.Float64):
                values[1] = float("nan")
            frame = pl.LazyFrame({"x": pl.Series(values, dtype=dtype),
                                  "g": ["a", "a", "a", "b", "b", "c", "d", "d"],
                                  "id": list(range(8))})
            with self.subTest(dtype=dtype):
                self.equal(frame.select(selectors(dtype)))
                self.equal(frame.group_by("g", maintain_order=True).agg(selectors(dtype)))
                self.equal(frame.sort("id", descending=True).group_by("g", maintain_order=True).agg(selectors(dtype)))
                self.equal(frame.filter(pl.col("id") > 0).select(selectors(dtype)))

    def test_empty_and_all_null_inputs(self):
        for dtype in (pl.Int64, pl.Boolean, pl.String, pl.Categorical, pl.Date,
                      pl.Datetime("ns"), pl.Duration("ns")):
            for values in ([], [None, None]):
                frame = pl.LazyFrame({"x": pl.Series(values, dtype=dtype),
                                      "g": pl.Series(["a"] * len(values), dtype=pl.String)})
                with self.subTest(dtype=dtype, values=values):
                    self.equal(frame.select(selectors(dtype)))
                    self.equal(frame.group_by("g").agg(selectors(dtype)))

    def test_computed_inputs_composed_reductions_and_temporary_collisions(self):
        frame = pl.LazyFrame({"x": [None, 2, 3], "g": ["a", "a", "b"],
                              "_ibex_edge_0_3": [None, 10, 20]})
        expressions = [(pl.col("x").first() + pl.col("x").sum()).alias("mixed"),
                       pl.col("x").last().alias("last"),
                       pl.col("x").mean().alias("mean")]
        self.equal(frame.select(expressions))
        self.equal(frame.group_by("g", maintain_order=True).agg(expressions))
        self.equal(frame.select(pl.col("_ibex_edge_0_3").first().alias("x"),
                                pl.col("x").first().alias("_ibex_edge_0_3")))
        computed = frame.select((pl.col("x") + 1).first().alias("x"))
        flags = pl.QueryOptFlags.none()
        assert_frame_equal(computed.collect(engine=IbexEngine(), optimizations=flags),
                           computed.collect(optimizations=flags))
        query = frame.group_by("g", maintain_order=True).agg(selectors())
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            query.collect(engine=IbexEngine())
            self.assertEqual(execute.call_count, 1)

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "native Parquet backend not built")
    def test_native_parquet(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "edges.parquet"
            pq.write_table(pa.table({"g": ["a", "a", "a", "b"],
                                     "x": pa.array([None, 1, None, 2], type=pa.int64())}), path)
            self.equal(pl.scan_parquet(path).group_by("g", maintain_order=True).agg(selectors()))

    def test_independent_api_uses_the_same_native_reducers(self):
        data = {"g": ["a", "a", "a", "b"], "x": [None, 1, None, 2]}
        def expressions(mod):
            return [getattr(mod.col("x"), name)(ignore_nulls=ignore).alias(f"{name}_{ignore}")
                    for name in ("first", "last") for ignore in (False, True)]
        query = ibex.DataFrame(data).lazy().group_by("g", maintain_order=True).agg(expressions(ibex))
        expected = pl.DataFrame(data).lazy().group_by("g", maintain_order=True).agg(expressions(pl)).collect()
        assert_frame_equal(pl.from_arrow(query.collect().to_arrow()), expected)
        with self.assertRaises(TypeError):
            ibex.col("x").first(ignore_nulls=1)
        with self.assertRaises(NotImplementedError):
            ibex.col("x").first().last()


if __name__ == "__main__":
    unittest.main()
