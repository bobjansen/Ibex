# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Join semantics against Polars, with both inputs executed natively."""

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


class JoinTests(unittest.TestCase):
    def equal(self, query, *, ordered=True):
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            expected = query.collect(optimizations=flags)
            actual = query.collect(engine=IbexEngine(), optimizations=flags)
            assert_frame_equal(actual, expected, check_row_order=ordered)

    def test_join_kinds_nulls_coalescing_and_suffixes(self):
        left = pl.LazyFrame({"a": [1, 2, None, 2], "v": ["a", "b", None, "d"]})
        right = pl.LazyFrame({"b": [2, 3, None, 2], "v": ["x", "y", "z", None]})
        for how in ("inner", "left", "right", "full", "semi", "anti", "cross"):
            for nulls_equal in (False, True):
                for coalesce in (None, False, True):
                    kwargs = {} if how == "cross" else {"left_on": "a", "right_on": "b"}
                    with self.subTest(how=how, nulls_equal=nulls_equal, coalesce=coalesce):
                        self.equal(left.join(right, how=how, nulls_equal=nulls_equal, coalesce=coalesce,
                                             suffix="_rhs", maintain_order="left_right", **kwargs))

    def test_empty_sides_preserve_schema(self):
        for left_empty, right_empty in ((True, False), (False, True), (True, True)):
            left = pl.LazyFrame({"k": pl.Series([] if left_empty else [1, None], dtype=pl.Int64),
                                 "v": pl.Series([] if left_empty else [2.0, None], dtype=pl.Float64)})
            right = pl.LazyFrame({"k": pl.Series([] if right_empty else [1, None], dtype=pl.Int64),
                                  "w": pl.Series([] if right_empty else [False, True], dtype=pl.Boolean)})
            for how in ("inner", "left", "right", "full", "semi", "anti", "cross"):
                self.equal(left.join(right, how=how, maintain_order="left_right", nulls_equal=True,
                                     **({} if how == "cross" else {"on": "k"})))

    def test_scalar_key_types_nan_signed_zero_and_composite_keys(self):
        for dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16,
                      pl.Float32, pl.Float64, pl.String, pl.Categorical, pl.Boolean,
                      pl.Date, pl.Datetime("ns", "Europe/Amsterdam"), pl.Duration("us")):
            lv, rv = [1, 2, None, 1], [2, None, 3, 1]
            if dtype in (pl.Float32, pl.Float64):
                lv, rv = [float("nan"), -0.0, None, 1.0], [0.0, float("nan"), 2.0, None]
            elif dtype in (pl.String, pl.Categorical):
                lv, rv = ["a", "b", None, "a"], ["b", None, "c", "a"]
            elif dtype == pl.Boolean:
                lv, rv = [True, False, None, True], [False, None, True, True]
            left = pl.LazyFrame({"k": pl.Series(lv, dtype=dtype), "id": list(range(4))})
            right = pl.LazyFrame({"k": pl.Series(rv, dtype=dtype), "v": [4, 5, 6, 7]})
            for how in ("inner", "full"):
                for nulls_equal in (False, True):
                    with self.subTest(dtype=dtype, how=how, nulls_equal=nulls_equal):
                        self.equal(left.join(right, on="k", how=how, coalesce=True,
                                             nulls_equal=nulls_equal, maintain_order="left_right"))
        left = pl.LazyFrame({"a": [1, 1, 2, None], "b": [None, "x", "y", "z"], "x": [1, 2, 3, 4]})
        right = pl.LazyFrame({"a": [1, 1, 2, None], "b": [None, "q", "y", "z"], "y": [5, 6, 7, 8]})
        self.equal(left.join(right, on=["a", "b"], how="full", coalesce=True,
                             nulls_equal=True, maintain_order="left_right"))

    def test_order_modes_and_join_slice(self):
        left = pl.LazyFrame({"k": [2, 1, 2, 9], "x": [0, 1, 2, 3]})
        right = pl.LazyFrame({"k": [2, 2, 1, 10], "y": [4, 5, 6, 7]})
        for how in ("inner", "left", "right", "full", "cross"):
            for order in ("none", "left", "right", "left_right", "right_left"):
                query = left.join(right, how=how, maintain_order=order, **({} if how == "cross" else {"on": "k"}))
                with self.subTest(how=how, order=order):
                    self.equal(query, ordered=order in ("left_right", "right_left"))
                    if order in ("left", "right"):
                        # A one-sided order leaves match ordering on the other
                        # side unspecified; verify the promised side directly.
                        name = "x" if order == "left" else "y"
                        for result in (query.collect(), query.collect(engine=IbexEngine())):
                            positions = result[name].drop_nulls().to_list()
                            self.assertEqual(positions, sorted(positions))
        self.equal(left.join(right, on="k", maintain_order="left_right").slice(1, 3))

    def test_nested_joins_and_inputs_with_native_scalar_bindings(self):
        a = pl.LazyFrame({"k": [1, 2, 3, 4], "x": [10, 20, 30, 40]}).slice(-3, 3)
        b = pl.LazyFrame({"q": [2, 3, 4], "y": [50, 60, 70]}).slice(-2, 2)
        c = pl.LazyFrame({"k": [3, 4], "z": [80, 90]})
        self.equal(a.join(b, left_on="k", right_on="q", how="left", maintain_order="left_right")
                   .join(c, on="k", how="left", maintain_order="left_right"))
        self.equal(a.join(b.join(c, left_on="q", right_on="k", maintain_order="left_right"),
                          left_on="k", right_on="q", how="left", maintain_order="left_right"))
        self.equal(a.join(c, on="k", how="left", maintain_order="left_right").group_by("k", maintain_order=True).agg(pl.col("x").sum()))

    def test_zero_width_projection_and_height_carrier_name_collision(self):
        left = pl.LazyFrame({"a": [1, 2, 3]})
        right = pl.LazyFrame({"_ibex_height": [1, 2, 3, 4, 5]})
        self.equal(left.join(right, how="cross").select("_ibex_height"), ordered=False)
        self.equal(left.join(right, how="cross").select(pl.len()))
        self.equal(left.join(right, how="cross").head(0).select(pl.len()))

    def test_unsupported_options_fail_before_native_execution(self):
        left = pl.LazyFrame({"k": [1, 1], "x": [2, 3]})
        right = pl.LazyFrame({"k": [1], "y": [4]})
        for query in (left.join(right, on="k", validate="1:1"),
                      left.join(right, left_on=pl.col("k") + 1, right_on="k")):
            with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
                with self.assertRaises(NotImplementedError):
                    query.collect(engine=IbexEngine(), optimizations=pl.QueryOptFlags.none())
                execute.assert_not_called()

    def test_shared_api_and_one_native_pipeline(self):
        data_left, data_right = {"k": [1, 2, 2], "x": [3, 4, 5]}, {"k": [2], "y": [6]}
        query = pl.LazyFrame(data_left).join(pl.LazyFrame(data_right), on="k", how="left", maintain_order="left_right")
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            query.collect(engine=IbexEngine())
            self.assertEqual(execute.call_count, 1)
        actual = ibex.DataFrame(data_left).lazy().join(ibex.DataFrame(data_right).lazy(),
                    on="k", how="left", maintain_order="left_right").collect()
        assert_frame_equal(pl.from_arrow(actual.to_arrow()), query.collect())
        with self.assertRaises(ValueError):
            ibex.DataFrame({"k": [1], "v": [2]}).lazy().join(
                ibex.DataFrame({"k": [1], "v": [3], "v_right": [4]}).lazy(), on="k").collect()

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "native Parquet backend not built")
    def test_two_parquet_inputs_share_one_import_and_capture_distinct_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            left, right = Path(directory) / "left.parquet", Path(directory) / "right.parquet"
            pq.write_table(pa.table({"k": [1, 2], "x": [3, 4]}), left)
            pq.write_table(pa.table({"k": [2, 3], "y": [5, 6]}), right)
            self.equal(pl.scan_parquet(left).join(pl.scan_parquet(right), on="k", how="full",
                                                 maintain_order="left_right", coalesce=True))


if __name__ == "__main__":
    unittest.main()
