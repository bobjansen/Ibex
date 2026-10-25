# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Row slices lower to seq/filter, with bounds resolved at the current stage."""

import unittest
from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal
import pyarrow as pa

import ibex
import ibex_pyarrow
from ibex_polars import IbexEngine


class SliceTests(unittest.TestCase):
    def equal(self, query):
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            assert_frame_equal(query.collect(engine=IbexEngine(), optimizations=flags),
                               query.collect(optimizations=flags))

    def test_python_bounds_and_steps_match_list_slicing(self):
        slices = [slice(None), slice(2, 10), slice(2, None, 3), slice(-5, -1, 2),
                  slice(None, None, -1), slice(8, 1, -2), slice(None, -1, -1),
                  slice(-1, None, -3), slice(100, 200), slice(-100, 100, 3),
                  slice(3, 3), slice(8, 2), slice(2, 8, -1), slice(0, 1, -1),
                  slice(2**100, None), slice(-2**100, 2**100),
                  slice(None, None, 2**100), slice(None, None, -2**100)]
        for size in (0, 1, 3, 12):
            data = {"x": list(range(size)), "_ibex_slice_row_0": list(range(size)),
                    "text": [None if i % 3 == 0 else str(i) for i in range(size)]}
            table = pa.table({"x": pa.array(data["x"], type=pa.int64()),
                              "_ibex_slice_row_0": pa.array(data["_ibex_slice_row_0"], type=pa.int64()),
                              "text": pa.array(data["text"], type=pa.string())})
            for key in slices:
                with self.subTest(size=size, key=key):
                    result = ibex.DataFrame(table).lazy()[key].collect()
                    self.assertEqual(result.to_dict(as_series=False), {name: values[key] for name, values in data.items()})
                    self.assertEqual(result.columns, list(data))
        with self.assertRaises(ValueError):
            ibex.DataFrame({"x": [1]}).lazy()[::0]

    def test_polars_offset_length_head_and_tail(self):
        for size in (0, 1, 3, 12):
            source = pl.LazyFrame({"x": pl.Series(range(size), dtype=pl.Int64)})
            for offset, length in ((0, None), (0, 0), (2, 10), (-20, 2), (-3, 2),
                                   (-1, None), (100, 4), (-100, 100)):
                with self.subTest(size=size, offset=offset, length=length):
                    self.equal(source.slice(offset, length))
            for count in (0, 1, 3, 20):
                self.equal(source.head(count))
                self.equal(source.tail(count))
                data = ibex.DataFrame(pa.table({"x": pa.array(range(size), type=pa.int64())})).lazy()
                self.assertEqual(data.head(count).collect().to_dict(as_series=False)["x"], source.head(count).collect()["x"].to_list())
                self.assertEqual(data.tail(count).collect().to_dict(as_series=False)["x"], source.tail(count).collect()["x"].to_list())

    def test_polars_strides_reverse_and_computed_projections(self):
        for size in (0, 1, 3, 12):
            source = pl.LazyFrame({"x": pl.Series(range(size), dtype=pl.Int64),
                                   "y": pl.Series(range(size), dtype=pl.Int64)})
            for key in (slice(2, None, 3), slice(None, None, -1), slice(None, None, -3),
                        slice(1, 8, 2), slice(-3, None), slice(4, None, -2)):
                self.equal(source[key])
            self.equal(source.with_columns((pl.col("x") + 1).alias("x")).gather_every(2, offset=1))
            self.equal(source.select((pl.col("x") + 1).gather_every(3).alias("z")))
            self.equal(source.filter(pl.col("x") > 2).gather_every(3))
            self.equal(source.gather_every(3).gather_every(2).reverse())

    def test_slices_after_sort_unique_and_grouping(self):
        source = pl.LazyFrame({"g": ["a", "b", "a", "c", "d"], "x": [0, 1, 2, 3, 4]})
        for offset, length in ((0, 2), (2, 10), (-2, 2), (20, 1)):
            self.equal(source.sort("x", descending=True).slice(offset, length))
            self.equal(source.unique(subset="g", keep="last", maintain_order=True).slice(offset, length))
            self.equal(source.group_by("g", maintain_order=True).agg(pl.col("x").sum()).slice(offset, length))
        self.equal(source.slice(1, 3).filter(pl.col("x") > 1).slice(-1, 1))

    def test_row_positions_span_large_inputs(self):
        size = 150_003
        source = ibex.DataFrame({"x": list(range(size))}).lazy()
        for key in (slice(65534, 131080, 17), slice(-100, None, 7), slice(-1, None, -8191)):
            self.assertEqual(source[key].collect().to_dict(as_series=False)["x"], list(range(size))[key])

    def test_preserves_types_nulls_and_executes_once(self):
        for dtype in (pl.Int8, pl.Float32, pl.String, pl.Categorical, pl.Date,
                      pl.Datetime("ns", "Europe/Amsterdam"), pl.Duration("us")):
            values = [None, "a", "b", None] if dtype in (pl.String, pl.Categorical) else [None, 1, 2, None]
            source = pl.LazyFrame({"x": pl.Series(values, dtype=dtype)})
            self.equal(source.slice(-3, 2))
            self.equal(source[::-2])
            self.equal(source.slice(20, 1))
        query = pl.LazyFrame({"x": range(12)})[2::3]
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            self.equal(query)
            self.assertEqual(execute.call_count, 2)  # one call for each optimization policy
        text = ibex.DataFrame({"x": [1, 2, 3]}).lazy()[2::3].explain()
        self.assertIn("seq()", text)
        self.assertIn("filter", text)
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaises(NotImplementedError):
                pl.LazyFrame({"x": range(5), "y": range(5)}).select(
                    pl.col("x").gather_every(2), pl.col("y")).collect(engine=IbexEngine())
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
