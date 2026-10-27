# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Temporal storage, precision, range and public dtype checks."""

from datetime import datetime, timedelta, timezone
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal
import pyarrow as pa
import pyarrow.parquet as pq

import ibex_pyarrow
from ibex_polars import IbexEngine


class TemporalTests(unittest.TestCase):
    def equal(self, query):
        for flags in (pl.QueryOptFlags(), pl.QueryOptFlags.none()):
            assert_frame_equal(query.collect(engine=IbexEngine(), optimizations=flags),
                               query.collect(optimizations=flags), rel_tol=1e-12)

    def check_type(self, dtype):
        frame = pl.LazyFrame({"x": pl.Series([-2, -1, 0, 1, 2, None], dtype=dtype),
                              "y": pl.Series([-1, -1, 0, 2, 1, None], dtype=dtype),
                              "g": ["a", "a", "b", "b", "c", "c"]})
        self.equal(frame)
        self.equal(frame.select(pl.col("x").alias("renamed")))
        for operator in ("__eq__", "__ne__", "__lt__", "__le__", "__gt__", "__ge__"):
            self.equal(frame.select(getattr(pl.col("x"), operator)(pl.col("y")).alias("comparison")))
            self.equal(frame.filter(getattr(pl.col("x"), operator)(pl.col("y"))))
        self.equal(frame.filter(pl.col("x").is_null()))
        self.equal(frame.sort("x", nulls_last=True))
        self.equal(frame.sort("x", descending=True, nulls_last=False))
        self.equal(frame.group_by("x", maintain_order=True).agg(pl.len()))
        self.equal(frame.select(lo=pl.col("x").min(), hi=pl.col("x").max()))
        self.equal(frame.group_by("g", maintain_order=True).agg(lo=pl.col("x").min(), hi=pl.col("x").max()))
        engine = IbexEngine().prepare(frame)
        _, bindings, *_ = engine._prepare(frame, pl.QueryOptFlags())
        native = ibex_pyarrow.execute_pipeline("input;", tables={"input": bindings["_ibex_input"]})
        self.assertEqual(native.schema.field("x").type, pa.int64())

    def test_timestamps_all_units_and_timezones(self):
        for unit in ("ms", "us", "ns"):
            for zone in (None, "UTC", "Europe/Amsterdam"):
                with self.subTest(unit=unit, zone=zone):
                    self.check_type(pl.Datetime(unit, zone))

    def test_durations_all_units(self):
        for unit in ("ms", "us", "ns"):
            with self.subTest(unit=unit):
                self.check_type(pl.Duration(unit))
                frame = pl.LazyFrame({"d": pl.Series([-2, 1, 3, None], dtype=pl.Duration(unit))})
                self.equal(frame.select(pl.col("d").sum()))
                self.equal(frame.filter(pl.col("d") > timedelta(0)))

    def test_empty_and_all_null_temporals(self):
        for dtype in (pl.Datetime("ns"), pl.Datetime("us", "UTC"), pl.Duration("us")):
            for values in ([], [None, None]):
                frame = pl.LazyFrame({"x": pl.Series(values, dtype=dtype)})
                self.equal(frame)
                self.equal(frame.select(lo=pl.col("x").min(), hi=pl.col("x").max()))

    def test_precise_nanoseconds_and_mixed_units(self):
        for kind in (pl.Datetime, pl.Duration):
            frame = pl.LazyFrame({"n": pl.Series([-1001, -1, 0, 1, 1001, None], dtype=kind("ns")),
                                  "u": pl.Series([-1, -1, 0, 0, 1, None], dtype=kind("us"))})
            # Polars inserts a precision-losing cast for mixed-unit comparison.
            # That operation is separate from exact nanosecond storage.
            with self.assertRaisesRegex(NotImplementedError, "lossy temporal casts"):
                frame.select(test=pl.col("n") > pl.col("u")).collect(engine=IbexEngine())
            self.equal(frame.select(pl.col("u").cast(kind("ns"))))

    def test_supported_timestamp_literals(self):
        for unit in ("ms", "us"):
            for zone in (None, "UTC"):
                literal = datetime(1970, 1, 1, tzinfo=timezone.utc if zone else None)
                frame = pl.LazyFrame({"t": pl.Series([-1, 0, 1, None], dtype=pl.Datetime(unit, zone))})
                self.equal(frame.filter(pl.col("t") >= pl.lit(literal).cast(pl.Datetime(unit, zone))))

    def test_nanosecond_timestamp_literal_bug_fails_explicitly(self):
        frame = pl.LazyFrame({"t": pl.Series([0, 1, 2], dtype=pl.Datetime("ns"))})
        query = frame.filter(pl.col("t") > pl.lit(1).cast(pl.Datetime("ns")))
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaisesRegex(NotImplementedError, "exact Polars visitor"):
                query.collect(engine=IbexEngine())
            execute.assert_not_called()

    def test_nanosecond_duration_literal_retains_exact_ticks(self):
        frame = pl.LazyFrame({"d": pl.Series([0, 1, 2, None], dtype=pl.Duration("ns"))})
        self.equal(frame.filter(pl.col("d") > pl.lit(1).cast(pl.Duration("ns"))))

    def test_range_checks_and_int64_nanosecond_endpoints(self):
        for dtype in (pl.Datetime("us"), pl.Duration("ms")):
            for value in (-(2**63), 2**63 - 1):
                frame = pl.LazyFrame({"x": pl.Series([value, None], dtype=dtype)})
                with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
                    with self.assertRaisesRegex(NotImplementedError, "Int64 nanosecond range"):
                        frame.collect(engine=IbexEngine())
                    execute.assert_not_called()
        for dtype in (pl.Datetime("ns"), pl.Duration("ns")):
            self.equal(pl.LazyFrame({"x": pl.Series([-(2**63), 2**63 - 1, None], dtype=dtype)}))

    def test_temporal_parquet_is_explicitly_unsupported(self):
        with tempfile.TemporaryDirectory() as directory:
            for dtype in (pa.timestamp("us"), pa.duration("us")):
                path = Path(directory) / "input.parquet"
                pq.write_table(pa.table({"x": pa.array([0, 1, None], type=dtype)}), path)
                with self.assertRaisesRegex(NotImplementedError, "temporal Parquet"):
                    pl.scan_parquet(path).collect(engine=IbexEngine())


if __name__ == "__main__":
    unittest.main()
