# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Semantic and execution-boundary checks; no Polars dependency."""

from datetime import date
from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

import ibex as pl
import ibex_pyarrow


def q06(frame):
    return (
        frame.filter(pl.col("l_shipdate").is_between(date(1994, 1, 1), date(1995, 1, 1), closed="left"))
        .filter(pl.col("l_discount").is_between(0.05, 0.07))
        .filter(pl.col("l_quantity") < 24)
        .with_columns((pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue"))
        .select(pl.sum("revenue"))
    )


class LazyTests(unittest.TestCase):
    def test_independent_import_and_execution(self):
        code = '''
import importlib.abc, sys
class BlockPolars(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] == "polars":
            raise AssertionError("core package tried to import Polars")
sys.meta_path.insert(0, BlockPolars())
import ibex as pl
assert pl.DataFrame({"x": [2, 3]}).lazy().select(pl.sum("x")).collect().item() == 5
assert "polars" not in sys.modules
'''
        subprocess.run([sys.executable, "-c", code], check=True)

    @unittest.skipIf(os.environ.get("IBEX_TEST_PARQUET") == "0", "Parquet backend disabled")
    def test_q06_boundaries_nulls_and_lazy_parquet(self):
        # Include both discount endpoints, excluded upper date/quantity
        # boundaries, a null predicate, and a null value that sum must skip.
        data = pa.table({
            "l_shipdate": pa.array([date(1994, 1, 1), date(1994, 12, 31), date(1995, 1, 1),
                                    date(1994, 5, 1), None, date(1994, 6, 1)], type=pa.date32()),
            "l_discount": [0.05, 0.07, 0.06, 0.06, 0.06, 0.06],
            "l_quantity": [23, 1, 1, 24, 1, 1],
            "l_extendedprice": [100.0, 200.0, 999.0, 999.0, 999.0, None],
        })
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input "quoted".parquet'
            # Even file existence is deferred; construction before the file
            # exists must work, and explain must not invoke native execution.
            with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
                query = q06(pl.scan_parquet(path))
                source = query.explain()
                self.assertNotIn(str(path), source)
                execute.assert_not_called()
                pq.write_table(data, path)
                result = query.collect()
                self.assertEqual(execute.call_count, 1)
                self.assertEqual(result.columns, ["revenue"])
                self.assertEqual(result.shape, (1, 1))
                self.assertEqual(result.schema["revenue"], pa.float64())
                self.assertAlmostEqual(result.item(), 19.0)
                self.assertEqual(query.collect().item(), result.item())
                self.assertEqual(execute.call_count, 2)

    def test_immutable_branches_and_input_lifetime(self):
        root = pl.DataFrame({"x": [1, 2, 3]}).lazy()
        smaller = root.filter(pl.col("x") < 3).select(pl.sum("x"))
        larger = root.filter(pl.col("x") > 1).select(pl.sum("x"))
        self.assertEqual(smaller.collect().item(), 3)
        self.assertEqual(larger.collect().item(), 5)
        self.assertEqual(root.collect()["x"].to_list(), [1, 2, 3])

    def test_empty_and_all_null_sum(self):
        for dtype in (pa.float64(), pa.int64()):
            for values in ([], [None, None]):
                frame = pl.DataFrame(pa.table({"x": pa.array(values, type=dtype)}))
                result = frame.lazy().select(pl.sum("x")).collect()
                self.assertEqual(result.item(), 0)
                self.assertEqual(result.schema["x"], dtype)

    def test_alias_projection_and_typed_scalar_collision(self):
        frame = pl.DataFrame({"_ibex_scalar_0": [8, 9], "price.usd": [10.0, 20.0]})
        result = frame.lazy().filter(pl.col("_ibex_scalar_0") > 8).select(
            amount=(pl.col("price.usd") * 0.5)
        ).collect()
        self.assertEqual(result.to_dict(as_series=False), {"amount": [10.0]})

    def test_between_all_closed_modes(self):
        root = pl.DataFrame({"x": [0, 1, 2, 3]}).lazy()
        for closed, expected in (("both", [1, 2]), ("left", [1]), ("right", [2]), ("none", [])):
            result = root.filter(pl.col("x").is_between(1, 2, closed=closed)).collect()
            self.assertEqual(result["x"].to_list(), expected)

    def test_constraints_and_expression_logic(self):
        result = pl.DataFrame({"x": [1, 2, 3], "ok": [True, True, False]}).lazy().filter(
            ~(pl.col("x") < 2), ok=True
        ).collect()
        self.assertEqual(result["x"].to_list(), [2])
        with self.assertRaises(TypeError):
            bool(pl.col("x") > 1)

    def test_reject_unsupported_before_execution(self):
        query = pl.DataFrame({"x": [1]}).lazy()
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaises(NotImplementedError):
                query.with_columns(a=pl.col("x"), b=pl.col("x"))
            with self.assertRaises(NotImplementedError):
                query.select(pl.sum("x"), pl.col("x").alias("y"))
            with self.assertRaises(NotImplementedError):
                query.filter(pl.sum("x") > 0)
            with self.assertRaises(TypeError):
                query.collect(engine="streaming")
            with self.assertRaises(NotImplementedError):
                pl.col('x`; Table(1); `')
            with self.assertRaises(ValueError):
                pl.col("x").is_between(0, 1, closed="invalid")
            execute.assert_not_called()

    def test_native_errors_and_result_ownership(self):
        if os.environ.get("IBEX_TEST_PARQUET") != "0":
            with self.assertRaises(OSError):
                pl.scan_parquet("__definitely_missing__.parquet").collect()
        with self.assertRaises(ValueError):
            pl.DataFrame({"x": [1]}).lazy().select("missing").collect()
        # Native execution session has already been destroyed on return.
        result = pl.DataFrame({"x": [1, 2]}).lazy().collect()
        self.assertEqual(result.to_arrow().to_pydict(), {"x": [1, 2]})

    def test_native_endpoint_propagates_errors(self):
        with self.assertRaises(RuntimeError):
            ibex_pyarrow.execute_pipeline("not valid Ibex;")
        with self.assertRaises(RuntimeError):
            ibex_pyarrow.execute_pipeline("1 + 1;")

    def test_grouped_aggregates_null_keys_and_output_types(self):
        frame = pl.DataFrame(pa.table({
            "g": ["b", None, "a", "b", None],
            "h": [1, 1, 2, 1, 1],
            "x": pa.array([2, None, 7, 4, None], type=pa.int64()),
            "v": [1.0, None, 3.0, 5.0, None],
        }))
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            grouped = frame.lazy().group_by("g", "h", maintain_order=True)
            query = grouped.agg(
                total=pl.sum("x"), average=pl.mean("x"),
                weighted=(pl.col("v") * 2.0).sum(), rows=pl.len(),
            ).sort("g", "h")
            execute.assert_not_called()
            result = query.collect()
            self.assertEqual(execute.call_count, 1)
        self.assertEqual(result.to_dict(as_series=False), {
            "g": [None, "a", "b"], "h": [1, 2, 1], "total": [0, 7, 6],
            "average": [None, 7.0, 3.0], "weighted": [0.0, 6.0, 12.0], "rows": [2, 1, 2],
        })
        self.assertEqual(result.schema["rows"], pa.uint32())
        self.assertEqual(result.schema["average"], pa.float64())
        # Grouping metadata is reusable, not a mutation of the original query.
        self.assertEqual(grouped.agg(pl.len()).collect()["g"].to_list(), ["b", None, "a"])

    def test_empty_grouped_aggregate_keeps_schema(self):
        query = pl.DataFrame(pa.table({"g": pa.array([], type=pa.string()),
                                      "x": pa.array([], type=pa.float64())})).lazy()
        result = query.group_by("g").agg(total=pl.sum("x"), average=pl.mean("x"), n=pl.len()).sort("g").collect()
        self.assertEqual(result.shape, (0, 4))
        self.assertEqual(result.schema, {"g": pa.large_string(), "total": pa.float64(),
                                         "average": pa.float64(), "n": pa.uint32()})

    def test_sort_directions_null_positions_stability_and_internal_name(self):
        frame = pl.DataFrame({"g": [None, "a", "a", "b", None], "h": [1, 2, 1, 0, 2],
                              "_ibex_sort_null_0_0": [10, 11, 12, 13, 14]})
        result = frame.lazy().sort(["g", "h"], descending=[True, False], nulls_last=[False, True], maintain_order=True).collect()
        self.assertEqual(result["g"].to_list(), [None, None, "b", "a", "a"])
        self.assertEqual(result["h"].to_list(), [1, 2, 0, 1, 2])
        self.assertEqual(result.columns, frame.columns)
        self.assertNotIn(b"ibex.ordering", result.to_arrow().schema.metadata or {})
        tied = pl.DataFrame({"g": [1, 1, 1], "id": [3, 2, 1]}).lazy().sort("g", maintain_order=True).collect()
        self.assertEqual(tied["id"].to_list(), [3, 2, 1])

    def test_global_mean_and_row_count(self):
        result = pl.DataFrame({"x": [1, None, 5]}).lazy().select(avg=pl.mean("x"), n=pl.len()).collect()
        self.assertEqual(result.to_dict(as_series=False), {"avg": [3.0], "n": [3]})
        self.assertEqual(result.schema["n"], pa.uint32())

    def test_group_sort_reject_unsupported_shapes(self):
        root = pl.DataFrame({"x": [1]}).lazy()
        with patch.object(ibex_pyarrow, "execute_pipeline") as execute:
            with self.assertRaises(NotImplementedError):
                root.group_by("x").agg(pl.col("x").alias("y"))
            with self.assertRaises(ValueError):
                root.group_by("x").agg(pl.sum("x"))
            with self.assertRaises(ValueError):
                root.sort(["x"], descending=[True, False])
            with self.assertRaises(NotImplementedError):
                pl.mean("x").sum()
            execute.assert_not_called()

    def test_prepared_input_reuses_ownership_and_categorical_codes(self):
        frame = pl.DataFrame({"g": ["a", "b"] * 500, "x": list(range(1000))})
        with patch.object(ibex_pyarrow, "bind_table", wraps=ibex_pyarrow.bind_table) as bind:
            root = frame.lazy()
            again = frame.lazy()
            self.assertEqual(bind.call_count, 1)
        # The native table retains the input after its Python frame is gone.
        del frame
        native = ibex_pyarrow.execute_pipeline("input;", tables={"input": root._source[1].capsule})
        self.assertTrue(pa.types.is_dictionary(native.schema.field("g").type))
        self.assertEqual(native["g"].to_pylist(), ["a", "b"] * 500)
        self.assertEqual(root.group_by("g").agg(n=pl.len()).sort("g").collect()["n"].to_list(), [500, 500])
        self.assertEqual(again.select(pl.sum("x")).collect().item(), 499500)
        changed = root.with_columns((pl.col("x") * 2).alias("x")).select(pl.sum("x"))
        self.assertEqual(changed.collect().item(), 999000)
        self.assertEqual(root.select(pl.sum("x")).collect().item(), 499500)

    def test_categorical_preparation_preserves_null_and_high_cardinality(self):
        root = pl.DataFrame({"g": [None, ""] * 500, "x": [1] * 1000}).lazy()
        result = root.group_by("g").agg(n=pl.len()).sort("g").collect()
        self.assertEqual(result.to_dict(as_series=False), {"g": [None, ""], "n": [500, 500]})
        unique = pl.DataFrame({"g": [f"key{i}" for i in range(1000)]}).lazy()
        native = ibex_pyarrow.execute_pipeline("input;", tables={"input": unique._source[1].capsule})
        self.assertFalse(pa.types.is_dictionary(native.schema.field("g").type))
        self.assertEqual(unique.select(pl.len()).collect().item(), 1000)


if __name__ == "__main__":
    unittest.main()
