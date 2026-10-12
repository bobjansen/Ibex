# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Check the unchanged upstream q01 with independent Ibex and the reference."""

import argparse
import importlib
import math
from pathlib import Path
import sys
from unittest.mock import patch

import pyarrow.parquet as pq
import polars
import ibex
import ibex_pyarrow


def assert_equal(result, reference):
    assert result.columns == reference.columns, (result.columns, reference.columns)
    assert result.shape == reference.shape, (result.shape, reference.shape)
    assert result.to_arrow().schema.types == reference.to_arrow().schema.types, (
        result.to_arrow().schema, reference.to_arrow().schema
    )
    for name in result.columns:
        actual = result[name].to_list()
        expected = reference[name].to_list()
        for left, right in zip(actual, expected):
            if isinstance(right, float):
                assert math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-8), (name, left, right)
            else:
                assert left == right, (name, left, right)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdsh-root", type=Path, required=True)
    parser.add_argument("--lineitem", type=Path, required=True)
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(args.pdsh_root.resolve()))
    module = importlib.import_module("queries.polars.q1")
    if args.in_memory:
        table = pq.read_table(args.lineitem).combine_chunks()
        native_input = ibex.DataFrame(table).lazy()
        reference_input = polars.from_arrow(table).lazy()
    else:
        native_input = ibex.scan_parquet(args.lineitem)
        reference_input = polars.scan_parquet(args.lineitem)
    reference = module.q(lineitem=reference_input).collect(engine="streaming")
    with patch.object(module, "pl", ibex):
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            query = module.q(lineitem=native_input)
            assert execute.call_count == 0
            result = query.collect()
            assert execute.call_count == 1
    assert_equal(result, reference)
    print(f"upstream q01 passed: {result.shape[0]} groups; {'memory' if args.in_memory else 'parquet'}; native calls=1")


if __name__ == "__main__":
    main()
