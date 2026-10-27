# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Run unchanged upstream q01/q06 through the Ibex Polars Engine."""

import argparse
import importlib
from pathlib import Path
import sys
from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal

import ibex_pyarrow
from ibex_polars import IbexEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pdsh-root", type=Path, required=True)
    parser.add_argument("--lineitem", type=Path, required=True)
    parser.add_argument("--in-memory", action="store_true")
    args = parser.parse_args()
    sys.path.insert(0, str(args.pdsh_root.resolve()))
    lineitem = pl.read_parquet(args.lineitem).lazy() if args.in_memory else pl.scan_parquet(args.lineitem)
    engine = IbexEngine()
    for number in (1, 6):
        module = importlib.import_module(f"queries.polars.q{number}")
        query = module.q(lineitem=lineitem)
        reference = query.collect(engine="streaming")
        with patch.object(ibex_pyarrow, "execute_pipeline", wraps=ibex_pyarrow.execute_pipeline) as execute:
            engine.prepare(query)
            execute.assert_not_called()
            result = query.collect(engine=engine)
            assert execute.call_count == 1
        assert_frame_equal(result, reference, rel_tol=1e-12, abs_tol=1e-8)
        print(f"upstream q{number:02} passed: {result.shape}; {'memory' if args.in_memory else 'parquet'}; native calls=1")


if __name__ == "__main__":
    main()
