# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Smoke test for an installed ibex-polars wheel (cibuildwheel's test-command).

Checks that the native bridge and its bundled Arrow/Parquet libraries load from
the wheel, and that an in-memory and a Parquet-scan query run in Ibex. The full
suites (test_ibex_polars.py and friends) run against the source tree in CI.
"""

from pathlib import Path
import tempfile

import polars as pl
from polars.testing import assert_frame_equal

import ibex_pyarrow
from ibex_polars import IbexEngine


def main():
    # Fail loudly if the source tree, not the installed wheel, was imported.
    assert "site-packages" in ibex_pyarrow.__file__, ibex_pyarrow.__file__
    engine = IbexEngine()
    frame = pl.DataFrame({"g": ["b", "a", "b", "a", "c"], "x": [1, 2, 3, 4, 5],
                          "y": [0.5, 1.5, 2.5, 3.5, 4.5]})
    query = frame.lazy().filter(pl.col("x") > 1).group_by("g").agg(
        total=pl.sum("x"), average=pl.mean("y"), n=pl.len()).sort("g")
    assert_frame_equal(query.collect(engine=engine), query.collect())

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "frame.parquet"
        frame.write_parquet(path)
        scan = pl.scan_parquet(path).group_by("g").agg(total=pl.sum("x")).sort("g")
        assert_frame_equal(scan.collect(engine=engine), scan.collect())
    print("ibex-polars wheel smoke test passed")


if __name__ == "__main__":
    main()
