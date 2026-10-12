# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Experimental independent Ibex frontend with a Polars-compatible subset.

The current subset supports PDS-H q06 and q01. It does not depend on Polars; collection
requires the ibex_pyarrow native extension. See python/README.md for scope.
"""

from ._expr import Expr, col, lit, sum, mean, len
from ._frame import DataFrame, LazyFrame, LazyGroupBy, Series, scan_parquet

__all__ = ["Expr", "col", "lit", "sum", "mean", "len", "DataFrame", "LazyFrame", "LazyGroupBy", "Series", "scan_parquet"]
