# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Polars 2.0.0 reproducer: query is correct, visitor loses nanoseconds.

Requires only polars==2.0.0. The final assertion intentionally fails.
"""

import polars as pl

query = pl.LazyFrame({"t": pl.Series([0, 1, 2], dtype=pl.Datetime("ns"))}).filter(
    pl.col("t") > pl.lit(1).cast(pl.Datetime("ns"))
)
assert query.collect()["t"].cast(pl.Int64).to_list() == [2]

visitor = query._ldf.with_optimizations(pl.QueryOptFlags()._pyoptflags).visit()
predicate = visitor.view_expression(visitor.view_current_node().predicate.node)
literal = visitor.view_expression(predicate.right)
reconstructed = pl.Series([literal.value], dtype=literal.dtype).cast(pl.Int64).item()

print("Polars:", pl.__version__)
print("Visitor dtype:", literal.dtype)
print("Visitor value:", repr(literal.value))
print("Reconstructed ticks:", reconstructed)
assert reconstructed == 1, "visitor lost the timestamp literal's nanosecond"
