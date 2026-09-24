#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build a fixture the way Polars writes Parquet, for the three reader bugs its
files exposed (the PDS-H benchmark data is written by Polars).

Polars' writer, with its defaults, differs from Arrow's in ways that matter to
the reader:

- It writes no per-page encoding stats. A low-cardinality string column has a
  dictionary page, but nothing says every data page used it, and Arrow then
  declines to expose the dictionary. The reader used to claim such a column as
  Categorical anyway and failed with "dictionary column changed encoding".
  `seg` is that column.
- Read dense instead, `seg` decodes to far more characters than its chunk
  stores (five values repeated 200,000 times from a tiny dictionary). The dense
  decoder sized its buffer from the chunk's uncompressed size and wrote past
  it.
- A high-cardinality string column has no dictionary and spans several ZSTD
  pages, so a fused `like` filter reads it in page stripes; that path handed
  the page reader a stream without Peek. `note` is that column.

Needs Polars (in the project's uv environment).
"""
import pathlib
import sys

import polars as pl

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_polars_writer_out.parquet")
n = 200_000
segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
df = pl.DataFrame({
    "id": pl.Series(range(n), dtype=pl.Int64),
    "seg": [segments[i % 5] for i in range(n)],
    # Unique per row, so Polars keeps it PLAIN; every 7th carries the needle.
    "note": [f"note {i:07d} " + ("needle " if i % 7 == 0 else "plain  ") + "x" * 24
             for i in range(n)],
})
df.write_parquet(out)
