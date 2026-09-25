#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build a string column whose data pages fall back from the dictionary to
PLAIN part way through its one column chunk.

Parquet writers abandon a chunk's dictionary once it outgrows its page-size
limit, and write the rest of the chunk PLAIN. Arrow records that in the
per-page encoding stats; Polars writes no encoding stats, so a reader of its
files cannot know up front. Ibex reads a stats-less column with a dictionary
page as Categorical and checks each data page as it decodes, interning a
fallen-back page's values instead of failing.

No writer at hand produces "stats-less AND falls back", so this file has the
stats and the check reads it with IBEX_PARQUET_IGNORE_ENCODING_STATS=1, which
makes the reader behave as it does on a Polars file.

`seg`: rows 0..99,999 cycle five segment names (dictionary-encoded pages);
rows 100,000..199,999 cycle 2,000 distinct codes, which overflow the 4 KiB
dictionary limit, so the pages after that point are PLAIN.

Needs pyarrow (in the project's uv environment).
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_dictionary_fallback_out.parquet")
n = 200_000
segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
seg = [segments[i % 5] if i < 100_000 else f"x{i % 2000:04d}" for i in range(n)]
table = pa.table({"id": pa.array(range(n), type=pa.int64()), "seg": seg})
pq.write_table(table, out, row_group_size=n, use_dictionary=["seg"],
               dictionary_pagesize_limit=4096, data_page_size=8192, compression="zstd")

# The encodings list cannot show the fallback (the dictionary page is PLAIN
# itself); the e2e step proves it instead: with its stats honoured, Ibex reads
# `seg` dense, because they show a PLAIN data page.
assert pq.ParquetFile(out).metadata.row_group(0).column(1).has_dictionary_page
