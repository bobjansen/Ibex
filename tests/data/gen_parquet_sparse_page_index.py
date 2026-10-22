#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build the fixture for the page-index sparse read test.

A sparse selected read skips the data pages that hold no selected row before
they are decompressed, using the offset index (libs/parquet SparsePagePlan).
The column reader then sees only the kept pages, so every selected row has to
be found at its position in that kept stream. This fixture makes the mapping
easy to get wrong and the error visible:

- write_page_index=True: the offset index the skipping needs;
- data_page_size=1024 with the default 1024-value write batch flushes a page
  every ~1024 rows, so each row group has hundreds of pages to skip;
- two 300k-row row groups: the check selects four rows in the first (sparse
  by the 64k-rows-per-selected-row threshold alone) and six in the second
  (dense by that threshold; sparse because the page plan skips most pages);
- every value is derived from its id (s = "text-NNNNNN", v = 7 * id), so a row
  read from the wrong position prints a different value, not an error.
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_sparse_page_index_out.parquet")
n = 600_000
table = pa.table({
    "id": pa.array(range(n), pa.int64()),
    "s": pa.array([f"text-{i:06d}" for i in range(n)], pa.string()),
    "v": pa.array([7 * i for i in range(n)], pa.int64()),
})
pq.write_table(
    table,
    out,
    compression="zstd",
    use_dictionary=False,
    data_page_size=1024,
    row_group_size=300_000,
    write_page_index=True,
)
