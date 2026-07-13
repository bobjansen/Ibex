#!/usr/bin/env python3
"""Build the fixture for the sparse plain-string page-boundary regression test.

Shape matters more than content here:

- strings are PLAIN-encoded (use_dictionary=False): the reader returns
  parquet::ByteArray descriptors pointing into the page decompression buffer;
- SNAPPY compression: each page is decompressed into a buffer that Arrow
  REUSES for the next page;
- data_page_size=1024 with the default 1024-value write batch flushes a page
  every 1024 rows, so rows 1023|1024 straddle the first page boundary;
- one 200k-row row group with null_count == 0 stats and only 2 selected rows
  keeps mean gap > 64k, which routes the read through the sparse Skip path
  whose run (1023,1024) crosses that page boundary in one emit batch.

A reader that batches ByteArrays across the boundary before copying them out
reports row 1023 as text-002047 (whatever the reused buffer holds), silently.
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_plain_page_boundary_out.parquet")
n = 200_000
table = pa.table({
    "id": pa.array(range(n), pa.int64()),
    "s": pa.array([f"text-{i:06d}" for i in range(n)], pa.string()),
})
pq.write_table(
    table,
    out,
    compression="snappy",
    use_dictionary=False,
    data_page_size=1024,
    row_group_size=n,
)
