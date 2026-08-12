#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build the fixture for the fused string filter's null handling.

The fused scan (`filtered_string_group_scan`) reads a nullable PLAIN string
column the hard way: values arrive COMPACTED, and definition levels are what
maps them back to rows. Getting that wrong shifts every value after the first
null onto the wrong row — which still returns a plausible row count, so the
test sums a key rather than counting.

`write_parquet` cannot produce this shape, hence pyarrow:

- use_dictionary=False keeps the column PLAIN, which is the only encoding the
  fused scan claims (a dictionary column is decoded to codes instead);
- nulls in `s` make max_definition_level 1, selecting the level-aware branch;
- row_group_size=500 over 1500 rows gives three row groups, so the per-group
  results also have to concatenate back in file order.

`like(null, p)` is null and a filter keeps only true, so a null row must fail
BOTH `like` and `not like` — the one place where a predicate and its negation
agree.
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_string_filter_nulls_out.parquet")
n = 1500
# Every 5th row is null; every 3rd non-null row carries the needle. The two
# strides are coprime, so nulls and matches interleave rather than coincide.
values = [None if i % 5 == 0 else (f"row {i} needle here" if i % 3 == 0 else f"row {i} plain")
          for i in range(n)]
table = pa.table({
    "id": pa.array(range(n), pa.int64()),
    "s": pa.array(values, pa.string()),
})
pq.write_table(table, out, use_dictionary=False, row_group_size=500)

# The expected sums, printed so the .ibex file's constants can be re-derived
# rather than trusted.
matched = [i for i in range(n) if i % 5 != 0 and i % 3 == 0]
rest = [i for i in range(n) if i % 5 != 0 and i % 3 != 0]
print(f"like:     rows={len(matched)} id_total={sum(matched)}")
print(f"not like: rows={len(rest)} id_total={sum(rest)}")
