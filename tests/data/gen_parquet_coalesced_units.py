#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build the fixture for streaming units that span several row groups.

The Parquet reader coalesces small row groups into decode units of about
rows/64 (capped at 1M) so every downstream breaker sees chunks big enough to
fan out over. That makes a unit a RANGE of row groups, which every per-unit
path has to honor: the decode, a selection intersected with the unit, and the
fused string filter's page stripes.

`write_parquet` cannot write row groups this small, hence pyarrow:

- 4,480,000 rows in row groups of 14,000 gives 320 groups; the unit target is
  4,480,000 / 64 = 70,000 rows, so five groups per unit and exactly 64 units;
- 70,000 rows clears the parallel-decode floor (65,536), so the fused `like`
  scan stripes the pages of a multi-group unit across the pool — the path that
  used to decline whenever a unit held more than one row group;
- `s` is PLAIN with a page index and small pages, which is what the stripe
  path needs;
- `x` holds multiples of 0.25, so its sums are exact in any order.
"""
import pathlib
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

out = pathlib.Path(sys.argv[1] if len(sys.argv) > 1
                   else "tests/data/parquet_coalesced_units_out.parquet")
n = 4_480_000
ids = np.arange(n, dtype=np.int64)
needle = ids % 3 == 0
table = pa.table({
    "id": pa.array(ids),
    "k": pa.array(ids % 7),
    "x": pa.array((ids % 4) * 0.25),
    "s": pa.array(np.where(needle, "a needle here", "plain"), pa.string()),
})
pq.write_table(table, out, use_dictionary=False, row_group_size=14_000,
               write_page_index=True, data_page_size=8_192)

# The expected numbers, printed so the e2e constants can be re-derived rather
# than trusted.
print(f"all: rows={n} id_total={int(ids.sum())} id_hi={n - 1} "
      f"x_total={float(((ids % 4) * 0.25).sum())}")
hit = needle & (ids >= 12_345)
for k in range(7):
    sel = ids[hit & (ids % 7 == k)]
    print(f"k={k}: hits={sel.size} hit_total={int(sel.sum())}")
