#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Build the fixtures for join-key filters over Date, Timestamp and Bool keys.

A join publishes its build keys into the probe scan as `join_key_domain`
integers (Date as days, Timestamp as nanoseconds, Bool as 0/1), and the Parquet
reader tests them inside its decoder only when the stored value already IS that
integer: DATE32 days and TIMESTAMP(NANO). A TIMESTAMP(MICRO) column must decline
the fused scan -- its decode scales every value -- and still filter correctly
after decoding. A wrong domain on either side drops or adds join rows, so the
check sums the payload rather than counting.

`parquet::write` cannot write Timestamp keys from integers, hence pyarrow.

- probe: 100,000 rows in five row groups (above the deferred-probe threshold of
  65,536 rows), key k = i % 1000; every 97th row has a null Date/Timestamp key,
  which must match nothing;
- build: all 1000 keys, of which the query keeps `pick` ({0, 5, ..., 45});
  one of those carries the `true` Bool key.
"""
import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq

out_dir = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "tests/data")
n = 100_000


def key_or_null(i, value):
    return None if i % 97 == 0 else value


ks = [i % 1000 for i in range(n)]
probe = pa.table({
    "k": pa.array([key_or_null(i, k) for i, k in enumerate(ks)], pa.int64()),
    "d": pa.array([key_or_null(i, k * 3) for i, k in enumerate(ks)], pa.date32()),
    "tsn": pa.array([key_or_null(i, k * 1_000_000_007) for i, k in enumerate(ks)],
                    pa.timestamp("ns")),
    "tsu": pa.array([key_or_null(i, k * 1000) for i, k in enumerate(ks)], pa.timestamp("us")),
    "b": pa.array([i % 100 == 0 for i in range(n)], pa.bool_()),
    "payload": pa.array(range(n), pa.int64()),
})
pq.write_table(probe, out_dir / "parquet_join_key_probe_out.parquet", row_group_size=20_000)

build_ks = list(range(0, 50, 5))
# A build table of every key, filtered to `pick` in the query: a build side that
# still carries its whole table covers the key domain and is (rightly) never
# worth deferring a probe for.
all_ks = list(range(1000))
build = pa.table({
    "bk": pa.array(all_ks, pa.int64()),
    "bd": pa.array([k * 3 for k in all_ks], pa.date32()),
    "btsn": pa.array([k * 1_000_000_007 for k in all_ks], pa.timestamp("ns")),
    "btsu": pa.array([k * 1000 for k in all_ks], pa.timestamp("us")),
    "bb": pa.array([k == 0 for k in all_ks], pa.bool_()),
    "pick": pa.array([k in build_ks for k in all_ks], pa.bool_()),
})
pq.write_table(build, out_dir / "parquet_join_key_build_out.parquet")

# Expected answers, printed for the check in scripts/ibex-e2e.sh.
keyed = [i for i in range(n) if ks[i] in set(build_ks) and i % 97 != 0]
print(f"keyed: n={len(keyed)} total={sum(keyed)} keys={sum(ks[i] for i in keyed)}")
flagged = [i for i in range(n) if i % 100 == 0]
print(f"bool: n={len(flagged)} total={sum(flagged)}")
