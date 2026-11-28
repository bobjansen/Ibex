# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Exact Int64 nanosecond storage for the supported temporal plan subset."""

import polars as pl
import pyarrow as pa


NANOS_PER_UNIT = {"ns": 1, "us": 1000, "ms": 1_000_000}
NANOS_PER_DAY = 86_400_000_000_000
# Size of the repeating cycle and of one result unit, both in nanoseconds.
TIME_PARTS = {
    "Hour": (NANOS_PER_DAY, 3_600_000_000_000),
    "Minute": (3_600_000_000_000, 60_000_000_000),
    "Second": (60_000_000_000, 1_000_000_000),
    "Millisecond": (1_000_000_000, 1_000_000),
    "Microsecond": (1_000_000_000, 1_000),
    "Nanosecond": (1_000_000_000, 1),
}
TEMPORAL_TYPES = (pl.Datetime, pl.Duration, pl.Time)


def is_temporal(dtype):
    return dtype.base_type() in TEMPORAL_TYPES


def storage_type(dtype):
    if dtype.base_type() == pl.Datetime:
        return pa.timestamp("ns", tz=dtype.time_zone)
    if dtype == pl.Time:
        return pa.time64("ns")
    return pa.duration("ns")


def prepare_column(column):
    dtype = column.type
    if pa.types.is_time(dtype):
        return column.cast(pa.time64("ns")).cast(pa.int64())
    target = pa.timestamp("ns", tz=dtype.tz) if pa.types.is_timestamp(dtype) else pa.duration("ns")
    try:
        return column.cast(target).cast(pa.int64())
    except pa.ArrowInvalid as error:
        raise NotImplementedError("temporal input is outside the Int64 nanosecond range") from error


def literal_ticks(value, dtype):
    if value is None:
        raise NotImplementedError("null temporal literals are not supported yet")
    if dtype == pl.Time:
        raise NotImplementedError("Time literal precision is not exposed by the Polars visitor")
    if dtype.base_type() == pl.Datetime:
        # Python datetime cannot carry sub-microsecond precision. The pinned
        # visitor has already discarded those bits; do not guess their value.
        if dtype.time_unit == "ns":
            raise NotImplementedError("nanosecond timestamp literals await exact Polars visitor values")
        try:
            return pa.scalar(value).cast(storage_type(dtype)).cast(pa.int64()).as_py()
        except pa.ArrowInvalid as error:
            raise NotImplementedError("timestamp literal is outside the Int64 nanosecond range") from error
    ticks = value * NANOS_PER_UNIT[dtype.time_unit]
    if not -(2**63) <= ticks < 2**63:
        raise NotImplementedError("duration literal is outside the Int64 nanosecond range")
    return ticks
