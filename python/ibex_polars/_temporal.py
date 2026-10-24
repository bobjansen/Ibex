# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Exact Int64 nanosecond storage for the supported temporal plan subset."""

import polars as pl
import pyarrow as pa


NANOS_PER_UNIT = {"ns": 1, "us": 1000, "ms": 1_000_000}
TEMPORAL_TYPES = (pl.Datetime, pl.Duration)


def is_temporal(dtype):
    return dtype.base_type() in TEMPORAL_TYPES


def storage_type(dtype):
    if dtype.base_type() == pl.Datetime:
        return pa.timestamp("ns", tz=dtype.time_zone)
    return pa.duration("ns")


def prepare_column(column):
    dtype = column.type
    target = pa.timestamp("ns", tz=dtype.tz) if pa.types.is_timestamp(dtype) else pa.duration("ns")
    try:
        return column.cast(target).cast(pa.int64())
    except pa.ArrowInvalid as error:
        raise NotImplementedError("temporal input is outside the Int64 nanosecond range") from error


def literal_ticks(value, dtype):
    if value is None:
        raise NotImplementedError("null temporal literals are not supported yet")
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
