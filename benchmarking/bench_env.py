#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Shared benchmark-environment reporting.

One job: put the scale factor on the face of every result. The TPC-H queries
read `benchmarking/data/tpch/parquet`, a SYMLINK that run_bench.sh and
check_answers.py repoint at `parquet_sf<N>/`. Three separate sessions have now
compared absolute timings from two different scale factors and concluded the
engine (or the machine) had changed, because no harness printed which dataset
produced the number. A ~2x or ~4x discrepancy between two runs of the same
binary is a scale factor until proven otherwise.
"""
import pathlib

PARQUET_LINK = pathlib.Path(__file__).resolve().parent / "data/tpch/parquet"


def scale_factor_line(link: pathlib.Path = PARQUET_LINK) -> str:
    """A one-line '# dataset: ...' banner naming the dataset actually in use."""
    try:
        target = link.readlink().name if link.is_symlink() else link.name
    except OSError as exc:
        return f"# dataset: UNREADABLE ({exc}) — {link}"
    if not link.exists():
        return f"# dataset: {target} — BROKEN LINK, queries will fail"
    return f"# dataset: {target}  ({link})"
