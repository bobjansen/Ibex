# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Compare native and Polars Parquet sinks and resident writers on warmed inputs."""

import argparse
from pathlib import Path
import resource
from statistics import median
import tempfile
from time import perf_counter

import polars as pl
import pyarrow as pa

import ibex_pyarrow
from ibex_polars import IbexEngine
from ibex_polars._engine import _native_parquet_schema


def cpu_seconds():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def measure(label, methods, paths, repeats):
    samples = [[], []]
    for method in methods:
        method()
    for i in range(repeats):
        for side in ((0, 1) if i % 2 == 0 else (1, 0)):
            wall, cpu = perf_counter(), cpu_seconds()
            methods[side]()
            samples[side].append((perf_counter() - wall, cpu_seconds() - cpu))
    for name, sample, path in zip(("Ibex", "Polars"), samples, paths, strict=True):
        wall, cpu = (1000 * median(values) for values in zip(*sample, strict=True))
        print(f"{label:28s} {name:6s} {wall:8.2f} ms wall  {cpu:8.2f} ms CPU  "
              f"{path.stat().st_size:12d} bytes", flush=True)


def bind(frame):
    table = frame.to_arrow()
    columns = []
    for column in table.columns:
        if pa.types.is_dictionary(column.type):
            column = column.cast(pa.dictionary(pa.int32(), pa.string()))
        elif pa.types.is_large_string(column.type):
            column = column.cast(pa.string())
        elif pa.types.is_unsigned_integer(column.type):
            column = column.cast(pa.int64())
        columns.append(column)
    return ibex_pyarrow.bind_table(pa.table(columns, names=table.column_names))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trades", type=Path, help="Parquet trades table from the IbexEngine demo")
    parser.add_argument("--lineitem", type=Path, help="Optional PDS-H lineitem Parquet file")
    parser.add_argument("--repeats", type=int, default=8)
    args = parser.parse_args()
    if args.repeats < 2 or args.repeats % 2:
        parser.error("--repeats must be a positive even number")

    trades = pl.scan_parquet(args.trades)
    value = pl.col("price") * pl.col("qty")
    queries = {
        "wide": trades.with_columns(notional=value, cheap=pl.col("price") < 30)
                      .filter(pl.col("qty") > 10),
        "filtered aggregate": trades.filter(pl.col("day").is_between(50, 99),
                                            pl.col("price") < 60, pl.col("qty") < 300)
                                    .select(revenue=value.sum(), n=pl.len()),
        "many groups": trades.group_by("symbol", "day")
                             .agg(avg_price=pl.mean("price"), hi=pl.max("price"),
                                  lo=pl.min("price"), volume=pl.sum("qty"))
                             .sort("symbol", "day"),
    }
    if args.lineitem:
        lineitem = pl.scan_parquet(args.lineitem)
        comment = "comments" if "comments" in lineitem.collect_schema() else "l_comment"
        queries["PDS-H projection"] = lineitem.select(
            "l_orderkey", "l_quantity", "l_extendedprice", comment).head(6_000_000)

    engine = IbexEngine()
    with tempfile.TemporaryDirectory(prefix="ibex-parquet-bench-") as directory:
        native, polars = (Path(directory) / filename for filename in ("native.parquet", "polars.parquet"))
        for name, query in queries.items():
            engine.prepare(query)
            measure(f"{name} sink", (
                lambda: query.sink_parquet(native, engine=engine),
                lambda: query.sink_parquet(polars, engine="streaming"),
            ), (native, polars), args.repeats)
            if name == "PDS-H projection":
                continue
            frame = pl.read_parquet(polars)
            bindings = {"t": bind(frame)}
            schema = _native_parquet_schema(frame.schema)
            measure(f"{name} writer", (
                lambda: ibex_pyarrow.execute_pipeline_parquet(
                    "t;", str(native), schema.__arrow_c_schema__(), tables=bindings),
                lambda: frame.lazy().sink_parquet(polars, engine="streaming"),
            ), (native, polars), args.repeats)


if __name__ == "__main__":
    main()
