#!/usr/bin/env python3
"""Time the 6 implemented PDS-H queries against Polars, reading the same
Parquet tables benchmarking/tpch/queries/*.ibex use. Mirrors Ibex's own
query semantics (same filters, join structure, and standard TPC-H
qualification parameters) so the comparison is apples-to-apples.

Usage:
  uv run benchmarking/tpch/bench_polars.py [--warmup N] [--iters N] [--out path]
"""
import argparse
import pathlib
import statistics
import sys
import time
from datetime import date

import polars as pl

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
DATA = IBEX_ROOT / "benchmarking/data/tpch/parquet"


def scan(name: str) -> pl.LazyFrame:
    return pl.scan_parquet(DATA / f"{name}.parquet")


def q01() -> pl.LazyFrame:
    return (
        scan("lineitem")
        .filter(pl.col("l_shipdate") <= date(1998, 9, 2))
        .group_by(["l_returnflag", "l_linestatus"])
        .agg(
            sum_qty=pl.sum("l_quantity"),
            sum_base_price=pl.sum("l_extendedprice"),
            sum_disc_price=(pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum(),
            sum_charge=(
                pl.col("l_extendedprice") * (1 - pl.col("l_discount")) * (1 + pl.col("l_tax"))
            ).sum(),
            avg_qty=pl.mean("l_quantity"),
            avg_price=pl.mean("l_extendedprice"),
            avg_disc=pl.mean("l_discount"),
            count_order=pl.len(),
        )
        .sort(["l_returnflag", "l_linestatus"])
    )


def q03() -> pl.LazyFrame:
    customer = scan("customer").filter(pl.col("c_mktsegment") == "BUILDING").select("c_custkey")
    orders = (
        scan("orders")
        .filter(pl.col("o_orderdate") < date(1995, 3, 15))
        .select(["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"])
    )
    lineitem = (
        scan("lineitem")
        .filter(pl.col("l_shipdate") > date(1995, 3, 15))
        .select(["l_orderkey", "l_extendedprice", "l_discount"])
    )
    return (
        customer.join(orders, left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .group_by(["o_orderkey", "o_orderdate", "o_shippriority"])
        .agg(revenue=(pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum())
        .sort(["revenue", "o_orderdate"], descending=[True, False])
        .head(10)
        .select(
            l_orderkey="o_orderkey", revenue="revenue",
            o_orderdate="o_orderdate", o_shippriority="o_shippriority",
        )
    )


def q05() -> pl.LazyFrame:
    customer = scan("customer").select(["c_custkey", "c_nationkey"])
    orders = (
        scan("orders")
        .filter((pl.col("o_orderdate") >= date(1994, 1, 1)) & (pl.col("o_orderdate") < date(1995, 1, 1)))
        .select(["o_orderkey", "o_custkey"])
    )
    lineitem = scan("lineitem").select(["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"])
    supplier = scan("supplier").select(["s_suppkey", "s_nationkey"])
    nation = scan("nation").select(["n_nationkey", "n_name", "n_regionkey"])
    region = scan("region").filter(pl.col("r_name") == "ASIA").select("r_regionkey")
    return (
        customer.join(orders, left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .filter(pl.col("c_nationkey") == pl.col("s_nationkey"))
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .join(region, left_on="n_regionkey", right_on="r_regionkey")
        .group_by("n_name")
        .agg(revenue=(pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum())
        .sort("revenue", descending=True)
    )


def q06() -> pl.LazyFrame:
    return (
        scan("lineitem")
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1))
            & (pl.col("l_shipdate") < date(1995, 1, 1))
            & (pl.col("l_discount") >= 0.05)
            & (pl.col("l_discount") <= 0.07)
            & (pl.col("l_quantity") < 24)
        )
        .select(revenue=(pl.col("l_extendedprice") * pl.col("l_discount")).sum())
    )


def q10() -> pl.LazyFrame:
    customer = scan("customer").select(
        ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_comment"]
    )
    orders = (
        scan("orders")
        .filter((pl.col("o_orderdate") >= date(1993, 10, 1)) & (pl.col("o_orderdate") < date(1994, 1, 1)))
        .select(["o_orderkey", "o_custkey"])
    )
    lineitem = (
        scan("lineitem")
        .filter(pl.col("l_returnflag") == "R")
        .select(["l_orderkey", "l_extendedprice", "l_discount"])
    )
    nation = scan("nation").select(["n_nationkey", "n_name"])
    return (
        customer.join(orders, left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .join(nation, left_on="c_nationkey", right_on="n_nationkey")
        .group_by(["c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"])
        .agg(revenue=(pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum())
        .sort("revenue", descending=True)
        .head(20)
    )


def q19() -> pl.LazyFrame:
    lineitem = scan("lineitem")
    part = scan("part")
    joined = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
    return joined.filter(
        (pl.col("l_shipinstruct") == "DELIVER IN PERSON")
        & pl.col("l_shipmode").is_in(["AIR", "AIR REG"])
        & (
            (
                (pl.col("p_brand") == "Brand#12")
                & pl.col("p_container").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
                & (pl.col("l_quantity") >= 1) & (pl.col("l_quantity") <= 11)
                & (pl.col("p_size") >= 1) & (pl.col("p_size") <= 5)
            )
            | (
                (pl.col("p_brand") == "Brand#23")
                & pl.col("p_container").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
                & (pl.col("l_quantity") >= 10) & (pl.col("l_quantity") <= 20)
                & (pl.col("p_size") >= 1) & (pl.col("p_size") <= 10)
            )
            | (
                (pl.col("p_brand") == "Brand#34")
                & pl.col("p_container").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
                & (pl.col("l_quantity") >= 20) & (pl.col("l_quantity") <= 30)
                & (pl.col("p_size") >= 1) & (pl.col("p_size") <= 15)
            )
        )
    ).select(revenue=(pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum())


QUERIES = {"q01": q01, "q03": q03, "q05": q05, "q06": q06, "q10": q10, "q19": q19}


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * p
    f, c = int(k), min(int(k) + 1, len(data) - 1)
    return data[f] + (data[c] - data[f]) * (k - f)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iters", type=int, default=5)
    parser.add_argument("--out", default=str(SCRIPT_DIR / "results" / "polars.tsv"))
    args = parser.parse_args()

    if not DATA.exists():
        print(f"error: {DATA} not found — run gen_data.sh then gen_parquet.sh first", file=sys.stderr)
        return 1

    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for qname, build in QUERIES.items():
        print(f"=== polars {qname} ===", file=sys.stderr)
        durations = []
        for i in range(args.warmup + args.iters):
            t0 = time.perf_counter()
            result = build().collect()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if i >= args.warmup:
                durations.append(elapsed_ms)
        avg_ms = statistics.mean(durations)
        print(
            f"  avg={avg_ms:.2f}ms min={min(durations):.2f}ms max={max(durations):.2f}ms rows={result.height}",
            file=sys.stderr,
        )
        rows.append({
            "framework": "polars",
            "query": qname,
            "avg_ms": avg_ms,
            "min_ms": min(durations),
            "max_ms": max(durations),
            "stddev_ms": statistics.pstdev(durations) if len(durations) > 1 else 0.0,
            "p95_ms": percentile(durations, 0.95),
            "p99_ms": percentile(durations, 0.99),
        })

    with open(out_path, "w") as f:
        f.write("framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\n")
        for r in rows:
            f.write(
                f"{r['framework']}\t{r['query']}\t{r['avg_ms']:.3f}\t{r['min_ms']:.3f}\t"
                f"{r['max_ms']:.3f}\t{r['stddev_ms']:.3f}\t{r['p95_ms']:.3f}\t{r['p99_ms']:.3f}\n"
            )
    print(f"results written to {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
