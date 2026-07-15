#!/usr/bin/env python3
"""Time the 9 implemented PDS-H queries against Polars, reading the same
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


def q02() -> pl.LazyFrame:
    # The correlated subquery, decorrelated the same way Ibex lowers it: the
    # minimum European supply cost per part, computed once and joined back.
    european_supply = (
        scan("partsupp")
        .join(scan("supplier"), left_on="ps_suppkey", right_on="s_suppkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .join(scan("region"), left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == "EUROPE")
    )
    minimum_cost = european_supply.group_by("ps_partkey").agg(
        min_supplycost=pl.min("ps_supplycost")
    )
    return (
        scan("part")
        .filter((pl.col("p_size") == 15) & pl.col("p_type").str.ends_with("BRASS"))
        .join(european_supply, left_on="p_partkey", right_on="ps_partkey")
        .join(minimum_cost, left_on="p_partkey", right_on="ps_partkey")
        .filter(pl.col("ps_supplycost") == pl.col("min_supplycost"))
        .select(
            "s_acctbal",
            "s_name",
            "n_name",
            "p_partkey",
            "p_mfgr",
            "s_address",
            "s_phone",
            "s_comment",
        )
        .sort(
            ["s_acctbal", "n_name", "s_name", "p_partkey"],
            descending=[True, False, False, False],
        )
        .head(100)
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


def q04() -> pl.LazyFrame:
    # SQL's correlated `exists` is a semi join, which is how Ibex writes it too.
    orders = scan("orders").filter(
        (pl.col("o_orderdate") >= date(1993, 7, 1)) & (pl.col("o_orderdate") < date(1993, 10, 1))
    )
    late = (
        scan("lineitem")
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .select("l_orderkey")
    )
    return (
        orders.join(late, left_on="o_orderkey", right_on="l_orderkey", how="semi")
        .group_by("o_orderpriority")
        .agg(order_count=pl.len())
        .sort("o_orderpriority")
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


def q11() -> pl.LazyFrame:
    # The uncorrelated scalar subquery: one value, the same for every row. Ibex
    # broadcasts it with a cross join against the subquery's single row; Polars
    # folds it into the predicate the same way.
    german_supply = (
        scan("partsupp")
        .join(scan("supplier"), left_on="ps_suppkey", right_on="s_suppkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == "GERMANY")
    )
    value = pl.col("ps_supplycost") * pl.col("ps_availqty")
    threshold = german_supply.select(threshold=value.sum() * 0.0001)
    return (
        german_supply.group_by("ps_partkey")
        .agg(value=value.sum())
        .join(threshold, how="cross")
        .filter(pl.col("value") > pl.col("threshold"))
        .select("ps_partkey", "value")
        .sort("value", descending=True)
    )


def q17() -> pl.LazyFrame:
    # The correlated subquery, decorrelated the same way Ibex lowers it: 20% of
    # each part's average order quantity, computed once and joined back. The
    # average is over ALL of a part's lineitem rows, not just the Brand#23
    # MED BOX ones, so it is computed before the part filter is applied.
    lineitem = scan("lineitem")
    quantity_limit = lineitem.group_by("l_partkey").agg(
        quantity_limit=0.2 * pl.mean("l_quantity")
    )
    part = scan("part").filter(
        (pl.col("p_brand") == "Brand#23") & (pl.col("p_container") == "MED BOX")
    )
    return (
        lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
        .join(quantity_limit, on="l_partkey")
        .filter(pl.col("l_quantity") < pl.col("quantity_limit"))
        .select(avg_yearly=pl.sum("l_extendedprice") / 7.0)
    )


def q18() -> pl.LazyFrame:
    # SQL's uncorrelated `in` is a semi join, which is how Ibex writes it too.
    lineitem = scan("lineitem")
    big_orders = (
        lineitem.group_by("l_orderkey")
        .agg(order_quantity=pl.sum("l_quantity"))
        .filter(pl.col("order_quantity") > 300)
        .select("l_orderkey")
    )
    return (
        scan("customer")
        .select(["c_custkey", "c_name"])
        .join(scan("orders"), left_on="c_custkey", right_on="o_custkey")
        .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
        .join(big_orders, left_on="o_orderkey", right_on="l_orderkey", how="semi")
        .group_by(["c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice"])
        .agg(sum_quantity=pl.sum("l_quantity"))
        .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
        .head(100)
    )


def q20() -> pl.LazyFrame:
    forest_parts = (
        scan("part").filter(pl.col("p_name").str.starts_with("forest")).select("p_partkey")
    )
    required = (
        scan("lineitem")
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1)) & (pl.col("l_shipdate") < date(1995, 1, 1))
        )
        .group_by(["l_partkey", "l_suppkey"])
        .agg(required=0.5 * pl.sum("l_quantity"))
    )
    target = (
        scan("partsupp")
        .join(forest_parts, left_on="ps_partkey", right_on="p_partkey", how="semi")
        .join(required, left_on=["ps_partkey", "ps_suppkey"], right_on=["l_partkey", "l_suppkey"])
        .filter(pl.col("ps_availqty") > pl.col("required"))
        .select("ps_suppkey")
        .unique()
    )
    return (
        scan("supplier")
        .join(scan("nation").filter(pl.col("n_name") == "CANADA"),
              left_on="s_nationkey", right_on="n_nationkey")
        .join(target, left_on="s_suppkey", right_on="ps_suppkey", how="semi")
        .select(["s_name", "s_address"])
        .sort("s_name")
    )


def q21() -> pl.LazyFrame:
    orders_f = scan("orders").filter(pl.col("o_orderstatus") == "F").select("o_orderkey")
    li_f = scan("lineitem").join(
        orders_f, left_on="l_orderkey", right_on="o_orderkey", how="semi"
    )
    n_sup = (
        li_f.unique(["l_orderkey", "l_suppkey"])
        .group_by("l_orderkey")
        .agg(n_sup=pl.len())
    )
    late = li_f.filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))
    n_late = late.unique(["l_orderkey", "l_suppkey"]).group_by("l_orderkey").agg(n_late=pl.len())
    qualifying = (
        late.select(["l_orderkey", "l_suppkey"])
        .join(n_sup, on="l_orderkey")
        .filter(pl.col("n_sup") > 1)
        .join(n_late, on="l_orderkey")
        .filter(pl.col("n_late") == 1)
        .select("l_suppkey")
    )
    return (
        qualifying.join(scan("supplier"), left_on="l_suppkey", right_on="s_suppkey")
        .join(scan("nation").filter(pl.col("n_name") == "SAUDI ARABIA"),
              left_on="s_nationkey", right_on="n_nationkey")
        .group_by("s_name")
        .agg(numwait=pl.len())
        .sort(["numwait", "s_name"], descending=[True, False])
        .head(100)
    )


def q22() -> pl.LazyFrame:
    codes = ["13", "31", "23", "29", "30", "18", "17"]
    in_scope = (
        scan("customer")
        .with_columns(cntrycode=pl.col("c_phone").str.slice(0, 2))
        .filter(pl.col("cntrycode").is_in(codes))
        .select(["c_custkey", "cntrycode", "c_acctbal"])
    )
    # The uncorrelated threshold: average positive balance among those customers,
    # broadcast with a cross join (one row) exactly as Ibex does.
    threshold = in_scope.filter(pl.col("c_acctbal") > 0).select(threshold=pl.mean("c_acctbal"))
    with_orders = scan("orders").select(c_custkey="o_custkey").unique()
    return (
        in_scope.join(threshold, how="cross")
        .filter(pl.col("c_acctbal") > pl.col("threshold"))
        .join(with_orders, on="c_custkey", how="anti")
        .group_by("cntrycode")
        .agg(numcust=pl.len(), totacctbal=pl.sum("c_acctbal"))
        .sort("cntrycode")
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

def q09() -> pl.LazyFrame:
    part = scan("part").filter(pl.col("p_name").str.contains("green", literal=True)).select(["p_partkey"])
    lineitem = scan("lineitem").select(
        ["l_partkey", "l_suppkey", "l_orderkey", "l_quantity", "l_extendedprice", "l_discount"]
    )
    supplier = scan("supplier").select(["s_suppkey", "s_nationkey"])
    partsupp = scan("partsupp").select(["ps_partkey", "ps_suppkey", "ps_supplycost"])
    orders = scan("orders").select(["o_orderkey", "o_orderdate"])
    nation = scan("nation").select(["n_nationkey", "n_name"])
    return (
        part.join(lineitem, left_on="p_partkey", right_on="l_partkey")
        .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
        .join(partsupp, left_on=["p_partkey", "l_suppkey"], right_on=["ps_partkey", "ps_suppkey"])
        .join(orders, left_on="l_orderkey", right_on="o_orderkey")
        .join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .select(
            nation=pl.col("n_name"),
            o_year=pl.col("o_orderdate").dt.year(),
            amount=pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
            - pl.col("ps_supplycost") * pl.col("l_quantity"),
        )
        .group_by(["nation", "o_year"])
        .agg(sum_profit=pl.col("amount").sum())
        .sort(["nation", "o_year"], descending=[False, True])
    )

def q13() -> pl.LazyFrame:
    customer = scan("customer").select(["c_custkey"])
    orders = (
        scan("orders")
        .filter(~pl.col("o_comment").str.contains("special.*requests"))
        .select(["o_custkey", "o_orderkey"])
    )
    return (
        customer.join(orders, left_on="c_custkey", right_on="o_custkey", how="left")
        .group_by("c_custkey")
        .agg(c_count=pl.col("o_orderkey").count())
        .group_by("c_count")
        .agg(custdist=pl.len())
        .sort(["custdist", "c_count"], descending=[True, True])
    )


def q16() -> pl.LazyFrame:
    excluded_suppliers = (
        scan("supplier")
        .filter(pl.col("s_comment").str.contains("Customer.*Complaints"))
        .select("s_suppkey")
    )
    part = (
        scan("part")
        .filter(
            (pl.col("p_brand") != "Brand#45")
            & ~pl.col("p_type").str.starts_with("MEDIUM POLISHED")
            & pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9])
        )
        .select(["p_partkey", "p_brand", "p_type", "p_size"])
    )
    partsupp = scan("partsupp").select(["ps_partkey", "ps_suppkey"])
    return (
        part.join(partsupp, left_on="p_partkey", right_on="ps_partkey")
        .join(excluded_suppliers, left_on="ps_suppkey", right_on="s_suppkey", how="anti")
        .unique(subset=["p_brand", "p_type", "p_size", "ps_suppkey"])
        .group_by(["p_brand", "p_type", "p_size"])
        .agg(supplier_cnt=pl.len())
        .sort(["supplier_cnt", "p_brand", "p_type", "p_size"], descending=[True, False, False, False])
    )


QUERIES = {"q01": q01, "q02": q02, "q03": q03, "q04": q04, "q05": q05, "q06": q06, "q09": q09, "q10": q10, "q11": q11, "q13": q13, "q16": q16, "q17": q17, "q18": q18, "q19": q19, "q20": q20, "q21": q21, "q22": q22}


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
