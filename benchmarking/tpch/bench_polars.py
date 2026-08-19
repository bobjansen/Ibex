#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Time the 22 PDS-H queries against Polars, reading the same Parquet
tables benchmarking/tpch/queries/*.ibex use.

Each query body below is a direct, shape-for-shape port of
polars-benchmark's queries/polars/q*.py (the reference "idiomatic Polars"
solution the PDS-H benchmark rules were written for): joins in the
original table order, filters applied where the reference applies them
(usually after the join, not hand-pushed before it), no manual column
pruning or join reordering beyond what the reference itself does. That
keeps this comparison honest under the PDS-H rules ("no inserting new
operations, e.g. no pruning a table before a join", "joins may not be
reordered manually") and lets Polars' own lazy optimizer do the pushdown
work — which is the whole point of comparing against it. Do not
reintroduce hand-tuned .select()/.filter() placement here; that produced
a comparison against ourselves, not against Polars. If a query needs a
non-trivial rewrite (e.g. a correlated subquery), the rewrite should
match what the reference does, not what looks fastest.

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
        .group_by("l_returnflag", "l_linestatus")
        .agg(
            pl.sum("l_quantity").alias("sum_qty"),
            pl.sum("l_extendedprice").alias("sum_base_price"),
            (pl.col("l_extendedprice") * (1.0 - pl.col("l_discount")))
            .sum()
            .alias("sum_disc_price"),
            (
                pl.col("l_extendedprice")
                * (1.0 - pl.col("l_discount"))
                * (1.0 + pl.col("l_tax"))
            )
            .sum()
            .alias("sum_charge"),
            pl.mean("l_quantity").alias("avg_qty"),
            pl.mean("l_extendedprice").alias("avg_price"),
            pl.mean("l_discount").alias("avg_disc"),
            pl.len().alias("count_order"),
        )
        .sort("l_returnflag", "l_linestatus")
    )


def q02() -> pl.LazyFrame:
    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    q1 = (
        scan("part")
        .join(scan("partsupp"), left_on="p_partkey", right_on="ps_partkey")
        .join(scan("supplier"), left_on="ps_suppkey", right_on="s_suppkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .join(scan("region"), left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("p_size") == var1)
        .filter(pl.col("p_type").str.ends_with(var2))
        .filter(pl.col("r_name") == var3)
    )

    return (
        q1.group_by("p_partkey")
        .agg(pl.min("ps_supplycost"))
        .join(q1, on=["p_partkey", "ps_supplycost"])
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
            by=["s_acctbal", "n_name", "s_name", "p_partkey"],
            descending=[True, False, False, False],
        )
        .head(100)
    )


def q03() -> pl.LazyFrame:
    var1 = "BUILDING"
    var2 = date(1995, 3, 15)

    return (
        scan("customer")
        .filter(pl.col("c_mktsegment") == var1)
        .join(scan("orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("o_orderdate") < var2)
        .filter(pl.col("l_shipdate") > var2)
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by("o_orderkey", "o_orderdate", "o_shippriority")
        .agg(pl.sum("revenue"))
        .select(
            pl.col("o_orderkey").alias("l_orderkey"),
            "revenue",
            "o_orderdate",
            "o_shippriority",
        )
        .sort(by=["revenue", "o_orderdate"], descending=[True, False])
        .head(10)
    )


def q04() -> pl.LazyFrame:
    var1 = date(1993, 7, 1)
    var2 = date(1993, 10, 1)

    return (
        # SQL exists translates to semi join in Polars API
        scan("orders")
        .join(
            scan("lineitem").filter(pl.col("l_commitdate") < pl.col("l_receiptdate")),
            left_on="o_orderkey",
            right_on="l_orderkey",
            how="semi",
        )
        .filter(pl.col("o_orderdate").is_between(var1, var2, closed="left"))
        .group_by("o_orderpriority")
        .agg(pl.len().alias("order_count"))
        .sort("o_orderpriority")
    )


def q05() -> pl.LazyFrame:
    var1 = "ASIA"
    var2 = date(1994, 1, 1)
    var3 = date(1995, 1, 1)

    return (
        scan("region")
        .join(scan("nation"), left_on="r_regionkey", right_on="n_regionkey")
        .join(scan("customer"), left_on="n_nationkey", right_on="c_nationkey")
        .join(scan("orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(
            scan("supplier"),
            left_on=["l_suppkey", "n_nationkey"],
            right_on=["s_suppkey", "s_nationkey"],
        )
        .filter(pl.col("r_name") == var1)
        .filter(pl.col("o_orderdate").is_between(var2, var3, closed="left"))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("revenue")
        )
        .group_by("n_name")
        .agg(pl.sum("revenue"))
        .sort(by="revenue", descending=True)
    )


def q06() -> pl.LazyFrame:
    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = 0.05
    var4 = 0.07
    var5 = 24

    return (
        scan("lineitem")
        .filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .filter(pl.col("l_discount").is_between(var3, var4))
        .filter(pl.col("l_quantity") < var5)
        .with_columns(
            (pl.col("l_extendedprice") * pl.col("l_discount")).alias("revenue")
        )
        .select(pl.sum("revenue"))
    )


def q07() -> pl.LazyFrame:
    var1 = "FRANCE"
    var2 = "GERMANY"
    var3 = date(1995, 1, 1)
    var4 = date(1996, 12, 31)

    nations = scan("nation").filter(pl.col("n_name").is_in([var1, var2]))

    return (
        scan("customer")
        .join(nations, left_on="c_nationkey", right_on="n_nationkey")
        .rename({"n_name": "cust_nation"})
        .join(scan("orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(scan("supplier"), left_on="l_suppkey", right_on="s_suppkey")
        .join(nations, left_on="s_nationkey", right_on="n_nationkey")
        .rename({"n_name": "supp_nation"})
        .filter(
            ((pl.col("cust_nation") == var1) & (pl.col("supp_nation") == var2))
            | ((pl.col("cust_nation") == var2) & (pl.col("supp_nation") == var1))
        )
        .filter(pl.col("l_shipdate").is_between(var3, var4))
        .with_columns(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
            pl.col("l_shipdate").dt.year().alias("l_year"),
        )
        .group_by("supp_nation", "cust_nation", "l_year")
        .agg(pl.sum("volume").alias("revenue"))
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )


def q08() -> pl.LazyFrame:
    var1 = "BRAZIL"
    var2 = "AMERICA"
    var3 = "ECONOMY ANODIZED STEEL"
    var4 = date(1995, 1, 1)
    var5 = date(1996, 12, 31)

    n1 = scan("nation").select("n_nationkey", "n_regionkey")
    n2 = scan("nation").select("n_nationkey", "n_name")

    return (
        scan("part")
        .join(scan("lineitem"), left_on="p_partkey", right_on="l_partkey")
        .join(scan("supplier"), left_on="l_suppkey", right_on="s_suppkey")
        .join(scan("orders"), left_on="l_orderkey", right_on="o_orderkey")
        .join(scan("customer"), left_on="o_custkey", right_on="c_custkey")
        .join(n1, left_on="c_nationkey", right_on="n_nationkey")
        .join(scan("region"), left_on="n_regionkey", right_on="r_regionkey")
        .filter(pl.col("r_name") == var2)
        .join(n2, left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(var4, var5))
        .filter(pl.col("p_type") == var3)
        .select(
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).alias("volume"),
            pl.col("n_name").alias("nation"),
        )
        .with_columns(
            pl.when(pl.col("nation") == var1)
            .then(pl.col("volume"))
            .otherwise(0)
            .alias("_tmp")
        )
        .group_by("o_year")
        .agg((pl.sum("_tmp") / pl.sum("volume")).round(2).alias("mkt_share"))
        .sort("o_year")
    )


def q09() -> pl.LazyFrame:
    return (
        scan("part")
        .join(scan("partsupp"), left_on="p_partkey", right_on="ps_partkey")
        .join(scan("supplier"), left_on="ps_suppkey", right_on="s_suppkey")
        .join(
            scan("lineitem"),
            left_on=["p_partkey", "ps_suppkey"],
            right_on=["l_partkey", "l_suppkey"],
        )
        .join(scan("orders"), left_on="l_orderkey", right_on="o_orderkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("p_name").str.contains("green"))
        .select(
            pl.col("n_name").alias("nation"),
            pl.col("o_orderdate").dt.year().alias("o_year"),
            (
                pl.col("l_extendedprice") * (1 - pl.col("l_discount"))
                - pl.col("ps_supplycost") * pl.col("l_quantity")
            ).alias("amount"),
        )
        .group_by("nation", "o_year")
        .agg(pl.sum("amount").round(2).alias("sum_profit"))
        .sort(by=["nation", "o_year"], descending=[False, True])
    )


def q10() -> pl.LazyFrame:
    var1 = date(1993, 10, 1)
    var2 = date(1994, 1, 1)

    return (
        scan("customer")
        .join(scan("orders"), left_on="c_custkey", right_on="o_custkey")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(scan("nation"), left_on="c_nationkey", right_on="n_nationkey")
        .filter(pl.col("o_orderdate").is_between(var1, var2, closed="left"))
        .filter(pl.col("l_returnflag") == "R")
        .group_by(
            "c_custkey",
            "c_name",
            "c_acctbal",
            "c_phone",
            "n_name",
            "c_address",
            "c_comment",
        )
        .agg(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .round(2)
            .alias("revenue")
        )
        .select(
            "c_custkey",
            "c_name",
            "revenue",
            "c_acctbal",
            "n_name",
            "c_address",
            "c_phone",
            "c_comment",
        )
        .sort(by="revenue", descending=True)
        .head(20)
    )


def q11() -> pl.LazyFrame:
    var1 = "GERMANY"
    var2 = 0.0001  # SCALE_FACTOR=1

    q1 = (
        scan("partsupp")
        .join(scan("supplier"), left_on="ps_suppkey", right_on="s_suppkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .filter(pl.col("n_name") == var1)
    )
    q2 = q1.select(
        (pl.col("ps_supplycost") * pl.col("ps_availqty")).sum().round(2).alias("tmp")
        * var2
    )

    return (
        q1.group_by("ps_partkey")
        .agg(
            (pl.col("ps_supplycost") * pl.col("ps_availqty"))
            .sum()
            .round(2)
            .alias("value")
        )
        .join(q2, how="cross")
        .filter(pl.col("value") > pl.col("tmp"))
        .select("ps_partkey", "value")
        .sort("value", descending=True)
    )


def q12() -> pl.LazyFrame:
    var1 = "MAIL"
    var2 = "SHIP"
    var3 = date(1994, 1, 1)
    var4 = date(1995, 1, 1)

    return (
        scan("orders")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .filter(pl.col("l_shipmode").is_in([var1, var2]))
        .filter(pl.col("l_commitdate") < pl.col("l_receiptdate"))
        .filter(pl.col("l_shipdate") < pl.col("l_commitdate"))
        .filter(pl.col("l_receiptdate").is_between(var3, var4, closed="left"))
        .with_columns(
            line_count=pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"])
        )
        .group_by("l_shipmode")
        .agg(
            high_line_count=pl.col("line_count").sum(),
            low_line_count=pl.col("line_count").not_().sum(),
        )
        .sort("l_shipmode")
    )


def q13() -> pl.LazyFrame:
    var1 = "special"
    var2 = "requests"

    orders = scan("orders").filter(
        pl.col("o_comment").str.contains(f"{var1}.*{var2}").not_()
    )
    return (
        scan("customer")
        .join(orders, left_on="c_custkey", right_on="o_custkey", how="left")
        .group_by("c_custkey")
        .agg(pl.col("o_orderkey").count().alias("c_count"))
        .group_by("c_count")
        .len()
        .select(pl.col("c_count"), pl.col("len").alias("custdist"))
        .sort(by=["custdist", "c_count"], descending=[True, True])
    )


def q14() -> pl.LazyFrame:
    var1 = date(1995, 9, 1)
    var2 = date(1995, 10, 1)

    return (
        scan("lineitem")
        .join(scan("part"), left_on="l_partkey", right_on="p_partkey")
        .filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .select(
            (
                100.00
                * pl.when(pl.col("p_type").str.starts_with("PROMO"))
                .then(pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
                .otherwise(0)
                .sum()
                / (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum()
            )
            .round(2)
            .alias("promo_revenue")
        )
    )


def q15() -> pl.LazyFrame:
    var1 = date(1996, 1, 1)
    var2 = date(1996, 4, 1)

    revenue = (
        scan("lineitem")
        .filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .group_by("l_suppkey")
        .agg(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .alias("total_revenue")
        )
        .select(pl.col("l_suppkey").alias("supplier_no"), pl.col("total_revenue"))
    )

    return (
        scan("supplier")
        .join(revenue, left_on="s_suppkey", right_on="supplier_no")
        .filter(pl.col("total_revenue") == pl.col("total_revenue").max())
        .with_columns(pl.col("total_revenue").round(2))
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .sort("s_suppkey")
    )


def q16() -> pl.LazyFrame:
    var1 = "Brand#45"

    supplier = scan("supplier").filter(
        pl.col("s_comment").str.contains(".*Customer.*Complaints.*")
    ).select(pl.col("s_suppkey"), pl.col("s_suppkey").alias("ps_suppkey"))

    return (
        scan("part")
        .join(scan("partsupp"), left_on="p_partkey", right_on="ps_partkey")
        .filter(pl.col("p_brand") != var1)
        .filter(pl.col("p_type").str.contains("MEDIUM POLISHED*").not_())
        .filter(pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9]))
        .join(supplier, left_on="ps_suppkey", right_on="s_suppkey", how="left")
        .filter(pl.col("ps_suppkey_right").is_null())
        .group_by("p_brand", "p_type", "p_size")
        .agg(pl.col("ps_suppkey").n_unique().alias("supplier_cnt"))
        .sort(
            by=["supplier_cnt", "p_brand", "p_type", "p_size"],
            descending=[True, False, False, False],
        )
    )


def q17() -> pl.LazyFrame:
    var1 = "Brand#23"
    var2 = "MED BOX"

    q1 = (
        scan("part")
        .filter(pl.col("p_brand") == var1)
        .filter(pl.col("p_container") == var2)
        .join(scan("lineitem"), how="inner", left_on="p_partkey", right_on="l_partkey")
    )

    return (
        q1.group_by("p_partkey")
        .agg((0.2 * pl.col("l_quantity").mean()).alias("avg_quantity"))
        .select(pl.col("p_partkey").alias("key"), pl.col("avg_quantity"))
        .join(q1, left_on="key", right_on="p_partkey")
        .filter(pl.col("l_quantity") < pl.col("avg_quantity"))
        .select((pl.col("l_extendedprice").sum() / 7.0).round(2).alias("avg_yearly"))
    )


def q18() -> pl.LazyFrame:
    var1 = 300

    q1 = (
        scan("lineitem")
        .group_by("l_orderkey")
        .agg(pl.col("l_quantity").sum().alias("sum_quantity"))
        .filter(pl.col("sum_quantity") > var1)
    )

    return (
        scan("orders")
        .join(q1, left_on="o_orderkey", right_on="l_orderkey", how="semi")
        .join(scan("lineitem"), left_on="o_orderkey", right_on="l_orderkey")
        .join(scan("customer"), left_on="o_custkey", right_on="c_custkey")
        .group_by("c_name", "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
        .agg(pl.col("l_quantity").sum().alias("col6"))
        .select(
            pl.col("c_name"),
            pl.col("o_custkey").alias("c_custkey"),
            pl.col("o_orderkey"),
            pl.col("o_orderdate").alias("o_orderdat"),
            pl.col("o_totalprice"),
            pl.col("col6"),
        )
        .sort(by=["o_totalprice", "o_orderdat"], descending=[True, False])
        .head(100)
    )


def q19() -> pl.LazyFrame:
    return (
        scan("part")
        .join(scan("lineitem"), left_on="p_partkey", right_on="l_partkey")
        .filter(pl.col("l_shipmode").is_in(["AIR", "AIR REG"]))
        .filter(pl.col("l_shipinstruct") == "DELIVER IN PERSON")
        .filter(
            (
                (pl.col("p_brand") == "Brand#12")
                & pl.col("p_container").is_in(
                    ["SM CASE", "SM BOX", "SM PACK", "SM PKG"]
                )
                & (pl.col("l_quantity").is_between(1, 11))
                & (pl.col("p_size").is_between(1, 5))
            )
            | (
                (pl.col("p_brand") == "Brand#23")
                & pl.col("p_container").is_in(
                    ["MED BAG", "MED BOX", "MED PKG", "MED PACK"]
                )
                & (pl.col("l_quantity").is_between(10, 20))
                & (pl.col("p_size").is_between(1, 10))
            )
            | (
                (pl.col("p_brand") == "Brand#34")
                & pl.col("p_container").is_in(
                    ["LG CASE", "LG BOX", "LG PACK", "LG PKG"]
                )
                & (pl.col("l_quantity").is_between(20, 30))
                & (pl.col("p_size").is_between(1, 15))
            )
        )
        .select(
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")))
            .sum()
            .round(2)
            .alias("revenue")
        )
    )


def q20() -> pl.LazyFrame:
    var1 = date(1994, 1, 1)
    var2 = date(1995, 1, 1)
    var3 = "CANADA"
    var4 = "forest"

    q1 = (
        scan("lineitem")
        .filter(pl.col("l_shipdate").is_between(var1, var2, closed="left"))
        .group_by("l_partkey", "l_suppkey")
        .agg((pl.col("l_quantity").sum() * 0.5).alias("sum_quantity"))
    )
    q2 = scan("nation").filter(pl.col("n_name") == var3)
    q3 = scan("supplier").join(q2, left_on="s_nationkey", right_on="n_nationkey")

    return (
        scan("part")
        .filter(pl.col("p_name").str.starts_with(var4))
        .select(pl.col("p_partkey").unique())
        .join(scan("partsupp"), left_on="p_partkey", right_on="ps_partkey")
        .join(
            q1,
            left_on=["ps_suppkey", "p_partkey"],
            right_on=["l_suppkey", "l_partkey"],
        )
        .filter(pl.col("ps_availqty") > pl.col("sum_quantity"))
        .select(pl.col("ps_suppkey").unique())
        .join(q3, left_on="ps_suppkey", right_on="s_suppkey")
        .select("s_name", "s_address")
        .sort("s_name")
    )


def q21() -> pl.LazyFrame:
    var1 = "SAUDI ARABIA"

    q1 = (
        scan("lineitem")
        .group_by("l_orderkey")
        .agg(pl.col("l_suppkey").len().alias("n_supp_by_order"))
        .filter(pl.col("n_supp_by_order") > 1)
        .join(
            scan("lineitem").filter(pl.col("l_receiptdate") > pl.col("l_commitdate")),
            on="l_orderkey",
        )
    )

    return (
        q1.group_by("l_orderkey")
        .agg(pl.col("l_suppkey").len().alias("n_supp_by_order"))
        .join(q1, on="l_orderkey")
        .join(scan("supplier"), left_on="l_suppkey", right_on="s_suppkey")
        .join(scan("nation"), left_on="s_nationkey", right_on="n_nationkey")
        .join(scan("orders"), left_on="l_orderkey", right_on="o_orderkey")
        .filter(pl.col("n_supp_by_order") == 1)
        .filter(pl.col("n_name") == var1)
        .filter(pl.col("o_orderstatus") == "F")
        .group_by("s_name")
        .agg(pl.len().alias("numwait"))
        .sort(by=["numwait", "s_name"], descending=[True, False])
        .head(100)
    )


def q22() -> pl.LazyFrame:
    q1 = (
        scan("customer")
        .with_columns(pl.col("c_phone").str.slice(0, 2).alias("cntrycode"))
        .filter(pl.col("cntrycode").str.contains("13|31|23|29|30|18|17"))
        .select("c_acctbal", "c_custkey", "cntrycode")
    )

    q2 = q1.filter(pl.col("c_acctbal") > 0.0).select(
        pl.col("c_acctbal").mean().alias("avg_acctbal")
    )

    q3 = scan("orders").select(pl.col("o_custkey").unique()).with_columns(
        pl.col("o_custkey").alias("c_custkey")
    )

    return (
        q1.join(q3, on="c_custkey", how="left")
        .filter(pl.col("o_custkey").is_null())
        .join(q2, how="cross")
        .filter(pl.col("c_acctbal") > pl.col("avg_acctbal"))
        .group_by("cntrycode")
        .agg(
            pl.col("c_acctbal").count().alias("numcust"),
            pl.col("c_acctbal").sum().round(2).alias("totacctbal"),
        )
        .sort("cntrycode")
    )


QUERIES = {q: globals()[q] for q in sorted(k for k in globals() if len(k) == 3 and k[0] == "q" and k[1:].isdigit())}


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
