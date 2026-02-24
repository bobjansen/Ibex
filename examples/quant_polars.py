#!/usr/bin/env python3
"""Polars equivalent of examples/quant.ibex.

Run:
  python3 examples/quant_polars.py
  python3 examples/quant_polars.py --benchmark --min-seconds 1 --scale 50
"""

from __future__ import annotations

import argparse
import pathlib
import time

import polars as pl


def show(title: str, df: pl.DataFrame, n: int = 10) -> None:
    print(f"\n=== {title} ===")
    print(df.head(n))


def scale_ohlcv(ohlcv: pl.DataFrame, scale: int) -> pl.DataFrame:
    if scale <= 1:
        return ohlcv
    return pl.concat([ohlcv] * scale, rechunk=True)


def run_pipeline(ohlcv: pl.DataFrame, fund: pl.DataFrame, emit: bool) -> None:
    ohlcv = ohlcv.sort(["symbol", "date"])

    # 1. Daily enrichment
    daily = ohlcv.with_columns(
        ret=((pl.col("close") - pl.col("open")) / pl.col("open")),
        range=((pl.col("high") - pl.col("low")) / pl.col("open")),
        notional=(pl.col("close") * pl.col("volume")),
    )

    # 2. Annual OHLC summary + total notional per symbol
    annual = daily.group_by("symbol").agg(
        open_price=pl.col("open").first(),
        year_high=pl.col("high").max(),
        year_low=pl.col("low").min(),
        close_price=pl.col("close").last(),
        avg_daily_vol=pl.col("volume").mean(),
        total_notional=pl.col("notional").sum(),
    )
    if emit:
        show("2. Annual summary", annual)

    # 3. Sector performance league table
    sector_perf = (
        daily.group_by("sector")
        .agg(
            avg_daily_ret=pl.col("ret").mean(),
            total_notional=pl.col("notional").sum(),
            avg_range=pl.col("range").mean(),
            n_sessions=pl.len(),
        )
        .sort("avg_daily_ret", descending=True)
    )
    if emit:
        show("3. Sector performance", sector_perf)

    # 4. Momentum screen
    up_stats = (
        daily.filter(pl.col("ret") > 0.0)
        .group_by("symbol")
        .agg(up_days=pl.len(), avg_up_ret=pl.col("ret").mean())
        .sort("up_days", descending=True)
    )
    if emit:
        show("4. Up-day momentum", up_stats)

    # 5. Volatility ranking
    vol_rank = (
        daily.group_by("symbol")
        .agg(avg_range=pl.col("range").mean(), max_range_day=pl.col("range").max(), n_sessions=pl.len())
        .sort("avg_range", descending=True)
    )
    if emit:
        show("5. Volatility ranking", vol_rank)

    # 6. High-stress sessions
    stress = (
        daily.filter(pl.col("range") > 0.025)
        .group_by("sector")
        .agg(sessions=pl.len(), avg_vol=pl.col("volume").mean(), avg_range=pl.col("range").mean())
        .sort("sessions", descending=True)
    )
    if emit:
        show("6. High-stress sessions", stress)

    # 7. Volume spike detection
    avg_vol_base = daily.group_by("symbol").agg(avg_volume=pl.col("volume").mean())
    enriched_vol = ohlcv.join(avg_vol_base, on="symbol", how="inner")
    with_ratio = enriched_vol.with_columns(vol_ratio=(pl.col("volume") / pl.col("avg_volume")))
    spikes = with_ratio.filter(pl.col("vol_ratio") > 1.8)
    spike_summary = (
        spikes.group_by("symbol")
        .agg(spike_days=pl.len(), max_ratio=pl.col("vol_ratio").max())
        .sort("spike_days", descending=True)
    )
    if emit:
        show("7. Volume spikes", spike_summary)

    # 8. VWAP per symbol (two-step)
    vwap_sums = daily.group_by("symbol").agg(
        sum_notional=pl.col("notional").sum(),
        sum_volume=pl.col("volume").sum(),
    )
    vwap = vwap_sums.with_columns(vwap=(pl.col("sum_notional") / pl.col("sum_volume"))).sort(
        "vwap", descending=True
    )
    if emit:
        show("8. VWAP", vwap)

    # 9. Join with fundamentals
    enriched = daily.join(fund, on="symbol", how="inner")

    # 10. Per-symbol annual stats + fundamentals
    sym_stats = enriched.group_by("symbol").agg(
        avg_ret=pl.col("ret").mean(),
        avg_range=pl.col("range").mean(),
        avg_volume=pl.col("volume").mean(),
        pe_ratio=pl.col("pe_ratio").first(),
        beta=pl.col("beta").first(),
        market_cap=pl.col("market_cap_bn").first(),
        div_yield=pl.col("div_yield").first(),
        analyst_rating=pl.col("analyst_rating").first(),
    )
    if emit:
        show("10. Symbol stats", sym_stats)

    # 11. Value x momentum
    if emit:
        show(
            "11. Value x momentum",
            sym_stats.filter((pl.col("pe_ratio") < 25.0) & (pl.col("avg_ret") > 0.0)).sort("avg_ret", descending=True),
        )

    # 12. Income + defensive
    if emit:
        show(
            "12. Income + defensive",
            sym_stats.filter((pl.col("div_yield") > 2.0) & (pl.col("beta") < 1.0)).sort("div_yield", descending=True),
        )

    # 13. Mega-cap performance
    if emit:
        show("13. Mega-cap", sym_stats.filter(pl.col("market_cap") > 400.0).sort("avg_ret", descending=True))

    # 14. Risk-adjusted return proxy
    sharpe_proxy = sym_stats.with_columns(sharpe_proxy=(pl.col("avg_ret") / pl.col("avg_range"))).sort(
        "sharpe_proxy", descending=True
    )
    if emit:
        show("14. Sharpe proxy", sharpe_proxy)

    # 15. Analyst accuracy
    if emit:
        show(
            "15. Top analyst-rated",
            sym_stats.filter(pl.col("analyst_rating") > 4.0).sort("avg_ret", descending=True),
        )

    # 16. Sector macro view
    sector_macro = (
        enriched.group_by("sector")
        .agg(
            avg_ret=pl.col("ret").mean(),
            avg_beta=pl.col("beta").mean(),
            avg_pe=pl.col("pe_ratio").mean(),
            total_notl=pl.col("notional").sum(),
            n_sessions=pl.len(),
        )
        .sort("total_notl", descending=True)
    )
    if emit:
        show("16. Sector macro view", sector_macro)


def benchmark(ohlcv: pl.DataFrame, fund: pl.DataFrame, min_seconds: float) -> None:
    run_pipeline(ohlcv, fund, emit=False)  # warm-up
    iterations = 0
    start = time.perf_counter()
    while (time.perf_counter() - start) < min_seconds:
        run_pipeline(ohlcv, fund, emit=False)
        iterations += 1
    total = time.perf_counter() - start
    avg_ms = 1000.0 * total / max(iterations, 1)
    print(
        f"polars benchmark: iterations={iterations}, total={total:.3f}s, "
        f"avg={avg_ms:.2f} ms/iter, rows={ohlcv.height:,}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", action="store_true", help="run pipeline repeatedly and report runtime")
    parser.add_argument("--min-seconds", type=float, default=1.0, help="minimum benchmark wall time")
    parser.add_argument("--scale", type=int, default=1, help="duplicate OHLCV rows N times")
    parser.add_argument("--ohlcv-path", type=pathlib.Path, help="override path to ohlcv.csv")
    parser.add_argument("--fundamentals-path", type=pathlib.Path, help="override path to fundamentals.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = pathlib.Path(__file__).resolve().parent
    ohlcv_path = args.ohlcv_path or (root / "data" / "ohlcv.csv")
    fundamentals_path = args.fundamentals_path or (root / "data" / "fundamentals.csv")
    ohlcv = pl.read_csv(ohlcv_path)
    fund = pl.read_csv(fundamentals_path)

    if args.scale < 1:
        raise ValueError("--scale must be >= 1")
    ohlcv = scale_ohlcv(ohlcv, args.scale)

    if args.benchmark:
        benchmark(ohlcv, fund, args.min_seconds)
    else:
        run_pipeline(ohlcv, fund, emit=True)


if __name__ == "__main__":
    main()
