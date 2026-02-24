#!/usr/bin/env python3
"""Pandas equivalent of examples/quant.ibex.

Run:
  python3 examples/quant_pandas.py
  python3 examples/quant_pandas.py --benchmark --min-seconds 1 --scale 50
"""

from __future__ import annotations

import argparse
import pathlib
import time

import pandas as pd


def show(title: str, df: pd.DataFrame, n: int = 10) -> None:
    print(f"\n=== {title} ===")
    print(df.head(n).to_string(index=False))


def scale_ohlcv(ohlcv: pd.DataFrame, scale: int) -> pd.DataFrame:
    if scale <= 1:
        return ohlcv
    return pd.concat([ohlcv] * scale, ignore_index=True)


def run_pipeline(ohlcv: pd.DataFrame, fund: pd.DataFrame, emit: bool) -> None:
    # Keep deterministic first/last semantics for group aggregations.
    ohlcv = ohlcv.sort_values(["symbol", "date"]).reset_index(drop=True)

    # 1. Daily enrichment
    daily = ohlcv.assign(
        ret=(ohlcv["close"] - ohlcv["open"]) / ohlcv["open"],
        range=(ohlcv["high"] - ohlcv["low"]) / ohlcv["open"],
        notional=ohlcv["close"] * ohlcv["volume"],
    )

    # 2. Annual OHLC summary + total notional per symbol
    annual = (
        daily.groupby("symbol", as_index=False)
        .agg(
            open_price=("open", "first"),
            year_high=("high", "max"),
            year_low=("low", "min"),
            close_price=("close", "last"),
            avg_daily_vol=("volume", "mean"),
            total_notional=("notional", "sum"),
        )
    )
    if emit:
        show("2. Annual summary", annual)

    # 3. Sector performance league table
    sector_perf = (
        daily.groupby("sector", as_index=False)
        .agg(
            avg_daily_ret=("ret", "mean"),
            total_notional=("notional", "sum"),
            avg_range=("range", "mean"),
            n_sessions=("ret", "size"),
        )
        .sort_values("avg_daily_ret", ascending=False)
    )
    if emit:
        show("3. Sector performance", sector_perf)

    # 4. Momentum screen
    up_stats = (
        daily[daily["ret"] > 0.0]
        .groupby("symbol", as_index=False)
        .agg(up_days=("ret", "size"), avg_up_ret=("ret", "mean"))
        .sort_values("up_days", ascending=False)
    )
    if emit:
        show("4. Up-day momentum", up_stats)

    # 5. Volatility ranking
    vol_rank = (
        daily.groupby("symbol", as_index=False)
        .agg(
            avg_range=("range", "mean"),
            max_range_day=("range", "max"),
            n_sessions=("range", "size"),
        )
        .sort_values("avg_range", ascending=False)
    )
    if emit:
        show("5. Volatility ranking", vol_rank)

    # 6. High-stress sessions
    stress = (
        daily[daily["range"] > 0.025]
        .groupby("sector", as_index=False)
        .agg(sessions=("range", "size"), avg_vol=("volume", "mean"), avg_range=("range", "mean"))
        .sort_values("sessions", ascending=False)
    )
    if emit:
        show("6. High-stress sessions", stress)

    # 7. Volume spike detection
    avg_vol_base = daily.groupby("symbol", as_index=False).agg(avg_volume=("volume", "mean"))
    enriched_vol = ohlcv.merge(avg_vol_base, on="symbol", how="inner")
    with_ratio = enriched_vol.assign(vol_ratio=enriched_vol["volume"] / enriched_vol["avg_volume"])
    spikes = with_ratio[with_ratio["vol_ratio"] > 1.8]
    spike_summary = (
        spikes.groupby("symbol", as_index=False)
        .agg(spike_days=("vol_ratio", "size"), max_ratio=("vol_ratio", "max"))
        .sort_values("spike_days", ascending=False)
    )
    if emit:
        show("7. Volume spikes", spike_summary)

    # 8. VWAP per symbol (two-step)
    vwap_sums = (
        daily.groupby("symbol", as_index=False)
        .agg(sum_notional=("notional", "sum"), sum_volume=("volume", "sum"))
    )
    vwap = vwap_sums.assign(vwap=vwap_sums["sum_notional"] / vwap_sums["sum_volume"]).sort_values(
        "vwap", ascending=False
    )
    if emit:
        show("8. VWAP", vwap)

    # 9. Join with fundamentals
    enriched = daily.merge(fund, on="symbol", how="inner")

    # 10. Per-symbol annual stats + fundamentals
    sym_stats = (
        enriched.groupby("symbol", as_index=False)
        .agg(
            avg_ret=("ret", "mean"),
            avg_range=("range", "mean"),
            avg_volume=("volume", "mean"),
            pe_ratio=("pe_ratio", "first"),
            beta=("beta", "first"),
            market_cap=("market_cap_bn", "first"),
            div_yield=("div_yield", "first"),
            analyst_rating=("analyst_rating", "first"),
        )
    )
    if emit:
        show("10. Symbol stats", sym_stats)

    # 11. Value x momentum
    if emit:
        show(
            "11. Value x momentum",
            sym_stats[(sym_stats["pe_ratio"] < 25.0) & (sym_stats["avg_ret"] > 0.0)].sort_values(
                "avg_ret", ascending=False
            ),
        )

    # 12. Income + defensive
    if emit:
        show(
            "12. Income + defensive",
            sym_stats[(sym_stats["div_yield"] > 2.0) & (sym_stats["beta"] < 1.0)].sort_values(
                "div_yield", ascending=False
            ),
        )

    # 13. Mega-cap performance
    if emit:
        show("13. Mega-cap", sym_stats[sym_stats["market_cap"] > 400.0].sort_values("avg_ret", ascending=False))

    # 14. Risk-adjusted return proxy
    sharpe_proxy = sym_stats.assign(sharpe_proxy=sym_stats["avg_ret"] / sym_stats["avg_range"]).sort_values(
        "sharpe_proxy", ascending=False
    )
    if emit:
        show("14. Sharpe proxy", sharpe_proxy)

    # 15. Analyst accuracy
    if emit:
        show(
            "15. Top analyst-rated",
            sym_stats[sym_stats["analyst_rating"] > 4.0].sort_values("avg_ret", ascending=False),
        )

    # 16. Sector macro view
    sector_macro = (
        enriched.groupby("sector", as_index=False)
        .agg(
            avg_ret=("ret", "mean"),
            avg_beta=("beta", "mean"),
            avg_pe=("pe_ratio", "mean"),
            total_notl=("notional", "sum"),
            n_sessions=("ret", "size"),
        )
        .sort_values("total_notl", ascending=False)
    )
    if emit:
        show("16. Sector macro view", sector_macro)


def benchmark(ohlcv: pd.DataFrame, fund: pd.DataFrame, min_seconds: float) -> None:
    run_pipeline(ohlcv, fund, emit=False)  # warm-up
    iterations = 0
    start = time.perf_counter()
    while (time.perf_counter() - start) < min_seconds:
        run_pipeline(ohlcv, fund, emit=False)
        iterations += 1
    total = time.perf_counter() - start
    avg_ms = 1000.0 * total / max(iterations, 1)
    print(
        f"pandas benchmark: iterations={iterations}, total={total:.3f}s, "
        f"avg={avg_ms:.2f} ms/iter, rows={len(ohlcv):,}"
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
    ohlcv = pd.read_csv(ohlcv_path)
    fund = pd.read_csv(fundamentals_path)

    if args.scale < 1:
        raise ValueError("--scale must be >= 1")
    ohlcv = scale_ohlcv(ohlcv, args.scale)

    if args.benchmark:
        benchmark(ohlcv, fund, args.min_seconds)
    else:
        run_pipeline(ohlcv, fund, emit=True)


if __name__ == "__main__":
    main()
