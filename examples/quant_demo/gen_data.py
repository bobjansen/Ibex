#!/usr/bin/env python3
"""Generate synthetic tick data + a reference table for the quant demo.

Outputs:
  examples/quant_demo/data/ticks.csv      — ~100K ticks across 6 symbols, 1 trading day
  examples/quant_demo/data/reference.csv  — one row per symbol with sector + vol regime

The data is deliberately small enough to commit-friendly inspect, large enough
that the pipeline timings are representative.
"""

from __future__ import annotations

import argparse
import pathlib

import numpy as np

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "META", "NVDA"]
SECTORS = {
    "AAPL": "tech",
    "MSFT": "tech",
    "GOOG": "tech",
    "AMZN": "consumer",
    "META": "tech",
    "NVDA": "semis",
}
# Realistic-ish daily vol per name, expressed as σ of 1-minute log returns.
VOL_REGIME = {
    "AAPL": 0.0012,
    "MSFT": 0.0011,
    "GOOG": 0.0014,
    "AMZN": 0.0017,
    "META": 0.0019,
    "NVDA": 0.0028,
}
START_PRICE = {
    "AAPL": 180.0,
    "MSFT": 415.0,
    "GOOG": 175.0,
    "AMZN": 185.0,
    "META": 510.0,
    "NVDA": 1100.0,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows-per-symbol", type=int, default=18_000,
                    help="ticks per symbol over the synthetic trading day")
    ap.add_argument("--seed", type=int, default=20260428)
    ap.add_argument("--out", type=pathlib.Path,
                    default=pathlib.Path(__file__).resolve().parent / "data")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    # 6.5h trading day, ts in ns from market open (09:30 UTC for simplicity).
    market_open_ns = 9 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000
    day_length_ns = int(6.5 * 3600 * 1e9)

    rows: list[tuple[int, str, float, int]] = []
    for symbol in SYMBOLS:
        n = args.rows_per_symbol
        # Geometric Brownian motion in log-prices.
        sigma = VOL_REGIME[symbol]
        log_rets = rng.normal(loc=0.0, scale=sigma, size=n)
        log_prices = np.log(START_PRICE[symbol]) + np.cumsum(log_rets)
        prices = np.exp(log_prices)

        # Tick times: monotonic within symbol, jittered around even spacing.
        even_step = day_length_ns // n
        ts = market_open_ns + np.cumsum(rng.integers(1, 2 * even_step, size=n))
        # Cap at end-of-day so timestamps stay sortable.
        ts = np.minimum(ts, market_open_ns + day_length_ns - 1)
        ts = np.sort(ts)

        # Lognormal share size with the heavy right tail you'd see on a tape.
        sizes = rng.lognormal(mean=4.5, sigma=0.9, size=n).astype(np.int64)

        rows.extend(zip(ts.tolist(), [symbol] * n, prices.tolist(), sizes.tolist()))

    # Sort globally by timestamp so an asof join sees a single monotonic stream.
    rows.sort(key=lambda r: r[0])

    ticks_path = args.out / "ticks.csv"
    with ticks_path.open("w") as f:
        f.write("ts,symbol,price,size\n")
        for ts, symbol, price, size in rows:
            f.write(f"{ts},{symbol},{price:.4f},{size}\n")

    ref_path = args.out / "reference.csv"
    with ref_path.open("w") as f:
        # ts column makes this a TimeFrame with a single point at market open
        # — asof joins will see every tick fall at-or-after this row.
        f.write("ts,symbol,sector,vol_regime\n")
        for symbol in SYMBOLS:
            f.write(f"{market_open_ns},{symbol},{SECTORS[symbol]},{VOL_REGIME[symbol]:.6f}\n")

    n_ticks = len(rows)
    size_mb = ticks_path.stat().st_size / (1024 * 1024)
    print(f"wrote {ticks_path}  ({n_ticks:,} ticks, {size_mb:.1f} MB)")
    print(f"wrote {ref_path}    ({len(SYMBOLS)} reference rows)")


if __name__ == "__main__":
    main()
