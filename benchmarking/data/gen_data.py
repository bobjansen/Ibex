#!/usr/bin/env python3
"""Generate synthetic benchmark data.

Outputs (written to the directory of this script by default):
  prices.csv       — symbol (str), price (f64)                    — N rows
  prices_multi.csv — symbol (str), price (f64), day (str)         — N rows
  trades.csv       — symbol (str), price (f64), qty (int64)       — N rows

Distinct groups:
  prices:       252        (by symbol)
  prices_multi: 1008       (by symbol × day, 252 symbols × 4 days)
  trades:       252        (by symbol; qty uniform [1, 500])

Usage:
  uv run data/gen_data.py [output_dir]
"""
import pathlib, sys, time
import numpy as np
import pandas as pd

N         = 4_000_000
N_SYMBOLS = 252
N_DAYS    = 4
SEED      = 42


def make_tickers(n: int) -> list[str]:
    """Generate n unique 3-char uppercase ticker strings (AAA, AAB, …)."""
    tickers = []
    for i in range(n):
        v, s = i, ""
        for _ in range(3):
            s = chr(ord("A") + v % 26) + s
            v //= 26
        tickers.append(s)
    return tickers


def generate(out_dir: pathlib.Path, n: int = N) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tickers = np.array(make_tickers(N_SYMBOLS))
    days    = np.array([f"2024-01-0{d}" for d in range(2, 2 + N_DAYS)])

    # ── prices.csv ───────────────────────────────────────────────────────────
    p = out_dir / "prices.csv"
    if p.exists():
        print(f"  {p} already exists, skipping")
    else:
        rng = np.random.default_rng(SEED)
        sym   = tickers[rng.integers(0, N_SYMBOLS, size=n)]
        price = np.round(rng.uniform(1.0, 1000.0, size=n), 4)
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": sym, "price": price}).to_csv(p, index=False)
        mb = p.stat().st_size / 1024 / 1024
        print(f"  wrote {p}  ({n:,} rows, {mb:.0f} MB, {time.perf_counter()-t0:.1f}s)")

    # ── prices_multi.csv ─────────────────────────────────────────────────────
    pm = out_dir / "prices_multi.csv"
    if pm.exists():
        print(f"  {pm} already exists, skipping")
    else:
        rng2  = np.random.default_rng(SEED + 1)
        sym2  = tickers[rng2.integers(0, N_SYMBOLS, size=n)]
        day   = days[rng2.integers(0, N_DAYS, size=n)]
        price2 = np.round(rng2.uniform(1.0, 1000.0, size=n), 4)
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": sym2, "price": price2, "day": day}).to_csv(pm, index=False)
        mb = pm.stat().st_size / 1024 / 1024
        print(f"  wrote {pm}  ({n:,} rows, {mb:.0f} MB, {time.perf_counter()-t0:.1f}s)")


    # ── trades.csv ───────────────────────────────────────────────────────────
    tr = out_dir / "trades.csv"
    if tr.exists():
        print(f"  {tr} already exists, skipping")
    else:
        rng3  = np.random.default_rng(SEED + 2)
        sym3  = tickers[rng3.integers(0, N_SYMBOLS, size=n)]
        price3 = np.round(rng3.uniform(1.0, 1000.0, size=n), 4)
        qty    = rng3.integers(1, 501, size=n)   # uniform [1, 500]
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": sym3, "price": price3, "qty": qty}).to_csv(tr, index=False)
        mb = tr.stat().st_size / 1024 / 1024
        print(f"  wrote {tr}  ({n:,} rows, {mb:.0f} MB, {time.perf_counter()-t0:.1f}s)")


if __name__ == "__main__":
    out = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(__file__).parent
    generate(out)
