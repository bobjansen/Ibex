#!/usr/bin/env python3
"""Generate synthetic OHLCV data for quant finance examples.

Outputs (written to the directory of this script):
  ohlcv.csv   — symbol, date, open, high, low, close, volume

30 symbols × 252 trading days ≈ 7,560 rows.
Prices follow correlated GBM; volumes follow log-normal.
"""

import pathlib, sys
import numpy as np
import pandas as pd

TICKERS = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL",
    "META", "TSLA", "BRK",  "LLY",  "JPM",
    "V",    "UNH",  "XOM",  "MA",   "AVGO",
    "PG",   "HD",   "MRK",  "COST", "ABBV",
    "CVX",  "KO",   "WMT",  "BAC",  "CRM",
    "PEP",  "TMO",  "ACN",  "AMD",  "DHR",
]

SECTORS = {
    "AAPL": "Tech",   "MSFT": "Tech",   "AMZN": "Tech",   "NVDA": "Tech",  "GOOGL": "Tech",
    "META": "Tech",   "TSLA": "Auto",   "BRK":  "Fin",    "LLY":  "Health","JPM":   "Fin",
    "V":    "Fin",    "UNH":  "Health", "XOM":  "Energy", "MA":   "Fin",   "AVGO":  "Tech",
    "PG":   "Staples","HD":   "Retail", "MRK":  "Health", "COST": "Retail","ABBV":  "Health",
    "CVX":  "Energy", "KO":   "Staples","WMT":  "Retail", "BAC":  "Fin",   "CRM":  "Tech",
    "PEP":  "Staples","TMO":  "Health", "ACN":  "Tech",   "AMD":  "Tech",  "DHR":  "Health",
}

N_DAYS   = 252
SEED     = 99
BASE_VOL = 0.015    # annualised daily vol ≈ 24%
CORR     = 0.35     # pairwise correlation (market factor)


def make_trading_dates(n: int) -> list[str]:
    dates = pd.bdate_range("2023-01-02", periods=n)
    return [d.strftime("%Y-%m-%d") for d in dates]


def simulate_prices(rng, n_sym: int, n_days: int) -> np.ndarray:
    """Correlated GBM: shape (n_sym, n_days+1), returns closing prices."""
    cov = np.full((n_sym, n_sym), CORR * BASE_VOL**2)
    np.fill_diagonal(cov, BASE_VOL**2)
    # Add idiosyncratic vol (each stock slightly different)
    idio = rng.uniform(0.8, 1.4, size=n_sym)
    cov = (cov * np.outer(idio, idio))
    np.fill_diagonal(cov, (BASE_VOL * idio)**2)

    returns = rng.multivariate_normal(np.zeros(n_sym), cov / 252, size=n_days)
    # Cumulative returns starting from random base prices [50, 1500]
    base = rng.uniform(50.0, 1500.0, size=n_sym)
    prices = np.zeros((n_sym, n_days + 1))
    prices[:, 0] = base
    for t in range(n_days):
        prices[:, t + 1] = prices[:, t] * np.exp(returns[t])
    return np.round(prices, 2)


def make_ohlcv(rng, prices: np.ndarray, n_sym: int, n_days: int) -> pd.DataFrame:
    rows = []
    dates = make_trading_dates(n_days)
    base_volumes = rng.lognormal(mean=14.5, sigma=1.2, size=n_sym).astype(int)  # ~2M shares/day

    for d in range(n_days):
        day_vol_factor = rng.lognormal(mean=0.0, sigma=0.25, size=n_sym)
        for s in range(n_sym):
            o = prices[s, d]
            c = prices[s, d + 1]

            # Intraday range: half-normal noise around the open-close move
            intraday_vol = abs(c - o) * rng.uniform(0.5, 2.5)
            h = max(o, c) + abs(rng.normal(0, intraday_vol))
            l = min(o, c) - abs(rng.normal(0, intraday_vol))
            h = round(max(h, o, c), 2)
            l = round(min(l, o, c), 2)

            vol = int(base_volumes[s] * day_vol_factor[s])
            rows.append({
                "symbol": TICKERS[s],
                "sector": SECTORS[TICKERS[s]],
                "date":   dates[d],
                "open":   round(o, 2),
                "high":   h,
                "low":    l,
                "close":  round(c, 2),
                "volume": vol,
            })

    return pd.DataFrame(rows)


def main() -> None:
    out_dir = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(__file__).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(SEED)
    n_sym  = len(TICKERS)
    prices = simulate_prices(rng, n_sym, N_DAYS)
    df     = make_ohlcv(rng, prices, n_sym, N_DAYS)

    path = out_dir / "ohlcv.csv"
    df.to_csv(path, index=False)
    mb = path.stat().st_size / 1024 / 1024
    print(f"wrote {path}  ({len(df):,} rows, {mb:.2f} MB)")
    print(f"  symbols: {n_sym}, days: {N_DAYS}")
    print(f"  price range: {df['close'].min():.2f} – {df['close'].max():.2f}")
    print(f"  avg daily volume: {df['volume'].mean():,.0f}")


if __name__ == "__main__":
    main()
