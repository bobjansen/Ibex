#!/usr/bin/env python3
"""Generate synthetic market data for Ibex quant finance demo.

Outputs (written to the directory of this script):
  ohlcv.csv        — symbol, sector, date, open, high, low, close, volume
  fundamentals.csv — symbol, market_cap_bn, beta, pe_ratio, div_yield, analyst_rating

30 symbols × 252 trading days ≈ 7,560 OHLCV rows + 30 fundamentals rows.
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

# Static fundamentals: (market_cap_bn, beta, pe_ratio, div_yield_pct, analyst_rating_1to5)
FUNDAMENTALS = {
    "AAPL":  (2800, 1.20, 29.0, 0.5, 4.2),
    "MSFT":  (2600, 0.90, 35.0, 0.8, 4.5),
    "AMZN":  (1500, 1.30, 60.0, 0.0, 4.3),
    "NVDA":  (1200, 1.70, 65.0, 0.1, 4.6),
    "GOOGL": (1600, 1.10, 25.0, 0.0, 4.1),
    "META":  ( 900, 1.40, 20.0, 0.0, 3.9),
    "TSLA":  ( 700, 2.00, 75.0, 0.0, 3.2),
    "BRK":   ( 800, 0.85, 15.0, 0.0, 4.0),
    "LLY":   ( 450, 0.50, 55.0, 1.0, 4.4),
    "JPM":   ( 450, 1.10, 11.0, 2.5, 3.8),
    "V":     ( 500, 0.95, 30.0, 0.7, 4.2),
    "UNH":   ( 450, 0.60, 22.0, 1.3, 4.1),
    "XOM":   ( 400, 1.20, 10.0, 3.5, 3.5),
    "MA":    ( 400, 1.00, 32.0, 0.6, 4.3),
    "AVGO":  ( 350, 1.30, 28.0, 2.0, 4.2),
    "PG":    ( 350, 0.55, 24.0, 2.4, 3.7),
    "HD":    ( 300, 1.05, 19.0, 2.6, 3.8),
    "MRK":   ( 270, 0.65, 16.0, 2.8, 3.9),
    "COST":  ( 260, 0.80, 38.0, 0.7, 4.0),
    "ABBV":  ( 250, 0.60, 18.0, 4.2, 3.6),
    "CVX":   ( 280, 1.15, 11.0, 3.8, 3.4),
    "KO":    ( 260, 0.55, 23.0, 3.1, 3.5),
    "WMT":   ( 420, 0.60, 27.0, 1.5, 3.9),
    "BAC":   ( 280, 1.35, 12.0, 2.2, 3.6),
    "CRM":   ( 200, 1.25, 45.0, 0.0, 3.7),
    "PEP":   ( 230, 0.60, 26.0, 2.8, 3.6),
    "TMO":   ( 200, 0.85, 30.0, 0.3, 4.0),
    "ACN":   ( 190, 1.05, 28.0, 1.4, 3.8),
    "AMD":   ( 180, 1.90, 55.0, 0.0, 4.1),
    "DHR":   ( 170, 0.80, 32.0, 0.4, 3.9),
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


def make_fundamentals() -> pd.DataFrame:
    rows = []
    for sym in TICKERS:
        market_cap, beta, pe, div, rating = FUNDAMENTALS[sym]
        rows.append({
            "symbol":          sym,
            "market_cap_bn":   float(market_cap),
            "beta":            beta,
            "pe_ratio":        pe,
            "div_yield":       div,
            "analyst_rating":  rating,
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

    fund = make_fundamentals()
    fpath = out_dir / "fundamentals.csv"
    fund.to_csv(fpath, index=False)
    print(f"wrote {fpath}  ({len(fund)} rows)")
    print(f"  market cap range: {fund['market_cap_bn'].min():.0f}bn – {fund['market_cap_bn'].max():.0f}bn")


if __name__ == "__main__":
    main()
