#!/usr/bin/env python3
"""Generate synthetic benchmark data.

Outputs (written to the directory of this script by default):
  prices.csv       — symbol (str), price (f64)                    — N rows
  prices_multi.csv — symbol (str), price (f64), day (str)         — N rows
  trades.csv       — symbol (str), price (f64), qty (int64)       — N rows
  events.csv       — user_id (str), amount (f64), quantity (int64),
                     symbol (str)                                  — N rows
  users.csv        — user_id (str), tier (int64), user_segment (str),
                     user_tier_multiplier (f64)                    — 100 000 rows

Distinct groups:
  prices:       252        (by symbol)
  prices_multi: 1008       (by symbol × day, 252 symbols × 4 days)
  trades:       252        (by symbol; qty uniform [1, 500])
  events:       100 000    (by user_id — exceeds 4096 categorical threshold,
                            stays as Column<std::string> to stress string gather
                            and high-cardinality group-by)
  users:        100 000    (one row per distinct events user_id; the dimension
                            for the high-cardinality inner-join benchmark. Fixed
                            size — independent of N.)

Usage:
  uv run data/gen_data.py [output_dir]
  uv run data/gen_data.py data/scales/1000000 --rows 1000000
"""
import argparse
import pathlib
import sys
import time
import numpy as np
import pandas as pd

N         = 4_000_000
N_SYMBOLS = 252
N_DAYS    = 4
N_USERS   = 100_000    # intentionally > 4096 categorical threshold
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


def generate(out_dir: pathlib.Path, n: int = N, force: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tickers = np.array(make_tickers(N_SYMBOLS))
    days    = np.array([f"2024-01-0{d}" for d in range(2, 2 + N_DAYS)])

    # ── prices.csv ───────────────────────────────────────────────────────────
    p = out_dir / "prices.csv"
    if p.exists() and not force:
        print(f"  {p} already exists, skipping")
    else:
        if p.exists() and force:
            p.unlink()
        rng = np.random.default_rng(SEED)
        sym   = tickers[rng.integers(0, N_SYMBOLS, size=n)]
        price = np.round(rng.uniform(1.0, 1000.0, size=n), 4)
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": sym, "price": price}).to_csv(p, index=False)
        mb = p.stat().st_size / 1024 / 1024
        print(f"  wrote {p}  ({n:,} rows, {mb:.0f} MB, {time.perf_counter()-t0:.1f}s)")

    # ── prices_multi.csv ─────────────────────────────────────────────────────
    pm = out_dir / "prices_multi.csv"
    if pm.exists() and not force:
        print(f"  {pm} already exists, skipping")
    else:
        if pm.exists() and force:
            pm.unlink()
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
    if tr.exists() and not force:
        print(f"  {tr} already exists, skipping")
    else:
        if tr.exists() and force:
            tr.unlink()
        rng3  = np.random.default_rng(SEED + 2)
        sym3  = tickers[rng3.integers(0, N_SYMBOLS, size=n)]
        price3 = np.round(rng3.uniform(1.0, 1000.0, size=n), 4)
        qty    = rng3.integers(1, 501, size=n)   # uniform [1, 500]
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": sym3, "price": price3, "qty": qty}).to_csv(tr, index=False)
        mb = tr.stat().st_size / 1024 / 1024
        print(f"  wrote {tr}  ({n:,} rows, {mb:.0f} MB, {time.perf_counter()-t0:.1f}s)")


    # ── lookup.csv ───────────────────────────────────────────────────────────
    lu = out_dir / "lookup.csv"
    if lu.exists() and not force:
        print(f"  {lu} already exists, skipping")
    else:
        if lu.exists() and force:
            lu.unlink()
        # Half the symbols (first 126 of 252) have sector info; the rest produce null.
        sectors = np.array(["Tech", "Finance", "Energy", "Health", "Consumer"])
        half = tickers[:N_SYMBOLS // 2]
        rng5 = np.random.default_rng(SEED + 4)
        sector_arr = sectors[rng5.integers(0, len(sectors), size=len(half))]
        t0 = time.perf_counter()
        pd.DataFrame({"symbol": half, "sector": sector_arr}).to_csv(lu, index=False)
        mb = lu.stat().st_size / 1024 / 1024
        print(f"  wrote {lu}  ({len(half)} rows, {mb:.3f} MB, {time.perf_counter()-t0:.3f}s)")

    # ── events.csv ───────────────────────────────────────────────────────────
    ev = out_dir / "events.csv"
    if ev.exists() and not force:
        print(f"  {ev} already exists, skipping")
    else:
        if ev.exists() and force:
            ev.unlink()
        rng4   = np.random.default_rng(SEED + 3)
        # "user_000001" … "user_100000"  (11 chars, SSO-friendly, NOT categorical)
        pool   = np.array([f"user_{i:06d}" for i in range(1, N_USERS + 1)])
        users  = pool[rng4.integers(0, N_USERS, size=n)]
        amount = np.round(rng4.uniform(1.0, 1000.0, size=n), 4)
        qty    = rng4.integers(1, 101, size=n)          # uniform [1, 100]
        # symbol: 252 distinct tickers — the group/partition key for the
        # join-anchored pipeline benchmarks (join_update_group, join_filter_rank).
        symbol_ev = tickers[rng4.integers(0, N_SYMBOLS, size=n)]
        t0 = time.perf_counter()
        pd.DataFrame({"user_id": users, "amount": amount, "quantity": qty,
                      "symbol": symbol_ev}).to_csv(ev, index=False)
        mb = ev.stat().st_size / 1024 / 1024
        print(f"  wrote {ev}  ({n:,} rows, {N_USERS:,} distinct users, {mb:.0f} MB, "
              f"{time.perf_counter()-t0:.1f}s)")

    # ── users.csv ────────────────────────────────────────────────────────────
    # Dimension table for the high-cardinality inner-join benchmark: exactly one
    # row per distinct events user_id (so events ⋈ users is one row per event).
    # Fixed 100K rows regardless of N — it is the build side, not the fact table.
    us = out_dir / "users.csv"
    if us.exists() and not force:
        print(f"  {us} already exists, skipping")
    else:
        if us.exists() and force:
            us.unlink()
        pool = np.array([f"user_{i:06d}" for i in range(1, N_USERS + 1)])
        rng6 = np.random.default_rng(SEED + 5)
        tier = rng6.integers(1, 6, size=N_USERS)   # uniform tier [1, 5]
        # Dimension attributes for the join-anchored pipeline benchmarks:
        #   user_segment         — string class; ~1/3 "premium" (filter target)
        #   user_tier_multiplier — revenue weight derived from tier (1.25 .. 2.25)
        segments = np.array(["premium", "standard", "basic"])
        user_segment = segments[rng6.integers(0, len(segments), size=N_USERS)]
        user_tier_multiplier = np.round(1.0 + tier * 0.25, 4)
        t0 = time.perf_counter()
        pd.DataFrame({"user_id": pool, "tier": tier, "user_segment": user_segment,
                      "user_tier_multiplier": user_tier_multiplier}).to_csv(us, index=False)
        mb = us.stat().st_size / 1024 / 1024
        print(f"  wrote {us}  ({N_USERS:,} rows, {mb:.1f} MB, {time.perf_counter()-t0:.1f}s)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate synthetic benchmark CSV datasets.")
    ap.add_argument("output_dir", nargs="?", default=str(pathlib.Path(__file__).parent))
    ap.add_argument("--rows", type=int, default=N, help="Number of rows per generated dataset")
    ap.add_argument("--force", action="store_true", help="Regenerate files even if they exist")
    args = ap.parse_args()

    out = pathlib.Path(args.output_dir)
    if args.rows <= 0:
        print("error: --rows must be > 0", file=sys.stderr)
        sys.exit(1)
    generate(out, n=args.rows, force=args.force)
