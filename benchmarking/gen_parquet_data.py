#!/usr/bin/env python3
"""Generate a synthetic Parquet fixture for benchmarking read_parquet.

Writes a single file with a handful of numeric/string columns and an
explicit row_group_size, so the file has genuine multi-row-group structure
independent of the reader's own batch size
(ChunkedParquetSourceOperator::kParquetRowsPerChunk = 65536). Row count
defaults large enough to show a real peak-RSS gap between "materialize whole
file" (old read_parquet) and "stream batches" (new chunked read_parquet) —
same order of magnitude as the CSV chunking phase-1 win (100M-row 1BRC:
3.5GB -> 15MB RSS).

Usage:
  uv run benchmarking/gen_parquet_data.py [output_dir]
  uv run benchmarking/gen_parquet_data.py benchmarking/data --rows 20000000
"""
import argparse
import pathlib
import sys
import time

import numpy as np
import pandas as pd

N = 20_000_000
N_SYMBOLS = 252
ROW_GROUP_SIZE = 500_000
SEED = 42


def make_tickers(n: int) -> list[str]:
    """Generate n unique 3-char uppercase ticker strings (AAA, AAB, ...)."""
    tickers = []
    for i in range(n):
        v, s = i, ""
        for _ in range(3):
            s = chr(ord("A") + v % 26) + s
            v //= 26
        tickers.append(s)
    return tickers


def generate(out_dir: pathlib.Path, n: int, row_group_size: int, force: bool) -> pathlib.Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "prices.parquet"
    if path.exists() and not force:
        print(f"  {path} already exists, skipping")
        return path
    if path.exists() and force:
        path.unlink()

    rng = np.random.default_rng(SEED)
    tickers = np.array(make_tickers(N_SYMBOLS))
    sym = tickers[rng.integers(0, N_SYMBOLS, size=n)]
    price = np.round(rng.uniform(1.0, 1000.0, size=n), 4)
    qty = rng.integers(1, 501, size=n)

    t0 = time.perf_counter()
    df = pd.DataFrame({"symbol": sym, "price": price, "qty": qty})
    df.to_parquet(path, engine="pyarrow", row_group_size=row_group_size, index=False)
    mb = path.stat().st_size / 1024 / 1024
    print(f"  wrote {path}  ({n:,} rows, {mb:.0f} MB, row_group_size={row_group_size:,}, "
          f"{time.perf_counter() - t0:.1f}s)")
    return path


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate a synthetic Parquet benchmark dataset.")
    ap.add_argument("output_dir", nargs="?", default=str(pathlib.Path(__file__).parent / "data"))
    ap.add_argument("--rows", type=int, default=N, help="Number of rows to generate")
    ap.add_argument("--row-group-size", type=int, default=ROW_GROUP_SIZE,
                     help="Rows per Parquet row group")
    ap.add_argument("--force", action="store_true", help="Regenerate the file even if it exists")
    args = ap.parse_args()

    if args.rows <= 0:
        print("error: --rows must be > 0", file=sys.stderr)
        sys.exit(1)
    if args.row_group_size <= 0:
        print("error: --row-group-size must be > 0", file=sys.stderr)
        sys.exit(1)

    generate(pathlib.Path(args.output_dir), args.rows, args.row_group_size, args.force)
