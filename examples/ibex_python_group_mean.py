# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""The benchmark's mean-by-symbol/day query using the independent package.

Run from the repository root after building the Python bridge:
  PYTHONPATH=build-release/python:python .venv/bin/python examples/ibex_python_group_mean.py
"""

import ibex as pl


def mean_by_symbol_day(prices):
    return prices.group_by("symbol", "day").agg(
        avg_price=pl.col("price").mean()
    )


def main():
    # Replace this dictionary with an already-resident Arrow table for real
    # data. Prepare once; constructing and collecting queries reuses the input.
    prices = pl.DataFrame({
        "symbol": ["AAPL", "AAPL", "AAPL", "MSFT"],
        "day": ["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-02"],
        "price": [100.0, 110.0, 120.0, 200.0],
    }).lazy()
    query = mean_by_symbol_day(prices)
    result = query.collect()
    print(result.to_dict(as_series=False))


if __name__ == "__main__":
    main()
