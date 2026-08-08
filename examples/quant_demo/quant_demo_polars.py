#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Quant-demo equivalent in Polars + scikit-learn.

Identical pipeline shape to quant_demo.ibex:
  ticks.csv → resample 1-minute OHLC by symbol → asof-join reference →
  feature engineering → ridge fit → save coefficients + features +
  predictions.

Used for the side-by-side LOC comparison on the landing page.
"""

from __future__ import annotations

import pathlib

import numpy as np
import polars as pl
from sklearn.linear_model import Ridge

ROOT = pathlib.Path(__file__).resolve().parent
DATA = ROOT / "data"


def main() -> None:
    ticks = pl.read_csv(DATA / "ticks.csv").with_columns(
        pl.col("ts").cast(pl.Datetime("ns"))
    )
    reference = pl.read_csv(DATA / "reference.csv").select(["symbol", "sector", "vol_regime"])

    # Ibex's `by symbol` grouping emits groups in first-appearance order, not
    # alphabetically. Model an Enum on that order so the output CSVs line up
    # row-for-row with the Ibex ones.
    symbol_order = ticks["symbol"].unique(maintain_order=True).to_list()
    symbol_enum = pl.Enum(symbol_order)
    ticks = ticks.with_columns(pl.col("symbol").cast(symbol_enum))
    reference = reference.with_columns(pl.col("symbol").cast(symbol_enum))

    # ── 1. Resample ticks to 1-minute OHLC + traded size, per symbol ───────
    #    `maintain_order=True` is belt-and-braces: gen_data.py now emits strictly
    #    increasing per-symbol timestamps, so `first`/`last` within a bar are
    #    unambiguous. It matters if the data is ever regenerated with tied ts —
    #    Polars' sort is unstable by default, and ties would then silently break
    #    differently here than in Ibex.
    bars = (
        ticks.sort(["symbol", "ts"], maintain_order=True)
        .group_by_dynamic("ts", every="1m", group_by="symbol")
        .agg(
            pl.col("price").first().alias("open"),
            pl.col("price").max().alias("high"),
            pl.col("price").min().alias("low"),
            pl.col("price").last().alias("close"),
            pl.col("size").sum().alias("bar_size"),
        )
    )

    # ── 2. Asof-join the reference table ───────────────────────────────────
    #    Polars's asof requires both sides sorted on the on-key; we
    #    additionally need to broadcast the static reference to a per-symbol
    #    join because Polars doesn't auto-broadcast on the asof key.
    enriched = bars.sort(["symbol", "ts"]).join(reference, on="symbol", how="left")

    # ── 3a. Per-symbol rolling mean (5-minute trailing window) ──────────────
    #    Ibex's `window 5m` is half-open, (t-5m, t], which is Polars' default
    #    `closed="right"` — so on 1-minute bars both span five of them.
    with_rolling = enriched.with_columns(
        rmean5=pl.col("close").rolling_mean_by("ts", window_size="5m").over("symbol"),
    )

    # ── 3b. Bar-level features built on top of the rolling state ────────────
    features = with_rolling.with_columns(
        spread=pl.col("high") - pl.col("low"),
        range=(pl.col("high") - pl.col("low")) / pl.col("open"),
        log_mid=((pl.col("high") + pl.col("low")) / 2.0).log(),
        z5=(pl.col("close") - pl.col("rmean5")) / (pl.col("rmean5") + 0.0001),
    )

    # ── 3c. The label: next bar's close, per symbol ─────────────────────────
    #    `.over("symbol")` is the counterpart of Ibex's `by symbol` — without a
    #    partition the shift reads across symbol boundaries.
    features = features.with_columns(
        next_close=pl.col("close").shift(-1).over("symbol"),
    )

    # ── 4. Train: ridge of next_close on the bar features ───────────────────
    terms = ["open", "range", "log_mid", "z5", "vol_regime"]
    fit_df = features.drop_nulls(["next_close"])
    X = fit_df.select(terms).to_numpy()
    y = fit_df["next_close"].to_numpy()
    model = Ridge(alpha=0.1)
    model.fit(X, y)

    # Coefficients in the same format as the Ibex output for diff-friendly comparison.
    coef_rows = [("(intercept)", float(model.intercept_))]
    for name, value in zip(terms, model.coef_):
        coef_rows.append((name, float(value)))
    coefficients = pl.DataFrame(coef_rows, schema=["term", "estimate"], orient="row")

    # ── 5. Score: predictions for the labelled rows ─────────────────────────
    predictions = pl.DataFrame({"fitted": model.predict(X)})

    # ── 6. Persist ──────────────────────────────────────────────────────────
    coefficients.write_csv(DATA / "coefficients_polars.csv")
    # Ibex writes the *labelled* (filtered) matrix, so write fit_df, not features.
    fit_df.write_csv(DATA / "features_polars.csv")
    predictions.write_csv(DATA / "predictions_polars.csv")

    print(coefficients)


if __name__ == "__main__":
    main()
