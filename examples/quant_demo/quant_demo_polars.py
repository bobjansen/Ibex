#!/usr/bin/env python3
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

    # ── 1. Resample ticks to 1-minute OHLC + traded size, per symbol ───────
    bars = (
        ticks.sort(["symbol", "ts"])
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
    enriched = bars.sort("ts").join(reference, on="symbol", how="left")

    # ── 3. Features ─────────────────────────────────────────────────────────
    features = enriched.with_columns(
        spread=pl.col("high") - pl.col("low"),
        range=(pl.col("high") - pl.col("low")) / pl.col("open"),
        mid=(pl.col("high") + pl.col("low")) / 2.0,
        next_close=pl.col("close").shift(-1).over("symbol"),
    )

    # ── 4. Train: ridge of next_close on the bar features ───────────────────
    fit_df = features.drop_nulls(["next_close"])
    X = fit_df.select(["open", "range", "mid", "vol_regime"]).to_numpy()
    y = fit_df["next_close"].to_numpy()
    model = Ridge(alpha=0.1)
    model.fit(X, y)

    # Coefficients in the same format as the Ibex output for diff-friendly comparison.
    coef_rows = [("(intercept)", float(model.intercept_))]
    for name, value in zip(["open", "range", "mid", "vol_regime"], model.coef_):
        coef_rows.append((name, float(value)))
    coefficients = pl.DataFrame(coef_rows, schema=["term", "estimate"], orient="row")

    # ── 5. Score: predictions for the labelled rows ─────────────────────────
    predictions = pl.DataFrame({"fitted": model.predict(X)})

    # ── 6. Persist ──────────────────────────────────────────────────────────
    coefficients.write_csv(DATA / "coefficients_polars.csv")
    features.write_csv(DATA / "features_polars.csv")
    predictions.write_csv(DATA / "predictions_polars.csv")

    print(coefficients)


if __name__ == "__main__":
    main()
