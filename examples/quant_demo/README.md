# Quant demo: tick → bars → asof-join → features → ridge fit

A small end-to-end pipeline that exercises the operations a quant audience
recognises: resample tick data into 1-minute OHLC bars per symbol, asof-join a
reference table of sector + volatility regime, compute bar-level features,
and fit a ridge regression of next-bar close on those features.

The same pipeline is implemented twice — once in Ibex, once in Polars +
scikit-learn — so the comparison is concrete. **29 lines of Ibex pipeline
code vs. 53 lines of Polars + scikit-learn**, both exclude comments and
blanks.

## What's here

```
gen_data.py              synthetic tick generator (~108K ticks, 6 symbols)
data/ticks.csv           generated; ~3.4 MB
data/reference.csv       generated; one row per symbol (sector, vol_regime)
quant_demo.ibex          Ibex batch pipeline
quant_demo_polars.py     Polars + scikit-learn equivalent
run.sh                   one-shot: generate data, run both, diff coefficients
```

After running, both pipelines write three files each:

| Ibex                       | Polars                       |
|----------------------------|------------------------------|
| `coefficients.csv`         | `coefficients_polars.csv`    |
| `features.csv`             | `features_polars.csv`        |
| `predictions.csv`          | `predictions_polars.csv`     |

## Run

```bash
# 1. Generate the synthetic data (108K ticks, 3.4 MB).
python3 examples/quant_demo/gen_data.py

# 2. Build the Ibex release binary if you haven't.
cmake --build build-release

# 3. Ibex (the script is loaded into the REPL because the demo uses
#    model_coef / model_fitted, which are REPL-bound today):
printf ':load examples/quant_demo/quant_demo.ibex\n:quit\n' | \
    ./build-release/tools/ibex --plugin-path build-release/tools

# 4. Polars + scikit-learn:
python3 examples/quant_demo/quant_demo_polars.py
```

Or just:

```bash
bash examples/quant_demo/run.sh
```

## What the pipeline does

```
ticks.csv  ─┐
            ├─► as_timeframe("ts")           promote to TimeFrame
            ├─► resample 1m by symbol        2,340 OHLC + size bars
            ├─► asof-join reference          sector / vol_regime per symbol
            ├─► update features              spread, range, mid, next_close
            ├─► model { ~ ridge }            L2 fit on bar features
            └─► save coefs + features + predictions
```

The `update`-based feature block is identical to what would run in a
`Stream {}` source/sink wrapper — see `examples/market_stream/` for the
streaming pattern. That's the train/serve-skew elimination story: the
batch backtest and the live predictor share the feature engineering by
construction, not by convention.

## Caveats

- The synthetic ticks are a geometric Brownian motion per symbol seeded for
  reproducibility — there's no real signal to predict. The fit will be
  near-noise. The demo is about pipeline shape, not alpha.
- Ibex and Polars produce different coefficients on the same input. The
  pipelines drop the same rows (last bar per symbol), but Ibex's ridge and
  scikit-learn's `Ridge(alpha=0.1)` parameterise the L2 penalty differently
  and standardise features differently. Both fits are internally consistent;
  the demo is about pipeline shape, not coefficient reproducibility.
- The demo includes a per-symbol rolling z-score (`z5 = (close - rmean5) /
  (rmean5 + 1e-4)`) as one of the ridge features. The `[window 5m, by symbol,
  update { ... }]` clause runs a 5-minute rolling mean partitioned per
  symbol — AAPL's mean never includes NVDA bars. Implemented 2026-04-28.
