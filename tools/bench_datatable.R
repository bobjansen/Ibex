#!/usr/bin/env Rscript
# data.table TimeFrame benchmark — equivalent to ibex_bench --timeframe-rows N.
#
# Requires: install.packages("data.table")
#
# Usage:
#   Rscript tools/bench_datatable.R --rows=1000000 --warmup=1 --iters=3
#
# Note: rolling_count and rolling_sum use fixed-width windows (n=60 rows for 1m,
# n=300 rows for 5m). This is equivalent to time-based windows for uniformly
# spaced 1-second data.  data.table does not have native variable-width
# time-based rolling, so frollsum / frollmean with n rows is the idiomatic
# equivalent.

suppressPackageStartupMessages(library(data.table))

# ---------- argument parsing --------------------------------------------------
parse_args <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  rows    <- 100000L
  warmup  <- 1L
  iters   <- 5L
  for (a in args) {
    if (startsWith(a, "--rows="))   rows   <- as.integer(sub("--rows=",   "", a))
    if (startsWith(a, "--warmup=")) warmup <- as.integer(sub("--warmup=", "", a))
    if (startsWith(a, "--iters="))  iters  <- as.integer(sub("--iters=",  "", a))
  }
  list(rows = rows, warmup = warmup, iters = iters)
}

# ---------- data generation ---------------------------------------------------
generate_data <- function(n) {
  # ts: POSIXct at 1-second spacing starting from Unix epoch
  # price: sawtooth 100 .. 199, matching ibex synthetic data
  data.table(
    ts    = .POSIXct(seq(0L, n - 1L), tz = "UTC"),
    price = 100.0 + ((seq_len(n) - 1L) %% 100L)
  )
}

# ---------- benchmark harness -------------------------------------------------
bench <- function(name, fn, warmup, iters) {
  for (i in seq_len(warmup)) fn()
  start <- proc.time()[["elapsed"]]
  rows  <- 0L
  for (i in seq_len(iters)) {
    result <- fn()
    rows   <- nrow(result)
  }
  elapsed_ms <- (proc.time()[["elapsed"]] - start) * 1000
  avg_ms     <- elapsed_ms / iters
  cat(sprintf(
    "bench %s: iters=%d, total_ms=%.3f, avg_ms=%.3f, rows=%d\n",
    name, iters, elapsed_ms, avg_ms, rows
  ))
}

# ---------- main --------------------------------------------------------------
args <- parse_args()
n    <- args$rows

cat(sprintf(
  "\n-- data.table %s TimeFrame benchmarks (%d rows, 1s spacing) --\n",
  packageVersion("data.table"), n
))

dt <- generate_data(n)

# as_timeframe: sort ascending by timestamp
bench("as_timeframe", function() dt[order(ts)], args$warmup, args$iters)

# tf_lag1: vectorized shift — O(n)
bench("tf_lag1", function() {
  d <- dt[order(ts)]
  d[, prev := shift(price, 1L)]
  d
}, args$warmup, args$iters)

# tf_rolling_count_1m: 60-row window (1s * 60 = 1 minute)
# frollsum over a constant-1 vector gives the count of rows in each window.
bench("tf_rolling_count_1m", function() {
  d <- dt[order(ts)]
  d[, c := frollsum(rep(1L, .N), n = 60L, fill = NA_integer_, align = "right")]
  d
}, args$warmup, args$iters)

# tf_rolling_sum_1m: 60-row window
bench("tf_rolling_sum_1m", function() {
  d <- dt[order(ts)]
  d[, s := frollsum(price, n = 60L, fill = NA_real_, align = "right")]
  d
}, args$warmup, args$iters)

# tf_rolling_mean_5m: 300-row window (1s * 300 = 5 minutes)
bench("tf_rolling_mean_5m", function() {
  d <- dt[order(ts)]
  d[, m := frollmean(price, n = 300L, fill = NA_real_, align = "right")]
  d
}, args$warmup, args$iters)

# tf_resample_1m_ohlc: bucket ticks into 1-minute OHLC bars
# floor(as.numeric(ts) / 60) gives an integer bucket ID per minute
bench("tf_resample_1m_ohlc", function() {
  d <- dt[order(ts)]
  d[, bucket := floor(as.numeric(ts) / 60)]
  d[, .(open  = first(price),
        high  = max(price),
        low   = min(price),
        close = last(price)),
    by = bucket]
}, args$warmup, args$iters)
