#!/usr/bin/env Rscript
# Benchmark data.table and dplyr on the same aggregation queries as ibex_bench.
#
# Writes tab-separated results to --out (default: results/r.tsv).
# Progress goes to stderr; TSV rows go to the output file.
#
# Usage:
#   Rscript bench_r.R --csv data/prices.csv [--csv-multi data/prices_multi.csv]
#                     [--warmup 1] [--iters 5] [--out results/r.tsv]
#                     [--skip-data-table] [--skip-dplyr]

# ── Arg parsing ───────────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(flag, default = NULL) {
    i <- which(args == flag)
    if (length(i) == 0) return(default)
    args[i + 1]
}

parse_flag <- function(flag) {
    any(args == flag)
}

csv_path        <- parse_arg("--csv")
csv_multi_path  <- parse_arg("--csv-multi")
csv_trades_path <- parse_arg("--csv-trades")
csv_events_path <- parse_arg("--csv-events")
csv_lookup_path <- parse_arg("--csv-lookup")
warmup          <- as.integer(parse_arg("--warmup", "1"))
iters           <- as.integer(parse_arg("--iters",  "5"))
out_path        <- parse_arg("--out", "results/r.tsv")
fill_rows       <- as.integer(parse_arg("--fill-rows", "4000000"))
skip_data_table <- parse_flag("--skip-data-table")
skip_dplyr      <- parse_flag("--skip-dplyr")

if (skip_data_table && skip_dplyr) {
    stop("both --skip-data-table and --skip-dplyr are set")
}

if (is.null(csv_path)) stop("--csv is required")

suppressPackageStartupMessages({
    if (!skip_data_table) {
        library(data.table)
    }
    if (!skip_dplyr) {
        library(dplyr)
    }
})

# ── Timing helper ─────────────────────────────────────────────────────────────
# proc.time()[["elapsed"]] uses gettimeofday() which is non-monotonic: an NTP
# correction during a long benchmark run can step the clock backward, producing
# a negative elapsed time.  Retry any such measurement up to 5 times before
# clamping to 0 as a last resort.
timer <- function(fn) {
    for (i in seq_len(warmup)) fn()
    times <- numeric(iters)
    r <- NULL
    for (i in seq_len(iters)) {
        elapsed <- -1
        for (attempt in seq_len(5L)) {
            t0      <- proc.time()[["elapsed"]]
            r       <- fn()
            elapsed <- (proc.time()[["elapsed"]] - t0) * 1000
            if (elapsed >= 0) break
        }
        times[i] <- max(elapsed, 0)
    }
    list(
        avg_ms    = mean(times),
        min_ms    = min(times),
        max_ms    = max(times),
        stddev_ms = if (iters > 1) sd(times) else 0.0,
        p95_ms    = as.numeric(quantile(times, 0.95)),
        p99_ms    = as.numeric(quantile(times, 0.99)),
        nrow      = nrow(r)
    )
}

results <- list()

bench <- function(framework, name, fn) {
    r <- timer(fn)
    message(sprintf("  %s/%s: avg_ms=%.3f, stddev_ms=%.3f, p99_ms=%.3f, rows=%d",
                    framework, name, r$avg_ms, r$stddev_ms, r$p99_ms, r$nrow))
    results[[length(results) + 1]] <<- data.frame(
        framework = framework,
        query     = name,
        avg_ms    = sprintf("%.3f", r$avg_ms),
        min_ms    = sprintf("%.3f", r$min_ms),
        max_ms    = sprintf("%.3f", r$max_ms),
        stddev_ms = sprintf("%.3f", r$stddev_ms),
        p95_ms    = sprintf("%.3f", r$p95_ms),
        p99_ms    = sprintf("%.3f", r$p99_ms),
        rows      = r$nrow,
        stringsAsFactors = FALSE
    )
}

# ── Load single-key data ──────────────────────────────────────────────────────
dt <- NULL
tb <- NULL
if (!skip_data_table) {
    message("data.table: loading prices.csv...")
    dt <- fread(csv_path)
}
if (!skip_dplyr) {
    message("dplyr: loading prices.csv...")
    tb <- as_tibble(fread(csv_path))
}

# ── Single-column group-by ────────────────────────────────────────────────────
if (!skip_data_table) {
    message("\n=== data.table ===")

    bench("data.table", "mean_by_symbol",
        function() dt[, .(avg_price = mean(price)), by = symbol])

    bench("data.table", "ohlc_by_symbol",
        function() dt[, .(open = data.table::first(price),
                          high = max(price),
                          low  = min(price),
                          last = data.table::last(price)),
                      by = symbol])

    copies <- lapply(seq_len(warmup + iters), function(...) copy(dt))
    copy_idx <- 0
    bench("data.table", "update_price_x2",
        function() {
            copy_idx <<- copy_idx + 1
            tmp <- copies[[copy_idx]]
            tmp[, price_x2 := price * 2][]
        })

    bench("data.table", "cumsum_price",
        function() dt[, cs := cumsum(price)][])

    bench("data.table", "cumprod_price",
        function() dt[, cp := cumprod(price)][])

    bench("data.table", "rand_uniform",
        function() dt[, r := runif(.N, 0.0, 1.0)][])

    bench("data.table", "rand_normal",
        function() dt[, n := rnorm(.N, 0.0, 1.0)][])

    bench("data.table", "rand_int",
        function() dt[, r := sample.int(100L, .N, replace = TRUE)][])

    bench("data.table", "rand_bernoulli",
        function() dt[, r := rbinom(.N, 1L, 0.3)][])
}

if (!skip_dplyr) {
    message("\n=== dplyr ===")

    bench("dplyr", "mean_by_symbol",
        function() tb |> group_by(symbol) |>
            summarise(avg_price = mean(price), .groups = "drop"))

    bench("dplyr", "ohlc_by_symbol",
        function() tb |> group_by(symbol) |>
            summarise(open = dplyr::first(price),
                      high = max(price),
                      low  = min(price),
                      last = dplyr::last(price),
                      .groups = "drop"))

    bench("dplyr", "update_price_x2",
        function() tb |> mutate(price_x2 = price * 2))

    bench("dplyr", "cumsum_price",
        function() tb |> mutate(cs = cumsum(price)))

    bench("dplyr", "cumprod_price",
        function() tb |> mutate(cp = cumprod(price)))

    bench("dplyr", "rand_uniform",
        function() tb |> mutate(r = runif(n(), 0.0, 1.0)))

    bench("dplyr", "rand_normal",
        function() tb |> mutate(n = rnorm(n(), 0.0, 1.0)))

    bench("dplyr", "rand_int",
        function() tb |> mutate(r = sample.int(100L, n(), replace = TRUE)))

    bench("dplyr", "rand_bernoulli",
        function() tb |> mutate(r = rbinom(n(), 1L, 0.3)))
}

# ── Multi-column group-by ─────────────────────────────────────────────────────
if (!is.null(csv_multi_path)) {
    dtm <- NULL
    tbm <- NULL
    if (!skip_data_table) {
        message("\ndata.table: loading prices_multi.csv...")
        dtm <- fread(csv_multi_path)

        message("\n=== data.table (multi-key) ===")

        bench("data.table", "count_by_symbol_day",
            function() dtm[, .(n = .N), by = .(symbol, day)])

        bench("data.table", "mean_by_symbol_day",
            function() dtm[, .(avg_price = mean(price)), by = .(symbol, day)])

        bench("data.table", "ohlc_by_symbol_day",
            function() dtm[, .(open = data.table::first(price),
                               high = max(price),
                               low  = min(price),
                               last = data.table::last(price)),
                           by = .(symbol, day)])
    }

    if (!skip_dplyr) {
        if (is.null(tbm)) {
            tbm <- as_tibble(fread(csv_multi_path))
        }
        message("\n=== dplyr (multi-key) ===")

        bench("dplyr", "count_by_symbol_day",
            function() tbm |> group_by(symbol, day) |>
                summarise(n = n(), .groups = "drop"))

        bench("dplyr", "mean_by_symbol_day",
            function() tbm |> group_by(symbol, day) |>
                summarise(avg_price = mean(price), .groups = "drop"))

        bench("dplyr", "ohlc_by_symbol_day",
            function() tbm |> group_by(symbol, day) |>
                summarise(open = dplyr::first(price),
                          high = max(price),
                          low  = min(price),
                          last = dplyr::last(price),
                          .groups = "drop"))
    }
}

# ── Reshape benchmarks (melt / dcast) ─────────────────────────────────────────
if (!is.null(csv_multi_path)) {
    if (!skip_data_table) {
        message("\ndata.table: loading multi for reshape...")
        dtm_reshape <- fread(csv_multi_path)

        # Build the wide OHLC table first
        wide_dt <- dtm_reshape[, .(open = data.table::first(price),
                                    high = max(price),
                                    low  = min(price),
                                    close = data.table::last(price)),
                                by = .(symbol, day)]
        message(sprintf("  data.table: wide table has %d rows", nrow(wide_dt)))

        message("\n=== data.table (reshape) ===")

        bench("data.table", "melt_wide_to_long",
            function() melt(wide_dt, id.vars = c("symbol", "day"),
                            measure.vars = c("open", "high", "low", "close")))

        # Build long table for dcast
        long_dt <- melt(wide_dt, id.vars = c("symbol", "day"),
                        measure.vars = c("open", "high", "low", "close"))

        bench("data.table", "dcast_long_to_wide",
            function() dcast(long_dt, symbol + day ~ variable, value.var = "value"))
    }

    if (!skip_dplyr) {
        message("\ndplyr: loading multi for reshape...")
        tbm_reshape <- as_tibble(fread(csv_multi_path))
        library(tidyr)

        # Build the wide OHLC table first
        wide_tb <- tbm_reshape |> group_by(symbol, day) |>
            summarise(open = dplyr::first(price),
                      high = max(price),
                      low  = min(price),
                      close = dplyr::last(price),
                      .groups = "drop")
        message(sprintf("  dplyr: wide table has %d rows", nrow(wide_tb)))

        message("\n=== dplyr (reshape) ===")

        bench("dplyr", "melt_wide_to_long",
            function() wide_tb |> pivot_longer(cols = c(open, high, low, close),
                                                names_to = "variable",
                                                values_to = "value"))

        # Build long table for dcast
        long_tb <- wide_tb |> pivot_longer(cols = c(open, high, low, close),
                                            names_to = "variable",
                                            values_to = "value")

        bench("dplyr", "dcast_long_to_wide",
            function() long_tb |> pivot_wider(names_from = variable,
                                               values_from = value))
    }
}

# ── Filter benchmarks ─────────────────────────────────────────────────────────
if (!is.null(csv_trades_path)) {
    dt_trades <- NULL
    tb_trades <- NULL
    if (!skip_data_table) {
        message("\ndata.table: loading trades.csv...")
        dt_trades <- fread(csv_trades_path)

        message("\n=== data.table (filter) ===")

        bench("data.table", "filter_simple",
            function() dt_trades[price > 500.0])

        bench("data.table", "filter_and",
            function() dt_trades[price > 500.0 & qty < 100])

        bench("data.table", "filter_arith",
            function() dt_trades[price * qty > 50000.0])

        bench("data.table", "filter_or",
            function() dt_trades[price > 900.0 | qty < 10])
    }

    if (!skip_dplyr) {
        if (is.null(tb_trades)) {
            tb_trades <- as_tibble(fread(csv_trades_path))
        }
        message("\n=== dplyr (filter) ===")

        bench("dplyr", "filter_simple",
            function() tb_trades |> filter(price > 500.0))

        bench("dplyr", "filter_and",
            function() tb_trades |> filter(price > 500.0, qty < 100))

        bench("dplyr", "filter_arith",
            function() tb_trades |> filter(price * qty > 50000.0))

        bench("dplyr", "filter_or",
            function() tb_trades |> filter(price > 900.0 | qty < 10))
    }
}

# ── High-cardinality group-by + string-gather filter ─────────────────────────
if (!is.null(csv_events_path)) {
    dt_ev <- NULL
    tb_ev <- NULL
    if (!skip_data_table) {
        message("\ndata.table: loading events.csv...")
        dt_ev <- fread(csv_events_path)

        message("\n=== data.table (events) ===")

        bench("data.table", "sum_by_user",
            function() dt_ev[, .(total = sum(amount)), by = user_id])

        bench("data.table", "filter_events",
            function() dt_ev[amount > 500.0])
    }

    if (!skip_dplyr) {
        if (is.null(tb_ev)) {
            tb_ev <- as_tibble(fread(csv_events_path))
        }
        message("\n=== dplyr (events) ===")

        bench("dplyr", "sum_by_user",
            function() tb_ev |> group_by(user_id) |>
                summarise(total = sum(amount), .groups = "drop"))

        bench("dplyr", "filter_events",
            function() tb_ev |> filter(amount > 500.0))
    }
}

# ── Null benchmarks: left join producing ~50% null right-column values ────────
if (!is.null(csv_lookup_path)) {
    dt_lookup <- NULL
    tb_lookup <- NULL
    if (!skip_data_table) {
        message("\ndata.table: loading lookup.csv...")
        dt_lookup <- fread(csv_lookup_path)
        lookup_symbols <- unique(dt_lookup$symbol)
        left_small <- copy(dt[seq_len(min(nrow(dt), 2000L))])
        right_small <- copy(dt_lookup[seq_len(min(nrow(dt_lookup), 64L))])
        left_small[, join_key := 1L]
        right_small[, join_key := 1L]

        message("\n=== data.table (null) ===")

        bench("data.table", "null_left_join",
            function() merge(dt, dt_lookup, by = "symbol", all.x = TRUE))

        bench("data.table", "null_semi_join",
            function() dt[symbol %chin% lookup_symbols])

        bench("data.table", "null_anti_join",
            function() dt[!symbol %chin% lookup_symbols])

        bench("data.table", "null_cross_join_small",
            function() merge(left_small, right_small, by = "join_key", allow.cartesian = TRUE)
                [, join_key := NULL][])
    }

    if (!skip_dplyr) {
        if (is.null(tb_lookup)) {
            tb_lookup <- as_tibble(fread(csv_lookup_path))
        }
        tb_lookup_symbols <- tb_lookup |> distinct(symbol)
        tb_left_small <- tb |> slice_head(n = min(nrow(tb), 2000L))
        tb_right_small <- tb_lookup |> slice_head(n = min(nrow(tb_lookup), 64L))
        message("\n=== dplyr (null) ===")

        bench("dplyr", "null_left_join",
            function() tb |> left_join(tb_lookup, by = "symbol"))

        bench("dplyr", "null_semi_join",
            function() tb |> semi_join(tb_lookup_symbols, by = "symbol"))

        bench("dplyr", "null_anti_join",
            function() tb |> anti_join(tb_lookup_symbols, by = "symbol"))

        bench("dplyr", "null_cross_join_small",
            function() tibble::as_tibble(merge(tb_left_small, tb_right_small, by = NULL)))
    }
}

# ── Fill benchmarks: fill_null, fill_forward (LOCF), fill_backward (NOCB) ────
# Uses an in-memory vector with alternating valid / NA doubles (~50% null).
# Row count is controlled by --fill-rows (default 4 000 000).
{
    message(sprintf("\nBuilding fill data (%d rows, 50%% NA)...", fill_rows))
    idx   <- seq_len(fill_rows) - 1L          # 0-based index
    valid <- (idx %% 2L == 0L)
    vals  <- ifelse(valid, 100.0 + (idx %% 100L), NA_real_)
    dt_fill <- NULL
    tb_fill <- NULL
    if (!skip_data_table) {
        dt_fill <- data.table(val = vals)

        message("\n=== data.table (fill) ===")

        bench("data.table", "fill_null",
            function() dt_fill[, v2 := nafill(val, fill = 0.0)][])

        bench("data.table", "fill_forward",
            function() dt_fill[, v2 := nafill(val, type = "locf")][])

        bench("data.table", "fill_backward",
            function() dt_fill[, v2 := nafill(val, type = "nocb")][])
    }

    if (!skip_dplyr) {
        library(tidyr)
        tb_fill <- tibble::tibble(val = vals)

        message("\n=== dplyr (fill) ===")

        bench("dplyr", "fill_null",
            function() tb_fill |> mutate(v2 = tidyr::replace_na(val, 0.0)))

        bench("dplyr", "fill_forward",
            function() tb_fill |> tidyr::fill(val, .direction = "down") |>
                rename(v2 = val))

        bench("dplyr", "fill_backward",
            function() tb_fill |> tidyr::fill(val, .direction = "up") |>
                rename(v2 = val))
    }
}

# ── Write results ─────────────────────────────────────────────────────────────
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
if (length(results) == 0) {
    out_df <- data.frame(
        framework = character(),
        query     = character(),
        avg_ms    = character(),
        min_ms    = character(),
        max_ms    = character(),
        stddev_ms = character(),
        p95_ms    = character(),
        p99_ms    = character(),
        rows      = integer(),
        stringsAsFactors = FALSE
    )
} else {
    out_df <- do.call(rbind, results)
}
write.table(out_df, out_path, sep = "\t", row.names = FALSE, quote = FALSE)
message(sprintf("\nresults written to %s", out_path))
