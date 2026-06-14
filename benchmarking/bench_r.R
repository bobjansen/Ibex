#!/usr/bin/env Rscript
# Benchmark data.table and dplyr on the same aggregation queries as ibex_bench.
#
# Writes tab-separated results to --out (default: results/r.tsv).
# Progress goes to stderr; TSV rows go to the output file.
#
# Usage:
#   Rscript bench_r.R --csv data/prices.csv [--csv-multi data/prices_multi.csv]
#                     [--warmup 1] [--iters 5] [--out results/r.tsv]
#                     [--reshape-rows 100000] [--fill-rows 4000000]
#                     [--skip-data-table] [--skip-dplyr] [--skip-frollapply]

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
csv_users_path  <- parse_arg("--csv-users")
warmup          <- as.integer(parse_arg("--warmup", "1"))
iters           <- as.integer(parse_arg("--iters",  "5"))
out_path        <- parse_arg("--out", "results/r.tsv")
fill_rows       <- as.integer(parse_arg("--fill-rows", "4000000"))
reshape_rows    <- as.integer(parse_arg("--reshape-rows", "100000"))
tf_rows         <- as.integer(parse_arg("--tf-rows",    "1000000"))
skip_data_table <- parse_flag("--skip-data-table")
skip_dplyr      <- parse_flag("--skip-dplyr")
# frollapply(median/sd) is O(n*window): the two biggest single cells in the whole
# suite. Their numbers are stable across ibex iterations, so they are pinned
# (reused on the web page) and skipped by default from run_scale_suite.sh.
skip_frollapply <- parse_flag("--skip-frollapply")

# Honor the cross-engine single-threaded mode set by run_all.sh.
if (nzchar(Sys.getenv("IBEX_BENCH_SINGLE_THREADED"))) {
    suppressMessages(library(data.table))
    setDTthreads(1L)
}

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
# ── Peak-RSS measurement (Linux only) ─────────────────────────────────────────
# Absolute peak RSS (VmHWM) during a query's measured iterations, in MiB.
# Writing "5" to /proc/self/clear_refs resets the kernel's per-process peak;
# we reset after warmup and read VmHWM after the measured loop. On platforms
# without these /proc files the functions degrade to a no-op / 0.0.
reset_peak_rss <- function() {
    tryCatch({
        con <- file("/proc/self/clear_refs", "w")
        writeLines("5", con)
        close(con)
    }, error = function(e) invisible(NULL))
}

peak_rss_mb <- function() {
    tryCatch({
        lines <- readLines("/proc/self/status")
        hwm   <- grep("^VmHWM:", lines, value = TRUE)
        if (length(hwm) == 0) return(0.0)
        kb <- as.numeric(strsplit(trimws(sub("VmHWM:", "", hwm[1])), "\\s+")[[1]][1])
        kb / 1024.0
    }, error = function(e) 0.0)
}

# Dynamic per-cell cutoff: a single (warmup) iteration over this many ms cuts the
# cell — the rest of the iterations are skipped and a sentinel (avg_ms = -1) row
# is dropped by the writer, so one pathologically slow op can't dominate the run.
cell_cutoff_ms <- as.numeric(Sys.getenv("IBEX_CELL_CUTOFF_MS", "30000"))

timer <- function(fn) {
    r <- NULL
    for (i in seq_len(warmup)) {
        t0 <- proc.time()[["elapsed"]]
        r  <- fn()
        if ((proc.time()[["elapsed"]] - t0) * 1000 > cell_cutoff_ms) {
            return(list(avg_ms = -1, min_ms = -1, max_ms = -1, stddev_ms = 0,
                        p95_ms = -1, p99_ms = -1,
                        nrow = if (is.null(r)) 0L else nrow(r), peak_rss_mb = 0))
        }
    }
    reset_peak_rss()
    times <- numeric(iters)
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
    peak_mb <- peak_rss_mb()
    list(
        avg_ms    = mean(times),
        min_ms    = min(times),
        max_ms    = max(times),
        stddev_ms = if (iters > 1) sd(times) else 0.0,
        p95_ms    = as.numeric(quantile(times, 0.95)),
        p99_ms    = as.numeric(quantile(times, 0.99)),
        nrow      = nrow(r),
        peak_rss_mb = peak_mb
    )
}

results <- list()

# Carry-forward skip: cells cut at a smaller scale (passed back by the suite via
# IBEX_SKIP_CELLS as "framework|query") are skipped outright — see bench_mem.py.
fw_suffix  <- Sys.getenv("IBEX_FW_SUFFIX", "")
skip_cells <- Filter(nzchar, strsplit(Sys.getenv("IBEX_SKIP_CELLS", ""), ",")[[1]])
should_skip <- function(framework, name) {
    paste0(framework, fw_suffix, "|", name) %in% skip_cells
}
cut_row_df <- function(framework, name) {
    data.frame(framework = framework, query = name,
               avg_ms = "-1.000", min_ms = "-1.000", max_ms = "-1.000",
               stddev_ms = "0.000", p95_ms = "-1.000", p99_ms = "-1.000",
               rows = 0L, peak_rss_mb = "0.0", stringsAsFactors = FALSE)
}

bench <- function(framework, name, fn) {
    if (should_skip(framework, name)) {
        message(sprintf("  %s/%s: SKIPPED (cut at a smaller scale)", framework, name))
        results[[length(results) + 1]] <<- cut_row_df(framework, name)
        return(invisible())
    }
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
        peak_rss_mb = sprintf("%.1f", r$peak_rss_mb),
        stringsAsFactors = FALSE
    )
}

# ── Cost-capped bench ─────────────────────────────────────────────────────────
# A few rolling ops are O(n*window) and scale linearly with rows: data.table's
# frollapply(median/sd) and dplyr's slide_index_dbl() with a closure. At large
# sizes a single such cell takes minutes * (warmup+iters) and dominates the whole
# run (and slows local iteration). Skip a query when its projected total
# wall-clock exceeds the budget. per_row_us is an empirical per-row cost; see the
# call sites. The skip is logged so the blank cell is explained, not silent.
COST_BUDGET_MS <- 120000  # 2 minutes total (across warmup + iters)
bench_capped <- function(framework, name, fn, per_row_us, n) {
    # Pinned by default: these frollapply/slide cells are stable and dominate
    # wall-clock, so the suite reuses stored numbers unless --with-frollapply.
    if (skip_frollapply) {
        message(sprintf("  %s/%s: SKIPPED — pinned (pass --with-frollapply to run)",
            framework, name))
        return(invisible())
    }
    est_ms <- n * per_row_us / 1000 * (warmup + iters)
    if (est_ms > COST_BUDGET_MS) {
        message(sprintf(
            "  %s/%s: SKIPPED — projected %.0fs > %.0fs budget (%d rows, %d runs)",
            framework, name, est_ms / 1000, COST_BUDGET_MS / 1000,
            n, warmup + iters))
        return(invisible())
    }
    bench(framework, name, fn)
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

    bench("data.table", "distinct_symbol",
        function() unique(dt[, .(symbol)]))

    bench("data.table", "order_head_topk",
        function() head(dt[order(-price)], 100L))

    bench("data.table", "order_head_topk_by_symbol",
        function() dt[order(-price), head(.SD, 3L), by = symbol])

    bench("data.table", "order_tail_topk",
        function() tail(dt[order(-price)], 100L))

    bench("data.table", "order_tail_topk_by_symbol",
        function() dt[order(-price), tail(.SD, 3L), by = symbol])

    # Full-table sorts (every row reordered, no head/tail).
    bench("data.table", "sort_price",
        function() dt[order(price)])

    bench("data.table", "sort_price_desc",
        function() dt[order(-price)])

    bench("data.table", "sort_symbol",
        function() dt[order(symbol)])

    bench("data.table", "sort_symbol_price",
        function() dt[order(symbol, price)])

    bench("data.table", "sort_symbol_price_desc",
        function() dt[order(symbol, -price)])

    # Grouped window functions (per symbol).
    bench("data.table", "rank_by_symbol",
        function() dt[, rk := frank(-price, ties.method = "dense"), by = symbol][])

    bench("data.table", "lag_by_symbol",
        function() dt[, prev := shift(price, 1L), by = symbol][])

    bench("data.table", "cumsum_by_symbol",
        function() dt[, cs_sym := cumsum(price), by = symbol][])

    # Expensive group aggregates (per symbol): median, p90 (type-7 linear), sd.
    bench("data.table", "median_by_symbol",
        function() dt[, .(med = median(price)), by = symbol])

    bench("data.table", "quantile_by_symbol",
        function() dt[, .(p90 = quantile(price, 0.9, type = 7)), by = symbol])

    bench("data.table", "std_by_symbol",
        function() dt[, .(sd = sd(price)), by = symbol])

    # Multi-stage pipeline: filter -> group-by -> order -> head.
    bench("data.table", "filter_group_sort",
        function() head(dt[price > 500, .(avg = mean(price)), by = symbol][order(-avg)], 10L))

    # update by -> filter on derived column -> re-aggregate.
    bench("data.table", "update_group_filter",
        function() dt[, lr := log(price / shift(price, 1L)), by = symbol][
            lr > 0, .(pos_days = .N), by = symbol])

    # rank within group -> top-N per group -> aggregate survivors.
    bench("data.table", "group_rank_filter",
        function() dt[, rk := frank(-price, ties.method = "dense"), by = symbol][
            rk <= 10, .(avg_top10 = mean(price)), by = symbol])

    # grouped z-score -> clip to +/-3 -> re-aggregate.
    bench("data.table", "normalize_by_group",
        function() dt[, z := (price - mean(price)) / sd(price), by = symbol][
            , clipped := pmin(pmax(z, -3.0), 3.0)][
            , .(mean_z = mean(clipped), sd_z = sd(clipped)), by = symbol])

    # Transforms / single-pass language features (non-mutating selects).
    bench("data.table", "pmin_clip",
        function() dt[, .(symbol, price, clipped = pmin(price, 500.0))])

    bench("data.table", "where_update_clip",
        function() dt[, .(symbol, price = fifelse(price > 900.0, 900.0, price))])

    bench("data.table", "rbind_two",
        function() rbindlist(list(dt[, .(symbol, price)], dt[, .(symbol, price)])))

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

    bench("dplyr", "distinct_symbol",
        function() tb |> distinct(symbol))

    bench("dplyr", "order_head_topk",
        function() tb |> arrange(desc(price)) |> head(100L))

    bench("dplyr", "order_head_topk_by_symbol",
        function() tb |> arrange(desc(price)) |> group_by(symbol) |>
            slice_head(n = 3L) |> ungroup())

    bench("dplyr", "order_tail_topk",
        function() tb |> arrange(desc(price)) |> tail(100L))

    bench("dplyr", "order_tail_topk_by_symbol",
        function() tb |> arrange(desc(price)) |> group_by(symbol) |>
            slice_tail(n = 3L) |> ungroup())

    # Full-table sorts (every row reordered, no head/tail).
    bench("dplyr", "sort_price",
        function() tb |> arrange(price))

    bench("dplyr", "sort_price_desc",
        function() tb |> arrange(desc(price)))

    bench("dplyr", "sort_symbol",
        function() tb |> arrange(symbol))

    bench("dplyr", "sort_symbol_price",
        function() tb |> arrange(symbol, price))

    bench("dplyr", "sort_symbol_price_desc",
        function() tb |> arrange(symbol, desc(price)))

    # Grouped window functions (per symbol).
    bench("dplyr", "rank_by_symbol",
        function() tb |> group_by(symbol) |>
            mutate(rk = dense_rank(desc(price))) |> ungroup())

    bench("dplyr", "lag_by_symbol",
        function() tb |> group_by(symbol) |>
            mutate(prev = lag(price, 1L)) |> ungroup())

    bench("dplyr", "cumsum_by_symbol",
        function() tb |> group_by(symbol) |>
            mutate(cs = cumsum(price)) |> ungroup())

    # Expensive group aggregates (per symbol): median, p90 (type-7 linear), sd.
    bench("dplyr", "median_by_symbol",
        function() tb |> group_by(symbol) |>
            summarise(med = median(price), .groups = "drop"))

    bench("dplyr", "quantile_by_symbol",
        function() tb |> group_by(symbol) |>
            summarise(p90 = quantile(price, 0.9, type = 7), .groups = "drop"))

    bench("dplyr", "std_by_symbol",
        function() tb |> group_by(symbol) |>
            summarise(sd = sd(price), .groups = "drop"))

    # Multi-stage pipeline: filter -> group-by -> order -> head.
    bench("dplyr", "filter_group_sort",
        function() tb |> filter(price > 500) |> group_by(symbol) |>
            summarise(avg = mean(price), .groups = "drop") |>
            arrange(desc(avg)) |> head(10L))

    # update by -> filter on derived column -> re-aggregate.
    bench("dplyr", "update_group_filter",
        function() tb |> group_by(symbol) |>
            mutate(lr = log(price / lag(price, 1L))) |>
            filter(lr > 0) |>
            summarise(pos_days = n(), .groups = "drop"))

    # rank within group -> top-N per group -> aggregate survivors.
    bench("dplyr", "group_rank_filter",
        function() tb |> group_by(symbol) |>
            mutate(rk = dense_rank(desc(price))) |>
            filter(rk <= 10) |>
            summarise(avg_top10 = mean(price), .groups = "drop"))

    # grouped z-score -> clip to +/-3 -> re-aggregate.
    bench("dplyr", "normalize_by_group",
        function() tb |> group_by(symbol) |>
            mutate(z = (price - mean(price)) / sd(price),
                   clipped = pmin(pmax(z, -3.0), 3.0)) |>
            summarise(mean_z = mean(clipped), sd_z = sd(clipped), .groups = "drop"))

    # Transforms / single-pass language features.
    bench("dplyr", "pmin_clip",
        function() tb |> mutate(clipped = pmin(price, 500.0)))

    bench("dplyr", "where_update_clip",
        function() tb |> mutate(price = if_else(price > 900.0, 900.0, price)))

    bench("dplyr", "rbind_two",
        function() bind_rows(tb, tb))

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

        # Two-level rollup (funnel): by {symbol, day} then re-aggregate by symbol.
        bench("data.table", "symbol_day_to_symbol",
            function() dtm[, .(daily_mean = mean(price), daily_vol = sd(price)),
                           by = .(symbol, day)][
                , .(mean_of_means = mean(daily_mean), mean_vol = mean(daily_vol)),
                by = symbol])
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

        # Two-level rollup (funnel): by {symbol, day} then re-aggregate by symbol.
        bench("dplyr", "symbol_day_to_symbol",
            function() tbm |> group_by(symbol, day) |>
                summarise(daily_mean = mean(price), daily_vol = sd(price),
                          .groups = "drop") |>
                group_by(symbol) |>
                summarise(mean_of_means = mean(daily_mean),
                          mean_vol = mean(daily_vol), .groups = "drop"))
    }
}

# ── Reshape benchmarks (melt / dcast) ─────────────────────────────────────────
# Synthetic wide OHLC table with configurable row count and 4 measure columns.
# reshape_rows <= 0 disables these (the suite sets it to 0 above its memory
# budget — the in-memory wide table is the bench's biggest RAM consumer).
if (reshape_rows <= 0) {
    message("R: reshape skipped (disabled for this size)")
} else {
    n_day <- 400L
    idx  <- seq_len(reshape_rows) - 1L
    sym_idx <- idx %/% n_day
    day_idx <- idx %% n_day + 1L
    base <- 100.0 + idx %% 1000L

    if (!skip_data_table) {
        message(sprintf("\ndata.table: building synthetic wide table (%d rows)...", reshape_rows))

        wide_dt <- data.table(
            symbol = sprintf("S%04d", sym_idx),
            day    = day_idx,
            open   = base,
            high   = base + 1.0,
            low    = base - 1.0,
            close  = base + 0.5
        )
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

        # Typed-pivot variants: integer pivot key, and an explicit factor
        # (categorical) key. Same value matrix as dcast_long_to_wide.
        long_int_dt <- copy(long_dt)[, pivot_id := as.integer(variable) - 1L]
        bench("data.table", "dcast_long_to_wide_int_pivot",
            function() dcast(long_int_dt, symbol + day ~ pivot_id, value.var = "value"))

        long_cat_dt <- copy(long_dt)[, pivot_cat := factor(variable)]
        bench("data.table", "dcast_long_to_wide_cat_pivot",
            function() dcast(long_cat_dt, symbol + day ~ pivot_cat, value.var = "value"))
    }

    if (!skip_dplyr) {
        library(tidyr)
        message(sprintf("\ndplyr: building synthetic wide table (%d rows)...", reshape_rows))

        wide_tb <- tibble(
            symbol = sprintf("S%04d", sym_idx),
            day    = day_idx,
            open   = base,
            high   = base + 1.0,
            low    = base - 1.0,
            close  = base + 0.5
        )
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

        # Typed-pivot variants: integer pivot key, and a factor (categorical) key.
        long_int_tb <- long_tb |>
            mutate(pivot_id = match(variable, c("open", "high", "low", "close")) - 1L)
        bench("dplyr", "dcast_long_to_wide_int_pivot",
            function() long_int_tb |> pivot_wider(id_cols = c(symbol, day),
                                                   names_from = pivot_id,
                                                   values_from = value))

        long_cat_tb <- long_tb |> mutate(pivot_cat = factor(variable))
        bench("dplyr", "dcast_long_to_wide_cat_pivot",
            function() long_cat_tb |> pivot_wider(id_cols = c(symbol, day),
                                                   names_from = pivot_cat,
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

        # Correlation over the numeric columns (price, qty).
        bench("data.table", "corr_price_vol",
            function() data.table(corr = cor(dt_trades$price, dt_trades$qty)))
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

        # Correlation over the numeric columns (price, qty).
        bench("dplyr", "corr_price_vol",
            function() summarise(tb_trades, corr = cor(price, qty)))
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

        # High-cardinality inner join: events |><| users on user_id (~100K keys).
        if (!is.null(csv_users_path)) {
            dt_users <- fread(csv_users_path)
            bench("data.table", "inner_join_user",
                function() merge(dt_ev, dt_users, by = "user_id"))

            # Join-anchored pipelines (Tier 2): join -> derive -> roll up.
            bench("data.table", "join_update_group",
                function() merge(dt_ev, dt_users, by = "user_id")[
                    , revenue := amount * user_tier_multiplier][
                    , .(total_rev = sum(revenue)), by = .(symbol, user_segment)])

            bench("data.table", "join_filter_rank",
                function() merge(dt_ev, dt_users, by = "user_id")[
                    user_segment == "premium"][
                    , rk := frank(-amount, ties.method = "dense"), by = symbol][rk <= 5])
        }
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

        # High-cardinality inner join: events |><| users on user_id (~100K keys).
        if (!is.null(csv_users_path)) {
            tb_users <- as_tibble(fread(csv_users_path))
            bench("dplyr", "inner_join_user",
                function() tb_ev |> inner_join(tb_users, by = "user_id"))

            # Join-anchored pipelines (Tier 2): join -> derive -> roll up.
            bench("dplyr", "join_update_group",
                function() tb_ev |> inner_join(tb_users, by = "user_id") |>
                    mutate(revenue = amount * user_tier_multiplier) |>
                    group_by(symbol, user_segment) |>
                    summarise(total_rev = sum(revenue), .groups = "drop"))

            bench("dplyr", "join_filter_rank",
                function() tb_ev |> inner_join(tb_users, by = "user_id") |>
                    filter(user_segment == "premium") |>
                    group_by(symbol) |>
                    mutate(rk = dense_rank(desc(amount))) |>
                    filter(rk <= 5) |> ungroup())
        }
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

        # Low-cardinality inner join: prices |><| lookup on symbol.
        bench("data.table", "inner_join_symbol",
            function() merge(dt, dt_lookup, by = "symbol"))

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

        # Low-cardinality inner join: prices |><| lookup on symbol.
        bench("dplyr", "inner_join_symbol",
            function() tb |> inner_join(tb_lookup, by = "symbol"))

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

# ── TimeFrame rolling + asof ──────────────────────────────────────────────────
# Data: 1s-spaced timestamps, sawtooth price (matches bench_python tf shape).
# data.table rolling uses frollmean / frollsum (row-based; 60 rows == 1m here).
# Asof join uses data.table's roll-join: quotes[trades, ..., roll = TRUE].
# dplyr/slider's slide_period_mean is the time-aware analogue.

if (tf_rows > 0) {
    message(sprintf("\nbuilding tf data (%d rows, 1s spacing)...", tf_rows))
    ts_vec    <- as.POSIXct(seq.int(0L, tf_rows - 1L), origin = "1970-01-01", tz = "UTC")
    price_vec <- 100.0 + (seq.int(0L, tf_rows - 1L) %% 100L)

    if (!skip_data_table) {
        dt_tf <- data.table(ts = ts_vec, price = price_vec)
        message("\n=== data.table (tf rolling) ===")
        bench("data.table", "tf_lag1",
            function() dt_tf[, prev := shift(price, 1L)][])
        bench("data.table", "tf_rolling_count_1m",
            function() dt_tf[, c := frollsum(rep(1.0, .N), 60L)][])
        bench("data.table", "tf_rolling_sum_1m",
            function() dt_tf[, s := frollsum(price, 60L)][])
        bench("data.table", "tf_rolling_mean_5m",
            function() dt_tf[, m := frollmean(price, 300L)][])
        # frollapply is generic but ~100× slower than the optimized fns and
        # scales O(n*window); cost-cap so large sizes don't dominate the run.
        # Empirical on c7i.xlarge: ~18us/row (median), ~7us/row (sd).
        bench_capped("data.table", "tf_rolling_median_1m",
            function() dt_tf[, med := frollapply(price, 60L, median)][],
            per_row_us = 18, n = tf_rows)
        bench_capped("data.table", "tf_rolling_std_1m",
            function() dt_tf[, s := frollapply(price, 60L, sd)][],
            per_row_us = 7, n = tf_rows)
        # Full-series EWMA (alpha=0.1, adjust=False) via TTR::EMA — matches the
        # pandas/polars ewm reference (n=1 seeds with the first value). Not
        # time-windowed (no engine here uses ibex's time-windowed EWMA math).
        bench("data.table", "tf_rolling_ewma_1m",
            function() dt_tf[, e := suppressWarnings(TTR::EMA(price, n = 1L, ratio = 0.1))][])
        bench("data.table", "tf_resample_1m_ohlc",
            function() dt_tf[, .(open = data.table::first(price),
                                  high = max(price),
                                  low  = min(price),
                                  close = data.table::last(price)),
                              by = .(bucket = lubridate::floor_date(ts, "minute"))])

        message("\n=== data.table (tf asof) ===")
        set.seed(42L)
        sample_idx <- sort(sample.int(tf_rows, size = tf_rows %/% 10L))
        # 100 symbols partition both sides (quote i and trades derived from it
        # share symbol i %% 100), for the by-symbol roll join below.
        q_symbol <- paste0("SYM", (seq_len(tf_rows) - 1L) %% 100L)
        t_symbol <- paste0("SYM", (sample_idx - 1L) %% 100L)
        trades_dt  <- data.table(
            ts  = as.POSIXct(sample_idx - 1L, origin = "1970-01-01", tz = "UTC") +
                  runif(length(sample_idx), 0, 0.999),
            symbol = t_symbol,
            qty = sample.int(99L, length(sample_idx), replace = TRUE))
        quotes_dt  <- data.table(ts = ts_vec, symbol = q_symbol,
                                 bid = 99.0 + (price_vec - 100.0) * 0.01)
        setkey(quotes_dt, ts)
        setkey(trades_dt, ts)
        bench("data.table", "tf_asof_join",
            function() quotes_dt[trades_dt, .(ts, qty, bid),
                                  on = "ts", roll = TRUE])
        # by-symbol: roll on ts within each symbol (ts is the last on-key).
        setkey(quotes_dt, symbol, ts)
        setkey(trades_dt, symbol, ts)
        bench("data.table", "tf_asof_join_by_symbol",
            function() quotes_dt[trades_dt, .(ts, qty, bid),
                                  on = .(symbol, ts), roll = TRUE])
    }

    if (!skip_dplyr) {
        library(slider)
        tb_tf <- tibble::tibble(ts = ts_vec, price = price_vec)
        message("\n=== dplyr (tf rolling) ===")
        bench("dplyr", "tf_lag1",
            function() tb_tf |> mutate(prev = lag(price, 1L)))
        bench("dplyr", "tf_rolling_count_1m",
            function() tb_tf |> mutate(c = slide_index_dbl(price, ts,
                                                            ~ length(.x),
                                                            .before = lubridate::seconds(60))))
        bench("dplyr", "tf_rolling_sum_1m",
            function() tb_tf |> mutate(s = slide_index_sum(price, ts,
                                                            before = lubridate::seconds(60))))
        bench("dplyr", "tf_rolling_mean_5m",
            function() tb_tf |> mutate(m = slide_index_mean(price, ts,
                                                            before = lubridate::seconds(300))))
        # Median / std via slide_index_dbl + a closure (slow O(n*window) path);
        # cost-cap as for data.table. Per-row cost is a conservative estimate
        # (slide_index_dbl(closure) is comparable-or-slower than frollapply).
        bench_capped("dplyr", "tf_rolling_median_1m",
            function() tb_tf |> mutate(med = slide_index_dbl(price, ts, median,
                                                              .before = lubridate::seconds(60))),
            per_row_us = 20, n = tf_rows)
        bench_capped("dplyr", "tf_rolling_std_1m",
            function() tb_tf |> mutate(s = slide_index_dbl(price, ts, sd,
                                                            .before = lubridate::seconds(60))),
            per_row_us = 20, n = tf_rows)
        # Full-series EWMA (alpha=0.1, adjust=False) via TTR::EMA — matches the
        # pandas/polars ewm reference. Not time-windowed.
        bench("dplyr", "tf_rolling_ewma_1m",
            function() tb_tf |> mutate(e = suppressWarnings(TTR::EMA(price, n = 1L, ratio = 0.1))))
        bench("dplyr", "tf_resample_1m_ohlc",
            function() tb_tf |>
                mutate(bucket = lubridate::floor_date(ts, "minute")) |>
                group_by(bucket) |>
                summarise(open  = dplyr::first(price),
                          high  = max(price),
                          low   = min(price),
                          close = dplyr::last(price),
                          .groups = "drop"))
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
        peak_rss_mb = character(),
        stringsAsFactors = FALSE
    )
} else {
    out_df <- do.call(rbind, results)
}
# Sentinel rows (avg_ms < 0) for cut/skipped cells are kept here so the suite can
# detect them, record the carry-forward skip, and drop them from the combined CSV.
write.table(out_df, out_path, sep = "\t", row.names = FALSE, quote = FALSE)
message(sprintf("\nresults written to %s", out_path))
