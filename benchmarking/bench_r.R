#!/usr/bin/env Rscript
# Benchmark data.table and dplyr on the same aggregation queries as ibex_bench.
#
# Writes tab-separated results to --out (default: results/r.tsv).
# Progress goes to stderr; TSV rows go to the output file.
#
# Usage:
#   Rscript bench_r.R --csv data/prices.csv [--csv-multi data/prices_multi.csv]
#                     [--warmup 1] [--iters 5] [--out results/r.tsv]

suppressPackageStartupMessages({
    library(data.table)
    library(dplyr)
})

# ── Arg parsing ───────────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(flag, default = NULL) {
    i <- which(args == flag)
    if (length(i) == 0) return(default)
    args[i + 1]
}

csv_path        <- parse_arg("--csv")
csv_multi_path  <- parse_arg("--csv-multi")
csv_trades_path <- parse_arg("--csv-trades")
csv_events_path <- parse_arg("--csv-events")
csv_lookup_path <- parse_arg("--csv-lookup")
warmup          <- as.integer(parse_arg("--warmup", "1"))
iters           <- as.integer(parse_arg("--iters",  "5"))
out_path        <- parse_arg("--out", "results/r.tsv")

if (is.null(csv_path)) stop("--csv is required")

# ── Timing helper ─────────────────────────────────────────────────────────────
timer <- function(fn) {
    for (i in seq_len(warmup)) fn()
    t0 <- proc.time()[["elapsed"]]
    r  <- NULL
    for (i in seq_len(iters)) r <- fn()
    avg_ms <- (proc.time()[["elapsed"]] - t0) * 1000 / iters
    list(avg_ms = avg_ms, nrow = nrow(r))
}

results <- list()

bench <- function(framework, name, fn) {
    r <- timer(fn)
    message(sprintf("  %s/%s: avg_ms=%.3f, rows=%d",
                    framework, name, r$avg_ms, r$nrow))
    results[[length(results) + 1]] <<- data.frame(
        framework = framework,
        query     = name,
        avg_ms    = sprintf("%.3f", r$avg_ms),
        rows      = r$nrow,
        stringsAsFactors = FALSE
    )
}

# ── Load single-key data ──────────────────────────────────────────────────────
message("data.table: loading prices.csv...")
dt <- fread(csv_path)

# ── Single-column group-by ────────────────────────────────────────────────────
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

message("\n=== dplyr ===")
message("dplyr: loading prices.csv...")
tb <- as_tibble(fread(csv_path))

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

# ── Multi-column group-by ─────────────────────────────────────────────────────
if (!is.null(csv_multi_path)) {
    message("\ndata.table: loading prices_multi.csv...")
    dtm <- fread(csv_multi_path)
    tbm <- as_tibble(dtm)

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

# ── Filter benchmarks ─────────────────────────────────────────────────────────
if (!is.null(csv_trades_path)) {
    message("\ndata.table: loading trades.csv...")
    dt_trades <- fread(csv_trades_path)
    tb_trades <- as_tibble(dt_trades)

    message("\n=== data.table (filter) ===")

    bench("data.table", "filter_simple",
        function() dt_trades[price > 500.0])

    bench("data.table", "filter_and",
        function() dt_trades[price > 500.0 & qty < 100])

    bench("data.table", "filter_arith",
        function() dt_trades[price * qty > 50000.0])

    bench("data.table", "filter_or",
        function() dt_trades[price > 900.0 | qty < 10])

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

# ── High-cardinality group-by + string-gather filter ─────────────────────────
if (!is.null(csv_events_path)) {
    message("\ndata.table: loading events.csv...")
    dt_ev <- fread(csv_events_path)
    tb_ev <- as_tibble(dt_ev)

    message("\n=== data.table (events) ===")

    bench("data.table", "sum_by_user",
        function() dt_ev[, .(total = sum(amount)), by = user_id])

    bench("data.table", "filter_events",
        function() dt_ev[amount > 500.0])

    message("\n=== dplyr (events) ===")

    bench("dplyr", "sum_by_user",
        function() tb_ev |> group_by(user_id) |>
            summarise(total = sum(amount), .groups = "drop"))

    bench("dplyr", "filter_events",
        function() tb_ev |> filter(amount > 500.0))
}

# ── Null benchmarks: left join producing ~50% null right-column values ────────
if (!is.null(csv_lookup_path)) {
    message("\ndata.table: loading lookup.csv...")
    dt_lookup <- fread(csv_lookup_path)
    tb_lookup <- as_tibble(dt_lookup)

    message("\n=== data.table (null) ===")

    bench("data.table", "null_left_join",
        function() merge(dt, dt_lookup, by = "symbol", all.x = TRUE))

    message("\n=== dplyr (null) ===")

    bench("dplyr", "null_left_join",
        function() tb |> left_join(tb_lookup, by = "symbol"))
}

# ── Write results ─────────────────────────────────────────────────────────────
dir.create(dirname(out_path), showWarnings = FALSE, recursive = TRUE)
out_df <- do.call(rbind, results)
write.table(out_df, out_path, sep = "\t", row.names = FALSE, quote = FALSE)
message(sprintf("\nresults written to %s", out_path))
