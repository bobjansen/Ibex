#!/usr/bin/env Rscript
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# A matched R-only slice of the website scale suite.  Every query below has the
# same name and input fixture as benchmarking/bench_r.R, and is expressed by
# all three R frontends.  The Ibex input binding is deliberately created before
# timing: this measures the lazy dplyr backend's query construction, execution,
# and result transfer, rather than repeatedly charging its one-time R-to-Ibex
# table import.
#
# Memory discipline, because the large scales otherwise exhaust the box:
#   * fixtures are loaded one phase at a time and dropped (R side and Ibex
#     side) before the next phase loads its own,
#   * the dplyr tibble shares columns with the data.table rather than being a
#     second full copy of it, and
#   * every collection happens between timed iterations, never inside one.

args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(flag, default = NULL) {
    i <- which(args == flag)
    if (!length(i)) return(default)
    if (i[[1]] == length(args)) stop(flag, " needs a value")
    args[[i[[1]] + 1L]]
}

csv_path <- parse_arg("--csv")
csv_multi_path <- parse_arg("--csv-multi")
csv_trades_path <- parse_arg("--csv-trades")
csv_events_path <- parse_arg("--csv-events")
csv_lookup_path <- parse_arg("--csv-lookup")
csv_users_path <- parse_arg("--csv-users")
warmup <- as.integer(parse_arg("--warmup", "1"))
iters <- as.integer(parse_arg("--iters", "5"))
out_path <- parse_arg("--out", "benchmarking/results/r_only.tsv")

if (is.null(csv_path)) stop("--csv is required")
if (is.na(warmup) || warmup < 0L || is.na(iters) || iters < 1L) {
    stop("--warmup must be non-negative and --iters must be positive")
}

suppressPackageStartupMessages({
    library(data.table)
    library(dplyr)
    library(ibex)
})

# Reclaim between iterations so no measured interval pays for the previous
# result's collection, and so peak live memory stays one result deep.
collect_garbage <- function() invisible(gc(verbose = FALSE, full = TRUE))

# Base R has no monotonic clock: both proc.time()'s elapsed field and
# Sys.time() read the wall clock, which on some hosts (WSL2 in particular)
# occasionally steps backwards mid-interval.  A negative duration is therefore
# impossible-but-observed, and left alone it lands in the output as a negative
# min_ms that drags the average with it.  Discard and re-measure those samples.
# A *forward* step is indistinguishable from a slow run, which is why min_ms is
# the statistic to trust on such a host.
#
# Sys.time() rather than proc.time(): R rounds proc.time to whole milliseconds,
# which silently quantizes every sub-10ms cell (see bench_r.R for the full
# rationale). Sys.time() resolves ~2us.
now_ms <- function() as.numeric(Sys.time()) * 1000
time_once <- function(fn, attempts = 4L) {
    for (attempt in seq_len(attempts)) {
        started <- now_ms()
        result <- fn()
        elapsed_ms <- now_ms() - started
        if (elapsed_ms >= 0) return(list(ms = elapsed_ms, result = result))
        message("    (wall clock stepped backwards; re-measuring)")
        result <- NULL
        collect_garbage()
    }
    stop("wall clock stepped backwards on every attempt")
}

timer <- function(fn) {
    result <- NULL
    for (i in seq_len(warmup)) {
        result <- NULL
        collect_garbage()
        result <- fn()
    }
    times <- numeric(iters)
    for (i in seq_len(iters)) {
        # Drop the prior result and collect *before* the clock starts: the
        # measurement covers the query alone.
        result <- NULL
        collect_garbage()
        measured <- time_once(fn)
        times[[i]] <- measured$ms
        result <- measured$result
        measured <- NULL
    }
    rows <- nrow(result)
    result <- NULL
    collect_garbage()
    list(
        avg_ms = mean(times), min_ms = min(times), max_ms = max(times),
        stddev_ms = if (iters > 1L) sd(times) else 0,
        p95_ms = as.numeric(quantile(times, 0.95)),
        p99_ms = as.numeric(quantile(times, 0.99)),
        rows = rows
    )
}

results <- list()

record <- function(framework, query, fn) {
    measurement <- timer(fn)
    message(sprintf("  %-12s %-28s avg=%9.3f ms rows=%d",
                    framework, query, measurement$avg_ms, measurement$rows))
    results[[length(results) + 1L]] <<- data.frame(
        framework = framework, query = query,
        avg_ms = sprintf("%.3f", measurement$avg_ms),
        min_ms = sprintf("%.3f", measurement$min_ms),
        max_ms = sprintf("%.3f", measurement$max_ms),
        stddev_ms = sprintf("%.3f", measurement$stddev_ms),
        p95_ms = sprintf("%.3f", measurement$p95_ms),
        p99_ms = sprintf("%.3f", measurement$p99_ms),
        rows = measurement$rows, peak_rss_mb = "0.0",
        stringsAsFactors = FALSE
    )
}

# Keep each query in lockstep.  The ibex expression must stay native: an
# accidental unsupported operation is a benchmark error, never a local dplyr
# fallback hidden inside the Ibex result.
bench_three <- function(name, data_table_fn, dplyr_fn, ibex_fn) {
    record("data.table", name, data_table_fn)
    record("dplyr", name, dplyr_fn)
    record("ibex-r", name, ibex_fn)
}

session <- create_session()

# Hand Ibex its string columns dictionary-encoded, which is the representation
# every other Ibex frontend already reads them in: `read_csv` and the Parquet
# reader both produce Categorical, and the website suite's Ibex numbers are
# measured over exactly that. Leaving them as R `character` made this suite the
# only place Ibex hashed group keys as text per row -- 100ms against 3ms for
# mean_by_symbol at 8M rows.
#
# Spelled as a factor rather than via `categorical_strings = TRUE` because the
# two differ on the way back: a factor collects as a factor, so the caller's own
# column has the type the result does, and `collect()` can hand those columns
# straight back instead of materializing them again. That is worth 21ms -> 6ms
# on update_price_x2. The conversion is one-off, and happens here, before any
# timing starts.
#
# The other two frontends read their fixture in their own native representation,
# which is what `fread` gives them.
ibex_source <- function(tb) {
    for (name in names(tb)) {
        if (is.character(tb[[name]])) tb[[name]] <- as.factor(tb[[name]])
    }
    ibex_tbl(tb, session = session, fallback = "error")
}

# The dplyr frontend reads the same columns the data.table frontend holds, so
# no fixture is materialised twice.  R's copy-on-modify keeps the two honest:
# every dplyr verb below copies before it writes, and the data.table `:=`
# queries only *add* columns, never overwrite a shared one.
#
# The shell, however, must not be shared: `names(dt)` is the data.table's own
# over-allocated attribute and `:=` rewrites it in place, which would leave the
# tibble claiming more names than it has columns.  Hand the tibble a fresh list
# and a fresh name vector; only the column data is held in common.
as_shared_tibble <- function(dt) {
    columns <- lapply(seq_along(dt), function(i) dt[[i]])
    names(columns) <- names(dt)[seq_along(columns)]
    as_tibble(columns)
}

# Each phase loads only the fixtures it needs, benchmarks them, then releases
# them on both sides: rm() plus a collection for the R tables, reset_session()
# for the Ibex bindings (which are otherwise held for the session's lifetime).
run_phase <- function(label, fn) {
    message(sprintf("\n=== %s ===", label))
    fn()
    reset_session(session)
    collect_garbage()
}

phase_prices_lookup <- function() {
    message("Loading prices.csv...")
    dt <- fread(csv_path)
    tb <- as_shared_tibble(dt)
    ib <- ibex_source(tb)

    bench_three("mean_by_symbol",
        function() dt[, .(avg_price = mean(price)), by = symbol],
        function() tb |> group_by(symbol) |> summarise(avg_price = mean(price), .groups = "drop"),
        function() ib |> group_by(symbol) |> summarise(avg_price = mean(price), .groups = "drop") |> collect())

    bench_three("ohlc_by_symbol",
        function() dt[, .(open = data.table::first(price), high = max(price), low = min(price), last = data.table::last(price)), by = symbol],
        function() tb |> group_by(symbol) |> summarise(open = dplyr::first(price), high = max(price), low = min(price), last = dplyr::last(price), .groups = "drop"),
        function() ib |> group_by(symbol) |> summarise(open = dplyr::first(price), high = max(price), low = min(price), last = dplyr::last(price), .groups = "drop") |> collect())

    bench_three("update_price_x2",
        function() dt[, price_x2 := price * 2][],
        function() tb |> mutate(price_x2 = price * 2),
        function() ib |> mutate(price_x2 = price * 2) |> collect())

    bench_three("distinct_symbol",
        function() unique(dt[, .(symbol)]),
        function() tb |> distinct(symbol),
        function() ib |> distinct(symbol) |> collect())

    bench_three("order_head_topk",
        function() head(dt[order(-price)], 100L),
        function() tb |> arrange(desc(price)) |> slice_head(n = 100L),
        function() ib |> arrange(desc(price)) |> slice_head(n = 100L) |> collect())

    bench_three("sort_price",
        function() dt[order(price)],
        function() tb |> arrange(price),
        function() ib |> arrange(price) |> collect())
    bench_three("sort_price_desc",
        function() dt[order(-price)],
        function() tb |> arrange(desc(price)),
        function() ib |> arrange(desc(price)) |> collect())
    bench_three("sort_symbol",
        function() dt[order(symbol)],
        function() tb |> arrange(symbol),
        function() ib |> arrange(symbol) |> collect())
    bench_three("sort_symbol_price",
        function() dt[order(symbol, price)],
        function() tb |> arrange(symbol, price),
        function() ib |> arrange(symbol, price) |> collect())
    bench_three("sort_symbol_price_desc",
        function() dt[order(symbol, -price)],
        function() tb |> arrange(symbol, desc(price)),
        function() ib |> arrange(symbol, desc(price)) |> collect())

    bench_three("filter_group_sort",
        function() head(dt[price > 500, .(avg = mean(price)), by = symbol][order(-avg)], 10L),
        function() tb |> filter(price > 500) |> group_by(symbol) |> summarise(avg = mean(price), .groups = "drop") |> arrange(desc(avg)) |> slice_head(n = 10L),
        function() ib |> filter(price > 500) |> group_by(symbol) |> summarise(avg = mean(price), .groups = "drop") |> arrange(desc(avg)) |> slice_head(n = 10L) |> collect())

    bench_three("pmin_clip",
        function() dt[, .(symbol, price, clipped = pmin(price, 500.0))],
        function() tb |> mutate(clipped = pmin(price, 500.0)),
        function() ib |> mutate(clipped = pmin(price, 500.0)) |> collect())

    bench_three("abs_price", function() dt[, v := abs(price)][], function() tb |> mutate(v = abs(price)), function() ib |> mutate(v = abs(price)) |> collect())
    bench_three("sqrt_price", function() dt[, v := sqrt(price)][], function() tb |> mutate(v = sqrt(price)), function() ib |> mutate(v = sqrt(price)) |> collect())
    bench_three("log_price", function() dt[, v := log(price)][], function() tb |> mutate(v = log(price)), function() ib |> mutate(v = log(price)) |> collect())
    bench_three("exp_price", function() dt[, v := exp(price / 1000.0)][], function() tb |> mutate(v = exp(price / 1000.0)), function() ib |> mutate(v = exp(price / 1000.0)) |> collect())
    bench_three("floor_price", function() dt[, v := floor(price)][], function() tb |> mutate(v = floor(price)), function() ib |> mutate(v = floor(price)) |> collect())
    bench_three("ceil_price", function() dt[, v := ceiling(price)][], function() tb |> mutate(v = ceiling(price)), function() ib |> mutate(v = ceiling(price)) |> collect())

    # The lookup joins are probed against prices, so they share this phase
    # rather than reloading the larger fixture.
    if (!is.null(csv_lookup_path)) {
        message("\n--- lookup: matched website queries ---")
        dt_lookup <- fread(csv_lookup_path)
        tb_lookup <- as_shared_tibble(dt_lookup)
        ib_lookup <- ibex_source(tb_lookup)
        dt_lookup_symbols <- unique(dt_lookup$symbol)
        tb_lookup_symbols <- tb_lookup |> distinct(symbol)
        ib_lookup_symbols <- ib_lookup |> distinct(symbol) |> compute()
        # data.table joins are spelled as indexed joins `y[x, on=]`, not
        # merge(x, y, by=): merge.data.table defaults to sort = TRUE and would
        # additionally sort the result by the join key, which neither dplyr nor
        # ibex is asked to do here. `y[x, on=]` preserves x's row order (same as
        # dplyr) and is content-identical. See bench_r.R for the full rationale.
        bench_three("null_left_join", function() dt_lookup[dt, on = "symbol"], function() tb |> left_join(tb_lookup, by = "symbol"), function() ib |> left_join(ib_lookup, by = "symbol") |> collect())
        bench_three("inner_join_symbol", function() dt_lookup[dt, on = "symbol", nomatch = NULL], function() tb |> inner_join(tb_lookup, by = "symbol"), function() ib |> inner_join(ib_lookup, by = "symbol") |> collect())
        bench_three("null_semi_join", function() dt[symbol %chin% dt_lookup_symbols], function() tb |> semi_join(tb_lookup_symbols, by = "symbol"), function() ib |> semi_join(ib_lookup_symbols, by = "symbol") |> collect())
        bench_three("null_anti_join", function() dt[!symbol %chin% dt_lookup_symbols], function() tb |> anti_join(tb_lookup_symbols, by = "symbol"), function() ib |> anti_join(ib_lookup_symbols, by = "symbol") |> collect())
        rm(dt_lookup, tb_lookup, ib_lookup, dt_lookup_symbols, tb_lookup_symbols, ib_lookup_symbols)
    }

    rm(dt, tb, ib)
}

phase_multi <- function() {
    dt_multi <- fread(csv_multi_path)
    tb_multi <- as_shared_tibble(dt_multi)
    ib_multi <- ibex_source(tb_multi)
    bench_three("count_by_symbol_day",
        function() dt_multi[, .(n = .N), by = .(symbol, day)],
        function() tb_multi |> group_by(symbol, day) |> summarise(n = n(), .groups = "drop"),
        function() ib_multi |> group_by(symbol, day) |> summarise(n = n(), .groups = "drop") |> collect())
    bench_three("mean_by_symbol_day",
        function() dt_multi[, .(avg_price = mean(price)), by = .(symbol, day)],
        function() tb_multi |> group_by(symbol, day) |> summarise(avg_price = mean(price), .groups = "drop"),
        function() ib_multi |> group_by(symbol, day) |> summarise(avg_price = mean(price), .groups = "drop") |> collect())
    bench_three("ohlc_by_symbol_day",
        function() dt_multi[, .(open = data.table::first(price), high = max(price), low = min(price), last = data.table::last(price)), by = .(symbol, day)],
        function() tb_multi |> group_by(symbol, day) |> summarise(open = dplyr::first(price), high = max(price), low = min(price), last = dplyr::last(price), .groups = "drop"),
        function() ib_multi |> group_by(symbol, day) |> summarise(open = dplyr::first(price), high = max(price), low = min(price), last = dplyr::last(price), .groups = "drop") |> collect())
    rm(dt_multi, tb_multi, ib_multi)
}

phase_trades <- function() {
    dt_trades <- fread(csv_trades_path)
    tb_trades <- as_shared_tibble(dt_trades)
    ib_trades <- ibex_source(tb_trades)
    bench_three("filter_simple", function() dt_trades[price > 500.0], function() tb_trades |> filter(price > 500.0), function() ib_trades |> filter(price > 500.0) |> collect())
    bench_three("filter_and", function() dt_trades[price > 500.0 & qty < 100], function() tb_trades |> filter(price > 500.0, qty < 100), function() ib_trades |> filter(price > 500.0, qty < 100) |> collect())
    bench_three("filter_arith", function() dt_trades[price * qty > 50000.0], function() tb_trades |> filter(price * qty > 50000.0), function() ib_trades |> filter(price * qty > 50000.0) |> collect())
    bench_three("filter_or", function() dt_trades[price > 900.0 | qty < 10], function() tb_trades |> filter(price > 900.0 | qty < 10), function() ib_trades |> filter(price > 900.0 | qty < 10) |> collect())
    rm(dt_trades, tb_trades, ib_trades)
}

phase_events_users <- function() {
    dt_events <- fread(csv_events_path)
    tb_events <- as_shared_tibble(dt_events)
    ib_events <- ibex_source(tb_events)
    bench_three("sum_by_user", function() dt_events[, .(total = sum(amount)), by = user_id], function() tb_events |> group_by(user_id) |> summarise(total = sum(amount), .groups = "drop"), function() ib_events |> group_by(user_id) |> summarise(total = sum(amount), .groups = "drop") |> collect())
    bench_three("filter_events", function() dt_events[amount > 500.0], function() tb_events |> filter(amount > 500.0), function() ib_events |> filter(amount > 500.0) |> collect())
    if (!is.null(csv_users_path)) {
        dt_users <- fread(csv_users_path)
        tb_users <- as_shared_tibble(dt_users)
        ib_users <- ibex_source(tb_users)
        bench_three("inner_join_user", function() dt_users[dt_events, on = "user_id", nomatch = NULL], function() tb_events |> inner_join(tb_users, by = "user_id"), function() ib_events |> inner_join(ib_users, by = "user_id") |> collect())
        bench_three("join_update_group", function() dt_users[dt_events, on = "user_id", nomatch = NULL][, revenue := amount * user_tier_multiplier][, .(total_rev = sum(revenue)), by = .(symbol, user_segment)], function() tb_events |> inner_join(tb_users, by = "user_id") |> mutate(revenue = amount * user_tier_multiplier) |> group_by(symbol, user_segment) |> summarise(total_rev = sum(revenue), .groups = "drop"), function() ib_events |> inner_join(ib_users, by = "user_id") |> mutate(revenue = amount * user_tier_multiplier) |> group_by(symbol, user_segment) |> summarise(total_rev = sum(revenue), .groups = "drop") |> collect())
        rm(dt_users, tb_users, ib_users)
    }
    rm(dt_events, tb_events, ib_events)
}

run_phase("prices + lookup: matched website queries", phase_prices_lookup)
if (!is.null(csv_multi_path)) run_phase("prices_multi: matched website queries", phase_multi)
if (!is.null(csv_trades_path)) run_phase("trades: matched website queries", phase_trades)
if (!is.null(csv_events_path)) run_phase("events + users: matched website queries", phase_events_users)

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
write.table(do.call(rbind, results), out_path, sep = "\t", row.names = FALSE, quote = FALSE)
message("\nWrote ", out_path)
