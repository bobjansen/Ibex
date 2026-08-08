# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# ─────────────────────────────────────────────────────────────────────────────
#  nycflights13 · the data.table rendering
#
#  The same six results as flights.ibex, written the way a data.table user would
#  write them, so the two query styles can be read side by side. The chained-`[`
#  style lines up closely with Ibex's chained brackets: both read as a sequence
#  of clause blocks applied to a table, and both put the filter, the aggregate,
#  and the grouping in one place.
#
#  compare.R checks that this and the Ibex version agree to the last digit.
#
#  Run from the repository root:
#      Rscript examples/nycflights13/flights_datatable.R
#
#  Requires: R -e 'install.packages(c("nycflights13", "ggplot2", "data.table"))'
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
    library(nycflights13)
    library(data.table)
})

example_dir <- file.path("examples", "nycflights13")
out_dir <- Sys.getenv("IBEX_EXAMPLE_OUT", unset = file.path(example_dir, "plots-datatable"))

the_year <- 2013L
min_flights <- 1000L

fl <- as.data.table(flights)
al <- as.data.table(airlines)
wx <- as.data.table(weather)
# `planes` also has a `year`, which would collide on the join, so take the two
# columns that matter and rename on the way in.
pl <- as.data.table(planes)[, .(tailnum, plane_year = year)]

# ── 1. Carrier delay ranking ─────────────────────────────────────────────────
#   `X[Y, on = ..., nomatch = NULL]` is an inner join.
carriers <- fl[!is.na(arr_delay), .(flights = .N, avg_arr_delay = mean(arr_delay)), by = carrier
             ][flights >= min_flights
             ][al, on = "carrier", nomatch = NULL
             ][order(-avg_arr_delay)]

# ── 2. Hourly departure profile per origin ───────────────────────────────────
hourly <- fl[!is.na(dep_delay), .(flights = .N, avg_dep_delay = mean(dep_delay)),
             by = .(origin, hour)][order(origin, hour)]

# ── 3. Daily delays with a trailing 7-day mean ───────────────────────────────
#   `as.Date()` on a POSIXct buckets in UTC, which is what Ibex's `resample 1d`
#   does to the underlying instant. The daily series is gapless, so an adaptive
#   `frollmean` with window lengths 1, 2, ... 7 reproduces Ibex's `window 7d`
#   exactly, partial leading window included.
daily <- fl[!is.na(dep_delay), .(flights = .N, avg_dep_delay = mean(dep_delay)),
            by = .(day = as.Date(time_hour))
          ][order(day)
          ][, rolling_7d := frollmean(avg_dep_delay, pmin(seq_len(.N), 7L), adaptive = TRUE)][]

# ── 4. Aircraft age vs. arrival delay ────────────────────────────────────────
by_age <- fl[!is.na(arr_delay) & !is.na(tailnum)
           ][pl, on = "tailnum", nomatch = NULL
           ][!is.na(plane_year), .(flights = .N, avg_arr_delay = mean(arr_delay)),
             by = .(age = the_year - plane_year)
           ][flights >= 500][order(age)]

# ── 5. Departure delay against visibility ────────────────────────────────────
by_visib <- fl[!is.na(dep_delay)
             ][wx, on = .(origin, time_hour), nomatch = NULL
             ][!is.na(visib), .(flights = .N, avg_dep_delay = mean(dep_delay)), by = visib
             ][order(visib)]

# ── 6. Flights per origin ────────────────────────────────────────────────────
origin_counts <- fl[, .(flights = .N), by = origin][order(origin)]

results <- list(carriers = carriers, hourly = hourly, daily = daily,
                by_age = by_age, by_visib = by_visib, origin_counts = origin_counts)

if (sys.nframe() == 0L) {
    print(carriers)
    print(by_visib)

    source(file.path(example_dir, "plots.R"))
    save_nycflights_plots(results, out_dir, "data.table", min_flights)
    message("done — 5 plots in ", out_dir)
}
