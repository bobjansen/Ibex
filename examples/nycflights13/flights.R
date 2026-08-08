# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# ─────────────────────────────────────────────────────────────────────────────
#  Ibex + R · nycflights13
#
#  An end-to-end example of the `ibex` bindings. The division of labour is the
#  point: every query lives in flights.ibex, R binds the data, sources that file
#  once, and then pulls each named result out of the same session as a plain
#  data.frame that goes straight into ggplot2. No Ibex source is embedded in
#  this script.
#
#  Run from the repository root:
#      Rscript examples/nycflights13/flights.R
#
#  Requires: an existing build (build-release/ or build/) and
#      R -e 'install.packages(c("nycflights13", "ggplot2"))'
#      IBEX_ROOT=$PWD IBEX_BUILD_DIR=$PWD/build-release R CMD INSTALL r/ibex
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
    library(ibex)
    library(nycflights13)
})

example_dir <- file.path("examples", "nycflights13")
query_file <- file.path(example_dir, "flights.ibex")
out_dir <- Sys.getenv("IBEX_EXAMPLE_OUT", unset = file.path(example_dir, "plots"))
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

stopifnot(file.exists(query_file))

# ── Bind the data and run the queries ────────────────────────────────────────
#   `tables` and `scalars` are visible for the duration of this one call. The
#   `let` bindings that flights.ibex creates from them outlive it, so the 336k
#   -row flights table is handed to the engine once rather than once per query.
the_year <- 2013L
min_flights <- 1000L

sess <- create_session()
session_eval_file(
    sess, query_file,
    tables = list(
        flights  = as.data.frame(flights),
        airlines = as.data.frame(airlines),
        planes   = as.data.frame(planes),
        weather  = as.data.frame(weather)
    ),
    scalars = list(min_flights = min_flights, the_year = the_year)
)

# ── Collect the results ──────────────────────────────────────────────────────
#   Naming a binding is a complete Ibex statement, so fetching a result is a
#   one-liner. `format = "nanoarrow"` skips the data.frame conversion and hands
#   back the Arrow C Data array for zero-copy use elsewhere.
fetch <- function(name, format = "data.frame") {
    session_eval(sess, paste0(name, ";"), format = format)
}

daily <- fetch("daily")
# `resample` keeps the name of the time index it bucketed. The dplyr and
# data.table versions of this example produce a plain Date called `day`, so
# match them — the shared plotting layer and compare.R expect one shape.
daily <- data.frame(
    day = as.Date(daily$time_hour),
    daily[c("flights", "avg_dep_delay", "rolling_7d")]
)

results <- list(
    carriers = fetch("carriers"),
    hourly = fetch("hourly"),
    daily = daily,
    by_age = fetch("by_age"),
    by_visib = fetch("by_visib")
)
origin_counts <- fetch("origin_counts", format = "nanoarrow")

# ── Report and plot ──────────────────────────────────────────────────────────
#   The ggplot layer is shared with flights_dplyr.R and flights_datatable.R, so
#   the three scripts differ only in how they express the queries. Skipped when
#   the script is sourced rather than run, so compare.R can collect `results`
#   without redrawing anything.
if (sys.nframe() == 0L) {
    print(results$carriers)
    print(results$by_visib)

    source(file.path(example_dir, "plots.R"))
    save_nycflights_plots(results, out_dir, "Ibex", min_flights)

    message("nanoarrow result: ", class(origin_counts)[1], " with ",
            origin_counts$length, " rows, ", length(origin_counts$children), " columns")
    message("done — ", length(list.files(out_dir, pattern = "[.]png$")), " plots in ", out_dir)
}

reset_session(sess)
