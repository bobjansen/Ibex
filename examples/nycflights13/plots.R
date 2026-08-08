# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# ─────────────────────────────────────────────────────────────────────────────
#  Shared plotting layer for the nycflights13 example.
#
#  flights.R (Ibex), flights_dplyr.R, and flights_datatable.R all compute the
#  same six results and hand them here, so the three scripts differ only in how
#  they express the queries. Sourced, not library()'d:
#
#      source(file.path(example_dir, "plots.R"))
#      save_nycflights_plots(results, out_dir, "dplyr")
#
#  `results` is a list with elements carriers, hourly, daily, by_age, by_visib,
#  each a data.frame with the columns the corresponding query produces.
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages(library(ggplot2))

save_nycflights_plots <- function(results, out_dir, engine, min_flights = 1000L) {
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

    save_plot <- function(plot, file, width = 8, height = 5) {
        path <- file.path(out_dir, file)
        ggsave(path, plot, width = width, height = height, dpi = 150)
        message("wrote ", path)
    }

    caption <- paste("Computed with", engine)

    p_carriers <- ggplot(
        results$carriers,
        aes(reorder(name, avg_arr_delay), avg_arr_delay)
    ) +
        geom_col(fill = "#3b6ea5") +
        coord_flip() +
        labs(
            title = "Mean arrival delay by carrier, NYC 2013",
            subtitle = sprintf("Carriers with at least %d scored flights", min_flights),
            x = NULL, y = "Mean arrival delay (minutes)", caption = caption
        ) +
        theme_minimal()
    save_plot(p_carriers, "carrier_delays.png", height = 6)

    p_hourly <- ggplot(results$hourly, aes(hour, avg_dep_delay, colour = origin)) +
        geom_line(linewidth = 0.9) +
        geom_point(size = 1.2) +
        labs(
            title = "Departure delay builds through the day",
            x = "Scheduled departure hour", y = "Mean departure delay (minutes)",
            colour = "Origin", caption = caption
        ) +
        theme_minimal()
    save_plot(p_hourly, "hourly_profile.png")

    p_daily <- ggplot(results$daily, aes(day)) +
        geom_line(aes(y = avg_dep_delay), colour = "grey70", linewidth = 0.4) +
        geom_line(aes(y = rolling_7d), colour = "#b5482a", linewidth = 1) +
        labs(
            title = "Daily mean departure delay with a 7-day moving average",
            subtitle = "Grey: daily mean. Red: trailing 7-day mean",
            x = NULL, y = "Departure delay (minutes)", caption = caption
        ) +
        theme_minimal()
    save_plot(p_daily, "daily_rolling.png")

    # The `flights >= 500` cutoff leaves the surviving ages with a hole in them:
    # fleets thin out after age 28, but one old cohort (age 37) is still flown
    # often enough to clear the bar. Fitting a smooth across that gap would draw
    # a trend through nine years of absent data, so the curve is fitted only to
    # the leading run of consecutive ages. The isolated cohort is still plotted —
    # it is a real number, just not something to interpolate towards.
    by_age <- results$by_age
    supported <- by_age[seq_len(which(c(diff(by_age$age), Inf) != 1)[1]), ]

    p_age <- ggplot(by_age, aes(age, avg_arr_delay)) +
        geom_point(aes(size = flights), colour = "#3b6ea5", alpha = 0.7) +
        geom_smooth(
            data = supported, aes(weight = flights),
            method = "loess", formula = y ~ x, se = FALSE, colour = "#b5482a"
        ) +
        labs(
            title = "Arrival delay by aircraft age",
            subtitle = sprintf(
                "Ages with at least 500 scored flights. Trend fitted on ages %d-%d, where cohorts are consecutive",
                min(supported$age), max(supported$age)
            ),
            x = "Aircraft age in 2013 (years)", y = "Mean arrival delay (minutes)",
            size = "Flights", caption = caption
        ) +
        theme_minimal()
    save_plot(p_age, "aircraft_age.png")

    p_visib <- ggplot(results$by_visib, aes(visib, avg_dep_delay)) +
        geom_point(aes(size = flights), colour = "#4a7c59", alpha = 0.8) +
        labs(
            title = "Departure delay against reported visibility",
            x = "Visibility (miles)", y = "Mean departure delay (minutes)",
            size = "Flights", caption = caption
        ) +
        theme_minimal()
    save_plot(p_visib, "visibility.png")

    invisible(list.files(out_dir, pattern = "[.]png$", full.names = TRUE))
}
