# ─────────────────────────────────────────────────────────────────────────────
#  nycflights13 · the dplyr rendering
#
#  The same six results as flights.ibex, written the way a dplyr user would
#  write them, so the two query styles can be read side by side. The plotting
#  layer is shared (plots.R), so this file is query code and nothing else.
#
#  compare.R checks that this and the Ibex version agree to the last digit.
#
#  Run from the repository root:
#      Rscript examples/nycflights13/flights_dplyr.R
#
#  Requires: R -e 'install.packages(c("nycflights13", "ggplot2", "dplyr", "slider"))'
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
    library(nycflights13)
    library(dplyr)
    library(slider)
})

example_dir <- file.path("examples", "nycflights13")
out_dir <- Sys.getenv("IBEX_EXAMPLE_OUT", unset = file.path(example_dir, "plots-dplyr"))

the_year <- 2013L
min_flights <- 1000L

# ── 1. Carrier delay ranking ─────────────────────────────────────────────────
carriers <- flights |>
    filter(!is.na(arr_delay)) |>
    summarise(flights = n(), avg_arr_delay = mean(arr_delay), .by = carrier) |>
    filter(flights >= min_flights) |>
    inner_join(airlines, by = "carrier") |>
    arrange(desc(avg_arr_delay))

# ── 2. Hourly departure profile per origin ───────────────────────────────────
hourly <- flights |>
    filter(!is.na(dep_delay)) |>
    summarise(flights = n(), avg_dep_delay = mean(dep_delay), .by = c(origin, hour)) |>
    arrange(origin, hour)

# ── 3. Daily delays with a trailing 7-day mean ───────────────────────────────
#   `as.Date()` on a POSIXct buckets in UTC, which is what Ibex's `resample 1d`
#   does to the underlying instant. slide_index_dbl indexes the window by date
#   rather than by row, so it is a genuine 7-day window and, like Ibex's
#   `window 7d`, it averages a partial window over the first six days.
daily <- flights |>
    filter(!is.na(dep_delay)) |>
    mutate(day = as.Date(time_hour)) |>
    summarise(flights = n(), avg_dep_delay = mean(dep_delay), .by = day) |>
    arrange(day) |>
    mutate(rolling_7d = slide_index_dbl(avg_dep_delay, day, mean, .before = 6))

# ── 4. Aircraft age vs. arrival delay ────────────────────────────────────────
#   `planes` also has a `year`, so the manufacture year is renamed on the way in
#   rather than left to the join's .x/.y suffixes.
by_age <- flights |>
    filter(!is.na(arr_delay), !is.na(tailnum)) |>
    inner_join(select(planes, tailnum, plane_year = year), by = "tailnum") |>
    filter(!is.na(plane_year)) |>
    mutate(age = the_year - plane_year) |>
    summarise(flights = n(), avg_arr_delay = mean(arr_delay), .by = age) |>
    filter(flights >= 500) |>
    arrange(age)

# ── 5. Departure delay against visibility ────────────────────────────────────
by_visib <- flights |>
    filter(!is.na(dep_delay)) |>
    inner_join(weather, by = c("origin", "time_hour")) |>
    filter(!is.na(visib)) |>
    summarise(flights = n(), avg_dep_delay = mean(dep_delay), .by = visib) |>
    arrange(visib)

# ── 6. Flights per origin ────────────────────────────────────────────────────
origin_counts <- flights |>
    summarise(flights = n(), .by = origin) |>
    arrange(origin)

results <- list(carriers = carriers, hourly = hourly, daily = daily,
                by_age = by_age, by_visib = by_visib, origin_counts = origin_counts)

if (sys.nframe() == 0L) {
    print(carriers)
    print(by_visib)

    source(file.path(example_dir, "plots.R"))
    save_nycflights_plots(results, out_dir, "dplyr", min_flights)
    message("done — 5 plots in ", out_dir)
}
