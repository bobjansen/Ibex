# ─────────────────────────────────────────────────────────────────────────────
#  Ibex + R · nycflights13
#
#  An end-to-end example of the `ribex` bindings. The division of labour is the
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
#      IBEX_ROOT=$PWD IBEX_BUILD_DIR=$PWD/build-release R CMD INSTALL r/ribex
# ─────────────────────────────────────────────────────────────────────────────

suppressPackageStartupMessages({
    library(ribex)
    library(nycflights13)
    library(ggplot2)
})

example_dir <- file.path("examples", "nycflights13")
query_file <- file.path(example_dir, "flights.ibex")
out_dir <- Sys.getenv("IBEX_EXAMPLE_OUT", unset = file.path(example_dir, "plots"))
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

stopifnot(file.exists(query_file))

save_plot <- function(plot, file, width = 8, height = 5) {
    path <- file.path(out_dir, file)
    ggsave(path, plot, width = width, height = height, dpi = 150)
    message("wrote ", path)
}

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

carriers <- fetch("carriers")
hourly <- fetch("hourly")
daily <- fetch("daily")
by_age <- fetch("by_age")
by_visib <- fetch("by_visib")
origin_counts <- fetch("origin_counts", format = "nanoarrow")

print(carriers)
print(by_visib)

# ── Plot ─────────────────────────────────────────────────────────────────────
p_carriers <- ggplot(carriers, aes(reorder(name, avg_arr_delay), avg_arr_delay)) +
    geom_col(fill = "#3b6ea5") +
    coord_flip() +
    labs(
        title = "Mean arrival delay by carrier, NYC 2013",
        subtitle = sprintf("Carriers with at least %d scored flights", min_flights),
        x = NULL, y = "Mean arrival delay (minutes)"
    ) +
    theme_minimal()
save_plot(p_carriers, "carrier_delays.png", height = 6)

p_hourly <- ggplot(hourly, aes(hour, avg_dep_delay, colour = origin)) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 1.2) +
    labs(
        title = "Departure delay builds through the day",
        x = "Scheduled departure hour", y = "Mean departure delay (minutes)",
        colour = "Origin"
    ) +
    theme_minimal()
save_plot(p_hourly, "hourly_profile.png")

p_daily <- ggplot(daily, aes(time_hour)) +
    geom_line(aes(y = avg_dep_delay), colour = "grey70", linewidth = 0.4) +
    geom_line(aes(y = rolling_7d), colour = "#b5482a", linewidth = 1) +
    labs(
        title = "Daily mean departure delay with a 7-day moving average",
        subtitle = "Grey: daily mean. Red: trailing 7-day mean, computed by Ibex",
        x = NULL, y = "Departure delay (minutes)"
    ) +
    theme_minimal()
save_plot(p_daily, "daily_rolling.png")

# The `flights >= 500` cutoff in flights.ibex leaves the surviving ages with a
# hole in them: fleets thin out after age 28, but one old cohort (age 37) is
# still flown often enough to clear the bar. Fitting a smooth across that gap
# would draw a trend through nine years of absent data, so the curve is fitted
# only to the leading run of consecutive ages. The isolated cohort is still
# plotted — it is a real number, just not something to interpolate towards.
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
        size = "Flights"
    ) +
    theme_minimal()
save_plot(p_age, "aircraft_age.png")

p_visib <- ggplot(by_visib, aes(visib, avg_dep_delay)) +
    geom_point(aes(size = flights), colour = "#4a7c59", alpha = 0.8) +
    labs(
        title = "Departure delay against reported visibility",
        x = "Visibility (miles)", y = "Mean departure delay (minutes)",
        size = "Flights"
    ) +
    theme_minimal()
save_plot(p_visib, "visibility.png")

message("nanoarrow result: ", class(origin_counts)[1], " with ",
        origin_counts$length, " rows, ", length(origin_counts$children), " columns")

reset_session(sess)
message("done — ", length(list.files(out_dir, pattern = "[.]png$")), " plots in ", out_dir)
