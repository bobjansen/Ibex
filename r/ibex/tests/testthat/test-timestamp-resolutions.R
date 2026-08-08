# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# R emits `tsu:<zone>` for a POSIXct -- microseconds, with an IANA zone. Ibex
# stores an instant in nanoseconds, so the import rescales the unit and carries
# the zone as column metadata. The instant needs no adjustment (an Arrow
# timestamp is UTC-relative whenever a zone is present); the zone is what says
# which wall clock to render it on, so keeping it is what stops a round trip
# from handing a producer back its own data relabelled UTC.
#
# Before the units were handled, the nanoarrow path rejected every POSIXct.

skip_if_no_nanoarrow <- function() {
    skip_if_not_installed("nanoarrow")
}

test_that("a zoned POSIXct binds through the Arrow path with its instant intact", {
    skip_if_no_nanoarrow()

    local <- as.POSIXct("2013-01-01 23:30:00", tz = "America/New_York")
    input <- nanoarrow::as_nanoarrow_array(data.frame(t = local, v = 1.5))

    out <- eval_ibex("src;", tables = list(src = input))

    # 23:30 in New York is 04:30 the next day in UTC.
    expect_equal(format(out$t, tz = "UTC"), "2013-01-02 04:30:00")
    expect_equal(as.numeric(out$t), as.numeric(local))
})

test_that("a UTC POSIXct binds unchanged", {
    skip_if_no_nanoarrow()

    utc <- as.POSIXct("2013-01-01 23:30:00", tz = "UTC")
    input <- nanoarrow::as_nanoarrow_array(data.frame(t = utc, v = 1.5))

    out <- eval_ibex("src;", tables = list(src = input))
    expect_equal(as.numeric(out$t), as.numeric(utc))
})

test_that("nulls survive the rescaling import", {
    skip_if_no_nanoarrow()

    stamps <- as.POSIXct(c("2013-06-01 12:00:00", NA), tz = "America/New_York")
    input <- nanoarrow::as_nanoarrow_array(data.frame(t = stamps, v = c(1.5, 2.5)))

    out <- eval_ibex("src;", tables = list(src = input))
    expect_false(is.na(out$t[1]))
    expect_true(is.na(out$t[2]))
})

test_that("timestamps bound through a data.frame agree with the Arrow path", {
    skip_if_no_nanoarrow()

    stamps <- as.POSIXct(c("2013-01-01 23:30:00", "2013-07-04 09:15:00"),
                         tz = "America/New_York")
    df <- data.frame(t = stamps, v = c(1.5, 2.5))

    via_frame <- eval_ibex("src;", tables = list(src = df))
    via_arrow <- eval_ibex("src;", tables = list(src = nanoarrow::as_nanoarrow_array(df)))

    expect_equal(as.numeric(via_frame$t), as.numeric(via_arrow$t))
    expect_equal(as.numeric(via_frame$t), as.numeric(stamps))
})

test_that("a POSIXct comes back in the zone it was handed over in", {
    skip_if_no_nanoarrow()

    stamps <- as.POSIXct(c("2013-01-01 23:30:00", "2013-07-04 09:15:00"),
                         tz = "America/New_York")
    input <- nanoarrow::as_nanoarrow_array(data.frame(t = stamps, v = c(1.5, 2.5)))

    out <- eval_ibex("src;", tables = list(src = input))

    expect_equal(attr(out$t, "tzone"), "America/New_York")
    # Same wall clock the caller supplied, not the UTC rendering of it.
    expect_equal(format(out$t), format(stamps))
})

test_that("a zoned index resamples on local days, not UTC days", {
    skip_if_no_nanoarrow()
    skip_if_not_installed("nycflights13")

    f <- as.data.frame(nycflights13::flights)[c("time_hour", "dep_delay")]
    f <- f[!is.na(f$dep_delay), ]
    query <- paste(
        'let tf = as_timeframe(src, "time_hour");',
        "tf[resample 1d, select { n = count() }];"
    )

    # The data.frame path drops `tzone`, so the instants bucket on the UTC grid.
    utc <- suppressWarnings(eval_ibex(query, tables = list(src = f)))
    # The Arrow path keeps the zone, so the buckets are New York calendar days.
    local <- suppressWarnings(
        eval_ibex(query, tables = list(src = nanoarrow::as_nanoarrow_array(f)))
    )

    expect_equal(nrow(utc), length(unique(as.Date(f$time_hour))))
    expect_equal(nrow(local),
                 length(unique(as.Date(f$time_hour, tz = "America/New_York"))))
    # 2013 has 366 UTC days' worth of NYC departures but only 365 local ones.
    expect_equal(nrow(utc), 366L)
    expect_equal(nrow(local), 365L)
})

test_that("an R integer column binds through the Arrow path", {
    skip_if_no_nanoarrow()

    # R integers are Arrow int32, which is spelled "i" -- the same as the usual
    # dictionary index type. Reading that as "always categorical" used to reject
    # every integer column with a complaint about missing dictionary storage.
    df <- data.frame(i = c(1L, 2L, NA_integer_), v = c(1.5, 2.5, 3.5),
                     f = factor(c("a", "b", "a")))
    out <- suppressWarnings(
        eval_ibex("src;", tables = list(src = nanoarrow::as_nanoarrow_array(df)))
    )

    expect_equal(out$i, c(1, 2, NA))
    # The factor is also "i", but with dictionary storage: still a categorical.
    expect_equal(out$f, c("a", "b", "a"))
})
