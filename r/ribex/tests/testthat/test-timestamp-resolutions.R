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
