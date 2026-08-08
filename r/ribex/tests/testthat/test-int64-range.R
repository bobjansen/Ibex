# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# R has no 64-bit integer vector, so Ibex's Int64 meets R twice and awkwardly
# in both directions.
#
# Coming in, `bit64::integer64` is the one R type that carries the full range,
# and it carries it in the payload of a double vector -- only the class
# attribute says the bits are an int64. A binder dispatching on `TYPEOF` alone
# sees a REALSXP and reads each element as the double those bits spell, which
# is silent corruption of exactly the values a caller chose bit64 to hold.
#
# Going out, there is no R type to receive an Int64 without loss, so it becomes
# a double. Narrowing to `integer()` when the values happen to fit is
# deliberately not done: it would make a column's R type depend on its
# contents, so filtering out one large row would quietly change it. Instead the
# conversion says when it may have cost something.

skip_if_no_bit64 <- function() {
    skip_if_not_installed("bit64")
}

test_that("a bit64 column binds as Int64 rather than as its own bit pattern", {
    skip_if_no_bit64()

    # 2^53 + 1: the smallest integer a double cannot represent, so reading
    # these bits as a double is both wrong and detectable.
    values <- bit64::as.integer64(c("9007199254740993", "-9007199254740993", NA, "42"))
    table <- ibex_tbl(data.frame(id = values, tag = c("a", "b", "c", "d")),
                      fallback = "error")

    expect_identical(table$schema$types, c("Int64", "String"))
    # Read as a double, the first element is 4.45e-308 -- a denormal, not a
    # large number. The type assertion above is what actually catches that, but
    # nullability confirms the NA sentinel was understood as a null and not as
    # the huge negative integer bit64 spells it with.
    expect_identical(table$schema$nullable, c(TRUE, FALSE))

    collected <- suppressWarnings(dplyr::collect(table))
    expect_true(is.na(collected$id[3]))
    expect_equal(collected$id[4], 42)
})

test_that("a bit64 key keeps values a double would collapse together", {
    skip_if_no_bit64()

    # These two differ by one, and both round to the same double. A join that
    # went through a double would match every row to every row.
    keys <- bit64::as.integer64(c("9007199254740993", "9007199254740992"))
    left <- data.frame(k = keys, v = c(1L, 2L))
    right <- data.frame(k = keys[1], w = 9)

    joined <- suppressWarnings(dplyr::collect(
        dplyr::inner_join(ibex_tbl(left, fallback = "error"), right, by = "k")
    ))
    expect_identical(nrow(joined), 1L)
    expect_equal(joined$v, 1)
})

test_that("an Int64 value a double cannot hold is not converted in silence", {
    skip_if_no_bit64()

    table <- ibex_tbl(data.frame(id = bit64::as.integer64("9007199254740993")),
                      fallback = "error")
    expect_warning(dplyr::collect(table), "may have lost precision")
    # The column is named, because a wide result gives the caller no other way
    # to find which one to worry about.
    expect_warning(dplyr::collect(table), "id")
})

test_that("Int64 values a double holds exactly convert quietly", {
    # The common case by far -- an R integer column becomes Int64 on the way
    # in, and must not warn its way back out.
    expect_silent(dplyr::collect(ibex_tbl(data.frame(x = 1:3), fallback = "error")))

    skip_if_no_bit64()
    # Just inside the boundary, where a double is still exact.
    expect_silent(dplyr::collect(
        ibex_tbl(data.frame(x = bit64::as.integer64("9007199254740991")), fallback = "error")
    ))
})

test_that("the R type of an Int64 column does not depend on its values", {
    skip_if_no_bit64()

    # The reason `integer()` is not restored when the values would fit: these
    # two columns hold the same kind of thing and differ only in magnitude, so
    # a type that tracked the data would disagree about them -- and a filter
    # dropping the large row would change a column's class.
    small <- dplyr::collect(ibex_tbl(data.frame(x = 1:3), fallback = "error"))
    large <- suppressWarnings(dplyr::collect(
        ibex_tbl(data.frame(x = bit64::as.integer64("9007199254740993")), fallback = "error")
    ))
    expect_identical(class(small$x), class(large$x))
    expect_identical(class(small$x), "numeric")
})
