# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# `.by` is dplyr's per-call grouping: it names the keys for one verb and leaves
# the result ungrouped, where `group_by()` sets grouping that persists until
# something clears it. Ibex has a single grouping construct -- the `by` clause
# on the bracket the verb becomes -- so both spellings lower to the same plan
# and differ only in what the frontend records about the result.
#
# The tests below pin the two halves separately: that the *values* match local
# dplyr, and that the *grouping metadata* afterwards does.

sales <- function() {
    tibble::tibble(
        region = c("north", "south", "north", "south", "north"),
        product = c("a", "a", "b", "b", "a"),
        units = c(2L, 5L, 3L, 7L, 11L)
    )
}

test_that("`.by` groups a summarise without grouping its result", {
    input <- sales()
    query <- ibex_tbl(input, fallback = "error")

    out <- dplyr::summarise(query, total = sum(units), .by = region)

    expect_identical(dplyr::group_vars(out), character())
    expect_identical(out$schema$names, c("region", "total"))
    expect_equal(
        dplyr::arrange(dplyr::collect(out), region),
        dplyr::arrange(dplyr::summarise(input, total = sum(units), .by = region), region)
    )
})

test_that("`.by` takes a tidyselect expression, not just one column", {
    input <- sales()
    query <- ibex_tbl(input, fallback = "error")

    out <- dplyr::summarise(query, total = sum(units), .by = c(region, product))
    expect_identical(out$schema$names, c("region", "product", "total"))
    expect_equal(
        dplyr::arrange(dplyr::collect(out), region, product),
        dplyr::arrange(
            dplyr::summarise(input, total = sum(units), .by = c(region, product)),
            region, product
        )
    )
})

test_that("`.by` reports groups in order of first appearance, as dplyr does", {
    # The visible difference between the two spellings: `group_by()` sorts the
    # groups, `.by` does not. Nothing in the plan asks Ibex to sort, so this is
    # a check that the two happen to agree rather than a promise the adapter
    # makes -- ordering after an aggregate is not claimed (see `ordering`).
    input <- tibble::tibble(g = c("z", "a", "z", "m"), v = 1:4)
    query <- ibex_tbl(input, fallback = "error")

    expect_identical(
        dplyr::collect(dplyr::summarise(query, n = dplyr::n(), .by = g))$g,
        dplyr::summarise(input, n = dplyr::n(), .by = g)$g
    )
})

test_that("`.by` on a mutate broadcasts per group and does not outlive the call", {
    input <- sales()
    query <- ibex_tbl(input, fallback = "error")

    out <- dplyr::mutate(query, region_total = sum(units), .by = region)

    expect_identical(dplyr::group_vars(out), character())
    expect_equal(
        dplyr::collect(out),
        tibble::as_tibble(dplyr::mutate(input, region_total = sum(units), .by = region))
    )
})

test_that("`.by` on a filter is accepted, since a row-local predicate ignores it", {
    # No aggregate means the predicate asks the same question of a row whatever
    # group it sits in, so grouping cannot change the answer. `fallback =
    # "error"` is what makes this a claim about the native path.
    input <- sales()
    query <- ibex_tbl(input, fallback = "error")

    out <- dplyr::filter(query, units > 2, .by = region)
    expect_identical(dplyr::group_vars(out), character())
    expect_equal(
        dplyr::collect(out),
        tibble::as_tibble(dplyr::filter(input, units > 2, .by = region))
    )
})

test_that("a misuse of `.by` is an error, not a fallback", {
    # These three are mistakes in the call rather than gaps in the translation,
    # so they raise dplyr's own message. Routing them through the fallback
    # would collect the whole input only for local dplyr to reject it, and
    # under `fallback = "error"` would report a translation failure instead of
    # the real problem.
    input <- sales()
    query <- ibex_tbl(input, fallback = "collect")

    expect_error(
        dplyr::summarise(dplyr::group_by(query, region), n = dplyr::n(), .by = product),
        "grouped data frame"
    )
    expect_error(
        dplyr::summarise(query, n = dplyr::n(), .by = region, .groups = "drop"),
        "both `.by` and `.groups`"
    )
    expect_error(
        dplyr::summarise(query, n = dplyr::n(), .by = c(area = region)),
        "rename"
    )
})

test_that("a join carries a captured scalar from either side", {
    # A join renders the right plan inside the left one and evaluates the whole
    # thing with one scalar environment, so both sides' captures have to end up
    # in it. `fallback = "error"` proves the scalar crossed natively rather
    # than the join quietly collecting.
    threshold <- 3L
    left <- ibex_tbl(sales(), fallback = "error")
    right <- tibble::tibble(region = c("north", "south"), lead = c("ana", "bo"))

    from_left <- dplyr::inner_join(dplyr::filter(left, units > threshold), right, by = "region")
    expect_equal(
        dplyr::arrange(dplyr::collect(from_left), units),
        dplyr::arrange(
            tibble::as_tibble(dplyr::inner_join(dplyr::filter(sales(), units > threshold),
                                                right, by = "region")),
            units
        )
    )

    wanted <- "south"
    from_right <- dplyr::inner_join(
        ibex_tbl(sales(), fallback = "error", session = left$session),
        dplyr::filter(ibex_tbl(right, fallback = "error", session = left$session),
                      region == wanted),
        by = "region"
    )
    expect_equal(
        dplyr::arrange(dplyr::collect(from_right), units),
        dplyr::arrange(
            tibble::as_tibble(dplyr::inner_join(sales(),
                                                dplyr::filter(right, region == wanted),
                                                by = "region")),
            units
        )
    )
})

test_that("a join declines when both sides captured a scalar", {
    # Both sides name their captures from one upwards, so the two claim the
    # same name for different values. Renaming one side means rewriting the
    # references inside its rendered plan, which is not done yet -- so this
    # falls back rather than binding the wrong value.
    low <- 1L
    high <- 4L
    session <- ibex_tbl(sales())$session
    left <- dplyr::filter(ibex_tbl(sales(), fallback = "error", session = session), units > low)
    right <- dplyr::filter(
        ibex_tbl(tibble::tibble(region = c("north", "south"), cap = c(9L, 9L)),
                 fallback = "error", session = session),
        cap > high
    )

    expect_error(
        dplyr::inner_join(left, right, by = "region"),
        "captured scalars from both inputs"
    )
})
