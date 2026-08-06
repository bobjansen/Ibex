# Adapted from poorman's tinytest suite at commit
# c9eb1f1429e6934e1b3233bb10c50c82adf05bd2 (c9eb1f1).
#
# Each case records its original tinytest file immediately above the adapted
# assertion.  See tests/poorman-c9eb1f1.md for the pinned-source and license
# record.  These tests intentionally use dplyr, rather than poorman, as their
# oracle: ribex implements dplyr's contract, not poorman's independent one.

poorman_dplyr_oracle <- function(input, operation) {
    expected <- operation(tibble::as_tibble(input))
    query <- operation(ibex_tbl(tibble::as_tibble(input), fallback = "error"))

    # A local tibble here would mean a fallback was taken.  Keep this assertion
    # before collect() so an unsupported operation cannot pass accidentally.
    expect_s3_class(query, "ibex_tbl")
    expect_equal(dplyr::collect(query), expected)
}

test_that("poorman filter cases execute natively", {
    input <- tibble::as_tibble(mtcars, rownames = "car")

    # Source: inst/tinytest/test_filter.R @ c9eb1f1 — multiple logical filters.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::filter(data, mpg > 20, cyl %in% c(4, 6))
    })

    # Source: inst/tinytest/test_filter.R @ c9eb1f1 — negation.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::filter(data, !(cyl %in% c(4, 6)), am != 0)
    })

    # Source: inst/tinytest/test_filter.R @ c9eb1f1 — membership has R NA semantics.
    poorman_dplyr_oracle(tibble::tibble(x = c(1L, NA_integer_, 2L)), function(data) {
        dplyr::filter(data, x %in% c(1L, NA_integer_))
    })
})

test_that("poorman mutate cases execute natively", {
    input <- tibble::tibble(x = 1:3, y = c(2L, 4L, 6L))

    # Source: inst/tinytest/test_mutate.R @ c9eb1f1 — add and transform columns.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::mutate(data, doubled = y * 2L, x = x + 1L)
    })

    # Source: inst/tinytest/test_mutate.R @ c9eb1f1 — progressive mutations.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::mutate(data, x2 = x + 1L, x3 = x2 + 1L)
    })
})

test_that("poorman select arrange and grouping cases execute natively", {
    input <- tibble::tibble(g = c("b", "a", "b", "a"), x = c(2L, 3L, 1L, 4L), y = 11:14)

    # Source: inst/tinytest/test_select.R @ c9eb1f1 — named selection and order.
    poorman_dplyr_oracle(input, function(data) dplyr::select(data, value = y, g, x))

    # Source: inst/tinytest/test_arrange.R @ c9eb1f1 — ascending and descending keys.
    poorman_dplyr_oracle(input, function(data) dplyr::arrange(data, g, dplyr::desc(x)))

    # Source: inst/tinytest/test_group_by.R @ c9eb1f1 — existing-column grouping.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::group_by(data, g) |> dplyr::arrange(x, .by_group = TRUE)
    })
})

test_that("poorman summarise distinct count and tally cases execute natively", {
    input <- tibble::tibble(g = c("a", "a", "b", "b"), x = c(1L, 2L, 3L, 4L), w = c(2, 1, 4, 3))

    # Source: inst/tinytest/test_summarise.R @ c9eb1f1 — named grouped summaries.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::group_by(data, g) |>
            dplyr::summarise(total = sum(x), average = mean(w), .groups = "drop")
    })

    # Source: inst/tinytest/test_distinct.R @ c9eb1f1 — selected distinct columns.
    poorman_dplyr_oracle(input, function(data) dplyr::distinct(data, g))

    # Source: inst/tinytest/test_count_tally.R @ c9eb1f1 — weighted sorted count.
    poorman_dplyr_oracle(input, function(data) dplyr::count(data, g, wt = w, sort = TRUE))

    # Source: inst/tinytest/test_count_tally.R @ c9eb1f1 — grouped tally.
    poorman_dplyr_oracle(input, function(data) dplyr::group_by(data, g) |> dplyr::tally())
})

test_that("poorman relocate rename and slice-head cases execute natively", {
    input <- tibble::tibble(a = 1:4, b = 5:8, c = c("x", "y", "x", "z"))

    # Source: inst/tinytest/test_relocate.R @ c9eb1f1 — relocate after a column.
    poorman_dplyr_oracle(input, function(data) dplyr::relocate(data, c, .after = a))

    # Source: inst/tinytest/test_rename.R @ c9eb1f1 — rename multiple columns.
    poorman_dplyr_oracle(input, function(data) dplyr::rename(data, alpha = a, charlie = c))

    # Source: inst/tinytest/test_slice.R @ c9eb1f1 — slice_head truncates at input size.
    poorman_dplyr_oracle(input, function(data) dplyr::slice_head(data, n = 6L))
})

test_that("poorman pull cases use an error-policy ibex source", {
    input <- tibble::tibble(a = 1:3, b = c("x", "y", "z"))

    # Source: inst/tinytest/test_pull.R @ c9eb1f1 — quoted and negative positions.
    expect_equal(
        dplyr::pull(ibex_tbl(input, fallback = "error"), "b"),
        dplyr::pull(input, "b")
    )
    expect_equal(
        dplyr::pull(ibex_tbl(input, fallback = "error"), -1L),
        dplyr::pull(input, -1L)
    )
})

test_that("poorman-derived rank cases execute natively", {
    input <- tibble::tibble(g = c("a", "a", "b", "b"), score = c(20, 10, 10, 10))

    # Source: inst/tinytest/test_window_rank.R @ c9eb1f1 — rank tie methods.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::mutate(
            data,
            minimum = dplyr::min_rank(score),
            dense = dplyr::dense_rank(score),
            ordinal = dplyr::row_number(score),
            cumulative = dplyr::cume_dist(score)
        )
    })

    # Source: inst/tinytest/test_window_rank.R @ c9eb1f1 — grouped ranks reset.
    poorman_dplyr_oracle(input, function(data) {
        dplyr::group_by(data, g) |> dplyr::mutate(dense = dplyr::dense_rank(score))
    })
})

test_that("poorman-derived equality joins execute natively", {
    left <- tibble::tibble(id = c(1L, 2L), value = c("left-1", "left-2"))
    right <- tibble::tibble(id = c(2L, 3L), value = c("right-2", "right-3"), score = c(10L, 20L))

    # Source: inst/tinytest/test_joins_filter.R @ c9eb1f1 — inner equality join.
    expected_inner <- dplyr::inner_join(left, right, by = "id", na_matches = "never")
    actual_inner <- dplyr::inner_join(
        ibex_tbl(left, fallback = "error"), right, by = "id", na_matches = "never"
    )
    expect_s3_class(actual_inner, "ibex_tbl")
    expect_equal(dplyr::collect(actual_inner), expected_inner)

    # Source: inst/tinytest/test_joins_filter.R @ c9eb1f1 — left equality join.
    expected_left <- dplyr::left_join(left, right, by = "id", na_matches = "never")
    actual_left <- dplyr::left_join(
        ibex_tbl(left, fallback = "error"), right, by = "id", na_matches = "never"
    )
    expect_s3_class(actual_left, "ibex_tbl")
    expect_equal(dplyr::collect(actual_left), expected_left)
})

test_that("poorman cases outside native ribex support are classified separately", {
    input <- tibble::tibble(g = c("a", "a", "b"), x = 1:3, y = 4:6)

    # Source: inst/tinytest/test_group_by.R @ c9eb1f1 — computed grouping columns.
    expect_error(
        dplyr::group_by(ibex_tbl(input, fallback = "error"), bucket = x * 2L),
        class = "ribex_translation_error"
    )

    # Source: inst/tinytest/test_distinct.R @ c9eb1f1 — .keep_all on a subset.
    expect_error(
        dplyr::distinct(ibex_tbl(input, fallback = "error"), g, .keep_all = TRUE),
        class = "ribex_translation_error"
    )

    # Source: inst/tinytest/test_slice.R @ c9eb1f1 — tail slicing.
    expect_error(
        dplyr::slice_tail(ibex_tbl(input, fallback = "error"), n = 1L),
        class = "ribex_translation_error"
    )

    # Source: inst/tinytest/test_joins_filter.R @ c9eb1f1 — default NA matching.
    expect_error(
        dplyr::inner_join(ibex_tbl(input, fallback = "error"), input, by = "g"),
        class = "ribex_translation_error"
    )
})
