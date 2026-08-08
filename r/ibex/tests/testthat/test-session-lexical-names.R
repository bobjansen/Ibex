# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# A session must carry its in-scope names into the lowerer. The static
# column-ref check treats a bare name in a filter or update expression as a
# column unless it is a known lexical binding, so a bound scalar used to be
# rejected as a missing column the moment the input schema became statically
# known -- which is exactly what happens when clauses are chained instead of
# split across intermediate `let`s. The equivalent query has always worked in
# the REPL, which populates those names.

sample_table <- function() {
    data.frame(
        k = c("x", "x", "y"),
        v = c(1L, 2L, 3L)
    )
}

test_that("a bound scalar resolves in a filter over a statically known schema", {
    sess <- create_session()
    out <- session_eval(
        sess,
        "src[select { c = count() }, by k][filter c >= threshold, order k];",
        tables = list(src = sample_table()),
        scalars = list(threshold = 2L)
    )

    expect_equal(out$k, "x")
    expect_equal(as.integer(out$c), 2L)
})

test_that("a bound scalar resolves in an update over a statically known schema", {
    sess <- create_session()
    out <- session_eval(
        sess,
        "src[filter v > 0][update { bumped = v + offset }, order v];",
        tables = list(src = sample_table()),
        scalars = list(offset = 10L)
    )

    expect_equal(as.integer(out$bumped), c(11L, 12L, 13L))
})

test_that("a let-bound table resolves as a lexical name in a later statement", {
    sess <- create_session()
    session_eval(sess, "let base = src;", tables = list(src = sample_table()))

    out <- session_eval(sess, "base[filter v >= 2, order v];")
    expect_equal(as.integer(out$v), c(2L, 3L))
})

test_that("a genuinely missing column is still rejected", {
    sess <- create_session()
    expect_error(
        session_eval(
            sess,
            "src[select { c = count() }, by k][filter c >= nope];",
            tables = list(src = sample_table())
        ),
        "nope"
    )
})
