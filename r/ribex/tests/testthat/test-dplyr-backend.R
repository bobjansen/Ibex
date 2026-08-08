# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

test_that("ibex_tbl identity is lazy, repeatable, and schema-aware", {
    input <- tibble::tibble(
        symbol = c("A", "B", "A"),
        price = c(11, 5, 20),
        size = c(2L, 3L, 4L)
    )
    query <- ibex_tbl(input, fallback = "error")

    expect_s3_class(query, "ibex_tbl")
    expect_identical(names(query), names(input))
    expect_equal(dim(query), c(3L, 3L))
    expect_equal(dplyr::collect(query), input)
    expect_equal(dplyr::collect(query), input)

    schema <- session_table_schema(query$session, query$source)
    expect_identical(schema$name, names(input))
    expect_identical(schema$type, c("String", "Float64", "Int64"))
    expect_false(any(schema$nullable))
    expect_identical(attr(schema, "rows"), 3)
    expect_named(attr(schema, "ordering"), c("name", "descending"))
    expect_length(attr(schema, "time_index"), 0L)
    expect_identical(attr(schema, "generation"), query$generation)
    expect_length(attr(schema, "grouped_by"), 0L)
})

test_that("R factors bind as categorical columns", {
    input <- tibble::tibble(
        symbol = factor(c("A", NA, "B", "A"), levels = c("B", "A")),
        value = 1:4
    )
    query <- ibex_tbl(input, fallback = "error")
    schema <- session_table_schema(query$session, query$source)

    expect_identical(schema$type, c("Categorical", "Int64"))
    expect_identical(schema$categorical, c(TRUE, FALSE))
    expect_identical(schema$nullable, c(TRUE, FALSE))
    expect_equal(as.character(dplyr::collect(query)$symbol), as.character(input$symbol))
})

test_that("core analytical pipeline stays lazy and matches dplyr", {
    input <- tibble::tibble(
        symbol = c("A", "B", "A", "B"),
        price = c(11, 5, 20, 12),
        size = c(2L, 3L, 4L, 2L)
    )
    cutoff <- 10

    actual <- ibex_tbl(input, fallback = "error") |>
        dplyr::filter(price > cutoff) |>
        dplyr::mutate(notional = price * size) |>
        dplyr::group_by(symbol) |>
        dplyr::summarise(total = sum(notional), .groups = "drop") |>
        dplyr::arrange(dplyr::desc(total))

    expected <- input |>
        dplyr::filter(price > cutoff) |>
        dplyr::mutate(notional = price * size) |>
        dplyr::group_by(symbol) |>
        dplyr::summarise(total = sum(notional), .groups = "drop") |>
        dplyr::arrange(dplyr::desc(total))

    expect_s3_class(actual, "ibex_tbl")
    expect_equal(dplyr::collect(actual), expected)
    rendered <- paste(capture.output(dplyr::show_query(actual)), collapse = "\n")
    expect_match(rendered, "\\^__ribex_dplyr_scalar_0001<numeric>")
    expect_false(any(grepl("10", rendered, fixed = TRUE)))
})

test_that("select rename relocate transmute and distinct preserve names", {
    input <- tibble::tibble(a = c(1L, 1L, 2L), `odd name` = c(2, 2, 3), z = 4:6)
    source <- ibex_tbl(input, fallback = "error")

    projected <- source |>
        dplyr::select(z, a, `odd name`) |>
        dplyr::rename(id = a) |>
        dplyr::relocate(`odd name`, .after = id)
    expect_equal(dplyr::collect(projected), input |> dplyr::select(z, a, `odd name`) |> dplyr::rename(id = a) |> dplyr::relocate(`odd name`, .after = id))

    expect_equal(
        dplyr::collect(dplyr::transmute(source, doubled = a * 2L)),
        dplyr::transmute(input, doubled = a * 2L)
    )
    expect_equal(
        dplyr::collect(dplyr::select(source, tidyselect::where(is.numeric))),
        dplyr::select(input, tidyselect::where(is.numeric))
    )
    expect_equal(dplyr::collect(dplyr::distinct(source, a)), dplyr::distinct(input, a))

    grouped <- source |>
        dplyr::group_by(a) |>
        dplyr::select(id = a, z)
    expect_identical(dplyr::group_vars(grouped), "id")
    expect_equal(
        dplyr::collect(grouped),
        input |> dplyr::group_by(a) |> dplyr::select(id = a, z)
    )
})

test_that("identifier quoting and translation identity are conservative", {
    quote_identifier <- getFromNamespace("ibex_quote_identifier", "ribex")
    expect_identical(quote_identifier("a`b\\c"), "`a\\`b\\\\c`")
    expect_error(quote_identifier("a${b}"), class = "ribex_unsupported")

    input <- tibble::tibble(x = 1:3)
    source <- ibex_tbl(input, fallback = "error")
    expect_s3_class(dplyr::summarise(source, avg = base::mean(x)), "ibex_tbl")

    mean <- function(...) 123
    expect_error(
        dplyr::summarise(source, avg = mean(x)),
        class = "ribex_translation_error"
    )
})

test_that("group metadata, grouped head, and compute are retained", {
    input <- tibble::tibble(g = c("a", "a", "b"), x = c(3L, 1L, 2L))
    query <- ibex_tbl(input, fallback = "error") |>
        dplyr::group_by(g) |>
        dplyr::arrange(x, .by_group = TRUE) |>
        dplyr::slice_head(n = 1L)

    expect_identical(dplyr::group_vars(query), "g")
    expect_equal(dplyr::collect(query), input |> dplyr::group_by(g) |> dplyr::arrange(x, .by_group = TRUE) |> dplyr::slice_head(n = 1L))

    materialized <- dplyr::compute(query)
    expect_s3_class(materialized, "ibex_tbl")
    expect_length(materialized$steps, 0L)
    expect_equal(dplyr::collect(materialized), dplyr::collect(query))
})

test_that("fallback is visible, one-way, and policy controlled", {
    input <- tibble::tibble(id = 1:3, x = c("a", "b", "c"))
    cutoff <- 1L
    warn_source <- ibex_tbl(input, fallback = "warn") |>
        dplyr::filter(id > .env$cutoff)
    expect_warning(
        local <- dplyr::mutate(warn_source, upper = toupper(x)),
        class = "ribex_fallback_warning"
    )
    expect_s3_class(local, "tbl_df")
    expect_false(inherits(local, "ibex_tbl"))
    expect_equal(local$upper, c("B", "C"))

    error_source <- ibex_tbl(input, fallback = "error")
    expect_error(
        dplyr::mutate(error_source, upper = toupper(x)),
        class = "ribex_translation_error"
    )
})

test_that("nullable aggregate semantics require explicit na.rm", {
    input <- tibble::tibble(g = c("a", "a"), x = c(1, NA_real_))

    native <- ibex_tbl(input, fallback = "error") |>
        dplyr::group_by(g) |>
        dplyr::summarise(total = sum(x, na.rm = TRUE), .groups = "drop")
    expect_equal(dplyr::collect(native), input |> dplyr::group_by(g) |> dplyr::summarise(total = sum(x, na.rm = TRUE), .groups = "drop"))

    expect_error(
        ibex_tbl(input, fallback = "error") |>
            dplyr::summarise(total = sum(x)),
        class = "ribex_translation_error"
    )
})

test_that("reset_session invalidates dependent lazy tables", {
    query <- ibex_tbl(tibble::tibble(x = 1:3))
    reset_session(query$session)
    expect_error(dplyr::collect(query), class = "ribex_invalid_session")
})

test_that("branches do not mutate one another", {
    source <- ibex_tbl(tibble::tibble(x = 1:3), fallback = "error")
    plus_one <- dplyr::mutate(source, y = x + 1L)
    plus_two <- dplyr::mutate(source, y = x + 2L)

    expect_identical(names(source), "x")
    expect_equal(dplyr::collect(plus_one)$y, 2:4)
    expect_equal(dplyr::collect(plus_two)$y, 3:5)
})

test_that("count and weighted count lower through native primitives", {
    input <- tibble::tibble(g = c("a", "a", "b"), w = c(1, 2, 3))
    source <- ibex_tbl(input, fallback = "error")

    expect_equal(dplyr::collect(dplyr::count(source, g)), dplyr::count(input, g))
    expect_equal(
        dplyr::collect(dplyr::count(source, g, wt = w, sort = TRUE)),
        dplyr::count(input, g, wt = w, sort = TRUE)
    )
    expect_equal(dplyr::pull(source, w), input$w)
})

test_that("join fallback applies the current verb once and stays local", {
    left <- tibble::tibble(id = c(1L, 2L), x = c("a", "b"))
    right <- tibble::tibble(id = 2L, y = "hit")
    source <- ibex_tbl(left, fallback = "collect")

    result <- dplyr::left_join(source, right, by = "id")
    expect_false(inherits(result, "ibex_tbl"))
    expect_equal(result, dplyr::left_join(left, right, by = "id"))
})

# Nullability comes from Ibex's own schema inference rather than a rule written
# out again per verb. These tests assert the proofs the adapter could not make
# for itself, and one that it must still not make.
test_that("column nullability is inferred by Ibex for the whole plan", {
    input <- tibble::tibble(g = c("a", "a", "b"), x = c(1, NA_real_, 3))
    query <- ibex_tbl(input, fallback = "error")

    # The base table is ground truth: a materialized column with no validity
    # bitmap holds no nulls.
    expect_identical(query$schema$nullable, c(FALSE, TRUE))

    # A filter proves the columns its predicate had to read. Both spellings
    # reach it -- `!is.na(x)` lowers to `!is_null(x)`, not to `is_not_null(x)`.
    expect_identical(dplyr::filter(query, !is.na(x))$schema$nullable, c(FALSE, FALSE))
    expect_identical(dplyr::filter(query, x > 0)$schema$nullable, c(FALSE, FALSE))

    # A disjunction proves neither branch, so the proof must not appear.
    expect_identical(dplyr::filter(query, x > 0 | g == "a")$schema$nullable, c(FALSE, TRUE))
})

test_that("a proved column lifts the na.rm requirement on aggregates", {
    # The payoff, and the reason the accuracy matters rather than only the
    # bookkeeping: the aggregate gate reads nullability, so a better proof is
    # the difference between running natively and falling back to dplyr.
    input <- tibble::tibble(g = c("a", "a", "b"), x = c(1, NA_real_, 3))

    expect_error(
        ibex_tbl(input, fallback = "error") |>
            dplyr::group_by(g) |>
            dplyr::summarise(m = mean(x)),
        class = "ribex_translation_error"
    )

    native <- ibex_tbl(input, fallback = "error") |>
        dplyr::filter(!is.na(x)) |>
        dplyr::group_by(g) |>
        dplyr::summarise(m = mean(x), .groups = "drop")
    expect_s3_class(native, "ibex_tbl")
    expect_equal(
        dplyr::collect(native),
        input |> dplyr::filter(!is.na(x)) |> dplyr::group_by(g) |>
            dplyr::summarise(m = mean(x), .groups = "drop")
    )
})

test_that("join nullability follows Ibex's rules, not the adapter's", {
    session <- create_session()
    left <- tibble::tibble(id = c(1L, NA_integer_, 2L), lv = c(1, 2, 3))
    right <- tibble::tibble(id = c(2L, 3L), rv = c(10, 20))
    lt <- ibex_tbl(left, session = session, fallback = "error")
    rt <- ibex_tbl(right, session = session, fallback = "error")
    expect_true(lt$schema$nullable[[match("id", lt$schema$names)]])

    # An inner join proves its key null-free even though neither input did: a
    # null key matches nothing, so no row carrying one survives. The adapter
    # had no way to say this.
    inner <- dplyr::inner_join(lt, rt, by = "id", na_matches = "never")
    expect_identical(inner$schema$names, c("id", "lv", "rv"))
    expect_identical(inner$schema$nullable, c(FALSE, FALSE, FALSE))

    # A left join nulls the right side for unmatched rows, and leaves the key
    # alone -- the folded key column takes the left value, which was nullable.
    outer <- dplyr::left_join(lt, rt, by = "id", na_matches = "never")
    expect_identical(outer$schema$names, c("id", "lv", "rv"))
    expect_identical(outer$schema$nullable, c(TRUE, FALSE, TRUE))
})

test_that("right and full joins run natively and agree with dplyr", {
    session <- create_session()
    # A duplicate left key, a left-only row and a right-only row, so each join
    # differs from the others in what it keeps.
    left <- tibble::tibble(id = c(1L, 2L, 2L, 4L), lv = c(10L, 20L, 21L, 40L))
    right <- tibble::tibble(id = c(2L, 3L), rv = c(200L, 300L))
    lt <- ibex_tbl(left, session = session, fallback = "error")

    # A join promises no row order, so compare as sets. dplyr's own order is
    # not the contract being tested here.
    sorted <- function(data) {
        data <- as.data.frame(dplyr::collect(data))
        data[do.call(order, c(unname(as.list(data)), list(na.last = TRUE))), , drop = FALSE]
    }

    for (verb in c("right_join", "full_join")) {
        join <- getExportedValue("dplyr", verb)
        actual <- join(lt, right, by = "id", na_matches = "never")
        expect_s3_class(actual, "ibex_tbl")
        expect_identical(actual$schema$names, c("id", "lv", "rv"))
        expect_equal(
            sorted(actual),
            sorted(join(left, right, by = "id", na_matches = "never")),
            ignore_attr = TRUE
        )
    }

    # The key stays proved through both, which is what unblocked these verbs:
    # an unmatched row's folded key comes from whichever side turned up, so it
    # needs a proof from each -- and both inputs have one.
    expect_identical(
        dplyr::right_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(FALSE, TRUE, FALSE)
    )
    expect_identical(
        dplyr::full_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(FALSE, TRUE, TRUE)
    )
})

test_that("a full join's key is only as proved as the two sides together", {
    # The mirror of the case above: one nullable input is enough to withdraw
    # the folded key's proof, because that side's rows reach the output.
    session <- create_session()
    left <- tibble::tibble(id = c(1L, NA_integer_), lv = c(1, 2))
    right <- tibble::tibble(id = 1L, rv = 10)
    lt <- ibex_tbl(left, session = session, fallback = "error")

    expect_identical(
        dplyr::full_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(TRUE, TRUE, TRUE)
    )
    # A right join drops the left-only rows, so the left's missing proof
    # cannot reach the key -- the right's proof carries every row.
    expect_identical(
        dplyr::right_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(FALSE, TRUE, FALSE)
    )
})

test_that("a captured scalar does not cost the plan its nullability proofs", {
    # The plan carries `^name` for a scalar bound at eval time rather than in
    # the session, so inference has to be told the name exists. Without that
    # the plan fails to lower and every column falls back to nullable.
    input <- tibble::tibble(x = c(1, NA_real_, 3), y = c(5, 6, 7))
    cutoff <- 0
    query <- ibex_tbl(input, fallback = "error") |>
        dplyr::filter(y > .env$cutoff & !is.na(x))
    expect_identical(query$schema$nullable, c(FALSE, FALSE))
})
