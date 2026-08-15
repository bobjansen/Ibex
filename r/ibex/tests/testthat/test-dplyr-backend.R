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

test_that("TimeFrame windows render lazily and execute rolling expressions", {
    ticks <- tibble::tibble(
        ts = as.POSIXct(c(0, 60, 120), origin = "1970-01-01", tz = "UTC"),
        price = c(10, 20, 30)
    )
    query <- ibex_tbl(ticks, fallback = "error") |>
        as_timeframe(ts) |>
        window("2m") |>
        dplyr::mutate(avg = rolling_mean(price), n = rolling_count())

    expect_s3_class(query, "ibex_tbl")
    rendered <- ibex_render_plan(query)
    expect_match(rendered, "as_timeframe\\(")
    expect_match(rendered, "window 2m, update")
    result <- dplyr::collect(query)
    expect_equal(result$avg, c(10, 15, 25))
    expect_equal(result$n, c(1, 2, 2))
})

test_that("resample creates lazy time bars, optionally partitioned", {
    ticks <- tibble::tibble(
        ts = as.POSIXct(c(0, 30, 60, 90), origin = "1970-01-01", tz = "UTC"),
        symbol = c("A", "A", "B", "B"),
        price = c(10, 20, 30, 40)
    )
    query <- ibex_tbl(ticks, fallback = "error") |>
        as_timeframe(ts) |>
        resample("1m", close = dplyr::last(price), n = dplyr::n(), .by = symbol)

    expect_s3_class(query, "ibex_tbl")
    expect_match(ibex_render_plan(query), "resample 1m, select")
    result <- dplyr::collect(query)
    expect_equal(result$symbol, c("A", "B"))
    expect_equal(result$close, c(20, 40))
    expect_equal(result$n, c(2, 2))
    expect_equal(as.numeric(result$ts), c(0, 60))
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

test_that("a categorical column collects back as a factor, levels intact", {
    input <- tibble::tibble(
        symbol = factor(c("A", NA, "B", "A"), levels = c("B", "A")),
        value = 1:4
    )
    collected <- dplyr::collect(ibex_tbl(input, fallback = "error"))

    # Decoding the dictionary to one R string per row loses the encoding and
    # costs a per-row materialization; a Categorical is an R factor already.
    expect_s3_class(collected$symbol, "factor")
    expect_identical(levels(collected$symbol), c("B", "A"))
    expect_identical(collected$symbol, input$symbol)
})

test_that("character columns stay String, and stay character on the way back", {
    input <- tibble::tibble(g = c("a", "b", "a"), v = 1:3)
    query <- ibex_tbl(input, fallback = "error")

    expect_identical(session_table_schema(query$session, query$source)$type[[1]], "String")
    expect_identical(dplyr::collect(query)$g, c("a", "b", "a"))
})

test_that("categorical_strings binds character columns as Categorical", {
    input <- tibble::tibble(g = c("a", NA, "b", "a"), v = 1:4)
    query <- ibex_tbl(input, fallback = "error", categorical_strings = TRUE)
    schema <- session_table_schema(query$session, query$source)

    expect_identical(schema$type, c("Categorical", "Int64"))
    expect_identical(schema$nullable, c(TRUE, FALSE))

    collected <- dplyr::collect(query)
    expect_s3_class(collected$g, "factor")
    expect_identical(as.character(collected$g), input$g)

    # The point of the encoding is that grouping sees the codes; the answer
    # must not change because of it.
    expect_equal(
        dplyr::collect(dplyr::summarise(dplyr::group_by(query, g), n = dplyr::n(),
                                        .groups = "drop")) |>
            dplyr::mutate(g = as.character(g)) |>
            dplyr::arrange(g),
        dplyr::arrange(dplyr::count(input, g, name = "n"), g),
        ignore_attr = TRUE
    )
})

test_that("the display name is the caller's expression, not the data", {
    # `substitute(x)` reports the promise's expression only until something
    # rebinds `x`. Both the nanoarrow conversion and `categorical_strings`
    # rebind it, and deparsing the *value* names the table after its own
    # contents -- and costs seconds on a large one.
    prices <- tibble::tibble(g = c("a", "b"), v = 1:2)

    expect_identical(ibex_tbl(prices, fallback = "error")$display_name, "prices")
    expect_identical(
        ibex_tbl(prices, fallback = "error", categorical_strings = TRUE)$display_name,
        "prices"
    )
    expect_identical(ibex_tbl(prices, fallback = "error", name = "custom")$display_name, "custom")
})

test_that("string encoding survives duplicate text arriving as distinct CHARSXPs", {
    # Two equal strings can reach C++ as different CHARSXPs when their
    # encodings differ, and two dictionary entries with equal text would split
    # one group in two.
    native <- "café"
    utf8 <- enc2utf8(native)
    input <- tibble::tibble(g = c(native, utf8, "x"), v = c(1L, 1L, 1L))
    query <- ibex_tbl(input, fallback = "error", categorical_strings = TRUE)

    grouped <- dplyr::collect(
        dplyr::summarise(dplyr::group_by(query, g), n = dplyr::n(), .groups = "drop")
    )
    expect_identical(nrow(grouped), 2L)
    expect_setequal(as.character(grouped$g), c(native, "x"))
    expect_identical(sort(grouped$n), c(1, 2))
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
    expect_match(rendered, "\\^__ibex_dplyr_scalar_0001<numeric>")
    # The captured value must not survive into the rendered plan. Anchor on the
    # comparison rather than searching for "10" anywhere: generated binding
    # names are zero-padded counters, so a bare substring search starts failing
    # the moment the tenth binding of the session is rendered.
    expect_false(any(grepl("> 10", rendered, fixed = TRUE)))
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
    quote_identifier <- getFromNamespace("ibex_quote_identifier", "ibex")
    expect_identical(quote_identifier("a`b\\c"), "`a\\`b\\\\c`")
    expect_error(quote_identifier("a${b}"), class = "ibex_unsupported")

    input <- tibble::tibble(x = 1:3)
    source <- ibex_tbl(input, fallback = "error")
    expect_s3_class(dplyr::summarise(source, avg = base::mean(x)), "ibex_tbl")

    mean <- function(...) 123
    expect_error(
        dplyr::summarise(source, avg = mean(x)),
        class = "ibex_translation_error"
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
        class = "ibex_fallback_warning"
    )
    expect_s3_class(local, "tbl_df")
    expect_false(inherits(local, "ibex_tbl"))
    expect_equal(local$upper, c("B", "C"))

    error_source <- ibex_tbl(input, fallback = "error")
    expect_error(
        dplyr::mutate(error_source, upper = toupper(x)),
        class = "ibex_translation_error"
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
        class = "ibex_translation_error"
    )
})

test_that("as.Date translates to the native Date cast and buckets on UTC", {
    # `as.Date.POSIXct` defaults to `tz = "UTC"`, and Ibex's `Date()` cast cuts
    # on UTC too, so the two agree without either being told about a zone.
    input <- tibble::tibble(
        ts = as.POSIXct(
            c("2024-01-15 00:00:00", "2024-01-15 23:59:59", "2024-01-16 00:00:00"),
            tz = "UTC"
        ),
        v = c(1, 2, 4)
    )
    source <- ibex_tbl(input, fallback = "error")

    day <- dplyr::mutate(source, day = as.Date(ts))
    expect_s3_class(day, "ibex_tbl")
    expect_match(
        paste(capture.output(dplyr::show_query(day)), collapse = "\n"),
        "Date(`ts`)",
        fixed = TRUE
    )
    collected <- dplyr::collect(day)
    expect_s3_class(collected$day, "Date")
    expect_equal(collected$day, as.Date(input$ts))

    # And it groups: the first two rows share a day, the third does not.
    grouped <- dplyr::summarise(day, n = dplyr::n(), total = sum(v, na.rm = TRUE), .by = day)
    expect_equal(
        as.data.frame(dplyr::arrange(dplyr::collect(grouped), day)),
        as.data.frame(
            input |>
                dplyr::mutate(day = as.Date(ts)) |>
                dplyr::summarise(n = dplyr::n(), total = sum(v), .by = day) |>
                dplyr::arrange(day)
        )
    )
})

test_that("as.Date declines the forms whose day boundaries would differ", {
    input <- tibble::tibble(
        ts = as.POSIXct("2024-01-15 23:00:00", tz = "UTC"),
        text = "2024-01-15"
    )
    source <- ibex_tbl(input, fallback = "error")

    # A non-UTC `tz` cuts on different boundaries than Ibex's cast.
    expect_error(
        dplyr::mutate(source, day = as.Date(ts, tz = "America/New_York")),
        class = "ibex_translation_error"
    )
    # Ibex has no string parse for Date().
    expect_error(
        dplyr::mutate(source, day = as.Date(text)),
        class = "ibex_translation_error"
    )
})

test_that("reset_session invalidates dependent lazy tables", {
    # An explicit session, so resetting it cannot disturb the shared default.
    query <- ibex_tbl(tibble::tibble(x = 1:3), session = create_session())
    reset_session(query$session)
    expect_error(dplyr::collect(query), class = "ibex_invalid_session")
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

    # `keep = TRUE` asks for both key columns, which the native join has no
    # spelling for. Any unsupported option would do -- what is under test is
    # the fallback, not the gate.
    result <- dplyr::left_join(source, right, by = "id", keep = TRUE)
    expect_false(inherits(result, "ibex_tbl"))
    expect_equal(result, dplyr::left_join(left, right, by = "id", keep = TRUE))
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
        class = "ibex_translation_error"
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

test_that("separately wrapped tables share a session and join natively", {
    # Every other join test threads one explicit session through both sides,
    # which is not how a user writes it. Two bare `ibex_tbl()` calls used to
    # land in two sessions and silently collect into local dplyr.
    left <- tibble::tibble(id = c(1L, 2L, 3L), lv = c(1, 2, 3))
    right <- tibble::tibble(id = c(2L, 3L), rv = c(10, 20))
    lt <- ibex_tbl(left, fallback = "error")
    rt <- ibex_tbl(right, fallback = "error")

    expect_identical(lt$session, rt$session)
    joined <- dplyr::inner_join(lt, rt, by = "id")
    expect_s3_class(joined, "ibex_tbl")
    expect_equal(
        as.data.frame(dplyr::collect(joined)),
        as.data.frame(dplyr::inner_join(left, right, by = "id"))
    )
})

test_that("an explicit session still overrides the shared default", {
    session <- create_session()
    isolated <- ibex_tbl(tibble::tibble(x = 1:3), session = session)
    expect_identical(isolated$session, session)
    expect_false(identical(isolated$session, ibex_default_session()))
})

test_that("dplyr's default NA matching runs natively across every kind", {
    # `na_matches = "na"` is dplyr's default, so this is the form nearly every
    # join is written in. It maps to Ibex's `nulls equal`.
    session <- create_session()
    left <- tibble::tibble(id = c("a", NA, "b"), lv = 1:3)
    right <- tibble::tibble(id = c("b", NA, "c"), rv = 4:6)
    lt <- ibex_tbl(left, session = session, fallback = "error")

    sorted <- function(data) {
        data <- as.data.frame(dplyr::collect(data))
        data[do.call(order, c(unname(as.list(data)), list(na.last = TRUE))), , drop = FALSE]
    }

    for (verb in c("inner_join", "left_join", "right_join", "full_join")) {
        join <- getExportedValue("dplyr", verb)
        for (na_matches in c("na", "never")) {
            actual <- join(lt, right, by = "id", na_matches = na_matches)
            expect_s3_class(actual, "ibex_tbl")
            expect_equal(
                sorted(actual),
                sorted(join(left, right, by = "id", na_matches = na_matches)),
                ignore_attr = TRUE,
                info = paste(verb, na_matches)
            )
        }
    }

    # The clause is only emitted when it says something: `never` is Ibex's
    # default, so it would be noise in the plan.
    rendered <- function(query) {
        paste(capture.output(dplyr::show_query(query)), collapse = "\n")
    }
    expect_match(rendered(dplyr::inner_join(lt, right, by = "id")), "nulls equal", fixed = TRUE)
    expect_false(grepl(
        "nulls", rendered(dplyr::inner_join(lt, right, by = "id", na_matches = "never")),
        fixed = TRUE
    ))

    # And a matching null key is a key that can be null, so the proof an inner
    # join otherwise carries is withdrawn -- by the core, on reading the clause.
    expect_identical(
        dplyr::inner_join(lt, right, by = "id")$schema$nullable[[1]], TRUE
    )
    expect_identical(
        dplyr::inner_join(lt, right, by = "id", na_matches = "never")$schema$nullable[[1]], FALSE
    )
})

test_that("a floating-point key falls back rather than mismatching NaN", {
    # `na_matches = "na"` matches NaN to NaN as well as NA to NA. Ibex's
    # `nulls equal` is about the validity bitmap, and a NaN is a present
    # value, so the two disagree on a float key -- and R keeps NaN and NA
    # apart, so no rewrite recovers dplyr's answer.
    input <- tibble::tibble(id = c(1, NA, 2), lv = 1:3)
    right <- tibble::tibble(id = c(2, NA, 3), rv = 4:6)

    expect_error(
        dplyr::inner_join(ibex_tbl(input, fallback = "error"), right, by = "id"),
        class = "ibex_translation_error"
    )
    # `never` matches no NaN on either side, so a float key is fine there.
    expect_s3_class(
        dplyr::inner_join(ibex_tbl(input, fallback = "error"), right,
                          by = "id", na_matches = "never"),
        "ibex_tbl"
    )
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

test_that("a mapped key joins columns whose names differ", {
    session <- create_session()
    left <- tibble::tibble(id = 1:3, v = c("a", "b", "c"))
    right <- tibble::tibble(rid = c(2L, 3L, 4L), w = c(20, 30, 40))
    lt <- ibex_tbl(left, session = session, fallback = "error")

    sorted <- function(data) {
        data <- as.data.frame(dplyr::collect(data))
        data[do.call(order, c(unname(as.list(data)), list(na.last = TRUE))), , drop = FALSE]
    }

    for (verb in c("inner_join", "left_join", "right_join", "full_join",
                   "semi_join", "anti_join")) {
        join <- getExportedValue("dplyr", verb)
        actual <- join(lt, right, by = c(id = "rid"))
        expect_s3_class(actual, "ibex_tbl")
        expect_equal(
            sorted(actual),
            sorted(join(left, right, by = c(id = "rid"))),
            ignore_attr = TRUE
        )
    }

    # One key column out, under the left name: Ibex keeps both halves of a
    # mapped pair, and dplyr keeps one.
    expect_identical(
        dplyr::inner_join(lt, right, by = c(id = "rid"))$schema$names,
        c("id", "v", "w")
    )
    query <- paste(
        capture.output(dplyr::show_query(dplyr::inner_join(lt, right, by = c(id = "rid")))),
        collapse = "\n"
    )
    expect_true(grepl("on { `id` = `rid` }", query, fixed = TRUE))

    # A bare name is Ibex's shorthand for a same-name pair, so a `by` mixing
    # both forms renders as a mix of both spellings.
    mixed_left <- tibble::tibble(g = c("p", "q"), id = 1:2, v = c(1, 2))
    mixed_right <- tibble::tibble(g = c("p", "q"), rid = 1:2, w = c(5, 6))
    mixed <- dplyr::inner_join(
        ibex_tbl(mixed_left, session = session, fallback = "error"),
        mixed_right, by = c("g", id = "rid")
    )
    expect_identical(mixed$schema$names, c("g", "id", "v", "w"))
    expect_true(grepl(
        "on { `g`, `id` = `rid` }",
        paste(capture.output(dplyr::show_query(mixed)), collapse = "\n"),
        fixed = TRUE
    ))
    expect_equal(
        sorted(mixed),
        sorted(dplyr::inner_join(mixed_left, mixed_right, by = c("g", id = "rid"))),
        ignore_attr = TRUE
    )
})

test_that("a mapped key merges to the right value where a row has no left side", {
    session <- create_session()
    left <- tibble::tibble(id = 1:3, v = c("a", "b", "c"))
    right <- tibble::tibble(rid = c(2L, 3L, 4L), w = c(20, 30, 40))
    lt <- ibex_tbl(left, session = session, fallback = "error")

    # The row that separates a correct translation from a plausible one. dplyr
    # reports `id = 4` for the right-only row; the left key is null there, so
    # simply dropping the right column would report NA. Ibex does this fold
    # itself for a same-name key and declines to for a mapped pair, which is
    # what leaves the coalesce to the backend.
    right_only <- dplyr::collect(dplyr::right_join(lt, right, by = c(id = "rid")))
    # Compared by value: an Int64 column comes back to R as a double, which is
    # the bridge's business and not this test's.
    expect_equal(right_only$id[is.na(right_only$v)], 4)
    expect_equal(
        right_only,
        dplyr::right_join(left, right, by = c(id = "rid")),
        ignore_attr = TRUE
    )

    plan_of <- function(query) {
        paste(capture.output(dplyr::show_query(query)), collapse = "\n")
    }
    expect_true(grepl(
        "`id` = coalesce(`id`, `rid`)",
        plan_of(dplyr::right_join(lt, right, by = c(id = "rid"))), fixed = TRUE
    ))
    # An inner or left join emits no row without a left side, so the left key
    # is already the answer and the coalesce would be dead weight.
    expect_false(grepl(
        "coalesce", plan_of(dplyr::left_join(lt, right, by = c(id = "rid"))), fixed = TRUE
    ))

    # The merge does not cost the key its proof: `coalesce` of a null-free
    # right key is null-free, and the core says so.
    expect_identical(
        dplyr::right_join(lt, right, by = c(id = "rid"))$schema$nullable,
        c(FALSE, TRUE, FALSE)
    )
})

test_that("a mapped key that the two would suffix differently falls back", {
    session <- create_session()
    # Ibex sees the right key `b` as an ordinary column colliding with the
    # left's `b`, and suffixes both. dplyr drops the right key outright and so
    # leaves the left `b` alone. Suffixing a column the caller never mentioned
    # is the wrong answer, not a different one.
    left <- tibble::tibble(a = 1:2, b = c("x", "y"))
    right <- tibble::tibble(b = 1:2, w = c(9, 8))
    expect_error(
        dplyr::inner_join(ibex_tbl(left, session = session, fallback = "error"),
                          right, by = c(a = "b")),
        class = "ibex_translation_error"
    )
    # The mirror: the left key's name occurs on the right as a non-key.
    expect_error(
        dplyr::inner_join(
            ibex_tbl(tibble::tibble(id = 1:2, v = c("x", "y")),
                     session = session, fallback = "error"),
            tibble::tibble(rid = 1:2, id = c(7L, 8L)), by = c(id = "rid")
        ),
        class = "ibex_translation_error"
    )
    # A filtering join emits no right column, so neither shape can arise and
    # the same `by` translates natively.
    semi <- dplyr::semi_join(ibex_tbl(left, session = session, fallback = "error"),
                             right, by = c(a = "b"))
    expect_s3_class(semi, "ibex_tbl")
    expect_equal(dplyr::collect(semi), dplyr::semi_join(left, right, by = c(a = "b")))

    # Two left keys naming one right column: dplyr rejects this outright, so
    # falling back is what reproduces its error rather than inventing one.
    expect_error(
        dplyr::inner_join(ibex_tbl(left, session = session, fallback = "error"),
                          right, by = c(a = "b", b = "b")),
        class = "ibex_translation_error"
    )
})

test_that("a cross join pairs every row with every row", {
    session <- create_session()
    left <- tibble::tibble(id = c(1L, 2L), v = c("x", "y"))
    right <- tibble::tibble(k = c("p", "q", "r"), w = c(10, 20, 30))
    lt <- ibex_tbl(left, session = session, fallback = "error")

    sorted <- function(data) {
        data <- as.data.frame(dplyr::collect(data))
        data[do.call(order, c(unname(as.list(data)), list(na.last = TRUE))), , drop = FALSE]
    }

    actual <- dplyr::cross_join(lt, right)
    expect_s3_class(actual, "ibex_tbl")
    expect_identical(nrow(dplyr::collect(actual)), nrow(left) * nrow(right))
    expect_equal(sorted(actual), sorted(dplyr::cross_join(left, right)), ignore_attr = TRUE)

    # Two clauses must be absent, and for opposite reasons: Ibex has no
    # spelling for an empty `on`, and it rejects a `nulls` clause outright on a
    # join with no equality keys rather than accepting an inert one.
    query <- paste(capture.output(dplyr::show_query(actual)), collapse = "\n")
    expect_false(grepl(" on ", query, fixed = TRUE))
    expect_false(grepl("nulls", query, fixed = TRUE))

    # An empty side makes an empty product, not an error.
    expect_identical(nrow(dplyr::collect(dplyr::cross_join(lt, right[0, ]))), 0L)
})

test_that("a cross join suffixes every shared name, having no key to fold", {
    session <- create_session()
    left <- tibble::tibble(id = 1:2, v = c("a", "b"))
    right <- tibble::tibble(id = 3:4, v = c("c", "d"))
    lt <- ibex_tbl(left, session = session, fallback = "error")

    # `id` would be a folded key in any other kind. Here it is an ordinary
    # collision on both counts, so all four columns survive under suffixes.
    expect_identical(
        dplyr::cross_join(lt, right)$schema$names,
        c("id.x", "v.x", "id.y", "v.y")
    )
    expect_identical(
        dplyr::cross_join(lt, right, suffix = c("_L", "_R"))$schema$names,
        c("id_L", "v_L", "id_R", "v_R")
    )
    expect_equal(
        as.data.frame(dplyr::collect(dplyr::cross_join(lt, right))),
        as.data.frame(dplyr::cross_join(left, right)),
        ignore_attr = TRUE
    )
    # Ibex would reject the join it produces, so refuse before submitting.
    expect_error(
        dplyr::cross_join(lt, right, suffix = c("", "")),
        class = "ibex_translation_error"
    )
})

test_that("the filtering joins keep left rows, and keep each of them once", {
    session <- create_session()
    # The right side repeats a key that the left also repeats. A filtering join
    # must not multiply rows the way an inner join on the same data would --
    # that is the whole difference between the two.
    left <- tibble::tibble(id = c("a", "b", NA, "c", "b"), lv = 1:5)
    right <- tibble::tibble(id = c("b", "b", NA, "e"), rv = 1:4)
    lt <- ibex_tbl(left, session = session, fallback = "error")

    sorted <- function(data) {
        data <- as.data.frame(dplyr::collect(data))
        data[do.call(order, c(unname(as.list(data)), list(na.last = TRUE))), , drop = FALSE]
    }

    for (verb in c("semi_join", "anti_join")) {
        join <- getExportedValue("dplyr", verb)
        for (na_matches in c("na", "never")) {
            actual <- join(lt, right, by = "id", na_matches = na_matches)
            expect_s3_class(actual, "ibex_tbl")
            # The left schema, unchanged: no right column, no suffix, no
            # reordering. This is what makes these the simplest kinds.
            expect_identical(actual$schema$names, c("id", "lv"))
            expect_equal(
                sorted(actual),
                sorted(join(left, right, by = "id", na_matches = na_matches)),
                ignore_attr = TRUE
            )
        }
    }

    # Named separately from the comparison above, because a semi join that
    # duplicated `b` would agree with dplyr on every column and disagree only
    # on how many rows carry them.
    expect_identical(nrow(dplyr::collect(dplyr::semi_join(lt, right, by = "id"))), 3L)
})

test_that("a filtering join's key proof follows the rows it keeps", {
    session <- create_session()
    left <- tibble::tibble(id = c("a", NA), lv = 1:2)
    right <- tibble::tibble(id = c("a", NA), rv = 1:2)
    lt <- ibex_tbl(left, session = session, fallback = "error")

    # A semi join keeps the rows that matched, and under `nulls never` a null
    # key matches nothing -- so every surviving key is present.
    expect_identical(
        dplyr::semi_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(FALSE, FALSE)
    )
    # The anti join is the exact mirror and gets the opposite answer from the
    # same rule: the rows it keeps are the ones that did not match, which is
    # precisely where a null key ends up.
    expect_identical(
        dplyr::anti_join(lt, right, by = "id", na_matches = "never")$schema$nullable,
        c(TRUE, FALSE)
    )
    # `nulls equal` lets a null match, so the semi join's proof goes away.
    expect_identical(
        dplyr::semi_join(lt, right, by = "id", na_matches = "na")$schema$nullable,
        c(TRUE, FALSE)
    )
})

test_that("a filtering join keeps the grouping it was handed", {
    session <- create_session()
    left <- tibble::tibble(g = c("a", "a", "b"), x = 1:3)
    right <- tibble::tibble(g = "a")
    grouped <- dplyr::group_by(ibex_tbl(left, session = session, fallback = "error"), g)

    # Unlike the mutating kinds, nothing here can rename a group column or add
    # nulls to it, so the grouping survives -- as it does in dplyr.
    expect_identical(dplyr::group_vars(dplyr::semi_join(grouped, right, by = "g")), "g")
    expect_identical(dplyr::group_vars(dplyr::anti_join(grouped, right, by = "g")), "g")
    expect_identical(
        dplyr::group_vars(dplyr::semi_join(dplyr::group_by(left, g), right, by = "g")),
        "g"
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

test_that("a mutate serves untouched columns from the caller's own vectors", {
    input <- tibble::tibble(g = factor(c("a", "b", "a")), v = c(1.5, 2.5, 3.5))
    query <- ibex_tbl(input, fallback = "error") |> dplyr::mutate(w = v * 2)
    collected <- dplyr::collect(query)

    # Same answer, same column order, same types as a full materialization.
    expect_identical(names(collected), c("g", "v", "w"))
    expect_identical(collected$g, input$g)
    expect_identical(collected$v, input$v)
    expect_identical(collected$w, c(3, 5, 7))
})

test_that("a mutate that overwrites a column takes it from Ibex, not the source", {
    # `v` is derived here, so serving it from the caller's vector would return
    # the input values and silently drop the computation.
    input <- tibble::tibble(g = c("a", "b"), v = c(1.5, 2.5))
    collected <- dplyr::collect(
        ibex_tbl(input, fallback = "error") |> dplyr::mutate(v = v * 10)
    )
    expect_identical(collected$v, c(15, 25))
    expect_identical(collected$g, c("a", "b"))
})

test_that("a column whose R type differs from the collected type is not reused", {
    # An R integer binds as Int64 and collects back as a double. Handing the
    # caller's integer vector back would make the same query return a different
    # type depending on whether the reuse path ran.
    input <- tibble::tibble(i = c(1L, 2L, 3L), v = c(1.5, 2.5, 3.5))
    collected <- dplyr::collect(
        ibex_tbl(input, fallback = "error") |> dplyr::mutate(w = v * 2)
    )
    expect_type(collected$i, "double")
    expect_identical(collected$i, c(1, 2, 3))
})

test_that("a step that breaks the row correspondence releases the caller's table", {
    # The retained reference is strong, so it must not outlive its usefulness:
    # anything that changes which rows come back, or their order, drops it.
    input <- tibble::tibble(g = c("a", "b", "a"), v = c(1.5, 2.5, 3.5))
    source <- ibex_tbl(input, fallback = "error")
    expect_false(is.null(source$source_frame))

    expect_null((source |> dplyr::filter(v > 2))$source_frame)
    expect_null((source |> dplyr::arrange(v))$source_frame)
    expect_null((source |> dplyr::distinct(g))$source_frame)
    expect_null((source |> dplyr::group_by(g) |> dplyr::mutate(w = v * 2))$source_frame)
    # A plain mutate keeps it, which is the whole point.
    expect_false(is.null((source |> dplyr::mutate(w = v * 2))$source_frame))
})

test_that("filtering after a mutate still returns the filtered rows", {
    # The reuse path must not fire once a filter is in the plan: the caller's
    # vectors have every row, the answer does not.
    input <- tibble::tibble(g = c("a", "b", "a"), v = c(1.5, 2.5, 3.5))
    collected <- dplyr::collect(
        ibex_tbl(input, fallback = "error") |>
            dplyr::mutate(w = v * 2) |>
            dplyr::filter(v > 2)
    )
    expect_identical(nrow(collected), 2L)
    expect_identical(collected$v, c(2.5, 3.5))
    expect_identical(collected$w, c(5, 7))
})
