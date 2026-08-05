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
