arrow_buffer_addresses <- function(array) {
    .Call(ribex:::ribex_c_arrow_buffer_addresses, array)
}

supported_array <- function() {
    schema <- nanoarrow::na_struct(list(
        value = nanoarrow::na_double(),
        flag = nanoarrow::na_bool(),
        label = nanoarrow::na_string(),
        symbol = nanoarrow::na_dictionary(
            nanoarrow::na_string(),
            nanoarrow::na_int32()
        )
    ))
    nanoarrow::as_nanoarrow_array(
        data.frame(
            value = c(1.5, NA, 3.5, 4.5),
            flag = c(TRUE, NA, FALSE, TRUE),
            label = c("zero", NA, "two", "three"),
            symbol = factor(c("A", NA, "B", "A"))
        ),
        schema = schema
    )
}

slice_array <- function(array, offset, length) {
    nanoarrow::nanoarrow_array_modify(
        array,
        list(offset = offset, length = length)
    )
}

test_that("nanoarrow bindings retain every supported buffer without copying", {
    input <- supported_array()
    before <- arrow_buffer_addresses(input)

    output <- eval_ibex("input;", tables = list(input = input), format = "nanoarrow")

    expect_identical(arrow_buffer_addresses(output), before)
    expect_true(nanoarrow::nanoarrow_pointer_is_valid(input))
    expect_equal(as.data.frame(output), as.data.frame(input))
})

test_that("sliced nullable arrays retain offsets and buffers", {
    input <- slice_array(supported_array(), 1, 2)
    before <- arrow_buffer_addresses(input)

    output <- eval_ibex("input;", tables = list(input = input), format = "nanoarrow")

    expect_identical(arrow_buffer_addresses(output), before)
    result <- as.data.frame(output)
    expect_equal(result$value, c(NA_real_, 3.5))
    expect_equal(result$flag, c(NA, FALSE))
    expect_equal(result$label, c(NA_character_, "two"))
    expect_equal(as.character(result$symbol), c(NA_character_, "B"))
})

test_that("session tables and exported results own nanoarrow leases independently", {
    session <- create_session()
    input <- supported_array()
    before <- arrow_buffer_addresses(input)

    expect_invisible(
        session_eval(session, "let retained = input;", tables = list(input = input))
    )
    rm(input)
    invisible(gc())

    output <- session_eval(session, "retained;", format = "nanoarrow")
    expect_identical(arrow_buffer_addresses(output), before)

    reset_session(session)
    rm(session)
    invisible(gc())
    expect_equal(as.data.frame(output)$label, c("zero", NA, "two", "three"))
})

test_that("an exported lease survives host release before execution", {
    input <- supported_array()
    before <- arrow_buffer_addresses(input)
    leased_tables <- ribex:::normalize_table_bindings(list(input = input))

    rm(input)
    invisible(gc())

    payload <- .Call(
        ribex:::ribex_c_eval_ibex,
        "input;",
        character(),
        leased_tables,
        NULL
    )
    nanoarrow::nanoarrow_array_set_schema(payload$array, payload$schema)
    expect_identical(arrow_buffer_addresses(payload$array), before)
    expect_equal(as.data.frame(payload$array)$label, c("zero", NA, "two", "three"))
})

test_that("repeated nanoarrow round trips keep the same buffers", {
    current <- supported_array()
    original <- arrow_buffer_addresses(current)

    for (i in seq_len(4)) {
        current <- eval_ibex("input;", tables = list(input = current), format = "nanoarrow")
        expect_identical(arrow_buffer_addresses(current), original)
    }
})

test_that("mutation detaches only the written external column", {
    input <- supported_array()
    input_addresses <- arrow_buffer_addresses(input)

    output <- eval_ibex(
        "input[update { value = value + 1.0 }];",
        tables = list(input = input),
        format = "nanoarrow"
    )
    output_addresses <- arrow_buffer_addresses(output)

    expect_false(identical(
        output_addresses[["root.child0.buffer1"]],
        input_addresses[["root.child0.buffer1"]]
    ))
    expect_identical(
        output_addresses[["root.child2.buffer2"]],
        input_addresses[["root.child2.buffer2"]]
    )
    expect_identical(
        output_addresses[["root.child3.dictionary.buffer2"]],
        input_addresses[["root.child3.dictionary.buffer2"]]
    )
    expect_equal(as.data.frame(input)$value, c(1.5, NA, 3.5, 4.5))
    expect_equal(as.data.frame(output)$value, c(2.5, NA, 4.5, 5.5))
})

test_that("failed adoption leaves the caller's nanoarrow array valid", {
    # Needs a type Ibex genuinely cannot import. An integer column used to serve
    # here; it is supported now, so this uses uint64, which cannot be widened
    # into Ibex's Int64 without losing values.
    input <- nanoarrow::as_nanoarrow_array(
        data.frame(value = 1:3),
        schema = nanoarrow::na_struct(list(value = nanoarrow::na_uint64()))
    )

    expect_error(
        eval_ibex("input;", tables = list(input = input)),
        "Arrow"
    )
    expect_true(nanoarrow::nanoarrow_pointer_is_valid(input))
    expect_equal(as.data.frame(input)$value, 1:3)
})
