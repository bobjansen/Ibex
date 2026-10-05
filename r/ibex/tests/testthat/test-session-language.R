# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Sessions run on the same evaluator as the `ibex` REPL, so the whole language
# works from R: functions, scalar bindings, imports, print(). The bindings used
# to have an evaluator of their own that refused all four.

test_that("a session runs functions and scalar bindings", {
    sess <- create_session()
    session_eval(sess, "fn twice(x: Int) -> Int { x * 2; }")
    session_eval(sess, "let n = twice(21);")
    result <- session_eval(sess, "Table { a = [1, 2, 3] }[filter a < n][select { a, b = twice(a) }];")
    expect_equal(result$a, c(1, 2, 3))
    expect_equal(result$b, c(2, 4, 6))
})

test_that("a table function with a scalar binding works nested in another call", {
    sess <- create_session()
    session_eval(sess, "let src = Table { id = [1, 2], v = [10, 20] };")
    session_eval(sess, "
        fn tagged(k: Int) -> DataFrame {
            let m = scalar(src[filter id == k], v);
            Table { a = [0] }[update { a = k, b = m }];
        }
    ")
    result <- session_eval(sess, "rbind(tagged(1), tagged(2));")
    expect_equal(result$b, c(10, 20))
})

test_that("print() output reaches the R console and errors are raised once", {
    sess <- create_session()
    expect_output(session_eval(sess, 'print("from ibex");'), "from ibex")
    expect_error(session_eval(sess, "missing_table[select { a }];"), "missing_table")
})

test_that("a call's tables and scalars last for that call only", {
    sess <- create_session()
    session_eval(sess, "let kept = Table { v = [1, 2, 3] };")
    out <- session_eval(
        sess,
        "kept[filter v > cutoff];",
        tables = list(kept = data.frame(v = c(10L, 20L))),
        scalars = list(cutoff = 15L)
    )
    expect_equal(out$v, 20)
    expect_equal(session_eval(sess, "kept;")$v, c(1, 2, 3))
    expect_error(session_eval(sess, "kept[filter v > cutoff];"))
})

test_that("a session imports a library", {
    paths <- default_plugin_paths()
    skip_if_not(any(file.exists(file.path(paths, "csv.ibex"))), "csv library not on the plugin path")
    csv <- tempfile(fileext = ".csv")
    on.exit(unlink(csv))
    writeLines(c("k,v", "a,1", "b,2"), csv)
    sess <- create_session(plugin_paths = paths)
    result <- session_eval(sess, sprintf('import "csv"; csv::read("%s")[filter v > 1];', csv))
    expect_equal(result$k, "b")
})
