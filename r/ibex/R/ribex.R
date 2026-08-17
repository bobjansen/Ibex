# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

knitr_state <- new.env(parent = emptyenv())
knitr_state$sessions <- new.env(parent = emptyenv())

.onUnload <- function(libpath) {
    # The runtime owns a process-wide C++ worker pool.  R namespace unloads
    # remove the shared library while the R process continues, so join those
    # workers before that can happen.
    .Call(ibex_c_shutdown_runtime)
}

`%||%` <- function(lhs, rhs) {
    if (is.null(lhs)) rhs else lhs
}

# These are deliberately only expression markers.  They are recognised while
# dplyr captures a lazy ibex_tbl expression and are never evaluated in R.
ibex_window_marker <- function(...) {
    rlang::abort("Window functions are available only inside mutate() or summarise() on ibex_tbl.")
}

#' @export
rolling_sum <- ibex_window_marker
#' @export
rolling_mean <- ibex_window_marker
#' @export
rolling_min <- ibex_window_marker
#' @export
rolling_max <- ibex_window_marker
#' @export
rolling_count <- ibex_window_marker
#' @export
rolling_median <- ibex_window_marker
#' @export
rolling_std <- ibex_window_marker
#' @export
rolling_ewma <- ibex_window_marker
#' @export
rolling_quantile <- ibex_window_marker
#' @export
rolling_skew <- ibex_window_marker
#' @export
rolling_kurtosis <- ibex_window_marker
#' @export
rolling_first <- ibex_window_marker
#' @export
rolling_last <- ibex_window_marker
#' @export
window_start <- ibex_window_marker
#' @export
window_end <- ibex_window_marker

default_plugin_paths <- function() {
    split_env_paths <- function(value) {
        if (!nzchar(value)) {
            return(character())
        }
        Filter(nzchar, strsplit(value, .Platform$path.sep, fixed = TRUE)[[1]])
    }

    env_build_dir <- Sys.getenv("IBEX_BUILD_DIR", unset = "")
    env_plugin_paths <- c(
        split_env_paths(Sys.getenv("IBEX_PLUGIN_PATHS", unset = "")),
        split_env_paths(Sys.getenv("IBEX_LIBRARY_PATH", unset = ""))
    )
    candidates <- c(
        system.file("plugins", package = "ibex"),
        env_plugin_paths,
        if (nzchar(env_build_dir)) file.path(env_build_dir, "tools") else character(),
        file.path(getwd(), "build-release", "tools"),
        file.path(getwd(), "build", "tools")
    )

    candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
    candidates[dir.exists(candidates)]
}

as_ibex_result <- function(payload, format) {
    if (is.null(payload)) {
        return(invisible(NULL))
    }

    stopifnot(is.list(payload), all(c("array", "schema") %in% names(payload)))

    array <- payload$array
    schema <- payload$schema
    nanoarrow::nanoarrow_array_set_schema(array, schema)
    attr(array, "schema_xptr") <- schema

    if (identical(format, "nanoarrow")) {
        return(array)
    }

    warn_int64_precision(convert_ibex_array(array, schema), schema)
}

# An Ibex Categorical is exported as an Arrow dictionary array, which is
# exactly what an R factor is: integer codes plus a levels vector. nanoarrow's
# default conversion does not see it that way -- it decodes the dictionary and
# materializes one R string per row, which costs 165ms on an 8M-row column
# against 17ms for the factor it already had the parts for, and throws away the
# encoding on the way.
#
# So name the ptype rather than letting it be inferred: `factor()` for the
# dictionary columns, and the inferred ptype for every other column, which is
# the same conversion `as.data.frame()` would have chosen for them.
convert_ibex_array <- function(array, schema) {
    children <- schema$children
    if (!length(children)) {
        return(as.data.frame(array))
    }
    is_dictionary <- vapply(children, function(child) !is.null(child$dictionary), logical(1))
    if (!any(is_dictionary)) {
        return(as.data.frame(array))
    }

    ptype <- lapply(seq_along(children), function(i) {
        if (is_dictionary[[i]]) factor() else nanoarrow::infer_nanoarrow_ptype(children[[i]])
    })
    names(ptype) <- names(children)
    nanoarrow::convert_array(
        array,
        to = structure(ptype, class = "data.frame", row.names = integer(0))
    )
}

# R has no 64-bit integer vector, so an Ibex Int64 column reaches R as a
# double. That is the right trade -- the alternative, narrowing to `integer()`
# when the values happen to fit, would make a column's R type depend on its
# contents, so filtering out one large row would silently change it.
#
# What is not acceptable is losing a value in silence. A double represents
# every integer up to 2^53 exactly and only some beyond it, so a magnitude of
# at least that is the condition under which the conversion may not have
# round-tripped. The bound is inclusive because this runs *after* the
# conversion and can only see its output: 2^53 + 1 rounds to exactly 2^53, so
# a column reading 2^53 is either an exact value or a rounded one and there is
# no longer anything to tell them apart. Warn, name the columns, and leave the
# data alone: a caller who needs those values intact can reach for bit64 on
# the way in and knows to do something different on the way out.
warn_int64_precision <- function(data, schema) {
    children <- schema$children
    if (!is.data.frame(data) || !length(children) || length(children) != length(data)) {
        return(data)
    }
    limit <- 2^53
    lossy <- vapply(seq_along(children), function(i) {
        # "l" is Arrow's format string for int64. Timestamps are int64 too but
        # spell themselves differently, and nanoarrow already warns for those.
        if (!identical(children[[i]]$format, "l")) {
            return(FALSE)
        }
        column <- data[[i]]
        is.numeric(column) && any(is.finite(column) & abs(column) >= limit)
    }, logical(1))
    if (any(lossy)) {
        rlang::warn(paste0(
            "Int64 values at or beyond 2^53 were converted to double and may have ",
            "lost precision: ", paste(names(data)[lossy], collapse = ", "), "."
        ))
    }
    data
}

normalize_table_binding <- function(value) {
    if (is.null(value) || inherits(value, "data.frame")) {
        return(value)
    }

    if (!inherits(value, "nanoarrow_array")) {
        converted <- tryCatch(nanoarrow::as_nanoarrow_array(value), error = function(e) NULL)
        if (is.null(converted)) {
            return(value)
        }
        value <- converted
    }

    # Export a fresh Arrow C Data shell rather than moving from the caller's
    # external pointer. nanoarrow's export callback keeps the R-owned buffers
    # alive without copying them and is safe for a non-R consumer to release.
    # The schema export is a small metadata-only deep copy.
    schema <- nanoarrow::infer_nanoarrow_schema(value)
    array_export <- nanoarrow::nanoarrow_allocate_array()
    schema_export <- nanoarrow::nanoarrow_allocate_schema()
    nanoarrow::nanoarrow_pointer_export(value, array_export)
    nanoarrow::nanoarrow_pointer_export(schema, schema_export)

    structure(
        list(array = array_export, schema = schema_export),
        class = "ibex_arrow_export"
    )
}

normalize_table_bindings <- function(tables) {
    if (is.null(tables)) {
        return(NULL)
    }
    if (!is.list(tables)) {
        return(tables)
    }

    lapply(tables, normalize_table_binding)
}

create_session <- function(plugin_paths = default_plugin_paths()) {
    .Call(ibex_c_create_session, plugin_paths)
}

reset_session <- function(session) {
    invisible(.Call(ibex_c_reset_session, session))
}

session_eval <- function(session,
                         query,
                         tables = NULL,
                         scalars = NULL,
                         format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ibex_c_session_eval_ibex, session, query, normalize_table_bindings(tables), scalars)
    as_ibex_result(payload, format)
}

session_eval_file <- function(session,
                              path,
                              tables = NULL,
                              scalars = NULL,
                              format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ibex_c_session_eval_file, session, path, normalize_table_bindings(tables), scalars)
    as_ibex_result(payload, format)
}

eval_ibex <- function(query,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ibex_c_eval_ibex, query, plugin_paths, normalize_table_bindings(tables), scalars)
    as_ibex_result(payload, format)
}

eval_file <- function(path,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ibex_c_eval_file, path, plugin_paths, normalize_table_bindings(tables), scalars)
    as_ibex_result(payload, format)
}

get_knitr_session <- function(name, plugin_paths) {
    stopifnot(is.character(name), length(name) == 1L, nzchar(name))
    if (!exists(name, envir = knitr_state$sessions, inherits = FALSE)) {
        assign(name, create_session(plugin_paths = plugin_paths), envir = knitr_state$sessions)
    }
    get(name, envir = knitr_state$sessions, inherits = FALSE)
}

knitr_session <- function(name = "default",
                          plugin_paths = default_plugin_paths()) {
    get_knitr_session(name, plugin_paths)
}

format_knitr_output <- function(result) {
    if (is.null(result)) {
        return(character())
    }
    paste(capture.output(print(result)), collapse = "\n")
}

register_knitr_engines <- function() {
    if (!requireNamespace("knitr", quietly = TRUE)) {
        stop("register_knitr_engines() requires the 'knitr' package")
    }

    knitr::knit_engines$set(ibex = function(options) {
        code <- paste(options$code, collapse = "\n")
        format <- if (is.null(options$format)) "data.frame" else options$format
        plugin_paths <- if (is.null(options$plugin_paths)) default_plugin_paths() else options$plugin_paths
        tables <- if (is.null(options$tables)) NULL else options$tables
        scalars <- if (is.null(options$scalars)) NULL else options$scalars
        quiet <- isTRUE(options$quiet)
        assign_name <- options$assign %||% NULL
        session_name <- options$session %||% NULL

        if (isTRUE(options$reset) && !is.null(session_name)) {
            reset_session(get_knitr_session(session_name, plugin_paths))
        }

        result <- if (is.null(session_name)) {
            eval_ibex(code, plugin_paths = plugin_paths, tables = tables, scalars = scalars, format = format)
        } else {
            session_eval(get_knitr_session(session_name, plugin_paths),
                         code, tables = tables, scalars = scalars, format = format)
        }

        if (!is.null(assign_name)) {
            assign(assign_name, result, envir = knitr::knit_global())
        }

        rendered <- if (quiet) character() else format_knitr_output(result)
        knitr::engine_output(options, options$code, rendered)
    })

    invisible(TRUE)
}

.onLoad <- function(libname, pkgname) {
    if (requireNamespace("knitr", quietly = TRUE)) {
        register_knitr_engines()
    }
}
