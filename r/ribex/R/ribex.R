knitr_state <- new.env(parent = emptyenv())
knitr_state$sessions <- new.env(parent = emptyenv())

`%||%` <- function(lhs, rhs) {
    if (is.null(lhs)) rhs else lhs
}

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
        env_plugin_paths,
        if (nzchar(env_build_dir)) file.path(env_build_dir, "tools") else character(),
        file.path(getwd(), "build-release", "tools"),
        file.path(getwd(), "build", "tools")
    )

    candidates <- unique(normalizePath(candidates, winslash = "/", mustWork = FALSE))
    candidates[dir.exists(candidates)]
}

as_ribex_result <- function(payload, format) {
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

    as.data.frame(array)
}

normalize_table_binding <- function(value) {
    if (is.null(value) || inherits(value, "data.frame") || inherits(value, "nanoarrow_array")) {
        return(value)
    }

    if (requireNamespace("nanoarrow", quietly = TRUE)) {
        converted <- tryCatch(nanoarrow::as_nanoarrow_array(value), error = function(e) NULL)
        if (!is.null(converted)) {
            return(converted)
        }
    }

    value
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
    .Call(ribex_c_create_session, plugin_paths)
}

reset_session <- function(session) {
    invisible(.Call(ribex_c_reset_session, session))
}

session_eval <- function(session,
                         query,
                         tables = NULL,
                         scalars = NULL,
                         format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ribex_c_session_eval_ibex, session, query, normalize_table_bindings(tables), scalars)
    as_ribex_result(payload, format)
}

session_eval_file <- function(session,
                              path,
                              tables = NULL,
                              scalars = NULL,
                              format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ribex_c_session_eval_file, session, path, normalize_table_bindings(tables), scalars)
    as_ribex_result(payload, format)
}

eval_ibex <- function(query,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ribex_c_eval_ibex, query, plugin_paths, normalize_table_bindings(tables), scalars)
    as_ribex_result(payload, format)
}

eval_file <- function(path,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ribex_c_eval_file, path, plugin_paths, normalize_table_bindings(tables), scalars)
    as_ribex_result(payload, format)
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
