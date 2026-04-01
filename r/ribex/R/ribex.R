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
    payload <- .Call(ribex_c_session_eval_ibex, session, query, tables, scalars)
    as_ribex_result(payload, format)
}

session_eval_file <- function(session,
                              path,
                              tables = NULL,
                              scalars = NULL,
                              format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ribex_c_session_eval_file, session, path, tables, scalars)
    as_ribex_result(payload, format)
}

eval_ibex <- function(query,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ribex_c_eval_ibex, query, plugin_paths, tables, scalars)
    as_ribex_result(payload, format)
}

eval_file <- function(path,
                      plugin_paths = default_plugin_paths(),
                      tables = NULL,
                      scalars = NULL,
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ribex_c_eval_file, path, plugin_paths, tables, scalars)
    as_ribex_result(payload, format)
}
