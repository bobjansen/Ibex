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

eval_ibex <- function(query,
                      plugin_paths = default_plugin_paths(),
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(query), length(query) == 1L)
    payload <- .Call(ribex_c_eval_ibex, query, plugin_paths)
    as_ribex_result(payload, format)
}

eval_file <- function(path,
                      plugin_paths = default_plugin_paths(),
                      format = c("data.frame", "nanoarrow")) {
    format <- match.arg(format)
    stopifnot(is.character(path), length(path) == 1L)
    payload <- .Call(ribex_c_eval_file, path, plugin_paths)
    as_ribex_result(payload, format)
}
