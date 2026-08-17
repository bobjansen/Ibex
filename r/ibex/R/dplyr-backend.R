# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

ibex_dplyr_state <- new.env(parent = emptyenv())
ibex_dplyr_state$binding_id <- 0L
ibex_dplyr_state$default_session <- NULL

ibex_quote_identifier <- function(name) {
    stopifnot(is.character(name), length(name) == 1L, !is.na(name))
    if (grepl("\\$\\{", name)) {
        rlang::abort(
            paste0("Ibex cannot currently quote a column name containing '${': ", encodeString(name)),
            class = "ibex_unsupported"
        )
    }
    characters <- strsplit(name, "", fixed = TRUE)[[1]]
    escaped <- paste0(
        ifelse(characters %in% c("\\", "`"), paste0("\\", characters), characters),
        collapse = ""
    )
    paste0("`", escaped, "`")
}

# A double-quoted Ibex string literal. Used for join suffixes, which are data
# rather than identifiers and so cannot go through the backtick form.
ibex_quote_string <- function(value) {
    stopifnot(is.character(value), length(value) == 1L, !is.na(value))
    characters <- strsplit(value, "", fixed = TRUE)[[1]]
    escaped <- paste0(
        ifelse(characters %in% c("\\", "\""), paste0("\\", characters), characters),
        collapse = ""
    )
    paste0("\"", escaped, "\"")
}

ibex_new_binding_name <- function(session, kind = "source") {
    repeat {
        ibex_dplyr_state$binding_id <- ibex_dplyr_state$binding_id + 1L
        candidate <- sprintf("__ibex_dplyr_%s_%08d", kind, ibex_dplyr_state$binding_id)
        occupied <- tryCatch({
            .Call(ibex_c_session_table_info, session, candidate)
            TRUE
        }, error = function(e) FALSE)
        if (!occupied) {
            return(candidate)
        }
    }
}

session_table_schema <- function(session, name) {
    stopifnot(is.character(name), length(name) == 1L, !is.na(name), nzchar(name))
    info <- .Call(ibex_c_session_table_info, session, name)
    schema <- tibble::tibble(
        name = info$names,
        type = info$types,
        nullable = info$nullable,
        categorical = info$categorical,
        timezone = info$timezone
    )
    attr(schema, "rows") <- info$rows
    attr(schema, "ordering") <- tibble::tibble(
        name = info$ordering,
        descending = info$descending
    )
    attr(schema, "time_index") <- info$time_index
    attr(schema, "generation") <- info$generation
    attr(schema, "grouped_by") <- info$grouped_by
    schema
}

ibex_schema_from_info <- function(info) {
    list(
        names = info$names,
        types = info$types,
        nullable = info$nullable,
        categorical = info$categorical,
        timezone = info$timezone,
        rows = info$rows
    )
}

ibex_table_info <- function(session, source) {
    .Call(ibex_c_session_table_info, session, source)
}

ibex_assert_live <- function(x) {
    info <- tryCatch(
        ibex_table_info(x$session, x$source),
        error = function(e) {
            rlang::abort(
                "This ibex_tbl was invalidated because its session was reset or finalized.",
                parent = e,
                class = "ibex_invalid_session"
            )
        }
    )
    if (!identical(as.numeric(info$generation), as.numeric(x$generation))) {
        rlang::abort(
            "This ibex_tbl was invalidated because its session was reset.",
            class = "ibex_invalid_session"
        )
    }
    invisible(info)
}

new_ibex_tbl <- function(session, source, schema, generation, steps = list(), groups = character(),
                         ordering = NULL, captured_scalars = list(), fallback_policy = "warn",
                         display_name = NULL, source_frame = NULL, time_index = NULL,
                         pending_window = NULL) {
    structure(
        list(
            session = session,
            source = source,
            source_frame = source_frame,
            time_index = time_index,
            pending_window = pending_window,
            schema = schema,
            steps = steps,
            groups = groups,
            ordering = ordering,
            captured_scalars = captured_scalars,
            fallback_policy = fallback_policy,
            host_bound = FALSE,
            generation = generation,
            display_name = display_name %||% source
        ),
        class = c("ibex_tbl", "tbl")
    )
}

#' The shared session `ibex_tbl()` uses by default
#'
#' Tables can only be combined natively — joined, or referenced from the same
#' query — when they live in the same Ibex session. `ibex_tbl()` therefore
#' binds into one lazily created per-R-session default rather than calling
#' [create_session()] afresh for every table, which would make even
#' `inner_join(ibex_tbl(a), ibex_tbl(b))` fall back to local dplyr.
#'
#' Bindings in this session are held for the lifetime of the R session, so
#' repeatedly wrapping large tables accumulates memory. Call
#' [ibex_reset_default_session()] to drop them, or pass an explicit `session`
#' to [ibex_tbl()] when you want an independent lifetime.
#'
#' @return An Ibex session handle.
#' @export
ibex_default_session <- function() {
    session <- ibex_dplyr_state$default_session
    if (is.null(session)) {
        session <- create_session()
        ibex_dplyr_state$default_session <- session
    }
    session
}

#' Discard the shared default Ibex session
#'
#' Invalidates every `ibex_tbl` created against the default session; the next
#' call to [ibex_default_session()] creates a fresh one.
#'
#' @return Invisibly, `NULL`.
#' @export
ibex_reset_default_session <- function() {
    ibex_dplyr_state$default_session <- NULL
    invisible(NULL)
}

#' Create a lazy dplyr table backed by Ibex
#'
#' @param x An in-memory data frame, tibble, or nanoarrow-compatible table.
#' @param session A persistent ibex session. Defaults to the shared session
#'   returned by [ibex_default_session()], so that tables wrapped separately
#'   can still be joined natively.
#' @param name An optional display name. Native binding names are generated.
#' @param fallback What to do when a verb cannot be translated: warn and collect,
#'   error, or collect silently.
#' @param categorical_strings Bind `character` columns as Ibex `Categorical`
#'   (dictionary-encoded) rather than `String`. Grouping, `distinct()` and
#'   sorting on a dictionary-encoded key compare dense integer codes instead of
#'   hashing text per row, which on 8M rows over 252 distinct symbols is 3ms
#'   against 100ms. The cost is on the way back: a Categorical column collects
#'   into R as a `factor`, and a large result pays to rebuild the levels, so a
#'   query that returns most of its rows as strings is better off without it.
#'   Off by default, because a `character` column collecting back as `character`
#'   is the contract every other dplyr backend keeps. Columns already passed as
#'   factors bind as Categorical either way.
#' @export
ibex_tbl <- function(x, session = ibex_default_session(), name = NULL,
                     fallback = c("warn", "error", "collect"),
                     categorical_strings = FALSE) {
    # Capture the caller's expression before anything below rebinds `x`.
    # `substitute(x)` reports the promise's expression only while `x` is still
    # the untouched argument binding; after an assignment to `x` it reports the
    # *value* instead, and deparsing a multi-million-row data frame to build a
    # display name took 4 seconds on an 8M-row table.
    default_display_name <- deparse1(substitute(x))
    fallback_missing <- missing(fallback)
    if (inherits(x, "ibex_tbl")) {
        if (!missing(session) && !identical(session, x$session)) {
            rlang::abort("An existing ibex_tbl cannot be moved to a different session.")
        }
        if (!fallback_missing) x$fallback_policy <- match.arg(fallback)
        return(x)
    }
    fallback <- match.arg(fallback)
    if (!inherits(x, "data.frame") && !inherits(x, "nanoarrow_array")) {
        converted <- tryCatch(nanoarrow::as_nanoarrow_array(x), error = function(e) NULL)
        if (is.null(converted)) {
            rlang::abort("`x` must be a data frame, tibble, or nanoarrow-compatible table.")
        }
        x <- converted
    }
    if (isTRUE(categorical_strings) && inherits(x, "data.frame")) {
        attr(x, "ibex_categorical_strings") <- TRUE
    }
    binding <- ibex_new_binding_name(session)
    input_name <- paste0(binding, "_input")
    session_eval(
        session,
        paste0("let ", binding, " = ", input_name, ";"),
        tables = stats::setNames(list(x), input_name)
    )
    info <- ibex_table_info(session, binding)
    new_ibex_tbl(
        session = session,
        source = binding,
        schema = ibex_schema_from_info(info),
        generation = info$generation,
        ordering = if (length(info$ordering)) {
            data.frame(name = info$ordering, descending = info$descending)
        } else {
            NULL
        },
        fallback_policy = fallback,
        display_name = name %||% default_display_name,
        # Held only while the plan can still use it; `ibex_append_step()` drops
        # it at the first step that makes reuse impossible. See
        # `ibex_reusable_source_columns()`.
        source_frame = if (inherits(x, "data.frame")) x else NULL,
        time_index = if (length(info$time_index)) info$time_index[[1]] else NULL
    )
}

#' Create a lazy Ibex table from a Parquet file
#'
#' The file remains a Parquet-backed Ibex source: filters and projections are
#' planned by Ibex and can be pushed into the scan.  No R data frame is created
#' until [dplyr::collect()] is called.
#'
#' @param path A local path to a Parquet file.
#' @param session A persistent Ibex session. Defaults to the shared session
#'   returned by [ibex_default_session()].
#' @param name An optional display name used when printing the lazy table.
#' @param fallback What to do when a dplyr expression cannot be translated:
#'   warn and collect, error, or collect silently.
#' @return An immutable lazy `ibex_tbl` backed by the Parquet file.
#' @export
ibex_read_parquet <- function(path, session = ibex_default_session(), name = NULL,
                              fallback = c("warn", "error", "collect")) {
    if (!is.character(path) || length(path) != 1L || is.na(path) || !nzchar(path)) {
        rlang::abort("`path` must be one non-empty, non-missing character string.")
    }
    fallback <- match.arg(fallback)

    # Declaring the extern is also the activation boundary for the R host: its
    # session loader finds parquet.so in default_plugin_paths() and registers it
    # once. Repeating the declaration in a session is harmless.
    session_eval(session,
                 'extern fn read_parquet(path: String) -> DataFrame from "parquet.hpp";')
    binding <- ibex_new_binding_name(session, "parquet")
    session_eval(session, paste0("let ", binding, " = read_parquet(",
                                 ibex_quote_string(path), ");"))
    info <- ibex_table_info(session, binding)
    new_ibex_tbl(
        session = session,
        source = binding,
        schema = ibex_schema_from_info(info),
        generation = info$generation,
        ordering = if (length(info$ordering)) {
            data.frame(name = info$ordering, descending = info$descending)
        } else {
            NULL
        },
        fallback_policy = fallback,
        display_name = name %||% path,
        time_index = if (length(info$time_index)) info$time_index[[1]] else NULL
    )
}

names.ibex_tbl <- function(x) x$schema$names

dim.ibex_tbl <- function(x) {
    rows <- x$schema$rows
    c(if (is.na(rows)) NA_integer_ else as.integer(rows), length(x$schema$names))
}

ibex_schema_proxy <- function(x) {
    columns <- Map(function(type, categorical, timezone) {
        if (isTRUE(categorical) || identical(type, "Categorical")) return(factor())
        zone <- if (length(timezone) && !is.na(timezone) && nzchar(timezone)) timezone else "UTC"
        switch(
            type,
            Int64 = integer(),
            Float64 = double(),
            Bool = logical(),
            String = character(),
            Date = as.Date(numeric(), origin = "1970-01-01"),
            Timestamp = as.POSIXct(numeric(), origin = "1970-01-01", tz = zone),
            logical()
        )
    }, x$schema$types, x$schema$categorical, x$schema$timezone)
    stats::setNames(columns, x$schema$names)
}

ibex_append_step <- function(x, step, schema = x$schema, groups = x$groups,
                             ordering = x$ordering, scalars = x$captured_scalars) {
    ibex_assert_live(x)
    # Release the caller's table the moment the plan stops being one whose rows
    # and row order match it. Reuse is the only reason it is held, so holding it
    # past that point would extend a lifetime for nothing -- and a plan that
    # filters, sorts, joins or aggregates can never use it again.
    if (!identical(step$kind, "update") || length(step$groups) || length(groups)) {
        x$source_frame <- NULL
    }
    x$steps <- c(x$steps, list(step))
    x$schema <- schema
    x$groups <- groups
    x$ordering <- ordering
    x$captured_scalars <- scalars
    x$schema$nullable <- ibex_infer_nullable(x)
    x
}

# Ask Ibex which columns of the plan built so far can hold nulls.
#
# Every verb routes through `ibex_append_step`, so this is the single place the
# answer is produced -- deliberately, because the alternative is what this
# replaced: a nullability rule written out again in each verb, which is a second
# implementation of the core's propagation that can only ever drift from it.
# Ibex's rules are also strictly better than the ones that were here, since they
# see the whole plan rather than one step: a `filter` proves the columns its
# predicate had to read, and an inner join proves its key columns, neither of
# which a per-verb rule can express.
#
# Nothing here can fail loudly. Ibex returns NULL for a plan it cannot lower or
# whose schema is Unknown, and every column is then assumed nullable, which is
# what the adapter assumed before asking.
ibex_infer_nullable <- function(x) {
    unproven <- rep(TRUE, length(x$schema$names))
    inferred <- tryCatch(
        .Call(
            ibex_c_session_infer_schema,
            x$session,
            ibex_render_plan(x),
            names(x$captured_scalars) %||% character()
        ),
        error = function(e) NULL
    )
    if (is.null(inferred)) {
        return(unproven)
    }
    # Align by name rather than by position, and keep the conservative answer
    # for anything Ibex did not describe. The two column lists agree today; a
    # disagreement means the adapter and the core read the plan differently,
    # and silently pairing them off by index would turn that into a wrong
    # proof about the wrong column.
    pos <- match(x$schema$names, inferred$names)
    ifelse(is.na(pos), TRUE, inferred$nullable[pos])
}

ibex_render_fields <- function(fields) {
    paste(vapply(fields, function(field) {
        if (identical(field$code, ibex_quote_identifier(field$name))) {
            field$code
        } else {
            paste0(ibex_quote_identifier(field$name), " = ", field$code)
        }
    }, character(1)), collapse = ", ")
}

ibex_render_window <- function(window) {
    paste0("window ", window$duration, if (isTRUE(window$aligned)) " aligned" else "")
}

ibex_render_plan <- function(x, redact = FALSE) {
    ibex_assert_live(x)
    if (!is.null(x$pending_window)) {
        rlang::abort("window() must be followed by mutate() or summarise() before execution.",
                     class = "ibex_unsupported")
    }
    code <- x$source
    for (step in x$steps) {
        code <- switch(
            step$kind,
            filter = paste0(code, "[filter ", paste(step$predicates, collapse = " && "), "]"),
            project = paste0(code, "[select { ", ibex_render_fields(step$fields), " }]"),
            rename = paste0(code, "[rename { ", paste(vapply(step$mappings, function(mapping) {
                paste0(ibex_quote_identifier(mapping$new), " = ", ibex_quote_identifier(mapping$old))
            }, character(1)), collapse = ", "), " }]"),
            update = paste0(
                code, "[", if (!is.null(step$window)) paste0(ibex_render_window(step$window), ", ") else "",
                "update { ", ibex_render_fields(step$fields), " }",
                if (length(step$groups)) paste0(", by { ", paste(vapply(step$groups, ibex_quote_identifier, character(1)), collapse = ", "), " }") else "",
                "]"
            ),
            aggregate = paste0(
                code, "[", if (!is.null(step$window)) paste0(ibex_render_window(step$window), ", ") else "",
                "select { ", ibex_render_fields(step$fields), " }",
                if (length(step$groups)) paste0(", by { ", paste(vapply(step$groups, ibex_quote_identifier, character(1)), collapse = ", "), " }") else "",
                "]"
            ),
            timeframe = paste0("as_timeframe(", code, ", ", ibex_quote_string(step$index), ")"),
            resample = paste0(
                code, "[resample ", step$duration, if (isTRUE(step$aligned)) " aligned" else "",
                ", select { ", ibex_render_fields(step$fields), " }",
                if (length(step$groups)) paste0(", by { ", paste(vapply(step$groups, ibex_quote_identifier, character(1)), collapse = ", "), " }") else "",
                "]"
            ),
            order = paste0(code, "[order { ", paste(vapply(seq_along(step$names), function(i) {
                paste(ibex_quote_identifier(step$names[[i]]), if (step$descending[[i]]) "desc" else "asc")
            }, character(1)), collapse = ", "), " }]"),
            head = paste0(
                code, "[head ", step$n,
                if (length(step$groups)) paste0(", by { ", paste(vapply(step$groups, ibex_quote_identifier, character(1)), collapse = ", "), " }") else "",
                "]"
            ),
            distinct = paste0(code, "[distinct { ", paste(vapply(step$names, ibex_quote_identifier, character(1)), collapse = ", "), " }]"),
            join = paste0(
                "(", code, ibex_join_operator(step$join_kind),
                sub(";$", "", ibex_render_plan(step$right)),
                # A cross join has no keys, and Ibex has no spelling for an
                # empty `on`. Every other kind carries at least one.
                if (length(step$keys)) {
                    paste0(" on { ",
                           paste(vapply(seq_along(step$keys), function(i) {
                               left <- step$keys[[i]]
                               right <- step$right_keys[[i]]
                               # `a` is Ibex's shorthand for `a = a`. Spelling
                               # the pair out only when it differs keeps the
                               # rendered plan the one a reader would write.
                               if (identical(left, right)) {
                                   ibex_quote_identifier(left)
                               } else {
                                   paste0(ibex_quote_identifier(left), " = ",
                                          ibex_quote_identifier(right))
                               }
                           }, character(1)), collapse = ", "),
                           " }")
                } else "",
                # Ibex refuses a non-key collision without this clause, and its
                # two-suffix form is exactly dplyr's `suffix` argument, so the
                # names come out right without a rename pass afterwards.
                if (length(step$suffix) == 2L) {
                    paste0(" suffix { ", ibex_quote_string(step$suffix[[1]]), ", ",
                           ibex_quote_string(step$suffix[[2]]), " }")
                } else "",
                # Trails the suffix clause, which is where Ibex's grammar wants
                # it. `never` is the default, so only `equal` needs saying --
                # and saying it is what tells the core to withdraw the key's
                # non-null proof, since a null key now matches.
                if (identical(step$null_match, "equal")) " nulls equal" else "",
                ")"
            ),
            rlang::abort(paste0("Unknown ibex lazy-plan step: ", step$kind))
        )
    }
    if (redact && length(x$captured_scalars)) {
        for (scalar in names(x$captured_scalars)) {
            code <- gsub(paste0("\\^", scalar, "\\b"), paste0("^", scalar, "<", class(x$captured_scalars[[scalar]])[[1]], ">"), code)
        }
    }
    paste0(code, ";")
}

show_query.ibex_tbl <- function(x, ...) {
    cat("<Ibex lazy query>\n", ibex_render_plan(x, redact = TRUE), "\n", sep = "")
    invisible(x)
}

print.ibex_tbl <- function(x, ..., n = NULL, width = NULL) {
    ibex_assert_live(x)
    dims <- if (is.na(x$schema$rows)) "? rows" else paste0(format(x$schema$rows, scientific = FALSE), " rows")
    cat("# ibex_tbl [", dims, " x ", length(x$schema$names), "]\n", sep = "")
    cat("# Source: ", x$display_name, "\n", sep = "")
    if (length(x$groups)) cat("# Groups: ", paste(x$groups, collapse = ", "), "\n", sep = "")
    if (length(x$schema$names)) {
        columns <- paste0(x$schema$names, " <", x$schema$types, ifelse(x$schema$nullable, "?", ""), ">")
        cat("# Columns: ", paste(columns, collapse = ", "), "\n", sep = "")
    }
    cat("# Query: ", ibex_render_plan(x, redact = TRUE), "\n", sep = "")
    invisible(x)
}

# The R class `collect()` produces for each Ibex column type. A source column
# may only be handed back verbatim when its class already equals the one a full
# collect would have produced -- otherwise the two paths would disagree about
# the type of the same query's result. An R `integer` bound as Int64 comes back
# as a double, for instance, and a `character` bound with
# `categorical_strings = TRUE` comes back as a factor; neither is reusable.
ibex_collect_class <- c(
    Float64 = "numeric", Int64 = "numeric", String = "character",
    Categorical = "factor", Bool = "logical", Date = "Date", Timestamp = "POSIXct"
)

# Which output columns can be served from the caller's own R vectors.
#
# Materializing a column out of Ibex costs what its bytes cost -- ~3ms per 8M
# doubles, ~10ms for 8M dictionary codes -- and a `mutate` re-materializes every
# column it did not touch. Those columns are, by construction, the ones the
# caller already holds: same values, same order, same rows.
#
# The plan therefore has to be one that changes neither the row set nor its
# order, which restricts this to a chain of ungrouped `update` steps. A filter,
# a sort, a join, a distinct, an aggregate or a grouped update all break the
# correspondence and disqualify the whole plan. Returns NULL when nothing can be
# reused, in which case the caller collects normally.
#
# The reference to the caller's table is a strong one, and deliberately narrow
# because of it: R can only weakly reference environments and external pointers,
# never a data frame, so there is no way to hold one that a collection can take
# back. `ibex_append_step()` therefore drops it at the first step that makes
# reuse impossible, which bounds the retention to plans that can actually use
# it. While the caller still holds the table -- the ordinary case, since they
# are the one who passed it -- this costs nothing at all.
ibex_reusable_source_columns <- function(x) {
    if (is.null(x$source_frame) || !length(x$steps) || length(x$groups)) {
        return(NULL)
    }
    derived <- character()
    for (step in x$steps) {
        if (!identical(step$kind, "update") || length(step$groups)) {
            return(NULL)
        }
        derived <- c(derived, vapply(step$fields, `[[`, character(1), "name"))
    }

    source <- x$source_frame

    candidates <- setdiff(x$schema$names, derived)
    keep <- candidates[vapply(candidates, function(name) {
        if (!name %in% names(source)) return(FALSE)
        type <- x$schema$types[[match(name, x$schema$names)]]
        expected <- ibex_collect_class[[type]] %||% NA_character_
        !is.na(expected) && identical(class(source[[name]])[[1]], expected)
    }, logical(1))]
    if (!length(keep)) {
        return(NULL)
    }
    list(source = source, reuse = keep)
}

# Collect a plan while serving `reusable$reuse` from the caller's own vectors.
#
# Ibex is asked for exactly the columns that are left, via a projection appended
# to the plan, and the result is reassembled in the schema's column order so it
# is indistinguishable from a full collect.
ibex_collect_reusing <- function(x, reusable, format) {
    wanted <- setdiff(x$schema$names, reusable$reuse)
    computed <- NULL
    if (length(wanted)) {
        projected <- x
        projected$steps <- c(
            x$steps,
            list(list(kind = "project", fields = lapply(wanted, function(name) {
                list(name = name, code = ibex_quote_identifier(name))
            })))
        )
        computed <- collect(projected, format = "data.frame")
    }

    columns <- lapply(x$schema$names, function(name) {
        if (name %in% reusable$reuse) reusable$source[[name]] else computed[[name]]
    })
    names(columns) <- x$schema$names
    if (identical(format, "data.frame")) {
        return(as.data.frame(columns, stringsAsFactors = FALSE, optional = TRUE))
    }
    tibble::as_tibble(columns)
}

collect.ibex_tbl <- function(x, ..., format = c("tibble", "data.frame", "nanoarrow")) {
    format <- match.arg(format)
    ibex_assert_live(x)
    if (!identical(format, "nanoarrow")) {
        reusable <- ibex_reusable_source_columns(x)
        if (!is.null(reusable)) {
            return(ibex_collect_reusing(x, reusable, format))
        }
    }
    native_format <- if (identical(format, "nanoarrow")) "nanoarrow" else "data.frame"
    result <- tryCatch(
        session_eval(
            x$session,
            ibex_render_plan(x),
            scalars = if (length(x$captured_scalars)) x$captured_scalars else NULL,
            format = native_format
        ),
        error = function(e) {
            rlang::abort("Ibex failed while executing a lazy dplyr query.", parent = e, class = "ibex_execution_error")
        }
    )
    if (identical(format, "nanoarrow")) return(result)
    if (identical(format, "data.frame")) return(result)
    result <- tibble::as_tibble(result, .name_repair = "minimal")
    if (length(x$groups)) {
        result <- rlang::inject(dplyr::group_by(result, !!!rlang::syms(x$groups)))
    }
    result
}

compute.ibex_tbl <- function(x, name = NULL, temporary = TRUE, ...) {
    ibex_assert_live(x)
    binding <- ibex_new_binding_name(x$session, "compute")
    session_eval(
        x$session,
        paste0("let ", binding, " = ", sub(";$", "", ibex_render_plan(x)), ";"),
        scalars = if (length(x$captured_scalars)) x$captured_scalars else NULL
    )
    info <- ibex_table_info(x$session, binding)
    new_ibex_tbl(
        session = x$session,
        source = binding,
        schema = ibex_schema_from_info(info),
        generation = info$generation,
        groups = x$groups,
        ordering = x$ordering,
        time_index = if (length(info$time_index)) info$time_index[[1]] else NULL,
        fallback_policy = x$fallback_policy,
        display_name = name %||% binding
    )
}

as.data.frame.ibex_tbl <- function(x, row.names = NULL, optional = FALSE, ...) {
    collect(x, format = "data.frame")
}

as_tibble.ibex_tbl <- function(x, ...) collect(x, format = "tibble")

pull.ibex_tbl <- function(.data, var = -1, name = NULL, ...) {
    var <- rlang::enquo(var)
    name <- rlang::enquo(name)
    dplyr::pull(collect(.data), var = !!var, name = !!name, ...)
}

ibex_unsupported <- function(message, expr = NULL) {
    label <- if (is.null(expr)) NULL else rlang::expr_text(expr)
    rlang::abort(message, class = "ibex_unsupported", expression = label)
}

ibex_fallback <- function(x, verb, condition, replay) {
    expression <- condition$expression %||% condition$message
    if (identical(x$fallback_policy, "error")) {
        rlang::abort(
            paste0("Cannot translate `", expression, "` in ", verb, "(). Call collect() explicitly or choose a collection fallback."),
            parent = condition,
            class = "ibex_translation_error"
        )
    }
    if (identical(x$fallback_policy, "warn")) {
        rlang::warn(
            paste0("Ibex cannot translate `", expression, "` in ", verb, "(); collected native prefix and continuing in local dplyr."),
            class = "ibex_fallback_warning"
        )
    }
    replay(collect(x))
}

ibex_with_fallback <- function(x, verb, native, replay) {
    tryCatch(native(), ibex_unsupported = function(e) ibex_fallback(x, verb, e, replay))
}

ibex_capture_scalar <- function(state, value, expr) {
    if (length(value) != 1L || is.object(value) && !inherits(value, c("Date", "POSIXct"))) {
        ibex_unsupported("Captured values must be supported length-one scalars.", expr)
    }
    if (is.na(value)) ibex_unsupported("Captured NA values are not supported by the scalar bridge.", expr)
    if (!is.logical(value) && !is.integer(value) && !is.double(value) && !is.character(value) &&
        !inherits(value, c("Date", "POSIXct"))) {
        ibex_unsupported("Captured value has no Ibex scalar representation.", expr)
    }
    name <- sprintf("__ibex_dplyr_scalar_%04d", length(state$scalars) + 1L)
    state$scalars[[name]] <- value
    list(code = paste0("^", name), type = if (inherits(value, "Date")) "Date" else if (inherits(value, "POSIXct")) "Timestamp" else if (is.logical(value)) "Bool" else if (is.integer(value)) "Int64" else if (is.double(value)) "Float64" else "String", nullable = FALSE, aggregate = FALSE, refs = character())
}

ibex_expr_result <- function(code, type = "Unknown", nullable = TRUE, aggregate = FALSE,
                             refs = character(), rank = FALSE) {
    list(code = code, type = type, nullable = nullable, aggregate = aggregate,
         refs = unique(refs), rank = rank)
}

ibex_column_expr <- function(name, x) {
    pos <- match(name, x$schema$names)
    if (is.na(pos)) ibex_unsupported(paste0("Unknown column `", name, "`."), as.name(name))
    ibex_expr_result(
        ibex_quote_identifier(name), x$schema$types[[pos]], x$schema$nullable[[pos]], FALSE, name
    )
}

ibex_call_identity <- function(head, env) {
    if (is.call(head) && identical(head[[1]], as.name("::")) && length(head) == 3L) {
        return(paste0(as.character(head[[2]]), "::", as.character(head[[3]])))
    }
    if (!is.symbol(head)) return(NULL)
    name <- as.character(head)
    known <- list(
        abs = base::abs, log = base::log, sqrt = base::sqrt, exp = base::exp,
        ceiling = base::ceiling, floor = base::floor, trunc = base::trunc,
        pmin = base::pmin, pmax = base::pmax, sum = base::sum, mean = base::mean,
        min = base::min, max = base::max, is.na = base::is.na, is.nan = base::is.nan,
        coalesce = dplyr::coalesce, between = dplyr::between, first = dplyr::first,
        last = dplyr::last, n = dplyr::n, min_rank = dplyr::min_rank,
        dense_rank = dplyr::dense_rank, row_number = dplyr::row_number,
        cume_dist = dplyr::cume_dist, as.Date = base::as.Date
    )
    rolling <- c("rolling_sum", "rolling_mean", "rolling_min", "rolling_max",
                 "rolling_count", "rolling_median", "rolling_std", "rolling_ewma",
                 "rolling_quantile", "rolling_skew", "rolling_kurtosis", "rolling_first",
                 "rolling_last", "window_start", "window_end")
    for (rolling_name in rolling) known[[rolling_name]] <- get(rolling_name, envir = asNamespace("ibex"))
    if (!name %in% names(known)) return(NULL)
    resolved <- tryCatch(rlang::eval_bare(head, env), error = function(e) NULL)
    if (!identical(resolved, known[[name]])) return(NULL)
    if (name %in% rolling) return(paste0("ibex::", name))
    paste0(if (name %in% c("coalesce", "between", "first", "last", "n")) "dplyr" else "base", "::", name)
}

ibex_translate_expr <- function(expr, quo_env, x, context, state, inside_aggregate = FALSE) {
    if (is.atomic(expr) && length(expr) == 1L) {
        return(ibex_capture_scalar(state, expr, expr))
    }
    if (is.symbol(expr)) {
        name <- as.character(expr)
        if (name %in% x$schema$names) {
            result <- ibex_column_expr(name, x)
            if (inside_aggregate) result$refs <- character()
            return(result)
        }
        value <- tryCatch(rlang::eval_bare(expr, quo_env), error = function(e) e)
        if (inherits(value, "error")) ibex_unsupported(paste0("Unknown R name `", name, "`."), expr)
        return(ibex_capture_scalar(state, value, expr))
    }
    if (!is.call(expr)) ibex_unsupported("Unsupported R expression.", expr)

    head <- expr[[1]]
    operator <- if (is.symbol(head)) as.character(head) else NULL
    if (identical(operator, "(")) return(ibex_translate_expr(expr[[2]], quo_env, x, context, state, inside_aggregate))

    if (identical(operator, "%in%")) {
        if (length(expr) != 3L) ibex_unsupported("`%in%` requires a value and a vector of candidates.", expr)
        value <- ibex_translate_expr(expr[[2]], quo_env, x, context, state, inside_aggregate)
        candidates <- tryCatch(rlang::eval_bare(expr[[3]], quo_env), error = function(e) e)
        if (inherits(candidates, "error") || !is.atomic(candidates) || is.object(candidates)) {
            ibex_unsupported("Native `%in%` requires an atomic candidate vector.", expr)
        }
        if (!length(candidates)) return(ibex_expr_result("false", "Bool", FALSE, FALSE, value$refs))
        non_missing <- candidates[!is.na(candidates)]
        comparisons <- vapply(seq_along(non_missing), function(i) {
            candidate <- ibex_capture_scalar(state, non_missing[[i]], expr[[3]])
            paste0("(", value$code, " == ", candidate$code, ")")
        }, character(1))
        if (anyNA(candidates)) comparisons <- c(comparisons, paste0("is_null(", value$code, ")"))
        if (!length(comparisons)) return(ibex_expr_result("false", "Bool", FALSE, FALSE, value$refs))
        return(ibex_expr_result(
            paste0("(", paste(comparisons, collapse = " || "), ")"),
            "Bool", value$nullable, value$aggregate, value$refs
        ))
    }

    if (!is.null(operator) && operator %in% c("+", "-", "*", "/", "%%", "==", "!=", "<", "<=", ">", ">=", "&", "|")) {
        if (length(expr) == 2L && operator %in% c("+", "-")) {
            value <- ibex_translate_expr(expr[[2]], quo_env, x, context, state, inside_aggregate)
            if (operator == "+") return(value)
            value$code <- paste0("(-", value$code, ")")
            return(value)
        }
        if (length(expr) != 3L) ibex_unsupported("Operator has unsupported arity.", expr)
        left <- ibex_translate_expr(expr[[2]], quo_env, x, context, state, inside_aggregate)
        right <- ibex_translate_expr(expr[[3]], quo_env, x, context, state, inside_aggregate)
        rendered <- switch(operator, "%%" = "%", "&" = "&&", "|" = "||", operator)
        type <- if (operator %in% c("==", "!=", "<", "<=", ">", ">=", "&", "|")) "Bool" else if (operator == "/" || "Float64" %in% c(left$type, right$type)) "Float64" else left$type
        return(ibex_expr_result(paste0("(", left$code, " ", rendered, " ", right$code, ")"), type, left$nullable || right$nullable, left$aggregate || right$aggregate, c(left$refs, right$refs)))
    }
    if (identical(operator, "!")) {
        if (length(expr) != 2L) ibex_unsupported("`!` has unsupported arity.", expr)
        value <- ibex_translate_expr(expr[[2]], quo_env, x, context, state, inside_aggregate)
        value$code <- paste0("(!", value$code, ")")
        value$type <- "Bool"
        return(value)
    }
    if (!is.null(operator) && operator %in% c("[[", "$")) {
        root <- expr[[2]]
        if (!is.symbol(root) || !as.character(root) %in% c(".data", ".env")) ibex_unsupported("Only `.data` and `.env` pronouns are translated.", expr)
        key <- if (operator == "$") as.character(expr[[3]]) else tryCatch(rlang::eval_bare(expr[[3]], quo_env), error = function(e) NULL)
        if (!is.character(key) || length(key) != 1L) ibex_unsupported("Pronoun subscripts must resolve to one string.", expr)
        if (as.character(root) == ".data") return(ibex_column_expr(key, x))
        value <- tryCatch(rlang::env_get(quo_env, key, inherit = TRUE), error = function(e) e)
        if (inherits(value, "error")) ibex_unsupported(paste0("Unknown `.env` value `", key, "`."), expr)
        return(ibex_capture_scalar(state, value, expr))
    }

    identity <- ibex_call_identity(head, quo_env)
    if (is.null(identity)) ibex_unsupported("Call is not in the Ibex translation registry or its R binding is masked.", expr)
    args <- as.list(expr)[-1]
    arg_names <- names(args) %||% rep("", length(args))

    if (identity %in% c("dplyr::n")) {
        if (length(args)) ibex_unsupported("`n()` takes no arguments.", expr)
        return(ibex_expr_result("count()", "Int64", FALSE, TRUE))
    }
    if (startsWith(identity, "ibex::")) {
        fn <- sub(".*::", "", identity)
        if (fn %in% c("rolling_count", "window_start", "window_end")) {
            if (length(args) || any(nzchar(arg_names))) ibex_unsupported(paste0("`", fn, "()` takes no arguments."), expr)
        } else if (fn %in% c("rolling_ewma", "rolling_quantile")) {
            if (length(args) != 2L || any(nzchar(arg_names))) ibex_unsupported(paste0("`", fn, "()` requires a column and a numeric parameter."), expr)
        } else if (!length(args) %in% c(1L, 2L) || any(nzchar(arg_names))) {
            ibex_unsupported(paste0("`", fn, "()` requires a column and an optional count window."), expr)
        }
        translated <- lapply(args, function(arg) ibex_translate_expr(arg, quo_env, x, context, state, inside_aggregate))
        # Ibex deliberately requires the rolling count, EWMA alpha, and
        # quantile probability to be source literals. Keep those literals in
        # the emitted Ibex rather than turning them into scalar bindings.
        literal_pos <- if (fn %in% c("rolling_ewma", "rolling_quantile")) 2L else if (length(args) == 2L) 2L else NULL
        if (!is.null(literal_pos)) {
            literal <- args[[literal_pos]]
            if (!is.atomic(literal) || length(literal) != 1L || !is.numeric(literal) || is.na(literal) || !is.finite(literal)) {
                ibex_unsupported(paste0("`", fn, "()` requires a finite numeric literal for its final argument."), expr)
            }
            translated[[literal_pos]] <- ibex_expr_result(
                format(literal, scientific = FALSE, trim = TRUE),
                if (is.integer(literal)) "Int64" else "Float64", FALSE
            )
        }
        codes <- vapply(translated, `[[`, character(1), "code")
        nullable <- any(vapply(translated, `[[`, logical(1), "nullable"))
        refs <- unlist(lapply(translated, `[[`, "refs"), use.names = FALSE)
        type <- if (fn %in% c("rolling_mean", "rolling_median", "rolling_std", "rolling_ewma", "rolling_quantile", "rolling_skew", "rolling_kurtosis")) "Float64" else if (fn == "rolling_count") "Int64" else if (fn %in% c("window_start", "window_end")) "Timestamp" else if (length(translated)) translated[[1]]$type else "Unknown"
        return(ibex_expr_result(paste0(fn, "(", paste(codes, collapse = ", "), ")"), type, nullable, FALSE, refs))
    }
    if (identity %in% c("dplyr::min_rank", "dplyr::dense_rank", "dplyr::row_number", "dplyr::cume_dist")) {
        if (length(args) != 1L || any(nzchar(arg_names))) {
            ibex_unsupported("Native rank helpers require exactly one column.", expr)
        }
        value <- ibex_translate_expr(args[[1]], quo_env, x, context, state, inside_aggregate)
        if (value$aggregate) ibex_unsupported("Rank helpers cannot rank an aggregate.", expr)
        rank_spec <- switch(
            identity,
            "dplyr::min_rank" = "method = min",
            "dplyr::dense_rank" = "method = dense",
            "dplyr::row_number" = "method = first",
            "dplyr::cume_dist" = "method = max, pct = true"
        )
        return(ibex_expr_result(
            paste0("rank(", value$code, ", ", rank_spec, ")"),
            if (identity == "dplyr::cume_dist") "Float64" else "Int64",
            value$nullable, FALSE, value$refs, rank = TRUE
        ))
    }
    if (identity %in% c("base::sum", "base::mean", "base::min", "base::max", "dplyr::first", "dplyr::last")) {
        fn <- sub(".*::", "", identity)
        value_args <- args[arg_names == "" | is.na(arg_names)]
        if (length(value_args) != 1L) ibex_unsupported(paste0("`", fn, "()` requires one data argument."), expr)
        value <- ibex_translate_expr(value_args[[1]], quo_env, x, context, state, TRUE)
        na_index <- which(arg_names %in% c("na.rm", "na_rm"))
        na_rm <- if (length(na_index)) tryCatch(isTRUE(rlang::eval_bare(args[[na_index[[1]]]], quo_env)), error = function(e) FALSE) else FALSE
        allowed_named <- c("", "na.rm", "na_rm")
        if (any(!arg_names %in% allowed_named)) ibex_unsupported(paste0("Unsupported arguments to `", fn, "()`."), expr)
        if (value$nullable && !na_rm) ibex_unsupported(paste0("`", fn, "()` on nullable data requires `na.rm = TRUE` for native execution."), expr)
        type <- if (fn == "mean") "Float64" else value$type
        return(ibex_expr_result(paste0(fn, "(", value$code, ")"), type, TRUE, TRUE, value$refs))
    }

    translated <- lapply(args, function(arg) ibex_translate_expr(arg, quo_env, x, context, state, inside_aggregate))
    codes <- vapply(translated, `[[`, character(1), "code")
    nullable <- any(vapply(translated, `[[`, logical(1), "nullable"))
    aggregate <- any(vapply(translated, `[[`, logical(1), "aggregate"))
    refs <- unlist(lapply(translated, `[[`, "refs"), use.names = FALSE)
    fn <- sub(".*::", "", identity)
    if (identity == "dplyr::between") {
        if (length(codes) != 3L || any(nzchar(arg_names))) ibex_unsupported("`between()` requires three unnamed arguments.", expr)
        return(ibex_expr_result(paste0("(", codes[[1]], " >= ", codes[[2]], " && ", codes[[1]], " <= ", codes[[3]], ")"), "Bool", nullable, aggregate, refs))
    }
    if (identity == "base::is.na") {
        if (length(codes) != 1L) ibex_unsupported("`is.na()` requires one argument.", expr)
        return(ibex_expr_result(paste0("is_null(", codes[[1]], ")"), "Bool", FALSE, aggregate, refs))
    }
    if (identity == "base::as.Date") {
        # Ibex's `Date()` cast truncates an instant to its UTC calendar day,
        # which is exactly `as.Date.POSIXct`'s own default (`tz = "UTC"`). Any
        # other `tz` would cut on different boundaries, so it is not translated;
        # neither is parsing a character vector, which Ibex has no `Date()`
        # overload for.
        if (length(codes) != 1L || any(nzchar(arg_names))) {
            ibex_unsupported("Only `as.Date(x)` with no further arguments translates natively; a `tz` or `format` changes the day boundaries Ibex cuts on.", expr)
        }
        if (!translated[[1]]$type %in% c("Timestamp", "Date")) {
            ibex_unsupported(paste0("`as.Date()` needs a Timestamp or Date column, not ", translated[[1]]$type, "."), expr)
        }
        return(ibex_expr_result(paste0("Date(", codes[[1]], ")"), "Date", nullable, aggregate, refs))
    }
    if (identity == "base::is.nan") {
        if (length(codes) != 1L) ibex_unsupported("`is.nan()` requires one argument.", expr)
        return(ibex_expr_result(paste0("is_nan(", codes[[1]], ")"), "Bool", nullable, aggregate, refs))
    }
    fn <- switch(fn, ceiling = "ceil", fn)
    arity <- switch(fn, abs = c(1L), log = c(1L), sqrt = c(1L), exp = c(1L), ceil = c(1L), floor = c(1L), trunc = c(1L), pmin = 2:1000, pmax = 2:1000, coalesce = 2:1000, integer())
    if (!length(arity) || !length(codes) %in% arity || any(nzchar(arg_names))) ibex_unsupported(paste0("Unsupported arguments to `", fn, "()`."), expr)
    out_nullable <- if (fn == "coalesce") all(vapply(translated, `[[`, logical(1), "nullable")) else nullable
    out_type <- if (fn %in% c("log", "sqrt", "exp")) "Float64" else translated[[1]]$type
    ibex_expr_result(paste0(fn, "(", paste(codes, collapse = ", "), ")"), out_type, out_nullable, aggregate, refs)
}

ibex_translate_quosure <- function(quo, x, context, state) {
    ibex_translate_expr(rlang::quo_get_expr(quo), rlang::quo_get_env(quo), x, context, state)
}

ibex_state <- function(x) {
    state <- new.env(parent = emptyenv())
    state$scalars <- x$captured_scalars
    state
}

ibex_duration <- function(duration, call = rlang::caller_env()) {
    if (!is.character(duration) || length(duration) != 1L || is.na(duration) ||
        !grepl("^[1-9][0-9]*(ns|us|ms|s|m|h|d|w)$", duration)) {
        rlang::abort("`duration` must be one Ibex duration such as \"5m\" or \"250ms\".", call = call)
    }
    duration
}

ibex_require_timeframe <- function(x, verb) {
    if (is.null(x$time_index)) {
        ibex_unsupported(paste0(verb, "() requires as_timeframe() first."))
    }
}

#' Mark an Ibex lazy table as time-indexed
#'
#' @param .data An [ibex_tbl()].
#' @param index A Timestamp, Date, or integer-nanosecond column.
#' @export
as_timeframe <- function(.data, index) UseMethod("as_timeframe")

#' @export
as_timeframe.ibex_tbl <- function(.data, index) {
    ibex_assert_live(.data)
    index_quo <- rlang::enquo(index)
    positions <- tidyselect::eval_select(index_quo, data = ibex_schema_proxy(.data))
    if (length(positions) != 1L || !identical(names(positions), .data$schema$names[unname(positions)])) {
        rlang::abort("`index` must select exactly one existing column.")
    }
    name <- .data$schema$names[[unname(positions)[[1]]]]
    type <- .data$schema$types[[match(name, .data$schema$names)]]
    if (!type %in% c("Timestamp", "Date", "Int64")) {
        rlang::abort("The TimeFrame index must be a Timestamp, Date, or Int64 column.")
    }
    if (.data$schema$nullable[[match(name, .data$schema$names)]]) {
        rlang::abort("The TimeFrame index must not contain missing values.")
    }
    schema <- .data$schema
    out <- ibex_append_step(
        .data, list(kind = "timeframe", index = name), schema = schema,
        ordering = data.frame(name = name, descending = FALSE)
    )
    out$time_index <- name
    out
}

#' Set the time window used by the next mutate or summarise
#'
#' @param x A TimeFrame created with [as_timeframe()].
#' @param duration An Ibex duration string such as `"5m"`.
#' @param aligned Reset windows on epoch-aligned duration boundaries.
#' @export
window <- function(x, ...) UseMethod("window")

#' @export
window.ibex_tbl <- function(x, duration, aligned = FALSE, ...) {
    ibex_assert_live(x)
    if (length(list(...))) rlang::abort("window() has no arguments beyond `duration` and `aligned`.")
    ibex_require_timeframe(x, "window")
    if (!is.null(x$pending_window)) rlang::abort("A window is already pending; follow it with mutate() or summarise().")
    if (!is.logical(aligned) || length(aligned) != 1L || is.na(aligned)) rlang::abort("`aligned` must be TRUE or FALSE.")
    x$pending_window <- list(duration = ibex_duration(duration), aligned = aligned)
    x
}

ibex_take_window <- function(x) x$pending_window

ibex_select_positions <- function(x, quos, strict = TRUE) {
    tidyselect::eval_select(rlang::expr(c(!!!quos)), data = ibex_schema_proxy(x), strict = strict)
}

# Resolve a `.by` argument to column names, or NULL when it was not supplied.
#
# `.by` is per-verb grouping: it names the keys for this one call and leaves the
# result ungrouped, where `group_by()` sets grouping that persists. Ibex has one
# grouping construct -- the `by` clause on the bracket a verb becomes -- so the
# two spellings translate identically and differ only in what the *frontend*
# records afterwards.
#
# The three rejections are dplyr's own, and they are raised rather than routed
# through the fallback: each is a mistake in the call, not a limit of the
# translation, so falling back would collect the whole input only for local
# dplyr to raise the same error against it.
ibex_resolve_by <- function(x, by) {
    if (rlang::quo_is_null(by)) {
        return(NULL)
    }
    if (length(x$groups)) {
        rlang::abort("Can't supply `.by` when `.data` is a grouped data frame.")
    }
    positions <- tidyselect::eval_select(by, data = ibex_schema_proxy(x))
    names <- x$schema$names[unname(positions)]
    # `eval_select` accepts `c(new = old)`; dplyr does not allow it here, and a
    # silent rename would put a name in the output that the plan never binds.
    if (!identical(names(positions), names)) {
        rlang::abort("Can't rename variables in this context.")
    }
    unique(names)
}

select.ibex_tbl <- function(.data, ...) {
    quos <- rlang::enquos(...)
    positions <- ibex_select_positions(.data, quos)
    selected <- names(positions)
    inputs <- .data$schema$names[unname(positions)]
    missing_groups <- setdiff(.data$groups, inputs)
    if (length(missing_groups)) {
        rlang::inform(paste0("Adding missing grouping variables: ", paste(missing_groups, collapse = ", ")))
        selected <- c(missing_groups, selected)
        inputs <- c(missing_groups, inputs)
    }
    source_pos <- match(inputs, .data$schema$names)
    schema <- lapply(.data$schema[c("names", "types", "categorical", "timezone")], function(v) v[source_pos])
    schema$names <- selected
    schema$rows <- .data$schema$rows
    fields <- Map(function(out, input) list(name = out, code = ibex_quote_identifier(input)), selected, inputs)
    output_name <- function(input) selected[[match(input, inputs)]]
    groups <- unname(vapply(.data$groups, output_name, character(1)))
    ordering <- if (!is.null(.data$ordering) && all(.data$ordering$name %in% inputs)) {
        ordering <- .data$ordering
        ordering$name <- vapply(ordering$name, output_name, character(1))
        ordering
    } else {
        NULL
    }
    out <- ibex_append_step(.data, list(kind = "project", fields = fields), schema = schema,
                            groups = groups, ordering = ordering)
    if (!is.null(.data$time_index)) {
        index_output <- output_name(.data$time_index)
        out$time_index <- if (.data$time_index %in% inputs) index_output else NULL
    }
    out
}

rename.ibex_tbl <- function(.data, ...) {
    quos <- rlang::enquos(...)
    positions <- tidyselect::eval_rename(rlang::expr(c(!!!quos)), data = ibex_schema_proxy(.data))
    if (!length(positions)) return(.data)
    new_names <- .data$schema$names
    new_names[unname(positions)] <- names(positions)
    mappings <- Map(function(new, pos) list(new = new, old = .data$schema$names[[pos]]), names(positions), unname(positions))
    rename_one <- function(values) {
        stats::setNames(vapply(values, function(value) {
            hit <- match(value, vapply(mappings, `[[`, character(1), "old"))
            if (is.na(hit)) value else mappings[[hit]]$new
        }, character(1)), NULL)
    }
    schema <- .data$schema
    schema$names <- new_names
    groups <- rename_one(.data$groups)
    ordering <- .data$ordering
    if (!is.null(ordering)) ordering$name <- rename_one(ordering$name)
    out <- ibex_append_step(.data, list(kind = "rename", mappings = mappings), schema, groups, ordering)
    if (!is.null(.data$time_index)) out$time_index <- rename_one(.data$time_index)[[1]]
    out
}

relocate.ibex_tbl <- function(.data, ..., .before = NULL, .after = NULL) {
    quos <- rlang::enquos(...)
    moved <- unname(ibex_select_positions(.data, quos))
    if (!length(moved)) return(.data)
    remaining <- setdiff(seq_along(.data$schema$names), moved)
    before_quo <- rlang::enquo(.before)
    after_quo <- rlang::enquo(.after)
    if (!rlang::quo_is_null(before_quo) && !rlang::quo_is_null(after_quo)) rlang::abort("Only one of `.before` and `.after` may be supplied.")
    insert_at <- 1L
    if (!rlang::quo_is_null(after_quo)) {
        anchor <- tidyselect::eval_select(after_quo, ibex_schema_proxy(.data))
        insert_at <- match(tail(unname(anchor), 1L), remaining) + 1L
        if (is.na(insert_at)) insert_at <- length(remaining) + 1L
    } else if (!rlang::quo_is_null(before_quo)) {
        anchor <- tidyselect::eval_select(before_quo, ibex_schema_proxy(.data))
        insert_at <- match(head(unname(anchor), 1L), remaining)
        if (is.na(insert_at)) insert_at <- 1L
    }
    order <- append(remaining, moved, after = insert_at - 1L)
    fields <- lapply(.data$schema$names[order], function(name) list(name = name, code = ibex_quote_identifier(name)))
    schema <- lapply(.data$schema[c("names", "types", "categorical", "timezone")], function(v) v[order])
    schema$rows <- .data$schema$rows
    ibex_append_step(.data, list(kind = "project", fields = fields), schema = schema)
}

filter.ibex_tbl <- function(.data, ..., .by = NULL, .preserve = FALSE) {
    dots <- rlang::enquos(...)
    by <- rlang::enquo(.by)
    replay <- function(local) dplyr::filter(local, !!!dots, .by = !!by, .preserve = .preserve)
    # `.by` is resolved for its checks, then discarded: a predicate with no
    # aggregate in it asks the same question of a row whatever group the row is
    # in, so grouping cannot change which rows survive. The aggregate case is
    # the one where `.by` would matter, and native filter refuses it below.
    ibex_resolve_by(.data, by)
    ibex_with_fallback(.data, "filter", function() {
        if (isTRUE(.preserve)) ibex_unsupported("`.preserve = TRUE` is not a native filter option.")
        state <- ibex_state(.data)
        translated <- lapply(dots, ibex_translate_quosure, x = .data, context = "filter", state = state)
        if (!length(translated)) return(.data)
        if (any(vapply(translated, `[[`, logical(1), "aggregate"))) ibex_unsupported("Aggregate expressions are not valid in native filter().")
        schema <- .data$schema
        schema$rows <- NA_real_
        ibex_append_step(.data, list(kind = "filter", predicates = vapply(translated, `[[`, character(1), "code")), schema = schema, scalars = state$scalars)
    }, replay)
}

ibex_mutate_impl <- function(.data, dots, keep = "all", before = NULL, after = NULL,
                             verb = "mutate", replay = NULL, by = NULL) {
    if (is.null(replay)) {
        replay <- function(local) dplyr::mutate(local, !!!dots, .keep = keep, .before = !!before, .after = !!after)
    }
    # `.by` seeds the working copy's grouping, not `.data`'s: the table handed
    # to the fallback must stay ungrouped, or `collect()` would hand local dplyr
    # a grouped frame and `.by` would be an error there rather than a replay.
    ibex_with_fallback(.data, verb, function() {
        before_supplied <- !is.null(before) && !rlang::quo_is_null(before)
        after_supplied <- !is.null(after) && !rlang::quo_is_null(after)
        if (!identical(keep, "all") || before_supplied || after_supplied) ibex_unsupported("Native mutate() currently requires `.keep = \"all\"` and appends new columns.")
        out <- .data
        if (!is.null(by)) out$groups <- by
        for (i in seq_along(dots)) {
            name <- names(dots)[[i]]
            if (!nzchar(name)) ibex_unsupported("Every native mutate expression must be named.", rlang::quo_get_expr(dots[[i]]))
            state <- ibex_state(out)
            translated <- ibex_translate_quosure(dots[[i]], out, "update", state)
            if (length(out$groups) && translated$aggregate && !all(translated$refs %in% out$groups)) {
                ibex_unsupported("Grouped mutate expressions may only reference grouping keys outside aggregate calls.", rlang::quo_get_expr(dots[[i]]))
            }
            step_groups <- if (length(out$groups) && (translated$aggregate || translated$rank)) out$groups else character()
            schema <- out$schema
            pos <- match(name, schema$names)
            if (is.na(pos)) {
                schema$names <- c(schema$names, name)
                schema$types <- c(schema$types, translated$type)
                schema$categorical <- c(schema$categorical, FALSE)
                schema$timezone <- c(schema$timezone, NA_character_)
            } else {
                schema$types[[pos]] <- translated$type
                schema$categorical[[pos]] <- FALSE
                schema$timezone[[pos]] <- NA_character_
            }
            ordering <- out$ordering
            if (!is.null(ordering) && name %in% ordering$name) ordering <- NULL
            out <- ibex_append_step(out, list(kind = "update", fields = list(list(name = name, code = translated$code)), groups = step_groups, window = ibex_take_window(out)), schema, ordering = ordering, scalars = state$scalars)
        }
        out$pending_window <- NULL
        # Grouping supplied per-call does not outlive the call.
        if (!is.null(by)) out$groups <- character()
        out
    }, replay)
}

mutate.ibex_tbl <- function(.data, ..., .by = NULL, .keep = c("all", "used", "unused", "none"), .before = NULL, .after = NULL) {
    dots <- rlang::enquos(...)
    by <- rlang::enquo(.by)
    before <- rlang::enquo(.before)
    after <- rlang::enquo(.after)
    keep <- match.arg(.keep)
    by_cols <- ibex_resolve_by(.data, by)
    ibex_mutate_impl(
        .data, dots, keep, before, after, by = by_cols,
        replay = function(local) dplyr::mutate(local, !!!dots, .by = !!by, .keep = keep, .before = !!before, .after = !!after)
    )
}

transmute.ibex_tbl <- function(.data, ...) {
    dots <- rlang::enquos(...)
    out <- ibex_mutate_impl(
        .data,
        dots,
        verb = "transmute",
        replay = function(local) dplyr::transmute(local, !!!dots)
    )
    if (!inherits(out, "ibex_tbl")) return(out)
    keep <- unique(c(out$groups, names(dots)))
    rlang::inject(select(out, !!!rlang::syms(keep)))
}

group_by.ibex_tbl <- function(.data, ..., .add = FALSE, .drop = TRUE) {
    ibex_assert_live(.data)
    dots <- rlang::enquos(...)
    replay <- function(local) dplyr::group_by(local, !!!dots, .add = .add, .drop = .drop)
    ibex_with_fallback(.data, "group_by", function() {
        if (!isTRUE(.drop)) ibex_unsupported("`.drop = FALSE` cannot be represented by native grouping.")
        names <- vapply(dots, function(quo) {
            expr <- rlang::quo_get_expr(quo)
            if (!is.symbol(expr)) ibex_unsupported("Native group_by() currently accepts existing columns only.", expr)
            as.character(expr)
        }, character(1))
        if (any(!names %in% .data$schema$names)) ibex_unsupported("group_by() references an unknown column.")
        .data$groups <- if (isTRUE(.add)) unique(c(.data$groups, names)) else unique(names)
        .data
    }, replay)
}

group_vars.ibex_tbl <- function(x) x$groups

ungroup.ibex_tbl <- function(x, ...) {
    ibex_assert_live(x)
    dots <- rlang::enquos(...)
    if (!length(dots)) {
        x$groups <- character()
        return(x)
    }
    remove <- vapply(dots, function(quo) {
        expr <- rlang::quo_get_expr(quo)
        if (!is.symbol(expr)) rlang::abort("ungroup() arguments must be grouping column names.")
        as.character(expr)
    }, character(1))
    x$groups <- setdiff(x$groups, remove)
    x
}

summarise.ibex_tbl <- function(.data, ..., .by = NULL, .groups = NULL) {
    dots <- rlang::enquos(...)
    by <- rlang::enquo(.by)
    replay <- function(local) dplyr::summarise(local, !!!dots, .by = !!by, .groups = .groups)
    ibex_with_fallback(.data, "summarise", function() {
        by_cols <- ibex_resolve_by(.data, by)
        if (!is.null(by_cols) && !is.null(.groups)) rlang::abort("Can't supply both `.by` and `.groups`.")
        # `.by` groups this call and nothing after it, so the result is always
        # ungrouped -- there is no `.groups` to honour and no last key to drop.
        keys <- by_cols %||% .data$groups
        groups_mode <- if (!is.null(by_cols)) "drop" else .groups %||% if (length(.data$groups)) "drop_last" else "drop"
        if (!groups_mode %in% c("drop", "drop_last", "keep")) ibex_unsupported("Unsupported `.groups` value.")
        state <- ibex_state(.data)
        translated <- Map(function(quo, name) {
            if (!nzchar(name)) ibex_unsupported("Every native summarise expression must be named.", rlang::quo_get_expr(quo))
            value <- ibex_translate_quosure(quo, .data, "aggregate", state)
            if (!value$aggregate) ibex_unsupported("Native summarise expressions must contain an aggregate.", rlang::quo_get_expr(quo))
            if (!all(value$refs %in% keys)) ibex_unsupported("summarise() references non-group columns outside aggregate calls.", rlang::quo_get_expr(quo))
            list(name = name, code = value$code, type = value$type)
        }, dots, names(dots))
        group_pos <- match(keys, .data$schema$names)
        group_fields <- lapply(keys, function(name) list(name = name, code = ibex_quote_identifier(name)))
        fields <- c(group_fields, translated)
        schema <- list(
            names = c(keys, names(dots)),
            types = c(.data$schema$types[group_pos], vapply(translated, `[[`, character(1), "type")),
            categorical = c(.data$schema$categorical[group_pos], rep(FALSE, length(translated))),
            timezone = c(.data$schema$timezone[group_pos], rep(NA_character_, length(translated))),
            rows = NA_real_
        )
        groups <- switch(groups_mode, drop = character(), drop_last = head(keys, -1L), keep = keys)
        out <- ibex_append_step(.data, list(kind = "aggregate", fields = fields, groups = keys,
                                             window = ibex_take_window(.data)), schema, groups, NULL, state$scalars)
        out$pending_window <- NULL
        out
    }, replay)
}

#' Aggregate a TimeFrame into fixed time bars
#'
#' @param .data A TimeFrame created with [as_timeframe()].
#' @param duration An Ibex duration string such as `"1m"`.
#' @param ... Named aggregate expressions, for example `close = last(price)`.
#' @param .by Optional grouping columns for separate bars per group.
#' @param aligned Accepted for symmetry with [window()]; resample buckets are
#'   always epoch-aligned.
#' @export
resample <- function(.data, ...) UseMethod("resample")

#' @export
resample.ibex_tbl <- function(.data, duration, ..., .by = NULL, aligned = FALSE) {
    dots <- rlang::enquos(...)
    by <- rlang::enquo(.by)
    ibex_with_fallback(.data, "resample", function() {
        ibex_require_timeframe(.data, "resample")
        if (!is.logical(aligned) || length(aligned) != 1L || is.na(aligned)) rlang::abort("`aligned` must be TRUE or FALSE.")
        keys <- ibex_resolve_by(.data, by) %||% .data$groups
        state <- ibex_state(.data)
        translated <- Map(function(quo, name) {
            if (!nzchar(name)) ibex_unsupported("Every resample expression must be named.", rlang::quo_get_expr(quo))
            value <- ibex_translate_quosure(quo, .data, "aggregate", state)
            if (!value$aggregate) ibex_unsupported("resample() expressions must contain an aggregate.", rlang::quo_get_expr(quo))
            if (!all(value$refs %in% keys)) ibex_unsupported("resample() references non-group columns outside aggregate calls.", rlang::quo_get_expr(quo))
            list(name = name, code = value$code, type = value$type)
        }, dots, names(dots))
        index_pos <- match(.data$time_index, .data$schema$names)
        group_pos <- match(keys, .data$schema$names)
        # The runtime always retains the index as the bar label, even when it
        # was not named in select; include it in the frontend schema too.
        output_keys <- setdiff(keys, .data$time_index)
        output_key_pos <- match(output_keys, .data$schema$names)
        fields <- c(
            lapply(output_keys, function(name) list(name = name, code = ibex_quote_identifier(name))),
            translated
        )
        schema <- list(
            names = c(.data$time_index, output_keys, names(dots)),
            types = c(.data$schema$types[[index_pos]], .data$schema$types[output_key_pos], vapply(translated, `[[`, character(1), "type")),
            categorical = c(.data$schema$categorical[[index_pos]], .data$schema$categorical[output_key_pos], rep(FALSE, length(translated))),
            timezone = c(.data$schema$timezone[[index_pos]], .data$schema$timezone[output_key_pos], rep(NA_character_, length(translated))),
            rows = NA_real_
        )
        out <- ibex_append_step(.data, list(kind = "resample", duration = ibex_duration(duration),
                                             aligned = aligned, fields = fields, groups = keys), schema,
                                groups = character(),
                                ordering = data.frame(name = .data$time_index, descending = FALSE),
                                scalars = state$scalars)
        out$pending_window <- NULL
        out
    }, function(local) {
        ibex_unsupported("resample() cannot be replayed with local dplyr; use fallback = \"error\".")
    })
}

arrange.ibex_tbl <- function(.data, ..., .by_group = FALSE) {
    dots <- rlang::enquos(...)
    replay <- function(local) dplyr::arrange(local, !!!dots, .by_group = .by_group)
    ibex_with_fallback(.data, "arrange", function() {
        parsed <- lapply(dots, function(quo) {
            expr <- rlang::quo_get_expr(quo)
            descending <- FALSE
            is_desc <- is.call(expr) && length(expr) == 2L && (
                identical(expr[[1]], as.name("desc")) ||
                    identical(ibex_call_identity(expr[[1]], rlang::quo_get_env(quo)), "dplyr::desc")
            )
            if (is_desc) {
                if (identical(expr[[1]], as.name("desc"))) {
                    resolved <- tryCatch(rlang::eval_bare(expr[[1]], rlang::quo_get_env(quo)), error = function(e) NULL)
                    if (!identical(resolved, dplyr::desc)) ibex_unsupported("Masked desc() is not translated.", expr)
                }
                expr <- expr[[2]]
                descending <- TRUE
            }
            if (!is.symbol(expr)) ibex_unsupported("Native arrange() accepts column names and desc(column).", expr)
            name <- as.character(expr)
            if (!name %in% .data$schema$names) ibex_unsupported("arrange() references an unknown column.", expr)
            list(name = name, descending = descending)
        })
        if (isTRUE(.by_group)) {
            parsed <- c(lapply(.data$groups, function(name) list(name = name, descending = FALSE)), parsed)
        }
        if (!length(parsed)) return(.data)
        names <- vapply(parsed, `[[`, character(1), "name")
        descending <- vapply(parsed, `[[`, logical(1), "descending")
        ordering <- data.frame(name = names, descending = descending)
        ibex_append_step(.data, list(kind = "order", names = names, descending = descending), ordering = ordering)
    }, replay)
}

slice_head.ibex_tbl <- function(.data, ..., n, prop, by = NULL) {
    dots <- list(...)
    n_missing <- missing(n)
    prop_missing <- missing(prop)
    by_quo <- rlang::enquo(by)
    replay <- function(local) {
        if (!rlang::quo_is_null(by_quo)) {
            if (!n_missing) return(dplyr::slice_head(local, !!!dots, n = n, by = !!by_quo))
            if (!prop_missing) return(dplyr::slice_head(local, !!!dots, prop = prop, by = !!by_quo))
            return(dplyr::slice_head(local, !!!dots, by = !!by_quo))
        }
        if (!n_missing) return(dplyr::slice_head(local, !!!dots, n = n))
        if (!prop_missing) return(dplyr::slice_head(local, !!!dots, prop = prop))
        dplyr::slice_head(local, !!!dots)
    }
    ibex_with_fallback(.data, "slice_head", function() {
        if (length(dots) || !prop_missing || !rlang::quo_is_null(by_quo)) ibex_unsupported("Native slice_head() supports only a constant `n` and existing grouping.")
        count <- if (n_missing) 1L else n
        if (!is.numeric(count) || length(count) != 1L || is.na(count) || count < 0 || count != as.integer(count)) ibex_unsupported("`n` must be one non-negative integer.")
        schema <- .data$schema
        if (!is.na(schema$rows) && !length(.data$groups)) schema$rows <- min(schema$rows, count) else schema$rows <- NA_real_
        ibex_append_step(.data, list(kind = "head", n = as.integer(count), groups = .data$groups), schema = schema)
    }, replay)
}

head.ibex_tbl <- function(x, n = 6L, ...) slice_head(x, n = n)

slice_tail.ibex_tbl <- function(.data, ..., n, prop, by = NULL) {
    dots <- list(...)
    n_missing <- missing(n)
    prop_missing <- missing(prop)
    by_quo <- rlang::enquo(by)
    condition <- structure(
        list(message = "slice_tail() is not in the native MVP.", expression = "slice_tail()"),
        class = c("ibex_unsupported", "error", "condition")
    )
    ibex_fallback(.data, "slice_tail", condition, function(local) {
        if (!rlang::quo_is_null(by_quo)) {
            if (!n_missing) return(dplyr::slice_tail(local, !!!dots, n = n, by = !!by_quo))
            if (!prop_missing) return(dplyr::slice_tail(local, !!!dots, prop = prop, by = !!by_quo))
            return(dplyr::slice_tail(local, !!!dots, by = !!by_quo))
        }
        if (!n_missing) return(dplyr::slice_tail(local, !!!dots, n = n))
        if (!prop_missing) return(dplyr::slice_tail(local, !!!dots, prop = prop))
        dplyr::slice_tail(local, !!!dots)
    })
}

distinct.ibex_tbl <- function(.data, ..., .keep_all = FALSE) {
    dots <- rlang::enquos(...)
    replay <- function(local) dplyr::distinct(local, !!!dots, .keep_all = .keep_all)
    ibex_with_fallback(.data, "distinct", function() {
        positions <- if (length(dots)) ibex_select_positions(.data, dots) else stats::setNames(seq_along(.data$schema$names), .data$schema$names)
        if (isTRUE(.keep_all) && length(positions) != length(.data$schema$names)) ibex_unsupported("`.keep_all = TRUE` with a subset has no native stable-row equivalent yet.")
        selected <- if (isTRUE(.keep_all)) .data$schema$names else names(positions)
        inputs <- if (isTRUE(.keep_all)) selected else .data$schema$names[unname(positions)]
        if (!identical(selected, inputs)) ibex_unsupported("Computed or renamed distinct columns are not supported natively.")
        pos <- match(selected, .data$schema$names)
        schema <- lapply(.data$schema[c("names", "types", "categorical", "timezone")], function(v) v[pos])
        schema$rows <- NA_real_
        groups <- intersect(.data$groups, selected)
        ibex_append_step(.data, list(kind = "distinct", names = selected), schema, groups, NULL)
    }, replay)
}

tally.ibex_tbl <- function(x, wt = NULL, sort = FALSE, name = NULL) {
    wt <- rlang::enquo(wt)
    output <- name %||% "n"
    if (output %in% x$schema$names) {
        while (output %in% x$schema$names) output <- paste0(output, "n")
    }
    aggregate <- if (rlang::quo_is_null(wt)) {
        rlang::new_quosure(rlang::expr(dplyr::n()), rlang::caller_env())
    } else {
        rlang::new_quosure(rlang::expr(base::sum(!!rlang::quo_get_expr(wt), na.rm = TRUE)), rlang::quo_get_env(wt))
    }
    dots <- stats::setNames(list(aggregate), output)
    # tally() follows summarise()'s default grouping contract: each tally
    # consumes the last grouping key while retaining any preceding keys.
    result <- rlang::inject(summarise(x, !!!dots, .groups = "drop_last"))
    if (inherits(result, "ibex_tbl") && isTRUE(sort)) {
        order_quo <- rlang::new_quosure(rlang::expr(dplyr::desc(!!as.name(output))), rlang::caller_env())
        result <- rlang::inject(arrange(result, !!order_quo))
    } else if (!inherits(result, "ibex_tbl") && isTRUE(sort)) {
        result <- dplyr::arrange(result, dplyr::desc(.data[[output]]))
    }
    result
}

count.ibex_tbl <- function(x, ..., wt = NULL, sort = FALSE, name = NULL, .drop = TRUE) {
    dots <- rlang::enquos(...)
    original_groups <- x$groups
    grouped <- rlang::inject(group_by(x, !!!dots, .add = TRUE, .drop = .drop))
    result <- tally(grouped, wt = {{ wt }}, sort = sort, name = name)
    if (inherits(result, "ibex_tbl")) result$groups <- original_groups
    result
}

ibex_join_fallback <- function(x, y, verb, by, copy, suffix, ..., keep = NULL,
                               na_matches = c("na", "never"), multiple = "all",
                               unmatched = "drop", relationship = NULL) {
    na_matches <- match.arg(na_matches)
    condition <- structure(
        list(message = paste0(verb, "() is not in the native MVP."), expression = paste0(verb, "()")),
        class = c("ibex_unsupported", "error", "condition")
    )
    ibex_fallback(x, verb, condition, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        join <- getExportedValue("dplyr", verb)
        join(local, right, by = by, copy = copy, suffix = suffix, ..., keep = keep,
             na_matches = na_matches, multiple = multiple,
             unmatched = unmatched, relationship = relationship)
    })
}

# Ibex suffixes both sides of a collision and leaves everything else alone,
# which is dplyr's rule too. `suffix` is NULL when the caller has not reached
# the point of supplying one; then no name changes.
ibex_join_output_names <- function(x, y, keys, suffix) {
    right <- setdiff(y$schema$names, keys)
    left <- x$schema$names
    overlap <- intersect(setdiff(left, keys), right)
    if (!length(overlap) || length(suffix) != 2L) {
        return(list(left = left, right = right))
    }
    list(
        left = ifelse(left %in% overlap, paste0(left, suffix[[1]]), left),
        right = ifelse(right %in% overlap, paste0(right, suffix[[2]]), right)
    )
}

# dplyr's verb names and Ibex's operators agree on all but one; dplyr's `full`
# is Ibex's `outer`. A lookup rather than a chain of `if`s so an unmapped kind
# stops here instead of quietly rendering an inner join.
ibex_join_operator <- function(kind) {
    c(inner = " join ", left = " left join ",
      right = " right join ", full = " outer join ",
      semi = " semi join ", anti = " anti join ",
      cross = " cross join ")[[kind]]
}

# `semi join` and `anti join` return the left columns only. That one fact is
# what makes them the simplest kinds to translate -- no right columns means no
# collision, so no suffix; the output schema is the input's; and the grouping
# survives, because every column it names is still there under its own name.
ibex_join_is_filtering <- function(kind) kind %in% c("semi", "anti")

# dplyr's `na_matches` and Ibex's `nulls` clause ask the same question of
# nulls, so the translation is a word swap -- with one exception, which is why
# this is a function rather than a lookup.
#
# `na_matches = "na"` also makes NaN match NaN, and Ibex's `nulls equal` does
# not: its subject is the validity bitmap, and a NaN is a value that is
# present. R keeps the two apart as well (NaN does not match NA under either
# option), so there is no rewrite that recovers dplyr's answer -- mapping NaN
# to null would wrongly pair it with NA. A float key therefore falls back
# rather than returning a differently-shaped result, which is a divergence a
# caller would have no way to notice.
#
# `"never"` needs no such care: neither side matches a NaN under it.
ibex_join_null_match <- function(x, y, left_keys, right_keys, na_matches) {
    if (identical(na_matches, "never")) {
        return("never")
    }
    key_type <- function(tbl, keys) tbl$schema$types[match(keys, tbl$schema$names)]
    if (any(c(key_type(x, left_keys), key_type(y, right_keys)) == "Float64")) {
        ibex_unsupported(paste(
            "Native joins cannot match NaN keys the way `na_matches = \"na\"` does;",
            "a floating-point key needs `na_matches = \"never\"` or local dplyr."
        ))
    }
    "equal"
}

# Every kind emits the left input's columns first and folds a same-named key
# into one column, so the output names do not depend on the kind. Which of
# them can hold a null does, and that answer comes back from Ibex itself --
# see `ibex_infer_nullable()`.
#
# `keys` here is the folded set only. A mapped pair (`on { a = b }`) folds
# nothing and keeps both columns, so `b` is an ordinary right column as far as
# these names are concerned; dropping it to match dplyr is a later step.
ibex_join_schema <- function(x, y, keys, suffix) {
    right <- setdiff(y$schema$names, keys)
    names <- ibex_join_output_names(x, y, keys, suffix)
    right_schema <- lapply(y$schema[c("names", "types", "categorical", "timezone")], function(v) v[match(right, y$schema$names)])
    right_schema$names <- names$right
    schema <- lapply(x$schema[c("names", "types", "categorical", "timezone")], identity)
    schema$names <- names$left
    for (field in names(right_schema)) schema[[field]] <- c(schema[[field]], right_schema[[field]])
    schema$rows <- NA_real_
    schema
}

ibex_native_join <- function(x, y, kind, by, copy, suffix, ..., keep,
                             na_matches, multiple, unmatched, relationship) {
    dots <- rlang::list2(...)
    if (length(dots) || isTRUE(copy) || !(is.null(keep) || identical(keep, FALSE)) ||
        !identical(multiple, "all") || !identical(unmatched, "drop") ||
        !is.null(relationship)) {
        ibex_unsupported("Native joins currently require simple equality keys and default join options.")
    }
    if (!inherits(y, "ibex_tbl")) y <- ibex_tbl(y, session = x$session, fallback = x$fallback_policy)
    if (!identical(x$session, y$session)) ibex_unsupported("Native joins require both inputs to use the same Ibex session.")
    # A join renders the right input's plan inside the left one, so the result
    # is evaluated with a single scalar environment and needs both sides'
    # captures in it. They can be merged whenever their names are distinct.
    #
    # Names are handed out per table, counting from one, so two sides that both
    # captured something claim the same names for different values. Renaming
    # one side means rewriting the references inside its rendered plan, which
    # is the part not done yet -- so that case, and only that case, declines.
    if (length(intersect(names(x$captured_scalars), names(y$captured_scalars)))) {
        ibex_unsupported("Native joins cannot yet merge captured scalars from both inputs.")
    }
    join_scalars <- c(x$captured_scalars, y$captured_scalars)
    # A cross join pairs every row with every row, so it has no keys to derive
    # and `cross_join()` has no `by` argument to derive them from. Every other
    # kind needs at least one key, which is what the block below insists on.
    cross <- identical(kind, "cross")
    left_keys <- character()
    right_keys <- character()
    if (!cross) {
        if (is.null(by)) by <- intersect(x$schema$names, y$schema$names)
        if (!is.character(by) || !length(by)) {
            # `join_by()` and its non-equality conditions land here too.
            ibex_unsupported("Native joins currently require character `by` keys.")
        }
        # dplyr writes a mapped pair as `c(left = "right")` and an unmapped one
        # as a bare string, so an entry's name is its left column when it has
        # one. `names()` is NULL when nothing is mapped and "" per unmapped
        # entry otherwise, which is why both forms need handling.
        right_keys <- unname(by)
        left_keys <- if (is.null(names(by))) {
            right_keys
        } else {
            ifelse(nzchar(names(by)), names(by), right_keys)
        }
        if (any(!nzchar(left_keys)) || any(!nzchar(right_keys)) ||
            any(!left_keys %in% x$schema$names) || any(!right_keys %in% y$schema$names)) {
            ibex_unsupported("Native joins require every key column to be present in its own input.")
        }
        if (anyDuplicated(left_keys) || anyDuplicated(right_keys)) {
            ibex_unsupported("Native joins require each key column to be named once per side.")
        }
    }
    # A same-name pair folds into one output column; a mapped pair does not,
    # and Ibex keeps both of its columns. dplyr keeps only the left one, so a
    # mapped join needs a projection afterwards -- see below.
    mapped <- left_keys != right_keys
    folded <- left_keys[!mapped]
    filtering <- ibex_join_is_filtering(kind)
    if (any(mapped) && !filtering) {
        # Two shapes where Ibex's suffixing and dplyr's differ, rather than
        # just its column count. Ibex sees a mapped key as an ordinary column
        # and so as a collision; dplyr, which drops the right key outright,
        # sees nothing to resolve and leaves both original names alone. The
        # suffix would land on a column the caller never asked about, so these
        # fall back rather than renaming something dplyr does not.
        if (any(right_keys[mapped] %in% x$schema$names) ||
            any(left_keys[mapped] %in% y$schema$names)) {
            ibex_unsupported(paste(
                "A mapped key whose name also exists in the other input would be",
                "suffixed by Ibex but not by dplyr; this join needs local dplyr."
            ))
        }
    }
    overlap <- if (filtering) character() else {
        intersect(setdiff(x$schema$names, folded), setdiff(y$schema$names, folded))
    }
    if (length(overlap) && (length(suffix) != 2L || any(!nzchar(suffix)))) {
        ibex_unsupported("Native joins require two non-empty suffixes.")
    }
    # Ibex rejects a `nulls` clause on a join with no equality keys, rather
    # than accepting one that could not do anything. Nothing is lost: with no
    # key to compare, there is no null-matching question to answer.
    null_match <- if (cross) NULL else {
        ibex_join_null_match(x, y, left_keys, right_keys, na_matches)
    }
    # Only emit the clause when something actually collides: Ibex accepts it
    # either way, but a clause with nothing to rename is noise in the plan.
    step_suffix <- if (length(overlap)) suffix else NULL
    schema <- if (filtering) {
        # The left columns, unchanged. Only the row count is unknown, and
        # nullability is re-derived from the plan like every other verb's.
        filtered <- x$schema
        filtered$rows <- NA_real_
        filtered
    } else {
        ibex_join_schema(x, y, folded, step_suffix)
    }
    joined <- ibex_append_step(
        x,
        list(kind = "join", join_kind = kind, right = y, keys = left_keys,
             right_keys = right_keys, suffix = step_suffix, null_match = null_match),
        # A filtering join drops rows and touches nothing else, so it keeps the
        # grouping it was handed -- the columns naming it all survive. The
        # other kinds cannot promise that, since a group column may be
        # suffixed or gain nulls.
        schema = schema, groups = if (filtering) x$groups else character(),
        ordering = NULL, scalars = join_scalars
    )
    if (!any(mapped) || filtering) {
        return(joined)
    }
    # dplyr merges a mapped pair into one column under the left name; Ibex
    # keeps both, deliberately, because for an unmatched row they differ. A
    # projection reconciles the two. The gate above guarantees these names are
    # unsuffixed, so they can be matched literally.
    ibex_join_merge_mapped_keys(joined, left_keys[mapped], right_keys[mapped],
                                merge = kind %in% c("right", "full"))
}

# The projection that turns Ibex's two key columns back into dplyr's one.
#
# `merge` is the whole subtlety. Where every output row has a left side --
# inner and left joins -- the left key is the answer and the right one is
# redundant, so dropping it loses nothing. A right or full join also emits
# rows with no left side, and there the left key is null while the right key
# holds the value dplyr reports. So the merged column is `coalesce(left,
# right)`, which is precisely what the core does for itself when a same-name
# key folds; a mapped pair asks the frontend to do it instead.
ibex_join_merge_mapped_keys <- function(x, left_keys, right_keys, merge) {
    keep <- setdiff(x$schema$names, right_keys)
    positions <- match(keep, x$schema$names)
    schema <- lapply(x$schema[c("names", "types", "categorical", "timezone")],
                     function(values) values[positions])
    schema$names <- keep
    schema$rows <- x$schema$rows
    fields <- lapply(keep, function(name) {
        peer <- if (merge) right_keys[match(name, left_keys)] else NA_character_
        code <- if (!is.na(peer)) {
            paste0("coalesce(", ibex_quote_identifier(name), ", ",
                   ibex_quote_identifier(peer), ")")
        } else {
            ibex_quote_identifier(name)
        }
        list(name = name, code = code)
    })
    ibex_append_step(x, list(kind = "project", fields = fields), schema = schema,
                     groups = x$groups, ordering = x$ordering)
}

inner_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...,
                                keep = NULL, na_matches = c("na", "never"), multiple = "all",
                                unmatched = "drop", relationship = NULL) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "inner_join", function() {
        ibex_native_join(x, y, "inner", by, copy, suffix, ..., keep = keep,
                         na_matches = na_matches, multiple = multiple,
                         unmatched = unmatched, relationship = relationship)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::inner_join(local, right, by = by, copy = copy, suffix = suffix, ...,
                          keep = keep, na_matches = na_matches, multiple = multiple,
                          unmatched = unmatched, relationship = relationship)
    })
}

left_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...,
                               keep = NULL, na_matches = c("na", "never"), multiple = "all",
                               unmatched = "drop", relationship = NULL) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "left_join", function() {
        ibex_native_join(x, y, "left", by, copy, suffix, ..., keep = keep,
                         na_matches = na_matches, multiple = multiple,
                         unmatched = unmatched, relationship = relationship)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::left_join(local, right, by = by, copy = copy, suffix = suffix, ...,
                         keep = keep, na_matches = na_matches, multiple = multiple,
                         unmatched = unmatched, relationship = relationship)
    })
}

right_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...,
                                keep = NULL, na_matches = c("na", "never"), multiple = "all",
                                unmatched = "drop", relationship = NULL) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "right_join", function() {
        ibex_native_join(x, y, "right", by, copy, suffix, ..., keep = keep,
                         na_matches = na_matches, multiple = multiple,
                         unmatched = unmatched, relationship = relationship)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::right_join(local, right, by = by, copy = copy, suffix = suffix, ...,
                          keep = keep, na_matches = na_matches, multiple = multiple,
                          unmatched = unmatched, relationship = relationship)
    })
}

# `full_join()` takes no `unmatched`, which is dplyr's signature rather than an
# omission: there is no side whose rows it could drop. `ibex_native_join()`
# still gates on the argument, so pass the value that means "nothing dropped".
full_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ...,
                               keep = NULL, na_matches = c("na", "never"), multiple = "all",
                               relationship = NULL) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "full_join", function() {
        ibex_native_join(x, y, "full", by, copy, suffix, ..., keep = keep,
                         na_matches = na_matches, multiple = multiple,
                         unmatched = "drop", relationship = relationship)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::full_join(local, right, by = by, copy = copy, suffix = suffix, ...,
                         keep = keep, na_matches = na_matches, multiple = multiple,
                         relationship = relationship)
    })
}

# `cross_join()` takes neither `by` nor `na_matches`: with no keys there is
# nothing to match on and so no null-matching question. It keeps `suffix`,
# though, and needs it more than the others do -- a same-named column is a
# plain collision here, since no key folds the two into one.
cross_join.ibex_tbl <- function(x, y, ..., copy = FALSE, suffix = c(".x", ".y")) {
    ibex_with_fallback(x, "cross_join", function() {
        ibex_native_join(x, y, "cross", by = NULL, copy, suffix, ..., keep = NULL,
                         na_matches = "never", multiple = "all",
                         unmatched = "drop", relationship = NULL)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::cross_join(local, right, ..., copy = copy, suffix = suffix)
    })
}

# The filtering joins take a smaller argument list than the mutating ones --
# no `suffix`, `keep`, `multiple`, `unmatched` or `relationship`, because a
# join that adds no columns and duplicates no rows has nothing for them to
# say. `ibex_native_join()` still gates on all of them, so pass the values
# that mean "default" rather than widening the signature past dplyr's.
semi_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, ...,
                               na_matches = c("na", "never")) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "semi_join", function() {
        ibex_native_join(x, y, "semi", by, copy, suffix = NULL, ..., keep = NULL,
                         na_matches = na_matches, multiple = "all",
                         unmatched = "drop", relationship = NULL)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::semi_join(local, right, by = by, copy = copy, ..., na_matches = na_matches)
    })
}

anti_join.ibex_tbl <- function(x, y, by = NULL, copy = FALSE, ...,
                               na_matches = c("na", "never")) {
    na_matches <- match.arg(na_matches)
    ibex_with_fallback(x, "anti_join", function() {
        ibex_native_join(x, y, "anti", by, copy, suffix = NULL, ..., keep = NULL,
                         na_matches = na_matches, multiple = "all",
                         unmatched = "drop", relationship = NULL)
    }, function(local) {
        right <- if (inherits(y, "ibex_tbl")) collect(y) else y
        dplyr::anti_join(local, right, by = by, copy = copy, ..., na_matches = na_matches)
    })
}
