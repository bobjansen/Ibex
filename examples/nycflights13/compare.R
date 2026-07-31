# ─────────────────────────────────────────────────────────────────────────────
#  nycflights13 · do the three renderings agree?
#
#  Runs the Ibex, dplyr, and data.table versions of the same six queries and
#  compares every result column by column. Any disagreement is a bug in one of
#  them — an example that claims three engines compute "the same" thing should
#  be able to prove it.
#
#  Run from the repository root:
#      Rscript examples/nycflights13/compare.R
# ─────────────────────────────────────────────────────────────────────────────

example_dir <- file.path("examples", "nycflights13")

# Each script leaves a `results` list behind and skips its plotting block when
# it is sourced rather than run (see the `sys.nframe()` guard at its foot).
run_script <- function(file) {
    env <- new.env(parent = globalenv())
    sys.source(file.path(example_dir, file), envir = env, keep.source = FALSE)
    env$results
}

message("running Ibex ...")
ibex <- run_script("flights.R")
message("running dplyr ...")
dplyr_res <- run_script("flights_dplyr.R")
message("running data.table ...")
dt_res <- run_script("flights_datatable.R")

# The three disagree on inessentials: column order after a join, and integer vs.
# double counts (Ibex counts in Int64, dplyr's n() is integer). Normalize those,
# then demand exact agreement on everything that is actually a result.
normalize <- function(df) {
    df <- as.data.frame(df)
    df <- df[, order(names(df)), drop = FALSE]
    num <- vapply(df, is.numeric, logical(1))
    df[num] <- lapply(df[num], as.numeric)
    rownames(df) <- NULL
    df
}

compare_one <- function(name, a, b, label_a, label_b) {
    a <- normalize(a)
    b <- normalize(b)

    shared <- intersect(names(a), names(b))
    verdict <- all.equal(a[shared], b[shared], tolerance = 1e-12)

    if (isTRUE(verdict)) {
        message(sprintf("  %-14s %s == %s  (%d rows, %d cols)",
                        name, label_a, label_b, nrow(a), length(shared)))
        return(TRUE)
    }

    message(sprintf("  %-14s %s != %s", name, label_a, label_b))
    for (line in verdict) message("      ", line)
    FALSE
}

datasets <- c("carriers", "hourly", "daily", "by_age", "by_visib")
ok <- TRUE

message("\nIbex vs dplyr")
for (name in datasets) {
    ok <- compare_one(name, ibex[[name]], dplyr_res[[name]], "ibex", "dplyr") && ok
}

message("\nIbex vs data.table")
for (name in datasets) {
    ok <- compare_one(name, ibex[[name]], dt_res[[name]], "ibex", "data.table") && ok
}

if (!ok) {
    stop("the three renderings disagree — see above")
}
message("\nall three agree on all ", length(datasets), " results")
