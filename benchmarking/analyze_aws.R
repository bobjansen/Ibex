#!/usr/bin/env Rscript
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# analyze_aws.R — compare one AWS benchmark run with the references, and
# optionally with an earlier run, using Ibex itself from R (r/ibex).
#
#   Rscript benchmarking/analyze_aws.R inmem NEW.csv [OLD.csv]
#   Rscript benchmarking/analyze_aws.R pdsh  NEW.tar.gz [OLD.tar.gz]
#
#   inmem: benchmarking/results/thread_scaling_aws_<ts>.csv (run-thread-scaling.sh)
#   pdsh:  benchmarking/results/tpch_aws_<ts>.tar.gz        (run-tpch.sh)
#
# R finds and loads the files; every comparison is an Ibex query in one session.
# Ratios are ibex_ms / reference_ms, so below 1.0 means Ibex is faster. The
# reference per query is the FASTEST of its engine's variants:
#
#   inmem  Polars 2 = polars: its streaming engine with string keys as
#          Categorical (cast at load, untimed) -- `polars-categorical` in runs
#          from before 2026-10-10, which also timed other variants;
#          DuckDB = duckdb
#   pdsh   Polars 2 = pdsh-polars-stream; DuckDB = pdsh-duckdb
#
# Polars' in-memory engine is an opt-in experiment (run_scale_suite.sh,
# run-tpch.sh) and is not a reference here.
#
# Quote the geomean, not the total: the in-memory totals are dominated by three
# dcast queries. inmem leaves out rand_* (RNG, not query work), corr and
# floor_price, as the published comparison does.
#
# With OLD, "same engine, new/old" puts every engine's change between the two
# runs side by side. It is the box-drift check: when Polars and DuckDB move
# together, the box moved; when only Ibex moves, look at the code (and confirm
# with a same-box A/B before calling it a regression; see MEASURING.md).
#
# Needs the R package from this tree (see r/ibex/README.md) and plugins on the
# usual path (build-release/tools, IBEX_PLUGIN_PATHS or IBEX_LIBRARY_PATH).

suppressPackageStartupMessages(library(ibex))

usage <- function() {
    cat("usage: Rscript benchmarking/analyze_aws.R {inmem|pdsh} NEW [OLD]\n")
    quit(status = 2)
}

argv <- commandArgs(trailingOnly = TRUE)
if (length(argv) < 2 || !(argv[[1]] %in% c("inmem", "pdsh"))) {
    usage()
}
kind <- argv[[1]]
new_path <- argv[[2]]
old_path <- if (length(argv) >= 3) argv[[3]] else NA_character_
for (p in na.omit(c(new_path, old_path))) {
    if (!file.exists(p)) stop("no such file: ", p)
}
has_old <- !is.na(old_path)

session <- create_session()
ibex <- function(query, tables = NULL) session_eval(session, query, tables = tables)

show <- function(title, df) {
    cat("\n== ", title, " ==\n", sep = "")
    if (is.null(df) || nrow(df) == 0) {
        cat("(none)\n")
        return(invisible())
    }
    num <- vapply(df, is.double, logical(1))
    df[num] <- lapply(df[num], round, 3)
    print(df, row.names = FALSE)
}

# ── Load: one long table `raw` {run, cores, framework, query, avg_ms} ────────

if (kind == "inmem") {
    # csv::read straight into the session; `run` tags which file a row came from.
    load_run <- function(path, run) {
        sprintf('csv::read("%s")[select { run = "%s", cores = threads, framework, query, avg_ms }]',
                normalizePath(path), run)
    }
    ibex(paste0(
        'import "csv";\n',
        "let raw = ",
        if (has_old) {
            sprintf("rbind(%s, %s)", load_run(new_path, "new"), load_run(old_path, "old"))
        } else {
            load_run(new_path, "new")
        },
        ";"
    ))
    exclude <- '!(like(query, "rand_%") || like(query, "%corr%") || like(query, "%floor%"))'
    # Runs before 2026-10-10 timed `polars` with String keys and the reference
    # configuration as `polars-categorical`; later runs time only the reference,
    # as `polars`.
    frameworks <- ibex("raw[select { n = count() }, by framework];")$framework
    polars_ref <- if ("polars-categorical" %in% frameworks) "polars-categorical" else "polars"
    references <- list(
        "Polars 2 (streaming, Categorical keys)" = sprintf('framework == "%s"', polars_ref),
        "DuckDB" = 'framework == "duckdb"'
    )
} else {
    # The tarball holds one directory per core count (results/runs/<id>/), each
    # with per-engine TSVs and a manifest.json naming its core count.
    read_tarball <- function(path, run) {
        dir <- tempfile("pdsh-")
        utils::untar(path, exdir = dir)
        runs <- list.dirs(file.path(dir, "results", "runs"), recursive = FALSE)
        rows <- lapply(runs, function(run_dir) {
            manifest <- readLines(file.path(run_dir, "manifest.json"), warn = FALSE)
            cores <- as.integer(sub('.*"cores": *"?([0-9]+)"?.*', "\\1",
                                    grep('"cores"', manifest, value = TRUE)[[1]]))
            tsvs <- list.files(run_dir, pattern = "\\.tsv$", full.names = TRUE)
            do.call(rbind, lapply(tsvs, function(f) {
                d <- utils::read.delim(f, stringsAsFactors = FALSE)
                data.frame(run = run, cores = cores, framework = d$framework,
                           query = d$query, avg_ms = as.double(d$avg_ms))
            }))
        })
        list(raw = do.call(rbind, rows), dir = dir)
    }
    new_run <- read_tarball(new_path, "new")
    raw <- new_run$raw
    if (has_old) {
        raw <- rbind(raw, read_tarball(old_path, "old")$raw)
    }
    # Bound from R by copy: the data.frame becomes a session table.
    ibex("let raw = input;", tables = list(input = raw))
    exclude <- "true"
    references <- list(
        "Polars 2 (streaming)" = 'framework == "pdsh-polars-stream"',
        "DuckDB" = 'framework == "pdsh-duckdb"'
    )
}

# ── Ibex against each reference, per run and core count ──────────────────────

compare <- function(reference) {
    invisible(ibex(sprintf('
        let ib = raw[filter framework == "ibex" && %s,
                     select { run, cores, query, ibex_ms = avg_ms }];
        let rf = raw[filter (%s) && %s,
                     select { ref_ms = min(avg_ms) }, by { run, cores, query }];
        let cmp = (ib join rf on { run, cores, query })
                    [filter ibex_ms > 0.0 && ref_ms > 0.0]
                    [update { ratio = ibex_ms / ref_ms }];
        cmp;', exclude, reference, exclude)))
}

for (name in names(references)) {
    compare(references[[name]])
    show(paste("Ibex vs", name, "(ratio < 1: Ibex faster)"), ibex('
        cmp[select { queries = count(),
                     total = sum(ibex_ms) / sum(ref_ms),
                     geomean = exp(mean(log(ratio))),
                     wins = sum(Int64(ratio < 1.0)) },
            by { run, cores }]
           [order { run asc, cores asc }];'))
}

# The work queue: where Ibex trails the first (headline) reference at the
# highest core count of the new run.
compare(references[[1]])
show(paste("New run, most cores: Ibex behind", names(references)[[1]], "(worst first)"), ibex('
    let top = scalar(cmp[filter run == "new", select { m = max(cores) }], m);
    cmp[filter run == "new" && cores == top && ratio > 1.0,
        select { query, ibex_ms, ref_ms, ratio, behind_ms = ibex_ms - ref_ms }]
       [order { ratio desc }];'))

# ── Two runs: what moved, and was it the box? ─────────────────────────────────

if (has_old) {
    show("Same engine, new/old geomean (box drift: references move together)", ibex(sprintf('
        let nw = raw[filter run == "new" && %s, select { cores, framework, query, new_ms = avg_ms }];
        let ol = raw[filter run == "old" && %s, select { cores, framework, query, old_ms = avg_ms }];
        let moved = (nw join ol on { cores, framework, query })
                      [filter new_ms > 0.05 && old_ms > 0.05]
                      [update { r = new_ms / old_ms, at = `${cores}c` }];
        moved[select { g = exp(mean(log(r))) }, by { framework, at }]
             [dcast at, select g, by framework]
             [order { framework asc }];', exclude, exclude)))

    show("Ibex queries that moved most, new/old, at the new run's most cores", ibex('
        let top = scalar(raw[filter run == "new", select { m = max(cores) }], m);
        let ib_moved = moved[filter framework == "ibex" && cores == top,
                             select { query, old_ms, new_ms, r }];
        rbind(ib_moved[order { r desc }][head 8], ib_moved[order { r asc }][head 8]);'))
}

# ── PDS-H host facts: THP state and perf counters (bootstrap.sh, 2026-10-09+) ─

if (kind == "pdsh") {
    host <- file.path(new_run$dir, "results", "host")
    perf_files <- list.files(host, pattern = "^perf_stat_.*\\.txt$", recursive = TRUE,
                             full.names = TRUE)
    counter <- function(lines, event) {
        hit <- grep(paste0("[[:space:]]", event, "(:u)?[[:space:]]"), lines, value = TRUE)
        if (length(hit) == 0) return(NA_real_)
        value <- sub("^[[:space:]]*([0-9.,]+|<not supported>).*", "\\1", hit[[1]])
        suppressWarnings(as.double(gsub(",", "", value)))
    }
    if (length(perf_files) > 0) {
        perf <- do.call(rbind, lapply(perf_files, function(f) {
            lines <- readLines(f, warn = FALSE)
            m <- regmatches(basename(f), regexec("perf_stat_(q[0-9]+)_([0-9]+)c", basename(f)))[[1]]
            # Older perf prints task-clock as "<x> msec task-clock", newer
            # perf (the AWS box's) as bare nanoseconds.
            task_line <- grep("task-clock", lines, value = TRUE)
            task_clock <- counter(lines, "task-clock")
            if (length(task_line) == 0 || !grepl("msec", task_line[[1]])) {
                task_clock <- task_clock / 1e6
            }
            data.frame(query = m[[2]], cores = as.integer(m[[3]]),
                       task_clock_ms = task_clock,
                       page_faults = counter(lines, "page-faults"),
                       major_faults = counter(lines, "major-faults"),
                       dtlb_load_misses = counter(lines, "dTLB-load-misses"),
                       cycles = counter(lines, "cycles"),
                       instructions = counter(lines, "instructions"))
        }))
        show("perf stat, Ibex (warm REPL, same warmup/iters as the timed run)", ibex('
            perf[update { faults_per_cpu_ms = Float64(page_faults) / task_clock_ms,
                          ipc = Float64(instructions) / Float64(cycles) }]
                [select { query, cores, task_clock_ms, page_faults, faults_per_cpu_ms,
                          dtlb_load_misses, ipc }]
                [order { query asc, cores asc }];', tables = list(perf = perf)))
    }
    mem_files <- list.files(host, pattern = "^memory_.*\\.txt$", full.names = TRUE)
    if (length(mem_files) > 0) {
        field <- function(lines, key) {
            hit <- grep(paste0("^", key, "[ :=]"), lines, value = TRUE)
            if (length(hit) == 0) return(NA_character_)
            trimws(sub(paste0("^", key, "[ :=]+"), "", hit[[1]]))
        }
        mem <- do.call(rbind, lapply(mem_files, function(f) {
            lines <- readLines(f, warn = FALSE)
            data.frame(snapshot = sub("^memory_(.*)\\.txt$", "\\1", basename(f)),
                       thp_enabled = field(lines, "thp_enabled"),
                       anon_huge_kb = as.double(sub(" kB", "", field(lines, "AnonHugePages"))),
                       thp_fault_alloc = as.double(field(lines, "thp_fault_alloc")),
                       thp_fault_fallback = as.double(field(lines, "thp_fault_fallback")))
        }))
        show("Host memory snapshots (THP)", mem)
    }
}
