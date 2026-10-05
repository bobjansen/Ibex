#!/usr/bin/env Rscript
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# PDS-H report: Ibex does the analysis (analysis.ibex), R draws it.
#
# Takes any set of PDS-H measurements -- run archives from the AWS harness
# (`benchmarking/results/tpch_aws_*.tar.gz`) or result directories from
# `run_bench.sh` (`benchmarking/tpch/results`) -- loads every pass of every
# run into one Ibex session through r/ibex, asks Ibex for the summary tables,
# writes them as CSV and plots them with ggplot2. Engines, core counts and
# scale factors are whatever the measurements hold; Ibex is compared with
# every other engine present.
#
# Run from the repository root, after installing r/ibex (see r/ibex/README.md):
#
#   Rscript benchmarking/tpch/report/report.R [--out DIR] [label=]PATH ...
#
# A run's label defaults to its file or directory name. With no PATH, the
# 2026-10-05 AWS runs are used.

suppressPackageStartupMessages({
    library(ibex)
    library(ggplot2)
})

args <- commandArgs(trailingOnly = TRUE)
out_dir <- "benchmarking/tpch/report/out"
if (length(args) >= 2 && args[[1]] == "--out") {
    out_dir <- args[[2]]
    args <- args[-(1:2)]
}
if (length(args) == 0) {
    args <- c("main=benchmarking/results/tpch_aws_20261005T071555.tar.gz",
              "confirmation=benchmarking/results/tpch_aws_20261005T091728.tar.gz")
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ── Find the passes ─────────────────────────────────────────────────────────
run_label <- function(path) sub("\\.tar\\.gz$|\\.tgz$", "", basename(sub("/+$", "", path)))

# The directory holding `runs/<pass>/`: an archive is unpacked first; a result
# directory may be given as itself or as its parent.
runs_root <- function(path, label) {
    if (grepl("\\.(tar\\.gz|tgz)$", path)) {
        dest <- file.path(tempdir(), "pdsh-report", label)
        untar(path, exdir = dest)
        path <- dest
    }
    for (candidate in c(path, file.path(path, "results"))) {
        if (dir.exists(file.path(candidate, "runs"))) {
            return(file.path(candidate, "runs"))
        }
    }
    stop("no runs/ directory under ", path)
}

inputs <- lapply(args, function(arg) {
    parts <- regmatches(arg, regexpr("=", arg), invert = TRUE)[[1]]
    if (length(parts) == 2) list(label = parts[[1]], path = parts[[2]])
    else list(label = run_label(arg), path = arg)
})

# ── Ibex: load and analyse ──────────────────────────────────────────────────
build_dir <- Sys.getenv("IBEX_BUILD_DIR", "build-release")
session <- create_session(plugin_paths = file.path(build_dir, "tools"))
invisible(session_eval_file(session, "benchmarking/tpch/report/analysis.ibex"))

first <- TRUE
for (input in inputs) {
    passes <- sort(list.dirs(runs_root(input$path, input$label), recursive = FALSE))
    for (i in seq_along(passes)) {
        for (file in list.files(passes[[i]], pattern = "\\.tsv$")) {
            load <- sprintf('load_timings("%s", "%s", "%s", "%s-%d")',
                            passes[[i]], file, input$label, input$label, i)
            invisible(session_eval(session, if (first) {
                sprintf("let timings = %s;", load)
            } else {
                sprintf("let timings = rbind(timings, %s);", load)
            }))
            first <- FALSE
        }
    }
}
if (first) stop("no timing TSVs found")

ask <- function(query) session_eval(session, query)
tables <- list(
    suite = ask('suite(timings, "multi", "");'),
    suite_no_q21 = ask('suite(timings, "multi", "q21");'),
    suite_vs_faster = ask('suite_vs_faster(timings, "");'),
    suite_vs_faster_no_q21 = ask('suite_vs_faster(timings, "q21");'),
    totals = ask("totals(timings);"),
    one_core = ask("one_core(timings);"),
    scaling = ask("scaling(timings);"),
    per_query = ask("per_query(timings);")
)
for (name in names(tables)) {
    write.csv(tables[[name]], file.path(out_dir, paste0(name, ".csv")), row.names = FALSE)
}

# ── R: plot ─────────────────────────────────────────────────────────────────
known_colors <- c(Ibex = "#1f7a63", `Polars streaming` = "#4f5fc4",
                  `Polars in-memory` = "#8fa0e8", DuckDB = "#b07a12")
engines <- sort(unique(tables$totals$engine))
others <- setdiff(engines, names(known_colors))
engine_colors <- c(known_colors[intersect(names(known_colors), engines)],
                   setNames(grDevices::hcl.colors(max(1, length(others)), "Dark 3")[seq_along(others)],
                            others))
core_breaks <- sort(unique(c(1, tables$totals$cores)))
by_run <- if (length(inputs) > 1) facet_wrap(~run) else NULL
theme_report <- theme_minimal(base_size = 12) +
    theme(panel.grid.minor = element_blank(), legend.position = "top",
          legend.title = element_blank(), plot.title.position = "plot")
sf_text <- paste0("SF-", paste(sort(unique(tables$totals$scale_factor)), collapse = "/"))
save <- function(plot, name, width = 7.5, height = 4.6) {
    if (length(inputs) > 1) width <- width * 1.6
    ggsave(file.path(out_dir, name), plot, width = width, height = height, dpi = 160, bg = "white")
}
cores_axis <- scale_x_continuous(trans = "log2", breaks = core_breaks)

# Ibex time over each reference's, by core count. 1 core: the single-threaded
# rows of the run's passes, averaged.
one <- tables$one_core
ibex_one <- one[one$engine == "Ibex", c("run", "seconds")]
names(ibex_one)[2] <- "ibex_s"
refs_one <- merge(one[one$engine != "Ibex", ], ibex_one, by = "run")
ratios <- rbind(
    data.frame(run = refs_one$run, cores = 1, reference = refs_one$engine,
               ratio = refs_one$ibex_s / refs_one$seconds),
    data.frame(run = tables$suite$run, cores = tables$suite$cores,
               reference = tables$suite$reference, ratio = tables$suite$total_ratio)
)
ratio_means <- aggregate(ratio ~ run + cores + reference, data = ratios, FUN = mean)
save(ggplot(ratios, aes(cores, ratio, colour = reference)) +
        geom_hline(yintercept = 1, linetype = "dashed", colour = "grey55") +
        geom_line(data = ratio_means, linewidth = 1) + geom_point(size = 2.2) +
        geom_text(data = ratio_means, aes(label = sprintf("%.2f", ratio)), vjust = -0.9,
                  size = 3.2, show.legend = FALSE) +
        cores_axis + scale_y_continuous(expand = expansion(mult = c(0.05, 0.12))) +
        scale_colour_manual(values = engine_colors) + by_run +
        labs(x = "cores", y = "Ibex time / engine time",
             title = paste0("PDS-H ", sf_text, ", suite total: Ibex relative to each engine"),
             subtitle = "Below 1 Ibex is faster. Points: passes; line: their mean.") +
        theme_report,
    "ratio_by_cores.png")

# Suite total per engine (log scale), 1 core included.
multi <- tables$totals[tables$totals$mode == "multi", ]
totals_plot <- rbind(
    data.frame(run = one$run, cores = 1, engine = one$engine, seconds = one$seconds),
    data.frame(run = multi$run, cores = multi$cores, engine = multi$engine, seconds = multi$seconds)
)
totals_means <- aggregate(seconds ~ run + cores + engine, data = totals_plot, FUN = mean)
save(ggplot(totals_plot, aes(cores, seconds, colour = engine)) +
        geom_line(data = totals_means, linewidth = 1) + geom_point(size = 2.2) +
        cores_axis + scale_y_log10() +
        scale_colour_manual(values = engine_colors) + by_run +
        labs(x = "cores", y = "suite total (s)", title = paste0("PDS-H ", sf_text, ", suite total time")) +
        theme_report,
    "total_time.png")

# Speedup over each engine's own single-core time, against linear.
speedups <- rbind(
    data.frame(run = one$run, cores = 1, engine = one$engine, speedup = 1),
    data.frame(run = tables$scaling$run, cores = tables$scaling$cores,
               engine = tables$scaling$engine, speedup = tables$scaling$speedup)
)
speedup_means <- aggregate(speedup ~ run + cores + engine, data = speedups, FUN = mean)
save(ggplot(speedups, aes(cores, speedup, colour = engine)) +
        geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "grey55") +
        geom_line(data = speedup_means, linewidth = 1) + geom_point(size = 2.2) +
        cores_axis + scale_y_continuous(trans = "log2", breaks = core_breaks) +
        scale_colour_manual(values = engine_colors) + by_run +
        labs(x = "cores", y = "speedup over 1 core", title = paste0("PDS-H ", sf_text, ", scaling"),
             subtitle = "Dashed: linear.") +
        theme_report,
    "speedup.png")

# Per query against the fastest reference, one panel per run and core count
# (passes at the same core count averaged), queries ordered by their overall
# ratio.
per <- aggregate(vs_faster ~ run + cores + query, data = tables$per_query, FUN = mean)
query_order <- aggregate(vs_faster ~ query, data = per, FUN = mean)
per$query <- factor(per$query, levels = query_order$query[order(query_order$vs_faster)])
per$panel <- factor(sprintf("%s, %d cores", per$run, per$cores),
                    levels = unique(sprintf("%s, %d cores", per$run, per$cores)[order(per$run, per$cores)]))
per$outcome <- ifelse(per$vs_faster <= 1, "Ibex faster than every engine", "an engine is faster")
panels <- length(levels(per$panel))
ggsave(file.path(out_dir, "per_query.png"),
       ggplot(per, aes(vs_faster, query, colour = outcome)) +
           geom_vline(xintercept = 1, linetype = "dashed", colour = "grey55") +
           geom_segment(aes(x = 1, xend = vs_faster, yend = query), alpha = 0.35, linewidth = 1.6) +
           geom_point(size = 2.2) +
           facet_wrap(~panel, nrow = 1) +
           scale_colour_manual(values = c("Ibex faster than every engine" = "#1f7a63",
                                          "an engine is faster" = "#b4462f")) +
           labs(x = "Ibex time / fastest engine's time", y = NULL,
                title = paste0("PDS-H ", sf_text, " per query, against the fastest other engine")) +
           theme_report,
       width = min(4 + 3 * panels, 24), height = 6.2, dpi = 160, bg = "white")

cat(sprintf("Read %d run(s); wrote %d tables and 4 charts to %s\n",
            length(inputs), length(tables), out_dir))
print(tables$suite_vs_faster[, c("run", "pass", "cores", "total_ratio", "geomean")],
      row.names = FALSE, digits = 3)
