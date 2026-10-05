#!/usr/bin/env Rscript
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
#
# The PDS-H AWS report: Ibex does the analysis (analysis.ibex), R draws it.
#
# Unpacks each run archive, loads every pass into one Ibex session through the
# r/ibex package, asks Ibex for the summary tables, writes them as CSV and plots
# them with ggplot2.
#
# Run from the repository root, after installing r/ibex (see r/ibex/README.md):
#
#   Rscript benchmarking/tpch/report/report.R [out_dir] [label=archive ...]
#
# Default archives are the 2026-10-05 runs: `main` (2, 4, 8, 16 cores) and
# `confirmation` (two more 8-core passes on a second box).

suppressPackageStartupMessages({
    library(ibex)
    library(ggplot2)
})

args <- commandArgs(trailingOnly = TRUE)
out_dir <- if (length(args) >= 1) args[[1]] else "benchmarking/tpch/report/out"
runs <- c(
    main = "benchmarking/results/tpch_aws_20261005T071555.tar.gz",
    confirmation = "benchmarking/results/tpch_aws_20261005T091728.tar.gz"
)
if (length(args) >= 2) {
    pairs <- strsplit(args[-1], "=", fixed = TRUE)
    runs <- setNames(vapply(pairs, `[`, "", 2), vapply(pairs, `[`, "", 1))
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ── Ibex: load and analyse ──────────────────────────────────────────────────
build_dir <- Sys.getenv("IBEX_BUILD_DIR", "build-release")
session <- create_session(plugin_paths = file.path(build_dir, "tools"))
invisible(session_eval_file(session, "benchmarking/tpch/report/analysis.ibex"))

unpack_root <- file.path(tempdir(), "pdsh-report")
first <- TRUE
for (run in names(runs)) {
    dest <- file.path(unpack_root, run)
    untar(runs[[run]], exdir = dest)
    pass_dirs <- sort(list.dirs(file.path(dest, "results", "runs"), recursive = FALSE))
    for (i in seq_along(pass_dirs)) {
        load <- sprintf('load_pass("%s", "%s", "%s-%d")', pass_dirs[[i]], run, run, i)
        statement <- if (first) {
            sprintf("let timings = %s;", load)
        } else {
            sprintf("let timings = rbind(timings, %s);", load)
        }
        invisible(session_eval(session, statement))
        first <- FALSE
    }
}

ask <- function(query) session_eval(session, query)
suite <- ask('suite(timings, "multi", "");')
suite_no_q21 <- ask('suite(timings, "multi", "q21");')
single <- ask("one_core(timings);")
scaling <- ask("scaling(timings);")
repeat8 <- ask("repeatability(timings, 8);")
queries <- ask("per_query(timings);")

tables <- list(suite = suite, suite_no_q21 = suite_no_q21, one_core = single,
               scaling = scaling, repeatability_8_cores = repeat8, per_query = queries)
for (name in names(tables)) {
    write.csv(tables[[name]], file.path(out_dir, paste0(name, ".csv")), row.names = FALSE)
}

# ── R: plot ─────────────────────────────────────────────────────────────────
engine_colors <- c(Ibex = "#1f7a63", Polars = "#4f5fc4", DuckDB = "#b07a12")
core_breaks <- c(1, 2, 4, 8, 16)
theme_report <- theme_minimal(base_size = 12) +
    theme(panel.grid.minor = element_blank(), legend.position = "top",
          legend.title = element_blank(), plot.title.position = "plot")
save <- function(plot, name, width = 7.5, height = 4.6) {
    ggsave(file.path(out_dir, name), plot, width = width, height = height, dpi = 160, bg = "white")
}
main <- suite[suite$run == "main", ]
main_single <- single[single$run == "main", ]

# Ibex time over each engine's, by core count (1 core: the single-threaded rows).
ratios <- rbind(
    data.frame(cores = c(1, main$cores), engine = "Polars",
               ratio = c(main_single$vs_polars, main$vs_polars)),
    data.frame(cores = c(1, main$cores), engine = "DuckDB",
               ratio = c(main_single$vs_duckdb, main$vs_duckdb))
)
save(ggplot(ratios, aes(cores, ratio, colour = engine)) +
        geom_hline(yintercept = 1, linetype = "dashed", colour = "grey55") +
        geom_line(linewidth = 1) + geom_point(size = 2.4) +
        geom_text(aes(label = sprintf("%.2f", ratio)), vjust = -0.9, size = 3.3,
                  show.legend = FALSE) +
        scale_x_continuous(trans = "log2", breaks = core_breaks) +
        scale_y_continuous(expand = expansion(mult = c(0.05, 0.12))) +
        scale_colour_manual(values = engine_colors,
                            labels = c(Polars = "vs Polars streaming", DuckDB = "vs DuckDB")) +
        labs(x = "cores", y = "Ibex time / engine time",
             title = "PDS-H SF-10, suite total: Ibex relative to each engine",
             subtitle = "Below 1 Ibex is faster. AWS r7i.8xlarge, hyperthreading off.") +
        theme_report,
    "ratio_by_cores.png")

# Suite total time per engine (log scale).
totals <- rbind(
    data.frame(cores = 1, engine = c("Ibex", "Polars", "DuckDB"),
               seconds = c(main_single$ibex_s, main_single$polars_s, main_single$duckdb_s)),
    data.frame(cores = main$cores, engine = "Ibex", seconds = main$ibex_s),
    data.frame(cores = main$cores, engine = "Polars", seconds = main$polars_s),
    data.frame(cores = main$cores, engine = "DuckDB", seconds = main$duckdb_s)
)
save(ggplot(totals, aes(cores, seconds, colour = engine)) +
        geom_line(linewidth = 1) + geom_point(size = 2.4) +
        scale_x_continuous(trans = "log2", breaks = core_breaks) +
        scale_y_log10(breaks = c(4, 8, 16, 32, 64)) +
        scale_colour_manual(values = engine_colors) +
        labs(x = "cores", y = "suite total (s)", title = "PDS-H SF-10, suite total time") +
        theme_report,
    "total_time.png")

# Speedup over each engine's own single-core time, against linear.
main_scaling <- scaling[scaling$run == "main", ]
speedups <- rbind(
    data.frame(cores = 1, engine = c("Ibex", "Polars", "DuckDB"), speedup = 1),
    data.frame(cores = main_scaling$cores, engine = "Ibex", speedup = main_scaling$speedup_ibex),
    data.frame(cores = main_scaling$cores, engine = "Polars", speedup = main_scaling$speedup_polars),
    data.frame(cores = main_scaling$cores, engine = "DuckDB", speedup = main_scaling$speedup_duckdb)
)
save(ggplot(speedups, aes(cores, speedup, colour = engine)) +
        geom_abline(slope = 1, intercept = 0, linetype = "dashed", colour = "grey55") +
        geom_line(linewidth = 1) + geom_point(size = 2.4) +
        scale_x_continuous(trans = "log2", breaks = core_breaks) +
        scale_y_continuous(trans = "log2", breaks = core_breaks) +
        scale_colour_manual(values = engine_colors) +
        labs(x = "cores", y = "speedup over 1 core",
             title = "PDS-H SF-10, scaling", subtitle = "Dashed: linear.") +
        theme_report,
    "speedup.png")

# Per query against the faster engine, at 2, 8 and 16 cores.
per <- queries[queries$run == "main" & queries$cores %in% c(2, 8, 16), ]
order_8 <- per[per$cores == 8, ]
per$query <- factor(per$query, levels = order_8$query[order(order_8$vs_faster)])
per$cores <- factor(paste(per$cores, "cores"), levels = c("2 cores", "8 cores", "16 cores"))
per$outcome <- ifelse(per$vs_faster <= 1, "Ibex faster than both", "an engine is faster")
save(ggplot(per, aes(vs_faster, query, colour = outcome)) +
        geom_vline(xintercept = 1, linetype = "dashed", colour = "grey55") +
        geom_segment(aes(x = 1, xend = vs_faster, yend = query), alpha = 0.35, linewidth = 1.6) +
        geom_point(size = 2.2) +
        facet_wrap(~cores, nrow = 1) +
        scale_colour_manual(values = c("Ibex faster than both" = "#1f7a63",
                                       "an engine is faster" = "#b4462f")) +
        labs(x = "Ibex time / faster engine's time", y = NULL,
             title = "PDS-H SF-10 per query, against the faster of Polars and DuckDB") +
        theme_report,
    "per_query.png", width = 10, height = 6.2)

cat(sprintf("Wrote %d tables and 4 charts to %s\n", length(tables), out_dir))
print(repeat8[, c("pass", "vs_polars", "vs_duckdb", "no_q21_vs_polars", "no_q21_vs_duckdb")],
      row.names = FALSE, digits = 3)
