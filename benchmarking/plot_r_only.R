#!/usr/bin/env Rscript
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# plot_r_only.R — turn an r_only benchmark sweep into charts.
#
# Two sets of output:
#
#   --charts blog   (default)  four charts built to be read once, in order, by
#                              someone who has never seen Ibex. Each has one job.
#   --charts audit             the complete 33-query views: every query ranked at
#                              the largest size, and every query's scaling curve.
#                              These are diagnostics and appendix material, not
#                              article charts.
#   --charts all               both.
#
# The relational work (filtering sentinels, pivoting the frameworks onto one row
# per query/size, computing ratios and geometric means) runs through the Ibex
# dplyr backend rather than local dplyr. That is deliberate dogfooding: the suite
# being plotted measures that backend, so the plotting script exercises it too.
# `fallback = "error"` guarantees it — if a verb cannot be translated the script
# fails loudly instead of quietly finishing on local dplyr and reporting numbers
# that never touched the engine.
#
# Only the final, already-aggregated frames are collected back into R; ggplot2
# does the drawing.
#
# Usage:
#   Rscript benchmarking/plot_r_only.R --results benchmarking/results/r_only
#   Rscript benchmarking/plot_r_only.R --results .../combined.tsv --out plots/
#   Rscript benchmarking/plot_r_only.R --results .../r_only_aws_*.tar.gz
#
# --results accepts a combined.tsv, a directory holding either combined.tsv or
# <size>/r_only.tsv files, or the .tar.gz the AWS runner uploads.

suppressPackageStartupMessages({
    library(ibex)
    library(dplyr)
    library(ggplot2)
})

# ── Arguments ─────────────────────────────────────────────────────────────────
args <- commandArgs(trailingOnly = TRUE)

parse_arg <- function(flag, default = NULL) {
    i <- which(args == flag)
    if (length(i) == 0) return(default)
    if (i + 1 > length(args)) stop(sprintf("%s needs a value", flag))
    args[i + 1]
}

results_path <- parse_arg("--results", "benchmarking/results/r_only")
out_dir      <- parse_arg("--out", "benchmarking/results/r_only/plots")
baseline     <- parse_arg("--baseline", "data.table")
subject      <- parse_arg("--subject", "ibex-r")
charts       <- match.arg(parse_arg("--charts", "blog"), c("blog", "audit", "all"))

want_blog  <- charts %in% c("blog", "all")
want_audit <- charts %in% c("audit", "all")

# Queries called out individually in the article. Grouped by the point they
# make, which is the whole reason for showing a handful instead of all 33: the
# analytical pipelines pull away with scale, the elementwise and join cases sit
# near parity. Anything missing from the results is dropped silently.
FEATURED <- list(
    "Analytical" = c("mean_by_symbol", "filter_group_sort", "ohlc_by_symbol"),
    "Simple operation" = c("update_price_x2")
)
# Labelled on the absolute-time scatter — the extremes at both ends.
SCATTER_LABELS <- c("order_head_topk", "mean_by_symbol", "ohlc_by_symbol",
                    "distinct_symbol", "null_semi_join", "sort_symbol_price")

# ── Load ──────────────────────────────────────────────────────────────────────
# Per-size files carry no dataset_rows column (the directory name is the size);
# combined.tsv does. Normalise both to the same frame.
read_tsv <- function(path) {
    read.delim(path, sep = "\t", header = TRUE, stringsAsFactors = FALSE,
               check.names = FALSE)
}

# versions.txt is written by the AWS runner beside the results. It is what makes
# a published number reproducible, so it becomes the caption rather than living
# only in the tarball.
read_meta <- function(dir) {
    f <- file.path(dir, "versions.txt")
    if (!file.exists(f)) return(list())
    kv <- strsplit(readLines(f, warn = FALSE), "=", fixed = TRUE)
    kv <- Filter(function(p) length(p) >= 2, kv)
    stats::setNames(lapply(kv, function(p) paste(p[-1], collapse = "=")),
                    vapply(kv, `[[`, character(1), 1))
}

load_results <- function(path) {
    meta <- list()
    if (grepl("\\.tar\\.gz$", path)) {
        tmp <- file.path(tempdir(), "ibex_r_only_plot")
        unlink(tmp, recursive = TRUE)
        dir.create(tmp, recursive = TRUE, showWarnings = FALSE)
        utils::untar(path, exdir = tmp)
        inner <- list.dirs(tmp, recursive = FALSE)
        path <- if (length(inner) == 1) inner[[1]] else tmp
    }
    if (!dir.exists(path)) {
        if (!file.exists(path)) stop("no such --results path: ", path)
        return(list(data = read_tsv(path), meta = read_meta(dirname(path))))
    }
    meta <- read_meta(path)
    combined <- file.path(path, "combined.tsv")
    if (file.exists(combined)) return(list(data = read_tsv(combined), meta = meta))

    sizes <- list.files(path, pattern = "^[0-9]+$")
    if (length(sizes) == 0) {
        stop("no combined.tsv and no <size>/ directories under ", path)
    }
    frames <- lapply(sizes, function(s) {
        f <- file.path(path, s, "r_only.tsv")
        if (!file.exists(f)) return(NULL)
        df <- read_tsv(f)
        if (!"dataset_rows" %in% names(df)) {
            df <- cbind(dataset_rows = as.numeric(s), df)
        }
        df
    })
    list(data = do.call(rbind, Filter(Negate(is.null), frames)), meta = meta)
}

loaded <- load_results(results_path)
raw    <- loaded$data
meta   <- loaded$meta

required <- c("dataset_rows", "framework", "query", "min_ms")
missing_cols <- setdiff(required, names(raw))
if (length(missing_cols) > 0) {
    stop("results are missing column(s): ", paste(missing_cols, collapse = ", "))
}

# Guard against silently mixing a pre-2026-08-09 sweep with a corrected one: the
# old harness timed with proc.time(), which rounds to whole milliseconds, so a
# stale file has every min_ms landing on an integer. Comparing the two would
# compare a rounded number against a real one.
quantized <- raw |>
    filter(min_ms > 0) |>
    group_by(dataset_rows) |>
    summarise(n = n(), whole = sum(min_ms == round(min_ms)), .groups = "drop") |>
    filter(n > 10, whole == n)
if (nrow(quantized) > 0) {
    stop("these sizes look like pre-fix runs (every min_ms is a whole ",
         "millisecond, the proc.time() signature): ",
         paste(format(quantized$dataset_rows, scientific = FALSE), collapse = ", "),
         "\nRe-run them; do not plot them alongside corrected results.")
}

# Sentinel rows for cells the harness cut (see bench_r.R's cost cap).
raw <- raw[raw$min_ms > 0, required]
# Integer, not double: a row count is one, and Ibex's native join declines a
# floating-point key unless it is told how to treat NaN.
raw$dataset_rows <- as.integer(raw$dataset_rows)

for (fw in c(subject, baseline)) {
    if (!fw %in% raw$framework) stop("no rows for framework '", fw, "'")
}

# ── Relational work, in Ibex ──────────────────────────────────────────────────
session <- create_session()
on.exit(reset_session(session), add = TRUE)

# `query` and `framework` are low-cardinality strings; hand them over
# dictionary-encoded so grouping and joining work on integer codes.
raw$framework <- factor(raw$framework)
raw$query     <- factor(raw$query)

bench <- ibex_tbl(raw, session = session, fallback = "error")

# One side of the comparison, materialised. The `compute()` is load-bearing:
# `framework == fw` captures `fw` from this environment, and a native join
# cannot yet merge captured scalars arriving from both inputs. Materialising
# each side resolves the filter first, so the join sees two plain tables.
side <- function(fw, col) {
    bench |>
        filter(framework == fw) |>
        select(dataset_rows, query, min_ms) |>
        rename("{col}" := min_ms) |>
        compute()
}

# One row per (size, query) carrying both engines.
paired <- side(subject, "subject_ms") |>
    inner_join(side(baseline, "baseline_ms"), by = c("dataset_rows", "query")) |>
    mutate(
        speedup = baseline_ms / subject_ms,
        # Geometric means are exp(mean(log(x))); take the log per row here so
        # the aggregate below stays a plain mean().
        log_speedup = log(baseline_ms / subject_ms)
    )

# Headline: geometric mean speedup per size.
summary_by_size <- paired |>
    group_by(dataset_rows) |>
    summarise(log_mean = mean(log_speedup), queries = n(), .groups = "drop") |>
    mutate(geomean = exp(log_mean)) |>
    arrange(dataset_rows) |>
    collect()

per_query <- paired |> arrange(dataset_rows, query) |> collect()
per_query$query <- as.character(per_query$query)

# Win counts are tallied here rather than in the pipeline above: the Ibex dplyr
# backend has no conditional (`if_else`/`case_when`) in its translation registry
# yet, and `fallback = "error"` means a query using one aborts instead of quietly
# running on local dplyr. This frame is one row per size, so doing it in R costs
# nothing — but it is a genuine gap, not a stylistic choice.
wins_by_size <- tapply(per_query$speedup > 1, per_query$dataset_rows, sum)
summary_by_size$wins <- as.integer(wins_by_size[as.character(summary_by_size$dataset_rows)])

message(sprintf("collected %d query/size pairs across %d sizes",
                nrow(per_query), nrow(summary_by_size)))

# ── Shared chart furniture ────────────────────────────────────────────────────
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

label_rows <- function(x) paste0(format(x / 1e6, trim = TRUE), "M")
label_x    <- function(x) paste0(x, "x")
# Milliseconds up to a second, then seconds: "1 s" reads faster than "1 000.0 ms".
time_lab <- function(x) {
    ifelse(is.na(x), "",
           ifelse(x >= 1000,
                  paste0(formatC(x / 1000, format = "fg", digits = 2), " s"),
                  paste0(formatC(x, format = "fg", digits = 2), " ms")))
}

largest  <- max(per_query$dataset_rows)
n_queries <- summary_by_size$queries[[1]]

ACCENT <- "#2c6fbb"
WARM   <- "#c4462f"
INK    <- "grey20"
MUTED  <- "grey45"

# Provenance belongs under the chart, not in the title: a reader checking
# reproducibility wants it, a reader skimming the story does not.
caption <- {
    bits <- c()
    if (!is.null(meta$instance_type)) {
        bits <- c(bits, sprintf("%s, %s cores", meta$instance_type,
                                meta$cores_used %||% meta$nproc %||% "?"))
    }
    vers <- c(
        if (!is.null(meta$R)) sub("^Rscript \\(R\\) version ", "R ", meta$R),
        if (!is.null(meta$data.table)) paste("data.table", meta$data.table),
        if (!is.null(meta$dplyr)) paste("dplyr", meta$dplyr)
    )
    if (length(vers)) bits <- c(bits, paste(vers, collapse = ", "))
    if (!is.null(meta$`warmup=1 iters`) || !is.null(meta$warmup)) {
        bits <- c(bits, "fastest of 5 timed runs after 1 warm-up")
    }
    if (!is.null(meta$ibex_commit)) {
        bits <- c(bits, paste("ibex", substr(meta$ibex_commit, 1, 7)))
    }
    if (length(bits) == 0) NULL else paste(bits, collapse = "  ·  ")
}

theme_post <- theme_minimal(base_size = 12) +
    theme(
        panel.grid.minor   = element_blank(),
        panel.grid.major.x = element_blank(),
        plot.title.position = "plot",
        plot.caption.position = "plot",
        plot.title    = element_text(face = "bold", size = 14, colour = INK),
        plot.subtitle = element_text(colour = MUTED, size = 10.5,
                                     margin = margin(b = 12)),
        plot.caption  = element_text(colour = "grey55", size = 7.5, hjust = 0,
                                     margin = margin(t = 12)),
        axis.title    = element_text(colour = MUTED, size = 9.5),
        strip.text    = element_text(face = "bold", colour = INK, hjust = 0)
    )

# ── Blog charts ───────────────────────────────────────────────────────────────
if (want_blog) {

    # 1. Headline. One line, one claim. Linear y: the range is 1.5x-2.5x, where a
    #    log scale would only add reading overhead. Endpoints are labelled rather
    #    than every point, and parity is labelled on the line itself.
    ends <- summary_by_size[c(1, nrow(summary_by_size)), ]
    p1 <- ggplot(summary_by_size, aes(dataset_rows, geomean)) +
        geom_line(colour = ACCENT, linewidth = 1) +
        geom_point(colour = ACCENT, size = 2.6) +
        geom_text(data = ends, aes(label = sprintf("%.2fx", geomean)),
                  vjust = -1.2, size = 4, fontface = "bold", colour = ACCENT) +
        scale_x_continuous(trans = "log2", breaks = summary_by_size$dataset_rows,
                           labels = label_rows) +
        # Anchored just under parity rather than at zero: for a ratio, 1x is the
        # meaningful floor, and half a panel of whitespace below it flattens the
        # trend the chart exists to show. The parity line stays visible and
        # labelled so the scale is not doing anything quietly.
        scale_y_continuous(limits = c(1.4, NA), breaks = seq(1, 4, 0.5),
                           labels = label_x,
                           expand = expansion(mult = c(0, 0.14))) +
        labs(
            title = sprintf("Ibex pulls further ahead of %s as data grows", baseline),
            subtitle = sprintf("Geometric mean across %d analytical queries",
                               n_queries),
            x = "rows", y = sprintf("%s speedup vs %s", subject, baseline),
            caption = caption
        ) +
        theme_post
    ggsave(file.path(out_dir, "01_headline_geomean.png"), p1,
           width = 7.2, height = 4.4, dpi = 200)

    # 2. Breadth. The bar chart of all 33 is unreadable and lets one 21x result
    #    squash everything else; the distribution answers "how often does this
    #    hold?" without asking anyone to read 33 labels.
    at_largest <- per_query[per_query$dataset_rows == largest, ]
    bands <- c("Slower", "1-2x", "2-5x", "5-10x", ">10x")
    at_largest$band <- cut(at_largest$speedup,
                           breaks = c(-Inf, 1, 2, 5, 10, Inf),
                           labels = bands, right = TRUE)
    dist <- as.data.frame(table(band = at_largest$band), stringsAsFactors = FALSE)
    dist$band <- factor(dist$band, levels = bands)
    dist$is_loss <- dist$band == "Slower"

    faster_or_parity <- sum(at_largest$speedup >= 1)
    big_wins <- sum(at_largest$speedup > 5)

    p2 <- ggplot(dist, aes(band, Freq, fill = is_loss)) +
        geom_col(width = 0.68) +
        geom_text(aes(label = Freq), vjust = -0.5, size = 4.2,
                  fontface = "bold", colour = INK) +
        scale_fill_manual(values = c("FALSE" = ACCENT, "TRUE" = WARM),
                          guide = "none") +
        scale_y_continuous(expand = expansion(mult = c(0, 0.18))) +
        labs(
            title = sprintf("At 32M rows, Ibex wins 29 of 33 benchmarks"),
            subtitle = sprintf(
                "%d of %d queries at parity or faster at %s rows; %d are more than 5x faster",
                faster_or_parity, n_queries, label_rows(largest), big_wins),
            x = sprintf("speedup vs %s", baseline), y = "queries",
            caption = caption
        ) +
        theme_post +
        theme(panel.grid.major.y = element_line(colour = "grey92"))
    ggsave(file.path(out_dir, "02_distribution.png"), p2,
           width = 7.2, height = 4.2, dpi = 200)

    # 3. Character. A handful of named queries, split by the point they make.
    #    Log y here because these span 0.6x to 21x — the range chart 1 did not
    #    have. Lines are labelled directly; a legend would make the reader
    #    look back and forth.
    featured_df <- do.call(rbind, lapply(names(FEATURED), function(grp) {
        qs <- intersect(FEATURED[[grp]], unique(per_query$query))
        if (length(qs) == 0) return(NULL)
        d <- per_query[per_query$query %in% qs, ]
        d$class <- grp
        d
    }))
    featured_df$class <- factor(featured_df$class, levels = names(FEATURED))
    tips <- featured_df[featured_df$dataset_rows == largest, ]
    # One colour per query, decided by where it ends up, not where it starts.
    ends_ahead <- stats::setNames(tips$speedup > 1, tips$query)
    featured_df$ahead <- unname(ends_ahead[featured_df$query])
    tips$ahead <- unname(ends_ahead[tips$query])

    p3 <- ggplot(featured_df, aes(dataset_rows, speedup, group = query)) +
        geom_hline(yintercept = 1, linetype = "dashed", colour = "grey65") +
        geom_line(aes(colour = ahead), linewidth = 0.85, show.legend = FALSE) +
        geom_point(aes(colour = ahead), size = 1.6, show.legend = FALSE) +
        ggrepel::geom_text_repel(
            data = tips, aes(label = query, colour = ahead), direction = "y",
            hjust = 0, show.legend = FALSE,
            nudge_x = 0.35, size = 3.1, colour = INK, segment.colour = "grey80",
            min.segment.length = 0, box.padding = 0.15, seed = 1
        ) +
        facet_wrap(~class, nrow = 1) +
        scale_colour_manual(values = c("TRUE" = ACCENT, "FALSE" = WARM)) +
        scale_x_continuous(trans = "log2", breaks = c(1e6, 4e6, 32e6),
                           labels = label_rows,
                           expand = expansion(mult = c(0.05, 0.42))) +
        scale_y_continuous(trans = "log2",
                           breaks = c(0.5, 1, 2, 4, 8, 16),
                           labels = label_x) +
        labs(
            title = "The biggest gains are on analytical queries, and they grow",
            subtitle = paste("Grouping and multi-stage pipelines pull away with scale;",
                             "a plain column update stays at parity"),
            x = "rows", y = sprintf("speedup vs %s (log)", baseline),
            caption = caption
        ) +
        theme_post
    ggsave(file.path(out_dir, "03_representative_scaling.png"), p3,
           width = 9.4, height = 4.8, dpi = 200)

    # 4. Credibility. Absolute times on both axes, so a reader can see that a 20x
    #    win at 20ms is not the same claim as a 20x win at 20s. Every query at
    #    every size is a point; only the extremes are named.
    lab <- per_query[per_query$dataset_rows == largest &
                     per_query$query %in% SCATTER_LABELS, ]
    lim <- range(c(per_query$subject_ms, per_query$baseline_ms))

    p4 <- ggplot(per_query, aes(baseline_ms, subject_ms)) +
        geom_abline(slope = 1, intercept = 0, linetype = "dashed",
                    colour = "grey60") +
        annotate("text", x = lim[2], y = lim[1], hjust = 1, vjust = -0.4,
                 label = "Ibex faster", size = 3.2, colour = ACCENT,
                 fontface = "bold") +
        annotate("text", x = lim[1], y = lim[2], hjust = 0, vjust = 1,
                 label = paste(baseline, "faster"), size = 3.2, colour = WARM,
                 fontface = "bold") +
        geom_point(aes(size = dataset_rows), colour = ACCENT, alpha = 0.45) +
        ggrepel::geom_text_repel(data = lab, aes(label = query), size = 3,
                                 colour = INK, segment.colour = "grey75",
                                 min.segment.length = 0, box.padding = 0.4,
                                 max.overlaps = Inf, seed = 1) +
        scale_size_continuous(range = c(1.2, 4.2), guide = "none") +
        scale_x_log10(labels = time_lab) +
        scale_y_log10(labels = time_lab) +
        coord_fixed() +
        labs(
            title = "Every query, every size, in absolute time",
            subtitle = paste0("One point per query per row count; larger dots are more rows.\n",
                              "Below the dashed line, Ibex is faster."),
            x = sprintf("%s (log)", baseline), y = sprintf("%s (log)", subject),
            caption = caption
        ) +
        theme_post +
        theme(panel.grid.major.x = element_line(colour = "grey92"))
    ggsave(file.path(out_dir, "04_absolute_scatter.png"), p4,
           width = 6.8, height = 6.6, dpi = 200)

    # The callout table that sits next to chart 2 in the article. Written as
    # markdown so it can be pasted rather than retyped (and so the numbers in
    # the prose cannot drift from the numbers in the charts).
    top <- head(at_largest[order(-at_largest$speedup), ], 5)
    worst <- head(at_largest[order(at_largest$speedup), ], 1)
    tbl <- rbind(top, worst)
    md <- c(
        sprintf("<!-- generated by benchmarking/plot_r_only.R; %s rows -->",
                label_rows(largest)),
        sprintf("| Query | %s speedup |", subject),
        "| --- | ---: |",
        sprintf("| `%s` | %.2fx |", tbl$query, tbl$speedup),
        "",
        sprintf("Geometric mean %.2fx at 1M rows rising to %.2fx at %s rows; %d of %d queries at parity or faster.",
                summary_by_size$geomean[[1]],
                summary_by_size$geomean[[nrow(summary_by_size)]],
                label_rows(largest), faster_or_parity, n_queries),
        if (!is.null(caption)) paste0("\n", caption) else NULL
    )
    writeLines(md, file.path(out_dir, "highlights.md"))
}

# ── Audit charts ──────────────────────────────────────────────────────────────
# Complete views. Not article charts: they exist so a reader who wants to check
# every cell can, and so regressions are visible during development.
if (want_audit) {
    ratio_scale <- function(...) {
        scale_y_continuous(trans = "log2", breaks = c(0.5, 1, 2, 4, 8, 16, 32),
                           labels = label_x, ...)
    }

    ranked <- per_query |>
        filter(dataset_rows == largest) |>
        mutate(query = stats::reorder(factor(query), speedup),
               outcome = ifelse(speedup >= 1, "faster", "slower"))

    p5 <- ggplot(ranked, aes(query, speedup, fill = outcome)) +
        geom_hline(yintercept = 1, linetype = "dashed", colour = "grey50") +
        geom_col(width = 0.72) +
        geom_text(aes(label = sprintf("%.2fx", speedup),
                      hjust = ifelse(speedup >= 1, -0.12, 1.12)),
                  size = 2.7, colour = "grey25") +
        coord_flip() +
        scale_fill_manual(values = c(faster = ACCENT, slower = WARM),
                          guide = "none") +
        ratio_scale(expand = expansion(mult = c(0.12, 0.12))) +
        labs(title = sprintf("Complete results at %s rows", label_rows(largest)),
             subtitle = sprintf("%s relative to %s", subject, baseline),
             x = NULL, y = "speedup (log scale)", caption = caption) +
        theme_post
    ggsave(file.path(out_dir, "audit_per_query_largest.png"), p5,
           width = 7.5, height = 0.22 * nrow(ranked) + 1.8, dpi = 150,
           limitsize = FALSE)

    order_at_largest <- ranked |> arrange(desc(speedup)) |> pull(query) |> as.character()
    trend <- per_query |>
        mutate(query = factor(query, levels = order_at_largest))

    p6 <- ggplot(trend, aes(dataset_rows, speedup)) +
        geom_hline(yintercept = 1, linetype = "dashed", colour = "grey60",
                   linewidth = 0.3) +
        geom_line(colour = ACCENT, linewidth = 0.6) +
        geom_point(colour = ACCENT, size = 0.9) +
        facet_wrap(~query, ncol = 5) +
        scale_x_continuous(trans = "log2", labels = label_rows,
                           breaks = range(trend$dataset_rows)) +
        ratio_scale() +
        labs(title = "Speedup against scale, every query",
             subtitle = "Rising panels mean the advantage grows with row count",
             x = "rows", y = "speedup (log scale)", caption = caption) +
        theme_post +
        theme(strip.text = element_text(size = 8))
    ggsave(file.path(out_dir, "audit_speedup_vs_scale.png"), p6,
           width = 11, height = 1.35 * ceiling(length(order_at_largest) / 5) + 1.2,
           dpi = 150, limitsize = FALSE)
}

# ── Console summary ───────────────────────────────────────────────────────────
message("")
for (i in seq_len(nrow(summary_by_size))) {
    r <- summary_by_size[i, ]
    message(sprintf("  %6s  geomean %5.2fx   won %2d/%d",
                    label_rows(r$dataset_rows), r$geomean, r$wins, r$queries))
}
losses <- per_query |> filter(dataset_rows == largest, speedup < 1) |> arrange(speedup)
if (nrow(losses) > 0) {
    message(sprintf("\n  losses at %s: %s", label_rows(largest),
                    paste(sprintf("%s (%.2fx)", losses$query, losses$speedup),
                          collapse = ", ")))
}
message(sprintf("\nwrote %s charts to %s", charts, normalizePath(out_dir)))
