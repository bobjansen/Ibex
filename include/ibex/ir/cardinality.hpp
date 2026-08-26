// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace ibex::ir {

/// Metadata known before a source is decoded.  Lazy readers already provide
/// the exact row count from file metadata; other sources may be absent.
using SourceRowCounts = std::map<std::string, std::size_t>;

/// Estimated distinct values per column of one source, and the same per source.
///
/// These are ESTIMATES and nothing sound may be concluded from them. The driver
/// derives them from source metadata — for Parquet, `min(rows, max - min + 1)`
/// on an integer column, which is exact for a dense key (`l_suppkey`: 10,000)
/// and can read far high for a sparse one (`l_orderkey`: 6M over 1.5M actual).
/// They exist to rank join orders, where being roughly right is the whole job;
/// a *proof* about distinctness comes from `SchemaInfo::unique_keys` instead.
using ColumnDistinct = std::map<std::string, std::size_t>;
using SourceColumnDistinct = std::map<std::string, ColumnDistinct>;

/// What the planner knows about the sources a plan reads, before any of it is
/// decoded. Bundled because these three are always wanted together and are
/// threaded through every costing entry point.
/// One relation's real, data-sampled statistics -- a bounded decode of a
/// single unit (e.g. one Parquet row group), not the whole source. Real
/// numbers from actual data are the only way to see what footer min/max
/// cannot: a filter's true selectivity, and a sparse/skip-patterned key's
/// true distinct count (a footer span on `l_orderkey` reads far higher than
/// its true distinct count -- TPC-H's order-key generator skips values --
/// which is what made an earlier unguarded join-order attempt regress q09;
/// see join_order.cpp's kMaxReorderRelations for the full history).
struct RelationSample {
    std::size_t sampled_rows = 0;
    /// Present only when a predicate was given and evaluated successfully.
    std::optional<std::size_t> predicate_passed;
    /// Distinct values observed per requested column, present only for
    /// columns the sampler could actually decode and count.
    std::map<std::string, std::size_t> distinct;
};

/// Samples `source` for a caller costing a join order past what footer
/// statistics alone can safely support. `predicate`, if non-null, is
/// evaluated against the sample and the RESULT's rows (not `columns`'
/// distinct counts, which are then counted on the FILTERED sample -- callers
/// wanting raw, unfiltered distinct counts pass `predicate = nullptr`).
/// Returns nullopt when the source cannot be sampled at all (not lazy, no
/// units, decode failed) -- callers must fall back to the heuristic estimate
/// for such a source, never treat nullopt as "zero selectivity" or "no
/// distinct values".
using RelationSamplerFn = std::function<std::optional<RelationSample>(
    const std::string& source, const Expr* predicate, const std::vector<std::string>& columns)>;

struct SourceStats {
    SourceRowCounts rows;
    SourceSchemas schemas;
    SourceColumnDistinct distinct;
    /// Optional: see RelationSamplerFn. Unset (default) means "no sampling
    /// available", the same as it always was -- every existing caller that
    /// default-constructs SourceStats keeps the pure footer/heuristic path.
    RelationSamplerFn sample;
};

struct CardinalityEstimate {
    std::optional<std::size_t> rows;
    /// True when `rows` was derived from a planning heuristic rather than
    /// source metadata or an exact cardinality-preserving operator.
    bool heuristic = false;
};

struct CardinalityOptions {
    /// Conservative default used until a source supplies predicate statistics.
    /// It affects planning cost only, never query semantics.
    double filter_selectivity = 0.25;

    /// Selectivity of the filters a source has already ABSORBED into its scan.
    ///
    /// `remove_applied_scan_filters` deletes the Filter nodes whose predicates
    /// were pushed into a scan's decode, so a plan walked after that step shows
    /// every scan at its table's full size and estimates a filtered relation as
    /// if nothing had filtered it. Supplying the absorbed selectivity here
    /// restores the estimate the pre-absorption tree would have produced. Empty
    /// (the default) means nothing was absorbed, which is the case for every
    /// caller that runs before that step.
    std::map<std::string, double> absorbed_scan_selectivity;
};

/// `filter_selectivity` raised to the predicate's conjunct count, treating each
/// conjunct as an independent restriction (the standard independence
/// assumption). Exposed so a caller that absorbed a filter into a scan can
/// reconstruct the selectivity the Filter node would have contributed.
[[nodiscard]] auto compound_selectivity(const Expr& predicate, double filter_selectivity = 0.25)
    -> double;

/// Estimate output rows for a logical plan. Unknown inputs remain unknown;
/// this is deliberately more useful than inventing a global table-size default
/// when a planner cannot establish a source's scale.
///
/// `schemas` is consulted only for the unique constraints an inner join's
/// estimate needs (see the Join arm); pass the same map given to
/// `infer_schema`. Without it, inner joins simply go unestimated.
[[nodiscard]] auto estimate_cardinality(const Node& root, const SourceRowCounts& sources,
                                        const SourceSchemas& schemas = {},
                                        CardinalityOptions options = {}) -> CardinalityEstimate;

/// Estimated distinct values of `column` in `node`'s output, or nullopt when
/// nothing supports an answer — which a caller must treat as "do not guess",
/// not as "few".
///
/// Two sources, best first. A PROOF: when the node's schema is unique on
/// `{column}`, every row holds a different value, so distinct == rows exactly
/// (`SchemaInfo::unique_keys`, which is where a grouped aggregate's key count
/// comes from). Otherwise the source metadata in `stats.distinct`, followed
/// down through the row-wise operators — through renames, since
/// `select { o_orderkey = l_orderkey }` asks about a different column than it
/// answers about — and capped by the node's own row estimate, since no operator
/// here invents values and none can leave more distinct values than rows.
[[nodiscard]] auto distinct_estimate(const Node& node, const std::string& column,
                                     const SourceStats& stats) -> std::optional<std::size_t>;

}  // namespace ibex::ir
