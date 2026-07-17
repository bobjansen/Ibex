#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstddef>
#include <map>
#include <optional>
#include <string>

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
struct SourceStats {
    SourceRowCounts rows;
    SourceSchemas schemas;
    SourceColumnDistinct distinct;
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
};

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
