#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <map>
#include <optional>
#include <string>

namespace ibex::ir {

/// Metadata known before a source is decoded.  Lazy readers already provide
/// the exact row count from file metadata; other sources may be absent.
using SourceRowCounts = std::map<std::string, std::size_t>;

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
[[nodiscard]] auto estimate_cardinality(const Node& root, const SourceRowCounts& sources,
                                        CardinalityOptions options = {}) -> CardinalityEstimate;

}  // namespace ibex::ir
