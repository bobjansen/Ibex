#pragma once

#include <ibex/ir/node.hpp>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace ibex::ir {

using ScanPredicateMap = std::map<std::string, std::vector<Expr>>;

/// Collect row-local filter conjuncts that can be evaluated over a named Scan,
/// optionally through column-only Project nodes. A source is returned only
/// when it occurs exactly once in the plan: the materialized table registry is
/// keyed by source name, so applying
/// one occurrence's selection to a repeated/self-join scan would be unsound.
///
[[nodiscard]] auto scan_predicates(const Node& root) -> ScanPredicateMap;

/// Remove row-local filters which have already been applied by a lazy source.
/// `applied_sources` must contain only sources for which the caller actually
/// materialized the selection. The implementation repeats the scan-predicate
/// proof, so a repeated source or a partial/non-local predicate remains in the
/// plan even if it is named in `applied_sources`.
///
/// Fused filter nodes are de-fused while retaining their project, update, or
/// limit operation.
[[nodiscard]] auto remove_applied_scan_filters(NodePtr root,
                                               const std::set<std::string>& applied_sources)
    -> NodePtr;

}  // namespace ibex::ir
