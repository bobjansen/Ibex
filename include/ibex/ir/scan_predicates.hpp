#pragma once

#include <ibex/ir/node.hpp>

#include <map>
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
/// Filter nodes remain in the plan. The runtime therefore applies each
/// predicate again after the selected read, preserving correctness while scan
/// filtering is a partial optimization.
[[nodiscard]] auto scan_predicates(const Node& root) -> ScanPredicateMap;

}  // namespace ibex::ir
