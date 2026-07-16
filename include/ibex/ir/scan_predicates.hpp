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
/// Run `split_scan_instances` first to give repeated scans their own identity.
///
[[nodiscard]] auto scan_predicates(const Node& root) -> ScanPredicateMap;

struct ScanInstanceSplit {
    NodePtr plan;
    /// Instance scan name -> the source it reads. Only sources scanned more
    /// than once are split; a source scanned once keeps its original name and
    /// has no entry here.
    std::map<std::string, std::string> instances;
};

/// Give each scan of `sources` that occurs more than once in the plan its own
/// instance name (`name#k`), so selection pushdown and column demand stay
/// per-scan for repeated/self-join scans instead of being abandoned. `#`
/// cannot appear in an identifier, so an instance name cannot collide with a
/// user binding. The caller must materialize each instance name into the
/// registry it interprets against.
[[nodiscard]] auto split_scan_instances(NodePtr root, const std::set<std::string>& sources)
    -> ScanInstanceSplit;

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
