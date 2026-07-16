#pragma once

#include <ibex/ir/node.hpp>

#include <set>
#include <string>
#include <vector>

namespace ibex::ir {

/// A deterministic table-producing extern call hoisted out of a relational
/// plan.  `source_name` replaces each matching call with a ScanNode; callers
/// own the corresponding runtime source binding.
struct ExternSource {
    std::string source_name;
    std::string callee;
    std::vector<Expr> args;
};

/// Replace calls to eligible table readers with named scans and coalesce
/// repeated calls having the same literal arguments.  This exposes a whole
/// script as one plan: required-column and predicate passes can now compute a
/// single demand for every physical source.
///
/// Only calls whose arguments are literals are hoisted.  A dynamic argument
/// could depend on evaluation order or a scalar binding, so keeping it as an
/// ExternCall is the conservative choice.
[[nodiscard]] auto hoist_extern_sources(NodePtr root, const std::set<std::string>& eligible_callees)
    -> std::pair<NodePtr, std::vector<ExternSource>>;

}  // namespace ibex::ir
