#pragma once

#include <ibex/ir/node.hpp>

#include <optional>
#include <string>
#include <vector>

namespace ibex::runtime {

/// The order-sensitive metadata a `Table` carries: its sort `ordering` and, for
/// a TimeFrame, its `time_index`. Split out from `Table` so it can be derived
/// and reasoned about on its own — the runtime multithreading plan (Phase 0
/// item 3) requires these to be a planner-derived property of a parallel
/// *island*, not an accident of whichever morsel finishes first. Worker results
/// carry only sequence/rows/schema; the ordered merger derives one
/// `TableProperties` for the island and attaches it once to the final table.
///
/// Until that merger exists, this same type and the derivation helpers below are
/// what the serial operators use, so there is a single set of metadata rules
/// rather than a serial copy and a parallel copy that can drift.
struct TableProperties {
    std::optional<std::vector<ir::OrderKey>> ordering;
    std::optional<std::string> time_index;
};

}  // namespace ibex::runtime
