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
    /// Non-empty when a `by` clause upstream established that these keys
    /// partition the rows, so an adjacent row may belong to a different group.
    /// That makes an *unpartitioned* order-dependent call (lag/lead/cum*/
    /// rolling_*) read across a partition — see check_row_order in update.cpp.
    ///
    /// Two layouts qualify, and the diagnostic does not need to tell them
    /// apart. `window` + `by` emits group-major runs, where only the last row
    /// of each run reads across. `resample` + `by` emits time-major rows with
    /// the groups interleaved, where EVERY row reads across. The second is the
    /// more destructive of the two and the easier to write by accident.
    std::vector<std::string> grouped_by;
};

}  // namespace ibex::runtime
