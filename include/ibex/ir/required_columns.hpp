#pragma once

#include <ibex/ir/node.hpp>

#include <map>
#include <set>
#include <string>
#include <utility>

namespace ibex::ir {

/// The set of columns a plan reads from one scanned source.
///
/// `all` means the set could not be bounded: either the plan genuinely reads
/// every column (a bare `t`, a `distinct`, a `cov`) or it contains a node this
/// pass does not model. Both must materialize the source in full.
///
/// The asymmetry is deliberate. Over-approximating costs a wider read;
/// under-approximating drops a column the plan needs and yields an error or a
/// wrong answer. So every case this pass cannot prove widens to `all`.
struct ColumnDemand {
    bool all = false;
    std::set<std::string> names;

    void add(std::string name) {
        if (!all) {
            names.insert(std::move(name));
        }
    }
    /// Widen to "every column" — the irreversible direction.
    void widen() {
        all = true;
        names.clear();
    }
    void merge(const ColumnDemand& other) {
        if (all) {
            return;
        }
        if (other.all) {
            widen();
            return;
        }
        names.insert(other.names.begin(), other.names.end());
    }
};

/// Columns demanded from each source named by a `Scan` under `root`.
/// A source absent from the map is never scanned by this plan.
///
/// This is what makes projection pushdown possible: a source that can read
/// columns selectively (Parquet) need only decode the columns the plan above
/// the scan actually references.
[[nodiscard]] auto required_columns(const Node& root) -> std::map<std::string, ColumnDemand>;

}  // namespace ibex::ir
