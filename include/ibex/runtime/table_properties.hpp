// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// What became of one metadata-carrying column across a single operator.
///
/// The three cases are distinguished rather than collapsed into "survived or
/// not", because `Dropped` and `Overwritten` are the same for an ordering key
/// and OPPOSITE for a grouping key. A dropped key cannot be named at all, so the
/// grouping claim has to go with it. An overwritten key is still a column, and —
/// crucially — the rows have not moved: they sit in exactly the runs they sat in
/// before, so the boundary an unpartitioned `lag`/`rolling_*` would read across
/// is still there. Treating that as "gone" disarms the guard in
/// `check_row_order` over a hazard that is still live.
class KeyFate {
   public:
    /// Present and still meaning what it meant. `name` carries a rename.
    [[nodiscard]] static auto kept(std::string name) -> KeyFate {
        return KeyFate{Kind::Kept, std::move(name)};
    }

    /// Still a column under the same name, but an update rewrote its values. No
    /// longer a valid sort key; still a valid statement about the row layout.
    [[nodiscard]] static auto overwritten() -> KeyFate {
        return KeyFate{Kind::Overwritten, {}};
    }

    /// Gone from the output — projected away, aggregated over, or otherwise not
    /// nameable in the result.
    [[nodiscard]] static auto dropped() -> KeyFate {
        return KeyFate{Kind::Dropped, {}};
    }

    [[nodiscard]] auto is_kept() const noexcept -> bool { return kind_ == Kind::Kept; }
    [[nodiscard]] auto is_overwritten() const noexcept -> bool {
        return kind_ == Kind::Overwritten;
    }
    [[nodiscard]] auto is_dropped() const noexcept -> bool { return kind_ == Kind::Dropped; }

    /// The output name. Only meaningful when `is_kept()`.
    [[nodiscard]] auto name() const noexcept -> const std::string& { return name_; }

   private:
    enum class Kind : std::uint8_t { Kept, Overwritten, Dropped };

    KeyFate(Kind kind, std::string name) : kind_(kind), name_(std::move(name)) {}

    Kind kind_;
    std::string name_;
};

/// Where a sort key, the time index, or a grouping key ends up after one
/// operator. One hook unifies the three column-level metadata rules: presence
/// (project/filter), overwrite (update), and renaming (rename).
using KeyColumnFate = std::function<KeyFate(const std::string&)>;

/// What an operator did to the *rows*, as opposed to what `KeyColumnFate` says
/// it did to the columns. Both are needed: `fate` alone cannot distinguish a
/// filter (every surviving key still describes the output) from a sort (the
/// columns are untouched, the ordering claim is not).
///
/// Stating this is mandatory rather than defaulted on purpose. An operator whose
/// case is not described by the derivation is the one most likely to hand-roll
/// its metadata and get it wrong, so every caller is made to say which of the
/// four it is.
enum class RowTransform : std::uint8_t {
    /// Same rows, same order: update, project, rename, ascribe.
    Preserve,
    /// Rows removed, survivors keep their relative order: filter, head, tail,
    /// distinct, semi/anti join. Dropping rows cannot merge two partitions or
    /// unsort a sorted column, so this derives exactly like `Preserve`.
    Subset,
    /// Same rows, new order: order/sort. The input's ordering claim is void and
    /// the operator states the new one itself — it knows the keys it sorted by,
    /// and the derivation cannot infer them.
    Reorder,
    /// Rows built from groups or from more than one input: aggregate, join,
    /// resample, melt/dcast. No claim about the input's row layout survives, so
    /// everything is cleared and the operator states what it establishes afresh.
    Recombine,
};

/// The order-sensitive metadata a `Table` carries: its sort `ordering` and, for
/// a TimeFrame, its `time_index`. Split out from `Table` so it can be derived
/// and reasoned about on its own — the runtime multithreading plan (Phase 0
/// item 3) requires these to be a planner-derived property of a parallel
/// *island*, not an accident of whichever morsel finishes first. Worker results
/// carry only sequence/rows/schema; the ordered merger derives one
/// `TableProperties` for the island and attaches it once to the final table.
///
/// Until that merger exists, this same type and the derivation below are what
/// the serial operators use, so there is a single set of metadata rules rather
/// than a serial copy and a parallel copy that can drift.
///
/// **The fields are private on purpose.** A value can only be built by naming
/// the way an operator established it — `derive`, `sorted_by`, `time_frame`,
/// `grouped`, or `none`. Assembling one field at a time is what produced the
/// bugs this type exists to prevent: an operator that copies `time_index`
/// through and forgets that its rows moved has written something false, and
/// nothing in a plain struct stops it. Being made to pick a constructor is being
/// made to answer the question.
class TableProperties {
   public:
    /// Nothing is claimed about the rows.
    TableProperties() = default;

    /// Explicit form of the default, for a `Recombine` operator that wants to
    /// say so rather than let a bare `{}` imply it.
    [[nodiscard]] static auto none() -> TableProperties { return {}; }

    /// The operator sorted the rows and knows by which keys.
    [[nodiscard]] static auto sorted_by(std::vector<ir::OrderKey> keys) -> TableProperties {
        TableProperties props;
        props.ordering_ = std::move(keys);
        return props;
    }

    /// The operator established a time index (`as_timeframe`, `resample`). A
    /// TimeFrame is time-ascending by construction, so the ordering follows.
    [[nodiscard]] static auto time_frame(std::string index) -> TableProperties {
        TableProperties props;
        props.ordering_ = std::vector<ir::OrderKey>{{.name = index, .ascending = true}};
        props.time_index_ = std::move(index);
        return props;
    }

    /// The operator laid the rows out by group (`window`/`resample` + `by`).
    /// See `grouped_by()` for what the claim means downstream.
    [[nodiscard]] static auto grouped(std::vector<std::string> keys) -> TableProperties {
        TableProperties props;
        props.grouped_by_ = std::move(keys);
        return props;
    }

    /// Read metadata back off a table that already carries it. The one
    /// constructor that names no intent, because it makes no new claim — it
    /// recovers one already made. It exists only while `Table` still stores the
    /// three fields itself; it is not a way to assemble a claim field by field.
    [[nodiscard]] static auto recovered(std::optional<std::vector<ir::OrderKey>> ordering,
                                        std::optional<std::string> time_index,
                                        std::vector<std::string> grouped_by) -> TableProperties {
        TableProperties props;
        props.ordering_ = std::move(ordering);
        props.time_index_ = std::move(time_index);
        props.grouped_by_ = std::move(grouped_by);
        return props;
    }

    /// Derive an operator's output metadata from its input, applying `fate` to
    /// each ordering key, the time index, and each grouping key, then narrowing
    /// the result by what `transform` says happened to the rows.
    ///
    /// Ordering is all-or-nothing: cleared unless every key survives
    /// (rename-mapped, un-overwritten). Losing the time index also clears
    /// ordering — a by-time ordering is void without its column (for a TimeFrame
    /// the sole key *is* the time index, so this is already implied, but it
    /// keeps the rule self-contained for non-TimeFrame callers).
    ///
    /// `grouped_by` survives every transform but `Recombine`, including
    /// `Reorder`. That is deliberate: it is a hazard flag, not a capability. It
    /// records that a `by` upstream made adjacent rows potentially cross a
    /// partition, and a sort that reshuffles the rows does not make an
    /// order-dependent call over them any safer. Clearing it there would disarm
    /// the guard in `check_row_order`.
    [[nodiscard]] static auto derive(const TableProperties& input, const KeyColumnFate& fate,
                                     RowTransform transform) -> TableProperties;

    /// Add an ordering to properties that carry the rest. For an operator that
    /// derives what it can and then states the order it imposed.
    [[nodiscard]] auto with_ordering(std::vector<ir::OrderKey> keys) const -> TableProperties {
        TableProperties props = *this;
        props.ordering_ = std::move(keys);
        return props;
    }

    /// Add a grouping claim to properties that carry the rest.
    [[nodiscard]] auto with_grouping(std::vector<std::string> keys) const -> TableProperties {
        TableProperties props = *this;
        props.grouped_by_ = std::move(keys);
        return props;
    }

    /// This value with the TimeFrame invariant re-established: a table with a
    /// time index always states an ordering, defaulting to that index ascending.
    ///
    /// It only FILLS IN a missing ordering; it never overwrites one. An operator
    /// that states an ordering knows something this cannot infer, and the
    /// exceptions are not rare enough to enumerate: `window`/`update` + `by`
    /// leave the rows group-major, ordered (group keys..., time); `resample` +
    /// `by` interleaves the groups; and `order { symbol asc }` over a TimeFrame
    /// legitimately produces (symbol, ts), which is why sorting a TimeFrame by
    /// anything else is allowed at all. Overwriting any of those with "time
    /// index ascending" states something false about the rows.
    [[nodiscard]] auto normalized() const -> TableProperties {
        if (!time_index_.has_value() || ordering_.has_value()) {
            return *this;
        }
        TableProperties out = *this;
        out.ordering_ = std::vector<ir::OrderKey>{{.name = *time_index_, .ascending = true}};
        return out;
    }

    [[nodiscard]] auto ordering() const noexcept
        -> const std::optional<std::vector<ir::OrderKey>>& {
        return ordering_;
    }

    /// True when the rows are already in `requested` order, so sorting by it
    /// would move nothing.
    ///
    /// The test is prefix containment, which is the exact rule: rows ordered by
    /// (a, b) are ordered by (a) — ties on `a` are merely broken further — while
    /// rows ordered by (a) say nothing about (a, b). Directions must match key
    /// for key; a descending claim does not satisfy an ascending request.
    ///
    /// This decides an O(1) elision of an O(n log n) sort, so it has to be
    /// conservative in one direction only: a claim is made by the operator that
    /// laid the rows out, and every constructor of this type requires naming
    /// how. Answering "not satisfied" when it is costs a sort; answering
    /// "satisfied" when it is not returns wrong data.
    [[nodiscard]] auto satisfies(const std::vector<ir::OrderKey>& requested) const -> bool {
        if (!ordering_.has_value() || requested.size() > ordering_->size()) {
            return false;
        }
        for (std::size_t i = 0; i < requested.size(); ++i) {
            if ((*ordering_)[i].name != requested[i].name ||
                (*ordering_)[i].ascending != requested[i].ascending) {
                return false;
            }
        }
        return true;
    }
    [[nodiscard]] auto time_index() const noexcept -> const std::optional<std::string>& {
        return time_index_;
    }
    /// Non-empty when a `by` clause upstream established that these keys
    /// partition the rows, so an adjacent row may belong to a different group.
    /// That makes an *unpartitioned* order-dependent call (lag/lead/cum*/
    /// rolling_*) read across a partition — see check_row_order in update.cpp.
    ///
    /// Two layouts qualify, and the diagnostic does not need to tell them apart.
    /// `window` + `by` emits group-major runs, where only the last row of each
    /// run reads across. `resample` + `by` emits time-major rows with the groups
    /// interleaved, where EVERY row reads across. The second is the more
    /// destructive of the two and the easier to write by accident.
    [[nodiscard]] auto grouped_by() const noexcept -> const std::vector<std::string>& {
        return grouped_by_;
    }

   private:
    std::optional<std::vector<ir::OrderKey>> ordering_;
    std::optional<std::string> time_index_;
    std::vector<std::string> grouped_by_;
};

}  // namespace ibex::runtime
