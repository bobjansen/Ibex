// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Validation of a decode row selection, split so that the per-row-group part
// can run inside the decode tasks instead of on the calling thread.
//
// A selection is valid when it is strictly increasing and every index is below
// the source's row count. The whole-selection check is two full passes on one
// thread; `direct_decode_table` used to run it before every selected decode
// (up to ~6 ms per call on SF-8 lineitem, serial while the pool waited). The
// split form checks the same property in pieces: the slice of the selection
// that falls in each row group is checked by that group's task, and the parts
// before the first group and after the last (normally empty) by the caller.
// `split_check_equals_whole_check` in tests/test_parquet_selection_check.cpp
// pins that the two accept exactly the same selections.
//
// Arrow-free on purpose, so the unit tests can include it (as stats_range.hpp).

#include <algorithm>
#include <cstddef>
#include <functional>
#include <span>
#include <vector>

namespace ibex::parquet_selection {

/// The old check: strictly increasing, and inside `[0, source_rows)`.
[[nodiscard]] inline auto whole_valid(std::span<const std::size_t> selection,
                                      std::size_t source_rows) -> bool {
    return std::ranges::adjacent_find(selection, std::ranges::greater_equal{}) == selection.end() &&
           (selection.empty() || selection.back() < source_rows);
}

/// Where each group's slice starts: `bounds[k]..bounds[k+1]` is group k's slice,
/// `bounds.front()` is where the first group's slice starts and `bounds.back()`
/// where the last one ends. `first` is the first group's first source row;
/// `group_ends[k]` is group k's exclusive end row. These are the positions a
/// sorted selection would have; on an unsorted one they are merely monotone,
/// which is all the checks below need.
[[nodiscard]] inline auto slice_bounds(std::span<const std::size_t> selection, std::size_t first,
                                       std::span<const std::size_t> group_ends)
    -> std::vector<std::size_t> {
    std::vector<std::size_t> bounds;
    bounds.reserve(group_ends.size() + 1);
    auto it = std::ranges::lower_bound(selection, first);
    bounds.push_back(static_cast<std::size_t>(it - selection.begin()));
    for (const std::size_t end : group_ends) {
        it = std::ranges::lower_bound(it, selection.end(), end);
        bounds.push_back(static_cast<std::size_t>(it - selection.begin()));
    }
    return bounds;
}

/// The pieces outside every group: before the first (increasing, below `first`)
/// and after the last (increasing, at or past `last_end`, below `source_rows`).
[[nodiscard]] inline auto outside_valid(std::span<const std::size_t> selection,
                                        const std::vector<std::size_t>& bounds, std::size_t first,
                                        std::size_t last_end, std::size_t source_rows) -> bool {
    const auto head = selection.first(bounds.front());
    const auto tail = selection.subspan(bounds.back());
    const bool head_ok =
        std::ranges::adjacent_find(head, std::ranges::greater_equal{}) == head.end() &&
        (head.empty() || head.back() < first);
    const bool tail_ok =
        std::ranges::adjacent_find(tail, std::ranges::greater_equal{}) == tail.end() &&
        (tail.empty() || (tail.front() >= last_end && tail.back() < source_rows));
    return head_ok && tail_ok;
}

/// One group's slice `[lo, hi)`: every index inside `[first, end)`, strictly
/// increasing. Slices that pass, laid end to end in group order between valid
/// outside pieces, form a strictly increasing selection.
[[nodiscard]] inline auto slice_valid(std::span<const std::size_t> selection, std::size_t lo,
                                      std::size_t hi, std::size_t first, std::size_t end) -> bool {
    for (std::size_t i = lo; i < hi; ++i) {
        const std::size_t value = selection[i];
        if (value < first || value >= end || (i > lo && value <= selection[i - 1])) {
            return false;
        }
    }
    return true;
}

}  // namespace ibex::parquet_selection
