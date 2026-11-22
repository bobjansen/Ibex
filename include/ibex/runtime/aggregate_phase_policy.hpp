// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once

#include <algorithm>
#include <cstddef>
#include <initializer_list>
#include <limits>

namespace ibex::runtime {

/// Strategy thresholds for Accumulation and FinalOrdering. Units are explicit:
/// rows are input rows, groups are output groups, and runs are ordered summaries.
/// Grains depend on data and policy alone, never on worker count.
struct AggregatePhasePolicy {
    std::size_t accumulation_morsel_rows = 1U << 16U;
    std::size_t sorted_min_rows = 1U << 18U;
    std::size_t sorted_range_rows = 1U << 16U;
    std::size_t dense_gid_min_rows = 1U << 20U;
    std::size_t owned_range_min_rows = 1U << 20U;
    std::size_t owned_range_min_groups = 1U << 16U;
    std::size_t owned_range_morsel_rows = 1U << 16U;
    std::size_t owned_range_window_rows = 1U << 22U;
    std::size_t slot_init_min_bytes = 4U << 20U;
    std::size_t merge_min_groups = 1U << 17U;
    std::size_t merge_groups_per_worker = 4096;
    std::size_t ordered_min_runs = 1U << 16U;
    std::size_t ordered_runs_per_worker = 8192;
    /// A streamed input's chunks smaller than this are joined, up to it,
    /// before Discovery; 0 keeps every chunk on its own. The same floor as
    /// `owned_range_min_rows`: the many-group strategies start there.
    std::size_t stream_batch_rows = 1U << 20U;

    [[nodiscard]] constexpr auto accumulation_floor() const noexcept -> std::size_t {
        const auto two_morsels =
            accumulation_morsel_rows > std::numeric_limits<std::size_t>::max() / 2
                ? std::numeric_limits<std::size_t>::max()
                : accumulation_morsel_rows * 2;
        return std::min({two_morsels, sorted_min_rows, dense_gid_min_rows, owned_range_min_rows});
    }
    [[nodiscard]] constexpr auto final_ordering_floor() const noexcept -> std::size_t {
        return std::min(merge_min_groups, ordered_min_runs);
    }
    [[nodiscard]] constexpr auto valid() const noexcept -> bool {
        return accumulation_morsel_rows != 0 && sorted_min_rows != 0 && sorted_range_rows != 0 &&
               dense_gid_min_rows != 0 && owned_range_min_rows != 0 &&
               owned_range_min_groups != 0 && owned_range_morsel_rows != 0 &&
               owned_range_window_rows != 0 && slot_init_min_bytes != 0 && merge_min_groups != 0 &&
               merge_groups_per_worker != 0 && ordered_min_runs != 0 &&
               ordered_runs_per_worker != 0;
    }
};

}  // namespace ibex::runtime
