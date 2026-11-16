// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once

#include <cstddef>

namespace ibex::runtime {

/// Work grains for the remaining row-range executors. Shared query admission
/// (parallel_min_rows), worker budgets and algorithm memory limits still apply.
struct OperatorGrainPolicy {
    std::size_t filter_bounds_rows = 65536;
    std::size_t update_bucket_rows = 32768;
    std::size_t window_split_rows = 32768;
    std::size_t decimal_partial_rows = 65536;
    std::size_t map_min_rows = 4096;
    std::size_t map_max_rows = 65536;
    std::size_t map_morsels_per_worker = 4;

    [[nodiscard]] constexpr auto valid() const noexcept -> bool {
        return filter_bounds_rows != 0 && update_bucket_rows != 0 && window_split_rows != 0 &&
               decimal_partial_rows != 0 && map_min_rows != 0 && map_max_rows >= map_min_rows &&
               map_morsels_per_worker != 0;
    }
};

}  // namespace ibex::runtime
