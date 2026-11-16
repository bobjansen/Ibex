// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once

#include <algorithm>
#include <cstddef>
#include <initializer_list>

namespace ibex::runtime {

/// Runtime admission and grain policy for aggregate Discovery. Floors retain
/// their strategy-specific defaults; zero is invalid. Shape, cardinality and
/// state-transition checks remain with the operator. Accumulation has its own
/// policy and does not consume these overrides.
struct AggregateDiscoveryPolicy {
    std::size_t radix_min_rows = 1U << 18U;
    std::size_t packed_min_rows = 1U << 15U;
    std::size_t int_owned_min_rows = 1U << 16U;
    std::size_t pair_owned_min_rows = 1U << 16U;
    std::size_t sorted_min_rows = 1U << 18U;
    std::size_t key_fill_range_groups = 1U << 16U;
    std::size_t sorted_range_rows = 1U << 16U;
    std::size_t async_hot_job_rows = 1U << 20U;
    std::size_t pair_owned_max_first_chunk_rows = 1U << 21U;

    /// Conservative phase admission: it must permit every eligible strategy.
    /// Grains and upper cutoffs do not impose lower bounds on admission.
    [[nodiscard]] constexpr auto admission_floor() const noexcept -> std::size_t {
        return std::min({radix_min_rows, packed_min_rows, int_owned_min_rows, pair_owned_min_rows,
                         sorted_min_rows});
    }

    [[nodiscard]] constexpr auto valid() const noexcept -> bool {
        return admission_floor() != 0 && key_fill_range_groups != 0 && sorted_range_rows != 0 &&
               async_hot_job_rows != 0 && pair_owned_max_first_chunk_rows != 0;
    }

    auto operator==(const AggregateDiscoveryPolicy&) const -> bool = default;
};

}  // namespace ibex::runtime
