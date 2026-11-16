// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once

#include <cstddef>

namespace ibex::runtime {

/// Independent join passes retain their measured row floors. These thresholds
/// do not change join orientation, key capabilities, byte budgets or worker caps.
struct JoinThresholdPolicy {
    std::size_t build_min_rows = 1U << 17U;
    std::size_t probe_min_rows = 1U << 14U;
    std::size_t concat_min_rows = 1U << 16U;
    std::size_t semi_right_min_rows = 1U << 18U;

    [[nodiscard]] constexpr auto valid() const noexcept -> bool {
        return build_min_rows != 0 && probe_min_rows != 0 && concat_min_rows != 0 &&
               semi_right_min_rows != 0;
    }
};

}  // namespace ibex::runtime
