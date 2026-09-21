// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstdint>

namespace ibex::parquet_stats {

/// A footer min/max read as signed int64, or nothing usable.
///
/// Unsigned columns store their range in UNSIGNED order while the scans compare
/// the same bits as signed int64. A row group whose values span 2^63 therefore
/// reads back with min > max, and any decision taken from it (pruning the
/// group, deriving a span) would be about values the column does not hold.
[[nodiscard]] constexpr auto usable_signed_range(std::int64_t min, std::int64_t max) noexcept
    -> bool {
    return min <= max;
}

/// Whether a row group whose footer range is [min, max] provably holds no key
/// inside the inclusive interval [lo, hi]. False whenever the range cannot be
/// trusted, so an unusable footer costs a scan and never a row.
[[nodiscard]] constexpr auto group_excluded(std::int64_t min, std::int64_t max, std::int64_t lo,
                                            std::int64_t hi) noexcept -> bool {
    return usable_signed_range(min, max) && (max < lo || min > hi);
}

}  // namespace ibex::parquet_stats
