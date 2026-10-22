// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstddef>
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

/// Whether every value of a row group whose footer range is [min, max] lies
/// inside the inclusive interval [lo, hi]. False for an unusable range.
[[nodiscard]] constexpr auto group_covered(std::int64_t min, std::int64_t max, std::int64_t lo,
                                           std::int64_t hi) noexcept -> bool {
    return usable_signed_range(min, max) && min >= lo && max <= hi;
}

/// What one column chunk's footer statistics say, extracted from Arrow's
/// metadata (parquet.hpp) so the merge below can be tested without a file.
struct ChunkStats {
    /// Statistics are set at all. A chunk without them abandons its column.
    bool present = false;
    bool has_null_count = false;
    std::size_t null_count = 0;
    /// An integer min/max that `usable_signed_range` accepts.
    bool has_range = false;
    std::int64_t low = 0;
    std::int64_t high = 0;
};

/// One column's chunk statistics merged into a whole-file answer.
///
/// Every field is an OR, an AND, a sum, a min or a max, so partial merges over
/// disjoint row-group ranges `combine` into exactly the merge of all chunks in
/// file order -- which is what lets a footer with hundreds of row groups be
/// merged by several workers at binding time.
///
/// Any chunk without statistics abandons the column entirely: a range merged
/// from a subset of the row groups is not a conservative answer, it is a wrong
/// one -- the planner would read it as "no value lies outside this".
struct ColumnStatsMerge {
    bool abandoned = false;
    bool nulls_known = true;
    bool range_known = true;
    bool any_range = false;
    std::size_t nulls = 0;
    std::int64_t low = 0;
    std::int64_t high = 0;

    void add(const ChunkStats& chunk) noexcept {
        if (abandoned) {
            return;
        }
        if (!chunk.present) {
            abandoned = true;
            return;
        }
        if (chunk.has_null_count) {
            nulls += chunk.null_count;
        } else {
            nulls_known = false;
        }
        if (!range_known) {
            return;
        }
        if (!chunk.has_range) {
            range_known = false;
            return;
        }
        widen(chunk.low, chunk.high);
    }

    void combine(const ColumnStatsMerge& other) noexcept {
        abandoned = abandoned || other.abandoned;
        nulls += other.nulls;
        nulls_known = nulls_known && other.nulls_known;
        range_known = range_known && other.range_known;
        if (other.any_range) {
            widen(other.low, other.high);
        }
    }

    /// A whole-file range: every chunk had one, and there was a chunk.
    [[nodiscard]] auto has_range() const noexcept -> bool {
        return !abandoned && range_known && any_range;
    }
    [[nodiscard]] auto has_null_count() const noexcept -> bool { return !abandoned && nulls_known; }

   private:
    void widen(std::int64_t chunk_low, std::int64_t chunk_high) noexcept {
        if (!any_range || chunk_low < low) {
            low = chunk_low;
        }
        if (!any_range || chunk_high > high) {
            high = chunk_high;
        }
        any_range = true;
    }
};

}  // namespace ibex::parquet_stats
