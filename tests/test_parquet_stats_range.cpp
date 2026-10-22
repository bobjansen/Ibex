// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "../libs/parquet/stats_range.hpp"

namespace stats = ibex::parquet_stats;

TEST_CASE("parquet stats: a group is excluded only when its range misses the filter",
          "[parquet][stats]") {
    CHECK(stats::group_excluded(0, 10, 20, 30));   // entirely below
    CHECK(stats::group_excluded(40, 50, 20, 30));  // entirely above
    CHECK_FALSE(stats::group_excluded(0, 10, 10, 30));
    CHECK_FALSE(stats::group_excluded(0, 100, 20, 30));
}

TEST_CASE("parquet stats: an unsigned range spanning 2^63 is never trusted",
          "[parquet][stats][regression]") {
    // A UINT64 row group holding 0..99998 and 2^63+3 keeps its footer range in
    // unsigned order, so read as int64 it is min = 0, max = INT64_MIN + 3. The
    // scan sees the same bits as the same signed values, so key 5 is present;
    // pruning on the inverted range would drop it.
    const std::int64_t max = std::numeric_limits<std::int64_t>::min() + 3;
    CHECK_FALSE(stats::usable_signed_range(0, max));
    CHECK_FALSE(stats::group_excluded(0, max, 5, 7));
    CHECK_FALSE(stats::group_excluded(0, max, std::numeric_limits<std::int64_t>::min() + 3,
                                      std::numeric_limits<std::int64_t>::min() + 3));
}

TEST_CASE("parquet stats: a range wholly above 2^63 still orders consistently",
          "[parquet][stats]") {
    // All values >= 2^63 are negative as int64 and still ordered by their bits.
    const auto low = std::numeric_limits<std::int64_t>::min() + 1;
    const auto high = std::numeric_limits<std::int64_t>::min() + 9;
    CHECK(stats::usable_signed_range(low, high));
    CHECK(stats::group_excluded(low, high, 0, 100));
    CHECK_FALSE(stats::group_excluded(low, high, low, low));
}

TEST_CASE("parquet stats: an unbounded side never excludes a group", "[parquet][stats]") {
    // `ts >= 100` has no upper bound and `ts <= 5` no lower one; the scan passes
    // the missing side as the int64 extreme.
    constexpr auto lo = std::numeric_limits<std::int64_t>::min();
    constexpr auto hi = std::numeric_limits<std::int64_t>::max();
    CHECK(stats::group_excluded(0, 50, 100, hi));  // whole group below the bound
    CHECK_FALSE(stats::group_excluded(0, 150, 100, hi));
    CHECK(stats::group_excluded(10, 50, lo, 5));  // whole group above the bound
    CHECK_FALSE(stats::group_excluded(0, 50, lo, 5));
    CHECK_FALSE(stats::group_excluded(0, 50, lo, hi));
}

TEST_CASE("parquet stats: a group is covered only when its whole range is inside the interval",
          "[parquet][stats]") {
    CHECK(stats::group_covered(10, 20, 0, 100));
    CHECK(stats::group_covered(0, 100, 0, 100));
    CHECK_FALSE(stats::group_covered(-1, 20, 0, 100));
    CHECK_FALSE(stats::group_covered(10, 101, 0, 100));
    // An inverted (unsigned, 2^63-spanning) range is never trusted either way.
    CHECK_FALSE(stats::group_covered(0, std::numeric_limits<std::int64_t>::min() + 3,
                                     std::numeric_limits<std::int64_t>::min(),
                                     std::numeric_limits<std::int64_t>::max()));
}

namespace {

auto merged(const std::vector<stats::ChunkStats>& chunks, std::size_t begin, std::size_t end)
    -> stats::ColumnStatsMerge {
    stats::ColumnStatsMerge out;
    for (std::size_t i = begin; i < end; ++i) {
        out.add(chunks[i]);
    }
    return out;
}

void check_same(const stats::ColumnStatsMerge& a, const stats::ColumnStatsMerge& b) {
    REQUIRE(a.has_range() == b.has_range());
    if (a.has_range()) {
        CHECK(a.low == b.low);
        CHECK(a.high == b.high);
    }
    REQUIRE(a.has_null_count() == b.has_null_count());
    if (a.has_null_count()) {
        CHECK(a.nulls == b.nulls);
    }
}

}  // namespace

// The binding-time stats merge splits a footer's row groups across workers and
// combines the partial merges. That is only sound if every split reproduces the
// file-order merge, including the chunks that abandon a column or drop its
// range or null count partway through.
TEST_CASE("parquet stats: merges over split row-group ranges combine to the serial merge",
          "[parquet][stats]") {
    const std::vector<stats::ChunkStats> shapes{
        {.present = true,
         .has_null_count = true,
         .null_count = 2,
         .has_range = true,
         .low = 5,
         .high = 9},
        {.present = true,
         .has_null_count = true,
         .null_count = 0,
         .has_range = true,
         .low = -3,
         .high = 4},
        {.present = true, .has_null_count = false, .has_range = true, .low = 20, .high = 30},
        {.present = true, .has_null_count = true, .null_count = 1, .has_range = false},
        {.present = false},
    };
    // Every sequence of up to four chunks, split at every pair of cut points.
    for (std::size_t length = 1; length <= 4; ++length) {
        std::vector<std::size_t> pick(length, 0);
        while (true) {
            std::vector<stats::ChunkStats> chunks;
            for (const auto p : pick) {
                chunks.push_back(shapes[p]);
            }
            const auto serial = merged(chunks, 0, length);
            for (std::size_t cut1 = 0; cut1 <= length; ++cut1) {
                for (std::size_t cut2 = cut1; cut2 <= length; ++cut2) {
                    auto split = merged(chunks, 0, cut1);
                    split.combine(merged(chunks, cut1, cut2));
                    split.combine(merged(chunks, cut2, length));
                    check_same(split, serial);
                }
            }
            std::size_t digit = 0;
            while (digit < length && ++pick[digit] == shapes.size()) {
                pick[digit++] = 0;
            }
            if (digit == length) {
                break;
            }
        }
    }
}

TEST_CASE("parquet stats: a chunk without statistics abandons its column", "[parquet][stats]") {
    stats::ColumnStatsMerge merge;
    merge.add({.present = true,
               .has_null_count = true,
               .null_count = 0,
               .has_range = true,
               .low = 1,
               .high = 2});
    merge.add({.present = false});
    CHECK_FALSE(merge.has_range());
    CHECK_FALSE(merge.has_null_count());
}

TEST_CASE("parquet stats: a chunk without a range keeps the null count, and vice versa",
          "[parquet][stats]") {
    stats::ColumnStatsMerge no_range;
    no_range.add({.present = true,
                  .has_null_count = true,
                  .null_count = 3,
                  .has_range = true,
                  .low = 1,
                  .high = 2});
    no_range.add({.present = true, .has_null_count = true, .null_count = 4, .has_range = false});
    CHECK_FALSE(no_range.has_range());
    REQUIRE(no_range.has_null_count());
    CHECK(no_range.nulls == 7);

    stats::ColumnStatsMerge no_nulls;
    no_nulls.add(
        {.present = true, .has_null_count = false, .has_range = true, .low = 1, .high = 2});
    no_nulls.add({.present = true,
                  .has_null_count = true,
                  .null_count = 4,
                  .has_range = true,
                  .low = -5,
                  .high = 0});
    REQUIRE(no_nulls.has_range());
    CHECK(no_nulls.low == -5);
    CHECK(no_nulls.high == 2);
    CHECK_FALSE(no_nulls.has_null_count());
}
