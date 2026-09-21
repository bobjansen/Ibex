// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>

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
