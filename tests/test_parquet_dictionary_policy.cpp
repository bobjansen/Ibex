// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

#include "../libs/parquet/dictionary_policy.hpp"

namespace policy = ibex::parquet_dict;

TEST_CASE("dictionary policy keeps a small dictionary whatever it compresses",
          "[parquet][dictionary]") {
    // nation: 25 names for 25 rows. It compresses nothing, and it must still be
    // kept -- 25 entries cost nothing to intern, and the queries that group by
    // n_name hash by code instead of by string. Gating this on compression
    // alone regressed q09 by 14%.
    CHECK(policy::worth_keeping(25, 25));
    CHECK(policy::worth_keeping(5, 5));           // region
    CHECK(policy::worth_keeping(6000, 3000000));  // o_clerk
}

TEST_CASE("dictionary policy keeps a large dictionary that earns its keep",
          "[parquet][dictionary]") {
    // c_phone: 55k entries standing in for 300k rows. Too many to call small,
    // but each entry covers five rows, so every row-wise string operation above
    // runs once per entry instead of once per row.
    CHECK(policy::worth_keeping(55296, 300000));
    CHECK(policy::worth_keeping(410665, 12000000));  // l_comment
}

TEST_CASE("dictionary policy drops a dictionary with an entry per row", "[parquet][dictionary]") {
    // supplier's s_name/s_address/s_phone: 20000 entries for 20000 rows. Large
    // enough to cost real interning and compressing nothing in return. This is
    // the case that made a 20,000-row read take 9ms per column.
    CHECK_FALSE(policy::worth_keeping(20000, 20000));
    CHECK_FALSE(policy::worth_keeping(16384, 20000));  // s_comment
}

TEST_CASE("dictionary policy uses actual counts at the retention boundaries",
          "[parquet][dictionary]") {
    CHECK(policy::worth_keeping(8192, 8192));
    CHECK_FALSE(policy::worth_keeping(8193, 8193));
    CHECK(policy::worth_keeping(10000, 20000));
    CHECK_FALSE(policy::worth_keeping(10001, 20000));
    CHECK_FALSE(policy::worth_keeping(10001, 20001));
    CHECK(policy::worth_keeping(100, 10000));  // also true for arbitrarily long labels
}
