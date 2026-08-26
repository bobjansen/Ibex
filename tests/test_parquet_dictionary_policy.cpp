// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

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

TEST_CASE("dictionary policy settles the common cases from byte size alone",
          "[parquet][dictionary]") {
    // Reading a dictionary page is paid by every query that opens the file,
    // including those that never touch the column, so the bytes have to settle
    // as much as possible. These are the real SF-2 figures.
    CHECK(policy::verdict_from_bytes(293, 25) == policy::Verdict::Keep);             // n_name
    CHECK(policy::verdict_from_bytes(3707, 400000) == policy::Verdict::Keep);        // p_type
    CHECK(policy::verdict_from_bytes(114057, 3000000) == policy::Verdict::Keep);     // o_clerk
    CHECK(policy::verdict_from_bytes(12618454, 12000000) == policy::Verdict::Keep);  // l_comment

    CHECK(policy::verdict_from_bytes(440020, 20000) == policy::Verdict::Dense);  // s_name
    CHECK(policy::verdict_from_bytes(579071, 20000) == policy::Verdict::Dense);  // s_address
    CHECK(policy::verdict_from_bytes(380020, 20000) == policy::Verdict::Dense);  // s_phone
}

TEST_CASE("dictionary policy reads the count only when the bounds straddle",
          "[parquet][dictionary]") {
    // A dictionary big enough that its widest and narrowest readings disagree
    // is the only kind worth opening. Construct one directly: 200000 bytes over
    // 500000 rows spans "at most 40000 entries" (not worth it: 80000 > 50000)
    // and "at least 10000" (worth it: 20000 <= 500000).
    CHECK(policy::most_entries_in(200000) == 40000);
    CHECK(policy::fewest_entries_in(200000) == 10000);
    CHECK_FALSE(policy::worth_keeping(policy::most_entries_in(200000), 50000));
    CHECK(policy::worth_keeping(policy::fewest_entries_in(200000), 500000));
    CHECK(policy::verdict_from_bytes(200000, 60000) == policy::Verdict::NeedsCount);
}

TEST_CASE("dictionary policy bounds bracket any real entry count", "[parquet][dictionary]") {
    // The bounds exist to skip a read, so the upper one must never undercount:
    // an entry cannot be smaller than a 4-byte length plus one character.
    for (const std::int64_t bytes : {0, 5, 1000, 440020, 12618454}) {
        CHECK(policy::most_entries_in(bytes) >= policy::fewest_entries_in(bytes));
        CHECK(policy::most_entries_in(bytes) * 5 <= bytes + 4);
    }
}
