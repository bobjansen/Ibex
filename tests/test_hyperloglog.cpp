// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "hyperloglog.hpp"
#include "interpreter_internal.hpp"

// The parallel distinct pre-sizes its hash sets from this estimate. A bad
// estimate only costs rehashing, never an answer, but a broken one (say an
// order-of-magnitude error) would waste memory or lose the whole benefit, so
// the accuracy is pinned here.

using ibex::runtime::HyperLogLog;
using ibex::runtime::key_hash_finalize;

namespace {

/// A sketch of `distinct` values 0..distinct-1, each added `copies` times.
auto sketch_of(std::uint64_t distinct, int copies = 1, std::uint64_t offset = 0) -> HyperLogLog {
    HyperLogLog sketch;
    for (int c = 0; c < copies; ++c) {
        for (std::uint64_t v = 0; v < distinct; ++v) {
            sketch.add(key_hash_finalize(v + offset));
        }
    }
    return sketch;
}

}  // namespace

TEST_CASE("hyperloglog: an empty sketch estimates zero", "[runtime][hyperloglog]") {
    CHECK(HyperLogLog{}.estimate() == 0.0);
}

TEST_CASE("hyperloglog: small counts are near-exact (linear counting)", "[runtime][hyperloglog]") {
    // Linear counting's relative error grows with n/m: about 2.2% at n = 50
    // with 1024 registers (one collision is already likely there), so "within
    // one" only holds for the smallest counts.
    for (const std::uint64_t n : {1U, 2U, 10U, 50U}) {
        INFO("n = " << n);
        const double tolerance = std::max(1.0, 0.05 * static_cast<double>(n));
        CHECK(std::abs(sketch_of(n).estimate() - static_cast<double>(n)) <= tolerance);
    }
}

TEST_CASE("hyperloglog: estimates stay within 10% across six orders of magnitude",
          "[runtime][hyperloglog]") {
    // 1024 registers give a standard error of about 3.3%; 10% is three sigma.
    for (const std::uint64_t n : {100U, 1000U, 10000U, 100000U, 1000000U}) {
        INFO("n = " << n);
        const double estimate = sketch_of(n, 2).estimate();
        CHECK(std::abs(estimate - static_cast<double>(n)) <= 0.10 * static_cast<double>(n));
    }
}

TEST_CASE("hyperloglog: duplicates do not count", "[runtime][hyperloglog]") {
    CHECK(sketch_of(5000, 5) == sketch_of(5000, 1));
}

TEST_CASE("hyperloglog: merging two sketches is the sketch of the union",
          "[runtime][hyperloglog]") {
    // [0, 60000) and [40000, 100000) overlap; the union is [0, 100000).
    auto left = sketch_of(60000);
    left.merge(sketch_of(60000, 1, 40000));
    CHECK(left == sketch_of(100000));
    CHECK(std::abs(left.estimate() - 100000.0) <= 10000.0);
}

TEST_CASE("composite hashes distinguish dense integer pairs", "[runtime][hash]") {
    using namespace ibex::runtime;
    std::vector<std::uint64_t> hashes;
    for (std::int64_t a = 0; a < 100; ++a) {
        for (std::int64_t b = 0; b < 1000; ++b) {
            Key key;
            key.values.emplace_back(a);
            key.values.emplace_back(b);
            hashes.push_back(KeyHash{}(key));
        }
    }
    std::sort(hashes.begin(), hashes.end());
    const auto distinct = std::unique(hashes.begin(), hashes.end()) - hashes.begin();
    CHECK(distinct > 99000);
}

TEST_CASE("row and boxed migration hashes agree for signed integer keys", "[runtime][hash]") {
    using namespace ibex::runtime;
    ibex::Column<std::int64_t> a;
    ibex::Column<std::int64_t> b;
    for (const auto value : {std::int64_t{-1}, std::int64_t{0}, std::int64_t{42}}) {
        a.push_back(value);
        b.push_back(value * 7);
    }
    ColumnValue av = std::move(a);
    ColumnValue bv = std::move(b);
    std::vector<KeyCol> cols{*make_key_col(av, nullptr), *make_key_col(bv, nullptr)};
    for (std::size_t row = 0; row < 3; ++row) {
        Key key;
        key.values.emplace_back(std::get<ibex::Column<std::int64_t>>(av)[row]);
        key.values.emplace_back(std::get<ibex::Column<std::int64_t>>(bv)[row]);
        CHECK(hash_key_row(cols, row) == hash_key_value(key));
    }
}
