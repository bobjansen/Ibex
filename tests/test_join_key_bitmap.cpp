// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <vector>

using ibex::runtime::DynamicScanFilter;
using ibex::runtime::JoinKeyBitmap;

// The bitmap replaces the Bloom's answer in DynamicScanFilter::passes whenever
// it is published, so it has to be exact over the whole range, including both
// ends and keys just outside them, and it must never be built for a range that
// would overflow or cost far more memory than the Bloom it stands beside.

TEST_CASE("join key bitmap: exact membership across the range and at its ends", "[join][bitmap]") {
    const std::vector<std::int64_t> keys = {-70, -65, -1, 0, 1, 63, 64, 65, 127, 128, 200};
    JoinKeyBitmap bitmap(-70, 200);
    for (const auto key : keys) {
        bitmap.insert(key);
    }
    const std::set<std::int64_t> present(keys.begin(), keys.end());
    for (std::int64_t key = -80; key <= 210; ++key) {
        INFO("key " << key);
        CHECK(bitmap.contains(key) == present.contains(key));
    }
    // Far outside in both directions, including values whose offset wraps.
    CHECK_FALSE(bitmap.contains(std::numeric_limits<std::int64_t>::min()));
    CHECK_FALSE(bitmap.contains(std::numeric_limits<std::int64_t>::max()));
}

TEST_CASE("join key bitmap: a one-key range", "[join][bitmap]") {
    JoinKeyBitmap bitmap(42, 42);
    bitmap.insert(42);
    CHECK(bitmap.contains(42));
    CHECK_FALSE(bitmap.contains(41));
    CHECK_FALSE(bitmap.contains(43));
}

TEST_CASE("join key bitmap: agrees with the key set on random dense inputs", "[join][bitmap]") {
    // NOLINTNEXTLINE(bugprone-random-generator-seed,cert-msc51-cpp): reproducible sample
    std::mt19937_64 rng(7);
    for (int round = 0; round < 200; ++round) {
        const auto base = static_cast<std::int64_t>(rng() % 2001) - 1000;
        const auto span = static_cast<std::int64_t>(1 + (rng() % 5000));
        std::set<std::int64_t> present;
        for (int i = 0; i < 300; ++i) {
            present.insert(base +
                           static_cast<std::int64_t>(rng() % static_cast<std::uint64_t>(span)));
        }
        JoinKeyBitmap bitmap(*present.begin(), *present.rbegin());
        for (const auto key : present) {
            bitmap.insert(key);
        }
        for (std::int64_t key = base - 70; key < base + span + 70; ++key) {
            REQUIRE(bitmap.contains(key) == present.contains(key));
        }
    }
}

TEST_CASE("join key bitmap: built only for dense, bounded ranges", "[join][bitmap]") {
    constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
    constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
    // Up to 4x the Bloom's bits.
    CHECK(JoinKeyBitmap::worth_building(0, (std::int64_t{4} * 1024) - 1, 1024));
    CHECK_FALSE(JoinKeyBitmap::worth_building(0, std::int64_t{4} * 1024, 1024));
    // An inverted range, and ranges whose width overflows or exceeds the cap.
    CHECK_FALSE(JoinKeyBitmap::worth_building(10, 9, 1 << 20));
    CHECK_FALSE(JoinKeyBitmap::worth_building(kMin, kMax, std::size_t{1} << 40));
    CHECK_FALSE(JoinKeyBitmap::worth_building(0, std::int64_t{1} << 29, std::size_t{1} << 40));
    CHECK(JoinKeyBitmap::worth_building(0, (std::int64_t{1} << 29) - 1, std::size_t{1} << 40));
    // Negative ranges measure the same as positive ones.
    CHECK(JoinKeyBitmap::worth_building(-1000, 1000, 1024));
}

TEST_CASE("join key bitmap: DynamicScanFilter answers from it, ignoring the Bloom",
          "[join][bitmap]") {
    // A Bloom that contains a key the bitmap lacks, the false-positive case the
    // bitmap exists to remove.
    DynamicScanFilter filter;
    filter.ready = true;
    filter.min = 10;
    filter.max = 20;
    filter.bloom.emplace(8);
    for (std::int64_t key = 10; key <= 20; ++key) {
        filter.bloom->insert(key);
    }
    auto bitmap = std::make_shared<JoinKeyBitmap>(10, 20);
    bitmap->insert(10);
    bitmap->insert(15);
    bitmap->insert(20);
    filter.bitmap = bitmap;
    for (std::int64_t key = 5; key <= 25; ++key) {
        INFO("key " << key);
        CHECK(filter.passes(key) == (key == 10 || key == 15 || key == 20));
    }
    CHECK(filter.has_membership());
}

TEST_CASE("join key bitmap: a bitmap alone counts as membership", "[join][bitmap]") {
    // The join publishes a bitmap INSTEAD of a Bloom for a dense range, and
    // every scan path asks has_membership() before taking the fused key scan.
    DynamicScanFilter filter;
    filter.ready = true;
    filter.min = 1;
    filter.max = 3;
    auto bitmap = std::make_shared<JoinKeyBitmap>(1, 3);
    bitmap->insert(2);
    filter.bitmap = bitmap;
    CHECK_FALSE(filter.bloom.has_value());
    CHECK(filter.has_membership());
    CHECK_FALSE(filter.passes(1));
    CHECK(filter.passes(2));
    CHECK_FALSE(filter.passes(3));
}
