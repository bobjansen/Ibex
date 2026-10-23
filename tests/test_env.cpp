// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/env.hpp>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <string>

using ibex::runtime::huge_pages_tunables;

namespace {
constexpr const char* kMadvise = "always [madvise] never";
}  // namespace

TEST_CASE("huge_pages_tunables adds the huge-page tunables once", "[runtime][env]") {
    // Unset: just the tunable.
    CHECK(huge_pages_tunables(nullptr, nullptr, nullptr, kMadvise) ==
          std::optional<std::string>{"glibc.malloc.hugetlb=1:glibc.malloc.top_pad=67108864"});
    CHECK(huge_pages_tunables("", nullptr, nullptr, kMadvise) ==
          std::optional<std::string>{"glibc.malloc.hugetlb=1:glibc.malloc.top_pad=67108864"});
    // Other tunables are kept, the new one appended.
    CHECK(huge_pages_tunables("glibc.malloc.arena_max=2", nullptr, nullptr, kMadvise) ==
          std::optional<std::string>{"glibc.malloc.arena_max=2:glibc.malloc.hugetlb=1:glibc.malloc.top_pad=67108864"});
    // A top pad the caller chose is kept.
    CHECK(huge_pages_tunables("glibc.malloc.top_pad=1048576", nullptr, nullptr, kMadvise) ==
          std::optional<std::string>{"glibc.malloc.top_pad=1048576:glibc.malloc.hugetlb=1"});
    // THP set to always works as well as madvise.
    CHECK(huge_pages_tunables(nullptr, nullptr, nullptr, "[always] madvise never").has_value());
    // `IBEX_NO_HUGEPAGES=0` is not an opt-out.
    CHECK(huge_pages_tunables(nullptr, "0", nullptr, kMadvise).has_value());
    CHECK(huge_pages_tunables(nullptr, "", nullptr, kMadvise).has_value());
}

TEST_CASE("huge_pages_tunables leaves the process alone when it should", "[runtime][env]") {
    // The caller already chose a value, on or off.
    CHECK_FALSE(huge_pages_tunables("glibc.malloc.hugetlb=0", nullptr, nullptr, kMadvise));
    CHECK_FALSE(huge_pages_tunables("glibc.malloc.arena_max=2:glibc.malloc.hugetlb=2", nullptr,
                                    nullptr, kMadvise));
    // Opted out.
    CHECK_FALSE(huge_pages_tunables(nullptr, "1", nullptr, kMadvise));
    // Already re-executed: never twice, whatever the environment says now.
    CHECK_FALSE(huge_pages_tunables(nullptr, nullptr, "1", kMadvise));
    // No transparent huge pages to ask for.
    CHECK_FALSE(huge_pages_tunables(nullptr, nullptr, nullptr, "always madvise [never]"));
    CHECK_FALSE(huge_pages_tunables(nullptr, nullptr, nullptr, ""));
}
