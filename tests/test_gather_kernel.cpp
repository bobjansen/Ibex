// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The shared gather kernel's concurrency rules (parallelism-overview.md, I1).
//
// `make_gather_column` + `gather_range_into` is the one kernel every path that
// rewrites a column through an index array uses. Before it was shared, three
// paths answered "can this column type be gathered in parallel?" three ways:
// `gather_column` left `bool` and `std::string` serial, the sort split `bool`
// across 64-row-aligned ranges and made a string one indivisible task, and the
// two-phase filter parallelized both. Each was locally justified and none knew
// about the others.
//
// Two properties are worth pinning here, because a violation of either is a
// data race that a passing run does not disprove:
//
//   1. `for_row_ranges` hands out 64-ALIGNED range boundaries. This is what
//      makes a bit-packed destination safe to write from several ranges at
//      once, and it is checkable directly rather than by hoping a race shows
//      up under a stress loop.
//   2. A parallel gather equals a serial one, for every column type.

#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "kernel_gather.hpp"
#include "runtime_internal.hpp"

using namespace ibex;

namespace {

/// A context that says yes to every parallelism gate, so a test does not need a
/// 65k-row table to reach the worker path.
auto parallel_exec() -> runtime::ExecutionContext {
    runtime::ExecutionContext exec;
    exec.parallel = true;
    exec.parallel_min_rows = 0;
    exec.parallel_min_cells = 0;
    exec.parallel_threads = 8;
    return exec;
}

/// A reversing permutation, so no gathered value can sit at its source index and
/// an off-by-one in the range split shows up as a wrong value rather than a
/// coincidentally identical one.
auto reversed_indices(std::size_t n) -> std::vector<std::size_t> {
    std::vector<std::size_t> idx(n);
    for (std::size_t i = 0; i < n; ++i) {
        idx[i] = n - 1 - i;
    }
    return idx;
}

/// Element-wise equality: `Column<T>` has no `operator==`, and comparing the
/// variants directly would compile only for the alternatives that do.
/// Categorical compares by resolved VALUE rather than by code, because the two
/// gathers may legitimately reach the same value through different dictionaries.
void require_same(const runtime::ColumnValue& lhs, const runtime::ColumnValue& rhs) {
    std::visit(
        [&](const auto& left) {
            using ColT = std::decay_t<decltype(left)>;
            const auto* right = std::get_if<ColT>(&rhs);
            REQUIRE(right != nullptr);
            REQUIRE(left.size() == right->size());
            for (std::size_t i = 0; i < left.size(); ++i) {
                CAPTURE(i);
                REQUIRE(left[i] == (*right)[i]);
            }
        },
        lhs);
}

}  // namespace

TEST_CASE("for_row_ranges aligns range boundaries to 64 rows", "[runtime][gather][parallel]") {
    const auto exec = parallel_exec();

    // A row count that is deliberately NOT a multiple of 64, so the final range
    // is the only ragged one.
    for (const std::size_t rows : {size_t{1000}, size_t{4096}, size_t{65537}, size_t{100003}}) {
        CAPTURE(rows);
        std::mutex mutex;
        std::vector<std::pair<std::size_t, std::size_t>> seen;
        runtime::for_row_ranges(&exec, rows, [&](std::size_t begin, std::size_t end) {
            const std::scoped_lock lock(mutex);
            seen.emplace_back(begin, end);
        });

        REQUIRE_FALSE(seen.empty());
        std::size_t covered = 0;
        for (const auto& [begin, end] : seen) {
            CAPTURE(begin, end);
            // Every start is word-aligned, and every end is too unless it is the
            // end of the input. That is exactly the condition under which two
            // ranges never share a 64-bit word.
            CHECK(begin % 64 == 0);
            CHECK((end % 64 == 0 || end == rows));
            CHECK(begin < end);
            covered += end - begin;
        }
        // Ranges partition [0, rows): no gap, no overlap.
        CHECK(covered == rows);
    }
}

TEST_CASE("for_row_ranges covers every row when it declines to split",
          "[runtime][gather][parallel]") {
    // A null context is the "serial by decision" spelling, and must still visit
    // the whole input exactly once.
    std::size_t calls = 0;
    std::size_t covered = 0;
    runtime::for_row_ranges(nullptr, 1000, [&](std::size_t begin, std::size_t end) {
        ++calls;
        covered += end - begin;
        CHECK(begin == 0);
        CHECK(end == 1000);
    });
    CHECK(calls == 1);
    CHECK(covered == 1000);
}

TEST_CASE("a parallel gather equals a serial one for every column type",
          "[runtime][gather][parallel]") {
    // Past 64 rows so several aligned ranges exist, and not a multiple of 64 so
    // the ragged tail is exercised too.
    constexpr std::size_t kRows = 5000;
    const auto idx = reversed_indices(kRows);
    const auto exec = parallel_exec();

    const auto check = [&](const char* what, const runtime::ColumnValue& src) {
        CAPTURE(what);
        require_same(runtime::gather_column(src, idx.data(), idx.size(), nullptr),
                     runtime::gather_column(src, idx.data(), idx.size(), &exec));
    };

    SECTION("int64") {
        Column<std::int64_t> col;
        for (std::size_t i = 0; i < kRows; ++i) {
            col.push_back(static_cast<std::int64_t>(i) * 7);
        }
        check("int64", col);
    }

    SECTION("double") {
        Column<double> col;
        for (std::size_t i = 0; i < kRows; ++i) {
            col.push_back(static_cast<double>(i) * 0.5);
        }
        check("double", col);
    }

    // The case the alignment rule exists for: 64 rows share a word, so an
    // unaligned split would have two ranges read-modify-writing the same one.
    SECTION("bool") {
        Column<bool> col;
        for (std::size_t i = 0; i < kRows; ++i) {
            col.push_back(i % 3 == 0);
        }
        check("bool", col);
    }

    // Cumulative offsets: gathered whole, never by range. The parallel call must
    // still produce the same answer, by declining to split rather than by
    // splitting correctly.
    SECTION("string") {
        Column<std::string> col;
        for (std::size_t i = 0; i < kRows; ++i) {
            col.push_back(std::string(i % 17, 'x') + std::to_string(i));
        }
        check("string", col);
    }

    SECTION("categorical") {
        Column<Categorical> col;
        for (std::size_t i = 0; i < kRows; ++i) {
            col.push_back("sym" + std::to_string(i % 11));
        }
        check("categorical", col);
    }
}

TEST_CASE("a parallel validity gather equals a serial one", "[runtime][gather][parallel]") {
    // A validity bitmap packs 64 rows per word exactly as Column<bool> does, and
    // is written by the same aligned ranges.
    constexpr std::size_t kRows = 5000;
    const auto idx = reversed_indices(kRows);
    const auto exec = parallel_exec();

    runtime::ValidityBitmap src(kRows, true);
    for (std::size_t i = 0; i < kRows; ++i) {
        src.set(i, i % 5 != 0);
    }

    runtime::ValidityBitmap serial(kRows, false);
    runtime::gather_validity_range(serial, src, idx, 0, kRows);

    runtime::ValidityBitmap parallel(kRows, false);
    runtime::for_row_ranges(&exec, kRows, [&](std::size_t begin, std::size_t end) {
        runtime::gather_validity_range(parallel, src, idx, begin, end);
    });

    for (std::size_t i = 0; i < kRows; ++i) {
        CAPTURE(i);
        REQUIRE(serial[i] == parallel[i]);
    }
}

TEST_CASE("kernel validity gather packs selected bits and preserves false output bits",
          "[runtime][gather][kernel]") {
    runtime::ValidityBitmap src{true, false, true, false, false, true, true, false, true};
    const std::uint64_t selected[] = {0b1111'0111};
    runtime::ValidityBitmap dst(7, false);

    runtime::kernel::gather_selected_validity(
        runtime::kernel::ValidityView(src),
        runtime::kernel::Selection{
            runtime::kernel::RowWordBlocks{.words = selected, .word_count = 1, .row_base = 0}},
        runtime::kernel::BoolOutputSpan{.words = dst.words_data(), .begin = 0, .count = 7});

    // Selected source rows are 0, 1, 2, 4, 5, 6, 7: validity is copied while
    // false rows remain the zero-filled destination bits.
    const bool expected[] = {true, false, true, false, true, true, false};
    for (std::size_t i = 0; i < std::size(expected); ++i) {
        CAPTURE(i);
        CHECK(dst[i] == expected[i]);
    }
}

TEST_CASE("kernel validity gather handles an externally offset source and adjoining windows",
          "[runtime][gather][kernel]") {
    // Logical source starts at bit 3, deliberately not a word or byte boundary.
    const auto bytes = std::make_shared<std::vector<std::uint8_t>>(16, 0);
    for (std::size_t i = 0; i < 100; ++i) {
        if (i % 3 != 1) {
            const std::size_t bit = 3 + i;
            (*bytes)[bit / 8] |= std::uint8_t{1} << (bit % 8);
        }
    }
    const std::shared_ptr<const void> owner = bytes;
    const auto src = runtime::ValidityBitmap::from_external(owner, bytes->data(), 3, 100);
    runtime::ValidityBitmap dst(103, false);
    std::uint64_t select[2] = {~std::uint64_t{0}, (std::uint64_t{1} << 36) - 1};

    // Two output windows meet in word 1.  Each only sets true bits, so their
    // atomic OR boundary cannot clobber the other's false bits or true bits.
    runtime::kernel::gather_selected_validity(
        runtime::kernel::ValidityView(src),
        runtime::kernel::Selection{
            runtime::kernel::RowWordBlocks{.words = select, .word_count = 1, .row_base = 0}},
        runtime::kernel::BoolOutputSpan{.words = dst.words_data(), .begin = 3, .count = 64});
    runtime::kernel::gather_selected_validity(
        runtime::kernel::ValidityView(src),
        runtime::kernel::Selection{
            runtime::kernel::RowWordBlocks{.words = select + 1, .word_count = 1, .row_base = 64}},
        runtime::kernel::BoolOutputSpan{.words = dst.words_data(), .begin = 67, .count = 36});

    CHECK_FALSE(dst[0]);
    CHECK_FALSE(dst[1]);
    CHECK_FALSE(dst[2]);
    for (std::size_t i = 0; i < 100; ++i) {
        CAPTURE(i);
        CHECK(dst[3 + i] == (i % 3 != 1));
    }
}
