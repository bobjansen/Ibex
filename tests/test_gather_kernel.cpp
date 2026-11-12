// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The shared gather kernel's concurrency rules (inconsistency I1, resolved by
// collapsing the parallel gathers onto this one kernel).
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

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "kernel_gather.hpp"
#include "kernel_types.hpp"
#include "runtime_internal.hpp"

using namespace ibex;

namespace {

/// A context that says yes to every parallelism gate, so a test does not need a
/// 65k-row table to reach the worker path.
auto parallel_exec() -> runtime::ExecutionContext {
    runtime::ExecutionContext exec;
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
    const runtime::ValidityBitmap src{true, false, true, false, false, true, true, false, true};
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
            const auto mask = static_cast<std::uint8_t>(std::uint8_t{1} << (bit % 8));
            (*bytes)[bit / 8] = static_cast<std::uint8_t>((*bytes)[bit / 8] | mask);
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

TEST_CASE("word-block gather keeps every survivor at every density and stays in its window",
          "[runtime][gather]") {
    namespace kernel = ibex::runtime::kernel;
    // Words cycle through the densities that pick each path (empty, sparse,
    // either side of the vector threshold, dense, full). Every rotation of the
    // cycle is tried, so each density ends each window: that is where a vector
    // store overhanging its window would show, with no later word to cover it.
    const std::uint64_t patterns[] = {0,
                                      1,
                                      0x8000000000000000ULL,
                                      0x5555,
                                      0xFFFF,
                                      0x1FFFF,
                                      0xFF00FF00,
                                      0xAAAAAAAAAAAAAAAAULL,
                                      0x0123456789ABCDEFULL,
                                      0x7FFFFFFFFFFFFFFFULL,
                                      ~std::uint64_t{0}};
    constexpr std::size_t kWords = 48;
    constexpr std::size_t kSplit = kWords / 2;

    const auto check = [&]<typename T>(T marker, std::size_t rows, std::size_t rotation) {
        CAPTURE(sizeof(T), rows, rotation);
        std::vector<std::uint64_t> words(kWords);
        for (std::size_t w = 0; w < kWords; ++w) {
            words[w] = patterns[(w + rotation) % std::size(patterns)];
        }
        if (rows % 64 != 0) {
            words.back() &= (std::uint64_t{1} << (rows % 64)) - 1;  // ragged last word
        }
        std::vector<T> src(rows);
        for (std::size_t r = 0; r < rows; ++r) {
            src[r] = static_cast<T>((static_cast<std::int64_t>(r) * 3) + 1);
        }
        std::vector<T> expected;
        for (std::size_t r = 0; r < rows; ++r) {
            if (((words[r / 64] >> (r % 64)) & 1U) != 0) {
                expected.push_back(src[r]);
            }
        }
        const std::size_t kept = expected.size();
        const kernel::ColumnView<T> view(src.data(), src.size(), nullptr);

        // One exact-size window: nothing may land past `kept`.
        std::vector<T> out(kept + 16, marker);
        kernel::gather_selected(
            view, kernel::RowWordBlocks{.words = words.data(), .word_count = kWords, .row_base = 0},
            kernel::OutputSpan<T>{.data = out.data(), .begin = 0, .count = kept});
        CHECK(std::vector<T>(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(kept)) ==
              expected);
        CHECK(std::ranges::all_of(out.begin() + static_cast<std::ptrdiff_t>(kept), out.end(),
                                  [&](T v) { return v == marker; }));

        // Two adjoining windows, the right one written first: an overhanging
        // store from the left window would clobber what the right one wrote.
        std::size_t left_kept = 0;
        for (std::size_t w = 0; w < kSplit; ++w) {
            left_kept += static_cast<std::size_t>(std::popcount(words[w]));
        }
        std::vector<T> joined(kept, marker);
        kernel::gather_selected(
            view,
            kernel::RowWordBlocks{.words = words.data() + kSplit,
                                  .word_count = kWords - kSplit,
                                  .row_base = kSplit * 64},
            kernel::OutputSpan<T>{
                .data = joined.data(), .begin = left_kept, .count = kept - left_kept});
        kernel::gather_selected(
            view, kernel::RowWordBlocks{.words = words.data(), .word_count = kSplit, .row_base = 0},
            kernel::OutputSpan<T>{.data = joined.data(), .begin = 0, .count = left_kept});
        CHECK(joined == expected);
    };
    for (const std::size_t rows : {kWords * 64, ((kWords - 1) * 64) + 37}) {
        for (std::size_t rotation = 0; rotation < std::size(patterns); ++rotation) {
            check(double{-1.0}, rows, rotation);
            check(std::int64_t{-1}, rows, rotation);
            check(std::int32_t{-1}, rows, rotation);
        }
    }
}

TEST_CASE("compact integer gather retains width without narrowing values",
          "[compact-int32][gather]") {
    runtime::ColumnValue source =
        Column<std::int64_t>::from_compact_int32({INT32_MIN, INT32_MAX, 2000000000});
    const std::vector<std::size_t> indices{2, 0, 1, 2};
    auto result = runtime::gather_column(source, indices.data(), indices.size(), nullptr);
    const auto& output = std::get<Column<std::int64_t>>(result);
    REQUIRE(output.is_compact_int32());
    REQUIRE(output.compact_int32_data()[0] == 2000000000);
    REQUIRE(output[1] == INT64_C(-2147483648));
    REQUIRE(output[2] == INT64_C(2147483647));
    REQUIRE(output[0] + output[3] == INT64_C(4000000000));
}

TEST_CASE("nullable compact batched gather writes its physical output directly",
          "[compact-int32][no-cache]") {
    ibex::runtime::ColumnValue source = ibex::Column<std::int64_t>::from_compact_int32({5, 7});
    constexpr auto missing = std::numeric_limits<std::size_t>::max();
    const std::size_t index[]{1, missing, 0};
    const std::vector<ibex::runtime::ColumnGatherJob> jobs{
        {.column = &source, .idx = index, .null_index = missing}};
    if (ibex::runtime::process_worker_pool().size() < 2)
        SKIP("requires two pool workers");
    const ibex::runtime::ExecutionContext exec{
        .parallel_threads = 2, .parallel_min_rows = 0, .parallel_min_cells = 0};
    auto gathered = ibex::runtime::gather_columns_batched(
        jobs, 3, &exec, [](std::size_t) -> ibex::runtime::GatheredColumn {
            FAIL("the test must reach the ranged compact writer");
            return {};
        });
    const auto& result = std::get<ibex::Column<std::int64_t>>(gathered[0].first);
    REQUIRE(result.is_compact_int32());
    REQUIRE(result.compact_int32_data()[0] == 7);
    REQUIRE(result.compact_int32_data()[1] == 0);
    REQUIRE(result.compact_int32_data()[2] == 5);
    REQUIRE(gathered[0].second.has_value());
    REQUIRE_FALSE((*gathered[0].second)[1]);
}

TEST_CASE("widen_int32 copies every value at every alignment and length",
          "[compact-int32][kernel]") {
    // Past 2^20 values the copy streams after an alignment prologue; the
    // offsets move the output's start, the lengths leave every tail size.
    constexpr std::size_t stream = std::size_t{1} << 20;
    constexpr std::int64_t guard = 0x5a5a5a5a5a5a5a5a;
    for (std::size_t count : {std::size_t{0}, std::size_t{1}, std::size_t{5}, stream - 1, stream,
                              stream + 1, stream + 2, stream + 3, stream + 7}) {
        std::vector<std::int32_t> source(count);
        for (std::size_t i = 0; i < count; ++i)
            source[i] = static_cast<std::int32_t>((i * 2654435761U) ^ 0x80000000U);
        for (std::size_t offset = 0; offset < 4; ++offset) {
            CAPTURE(count, offset);
            std::vector<std::int64_t> out(count + offset + 1, guard);
            runtime::widen_int32(source.data(), count, out.data() + offset);
            for (std::size_t i = 0; i < offset; ++i)
                REQUIRE(out[i] == guard);
            for (std::size_t i = 0; i < count; ++i)
                if (out[offset + i] != source[i])
                    FAIL("value " << i << " is " << out[offset + i]);
            REQUIRE(out[offset + count] == guard);
        }
    }
}
