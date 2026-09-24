// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <random>
#include <span>
#include <vector>

#include "../libs/parquet/selection_check.hpp"

// direct_decode_table checks a row selection in pieces: each row group's
// slice inside that group's decode task, the parts outside every group on the
// calling thread. That is only a safe replacement for the old whole-selection
// check if the two accept exactly the same selections. These cases pin that.

namespace {

namespace sel = ibex::parquet_selection;
using Rows = std::vector<std::size_t>;

// Groups of the given sizes laid end to end from `first`.
struct Layout {
    std::size_t first;
    Rows ends;  // exclusive end row of each group
};

auto layout(std::size_t first, const Rows& sizes) -> Layout {
    Layout out{.first = first, .ends = {}};
    std::size_t end = first;
    for (const std::size_t size : sizes) {
        end += size;
        out.ends.push_back(end);
    }
    return out;
}

auto split_valid(const Rows& selection, const Layout& groups, std::size_t source_rows) -> bool {
    const std::span<const std::size_t> s{selection};
    const auto bounds = sel::slice_bounds(s, groups.first, groups.ends);
    const std::size_t last_end = groups.ends.empty() ? groups.first : groups.ends.back();
    if (!sel::outside_valid(s, bounds, groups.first, last_end, source_rows)) {
        return false;
    }
    std::size_t first = groups.first;
    for (std::size_t k = 0; k < groups.ends.size(); ++k) {
        if (!sel::slice_valid(s, bounds[k], bounds[k + 1], first, groups.ends[k])) {
            return false;
        }
        first = groups.ends[k];
    }
    return true;
}

}  // namespace

TEST_CASE("selection check: valid selections pass both ways", "[parquet][selection]") {
    const auto groups = layout(0, {4, 4, 4});  // rows 0..11
    for (const Rows& selection :
         {Rows{}, Rows{0}, Rows{11}, Rows{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, Rows{1, 5, 9},
          Rows{3, 4}, Rows{7, 8}}) {
        CHECK(sel::whole_valid(selection, 12));
        CHECK(split_valid(selection, groups, 12));
    }
}

TEST_CASE("selection check: each kind of invalid selection fails both ways",
          "[parquet][selection]") {
    const auto groups = layout(0, {4, 4, 4});
    for (const Rows& selection : {
             Rows{2, 1},      // out of order inside one group
             Rows{5, 2},      // out of order across groups
             Rows{9, 1, 10},  // an index moved into an earlier group's slice
             Rows{3, 3},      // duplicate inside a group
             Rows{3, 4, 4},   // duplicate on a group boundary
             Rows{1, 12},     // past the end of the source
             Rows{20, 5},     // out of range and out of order
         }) {
        CHECK_FALSE(sel::whole_valid(selection, 12));
        CHECK_FALSE(split_valid(selection, groups, 12));
    }
}

TEST_CASE("selection check: indices outside the decoded groups are checked too",
          "[parquet][selection]") {
    // A unit decode covers rows 4..7 of a 12-row source, and the selection is
    // in whole-source indices, so rows outside the unit are legitimately there.
    const auto unit = layout(4, {4});
    CHECK(split_valid(Rows{0, 2, 5, 9, 11}, unit, 12));
    CHECK_FALSE(split_valid(Rows{2, 0, 5}, unit, 12));   // bad before the unit
    CHECK_FALSE(split_valid(Rows{5, 11, 9}, unit, 12));  // bad after the unit
    CHECK_FALSE(split_valid(Rows{5, 12}, unit, 12));     // past the source
}

TEST_CASE("selection check: split check equals whole check on random selections",
          "[parquet][selection]") {
    // A fixed seed on purpose: the property must hold for a reproducible sample.
    std::mt19937_64 rng(20260924);  // NOLINT(bugprone-random-generator-seed)
    std::size_t valid_seen = 0;
    std::size_t invalid_seen = 0;
    for (int round = 0; round < 20000; ++round) {
        // A random layout of 1-6 groups of 1-8 rows starting at 0.
        Rows sizes(1 + (rng() % 6));
        for (auto& size : sizes) {
            size = 1 + (rng() % 8);
        }
        const auto groups = layout(0, sizes);
        const std::size_t source_rows = groups.ends.back();

        // Start from a random valid selection, then damage it half the time.
        Rows selection;
        for (std::size_t row = 0; row < source_rows; ++row) {
            if (rng() % 3 == 0) {
                selection.push_back(row);
            }
        }
        if (rng() % 2 == 0 && !selection.empty()) {
            const auto i = rng() % selection.size();
            switch (rng() % 4) {
                case 0:  // duplicate
                    selection.insert(selection.begin() + static_cast<std::ptrdiff_t>(i),
                                     selection[i]);
                    break;
                case 1:  // swap with a random position
                    std::swap(selection[i], selection[rng() % selection.size()]);
                    break;
                case 2:  // out of range
                    selection[i] = source_rows + (rng() % 3);
                    break;
                default:  // random value anywhere
                    selection[i] = rng() % (source_rows + 2);
                    break;
            }
        }
        const bool whole = sel::whole_valid(selection, source_rows);
        CHECK(split_valid(selection, groups, source_rows) == whole);
        (whole ? valid_seen : invalid_seen)++;
    }
    // Both outcomes were actually exercised.
    CHECK(valid_seen > 5000);
    CHECK(invalid_seen > 3000);
}
