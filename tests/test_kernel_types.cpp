// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <vector>

#include "kernel_types.hpp"

namespace {

using namespace ibex;
using ibex::runtime::kernel::ChunkView;
using ibex::runtime::kernel::ColumnView;
using ibex::runtime::kernel::RowBitmap;
using ibex::runtime::kernel::RowIndices;
using ibex::runtime::kernel::RowRange;
using ibex::runtime::kernel::Selection;
using ibex::runtime::kernel::selection_rows;

}  // namespace

TEST_CASE("ColumnView aliases the column without copying", "[kernel][view]") {
    Column<std::int64_t> col{1, 2, 3, 4};
    ColumnView<std::int64_t> view(col, nullptr);
    REQUIRE(view.rows() == 4);
    REQUIRE(view.data() == col.data());
    REQUIRE(view.value(2) == 3);
    // Mutating the source is visible through the view: it is a view.
    col.push_back(5);
    REQUIRE(view.rows() == 4);  // the view's length is fixed at construction
}

TEST_CASE("ColumnView validity: absent means all valid", "[kernel][view]") {
    Column<std::int64_t> col{10, 20, 30};
    ColumnView<std::int64_t> all_valid(col, nullptr);
    REQUIRE(all_valid.is_valid(0));
    REQUIRE(all_valid.is_valid(2));

    ibex::runtime::ValidityBitmap bits;
    bits.push_back(true);
    bits.push_back(false);
    bits.push_back(true);
    ColumnView<std::int64_t> with_nulls(col, &bits);
    REQUIRE(with_nulls.is_valid(0));
    REQUIRE_FALSE(with_nulls.is_valid(1));
    REQUIRE(with_nulls.is_valid(2));
}

TEST_CASE("ChunkView exposes position, index space, and typed views", "[kernel][view]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{1, 2, 3});
    chunk.add_column("vol", Column<double>{1.5, 2.5, 3.5});
    chunk.row_offset = 96;
    chunk.sequence = 7;

    ChunkView view(chunk);
    REQUIRE(view.rows() == 3);
    REQUIRE(view.columns() == 2);
    REQUIRE(view.row_offset() == 96);
    REQUIRE(view.sequence() == 7);

    const auto price = view.view<std::int64_t>(0);
    REQUIRE(price.rows() == 3);
    REQUIRE(price.value(1) == 2);
    const auto vol = view.view<double>(1);
    REQUIRE(vol.value(2) == 3.5);
    // The view observes the chunk's storage, not a copy.
    REQUIRE(price.data() == std::get<Column<std::int64_t>>(view.column(0)).data());
}

TEST_CASE("Selection shapes answer their survivor counts", "[kernel][view]") {
    const RowRange range{.begin = 4, .end = 9};
    REQUIRE(selection_rows(Selection{range}, 100) == 5);

    const std::vector<std::size_t> indices{2, 5, 6, 8};
    const RowIndices picked{.data = indices.data(), .count = indices.size()};
    REQUIRE(selection_rows(Selection{picked}, 100) == 4);
    REQUIRE(picked[2] == 6);

    ibex::runtime::ValidityBitmap bits;
    for (int i = 0; i < 6; ++i) {
        bits.push_back(i == 1 || i == 4);
    }
    const RowBitmap masked{.bits = &bits};
    REQUIRE(selection_rows(Selection{masked}, 6) == 2);
    REQUIRE(masked.test(4));
    REQUIRE_FALSE(masked.test(0));
}
