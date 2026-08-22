// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <vector>

#include "kernel_gather.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"

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

TEST_CASE("ChunkView metadata map shares columns and preserves morsel identity", "[kernel][map]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.add_column("vol", Column<double>{1.5, 2.5}, runtime::ValidityBitmap{true, false});
    chunk.row_offset = 64;
    chunk.sequence = 3;
    const auto input_column = chunk.columns[1].column;

    const ChunkView view(chunk);
    const std::vector<runtime::kernel::MappedChunkColumn> map{
        {.source_position = 1, .name = "volume"}, {.source_position = 0, .name = "price"}};
    const auto props = runtime::TableProperties::derive(
        view.properties(),
        [](const std::string& name) -> runtime::KeyFate { return runtime::KeyFate::kept(name); },
        runtime::RowTransform::Preserve);
    const auto output = runtime::kernel::map_chunk(view, map, props);

    REQUIRE(output.columns.size() == 2);
    REQUIRE(output.columns[0].name == "volume");
    REQUIRE(output.columns[1].name == "price");
    REQUIRE(output.columns[0].column == input_column);
    REQUIRE(output.columns[0].validity.has_value());
    REQUIRE_FALSE((*output.columns[0].validity)[1]);
    REQUIRE(output.row_offset == 64);
    REQUIRE(output.sequence == 3);
}

TEST_CASE("Row-local update kernel preserves chunk identity", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.sequence = 9;
    chunk.row_offset = 128;
    const auto input_column = chunk.columns.front().column;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "alias", .expr = ir::Expr{.node = ir::ColumnRef{.name = "price"}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->sequence == 9);
    REQUIRE(updated->row_offset == 128);
    REQUIRE(updated->columns.size() == 2);
    REQUIRE(updated->columns[1].name == "alias");
    REQUIRE(updated->columns[1].column == input_column);
}

TEST_CASE("Row-local alias update overwrites keys without copying its source", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("price", Column<std::int64_t>{10, 20});
    chunk.add_column("replacement", Column<std::int64_t>{30, 40},
                     runtime::ValidityBitmap{true, false});
    chunk.set_properties(runtime::TableProperties::sorted_by({{.name = "price"}}));
    const auto replacement = chunk.columns[1].column;
    const std::vector<ir::FieldSpec> fields{
        {.alias = "price", .expr = ir::Expr{.node = ir::ColumnRef{.name = "replacement"}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->columns.size() == 2);
    REQUIRE(updated->columns[0].name == "price");
    REQUIRE(updated->columns[0].column == replacement);
    REQUIRE(updated->columns[0].validity.has_value());
    REQUIRE_FALSE((*updated->columns[0].validity)[1]);
    REQUIRE_FALSE(updated->ordering().has_value());
}

TEST_CASE("Row-local alias update keeps the time-index write guard", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("time", Column<std::int64_t>{10, 20});
    chunk.add_column("replacement", Column<std::int64_t>{30, 40});
    chunk.set_properties(runtime::TableProperties::time_frame("time"));
    const std::vector<ir::FieldSpec> fields{
        {.alias = "time", .expr = ir::Expr{.node = ir::ColumnRef{.name = "replacement"}}}};
    const runtime::ExecutionContext exec{};

    const auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE_FALSE(updated.has_value());
    REQUIRE(updated.error() == "cannot update time index column: time");
}

TEST_CASE("Row-local fixed-width binary update uses all-valid column views", "[kernel][update]") {
    runtime::Chunk chunk;
    chunk.add_column("left", Column<std::int64_t>{2, 3, 4});
    chunk.add_column("right", Column<std::int64_t>{5, 7, 11});
    const std::vector<ir::FieldSpec> fields{
        {.alias = "product",
         .expr = ir::Expr{
             .node = ir::BinaryExpr{
                 .op = ir::ArithmeticOp::Mul,
                 .left = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "left"}}),
                 .right = ir::make_expr_ptr(ir::Expr{.node = ir::ColumnRef{.name = "right"}})}}}};
    const runtime::ExecutionContext exec{};

    auto updated =
        runtime::kernel::update_row_local_chunk(std::move(chunk), fields, nullptr, nullptr, exec);
    REQUIRE(updated.has_value());
    REQUIRE(updated->columns.size() == 3);
    const auto& product = std::get<Column<std::int64_t>>(*updated->columns[2].column);
    REQUIRE(product[0] == 10);
    REQUIRE(product[1] == 21);
    REQUIRE(product[2] == 44);
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

TEST_CASE("RowWordBlocks counts and iterates like the filter's mask", "[kernel][selection]") {
    // keep rows 1, 2, 7 in the first word and 64+3 in the second, over a
    // range beginning at absolute row 5.
    const std::vector<std::uint64_t> words{0b10000110U, 0b1000U};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = words.data(), .word_count = words.size(), .row_base = 5};
    REQUIRE(ibex::runtime::kernel::selection_rows(ibex::runtime::kernel::Selection{blocks}, 100) ==
            4);

    Column<std::int64_t> col;
    for (int i = 0; i < 80; ++i) {
        col.push_back(i);
    }
    std::vector<std::int64_t> out(4, -1);
    ibex::runtime::kernel::gather_selected(
        ibex::runtime::kernel::ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::OutputSpan<std::int64_t>{
            .data = out.data(), .begin = 0, .count = 4});
    // word 0, bits 1/2/7 are absolute rows 6/7/12; word 1, bit 3 is row 5+67.
    REQUIRE(out[0] == 6);
    REQUIRE(out[1] == 7);
    REQUIRE(out[2] == 12);
    REQUIRE(out[3] == 72);
}

TEST_CASE("gather_selected over every selection shape agrees", "[kernel][gather]") {
    Column<std::int64_t> col;
    for (int i = 0; i < 10; ++i) {
        col.push_back(100 + i);
    }
    using ibex::runtime::kernel::ColumnView;
    using ibex::runtime::kernel::OutputSpan;
    using ibex::runtime::kernel::Selection;
    const ColumnView<std::int64_t> view(col.data(), col.size(), nullptr);

    // Survivors: rows 2, 3, 5, 8 — expressed three ways.
    const std::vector<std::size_t> indices{2, 3, 5, 8};
    ibex::runtime::ValidityBitmap keep;
    for (int i = 0; i < 10; ++i) {
        keep.push_back(i == 2 || i == 3 || i == 5 || i == 8);
    }
    // rows 2,3,5,8 are bits 2,3,5,8 of word 0; word 1 is empty and must be
    // skipped by the iteration.
    const std::vector<std::uint64_t> blocks{(1U << 2) | (1U << 3) | (1U << 5) | (1U << 8), 0U};

    std::vector<std::int64_t> a(4), b(4), c(4), d(4);
    ibex::runtime::kernel::gather_selected(
        view, Selection{ibex::runtime::kernel::RowIndices{indices.data(), indices.size()}},
        OutputSpan<std::int64_t>{a.data(), 0, 4});
    ibex::runtime::kernel::gather_selected(view, Selection{ibex::runtime::kernel::RowBitmap{&keep}},
                                           OutputSpan<std::int64_t>{b.data(), 0, 4});
    ibex::runtime::kernel::gather_selected(
        view, Selection{ibex::runtime::kernel::RowWordBlocks{blocks.data(), blocks.size(), 0}},
        OutputSpan<std::int64_t>{c.data(), 0, 4});
    // A contiguous range: rows 2..5 (4 rows) — same count, different rows, to
    // prove the copy arm does not just agree with the others by accident.
    ibex::runtime::kernel::gather_selected(view, Selection{ibex::runtime::kernel::RowRange{2, 6}},
                                           OutputSpan<std::int64_t>{d.data(), 0, 4});

    REQUIRE(a[0] == 102);
    REQUIRE(a[1] == 103);
    REQUIRE(a[2] == 105);
    REQUIRE(a[3] == 108);
    REQUIRE(b == a);
    REQUIRE(c == a);
    REQUIRE(d[0] == 102);
    REQUIRE(d[1] == 103);
    REQUIRE(d[2] == 104);
    REQUIRE(d[3] == 105);
}

TEST_CASE("gather_selected honours the output offset for disjoint windows", "[kernel][gather]") {
    Column<std::int64_t> col{1, 2, 3, 4, 5, 6};
    using ibex::runtime::kernel::ColumnView;
    using ibex::runtime::kernel::OutputSpan;
    using ibex::runtime::kernel::Selection;
    std::vector<std::int64_t> out(6, -1);
    // Two workers, disjoint windows of one presized buffer: rows [0,3) at
    // offset 0, rows [3,6) at offset 3 — the two-phase filter's shape.
    ibex::runtime::kernel::gather_selected(
        ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        Selection{ibex::runtime::kernel::RowRange{0, 3}},
        OutputSpan<std::int64_t>{out.data(), 0, 3});
    ibex::runtime::kernel::gather_selected(
        ColumnView<std::int64_t>(col.data(), col.size(), nullptr),
        Selection{ibex::runtime::kernel::RowRange{3, 6}},
        OutputSpan<std::int64_t>{out.data(), 3, 3});
    REQUIRE(out == std::vector<std::int64_t>{1, 2, 3, 4, 5, 6});
}

TEST_CASE("Bool gather kernel: word-block selection matches per-bit truth",
          "[kernel][gather][bool]") {
    // 130 bools so the word machinery straddles word boundaries.
    Column<bool> src;
    std::vector<bool> truth;
    for (int i = 0; i < 130; ++i) {
        const bool v = (i % 3) == 0 || i == 97;
        src.push_back(v);
        truth.push_back(v);
    }
    // Keep rows 1, 64, 65, 129 (bits 1 of word 0; bits 0,1 of word 1; bit 1 of word 2).
    const std::vector<std::uint64_t> sel_words{(1U << 1), 0b11U, (1U << 1)};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = sel_words.data(), .word_count = sel_words.size(), .row_base = 0};

    std::vector<std::uint64_t> out_words(1, 0);  // 4 survivors fit one word
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src), ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 0, .count = 4});

    const auto bit = [&](std::size_t i) { return (out_words[i / 64] >> (i % 64)) & 1U; };
    REQUIRE(bit(0) == truth[1]);
    REQUIRE(bit(1) == truth[64]);
    REQUIRE(bit(2) == truth[65]);
    REQUIRE(bit(3) == truth[129]);
    // Nothing beyond the run was set.
    REQUIRE((out_words[0] >> 4) == 0);
}

TEST_CASE("Bool gather kernel: disjoint windows OR without clobbering", "[kernel][gather][bool]") {
    Column<bool> src;
    for (int i = 0; i < 100; ++i) {
        src.push_back(i % 2 == 0);
    }
    std::vector<std::uint64_t> out_words(2, 0);
    // Window A: rows [10, 20) at bit offset 10; window B: rows [20, 30) at 20.
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src),
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{10, 20}},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 10, .count = 10});
    ibex::runtime::kernel::gather_selected_bool(
        ibex::runtime::kernel::BoolView(src),
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{20, 30}},
        ibex::runtime::kernel::BoolOutputSpan{.words = out_words.data(), .begin = 20, .count = 10});
    // Output bits [10, 30) hold source rows 10..29: even rows are true.
    for (std::size_t i = 10; i < 30; ++i) {
        const bool got = ((out_words[i / 64] >> (i % 64)) & 1U) != 0;
        INFO("bit " << i);
        REQUIRE(got == (i % 2 == 0));
    }
    REQUIRE((out_words[0] & 0x3FFU) == 0);  // bits [0,10) untouched
}

TEST_CASE("String gather kernel: word-block selection packs slabs and offsets",
          "[kernel][gather][string]") {
    // Build a source: "a", "bb", "", "ccc", "dd" with offsets 0,1,3,3,6,8.
    Column<std::string> src{"a", "bb", "", "ccc", "dd"};
    // Keep rows 0, 2, 3, 4.
    const std::vector<std::uint64_t> sel_words{(1U << 0) | (1U << 2) | (1U << 3) | (1U << 4)};
    const ibex::runtime::kernel::RowWordBlocks blocks{
        .words = sel_words.data(), .word_count = sel_words.size(), .row_base = 0};

    std::vector<char> chars(6);
    std::vector<std::uint32_t> offsets(5, 9999);
    ibex::runtime::kernel::gather_selected_strings(
        ibex::runtime::kernel::StringView{
            .offsets = src.offsets_data(), .chars = src.chars_data(), .rows = src.size()},
        ibex::runtime::kernel::Selection{blocks},
        ibex::runtime::kernel::StringOutputSpan{.offsets = offsets.data(),
                                                .chars = chars.data(),
                                                .begin = 0,
                                                .count = 4,
                                                .char_base = 0});

    // Rows kept: "a" (len 1), "" (0), "ccc" (3), "dd" (2) -> 6 chars total.
    REQUIRE(offsets[1] == 1);
    REQUIRE(offsets[2] == 1);
    REQUIRE(offsets[3] == 4);
    REQUIRE(offsets[4] == 6);
    const std::string packed(chars.begin(), chars.end());
    REQUIRE(packed == "acccdd");
}

TEST_CASE("String gather kernel: offset window continues a prior run", "[kernel][gather][string]") {
    Column<std::string> src{"xx", "yy", "zzz"};
    // Window over rows [2,3) written at output begin=1, continuing after a
    // hypothetical first window that ended at char_base=4 ("xxyy" equivalent).
    std::vector<char> chars(7);
    std::vector<std::uint32_t> offsets(3, 9999);
    offsets[1] = 4;  // the previous window's end offset — this kernel's contract
    ibex::runtime::kernel::gather_selected_strings(
        ibex::runtime::kernel::StringView{
            .offsets = src.offsets_data(), .chars = src.chars_data(), .rows = src.size()},
        ibex::runtime::kernel::Selection{ibex::runtime::kernel::RowRange{2, 3}},
        ibex::runtime::kernel::StringOutputSpan{.offsets = offsets.data(),
                                                .chars = chars.data(),
                                                .begin = 1,
                                                .count = 1,
                                                .char_base = 4});
    REQUIRE(offsets[2] == 7);
    REQUIRE(std::string(chars.begin() + 4, chars.begin() + 7) == "zzz");
}
