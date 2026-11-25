// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// A selection decode copies runs of consecutive selected rows as blocks
// (`for_each_selected_run`). Runs of every length around the 64-row block,
// gaps and runs crossing 130-row row groups must still give the selected
// rows' values, for fixed-width columns and dictionary-encoded strings.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <string>
#include <utility>
#include <vector>

#include "../libs/parquet/parquet.hpp"

namespace {

constexpr std::size_t kRows = 2'000;
constexpr std::int64_t kRowGroupRows = 130;

auto int_at(std::size_t row) -> std::int64_t {
    return static_cast<std::int64_t>(row * 7) - 500;
}
auto double_at(std::size_t row) -> double {
    return static_cast<double>(row) / 4.0;
}
auto label_at(std::size_t row) -> std::string {
    return "L" + std::to_string((row * 13) % 37);
}

auto runs_reader() -> std::unique_ptr<parquet::arrow::FileReader> {
    arrow::Int64Builder ints;
    arrow::DoubleBuilder doubles;
    arrow::StringBuilder labels;
    for (std::size_t row = 0; row < kRows; ++row) {
        REQUIRE(ints.Append(int_at(row)).ok());
        REQUIRE(doubles.Append(double_at(row)).ok());
        REQUIRE(labels.Append(label_at(row)).ok());
    }
    const auto table = arrow::Table::Make(
        arrow::schema({arrow::field("i", arrow::int64()), arrow::field("d", arrow::float64()),
                       arrow::field("s", arrow::utf8())}),
        {ints.Finish().ValueOrDie(), doubles.Finish().ValueOrDie(), labels.Finish().ValueOrDie()});
    const auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, kRowGroupRows,
                                       parquet::WriterProperties::Builder().build())
                .ok());
    const auto buffer = output->Finish().ValueOrDie();
    return make_parquet_reader(std::make_shared<arrow::io::BufferReader>(buffer), "memory");
}

/// Runs of these lengths, each after a gap of the next length, from row 1.
auto run_selection() -> ibex::runtime::Selection {
    const std::vector<std::size_t> runs = {1, 2, 63, 64, 65, 127, 128, 129, 200, 3, 640};
    const std::vector<std::size_t> gaps = {1, 5, 1, 64, 2, 130, 1, 7, 1, 300};
    ibex::runtime::Selection selection;
    std::size_t row = 1;
    for (std::size_t i = 0; row < kRows; ++i) {
        for (std::size_t k = 0; k < runs[i % runs.size()] && row < kRows; ++k) {
            selection.push_back(row++);
        }
        row += gaps[i % gaps.size()];
    }
    return selection;
}

}  // namespace

TEST_CASE("for_each_selected_run splits strictly increasing rows into consecutive runs",
          "[parquet][selected-runs]") {
    std::vector<std::size_t> rows;
    for (std::size_t row = 10; row < 210; ++row) {  // one run of 200: blocks, then a tail
        rows.push_back(row);
    }
    rows.push_back(212);                             // a run of 1
    for (std::size_t row = 300; row < 364; ++row) {  // exactly one block
        rows.push_back(row);
    }
    rows.push_back(365);
    rows.push_back(366);
    std::vector<std::pair<std::size_t, std::size_t>> runs;
    for_each_selected_run(
        rows.data(), rows.data() + rows.size(), 10,
        [&](std::size_t offset, std::size_t count) { runs.emplace_back(offset, count); });
    const std::vector<std::pair<std::size_t, std::size_t>> expected = {
        {0, 200}, {202, 1}, {290, 64}, {355, 2}};
    CHECK(runs == expected);
    runs.clear();
    for_each_selected_run(rows.data(), rows.data(), 0, [&](std::size_t offset, std::size_t count) {
        runs.emplace_back(offset, count);
    });
    CHECK(runs.empty());
}

TEST_CASE("Parquet selected decode copies runs of rows across row groups",
          "[parquet][selected-runs]") {
    auto reader = runs_reader();
    const auto& metadata = *reader->parquet_reader()->metadata();
    REQUIRE(metadata.num_row_groups() > 10);
    const auto groups = all_decode_groups(metadata);
    const auto selection = run_selection();
    REQUIRE(selection.size() > 1'000);
    REQUIRE(selection.size() < kRows);

    const auto ints = direct_column(*reader, *arrow::field("i", arrow::int64()), 0, &selection,
                                    groups, selection.size());
    const auto doubles = direct_column(*reader, *arrow::field("d", arrow::float64()), 1, &selection,
                                       groups, selection.size());
    const auto labels =
        direct_column(*reader, *arrow::field("s", arrow::dictionary(arrow::int32(), arrow::utf8())),
                      2, &selection, groups, selection.size());
    const auto& int_values = std::get<ibex::Column<std::int64_t>>(*ints.column);
    const auto& double_values = std::get<ibex::Column<double>>(*doubles.column);
    const auto& label_values = std::get<ibex::Column<ibex::Categorical>>(*labels.column);
    REQUIRE(int_values.size() == selection.size());
    REQUIRE(double_values.size() == selection.size());
    REQUIRE(label_values.size() == selection.size());
    CHECK_FALSE(ints.validity.has_value());
    CHECK_FALSE(labels.validity.has_value());
    for (std::size_t i = 0; i < selection.size(); ++i) {
        INFO("output row " << i << " source row " << selection[i]);
        REQUIRE(int_values[i] == int_at(selection[i]));
        REQUIRE(double_values[i] == double_at(selection[i]));
        REQUIRE(std::string(label_values[i]) == label_at(selection[i]));
    }
}
