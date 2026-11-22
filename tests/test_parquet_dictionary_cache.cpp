// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// A scan's units share one dictionary per column (`ParquetDictionaryCache`)
// when their row groups' dictionary pages hold only labels the first unit
// found, in any order. A row group with a new label falls back to interning
// over the cached labels, so every unit still reads its own labels.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <arrow/array/builder_binary.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <string>
#include <vector>

#include "../libs/parquet/parquet.hpp"

namespace {

constexpr std::int64_t kRowGroupRows = 6;

// Row groups of 6: the same three labels in three orders, then one with a new
// label "d" and a null, then the first labels again.
auto labels() -> std::vector<std::optional<std::string>> {
    return {"a", "b", "c", "a", "b", "c",          //
            "c", "a", "b", "b", "c", "a",          //
            "b", "c", "a", "c", "a", "b",          //
            "d", "a", std::nullopt, "d", "c", "b",  //
            "a", "a", "b", "c", "c", "b"};
}

auto dictionary_reader() -> std::unique_ptr<parquet::arrow::FileReader> {
    arrow::StringBuilder builder;
    for (const auto& label : labels()) {
        REQUIRE((label.has_value() ? builder.Append(*label) : builder.AppendNull()).ok());
    }
    const auto table = arrow::Table::Make(arrow::schema({arrow::field("s", arrow::utf8())}),
                                          {builder.Finish().ValueOrDie()});
    const auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, kRowGroupRows,
                                       parquet::WriterProperties::Builder().build())
                .ok());
    return make_parquet_reader(
        std::make_shared<arrow::io::BufferReader>(output->Finish().ValueOrDie()), "memory");
}

}  // namespace

TEST_CASE("Parquet units share one dictionary when their pages hold its labels",
          "[parquet][dictionary-cache]") {
    auto reader = dictionary_reader();
    const auto& metadata = *reader->parquet_reader()->metadata();
    REQUIRE(metadata.num_row_groups() == 5);
    const auto expected = labels();
    const auto field = arrow::field("s", arrow::dictionary(arrow::int32(), arrow::utf8()));

    for (const bool selected : {false, true}) {
        CAPTURE(selected);
        ParquetDictionaryCache cache;
        std::vector<const void*> identities;
        for (int group = 0; group < metadata.num_row_groups(); ++group) {
            CAPTURE(group);
            const auto start = static_cast<std::size_t>(group) * kRowGroupRows;
            const DirectDecodeGroups unit{
                .begin = group, .end = group + 1, .source_start = start, .rows = kRowGroupRows};
            // Every other row, or all of them.
            ibex::runtime::Selection selection;
            for (std::size_t row = start; row < start + kRowGroupRows; row += 2) {
                selection.push_back(row);
            }
            const auto* chosen = selected ? &selection : nullptr;
            const auto rows = selected ? selection.size() : static_cast<std::size_t>(kRowGroupRows);
            const auto entry = direct_column(*reader, *field, 0, chosen, unit, rows, &cache);
            const auto& column = std::get<ibex::Column<ibex::Categorical>>(*entry.column);
            REQUIRE(column.size() == rows);
            for (std::size_t i = 0; i < rows; ++i) {
                const auto row = selected ? selection[i] : start + i;
                INFO("row " << row);
                const auto& label = expected[row];
                const bool valid = !entry.validity.has_value() || (*entry.validity)[i];
                REQUIRE(valid == label.has_value());
                if (label.has_value()) {
                    REQUIRE(std::string(column[i]) == *label);
                }
            }
            identities.push_back(column.dictionary_identity());
        }
        // The first unit published its dictionary; the next two only map onto
        // it. The one with "d" interns its own over the cached labels, and the
        // last shares the cache again.
        CHECK(identities[1] == identities[0]);
        CHECK(identities[2] == identities[0]);
        CHECK(identities[3] != identities[0]);
        CHECK(identities[4] == identities[0]);
    }
}
