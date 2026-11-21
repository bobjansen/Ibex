// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Bool columns decode into bytes and are then packed 64 rows to a word, both
// as one whole-column task (`direct_column`) and split by row group
// (`plan_sharded_column`, whose shards write disjoint byte slices that
// `finalize` packs). Row groups of 130 rows put every shard boundary mid-word.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
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
#include <vector>

#include "../libs/parquet/parquet.hpp"

namespace {

constexpr std::size_t kRows = 1'000;
constexpr std::int64_t kRowGroupRows = 130;

auto value_at(std::size_t row) -> bool {
    return ((row * 2654435761U) >> 7U) % 3 == 0;
}
auto null_at(std::size_t row) -> bool {
    return row % 17 == 3;
}

auto bool_reader(bool nulls) -> std::unique_ptr<parquet::arrow::FileReader> {
    arrow::BooleanBuilder builder;
    for (std::size_t row = 0; row < kRows; ++row) {
        if (nulls && null_at(row)) {
            REQUIRE(builder.AppendNull().ok());
        } else {
            REQUIRE(builder.Append(value_at(row)).ok());
        }
    }
    const auto table = arrow::Table::Make(arrow::schema({arrow::field("b", arrow::boolean())}),
                                          {builder.Finish().ValueOrDie()});
    const auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, kRowGroupRows,
                                       parquet::WriterProperties::Builder().build())
                .ok());
    const auto buffer = output->Finish().ValueOrDie();
    return make_parquet_reader(std::make_shared<arrow::io::BufferReader>(buffer), "memory");
}

/// `entry` holds `rows`, the source rows in order, with their values and nulls.
void check(const ibex::runtime::ColumnEntry& entry, const std::vector<std::size_t>& rows,
           bool nulls) {
    const auto& values = std::get<ibex::Column<bool>>(*entry.column);
    REQUIRE(values.size() == rows.size());
    // A decode that met no null keeps no bitmap.
    const bool any_null = nulls && std::ranges::any_of(rows, null_at);
    const ibex::runtime::ValidityBitmap* validity =
        entry.validity.has_value() ? &*entry.validity : nullptr;
    REQUIRE((validity != nullptr) == any_null);
    for (std::size_t i = 0; i < rows.size(); ++i) {
        INFO("output row " << i << " source row " << rows[i]);
        const bool is_null = nulls && null_at(rows[i]);
        if (validity != nullptr) {
            REQUIRE((*validity)[i] == !is_null);
        }
        if (!is_null) {
            REQUIRE(values[i] == value_at(rows[i]));
        }
    }
    if (const std::size_t rem = rows.size() % 64; rem != 0) {
        REQUIRE((values.words_data()[values.word_count() - 1] >> rem) == 0U);
    }
}

}  // namespace

TEST_CASE("Parquet Bool columns decode whole, selected and split by row group",
          "[parquet][bool-decode]") {
    std::vector<std::size_t> all(kRows);
    for (std::size_t row = 0; row < kRows; ++row) {
        all[row] = row;
    }
    for (const bool nulls : {false, true}) {
        CAPTURE(nulls);
        auto reader = bool_reader(nulls);
        const auto& metadata = *reader->parquet_reader()->metadata();
        REQUIRE(metadata.num_row_groups() > 2);
        const auto groups = all_decode_groups(metadata);
        const auto field = arrow::field("b", arrow::boolean());

        check(direct_column(*reader, *field, 0, nullptr, groups, kRows), all, nulls);

        // Sparse and dense selections, crossing row groups.
        for (const std::size_t stride : {1U, 3U, 61U, 400U}) {
            CAPTURE(stride);
            ibex::runtime::Selection selection;
            std::vector<std::size_t> picked;
            for (std::size_t row = 1; row < kRows; row += stride) {
                selection.push_back(row);
                picked.push_back(row);
            }
            check(direct_column(*reader, *field, 0, &selection, groups, picked.size()), picked,
                  nulls);
        }

        auto plan = plan_sharded_column(*reader, *field, 0, nullptr, groups, kRows);
        // Only a null-free column is split.
        REQUIRE(plan.has_value() == !nulls);
        if (plan.has_value()) {
            // Shards out of order, as workers may run them.
            std::size_t output_start = 0;
            std::vector<DirectDecodeGroups> ranges;
            std::vector<std::size_t> starts;
            for (int group = 0; group < metadata.num_row_groups(); ++group) {
                const auto rows = static_cast<std::size_t>(metadata.RowGroup(group)->num_rows());
                ranges.push_back(DirectDecodeGroups{
                    .begin = group, .end = group + 1, .source_start = output_start, .rows = rows});
                starts.push_back(output_start);
                output_start += rows;
            }
            for (std::size_t i = ranges.size(); i-- > 0;) {
                REQUIRE(plan->decode_range(*reader, ranges[i], starts[i], ranges[i].rows) ==
                        ranges[i].rows);
            }
            REQUIRE(plan->finalize);
            plan->finalize(plan->entry);
            check(plan->entry, all, false);
        }
    }
}
