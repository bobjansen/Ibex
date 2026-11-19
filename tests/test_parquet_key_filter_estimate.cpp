// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/memory.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/types.h>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../libs/parquet/parquet.hpp"

// A streamed unit's bitmap filter is skipped when its row groups' footer ranges
// predict it passes most rows (`footer_estimated_pass`), and kept per unit, so
// keys clustered by row group keep their pruning where the build is sparse.
namespace {

/// Four 1000-row groups of one Int64 column `k`: keys 0..999, 1000..1999,
/// 100000..100999 and 0..999 again.
auto four_group_metadata(bool statistics) -> std::shared_ptr<parquet::FileMetaData> {
    arrow::Int64Builder builder;
    for (const std::int64_t start : {0, 1000, 100000, 0}) {
        for (std::int64_t k = start; k < start + 1000; ++k) {
            REQUIRE(builder.Append(k).ok());
        }
    }
    auto table = arrow::Table::Make(arrow::schema({arrow::field("k", arrow::int64())}),
                                    {builder.Finish().ValueOrDie()});
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    parquet::WriterProperties::Builder properties;
    if (!statistics) {
        properties.disable_statistics();
    }
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 1000,
                                       properties.build())
                .ok());
    auto buffer = output->Finish().ValueOrDie();
    auto reader =
        parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
    auto metadata = reader->metadata();
    REQUIRE(metadata->num_row_groups() == 4);
    return metadata;
}

/// Build keys 0..1999, every tenth one missing below 1000 and every other one
/// missing from 1000 on: density 0.9 over the first group, 0.5 over the second.
auto build_filter() -> ibex::runtime::DynamicScanFilter {
    auto bitmap = std::make_shared<ibex::runtime::JoinKeyBitmap>(0, 1999);
    for (std::int64_t k = 0; k < 2000; ++k) {
        if ((k < 1000 && k % 10 != 0) || (k >= 1000 && k % 2 == 0)) {
            bitmap->insert(k);
        }
    }
    ibex::runtime::DynamicScanFilter filter;
    filter.min = 0;
    filter.max = 1999;
    filter.distinct_keys = bitmap->count();
    filter.bitmap = std::move(bitmap);
    filter.ready = true;
    return filter;
}

auto estimate(const parquet::FileMetaData& metadata, const ibex::runtime::DynamicScanFilter& filter,
              std::size_t start, std::size_t rows) -> std::optional<double> {
    return footer_estimated_pass<parquet::Int64Type>(
        metadata, 0, filter, ibex::runtime::SourceUnit{.start = start, .rows = rows});
}

}  // namespace

TEST_CASE("A unit's bitmap pass rate is estimated from its row groups' footer ranges",
          "[parquet][stream_filter]") {
    const auto metadata = four_group_metadata(true);
    const auto filter = build_filter();
    const auto approx = [](std::optional<double> got, double want) {
        REQUIRE(got.has_value());
        CHECK(*got > want - 1e-9);
        CHECK(*got < want + 1e-9);
    };
    approx(estimate(*metadata, filter, 0, 1000), 0.9);  // dense: skip the filter
    approx(estimate(*metadata, filter, 1000, 1000), 0.5);
    approx(estimate(*metadata, filter, 2000, 1000), 0.0);  // outside the build: filter
    approx(estimate(*metadata, filter, 0, 2000), 0.7);     // rows-weighted over two groups
    approx(estimate(*metadata, filter, 3000, 1000), 0.9);
    // Only the dense units pass the source's abandon threshold.
    const auto passes_threshold = [&](std::size_t start) {
        return estimate(*metadata, filter, start, 1000).value_or(0.0) > kAbandonPassRate;
    };
    CHECK(passes_threshold(0));
    CHECK_FALSE(passes_threshold(1000));
    CHECK_FALSE(passes_threshold(2000));
}

TEST_CASE("No footer estimate without statistics or without a bitmap", "[parquet][stream_filter]") {
    const auto bare = four_group_metadata(false);
    CHECK_FALSE(estimate(*bare, build_filter(), 0, 1000).has_value());
    const auto metadata = four_group_metadata(true);
    ibex::runtime::DynamicScanFilter range_only;
    range_only.min = 0;
    range_only.max = 1999;
    CHECK_FALSE(estimate(*metadata, range_only, 0, 1000).has_value());
}

TEST_CASE("Parquet dictionary scan keeps candidate rows across mixed encoding",
          "[parquet][dictionary]") {
    arrow::StringBuilder builder;
    const std::string long_value(256, 'x');
    const std::array<const char*, 12> values{"keep", "drop",  nullptr, "keep",
                                             "drop", "drop",  "keep",  long_value.c_str(),
                                             "keep", nullptr, "drop",  "keep"};
    for (const auto* value : values) {
        REQUIRE((value == nullptr ? builder.AppendNull() : builder.Append(value)).ok());
    }
    auto table = arrow::Table::Make(arrow::schema({arrow::field("v", arrow::utf8())}),
                                    {builder.Finish().ValueOrDie()});
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    parquet::WriterProperties::Builder properties;
    properties.dictionary_pagesize_limit(64)->write_batch_size(1);
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 6,
                                       properties.build())
                .ok());
    auto buffer = output->Finish().ValueOrDie();
    auto reader = make_parquet_reader(std::make_shared<arrow::io::BufferReader>(buffer), "memory");
    const auto& metadata = *reader->parquet_reader()->metadata();
    const auto groups = whole_file_scan_groups(metadata);
    REQUIRE(groups.size() == 2);
    REQUIRE(stats_prove_dictionary(*metadata.RowGroup(0)->ColumnChunk(0)));
    const auto mixed_chunk = metadata.RowGroup(1)->ColumnChunk(0);
    REQUIRE(mixed_chunk->has_dictionary_page());
    REQUIRE_FALSE(stats_prove_dictionary(*mixed_chunk));
    std::array readers{reader.get()};
    const ibex::runtime::DictionaryPredicate keep = [](std::span<const std::string_view> entries) {
        std::vector<char> flags;
        for (const auto value : entries) {
            flags.push_back(value == "keep" ? 1 : 0);
        }
        return flags;
    };

    const ibex::runtime::Selection candidates{0, 1, 2};
    auto selected = filtered_dictionary_string_selection(readers, 0, keep, &candidates, groups);
    REQUIRE(selected.has_value());
    CHECK(*selected == ibex::runtime::Selection{0});

    const ibex::runtime::Selection mixed{0, 2, 6, 7, 9, 10, 11};
    selected = filtered_dictionary_string_selection(readers, 0, keep, &mixed, groups);
    REQUIRE(selected);
    CHECK(*selected == ibex::runtime::Selection{0, 6, 11});
    selected = filtered_dictionary_string_selection(readers, 0, keep, nullptr, groups);
    REQUIRE(selected);
    CHECK(*selected == ibex::runtime::Selection{0, 3, 6, 8, 11});
}

TEST_CASE("Parquet String scan reads only indexed pages containing candidates",
          "[parquet][stream_filter]") {
    const auto indexed = GENERATE(false, true);
    constexpr std::int64_t rows = 1024;
    const std::string match(64, 'x');
    arrow::StringBuilder builder;
    for (std::int64_t row = 0; row < rows; ++row) {
        REQUIRE((row % 3 == 0 ? builder.AppendNull() : builder.Append(match)).ok());
    }
    auto table = arrow::Table::Make(arrow::schema({arrow::field("v", arrow::utf8())}),
                                    {builder.Finish().ValueOrDie()});
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    parquet::WriterProperties::Builder properties;
    properties.disable_dictionary()->data_pagesize(512)->write_batch_size(16);
    if (indexed) {
        properties.enable_write_page_index();
    } else {
        properties.disable_write_page_index();
    }
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 512,
                                       properties.build())
                .ok());
    auto input = std::make_shared<arrow::io::BufferReader>(output->Finish().ValueOrDie());
    auto reader = make_parquet_reader(input, "memory");
    const auto groups = whole_file_scan_groups(*reader->parquet_reader()->metadata());
    const ibex::runtime::Selection candidates{132, 133, 497, 901};
    ibex::runtime::StringScanFilter filter;
    filter.pattern.text = match;
    const auto stripes = string_page_stripes(*reader, 0, groups, 1, &candidates);
    CHECK(stripes.has_value() == indexed);
    std::optional<ibex::runtime::Selection> selected;
    if (indexed) {
        REQUIRE(stripes);
        std::size_t planned_rows = 0;
        for (const auto& stripe : *stripes) {
            CHECK_FALSE(string_candidates(candidates, stripe.base, stripe.rows).empty());
            planned_rows += stripe.rows;
        }
        CHECK(planned_rows < 128);
        selected = filtered_string_page_stripes(input, filter, *stripes, 1, &candidates);
    } else {
        const std::array readers{reader.get()};
        selected = filtered_string_selection(readers, 0, filter, groups, &candidates);
    }
    REQUIRE(selected);
    CHECK(*selected == ibex::runtime::Selection{133, 497, 901});
}
