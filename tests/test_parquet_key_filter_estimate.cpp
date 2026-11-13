// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>

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
#include <utility>

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
