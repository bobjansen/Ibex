// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <catch2/catch_test_macros.hpp>

#include "../libs/parquet/parquet.hpp"

// These tests run with IBEX_COMPACT_INT32=1. They inspect physical buffers:
// equivalent Int64 output alone cannot detect accidental eager widening.
namespace {
auto int32_array(bool nulls) -> std::shared_ptr<arrow::Array> {
    arrow::Int32Builder builder;
    REQUIRE(builder.Append(INT32_MIN).ok());
    if (nulls)
        REQUIRE(builder.AppendNull().ok());
    else
        REQUIRE(builder.Append(0).ok());
    REQUIRE(builder.Append(INT32_MAX).ok());
    return builder.Finish().ValueOrDie();
}
auto memory_reader(const std::shared_ptr<arrow::Table>& table, bool statistics = true)
    -> std::unique_ptr<parquet::arrow::FileReader> {
    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();
    parquet::WriterProperties::Builder properties;
    if (!statistics)
        properties.disable_statistics();
    REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 2,
                                       properties.build())
                .ok());
    auto buffer = output->Finish().ValueOrDie();
    return make_parquet_reader(std::make_shared<arrow::io::BufferReader>(buffer), "memory");
}
void check_values(const ibex::runtime::ColumnEntry& entry, bool nulls) {
    const auto& values = std::get<ibex::Column<std::int64_t>>(*entry.column);
    REQUIRE(values.is_compact_int32());
    REQUIRE(values.size() == 3);
    CHECK(values.compact_int32_data()[0] == INT32_MIN);
    CHECK(values.compact_int32_data()[1] == 0);
    CHECK(values.compact_int32_data()[2] == INT32_MAX);
    REQUIRE(entry.validity.has_value() == nulls);
    if (nulls) {
        CHECK((*entry.validity)[0]);
        CHECK_FALSE((*entry.validity)[1]);
        CHECK((*entry.validity)[2]);
    }
}
}  // namespace

TEST_CASE("Parquet Arrow fallback retains single-chunk slices and normalizes nullable chunks",
          "[parquet][int32-ingestion]") {
    if (!ibex::runtime::compact_int32_enabled())
        SKIP("requires IBEX_COMPACT_INT32=1");
    auto array = int32_array(false)->Slice(1, 2);
    std::weak_ptr<const void> lifetime = array;
    auto table = arrow::Table::Make(arrow::schema({arrow::field("v", arrow::int32())}), {array});
    ibex::runtime::Table output;
    populate_from_arrow_table(table, output);
    const auto& adopted = std::get<ibex::Column<std::int64_t>>(*output.find("v"));
    REQUIRE(adopted.compact_int32_data() ==
            static_cast<const arrow::Int32Array&>(*array).raw_values());
    table.reset();
    array.reset();
    REQUIRE_FALSE(lifetime.expired());
    REQUIRE(adopted.compact_int32_data()[1] == INT32_MAX);
    output = {};
    REQUIRE(lifetime.expired());

    auto nullable = int32_array(true);
    auto chunks = std::make_shared<arrow::ChunkedArray>(
        arrow::ArrayVector{nullable->Slice(0, 1), nullable->Slice(1)});
    table = arrow::Table::Make(arrow::schema({arrow::field("v", arrow::int32())}), {chunks});
    populate_from_arrow_table(table, output);
    check_values(*output.find_entry("v"), true);
}

TEST_CASE(
    "Parquet direct Int32 decoding preserves width for nulls selections and missing statistics",
    "[parquet][int32-ingestion]") {
    if (!ibex::runtime::compact_int32_enabled())
        SKIP("requires IBEX_COMPACT_INT32=1");
    for (bool nulls : {false, true})
        for (bool statistics : {false, true}) {
            CAPTURE(nulls, statistics);
            auto table = arrow::Table::Make(arrow::schema({arrow::field("v", arrow::int32())}),
                                            {int32_array(nulls)});
            auto reader = memory_reader(table, statistics);
            const auto groups = all_decode_groups(*reader->parquet_reader()->metadata());
            auto entry = direct_column(*reader, *table->field(0), 0, nullptr, groups, 3);
            check_values(entry, nulls);
            ibex::runtime::Selection selection{0, 2};
            auto selected = direct_column(*reader, *table->field(0), 0, &selection, groups, 2);
            const auto& values = std::get<ibex::Column<std::int64_t>>(*selected.column);
            REQUIRE(values.is_compact_int32());
            REQUIRE(values.size() == 2);
            CHECK(values.compact_int32_data()[0] == INT32_MIN);
            CHECK(values.compact_int32_data()[1] == INT32_MAX);
            auto full = direct_decode_table(*reader, *table->schema(), {0}, nullptr, 3);
            check_values(*full.find_entry("v"), nulls);
            auto plan = plan_sharded_column(*reader, *table->field(0), 0, nullptr, groups, 3);
            if (!nulls && statistics) {
                REQUIRE(plan.has_value());
                REQUIRE(plan->decode_range(*reader, groups, 0, 3) == 3);
                check_values(plan->entry, false);
            }
        }
}
