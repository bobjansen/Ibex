// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The writer dictionary-encodes null-free Int64, Double and text chunks itself
// (`write_dictionary_numbers`, `write_dictionary_text`): a PLAIN dictionary page,
// then RLE_DICTIONARY data pages of hybrid RLE / bit-packed indices
// (`encode_hybrid`). Every file here is read back by Arrow's own reader, not
// Ibex's, so a malformed run header, a misaligned literal group or a wrong bit
// width fails as a decode error or a wrong value. The run patterns put runs of
// eight or more at every offset modulo eight, so a literal group that borrows
// from the run after it is exercised at each fill.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "../libs/parquet/parquet.hpp"

namespace {

class TempParquet {
   public:
    TempParquet()
        : path_(std::filesystem::temp_directory_path() /
                ("ibex_writer_dictionary_" + std::to_string(::getpid()) + ".parquet")) {}
    TempParquet(const TempParquet&) = delete;
    TempParquet(TempParquet&&) = delete;
    auto operator=(const TempParquet&) -> TempParquet& = delete;
    auto operator=(TempParquet&&) -> TempParquet& = delete;
    ~TempParquet() {
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }
    [[nodiscard]] auto path() const -> const std::filesystem::path& { return path_; }

   private:
    std::filesystem::path path_;
};

/// Reads `path` with Arrow's reader into one contiguous column `x`.
auto read_back(const std::filesystem::path& path) -> std::shared_ptr<arrow::Array> {
    auto input = arrow::io::ReadableFile::Open(path.string()).ValueOrDie();
    auto reader = parquet::arrow::OpenFile(input, arrow::default_memory_pool()).ValueOrDie();
    std::shared_ptr<arrow::Table> table;
    REQUIRE(reader->ReadTable(&table).ok());
    auto column = table->GetColumnByName("x");
    REQUIRE(column != nullptr);
    return arrow::Concatenate(column->chunks()).ValueOrDie();
}

/// Every row group of `path` dictionary-encodes column 0.
auto every_group_is_dictionary_encoded(const std::filesystem::path& path) -> bool {
    const auto file = parquet::ParquetFileReader::OpenFile(path.string());
    const auto metadata = file->metadata();
    for (int g = 0; g < metadata->num_row_groups(); ++g) {
        const auto chunk = metadata->RowGroup(g)->ColumnChunk(0);
        const auto& encodings = chunk->encodings();
        if (!chunk->has_dictionary_page() ||
            std::ranges::find(encodings, parquet::Encoding::RLE_DICTIONARY) == encodings.end()) {
            return false;
        }
    }
    return true;
}

/// Index-run patterns: random-ish literals with a run of `run` equal values
/// starting at each offset 0..15, plus whole-column and tiny shapes.
auto int_patterns() -> std::vector<std::pair<std::string, std::vector<std::int64_t>>> {
    std::vector<std::pair<std::string, std::vector<std::int64_t>>> out;
    for (const std::size_t run : {7U, 8U, 9U, 15U, 16U, 17U, 40U}) {
        std::vector<std::int64_t> values;
        for (std::size_t offset = 0; offset < 16; ++offset) {
            for (std::size_t i = 0; i < offset; ++i) {
                values.push_back(static_cast<std::int64_t>((values.size() * 2654435761U) % 97));
            }
            values.insert(values.end(), run, 500 + static_cast<std::int64_t>(offset));
        }
        out.emplace_back("runs of " + std::to_string(run), std::move(values));
    }
    for (const std::size_t n : {1U, 7U, 8U, 9U, 16U, 17U}) {
        out.emplace_back("constant x" + std::to_string(n), std::vector<std::int64_t>(n, -3));
        std::vector<std::int64_t> distinct(n);
        for (std::size_t i = 0; i < n; ++i) {
            distinct[i] = static_cast<std::int64_t>(i * 3);
        }
        out.emplace_back("distinct x" + std::to_string(n), std::move(distinct));
    }
    // A wide range takes the hashed dictionary rather than the direct table,
    // and 300 entries need a 9-bit index.
    std::vector<std::int64_t> wide;
    for (std::size_t i = 0; i < 5'000; ++i) {
        const auto k = static_cast<std::int64_t>((i * 7919) % 300);
        wide.push_back(k % 2 == 0 ? k * 1'000'000'007LL
                                  : std::numeric_limits<std::int64_t>::min() + k);
    }
    out.emplace_back("wide range", std::move(wide));
    return out;
}

}  // namespace

TEST_CASE("Parquet writer dictionary-encodes Int64 chunks Arrow reads back exactly",
          "[parquet][writer]") {
    const TempParquet file;
    for (const auto& [name, values] : int_patterns()) {
        for (const std::int64_t row_group : {std::int64_t{100'000}, std::int64_t{37}}) {
            INFO(name << ", row groups of " << row_group);
            ibex::runtime::Table table;
            table.add_column("x", ibex::Column<std::int64_t>{values});
            REQUIRE(ibex::ext::parquet::write(table, file.path().string(), "zstd", 3, row_group) ==
                    static_cast<std::int64_t>(values.size()));
            // 37-row groups of the 300-value column are nearly all distinct,
            // which `dictionary_pays` writes PLAIN; the values must still match.
            if (name != "wide range" || row_group != 37) {
                CHECK(every_group_is_dictionary_encoded(file.path()));
            }
            const auto back = read_back(file.path());
            REQUIRE(back->length() == static_cast<std::int64_t>(values.size()));
            const auto& ints = static_cast<const arrow::Int64Array&>(*back);
            for (std::size_t i = 0; i < values.size(); ++i) {
                if (ints.Value(static_cast<std::int64_t>(i)) != values[i]) {
                    FAIL("row " << i << ": " << ints.Value(static_cast<std::int64_t>(i))
                                << " != " << values[i]);
                }
            }
        }
    }
}

TEST_CASE("Parquet writer dictionary-encodes Double chunks bit for bit", "[parquet][writer]") {
    const TempParquet file;
    const std::vector<double> specials{
        0.5, -0.0, 0.0, std::numeric_limits<double>::infinity(), 1e300, std::nan(""), -2.25};
    std::vector<double> values;
    for (std::size_t i = 0; i < 3'000; ++i) {
        values.push_back(specials[(i * 2654435761U >> 5) % specials.size()]);
    }
    ibex::runtime::Table table;
    table.add_column("x", ibex::Column<double>{values});
    ibex::ext::parquet::write(table, file.path().string(), "zstd", 3, 1'000);
    CHECK(every_group_is_dictionary_encoded(file.path()));
    const auto back = read_back(file.path());
    const auto& doubles = static_cast<const arrow::DoubleArray&>(*back);
    REQUIRE(doubles.length() == static_cast<std::int64_t>(values.size()));
    for (std::size_t i = 0; i < values.size(); ++i) {
        const double got = doubles.Value(static_cast<std::int64_t>(i));
        if (std::bit_cast<std::uint64_t>(got) != std::bit_cast<std::uint64_t>(values[i])) {
            FAIL("row " << i << ": " << got << " != " << values[i]);
        }
    }
}

TEST_CASE("Parquet writer dictionary-encodes text chunks Arrow reads back exactly",
          "[parquet][writer]") {
    const TempParquet file;
    std::vector<std::string> labels{"", "alpha", std::string(300, 'x'), "\xc3\xbcnicode"};
    for (int i = 0; i < 200; ++i) {
        labels.push_back("w" + std::to_string(i));
    }
    std::vector<std::string> strings;
    ibex::Column<ibex::Categorical>::codes_storage codes;
    for (std::size_t i = 0; i < 4'000; ++i) {
        // Runs of 13 every other stretch, scattered codes in between.
        const auto code =
            (i / 13) % 2 == 0 ? (i / 13) % labels.size() : (i * 2654435761U >> 3) % labels.size();
        strings.push_back(labels[code]);
        codes.push_back(static_cast<ibex::Column<ibex::Categorical>::code_type>(code));
    }
    for (const bool categorical : {false, true}) {
        INFO((categorical ? "Categorical" : "String"));
        ibex::runtime::Table table;
        if (categorical) {
            table.add_column("x", ibex::Column<ibex::Categorical>(labels, codes));
        } else {
            table.add_column("x", ibex::Column<std::string>{strings});
        }
        ibex::ext::parquet::write(table, file.path().string(), "zstd", 3, 1'500);
        CHECK(every_group_is_dictionary_encoded(file.path()));
        const auto back = read_back(file.path());
        REQUIRE(back->length() == static_cast<std::int64_t>(strings.size()));
        for (std::size_t i = 0; i < strings.size(); ++i) {
            const auto got = back->type_id() == arrow::Type::LARGE_STRING
                                 ? static_cast<const arrow::LargeStringArray&>(*back).GetView(
                                       static_cast<std::int64_t>(i))
                                 : static_cast<const arrow::StringArray&>(*back).GetView(
                                       static_cast<std::int64_t>(i));
            if (got != strings[i]) {
                FAIL("row " << i << ": '" << got << "' != '" << strings[i] << "'");
            }
        }
        // Chunk statistics come from the dictionary: the smallest and largest
        // label present in the row group.
        const auto reader = parquet::ParquetFileReader::OpenFile(file.path().string());
        const auto stats = reader->metadata()->RowGroup(0)->ColumnChunk(0)->encoded_statistics();
        REQUIRE(stats != nullptr);
        CHECK(stats->min() == "");
        CHECK(stats->max() == "\xc3\xbcnicode");
    }
}
