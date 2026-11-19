// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// The row-group streaming source behind `parquet::read`'s chunked registration.
//
// Parquet stores one dictionary per row group, so the same string can sit at a
// different code in each. `ChunkedParquetSourceOperator` emits every row group
// over its own dictionary and leaves unification to the runtime, which wraps
// every extern chunked source (src/runtime/CONTRACTS.md §1). These tests pin
// both halves: the file's row groups really disagree, and the labels survive.

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <optional>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

#include "../libs/parquet/parquet.hpp"

using namespace ibex;

namespace {

using Cat = Column<Categorical>;

// Two row groups of four rows. "gamma" is local code 0 in group 1, where
// "alpha" sits in group 0, so trusting local codes reads every gamma as alpha.
//   group 0: alpha, beta, alpha, null   -> dictionary [alpha, beta]
//   group 1: gamma, beta, gamma, beta   -> dictionary [gamma, beta]
class DisagreeingRowGroups {
   public:
    DisagreeingRowGroups()
        : path_(std::filesystem::temp_directory_path() /
                ("ibex_chunked_parquet_" + std::to_string(::getpid()) + ".parquet")) {
        arrow::StringBuilder keys;
        arrow::Int64Builder values;
        REQUIRE(keys.Append("alpha").ok());
        REQUIRE(keys.Append("beta").ok());
        REQUIRE(keys.Append("alpha").ok());
        REQUIRE(keys.AppendNull().ok());
        for (const char* key : {"gamma", "beta", "gamma", "beta"}) {
            REQUIRE(keys.Append(key).ok());
        }
        for (std::int64_t v = 1; v <= 8; ++v) {
            REQUIRE(values.Append(v).ok());
        }
        const auto table = arrow::Table::Make(
            arrow::schema({arrow::field("k", arrow::utf8()), arrow::field("v", arrow::int64())}),
            {keys.Finish().ValueOrDie(), values.Finish().ValueOrDie()});
        const auto output = arrow::io::FileOutputStream::Open(path_.string()).ValueOrDie();
        REQUIRE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output, 4,
                                           parquet::WriterProperties::Builder().build())
                    .ok());
        REQUIRE(output->Close().ok());
    }
    DisagreeingRowGroups(const DisagreeingRowGroups&) = delete;
    DisagreeingRowGroups& operator=(const DisagreeingRowGroups&) = delete;
    DisagreeingRowGroups(DisagreeingRowGroups&&) = delete;
    DisagreeingRowGroups& operator=(DisagreeingRowGroups&&) = delete;
    ~DisagreeingRowGroups() {
        std::error_code ignored;
        std::filesystem::remove(path_, ignored);
    }

    [[nodiscard]] auto path() const -> std::string { return path_.string(); }

   private:
    std::filesystem::path path_;
};

auto run(const std::string& path, const std::string& query, std::size_t cores)
    -> std::expected<runtime::Table, std::string> {
    runtime::ExternRegistry externs;
    externs.register_chunked_table("groups", [path](const runtime::ExternArgs&) {
        return ChunkedParquetSourceOperator::create(path);
    });
    auto parsed = parser::parse("extern fn groups() -> DataFrame from \"x.hpp\"; " + query);
    REQUIRE(parsed.has_value());
    auto ir = parser::lower(*parsed);
    REQUIRE(ir.has_value());
    runtime::ExecutionContext exec;
    exec.parallel_threads = cores;
    const runtime::TableRegistry registry;
    return runtime::interpret(**ir, registry, nullptr, &externs, nullptr, exec);
}

auto labels(const runtime::Table& table, const std::string& name) -> std::vector<std::string> {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    const auto* col = std::get_if<Cat>(entry->column.get());
    REQUIRE(col != nullptr);
    std::vector<std::string> out;
    out.reserve(col->size());
    for (std::size_t row = 0; row < col->size(); ++row) {
        const bool valid = !entry->validity.has_value() || (*entry->validity)[row];
        out.emplace_back(valid ? std::string((*col)[row]) : std::string("<null>"));
    }
    return out;
}

auto ints(const runtime::Table& table, const std::string& name) -> std::vector<std::int64_t> {
    const auto* col = std::get_if<Column<std::int64_t>>(table.find(name));
    REQUIRE(col != nullptr);
    std::vector<std::int64_t> out;
    out.reserve(col->size());
    for (const std::int64_t value : *col) {
        out.push_back(value);
    }
    return out;
}

}  // namespace

TEST_CASE("Parquet chunked source emits each row group over its own dictionary",
          "[parquet][chunked][categorical][dictionary]") {
    const DisagreeingRowGroups file;
    auto source = ChunkedParquetSourceOperator::create(file.path());
    REQUIRE(source.has_value());
    std::vector<runtime::Chunk> chunks;
    while (true) {
        auto chunk = (*source)->next();
        REQUIRE(chunk.has_value());
        if (!chunk->has_value()) {
            break;
        }
        chunks.push_back(std::move(**chunk));
    }
    REQUIRE(chunks.size() == 2);
    const auto* first = std::get_if<Cat>(chunks[0].columns[0].column.get());
    const auto* second = std::get_if<Cat>(chunks[1].columns[0].column.get());
    REQUIRE(first != nullptr);
    REQUIRE(second != nullptr);
    // The premise of the tests below: the same local code names different
    // strings, so only a unifying consumer gets the labels right.
    CHECK(first->code_at(0) == second->code_at(0));
    CHECK((*first)[0] == "alpha");
    CHECK((*second)[0] == "gamma");
}

TEST_CASE("Parquet chunked source keeps its labels across row-group dictionaries",
          "[parquet][chunked][categorical][dictionary]") {
    const DisagreeingRowGroups file;
    for (const std::size_t cores : {1U, 8U}) {
        CAPTURE(cores);
        SECTION("materialized") {
            auto out = run(file.path(), "groups();", cores);
            REQUIRE(out.has_value());
            CHECK(labels(*out, "k") == std::vector<std::string>{"alpha", "beta", "alpha", "<null>",
                                                                "gamma", "beta", "gamma", "beta"});
            CHECK(ints(*out, "v") == std::vector<std::int64_t>{1, 2, 3, 4, 5, 6, 7, 8});
        }
        SECTION("filtered on a label") {
            auto out = run(file.path(), "groups()[filter k == \"gamma\"];", cores);
            REQUIRE(out.has_value());
            CHECK(ints(*out, "v") == std::vector<std::int64_t>{5, 7});
        }
        SECTION("grouped") {
            auto out =
                run(file.path(),
                    "groups()[filter k != \"\", select { s = sum(v) }, by k, order k];", cores);
            REQUIRE(out.has_value());
            CHECK(labels(*out, "k") == std::vector<std::string>{"alpha", "beta", "gamma"});
            CHECK(ints(*out, "s") == std::vector<std::int64_t>{4, 16, 12});
        }
    }
}
