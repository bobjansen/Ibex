// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "ibex_bench_parquet.hpp"

#include <ibex/format.hpp>
#include <ibex/parquet/backend.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <array>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
#include <filesystem>
#include <memory>
#include <parquet/arrow/writer.h>
#include <parquet/platform.h>
#include <parquet/properties.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::bench_parquet {

namespace {

/// Bump when the generator or the writer settings change, so a build never
/// reads files an older generator wrote under the same row count.
constexpr int kGeneratorVersion = 1;

/// PDS-H's files (Polars' writer) have ~1M-row groups, ZSTD pages and footer
/// statistics; the per-unit scan decisions depend on all three.
constexpr std::int64_t kRowGroupRows = std::int64_t{1} << 20U;

/// splitmix64: deterministic across platforms and standard libraries, unlike
/// the <random> distributions.
auto mix(std::uint64_t x) -> std::uint64_t {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31U);
}

auto check(const arrow::Status& status, std::string_view what) -> void {
    if (!status.ok()) {
        throw std::runtime_error(ibex::formatting::format("{}: {}", what, status.ToString()));
    }
}

template <typename T>
auto check(arrow::Result<T> result, std::string_view what) -> T {
    check(result.status(), what);
    return std::move(result).ValueUnsafe();
}

auto int_column(std::size_t rows, auto&& value) -> std::shared_ptr<arrow::Array> {
    arrow::Int64Builder builder;
    check(builder.Reserve(static_cast<std::int64_t>(rows)), "reserve");
    for (std::size_t i = 0; i < rows; ++i) {
        builder.UnsafeAppend(value(i));
    }
    return check(builder.Finish(), "finish Int64 column");
}

/// Writes to `<path>.tmp` and renames, so an interrupted run never leaves a
/// partial file that a later run would take as finished.
auto write_table(const std::shared_ptr<arrow::Table>& table, const std::filesystem::path& path)
    -> void {
    const auto partial = std::filesystem::path(path.string() + ".tmp");
    {
        auto sink = check(arrow::io::FileOutputStream::Open(partial.string()), "open");
        auto properties = ::parquet::WriterProperties::Builder()
                              .compression(::parquet::Compression::ZSTD)
                              ->max_row_group_length(kRowGroupRows)
                              ->build();
        check(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink,
                                           kRowGroupRows, properties),
              "write " + path.string());
        check(sink->Close(), "close");
    }
    std::filesystem::rename(partial, path);
}

auto write_fact(const std::filesystem::path& path, std::size_t rows, std::size_t dim_rows) -> void {
    const auto k = int_column(
        rows, [&](std::size_t i) { return static_cast<std::int64_t>(1 + (mix(i) % dim_rows)); });
    const auto c =
        int_column(rows, [](std::size_t i) { return static_cast<std::int64_t>((i / 4) + 1); });
    const auto q = int_column(
        rows, [](std::size_t i) { return static_cast<std::int64_t>(1 + (mix(i ^ 0x51ULL) % 50)); });
    arrow::DoubleBuilder v_builder;
    check(v_builder.Reserve(static_cast<std::int64_t>(rows)), "reserve");
    arrow::StringBuilder flag_builder;
    check(flag_builder.Reserve(static_cast<std::int64_t>(rows)), "reserve");
    constexpr std::array<std::string_view, 3> kFlags = {"A", "N", "R"};
    for (std::size_t i = 0; i < rows; ++i) {
        v_builder.UnsafeAppend(static_cast<double>(mix(i ^ 0x76ULL) % 100000) / 100.0);
        check(flag_builder.Append(kFlags[mix(i ^ 0x66ULL) % kFlags.size()]), "append flag");
    }
    const auto v = check(v_builder.Finish(), "finish v");
    const auto flag = check(flag_builder.Finish(), "finish flag");
    const auto schema = arrow::schema({
        arrow::field("k", arrow::int64(), false),
        arrow::field("c", arrow::int64(), false),
        arrow::field("v", arrow::float64(), false),
        arrow::field("q", arrow::int64(), false),
        arrow::field("flag", arrow::utf8(), false),
    });
    write_table(arrow::Table::Make(schema, {k, c, v, q, flag}), path);
}

auto write_dim(const std::filesystem::path& path, std::size_t rows) -> void {
    const auto k = int_column(rows, [](std::size_t i) { return static_cast<std::int64_t>(i + 1); });
    const auto size = int_column(rows, [](std::size_t i) {
        return static_cast<std::int64_t>(1 + (mix((i + 1) ^ 0x73ULL) % 50));
    });
    const auto schema = arrow::schema({
        arrow::field("k", arrow::int64(), false),
        arrow::field("size", arrow::int64(), false),
    });
    write_table(arrow::Table::Make(schema, {k, size}), path);
}

auto write_orders(const std::filesystem::path& path, std::size_t rows) -> void {
    const auto c = int_column(rows, [](std::size_t i) { return static_cast<std::int64_t>(i + 1); });
    const auto w = int_column(rows, [](std::size_t i) {
        return static_cast<std::int64_t>(mix((i + 1) ^ 0x77ULL) % 1000);
    });
    const auto schema = arrow::schema({
        arrow::field("c", arrow::int64(), false),
        arrow::field("w", arrow::int64(), false),
    });
    write_table(arrow::Table::Make(schema, {c, w}), path);
}

auto externs() -> runtime::ExternRegistry& {
    static auto* registry = [] {
        auto* created = new runtime::ExternRegistry();  // NOLINT(cppcoreguidelines-owning-memory)
        parquet::register_backend(*created);
        return created;
    }();
    return *registry;
}

}  // namespace

auto prepare_files(const std::filesystem::path& directory, std::size_t fact_rows)
    -> std::expected<BenchFiles, std::string> {
    if (fact_rows < 64) {
        return std::unexpected("--parquet-rows must be at least 64");
    }
    BenchFiles files;
    files.fact_rows = fact_rows;
    // The PDS-H lineitem : part ratio (60M : 2M) and four fact rows per order.
    files.dim_rows = fact_rows / 30;
    files.order_rows = (fact_rows + 3) / 4;
    const auto dir = directory / ibex::formatting::format("v{}-{}", kGeneratorVersion, fact_rows);
    files.fact = (dir / "fact.parquet").string();
    files.dim = (dir / "dim.parquet").string();
    files.orders = (dir / "orders.parquet").string();
    try {
        std::filesystem::create_directories(dir);
        if (!std::filesystem::exists(files.fact)) {
            write_fact(files.fact, files.fact_rows, files.dim_rows);
        }
        if (!std::filesystem::exists(files.dim)) {
            write_dim(files.dim, files.dim_rows);
        }
        if (!std::filesystem::exists(files.orders)) {
            write_orders(files.orders, files.order_rows);
        }
    } catch (const std::exception& e) {
        return std::unexpected(ibex::formatting::format("writing benchmark Parquet files in {}: {}",
                                                        dir.string(), e.what()));
    }
    return files;
}

auto queries(const BenchFiles& files) -> std::vector<BenchQuery> {
    const auto read = [](const std::string& path) {
        return ibex::formatting::format("parquet::read(\"{}\")", path);
    };
    const std::string fact = read(files.fact);
    const std::string dim = read(files.dim);
    const std::string orders = read(files.orders);
    return {
        // Decode throughput: every row of two columns.
        {.name = "parquet_scan_sum",
         .expression = ibex::formatting::format("{}[select {{ s = sum(v), n = count() }}]", fact)},
        // A literal filter the footers cannot prune: the fused key scan.
        {.name = "parquet_filter_uniform",
         .expression = ibex::formatting::format("{}[filter q < 5, select {{ s = sum(v) }}]", fact)},
        // A literal range on a sorted column: whole row groups pruned by footer.
        {.name = "parquet_filter_clustered",
         .expression = ibex::formatting::format("{}[filter c <= {}, select {{ s = sum(v) }}]", fact,
                                                files.order_rows / 10)},
        // String dictionary decode and a low-cardinality group-by.
        {.name = "parquet_group_flag",
         .expression =
             ibex::formatting::format("{}[select {{ s = sum(v), n = count() }}, by flag]", fact)},
        // Every fact row matches: the streamed key filter is provably useless.
        {.name = "parquet_join_all",
         .expression = ibex::formatting::format(
             "({} join {} on k)[select {{ s = sum(v) }}, by size]", fact, dim)},
        // 90% of the dimension: a dense bitmap the footer estimate skips.
        {.name = "parquet_join_dense90",
         .expression = ibex::formatting::format(
             "({} join {}[filter k % 10 != 0] on k)[select {{ s = sum(v) }}, by size]", fact, dim)},
        // ~8% of the dimension: the streamed key filter prunes most rows.
        {.name = "parquet_join_selective",
         .expression = ibex::formatting::format(
             "({} join {}[filter size < 5] on k)[select {{ s = sum(v) }}, by size]", fact, dim)},
        // A prefix of a sorted key: the key filter's range prunes row groups.
        {.name = "parquet_join_clustered",
         .expression =
             ibex::formatting::format("({} join {}[filter c <= {}] on c)[select {{ s = sum(v) }}]",
                                      fact, orders, files.order_rows / 4)},
        // The streaming left join (every row kept, no key filter).
        {.name = "parquet_left_join",
         .expression = ibex::formatting::format(
             "({} left join {} on k)[select {{ s = sum(v) }}, by size]", fact, dim)},
        {.name = "parquet_semi_join",
         .expression = ibex::formatting::format(
             "({} semi join {}[filter size < 5] on k)[select {{ s = sum(v) }}]", fact, dim)},
        // q22's shape: a small left kept where a large scanned right misses.
        {.name = "parquet_anti_join",
         .expression = ibex::formatting::format(
             "({} anti join {}[filter q == 1] on k)[select {{ n = count() }}]", dim, fact)},
    };
}

auto run_query(const BenchQuery& query) -> std::expected<runtime::Table, std::string> {
    const std::string source = ibex::formatting::format(
        "extern fn parquet::read(path: String) -> DataFrame from \"parquet.hpp\";\n{};",
        query.expression);
    repl::ReplConfig config;
    config.persistent_history = false;
    repl::ReplSession session(config, externs());
    repl::ReplSession::ExecuteOptions options;
    options.render = false;
    options.plan_table_expressions = true;
    auto result = session.execute(source, std::move(options));
    if (!result.ok) {
        return std::unexpected(result.error);
    }
    if (!result.table.has_value()) {
        return std::unexpected("query produced no table");
    }
    return std::move(*result.table);
}

}  // namespace ibex::bench_parquet
