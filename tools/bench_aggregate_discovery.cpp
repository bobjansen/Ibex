// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <CLI/CLI.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <exception>
#include <expected>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {
using namespace ibex;
using namespace ibex::runtime;

class PreparedSource final : public Operator {
   public:
    PreparedSource(std::vector<Chunk> chunks, std::size_t& offered)
        : chunks_(std::move(chunks)), offered_(&offered) {}
    auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (next_ == chunks_.size())
            return std::optional<Chunk>{};
        auto chunk = std::move(chunks_[next_++]);
        *offered_ += chunk.rows();
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    std::vector<Chunk> chunks_;
    std::size_t next_ = 0;
    std::size_t* offered_;
};

auto prepare(std::size_t rows, std::size_t cardinality, std::size_t chunk_rows, bool generic)
    -> std::vector<Chunk> {
    std::vector<Chunk> chunks;
    const std::size_t grain = chunk_rows == 0 ? rows : chunk_rows;
    for (std::size_t begin = 0; begin < rows;) {
        const std::size_t end = begin + std::min(grain, rows - begin);
        Column<std::int64_t> a, b, c, v;
        Column<double> f;
        for (std::size_t row = begin; row < end; ++row) {
            const std::size_t key = row % cardinality;
            a.push_back(
                static_cast<std::int64_t>(static_cast<std::uint64_t>(key) * 0x9e3779b97f4a7c15ULL));
            b.push_back(static_cast<std::int64_t>(key % 991));
            c.push_back(static_cast<std::int64_t>(key % 7));
            v.push_back(static_cast<std::int64_t>(row % 101));
            if (generic)
                f.push_back(static_cast<double>(key % 17) * 0.25);
        }
        Table table;
        table.add_column("a", std::move(a));
        table.add_column("b", std::move(b));
        table.add_column("c", std::move(c));
        table.add_column("v", std::move(v));
        if (generic)
            table.add_column("f", std::move(f));
        Chunk chunk;
        chunk.columns = std::move(table.columns);
        chunks.push_back(std::move(chunk));
        begin = end;
    }
    return chunks;
}
}  // namespace

int main(int argc, char** argv) {
    using namespace ibex;
    using namespace ibex::runtime;
    CLI::App app{"Paired aggregate Discovery policy sweep; raw CSV goes to stdout"};
    std::vector<std::size_t> row_counts{65536, 262144};
    std::vector<std::size_t> cardinalities{64, 131072};
    std::vector<std::size_t> chunk_sizes{0, 8192};
    std::vector<std::size_t> core_counts{1, 8};
    std::size_t repeats = 8;
    std::string shape = "pair";
    std::string aggregate = "sum";
    AggregateDiscoveryPolicy target;
    app.add_option("--rows", row_counts)->delimiter(',');
    app.add_option("--cardinalities", cardinalities)->delimiter(',');
    app.add_option("--chunks", chunk_sizes, "0 means one whole-table chunk")->delimiter(',');
    app.add_option("--cores", core_counts)->delimiter(',');
    app.add_option("--repeats", repeats)->check(CLI::PositiveNumber);
    app.add_option("--shape", shape)->check(CLI::IsMember({"int", "pair", "packed", "generic"}));
    app.add_option("--aggregate", aggregate)->check(CLI::IsMember({"count", "sum", "both"}));
    app.add_option("--radix-rows", target.radix_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--packed-rows", target.packed_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--owned-int-rows", target.int_owned_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--owned-pair-rows", target.pair_owned_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--sorted-rows", target.sorted_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--sorted-range-rows", target.sorted_range_rows)->check(CLI::PositiveNumber);
    app.add_option("--hot-job-rows", target.async_hot_job_rows)->check(CLI::PositiveNumber);
    app.add_option("--pair-max-first-chunk-rows", target.pair_owned_max_first_chunk_rows)
        ->check(CLI::PositiveNumber);
    CLI11_PARSE(app, argc, argv);
    if (!target.valid()) {
        std::fprintf(stderr, "Policy rows and grains must be positive\n");
        return 1;
    }
    if (repeats % 2 != 0)
        ++repeats;
    const std::string keys = shape == "int"       ? "a"
                             : shape == "packed"  ? "a, b, c"
                             : shape == "generic" ? "a, f"
                                                  : "a, b";
    const std::string aggs = aggregate == "count"  ? "n = count()"
                             : aggregate == "both" ? "n = count(), s = sum(v)"
                                                   : "s = sum(v)";
    const std::string source =
        "extern fn policy_input() -> DataFrame from \"bench.hpp\"; "
        "policy_input()[select { " +
        aggs + " }, by { " + keys + " }];";
    try {
        auto parsed = parser::parse(source);
        if (!parsed)
            throw std::runtime_error(parsed.error().format());
        auto lowered = parser::lower(*parsed);
        if (!lowered)
            throw std::runtime_error(lowered.error().message);
        std::puts(
            "rows,cardinality,chunk_rows,cores,shape,aggregate,repeat,side,ms,cpu_ms,source_rows,"
            "groups,fanouts");
        for (const auto rows : row_counts)
            for (const auto requested : cardinalities)
                for (const auto chunk_rows : chunk_sizes) {
                    if (rows == 0 || requested == 0)
                        throw std::runtime_error("Rows and cardinalities must be positive");
                    const std::size_t cardinality = std::min(requested, rows);
                    const auto chunks = prepare(rows, cardinality, chunk_rows, shape == "generic");
                    std::size_t offered = 0;
                    ExternRegistry externs;
                    externs.register_chunked_table("policy_input", [&](const ExternArgs&) {
                        return std::expected<OperatorPtr, std::string>{
                            std::make_unique<PreparedSource>(chunks, offered)};
                    });
                    const TableRegistry registry;
                    ExecutionContext reference_exec;
                    reference_exec.parallel_threads = 1;
                    auto reference =
                        interpret(**lowered, registry, nullptr, &externs, nullptr, reference_exec);
                    if (!reference)
                        throw std::runtime_error(reference.error());
                    if (reference->rows() != cardinality)
                        throw std::runtime_error("Incorrect reference group cardinality");
                    const auto integer = [&](const char* name, std::size_t group) {
                        return std::get<Column<std::int64_t>>(*reference->find(name))[group];
                    };
                    // Group g first appears at row g, then repeats every cardinality
                    // rows. Check serial execution against this independent formula.
                    for (std::size_t group = 0; group < cardinality; ++group) {
                        const auto key = static_cast<std::int64_t>(
                            static_cast<std::uint64_t>(group) * 0x9e3779b97f4a7c15ULL);
                        if (integer("a", group) != key)
                            throw std::runtime_error("Incorrect reference group order");
                        if ((shape == "pair" || shape == "packed") &&
                            integer("b", group) != static_cast<std::int64_t>(group % 991))
                            throw std::runtime_error("Incorrect reference pair key");
                        if (shape == "packed" &&
                            integer("c", group) != static_cast<std::int64_t>(group % 7))
                            throw std::runtime_error("Incorrect reference packed key");
                        if (shape == "generic" &&
                            std::get<Column<double>>(*reference->find("f"))[group] !=
                                static_cast<double>(group % 17) * 0.25)
                            throw std::runtime_error("Incorrect reference floating key");
                        if (aggregate != "sum" &&
                            integer("n", group) !=
                                static_cast<std::int64_t>((rows - 1 - group) / cardinality + 1))
                            throw std::runtime_error("Incorrect reference count");
                        if (aggregate != "count") {
                            std::int64_t sum = 0;
                            for (std::size_t row = group; row < rows; row += cardinality)
                                sum += static_cast<std::int64_t>(row % 101);
                            if (integer("s", group) != sum)
                                throw std::runtime_error("Incorrect reference sum");
                        }
                    }
                    for (const auto cores : core_counts) {
                        if (cores == 0)
                            throw std::runtime_error("Cores must be positive");
                        const auto run = [&](bool candidate, std::size_t repeat, bool emit) {
                            offered = 0;
                            ExecutionContext exec;
                            exec.parallel_threads = cores;
                            exec.aggregate_discovery_policy =
                                candidate ? target : AggregateDiscoveryPolicy{};
                            ParallelPipelineStats stats;
                            exec.parallel_stats = &stats;
                            const auto cpu_start = std::clock();
                            const auto start = std::chrono::steady_clock::now();
                            auto result =
                                interpret(**lowered, registry, nullptr, &externs, nullptr, exec);
                            const double ms = std::chrono::duration<double, std::milli>(
                                                  std::chrono::steady_clock::now() - start)
                                                  .count();
                            const double cpu_ms = 1000.0 *
                                                  static_cast<double>(std::clock() - cpu_start) /
                                                  CLOCKS_PER_SEC;
                            if (!result)
                                throw std::runtime_error(result.error());
                            if (offered != rows)
                                throw std::runtime_error("Source rows multiplied or lost");
                            if (const auto mismatch = compare_tables(*reference, *result))
                                throw std::runtime_error(mismatch->message());
                            if (emit) {
                                std::printf("%zu,%zu,%zu,%zu,%s,%s,%zu,%s,%.6f,%.6f,%zu,%zu,%llu\n",
                                            rows, cardinality, chunk_rows, cores, shape.c_str(),
                                            aggregate.c_str(), repeat,
                                            candidate ? "target" : "default", ms, cpu_ms, offered,
                                            result->rows(),
                                            static_cast<unsigned long long>(
                                                stats.parallel_aggregate_partitions.load()));
                                std::fflush(stdout);
                            }
                        };
                        run(false, 0, false);
                        run(true, 0, false);
                        for (std::size_t repeat = 0; repeat < repeats; ++repeat) {
                            run(repeat % 2 != 0, repeat, true);
                            run(repeat % 2 == 0, repeat, true);
                        }
                    }
                }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 1;
    }
    return 0;
}
