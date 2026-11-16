// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <CLI/CLI.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <expected>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"

namespace {
void report(std::size_t rows, std::size_t groups, std::size_t cores, const std::string& operation,
            std::size_t repeat, bool candidate, std::chrono::steady_clock::time_point start,
            std::chrono::steady_clock::time_point end, std::clock_t cpu, std::clock_t cpu_end,
            const ibex::runtime::ExecutionContext& exec,
            const ibex::runtime::ParallelPipelineStats& stats) {
    std::uint64_t tasks = 0;
    for (const auto& row : exec.execution_profile->snapshot())
        tasks += row.pool_tasks;
    std::printf("%zu,%zu,%zu,%s,%zu,%s,%.6f,%.6f,%llu,%llu\n", rows, groups, cores,
                operation.c_str(), repeat, candidate ? "target" : "default",
                std::chrono::duration<double, std::milli>(end - start).count(),
                1000.0 * static_cast<double>(cpu_end - cpu) / CLOCKS_PER_SEC,
                static_cast<unsigned long long>(tasks),
                static_cast<unsigned long long>(stats.window_halo_pieces.load()));
}
}  // namespace

int main(int argc, char** argv) {
    using namespace ibex;
    using namespace ibex::runtime;
    CLI::App app{"Prepared-input paired sweeps for the remaining I6 grains"};
    std::vector<std::size_t> sizes{20000, 262144}, cardinalities{2, 64}, cores{1, 8};
    std::size_t repeats = 12;
    std::string operation = "filter";
    OperatorGrainPolicy target;
    app.add_option("--rows", sizes)->delimiter(',')->check(CLI::PositiveNumber);
    app.add_option("--groups", cardinalities)->delimiter(',')->check(CLI::PositiveNumber);
    app.add_option("--cores", cores)->delimiter(',')->check(CLI::PositiveNumber);
    app.add_option("--repeats", repeats)->check(CLI::PositiveNumber);
    app.add_option("--operation", operation)
        ->check(CLI::IsMember({"filter", "update", "window", "decimal", "map"}));
    app.add_option("--filter-rows", target.filter_bounds_rows)->check(CLI::PositiveNumber);
    app.add_option("--bucket-rows", target.update_bucket_rows)->check(CLI::PositiveNumber);
    app.add_option("--window-rows", target.window_split_rows)->check(CLI::PositiveNumber);
    app.add_option("--decimal-rows", target.decimal_partial_rows)->check(CLI::PositiveNumber);
    app.add_option("--map-min-rows", target.map_min_rows)->check(CLI::PositiveNumber);
    app.add_option("--map-max-rows", target.map_max_rows)->check(CLI::PositiveNumber);
    app.add_option("--map-morsels", target.map_morsels_per_worker)->check(CLI::PositiveNumber);
    CLI11_PARSE(app, argc, argv);
    if (!target.valid()) {
        std::fprintf(stderr, "Invalid grain policy\n");
        return 1;
    }
    if (repeats % 2)
        ++repeats;
    const std::string query =
        operation == "filter"    ? "input[filter v >= 10];"
        : operation == "update"  ? "input[update { z = cumsum(v) }, by a];"
        : operation == "window"  ? "input[window 40ns, by a, update { z = rolling_sum(v) }];"
        : operation == "decimal" ? "input[select { s = sum(d), m = mean(d) }, by a];"
                                 : "input[update { z = v * 2 + 1 }];";
    try {
        auto parsed = parser::parse(query);
        if (!parsed)
            throw std::runtime_error(parsed.error().format());
        auto tree = parser::lower(*parsed);
        if (!tree)
            throw std::runtime_error(tree.error().message);
        std::vector<ir::Expr> predicates;
        if (operation == "filter")
            predicates.push_back(ir::node_cast<ir::FilterNode>(**tree).take_predicate());
        std::puts("rows,groups,cores,operation,repeat,side,ms,cpu_ms,tasks,halo_pieces");
        for (const auto rows : sizes)
            for (const auto requested : cardinalities) {
                const auto groups = std::min(rows, requested);
                Column<std::int64_t> a, v;
                Column<Timestamp> ts;
                auto d = make_decimal_column(DecimalType{.precision = 10, .scale = 2});
                std::vector<Int128> sums(groups, 0);
                std::vector<std::int64_t> counts(groups, 0), cumulative(rows, 0);
                for (std::size_t row = 0; row < rows; ++row) {
                    const auto value = static_cast<std::int64_t>(row % 101);
                    a.push_back(static_cast<std::int64_t>(row % groups));
                    v.push_back(value);
                    ts.push_back(Timestamp{static_cast<std::int64_t>(row)});
                    d.push_back(Decimal{value});
                    sums[row % groups] += value;
                    ++counts[row % groups];
                    cumulative[row] = static_cast<std::int64_t>(sums[row % groups]);
                }
                Table input;
                input.add_column("a", std::move(a));
                input.add_column("v", std::move(v));
                if (operation == "window") {
                    input.add_column("ts", std::move(ts));
                    input.set_properties(TableProperties::time_frame("ts"));
                }
                if (operation == "decimal")
                    input.add_column("d", std::move(d));
                const TableRegistry registry{{"input", input}};
                ExecutionContext serial;
                serial.parallel_threads = 1;
                std::optional<Table> reference;
                std::vector<std::size_t> wanted;
                if (operation == "filter") {
                    for (std::size_t row = 0; row < rows; ++row)
                        if (row % 101 >= 10)
                            wanted.push_back(row);
                } else {
                    auto result = interpret(**tree, registry, nullptr, nullptr, nullptr, serial);
                    if (!result)
                        throw std::runtime_error(result.error());
                    reference = std::move(*result);
                    if (operation == "decimal") {
                        if (reference->rows() != groups)
                            throw std::runtime_error("Incorrect decimal cardinality");
                        const auto& actual = std::get<Column<Decimal>>(*reference->find("s"));
                        const auto& mean = std::get<Column<double>>(*reference->find("m"));
                        for (std::size_t g = 0; g < groups; ++g) {
                            if (actual[g].units != sums[g])
                                throw std::runtime_error("Incorrect decimal reference sum");
                            const double want = static_cast<double>(sums[g]) / 100.0 /
                                                static_cast<double>(counts[g]);
                            if (std::abs(mean[g] - want) > 1e-12)
                                throw std::runtime_error("Incorrect decimal reference mean");
                        }
                    } else {
                        if (reference->rows() != rows)
                            throw std::runtime_error("Input rows lost or multiplied");
                        if (operation == "update" || operation == "map") {
                            const auto& z = std::get<Column<std::int64_t>>(*reference->find("z"));
                            for (std::size_t row = 0; row < rows; ++row)
                                if (z[row] != (operation == "update" ? cumulative[row]
                                                                     : static_cast<std::int64_t>(
                                                                           (row % 101) * 2 + 1)))
                                    throw std::runtime_error("Incorrect update reference");
                        }
                    }
                }
                for (const auto budget : cores) {
                    const auto run = [&](bool candidate, std::size_t repeat, bool emit) {
                        ExecutionContext exec;
                        exec.parallel_threads = budget;
                        exec.parallel_min_rows = 0;
                        exec.parallel_min_cells = 0;
                        exec.operator_grain_policy = candidate ? target : OperatorGrainPolicy{};
                        exec.execution_profile =
                            std::make_shared<ExecutionProfileState>(budget, false);
                        ParallelPipelineStats stats;
                        exec.parallel_stats = &stats;
                        const auto cpu = std::clock();
                        const auto start = std::chrono::steady_clock::now();
                        if (operation == "filter") {
                            auto* entry = exec.execution_profile->stage("bounds");
                            std::expected<Selection, std::string> selected;
                            {
                                const ExecutionProfileScope scope(entry, ProfilePhase::Next);
                                selected = filter_selection(input, predicates, exec, nullptr);
                            }
                            const auto end = std::chrono::steady_clock::now();
                            const auto cpu_end = std::clock();
                            if (!selected)
                                throw std::runtime_error(selected.error());
                            if (std::vector<std::size_t>(selected->begin(), selected->end()) !=
                                wanted)
                                throw std::runtime_error("Incorrect filter indices");
                            if (emit)
                                report(rows, groups, budget, operation, repeat, candidate, start,
                                       end, cpu, cpu_end, exec, stats);
                        } else {
                            auto result =
                                interpret(**tree, registry, nullptr, nullptr, nullptr, exec);
                            const auto end = std::chrono::steady_clock::now();
                            const auto cpu_end = std::clock();
                            if (!result)
                                throw std::runtime_error(result.error());
                            if (auto mismatch = compare_tables(*reference, *result))
                                throw std::runtime_error(mismatch->message());
                            if (emit)
                                report(rows, groups, budget, operation, repeat, candidate, start,
                                       end, cpu, cpu_end, exec, stats);
                        }
                    };
                    run(false, 0, false);
                    run(true, 0, false);
                    for (std::size_t r = 0; r < repeats; ++r) {
                        run(r % 2 != 0, r, true);
                        run(r % 2 == 0, r, true);
                    }
                }
            }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 1;
    }
}
