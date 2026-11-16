// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/core/column.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <CLI/CLI.hpp>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <memory>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "physical_plan.hpp"
#include "runtime_internal.hpp"

int main(int argc, char** argv) {
    using namespace ibex;
    using namespace ibex::runtime;
    enable_huge_pages();
    CLI::App app{"I7 prepared-data rows-by-width fan-out sweep"};
    std::vector<std::size_t> sizes{65536, 131072, 262144}, widths{2, 6, 16};
    std::size_t repeats = 12, cores = 8, keep_every = 1, cell_floor = 0;
    std::string mode = "emit";
    app.add_option("--rows", sizes)->delimiter(',')->check(CLI::PositiveNumber);
    app.add_option("--widths", widths)->delimiter(',')->check(CLI::PositiveNumber);
    app.add_option("--repeats", repeats)->check(CLI::PositiveNumber);
    app.add_option("--cores", cores)->check(CLI::PositiveNumber);
    app.add_option("--keep-every", keep_every)->check(CLI::PositiveNumber);
    app.add_option("--mode", mode)->check(CLI::IsMember({"emit", "gather", "filter", "project"}));
    app.add_option("--cell-floor", cell_floor);
    CLI11_PARSE(app, argc, argv);
    if (repeats % 2)
        ++repeats;
    try {
        std::puts(
            "rows,width,output_rows,cores,mode,repeat,side,ms,total_ms,cpu_ms,pool_ms,tasks,cell_"
            "floor,pipelines");
        for (const auto rows : sizes)
            for (const auto width : widths) {
                if ((mode == "emit" || (mode == "filter" || mode == "project")) && width < 2)
                    throw std::runtime_error("Emission needs a key and an aggregate");
                Table input;
                for (std::size_t c = 0; c < width; ++c) {
                    Column<std::int64_t> col;
                    for (std::size_t r = 0; r < rows; ++r)
                        col.push_back(c == 0 ? static_cast<std::int64_t>(r * 0x9e3779b97f4a7c15ULL)
                                             : static_cast<std::int64_t>(r % 101 + c));
                    input.add_column(c == 0 ? "k" : "v" + std::to_string(c), std::move(col));
                }
                const TableRegistry registry{{"input", input}};
                // Enter through interpret once so the allocator policy matches a
                // normal query before comparing direct physical/kernel calls.
                auto ready_source = parser::parse("input;");
                if (!ready_source)
                    throw std::runtime_error(ready_source.error().format());
                auto ready_tree = parser::lower(*ready_source);
                if (!ready_tree)
                    throw std::runtime_error(ready_tree.error().message);
                ExecutionContext ready_exec;
                ready_exec.parallel_threads = 1;
                auto ready =
                    interpret(**ready_tree, registry, nullptr, nullptr, nullptr, ready_exec);
                if (!ready)
                    throw std::runtime_error(ready.error());
                std::vector<std::size_t> idx;
                if ((mode == "filter" || mode == "project")) {
                    for (std::size_t r = 0; r < rows; ++r)
                        if (r % 101 + 1 >= 50)
                            idx.push_back(r);
                } else {
                    for (std::size_t r = 0; r < rows; r += keep_every)
                        idx.push_back(rows - 1 - r);
                }
                std::vector<ColumnGatherJob> jobs;
                for (const auto& c : input.columns)
                    jobs.push_back({.column = c.column.get(), .idx = idx.data()});
                const std::size_t output_rows = mode == "emit" ? rows : idx.size();
                ir::NodePtr tree;
                physical::Plan plan;
                if (mode == "emit" || (mode == "filter" || mode == "project")) {
                    std::string query = "input[select { ";
                    for (std::size_t c = 1; c < width; ++c) {
                        if (c > 1)
                            query += ", ";
                        query += "s" + std::to_string(c) + " = sum(v" + std::to_string(c) + ")";
                    }
                    query += " }, by k];";
                    if ((mode == "filter" || mode == "project"))
                        query = mode == "project" ? "input[filter v1 >= 50][select { k, v1 }];"
                                                  : "input[filter v1 >= 50];";
                    auto parsed = parser::parse(query);
                    if (!parsed)
                        throw std::runtime_error(parsed.error().format());
                    auto lowered = parser::lower(*parsed);
                    if (!lowered)
                        throw std::runtime_error(lowered.error().message);
                    tree = std::move(*lowered);
                    plan = physical::plan_physical(*tree, registry, nullptr);
                    if (mode == "emit" && !plan.hash_aggregate)
                        throw std::runtime_error("No aggregate phase chain");
                }
                const std::size_t output_width = mode == "project" ? 2 : width;
                const auto verify = [&](const Table& output) {
                    if (output.rows() != output_rows || output.columns.size() != output_width)
                        throw std::runtime_error("Incorrect output dimensions");
                    for (std::size_t c = 0; c < output_width; ++c) {
                        const auto& col = std::get<Column<std::int64_t>>(*output.columns[c].column);
                        for (std::size_t r = 0; r < output_rows; ++r) {
                            const auto source = mode == "emit" ? r : idx[r];
                            const auto want =
                                c == 0 ? static_cast<std::int64_t>(source * 0x9e3779b97f4a7c15ULL)
                                       : static_cast<std::int64_t>(source % 101 + c);
                            if (col[r] != want)
                                throw std::runtime_error("Incorrect output value/order");
                        }
                    }
                };
                const auto run = [&](bool parallel, std::size_t repeat, bool emit) {
                    ExecutionContext exec;
                    exec.parallel_threads = mode == "emit" ? cores : parallel ? cores : 1;
                    exec.parallel_min_rows = 0;
                    exec.parallel_min_cells = cell_floor;
                    ParallelPipelineStats stats;
                    exec.parallel_stats = &stats;
                    exec.execution_profile = std::make_shared<ExecutionProfileState>(cores, false);
                    auto configured = plan;
                    if (mode == "emit") {
                        configured.hash_aggregate->emission.parallelism.row_floor = 1;
                        configured.hash_aggregate->emission.parallelism.breaker_max_workers =
                            parallel ? cores : 1;
                    }
                    const auto start = std::chrono::steady_clock::now();
                    const auto cpu = std::clock();
                    Table output;
                    std::optional<ExecutionProfileScope> root_scope;
                    if ((mode == "filter" || mode == "project"))
                        root_scope.emplace(exec.execution_profile->stage("filter"),
                                           ProfilePhase::Next);
                    if (mode == "emit" || (mode == "filter" || mode == "project")) {
                        auto op = build_operator_from_physical_plan(
                            configured, *tree, registry, nullptr, nullptr, exec, nullptr);
                        if (!op)
                            throw std::runtime_error(op.error());
                        auto result = materialize_operator(std::move(*op));
                        if (!result)
                            throw std::runtime_error(result.error());
                        output = std::move(*result);
                    } else {
                        auto* entry = exec.execution_profile->stage("gather");
                        const ExecutionProfileScope scope(entry, ProfilePhase::Next);
                        auto columns = gather_columns_batched(
                            jobs, idx.size(), &exec, [&](std::size_t c) -> GatheredColumn {
                                return {
                                    gather_column(*jobs[c].column, idx.data(), idx.size(), nullptr),
                                    std::nullopt};
                            });
                        for (std::size_t c = 0; c < width; ++c)
                            output.add_column(input.columns[c].name, std::move(columns[c].first));
                    }
                    root_scope.reset();
                    const auto cpu_end = std::clock();
                    const auto end = std::chrono::steady_clock::now();
                    verify(output);
                    std::uint64_t stage_ns = 0, pool_ns = 0, tasks = 0;
                    bool found = (mode == "filter" || mode == "project");
                    for (const auto& row : exec.execution_profile->snapshot()) {
                        if (mode != "filter" && mode != "project" &&
                            row.label != (mode == "emit" ? "Aggregate.Emission" : "gather"))
                            continue;
                        found = true;
                        stage_ns += row.next_self_ns;
                        pool_ns += row.pool_work_ns;
                        tasks += row.pool_tasks;
                    }
                    if (!found)
                        throw std::runtime_error("Measured stage missing");
                    const double total_ms =
                        std::chrono::duration<double, std::milli>(end - start).count();
                    if (emit)
                        std::printf(
                            "%zu,%zu,%zu,%zu,%s,%zu,%s,%.6f,%.6f,%.6f,%.6f,%llu,%zu,%llu\n", rows,
                            width, output_rows, cores, mode.c_str(), repeat,
                            parallel ? "parallel" : "serial",
                            (mode == "filter" || mode == "project")
                                ? total_ms
                                : static_cast<double>(stage_ns) / 1e6,
                            total_ms, 1000.0 * static_cast<double>(cpu_end - cpu) / CLOCKS_PER_SEC,
                            static_cast<double>(pool_ns) / 1e6,
                            static_cast<unsigned long long>(tasks), cell_floor,
                            static_cast<unsigned long long>(stats.parallel_pipelines.load()));
                };
                run(false, 0, false);
                run(true, 0, false);
                for (std::size_t r = 0; r < repeats; ++r) {
                    run(r % 2 != 0, r, true);
                    run(r % 2 == 0, r, true);
                }
            }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return 1;
    }
}
