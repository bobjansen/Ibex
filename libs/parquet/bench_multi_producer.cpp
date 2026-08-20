// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// THROWAWAY benchmark — not shipped, not linked from anything else.
//
// De-risks the "multiple producers" idea in
// plans/parallelism-overview.md ("First pass at the multiple-producers
// question (2026-08-20)") before touching chunked.cpp: q10's `orders` scan
// only starts decoding once build_operator's depth-first recursion reaches
// its join node, well after the `lineitem` scan ahead of it in the chain has
// started. Measured occupancy said lineitem's filter runs at 65% pool
// occupancy while orders' runs at only 18.5%, independently, in its own
// separate time window — real slack, on paper. This harness decodes the two
// tables' real columns via LazyTable::project(), the actual Parquet decode
// path, outside any query plan or chunked.cpp, sequentially vs. deliberately
// concurrently (a raw std::thread, matching PipelinedStageOperator's own
// pattern, not a pool submission), to check whether that slack actually
// converts to wall-clock savings before writing any production code.
//
// Usage: bench_multi_producer [reps] [sf_dir]
//   sf_dir defaults to parquet_sf2 (matches the recent SF-2 core-scaling run
//   this investigation followed up on).

#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "parquet.hpp"

namespace {

using Clock = std::chrono::steady_clock;

auto now_ms() -> double {
    return std::chrono::duration<double, std::milli>(Clock::now().time_since_epoch()).count();
}

auto decode_lineitem(const std::string& dir, const ibex::runtime::ExecutionContext& exec)
    -> double {
    const auto t0 = now_ms();
    // read_parquet_lazy et al. live at global scope, not ibex::parquet — see
    // backend.cpp, which includes this same header before opening its own
    // ibex::parquet namespace and calls these unqualified.
    auto lazy = read_parquet_lazy(dir + "/lineitem.parquet");
    auto result =
        lazy->project({"l_orderkey", "l_extendedprice", "l_discount", "l_returnflag"}, exec);
    if (!result) {
        std::fprintf(stderr, "lineitem decode failed: %s\n", result.error().c_str());
        std::exit(1);
    }
    return now_ms() - t0;
}

auto decode_orders(const std::string& dir, const ibex::runtime::ExecutionContext& exec) -> double {
    const auto t0 = now_ms();
    auto lazy = read_parquet_lazy(dir + "/orders.parquet");
    auto result = lazy->project({"o_orderkey", "o_custkey", "o_orderdate"}, exec);
    if (!result) {
        std::fprintf(stderr, "orders decode failed: %s\n", result.error().c_str());
        std::exit(1);
    }
    return now_ms() - t0;
}

auto median(std::vector<double> v) -> double {
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

}  // namespace

int main(int argc, char** argv) {
    const int reps = argc > 1 ? std::atoi(argv[1]) : 15;
    const std::string dir = argc > 2 ? argv[2] : "benchmarking/data/tpch/parquet_sf2";

    ibex::runtime::ExecutionContext exec;
    ibex::runtime::configure_parallel_from_env(exec);

    std::vector<double> sequential;
    std::vector<double> concurrent;

    // Warm the filesystem cache identically for both variants before timing:
    // one untimed pass of each decode.
    decode_lineitem(dir, exec);
    decode_orders(dir, exec);

    // Interleaved A/B, not two serial blocks — WSL2 drifts across a long
    // serial run (see project_bench_interleaved_methodology).
    for (int i = 0; i < reps; ++i) {
        {
            const auto t0 = now_ms();
            decode_lineitem(dir, exec);
            decode_orders(dir, exec);
            sequential.push_back(now_ms() - t0);
        }
        {
            const auto t0 = now_ms();
            std::thread producer([&] { decode_orders(dir, exec); });
            decode_lineitem(dir, exec);
            producer.join();
            concurrent.push_back(now_ms() - t0);
        }
    }

    const double seq_med = median(sequential);
    const double con_med = median(concurrent);
    std::printf("dir=%s reps=%d parallel_threads=%zu\n", dir.c_str(), reps, exec.parallel_threads);
    std::printf("sequential median: %.2f ms\n", seq_med);
    std::printf("concurrent median: %.2f ms\n", con_med);
    std::printf("delta: %.1f%% (%s)\n", 100.0 * (seq_med - con_med) / seq_med,
                con_med < seq_med ? "concurrent faster" : "concurrent slower");
    return 0;
}
