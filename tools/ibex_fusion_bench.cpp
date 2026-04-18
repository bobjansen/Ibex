// Benchmark harness for pipeline fusion.
//
// Targets the Scan -> Filter -> Project fusion: when a Project follows a
// Filter, the fused operator gathers only the projected columns rather than
// materializing every column and then dropping most of them. This benchmark
// sweeps wide tables where `select` keeps a small fraction of columns, which
// is the shape that benefits most.

#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace {

struct Stats {
    double avg_ms;
    double min_ms;
    double max_ms;
};

auto stats(std::vector<double> ts) -> Stats {
    double sum = 0.0;
    for (double t : ts) {
        sum += t;
    }
    double mn = ts[0];
    double mx = ts[0];
    for (double t : ts) {
        mn = std::min(mn, t);
        mx = std::max(mx, t);
    }
    return {sum / static_cast<double>(ts.size()), mn, mx};
}

auto make_wide_table(std::size_t rows, std::size_t n_cols, std::uint64_t seed)
    -> ibex::runtime::Table {
    std::mt19937_64 rng(seed);
    ibex::runtime::Table t;
    for (std::size_t c = 0; c < n_cols; ++c) {
        ibex::Column<std::int64_t> col;
        col.resize(rows);
        auto* p = col.data();
        for (std::size_t i = 0; i < rows; ++i) {
            p[i] = static_cast<std::int64_t>(rng() % 1000);
        }
        t.add_column(fmt::format("c{}", c), std::move(col));
    }
    return t;
}

auto run(const std::string& name, const std::string& src,
         const ibex::runtime::TableRegistry& tables, std::size_t warmup, std::size_t iters) -> int {
    std::string normalized = src;
    if (normalized.empty() || normalized.back() != ';') {
        normalized.push_back(';');
    }
    auto parsed = ibex::parser::parse(normalized);
    if (!parsed) {
        fmt::print("parse failed: {}\n", parsed.error().format());
        return 1;
    }
    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered) {
        fmt::print("lower failed: {}\n", lowered.error().message);
        return 1;
    }
    ibex::runtime::ScalarRegistry scalars;
    for (std::size_t i = 0; i < warmup; ++i) {
        auto r = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
        if (!r) {
            fmt::print("interpret failed: {}\n", r.error());
            return 1;
        }
    }
    std::vector<double> times(iters);
    std::size_t last_rows = 0;
    for (std::size_t i = 0; i < iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        auto r = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
        auto t1 = std::chrono::steady_clock::now();
        if (!r) {
            fmt::print("interpret failed: {}\n", r.error());
            return 1;
        }
        last_rows = r->rows();
        times[i] =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();
    }
    auto s = stats(std::move(times));
    fmt::print("bench {}: iters={}, avg_ms={:.3f}, min_ms={:.3f}, max_ms={:.3f}, rows={}\n", name,
               iters, s.avg_ms, s.min_ms, s.max_ms, last_rows);
    return 0;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    CLI::App app{"Ibex Scan -> Filter -> Project fusion benchmarks"};
    std::size_t rows = 2'000'000;
    std::size_t warmup = 2;
    std::size_t iters = 5;
    app.add_option("--rows", rows, "Rows per table");
    app.add_option("--warmup", warmup, "Warmup iterations");
    app.add_option("--iters", iters, "Measured iterations");
    CLI11_PARSE(app, argc, argv)

    ibex::runtime::TableRegistry tables;
    tables["narrow"] = make_wide_table(rows, 4, 42);
    tables["wide"] = make_wide_table(rows, 16, 42);

    struct Q {
        std::string name;
        std::string src;
    };
    const std::vector<Q> queries = {
        {"narrow_keep2_of_4", "narrow[filter c0 < 500, select { c1, c2 }]"},
        {"wide_keep2_of_16", "wide[filter c0 < 500, select { c1, c2 }]"},
        {"wide_keep2_of_16_lowsel", "wide[filter c0 < 100, select { c1, c2 }]"},
        {"wide_keep8_of_16", "wide[filter c0 < 500, select { c1, c2, c3, c4, c5, c6, c7, c8 }]"},
        {"wide_keep_all_16",
         "wide[filter c0 < 500, select { c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, "
         "c13, c14, c15 }]"},
    };

    int status = 0;
    for (const auto& q : queries) {
        status = run(q.name, q.src, tables, warmup, iters);
        if (status != 0) {
            return status;
        }
    }
    return 0;
}
