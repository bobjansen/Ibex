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

    // A table pre-sorted ascending on `k`, so `order k asc` exercises the
    // ChunkedOrderOperator's validated-passthrough path rather than the sort.
    {
        ibex::runtime::Table t;
        ibex::Column<std::int64_t> k;
        k.resize(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            k.data()[i] = static_cast<std::int64_t>(i);
        }
        t.add_column("k", std::move(k));
        ibex::Column<std::int64_t> v;
        v.resize(rows);
        std::mt19937_64 rng(43);
        for (std::size_t i = 0; i < rows; ++i) {
            v.data()[i] = static_cast<std::int64_t>(rng() % 1000);
        }
        t.add_column("v", std::move(v));
        tables["sorted"] = std::move(t);
    }

    // A table with a pre-sorted nanosecond-timestamp column `ts`, used by the
    // as_timeframe passthrough benchmark. Int column → promoted per-chunk.
    {
        ibex::runtime::Table t;
        ibex::Column<std::int64_t> ts;
        ts.resize(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            ts.data()[i] = static_cast<std::int64_t>(i) * 1'000'000;  // 1ms steps
        }
        t.add_column("ts", std::move(ts));
        ibex::Column<std::int64_t> v;
        v.resize(rows);
        std::mt19937_64 rng(44);
        for (std::size_t i = 0; i < rows; ++i) {
            v.data()[i] = static_cast<std::int64_t>(rng() % 1000);
        }
        t.add_column("v", std::move(v));
        tables["tf_sorted"] = std::move(t);
    }

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
        // Row-local update: `update` runs per chunk now. The wide_update case
        // also chains filter→update to exercise the chunked pipeline end-to-end.
        {"wide_update_arith", "wide[update { d = c0 * 2 + c1 }]"},
        {"wide_filter_update", "wide[filter c0 < 500, update { d = c1 + c2 }]"},
        // Global head(n): should stop pulling from the source once n rows are
        // emitted. On a 2M-row table these should be effectively constant-time.
        {"wide_head_10", "wide[head 10]"},
        {"wide_head_1000", "wide[head 1000]"},
        {"wide_filter_head_10", "wide[filter c0 < 500, head 10]"},
        // Tail pushdown through Filter: buffer only the last n matches as
        // chunks arrive, never hold the full filtered result in memory.
        {"wide_filter_tail_10", "wide[filter c0 < 500, tail 10]"},
        {"wide_filter_tail_1000", "wide[filter c0 < 500, tail 1000]"},
        // Head/Tail pushdown past Project: Head(Project(Filter(x))) rewrites to
        // Project(FilterHead(x)) so the row-limit reaches the fused FilterHead
        // and only n surviving rows flow through the projection.
        {"wide_filter_project_head_10", "wide[filter c0 < 500, select { c1, c2 }, head 10]"},
        {"wide_filter_project_tail_10", "wide[filter c0 < 500, select { c1, c2 }, tail 10]"},
        {"wide_filter_rename_head_10", "wide[filter c0 < 500, rename k = c1, head 10]"},
        // Filter → Update → Project: `select { cols, computed = expr }` lowers
        // to Project(Update(Filter(Scan))). The fused operator gathers only
        // the columns the update reads plus projected originals, skipping
        // the rest of the wide input.
        {"wide_filter_computed_select_keep2", "wide[filter c0 < 500, select { c1, d = c1 + c2 }]"},
        {"wide_filter_computed_select_keep4",
         "wide[filter c0 < 500, select { c1, c2, c3, d = c4 + c5 }]"},
        {"wide_filter_computed_select_keep_all_like",
         "wide[filter c0 < 500, "
         "select { c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, "
         "d = c0 + c1 }]"},
        // Order: chunked operator buffers + validates sortedness, falls back
        // to order_table on violation. The `sorted` table is already sorted
        // ascending on `k`, so the streaming path skips the sort entirely.
        {"wide_order_unsorted", "wide[order c0 asc]"},
        {"sorted_order_presorted", "sorted[order k asc]"},
        // Order-delay: Filter(Order(x)) → Order(Filter(x)). Without the
        // rewrite the sort runs on all 2M rows; with it, only surviving rows
        // (~998k at c1<500) pay the sort cost.
        {"order_then_filter", "wide[order c0 asc][filter c1 < 500]"},
        // Order-delay: Project(Order(x)) → Order(Project(x)). Without the
        // rewrite the sort carries all 16 columns; with it, only the 2
        // projected columns flow through the sort.
        {"order_then_project", "wide[order c0 asc][select { c0, c1 }]"},
        // Composite: Project(Filter(Order(x))) → Order(FilterProject(x)).
        {"order_then_filter_project", "wide[order c0 asc][filter c2 < 500, select { c0, c1 }]"},
        // Order-delay past Rename: Order(Rename(x)) → Rename(Order(x)) with
        // keys remapped. Exposes the sort to the source's pre-rename column
        // name, so the `sorted` passthrough path still fires after the push.
        {"sorted_rename_then_order", "sorted[rename key = k][order key asc]"},
        // as_timeframe on a pre-sorted nanosecond column streams chunks
        // through ChunkedAsTimeframeOperator without a sort; unsorted input
        // falls back to concat + order_table (spec §9.1).
        {"tf_sorted_as_timeframe", "as_timeframe(tf_sorted, \"ts\")"},
        {"wide_as_timeframe_unsorted", "as_timeframe(wide, \"c0\")"},
        // Order-after-Aggregate measurement: aggregate reduces 2M rows to K groups;
        // sorting K rows afterwards is cheap. Confirms pulling Order under Aggregate
        // has no payoff at current scales.
        {"agg_then_order_1k_groups",
         "wide[update { g = c0 % 1000 }][by g, select { g, s = sum(c1) }][order s desc]"},
        {"agg_then_order_100_groups",
         "wide[update { g = c0 % 100 }][by g, select { g, s = sum(c1) }][order s desc]"},
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
