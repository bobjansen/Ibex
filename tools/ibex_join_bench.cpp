// Join-focused benchmark suite for Ibex.
//
// Generates synthetic tables in-memory (no CSV required) and exercises the
// join patterns that matter most for optimization:
//
//   1. hash_join_*        — single/multi-key equijoins at varying selectivities
//   2. int_join_*         — single-key Int64 equijoins and membership joins
//   3. join_chain_*       — multi-table join chains (A ⋈ B ⋈ C ⋈ D)
//   4. filter_pushdown_*  — filter-then-join vs join-then-filter
//   5. projection_waste_* — join with many unused columns
//   6. asof_join_*        — TimeFrame as-of joins (sorted binary search)
//   7. theta_join_*       — non-equijoin (nested loop) at various sizes
//   8. semi_anti_*        — membership-style joins
//
// Usage:
//   ibex_join_bench [--rows N] [--iters N] [--warmup N] [--suite name,...]
//                   [--verify] [--max-output-rows N]

#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

// jemalloc: unlike ibex_bench, some join workloads have quadratic fan-out and
// can briefly materialize very large outputs. Keep a short decay so warm
// iterations can still reuse pages, but do not retain them indefinitely across
// benchmark cases.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern "C" const char* malloc_conf =
    "background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000";

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace {

// ─── Statistics ──────────────────────────────────────────────────────────────

struct BenchStats {
    double total_ms;
    double avg_ms;
    double min_ms;
    double max_ms;
    double stddev_ms;
    double p95_ms;
    double p99_ms;
};

auto compute_stats(std::vector<double> times) -> BenchStats {
    double total = 0.0;
    for (double t : times)
        total += t;
    double avg = total / static_cast<double>(times.size());
    double mn = times[0];
    double mx = times[0];
    double sq_sum = 0.0;
    for (double t : times) {
        mn = std::min(mn, t);
        mx = std::max(mx, t);
        sq_sum += (t - avg) * (t - avg);
    }
    double stddev = times.size() > 1 ? std::sqrt(sq_sum / static_cast<double>(times.size())) : 0.0;
    std::sort(times.begin(), times.end());
    auto percentile = [&](double p) -> double {
        double idx = p * static_cast<double>(times.size() - 1);
        auto lo = static_cast<std::size_t>(idx);
        double frac = idx - static_cast<double>(lo);
        if (lo + 1 < times.size()) {
            return times[lo] * (1.0 - frac) + times[lo + 1] * frac;
        }
        return times[lo];
    };
    return {total, avg, mn, mx, stddev, percentile(0.95), percentile(0.99)};
}

// ─── Benchmark runner ────────────────────────────────────────────────────────

struct BenchQuery {
    std::string name;
    std::string source;
};

constexpr std::size_t kDefaultRows = 200'000;
constexpr std::size_t kDefaultMaxOutputRows = 20'000'000;

auto normalize_input(std::string_view input) -> std::string {
    auto start = input.find_first_not_of(" \t\n\r");
    if (start == std::string_view::npos) {
        return ";";
    }
    auto end = input.find_last_not_of(" \t\n\r");
    auto normalized = std::string(input.substr(start, end - start + 1));
    if (normalized.back() != ';') {
        normalized.push_back(';');
    }
    return normalized;
}

auto cap_rows_for_output_budget(std::size_t requested_rows, long double output_scale,
                                std::size_t max_output_rows) -> std::size_t {
    if (requested_rows == 0 || max_output_rows == 0) {
        return requested_rows;
    }
    const long double requested = static_cast<long double>(requested_rows);
    const long double expected_output = output_scale * requested * requested;
    if (expected_output <= static_cast<long double>(max_output_rows)) {
        return requested_rows;
    }

    const long double max_rows =
        std::sqrt(static_cast<long double>(max_output_rows) / output_scale);
    return std::max<std::size_t>(1, static_cast<std::size_t>(std::floor(max_rows)));
}

void print_rows_cap(std::string_view bench_name, std::size_t requested_rows,
                    std::size_t capped_rows, std::size_t max_output_rows) {
    if (requested_rows == capped_rows) {
        return;
    }
    fmt::print("  note: {} capped rows from {} to {} to keep expected output <= {} rows\n",
               bench_name, requested_rows, capped_rows, max_output_rows);
}

auto run_benchmark(const BenchQuery& query, const ibex::runtime::TableRegistry& tables,
                   std::size_t warmup_iters, std::size_t iters) -> int {
    auto normalized = normalize_input(query.source);
    ibex::runtime::ScalarRegistry scalars;

    auto parsed = ibex::parser::parse(normalized);
    if (!parsed) {
        fmt::print("error: parse failed for {}: {}\n", query.name, parsed.error().format());
        return 1;
    }
    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered) {
        fmt::print("error: lower failed for {}: {}\n", query.name, lowered.error().message);
        return 1;
    }

    for (std::size_t i = 0; i < warmup_iters; ++i) {
        auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
        if (!result) {
            fmt::print("error: interpret failed for {}: {}\n", query.name, result.error());
            return 1;
        }
    }

    std::size_t last_rows = 0;
    std::vector<double> times(iters);
    for (std::size_t i = 0; i < iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
        auto t1 = std::chrono::steady_clock::now();
        if (!result) {
            fmt::print("error: interpret failed for {}: {}\n", query.name, result.error());
            return 1;
        }
        last_rows = result->rows();
        times[i] =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();
    }
    auto s = compute_stats(std::move(times));
    fmt::print(
        "bench {}: iters={}, total_ms={:.3f}, avg_ms={:.3f}, min_ms={:.3f}, "
        "max_ms={:.3f}, stddev_ms={:.3f}, p95_ms={:.3f}, p99_ms={:.3f}, rows={}\n",
        query.name, iters, s.total_ms, s.avg_ms, s.min_ms, s.max_ms, s.stddev_ms, s.p95_ms,
        s.p99_ms, last_rows);
    return 0;
}

auto run_suite_benchmarks(const std::vector<BenchQuery>& queries,
                          const ibex::runtime::TableRegistry& tables, std::size_t warmup_iters,
                          std::size_t iters, bool verify) -> int {
    for (const auto& query : queries) {
        if (verify) {
            auto normalized = normalize_input(query.source);
            ibex::runtime::ScalarRegistry scalars;
            auto parsed = ibex::parser::parse(normalized);
            if (!parsed) {
                fmt::print("error: verify parse failed for {}: {}\n", query.name,
                           parsed.error().format());
                return 1;
            }
            auto lowered = ibex::parser::lower(*parsed);
            if (!lowered) {
                fmt::print("error: verify lower failed for {}: {}\n", query.name,
                           lowered.error().message);
                return 1;
            }
            auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
            if (!result) {
                fmt::print("error: verify interpret failed for {}: {}\n", query.name,
                           result.error());
                return 1;
            }
            fmt::print("  verify {}: {} rows OK\n", query.name, result->rows());
        }
        int status = run_benchmark(query, tables, warmup_iters, iters);
        if (status != 0) {
            return status;
        }
    }
    return 0;
}

// ─── Data generators ─────────────────────────────────────────────────────────

// Build a table with N rows:
//   id:     sequential 0..N-1
//   key:    "K" + (id % n_distinct)  — controls join selectivity
//   symbol: "SYM" + (id % 252)      — realistic stock universe
//   value:  100.0 + (id % 1000)     — payload column
auto make_fact_table(std::size_t rows, std::size_t n_distinct_keys, const std::string& key_prefix)
    -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::int64_t> id_col;
    ibex::Column<std::string> key_col;
    ibex::Column<std::string> symbol_col;
    ibex::Column<double> value_col;

    id_col.reserve(rows);
    key_col.reserve(rows);
    symbol_col.reserve(rows);
    value_col.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        id_col.push_back(static_cast<std::int64_t>(i));
        key_col.push_back(fmt::format("{}{:06d}", key_prefix, i % n_distinct_keys));
        symbol_col.push_back(fmt::format("SYM{:03d}", i % 252));
        value_col.push_back(100.0 + static_cast<double>(i % 1000));
    }

    t.add_column("id", std::move(id_col));
    t.add_column("key", std::move(key_col));
    t.add_column("symbol", std::move(symbol_col));
    t.add_column("value", std::move(value_col));
    return t;
}

// Build a narrow dimension/lookup table with n_keys rows:
//   key:    "K" + 0..n_keys-1
//   label:  "label_" + key
//   weight: 1.0 + (i % 100)
auto make_dim_table(std::size_t n_keys, const std::string& key_prefix) -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::string> key_col;
    ibex::Column<std::string> label_col;
    ibex::Column<double> weight_col;

    key_col.reserve(n_keys);
    label_col.reserve(n_keys);
    weight_col.reserve(n_keys);

    for (std::size_t i = 0; i < n_keys; ++i) {
        auto k = fmt::format("{}{:06d}", key_prefix, i);
        key_col.push_back(k);
        label_col.push_back("label_" + k);
        weight_col.push_back(1.0 + static_cast<double>(i % 100));
    }

    t.add_column("key", std::move(key_col));
    t.add_column("label", std::move(label_col));
    t.add_column("weight", std::move(weight_col));
    return t;
}

// Build an Int64-key fact table with N rows:
//   id:    i % n_distinct_ids        — join key
//   seq:   sequential 0..N-1         — stable left payload
//   value: 100.0 + (i % 1000)        — numeric payload
auto make_int_fact_table(std::size_t rows, std::size_t n_distinct_ids) -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::int64_t> id_col;
    ibex::Column<std::int64_t> seq_col;
    ibex::Column<double> value_col;

    id_col.reserve(rows);
    seq_col.reserve(rows);
    value_col.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        id_col.push_back(static_cast<std::int64_t>(i % n_distinct_ids));
        seq_col.push_back(static_cast<std::int64_t>(i));
        value_col.push_back(100.0 + static_cast<double>(i % 1000));
    }

    t.add_column("id", std::move(id_col));
    t.add_column("seq", std::move(seq_col));
    t.add_column("value", std::move(value_col));
    return t;
}

// Build an Int64-key dimension table with n_ids rows:
//   id:     sequential 0..n_ids-1
//   weight: 1.0 + (i % 100)
//   bucket: i % 17
auto make_int_dim_table_with_offset(std::size_t first_id, std::size_t n_ids)
    -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::int64_t> id_col;
    ibex::Column<double> weight_col;
    ibex::Column<std::int64_t> bucket_col;

    id_col.reserve(n_ids);
    weight_col.reserve(n_ids);
    bucket_col.reserve(n_ids);

    for (std::size_t i = 0; i < n_ids; ++i) {
        id_col.push_back(static_cast<std::int64_t>(first_id + i));
        weight_col.push_back(1.0 + static_cast<double>(i % 100));
        bucket_col.push_back(static_cast<std::int64_t>(i % 17));
    }

    t.add_column("id", std::move(id_col));
    t.add_column("weight", std::move(weight_col));
    t.add_column("bucket", std::move(bucket_col));
    return t;
}

auto make_int_dim_table(std::size_t n_ids) -> ibex::runtime::Table {
    return make_int_dim_table_with_offset(0, n_ids);
}

// Build a wide table with many payload columns (for projection pushdown benchmarks).
// Columns: id, key, col_0 .. col_{n_cols-1}
auto make_wide_table(std::size_t rows, std::size_t n_distinct_keys, std::size_t n_extra_cols,
                     const std::string& key_prefix) -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::int64_t> id_col;
    ibex::Column<std::string> key_col;
    id_col.reserve(rows);
    key_col.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        id_col.push_back(static_cast<std::int64_t>(i));
        key_col.push_back(fmt::format("{}{:06d}", key_prefix, i % n_distinct_keys));
    }
    t.add_column("id", std::move(id_col));
    t.add_column("key", std::move(key_col));

    for (std::size_t c = 0; c < n_extra_cols; ++c) {
        ibex::Column<double> col;
        col.reserve(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            col.push_back(static_cast<double>((i * (c + 1)) % 9973));
        }
        t.add_column(fmt::format("col_{}", c), std::move(col));
    }
    return t;
}

// Build a TimeFrame table for as-of join benchmarks.
//   ts:     Timestamp{i * step_nanos}
//   symbol: "SYM" + (i % n_symbols)
//   price:  100.0 + (i % 1000)
auto make_timeframe_table(std::size_t rows, std::size_t n_symbols, std::int64_t step_nanos)
    -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<ibex::Timestamp> ts_col;
    ibex::Column<std::string> symbol_col;
    ibex::Column<double> price_col;
    ts_col.reserve(rows);
    symbol_col.reserve(rows);
    price_col.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        ts_col.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * step_nanos});
        symbol_col.push_back(fmt::format("SYM{:03d}", i % n_symbols));
        price_col.push_back(100.0 + static_cast<double>(i % 1000));
    }

    t.add_column("ts", std::move(ts_col));
    t.add_column("symbol", std::move(symbol_col));
    t.add_column("price", std::move(price_col));
    t.time_index = "ts";
    return t;
}

// Build a table for multi-key join benchmarks.
//   key_a:  "A" + (i % n_a)
//   key_b:  "B" + (i % n_b)
//   value:  double payload
//
// Note: because both keys are derived from the same row index, the number of
// distinct composite keys is lcm(n_a, n_b), not n_a * n_b.
auto make_multikey_table(std::size_t rows, std::size_t n_a, std::size_t n_b)
    -> ibex::runtime::Table {
    ibex::runtime::Table t;
    ibex::Column<std::string> ka;
    ibex::Column<std::string> kb;
    ibex::Column<double> val;
    ka.reserve(rows);
    kb.reserve(rows);
    val.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        ka.push_back(fmt::format("A{:04d}", i % n_a));
        kb.push_back(fmt::format("B{:04d}", i % n_b));
        val.push_back(static_cast<double>(i % 997));
    }

    t.add_column("key_a", std::move(ka));
    t.add_column("key_b", std::move(kb));
    t.add_column("value", std::move(val));
    return t;
}

auto normalize_suite_name(std::string name) -> std::string {
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char ch) {
        if (ch == '-') {
            return '_';
        }
        return static_cast<char>(std::tolower(ch));
    });
    return name;
}

}  // namespace

int main(int argc, char** argv) {
    CLI::App app{"Ibex join benchmark suite"};

    std::size_t rows = kDefaultRows;
    std::size_t warmup_iters = 2;
    std::size_t iters = 5;
    std::size_t max_output_rows = kDefaultMaxOutputRows;
    bool verify = false;
    std::vector<std::string> suites;

    app.add_option("--rows", rows, "Base row count for fact tables (default: 200000)")
        ->check(CLI::PositiveNumber);
    app.add_option("--warmup", warmup_iters, "Warmup iterations (default: 2)")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations (default: 5)")
        ->check(CLI::PositiveNumber);
    app.add_option("--max-output-rows", max_output_rows,
                   "Cap high-fanout scenarios to this many expected output rows "
                   "(default: 20000000, 0 disables capping)")
        ->check(CLI::NonNegativeNumber);
    app.add_flag("--verify", verify, "Verify benchmark outputs before timing");
    app.add_option("--suite", suites,
                   "Suite(s) to run (comma-separated). "
                   "Supported: all, hash_join, int_join, join_chain, filter_pushdown, "
                   "projection_waste, asof_join, theta_join, semi_anti")
        ->delimiter(',');

    CLI11_PARSE(app, argc, argv);

    const std::unordered_set<std::string> allowed = {
        "all",       "hash_join",  "int_join", "join_chain", "filter_pushdown", "projection_waste",
        "asof_join", "theta_join", "semi_anti"};
    std::unordered_set<std::string> selected;
    for (const auto& s : suites) {
        auto n = normalize_suite_name(s);
        if (!allowed.contains(n)) {
            fmt::print("error: unknown suite '{}'\n", s);
            return 1;
        }
        selected.insert(std::move(n));
    }
    if (selected.empty()) {
        selected.insert("all");
    }
    auto want = [&](std::string_view suite) -> bool {
        return selected.contains("all") || selected.contains(std::string(suite));
    };

    int status = 0;

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 1: hash_join — equijoins at varying selectivities / key counts
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("hash_join")) {
        fmt::print("\n══ hash_join suite ({} fact rows) ══\n", rows);

        // 1a. High-selectivity join: fact N rows, dim 100 keys → ~N result rows
        // All fact rows match; exercises the "build small, probe large" path.
        {
            constexpr std::size_t kDimKeys = 100;
            auto fact = make_fact_table(rows, kDimKeys, "K");
            auto dim = make_dim_table(kDimKeys, "K");
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact", std::move(fact));
            reg.emplace("dim", std::move(dim));

            fmt::print("-- hash_join: fact({}) x dim({}) on key --\n", rows, kDimKeys);
            status = run_suite_benchmarks(
                {
                    {"hj_inner_high_sel", "fact join dim on key"},
                    {"hj_left_high_sel", "fact left join dim on key"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 1b. Low-selectivity join: only half the fact keys exist in dim.
        // Dim has 100 keys; fact uses 200 distinct keys → 50% match rate.
        if (status == 0) {
            constexpr std::size_t kFactKeys = 200;
            constexpr std::size_t kDimKeys = 100;
            auto fact = make_fact_table(rows, kFactKeys, "K");
            auto dim = make_dim_table(kDimKeys, "K");
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact", std::move(fact));
            reg.emplace("dim", std::move(dim));

            fmt::print("-- hash_join: fact({}, {} distinct) x dim({}) — 50%% match --\n", rows,
                       kFactKeys, kDimKeys);
            status = run_suite_benchmarks(
                {
                    {"hj_inner_low_sel", "fact join dim on key"},
                    {"hj_left_low_sel", "fact left join dim on key"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 1b2. Small-left / large-right inner join: exercises build-side
        // selection while keeping output linear in the left input size.
        if (status == 0) {
            const std::size_t small_left_rows = std::max<std::size_t>(1, rows / 4);
            auto fact_small = make_fact_table(small_left_rows, small_left_rows, "K");
            auto dim_large = make_dim_table(rows, "K");
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_small", std::move(fact_small));
            reg.emplace("dim_large", std::move(dim_large));

            fmt::print("-- hash_join: fact_small({}) x dim_large({}) on key --\n", small_left_rows,
                       rows);
            status = run_suite_benchmarks(
                {
                    {"hj_inner_small_left", "fact_small join dim_large on key"},
                    {"hj_left_small_left", "fact_small left join dim_large on key"},
                    {"hj_right_small_left", "fact_small right join dim_large on key"},
                    {"hj_outer_small_left", "fact_small outer join dim_large on key"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 1c. Multi-key equijoin: compound key {key_a, key_b}
        if (status == 0) {
            constexpr std::size_t kNA = 50;
            constexpr std::size_t kNB = 20;
            constexpr std::size_t kDistinctCompositeKeys = std::lcm(kNA, kNB);
            constexpr long double kOutputScale =
                1.0L / static_cast<long double>(10 * kDistinctCompositeKeys);
            const std::size_t multikey_rows =
                cap_rows_for_output_budget(rows, kOutputScale, max_output_rows);
            print_rows_cap("hj_multikey", rows, multikey_rows, max_output_rows);

            auto mk_left = make_multikey_table(multikey_rows, kNA, kNB);
            auto mk_right = make_multikey_table(multikey_rows / 10, kNA, kNB);
            ibex::runtime::TableRegistry reg;
            reg.emplace("mk_lhs", std::move(mk_left));
            reg.emplace("mk_rhs", std::move(mk_right));

            fmt::print("-- hash_join: multikey mk_lhs({}) x mk_rhs({}) on {{key_a, key_b}} --\n",
                       multikey_rows, multikey_rows / 10);
            status = run_suite_benchmarks({{"hj_multikey", "mk_lhs join mk_rhs on {key_a, key_b}"}},
                                          reg, warmup_iters, iters, verify);
        }

        // 1d. Large-on-large equijoin: both sides are big, many duplicates per key.
        // Both tables have N/2 rows, 500 distinct keys → ~N/1000 matches per key pair.
        if (status == 0) {
            constexpr std::size_t kKeys = 500;
            constexpr long double kOutputScale = 1.0L / static_cast<long double>(4 * kKeys);
            const std::size_t large_large_rows =
                cap_rows_for_output_budget(rows, kOutputScale, max_output_rows);
            print_rows_cap("hj_large_large", rows, large_large_rows, max_output_rows);

            const std::size_t half = large_large_rows / 2;
            auto big_lhs = make_fact_table(half, kKeys, "K");
            auto big_rhs_raw = make_fact_table(half, kKeys, "K");
            // Rename right columns to avoid collision.
            ibex::runtime::Table big_rhs;
            big_rhs.add_column("key", std::move(*big_rhs_raw.find("key")));
            big_rhs.add_column("r_id", std::move(*big_rhs_raw.find("id")));
            big_rhs.add_column("r_value", std::move(*big_rhs_raw.find("value")));

            ibex::runtime::TableRegistry reg;
            reg.emplace("big_lhs", std::move(big_lhs));
            reg.emplace("big_rhs", std::move(big_rhs));

            fmt::print(
                "-- hash_join: large-on-large big_lhs({}) x big_rhs({}) on key, {} distinct --\n",
                half, half, kKeys);
            status = run_suite_benchmarks({{"hj_large_large", "big_lhs join big_rhs on key"}}, reg,
                                          warmup_iters, iters, verify);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 2: int_join — single-key Int64 joins and membership joins
    // Tracks the dedicated Int64 fast path separately from string-key joins.
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("int_join")) {
        fmt::print("\n══ int_join suite ({} fact rows) ══\n", rows);

        // 2a. 100% match, exactly one right row per key → output rows ~= left rows.
        {
            constexpr std::size_t kIds = 200'000;
            const std::size_t dim_ids = std::min(rows, kIds);
            auto fact = make_int_fact_table(rows, dim_ids);
            auto dim = make_int_dim_table(dim_ids);
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_i64", std::move(fact));
            reg.emplace("dim_i64", std::move(dim));

            fmt::print("-- int_join: fact_i64({}) x dim_i64({}) on id --\n", rows, dim_ids);
            status = run_suite_benchmarks(
                {
                    {"i64_inner_high_sel", "fact_i64 join dim_i64 on id"},
                    {"i64_left_high_sel", "fact_i64 left join dim_i64 on id"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 2b. 50% match rate with unique right keys.  Good for lookup-heavy
        // inner/left/semi/anti cases without high fan-out materialization.
        if (status == 0) {
            const std::size_t dim_ids = std::max<std::size_t>(1, rows / 2);
            auto fact = make_int_fact_table(rows, rows);
            auto dim = make_int_dim_table(dim_ids);
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_i64", std::move(fact));
            reg.emplace("dim_i64", std::move(dim));

            fmt::print("-- int_join: fact_i64({}, {} distinct) x dim_i64({}) — 50%% match --\n",
                       rows, rows, dim_ids);
            status = run_suite_benchmarks(
                {
                    {"i64_inner_low_sel", "fact_i64 join dim_i64 on id"},
                    {"i64_left_low_sel", "fact_i64 left join dim_i64 on id"},
                    {"i64_semi_low_sel", "fact_i64 semi join dim_i64 on id"},
                    {"i64_anti_low_sel", "fact_i64 anti join dim_i64 on id"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 2b2. Small-left / large-right inner join: directly exercises the
        // Int64 build-left path while preserving left-row output order.
        if (status == 0) {
            const std::size_t small_left_rows = std::max<std::size_t>(1, rows / 4);
            auto fact_small = make_int_fact_table(small_left_rows, small_left_rows);
            auto dim_large = make_int_dim_table(rows);
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_small_i64", std::move(fact_small));
            reg.emplace("dim_large_i64", std::move(dim_large));

            fmt::print("-- int_join: fact_small_i64({}) x dim_large_i64({}) on id --\n",
                       small_left_rows, rows);
            status = run_suite_benchmarks(
                {
                    {"i64_inner_small_left", "fact_small_i64 join dim_large_i64 on id"},
                    {"i64_left_small_left", "fact_small_i64 left join dim_large_i64 on id"},
                    {"i64_semi_small_left", "fact_small_i64 semi join dim_large_i64 on id"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 2b3. Small-left anti join with a disjoint large right side: exercises
        // the build-left membership path when no left ids match.
        if (status == 0) {
            const std::size_t small_left_rows = std::max<std::size_t>(1, rows / 4);
            auto fact_small = make_int_fact_table(small_left_rows, small_left_rows);
            auto dim_large_miss = make_int_dim_table_with_offset(rows, rows);
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_small_i64", std::move(fact_small));
            reg.emplace("dim_large_miss_i64", std::move(dim_large_miss));

            fmt::print("-- int_join: fact_small_i64({}) anti dim_large_miss_i64({}) on id --\n",
                       small_left_rows, rows);
            status = run_suite_benchmarks(
                {
                    {"i64_anti_small_left", "fact_small_i64 anti join dim_large_miss_i64 on id"},
                },
                reg, warmup_iters, iters, verify);
        }

        // 2c. Right/outer-preserving case: all left keys match, but the right
        // side has extra unmatched ids that must be emitted.
        if (status == 0) {
            constexpr std::size_t kFactIds = 200'000;
            constexpr std::size_t kDimIds = 300'000;
            auto fact = make_int_fact_table(rows, std::min(rows, kFactIds));
            auto dim = make_int_dim_table(kDimIds);
            ibex::runtime::TableRegistry reg;
            reg.emplace("fact_i64", std::move(fact));
            reg.emplace("dim_i64", std::move(dim));

            fmt::print("-- int_join: fact_i64({}) x dim_i64({}) on id with extra right ids --\n",
                       rows, kDimIds);
            status = run_suite_benchmarks(
                {
                    {"i64_right_extra_rhs", "fact_i64 right join dim_i64 on id"},
                    {"i64_outer_extra_rhs", "fact_i64 outer join dim_i64 on id"},
                },
                reg, warmup_iters, iters, verify);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 3: join_chain — multi-table join sequences (A ⋈ B ⋈ C ⋈ D)
    // Measures the impact of join ordering and intermediate materialization.
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("join_chain")) {
        fmt::print("\n══ join_chain suite ({} fact rows) ══\n", rows);

        // Scenario: fact(N) ⋈ dim_a(100) ⋈ dim_b(50) ⋈ dim_c(25)
        // All use the same "key" column with different cardinalities.
        // Optimal order: fact ⋈ dim_c (smallest) first.
        constexpr std::size_t kDimA = 100;
        constexpr std::size_t kDimB = 50;
        constexpr std::size_t kDimC = 25;

        // fact uses 100 distinct keys so all dims match.
        auto fact = make_fact_table(rows, kDimA, "K");

        // Dimension tables — each has different extra columns.
        ibex::runtime::Table dim_a;
        {
            auto raw = make_dim_table(kDimA, "K");
            dim_a.add_column("key", std::move(*raw.find("key")));
            dim_a.add_column("label_a", std::move(*raw.find("label")));
            dim_a.add_column("weight_a", std::move(*raw.find("weight")));
        }

        ibex::runtime::Table dim_b;
        {
            auto raw = make_dim_table(kDimB, "K");
            dim_b.add_column("key", std::move(*raw.find("key")));
            dim_b.add_column("label_b", std::move(*raw.find("label")));
            dim_b.add_column("weight_b", std::move(*raw.find("weight")));
        }

        ibex::runtime::Table dim_c;
        {
            auto raw = make_dim_table(kDimC, "K");
            dim_c.add_column("key", std::move(*raw.find("key")));
            dim_c.add_column("label_c", std::move(*raw.find("label")));
            dim_c.add_column("weight_c", std::move(*raw.find("weight")));
        }

        ibex::runtime::TableRegistry reg;
        reg.emplace("fact", std::move(fact));
        reg.emplace("dim_a", std::move(dim_a));
        reg.emplace("dim_b", std::move(dim_b));
        reg.emplace("dim_c", std::move(dim_c));

        // 2-table chain
        status = run_suite_benchmarks({{"chain_2_fact_dima", "fact join dim_a on key"}}, reg,
                                      warmup_iters, iters, verify);

        // 3-table chain
        if (status == 0) {
            status = run_suite_benchmarks(
                {{"chain_3_fact_dima_dimb", "fact join dim_a on key join dim_b on key"}}, reg,
                warmup_iters, iters, verify);
        }

        // 4-table chain — this is where join ordering matters most
        if (status == 0) {
            status = run_suite_benchmarks(
                {{"chain_4_fact_all",
                  "fact join dim_a on key join dim_b on key join dim_c on key"}},
                reg, warmup_iters, iters, verify);
        }

        // Reversed order: start with smallest dim — if optimizer reorders,
        // this should be equivalent to the above.
        if (status == 0) {
            status = run_suite_benchmarks(
                {{"chain_4_reversed",
                  "fact join dim_c on key join dim_b on key join dim_a on key"}},
                reg, warmup_iters, iters, verify);
        }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 4: filter_pushdown — filter before vs after join
    // Measures the benefit of pushing filters below joins.
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("filter_pushdown")) {
        fmt::print("\n══ filter_pushdown suite ({} fact rows) ══\n", rows);

        constexpr std::size_t kDimKeys = 100;
        auto fact = make_fact_table(rows, kDimKeys, "K");
        auto dim = make_dim_table(kDimKeys, "K");
        ibex::runtime::TableRegistry reg;
        reg.emplace("fact", std::move(fact));
        reg.emplace("dim", std::move(dim));

        // Baseline: join then filter (what users write naturally)
        // "value > 500" filters ~50% of rows.
        status = run_suite_benchmarks(
            {
                // Join first, then filter — must process all N join matches.
                {"fp_join_then_filter", "let j = fact join dim on key; j[filter value > 500.0]"},
                // Filter first, then join — joins only ~N/2 rows.
                {"fp_filter_then_join", "fact[filter value > 500.0] join dim on key"},
                // Tight filter (value > 900 → ~10% pass), then join.
                {"fp_join_then_tight_filter",
                 "let j = fact join dim on key; j[filter value > 900.0]"},
                {"fp_tight_filter_then_join", "fact[filter value > 900.0] join dim on key"},
            },
            reg, warmup_iters, iters, verify);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 5: projection_waste — join with many unused columns
    // Measures the cost of materializing wide intermediate tables.
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("projection_waste")) {
        fmt::print("\n══ projection_waste suite ({} fact rows) ══\n", rows);

        constexpr std::size_t kDimKeys = 100;
        constexpr std::size_t kExtraCols = 20;

        auto wide_left = make_wide_table(rows, kDimKeys, kExtraCols, "K");
        auto wide_right = make_wide_table(kDimKeys, kDimKeys, kExtraCols, "K");

        // Rename right-side payload columns to avoid collisions.
        ibex::runtime::Table rhs;
        rhs.add_column("key", std::move(*wide_right.find("key")));
        rhs.add_column("r_id", std::move(*wide_right.find("id")));
        for (std::size_t c = 0; c < kExtraCols; ++c) {
            auto col_name = fmt::format("col_{}", c);
            auto r_col_name = fmt::format("r_col_{}", c);
            rhs.add_column(r_col_name, std::move(*wide_right.find(col_name)));
        }

        ibex::runtime::TableRegistry reg;
        reg.emplace("wide_left", std::move(wide_left));
        reg.emplace("wide_right", std::move(rhs));

        // Join two 22-column tables but only use 2 output columns.
        // Without projection pushdown, the join materializes all 42 columns.
        status = run_suite_benchmarks(
            {
                {"pw_join_all_cols", "wide_left join wide_right on key"},
                {"pw_join_select_2",
                 "let j = wide_left join wide_right on key; j[select {id, r_col_0}]"},
                // Pre-project baseline: what an optimizer could do.
                {"pw_preproject_join",
                 "wide_left[select {id, key}] join wide_right[select {key, r_col_0}] on key"},
            },
            reg, warmup_iters, iters, verify);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 6: asof_join — TimeFrame as-of joins
    // Measures binary search join on sorted timestamp columns.
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("asof_join")) {
        fmt::print("\n══ asof_join suite ({} rows) ══\n", rows);

        constexpr std::size_t kSymbols = 10;
        constexpr std::int64_t kSecondNanos = 1'000'000'000LL;

        // Left: dense ticks, 1 second apart.
        auto quotes = make_timeframe_table(rows, kSymbols, kSecondNanos);

        // Right: sparse reference data, every 60 seconds.
        // Roughly rows/60 entries — each left row does a binary search.
        auto ref = make_timeframe_table(rows / 60, kSymbols, kSecondNanos * 60);
        // Rename price column to avoid collision.
        ibex::runtime::Table ref_table;
        ref_table.add_column("ts", std::move(*ref.find("ts")));
        ref_table.add_column("symbol", std::move(*ref.find("symbol")));
        ref_table.add_column("ref_price", std::move(*ref.find("price")));
        ref_table.time_index = "ts";

        ibex::runtime::TableRegistry reg;
        reg.emplace("quotes", std::move(quotes));
        reg.emplace("ref", std::move(ref_table));

        status = run_suite_benchmarks(
            {
                // Full as-of join with group key (ts + symbol).
                {"asof_grouped", "quotes asof join ref on {ts, symbol}"},
                // As-of join on ts only (no group key — tests pure binary search path).
                {"asof_ts_only", "quotes asof join ref on ts"},
            },
            reg, warmup_iters, iters, verify);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 7: theta_join — non-equijoin (nested loop)
    // Intentionally uses small tables since theta joins are O(N×M).
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("theta_join")) {
        // Scale down: theta join is O(N*M), so keep it manageable.
        const std::size_t theta_left = std::min(rows, std::size_t{5000});
        const std::size_t theta_right = std::min(rows, std::size_t{1000});

        fmt::print("\n══ theta_join suite (left={}, right={}) ══\n", theta_left, theta_right);

        ibex::runtime::Table lhs;
        {
            ibex::Column<std::int64_t> val;
            val.reserve(theta_left);
            for (std::size_t i = 0; i < theta_left; ++i) {
                val.push_back(static_cast<std::int64_t>(i));
            }
            lhs.add_column("a", std::move(val));
        }

        ibex::runtime::Table rhs;
        {
            ibex::Column<std::int64_t> lo, hi;
            lo.reserve(theta_right);
            hi.reserve(theta_right);
            // Non-overlapping ranges: [i*5, i*5+3) — each left row matches at most 1 range.
            for (std::size_t i = 0; i < theta_right; ++i) {
                lo.push_back(static_cast<std::int64_t>(i * 5));
                hi.push_back(static_cast<std::int64_t>(i * 5 + 3));
            }
            rhs.add_column("lo", std::move(lo));
            rhs.add_column("hi", std::move(hi));
        }

        ibex::runtime::TableRegistry reg;
        reg.emplace("lhs", std::move(lhs));
        reg.emplace("rhs", std::move(rhs));

        status = run_suite_benchmarks(
            {
                // Range predicate: lo <= a && a < hi
                {"theta_range", "lhs join rhs on lo <= a && a < hi"},
                // Inequality: a < lo  (many matches for small a values)
                {"theta_lt", "lhs join rhs on a < lo"},
                // Left join with range: preserves unmatched left rows.
                {"theta_range_left", "lhs left join rhs on lo <= a && a < hi"},
            },
            reg, warmup_iters, iters, verify);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Suite 8: semi_anti — membership-style joins
    // ═══════════════════════════════════════════════════════════════════════
    if (status == 0 && want("semi_anti")) {
        fmt::print("\n══ semi_anti suite ({} fact rows) ══\n", rows);

        // Fact with 1000 distinct keys; dim with only 500 → 50% match.
        constexpr std::size_t kFactKeys = 1000;
        constexpr std::size_t kDimKeys = 500;
        auto fact = make_fact_table(rows, kFactKeys, "K");
        auto dim = make_dim_table(kDimKeys, "K");
        ibex::runtime::TableRegistry reg;
        reg.emplace("fact", std::move(fact));
        reg.emplace("dim", std::move(dim));

        status = run_suite_benchmarks(
            {
                {"sa_semi", "fact semi join dim on key"},
                {"sa_anti", "fact anti join dim on key"},
                // Compare with inner join (which also copies right-side columns).
                {"sa_inner_baseline", "fact join dim on key"},
            },
            reg, warmup_iters, iters, verify);

        if (status == 0) {
            const std::size_t small_left_rows = std::max<std::size_t>(1, rows / 4);
            auto fact_small = make_fact_table(small_left_rows, small_left_rows, "K");
            auto dim_large = make_dim_table(rows, "K");
            auto dim_large_miss = make_dim_table(rows, "Z");
            ibex::runtime::TableRegistry small_reg;
            small_reg.emplace("fact_small", std::move(fact_small));
            small_reg.emplace("dim_large", std::move(dim_large));
            small_reg.emplace("dim_large_miss", std::move(dim_large_miss));

            fmt::print("-- semi_anti: fact_small({}) vs dim_large({}) on key --\n", small_left_rows,
                       rows);
            status = run_suite_benchmarks(
                {
                    {"sa_semi_small_left", "fact_small semi join dim_large on key"},
                    {"sa_anti_small_left", "fact_small anti join dim_large_miss on key"},
                },
                small_reg, warmup_iters, iters, verify);
        }
    }

    return status;
}
