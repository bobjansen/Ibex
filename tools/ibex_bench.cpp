#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include <chrono>
#include <csv.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace {

auto trim(std::string_view input) -> std::string_view {
    auto start = input.find_first_not_of(" \t\n\r");
    if (start == std::string_view::npos) {
        return {};
    }
    auto end = input.find_last_not_of(" \t\n\r");
    return input.substr(start, end - start + 1);
}

auto normalize_input(std::string_view input) -> std::string {
    auto normalized = std::string(trim(input));
    auto last_non_space = normalized.find_last_not_of(" \t\n\r");
    if (last_non_space != std::string::npos && normalized[last_non_space] != ';') {
        normalized.push_back(';');
    }
    return normalized;
}

struct BenchQuery {
    std::string name;
    std::string source;
};

auto run_benchmark(const BenchQuery& query, const ibex::runtime::TableRegistry& tables,
                   std::size_t warmup_iters, std::size_t iters, bool include_parse) -> int {
    auto normalized = normalize_input(query.source);
    ibex::runtime::ScalarRegistry scalars;

    if (!include_parse) {
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
        auto start = std::chrono::steady_clock::now();
        for (std::size_t i = 0; i < iters; ++i) {
            auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
            if (!result) {
                fmt::print("error: interpret failed for {}: {}\n", query.name, result.error());
                return 1;
            }
            last_rows = result->rows();
        }
        auto end = std::chrono::steady_clock::now();

        auto total_ms =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start)
                .count();
        auto avg_ms = total_ms / static_cast<double>(iters);

        fmt::print("bench {}: iters={}, total_ms={:.3f}, avg_ms={:.3f}, rows={}\n", query.name,
                   iters, total_ms, avg_ms, last_rows);

        return 0;
    }

    auto run_once = [&](std::size_t& last_rows) -> int {
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
        auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars);
        if (!result) {
            fmt::print("error: interpret failed for {}: {}\n", query.name, result.error());
            return 1;
        }
        last_rows = result->rows();
        return 0;
    };

    std::size_t warmup_rows = 0;
    for (std::size_t i = 0; i < warmup_iters; ++i) {
        if (run_once(warmup_rows) != 0) {
            return 1;
        }
    }

    std::size_t last_rows = 0;
    auto start = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < iters; ++i) {
        if (run_once(last_rows) != 0) {
            return 1;
        }
    }
    auto end = std::chrono::steady_clock::now();

    auto total_ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
    auto avg_ms = total_ms / static_cast<double>(iters);

    fmt::print("bench {}: iters={}, total_ms={:.3f}, avg_ms={:.3f}, rows={}\n", query.name, iters,
               total_ms, avg_ms, last_rows);

    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    CLI::App app{"Ibex benchmark harness"};

    std::string csv_path = "prices.csv";
    std::string csv_multi_path;
    std::size_t warmup_iters = 1;
    std::size_t iters = 5;
    bool include_parse = true;

    app.add_option("--csv", csv_path, "CSV file path (symbol, price)")->check(CLI::ExistingFile);
    app.add_option("--csv-multi", csv_multi_path,
                   "CSV file path for multi-column group-by benchmarks (symbol, price, day)")
        ->check(CLI::ExistingFile);
    app.add_option("--warmup", warmup_iters, "Warmup iterations")->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations")->check(CLI::PositiveNumber);
    app.add_flag("--include-parse", include_parse,
                 "Include parse + lower in timing (default: enabled)");
    app.add_flag("--no-include-parse", include_parse,
                 "Exclude parse + lower from timing (legacy mode)")
        ->excludes("--include-parse");

    CLI11_PARSE(app, argc, argv);

    ibex::runtime::Table table;
    try {
        table = read_csv(csv_path);
    } catch (const std::exception& e) {
        fmt::print("error: failed to read CSV: {}\n", e.what());
        return 1;
    }

    ibex::runtime::TableRegistry tables;
    tables.emplace("prices", std::move(table));

    // Single-column group-by: exercises the string fast path (robin_hood).
    std::vector<BenchQuery> queries = {
        {
            "mean_by_symbol",
            "prices[select {avg_price = mean(price)}, by symbol]",
        },
        {
            "ohlc_by_symbol",
            "prices[select {open = first(price), high = max(price), low = min(price), last = "
            "last(price)}, by symbol]",
        },
        {
            "update_price_x2",
            "prices[update {price_x2 = price * 2}]",
        },
        // Parse + lower overhead: same queries timed with parsing included.
        // Run with --include-parse (default) to capture lexer/parser maps.
        {
            "parse_mean_by_symbol",
            "prices[select {avg_price = mean(price)}, by symbol]",
        },
        {
            "parse_ohlc_by_symbol",
            "prices[select {open = first(price), high = max(price), low = min(price), last = "
            "last(price)}, by symbol]",
        },
        {
            "parse_update_price_x2",
            "prices[update {price_x2 = price * 2}]",
        },
    };

    int status = 0;
    // The first two queries benchmark pure execution (use --no-include-parse for isolation).
    // The last two use default include_parse to measure parsing cost.
    bool saved_include_parse = include_parse;
    for (std::size_t qi = 0; qi < queries.size(); ++qi) {
        // parse_* queries always include parsing in the timing
        bool this_include_parse = (qi >= 3) ? true : saved_include_parse;
        status = run_benchmark(queries[qi], tables, warmup_iters, iters, this_include_parse);
        if (status != 0) {
            break;
        }
    }

    // Multi-column group-by: exercises the compound-key fallback path (std::unordered_map<Key>).
    if (status == 0 && !csv_multi_path.empty()) {
        ibex::runtime::Table multi_table;
        try {
            multi_table = read_csv(csv_multi_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read multi CSV: {}\n", e.what());
            return 1;
        }
        ibex::runtime::TableRegistry multi_tables;
        multi_tables.emplace("prices_multi", std::move(multi_table));

        // ~1008 distinct (symbol, day) groups across 4M rows.
        std::vector<BenchQuery> multi_queries = {
            {
                "count_by_symbol_day",
                "prices_multi[select {n = count()}, by {symbol, day}]",
            },
            {
                "mean_by_symbol_day",
                "prices_multi[select {avg_price = mean(price)}, by {symbol, day}]",
            },
            {
                "ohlc_by_symbol_day",
                "prices_multi[select {open = first(price), high = max(price), low = min(price), "
                "last = last(price)}, by {symbol, day}]",
            },
        };

        for (const auto& query : multi_queries) {
            status = run_benchmark(query, multi_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }
    }

    return status;
}
