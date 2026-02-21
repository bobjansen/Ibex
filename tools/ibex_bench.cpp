#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/csv.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include <chrono>
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

    auto total_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start)
                        .count();
    auto avg_ms = total_ms / static_cast<double>(iters);

    fmt::print("bench {}: iters={}, total_ms={:.3f}, avg_ms={:.3f}, rows={}\n", query.name, iters,
               total_ms, avg_ms, last_rows);

    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    CLI::App app{"Ibex benchmark harness"};

    std::string csv_path = "prices.csv";
    std::size_t warmup_iters = 1;
    std::size_t iters = 5;
    bool include_parse = true;

    app.add_option("--csv", csv_path, "CSV file path")->check(CLI::ExistingFile);
    app.add_option("--warmup", warmup_iters, "Warmup iterations")->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations")->check(CLI::PositiveNumber);
    app.add_flag("--include-parse", include_parse,
                 "Include parse + lower in timing (default: enabled)");
    app.add_flag("--no-include-parse", include_parse,
                 "Exclude parse + lower from timing (legacy mode)")
        ->excludes("--include-parse");

    CLI11_PARSE(app, argc, argv);

    auto table = ibex::runtime::read_csv_simple(csv_path);
    if (!table) {
        fmt::print("error: failed to read CSV: {}\n", table.error());
        return 1;
    }

    ibex::runtime::TableRegistry tables;
    tables.emplace("prices", std::move(*table));

    std::vector<BenchQuery> queries = {
        {
            "mean_by_symbol",
            "prices[select {avg_price = mean(price)}, by symbol]",
        },
        {
            "ohlc_by_symbol",
            "prices[select {open = first(price), high = max(price), low = min(price), last = last(price)}, by symbol]",
        },
    };

    int status = 0;
    for (const auto& query : queries) {
        status = run_benchmark(query, tables, warmup_iters, iters, include_parse);
        if (status != 0) {
            break;
        }
    }

    return status;
}
