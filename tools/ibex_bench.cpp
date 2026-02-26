#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <csv.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
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

auto column_type_name(const ibex::runtime::ColumnValue& column) -> std::string_view {
    if (std::holds_alternative<ibex::Column<std::int64_t>>(column)) {
        return "Int";
    }
    if (std::holds_alternative<ibex::Column<double>>(column)) {
        return "Double";
    }
    if (std::holds_alternative<ibex::Column<std::string>>(column)) {
        return "String";
    }
    if (std::holds_alternative<ibex::Column<ibex::Categorical>>(column)) {
        return "Categorical";
    }
    return "Unknown";
}

void print_table_types(const std::string_view name, const ibex::runtime::Table& table) {
    fmt::print("table {}: rows={}\n", name, table.rows());
    for (const auto& entry : table.columns) {
        fmt::print("  {}: {}\n", entry.name, column_type_name(*entry.column));
    }
}

auto string_view_at(const ibex::runtime::ColumnValue& column, std::size_t row) -> std::string_view {
    if (const auto* str_col = std::get_if<ibex::Column<std::string>>(&column)) {
        return std::string_view((*str_col)[row]);
    }
    if (const auto* cat_col = std::get_if<ibex::Column<ibex::Categorical>>(&column)) {
        return (*cat_col)[row];
    }
    return {};
}

auto double_at(const ibex::runtime::ColumnValue& column, std::size_t row) -> double {
    if (const auto* dbl_col = std::get_if<ibex::Column<double>>(&column)) {
        return (*dbl_col)[row];
    }
    if (const auto* int_col = std::get_if<ibex::Column<std::int64_t>>(&column)) {
        return static_cast<double>((*int_col)[row]);
    }
    return 0.0;
}

auto int_at(const ibex::runtime::ColumnValue& column, std::size_t row) -> std::int64_t {
    if (const auto* int_col = std::get_if<ibex::Column<std::int64_t>>(&column)) {
        return (*int_col)[row];
    }
    if (const auto* dbl_col = std::get_if<ibex::Column<double>>(&column)) {
        return static_cast<std::int64_t>((*dbl_col)[row]);
    }
    return 0;
}

auto verify_mean_by_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                           std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_sym = result.find("symbol");
    const auto* out_avg = result.find("avg_price");
    if (sym_col == nullptr || price_col == nullptr || out_sym == nullptr || out_avg == nullptr) {
        return false;
    }
    std::unordered_map<std::string, std::size_t> index;
    std::vector<std::string> order;
    std::vector<double> sum;
    std::vector<std::int64_t> count;
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        auto it = index.find(key);
        std::size_t gid = 0;
        if (it == index.end()) {
            gid = order.size();
            index.emplace(key, gid);
            order.push_back(key);
            sum.push_back(0.0);
            count.push_back(0);
        } else {
            gid = it->second;
        }
        sum[gid] += double_at(*price_col, row);
        count[gid] += 1;
    }
    if (result.rows() != order.size()) {
        return false;
    }
    for (std::size_t i = 0; i < order.size(); ++i) {
        if (string_view_at(*out_sym, i) != order[i]) {
            return false;
        }
        double expected = count[i] == 0 ? 0.0 : sum[i] / static_cast<double>(count[i]);
        double actual = double_at(*out_avg, i);
        if (std::abs(actual - expected) > 1e-9) {
            return false;
        }
    }
    return true;
}

auto verify_ohlc_by_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                           std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_sym = result.find("symbol");
    const auto* out_open = result.find("open");
    const auto* out_high = result.find("high");
    const auto* out_low = result.find("low");
    const auto* out_last = result.find("last");
    if (sym_col == nullptr || price_col == nullptr || out_sym == nullptr || out_open == nullptr ||
        out_high == nullptr || out_low == nullptr || out_last == nullptr) {
        return false;
    }
    std::unordered_map<std::string, std::size_t> index;
    std::vector<std::string> order;
    std::vector<double> open;
    std::vector<double> high;
    std::vector<double> low;
    std::vector<double> last;
    std::vector<bool> seen;
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        auto it = index.find(key);
        std::size_t gid = 0;
        if (it == index.end()) {
            gid = order.size();
            index.emplace(key, gid);
            order.push_back(key);
            open.push_back(0.0);
            high.push_back(-std::numeric_limits<double>::infinity());
            low.push_back(std::numeric_limits<double>::infinity());
            last.push_back(0.0);
            seen.push_back(false);
        } else {
            gid = it->second;
        }
        double v = double_at(*price_col, row);
        if (!seen[gid]) {
            open[gid] = v;
            seen[gid] = true;
        }
        high[gid] = std::max(v, high[gid]);

        low[gid] = std::min(v, low[gid]);
        last[gid] = v;
    }
    if (result.rows() != order.size()) {
        return false;
    }
    for (std::size_t i = 0; i < order.size(); ++i) {
        if (string_view_at(*out_sym, i) != order[i]) {
            return false;
        }
        if (std::abs(double_at(*out_open, i) - open[i]) > 1e-9) {
            return false;
        }
        if (std::abs(double_at(*out_high, i) - high[i]) > 1e-9) {
            return false;
        }
        if (std::abs(double_at(*out_low, i) - low[i]) > 1e-9) {
            return false;
        }
        if (std::abs(double_at(*out_last, i) - last[i]) > 1e-9) {
            return false;
        }
    }
    return true;
}

struct Key2 {
    std::string a;
    std::string b;
    bool operator==(const Key2& other) const { return a == other.a && b == other.b; }
};

struct Key2Hash {
    std::size_t operator()(const Key2& k) const noexcept {
        return std::hash<std::string>()(k.a) ^ (std::hash<std::string>()(k.b) << 1);
    }
};

auto verify_group_by_symbol_day(const ibex::runtime::Table& table,
                                const ibex::runtime::Table& result, std::size_t rows,
                                const std::string_view mode) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* day_col = table.find("day");
    const auto* price_col = table.find("price");
    const auto* out_sym = result.find("symbol");
    const auto* out_day = result.find("day");
    if (sym_col == nullptr || day_col == nullptr || out_sym == nullptr || out_day == nullptr) {
        return false;
    }

    std::unordered_map<Key2, std::size_t, Key2Hash> index;
    std::vector<Key2> order;
    std::vector<std::int64_t> count;
    std::vector<double> sum;
    std::vector<double> open;
    std::vector<double> high;
    std::vector<double> low;
    std::vector<double> last;
    std::vector<bool> seen;

    for (std::size_t row = 0; row < rows; ++row) {
        Key2 key{.a = std::string(string_view_at(*sym_col, row)),
                 .b = std::string(string_view_at(*day_col, row))};
        auto it = index.find(key);
        std::size_t gid = 0;
        if (it == index.end()) {
            gid = order.size();
            index.emplace(key, gid);
            order.push_back(key);
            count.push_back(0);
            sum.push_back(0.0);
            open.push_back(0.0);
            high.push_back(-std::numeric_limits<double>::infinity());
            low.push_back(std::numeric_limits<double>::infinity());
            last.push_back(0.0);
            seen.push_back(false);
        } else {
            gid = it->second;
        }
        count[gid] += 1;
        if (price_col != nullptr) {
            double v = double_at(*price_col, row);
            sum[gid] += v;
            if (!seen[gid]) {
                open[gid] = v;
                seen[gid] = true;
            }
            high[gid] = std::max(v, high[gid]);
            low[gid] = std::min(v, low[gid]);
            last[gid] = v;
        }
    }

    if (result.rows() != order.size()) {
        return false;
    }

    for (std::size_t i = 0; i < order.size(); ++i) {
        if (string_view_at(*out_sym, i) != order[i].a) {
            return false;
        }
        if (string_view_at(*out_day, i) != order[i].b) {
            return false;
        }
        if (mode == "count") {
            const auto* out_n = result.find("n");
            if (out_n == nullptr) {
                return false;
            }
            if (int_at(*out_n, i) != count[i]) {
                return false;
            }
        } else if (mode == "mean") {
            const auto* out_avg = result.find("avg_price");
            if (out_avg == nullptr) {
                return false;
            }
            double expected = count[i] == 0 ? 0.0 : sum[i] / static_cast<double>(count[i]);
            if (std::abs(double_at(*out_avg, i) - expected) > 1e-9) {
                return false;
            }
        } else if (mode == "ohlc") {
            const auto* out_open = result.find("open");
            const auto* out_high = result.find("high");
            const auto* out_low = result.find("low");
            const auto* out_last = result.find("last");
            if (out_open == nullptr || out_high == nullptr || out_low == nullptr ||
                out_last == nullptr) {
                return false;
            }
            if (std::abs(double_at(*out_open, i) - open[i]) > 1e-9) {
                return false;
            }
            if (std::abs(double_at(*out_high, i) - high[i]) > 1e-9) {
                return false;
            }
            if (std::abs(double_at(*out_low, i) - low[i]) > 1e-9) {
                return false;
            }
            if (std::abs(double_at(*out_last, i) - last[i]) > 1e-9) {
                return false;
            }
        }
    }
    return true;
}

auto verify_update_price_x2(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                            std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* price_x2 = result.find("price_x2");
    if (price_col == nullptr || price_x2 == nullptr) {
        return false;
    }
    if (result.rows() != table.rows()) {
        return false;
    }
    for (std::size_t row = 0; row < rows; ++row) {
        double expected = double_at(*price_col, row) * 2.0;
        double actual = double_at(*price_x2, row);
        if (std::abs(actual - expected) > 1e-9) {
            return false;
        }
    }
    return true;
}

auto verify_filter(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                   std::size_t rows, const std::string_view mode) -> bool {
    const auto* price_col = table.find("price");
    const auto* qty_col = table.find("qty");
    if (price_col == nullptr || qty_col == nullptr) {
        return false;
    }
    std::size_t expected = 0;
    for (std::size_t row = 0; row < rows; ++row) {
        double price = double_at(*price_col, row);
        std::int64_t qty = int_at(*qty_col, row);
        bool keep = false;
        if (mode == "simple") {
            keep = price > 500.0;
        } else if (mode == "and") {
            keep = price > 500.0 && qty < 100;
        } else if (mode == "arith") {
            keep = price * static_cast<double>(qty) > 50000.0;
        } else if (mode == "or") {
            keep = price > 900.0 || qty < 10;
        }
        if (keep) {
            expected++;
        }
    }
    return result.rows() == expected;
}

auto slice_table(const ibex::runtime::Table& table, std::size_t rows) -> ibex::runtime::Table {
    ibex::runtime::Table out;
    std::size_t n = std::min(rows, table.rows());
    out.columns.reserve(table.columns.size());
    for (const auto& entry : table.columns) {
        const auto& column = *entry.column;
        ibex::runtime::ColumnValue sliced = std::visit(
            [&](const auto& col) -> ibex::runtime::ColumnValue {
                using ColType = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColType, ibex::Column<std::int64_t>>) {
                    std::vector<std::int64_t> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.push_back(col[i]);
                    }
                    return ibex::Column<std::int64_t>(std::move(data));
                } else if constexpr (std::is_same_v<ColType, ibex::Column<double>>) {
                    std::vector<double> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.push_back(col[i]);
                    }
                    return ibex::Column<double>(std::move(data));
                } else if constexpr (std::is_same_v<ColType, ibex::Column<std::string>>) {
                    std::vector<std::string> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.emplace_back(col[i]);
                    }
                    return ibex::Column<std::string>(std::move(data));
                } else if constexpr (std::is_same_v<ColType, ibex::Column<ibex::Date>>) {
                    std::vector<ibex::Date> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.push_back(col[i]);
                    }
                    return ibex::Column<ibex::Date>(std::move(data));
                } else if constexpr (std::is_same_v<ColType, ibex::Column<ibex::Timestamp>>) {
                    std::vector<ibex::Timestamp> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.push_back(col[i]);
                    }
                    return ibex::Column<ibex::Timestamp>(std::move(data));
                } else if constexpr (std::is_same_v<ColType, ibex::Column<ibex::Categorical>>) {
                    std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
                    codes.reserve(n);
                    const auto* src = col.codes_data();
                    for (std::size_t i = 0; i < n; ++i) {
                        codes.push_back(src[i]);
                    }
                    return ibex::Column<ibex::Categorical>(col.dictionary_ptr(), col.index_ptr(),
                                                           std::move(codes));
                } else {
                    static_assert(std::is_same_v<ColType, void>, "Unhandled column type");
                }
            },
            column);
        out.add_column(entry.name, std::move(sliced));
    }
    return out;
}

auto verify_benchmark(const BenchQuery& query, const ibex::runtime::TableRegistry& tables,
                      std::size_t max_rows) -> std::optional<std::string> {
    auto normalized = normalize_input(query.source);
    ibex::runtime::ScalarRegistry scalars;
    auto parsed = ibex::parser::parse(normalized);
    if (!parsed) {
        return "parse failed: " + parsed.error().format();
    }
    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered) {
        return "lower failed: " + lowered.error().message;
    }
    ibex::runtime::TableRegistry sliced;
    const ibex::runtime::Table* table = nullptr;
    if (tables.contains("prices")) {
        table = &tables.at("prices");
        sliced.emplace("prices", slice_table(*table, max_rows));
    }
    const ibex::runtime::Table* trades = nullptr;
    if (tables.contains("trades")) {
        trades = &tables.at("trades");
        sliced.emplace("trades", slice_table(*trades, max_rows));
    }
    const ibex::runtime::Table* multi = nullptr;
    if (tables.contains("prices_multi")) {
        multi = &tables.at("prices_multi");
        sliced.emplace("prices_multi", slice_table(*multi, max_rows));
    }

    auto result = ibex::runtime::interpret(*lowered.value(), sliced, &scalars);
    if (!result) {
        return "interpret failed: " + result.error();
    }
    if (query.name == "mean_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_mean_by_symbol(sliced.at("prices"), *result, rows)) {
            return "mean_by_symbol verification failed";
        }
    } else if (query.name == "ohlc_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_ohlc_by_symbol(sliced.at("prices"), *result, rows)) {
            return "ohlc_by_symbol verification failed";
        }
    } else if (query.name == "update_price_x2") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_update_price_x2(sliced.at("prices"), *result, rows)) {
            return "update_price_x2 verification failed";
        }
    } else if (query.name == "count_by_symbol_day") {
        if (multi == nullptr) {
            return "missing prices_multi table";
        }
        std::size_t rows = sliced.at("prices_multi").rows();
        if (!verify_group_by_symbol_day(sliced.at("prices_multi"), *result, rows, "count")) {
            return "count_by_symbol_day verification failed";
        }
    } else if (query.name == "mean_by_symbol_day") {
        if (multi == nullptr) {
            return "missing prices_multi table";
        }
        std::size_t rows = sliced.at("prices_multi").rows();
        if (!verify_group_by_symbol_day(sliced.at("prices_multi"), *result, rows, "mean")) {
            return "mean_by_symbol_day verification failed";
        }
    } else if (query.name == "ohlc_by_symbol_day") {
        if (multi == nullptr) {
            return "missing prices_multi table";
        }
        std::size_t rows = sliced.at("prices_multi").rows();
        if (!verify_group_by_symbol_day(sliced.at("prices_multi"), *result, rows, "ohlc")) {
            return "ohlc_by_symbol_day verification failed";
        }
    } else if (query.name == "filter_simple") {
        if (trades == nullptr) {
            return "missing trades table";
        }
        std::size_t rows = sliced.at("trades").rows();
        if (!verify_filter(sliced.at("trades"), *result, rows, "simple")) {
            return "filter_simple verification failed";
        }
    } else if (query.name == "filter_and") {
        if (trades == nullptr) {
            return "missing trades table";
        }
        std::size_t rows = sliced.at("trades").rows();
        if (!verify_filter(sliced.at("trades"), *result, rows, "and")) {
            return "filter_and verification failed";
        }
    } else if (query.name == "filter_arith") {
        if (trades == nullptr) {
            return "missing trades table";
        }
        std::size_t rows = sliced.at("trades").rows();
        if (!verify_filter(sliced.at("trades"), *result, rows, "arith")) {
            return "filter_arith verification failed";
        }
    } else if (query.name == "filter_or") {
        if (trades == nullptr) {
            return "missing trades table";
        }
        std::size_t rows = sliced.at("trades").rows();
        if (!verify_filter(sliced.at("trades"), *result, rows, "or")) {
            return "filter_or verification failed";
        }
    }
    return std::nullopt;
}

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

    std::string csv_path;
    std::string csv_multi_path;
    std::string csv_trades_path;
    std::string csv_events_path;
    std::string csv_lookup_path;
    std::size_t warmup_iters = 1;
    std::size_t iters = 5;
    bool include_parse = true;
    bool print_types = false;
    bool verify = false;
    std::size_t verify_rows = 10000;
    std::size_t timeframe_rows = 0;

    app.add_option("--csv", csv_path, "CSV file path (symbol, price)")->check(CLI::ExistingFile);
    app.add_option("--csv-multi", csv_multi_path,
                   "CSV file path for multi-column group-by benchmarks (symbol, price, day)")
        ->check(CLI::ExistingFile);
    app.add_option("--csv-trades", csv_trades_path,
                   "CSV file path for filter benchmarks (symbol, price, qty)")
        ->check(CLI::ExistingFile);
    app.add_option("--csv-events", csv_events_path,
                   "CSV file path for high-cardinality benchmarks (user_id, amount, quantity)")
        ->check(CLI::ExistingFile);
    app.add_option("--csv-lookup", csv_lookup_path,
                   "CSV file path for null benchmarks (symbol, sector) — half of prices symbols")
        ->check(CLI::ExistingFile);
    app.add_option("--warmup", warmup_iters, "Warmup iterations")->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations")->check(CLI::PositiveNumber);
    app.add_flag("--include-parse", include_parse,
                 "Include parse + lower in timing (default: enabled)");
    app.add_flag("--no-include-parse", include_parse,
                 "Exclude parse + lower from timing (legacy mode)")
        ->excludes("--include-parse");
    app.add_flag("--print-types", print_types, "Print column types for loaded benchmark tables");
    app.add_flag("--verify", verify, "Verify benchmark outputs on a sample of rows");
    app.add_option("--verify-rows", verify_rows,
                   "Rows to sample per table during verification (default: 10000)")
        ->check(CLI::PositiveNumber);
    app.add_option(
           "--timeframe-rows", timeframe_rows,
           "Row count for in-memory TimeFrame benchmarks (lag, rolling_*). 0 = skip (default).")
        ->check(CLI::NonNegativeNumber);

    CLI11_PARSE(app, argc, argv);

    int status = 0;
    bool saved_include_parse = include_parse;

    if (!csv_path.empty()) {
        ibex::runtime::Table table;
        try {
            table = read_csv(csv_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read CSV: {}\n", e.what());
            return 1;
        }

        ibex::runtime::TableRegistry tables;
        tables.emplace("prices", std::move(table));
        if (print_types) {
            print_table_types("prices", tables.find("prices")->second);
        }

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

        // The first three queries benchmark pure execution (use --no-include-parse for isolation).
        // The last three use default include_parse to measure parsing cost.
        for (std::size_t qi = 0; qi < queries.size(); ++qi) {
            // parse_* queries always include parsing in the timing
            bool this_include_parse = (qi >= 3) ? true : saved_include_parse;
            if (verify && queries[qi].name.rfind("parse_", 0) != 0) {
                if (auto err = verify_benchmark(queries[qi], tables, verify_rows)) {
                    fmt::print("error: verify failed for {}: {}\n", queries[qi].name, *err);
                    return 1;
                }
            }
            status = run_benchmark(queries[qi], tables, warmup_iters, iters, this_include_parse);
            if (status != 0) {
                break;
            }
        }

        // Null benchmarks: left join produces ~50% null right-column values.
        // lookup.csv has half the symbols (first 126 of 252); the other half
        // get null sector values. This exercises validity-bitmap tracking.
        if (status == 0 && !csv_lookup_path.empty()) {
            ibex::runtime::Table lookup_table;
            try {
                lookup_table = read_csv(csv_lookup_path);
            } catch (const std::exception& e) {
                fmt::print("error: failed to read lookup CSV: {}\n", e.what());
                return 1;
            }
            if (print_types) {
                print_table_types("lookup", lookup_table);
            }

            // Combine prices + lookup in one registry for the join query.
            ibex::runtime::TableRegistry null_reg;
            null_reg.emplace("prices", tables.at("prices"));  // shared_ptr columns — cheap copy
            null_reg.emplace("lookup", std::move(lookup_table));

            fmt::print("\n-- Null benchmarks ({} prices rows, {} lookup rows) --\n",
                       null_reg.at("prices").rows(), null_reg.at("lookup").rows());

            // Left join: ~50% of rows get null sector; measures validity-bitmap
            // tracking and allocation overhead vs polars/data.table.
            std::vector<BenchQuery> null_queries = {
                {"null_left_join", "prices left join lookup on symbol"},
            };

            for (const auto& q : null_queries) {
                status = run_benchmark(q, null_reg, warmup_iters, iters, saved_include_parse);
                if (status != 0) {
                    break;
                }
            }
        }
    }

    // Compound filter benchmarks: exercises the recursive FilterExpr evaluator.
    if (status == 0 && !csv_trades_path.empty()) {
        ibex::runtime::Table trades_table;
        try {
            trades_table = read_csv(csv_trades_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read trades CSV: {}\n", e.what());
            return 1;
        }
        ibex::runtime::TableRegistry trades_tables;
        trades_tables.emplace("trades", std::move(trades_table));
        if (print_types) {
            print_table_types("trades", trades_tables.find("trades")->second);
        }

        // price uniform [1, 1000], qty uniform [1, 500].
        // filter_simple:  ~50% rows (price > 500)
        // filter_and:     ~10% rows (price > 500 && qty < 100)
        // filter_arith:   ~50% rows (price * qty > 50000)
        // filter_or:      ~12% rows (price > 900 || qty < 10)
        std::vector<BenchQuery> filter_queries = {
            {"filter_simple", "trades[filter price > 500.0]"},
            {"filter_and", "trades[filter price > 500.0 && qty < 100]"},
            {"filter_arith", "trades[filter price * qty > 50000.0]"},
            {"filter_or", "trades[filter price > 900.0 || qty < 10]"},
        };

        for (const auto& query : filter_queries) {
            if (verify) {
                if (auto err = verify_benchmark(query, trades_tables, verify_rows)) {
                    fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                    return 1;
                }
            }
            status = run_benchmark(query, trades_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
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
        if (print_types) {
            print_table_types("prices_multi", multi_tables.find("prices_multi")->second);
        }

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
            if (verify) {
                if (auto err = verify_benchmark(query, multi_tables, verify_rows)) {
                    fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                    return 1;
                }
            }
            status = run_benchmark(query, multi_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }
    }

    // High-cardinality group-by + string-gather filter: exercises ibex's limits.
    // user_id has 100 000 distinct values (> 4096 categorical threshold) so the
    // column stays as Column<std::string>.  This stresses:
    //   sum_by_user   — hash-map pass over 100K string keys; flat accumulator too
    //                   large for L2, revealing the parallelism gap vs polars.
    //   filter_events — gather copies 2M SSO std::string objects; polars copies
    //                   Arrow offset pairs (8 bytes each, done in parallel).
    if (status == 0 && !csv_events_path.empty()) {
        ibex::runtime::Table events_table;
        try {
            events_table = read_csv(csv_events_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read events CSV: {}\n", e.what());
            return 1;
        }
        ibex::runtime::TableRegistry events_tables;
        events_tables.emplace("events", std::move(events_table));
        if (print_types) {
            print_table_types("events", events_tables.find("events")->second);
        }

        // amount uniform [1, 1000]: filter_events selects ~50% of rows.
        std::vector<BenchQuery> events_queries = {
            {"sum_by_user", "events[select {total = sum(amount)}, by user_id]"},
            {"filter_events", "events[filter amount > 500.0]"},
        };

        for (const auto& query : events_queries) {
            status = run_benchmark(query, events_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }
    }

    // TimeFrame benchmarks: synthetic in-memory data, no CSV required.
    // Pass --timeframe-rows N (e.g. 1000000) to enable.
    //
    // Data layout: N rows, 1-second spacing.
    //   ts:    Timestamp{i * 1_000_000_000}  (0 s, 1 s, 2 s, ...)
    //   price: 100.0 + (i % 100)             (sawtooth double)
    //
    // Window sizes and expected work per row:
    //   1m  (60 s)  → ~60 rows in window
    //   5m (300 s)  → ~300 rows in window
    if (status == 0 && timeframe_rows > 0) {
        ibex::runtime::Table tf_table;
        {
            ibex::Column<ibex::Timestamp> ts_col;
            ibex::Column<double> price_col;
            ts_col.reserve(timeframe_rows);
            price_col.reserve(timeframe_rows);
            for (std::size_t i = 0; i < timeframe_rows; ++i) {
                ts_col.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL});
                price_col.push_back(100.0 + static_cast<double>(i % 100));
            }
            tf_table.add_column("ts", std::move(ts_col));
            tf_table.add_column("price", std::move(price_col));
        }
        ibex::runtime::TableRegistry tf_tables;
        tf_tables.emplace("tf_data", std::move(tf_table));
        if (print_types) {
            print_table_types("tf_data", tf_tables.find("tf_data")->second);
        }

        fmt::print("\n-- TimeFrame benchmarks ({} rows, 1s spacing) --\n", timeframe_rows);

        // Queries listed from cheapest to most expensive.
        // as_timeframe:       sort + time_index assignment — O(n log n)
        // tf_lag1:            vectorized shift — O(n)
        // tf_rolling_count_1m: binary search per row — O(n log n)
        // tf_rolling_sum_1m:   binary search + 60-row accumulate per row — O(60n)
        // tf_rolling_mean_5m:  binary search + 300-row accumulate per row — O(300n)
        std::vector<BenchQuery> tf_queries = {
            {"as_timeframe", R"(as_timeframe(tf_data, "ts"))"},
            {"tf_lag1", R"(as_timeframe(tf_data, "ts")[update { prev = lag(price, 1) }])"},
            {"tf_rolling_count_1m",
             R"(as_timeframe(tf_data, "ts")[window 1m, update { c = rolling_count() }])"},
            {"tf_rolling_sum_1m",
             R"(as_timeframe(tf_data, "ts")[window 1m, update { s = rolling_sum(price) }])"},
            {"tf_rolling_mean_5m",
             R"(as_timeframe(tf_data, "ts")[window 5m, update { m = rolling_mean(price) }])"},
            {"tf_resample_1m_ohlc",
             R"(as_timeframe(tf_data, "ts")[resample 1m, select { open = first(price), high = max(price), low = min(price), close = last(price) }])"},
        };

        for (const auto& query : tf_queries) {
            status = run_benchmark(query, tf_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }
    }

    return status;
}
