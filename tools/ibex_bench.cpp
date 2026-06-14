#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/rng.hpp>

#include <CLI/CLI.hpp>
#include <fmt/core.h>

// When jemalloc is linked, prevent large allocations from being returned to the OS
// between benchmark iterations.  By default jemalloc decays dirty pages after 10 s,
// but on WSL2 huge allocations (>= 2 MB) are munmap'd immediately on free, so every
// warm iteration re-pays the page-fault cost (~40 ms/128 MB at 4 M rows).
// Setting dirty_decay_ms:-1 keeps all freed pages in jemalloc's dirty cache so
// subsequent iterations reuse physical pages at full DRAM bandwidth.
// jemalloc reads this symbol during its C++ static-init phase, before main().
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern "C" {
const char* malloc_conf = "dirty_decay_ms:-1,muzzy_decay_ms:-1";
}

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <csv.hpp>
#include <fstream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

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

auto normalize_suite_name(std::string name) -> std::string {
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char ch) {
        if (ch == '-') {
            return '_';
        }
        return static_cast<char>(std::tolower(ch));
    });
    return name;
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

auto verify_distinct_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                            std::size_t rows) -> bool {
    const auto* symbol_col = table.find("symbol");
    const auto* out_symbol = result.find("symbol");
    if (symbol_col == nullptr || out_symbol == nullptr) {
        return false;
    }

    std::unordered_set<std::string> seen;
    std::vector<std::string> expected;
    expected.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        std::string symbol{string_view_at(*symbol_col, row)};
        if (!seen.insert(symbol).second) {
            continue;
        }
        expected.push_back(std::move(symbol));
    }

    if (result.rows() != expected.size()) {
        return false;
    }
    for (std::size_t i = 0; i < expected.size(); ++i) {
        if (string_view_at(*out_symbol, i) != expected[i]) {
            return false;
        }
    }
    return true;
}

auto verify_order_head_topk(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                            std::size_t rows, std::size_t k) -> bool {
    const auto* price_col = table.find("price");
    const auto* out_price = result.find("price");
    if (price_col == nullptr || out_price == nullptr) {
        return false;
    }

    std::vector<double> expected;
    expected.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        expected.push_back(double_at(*price_col, row));
    }
    std::stable_sort(expected.begin(), expected.end(), std::greater<double>{});
    if (expected.size() > k) {
        expected.resize(k);
    }

    if (result.rows() != expected.size()) {
        return false;
    }
    for (std::size_t i = 0; i < expected.size(); ++i) {
        if (std::abs(double_at(*out_price, i) - expected[i]) > 1e-9) {
            return false;
        }
    }
    return true;
}

auto verify_order_head_topk_by_symbol(const ibex::runtime::Table& table,
                                      const ibex::runtime::Table& result, std::size_t rows,
                                      std::size_t k) -> bool {
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    const auto* out_price = result.find("price");
    const auto* out_symbol = result.find("symbol");
    if (price_col == nullptr || symbol_col == nullptr || out_price == nullptr ||
        out_symbol == nullptr) {
        return false;
    }

    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        return double_at(*price_col, lhs) > double_at(*price_col, rhs);
    });

    std::unordered_map<std::string, std::size_t> seen;
    std::vector<std::size_t> expected_idx;
    expected_idx.reserve(std::min(rows, k * std::size_t{512}));
    for (std::size_t row : idx) {
        std::string symbol{string_view_at(*symbol_col, row)};
        auto& count = seen[symbol];
        if (count >= k) {
            continue;
        }
        ++count;
        expected_idx.push_back(row);
    }

    if (result.rows() != expected_idx.size()) {
        return false;
    }
    for (std::size_t i = 0; i < expected_idx.size(); ++i) {
        const std::size_t row = expected_idx[i];
        if (string_view_at(*out_symbol, i) != string_view_at(*symbol_col, row)) {
            return false;
        }
        if (std::abs(double_at(*out_price, i) - double_at(*price_col, row)) > 1e-9) {
            return false;
        }
    }
    return true;
}

auto verify_order_tail_topk(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                            std::size_t rows, std::size_t k) -> bool {
    const auto* price_col = table.find("price");
    const auto* out_price = result.find("price");
    if (price_col == nullptr || out_price == nullptr) {
        return false;
    }

    std::vector<double> expected;
    expected.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        expected.push_back(double_at(*price_col, row));
    }
    std::stable_sort(expected.begin(), expected.end(), std::greater<double>{});
    if (expected.size() > k) {
        expected.erase(expected.begin(), expected.end() - static_cast<std::ptrdiff_t>(k));
    }

    if (result.rows() != expected.size()) {
        return false;
    }
    for (std::size_t i = 0; i < expected.size(); ++i) {
        if (std::abs(double_at(*out_price, i) - expected[i]) > 1e-9) {
            return false;
        }
    }
    return true;
}

auto verify_order_tail_topk_by_symbol(const ibex::runtime::Table& table,
                                      const ibex::runtime::Table& result, std::size_t rows,
                                      std::size_t k) -> bool {
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    const auto* out_price = result.find("price");
    const auto* out_symbol = result.find("symbol");
    if (price_col == nullptr || symbol_col == nullptr || out_price == nullptr ||
        out_symbol == nullptr) {
        return false;
    }

    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        return double_at(*price_col, lhs) > double_at(*price_col, rhs);
    });

    std::unordered_map<std::string, std::vector<std::size_t>> groups;
    for (std::size_t row : idx) {
        groups[std::string{string_view_at(*symbol_col, row)}].push_back(row);
    }

    std::vector<std::size_t> expected_idx;
    expected_idx.reserve(std::min(rows, k * std::size_t{512}));
    for (auto& [_, group_rows] : groups) {
        const std::size_t keep = std::min(k, group_rows.size());
        expected_idx.insert(expected_idx.end(),
                            group_rows.end() - static_cast<std::ptrdiff_t>(keep), group_rows.end());
    }
    std::stable_sort(expected_idx.begin(), expected_idx.end(),
                     [&](std::size_t lhs, std::size_t rhs) {
                         const double l = double_at(*price_col, lhs);
                         const double r = double_at(*price_col, rhs);
                         if (l != r) {
                             return l > r;
                         }
                         return lhs < rhs;
                     });

    if (result.rows() != expected_idx.size()) {
        return false;
    }
    for (std::size_t i = 0; i < expected_idx.size(); ++i) {
        const std::size_t row = expected_idx[i];
        if (string_view_at(*out_symbol, i) != string_view_at(*symbol_col, row)) {
            return false;
        }
        if (std::abs(double_at(*out_price, i) - double_at(*price_col, row)) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Full-table single-key sort: result must be every input row, reordered into
// ascending `price` order. Sorting a copy of the input prices and comparing
// element-wise verifies both the permutation (same multiset) and the ordering.
auto verify_sort_price(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                       std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* out_price = result.find("price");
    if (price_col == nullptr || out_price == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::vector<double> expected;
    expected.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        expected.push_back(double_at(*price_col, row));
    }
    std::sort(expected.begin(), expected.end());
    for (std::size_t i = 0; i < rows; ++i) {
        if (std::abs(double_at(*out_price, i) - expected[i]) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Full-table multi-key sort: rows ordered by (symbol asc, price asc). Comparing
// against a stable_sort of the input indices on the same composite key checks
// the whole permutation, including tie-break behaviour within a symbol.
auto verify_sort_symbol_price(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                              std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    const auto* out_price = result.find("price");
    const auto* out_symbol = result.find("symbol");
    if (price_col == nullptr || symbol_col == nullptr || out_price == nullptr ||
        out_symbol == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        const auto ls = string_view_at(*symbol_col, lhs);
        const auto rs = string_view_at(*symbol_col, rhs);
        if (ls != rs) {
            return ls < rs;
        }
        return double_at(*price_col, lhs) < double_at(*price_col, rhs);
    });
    for (std::size_t i = 0; i < rows; ++i) {
        const std::size_t row = idx[i];
        if (string_view_at(*out_symbol, i) != string_view_at(*symbol_col, row)) {
            return false;
        }
        if (std::abs(double_at(*out_price, i) - double_at(*price_col, row)) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Full-table descending sort on a single Double key — exercises the descending
// radix path (order-preserving codes inverted before the radix pass).
auto verify_sort_price_desc(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                            std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* out_price = result.find("price");
    if (price_col == nullptr || out_price == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::vector<double> expected;
    expected.reserve(rows);
    for (std::size_t row = 0; row < rows; ++row) {
        expected.push_back(double_at(*price_col, row));
    }
    std::sort(expected.begin(), expected.end(), std::greater<double>{});
    for (std::size_t i = 0; i < rows; ++i) {
        if (std::abs(double_at(*out_price, i) - expected[i]) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Full-table sort on a single String key — exercises the comparison-sort
// (pdqsort) fallback. Compared against a stable_sort of the input indices, so
// the within-symbol tie order (input order) is checked too.
auto verify_sort_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                        std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    const auto* out_price = result.find("price");
    const auto* out_symbol = result.find("symbol");
    if (price_col == nullptr || symbol_col == nullptr || out_price == nullptr ||
        out_symbol == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        return string_view_at(*symbol_col, lhs) < string_view_at(*symbol_col, rhs);
    });
    for (std::size_t i = 0; i < rows; ++i) {
        const std::size_t row = idx[i];
        if (string_view_at(*out_symbol, i) != string_view_at(*symbol_col, row)) {
            return false;
        }
        if (std::abs(double_at(*out_price, i) - double_at(*price_col, row)) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Full-table mixed-direction multi-key sort: (symbol asc, price desc). Exercises
// the multi-key radix with a descending secondary key.
auto verify_sort_symbol_price_desc(const ibex::runtime::Table& table,
                                   const ibex::runtime::Table& result, std::size_t rows) -> bool {
    const auto* price_col = table.find("price");
    const auto* symbol_col = table.find("symbol");
    const auto* out_price = result.find("price");
    const auto* out_symbol = result.find("symbol");
    if (price_col == nullptr || symbol_col == nullptr || out_price == nullptr ||
        out_symbol == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    std::stable_sort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
        const auto ls = string_view_at(*symbol_col, lhs);
        const auto rs = string_view_at(*symbol_col, rhs);
        if (ls != rs) {
            return ls < rs;
        }
        return double_at(*price_col, lhs) > double_at(*price_col, rhs);
    });
    for (std::size_t i = 0; i < rows; ++i) {
        const std::size_t row = idx[i];
        if (string_view_at(*out_symbol, i) != string_view_at(*symbol_col, row)) {
            return false;
        }
        if (std::abs(double_at(*out_price, i) - double_at(*price_col, row)) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Grouped cumulative sum: cumsum(price) by symbol. `update` preserves input row
// order, so output row i corresponds to input row i; we recompute the running
// per-symbol total and compare. A grouping bug (running over the whole table
// instead of per symbol) shows up immediately.
auto verify_cumsum_by_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                             std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_cs = result.find("cs");
    if (sym_col == nullptr || price_col == nullptr || out_cs == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::unordered_map<std::string, double> running;
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        double& acc = running[key];
        acc += double_at(*price_col, row);
        if (std::abs(double_at(*out_cs, row) - acc) > 1e-6) {
            return false;
        }
    }
    return true;
}

// Grouped lag: lag(price, 1) by symbol. The first row of each symbol is null
// (skipped here); every later row must equal the previous price *within the same
// symbol*, not the previous row of the table.
auto verify_lag_by_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                          std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_prev = result.find("prev");
    if (sym_col == nullptr || price_col == nullptr || out_prev == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    std::unordered_map<std::string, double> prev_price;
    std::unordered_set<std::string> seen;
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        if (seen.contains(key)) {
            if (std::abs(double_at(*out_prev, row) - prev_price[key]) > 1e-9) {
                return false;
            }
        }
        seen.insert(key);
        prev_price[key] = double_at(*price_col, row);
    }
    return true;
}

// Grouped dense rank: rank(price, method = dense, ascending = false) by symbol.
// Recompute the dense rank of each row's price within its symbol's distinct,
// descending-sorted price set.
auto verify_rank_by_symbol(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                           std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_rk = result.find("rk");
    if (sym_col == nullptr || price_col == nullptr || out_rk == nullptr) {
        return false;
    }
    if (result.rows() != rows) {
        return false;
    }
    // Per-symbol sorted-descending list of distinct prices → dense rank.
    std::unordered_map<std::string, std::unordered_map<double, std::int64_t>> dense;
    {
        std::unordered_map<std::string, std::vector<double>> by_sym;
        for (std::size_t row = 0; row < rows; ++row) {
            by_sym[std::string(string_view_at(*sym_col, row))].push_back(
                double_at(*price_col, row));
        }
        for (auto& [sym, vals] : by_sym) {
            std::sort(vals.begin(), vals.end(), std::greater<double>{});
            auto& rank_of = dense[sym];
            std::int64_t r = 0;
            double last = 0.0;
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (i == 0 || vals[i] != last) {
                    ++r;
                    last = vals[i];
                }
                rank_of.emplace(vals[i], r);
            }
        }
    }
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        std::int64_t expected = dense[key][double_at(*price_col, row)];
        if (int_at(*out_rk, row) != expected) {
            return false;
        }
    }
    return true;
}

// Per-group statistic in encounter order: median / 90th-percentile / sample
// stddev of price by symbol. `kind` selects which, and must match the result's
// single value column name. The quantile/median/stddev math mirrors the
// interpreter (linear-interpolated quantile, sample stddev with n-1).
auto verify_group_stat(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                       std::size_t rows, std::string_view kind, std::string_view out_name) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_sym = result.find("symbol");
    const auto* out_val = result.find(std::string(out_name));
    if (sym_col == nullptr || price_col == nullptr || out_sym == nullptr || out_val == nullptr) {
        return false;
    }
    std::unordered_map<std::string, std::size_t> index;
    std::vector<std::string> order;
    std::vector<std::vector<double>> values;
    for (std::size_t row = 0; row < rows; ++row) {
        std::string key(string_view_at(*sym_col, row));
        auto it = index.find(key);
        std::size_t gid = 0;
        if (it == index.end()) {
            gid = order.size();
            index.emplace(key, gid);
            order.push_back(key);
            values.emplace_back();
        } else {
            gid = it->second;
        }
        values[gid].push_back(double_at(*price_col, row));
    }
    if (result.rows() != order.size()) {
        return false;
    }
    for (std::size_t i = 0; i < order.size(); ++i) {
        if (string_view_at(*out_sym, i) != order[i]) {
            return false;
        }
        std::vector<double> sorted = values[i];
        std::sort(sorted.begin(), sorted.end());
        const std::size_t n = sorted.size();
        double expected = 0.0;
        if (kind == "median") {
            if (n > 0) {
                expected = (n % 2 == 1) ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
            }
        } else if (kind == "quantile90") {
            if (n > 0) {
                double idx = 0.9 * static_cast<double>(n - 1);
                std::size_t lo = static_cast<std::size_t>(idx);
                std::size_t hi = lo + 1 < n ? lo + 1 : lo;
                double frac = idx - static_cast<double>(lo);
                expected = sorted[lo] + (frac * (sorted[hi] - sorted[lo]));
            }
        } else {  // std (sample)
            if (n >= 2) {
                double mean = 0.0;
                for (double v : sorted) {
                    mean += v;
                }
                mean /= static_cast<double>(n);
                double m2 = 0.0;
                for (double v : sorted) {
                    m2 += (v - mean) * (v - mean);
                }
                expected = std::sqrt(m2 / static_cast<double>(n - 1));
            }
        }
        if (std::abs(double_at(*out_val, i) - expected) > 1e-6) {
            return false;
        }
    }
    return true;
}

// Multi-stage pipeline: filter price > 500 → mean(price) by symbol → order avg
// desc → head 10. Recompute the same chain and compare the top-10 (symbol, avg).
auto verify_filter_group_sort(const ibex::runtime::Table& table, const ibex::runtime::Table& result,
                              std::size_t rows) -> bool {
    const auto* sym_col = table.find("symbol");
    const auto* price_col = table.find("price");
    const auto* out_sym = result.find("symbol");
    const auto* out_avg = result.find("avg");
    if (sym_col == nullptr || price_col == nullptr || out_sym == nullptr || out_avg == nullptr) {
        return false;
    }
    std::unordered_map<std::string, std::size_t> index;
    std::vector<std::string> order;
    std::vector<double> sum;
    std::vector<std::int64_t> count;
    for (std::size_t row = 0; row < rows; ++row) {
        if (double_at(*price_col, row) <= 500.0) {
            continue;
        }
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
    std::vector<std::size_t> gids(order.size());
    for (std::size_t i = 0; i < gids.size(); ++i) {
        gids[i] = i;
    }
    auto avg_of = [&](std::size_t g) {
        return count[g] == 0 ? 0.0 : sum[g] / static_cast<double>(count[g]);
    };
    std::stable_sort(gids.begin(), gids.end(),
                     [&](std::size_t l, std::size_t r) { return avg_of(l) > avg_of(r); });
    const std::size_t keep = std::min<std::size_t>(10, gids.size());
    if (result.rows() != keep) {
        return false;
    }
    for (std::size_t i = 0; i < keep; ++i) {
        if (string_view_at(*out_sym, i) != order[gids[i]]) {
            return false;
        }
        if (std::abs(double_at(*out_avg, i) - avg_of(gids[i])) > 1e-9) {
            return false;
        }
    }
    return true;
}

// Inner join row count: rows of `left` whose `key` appears in `right`'s key set.
// `right` keys are assumed unique (lookup symbols / users) so the inner join is
// one row per matching left row.
auto verify_inner_join(const ibex::runtime::Table& left, const ibex::runtime::Table& right,
                       const ibex::runtime::Table& result, std::size_t left_rows,
                       std::size_t right_rows, std::string_view key) -> bool {
    const auto* l_key = left.find(std::string(key));
    const auto* r_key = right.find(std::string(key));
    if (l_key == nullptr || r_key == nullptr) {
        return false;
    }
    std::unordered_set<std::string> keys;
    for (std::size_t row = 0; row < right_rows; ++row) {
        keys.emplace(string_view_at(*r_key, row));
    }
    std::size_t expected = 0;
    for (std::size_t row = 0; row < left_rows; ++row) {
        if (keys.contains(std::string(string_view_at(*l_key, row)))) {
            ++expected;
        }
    }
    return result.rows() == expected;
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
                } else if constexpr (std::is_same_v<ColType, ibex::Column<bool>>) {
                    ibex::Column<bool> data;
                    data.reserve(n);
                    for (std::size_t i = 0; i < n; ++i) {
                        data.push_back(col[i]);
                    }
                    return data;
                } else {
                    static_assert(std::is_same_v<ColType, void>, "Unhandled column type");
                }
            },
            column);
        out.add_column(entry.name, std::move(sliced));
    }
    return out;
}

auto verify_join_row_count(const ibex::runtime::Table& result, std::size_t expected_rows,
                           std::string_view label) -> std::optional<std::string> {
    if (result.rows() != expected_rows) {
        return fmt::format("{} row-count mismatch: expected {}, got {}", label, expected_rows,
                           result.rows());
    }
    return std::nullopt;
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
    const ibex::runtime::Table* lookup = nullptr;
    if (tables.contains("lookup")) {
        lookup = &tables.at("lookup");
        sliced.emplace("lookup", slice_table(*lookup, max_rows));
    }
    const ibex::runtime::Table* prices_small = nullptr;
    if (tables.contains("prices_small")) {
        prices_small = &tables.at("prices_small");
        sliced.emplace("prices_small", slice_table(*prices_small, max_rows));
    }
    const ibex::runtime::Table* lookup_small = nullptr;
    if (tables.contains("lookup_small")) {
        lookup_small = &tables.at("lookup_small");
        sliced.emplace("lookup_small", slice_table(*lookup_small, max_rows));
    }
    const ibex::runtime::Table* events = nullptr;
    if (tables.contains("events")) {
        events = &tables.at("events");
        sliced.emplace("events", slice_table(*events, max_rows));
    }
    const ibex::runtime::Table* users = nullptr;
    if (tables.contains("users")) {
        users = &tables.at("users");
        sliced.emplace("users", slice_table(*users, max_rows));
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
    } else if (query.name == "distinct_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_distinct_symbol(sliced.at("prices"), *result, rows)) {
            return "distinct_symbol verification failed";
        }
    } else if (query.name == "order_head_topk") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_order_head_topk(sliced.at("prices"), *result, rows, 100)) {
            return "order_head_topk verification failed";
        }
    } else if (query.name == "order_head_topk_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_order_head_topk_by_symbol(sliced.at("prices"), *result, rows, 3)) {
            return "order_head_topk_by_symbol verification failed";
        }
    } else if (query.name == "order_tail_topk") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_order_tail_topk(sliced.at("prices"), *result, rows, 100)) {
            return "order_tail_topk verification failed";
        }
    } else if (query.name == "order_tail_topk_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_order_tail_topk_by_symbol(sliced.at("prices"), *result, rows, 3)) {
            return "order_tail_topk_by_symbol verification failed";
        }
    } else if (query.name == "sort_price") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_sort_price(sliced.at("prices"), *result, rows)) {
            return "sort_price verification failed";
        }
    } else if (query.name == "sort_symbol_price") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_sort_symbol_price(sliced.at("prices"), *result, rows)) {
            return "sort_symbol_price verification failed";
        }
    } else if (query.name == "sort_price_desc") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_sort_price_desc(sliced.at("prices"), *result, rows)) {
            return "sort_price_desc verification failed";
        }
    } else if (query.name == "sort_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_sort_symbol(sliced.at("prices"), *result, rows)) {
            return "sort_symbol verification failed";
        }
    } else if (query.name == "sort_symbol_price_desc") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_sort_symbol_price_desc(sliced.at("prices"), *result, rows)) {
            return "sort_symbol_price_desc verification failed";
        }
    } else if (query.name == "cumsum_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_cumsum_by_symbol(sliced.at("prices"), *result, rows)) {
            return "cumsum_by_symbol verification failed";
        }
    } else if (query.name == "lag_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_lag_by_symbol(sliced.at("prices"), *result, rows)) {
            return "lag_by_symbol verification failed";
        }
    } else if (query.name == "rank_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_rank_by_symbol(sliced.at("prices"), *result, rows)) {
            return "rank_by_symbol verification failed";
        }
    } else if (query.name == "median_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_group_stat(sliced.at("prices"), *result, rows, "median", "med")) {
            return "median_by_symbol verification failed";
        }
    } else if (query.name == "quantile_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_group_stat(sliced.at("prices"), *result, rows, "quantile90", "p90")) {
            return "quantile_by_symbol verification failed";
        }
    } else if (query.name == "std_by_symbol") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_group_stat(sliced.at("prices"), *result, rows, "std", "sd")) {
            return "std_by_symbol verification failed";
        }
    } else if (query.name == "filter_group_sort") {
        if (table == nullptr) {
            return "missing prices table";
        }
        std::size_t rows = sliced.at("prices").rows();
        if (!verify_filter_group_sort(sliced.at("prices"), *result, rows)) {
            return "filter_group_sort verification failed";
        }
    } else if (query.name == "inner_join_symbol") {
        if (table == nullptr || lookup == nullptr) {
            return "missing prices or lookup table";
        }
        if (!verify_inner_join(sliced.at("prices"), sliced.at("lookup"), *result,
                               sliced.at("prices").rows(), sliced.at("lookup").rows(), "symbol")) {
            return "inner_join_symbol verification failed";
        }
    } else if (query.name == "inner_join_user") {
        if (events == nullptr || users == nullptr) {
            return "missing events or users table";
        }
        if (!verify_inner_join(sliced.at("events"), sliced.at("users"), *result,
                               sliced.at("events").rows(), sliced.at("users").rows(), "user_id")) {
            return "inner_join_user verification failed";
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
    } else if (query.name == "null_left_join") {
        if (table == nullptr) {
            return "missing prices table";
        }
        if (auto err =
                verify_join_row_count(*result, sliced.at("prices").rows(), "null_left_join")) {
            return *err;
        }
    } else if (query.name == "null_semi_join") {
        if (table == nullptr || lookup == nullptr) {
            return "missing prices/lookup tables";
        }
        std::size_t expected = 0;
        const auto* p_sym = sliced.at("prices").find("symbol");
        const auto* l_sym = sliced.at("lookup").find("symbol");
        if (p_sym == nullptr || l_sym == nullptr) {
            return "missing symbol column for semi join verification";
        }
        std::unordered_set<std::string> right_keys;
        right_keys.reserve(sliced.at("lookup").rows());
        for (std::size_t r = 0; r < sliced.at("lookup").rows(); ++r) {
            right_keys.emplace(string_view_at(*l_sym, r));
        }
        for (std::size_t l = 0; l < sliced.at("prices").rows(); ++l) {
            if (right_keys.contains(std::string(string_view_at(*p_sym, l)))) {
                ++expected;
            }
        }
        if (auto err = verify_join_row_count(*result, expected, "null_semi_join")) {
            return *err;
        }
    } else if (query.name == "null_anti_join") {
        if (table == nullptr || lookup == nullptr) {
            return "missing prices/lookup tables";
        }
        std::size_t expected = 0;
        const auto* p_sym = sliced.at("prices").find("symbol");
        const auto* l_sym = sliced.at("lookup").find("symbol");
        if (p_sym == nullptr || l_sym == nullptr) {
            return "missing symbol column for anti join verification";
        }
        std::unordered_set<std::string> right_keys;
        right_keys.reserve(sliced.at("lookup").rows());
        for (std::size_t r = 0; r < sliced.at("lookup").rows(); ++r) {
            right_keys.emplace(string_view_at(*l_sym, r));
        }
        for (std::size_t l = 0; l < sliced.at("prices").rows(); ++l) {
            if (!right_keys.contains(std::string(string_view_at(*p_sym, l)))) {
                ++expected;
            }
        }
        if (auto err = verify_join_row_count(*result, expected, "null_anti_join")) {
            return *err;
        }
    } else if (query.name == "null_cross_join_small") {
        if (prices_small == nullptr || lookup_small == nullptr) {
            return "missing prices_small/lookup_small tables";
        }
        std::size_t expected = sliced.at("prices_small").rows() * sliced.at("lookup_small").rows();
        if (auto err = verify_join_row_count(*result, expected, "null_cross_join_small")) {
            return *err;
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
        if (t < mn)
            mn = t;
        if (t > mx)
            mx = t;
        sq_sum += (t - avg) * (t - avg);
    }
    double stddev = times.size() > 1 ? std::sqrt(sq_sum / static_cast<double>(times.size())) : 0.0;
    std::sort(times.begin(), times.end());
    auto percentile = [&](double p) -> double {
        double idx = p * static_cast<double>(times.size() - 1);
        auto lo = static_cast<std::size_t>(idx);
        double frac = idx - static_cast<double>(lo);
        if (lo + 1 < times.size()) {
            return (times[lo] * (1.0 - frac)) + (times[lo + 1] * frac);
        }
        return times[lo];
    };
    return {total, avg, mn, mx, stddev, percentile(0.95), percentile(0.99)};
}

// Reset the kernel's peak-RSS counter (VmHWM) so the next peak_rss_mb() read
// reflects only the work done since this call. Writing "5" to clear_refs clears
// the per-process peak. Linux-only; a no-op where /proc/self/clear_refs is
// unavailable (the subsequent peak read then reports the lifetime peak).
void reset_peak_rss() {
    if (std::FILE* f = std::fopen("/proc/self/clear_refs", "w")) {
        std::fputs("5\n", f);
        std::fclose(f);
    }
}

// Read VmHWM (peak resident set size) from /proc/self/status, in MiB.
// Returns 0.0 where unavailable.
auto peak_rss_mb() -> double {
    std::ifstream status("/proc/self/status");
    std::string key;
    while (status >> key) {
        if (key == "VmHWM:") {
            long kb = 0;
            status >> kb;
            return static_cast<double>(kb) / 1024.0;
        }
        status.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 0.0;
}

// Print one benchmark result line in the key=value format that bench_ibex.sh
// parses into a TSV row. peak_rss_mb is the absolute VmHWM during the measured
// iterations (reset via reset_peak_rss() just before the timed loop).
void print_bench_line(std::string_view name, std::size_t iters, const BenchStats& s,
                      std::size_t rows, double peak_mb) {
    fmt::print(
        "bench {}: iters={}, total_ms={:.3f}, avg_ms={:.3f}, min_ms={:.3f}, "
        "max_ms={:.3f}, stddev_ms={:.3f}, p95_ms={:.3f}, p99_ms={:.3f}, rows={}, "
        "peak_rss_mb={:.1f}\n",
        name, iters, s.total_ms, s.avg_ms, s.min_ms, s.max_ms, s.stddev_ms, s.p95_ms, s.p99_ms,
        rows, peak_mb);
}

volatile std::uint64_t g_bench_sink = 0;

auto pack_filter_micro_word_scalar(const std::uint8_t* mp, std::size_t lim) noexcept
    -> std::uint64_t {
    std::uint64_t bits = 0;
    for (std::size_t i = 0; i < lim; ++i) {
        bits |= static_cast<std::uint64_t>(mp[i] != 0) << i;
    }
    return bits;
}

#if defined(__AVX2__)
auto pack_filter_micro_word_avx2(const std::uint8_t* mp) noexcept -> std::uint64_t {
    const __m256i zero = _mm256_setzero_si256();
    const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mp));
    const __m256i hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(mp + 32));
    const auto lo_bits =
        static_cast<std::uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(lo, zero)));
    const auto hi_bits =
        static_cast<std::uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(hi, zero)));
    return static_cast<std::uint64_t>(lo_bits) | (static_cast<std::uint64_t>(hi_bits) << 32);
}
#endif

auto pack_filter_micro_mask(const std::vector<std::uint8_t>& mask,
                            std::vector<std::uint64_t>& keep_words) -> std::size_t {
    const std::size_t n = mask.size();
    const std::size_t n_words = (n + 63) / 64;
    keep_words.assign(n_words, 0);
    std::size_t kept = 0;
    for (std::size_t w = 0; w < n_words; ++w) {
        const std::size_t base = w * 64;
        const std::size_t lim = std::min<std::size_t>(64, n - base);
#if defined(__AVX2__)
        const std::uint64_t bits = (lim == 64)
                                       ? pack_filter_micro_word_avx2(mask.data() + base)
                                       : pack_filter_micro_word_scalar(mask.data() + base, lim);
#else
        const std::uint64_t bits = pack_filter_micro_word_scalar(mask.data() + base, lim);
#endif
        keep_words[w] = bits;
        kept += static_cast<std::size_t>(std::popcount(bits));
    }
    return kept;
}

auto build_filter_micro_indices(const std::vector<std::uint64_t>& keep_words, std::size_t out_n)
    -> std::vector<std::size_t> {
    std::vector<std::size_t> indices;
    indices.reserve(out_n);
    for (std::size_t w = 0; w < keep_words.size(); ++w) {
        std::uint64_t bits = keep_words[w];
        const std::size_t base = w * 64;
        while (bits != 0) {
            const int bit = std::countr_zero(bits);
            indices.push_back(base + static_cast<std::size_t>(bit));
            bits &= bits - 1;
        }
    }
    return indices;
}

template <typename T>
auto digest_primitive_column(const ibex::Column<T>& col) -> std::uint64_t {
    if (col.size() == 0) {
        return 0;
    }
    const std::size_t mid = col.size() / 2;
    auto value_bits = [](T value) -> std::uint64_t {
        if constexpr (std::is_floating_point_v<T>) {
            std::uint64_t bits = 0;
            std::memcpy(&bits, &value, sizeof(value));
            return bits;
        } else {
            return static_cast<std::uint64_t>(value);
        }
    };
    return value_bits(col[0]) ^ (value_bits(col[mid]) << 1) ^
           (value_bits(col[col.size() - 1]) << 2) ^ static_cast<std::uint64_t>(col.size());
}

template <typename Fn>
auto run_bitmap_kernel_benchmark(std::string_view bench_name, std::size_t rows,
                                 std::size_t warmup_iters, std::size_t iters, Fn&& run_once)
    -> int {
    auto run_and_touch = [&]() -> int {
        auto merged = run_once();
        if (!merged.has_value()) {
            fmt::print("error: {} returned nullopt unexpectedly\n", bench_name);
            return 1;
        }
        const auto sample_index = static_cast<std::size_t>(g_bench_sink) % rows;
        g_bench_sink ^= static_cast<std::uint64_t>(merged->size());
        g_bench_sink ^= static_cast<std::uint64_t>((*merged)[sample_index]);
        return 0;
    };

    for (std::size_t i = 0; i < warmup_iters; ++i) {
        if (run_and_touch() != 0) {
            return 1;
        }
    }

    reset_peak_rss();
    std::vector<double> times(iters);
    for (std::size_t i = 0; i < iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        if (run_and_touch() != 0) {
            return 1;
        }
        auto t1 = std::chrono::steady_clock::now();
        times[i] =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();
    }
    double peak_mb = peak_rss_mb();

    auto s = compute_stats(std::move(times));
    print_bench_line(bench_name, iters, s, rows, peak_mb);
    return 0;
}

template <typename Fn>
auto run_scalar_kernel_benchmark(std::string_view bench_name, std::size_t rows,
                                 std::size_t warmup_iters, std::size_t iters, Fn&& run_once)
    -> int {
    auto run_and_touch = [&]() {
        g_bench_sink ^= run_once();
    };

    for (std::size_t i = 0; i < warmup_iters; ++i) {
        run_and_touch();
    }

    reset_peak_rss();
    std::vector<double> times(iters);
    for (std::size_t i = 0; i < iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        run_and_touch();
        auto t1 = std::chrono::steady_clock::now();
        times[i] =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();
    }
    double peak_mb = peak_rss_mb();

    auto s = compute_stats(std::move(times));
    print_bench_line(bench_name, iters, s, rows, peak_mb);
    return 0;
}

// Pair of (binding name, CSV path) for include-read mode. When non-empty,
// each timed iteration reloads the listed CSVs into a fresh registry so the
// timer includes read_csv() — apples-to-apples with `pl.scan_csv(...).collect()`.
using ScanPaths = std::vector<std::pair<std::string, std::string>>;

auto run_benchmark(const BenchQuery& query, const ibex::runtime::TableRegistry& tables,
                   std::size_t warmup_iters, std::size_t iters, bool include_parse,
                   const ScanPaths& scan_paths = {}) -> int {
    auto normalized = normalize_input(query.source);
    ibex::runtime::ScalarRegistry scalars;

    if (!scan_paths.empty()) {
        // Scan mode: time CSV-read + parse + lower + interpret per iteration.
        // The supplied `tables` arg is ignored — we build a fresh registry
        // inside the loop. Forces include_parse semantics regardless of flag.
        auto run_once_scan = [&](std::size_t& last_rows) -> int {
            ibex::runtime::TableRegistry fresh;
            for (const auto& [name, path] : scan_paths) {
                try {
                    fresh.emplace(name, read_csv(path));
                } catch (const std::exception& e) {
                    fmt::print("error: read_csv({}) failed for {}: {}\n", path, query.name,
                               e.what());
                    return 1;
                }
            }
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
            auto result = ibex::runtime::interpret(*lowered.value(), fresh, &scalars);
            if (!result) {
                fmt::print("error: interpret failed for {}: {}\n", query.name, result.error());
                return 1;
            }
            last_rows = result->rows();
            return 0;
        };

        std::size_t warmup_rows = 0;
        for (std::size_t i = 0; i < warmup_iters; ++i) {
            if (run_once_scan(warmup_rows) != 0) {
                return 1;
            }
        }
        std::size_t last_rows = 0;
        reset_peak_rss();
        std::vector<double> times(iters);
        for (std::size_t i = 0; i < iters; ++i) {
            auto t0 = std::chrono::steady_clock::now();
            if (run_once_scan(last_rows) != 0) {
                return 1;
            }
            auto t1 = std::chrono::steady_clock::now();
            times[i] =
                std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0)
                    .count();
        }
        double peak_mb = peak_rss_mb();
        auto s = compute_stats(std::move(times));
        print_bench_line(query.name, iters, s, last_rows, peak_mb);
        return 0;
    }

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
        reset_peak_rss();
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
                std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0)
                    .count();
        }
        double peak_mb = peak_rss_mb();
        auto s = compute_stats(std::move(times));
        print_bench_line(query.name, iters, s, last_rows, peak_mb);

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
    reset_peak_rss();
    std::vector<double> times(iters);
    for (std::size_t i = 0; i < iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();
        if (run_once(last_rows) != 0) {
            return 1;
        }
        auto t1 = std::chrono::steady_clock::now();
        times[i] =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(t1 - t0).count();
    }
    double peak_mb = peak_rss_mb();
    auto s = compute_stats(std::move(times));
    print_bench_line(query.name, iters, s, last_rows, peak_mb);

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
    std::string csv_users_path;
    std::size_t warmup_iters = 1;
    std::size_t iters = 5;
    bool include_parse = true;
    bool include_read = false;
    bool print_types = false;
    bool verify = false;
    std::size_t verify_rows = 10000;
    std::size_t timeframe_rows = 0;
    std::size_t reshape_rows = 100'000;
    std::size_t merge_validity_rows = 4'000'000;
    std::size_t rng_micro_rows = 4'000'000;
    std::size_t bool_rows = 4'000'000;
    std::size_t filter_micro_rows = 0;
    std::vector<std::string> suites;

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
    app.add_option("--csv-users", csv_users_path,
                   "CSV file path for the join benchmark dimension (user_id, tier) — one row per "
                   "distinct events user_id")
        ->check(CLI::ExistingFile);
    app.add_option("--warmup", warmup_iters, "Warmup iterations")->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations")->check(CLI::PositiveNumber);
    app.add_flag("--include-parse", include_parse,
                 "Include parse + lower in timing (default: enabled)");
    app.add_flag("--no-include-parse", include_parse,
                 "Exclude parse + lower from timing (legacy mode)")
        ->excludes("--include-parse");
    app.add_flag("--include-read", include_read,
                 "Time read_csv() inside each iteration ('data still needs to be read'); "
                 "matches pl.scan_csv(path).<chain>.collect(). Tagged as ibex_scan in output.");
    app.add_flag("--print-types", print_types, "Print column types for loaded benchmark tables");
    app.add_flag("--verify", verify, "Verify benchmark outputs on a sample of rows");
    app.add_option("--verify-rows", verify_rows,
                   "Rows to sample per table during verification (default: 10000)")
        ->check(CLI::PositiveNumber);
    app.add_option(
           "--timeframe-rows", timeframe_rows,
           "Row count for in-memory TimeFrame benchmarks (lag, rolling_*). 0 = skip (default).")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--reshape-rows", reshape_rows,
                   "Row count for synthetic reshape benchmark table (default: 100000); "
                   "0 skips the reshape benchmarks")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--merge-validity-rows", merge_validity_rows,
                   "Row count for merge_validity micro benchmark (default: 4000000)")
        ->check(CLI::PositiveNumber);
    app.add_option("--rng-micro-rows", rng_micro_rows,
                   "Row count for rng_micro kernel benchmark (default: 4000000)")
        ->check(CLI::PositiveNumber);
    app.add_option("--bool-rows", bool_rows,
                   "Row count for bool-column benchmark suite (default: 4000000)")
        ->check(CLI::PositiveNumber);
    app.add_option("--filter-micro-rows", filter_micro_rows,
                   "Rows from csv-trades to use for filter_micro; 0 = all rows (default)")
        ->check(CLI::NonNegativeNumber);
    app.add_option("--suite", suites,
                   "Benchmark suite(s) to run (comma-separated or repeated). "
                   "Supported: all, core, cumulative, sort, window, groupagg, pipeline, join, "
                   "rng, fill, null, filter, multi, "
                   "events, reshape, timeframe, merge_validity, rng_micro, bool, filter_micro")
        ->delimiter(',');

    CLI11_PARSE(app, argc, argv);

    std::unordered_set<std::string> selected_suites;
    const std::unordered_set<std::string> allowed_suites = {
        "all",      "core",         "cumulative", "sort",      "window",         "groupagg",
        "pipeline", "join",         "rng",        "fill",      "null",           "filter",
        "multi",    "events",       "reshape",    "timeframe", "merge_validity", "rng_micro",
        "bool",     "filter_micro", "transform",  "stats"};
    for (const auto& token : suites) {
        auto normalized = normalize_suite_name(token);
        if (!allowed_suites.contains(normalized)) {
            fmt::print("error: unknown suite '{}'\n", token);
            return 1;
        }
        selected_suites.insert(std::move(normalized));
    }
    if (selected_suites.empty()) {
        selected_suites.insert("all");
    }
    auto run_suite = [&](std::string_view suite) -> bool {
        if (selected_suites.contains(std::string(suite))) {
            return true;
        }
        if (!selected_suites.contains("all")) {
            return false;
        }
        // Keep legacy --suite all behavior stable; micro suites are opt-in.
        return suite != "merge_validity" && suite != "rng_micro" && suite != "bool" &&
               suite != "filter_micro";
    };

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
            {
                "distinct_symbol",
                "prices[distinct { symbol }]",
            },
            {
                "order_head_topk",
                "prices[order price desc, head 100]",
            },
            {
                "order_head_topk_by_symbol",
                "prices[order price desc, head 3, by symbol]",
            },
            {
                "order_tail_topk",
                "prices[order price desc, tail 100]",
            },
            {
                "order_tail_topk_by_symbol",
                "prices[order price desc, tail 3, by symbol]",
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

        // The first seven queries benchmark pure execution (use --no-include-parse for isolation).
        // The last three use default include_parse to measure parsing cost.
        if (run_suite("core")) {
            for (std::size_t qi = 0; qi < queries.size(); ++qi) {
                // parse_* queries always include parsing in the timing
                bool this_include_parse = (qi >= 7) ? true : saved_include_parse;
                if (verify && queries[qi].name.rfind("parse_", 0) != 0) {
                    if (auto err = verify_benchmark(queries[qi], tables, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", queries[qi].name, *err);
                        return 1;
                    }
                }
                ScanPaths sp;
                if (include_read && queries[qi].name.rfind("parse_", 0) != 0) {
                    sp.emplace_back("prices", csv_path);
                }
                status =
                    run_benchmark(queries[qi], tables, warmup_iters, iters, this_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Cumulative function benchmarks: cumsum and cumprod on a 4M-row price
        // column. No grouping — exercises the simple scan-and-accumulate path.
        if (status == 0 && run_suite("cumulative")) {
            fmt::print("\n-- Cumulative function benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> cumulative_queries = {
                {"cumsum_price", "prices[update { cs = cumsum(price) }]"},
                {"cumprod_price", "prices[update { cp = cumprod(price) }]"},
            };
            for (const auto& query : cumulative_queries) {
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Full-table sort benchmarks: unlike the order_*_topk queries (which
        // only materialise the top/bottom N rows and can use a partial sort),
        // these reorder *every* row, covering each sort code path:
        //   sort_price            — single ascending Double key (F64 radix)
        //   sort_price_desc       — single descending Double key (inverted radix)
        //   sort_symbol           — single String key (pdqsort comparison fallback)
        //   sort_symbol_price     — (String asc, Double asc) multi-key radix
        //   sort_symbol_price_desc— (String asc, Double desc) mixed multi-key radix
        if (status == 0 && run_suite("sort")) {
            fmt::print("\n-- Full-sort benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> sort_queries = {
                {"sort_price", "prices[order price]"},
                {"sort_price_desc", "prices[order { price desc }]"},
                {"sort_symbol", "prices[order symbol]"},
                {"sort_symbol_price", "prices[order { symbol asc, price asc }]"},
                {"sort_symbol_price_desc", "prices[order { symbol asc, price desc }]"},
            };
            for (const auto& query : sort_queries) {
                if (verify) {
                    if (auto err = verify_benchmark(query, tables, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                        return 1;
                    }
                }
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Grouped/partitioned window functions (by symbol). Unlike the global
        // cumsum_price / tf_lag1 queries, these run a window primitive *within
        // each group*: dense rank, lag(1), and a running sum reset per symbol.
        if (status == 0 && run_suite("window")) {
            fmt::print("\n-- Grouped window benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> window_queries = {
                {"rank_by_symbol",
                 "prices[update { rk = rank(price, method = dense, ascending = false) }, by "
                 "symbol]"},
                {"lag_by_symbol", "prices[update { prev = lag(price, 1) }, by symbol]"},
                {"cumsum_by_symbol", "prices[update { cs = cumsum(price) }, by symbol]"},
            };
            for (const auto& query : window_queries) {
                if (verify) {
                    if (auto err = verify_benchmark(query, tables, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                        return 1;
                    }
                }
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Expensive group-by aggregates (by symbol): exact median, 90th
        // percentile, and sample standard deviation. These collect-and-reduce
        // per group rather than the streaming sum/min/max of mean_by_symbol.
        if (status == 0 && run_suite("groupagg")) {
            fmt::print("\n-- Group aggregate benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> groupagg_queries = {
                {"median_by_symbol", "prices[select { med = median(price) }, by symbol]"},
                {"quantile_by_symbol", "prices[select { p90 = quantile(price, 0.9) }, by symbol]"},
                {"std_by_symbol", "prices[select { sd = std(price) }, by symbol]"},
            };
            for (const auto& query : groupagg_queries) {
                if (verify) {
                    if (auto err = verify_benchmark(query, tables, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                        return 1;
                    }
                }
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Multi-stage pipelines: deeper operator chains than the single-operator
        // benchmarks above, exercising the pipeline planner / operator fusion
        // and the handoff of derived columns between stages.
        //   filter_group_sort   — filter → group-by → order → head (fused TopK)
        //   update_group_filter — update by → filter on derived col → re-aggregate
        //   group_rank_filter   — rank within group → top-N filter → aggregate
        //   normalize_by_group  — grouped z-score (per-row col + group aggregate)
        //                         → clip → re-aggregate
        if (status == 0 && run_suite("pipeline")) {
            fmt::print("\n-- Pipeline benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> pipeline_queries = {
                {"filter_group_sort",
                 "prices[filter price > 500.0][select { avg = mean(price) }, by symbol]"
                 "[order avg desc, head 10]"},
                {"update_group_filter",
                 "prices[update { lr = log(price / lag(price, 1)) }, by symbol]"
                 "[filter lr > 0.0][select { pos_days = count() }, by symbol]"},
                {"group_rank_filter",
                 "prices[update { rk = rank(price, method = dense, ascending = false) }, by symbol]"
                 "[filter rk <= 10][select { avg_top10 = mean(price) }, by symbol]"},
                {"normalize_by_group",
                 "prices[update { z = (price - mean(price)) / std(price) }, by symbol]"
                 "[update { clipped = pmin(pmax(z, -3.0), 3.0) }]"
                 "[select { mean_z = mean(clipped), sd_z = std(clipped) }, by symbol]"},
            };
            for (const auto& query : pipeline_queries) {
                if (verify) {
                    if (auto err = verify_benchmark(query, tables, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", query.name, *err);
                        return 1;
                    }
                }
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Transform benchmarks: core single-pass language features that had no
        // coverage. pmin_clip exercises the vectorised pmin path (winsorising a
        // column against a scalar); where_update_clip is a guarded update (the
        // ibex equivalent of SQL CASE WHEN — replaces matching rows, keeps
        // cardinality); rbind_two vertically concatenates two tables (RbindNode).
        if (status == 0 && run_suite("transform")) {
            fmt::print("\n-- Transform benchmarks ({} prices rows) --\n",
                       tables.at("prices").rows());
            std::vector<BenchQuery> transform_queries = {
                {"pmin_clip", "prices[update { clipped = pmin(price, 500.0) }]"},
                {"where_update_clip", "prices[where price > 900.0 update { price = 900.0 }]"},
                {"rbind_two", "rbind(prices, prices)"},
            };
            for (const auto& query : transform_queries) {
                ScanPaths sp;
                if (include_read) {
                    sp.emplace_back("prices", csv_path);
                }
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse, sp);
                if (status != 0) {
                    break;
                }
            }
        }

        // Vectorized RNG benchmarks: rand_uniform and rand_normal appended as a
        // new column. Measures the column-at-a-time PRNG throughput.
        if (status == 0 && run_suite("rng")) {
            fmt::print("\n-- RNG benchmarks ({} prices rows) --\n", tables.at("prices").rows());
            std::vector<BenchQuery> rng_queries = {
                {"rand_uniform", "prices[update { r = rand_uniform(0.0, 1.0) }]"},
                {"rand_normal", "prices[update { n = rand_normal(0.0, 1.0) }]"},
                {"rand_int", "prices[update { r = rand_int(1, 100) }]"},
                {"rand_bernoulli", "prices[update { r = rand_bernoulli(0.3) }]"},
            };
            for (const auto& query : rng_queries) {
                status = run_benchmark(query, tables, warmup_iters, iters, saved_include_parse);
                if (status != 0) {
                    break;
                }
            }
        }

        // Fill benchmarks: fill_null, fill_forward (LOCF), fill_backward (NOCB).
        // Uses an in-memory table with alternating valid / null doubles (~50% null).
        // Row count matches the prices table to keep wall-clock times comparable.
        if (status == 0 && run_suite("fill")) {
            std::size_t fill_rows = tables.at("prices").rows();
            ibex::runtime::Table fill_table;
            {
                ibex::Column<double> val_col;
                std::vector<bool> validity;
                val_col.reserve(fill_rows);
                validity.reserve(fill_rows);
                for (std::size_t i = 0; i < fill_rows; ++i) {
                    val_col.push_back(100.0 + static_cast<double>(i % 100));
                    validity.push_back(i % 2 == 0);  // even rows valid, odd rows null
                }
                fill_table.add_column("val", std::move(val_col), std::move(validity));
            }
            ibex::runtime::TableRegistry fill_tables;
            fill_tables.emplace("fill_data", std::move(fill_table));

            fmt::print("\n-- Fill benchmarks ({} rows, 50% nulls) --\n", fill_rows);
            std::vector<BenchQuery> fill_queries = {
                {"fill_null", "fill_data[update { v2 = fill_null(val, 0.0) }]"},
                {"fill_forward", "fill_data[update { v2 = fill_forward(val) }]"},
                {"fill_backward", "fill_data[update { v2 = fill_backward(val) }]"},
            };
            for (const auto& query : fill_queries) {
                status =
                    run_benchmark(query, fill_tables, warmup_iters, iters, saved_include_parse);
                if (status != 0) {
                    break;
                }
            }
        }

        // Null benchmarks: left join produces ~50% null right-column values.
        // lookup.csv has half the symbols (first 126 of 252); the other half
        // get null sector values. This exercises validity-bitmap tracking.
        if (status == 0 && run_suite("null") && !csv_lookup_path.empty()) {
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

            // Join benchmarks on the same lookup workload.
            // - left: exercises null bitmap tracking on right-side columns.
            // - semi/anti: membership-style joins returning left schema only.
            std::vector<BenchQuery> null_queries = {
                {"null_left_join", "prices left join lookup on symbol"},
                {"null_semi_join", "prices semi join lookup on symbol"},
                {"null_anti_join", "prices anti join lookup on symbol"},
            };

            for (const auto& q : null_queries) {
                if (verify) {
                    if (auto err = verify_benchmark(q, null_reg, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", q.name, *err);
                        return 1;
                    }
                }
                status = run_benchmark(q, null_reg, warmup_iters, iters, saved_include_parse);
                if (status != 0) {
                    break;
                }
            }

            if (status == 0) {
                // Cross join can explode in cardinality, so benchmark a bounded subset.
                constexpr std::size_t kCrossLeftRows = 2000;
                constexpr std::size_t kCrossRightRows = 64;
                ibex::runtime::TableRegistry cross_reg;
                cross_reg.emplace("prices_small",
                                  slice_table(null_reg.at("prices"), kCrossLeftRows));
                cross_reg.emplace("lookup_small",
                                  slice_table(null_reg.at("lookup"), kCrossRightRows));
                fmt::print("-- Cross join benchmark subset ({} x {} rows) --\n",
                           cross_reg.at("prices_small").rows(),
                           cross_reg.at("lookup_small").rows());

                BenchQuery cross_query{"null_cross_join_small",
                                       "prices_small cross join lookup_small"};
                if (verify) {
                    if (auto err = verify_benchmark(cross_query, cross_reg, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", cross_query.name, *err);
                        return 1;
                    }
                }
                status =
                    run_benchmark(cross_query, cross_reg, warmup_iters, iters, saved_include_parse);
            }
        }

        // Inner equi-join benchmarks. The null suite above covers left/semi/anti
        // against the small `lookup` dimension; this suite covers plain inner
        // joins, including a *high-cardinality* build side: events (4M rows over
        // 100K users) joined to the `users` dimension (100K rows). That builds a
        // 100K-entry hash table and probes it 4M times — a very different shape
        // from the ~126-key symbol join.
        if (status == 0 && run_suite("join")) {
            // Low-cardinality inner join: prices ⋈ lookup on symbol (~126 keys).
            if (!csv_lookup_path.empty()) {
                ibex::runtime::Table lookup_table;
                try {
                    lookup_table = read_csv(csv_lookup_path);
                } catch (const std::exception& e) {
                    fmt::print("error: failed to read lookup CSV: {}\n", e.what());
                    return 1;
                }
                ibex::runtime::TableRegistry join_reg;
                join_reg.emplace("prices", tables.at("prices"));
                join_reg.emplace("lookup", std::move(lookup_table));
                fmt::print("\n-- Inner join benchmark ({} prices x {} lookup rows) --\n",
                           join_reg.at("prices").rows(), join_reg.at("lookup").rows());
                BenchQuery q{"inner_join_symbol", "prices join lookup on symbol"};
                if (verify) {
                    if (auto err = verify_benchmark(q, join_reg, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", q.name, *err);
                        return 1;
                    }
                }
                status = run_benchmark(q, join_reg, warmup_iters, iters, saved_include_parse);
            }

            // High-cardinality inner join: events ⋈ users on user_id (~100K keys).
            if (status == 0 && !csv_events_path.empty() && !csv_users_path.empty()) {
                ibex::runtime::Table events_table;
                ibex::runtime::Table users_table;
                try {
                    events_table = read_csv(csv_events_path);
                    users_table = read_csv(csv_users_path);
                } catch (const std::exception& e) {
                    fmt::print("error: failed to read events/users CSV: {}\n", e.what());
                    return 1;
                }
                ibex::runtime::TableRegistry join_reg;
                join_reg.emplace("events", std::move(events_table));
                join_reg.emplace("users", std::move(users_table));
                fmt::print("\n-- Inner join benchmark ({} events x {} users rows) --\n",
                           join_reg.at("events").rows(), join_reg.at("users").rows());
                BenchQuery q{"inner_join_user", "events join users on user_id"};
                if (verify) {
                    if (auto err = verify_benchmark(q, join_reg, verify_rows)) {
                        fmt::print("error: verify failed for {}: {}\n", q.name, *err);
                        return 1;
                    }
                }
                status = run_benchmark(q, join_reg, warmup_iters, iters, saved_include_parse);
            }
        }
    }

    // Compound filter benchmarks: exercises the recursive FilterExpr evaluator.
    if (status == 0 && run_suite("filter") && !csv_trades_path.empty()) {
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
            ScanPaths sp;
            if (include_read) {
                sp.emplace_back("trades", csv_trades_path);
            }
            status =
                run_benchmark(query, trades_tables, warmup_iters, iters, saved_include_parse, sp);
            if (status != 0) {
                break;
            }
        }
    }

    // Filter micro benchmarks: split predicate evaluation, mask packing, index
    // materialization, and output gather so full filter timings are easier to
    // attribute. These intentionally use the same trades.csv column shape as the
    // filter suite but bypass the interpreter.
    if (status == 0 && run_suite("filter_micro") && !csv_trades_path.empty()) {
        ibex::runtime::Table trades_table;
        try {
            trades_table = read_csv(csv_trades_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read trades CSV: {}\n", e.what());
            return 1;
        }
        if (print_types) {
            print_table_types("trades", trades_table);
        }

        const auto* price_value = trades_table.find("price");
        const auto* qty_value = trades_table.find("qty");
        const auto* symbol_value = trades_table.find("symbol");
        const auto* price_col =
            price_value == nullptr ? nullptr : std::get_if<ibex::Column<double>>(price_value);
        const auto* qty_col =
            qty_value == nullptr ? nullptr : std::get_if<ibex::Column<std::int64_t>>(qty_value);
        if (price_col == nullptr || qty_col == nullptr || symbol_value == nullptr) {
            fmt::print(
                "error: filter_micro requires trades columns symbol, price:Double, qty:Int\n");
            return 1;
        }

        const std::size_t n = filter_micro_rows == 0
                                  ? trades_table.rows()
                                  : std::min<std::size_t>(filter_micro_rows, trades_table.rows());
        const double* price = price_col->data();
        const std::int64_t* qty = qty_col->data();
        std::vector<std::uint8_t> scratch_mask(n);

        auto digest_mask = [&](const std::vector<std::uint8_t>& mask,
                               std::size_t count) -> std::uint64_t {
            if (mask.empty()) {
                return 0;
            }
            return static_cast<std::uint64_t>(count) ^ (static_cast<std::uint64_t>(mask[0]) << 1) ^
                   (static_cast<std::uint64_t>(mask[mask.size() / 2]) << 2) ^
                   (static_cast<std::uint64_t>(mask[mask.size() - 1]) << 3);
        };

        auto fill_simple_mask = [&]() -> std::size_t {
            std::size_t count = 0;
            for (std::size_t i = 0; i < n; ++i) {
                const auto keep = static_cast<std::uint8_t>(price[i] > 500.0);
                scratch_mask[i] = keep;
                count += keep;
            }
            return count;
        };
        auto fill_and_mask = [&]() -> std::size_t {
            std::size_t count = 0;
            for (std::size_t i = 0; i < n; ++i) {
                const auto keep = static_cast<std::uint8_t>((price[i] > 500.0) & (qty[i] < 100));
                scratch_mask[i] = keep;
                count += keep;
            }
            return count;
        };
        auto fill_arith_mask = [&]() -> std::size_t {
            std::size_t count = 0;
            for (std::size_t i = 0; i < n; ++i) {
                const auto keep =
                    static_cast<std::uint8_t>((price[i] * static_cast<double>(qty[i])) > 50000.0);
                scratch_mask[i] = keep;
                count += keep;
            }
            return count;
        };
        auto fill_or_mask = [&]() -> std::size_t {
            std::size_t count = 0;
            for (std::size_t i = 0; i < n; ++i) {
                const auto keep = static_cast<std::uint8_t>((price[i] > 900.0) | (qty[i] < 10));
                scratch_mask[i] = keep;
                count += keep;
            }
            return count;
        };

        fmt::print("\n-- Filter micro benchmarks ({} rows) --\n", n);
        status = run_scalar_kernel_benchmark("filter_micro_mask_simple", n, warmup_iters, iters,
                                             [&]() -> std::uint64_t {
                                                 const auto count = fill_simple_mask();
                                                 return digest_mask(scratch_mask, count);
                                             });
        if (status == 0) {
            status = run_scalar_kernel_benchmark("filter_micro_mask_and", n, warmup_iters, iters,
                                                 [&]() -> std::uint64_t {
                                                     const auto count = fill_and_mask();
                                                     return digest_mask(scratch_mask, count);
                                                 });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark("filter_micro_mask_arith", n, warmup_iters, iters,
                                                 [&]() -> std::uint64_t {
                                                     const auto count = fill_arith_mask();
                                                     return digest_mask(scratch_mask, count);
                                                 });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark("filter_micro_mask_or", n, warmup_iters, iters,
                                                 [&]() -> std::uint64_t {
                                                     const auto count = fill_or_mask();
                                                     return digest_mask(scratch_mask, count);
                                                 });
        }

        std::vector<std::uint8_t> simple_mask(n);
        std::vector<std::uint8_t> arith_mask(n);
        for (std::size_t i = 0; i < n; ++i) {
            simple_mask[i] = static_cast<std::uint8_t>(price[i] > 500.0);
            arith_mask[i] =
                static_cast<std::uint8_t>((price[i] * static_cast<double>(qty[i])) > 50000.0);
        }
        std::vector<std::uint64_t> simple_words;
        std::vector<std::uint64_t> arith_words;
        const std::size_t simple_out = pack_filter_micro_mask(simple_mask, simple_words);
        const std::size_t arith_out = pack_filter_micro_mask(arith_mask, arith_words);
        const auto simple_indices = build_filter_micro_indices(simple_words, simple_out);
        const auto arith_indices = build_filter_micro_indices(arith_words, arith_out);

        std::vector<std::uint64_t> scratch_words;
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_pack_simple", n, warmup_iters, iters, [&]() -> std::uint64_t {
                    const auto count = pack_filter_micro_mask(simple_mask, scratch_words);
                    return static_cast<std::uint64_t>(count) ^
                           static_cast<std::uint64_t>(scratch_words.size());
                });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_pack_arith", n, warmup_iters, iters, [&]() -> std::uint64_t {
                    const auto count = pack_filter_micro_mask(arith_mask, scratch_words);
                    return static_cast<std::uint64_t>(count) ^
                           static_cast<std::uint64_t>(scratch_words.size());
                });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_index_simple", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    auto idx = build_filter_micro_indices(simple_words, simple_out);
                    return idx.empty() ? 0 : static_cast<std::uint64_t>(idx.front() ^ idx.back());
                });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_index_arith", arith_out, warmup_iters, iters, [&]() -> std::uint64_t {
                    auto idx = build_filter_micro_indices(arith_words, arith_out);
                    return idx.empty() ? 0 : static_cast<std::uint64_t>(idx.front() ^ idx.back());
                });
        }

        auto gather_price = [&](const std::vector<std::size_t>& indices) -> std::uint64_t {
            ibex::Column<double> out;
            out.resize(indices.size());
            auto* dst = out.data();
            for (std::size_t i = 0; i < indices.size(); ++i) {
                dst[i] = price[indices[i]];
            }
            return digest_primitive_column(out);
        };
        auto gather_qty = [&](const std::vector<std::size_t>& indices) -> std::uint64_t {
            ibex::Column<std::int64_t> out;
            out.resize(indices.size());
            auto* dst = out.data();
            for (std::size_t i = 0; i < indices.size(); ++i) {
                dst[i] = qty[indices[i]];
            }
            return digest_primitive_column(out);
        };
        auto gather_price_words = [&](const std::vector<std::uint64_t>& keep_words,
                                      std::size_t out_n) -> std::uint64_t {
            ibex::Column<double> out;
            out.resize(out_n);
            auto* dst = out.data();
            std::size_t j = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    dst[j++] = price[base + static_cast<std::size_t>(bit)];
                    bits &= bits - 1;
                }
            }
            return digest_primitive_column(out);
        };
        auto gather_qty_words = [&](const std::vector<std::uint64_t>& keep_words,
                                    std::size_t out_n) -> std::uint64_t {
            ibex::Column<std::int64_t> out;
            out.resize(out_n);
            auto* dst = out.data();
            std::size_t j = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    dst[j++] = qty[base + static_cast<std::size_t>(bit)];
                    bits &= bits - 1;
                }
            }
            return digest_primitive_column(out);
        };
        auto gather_symbol = [&](const std::vector<std::size_t>& indices) -> std::uint64_t {
            if (const auto* cat = std::get_if<ibex::Column<ibex::Categorical>>(symbol_value)) {
                ibex::Column<ibex::Categorical> out(cat->dictionary_ptr(), cat->index_ptr());
                out.resize(indices.size());
                const auto* src = cat->codes_data();
                auto* dst = out.codes_data();
                for (std::size_t i = 0; i < indices.size(); ++i) {
                    dst[i] = src[indices[i]];
                }
                if (out.size() == 0) {
                    return std::uint64_t{0};
                }
                return static_cast<std::uint64_t>(out.code_at(0)) ^
                       (static_cast<std::uint64_t>(out.code_at(out.size() / 2)) << 1) ^
                       (static_cast<std::uint64_t>(out.code_at(out.size() - 1)) << 2) ^
                       static_cast<std::uint64_t>(out.size());
            }
            const auto* str = std::get_if<ibex::Column<std::string>>(symbol_value);
            if (str == nullptr) {
                return std::uint64_t{0};
            }
            const auto* src_off = str->offsets_data();
            std::size_t total_chars = 0;
            for (const auto si : indices) {
                total_chars += src_off[si + 1] - src_off[si];
            }
            ibex::Column<std::string> out;
            out.resize_for_gather(indices.size(), total_chars);
            auto* dst_off = out.offsets_data();
            auto* dst_chars = out.chars_data();
            const auto* src_chars = str->chars_data();
            dst_off[0] = 0;
            std::uint32_t cur = 0;
            for (std::size_t i = 0; i < indices.size(); ++i) {
                const auto si = indices[i];
                const auto len = src_off[si + 1] - src_off[si];
                std::memcpy(dst_chars + cur, src_chars + src_off[si], len);
                cur += len;
                dst_off[i + 1] = cur;
            }
            return static_cast<std::uint64_t>(out.size()) ^
                   (static_cast<std::uint64_t>(total_chars) << 1);
        };
        auto gather_symbol_words = [&](const std::vector<std::uint64_t>& keep_words,
                                       std::size_t out_n) -> std::uint64_t {
            if (const auto* cat = std::get_if<ibex::Column<ibex::Categorical>>(symbol_value)) {
                ibex::Column<ibex::Categorical> out(cat->dictionary_ptr(), cat->index_ptr());
                out.resize(out_n);
                const auto* src = cat->codes_data();
                auto* dst = out.codes_data();
                std::size_t j = 0;
                for (std::size_t w = 0; w < keep_words.size(); ++w) {
                    std::uint64_t bits = keep_words[w];
                    const std::size_t base = w * 64;
                    while (bits != 0) {
                        const int bit = std::countr_zero(bits);
                        dst[j++] = src[base + static_cast<std::size_t>(bit)];
                        bits &= bits - 1;
                    }
                }
                if (out.size() == 0) {
                    return std::uint64_t{0};
                }
                return static_cast<std::uint64_t>(out.code_at(0)) ^
                       (static_cast<std::uint64_t>(out.code_at(out.size() / 2)) << 1) ^
                       (static_cast<std::uint64_t>(out.code_at(out.size() - 1)) << 2) ^
                       static_cast<std::uint64_t>(out.size());
            }
            const auto* str = std::get_if<ibex::Column<std::string>>(symbol_value);
            if (str == nullptr) {
                return std::uint64_t{0};
            }
            const auto* src_off = str->offsets_data();
            std::size_t total_chars = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    const std::size_t si = base + static_cast<std::size_t>(bit);
                    total_chars += src_off[si + 1] - src_off[si];
                    bits &= bits - 1;
                }
            }
            ibex::Column<std::string> out;
            out.resize_for_gather(out_n, total_chars);
            auto* dst_off = out.offsets_data();
            auto* dst_chars = out.chars_data();
            const auto* src_chars = str->chars_data();
            dst_off[0] = 0;
            std::uint32_t cur = 0;
            std::size_t j = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    const std::size_t si = base + static_cast<std::size_t>(bit);
                    const auto len = src_off[si + 1] - src_off[si];
                    std::memcpy(dst_chars + cur, src_chars + src_off[si], len);
                    cur += len;
                    dst_off[++j] = cur;
                    bits &= bits - 1;
                }
            }
            return static_cast<std::uint64_t>(out.size()) ^
                   (static_cast<std::uint64_t>(total_chars) << 1);
        };
        auto gather_all = [&](const std::vector<std::size_t>& indices) -> std::uint64_t {
            return gather_symbol(indices) ^ (gather_price(indices) << 1) ^
                   (gather_qty(indices) << 2);
        };
        auto gather_all_words = [&](const std::vector<std::uint64_t>& keep_words,
                                    std::size_t out_n) -> std::uint64_t {
            return gather_symbol_words(keep_words, out_n) ^
                   (gather_price_words(keep_words, out_n) << 1) ^
                   (gather_qty_words(keep_words, out_n) << 2);
        };
        auto gather_all_combined_words = [&](const std::vector<std::uint64_t>& keep_words,
                                             std::size_t out_n) -> std::uint64_t {
            ibex::Column<double> price_out;
            ibex::Column<std::int64_t> qty_out;
            price_out.resize(out_n);
            qty_out.resize(out_n);
            auto* price_dst = price_out.data();
            auto* qty_dst = qty_out.data();

            if (const auto* cat = std::get_if<ibex::Column<ibex::Categorical>>(symbol_value)) {
                ibex::Column<ibex::Categorical> symbol_out(cat->dictionary_ptr(), cat->index_ptr());
                symbol_out.resize(out_n);
                const auto* symbol_src = cat->codes_data();
                auto* symbol_dst = symbol_out.codes_data();
                std::size_t j = 0;
                for (std::size_t w = 0; w < keep_words.size(); ++w) {
                    std::uint64_t bits = keep_words[w];
                    const std::size_t base = w * 64;
                    while (bits != 0) {
                        const int bit = std::countr_zero(bits);
                        const std::size_t si = base + static_cast<std::size_t>(bit);
                        symbol_dst[j] = symbol_src[si];
                        price_dst[j] = price[si];
                        qty_dst[j] = qty[si];
                        ++j;
                        bits &= bits - 1;
                    }
                }
                const auto symbol_digest =
                    symbol_out.size() == 0
                        ? std::uint64_t{0}
                        : static_cast<std::uint64_t>(symbol_out.code_at(0)) ^
                              (static_cast<std::uint64_t>(symbol_out.code_at(symbol_out.size() / 2))
                               << 1) ^
                              (static_cast<std::uint64_t>(symbol_out.code_at(symbol_out.size() - 1))
                               << 2) ^
                              static_cast<std::uint64_t>(symbol_out.size());
                return symbol_digest ^ (digest_primitive_column(price_out) << 1) ^
                       (digest_primitive_column(qty_out) << 2);
            }

            const auto* str = std::get_if<ibex::Column<std::string>>(symbol_value);
            if (str == nullptr) {
                return (digest_primitive_column(price_out) << 1) ^
                       (digest_primitive_column(qty_out) << 2);
            }
            const auto* src_off = str->offsets_data();
            std::size_t total_chars = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    const std::size_t si = base + static_cast<std::size_t>(bit);
                    total_chars += src_off[si + 1] - src_off[si];
                    bits &= bits - 1;
                }
            }
            ibex::Column<std::string> symbol_out;
            symbol_out.resize_for_gather(out_n, total_chars);
            auto* dst_off = symbol_out.offsets_data();
            auto* dst_chars = symbol_out.chars_data();
            const auto* src_chars = str->chars_data();
            dst_off[0] = 0;
            std::uint32_t cur = 0;
            std::size_t j = 0;
            for (std::size_t w = 0; w < keep_words.size(); ++w) {
                std::uint64_t bits = keep_words[w];
                const std::size_t base = w * 64;
                while (bits != 0) {
                    const int bit = std::countr_zero(bits);
                    const std::size_t si = base + static_cast<std::size_t>(bit);
                    const auto len = src_off[si + 1] - src_off[si];
                    std::memcpy(dst_chars + cur, src_chars + src_off[si], len);
                    cur += len;
                    dst_off[++j] = cur;
                    price_dst[j - 1] = price[si];
                    qty_dst[j - 1] = qty[si];
                    bits &= bits - 1;
                }
            }
            return (static_cast<std::uint64_t>(symbol_out.size()) ^
                    (static_cast<std::uint64_t>(total_chars) << 1)) ^
                   (digest_primitive_column(price_out) << 1) ^
                   (digest_primitive_column(qty_out) << 2);
        };

        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_price_simple", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_price(simple_indices); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_qty_simple", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_qty(simple_indices); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_symbol_simple", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_symbol(simple_indices); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_simple", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_all(simple_indices); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_simple_words", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_all_words(simple_words, simple_out); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_simple_combined_words", simple_out, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    return gather_all_combined_words(simple_words, simple_out);
                });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_arith", arith_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_all(arith_indices); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_arith_words", arith_out, warmup_iters, iters,
                [&]() -> std::uint64_t { return gather_all_words(arith_words, arith_out); });
        }
        if (status == 0) {
            status = run_scalar_kernel_benchmark(
                "filter_micro_gather_all_arith_combined_words", arith_out, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    return gather_all_combined_words(arith_words, arith_out);
                });
        }
    }

    // Statistics benchmarks: the correlation matrix (CorrNode) over all numeric
    // columns. Uses trades.csv (price:Double, qty:Int) for a 2x2 matrix; the
    // cost is dominated by the two-pass mean/variance/covariance scan.
    if (status == 0 && run_suite("stats") && !csv_trades_path.empty()) {
        ibex::runtime::Table trades_table;
        try {
            trades_table = read_csv(csv_trades_path);
        } catch (const std::exception& e) {
            fmt::print("error: failed to read trades CSV: {}\n", e.what());
            return 1;
        }
        ibex::runtime::TableRegistry trades_tables;
        trades_tables.emplace("trades", std::move(trades_table));
        fmt::print("\n-- Statistics benchmarks ({} trades rows) --\n",
                   trades_tables.at("trades").rows());
        BenchQuery q{"corr_price_vol", "trades[corr]"};
        ScanPaths sp;
        if (include_read) {
            sp.emplace_back("trades", csv_trades_path);
        }
        status = run_benchmark(q, trades_tables, warmup_iters, iters, saved_include_parse, sp);
    }

    // Multi-column group-by: exercises the compound-key fallback path (std::unordered_map<Key>).
    if (status == 0 && run_suite("multi") && !csv_multi_path.empty()) {
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
            ScanPaths sp;
            if (include_read) {
                sp.emplace_back("prices_multi", csv_multi_path);
            }
            status =
                run_benchmark(query, multi_tables, warmup_iters, iters, saved_include_parse, sp);
            if (status != 0) {
                break;
            }
        }
    }

    // High-cardinality group-by + string-gather filter.
    if (status == 0 && run_suite("events") && !csv_events_path.empty()) {
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

    // Reshape benchmarks: melt (wide→long) and dcast (long→wide).
    // Uses a synthetic wide OHLC table with configurable row count to exercise
    // reshape paths at scale. reshape_rows == 0 means "skip" — the scale suite
    // passes 0 above its memory cap so reshape stays blank for *every* engine at
    // the largest sizes (the in-memory wide table OOMs the box), ibex included.
    if (status == 0 && run_suite("reshape") && reshape_rows > 0) {
        constexpr std::size_t reshape_n_days = 400;
        const std::size_t reshape_n_symbols = (reshape_rows + reshape_n_days - 1) / reshape_n_days;

        ibex::Column<std::string> sym_col;
        ibex::Column<std::int64_t> day_col;
        ibex::Column<double> open_col, high_col, low_col, close_col;
        sym_col.reserve(reshape_rows);
        day_col.reserve(reshape_rows);
        open_col.reserve(reshape_rows);
        high_col.reserve(reshape_rows);
        low_col.reserve(reshape_rows);
        close_col.reserve(reshape_rows);

        for (std::size_t i = 0; i < reshape_rows; ++i) {
            const std::size_t sym_idx = i / reshape_n_days;
            const std::size_t day_idx = i % reshape_n_days;
            sym_col.push_back(fmt::format("S{:04d}", sym_idx));
            day_col.push_back(static_cast<std::int64_t>(day_idx + 1));
            const double base = 100.0 + static_cast<double>(i % 1000);
            open_col.push_back(base);
            high_col.push_back(base + 1.0);
            low_col.push_back(base - 1.0);
            close_col.push_back(base + 0.5);
        }

        ibex::runtime::Table wide_table;
        wide_table.add_column("symbol", std::move(sym_col));
        wide_table.add_column("day", std::move(day_col));
        wide_table.add_column("open", std::move(open_col));
        wide_table.add_column("high", std::move(high_col));
        wide_table.add_column("low", std::move(low_col));
        wide_table.add_column("close", std::move(close_col));

        fmt::print(
            "\n-- Reshape benchmarks ({} wide rows, 4 measure cols; {} symbols, {} days) --\n",
            reshape_rows, reshape_n_symbols, reshape_n_days);

        ibex::runtime::TableRegistry reshape_tables;
        reshape_tables.emplace("wide", std::move(wide_table));

        // melt: wide→long (id={symbol,day}, measures=open,high,low,close)
        BenchQuery melt_query{
            "melt_wide_to_long",
            "wide[melt {symbol, day}]",
        };
        status =
            run_benchmark(melt_query, reshape_tables, warmup_iters, iters, saved_include_parse);

        // Build the long table for dcast benchmark
        if (status == 0) {
            ibex::runtime::ScalarRegistry melt_scalars;
            auto melt_src = normalize_input("wide[melt {symbol, day}]");
            auto melt_parsed = ibex::parser::parse(melt_src);
            auto melt_lowered = ibex::parser::lower(*melt_parsed);
            auto long_result =
                ibex::runtime::interpret(*melt_lowered.value(), reshape_tables, &melt_scalars);
            if (!long_result) {
                fmt::print("error: failed to build long table for dcast: {}\n",
                           long_result.error());
                return 1;
            }
            ibex::runtime::Table long_table = std::move(*long_result);

            ibex::runtime::TableRegistry dcast_tables;
            dcast_tables.emplace("long", long_table);

            // dcast: long→wide (pivot=variable, value=value, by={symbol,day})
            BenchQuery dcast_query{
                "dcast_long_to_wide",
                "long[dcast variable, select value, by {symbol, day}]",
            };
            status =
                run_benchmark(dcast_query, dcast_tables, warmup_iters, iters, saved_include_parse);

            if (status == 0) {
                const auto* sym_entry = long_table.find_entry("symbol");
                const auto* day_entry = long_table.find_entry("day");
                const auto* value_entry = long_table.find_entry("value");
                if (sym_entry == nullptr || day_entry == nullptr || value_entry == nullptr) {
                    fmt::print("error: long table is missing expected columns for typed dcast\n");
                    return 1;
                }

                const std::size_t long_rows = long_table.rows();

                auto make_typed_long_table =
                    [&](ibex::runtime::ColumnValue pivot_column,
                        const std::string& pivot_name) -> ibex::runtime::Table {
                    ibex::runtime::Table typed;
                    typed.columns.reserve(4);
                    typed.columns.push_back(*sym_entry);
                    typed.index.emplace("symbol", 0);
                    typed.columns.push_back(*day_entry);
                    typed.index.emplace("day", 1);
                    typed.columns.push_back(*value_entry);
                    typed.index.emplace("value", 2);
                    typed.add_column(pivot_name, std::move(pivot_column));
                    return typed;
                };

                ibex::Column<std::int64_t> pivot_int_col;
                pivot_int_col.reserve(long_rows);
                for (std::size_t i = 0; i < long_rows; ++i) {
                    pivot_int_col.push_back(static_cast<std::int64_t>(i % 4));
                }
                ibex::runtime::TableRegistry dcast_int_tables;
                dcast_int_tables.emplace(
                    "long_int", make_typed_long_table(std::move(pivot_int_col), "pivot_id"));
                BenchQuery dcast_int_query{
                    "dcast_long_to_wide_int_pivot",
                    "long_int[dcast pivot_id, select value, by {symbol, day}]",
                };
                status = run_benchmark(dcast_int_query, dcast_int_tables, warmup_iters, iters,
                                       saved_include_parse);
                if (status != 0) {
                    return status;
                }

                using CatCol = ibex::Column<ibex::Categorical>;
                auto dict = std::make_shared<std::vector<std::string>>();
                dict->reserve(4);
                dict->push_back("open");
                dict->push_back("high");
                dict->push_back("low");
                dict->push_back("close");
                auto index = std::make_shared<CatCol::index_map>();
                index->reserve(dict->size());
                for (std::size_t i = 0; i < dict->size(); ++i) {
                    index->emplace(dict->at(i), static_cast<CatCol::code_type>(i));
                }
                std::vector<CatCol::code_type> codes;
                codes.reserve(long_rows);
                for (std::size_t i = 0; i < long_rows; ++i) {
                    codes.push_back(static_cast<CatCol::code_type>(i % 4));
                }
                CatCol pivot_cat_col(dict, index, std::move(codes));
                ibex::runtime::TableRegistry dcast_cat_tables;
                dcast_cat_tables.emplace(
                    "long_cat", make_typed_long_table(std::move(pivot_cat_col), "pivot_cat"));
                BenchQuery dcast_cat_query{
                    "dcast_long_to_wide_cat_pivot",
                    "long_cat[dcast pivot_cat, select value, by {symbol, day}]",
                };
                status = run_benchmark(dcast_cat_query, dcast_cat_tables, warmup_iters, iters,
                                       saved_include_parse);
            }
        }
    }

    // merge_validity micro benchmark: isolates validity bitmap merge cost in
    // arithmetic expression evaluation on nullable columns.
    if (status == 0 && run_suite("merge_validity")) {
        ibex::runtime::Table merge_table;
        {
            ibex::Column<double> a_col;
            ibex::Column<double> b_col;
            ibex::Column<double> c_col;
            ibex::Column<double> d_col;
            ibex::runtime::ValidityBitmap a_valid(merge_validity_rows, true);
            ibex::runtime::ValidityBitmap b_valid(merge_validity_rows, true);
            ibex::runtime::ValidityBitmap c_valid(merge_validity_rows, true);
            ibex::runtime::ValidityBitmap d_valid(merge_validity_rows, true);

            a_col.reserve(merge_validity_rows);
            b_col.reserve(merge_validity_rows);
            c_col.reserve(merge_validity_rows);
            d_col.reserve(merge_validity_rows);
            for (std::size_t i = 0; i < merge_validity_rows; ++i) {
                a_col.push_back(static_cast<double>(i % 251));
                b_col.push_back(static_cast<double>((i * 3) % 997));
                c_col.push_back(static_cast<double>((i * 7) % 1009));
                d_col.push_back(static_cast<double>((i * 11) % 4099));

                a_valid.set(i, (i % 2) == 0);
                b_valid.set(i, (i % 3) != 0);
                c_valid.set(i, (i % 5) != 0);
                d_valid.set(i, (i % 7) != 0);
            }

            merge_table.add_column("a", std::move(a_col), std::move(a_valid));
            merge_table.add_column("b", std::move(b_col), std::move(b_valid));
            merge_table.add_column("c", std::move(c_col), std::move(c_valid));
            merge_table.add_column("d", std::move(d_col), std::move(d_valid));
        }

        ibex::runtime::TableRegistry merge_tables;
        merge_tables.emplace("merge_data", std::move(merge_table));
        if (print_types) {
            print_table_types("merge_data", merge_tables.find("merge_data")->second);
        }

        const auto* merge_data = &merge_tables.find("merge_data")->second;
        const auto* a_entry = merge_data->find_entry("a");
        const auto* b_entry = merge_data->find_entry("b");
        const auto* c_entry = merge_data->find_entry("c");
        const auto* d_entry = merge_data->find_entry("d");
        if (a_entry == nullptr || b_entry == nullptr || c_entry == nullptr || d_entry == nullptr ||
            !a_entry->validity.has_value() || !b_entry->validity.has_value() ||
            !c_entry->validity.has_value() || !d_entry->validity.has_value()) {
            fmt::print("error: merge_validity suite requires nullable columns a,b,c,d\n");
            return 1;
        }
        const auto* a_valid = &*a_entry->validity;
        const auto* b_valid = &*b_entry->validity;
        const auto* c_valid = &*c_entry->validity;
        const auto* d_valid = &*d_entry->validity;

        fmt::print("\n-- merge_validity function micro benchmarks ({} rows) --\n",
                   merge_validity_rows);
        status = run_bitmap_kernel_benchmark(
            "merge_validity_fn_pair", merge_validity_rows, warmup_iters, iters, [&]() {
                return ibex::runtime::merge_validity_bitmaps(a_valid, b_valid, merge_validity_rows);
            });
        if (status == 0) {
            status = run_bitmap_kernel_benchmark(
                "merge_validity_fn_chain8", merge_validity_rows, warmup_iters, iters, [&]() {
                    auto ab = ibex::runtime::merge_validity_bitmaps(a_valid, b_valid,
                                                                    merge_validity_rows);
                    if (!ab.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    auto cd = ibex::runtime::merge_validity_bitmaps(c_valid, d_valid,
                                                                    merge_validity_rows);
                    if (!cd.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    auto ac = ibex::runtime::merge_validity_bitmaps(a_valid, c_valid,
                                                                    merge_validity_rows);
                    if (!ac.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    auto bd = ibex::runtime::merge_validity_bitmaps(b_valid, d_valid,
                                                                    merge_validity_rows);
                    if (!bd.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    auto lhs =
                        ibex::runtime::merge_validity_bitmaps(&*ab, &*cd, merge_validity_rows);
                    if (!lhs.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    auto rhs =
                        ibex::runtime::merge_validity_bitmaps(&*ac, &*bd, merge_validity_rows);
                    if (!rhs.has_value())
                        return std::optional<ibex::runtime::ValidityBitmap>{};
                    return ibex::runtime::merge_validity_bitmaps(&*lhs, &*rhs, merge_validity_rows);
                });
        }
        if (status != 0) {
            return status;
        }

        fmt::print("\n-- merge_validity expression micro benchmarks ({} rows) --\n",
                   merge_validity_rows);
        std::vector<BenchQuery> merge_queries = {
            {"merge_validity_pair", "merge_data[update { out = a + b }]"},
            {"merge_validity_chain8",
             "merge_data[update { out = ((a + b) + (c + d)) + ((a + c) + (b + d)) }]"},
        };
        for (const auto& query : merge_queries) {
            status = run_benchmark(query, merge_tables, warmup_iters, iters, saved_include_parse);
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
    if (status == 0 && run_suite("timeframe") && timeframe_rows > 0) {
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
            {"tf_rolling_median_1m",
             R"(as_timeframe(tf_data, "ts")[window 1m, update { med = rolling_median(price) }])"},
            {"tf_rolling_std_1m",
             R"(as_timeframe(tf_data, "ts")[window 1m, update { s = rolling_std(price) }])"},
            {"tf_rolling_ewma_1m",
             R"(as_timeframe(tf_data, "ts")[window 1m, update { e = rolling_ewma(price, 0.1) }])"},
            {"tf_resample_1m_ohlc",
             R"(as_timeframe(tf_data, "ts")[resample 1m, select { open = first(price), high = max(price), low = min(price), close = last(price) }])"},
        };

        for (const auto& query : tf_queries) {
            status = run_benchmark(query, tf_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }

        // As-of join: for each trade (~10% sampled, jittered timestamps), find
        // the most-recent quote at or before the trade time. Cross-engine
        // mirrors live in bench_python / bench_duckdb / bench_r tf_asof_join.
        {
            ibex::Column<ibex::Timestamp> q_ts;
            ibex::Column<double> q_bid;
            q_ts.reserve(timeframe_rows);
            q_bid.reserve(timeframe_rows);
            for (std::size_t i = 0; i < timeframe_rows; ++i) {
                q_ts.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL});
                q_bid.push_back(99.0 + static_cast<double>(i % 100) * 0.01);
            }
            ibex::runtime::Table quotes_table;
            quotes_table.add_column("ts", std::move(q_ts));
            quotes_table.add_column("bid", std::move(q_bid));

            // Trade indices: deterministic 10% reservoir-style sample.
            std::vector<std::size_t> trade_idx;
            trade_idx.reserve(timeframe_rows / 10);
            std::mt19937_64 rng{42};
            for (std::size_t i = 0; i < timeframe_rows; ++i) {
                if ((rng() % 10ULL) == 0ULL) {
                    trade_idx.push_back(i);
                }
            }
            std::sort(trade_idx.begin(), trade_idx.end());

            ibex::Column<ibex::Timestamp> t_ts;
            ibex::Column<std::int64_t> t_qty;
            t_ts.reserve(trade_idx.size());
            t_qty.reserve(trade_idx.size());
            for (auto i : trade_idx) {
                const auto jitter_ms = static_cast<std::int64_t>(rng() % 1000ULL);
                t_ts.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL +
                                               jitter_ms * 1'000'000LL});
                t_qty.push_back(static_cast<std::int64_t>(rng() % 99ULL) + 1);
            }
            ibex::runtime::Table trades_table;
            trades_table.add_column("ts", std::move(t_ts));
            trades_table.add_column("qty", std::move(t_qty));

            ibex::runtime::TableRegistry asof_tables;
            asof_tables.emplace("quotes_tf", std::move(quotes_table));
            asof_tables.emplace("trades_tf", std::move(trades_table));

            BenchQuery asof_query{
                "tf_asof_join",
                R"(as_timeframe(trades_tf, "ts") asof join as_timeframe(quotes_tf, "ts") on ts)"};
            status =
                run_benchmark(asof_query, asof_tables, warmup_iters, iters, saved_include_parse);
        }

        // As-of join with an extra equality key (symbol): exercises the grouped
        // asof path (one per-symbol merge), versus the time-only join above which
        // takes the single-merge fast path. ~100 symbols partition both sides;
        // each trade matches the latest same-symbol quote at or before its time.
        // Cross-engine mirrors live in bench_python / bench_duckdb / bench_r.
        if (status == 0) {
            constexpr std::size_t kAsofSymbols = 100;
            std::vector<std::string> sym_names;
            sym_names.reserve(kAsofSymbols);
            for (std::size_t s = 0; s < kAsofSymbols; ++s) {
                sym_names.push_back("SYM" + std::to_string(s));
            }

            ibex::Column<ibex::Timestamp> q_ts;
            ibex::Column<std::string> q_sym;
            ibex::Column<double> q_bid;
            q_ts.reserve(timeframe_rows);
            q_bid.reserve(timeframe_rows);
            for (std::size_t i = 0; i < timeframe_rows; ++i) {
                q_ts.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL});
                q_sym.push_back(sym_names[i % kAsofSymbols]);
                q_bid.push_back(99.0 + static_cast<double>(i % 100) * 0.01);
            }
            ibex::runtime::Table quotes_table;
            quotes_table.add_column("ts", std::move(q_ts));
            quotes_table.add_column("symbol", std::move(q_sym));
            quotes_table.add_column("bid", std::move(q_bid));

            // Trades: deterministic 10% sample, carrying the symbol of the quote
            // index they derive from so every trade has same-symbol candidates.
            std::vector<std::size_t> trade_idx;
            trade_idx.reserve(timeframe_rows / 10);
            std::mt19937_64 rng{42};
            for (std::size_t i = 0; i < timeframe_rows; ++i) {
                if ((rng() % 10ULL) == 0ULL) {
                    trade_idx.push_back(i);
                }
            }
            std::sort(trade_idx.begin(), trade_idx.end());

            ibex::Column<ibex::Timestamp> t_ts;
            ibex::Column<std::string> t_sym;
            ibex::Column<std::int64_t> t_qty;
            t_ts.reserve(trade_idx.size());
            t_qty.reserve(trade_idx.size());
            for (auto i : trade_idx) {
                const auto jitter_ms = static_cast<std::int64_t>(rng() % 1000ULL);
                t_ts.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL +
                                               jitter_ms * 1'000'000LL});
                t_sym.push_back(sym_names[i % kAsofSymbols]);
                t_qty.push_back(static_cast<std::int64_t>(rng() % 99ULL) + 1);
            }
            ibex::runtime::Table trades_table;
            trades_table.add_column("ts", std::move(t_ts));
            trades_table.add_column("symbol", std::move(t_sym));
            trades_table.add_column("qty", std::move(t_qty));

            ibex::runtime::TableRegistry asof_tables;
            asof_tables.emplace("quotes_tf", std::move(quotes_table));
            asof_tables.emplace("trades_tf", std::move(trades_table));

            BenchQuery asof_by_symbol{
                "tf_asof_join_by_symbol",
                R"(as_timeframe(trades_tf, "ts") asof join as_timeframe(quotes_tf, "ts") on {ts, symbol})"};
            status = run_benchmark(asof_by_symbol, asof_tables, warmup_iters, iters,
                                   saved_include_parse);
        }
    }

    // Bool-column benchmarks: isolate projection/gather/sort/group patterns on
    // a large in-memory bool column so storage changes can be measured directly.
    if (status == 0 && run_suite("bool")) {
        ibex::runtime::Table bool_table;
        {
            ibex::Column<std::int64_t> id_col;
            ibex::Column<double> value_col;
            ibex::Column<bool> flag_col;
            ibex::Column<bool> alt_flag_col;

            id_col.reserve(bool_rows);
            value_col.reserve(bool_rows);
            flag_col.reserve(bool_rows);
            alt_flag_col.reserve(bool_rows);

            for (std::size_t i = 0; i < bool_rows; ++i) {
                id_col.push_back(static_cast<std::int64_t>(i));
                value_col.push_back(static_cast<double>((i * 17) % 1000));
                flag_col.push_back((i & 1U) == 0U);
                alt_flag_col.push_back((i % 3U) == 0U);
            }

            bool_table.add_column("id", std::move(id_col));
            bool_table.add_column("value", std::move(value_col));
            bool_table.add_column("flag", std::move(flag_col));
            bool_table.add_column("alt_flag", std::move(alt_flag_col));
        }

        ibex::runtime::TableRegistry bool_tables;
        bool_tables.emplace("bool_data", std::move(bool_table));
        if (print_types) {
            print_table_types("bool_data", bool_tables.find("bool_data")->second);
        }

        fmt::print("\n-- Bool-column benchmarks ({} rows) --\n", bool_rows);
        std::vector<BenchQuery> bool_queries = {
            {"bool_project", "bool_data[select { id, flag, alt_flag }]"},
            {"bool_filter_project",
             "bool_data[filter value > 500.0, select { id, flag, alt_flag }]"},
            {"bool_order", "bool_data[order { flag asc, id asc }]"},
            {"bool_update_copy", "bool_data[update { flag_copy = flag }]"},
        };
        for (const auto& query : bool_queries) {
            status = run_benchmark(query, bool_tables, warmup_iters, iters, saved_include_parse);
            if (status != 0) {
                break;
            }
        }
    }

    // RNG kernel micro benchmark: isolates scalar-vs-x4 generation cost for
    // distributions where x4 paths are plausible.
    if (status == 0 && run_suite("rng_micro")) {
        constexpr std::uint64_t kSeed = 0x9e3779b97f4a7c15ULL;
        constexpr double kLambda = 1.7;
        constexpr double kBernoulliP = 0.3;
        constexpr std::int64_t kIntLo = 1;
        constexpr std::int64_t kIntHi = 100;
        const double inv_lambda = 1.0 / kLambda;

        ibex::Column<double> out_double;
        out_double.resize(rng_micro_rows);
        ibex::Column<std::int64_t> out_int;
        out_int.resize(rng_micro_rows);

        auto digest_double = [&](const ibex::Column<double>& col) -> std::uint64_t {
            const std::size_t mid = col.size() / 2;
            const auto a = static_cast<std::uint64_t>(col[0] * 1.0e12);
            const auto b = static_cast<std::uint64_t>(col[mid] * 1.0e12);
            const auto c = static_cast<std::uint64_t>(col[col.size() - 1] * 1.0e12);
            return a ^ (b << 1) ^ (c << 2);
        };
        auto digest_int = [&](const ibex::Column<std::int64_t>& col) -> std::uint64_t {
            const std::size_t mid = col.size() / 2;
            return static_cast<std::uint64_t>(col[0]) ^
                   (static_cast<std::uint64_t>(col[mid]) << 1) ^
                   (static_cast<std::uint64_t>(col[col.size() - 1]) << 2);
        };

        fmt::print("\n-- RNG kernel micro benchmarks ({} rows) --\n", rng_micro_rows);

        // Scalar baseline: single-stream xoshiro256++ with manual loop.
        ibex::runtime::reseed(kSeed);
        status = run_scalar_kernel_benchmark(
            "rng_uniform_scalar_fn", rng_micro_rows, warmup_iters, iters, [&]() -> std::uint64_t {
                auto& rng = ibex::runtime::get_rng();
                for (std::size_t i = 0; i < rng_micro_rows; ++i) {
                    out_double[i] = ibex::runtime::bits_to_01(rng());
                }
                return digest_double(out_double);
            });
        if (status == 0) {
            // zorro::Rng — auto-dispatched SIMD (AVX-512 / AVX2 / portable).
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_uniform_zorro_fn", rng_micro_rows, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    ibex::runtime::fill_uniform(out_double.data(), rng_micro_rows, 0.0, 1.0);
                    return digest_double(out_double);
                });
        }
        if (status == 0) {
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_exponential_scalar_fn", rng_micro_rows, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    auto& rng = ibex::runtime::get_rng();
                    for (std::size_t i = 0; i < rng_micro_rows; ++i) {
                        const double u = ibex::runtime::bits_to_01(rng()) + 1e-300;
                        out_double[i] = -std::log(u) * inv_lambda;
                    }
                    return digest_double(out_double);
                });
        }
        if (status == 0) {
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_exponential_zorro_fn", rng_micro_rows, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    ibex::runtime::fill_exponential(out_double.data(), rng_micro_rows, kLambda);
                    return digest_double(out_double);
                });
        }
        if (status == 0) {
            // Bernoulli scalar: integer threshold comparison.
            constexpr double kScale53 = 9007199254740992.0;  // 2^53
            const auto kBernoulliThreshold = static_cast<std::uint64_t>(kBernoulliP * kScale53);
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_bernoulli_scalar_fn", rng_micro_rows, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    auto& rng = ibex::runtime::get_rng();
                    for (std::size_t i = 0; i < rng_micro_rows; ++i) {
                        out_int[i] = ((rng() >> 11) < kBernoulliThreshold) ? 1 : 0;
                    }
                    return digest_int(out_int);
                });
        }
        if (status == 0) {
            // Bernoulli x4: production code path via fill_bernoulli.
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_bernoulli_zorro_fn", rng_micro_rows, warmup_iters, iters,
                [&]() -> std::uint64_t {
                    ibex::runtime::fill_bernoulli(out_int.data(), rng_micro_rows, kBernoulliP);
                    return digest_int(out_int);
                });
        }
        if (status == 0) {
            // Int scalar: integer multiply-shift to map a raw u64 into [lo, hi].
            const auto kIntSpan = static_cast<std::uint64_t>(kIntHi - kIntLo + 1);
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_int_scalar_fn", rng_micro_rows, warmup_iters, iters, [&]() -> std::uint64_t {
                    auto& rng = ibex::runtime::get_rng();
                    for (std::size_t i = 0; i < rng_micro_rows; ++i) {
                        out_int[i] =
                            kIntLo + static_cast<std::int64_t>((rng() >> 11) * kIntSpan >> 53);
                    }
                    return digest_int(out_int);
                });
        }
        if (status == 0) {
            // Int x4: production code path — fill_int uses __uint128_t multiply-shift.
            const auto kIntSpan = static_cast<std::uint64_t>(kIntHi - kIntLo + 1);
            ibex::runtime::reseed(kSeed);
            status = run_scalar_kernel_benchmark(
                "rng_int_zorro_fn", rng_micro_rows, warmup_iters, iters, [&]() -> std::uint64_t {
                    ibex::runtime::fill_int(out_int.data(), rng_micro_rows, kIntLo, kIntSpan);
                    return digest_int(out_int);
                });
        }
    }

    return status;
}
