#pragma once
// Ibex CSV library — RFC 4180 compliant CSV reading via rapidcsv.
//
// Usage in .ibex:
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv");
// Optional null controls:
//   extern fn read_csv(path: String, nulls: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv", "<empty>,NA");

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <rapidcsv.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

struct CsvReadOptions {
    bool null_if_empty = false;
    std::unordered_set<std::string> null_tokens;
};

inline auto csv_trim(std::string_view text) -> std::string_view {
    auto begin = text.find_first_not_of(" \t\r\n");
    if (begin == std::string_view::npos) {
        return {};
    }
    auto end = text.find_last_not_of(" \t\r\n");
    return text.substr(begin, end - begin + 1);
}

inline auto csv_parse_null_spec(std::string_view spec) -> CsvReadOptions {
    CsvReadOptions options;
    std::size_t pos = 0;
    while (pos <= spec.size()) {
        std::size_t comma = spec.find(',', pos);
        if (comma == std::string_view::npos) {
            comma = spec.size();
        }
        auto token = csv_trim(spec.substr(pos, comma - pos));
        if (!token.empty()) {
            if (token == "<empty>") {
                options.null_if_empty = true;
            } else {
                options.null_tokens.emplace(token);
            }
        }
        if (comma == spec.size()) {
            break;
        }
        pos = comma + 1;
    }
    return options;
}

inline auto csv_try_int(const std::string& text, std::int64_t& out) -> bool {
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    auto result = std::from_chars(begin, end, out);
    return result.ec == std::errc() && result.ptr == end;
}

inline auto csv_try_double(const std::string& text, double& out) -> bool {
    char* end_ptr = nullptr;
    out = std::strtod(text.c_str(), &end_ptr);
    return end_ptr != text.c_str() && *end_ptr == '\0';
}

}  // namespace

inline auto read_csv_with_options(std::string_view path, const CsvReadOptions& options)
    -> ibex::runtime::Table {
    rapidcsv::Document doc(std::string(path),
                           rapidcsv::LabelParams(0, -1),   // row 0 = header, no row-index column
                           rapidcsv::SeparatorParams(',')  // handles RFC 4180 quoting
    );

    auto col_names = doc.GetColumnNames();
    ibex::runtime::Table table;
    constexpr std::size_t kMaxCategoricalUniques = 4096;
    constexpr double kMaxCategoricalRatio = 0.05;

    for (const auto& name : col_names) {
        std::vector<std::string> vals = doc.GetColumn<std::string>(name);
        std::vector<bool> validity(vals.size(), true);
        bool has_nulls = false;
        for (std::size_t i = 0; i < vals.size(); ++i) {
            const bool is_null =
                (options.null_if_empty && vals[i].empty()) || options.null_tokens.contains(vals[i]);
            validity[i] = !is_null;
            has_nulls = has_nulls || is_null;
        }

        // Try int64
        bool all_int = !vals.empty();
        bool any_valid = false;
        for (const auto& v : vals) {
            if (options.null_if_empty && v.empty()) {
                continue;
            }
            if (options.null_tokens.contains(v)) {
                continue;
            }
            std::int64_t iv{};
            if (!csv_try_int(v, iv)) {
                all_int = false;
                break;
            }
            any_valid = true;
        }
        if (all_int && any_valid) {
            ibex::Column<std::int64_t> col;
            col.reserve(vals.size());
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (!validity[i]) {
                    col.push_back(0);
                    continue;
                }
                std::int64_t iv{};
                csv_try_int(vals[i], iv);
                col.push_back(iv);
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
            continue;
        }

        // Try double
        bool all_double = !vals.empty();
        any_valid = false;
        for (const auto& v : vals) {
            if (options.null_if_empty && v.empty()) {
                continue;
            }
            if (options.null_tokens.contains(v)) {
                continue;
            }
            double dv{};
            if (!csv_try_double(v, dv)) {
                all_double = false;
                break;
            }
            any_valid = true;
        }
        if (all_double && any_valid) {
            ibex::Column<double> col;
            col.reserve(vals.size());
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (!validity[i]) {
                    col.push_back(0.0);
                    continue;
                }
                double dv{};
                csv_try_double(vals[i], dv);
                col.push_back(dv);
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
            continue;
        }

        // String fallback. When nulls are present, keep plain string + validity bitmap.
        if (has_nulls) {
            ibex::Column<std::string> col;
            col.reserve(vals.size());
            for (std::size_t i = 0; i < vals.size(); ++i) {
                if (!validity[i]) {
                    col.push_back("");
                } else {
                    col.push_back(vals[i]);
                }
            }
            table.add_column(name, std::move(col), std::move(validity));
            continue;
        }

        // String fallback (with categorical detection)
        const std::size_t n = vals.size();
        if (n > 0) {
            const std::size_t ratio_limit = std::max<std::size_t>(
                1, static_cast<std::size_t>(static_cast<double>(n) * kMaxCategoricalRatio));
            const std::size_t max_uniques = std::min(kMaxCategoricalUniques, ratio_limit);
            std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
            codes.reserve(n);
            std::vector<std::string> dict;
            dict.reserve(std::min(n, max_uniques));
            std::unordered_map<std::string_view, ibex::Column<ibex::Categorical>::code_type> index;
            index.reserve(std::min(n, max_uniques));
            bool ok = true;
            for (const auto& v : vals) {
                std::string_view key{v};
                auto it = index.find(key);
                if (it != index.end()) {
                    codes.push_back(it->second);
                    continue;
                }
                if (index.size() + 1 > max_uniques) {
                    ok = false;
                    break;
                }
                auto code = static_cast<ibex::Column<ibex::Categorical>::code_type>(dict.size());
                dict.push_back(v);
                index.emplace(dict.back(), code);
                codes.push_back(code);
            }
            if (ok) {
                table.add_column(
                    name, ibex::Column<ibex::Categorical>(std::move(dict), std::move(codes)));
                continue;
            }
        }
        table.add_column(name, ibex::Column<std::string>(std::move(vals)));
    }

    return table;
}

inline auto read_csv(std::string_view path) -> ibex::runtime::Table {
    return read_csv_with_options(path, CsvReadOptions{});
}

inline auto read_csv(std::string_view path, std::string_view null_spec) -> ibex::runtime::Table {
    return read_csv_with_options(path, csv_parse_null_spec(null_spec));
}
