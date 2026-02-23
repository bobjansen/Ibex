#pragma once
// Ibex CSV library — RFC 4180 compliant CSV reading via rapidcsv.
//
// Usage in .ibex:
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv");

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
#include <vector>

namespace {

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

inline auto read_csv(std::string_view path) -> ibex::runtime::Table {
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

        // Try int64
        bool all_int = !vals.empty();
        for (const auto& v : vals) {
            std::int64_t iv{};
            if (!csv_try_int(v, iv)) {
                all_int = false;
                break;
            }
        }
        if (all_int) {
            ibex::Column<std::int64_t> col;
            col.reserve(vals.size());
            for (const auto& v : vals) {
                std::int64_t iv{};
                csv_try_int(v, iv);
                col.push_back(iv);
            }
            table.add_column(name, std::move(col));
            continue;
        }

        // Try double
        bool all_double = !vals.empty();
        for (const auto& v : vals) {
            double dv{};
            if (!csv_try_double(v, dv)) {
                all_double = false;
                break;
            }
        }
        if (all_double) {
            ibex::Column<double> col;
            col.reserve(vals.size());
            for (const auto& v : vals) {
                double dv{};
                csv_try_double(v, dv);
                col.push_back(dv);
            }
            table.add_column(name, std::move(col));
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
