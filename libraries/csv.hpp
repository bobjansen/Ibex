#pragma once
// Ibex CSV library — provides read_csv() for use in Ibex scripts.
//
// Usage in .ibex:
//   extern fn read_csv(path: String) -> DataFrame from "csv.hpp";
//   let df = read_csv("data/myfile.csv");
//
// Compile with: -I$(IBEX_ROOT)/libraries

#include <ibex/runtime/interpreter.hpp>
#include <ibex/core/column.hpp>

#include <charconv>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

inline auto csv_split_line(const std::string& line) -> std::vector<std::string> {
    std::vector<std::string> fields;
    std::string field;
    std::stringstream ss(line);
    while (std::getline(ss, field, ',')) {
        fields.push_back(field);
    }
    if (!line.empty() && line.back() == ',') {
        fields.emplace_back();
    }
    return fields;
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

inline auto read_csv(std::string_view path) -> ibex::runtime::Table {
    std::ifstream input{std::string(path)};
    if (!input) {
        throw std::runtime_error("read_csv: failed to open: " + std::string(path));
    }

    std::string header_line;
    if (!std::getline(input, header_line)) {
        throw std::runtime_error("read_csv: file is empty: " + std::string(path));
    }

    auto headers = csv_split_line(header_line);
    if (headers.empty()) {
        throw std::runtime_error("read_csv: no headers in: " + std::string(path));
    }

    std::vector<std::vector<std::string>> raw(headers.size());
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) continue;
        auto fields = csv_split_line(line);
        if (fields.size() != headers.size()) {
            throw std::runtime_error("read_csv: row column count mismatch in: " + std::string(path));
        }
        for (std::size_t i = 0; i < fields.size(); ++i) {
            raw[i].push_back(std::move(fields[i]));
        }
    }

    ibex::runtime::Table table;
    for (std::size_t i = 0; i < headers.size(); ++i) {
        bool all_int = true;
        bool all_double = true;
        std::vector<std::int64_t> ints;
        std::vector<double> doubles;
        ints.reserve(raw[i].size());
        doubles.reserve(raw[i].size());

        for (const auto& value : raw[i]) {
            std::int64_t iv = 0;
            double dv = 0.0;
            if (csv_try_int(value, iv)) {
                ints.push_back(iv);
                doubles.push_back(static_cast<double>(iv));
                continue;
            }
            all_int = false;
            if (csv_try_double(value, dv)) {
                doubles.push_back(dv);
                continue;
            }
            all_double = false;
            break;
        }

        if (all_int) {
            table.add_column(headers[i], ibex::Column<std::int64_t>{std::move(ints)});
        } else if (all_double) {
            table.add_column(headers[i], ibex::Column<double>{std::move(doubles)});
        } else {
            ibex::Column<std::string> strings;
            strings.reserve(raw[i].size());
            for (auto& value : raw[i]) {
                strings.push_back(std::move(value));
            }
            table.add_column(headers[i], std::move(strings));
        }
    }

    return table;
}
