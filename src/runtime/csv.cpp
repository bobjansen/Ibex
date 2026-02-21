#include <ibex/runtime/csv.hpp>

#include <charconv>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace ibex::runtime {

namespace {

auto split_line(const std::string& line) -> std::vector<std::string> {
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

auto try_parse_int(const std::string& text, std::int64_t& out) -> bool {
    const char* begin = text.data();
    const char* end = text.data() + text.size();
    auto result = std::from_chars(begin, end, out);
    return result.ec == std::errc() && result.ptr == end;
}

auto try_parse_double(const std::string& text, double& out) -> bool {
    char* end = nullptr;
    out = std::strtod(text.c_str(), &end);
    return end != text.c_str() && *end == '\0';
}

}  // namespace

auto read_csv_simple(std::string_view path) -> std::expected<Table, std::string> {
    std::ifstream input{std::string(path)};
    if (!input) {
        return std::unexpected("failed to open csv: " + std::string(path));
    }

    std::string header_line;
    if (!std::getline(input, header_line)) {
        return std::unexpected("csv is empty");
    }

    auto headers = split_line(header_line);
    if (headers.empty()) {
        return std::unexpected("csv has no headers");
    }

    std::vector<std::vector<std::string>> columns(headers.size());
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        auto fields = split_line(line);
        if (fields.size() != headers.size()) {
            return std::unexpected("csv row has wrong number of columns");
        }
        for (std::size_t i = 0; i < fields.size(); ++i) {
            columns[i].push_back(fields[i]);
        }
    }

    Table table;
    for (std::size_t i = 0; i < headers.size(); ++i) {
        bool all_int = true;
        bool all_double = true;
        std::vector<std::int64_t> ints;
        std::vector<double> doubles;
        ints.reserve(columns[i].size());
        doubles.reserve(columns[i].size());
        for (const auto& value : columns[i]) {
            std::int64_t int_value = 0;
            double double_value = 0.0;
            if (try_parse_int(value, int_value)) {
                ints.push_back(int_value);
                doubles.push_back(static_cast<double>(int_value));
                continue;
            }
            all_int = false;
            if (try_parse_double(value, double_value)) {
                doubles.push_back(double_value);
                continue;
            }
            all_double = false;
            break;
        }
        if (all_int) {
            table.add_column(headers[i], Column<std::int64_t>{std::move(ints)});
        } else if (all_double) {
            table.add_column(headers[i], Column<double>{std::move(doubles)});
        } else {
            Column<std::string> strings;
            strings.reserve(columns[i].size());
            for (auto& value : columns[i]) {
                strings.push_back(std::move(value));
            }
            table.add_column(headers[i], std::move(strings));
        }
    }

    return table;
}

}  // namespace ibex::runtime
