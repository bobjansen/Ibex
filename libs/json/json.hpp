#pragma once
// Ibex JSON library — row-oriented JSON reading and writing via nlohmann/json.
//
// Reading:
//   extern fn read_json(path: String) -> DataFrame from "json.hpp";
//   let df = read_json("data/myfile.json");
//
// The input file must contain either:
//   1. A JSON array of objects (row-oriented):
//        [{"a":1,"b":"x"}, {"a":2,"b":"y"}]
//   2. A JSON-Lines file (one JSON object per line, .jsonl):
//        {"a":1,"b":"x"}
//        {"a":2,"b":"y"}
//
// Writing:
//   extern fn write_json(df: DataFrame, path: String) -> Int from "json.hpp";
//   let rows = write_json(df, "data/out.json");
//
// Output is a JSON array of objects with proper type preservation.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace {

using json = nlohmann::json;

/// Determine the best Ibex column type for a JSON value.
enum class JsonColType { Unknown, Int, Double, Bool, String };

inline auto json_classify(const json& val) -> JsonColType {
    if (val.is_number_integer())
        return JsonColType::Int;
    if (val.is_number_float())
        return JsonColType::Double;
    if (val.is_boolean())
        return JsonColType::Bool;
    if (val.is_string())
        return JsonColType::String;
    return JsonColType::Unknown;
}

/// Widen the column type when values disagree.
inline auto json_widen(JsonColType current, JsonColType incoming) -> JsonColType {
    if (current == JsonColType::Unknown)
        return incoming;
    if (current == incoming)
        return current;
    // Int + Double -> Double
    if ((current == JsonColType::Int && incoming == JsonColType::Double) ||
        (current == JsonColType::Double && incoming == JsonColType::Int)) {
        return JsonColType::Double;
    }
    // Everything else falls back to String
    return JsonColType::String;
}

/// Parse JSON from a file, supporting both array-of-objects and JSON-Lines.
inline auto json_parse_file(std::string_view path) -> std::vector<json> {
    std::string path_str{path};
    std::error_code ec;
    const bool exists = std::filesystem::exists(path_str, ec);
    if (ec) {
        throw std::runtime_error("read_json: failed to inspect path '" + path_str +
                                 "': " + ec.message());
    }
    if (!exists) {
        throw std::runtime_error("read_json: file not found: '" + path_str + "'");
    }

    std::ifstream ifs{path_str};
    if (!ifs) {
        throw std::runtime_error("read_json: failed to open '" + path_str + "'");
    }

    // Try to parse as a single JSON value first (array or object).
    json parsed;
    try {
        parsed = json::parse(ifs);
    } catch (const json::parse_error&) {
        // Fall back to JSON-Lines: rewind and parse line by line.
        ifs.clear();
        ifs.seekg(0);
        std::vector<json> rows;
        std::string line;
        while (std::getline(ifs, line)) {
            // Skip empty lines
            if (line.empty() || line.find_first_not_of(" \t\r\n") == std::string::npos) {
                continue;
            }
            rows.push_back(json::parse(line));
        }
        return rows;
    }

    if (parsed.is_array()) {
        std::vector<json> rows;
        rows.reserve(parsed.size());
        for (auto& item : parsed) {
            rows.push_back(std::move(item));
        }
        return rows;
    }
    if (parsed.is_object()) {
        // Single object → one-row DataFrame
        return {std::move(parsed)};
    }
    throw std::runtime_error("read_json: expected array of objects or JSON-Lines");
}

}  // namespace

/// Read a JSON file into a Table.
///
/// Type inference per column:
///   - All integer → Int64
///   - Mixed integer/float → Double
///   - All boolean → Bool
///   - All string → String (with categorical compression for low cardinality)
///   - Mixed types → String
///   - Missing/null values → null bitmap
inline auto read_json(std::string_view path) -> ibex::runtime::Table {
    auto rows = json_parse_file(path);
    if (rows.empty()) {
        return ibex::runtime::Table{};
    }

    // First pass: collect column names in order and determine types.
    std::vector<std::string> col_names;
    std::unordered_map<std::string, std::size_t> col_index;
    std::vector<JsonColType> col_types;

    for (const auto& row : rows) {
        if (!row.is_object()) {
            throw std::runtime_error("read_json: each row must be a JSON object");
        }
        for (auto it = row.begin(); it != row.end(); ++it) {
            auto idx_it = col_index.find(it.key());
            if (idx_it == col_index.end()) {
                col_index[it.key()] = col_names.size();
                col_names.push_back(it.key());
                col_types.push_back(JsonColType::Unknown);
                idx_it = col_index.find(it.key());
            }
            if (!it.value().is_null()) {
                col_types[idx_it->second] =
                    json_widen(col_types[idx_it->second], json_classify(it.value()));
            }
        }
    }

    // Default unknown columns to String.
    for (auto& t : col_types) {
        if (t == JsonColType::Unknown)
            t = JsonColType::String;
    }

    const std::size_t n_rows = rows.size();
    const std::size_t n_cols = col_names.size();

    // Second pass: build columns.
    ibex::runtime::Table table;

    constexpr std::size_t kMaxCategoricalUniques = 4096;
    constexpr double kMaxCategoricalRatio = 0.05;

    for (std::size_t c = 0; c < n_cols; ++c) {
        const auto& name = col_names[c];
        const auto type = col_types[c];
        std::vector<bool> validity(n_rows, true);
        bool has_nulls = false;

        // Check for nulls / missing keys.
        for (std::size_t r = 0; r < n_rows; ++r) {
            auto it = rows[r].find(name);
            if (it == rows[r].end() || it->is_null()) {
                validity[r] = false;
                has_nulls = true;
            }
        }

        if (type == JsonColType::Int) {
            ibex::Column<std::int64_t> col;
            col.reserve(n_rows);
            for (std::size_t r = 0; r < n_rows; ++r) {
                if (!validity[r]) {
                    col.push_back(0);
                    continue;
                }
                col.push_back(rows[r][name].get<std::int64_t>());
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
        } else if (type == JsonColType::Double) {
            ibex::Column<double> col;
            col.reserve(n_rows);
            for (std::size_t r = 0; r < n_rows; ++r) {
                if (!validity[r]) {
                    col.push_back(0.0);
                    continue;
                }
                col.push_back(rows[r][name].get<double>());
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
        } else if (type == JsonColType::Bool) {
            ibex::Column<bool> col;
            col.reserve(n_rows);
            for (std::size_t r = 0; r < n_rows; ++r) {
                if (!validity[r]) {
                    col.push_back(false);
                    continue;
                }
                col.push_back(rows[r][name].get<bool>());
            }
            if (has_nulls) {
                table.add_column(name, std::move(col), std::move(validity));
            } else {
                table.add_column(name, std::move(col));
            }
        } else {
            // String type — convert all values to string representation.
            std::vector<std::string> vals;
            vals.reserve(n_rows);
            for (std::size_t r = 0; r < n_rows; ++r) {
                if (!validity[r]) {
                    vals.emplace_back("");
                    continue;
                }
                const auto& val = rows[r][name];
                if (val.is_string()) {
                    vals.push_back(val.get<std::string>());
                } else {
                    // Non-string values in a string column: serialize.
                    vals.push_back(val.dump());
                }
            }

            if (has_nulls) {
                ibex::Column<std::string> col(vals);
                table.add_column(name, std::move(col), std::move(validity));
                continue;
            }

            // Try categorical compression.
            const std::size_t ratio_limit = std::max<std::size_t>(
                1, static_cast<std::size_t>(static_cast<double>(n_rows) * kMaxCategoricalRatio));
            const std::size_t max_uniques = std::min(kMaxCategoricalUniques, ratio_limit);
            std::vector<ibex::Column<ibex::Categorical>::code_type> codes;
            codes.reserve(n_rows);
            std::vector<std::string> dict;
            dict.reserve(std::min(n_rows, max_uniques));
            std::unordered_map<std::string_view, ibex::Column<ibex::Categorical>::code_type> idx;
            idx.reserve(std::min(n_rows, max_uniques));
            bool ok = true;
            for (const auto& v : vals) {
                std::string_view key{v};
                auto it = idx.find(key);
                if (it != idx.end()) {
                    codes.push_back(it->second);
                    continue;
                }
                if (idx.size() + 1 > max_uniques) {
                    ok = false;
                    break;
                }
                auto code = static_cast<ibex::Column<ibex::Categorical>::code_type>(dict.size());
                dict.push_back(v);
                idx.emplace(dict.back(), code);
                codes.push_back(code);
            }
            if (ok) {
                table.add_column(
                    name, ibex::Column<ibex::Categorical>(std::move(dict), std::move(codes)));
            } else {
                table.add_column(name, ibex::Column<std::string>(vals));
            }
        }
    }

    return table;
}

namespace {

/// Write one cell of a column at row `r` as a JSON value.
inline auto json_cell_value(const ibex::runtime::ColumnEntry& entry, std::size_t r) -> json {
    if (ibex::runtime::is_null(entry, r)) {
        return json(nullptr);
    }
    return std::visit(
        [&](const auto& col) -> json {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, ibex::Column<std::int64_t>>) {
                return json(col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<double>>) {
                return json(col[r]);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<std::string>>) {
                return json(std::string(col[r]));
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Categorical>>) {
                return json(std::string(col[r]));
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Date>>) {
                return json(col[r].days);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<ibex::Timestamp>>) {
                return json(col[r].nanos);
            } else if constexpr (std::is_same_v<ColT, ibex::Column<bool>>) {
                return json(col[r]);
            } else {
                return json(nullptr);
            }
        },
        *entry.column);
}

}  // namespace

/// Write `table` to a JSON file at `path`.
///
/// Output format is a JSON array of objects. Null values are represented
/// as JSON null. Returns the number of rows written.
inline auto write_json(const ibex::runtime::Table& table, std::string_view path) -> std::int64_t {
    std::string path_str{path};
    std::ofstream ofs{path_str};
    if (!ofs) {
        throw std::runtime_error("write_json: cannot open for writing: " + path_str);
    }

    const auto& cols = table.columns;
    const std::size_t n_cols = cols.size();
    const std::size_t n_rows = table.rows();

    json arr = json::array();
    arr.get_ref<json::array_t&>().reserve(n_rows);

    for (std::size_t r = 0; r < n_rows; ++r) {
        json obj = json::object();
        for (std::size_t c = 0; c < n_cols; ++c) {
            obj[cols[c].name] = json_cell_value(cols[c], r);
        }
        arr.push_back(std::move(obj));
    }

    ofs << arr.dump(2) << '\n';
    ofs.flush();
    if (!ofs) {
        throw std::runtime_error("write_json: I/O error writing: " + path_str);
    }

    return static_cast<std::int64_t>(n_rows);
}
