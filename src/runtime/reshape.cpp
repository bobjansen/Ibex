#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <robin_hood.h>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "reshape_internal.hpp"

namespace ibex::runtime {

namespace {

auto is_simple_identifier(std::string_view name) -> bool {
    if (name.empty()) {
        return false;
    }
    auto is_alpha = [](unsigned char ch) -> bool {
        return std::isalpha(ch) != 0;
    };
    auto is_alnum = [](unsigned char ch) -> bool {
        return std::isalnum(ch) != 0;
    };
    unsigned char first = static_cast<unsigned char>(name.front());
    if (!is_alpha(first) && first != '_') {
        return false;
    }
    for (std::size_t i = 1; i < name.size(); ++i) {
        unsigned char ch = static_cast<unsigned char>(name[i]);
        if (!is_alnum(ch) && ch != '_') {
            return false;
        }
    }
    return true;
}

auto format_columns(const Table& table) -> std::string {
    if (table.columns.empty()) {
        return "<none>";
    }
    std::string out;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        const auto& name = table.columns[i].name;
        if (is_simple_identifier(name)) {
            out.append(name);
        } else {
            out.push_back('`');
            out.append(name);
            out.push_back('`');
        }
    }
    return out;
}

[[noreturn]] void invariant_violation(std::string_view detail) {
    (void)std::fputs("ibex internal invariant violated (runtime/reshape): ", stderr);
    (void)std::fwrite(detail.data(), sizeof(char), detail.size(), stderr);
    (void)std::fputc('\n', stderr);
    std::abort();
}

auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                invariant_violation("append_value: source/destination column type mismatch");
            }
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                dst_col.push_code(src_col->code_at(index));
            } else {
                dst_col.push_back((*src_col)[index]);
            }
        },
        out);
}

auto make_empty_like(const ColumnValue& src) -> ColumnValue {
    return std::visit(
        [](const auto& col) -> ColumnValue {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                return Column<Categorical>{col.dictionary_ptr(), col.index_ptr(), {}};
            }
            return ColType{};
        },
        src);
}

struct StringViewHash {
    using is_transparent = void;
    auto operator()(std::string_view sv) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(sv);
    }
};

struct StringViewEq {
    using is_transparent = void;
    auto operator()(std::string_view a, std::string_view b) const noexcept -> bool {
        return a == b;
    }
};

auto extract_numeric(const Table& input)
    -> std::pair<std::vector<std::string>, std::vector<std::vector<double>>> {
    std::vector<std::string> names;
    std::vector<std::vector<double>> data;
    const std::size_t rows = input.rows();
    for (const auto& entry : input.columns) {
        std::vector<double> col;
        col.reserve(rows);
        bool ok = std::visit(
            [&](const auto& c) -> bool {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, Column<double>>) {
                    for (std::size_t i = 0; i < rows; ++i) {
                        col.push_back(c[i]);
                    }
                    return true;
                } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                    for (std::size_t i = 0; i < rows; ++i) {
                        col.push_back(static_cast<double>(c[i]));
                    }
                    return true;
                } else {
                    return false;
                }
            },
            *entry.column);
        if (ok) {
            names.push_back(entry.name);
            data.push_back(std::move(col));
        }
    }
    return {std::move(names), std::move(data)};
}

struct DcastKey {
    static constexpr std::size_t kMaxCols = 8;
    std::array<std::int64_t, kMaxCols> v{};
    std::uint8_t n{0};

    auto operator==(const DcastKey& o) const noexcept -> bool {
        return std::memcmp(v.data(), o.v.data(), n * sizeof(std::int64_t)) == 0;
    }
};

struct DcastKeyHash {
    auto operator()(const DcastKey& k) const noexcept -> std::size_t {
        std::uint64_t h = 0;
        for (std::size_t i = 0; i < k.n; ++i) {
            h ^= static_cast<std::uint64_t>(k.v[i]);
            h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
            h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
            h ^= h >> 31;
        }
        return static_cast<std::size_t>(h);
    }
};

}  // namespace

auto cov_table(const Table& input) -> std::expected<Table, std::string> {
    auto [names, data] = extract_numeric(input);
    const std::size_t n = names.size();
    const std::size_t rows = data.empty() ? 0 : data[0].size();

    if (n == 0) {
        return std::unexpected("cov: no numeric columns found");
    }
    if (rows < 2) {
        return std::unexpected("cov: need at least 2 rows to compute covariance");
    }

    std::vector<double> mean(n, 0.0);
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t i = 0; i < rows; ++i) {
            mean[j] += data[j][i];
        }
        mean[j] /= static_cast<double>(rows);
    }

    const double denom = static_cast<double>(rows - 1);
    std::vector<std::vector<double>> cov(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = a; b < n; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < rows; ++i) {
                s += (data[a][i] - mean[a]) * (data[b][i] - mean[b]);
            }
            cov[a][b] = cov[b][a] = s / denom;
        }
    }

    Table out;
    Column<std::string> label_col;
    for (const auto& nm : names) {
        label_col.push_back(nm);
    }
    out.add_column("column", std::move(label_col));
    for (std::size_t b = 0; b < n; ++b) {
        Column<double> col_data(std::vector<double>(cov[b].begin(), cov[b].end()));
        out.add_column(names[b], std::move(col_data));
    }
    return out;
}

auto corr_table(const Table& input) -> std::expected<Table, std::string> {
    auto [names, data] = extract_numeric(input);
    const std::size_t n = names.size();
    const std::size_t rows = data.empty() ? 0 : data[0].size();

    if (n == 0) {
        return std::unexpected("corr: no numeric columns found");
    }
    if (rows < 2) {
        return std::unexpected("corr: need at least 2 rows to compute correlation");
    }

    std::vector<double> mean(n, 0.0);
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t i = 0; i < rows; ++i) {
            mean[j] += data[j][i];
        }
        mean[j] /= static_cast<double>(rows);
    }

    const double denom = static_cast<double>(rows - 1);
    std::vector<std::vector<double>> cov(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = a; b < n; ++b) {
            double s = 0.0;
            for (std::size_t i = 0; i < rows; ++i) {
                s += (data[a][i] - mean[a]) * (data[b][i] - mean[b]);
            }
            cov[a][b] = cov[b][a] = s / denom;
        }
    }

    std::vector<std::vector<double>> corr_mat(n, std::vector<double>(n, 0.0));
    for (std::size_t a = 0; a < n; ++a) {
        for (std::size_t b = 0; b < n; ++b) {
            const double sigma_a = std::sqrt(cov[a][a]);
            const double sigma_b = std::sqrt(cov[b][b]);
            if (sigma_a == 0.0 || sigma_b == 0.0) {
                corr_mat[a][b] = (a == b) ? 1.0 : 0.0;
            } else {
                corr_mat[a][b] = cov[a][b] / (sigma_a * sigma_b);
            }
        }
    }

    Table out;
    Column<std::string> label_col;
    for (const auto& nm : names) {
        label_col.push_back(nm);
    }
    out.add_column("column", std::move(label_col));
    for (std::size_t b = 0; b < n; ++b) {
        Column<double> col_data(std::vector<double>(corr_mat[b].begin(), corr_mat[b].end()));
        out.add_column(names[b], std::move(col_data));
    }
    return out;
}

auto transpose_table(const Table& input) -> std::expected<Table, std::string> {
    if (input.columns.empty()) {
        return std::unexpected("transpose: input table has no columns");
    }

    int label_idx = -1;
    for (int i = 0; i < static_cast<int>(input.columns.size()); ++i) {
        const auto& cv = *input.columns[static_cast<std::size_t>(i)].column;
        if (std::holds_alternative<Column<std::string>>(cv) ||
            std::holds_alternative<Column<Categorical>>(cv)) {
            if (label_idx == -1) {
                label_idx = i;
                break;
            }
        }
    }

    std::vector<std::size_t> data_idxs;
    for (std::size_t i = 0; i < input.columns.size(); ++i) {
        if (static_cast<int>(i) != label_idx) {
            data_idxs.push_back(i);
        }
    }

    if (data_idxs.empty()) {
        return std::unexpected("transpose: no data columns to transpose");
    }

    std::size_t first_type = input.columns[data_idxs[0]].column->index();
    for (std::size_t idx : data_idxs) {
        if (input.columns[idx].column->index() != first_type) {
            return std::unexpected(
                "transpose: all data columns must have the same type (found mixed types)");
        }
    }

    const std::size_t n_data_cols = data_idxs.size();
    const std::size_t n_rows = input.rows();

    std::vector<std::string> out_col_names;
    out_col_names.reserve(n_rows);
    if (label_idx >= 0) {
        const auto& label_entry = input.columns[static_cast<std::size_t>(label_idx)];
        if (const auto* sc = std::get_if<Column<std::string>>(&*label_entry.column)) {
            for (std::size_t i = 0; i < n_rows; ++i) {
                out_col_names.push_back(std::string((*sc)[i]));
            }
        } else if (const auto* cc = std::get_if<Column<Categorical>>(&*label_entry.column)) {
            for (std::size_t i = 0; i < n_rows; ++i) {
                out_col_names.push_back(std::string((*cc)[i]));
            }
        }
    } else {
        for (std::size_t i = 0; i < n_rows; ++i) {
            out_col_names.push_back("r" + std::to_string(i));
        }
    }

    {
        std::unordered_set<std::string> seen;
        for (const auto& name : out_col_names) {
            if (!seen.insert(name).second) {
                return std::unexpected("transpose: duplicate label value '" + name +
                                       "' — output column names must be unique");
            }
        }
    }

    Table out;
    Column<std::string> row_labels;
    for (std::size_t i : data_idxs) {
        row_labels.push_back(input.columns[i].name);
    }
    out.add_column("column", std::move(row_labels));

    std::visit(
        [&](const auto& first_col_ref) {
            using ColT = std::decay_t<decltype(first_col_ref)>;
            for (std::size_t r = 0; r < n_rows; ++r) {
                if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                              std::is_same_v<ColT, Column<Categorical>>) {
                    Column<std::string> out_col;
                    out_col.reserve(n_data_cols);
                    for (std::size_t ci : data_idxs) {
                        const auto& src = std::get<ColT>(*input.columns[ci].column);
                        out_col.push_back(src[r]);
                    }
                    out.add_column(out_col_names[r], std::move(out_col));
                } else {
                    using ElemT = typename ColT::value_type;
                    Column<ElemT> out_col;
                    out_col.reserve(n_data_cols);
                    for (std::size_t ci : data_idxs) {
                        const auto& src = std::get<ColT>(*input.columns[ci].column);
                        out_col.push_back(src[r]);
                    }
                    out.add_column(out_col_names[r], std::move(out_col));
                }
            }
        },
        *input.columns[data_idxs[0]].column);

    return out;
}

auto matmul_table(const Table& left, const Table& right) -> std::expected<Table, std::string> {
    auto [left_names, left_data] = extract_numeric(left);
    auto [right_names, right_data] = extract_numeric(right);

    const std::size_t m = left_data.empty() ? 0 : left_data[0].size();
    const std::size_t k = left_names.size();
    const std::size_t n = right_names.size();

    if (k == 0 || n == 0) {
        return std::unexpected("matmul: no numeric columns found in one or both operands");
    }
    if (right_data.empty() || right_data[0].size() != k) {
        return std::unexpected("matmul: inner dimensions do not match — left has " +
                               std::to_string(k) + " numeric columns but right has " +
                               std::to_string(right_data[0].size()) + " rows");
    }

    std::vector<std::vector<double>> result(n, std::vector<double>(m, 0.0));
    for (std::size_t j = 0; j < n; ++j) {
        for (std::size_t p = 0; p < k; ++p) {
            const double bpj = right_data[j][p];
            for (std::size_t i = 0; i < m; ++i) {
                result[j][i] += left_data[p][i] * bpj;
            }
        }
    }

    Table out;
    for (std::size_t j = 0; j < n; ++j) {
        Column<double> col_data(result[j]);
        out.add_column(right_names[j], std::move(col_data));
    }
    return out;
}

auto melt_table(const Table& input, const std::vector<std::string>& id_columns,
                const std::vector<std::string>& measure_columns)
    -> std::expected<Table, std::string> {
    std::vector<std::size_t> id_indices;
    id_indices.reserve(id_columns.size());
    for (const auto& name : id_columns) {
        auto it = input.index.find(name);
        if (it == input.index.end()) {
            return std::unexpected("melt: id column not found: " + name +
                                   " (available: " + format_columns(input) + ")");
        }
        id_indices.push_back(it->second);
    }

    std::unordered_set<std::string> id_set(id_columns.begin(), id_columns.end());
    std::vector<std::size_t> measure_indices;
    std::vector<std::string> measure_names;
    if (measure_columns.empty()) {
        for (std::size_t i = 0; i < input.columns.size(); ++i) {
            if (!id_set.contains(input.columns[i].name)) {
                measure_indices.push_back(i);
                measure_names.push_back(input.columns[i].name);
            }
        }
    } else {
        measure_indices.reserve(measure_columns.size());
        measure_names.reserve(measure_columns.size());
        for (const auto& name : measure_columns) {
            auto it = input.index.find(name);
            if (it == input.index.end()) {
                return std::unexpected("melt: measure column not found: " + name +
                                       " (available: " + format_columns(input) + ")");
            }
            measure_indices.push_back(it->second);
            measure_names.push_back(name);
        }
    }

    if (measure_indices.empty()) {
        return std::unexpected("melt: no measure columns to melt");
    }

    std::size_t first_type = input.columns[measure_indices[0]].column->index();
    for (std::size_t i = 1; i < measure_indices.size(); ++i) {
        if (input.columns[measure_indices[i]].column->index() != first_type) {
            return std::unexpected("melt: all measure columns must have the same type");
        }
    }

    std::size_t rows = input.rows();
    std::size_t n_measures = measure_indices.size();
    std::size_t out_rows = rows * n_measures;

    Table output;

    for (std::size_t id_idx : id_indices) {
        const auto& entry = input.columns[id_idx];
        auto col = std::visit(
            [&](const auto& src_col) -> ColumnValue {
                using SrcCol = std::decay_t<decltype(src_col)>;
                if constexpr (std::is_same_v<SrcCol, Column<std::string>>) {
                    Column<std::string> out_col;
                    const auto* src_offs = src_col.offsets_data();
                    const char* src_chars = src_col.chars_data();
                    const std::size_t total_chars =
                        rows > 0 ? static_cast<std::size_t>(src_offs[rows]) * n_measures : 0;
                    out_col.resize_for_gather(out_rows, total_chars);
                    auto* dst_offs = out_col.offsets_data();
                    char* dst_chars = out_col.chars_data();
                    dst_offs[0] = 0;
                    std::size_t out_i = 0;
                    std::size_t out_char = 0;
                    auto emit_repeat_n = [&]<std::size_t N>() {
                        for (std::size_t r = 0; r < rows; ++r) {
                            const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                            const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                            const std::size_t len = end - start;
                            const char* p = src_chars + start;
                            const std::size_t row_char_base = out_char;
                            if (len > 0) {
                                if constexpr (N >= 1) {
                                    std::memcpy(dst_chars + row_char_base, p, len);
                                }
                                if constexpr (N >= 2) {
                                    std::memcpy(dst_chars + row_char_base + len, p, len);
                                }
                                if constexpr (N >= 3) {
                                    std::memcpy(dst_chars + row_char_base + (2 * len), p, len);
                                }
                                if constexpr (N >= 4) {
                                    std::memcpy(dst_chars + row_char_base + (3 * len), p, len);
                                }
                            }
                            if constexpr (N >= 1) {
                                dst_offs[++out_i] = static_cast<std::uint32_t>(row_char_base + len);
                            }
                            if constexpr (N >= 2) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + (2 * len));
                            }
                            if constexpr (N >= 3) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + (3 * len));
                            }
                            if constexpr (N >= 4) {
                                dst_offs[++out_i] =
                                    static_cast<std::uint32_t>(row_char_base + (4 * len));
                            }
                            out_char += len * N;
                        }
                    };
                    switch (n_measures) {
                        case 1:
                            emit_repeat_n.template operator()<1>();
                            break;
                        case 2:
                            emit_repeat_n.template operator()<2>();
                            break;
                        case 3:
                            emit_repeat_n.template operator()<3>();
                            break;
                        case 4:
                            emit_repeat_n.template operator()<4>();
                            break;
                        default:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                                const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                                const std::size_t len = end - start;
                                const char* p = src_chars + start;
                                const std::size_t row_char_base = out_char;
                                const std::size_t repeated_chars = len * n_measures;
                                if (len > 0 && n_measures > 0) {
                                    std::memcpy(dst_chars + row_char_base, p, len);
                                    std::size_t copied = len;
                                    while (copied < repeated_chars) {
                                        const std::size_t chunk =
                                            std::min(copied, repeated_chars - copied);
                                        std::memcpy(dst_chars + row_char_base + copied,
                                                    dst_chars + row_char_base, chunk);
                                        copied += chunk;
                                    }
                                }
                                for (std::size_t m = 0; m < n_measures; ++m) {
                                    dst_offs[++out_i] =
                                        static_cast<std::uint32_t>(row_char_base + ((m + 1) * len));
                                }
                                out_char += repeated_chars;
                            }
                            break;
                    }
                    return out_col;
                } else if constexpr (std::is_same_v<SrcCol, Column<Categorical>>) {
                    Column<Categorical> out_col{src_col.dictionary_ptr(), src_col.index_ptr(), {}};
                    out_col.resize(out_rows);
                    auto* dst_codes = out_col.codes_data();
                    std::size_t out_i = 0;
                    for (std::size_t r = 0; r < rows; ++r) {
                        auto code = src_col.code_at(r);
                        for (std::size_t m = 0; m < n_measures; ++m) {
                            dst_codes[out_i++] = code;
                        }
                    }
                    return out_col;
                } else {
                    SrcCol out_col;
                    out_col.reserve(out_rows);
                    out_col.resize(out_rows);
                    if constexpr (std::is_same_v<SrcCol, Column<bool>>) {
                        switch (n_measures) {
                            case 1:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    out_col.set(r, src_col[r]);
                                }
                                break;
                            case 2:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 2;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                }
                                break;
                            case 3:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 3;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                    out_col.set(base + 2, v);
                                }
                                break;
                            case 4:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    const std::size_t base = r * 4;
                                    out_col.set(base, v);
                                    out_col.set(base + 1, v);
                                    out_col.set(base + 2, v);
                                    out_col.set(base + 3, v);
                                }
                                break;
                            default: {
                                std::size_t out_i = 0;
                                for (std::size_t r = 0; r < rows; ++r) {
                                    const bool v = src_col[r];
                                    for (std::size_t m = 0; m < n_measures; ++m) {
                                        out_col.set(out_i++, v);
                                    }
                                }
                                break;
                            }
                        }
                    } else {
                        auto* dst = out_col.data();
                        switch (n_measures) {
                            case 1:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    dst[r] = src_col[r];
                                }
                                break;
                            case 2:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 2;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                }
                                break;
                            case 3:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 3;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                    dst[base + 2] = v;
                                }
                                break;
                            case 4:
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    const std::size_t base = r * 4;
                                    dst[base] = v;
                                    dst[base + 1] = v;
                                    dst[base + 2] = v;
                                    dst[base + 3] = v;
                                }
                                break;
                            default: {
                                std::size_t out_i = 0;
                                for (std::size_t r = 0; r < rows; ++r) {
                                    auto v = src_col[r];
                                    for (std::size_t m = 0; m < n_measures; ++m) {
                                        dst[out_i++] = v;
                                    }
                                }
                                break;
                            }
                        }
                    }
                    return out_col;
                }
            },
            *entry.column);

        if (entry.validity.has_value()) {
            ValidityBitmap validity;
            validity.reserve(out_rows);
            for (std::size_t r = 0; r < rows; ++r) {
                bool valid = (*entry.validity)[r];
                for (std::size_t m = 0; m < n_measures; ++m) {
                    validity.push_back(valid);
                }
            }
            output.add_column(entry.name, std::move(col), std::move(validity));
        } else {
            output.add_column(entry.name, std::move(col));
        }
    }

    {
        Column<Categorical> var_col{std::vector<std::string>(measure_names)};
        var_col.resize(out_rows);
        auto* codes = var_col.codes_data();
        for (std::size_t mi = 0; mi < n_measures; ++mi) {
            codes[mi] = static_cast<Column<Categorical>::code_type>(mi);
        }
        std::size_t copied = n_measures;
        while (copied < out_rows) {
            const std::size_t chunk = std::min(copied, out_rows - copied);
            std::memcpy(codes + copied, codes, chunk * sizeof(*codes));
            copied += chunk;
        }
        output.add_column("variable", std::move(var_col));
    }

    bool any_measure_validity = false;
    for (std::size_t mi = 0; mi < n_measures; ++mi) {
        if (input.columns[measure_indices[mi]].validity.has_value()) {
            any_measure_validity = true;
            break;
        }
    }

    auto value_col = std::visit(
        [&](const auto& first_col) -> ColumnValue {
            using SrcCol = std::decay_t<decltype(first_col)>;
            if constexpr (std::is_same_v<SrcCol, Column<std::string>>) {
                Column<std::string> out_col;
                std::vector<const Column<std::string>*> measures;
                measures.reserve(n_measures);
                std::size_t total_chars = 0;
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<Column<std::string>>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                    const auto* offs = src->offsets_data();
                    total_chars += rows > 0 ? static_cast<std::size_t>(offs[rows]) : 0;
                }
                out_col.resize_for_gather(out_rows, total_chars);
                auto* dst_offs = out_col.offsets_data();
                char* dst_chars = out_col.chars_data();
                dst_offs[0] = 0;
                std::size_t out_i = 0;
                std::size_t out_char = 0;
                for (std::size_t r = 0; r < rows; ++r) {
                    for (std::size_t mi = 0; mi < n_measures; ++mi) {
                        const auto* src_offs = measures[mi]->offsets_data();
                        const char* src_chars = measures[mi]->chars_data();
                        const std::size_t start = static_cast<std::size_t>(src_offs[r]);
                        const std::size_t end = static_cast<std::size_t>(src_offs[r + 1]);
                        const std::size_t len = end - start;
                        if (len > 0) {
                            std::memcpy(dst_chars + out_char, src_chars + start, len);
                        }
                        out_char += len;
                        dst_offs[++out_i] = static_cast<std::uint32_t>(out_char);
                    }
                }
                return out_col;
            } else if constexpr (std::is_same_v<SrcCol, Column<Categorical>>) {
                Column<Categorical> out_col{first_col.dictionary_ptr(), first_col.index_ptr(), {}};
                std::vector<const Column<Categorical>*> measures;
                measures.reserve(n_measures);
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<Column<Categorical>>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                }
                out_col.resize(out_rows);
                auto* dst_codes = out_col.codes_data();
                std::size_t out_i = 0;
                for (std::size_t r = 0; r < rows; ++r) {
                    for (std::size_t mi = 0; mi < n_measures; ++mi) {
                        dst_codes[out_i++] = measures[mi]->code_at(r);
                    }
                }
                return out_col;
            } else {
                SrcCol out_col;
                std::vector<const SrcCol*> measures;
                measures.reserve(n_measures);
                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                    const auto& entry = input.columns[measure_indices[mi]];
                    const auto* src = std::get_if<SrcCol>(entry.column.get());
                    if (src == nullptr) {
                        invariant_violation(
                            "melt_table: measure column type mismatch after upfront validation");
                    }
                    measures.push_back(src);
                }
                out_col.resize(out_rows);
                if constexpr (std::is_same_v<SrcCol, Column<bool>>) {
                    switch (n_measures) {
                        case 1:
                            for (std::size_t r = 0; r < rows; ++r) {
                                out_col.set(r, (*measures[0])[r]);
                            }
                            break;
                        case 2:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 2;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                            }
                            break;
                        case 3:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 3;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                                out_col.set(base + 2, (*measures[2])[r]);
                            }
                            break;
                        case 4:
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 4;
                                out_col.set(base, (*measures[0])[r]);
                                out_col.set(base + 1, (*measures[1])[r]);
                                out_col.set(base + 2, (*measures[2])[r]);
                                out_col.set(base + 3, (*measures[3])[r]);
                            }
                            break;
                        default: {
                            std::size_t out_i = 0;
                            for (std::size_t r = 0; r < rows; ++r) {
                                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                                    out_col.set(out_i++, (*measures[mi])[r]);
                                }
                            }
                            break;
                        }
                    }
                } else {
                    auto* dst = out_col.data();
                    switch (n_measures) {
                        case 1: {
                            const auto* m0 = measures[0]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                dst[r] = m0[r];
                            }
                            break;
                        }
                        case 2: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 2;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                            }
                            break;
                        }
                        case 3: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            const auto* m2 = measures[2]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 3;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                                dst[base + 2] = m2[r];
                            }
                            break;
                        }
                        case 4: {
                            const auto* m0 = measures[0]->data();
                            const auto* m1 = measures[1]->data();
                            const auto* m2 = measures[2]->data();
                            const auto* m3 = measures[3]->data();
                            for (std::size_t r = 0; r < rows; ++r) {
                                const std::size_t base = r * 4;
                                dst[base] = m0[r];
                                dst[base + 1] = m1[r];
                                dst[base + 2] = m2[r];
                                dst[base + 3] = m3[r];
                            }
                            break;
                        }
                        default: {
                            std::size_t out_i = 0;
                            for (std::size_t r = 0; r < rows; ++r) {
                                for (std::size_t mi = 0; mi < n_measures; ++mi) {
                                    dst[out_i++] = (*measures[mi])[r];
                                }
                            }
                            break;
                        }
                    }
                }
                return out_col;
            }
        },
        *input.columns[measure_indices[0]].column);

    if (any_measure_validity) {
        ValidityBitmap value_validity(out_rows, true);
        std::size_t out_i = 0;
        for (std::size_t r = 0; r < rows; ++r) {
            for (std::size_t mi = 0; mi < n_measures; ++mi, ++out_i) {
                if (is_null(input.columns[measure_indices[mi]], r)) {
                    value_validity.set(out_i, false);
                }
            }
        }
        output.add_column("value", std::move(value_col), std::move(value_validity));
    } else {
        output.add_column("value", std::move(value_col));
    }

    return output;
}

auto dcast_table(const Table& input, const std::string& pivot_column,
                 const std::string& value_column, const std::vector<std::string>& row_keys)
    -> std::expected<Table, std::string> {
    constexpr std::size_t kMissingPivot = std::numeric_limits<std::size_t>::max();
    constexpr std::size_t kMissingCell = std::numeric_limits<std::size_t>::max();

    auto pivot_it = input.index.find(pivot_column);
    if (pivot_it == input.index.end()) {
        return std::unexpected("dcast: pivot column not found: " + pivot_column +
                               " (available: " + format_columns(input) + ")");
    }
    auto value_it = input.index.find(value_column);
    if (value_it == input.index.end()) {
        return std::unexpected("dcast: value column not found: " + value_column +
                               " (available: " + format_columns(input) + ")");
    }
    std::vector<std::size_t> key_indices;
    key_indices.reserve(row_keys.size());
    for (const auto& name : row_keys) {
        auto it = input.index.find(name);
        if (it == input.index.end()) {
            return std::unexpected("dcast: row key column not found: " + name +
                                   " (available: " + format_columns(input) + ")");
        }
        key_indices.push_back(it->second);
    }

    std::size_t pivot_idx = pivot_it->second;
    std::size_t value_idx = value_it->second;
    std::size_t rows = input.rows();

    std::vector<std::string> pivot_values;
    const auto& pivot_col = *input.columns[pivot_idx].column;

    std::vector<std::size_t> cat_code_to_pvi;
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> int_pvi_map;
    robin_hood::unordered_flat_map<std::string, std::size_t, StringViewHash, StringViewEq>
        str_pvi_map;

    if (const auto* cat_col = std::get_if<Column<Categorical>>(&pivot_col)) {
        const auto& dict = cat_col->dictionary();
        cat_code_to_pvi.assign(dict.size(), kMissingPivot);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r)) {
                continue;
            }
            const auto code = cat_col->code_at(r);
            if (code < 0) {
                continue;
            }
            const auto ci = static_cast<std::size_t>(code);
            if (ci >= dict.size()) {
                continue;
            }
            if (cat_code_to_pvi[ci] == kMissingPivot) {
                cat_code_to_pvi[ci] = pivot_values.size();
                pivot_values.push_back(dict[ci]);
            }
        }
    } else if (const auto* int_col = std::get_if<Column<std::int64_t>>(&pivot_col)) {
        int_pvi_map.reserve(16);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r)) {
                continue;
            }
            const std::int64_t pv = (*int_col)[r];
            auto [it, inserted] = int_pvi_map.try_emplace(pv, pivot_values.size());
            if (inserted) {
                pivot_values.push_back(std::to_string(pv));
            }
        }
    } else {
        str_pvi_map.reserve(16);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[pivot_idx], r)) {
                continue;
            }
            std::string pv = std::visit(
                [r](const auto& col) -> std::string {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                        return std::string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                        return std::string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                        return std::to_string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                        return std::to_string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<bool>>) {
                        return col[r] ? "true" : "false";
                    } else {
                        return std::to_string(r);
                    }
                },
                pivot_col);
            auto [it, inserted] = str_pvi_map.try_emplace(std::move(pv), pivot_values.size());
            if (inserted) {
                pivot_values.push_back(it->first);
            }
        }
    }

    if (pivot_values.empty()) {
        Table output;
        for (std::size_t ki : key_indices) {
            const auto& entry = input.columns[ki];
            output.add_column(entry.name, *entry.column);
            if (entry.validity.has_value()) {
                output.columns.back().validity = entry.validity;
            }
        }
        return output;
    }

    std::size_t n_pivots = pivot_values.size();

    constexpr std::int64_t kNullKey = std::numeric_limits<std::int64_t>::min();
    const std::size_t n_keys = key_indices.size();
    std::vector<std::vector<std::int32_t>> str_intern(n_keys);

    for (std::size_t k = 0; k < n_keys; ++k) {
        const std::size_t ki = key_indices[k];
        const auto* str_col = std::get_if<Column<std::string>>(input.columns[ki].column.get());
        if (str_col == nullptr) {
            continue;
        }
        auto& codes = str_intern[k];
        codes.reserve(rows);
        robin_hood::unordered_flat_map<std::string_view, std::int32_t, StringViewHash, StringViewEq>
            sv_to_code;
        sv_to_code.reserve((rows / n_pivots) + 1);
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(input.columns[ki], r)) {
                codes.push_back(-1);
                continue;
            }
            const std::string_view sv = (*str_col)[r];
            auto [it, inserted] =
                sv_to_code.try_emplace(sv, static_cast<std::int32_t>(sv_to_code.size()));
            (void)inserted;
            codes.push_back(it->second);
        }
    }

    const std::size_t est_out_rows = (rows / n_pivots) + 1;

    std::vector<std::size_t> first_input_row;
    first_input_row.reserve(est_out_rows);
    std::vector<std::size_t> cell_rows(est_out_rows * n_pivots, kMissingCell);

    const auto encode_key = [&](std::size_t k, std::size_t r) -> std::int64_t {
        const std::size_t ki = key_indices[k];
        if (is_null(input.columns[ki], r)) {
            return kNullKey;
        }
        if (!str_intern[k].empty()) {
            return str_intern[k][r];
        }
        return std::visit(
            [r](const auto& c) -> std::int64_t {
                using T = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<T, Column<Categorical>>) {
                    return static_cast<std::int64_t>(c.code_at(r));
                } else if constexpr (std::is_same_v<T, Column<std::int64_t>>) {
                    return c[r];
                } else if constexpr (std::is_same_v<T, Column<double>>) {
                    return static_cast<std::int64_t>(c[r]);
                } else if constexpr (std::is_same_v<T, Column<bool>>) {
                    return static_cast<std::int64_t>(c[r] ? 1 : 0);
                } else if constexpr (std::is_same_v<T, Column<Date>>) {
                    return static_cast<std::int64_t>(c[r].days);
                } else if constexpr (std::is_same_v<T, Column<Timestamp>>) {
                    return c[r].nanos;
                } else {
                    return kNullKey;
                }
            },
            *input.columns[ki].column);
    };

    const auto resolve_pvi = [&](std::size_t r) -> std::size_t {
        if (is_null(input.columns[pivot_idx], r)) {
            return kMissingPivot;
        }
        if (!cat_code_to_pvi.empty()) {
            const auto* cat_col = std::get_if<Column<Categorical>>(&pivot_col);
            const auto code = cat_col->code_at(r);
            if (code >= 0) {
                const auto ci = static_cast<std::size_t>(code);
                if (ci < cat_code_to_pvi.size()) {
                    return cat_code_to_pvi[ci];
                }
            }
            return kMissingPivot;
        }
        if (!int_pvi_map.empty()) {
            const auto* int_col = std::get_if<Column<std::int64_t>>(&pivot_col);
            auto it = int_pvi_map.find((*int_col)[r]);
            return it != int_pvi_map.end() ? it->second : kMissingPivot;
        }
        if (const auto* str_col_ptr = std::get_if<Column<std::string>>(&pivot_col)) {
            auto it = str_pvi_map.find((*str_col_ptr)[r]);
            return it != str_pvi_map.end() ? it->second : kMissingPivot;
        }
        const std::string pv = std::visit(
            [r](const auto& col) -> std::string {
                using ColType = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                    return std::string(col[r]);
                } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                    return std::to_string(col[r]);
                } else if constexpr (std::is_same_v<ColType, Column<bool>>) {
                    return col[r] ? "true" : "false";
                } else {
                    return std::to_string(r);
                }
            },
            pivot_col);
        auto it = str_pvi_map.find(pv);
        return it != str_pvi_map.end() ? it->second : kMissingPivot;
    };

    if (n_keys <= DcastKey::kMaxCols) {
        robin_hood::unordered_flat_map<DcastKey, std::size_t, DcastKeyHash> key_to_row;
        key_to_row.reserve(est_out_rows);
        DcastKey prev_key{};
        prev_key.n = static_cast<std::uint8_t>(n_keys);
        std::size_t prev_out_row = std::numeric_limits<std::size_t>::max();

        for (std::size_t r = 0; r < rows; ++r) {
            const std::size_t pvi = resolve_pvi(r);
            if (pvi == kMissingPivot) {
                continue;
            }

            DcastKey key{};
            key.n = static_cast<std::uint8_t>(n_keys);
            for (std::size_t k = 0; k < n_keys; ++k) {
                key.v[k] = encode_key(k, r);
            }

            std::size_t out_row{};
            if (key == prev_key && prev_out_row != std::numeric_limits<std::size_t>::max()) {
                out_row = prev_out_row;
            } else {
                auto [it, inserted] = key_to_row.try_emplace(key, first_input_row.size());
                if (inserted) {
                    first_input_row.push_back(r);
                    const std::size_t needed = first_input_row.size() * n_pivots;
                    if (needed > cell_rows.size()) {
                        cell_rows.resize(std::max(needed, cell_rows.size() * 2), kMissingCell);
                    }
                }
                out_row = it->second;
                prev_key = key;
                prev_out_row = out_row;
            }
            cell_rows[(out_row * n_pivots) + pvi] = r;
        }
    } else {
        robin_hood::unordered_map<std::string, std::size_t> key_to_row_str;
        key_to_row_str.reserve(est_out_rows);
        const std::size_t key_bytes = n_keys * sizeof(std::int64_t);
        std::string prev_key_str(key_bytes, '\0');
        std::size_t prev_out_row = std::numeric_limits<std::size_t>::max();

        for (std::size_t r = 0; r < rows; ++r) {
            const std::size_t pvi = resolve_pvi(r);
            if (pvi == kMissingPivot) {
                continue;
            }

            std::string key(key_bytes, '\0');
            for (std::size_t k = 0; k < n_keys; ++k) {
                std::int64_t v = encode_key(k, r);
                std::memcpy(key.data() + (k * sizeof(std::int64_t)), &v, sizeof(v));
            }

            std::size_t out_row{};
            if (key == prev_key_str && prev_out_row != std::numeric_limits<std::size_t>::max()) {
                out_row = prev_out_row;
            } else {
                auto [it, inserted] = key_to_row_str.try_emplace(key, first_input_row.size());
                if (inserted) {
                    first_input_row.push_back(r);
                    const std::size_t needed = first_input_row.size() * n_pivots;
                    if (needed > cell_rows.size()) {
                        cell_rows.resize(std::max(needed, cell_rows.size() * 2), kMissingCell);
                    }
                }
                out_row = it->second;
                prev_key_str = std::move(key);
                prev_out_row = out_row;
            }
            cell_rows[(out_row * n_pivots) + pvi] = r;
        }
    }

    std::size_t out_rows = first_input_row.size();

    Table output;

    for (std::size_t k = 0; k < row_keys.size(); ++k) {
        std::size_t ki = key_indices[k];
        const auto& entry = input.columns[ki];
        auto col = make_empty_like(*entry.column);
        std::visit([out_rows](auto& c) { c.reserve(out_rows); }, col);
        for (std::size_t or_idx = 0; or_idx < out_rows; ++or_idx) {
            append_value(col, *entry.column, first_input_row[or_idx]);
        }
        output.add_column(entry.name, std::move(col));
    }

    const auto& value_entry = input.columns[value_idx];
    for (std::size_t pi = 0; pi < n_pivots; ++pi) {
        auto col = make_empty_like(*value_entry.column);
        std::visit([out_rows](auto& c) { c.reserve(out_rows); }, col);
        ValidityBitmap validity(out_rows, false);
        bool has_nulls = false;

        for (std::size_t or_idx = 0; or_idx < out_rows; ++or_idx) {
            std::size_t cell_key = (or_idx * n_pivots) + pi;
            const std::size_t input_row = cell_rows[cell_key];
            if (input_row != kMissingCell) {
                append_value(col, *value_entry.column, input_row);
                bool val_null = is_null(value_entry, input_row);
                validity.set(or_idx, !val_null);
                if (val_null) {
                    has_nulls = true;
                }
            } else {
                std::visit([](auto& c) { c.push_back({}); }, col);
                has_nulls = true;
            }
        }

        if (has_nulls) {
            output.add_column(pivot_values[pi], std::move(col), std::move(validity));
        } else {
            output.add_column(pivot_values[pi], std::move(col));
        }
    }

    return output;
}

}  // namespace ibex::runtime
