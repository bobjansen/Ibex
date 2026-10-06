// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <limits>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

// Named apart from the shared `invariant_violation` that runtime_internal.hpp
// declares: this one carries a reshape-specific prefix, and an unqualified call
// to either would otherwise be ambiguous.
[[noreturn]] void reshape_invariant_violation(std::string_view detail) {
    (void)std::fputs("ibex internal invariant violated (runtime/reshape): ", stderr);
    (void)std::fwrite(detail.data(), sizeof(char), detail.size(), stderr);
    (void)std::fputc('\n', stderr);
    std::abort();
}

auto extract_numeric(const Table& input)
    -> std::pair<std::vector<std::string>, std::vector<std::vector<double>>> {
    std::vector<std::string> names;
    std::vector<std::vector<double>> data;
    const std::size_t rows = input.rows();
    for (const auto& entry : input.columns) {
        std::vector<double> col;
        col.reserve(rows);
        const bool ok = std::visit(
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

/// A row whose pivot is null (or a code outside the dictionary): it adds no
/// cell and no output row.
constexpr std::uint32_t kDcastNoPivot = std::numeric_limits<std::uint32_t>::max();

/// The row key of a dcast, read straight from the key columns.
///
/// Equality compares the VALUES, so it is exact for every column type by
/// construction: there is no per-column code to keep injective. Two nulls in
/// the same column are equal (a null row key is a group of its own), a null
/// and a value are not. A Float64 compares by bit pattern with -0.0 folded
/// onto 0.0, so the two group together and every NaN with the same bits forms
/// one group, as with any other key.
struct DcastRowKey {
    std::vector<const ColumnEntry*> columns;

    [[nodiscard]] static auto double_bits(double value) noexcept -> std::uint64_t {
        return std::bit_cast<std::uint64_t>(value == 0.0 ? 0.0 : value);
    }

    [[nodiscard]] auto equal(std::size_t a, std::size_t b) const -> bool {
        for (const auto* entry : columns) {
            const bool a_null = is_null(*entry, a);
            const bool b_null = is_null(*entry, b);
            if (a_null || b_null) {
                if (a_null != b_null) {
                    return false;
                }
                continue;
            }
            const bool same = std::visit(
                [a, b](const auto& c) -> bool {
                    using T = std::decay_t<decltype(c)>;
                    if constexpr (std::is_same_v<T, Column<Categorical>>) {
                        return c.code_at(a) == c.code_at(b);
                    } else if constexpr (std::is_same_v<T, Column<double>>) {
                        return double_bits(c[a]) == double_bits(c[b]);
                    } else {
                        return c[a] == c[b];
                    }
                },
                *entry->column);
            if (!same) {
                return false;
            }
        }
        return true;
    }

    /// Set `continues[r]` for each row of [begin, end) whose key equals the
    /// key of the row before it, both rows having a pivot. The first row of
    /// the range starts a run. One typed pass per key column, rather than a
    /// type dispatch per row.
    void mark_runs(std::size_t begin, std::size_t end, const std::uint32_t* pvi,
                   const std::uint64_t* hashes, std::uint8_t* continues) const {
        if (begin == end) {
            return;
        }
        continues[begin] = false;
        for (std::size_t r = begin + 1; r < end; ++r) {
            continues[r] = pvi[r] != kDcastNoPivot && pvi[r - 1] != kDcastNoPivot &&
                           hashes[r] == hashes[r - 1];
        }
        for (const auto* entry : columns) {
            const bool nullable = entry->validity.has_value();
            std::visit(
                [&](const auto& c) {
                    using T = std::decay_t<decltype(c)>;
                    for (std::size_t r = begin + 1; r < end; ++r) {
                        if (!continues[r]) {
                            continue;
                        }
                        if (nullable) {
                            const bool now_null = is_null(*entry, r);
                            const bool prev_null = is_null(*entry, r - 1);
                            if (now_null || prev_null) {
                                continues[r] = now_null == prev_null;
                                continue;
                            }
                        }
                        if constexpr (std::is_same_v<T, Column<Categorical>>) {
                            continues[r] = c.code_at(r) == c.code_at(r - 1);
                        } else if constexpr (std::is_same_v<T, Column<double>>) {
                            continues[r] = double_bits(c[r]) == double_bits(c[r - 1]);
                        } else {
                            continues[r] = c[r] == c[r - 1];
                        }
                    }
                },
                *entry->column);
        }
    }

    /// Fold key column `k` of rows [begin, end) into `hashes`. The first
    /// column seeds the hash; every column mixes its value in the same way.
    void hash_rows(std::size_t k, std::size_t begin, std::size_t end, const std::uint32_t* pvi,
                   std::uint64_t* hashes) const {
        constexpr std::uint64_t kNullValue = 0x9e3779b97f4a7c15ULL;
        const ColumnEntry& entry = *columns[k];
        const auto mix = [](std::uint64_t h, std::uint64_t v) -> std::uint64_t {
            h ^= v;
            h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
            h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
            return h ^ (h >> 31);
        };
        std::visit(
            [&](const auto& c) {
                using T = std::decay_t<decltype(c)>;
                const auto value_hash = [&c](std::size_t r) -> std::uint64_t {
                    if constexpr (std::is_same_v<T, Column<Categorical>>) {
                        return static_cast<std::uint64_t>(c.code_at(r));
                    } else if constexpr (std::is_same_v<T, Column<std::string>>) {
                        const std::string_view sv = c[r];
                        return robin_hood::hash_bytes(sv.data(), sv.size());
                    } else if constexpr (std::is_same_v<T, Column<double>>) {
                        return double_bits(c[r]);
                    } else if constexpr (std::is_same_v<T, Column<bool>>) {
                        return c[r] ? 1U : 0U;
                    } else if constexpr (std::is_same_v<T, Column<Date>>) {
                        return static_cast<std::uint64_t>(c[r].days);
                    } else if constexpr (std::is_same_v<T, Column<Timestamp>>) {
                        return static_cast<std::uint64_t>(c[r].nanos);
                    } else if constexpr (std::is_same_v<T, Column<Decimal>>) {
                        const Decimal value = c[r];
                        return robin_hood::hash_bytes(&value.units, sizeof(value.units));
                    } else {
                        return static_cast<std::uint64_t>(c[r]);
                    }
                };
                const bool nullable = entry.validity.has_value();
                for (std::size_t r = begin; r < end; ++r) {
                    if (pvi[r] == kDcastNoPivot) {
                        continue;
                    }
                    const std::uint64_t v =
                        nullable && is_null(entry, r) ? kNullValue : value_hash(r);
                    hashes[r] = mix(k == 0 ? 0 : hashes[r], v);
                }
            },
            *entry.column);
    }
};

/// Run `body(i)` for every `i` in `[0, count)` on up to `workers` threads,
/// or inline, in order, when `workers` is 1.
template <typename Body>
void for_dcast_tasks(std::size_t workers, std::size_t count, const Body& body) {
    if (workers < 2 || count < 2) {
        for (std::size_t i = 0; i < count; ++i) {
            body(i);
        }
        return;
    }
    std::atomic<std::size_t> cursor{0};
    auto batch = process_worker_pool().submit(std::min(workers, count), [&](std::size_t) {
        for (std::size_t i = cursor.fetch_add(1, std::memory_order_relaxed); i < count;
             i = cursor.fetch_add(1, std::memory_order_relaxed)) {
            body(i);
        }
    });
    batch.wait();
}

/// Run `body(range, begin, end)` over `ranges` equal slices of `[0, n)`,
/// aligned to 64 rows. A caller that keeps one result per slice and reads them
/// in slice order sees the rows in input order.
template <typename Body>
void for_dcast_ranges(std::size_t workers, std::size_t n, std::size_t ranges, const Body& body) {
    constexpr std::size_t kAlign = 64;
    const std::size_t grain =
        std::max<std::size_t>(kAlign, (((n + ranges - 1) / ranges) + kAlign - 1) / kAlign * kAlign);
    for_dcast_tasks(workers, ranges, [&](std::size_t range) {
        const std::size_t begin = std::min(n, range * grain);
        body(range, begin, std::min(n, begin + grain));
    });
}

/// True when `validity` marks at least one row null.
[[nodiscard]] auto validity_has_null(ValidityBitmap& validity) -> bool {
    const std::size_t bits = validity.size();
    const auto* words = validity.words_data();
    for (std::size_t w = 0; w < bits / 64; ++w) {
        if (words[w] != ~std::uint64_t{0}) {
            return true;
        }
    }
    const std::size_t tail = bits % 64;
    return tail != 0 && (words[bits / 64] | (~std::uint64_t{0} << tail)) != ~std::uint64_t{0};
}

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

    const auto denom = static_cast<double>(rows - 1);
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

    const auto denom = static_cast<double>(rows - 1);
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
    for (int i = 0; std::cmp_less(i, input.columns.size()); ++i) {
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
        if (std::cmp_not_equal(i, label_idx)) {
            data_idxs.push_back(i);
        }
    }

    if (data_idxs.empty()) {
        return std::unexpected("transpose: no data columns to transpose");
    }

    const std::size_t first_type = input.columns[data_idxs[0]].column->index();
    for (const std::size_t idx : data_idxs) {
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
                out_col_names.emplace_back((*sc)[i]);
            }
        } else if (const auto* cc = std::get_if<Column<Categorical>>(&*label_entry.column)) {
            for (std::size_t i = 0; i < n_rows; ++i) {
                out_col_names.emplace_back((*cc)[i]);
            }
        }
    } else {
        for (std::size_t i = 0; i < n_rows; ++i) {
            out_col_names.push_back("r" + std::to_string(i));
        }
    }

    {
        robin_hood::unordered_set<std::string> seen;
        for (const auto& name : out_col_names) {
            if (!seen.insert(name).second) {
                return std::unexpected("transpose: duplicate label value '" + name +
                                       "' — output column names must be unique");
            }
        }
    }

    Table out;
    Column<std::string> row_labels;
    for (const std::size_t i : data_idxs) {
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
                    for (const std::size_t ci : data_idxs) {
                        const auto& src = std::get<ColT>(*input.columns[ci].column);
                        out_col.push_back(src[r]);
                    }
                    out.add_column(out_col_names[r], std::move(out_col));
                } else {
                    using ElemT = ColT::value_type;
                    Column<ElemT> out_col;
                    out_col.reserve(n_data_cols);
                    for (const std::size_t ci : data_idxs) {
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

namespace {

// Human-readable name for the active alternative of a ColumnValue, for use in
// rbind's schema-mismatch diagnostics.
auto column_type_label(const ColumnValue& column) -> std::string_view {
    return std::visit(
        [](const auto& col) -> std::string_view {
            using Col = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<Col, Column<std::int64_t>>) {
                return "Int64";
            } else if constexpr (std::is_same_v<Col, Column<double>>) {
                return "Float64";
            } else if constexpr (std::is_same_v<Col, Column<std::string>>) {
                return "String";
            } else if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                return "Categorical";
            } else if constexpr (std::is_same_v<Col, Column<Date>>) {
                return "Date";
            } else if constexpr (std::is_same_v<Col, Column<Timestamp>>) {
                return "Timestamp";
            } else {
                return "Bool";
            }
        },
        column);
}

// Extract the merge column of `table` (at position `pos`) as ascending-order
// comparison keys. Only Timestamp/Date/Int64 are orderable time indices; any
// other type yields nullopt so the caller can report it.
auto extract_order_keys(const Table& table, std::size_t pos)
    -> std::optional<std::vector<std::int64_t>> {
    return std::visit(
        [](const auto& col) -> std::optional<std::vector<std::int64_t>> {
            using Col = std::decay_t<decltype(col)>;
            std::vector<std::int64_t> keys;
            keys.reserve(col.size());
            if constexpr (std::is_same_v<Col, Column<Timestamp>>) {
                for (std::size_t r = 0; r < col.size(); ++r) {
                    keys.push_back(col[r].nanos);
                }
                return keys;
            } else if constexpr (std::is_same_v<Col, Column<Date>>) {
                for (std::size_t r = 0; r < col.size(); ++r) {
                    keys.push_back(col[r].days);
                }
                return keys;
            } else if constexpr (std::is_same_v<Col, Column<std::int64_t>>) {
                for (std::size_t r = 0; r < col.size(); ++r) {
                    keys.push_back(col[r]);
                }
                return keys;
            } else {
                return std::nullopt;
            }
        },
        *table.columns[pos].column);
}

}  // namespace

auto rbind_table(const std::vector<const Table*>& tables,
                 const std::optional<std::string>& merge_key) -> std::expected<Table, std::string> {
    if (tables.empty()) {
        return std::unexpected("rbind: requires at least one operand");
    }
    const Table& ref = *tables[0];
    const std::size_t ncols = ref.columns.size();

    // Validate that every operand exposes exactly the reference column set
    // (same names, same types), and record where each reference column lives in
    // each operand so reordered-but-matching schemas bind correctly.
    std::vector<std::vector<std::size_t>> col_pos(tables.size());
    col_pos[0].reserve(ncols);
    for (std::size_t ci = 0; ci < ncols; ++ci) {
        col_pos[0].push_back(ci);
    }
    for (std::size_t ti = 1; ti < tables.size(); ++ti) {
        const Table& other = *tables[ti];
        if (other.columns.size() != ncols) {
            return std::unexpected("rbind: operand " + std::to_string(ti + 1) + " has " +
                                   std::to_string(other.columns.size()) +
                                   " columns but operand 1 has " + std::to_string(ncols) +
                                   " (operand 1: " + format_columns(ref) + "; operand " +
                                   std::to_string(ti + 1) + ": " + format_columns(other) + ")");
        }
        col_pos[ti].reserve(ncols);
        for (std::size_t ci = 0; ci < ncols; ++ci) {
            const ColumnEntry& ref_entry = ref.columns[ci];
            auto it = other.index.find(ref_entry.name);
            if (it == other.index.end()) {
                return std::unexpected("rbind: operand " + std::to_string(ti + 1) +
                                       " is missing column '" + ref_entry.name +
                                       "' present in operand 1 (operand " + std::to_string(ti + 1) +
                                       ": " + format_columns(other) + ")");
            }
            const std::size_t pos = it->second;
            const ColumnEntry& other_entry = other.columns[pos];
            if (other_entry.column->index() != ref_entry.column->index()) {
                return std::unexpected("rbind: column '" + ref_entry.name + "' is " +
                                       std::string(column_type_label(*ref_entry.column)) +
                                       " in operand 1 but " +
                                       std::string(column_type_label(*other_entry.column)) +
                                       " in operand " + std::to_string(ti + 1));
            }
            col_pos[ti].push_back(pos);
        }
    }

    std::size_t total_rows = 0;
    for (const Table* t : tables) {
        total_rows += t->rows();
    }

    // Output row sequence as (operand, source-row) pairs. Without a merge key
    // this is just every operand's rows in order; with one we k-way merge the
    // already-sorted operands so the result lands sorted in a single pass —
    // no concatenate-then-sort. Every column (and validity bitmap) is then
    // gathered through this one order.
    std::vector<std::pair<std::size_t, std::size_t>> order;
    order.reserve(total_rows);
    if (merge_key.has_value()) {
        const std::size_t mci = ref.index.at(*merge_key);
        std::vector<std::vector<std::int64_t>> keys(tables.size());
        for (std::size_t ti = 0; ti < tables.size(); ++ti) {
            auto extracted = extract_order_keys(*tables[ti], col_pos[ti][mci]);
            if (!extracted.has_value()) {
                return std::unexpected("rbind: time index column '" + *merge_key +
                                       "' must be Timestamp, Date, or Int64 to merge TimeFrames");
            }
            keys[ti] = std::move(extracted.value());
        }
        std::vector<std::size_t> cursor(tables.size(), 0);
        for (std::size_t step = 0; step < total_rows; ++step) {
            std::size_t best = tables.size();
            std::int64_t best_key = 0;
            for (std::size_t ti = 0; ti < tables.size(); ++ti) {
                if (cursor[ti] >= keys[ti].size()) {
                    continue;
                }
                const std::int64_t kv = keys[ti][cursor[ti]];
                // Strict `<` keeps the merge stable: on equal keys the
                // earlier operand's row is emitted first.
                if (best == tables.size() || kv < best_key) {
                    best = ti;
                    best_key = kv;
                }
            }
            order.emplace_back(best, cursor[best]);
            ++cursor[best];
        }
    } else {
        for (std::size_t ti = 0; ti < tables.size(); ++ti) {
            const std::size_t n = tables[ti]->rows();
            for (std::size_t r = 0; r < n; ++r) {
                order.emplace_back(ti, r);
            }
        }
    }

    Table out;
    out.columns.reserve(ncols);
    for (std::size_t ci = 0; ci < ncols; ++ci) {
        const ColumnEntry& ref_entry = ref.columns[ci];

        // Any operand carrying nulls in this column forces a result bitmap.
        bool any_null = false;
        for (std::size_t ti = 0; ti < tables.size(); ++ti) {
            if (tables[ti]->columns[col_pos[ti][ci]].validity.has_value()) {
                any_null = true;
                break;
            }
        }

        // Decimal operands may differ in precision and scale; their units are
        // only comparable at one scale. The result takes the narrowest type
        // holding every operand exactly and rescales each row into it (null
        // slots are never read -- their payload is undefined).
        std::optional<ColumnValue> decimal_built;
        if (std::holds_alternative<Column<Decimal>>(*ref_entry.column)) {
            DecimalType unified = decimal_type_of(
                std::get<Column<Decimal>>(*tables[0]->columns[col_pos[0][ci]].column));
            for (std::size_t ti = 1; ti < tables.size(); ++ti) {
                unified = decimal::union_type(unified,
                                              decimal_type_of(std::get<Column<Decimal>>(
                                                  *tables[ti]->columns[col_pos[ti][ci]].column)));
            }
            Column<Decimal> dst = make_decimal_column(unified);
            dst.reserve(total_rows);
            for (const auto& [ti, r] : order) {
                const ColumnEntry& src_entry = tables[ti]->columns[col_pos[ti][ci]];
                if (is_null(src_entry, r)) {
                    dst.push_back(Decimal{});
                    continue;
                }
                const auto& src = std::get<Column<Decimal>>(*src_entry.column);
                Int128 units = 0;
                if (!decimal::rescale(src[r].units, decimal_type_of(src).scale, unified.scale,
                                      units) ||
                    !decimal::fits(units, unified.precision)) {
                    return std::unexpected("rbind: column '" + ref_entry.name + "' does not fit " +
                                           decimal::type_name(unified));
                }
                dst.push_back(Decimal{units});
            }
            decimal_built = ColumnValue{std::move(dst)};
        }

        ColumnValue built = std::visit(
            [&](const auto& ref_col) -> ColumnValue {
                using Col = std::decay_t<decltype(ref_col)>;
                if constexpr (std::is_same_v<Col, Column<Decimal>>) {
                    // Built above, with the operands' scales unified.
                    if (decimal_built.has_value()) {
                        return std::move(*decimal_built);
                    }
                }
                Col dst;
                dst.reserve(total_rows);
                if (merge_key.has_value()) {
                    for (const auto& [ti, r] : order) {
                        const auto& src =
                            std::get<Col>(*tables[ti]->columns[col_pos[ti][ci]].column);
                        // For Categorical this remaps through each source's
                        // dictionary into the result's; other types copy values.
                        dst.push_back(src[r]);
                    }
                } else if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                    // Every operand's dictionary remap normally needs a
                    // per-row hash lookup (find_or_insert). But when every
                    // operand shares the exact same dictionary object --
                    // the common case for `rbind(t, t)` or operands drawn
                    // from a common upstream scan -- no lookup can ever add
                    // a new entry, so the codes are already valid as-is and
                    // can be bulk-copied like any other column.
                    bool shared_dict = tables[0]->columns[col_pos[0][ci]].column != nullptr;
                    const auto& first_src =
                        std::get<Col>(*tables[0]->columns[col_pos[0][ci]].column);
                    for (std::size_t ti = 1; shared_dict && ti < tables.size(); ++ti) {
                        const auto& src =
                            std::get<Col>(*tables[ti]->columns[col_pos[ti][ci]].column);
                        shared_dict = src.dictionary_ptr() == first_src.dictionary_ptr();
                    }
                    if (shared_dict) {
                        dst = Col(first_src.dictionary_ptr(), first_src.index_ptr(), {});
                        dst.reserve(total_rows);
                        for (std::size_t ti = 0; ti < tables.size(); ++ti) {
                            const auto& src =
                                std::get<Col>(*tables[ti]->columns[col_pos[ti][ci]].column);
                            const auto& src_codes = src.codes();
                            dst.append_codes(src_codes.begin(), src_codes.end());
                        }
                    } else {
                        for (std::size_t ti = 0; ti < tables.size(); ++ti) {
                            const auto& src =
                                std::get<Col>(*tables[ti]->columns[col_pos[ti][ci]].column);
                            for (std::size_t r = 0, n = src.size(); r < n; ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    }
                } else {
                    // Plain append: every operand's rows land contiguously in
                    // the output, so each operand can be copied in one shot
                    // instead of gathered row-by-row through `order` — for
                    // span-backed types that turns an O(rows) scalar loop
                    // (with a variant unwrap on every row) into one `insert`
                    // per operand that the compiler can fold into a memcpy.
                    for (std::size_t ti = 0; ti < tables.size(); ++ti) {
                        const auto& src =
                            std::get<Col>(*tables[ti]->columns[col_pos[ti][ci]].column);
                        if constexpr (requires { src.span(); }) {
                            auto sp = src.span();
                            (void)dst.insert(dst.end(), sp.begin(), sp.end());
                        } else {
                            for (std::size_t r = 0, n = src.size(); r < n; ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    }
                }
                return ColumnValue{std::move(dst)};
            },
            *ref_entry.column);

        if (any_null) {
            ValidityBitmap validity;
            validity.reserve(total_rows);
            for (const auto& [ti, r] : order) {
                validity.push_back(!is_null(tables[ti]->columns[col_pos[ti][ci]], r));
            }
            out.add_column(ref_entry.name, std::move(built), std::move(validity));
        } else {
            out.add_column(ref_entry.name, std::move(built));
        }
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

    const robin_hood::unordered_set<std::string> id_set(id_columns.begin(), id_columns.end());
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

    const std::size_t first_type = input.columns[measure_indices[0]].column->index();
    for (std::size_t i = 1; i < measure_indices.size(); ++i) {
        if (input.columns[measure_indices[i]].column->index() != first_type) {
            return std::unexpected("melt: all measure columns must have the same type");
        }
    }

    std::size_t rows = input.rows();
    std::size_t n_measures = measure_indices.size();
    std::size_t out_rows = rows * n_measures;

    Table output;

    for (const std::size_t id_idx : id_indices) {
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
                            const auto start = static_cast<std::size_t>(src_offs[r]);
                            const auto end = static_cast<std::size_t>(src_offs[r + 1]);
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
                                const auto start = static_cast<std::size_t>(src_offs[r]);
                                const auto end = static_cast<std::size_t>(src_offs[r + 1]);
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
                const bool valid = (*entry.validity)[r];
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
                        reshape_invariant_violation(
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
                        const auto start = static_cast<std::size_t>(src_offs[r]);
                        const auto end = static_cast<std::size_t>(src_offs[r + 1]);
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
                        reshape_invariant_violation(
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
                        reshape_invariant_violation(
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
                 const std::string& value_column, const std::vector<std::string>& row_keys,
                 const ExecutionContext* exec) -> std::expected<Table, std::string> {
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
    DcastRowKey key;
    key.columns.reserve(row_keys.size());
    for (const auto& name : row_keys) {
        auto it = input.index.find(name);
        if (it == input.index.end()) {
            return std::unexpected("dcast: row key column not found: " + name +
                                   " (available: " + format_columns(input) + ")");
        }
        key.columns.push_back(&input.columns[it->second]);
    }

    const ColumnEntry& pivot_entry = input.columns[pivot_it->second];
    const ColumnEntry& value_entry = input.columns[value_it->second];
    const auto& pivot_col = *pivot_entry.column;
    const std::size_t rows = input.rows();

    // Every pass below is a row-range or partition fan-out whose results are
    // read back in input order, so the answer does not depend on `workers`.
    std::size_t workers = 1;
    if (exec != nullptr && exec->can_fan_out() && rows >= exec->parallel_min_rows &&
        !on_worker_pool_thread()) {
        workers = std::min(exec->compute_budget(), process_worker_pool().size());
    }
    const std::size_t ranges = workers < 2 ? 1 : workers * 4;

    // ── Pivot values, in order of first appearance, and each row's pivot ──────
    // Each range lists the values it sees first, in order; reading the lists in
    // range order and keeping each value's first sighting gives the global
    // first-appearance order.
    std::vector<std::string> pivot_values;
    std::vector<std::uint32_t> pvi(rows, kDcastNoPivot);
    if (const auto* cat_col = std::get_if<Column<Categorical>>(&pivot_col)) {
        const auto& dict = cat_col->dictionary();
        const auto code_of = [&](std::size_t r) -> std::size_t {
            if (is_null(pivot_entry, r)) {
                return dict.size();
            }
            const auto code = cat_col->code_at(r);
            return code < 0 ? dict.size() : std::min(static_cast<std::size_t>(code), dict.size());
        };
        std::vector<std::vector<std::size_t>> seen(ranges);
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t range, std::size_t begin, std::size_t end) {
                             std::vector<bool> local(dict.size(), false);
                             for (std::size_t r = begin; r < end; ++r) {
                                 const std::size_t ci = code_of(r);
                                 if (ci < dict.size() && !local[ci]) {
                                     local[ci] = true;
                                     seen[range].push_back(ci);
                                 }
                             }
                         });
        std::vector<std::uint32_t> code_to_pvi(dict.size() + 1, kDcastNoPivot);
        for (const auto& list : seen) {
            for (const std::size_t ci : list) {
                if (code_to_pvi[ci] == kDcastNoPivot) {
                    code_to_pvi[ci] = static_cast<std::uint32_t>(pivot_values.size());
                    pivot_values.push_back(dict[ci]);
                }
            }
        }
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t, std::size_t begin, std::size_t end) {
                             for (std::size_t r = begin; r < end; ++r) {
                                 pvi[r] = code_to_pvi[code_of(r)];
                             }
                         });
    } else if (const auto* int_col = std::get_if<Column<std::int64_t>>(&pivot_col)) {
        std::vector<std::vector<std::int64_t>> seen(ranges);
        for_dcast_ranges(
            workers, rows, ranges, [&](std::size_t range, std::size_t begin, std::size_t end) {
                robin_hood::unordered_flat_set<std::int64_t> local;
                for (std::size_t r = begin; r < end; ++r) {
                    if (!is_null(pivot_entry, r) && local.insert((*int_col)[r]).second) {
                        seen[range].push_back((*int_col)[r]);
                    }
                }
            });
        robin_hood::unordered_flat_map<std::int64_t, std::uint32_t> to_pvi;
        for (const auto& list : seen) {
            for (const std::int64_t pv : list) {
                if (to_pvi.try_emplace(pv, static_cast<std::uint32_t>(pivot_values.size()))
                        .second) {
                    pivot_values.push_back(std::to_string(pv));
                }
            }
        }
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t, std::size_t begin, std::size_t end) {
                             for (std::size_t r = begin; r < end; ++r) {
                                 if (!is_null(pivot_entry, r)) {
                                     pvi[r] = to_pvi.find((*int_col)[r])->second;
                                 }
                             }
                         });
    } else if (const auto* str_col = std::get_if<Column<std::string>>(&pivot_col)) {
        std::vector<std::vector<std::string_view>> seen(ranges);
        for_dcast_ranges(
            workers, rows, ranges, [&](std::size_t range, std::size_t begin, std::size_t end) {
                robin_hood::unordered_flat_set<std::string_view> local;
                for (std::size_t r = begin; r < end; ++r) {
                    if (!is_null(pivot_entry, r) && local.insert((*str_col)[r]).second) {
                        seen[range].push_back((*str_col)[r]);
                    }
                }
            });
        robin_hood::unordered_flat_map<std::string_view, std::uint32_t> to_pvi;
        for (const auto& list : seen) {
            for (const std::string_view pv : list) {
                if (to_pvi.try_emplace(pv, static_cast<std::uint32_t>(pivot_values.size()))
                        .second) {
                    pivot_values.emplace_back(pv);
                }
            }
        }
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t, std::size_t begin, std::size_t end) {
                             for (std::size_t r = begin; r < end; ++r) {
                                 if (!is_null(pivot_entry, r)) {
                                     pvi[r] = to_pvi.find((*str_col)[r])->second;
                                 }
                             }
                         });
    } else {
        // Any other pivot type is keyed by its printed label, serially: no
        // workload pivots on these at scale.
        robin_hood::unordered_flat_map<std::string, std::uint32_t> to_pvi;
        for (std::size_t r = 0; r < rows; ++r) {
            if (is_null(pivot_entry, r)) {
                continue;
            }
            std::string label = std::visit(
                [r](const auto& col) -> std::string {
                    using ColType = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColType, Column<double>>) {
                        return std::to_string(col[r]);
                    } else if constexpr (std::is_same_v<ColType, Column<bool>>) {
                        return col[r] ? "true" : "false";
                    } else {
                        return std::to_string(r);
                    }
                },
                pivot_col);
            auto [it, inserted] = to_pvi.try_emplace(
                std::move(label), static_cast<std::uint32_t>(pivot_values.size()));
            if (inserted) {
                pivot_values.push_back(it->first);
            }
            pvi[r] = it->second;
        }
    }

    if (pivot_values.empty()) {
        Table output;
        for (const auto* entry : key.columns) {
            output.add_column(entry->name, *entry->column);
            if (entry->validity.has_value()) {
                output.columns.back().validity = entry->validity;
            }
        }
        return output;
    }
    const std::size_t n_pivots = pivot_values.size();

    // ── Row-key hashes and runs ────────────────────────────────────────────────
    // Long input usually repeats a row key for each of its pivots in a row (melt
    // writes it that way), so a row with the same key as the row before it
    // continues that row's run. Runs are found here, in one sequential pass,
    // and every later step handles a run as one unit.
    detail::NoInitVector<std::uint64_t> hashes;
    hashes.resize(rows);
    detail::NoInitVector<std::uint8_t> continues;
    continues.resize(rows);
    for_dcast_ranges(workers, rows, ranges, [&](std::size_t, std::size_t begin, std::size_t end) {
        if (key.columns.empty()) {
            std::fill(hashes.data() + begin, hashes.data() + end, std::uint64_t{0});
        }
        for (std::size_t k = 0; k < key.columns.size(); ++k) {
            key.hash_rows(k, begin, end, pvi.data(), hashes.data());
        }
        key.mark_runs(begin, end, pvi.data(), hashes.data(), continues.data());
    });
    const auto is_run_start = [&](std::size_t r) {
        return pvi[r] != kDcastNoPivot && !continues[r];
    };

    // ── Partition the runs by hash ─────────────────────────────────────────────
    // A row key lives in exactly one partition, and each partition holds its
    // runs in input order, so a partition sees every key's first row first and
    // its last row last: the same first-appearance and last-wins answer as one
    // pass over the whole input. Small inputs keep one partition and skip the
    // scatter.
    const bool partitioned = workers > 1 && rows <= std::numeric_limits<std::uint32_t>::max();
    const std::size_t part_bits =
        partitioned
            ? std::min<std::size_t>(8, static_cast<std::size_t>(std::bit_width((workers * 8) - 1)))
            : 0;
    const std::size_t parts = std::size_t{1} << part_bits;
    detail::NoInitVector<std::uint32_t> order;
    std::vector<std::size_t> part_begin(parts + 1, 0);
    if (partitioned) {
        const auto part_of = [&](std::size_t r) -> std::size_t {
            return static_cast<std::size_t>(hashes[r] >> (64 - part_bits));
        };
        std::vector<std::size_t> offsets(ranges * parts, 0);
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t range, std::size_t begin, std::size_t end) {
                             std::size_t* counts = offsets.data() + (range * parts);
                             for (std::size_t r = begin; r < end; ++r) {
                                 if (is_run_start(r)) {
                                     ++counts[part_of(r)];
                                 }
                             }
                         });
        std::size_t running = 0;
        for (std::size_t p = 0; p < parts; ++p) {
            part_begin[p] = running;
            for (std::size_t range = 0; range < ranges; ++range) {
                const std::size_t count = offsets[(range * parts) + p];
                offsets[(range * parts) + p] = running;
                running += count;
            }
        }
        part_begin[parts] = running;
        order.resize(running);
        for_dcast_ranges(workers, rows, ranges,
                         [&](std::size_t range, std::size_t begin, std::size_t end) {
                             std::size_t* next = offsets.data() + (range * parts);
                             for (std::size_t r = begin; r < end; ++r) {
                                 if (is_run_start(r)) {
                                     order[next[part_of(r)]++] = static_cast<std::uint32_t>(r);
                                 }
                             }
                         });
    }

    // ── Group each partition ───────────────────────────────────────────────────
    struct Part {
        std::vector<std::size_t> first_rows;  ///< per group, its first input row
        std::vector<std::size_t> cells;       ///< group x pivot -> last input row
        std::vector<bool> missing;            ///< per pivot: some group lacks it
    };
    struct RowHash {
        const std::uint64_t* hashes;
        auto operator()(std::size_t r) const noexcept -> std::size_t { return hashes[r]; }
    };
    struct RowEq {
        const DcastRowKey* key;
        auto operator()(std::size_t a, std::size_t b) const -> bool { return key->equal(a, b); }
    };
    std::vector<Part> part_out(parts);
    for_dcast_tasks(workers, parts, [&](std::size_t p) {
        Part& part = part_out[p];
        // A partition's runs bound its groups; unpartitioned, a key per
        // pivot's worth of rows is the usual shape of long input.
        const std::size_t expected =
            partitioned ? part_begin[p + 1] - part_begin[p] : (rows / n_pivots) + 1;
        robin_hood::unordered_flat_map<std::size_t, std::size_t, RowHash, RowEq> groups(
            0, RowHash{hashes.data()}, RowEq{&key});
        groups.reserve(expected);
        part.first_rows.reserve(expected);
        part.cells.reserve(expected * n_pivots);
        const auto add_run = [&](std::size_t start) {
            auto [it, inserted] = groups.try_emplace(start, part.first_rows.size());
            if (inserted) {
                part.first_rows.push_back(start);
                part.cells.resize(part.cells.size() + n_pivots, kMissingCell);
            }
            std::size_t* cells = part.cells.data() + (it->second * n_pivots);
            std::size_t r = start;
            do {
                cells[pvi[r]] = r;
                ++r;
            } while (r < rows && continues[r]);
        };
        if (partitioned) {
            for (std::size_t i = part_begin[p]; i < part_begin[p + 1]; ++i) {
                add_run(order[i]);
            }
        } else {
            for (std::size_t r = 0; r < rows; ++r) {
                if (is_run_start(r)) {
                    add_run(r);
                }
            }
        }
        part.missing.assign(n_pivots, false);
        for (std::size_t c = 0; c < part.cells.size(); ++c) {
            if (part.cells[c] == kMissingCell) {
                part.missing[c % n_pivots] = true;
            }
        }
    });
    order = {};

    // ── Number the groups in order of first appearance ─────────────────────────
    // Each group's first row is tagged with the group's id; a scan over the
    // rows in input order then meets the groups in output order, and writes
    // the output's index arrays front to back.
    constexpr std::uint32_t kNotFirst = std::numeric_limits<std::uint32_t>::max();
    std::vector<std::size_t> part_base(parts + 1, 0);
    for (std::size_t p = 0; p < parts; ++p) {
        part_base[p + 1] = part_base[p] + part_out[p].first_rows.size();
    }
    const std::size_t out_rows = part_base[parts];
    if (out_rows >= kNotFirst) {
        return std::unexpected("dcast: more than 2^32 - 1 output rows");
    }
    detail::NoInitVector<std::uint32_t> group_at;
    group_at.resize(rows);
    for_dcast_ranges(workers, rows, ranges, [&](std::size_t, std::size_t begin, std::size_t end) {
        std::fill(group_at.data() + begin, group_at.data() + end, kNotFirst);
    });
    for_dcast_tasks(workers, parts, [&](std::size_t p) {
        const auto& first_rows = part_out[p].first_rows;
        for (std::size_t g = 0; g < first_rows.size(); ++g) {
            group_at[first_rows[g]] = static_cast<std::uint32_t>(part_base[p] + g);
        }
    });
    std::vector<std::size_t> range_base(ranges + 1, 0);
    for_dcast_ranges(workers, rows, ranges,
                     [&](std::size_t range, std::size_t begin, std::size_t end) {
                         range_base[range + 1] = static_cast<std::size_t>(
                             std::ranges::count_if(group_at.data() + begin, group_at.data() + end,
                                                   [](std::uint32_t g) { return g != kNotFirst; }));
                     });
    for (std::size_t range = 0; range < ranges; ++range) {
        range_base[range + 1] += range_base[range];
    }
    // Every slot is written below, so none is zeroed first.
    detail::NoInitVector<std::size_t> first_input_row;
    first_input_row.resize(out_rows);
    std::vector<detail::NoInitVector<std::size_t>> cell_idx(n_pivots);
    for (std::size_t pi = 0; pi < n_pivots; ++pi) {
        cell_idx[pi].resize(out_rows);
    }
    for_dcast_ranges(workers, rows, ranges,
                     [&](std::size_t range, std::size_t begin, std::size_t end) {
                         std::size_t out = range_base[range];
                         for (std::size_t r = begin; r < end; ++r) {
                             const std::uint32_t g = group_at[r];
                             if (g == kNotFirst) {
                                 continue;
                             }
                             const std::size_t p = static_cast<std::size_t>(
                                 std::ranges::upper_bound(part_base, g) - part_base.begin() - 1);
                             const std::size_t* cells =
                                 part_out[p].cells.data() + ((g - part_base[p]) * n_pivots);
                             first_input_row[out] = r;
                             for (std::size_t pi = 0; pi < n_pivots; ++pi) {
                                 cell_idx[pi][out] = cells[pi];
                             }
                             ++out;
                         }
                     });
    std::vector<bool> missing(n_pivots, false);
    for (const auto& part : part_out) {
        for (std::size_t pi = 0; pi < n_pivots; ++pi) {
            if (part.missing[pi]) {
                missing[pi] = true;
            }
        }
    }
    part_out.clear();
    group_at = {};

    // ── Gather the output ──────────────────────────────────────────────────────
    std::vector<ColumnGatherJob> jobs;
    std::vector<const ColumnEntry*> sources;
    jobs.reserve(key.columns.size() + n_pivots);
    sources.reserve(key.columns.size() + n_pivots);
    const auto add_job = [&](const ColumnEntry& entry, const std::size_t* idx, bool sentinel) {
        jobs.push_back({
            .column = entry.column.get(),
            .validity = entry.validity.has_value() ? &*entry.validity : nullptr,
            .idx = idx,
            .indivisible = sentinel,
        });
        sources.push_back(&entry);
    };
    for (const auto* entry : key.columns) {
        add_job(*entry, first_input_row.data(), false);
    }
    for (std::size_t pi = 0; pi < n_pivots; ++pi) {
        add_job(value_entry, cell_idx[pi].data(), missing[pi]);
    }
    auto gathered =
        gather_columns_batched(jobs, out_rows, exec, [&](std::size_t j) -> GatheredColumn {
            return gather_entry_with_nulls(*sources[j], jobs[j].idx, out_rows, kMissingCell);
        });

    // A column keeps a validity bitmap only when it has a null to carry.
    Table output;
    for (std::size_t j = 0; j < gathered.size(); ++j) {
        const std::string& name =
            j < key.columns.size() ? key.columns[j]->name : pivot_values[j - key.columns.size()];
        auto& [column, validity] = gathered[j];
        if (validity.has_value() && validity_has_null(*validity)) {
            output.add_column(name, std::move(column), std::move(*validity));
        } else {
            output.add_column(name, std::move(column));
        }
    }
    return output;
}

}  // namespace ibex::runtime
