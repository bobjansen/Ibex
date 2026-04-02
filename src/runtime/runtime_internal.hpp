#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

namespace ibex::runtime {

enum class ExprType : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
};

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

struct Mask {
    std::vector<uint8_t> value;
    std::optional<std::vector<uint8_t>> valid;  // nullopt = all rows valid

    void apply_validity(const ValidityBitmap* v, std::size_t n) {
        if (v == nullptr) {
            return;
        }
        valid.emplace(n, uint8_t{1});
        for (std::size_t i = 0; i < n; ++i) {
            (*valid)[i] = static_cast<uint8_t>((*v)[i]);
        }
    }
};

[[nodiscard]] auto is_simple_identifier(std::string_view name) -> bool;
[[nodiscard]] auto format_columns(const Table& table) -> std::string;
auto normalize_time_index(Table& table) -> void;
[[nodiscard]] auto int64_to_date_checked(std::int64_t value) -> Date;
[[nodiscard]] auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue;
[[nodiscard]] auto column_kind(const ColumnValue& column) -> ExprType;

inline auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                throw std::runtime_error("append_value: source/destination column type mismatch");
            }
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                dst_col.push_code(src_col->code_at(index));
            } else {
                dst_col.push_back((*src_col)[index]);
            }
        },
        out);
}

/// Bulk-gather rows from `src` into a new column using the index array.
/// One std::visit per column, not per row — much faster for large gathers.
[[nodiscard]] inline auto gather_column(const ColumnValue& src, const std::size_t* indices,
                                        std::size_t n) -> ColumnValue {
    return std::visit(
        [&](const auto& col) -> ColumnValue {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                std::vector<Column<Categorical>::code_type> codes(n);
                const auto* sp = col.codes_data();
                for (std::size_t i = 0; i < n; ++i)
                    codes[i] = sp[indices[i]];
                return Column<Categorical>(col.dictionary_ptr(), col.index_ptr(), std::move(codes));
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                const auto* src_off = col.offsets_data();
                const char* src_char = col.chars_data();
                std::size_t total_chars = 0;
                for (std::size_t i = 0; i < n; ++i)
                    total_chars += src_off[indices[i] + 1] - src_off[indices[i]];
                ColT dst;
                dst.resize_for_gather(n, total_chars);
                auto* dst_off = dst.offsets_data();
                char* dst_char = dst.chars_data();
                dst_off[0] = 0;
                std::uint32_t cur = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    std::uint32_t len = src_off[indices[i] + 1] - src_off[indices[i]];
                    ::memcpy(dst_char + cur, src_char + src_off[indices[i]], len);
                    cur += len;
                    dst_off[i + 1] = cur;
                }
                return dst;
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                ColT dst;
                dst.resize(n);
                for (std::size_t i = 0; i < n; ++i)
                    dst.set(i, col[indices[i]]);
                return dst;
            } else {
                ColT dst;
                dst.resize(n);
                auto* dp = dst.data();
                const auto* sp = col.data();
                for (std::size_t i = 0; i < n; ++i)
                    dp[i] = sp[indices[i]];
                return dst;
            }
        },
        src);
}

/// Bulk-gather rows from `src`, treating sentinel values (kNull = SIZE_MAX) as null positions
/// that receive default values. Returns the new column plus a validity bitmap if any nulls exist.
[[nodiscard]] inline auto gather_column_with_nulls(const ColumnValue& src,
                                                   const std::size_t* indices, std::size_t n,
                                                   std::size_t kNull)
    -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
    return std::visit(
        [&](const auto& col) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
            using ColT = std::decay_t<decltype(col)>;
            bool has_null = false;
            for (std::size_t i = 0; i < n && !has_null; ++i)
                has_null = (indices[i] == kNull);
            if (!has_null) {
                return {gather_column(src, indices, n), std::nullopt};
            }
            ValidityBitmap bm(n, true);
            if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                std::vector<Column<Categorical>::code_type> codes(n);
                const auto* sp = col.codes_data();
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        codes[i] = sp[indices[i]];
                    } else {
                        codes[i] = 0;
                        bm.set(i, false);
                    }
                }
                return {
                    Column<Categorical>(col.dictionary_ptr(), col.index_ptr(), std::move(codes)),
                    std::move(bm)};
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                const auto* src_off = col.offsets_data();
                const char* src_char = col.chars_data();
                std::size_t total_chars = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull)
                        total_chars += src_off[indices[i] + 1] - src_off[indices[i]];
                }
                ColT dst;
                dst.resize_for_gather(n, total_chars);
                auto* dst_off = dst.offsets_data();
                char* dst_char = dst.chars_data();
                dst_off[0] = 0;
                std::uint32_t cur = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        std::uint32_t len = src_off[indices[i] + 1] - src_off[indices[i]];
                        ::memcpy(dst_char + cur, src_char + src_off[indices[i]], len);
                        cur += len;
                    } else {
                        bm.set(i, false);
                    }
                    dst_off[i + 1] = cur;
                }
                return {std::move(dst), std::move(bm)};
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                ColT dst;
                dst.resize(n, false);
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        dst.set(i, col[indices[i]]);
                    } else {
                        bm.set(i, false);
                    }
                }
                return {std::move(dst), std::move(bm)};
            } else {
                ColT dst;
                dst.resize(n);
                auto* dp = dst.data();
                const auto* sp = col.data();
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        dp[i] = sp[indices[i]];
                    } else {
                        dp[i] = {};
                        bm.set(i, false);
                    }
                }
                return {std::move(dst), std::move(bm)};
            }
        },
        src);
}

/// Append a default (zero / empty) value to a type-erased column.
inline auto append_default(ColumnValue& col) -> void {
    std::visit(
        [](auto& c) {
            using ColType = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                c.push_back(std::int64_t{0});
            } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                c.push_back(0.0);
            } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                c.push_back(std::string_view{});
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                c.push_code(0);
            } else {
                c.push_back({});
            }
        },
        col);
}

/// Append `count` default (zero / empty) values to a type-erased column.
inline auto append_defaults(ColumnValue& col, std::size_t count) -> void {
    std::visit(
        [count](auto& c) {
            using ColType = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                (void)c.insert(c.end(), count, std::int64_t{0});
            } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                (void)c.insert(c.end(), count, 0.0);
            } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_back(std::string_view{});
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_code(0);
            } else {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_back({});
            }
        },
        col);
}

[[nodiscard]] inline auto make_empty_like(const ColumnValue& src) -> ColumnValue {
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

}  // namespace ibex::runtime
