#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
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
