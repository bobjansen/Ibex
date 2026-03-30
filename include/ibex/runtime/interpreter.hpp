#pragma once

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>

#include <expected>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace ibex::runtime {

enum class ScalarKind : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
};

using ColumnValue =
    std::variant<Column<std::int64_t>, Column<double>, Column<std::string>, Column<Categorical>,
                 Column<Date>, Column<Timestamp>, Column<bool>>;
using ScalarValue = std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>;

/// Packed validity bitmap (1 bit per row): true = valid, false = null.
/// Designed for row-validity propagation where bulk bitwise ops dominate.
class ValidityBitmap {
   public:
    using word_type = std::uint64_t;
    using size_type = std::size_t;

   private:
    static constexpr size_type kBitsPerWord = sizeof(word_type) * 8;

    std::vector<word_type> words_;
    size_type size_bits_ = 0;

    static constexpr auto word_index(size_type bit) noexcept -> size_type {
        return bit / kBitsPerWord;
    }
    static constexpr auto bit_offset(size_type bit) noexcept -> size_type {
        return bit % kBitsPerWord;
    }
    static constexpr auto bit_mask(size_type bit) noexcept -> word_type {
        return word_type{1} << bit_offset(bit);
    }
    static constexpr auto words_for_bits(size_type bits) noexcept -> size_type {
        return (bits + kBitsPerWord - 1) / kBitsPerWord;
    }
    static constexpr auto low_bits_mask(size_type bits) noexcept -> word_type {
        if (bits == 0) {
            return word_type{0};
        }
        if (bits >= kBitsPerWord) {
            return ~word_type{0};
        }
        return (word_type{1} << bits) - 1;
    }

    auto clear_unused_tail_bits() noexcept -> void {
        if (words_.empty()) {
            return;
        }
        const size_type rem = bit_offset(size_bits_);
        if (rem == 0) {
            return;
        }
        words_.back() &= low_bits_mask(rem);
    }

   public:
    ValidityBitmap() = default;

    explicit ValidityBitmap(size_type count, bool value = false) { assign(count, value); }

    ValidityBitmap(const std::vector<bool>& values) {
        reserve(values.size());
        for (bool v : values) {
            push_back(v);
        }
    }

    ValidityBitmap(std::initializer_list<bool> init) {
        reserve(init.size());
        for (bool v : init) {
            push_back(v);
        }
    }

    [[nodiscard]] auto size() const noexcept -> size_type { return size_bits_; }
    [[nodiscard]] auto empty() const noexcept -> bool { return size_bits_ == 0; }
    [[nodiscard]] auto word_count() const noexcept -> size_type { return words_.size(); }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> bool {
        return (words_[word_index(idx)] & bit_mask(idx)) != 0;
    }

    auto set(size_type idx, bool value) noexcept -> void {
        auto& w = words_[word_index(idx)];
        const word_type m = bit_mask(idx);
        if (value) {
            w |= m;
        } else {
            w &= ~m;
        }
    }

    auto push_back(bool value) -> void {
        const size_type idx = size_bits_;
        if (bit_offset(idx) == 0) {
            words_.push_back(0);
        }
        if (value) {
            words_.back() |= bit_mask(idx);
        }
        ++size_bits_;
    }

    auto reserve(size_type count_bits) -> void { words_.reserve(words_for_bits(count_bits)); }

    auto resize(size_type count_bits) -> void { resize(count_bits, false); }

    auto resize(size_type count_bits, bool value) -> void {
        if (count_bits <= size_bits_) {
            size_bits_ = count_bits;
            words_.resize(words_for_bits(count_bits));
            clear_unused_tail_bits();
            return;
        }

        const size_type old_size = size_bits_;
        const size_type new_words = words_for_bits(count_bits);
        words_.resize(new_words, 0);
        size_bits_ = count_bits;
        if (value) {
            for (size_type i = old_size; i < count_bits; ++i) {
                set(i, true);
            }
        }
    }

    auto assign(size_type count_bits, bool value) -> void {
        size_bits_ = count_bits;
        words_.assign(words_for_bits(count_bits), value ? ~word_type{0} : word_type{0});
        clear_unused_tail_bits();
    }

    [[nodiscard]] auto words_data() noexcept -> word_type* { return words_.data(); }
    [[nodiscard]] auto words_data() const noexcept -> const word_type* { return words_.data(); }
};

struct ColumnEntry {
    std::string name;
    std::shared_ptr<ColumnValue> column;
    // Validity bitmap: true = valid (not null), false = null.
    // nullopt means every row is valid — the common case, with zero overhead.
    std::optional<ValidityBitmap> validity;
};

/// Returns true if row `row` of `entry` is null.
[[nodiscard]] inline auto is_null(const ColumnEntry& entry, std::size_t row) -> bool {
    return entry.validity.has_value() && !(*entry.validity)[row];
}

struct Table {
    std::vector<ColumnEntry> columns;
    std::unordered_map<std::string, std::size_t> index;
    std::optional<std::vector<ir::OrderKey>> ordering;
    std::optional<std::string> time_index;

    void add_column(std::string name, ColumnValue column);
    /// Add a column with an explicit validity bitmap (true = valid, false = null).
    void add_column(std::string name, ColumnValue column, ValidityBitmap validity);
    [[nodiscard]] auto find(const std::string& name) -> ColumnValue*;
    [[nodiscard]] auto find(const std::string& name) const -> const ColumnValue*;
    [[nodiscard]] auto find_entry(const std::string& name) const -> const ColumnEntry*;
    [[nodiscard]] auto rows() const noexcept -> std::size_t;
};

using TableRegistry = std::unordered_map<std::string, Table>;
using ScalarRegistry = std::unordered_map<std::string, ScalarValue>;

/// Opaque model result produced by the `model { ... }` clause.
/// Accessor functions (`coef`, `residuals`, `fitted`, `summary`) extract
/// sub-tables; `predict` applies the stored formula to new data.
struct ModelResult {
    Table coefficients;   ///< term | estimate
    Table summary;        ///< term | estimate | std_error | t_stat | p_value
    Table fitted_values;  ///< single column: fitted
    Table residuals;      ///< single column: residual
    ir::ModelFormula formula;
    std::string method;
    double r_squared = 0.0;
    double adj_r_squared = 0.0;
    std::size_t n_obs = 0;
    std::size_t n_params = 0;
};

using ModelRegistry = std::unordered_map<std::string, ModelResult>;

/// Interpret an IR node tree against a table registry.
class ExternRegistry;

[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry,
                             const ScalarRegistry* scalars = nullptr,
                             const ExternRegistry* externs = nullptr,
                             ModelResult* model_out = nullptr) -> std::expected<Table, std::string>;

[[nodiscard]] auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                               const std::vector<std::string>& keys,
                               const ir::FilterExpr* predicate = nullptr,
                               const ScalarRegistry* scalars = nullptr)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string>;

/// Merge two validity bitmaps (`a && b`) for the first `n` rows.
/// Returns nullopt when both inputs are nullopt-equivalent (nullptr).
/// Exposed for micro-benchmarking and runtime-level utilities.
[[nodiscard]] auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b,
                                          std::size_t n) -> std::optional<ValidityBitmap>;

}  // namespace ibex::runtime
