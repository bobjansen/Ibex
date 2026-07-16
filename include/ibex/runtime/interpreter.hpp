#pragma once

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
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
        for (const bool v : values) {
            push_back(v);
        }
    }

    ValidityBitmap(std::initializer_list<bool> init) {
        reserve(init.size());
        for (const bool v : init) {
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

/// Returns the number of elements in a type-erased column.
[[nodiscard]] inline auto column_size(const ColumnValue& column) noexcept -> std::size_t {
    return std::visit([](const auto& col) { return col.size(); }, column);
}

struct Table {
    std::vector<ColumnEntry> columns;
    robin_hood::unordered_map<std::string, std::size_t> index;
    std::optional<std::vector<ir::OrderKey>> ordering;
    std::optional<std::string> time_index;
    /// Logical row count for a column-less frame (e.g. produced by `Table(n)`).
    /// Only consulted by `rows()` when `columns` is empty; once any column is
    /// added the count is derived from the columns as usual.
    std::optional<std::size_t> logical_rows;

    void add_column(std::string name, ColumnValue column);
    /// Add a column with an explicit validity bitmap (true = valid, false = null).
    void add_column(std::string name, ColumnValue column, ValidityBitmap validity);
    /// Replace the storage for an existing column, preserving its name and validity.
    /// This keeps copy-on-write explicit: callers reseat the column handle rather
    /// than mutating a potentially shared ColumnValue in place.
    void replace_column(std::size_t pos, ColumnValue column);
    /// Replace the storage and validity for an existing column.
    void replace_column(std::size_t pos, ColumnValue column,
                        std::optional<ValidityBitmap> validity);
    /// Rename an existing column and keep the index map in sync.
    void rename_column(std::size_t pos, std::string name);
    /// Return a mutable column after detaching shared storage if necessary.
    [[nodiscard]] auto mutable_column(std::size_t pos) -> ColumnValue&;
    /// Share an existing column without copying its data. Safe under the
    /// copy-on-write invariant: shared columns are never mutated in place —
    /// any modification reseats a fresh shared_ptr (see add_column above).
    /// Used by zero-copy projection/rename to avoid deep-copying key columns.
    void add_column_shared(std::string name, std::shared_ptr<ColumnValue> column,
                           std::optional<ValidityBitmap> validity = std::nullopt);
    [[nodiscard]] auto find(const std::string& name) -> ColumnValue*;
    [[nodiscard]] auto find(const std::string& name) const -> const ColumnValue*;
    [[nodiscard]] auto find_entry(const std::string& name) const -> const ColumnEntry*;
    [[nodiscard]] auto rows() const noexcept -> std::size_t {
        if (columns.empty()) {
            return logical_rows.value_or(0);
        }
        return column_size(*columns.front().column);
    }
};

using TableRegistry = robin_hood::unordered_map<std::string, Table>;
using ScalarRegistry = robin_hood::unordered_map<std::string, ScalarValue>;

/// Opaque model result produced by the `model { ... }` clause.
/// Accessor functions (`coef`, `residuals`, `fitted`, `summary`) extract
/// sub-tables; `predict` applies the stored formula to new data.
struct ModelResult {
    Table coefficients;   ///< term | estimate (empty for non-linear plugins)
    Table summary;        ///< term | estimate | std_error | t_stat | p_value
    Table fitted_values;  ///< single column: fitted
    Table residuals;      ///< single column: residual
    Table importance;     ///< term | gain (tree models; empty otherwise)
    /// Opaque, self-freeing handle to a plugin-owned native model (e.g. a
    /// LightGBM booster), set by model plugins. Reused by model_predict; null
    /// for built-in linear methods. See ExternRegistry::ModelOps.
    std::shared_ptr<void> native;
    ir::ModelFormula formula;
    std::string method;
    double r_squared = 0.0;
    double adj_r_squared = 0.0;
    std::size_t n_obs = 0;
    std::size_t n_params = 0;
};

using ModelRegistry = robin_hood::unordered_map<std::string, ModelResult>;

/// Interpret an IR node tree against a table registry.
class ExternRegistry;

[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry,
                             const ScalarRegistry* scalars = nullptr,
                             const ExternRegistry* externs = nullptr,
                             ModelResult* model_out = nullptr) -> std::expected<Table, std::string>;

/// Invoke an extern whose first argument is a table. The scalar result, if
/// any, is intentionally discarded: this API is the execution seam for
/// top-level script effects such as write_csv and write_parquet.
[[nodiscard]] auto invoke_table_consumer(const ExternRegistry& externs, const std::string& callee,
                                         const Table& input, const std::vector<ScalarValue>& args)
    -> std::expected<void, std::string>;

/// Evaluate row-local filter conjuncts and return the surviving row indices in
/// ascending order. Null predicate values do not survive (the same three-valued
/// logic used by a Filter node). This is the seam used by deferred file readers
/// to late-materialize non-predicate columns without duplicating expression
/// evaluation inside an I/O plugin. Later conjuncts compact their referenced
/// columns once earlier conjuncts have made the candidate set selective.
[[nodiscard]] auto filter_selection(const Table& input, const std::vector<ir::Expr>& conjuncts,
                                    const ScalarRegistry* scalars = nullptr)
    -> std::expected<std::vector<std::size_t>, std::string>;

/// Predicts on new data with a previously fitted plugin model, reusing its
/// native handle. Rebuilds the design matrix from `newdata` using the model's
/// stored formula. Returns a single-column "prediction" table.
[[nodiscard]] auto predict_model(const ModelResult& model, const Table& newdata,
                                 const ExternRegistry& externs)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto join_tables(const Table& left, const Table& right, ir::JoinKind kind,
                               const std::vector<std::string>& keys,
                               const ir::Expr* predicate = nullptr,
                               const ScalarRegistry* scalars = nullptr)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string>;

/// Row-wise scalar builtins (abs, sqrt, the transcendentals, ceil/floor/trunc,
/// date parts, pmin/pmax, is_nan, casts) live in one registry shared by the
/// table-expression evaluators. These expose that registry so a scalar-only
/// caller (the REPL's scalar evaluator) can route to it instead of maintaining
/// a parallel table. `is_scalar_builtin` reports membership; `eval_scalar_builtin`
/// applies the builtin to already-evaluated scalar arguments (validating arity).
[[nodiscard]] auto is_scalar_builtin(std::string_view name) -> bool;
[[nodiscard]] auto eval_scalar_builtin(std::string_view name, const std::vector<ScalarValue>& args)
    -> std::expected<ScalarValue, std::string>;

/// Reduce a whole series to a scalar with an aggregate function (sum, mean, min,
/// max, count, median, std, first, last, skew, kurtosis, and ewma/quantile with
/// `param`). Lets `max(series)` etc. work in scalar position — distinct from the
/// element-wise `pmax`/`pmin` (which are `is_scalar_builtin`).
[[nodiscard]] auto aggregate_series(std::string_view name, const ColumnValue& column,
                                    double param = 0.0) -> std::expected<ScalarValue, std::string>;

[[nodiscard]] auto evaluate_row_count_expr(const ir::Expr& expr,
                                           const ScalarRegistry* scalars = nullptr,
                                           const ExternRegistry* externs = nullptr)
    -> std::expected<std::size_t, std::string>;

/// Merge two validity bitmaps (`a && b`) for the first `n` rows.
/// Returns nullopt when both inputs are nullopt-equivalent (nullptr).
/// Exposed for micro-benchmarking and runtime-level utilities.
[[nodiscard]] auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b,
                                          std::size_t n) -> std::optional<ValidityBitmap>;

}  // namespace ibex::runtime
