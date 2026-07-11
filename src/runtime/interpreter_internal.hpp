#pragma once

// interpreter_internal.hpp — shared internal surface of the interpreter TUs.
//
// The interpreter was originally one translation unit; it is now split into
// per-operator TUs (filter.cpp, sort.cpp, aggregate.cpp, window.cpp,
// update.cpp, expr.cpp, chunked.cpp, interpreter.cpp). Everything declared
// here crosses a TU boundary. The split boundaries are per-operator / per-
// column calls (one call per query node or per evaluated field), never
// per-row, so the loss of cross-boundary inlining is not performance-
// relevant. Helpers that ARE called per row from more than one TU are
// defined inline in this header (append_scalar, the AggSlot accumulators,
// gather_rows) so they keep inlining exactly as before the split.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <expected>
#include <functional>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

// Abort with a diagnostic on a broken internal invariant (defined in
// interpreter.cpp).
[[noreturn]] void invariant_violation(std::string_view detail);

struct ComputedColumn {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

// Column result: either a pointer into the table (zero-copy) or an owned computed column,
// plus optional validity tracking for null propagation.
struct ColResult {
    std::variant<const ColumnValue*, ColumnValue> data;
    const ValidityBitmap* validity = nullptr;      // source column validity (no-copy)
    std::optional<ValidityBitmap> owned_validity;  // for computed expressions

    explicit ColResult(const ColumnValue* p) : data(p) {}
    explicit ColResult(ColumnValue v) : data(std::move(v)) {}
    ColResult(ColumnValue v, std::optional<ValidityBitmap> ov)
        : data(std::move(v)), owned_validity(std::move(ov)) {}

    [[nodiscard]] const ValidityBitmap* get_validity() const noexcept {
        return owned_validity ? &*owned_validity : validity;
    }
};

inline auto deref_col(const ColResult& r) -> const ColumnValue& {
    return std::visit(
        [](const auto& v) -> const ColumnValue& {
            if constexpr (std::is_same_v<std::decay_t<decltype(v)>, const ColumnValue*>) {
                return *v;
            } else {
                return v;  // NOLINT(bugprone-return-const-ref-from-parameter) — rvalue overloads
                           // are deleted below
            }
        },
        r.data);
}

// Deleted rvalue overloads: deref_col may return a reference into the argument,
// so a temporary ColResult must not be passed.
auto deref_col(ColResult&&) -> const ColumnValue& = delete;
auto deref_col(const ColResult&&) -> const ColumnValue& = delete;

struct LagLeadResult {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

template <typename T>
constexpr bool is_string_like_v =
    std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>;

// Grouping key for the row-wise hash-grouping paths (grouped update/aggregate/
// distinct/head/tail and the chunked operators).
//
// Deliberately in an anonymous namespace *in this header*: each TU gets its own
// internal-linkage copy of Key/KeyHash/KeyEq and therefore its own internal
// robin_hood table instantiations. With external linkage the hash/emplace path
// stopped inlining (linkonce_odr symbols must be emitted anyway, so LLVM
// inlines them less aggressively than internal ones), costing ~15% on the
// grouped-update benchmarks. Key never appears in a cross-TU function
// signature, so per-TU distinct types are safe.
namespace {  // NOLINT(cert-dcl59-cpp,misc-anonymous-namespace-in-header) — deliberate per-TU
             // internal linkage, see comment above

struct Key {
    std::vector<ScalarValue> values;
};

struct KeyHash {
    auto operator()(const Key& key) const -> std::size_t {
        std::size_t seed = 0;
        auto hash_combine = [&](std::size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        for (const auto& value : key.values) {
            const std::size_t h = std::visit(
                [](const auto& v) { return std::hash<std::decay_t<decltype(v)>>{}(v); }, value);
            hash_combine(h);
        }
        return seed;
    }
};

struct KeyEq {
    auto operator()(const Key& a, const Key& b) const -> bool { return a.values == b.values; }
};

}  // namespace

/// The per-row null: a cell whose value is missing (validity bit clear).
/// plans/exprvalue-null-arm-plan.md — the alternative exists so the per-row
/// evaluator can represent missing cells directly instead of computing on
/// undefined payloads and masking afterwards. ScalarValue (REPL scalars,
/// extern args) deliberately stays null-free; convert at the boundary with
/// the helpers below.
struct Null {
    auto operator==(const Null&) const -> bool = default;
};

using ExprValue = std::variant<Null, std::int64_t, double, bool, std::string, Date, Timestamp>;

/// ScalarValue -> ExprValue: always valid (ScalarValue is the null-free subset).
inline auto expr_from_scalar(const ScalarValue& v) -> ExprValue {
    return std::visit([](const auto& x) -> ExprValue { return x; }, v);
}

/// ExprValue -> ScalarValue: nullopt on Null (no scalar image). Callers at
/// the REPL/extern boundary decide how to surface that.
inline auto scalar_from_expr(const ExprValue& v) -> std::optional<ScalarValue> {
    return std::visit(
        [](const auto& x) -> std::optional<ScalarValue> {
            if constexpr (std::is_same_v<std::decay_t<decltype(x)>, Null>) {
                return std::nullopt;
            } else {
                return ScalarValue{x};
            }
        },
        v);
}

struct AggSlot {
    ir::AggFunc func = ir::AggFunc::Sum;
    ExprType kind = ExprType::Int;
    bool has_value = false;
    std::int64_t count = 0;
    std::int64_t int_value = 0;
    double double_value = 0.0;
    double sum = 0.0;
    double m2 = 0.0;     ///< Welford M2 accumulator: Σ(x-mean)². `double_value`
                         ///< doubles as the running mean for the moment aggs.
    double m3 = 0.0;     ///< Σ(x-mean)³ (online), for skewness.
    double m4 = 0.0;     ///< Σ(x-mean)⁴ (online), for kurtosis.
    double param = 0.0;  ///< Function-specific parameter (e.g. EWMA alpha).
    ScalarValue first_value;
    ScalarValue last_value;
    std::vector<double> values;  ///< Collected values for median.
};

// Online central-moment accumulators (Welford / Pébay), shared by the chunked
// aggregate operators. `double_value` holds the running mean; m2/m3/m4 hold
// Σ(x-mean)^k. These match the two-pass central moments the materializing
// aggregate computes to within floating-point rounding — and for stddev the
// M2 update is bit-identical (Pébay's term1 reduces to the simple Welford
// step), so `stddev` results agree exactly across paths.
inline void agg_update_stddev(AggSlot& slot, double x) {
    slot.count += 1;
    const double delta = x - slot.double_value;
    slot.double_value += delta / static_cast<double>(slot.count);
    slot.m2 += delta * (x - slot.double_value);
}

// Full m2/m3/m4 update for skewness/kurtosis (Pébay single-value recurrence).
// Updates m4 and m3 before m2 because they read the pre-update accumulators.
inline void agg_update_moments(AggSlot& slot, double x) {
    const auto n1 = static_cast<double>(slot.count);
    slot.count += 1;
    const auto n = static_cast<double>(slot.count);
    const double delta = x - slot.double_value;
    const double delta_n = delta / n;
    const double delta_n2 = delta_n * delta_n;
    const double term1 = delta * delta_n * n1;
    slot.double_value += delta_n;
    slot.m4 += (term1 * delta_n2 * ((n * n) - (3.0 * n) + 3.0)) + (6.0 * delta_n2 * slot.m2) -
               (4.0 * delta_n * slot.m3);
    slot.m3 += (term1 * delta_n * (n - 2.0)) - (3.0 * delta_n * slot.m2);
    slot.m2 += term1;
}

inline auto agg_finalize_stddev(const AggSlot& slot) -> double {
    return slot.count < 2 ? 0.0 : std::sqrt(slot.m2 / static_cast<double>(slot.count - 1));
}

inline auto agg_finalize_skew(const AggSlot& slot) -> double {
    if (slot.count < 3 || slot.m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Fisher–Pearson sample skewness (matches pandas/scipy default).
    return (n * std::sqrt(n - 1.0) / (n - 2.0)) * (slot.m3 / std::pow(slot.m2, 1.5));
}

inline auto agg_finalize_kurtosis(const AggSlot& slot) -> double {
    if (slot.count < 4 || slot.m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Unbiased Fisher excess kurtosis (matches pandas/scipy default).
    return (n - 1.0) / ((n - 2.0) * (n - 3.0)) *
           (((n + 1.0) * n * slot.m4 / (slot.m2 * slot.m2)) - (3.0 * (n - 1.0)));
}

// Whether a streamed aggregate slot has enough observations to be non-null.
// Mirrors the materializing aggregate's `agg_result_is_valid`.
inline auto chunked_agg_valid(ir::AggFunc func, const AggSlot& slot) -> bool {
    switch (func) {
        case ir::AggFunc::Mean:
            return slot.count > 0;
        case ir::AggFunc::Sum:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::First:
        case ir::AggFunc::Last:
            return slot.has_value;
        case ir::AggFunc::Stddev:
            return slot.count >= 2;
        case ir::AggFunc::Skew:
            return slot.count >= 3;
        case ir::AggFunc::Kurtosis:
            return slot.count >= 4;
        default:  // Count
            return true;
    }
}

// Whether a streamed aggregate carries a validity bitmap at all (Count never
// produces nulls; the value-bearing aggs may).
inline auto chunked_agg_tracks_validity(ir::AggFunc func) -> bool {
    switch (func) {
        case ir::AggFunc::Sum:
        case ir::AggFunc::Mean:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::First:
        case ir::AggFunc::Last:
        case ir::AggFunc::Stddev:
        case ir::AggFunc::Skew:
        case ir::AggFunc::Kurtosis:
            return true;
        default:
            return false;
    }
}

struct AggState {
    std::vector<AggSlot> slots;
};

struct BroadcastAggregateColumn {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

// Evaluation context threaded to whole-column builtins. Generators ignore it;
// Transforms need the scalar/extern registries (lag/lead default arguments)
// and, for rolling_*, the enclosing `window` clause's duration (a per-call
// window argument overrides it; with neither the kernel errors).
struct ColumnEvalCtx {
    const ScalarRegistry* scalars = nullptr;
    const ExternRegistry* externs = nullptr;
    std::optional<ir::Duration> window;
};

// How a Scalar builtin's per-row `eval` meets a Null argument
// (plans/exprvalue-null-arm-plan.md, stage 3).
enum class NullPolicy : std::uint8_t {
    Propagate,  // default: any Null argument -> Null result; eval never sees Null
    Handles,    // eval receives Null arguments and decides (coalesce, fill_null, ...)
};

// Builtin function registry entry (registry lives in expr.cpp; type inference
// and evaluation dispatch through it). Generalizes the former scalar-only
// registry per plans/function-kind-registry-plan.md: every builtin declares its
// `kind` plus a kind-appropriate evaluator — `eval` for row-local Scalar
// builtins, `column_eval` for whole-column kinds (Generator and Transform),
// aggregate builtins are registry-visible for inference/classification, while
// aggregate execution keeps its compact name-to-enum mapping in aggregate.cpp.
// A Scalar entry may carry BOTH `eval`
// and `column_eval`: the column kernel is then a whole-column fast path used
// when the call is a top-level field / value leaf, and the per-row eval is
// the general (and semantic-reference) form. Only column-ONLY entries
// (eval == nullptr) force the enclosing expression onto the vectorized path.
struct BuiltinFn {
    ir::FnKind kind = ir::FnKind::Scalar;
    NullPolicy null_policy = NullPolicy::Propagate;
    int min_args = 1;
    int max_args = 1;  // -1 == variadic
    std::expected<ExprType, std::string> (*infer)(std::string_view, const std::vector<ExprType>&){};
    // Scalar (row-local) evaluation: args at row i -> value at row i.
    std::expected<ExprValue, std::string> (*eval)(std::string_view,
                                                  const std::vector<ExprValue>&){};
    // Whole-column evaluation (Generator/Transform): the raw call (for arg
    // literals / named args), the input table, the output row count, and the
    // evaluation context.
    std::expected<ComputedColumn, std::string> (*column_eval)(const ir::CallExpr&, const Table&,
                                                              std::size_t rows,
                                                              const ColumnEvalCtx&){};
};

// ── Inline helpers shared by per-row/per-group loops in several TUs ──────────

inline auto append_scalar(ColumnValue& column, const ScalarValue& value) -> void {
    std::visit(
        [&](auto& col) {
            using ColType = std::decay_t<decltype(col)>;
            using ValueType = ColType::value_type;
            if constexpr (std::is_same_v<ValueType, std::int64_t>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value);
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(static_cast<std::int64_t>(*double_value));
                } else {
                    invariant_violation("append_scalar: expected Int64-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, double>) {
                if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(static_cast<double>(*int_value));
                } else if (const auto* double_value = std::get_if<double>(&value)) {
                    col.push_back(*double_value);
                } else {
                    invariant_violation("append_scalar: expected Float64-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, bool>) {
                if (const auto* bool_value = std::get_if<bool>(&value)) {
                    col.push_back(*bool_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(*int_value != 0);
                } else {
                    invariant_violation("append_scalar: expected Bool-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, std::string_view>) {
                // Column<std::string> flat-buffer specialization uses value_type=string_view.
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    invariant_violation("append_scalar: expected String scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, Date>) {
                if (const auto* date_value = std::get_if<Date>(&value)) {
                    col.push_back(*date_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(int64_to_date_checked(*int_value));
                } else {
                    invariant_violation("append_scalar: expected Date-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ValueType, Timestamp>) {
                if (const auto* ts_value = std::get_if<Timestamp>(&value)) {
                    col.push_back(*ts_value);
                } else if (const auto* int_value = std::get_if<std::int64_t>(&value)) {
                    col.push_back(Timestamp{*int_value});
                } else {
                    invariant_violation("append_scalar: expected Timestamp-compatible scalar");
                }
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                if (const auto* str_value = std::get_if<std::string>(&value)) {
                    col.push_back(*str_value);
                } else {
                    invariant_violation("append_scalar: expected String scalar for Categorical");
                }
            }
        },
        column);
}

inline auto broadcast_scalar_column(const ScalarValue& value, std::size_t rows) -> ColumnValue {
    return std::visit(
        [rows](const auto& v) -> ColumnValue {
            using V = std::decay_t<decltype(v)>;
            Column<V> col;
            col.resize(rows, v);
            return ColumnValue{std::move(col)};
        },
        value);
}

inline auto scalar_kind_from_value(const ScalarValue& value) -> ExprType {
    if (std::holds_alternative<std::int64_t>(value)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<double>(value)) {
        return ExprType::Double;
    }
    if (std::holds_alternative<bool>(value)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Date>(value)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Timestamp>(value)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

inline auto scalar_from_literal(const ir::Literal& literal) -> ScalarValue {
    return std::visit([](const auto& v) -> ScalarValue { return v; }, literal.value);
}

/// Gather `idx`-selected rows of `input` into a new table (one visit per
/// column). Idx is uint32_t for tables that fit, uint64_t otherwise. Used by
/// the sort/head/tail paths, grouped update, and the chunked operators.
template <typename Idx>
auto gather_rows(const Table& input, const std::vector<Idx>& idx,
                 const std::vector<ir::OrderKey>* ordering = nullptr) -> Table {
    const std::size_t rows = idx.size();
    Table output;
    output.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        ColumnValue gathered = std::visit(
            [&](const auto& src) -> ColumnValue {
                using ColT = std::decay_t<decltype(src)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    std::vector<Column<Categorical>::code_type> codes(rows);
                    const auto* sp = src.codes_data();
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        codes[pos] = sp[static_cast<std::size_t>(idx[pos])];
                    return Column<Categorical>(src.dictionary_ptr(), src.index_ptr(),
                                               std::move(codes));
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    std::size_t total_chars = 0;
                    const auto* src_off = src.offsets_data();
                    const auto* src_char = src.chars_data();
                    for (std::size_t pos = 0; pos < rows; ++pos) {
                        auto si = static_cast<std::size_t>(idx[pos]);
                        total_chars += src_off[si + 1] - src_off[si];
                    }
                    ColT dst;
                    dst.resize_for_gather(rows, total_chars);
                    auto* dst_off = dst.offsets_data();
                    auto* dst_char = dst.chars_data();
                    dst_off[0] = 0;
                    std::uint32_t cur = 0;
                    for (std::size_t pos = 0; pos < rows; ++pos) {
                        auto si = static_cast<std::size_t>(idx[pos]);
                        std::uint32_t len = src_off[si + 1] - src_off[si];
                        std::memcpy(dst_char + cur, src_char + src_off[si], len);
                        cur += len;
                        dst_off[pos + 1] = cur;
                    }
                    return dst;
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    ColT dst;
                    dst.resize(rows);
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        dst.set(pos, src[static_cast<std::size_t>(idx[pos])]);
                    return dst;
                } else {
                    ColT dst;
                    dst.resize(rows);
                    for (std::size_t pos = 0; pos < rows; ++pos)
                        dst[pos] = src[static_cast<std::size_t>(idx[pos])];
                    return dst;
                }
            },
            *entry.column);
        output.add_column(entry.name, std::move(gathered));
        if (entry.validity.has_value()) {
            const auto& src_bm = *entry.validity;
            ValidityBitmap dst_bm(rows, false);
            for (std::size_t pos = 0; pos < rows; ++pos)
                dst_bm.set(pos, src_bm[static_cast<std::size_t>(idx[pos])]);
            output.columns.back().validity = std::move(dst_bm);
        }
    }

    if (ordering != nullptr) {
        output.ordering = *ordering;
    } else {
        output.ordering = input.ordering;
    }
    output.time_index = input.time_index;
    normalize_time_index(output);
    return output;
}

// ── Cross-TU function declarations ───────────────────────────────────────────

// interpreter.cpp — dispatcher, small table ops, registries.
[[nodiscard]] auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  ModelResult* model_out = nullptr)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto ordering_keys_present(
    const std::vector<ir::OrderKey>& keys,
    const robin_hood::unordered_map<std::string, std::size_t>& index) -> bool;
[[nodiscard]] auto ordering_keys_for_table(const Table& input,
                                           const std::vector<ir::OrderKey>& keys)
    -> std::vector<ir::OrderKey>;
[[nodiscard]] auto format_tables(const TableRegistry& registry) -> std::string;
[[nodiscard]] auto expr_type_for_column(const ColumnValue& column) -> ExprType;
[[nodiscard]] auto project_table(const Table& input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto rename_table(const Table& input, const std::vector<ir::RenameSpec>& renames)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto columns_table(const Table& input) -> std::expected<Table, std::string>;
[[nodiscard]] auto distinct_table(const Table& input) -> std::expected<Table, std::string>;

// filter.cpp — vectorized predicate evaluation and filtering.
[[nodiscard]] auto compute_mask(const ir::Expr& expr, const Table& table,
                                const ScalarRegistry* scalars, std::size_t n)
    -> std::expected<Mask, std::string>;
// coalesce kernel (validity-aware Transform; args evaluated via eval_value_vec).
[[nodiscard]] auto eval_coalesce_column(const ir::CallExpr& call, const Table& input,
                                        const ScalarRegistry* scalars, std::size_t rows)
    -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto eval_value_vec(const ir::Expr& expr, const Table& table,
                                  const ScalarRegistry* scalars, std::size_t n)
    -> std::expected<ColResult, std::string>;
[[nodiscard]] auto arith_vec(ir::ArithmeticOp op, const ColumnValue& lhs, const ColumnValue& rhs,
                             std::size_t n) -> std::expected<ColumnValue, std::string>;
[[nodiscard]] auto merge_validity(const ValidityBitmap* a, const ValidityBitmap* b, std::size_t n)
    -> std::optional<ValidityBitmap>;
[[nodiscard]] auto collect_expr_validity(const ir::Expr& expr, const Table& table, std::size_t n)
    -> std::optional<ValidityBitmap>;
[[nodiscard]] auto filter_table(const Table& input, const ir::Expr& predicate,
                                const ScalarRegistry* scalars) -> std::expected<Table, std::string>;
[[nodiscard]] auto filter_project_table(const Table& input, const ir::Expr& predicate,
                                        const std::vector<ir::ColumnRef>& columns,
                                        const ScalarRegistry* scalars)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto filter_table_limit(const Table& input, const ir::Expr& predicate,
                                      std::size_t row_limit, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string>;

// sort.cpp — ordering, head/tail.
// Index type for radix-sorted permutations: uint32_t for tables that fit,
// uint64_t otherwise. Keys are taken by move — the caller's u64 buffer is
// consumed, no copy.
using SortIdx = std::variant<std::vector<std::uint32_t>, std::vector<std::uint64_t>>;
[[nodiscard]] auto radix_sort_u64_asc(std::vector<std::uint64_t> keys, std::size_t rows) -> SortIdx;

// Map an IEEE-754 double to a uint64 whose unsigned order matches ascending
// double order, so radix_sort_u64_asc can sort doubles directly. For positive
// values flip the sign bit; for negatives flip all bits. NaNs (sign bit clear)
// sort to the end. The transform is a bijection, so radix stays stable.
inline auto double_to_sortable_u64(double value) -> std::uint64_t {
    const auto bits = std::bit_cast<std::uint64_t>(value);
    return bits ^ ((static_cast<std::uint64_t>(-static_cast<std::int64_t>(bits >> 63))) |
                   (std::uint64_t{1} << 63));
}

[[nodiscard]] auto order_table(const Table& input, const std::vector<ir::OrderKey>& keys)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto head_table(const Table& input, std::size_t count,
                              const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto tail_table(const Table& input, std::size_t count,
                              const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string>;

// aggregate.cpp — grouped/global aggregation.
[[nodiscard]] auto aggregate_table(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                                   const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto parse_aggregate_func(std::string_view name) -> std::optional<ir::AggFunc>;
[[nodiscard]] auto aggregate_call_to_spec(const ir::CallExpr& call, std::string alias)
    -> std::expected<std::optional<ir::AggSpec>, std::string>;
[[nodiscard]] auto expr_contains_aggregate_call(const ir::Expr& expr) -> bool;
// The scalar-collapse pair returns ExprValue so a null aggregate result (an
// all-null group has no mean/first/...) is carried as Null instead of a
// garbage payload; callers broadcast Null as an all-invalid column.
[[nodiscard]] auto eval_aggregate_call_scalar(const ir::CallExpr& node, const Table& input,
                                              const ScalarRegistry* scalars)
    -> std::expected<ExprValue, std::string>;
[[nodiscard]] auto eval_aggregate_scalar(const ir::Expr& expr, const Table& input,
                                         const ScalarRegistry* scalars)
    -> std::expected<ExprValue, std::string>;
[[nodiscard]] auto expr_has_bare_column(const ir::Expr& expr) -> bool;
[[nodiscard]] auto fold_aggregates_to_columns(ir::Expr& expr, const Table& group_input,
                                              Table& working, const ScalarRegistry* scalars,
                                              int& counter) -> std::expected<void, std::string>;
[[nodiscard]] auto broadcast_aggregate_column(const Table& input, const ir::FieldSpec& field,
                                              const ScalarRegistry* scalars)
    -> std::expected<std::optional<BroadcastAggregateColumn>, std::string>;

// window.cpp — rolling aggregates and resampling.

/// A rolling window specified by a fixed number of preceding rows (inclusive of
/// the current row). Needs no time index — valid on any ordered frame.
struct CountWindow {
    std::int64_t n;
};

/// The window a rolling aggregate spans: either a time `Duration` (requires a
/// TimeFrame) or a `CountWindow` of the last N rows.
using WindowSpec = std::variant<ir::Duration, CountWindow>;

/// Resolve the effective window for a rolling call. Reads the sentinel named
/// args attached by lowering (`__window_n` → count, `__window_ns` → duration in
/// nanoseconds). If neither is present, falls back to `block_default` (the
/// enclosing `window` clause); if that is also absent, returns an error.
[[nodiscard]] auto rolling_window_spec(const ir::CallExpr& call,
                                       std::optional<ir::Duration> block_default)
    -> std::expected<WindowSpec, std::string>;

[[nodiscard]] auto apply_rolling_func(const ir::CallExpr& call, const Table& table, WindowSpec spec)
    -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto resample_table(const Table& input, ir::Duration bucket_dur,
                                  const std::vector<ir::ColumnRef>& extra_group_by,
                                  const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string>;

// update.cpp — update/select field application (incl. fast numeric paths).
[[nodiscard]] auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto grouped_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                        const std::vector<ir::ColumnRef>& group_by,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                         ir::Duration duration, const ScalarRegistry* scalars,
                                         const ExternRegistry* externs)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto grouped_windowed_update_table(
    Table input, const std::vector<ir::FieldSpec>& fields, ir::Duration duration,
    const std::vector<ir::ColumnRef>& group_by, const ScalarRegistry* scalars,
    const ExternRegistry* externs) -> std::expected<Table, std::string>;
[[nodiscard]] auto apply_guarded_update(Table input, const ir::UpdateNode& update,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto try_fast_update_numeric_expr(const ir::Expr& expr, const Table& input,
                                                std::size_t rows, ExprType output_kind,
                                                const ScalarRegistry* scalars)
    -> std::optional<ColumnValue>;

// expr.cpp — builtin-function registry, type inference, per-row and per-field
// expression evaluation, lag/lead/fill/cum transforms, RNG/rep generators.

struct FillResult {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;  // nullopt = all rows valid
};

enum class FloatCleanMode : std::uint8_t {
    NullIfNan,
    NullIfNotFinite,
};

[[nodiscard]] auto eval_cumsum_cumprod_column(const ir::CallExpr& call, const Table& input,
                                              bool is_prod)
    -> std::expected<ColumnValue, std::string>;
[[nodiscard]] auto eval_fill_null(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string>;
[[nodiscard]] auto eval_fill_forward(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string>;
[[nodiscard]] auto eval_fill_backward(const ir::CallExpr& call, const Table& input)
    -> std::expected<FillResult, std::string>;
[[nodiscard]] auto eval_float_clean(const ir::CallExpr& call, const Table& input,
                                    FloatCleanMode mode) -> std::expected<FillResult, std::string>;
[[nodiscard]] auto builtins() -> const robin_hood::unordered_map<std::string_view, BuiltinFn>&;
// Registry lookup by callee name; nullptr when `name` is not a builtin.
[[nodiscard]] auto find_builtin(std::string_view name) -> const BuiltinFn*;

// Should this call go to the entry's whole-column kernel? Column-only entries
// (no per-row eval) always do. Hybrid Scalar entries (fill_null/null_if_*/
// coalesce keep their kernels as fast paths) use the kernel only for the
// kernel-shaped call — every positional argument a bare column or literal;
// computed arguments evaluate per-row via the NullPolicy::Handles eval.
[[nodiscard]] inline auto use_column_kernel(const BuiltinFn& fn, const ir::CallExpr& call) -> bool {
    if (fn.column_eval == nullptr) {
        return false;
    }
    if (fn.eval == nullptr) {
        return true;
    }
    return std::ranges::all_of(call.args, [](const auto& a) {
        return std::holds_alternative<ir::ColumnRef>(a->node) ||
               std::holds_alternative<ir::Literal>(a->node);
    });
}
[[nodiscard]] auto infer_expr_type(const ir::Expr& expr, const Table& input,
                                   const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ExprType, std::string>;
[[nodiscard]] auto eval_expr(const ir::Expr& expr, const Table& input, std::size_t row,
                             const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ExprValue, std::string>;
[[nodiscard]] auto evaluate_row_count_expr_impl(const ir::Expr& expr, const ScalarRegistry* scalars,
                                                const ExternRegistry* externs)
    -> std::expected<std::size_t, std::string>;
[[nodiscard]] auto field_uses_vectorized_eval(const ir::Expr& expr) -> bool;
// The single field-expression evaluator: top-level whole-column builtin via
// the registry, then vectorized / fast / per-row. All update paths and
// the vectorized evaluator's scalar-call delegation dispatch through it
// (stage 6 of the plan).
[[nodiscard]] auto evaluate_field(const ir::Expr& expr, const Table& input,
                                  const ColumnEvalCtx& ctx)
    -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto eval_lag_lead_column(const ir::CallExpr& call, const Table& input, bool is_lag,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs)
    -> std::expected<LagLeadResult, std::string>;
[[nodiscard]] auto apply_rng_func(const ir::CallExpr& call, std::size_t rows)
    -> std::expected<ColumnValue, std::string>;
[[nodiscard]] auto apply_rep_func(const ir::CallExpr& call, const Table& input, std::size_t rows)
    -> std::expected<ColumnValue, std::string>;
[[nodiscard]] auto expr_value_to_double(const ExprValue& v) -> std::optional<double>;
[[nodiscard]] auto expr_value_to_string(const ExprValue& v) -> std::string;

// chunked.cpp — streaming operator pipeline, rank, extern-call execution.
[[nodiscard]] auto build_operator(const ir::Node& node, const TableRegistry& registry,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;
[[nodiscard]] auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string>;
[[nodiscard]] auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                                        const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto compare_scalar_for_order(const ScalarValue& lhs, const ScalarValue& rhs) -> int;
[[nodiscard]] auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                                      const ExternRegistry* externs)
    -> std::expected<ExternValue, std::string>;
[[nodiscard]] auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                                            const ScalarRegistry* scalars,
                                            const ExternRegistry* externs)
    -> std::expected<void, std::string>;

}  // namespace ibex::runtime
