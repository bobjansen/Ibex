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

/// A grouping key: one ScalarValue per key column, plus which of them are null.
///
/// The null bits carry the whole meaning of a null key — `values[i]` is not to be
/// read where bit i is set, and KeyHash/KeyEq do not read it. A null cell's
/// payload is whatever its producer happened to leave there (Arrow, for one,
/// leaves it undefined), so a key that compared payloads would either merge nulls
/// into a genuine `0` or scatter them across separate groups, depending on the
/// producer. Reading only the mask makes a null equal to a null and to nothing
/// else regardless — which is what SQL, Polars and pandas all do.
///
/// A bitmask rather than a per-value flag: keys are compared and hashed in the
/// hot loop, and this keeps both to a single extra word.
struct Key {
    std::vector<ScalarValue> values;
    std::uint64_t null_mask = 0;  ///< bit i set → values[i] is null

    /// Mark key column `index` null. Beyond 64 key columns the bit is dropped,
    /// which would merge nulls back into the zero group — so the callers that
    /// build keys reject that case rather than answer wrongly.
    void set_null(std::size_t index) noexcept {
        if (index < 64) {
            null_mask |= std::uint64_t{1} << index;
        }
    }

    [[nodiscard]] auto is_null(std::size_t index) const noexcept -> bool {
        return index < 64 && (null_mask & (std::uint64_t{1} << index)) != 0;
    }
};

/// Ibex supports at most this many key columns in one grouping key, because
/// `Key::null_mask` is one bit per column. Callers must check.
inline constexpr std::size_t kMaxKeyColumns = 64;

/// Hash and equality skip the value of any slot the mask flags as null.
///
/// A null cell's payload is not merely uninteresting, it is *undefined*: Arrow
/// leaves whatever was last in the buffer there (in one measurement, 1.1M of 3M
/// null slots held stale non-zero doubles). If the key compared those payloads,
/// two null keys would hash and compare differently and scatter into separate
/// groups. Ignoring them is what makes a null equal to a null and to nothing
/// else, without the key having to trust the producer to have blanked the cell.
struct KeyHash {
    auto operator()(const Key& key) const -> std::size_t {
        std::size_t seed = 0;
        auto hash_combine = [&](std::size_t value) {
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        };
        for (std::size_t i = 0; i < key.values.size(); ++i) {
            if (key.is_null(i)) {
                continue;  // undefined payload — the mask already says "null"
            }
            const std::size_t h =
                std::visit([](const auto& v) { return std::hash<std::decay_t<decltype(v)>>{}(v); },
                           key.values[i]);
            hash_combine(h);
        }
        hash_combine(std::hash<std::uint64_t>{}(key.null_mask));
        return seed;
    }
};

struct KeyEq {
    auto operator()(const Key& a, const Key& b) const -> bool {
        if (a.null_mask != b.null_mask || a.values.size() != b.values.size()) {
            return false;
        }
        for (std::size_t i = 0; i < a.values.size(); ++i) {
            if (a.is_null(i)) {
                continue;  // both null here (masks are equal) — payloads irrelevant
            }
            if (a.values[i] != b.values[i]) {
                return false;
            }
        }
        return true;
    }
};

/// Append one column's cell to a grouping key, recording it as null when the
/// column's validity bit is clear. Every Key builder should go through this —
/// pushing the raw scalar without the null bit is exactly the bug that merges a
/// null key into the zero group.
inline void push_key_value(Key& key, const ColumnEntry& entry, std::size_t row) {
    if (is_null(entry, row)) {
        key.set_null(key.values.size());
    }
    key.values.push_back(scalar_from_column(*entry.column, row));
}

/// Same, where the caller holds the column and its validity separately.
/// `validity` may be null, meaning the column has no nulls.
inline void push_key_value(Key& key, const ColumnValue& column, const ValidityBitmap* validity,
                           std::size_t row) {
    if (validity != nullptr && !(*validity)[row]) {
        key.set_null(key.values.size());
    }
    key.values.push_back(scalar_from_column(column, row));
}

/// A key column resolved once, so a row loop can read its values in place
/// instead of boxing them into a Key.
///
/// Building a `Key` per row to probe a group index costs a heap-allocated
/// vector plus a std::string copy for every string key column — on the order of
/// one allocation per row, to answer a question about groups. Hashing and
/// comparing the row where it sits lets a Key be built once per *group*, which
/// is what the group index actually needs to keep.
struct KeyCol {
    enum class Kind : std::uint8_t { Int64, Double, Bool, Str, Cat, Date, Ts };
    Kind kind{Kind::Int64};
    const std::int64_t* i64{nullptr};
    const double* f64{nullptr};
    const Column<bool>* boolean{nullptr};
    const Column<std::string>* str{nullptr};
    const Column<Categorical>* cat{nullptr};
    const Date* date{nullptr};
    const Timestamp* ts{nullptr};
    const ValidityBitmap* validity{nullptr};

    [[nodiscard]] auto is_null(std::size_t row) const noexcept -> bool {
        return validity != nullptr && !(*validity)[row];
    }
    /// Categorical compares by value, not by code: chunked callers may see a
    /// different dictionary in a later chunk, and the Key it is compared
    /// against holds a string either way.
    [[nodiscard]] auto text(std::size_t row) const -> std::string_view {
        if (kind == Kind::Str) {
            return {(*str)[row]};
        }
        return {cat->dictionary()[static_cast<std::size_t>(cat->code_at(row))]};
    }
};

inline auto make_key_col(const ColumnValue& column, const ValidityBitmap* validity)
    -> std::optional<KeyCol> {
    KeyCol key_col;
    key_col.validity = validity;
    if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
        key_col.kind = KeyCol::Kind::Int64;
        key_col.i64 = c_int->data();
    } else if (const auto* c_dbl = std::get_if<Column<double>>(&column)) {
        key_col.kind = KeyCol::Kind::Double;
        key_col.f64 = c_dbl->data();
    } else if (const auto* c_bool = std::get_if<Column<bool>>(&column)) {
        key_col.kind = KeyCol::Kind::Bool;
        key_col.boolean = c_bool;
    } else if (const auto* c_str = std::get_if<Column<std::string>>(&column)) {
        key_col.kind = KeyCol::Kind::Str;
        key_col.str = c_str;
    } else if (const auto* c_cat = std::get_if<Column<Categorical>>(&column)) {
        key_col.kind = KeyCol::Kind::Cat;
        key_col.cat = c_cat;
    } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
        key_col.kind = KeyCol::Kind::Date;
        key_col.date = c_date->data();
    } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
        key_col.kind = KeyCol::Kind::Ts;
        key_col.ts = c_ts->data();
    } else {
        return std::nullopt;
    }
    return key_col;
}

inline auto make_key_col(const ColumnEntry& entry) -> std::optional<KeyCol> {
    return make_key_col(*entry.column, entry.validity.has_value() ? &*entry.validity : nullptr);
}

inline auto hash_key_row(const std::vector<KeyCol>& cols, std::size_t row) -> std::uint64_t {
    std::uint64_t seed = 0;
    const auto mix = [&seed](std::uint64_t value) {
        seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
    };
    for (std::size_t i = 0; i < cols.size(); ++i) {
        const KeyCol& col = cols[i];
        if (col.is_null(row)) {
            // A null's payload is undefined, so hash its position instead —
            // matching KeyEq, which compares the null mask and skips the value.
            mix(0xd1b54a32d192ed03ULL + i);
            continue;
        }
        switch (col.kind) {
            case KeyCol::Kind::Int64:
                mix(std::hash<std::int64_t>{}(col.i64[row]));
                break;
            case KeyCol::Kind::Double:
                // std::hash<double> folds -0.0 onto 0.0, keeping it consistent
                // with the `==` used to compare them below.
                mix(std::hash<double>{}(col.f64[row]));
                break;
            case KeyCol::Kind::Bool:
                mix(std::hash<bool>{}((*col.boolean)[row]));
                break;
            case KeyCol::Kind::Date:
                mix(std::hash<std::int32_t>{}(col.date[row].days));
                break;
            case KeyCol::Kind::Ts:
                mix(std::hash<std::int64_t>{}(col.ts[row].nanos));
                break;
            case KeyCol::Kind::Str:
            case KeyCol::Kind::Cat:
                mix(std::hash<std::string_view>{}(col.text(row)));
                break;
        }
    }
    return seed;
}

/// Does the group's stored key describe this row? Mirrors KeyEq: null matches
/// only null, and a double compares with `==`, so NaN keys stay distinct and
/// -0.0 still finds the 0.0 group — exactly as a boxed Key comparison did.
inline auto key_equals_row(const Key& key, const std::vector<KeyCol>& cols, std::size_t row)
    -> bool {
    for (std::size_t i = 0; i < cols.size(); ++i) {
        const KeyCol& col = cols[i];
        const bool row_null = col.is_null(row);
        if (row_null != key.is_null(i)) {
            return false;
        }
        if (row_null) {
            continue;
        }
        const ScalarValue& value = key.values[i];
        switch (col.kind) {
            case KeyCol::Kind::Int64:
                if (std::get<std::int64_t>(value) != col.i64[row]) {
                    return false;
                }
                break;
            case KeyCol::Kind::Double:
                if (!(std::get<double>(value) == col.f64[row])) {
                    return false;
                }
                break;
            case KeyCol::Kind::Bool:
                if (std::get<bool>(value) != (*col.boolean)[row]) {
                    return false;
                }
                break;
            case KeyCol::Kind::Date:
                if (std::get<Date>(value).days != col.date[row].days) {
                    return false;
                }
                break;
            case KeyCol::Kind::Ts:
                if (std::get<Timestamp>(value).nanos != col.ts[row].nanos) {
                    return false;
                }
                break;
            case KeyCol::Kind::Str:
            case KeyCol::Kind::Cat:
                if (std::string_view{std::get<std::string>(value)} != col.text(row)) {
                    return false;
                }
                break;
        }
    }
    return true;
}

/// Open addressing over group ids, probed by a row's key hash: slot 0 is empty,
/// otherwise it holds gid + 1. The caller keeps each group's key and that key's
/// hash, so a probe compares hashes before touching the key at all.
struct KeyRowIndex {
    std::vector<std::uint32_t> slots;
    std::vector<std::uint64_t> hashes;  ///< parallel to the caller's group keys

    void rehash(std::size_t capacity) {
        slots.assign(capacity, 0U);
        const std::size_t mask = capacity - 1;
        for (std::size_t group = 0; group < hashes.size(); ++group) {
            std::size_t probe = static_cast<std::size_t>(hashes[group]) & mask;
            while (slots[probe] != 0) {
                probe = (probe + 1) & mask;
            }
            slots[probe] = static_cast<std::uint32_t>(group) + 1;
        }
    }

    /// Find the row's group, or create one via `make_group()` — which must
    /// append the row's key to the caller's group vector and return its gid.
    template <typename MakeGroup>
    auto find_or_insert(const std::vector<Key>& groups, const std::vector<KeyCol>& cols,
                        std::size_t row, MakeGroup&& make_group) -> std::uint32_t {
        if (slots.empty()) {
            rehash(1024);
        }
        const std::uint64_t hash = hash_key_row(cols, row);
        const std::size_t mask = slots.size() - 1;
        std::size_t probe = static_cast<std::size_t>(hash) & mask;
        while (true) {
            const std::uint32_t slot = slots[probe];
            if (slot == 0) {
                const std::uint32_t gid = make_group();
                hashes.push_back(hash);
                slots[probe] = gid + 1;
                if ((hashes.size() * 10) > (slots.size() * 7)) {
                    rehash(slots.size() * 2);
                }
                return gid;
            }
            const std::uint32_t gid = slot - 1;
            if (hashes[gid] == hash && key_equals_row(groups[gid], cols, row)) {
                return gid;
            }
            probe = (probe + 1) & mask;
        }
    }
};

/// Collect the validity bitmap of each named column (null when it has no nulls),
/// parallel to a `group_columns`-style vector. Lets a key builder record nulls
/// without restructuring how it looks its columns up.
inline auto collect_key_validity(const Table& table, const std::vector<ir::ColumnRef>& keys)
    -> std::vector<const ValidityBitmap*> {
    std::vector<const ValidityBitmap*> out;
    out.reserve(keys.size());
    for (const auto& key : keys) {
        const auto* entry = table.find_entry(key.name);
        out.push_back(entry != nullptr && entry->validity.has_value() ? &*entry->validity
                                                                      : nullptr);
    }
    return out;
}

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

// Signatures shared by the registry payloads below.
using InferFn = std::expected<ExprType, std::string> (*)(std::string_view,
                                                         const std::vector<ExprType>&);
// Row-local evaluation: args at row i -> value at row i.
using RowEvalFn = std::expected<ExprValue, std::string> (*)(std::string_view,
                                                            const std::vector<ExprValue>&);
// Whole-column evaluation: the raw call (for arg literals / named args), the
// input table, the output row count, and the evaluation context.
using ColumnEvalFn = std::expected<ComputedColumn, std::string> (*)(const ir::CallExpr&,
                                                                    const Table&, std::size_t rows,
                                                                    const ColumnEvalCtx&);

// ── Per-kind execution payloads ──────────────────────────────────────────────
// One alternative per ir::FnKind, in enum order (fn_kind_of relies on it;
// pinned by the static_asserts below BuiltinFn). What a builtin *can do* is
// carried by which alternative it holds, so kind and capability cannot
// disagree — the former flat struct encoded capability in pointer-nullness,
// which every dispatch site had to re-test by convention.

// Row-local scalar. `eval` is the general form and the semantic reference. A
// few entries also have an optional whole-column kernel fast path, taken only
// for the kernel-shaped call (every positional argument a bare column or
// literal, see use_column_kernel); since only a handful of builtins have one,
// the kernel is not a pointer here but a one-byte ScalarKernel id in BuiltinFn's
// flat metadata — like NullPolicy, it packs into padding instead of widening
// the variant for every entry (see the sizeof note on BuiltinFn).
struct ScalarExec {
    RowEvalFn eval{};
};
// Non-row-local (rolling_* / cumsum / cumprod / lag / lead / fill_forward /
// fill_backward): output row i reads neighbouring rows, so evaluation is
// whole-column only.
struct TransformExec {
    ColumnEvalFn column_eval{};
};
// Produces a column from parameters/pattern (rand_*, rep); input rows are not
// read. Same payload shape as TransformExec, but a distinct alternative: the
// planner treats the kinds differently (generators ignore input order).
struct GeneratorExec {
    ColumnEvalFn column_eval{};
};
// Reduces a column (or group) to one value. Execution routes through the
// aggregate machinery keyed by ir::AggFunc; the registry is the single
// name -> AggFunc mapping (parse_aggregate_func reads it).
struct AggregateExec {
    // Invalid sentinel makes an omitted aggregate mapping fail registry
    // validation instead of silently becoming AggFunc::Sum (enum value zero).
    ir::AggFunc func = static_cast<ir::AggFunc>(0xFFU);
};

// Whole-column fast-path kernel of a Scalar entry, as a one-byte id resolved
// by scalar_kernel_fn (expr.cpp). None for the vast majority of scalars.
enum class ScalarKernel : std::uint8_t {
    None,
    FillNull,    // fill_null
    FloatClean,  // null_if_nan / null_if_not_finite (kernel branches on callee)
    Coalesce,    // coalesce
    Like,        // like (compiles the pattern once, then scans the column)
};
[[nodiscard]] auto scalar_kernel_fn(ScalarKernel kernel) -> ColumnEvalFn;

// Builtin function registry entry (registry lives in expr.cpp; type inference
// and evaluation dispatch through it). Common metadata is flat; the
// kind-specific execution surface is the `exec` variant.
//
// Layout matters: builtins() sits on per-row dispatch paths, and growing the
// entry 40 -> 48 bytes measurably regressed unrelated benchmarks once before
// (fill_forward +20% on AWS; see c18ea8f). Hence int16 arity, and null_policy
// plus the scalar kernel id packed as bytes into the flat region rather than
// widening ScalarExec (both are meaningful only when `exec` holds a
// ScalarExec; either would pad the variant by 8 for every entry). The
// static_assert below pins the size.
struct BuiltinFn {
    std::int16_t min_args = 1;
    std::int16_t max_args = 1;  // -1 == variadic
    NullPolicy null_policy = NullPolicy::Propagate;
    ScalarKernel scalar_kernel = ScalarKernel::None;
    InferFn infer{};
    std::variant<ScalarExec, TransformExec, GeneratorExec, AggregateExec> exec;
};

static_assert(sizeof(BuiltinFn) <= 4 * sizeof(void*),
              "BuiltinFn grew past 32 bytes — entry bloat regressed fill_forward +20% on AWS "
              "once before (c18ea8f); shrink it or re-benchmark deliberately");

static_assert(
    std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(ir::FnKind::Scalar),
                                              decltype(BuiltinFn::exec)>,
                   ScalarExec> &&
        std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(ir::FnKind::Transform),
                                                  decltype(BuiltinFn::exec)>,
                       TransformExec> &&
        std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(ir::FnKind::Generator),
                                                  decltype(BuiltinFn::exec)>,
                       GeneratorExec> &&
        std::is_same_v<std::variant_alternative_t<static_cast<std::size_t>(ir::FnKind::Aggregate),
                                                  decltype(BuiltinFn::exec)>,
                       AggregateExec>,
    "BuiltinFn::exec alternatives must mirror ir::FnKind order (fn_kind_of casts the index)");

// The entry's kind, derived from the alternative it holds — cannot drift.
[[nodiscard]] inline auto fn_kind_of(const BuiltinFn& fn) -> ir::FnKind {
    return static_cast<ir::FnKind>(fn.exec.index());
}

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

// Column-ONLY builtins (Transform/Generator) have no per-row form; an
// expression containing one must evaluate on the vectorized path.
[[nodiscard]] inline auto is_column_only(const BuiltinFn& fn) -> bool {
    return std::holds_alternative<TransformExec>(fn.exec) ||
           std::holds_alternative<GeneratorExec>(fn.exec);
}

// The whole-column entry point for `fn`, or nullptr when it has none: a
// Transform/Generator's column_eval, or a Scalar's optional kernel.
// Aggregates never have one (they route through the aggregate machinery).
[[nodiscard]] inline auto column_eval_of(const BuiltinFn& fn) -> ColumnEvalFn {
    return std::visit(
        [&fn](const auto& exec) -> ColumnEvalFn {
            using T = std::decay_t<decltype(exec)>;
            if constexpr (std::is_same_v<T, ScalarExec>) {
                return scalar_kernel_fn(fn.scalar_kernel);
            } else if constexpr (std::is_same_v<T, AggregateExec>) {
                return nullptr;
            } else {
                return exec.column_eval;
            }
        },
        fn.exec);
}

// Should this call go to the entry's whole-column kernel? Column-only entries
// (Transform/Generator) always do. Hybrid Scalar entries (fill_null/null_if_*/
// coalesce keep their kernels as fast paths) use the kernel only for the
// kernel-shaped call — every positional argument a bare column or literal;
// computed arguments evaluate per-row via the NullPolicy::Handles eval.
[[nodiscard]] inline auto use_column_kernel(const BuiltinFn& fn, const ir::CallExpr& call) -> bool {
    if (is_column_only(fn)) {
        return true;
    }
    if (!std::holds_alternative<ScalarExec>(fn.exec) || fn.scalar_kernel == ScalarKernel::None) {
        return false;
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
