// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

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

#include <algorithm>
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

// `invariant_violation` and the gather kernel (`make_gather_column`,
// `gather_range_into`, `gather_validity_range`) moved down to
// runtime_internal.hpp, which this includes: `gather_column` lives in that
// lower layer and had grown a second, divergent copy of the same kernel.
// `gather_rows` below still lives here, because it alone needs ir::OrderKey and
// TableProperties.

struct ComputedColumn {
    ColumnValue column;
    std::optional<ValidityBitmap> validity;
};

/// A half-open row range `[begin, begin + count)` of some table.
///
/// Threaded through the vectorized evaluators so a caller can evaluate over a
/// slice of a table without first gathering that slice into a table of its own
/// — the zero-copy morsel path (runtime multithreading plan, Phase 2).
///
/// Deliberately not convertible from `std::size_t`, and the evaluators take it
/// with no default: every former `n` call site has to name its range, so a
/// missed one is a compile error rather than a silent whole-table evaluation.
///
/// The asymmetry to keep in mind when reading the kernels: *inputs* borrowed
/// from the table are read at `begin + i`, while *outputs* are always dense and
/// written at `i`. A computed intermediate is therefore already at offset 0.
struct RowRange {
    std::size_t begin = 0;
    std::size_t count = 0;

    [[nodiscard]] static constexpr auto whole(std::size_t n) noexcept -> RowRange {
        return {.begin = 0, .count = n};
    }
    [[nodiscard]] constexpr auto end() const noexcept -> std::size_t { return begin + count; }
    /// True when this range covers a whole `n`-row table, so offset-free fast
    /// paths (whole-bitmap copies, borrowed columns) stay valid.
    [[nodiscard]] constexpr auto is_whole(std::size_t n) const noexcept -> bool {
        return begin == 0 && count == n;
    }
};

/// Read-only column lookup contract for vectorized predicate evaluation.
///
/// A predicate only needs a row count and named column entries for its
/// range-native paths (comparisons, 3VL, null tests, and categorical
/// membership).  Keeping that smaller contract explicit lets a chunk use the
/// same evaluator without constructing a transient Table and its name index.
/// `table()` is deliberately optional: whole-table builtins still require the
/// richer Table surface and their callers must take the existing Table path.
class PredicateInput {
   public:
    using RowsFn = std::size_t (*)(const void*) noexcept;
    using FindFn = const ColumnEntry* (*)(const void*, const std::string&) noexcept;

    PredicateInput(const Table& table) noexcept
        : state_(&table),
          rows_(
              [](const void* state) noexcept { return static_cast<const Table*>(state)->rows(); }),
          find_([](const void* state, const std::string& name) noexcept -> const ColumnEntry* {
              const auto* input_table = static_cast<const Table*>(state);
              return input_table->find_entry(name);
          }),
          table_(&table) {}

    PredicateInput(const void* state, RowsFn rows_fn, FindFn find_fn) noexcept
        : state_(state), rows_(rows_fn), find_(find_fn) {}

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_(state_); }
    [[nodiscard]] auto find(const std::string& name) const noexcept -> const ColumnEntry* {
        return find_(state_, name);
    }
    [[nodiscard]] auto table() const noexcept -> const Table* { return table_; }

   private:
    const void* state_ = nullptr;
    RowsFn rows_ = nullptr;
    FindFn find_ = nullptr;
    const Table* table_ = nullptr;
};

// Column result: either a pointer into the table (zero-copy) or an owned computed column,
// plus optional validity tracking for null propagation.
struct ColResult {
    std::variant<const ColumnValue*, ColumnValue> data;
    const ValidityBitmap* validity = nullptr;      // source column validity (no-copy)
    std::optional<ValidityBitmap> owned_validity;  // for computed expressions
    /// Logical row 0 of this result lives at index `offset` of `data` (and of
    /// `validity`). Non-zero only for a column borrowed from the table under a
    /// non-whole `RowRange`; an owned computed column is dense, so 0.
    std::size_t offset = 0;

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

/// Fold one column's hash into the running key hash.
///
/// Shared by `hash_key_row` (live row) and `hash_key_value` (boxed `Key`),
/// which MUST produce identical results for the same logical key — a
/// disagreement makes a probe miss the slot its own group occupies and
/// silently duplicate it. They were two copies of this expression; now there is
/// one, so they cannot drift.
inline void key_hash_mix(std::uint64_t& seed, std::uint64_t value) {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}

/// Final avalanche (murmur3's fmix64), applied once per key.
///
/// `key_hash_mix` is boost's `hash_combine`, whose weakness is that it never
/// diffuses into the LOW bits — and the low bits are exactly what
/// `KeyRowIndex` masks to pick a slot. `std::hash<int64_t>` is the identity on
/// libstdc++, so for two small integer keys the combined value is very nearly
/// `b + (a << 6)`: a linear function of the key values, which linear probing
/// turns into one long cluster.
///
/// Measured, 1M rows, `median(v) by {a, b}` with 9800 groups: **517ms before,
/// 20ms after**. The pathology needed BOTH multiple keys and a particular table
/// size, which is why it hid — a single key was always fine (100k groups in
/// 67ms), and two keys were fine at 5000 groups in an 8192-slot table (20ms)
/// while collapsing at 9800 in a 16384-slot one. Same load factor, different
/// mask width: the aliasing pattern depends on how many low bits are taken.
inline auto key_hash_finalize(std::uint64_t seed) -> std::uint64_t {
    seed ^= seed >> 33U;
    seed *= 0xff51afd7ed558ccdULL;
    seed ^= seed >> 33U;
    seed *= 0xc4ceb9fe1a85ec53ULL;
    seed ^= seed >> 33U;
    return seed;
}

inline auto hash_key_row(const std::vector<KeyCol>& cols, std::size_t row) -> std::uint64_t {
    std::uint64_t seed = 0;
    const auto mix = [&seed](std::uint64_t value) { key_hash_mix(seed, value); };
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
    return key_hash_finalize(seed);
}

/// Hash a `Key` exactly as `hash_key_row` hashes the equivalent row.
///
/// For seeding a `KeyRowIndex` with values that never sat in a live column —
/// a fast-path group migrated to the generic index after a later chunk's
/// nulls made the fast-path state unrepresentable. `KeyHash` (above) is NOT
/// interchangeable with this: it always mixes in one extra term for
/// `null_mask`, which `hash_key_row` never does, so `KeyHash` on a Key and
/// `hash_key_row` on the row it describes disagree even when both key values
/// are non-null — and if a migrated group's hash does not match what a later
/// chunk's row-based probe computes for the same value, the probe misses the
/// slot the group actually occupies and silently duplicates it. `Date` and
/// `Timestamp` hash identically to `hash_key_row`'s own `int32_t`/`int64_t`
/// cases (see the `std::hash` specializations in `time.hpp`), so visiting the
/// `ScalarValue` by its held type reproduces `hash_key_row` exactly.
inline auto hash_key_value(const Key& key) -> std::uint64_t {
    std::uint64_t seed = 0;
    const auto mix = [&seed](std::uint64_t value) { key_hash_mix(seed, value); };
    for (std::size_t i = 0; i < key.values.size(); ++i) {
        if (key.is_null(i)) {
            mix(0xd1b54a32d192ed03ULL + i);
            continue;
        }
        const auto h = std::visit(
            [](const auto& v) -> std::size_t {
                using T = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::hash<std::string_view>{}(std::string_view{v});
                } else {
                    return std::hash<T>{}(v);
                }
            },
            key.values[i]);
        mix(static_cast<std::uint64_t>(h));
    }
    return key_hash_finalize(seed);
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

/// The numeric half of an aggregate's per-group state, and deliberately a POD.
///
/// Triviality is the point, not the size. `flat_slots_.resize()` is called once
/// per NEW GROUP, so a 100k-group aggregate grows the array ~17 times and moves
/// ~200k elements; with a `ScalarValue` member every one of those is a
/// discriminant check plus a string move instead of a memcpy, and every slot
/// costs a constructor and a destructor. Measured on 100k slots: 1.59ms to
/// build and tear down with a ScalarValue, 0.33ms without.
/// The moment accumulators (Σ(x-mean)^k) are deliberately NOT here. Only three
/// of thirteen aggregates need any of them, so they live in the caller's
/// per-group scratch region rather than taxing every slot of every group: this
/// struct is allocated once per GROUP, and a group-by can carry millions.
/// `double_value` still doubles as the running mean while they accumulate.
/// Two words, and every bit of both is live. What used to be here and is not
/// any more, because none of it was information THIS slot carried:
///
/// - `func` and `kind` were a copy, per GROUP, of something that varies only
///   per AGGREGATE. Every reader already had `plan_[agg_i]` in hand and passed
///   it in — `agg_combine` takes both as parameters and never read the slot's.
///   On a 3M-group aggregate that was 6MB of the same two bytes.
/// - `has_value` is `count != 0`. The Stddev arm of `agg_combine` had already
///   worked that out and tested `count == 0` for presence; the other arms kept
///   a separate flag saying the same thing. Aggregates that do not otherwise
///   count now store 1 rather than setting a bool — the same single store, to a
///   field that was there anyway.
///
/// Dropping them takes the slot from 24 bytes to 16, which is a third off
/// everything the array costs: `mremap` growth, the first-touch fill (44ms of
/// q18 alone), every accumulate pass, and the `per_morsel_bytes` budget that
/// decides whether a group-by can afford replicated partial state at all. 16 is
/// also the size at which n_aggs slots start sharing cache lines — four to a
/// line instead of two and a bit.
struct AggSlotCore {
    /// Rows accumulated into this slot, and the presence flag. Aggregates that
    /// need a real count (Count, Mean, the moments) increment it; the rest set
    /// it to 1 on their first value. Read presence through `present()` rather
    /// than comparing here, so the two meanings stay legible.
    std::int64_t count = 0;
    /// A slot belongs to one aggregate and one column type, so the integer and
    /// double accumulators are never both live — they share storage. Anything
    /// that writes one and reads the other is a bug; the aggregate's `kind` in
    /// `plan_` says which is active, and agg_combine() takes it as a parameter
    /// for exactly this reason.
    union {
        std::int64_t int_value = 0;
        double double_value;
    };

    /// Has any non-null row reached this slot? See `count`.
    [[nodiscard]] auto present() const noexcept -> bool { return count != 0; }
    /// Mark presence for an aggregate that keeps no running count. A plain
    /// store, not an increment: nothing reads the magnitude, and a store has no
    /// dependency on the old value.
    void mark_present() noexcept { count = 1; }
};

/// The materializing aggregate's per-group state (`aggregate.cpp`). Standalone
/// rather than derived from `AggSlotCore`: that path keeps one slot per group in
/// ordinary containers, never calls `agg_combine`, and reads `func`/`kind`/
/// `has_value` inline — so it wants the wide, self-describing slot, while the
/// chunked operator allocating by the million wants the lean one. Sharing a base
/// only forced the wide fields onto the path that cannot afford them.
struct AggSlot {
    ir::AggFunc func = ir::AggFunc::Sum;
    ExprType kind = ExprType::Int;
    bool has_value = false;
    std::int64_t count = 0;
    union {
        std::int64_t int_value = 0;
        double double_value;
    };
    /// The boxed value First/Last needs for a non-numeric column (String, but
    /// also Bool/Date/Timestamp). The chunked operator keeps these in a side
    /// array that stays EMPTY — and therefore free — for an all-numeric query.
    ScalarValue text_value;
    /// Welford M2, inline here because the materializing aggregate keeps one
    /// slot per group in ordinary containers and has no scratch region to put
    /// it in. The chunked operator, which allocates slots by the million, keeps
    /// it in scratch instead — that asymmetry is the whole point of the split.
    double m2 = 0.0;
};

// The whole point of the split — lock it in so a future field cannot quietly
// put a constructor back on the per-group hot path.
static_assert(std::is_trivially_destructible_v<AggSlotCore>);
static_assert(std::is_trivially_copyable_v<AggSlotCore>);
// One slot per GROUP, and a group-by can carry millions, so its size is a
// deliberate number rather than whatever the fields happen to add up to. Two
// 8-byte words with no padding and nothing redundant left in either; a third
// would cost 50% more memory traffic across allocation, growth and every
// accumulate pass. If one is genuinely needed, put it in the caller's per-group
// scratch — that is what moved Σ(x-mean)^k out of here.
static_assert(sizeof(AggSlotCore) == 16);

// Online central-moment accumulators (Welford / Pébay), shared by the chunked
// aggregate operators. `double_value` holds the running mean; m2/m3/m4 hold
// Σ(x-mean)^k. These match the two-pass central moments the materializing
// aggregate computes to within floating-point rounding — and for stddev the
// M2 update is bit-identical (Pébay's term1 reduces to the simple Welford
// step), so `stddev` results agree exactly across paths.
inline void agg_update_stddev(AggSlotCore& slot, double& m2, double x) {
    slot.count += 1;
    const double delta = x - slot.double_value;
    slot.double_value += delta / static_cast<double>(slot.count);
    m2 += delta * (x - slot.double_value);
}

// Full m2/m3/m4 update for skewness/kurtosis (Pébay single-value recurrence).
// Updates m4 and m3 before m2 because they read the pre-update accumulators.
inline void agg_update_moments(AggSlotCore& slot, double& m2, double& m3, double& m4, double x) {
    const auto n1 = static_cast<double>(slot.count);
    slot.count += 1;
    const auto n = static_cast<double>(slot.count);
    const double delta = x - slot.double_value;
    const double delta_n = delta / n;
    const double delta_n2 = delta_n * delta_n;
    const double term1 = delta * delta_n * n1;
    slot.double_value += delta_n;
    m4 += (term1 * delta_n2 * ((n * n) - (3.0 * n) + 3.0)) + (6.0 * delta_n2 * m2) -
          (4.0 * delta_n * m3);
    m3 += (term1 * delta_n * (n - 2.0)) - (3.0 * delta_n * m2);
    m2 += term1;
}

inline auto agg_finalize_stddev(const AggSlotCore& slot, double m2) -> double {
    return slot.count < 2 ? 0.0 : std::sqrt(m2 / static_cast<double>(slot.count - 1));
}

inline auto agg_finalize_skew(const AggSlotCore& slot, double m2, double m3) -> double {
    if (slot.count < 3 || m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Fisher–Pearson sample skewness (matches pandas/scipy default).
    return (n * std::sqrt(n - 1.0) / (n - 2.0)) * (m3 / std::pow(m2, 1.5));
}

inline auto agg_finalize_kurtosis(const AggSlotCore& slot, double m2, double m4) -> double {
    if (slot.count < 4 || m2 == 0.0) {
        return 0.0;
    }
    const auto n = static_cast<double>(slot.count);
    // Unbiased Fisher excess kurtosis (matches pandas/scipy default).
    return (n - 1.0) / ((n - 2.0) * (n - 3.0)) *
           (((n + 1.0) * n * m4 / (m2 * m2)) - (3.0 * (n - 1.0)));
}

/// Can this aggregate be computed as independent partials and merged?
///
/// Skew/Kurtosis are excluded deliberately. Their m3/m4 recurrences have a
/// pairwise combine (Pébay), but it is numerically delicate and they are the
/// rarest aggregates here — not worth the risk for the first parallel slice.
/// Median/Quantile never reach these slots (the collect path owns them).
[[nodiscard]] constexpr auto agg_is_combinable(ir::AggFunc func) noexcept -> bool {
    switch (func) {
        case ir::AggFunc::Count:
        case ir::AggFunc::Sum:
        case ir::AggFunc::Mean:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::Stddev:
        case ir::AggFunc::First:
        case ir::AggFunc::Last:
            return true;
        default:
            return false;
    }
}

/// Copy whichever value member `kind` makes active. int_value and double_value
/// share storage, so only the one `kind` names may be touched.
inline void copy_active_value(AggSlotCore& dst, const AggSlotCore& src, ExprType kind) {
    if (kind == ExprType::Int) {
        dst.int_value = src.int_value;
    } else {
        // Double. A non-numeric First/Last keeps its value in the caller's side
        // array, which this cannot reach — parallel callers must exclude those
        // kinds, and agg_is_combinable()'s users assert it.
        dst.double_value = src.double_value;
    }
}

/// Merge `src` into `dst`, where `src` covers the row range immediately AFTER
/// it. `func`/`kind` come from the caller's plan: AggSlot's own `func`/`kind`
/// fields are left at their defaults by the flat-slot allocators, so reading
/// them here would silently treat every aggregate as an Int Sum.
/// `dst`'s. Order matters for First/Last, so callers must merge partials in
/// ascending range order — which is also what makes the float results
/// deterministic: the merge tree is fixed by row position, never by which
/// worker happened to finish first.
///
/// NOTE the reduction order still differs from a serial left-to-right
/// accumulation, so sums and stddev may differ from the serial path in the
/// last ulps. That is inherent to any parallel float reduction; it is stable
/// run-to-run, which is the property that matters.
/// `dst_m2` / `src_m2` are the pair's Welford accumulators, which live outside
/// the slot. Only the Stddev case reads them, so every other aggregate may pass
/// null — but a caller that has them should just pass them, since a wrong null
/// here is a silently zero variance rather than a crash.
inline void agg_combine(AggSlotCore& dst, const AggSlotCore& src, ir::AggFunc func, ExprType kind,
                        double* dst_m2 = nullptr, const double* src_m2 = nullptr) {
    switch (func) {
        case ir::AggFunc::Count:
            dst.count += src.count;
            return;
        case ir::AggFunc::Sum:
            if (kind == ExprType::Int) {
                dst.int_value += src.int_value;
            } else {
                dst.double_value += src.double_value;
            }
            dst.count += src.count;
            return;
        case ir::AggFunc::Mean:
            dst.double_value += src.double_value;
            dst.count += src.count;
            return;
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
            if (!src.present()) {
                return;
            }
            // int_value and double_value SHARE storage, so only the member
            // `kind` names may be touched — copying both would reinterpret one
            // as the other.
            if (!dst.present()) {
                if (kind == ExprType::Int) {
                    dst.int_value = src.int_value;
                } else {
                    dst.double_value = src.double_value;
                }
                dst.mark_present();
                return;
            }
            if (kind == ExprType::Int) {
                dst.int_value = func == ir::AggFunc::Min ? std::min(dst.int_value, src.int_value)
                                                         : std::max(dst.int_value, src.int_value);
            } else {
                dst.double_value = func == ir::AggFunc::Min
                                       ? std::min(dst.double_value, src.double_value)
                                       : std::max(dst.double_value, src.double_value);
            }
            return;
        case ir::AggFunc::Stddev: {
            // Chan/Golub/LeVeque pairwise combine. `double_value` is the
            // running mean and `m2` is Σ(x-mean)², matching agg_update_stddev.
            if (src.count == 0) {
                return;
            }
            if (dst.count == 0) {
                dst.count = src.count;
                dst.double_value = src.double_value;
                if (dst_m2 != nullptr && src_m2 != nullptr) {
                    *dst_m2 = *src_m2;
                }
                return;
            }
            const auto na = static_cast<double>(dst.count);
            const auto nb = static_cast<double>(src.count);
            const double n = na + nb;
            const double delta = src.double_value - dst.double_value;
            dst.double_value += delta * (nb / n);
            if (dst_m2 != nullptr && src_m2 != nullptr) {
                *dst_m2 += *src_m2 + (delta * delta * (na * nb / n));
            }
            dst.count += src.count;
            return;
        }
        case ir::AggFunc::First:
            // Leftmost wins: `dst` is the earlier range.
            if (!dst.present() && src.present()) {
                copy_active_value(dst, src, kind);
                dst.mark_present();
            }
            return;
        case ir::AggFunc::Last:
            // Rightmost wins: `src` is the later range.
            if (src.present()) {
                copy_active_value(dst, src, kind);
                dst.mark_present();
            }
            return;
        default:
            // agg_is_combinable() gates this; reaching here is a caller bug.
            invariant_violation("agg_combine: aggregate is not combinable");
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
    // True when the enclosing `window` clause is `aligned`: a rolling duration
    // window resets on the epoch grid (`[floor(t/dur)*dur, t]`) instead of
    // trailing (`[t-dur, t]`). Ignored for count windows.
    bool window_aligned = false;
    // Query-scoped execution state (deferred scans now; the RNG stream in a
    // later phase), carried so a column kernel can reach it explicitly instead
    // of via a thread-local — the ownership path a worker thread will need (see
    // runtime multithreading plan, Phase 0 item 5). Null in the deliberately
    // restricted evaluators that also null `externs` (filter predicate position,
    // the numeric fast path): those do not thread query state either.
    const ExecutionContext* exec = nullptr;
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
    FillNull,      // fill_null
    FloatClean,    // null_if_nan / null_if_not_finite (kernel branches on callee)
    Coalesce,      // coalesce
    Like,          // like (compiles the pattern once, then scans the column)
    StringLength,  // length / byte_length
    NumericCast,   // Int64/Int32/Int / Float64/Float32 (kernel branches on callee)
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

// The 32-bit WASM ABI packs the four function pointers here into a struct that
// exceeds `4 * sizeof(void*)` (16 bytes there) by one pointer's worth of
// padding. The bound is a native-performance guard — the WASM build is not on
// any measured hot path — so it is checked only off Emscripten.
#if !defined(__EMSCRIPTEN__)
static_assert(sizeof(BuiltinFn) <= 4 * sizeof(void*),
              "BuiltinFn grew past 32 bytes — entry bloat regressed fill_forward +20% on AWS "
              "once before (c18ea8f); shrink it or re-benchmark deliberately");
#endif

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
        ColumnValue gathered = make_gather_column(*entry.column, rows);
        gather_range_into(gathered, *entry.column, idx, 0, rows);
        output.add_column(entry.name, std::move(gathered));
        if (entry.validity.has_value()) {
            ValidityBitmap dst_bm(rows, false);
            gather_validity_range(dst_bm, *entry.validity, idx, 0, rows);
            output.columns.back().validity = std::move(dst_bm);
        }
    }

    // A gather either subsets the rows or reorders them; under neither does a
    // group boundary stop existing, so the whole claim rides along and only the
    // ordering is restated when the caller imposed a new one. Dropping the
    // grouping here would disarm the row-order guard for every operator built
    // on a gather.
    output.set_properties(ordering != nullptr ? input.properties().with_ordering(*ordering)
                                              : input.properties());
    return output;
}

// ── Cross-TU function declarations ───────────────────────────────────────────

// interpreter.cpp — dispatcher, small table ops, registries.
[[nodiscard]] auto interpret_node(const ir::Node& node, const TableRegistry& registry,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, ModelResult* model_out = nullptr)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto ordering_keys_for_table(const Table& input,
                                           const std::vector<ir::OrderKey>& keys)
    -> std::vector<ir::OrderKey>;
[[nodiscard]] auto format_tables(const TableRegistry& registry) -> std::string;
[[nodiscard]] auto expr_type_for_column(const ColumnValue& column) -> ExprType;
[[nodiscard]] auto project_table(const Table& input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto rename_table(Table input, const std::vector<ir::RenameSpec>& renames)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto columns_table(const Table& input) -> std::expected<Table, std::string>;
/// Whole-table `distinct`, implemented in `chunked.cpp` over
/// `ChunkedDistinctOperator` rather than a second time.
///
/// The first I4 convergence: a whole-table SIGNATURE over the chunked
/// IMPLEMENTATION, the split `ops.hpp` already uses. There used to be a serial
/// dedup loop here that boxed a `Key` per row and could not be parallel, while
/// the operator every real query reaches has single-column and packed-key fast
/// paths. Nothing routed to the serial one except `interpret_node`'s fallback,
/// so the duplicate bought nothing and could only drift.
///
/// Takes `exec` because the implementation is now the chunked operator, which
/// needs it. Metadata is unchanged by the move: `distinct` is a
/// `RowTransform::Subset` that keeps every column, and `Subset` derives exactly
/// like `Preserve`, so the properties the operator passes through are the ones
/// the old `distinct_properties` computed.
[[nodiscard]] auto distinct_table(const Table& input, const ExecutionContext& exec)
    -> std::expected<Table, std::string>;

/// Whether this join is the exact semantic subset `ChunkedInnerJoinOperator`
/// implements: inner, no predicate, one key, `nulls never`, no `expect`
/// assertion, `take` all.
///
/// One definition, two callers — `build_operator` picking the streaming path and
/// `interpret_node` picking the whole-table adapter. They were written out
/// separately at first and were character-identical, which is the I4 failure
/// mode in miniature: a six-clause predicate duplicated across two files, where
/// a later clause added to one copy silently routes a join the operator cannot
/// handle. Collapsing the implementation while leaving the gate duplicated would
/// have replaced one drift hazard with a subtler one.
[[nodiscard]] auto is_streamable_inner_join(const ir::JoinNode& join) -> bool;

/// The other two streaming gates, shared for the same reason as the one above
/// and with the same hazard in mind. The physical planner reads all three
/// rather than restating them: a plan that reimplemented these clauses said
/// `MaterializeBoth` for a two-key Int64 join the builder streams, and its
/// equivalence probe -- written from the same reading -- agreed with it.
[[nodiscard]] auto is_streamable_semi_anti_join(const ir::JoinNode& join) -> bool;

/// Whether every aggregation can be computed incrementally, and so streamed
/// rather than materialized. Shared so the physical planner relays it.
[[nodiscard]] auto aggregate_is_streamable(const ir::AggregateNode& agg) -> bool;

/// Two Int64 keys on both sides, schema-provable. Not a property of the node
/// alone: it calls `infer_schema` on both children.
[[nodiscard]] auto is_streamable_pair_int_join(const ir::JoinNode& join) -> bool;

/// Whole-table single-key inner join, implemented over
/// `ChunkedInnerJoinOperator`. Callers must check `is_streamable_inner_join`
/// first; richer join semantics remain in `join_table_impl`, which is the
/// implementation of those semantics rather than a fallback.
[[nodiscard]] auto inner_join_table(const Table& left, const Table& right,
                                    const std::vector<ir::JoinKey>& keys,
                                    const ir::JoinSuffixPolicy& suffix,
                                    const std::vector<ir::OrderKey>& pending_order,
                                    const ExecutionContext& exec)
    -> std::expected<Table, std::string>;

// filter.cpp — vectorized predicate evaluation and filtering.
[[nodiscard]] auto compute_mask(const ir::Expr& expr, const PredicateInput& input,
                                const ScalarRegistry* scalars, RowRange rows)
    -> std::expected<Mask, std::string>;
[[nodiscard]] auto compute_mask(const ir::Expr& expr, const Table& table,
                                const ScalarRegistry* scalars, RowRange rows)
    -> std::expected<Mask, std::string>;
// coalesce kernel (validity-aware Transform; args evaluated via eval_value_vec).
[[nodiscard]] auto eval_coalesce_column(const ir::CallExpr& call, const Table& input,
                                        const ScalarRegistry* scalars, RowRange rows)
    -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto eval_value_vec(const ir::Expr& expr, const PredicateInput& input,
                                  const ScalarRegistry* scalars, RowRange rows,
                                  std::optional<ir::Duration> window = std::nullopt,
                                  bool window_aligned = false)
    -> std::expected<ColResult, std::string>;
/// Element-wise arithmetic. `lhs_off`/`rhs_off` are the operands' `ColResult::offset`
/// — the output is always dense, so only the inputs carry one.
[[nodiscard]] auto arith_vec(ir::ArithmeticOp op, const ColumnValue& lhs, std::size_t lhs_off,
                             const ColumnValue& rhs, std::size_t rhs_off, std::size_t n)
    -> std::expected<ColumnValue, std::string>;
/// AND two validity bitmaps into a dense `n`-bit result, reading each from its
/// own offset. Keeps the whole-bitmap copy when both offsets are 0.
[[nodiscard]] auto merge_validity(const ValidityBitmap* a, std::size_t a_off,
                                  const ValidityBitmap* b, std::size_t b_off, std::size_t n)
    -> std::optional<ValidityBitmap>;
[[nodiscard]] auto collect_expr_validity(const ir::Expr& expr, const PredicateInput& input,
                                         RowRange rows) -> std::optional<ValidityBitmap>;
[[nodiscard]] auto filter_table(const Table& input, const ir::Expr& predicate,
                                const ScalarRegistry* scalars) -> std::expected<Table, std::string>;
/// True when every sub-expression of `expr` is evaluated by a range-aware path,
/// so evaluating it under a partial `RowRange` touches only that range's rows.
///
/// **Callers passing a partial range must check this first.** Three evaluator
/// branches still evaluate whole-table and slice (see `slice_column` in
/// filter.cpp), and they do not produce a wrong answer — they produce a silent
/// O(morsels x rows) blowup, because each morsel re-evaluates the fallback over
/// the entire table. A morsel pipeline that absorbed such a predicate measured
/// 10x *slower* than the serial path it replaced.
///
/// This mirrors `eval_value_vec` / `compute_mask` and has to be kept in step
/// with them: it is a claim about what those functions do, not an independent
/// rule. Widening either evaluator without widening this leaves performance on
/// the floor; widening this without the evaluator reintroduces the blowup.
[[nodiscard]] auto is_range_native_expr(const ir::Expr& expr) -> bool;

/// The morsel row-grain to partition `rows` into, honouring an explicit
/// `exec.parallel_grain` and otherwise deriving one.
///
/// Derivation, from a 96-config sweep of 4k..4M grains over 2/6/16-column
/// tables at both selectivities: **every** grain in that 1000x band beat the
/// serial path, and 16k-256k was within ~20% of optimal everywhere. So this is
/// not a tuning knob, and there is nothing here to ask a user about.
///
/// The one consistent degradation was at very large grains, and it is purely
/// load imbalance — it tracks morsels-per-thread falling below ~2, not the
/// grain in absolute terms. Hence `rows / (threads * 4)`: enough morsels that
/// every worker gets several, so a slow one cannot strand the rest.
///
/// **The upper clamp is load-bearing.** An uncapped `rows / (threads * 4)`
/// gives 625k rows at 20M/8 threads, which the sweep measured as clearly worse
/// than 64k. The formula may only shrink the grain below the plateau for small
/// inputs, never grow it past.
[[nodiscard]] auto morsel_grain(const ExecutionContext& exec, std::size_t rows) -> std::size_t;

/// Filter rows `[rows.begin, rows.end())` of `input` without gathering that
/// slice first. For a partial range the predicate must satisfy
/// `is_range_native_expr`, or the evaluation degrades to whole-table work per
/// call.
[[nodiscard]] auto filter_table_range(const Table& input, const ir::Expr& predicate, RowRange rows,
                                      const ScalarRegistry* scalars)
    -> std::expected<Table, std::string>;

// ---------------------------------------------------------------------------
// A filter, taken apart. `filter_table_range` runs these four steps back to
// back over one range and owns the whole result. A *parallel* filter cannot:
// it has to learn how many rows every morsel keeps before it knows where any
// morsel's rows belong, so it runs step 1 over every morsel, prefix-sums the
// counts, sizes the output once, and only then runs step 4 with each morsel
// writing a disjoint slice. Splitting them here is what lets both callers share
// one gather rather than growing a second one that can disagree.
// ---------------------------------------------------------------------------

/// Which rows of a range survive a predicate: one bit per row packed 64 to a
/// word, plus the popcount. Range-relative — bit `w*64 + b` of `keep_words` is
/// source row `rows.begin + w*64 + b` — because the mask, the keep words and
/// the output are all dense; only the gather converts back to a source index.
struct FilterSelection {
    std::vector<std::uint64_t> keep_words;
    std::size_t kept = 0;
};

/// Step 1: evaluate `predicate` over `rows` and pack the surviving rows.
/// `row_limit` stops the scan once that many rows are kept (0 = no limit); any
/// suffix words stay zero and the gather skips them.
[[nodiscard]] auto compute_filter_selection(const Table& input, const ir::Expr& predicate,
                                            const ScalarRegistry* scalars, RowRange rows,
                                            std::size_t row_limit)
    -> std::expected<FilterSelection, std::string>;
[[nodiscard]] auto compute_filter_selection(const PredicateInput& input, const ir::Expr& predicate,
                                            const ScalarRegistry* scalars, RowRange rows,
                                            std::size_t row_limit)
    -> std::expected<FilterSelection, std::string>;

/// A filter's output columns, and where each one reads from:
/// `src_of_dst[d]` indexes `input.columns` for output column `d`.
struct FilterOutputLayout {
    Table output;
    std::vector<std::size_t> src_of_dst;
};

/// Step 2: the empty output skeleton — every input column, or just the
/// projected subset. Fails if a projected name is not in `input`.
[[nodiscard]] auto build_filter_output_layout(const Table& input,
                                              const std::vector<ir::ColumnRef>* project)
    -> std::expected<FilterOutputLayout, std::string>;

/// Step 3a: add the bytes this selection's rows contribute to each string
/// output column into `chars` (indexed by output column; untouched for every
/// other type). Additive so a parallel filter can total across morsels.
void count_selected_chars(const Table& input, const std::vector<std::size_t>& src_of_dst,
                          const FilterSelection& sel, RowRange rows,
                          std::vector<std::size_t>& chars);

/// Step 3b: size every output column for `rows_total` rows and, for string
/// columns, `chars_total[d]` bytes. Also allocates an all-false validity bitmap
/// wherever the source column has one. Must be called once, before any gather.
void presize_filter_output(Table& output, const Table& input,
                           const std::vector<std::size_t>& src_of_dst, std::size_t rows_total,
                           const std::vector<std::size_t>& chars_total);

/// Where one selection's rows land in a presized output. `row` is the first
/// output row it writes; `char_base[d]` the first byte, which only a string
/// column reads. Both zero for a filter that owns its whole output; non-zero
/// for one morsel of a parallel filter writing into a shared one.
///
/// **Concurrency:** distinct `GatherDest`s write disjoint rows, which is
/// disjoint *memory* for every column that stores at least one addressable unit
/// per row. `Column<bool>` data and validity bitmaps do not — they pack 64 rows
/// into a word, so two gathers meeting mid-word write the same word. Those are
/// handled rather than excluded: the destination is zero-filled, the writes only
/// ever set bits, and the (at most two) words a gather can share with a
/// neighbour are OR-ed in atomically. See `SharedBitWords` in filter.cpp.
struct GatherDest {
    std::size_t row = 0;
    const std::vector<std::size_t>* char_base = nullptr;  ///< null = all zero
};

/// Step 4: copy this selection's rows (and their validity) into a presized
/// `output` at `dst`.
void gather_selection_into(Table& output, const Table& input,
                           const std::vector<std::size_t>& src_of_dst, const FilterSelection& sel,
                           RowRange rows, GatherDest dst);
[[nodiscard]] auto filter_table_selection(const Table& input, const FilterSelection& selection,
                                          const std::vector<ir::ColumnRef>* project, RowRange rows)
    -> std::expected<Table, std::string>;

/// True when the columns `src_of_dst` selects can be gathered into by several
/// threads at once.
///
/// Every column kind currently in `ColumnValue` qualifies: most store at least
/// one addressable unit per row, so disjoint rows are disjoint memory, and the
/// two that are bit-packed (`Column<bool>` and any column's validity bitmap)
/// are handled by the shared-word rule in `gather_selection_into` rather than
/// excluded. So this answers true for every table today.
///
/// It is kept, rather than deleted as vacuous, because it is written as an
/// allowlist per column kind: a new `ColumnValue` alternative answers **false**
/// until someone has checked it, which costs a fallback to the ordered merger.
/// Deleting it would make the same omission a silent data race instead. This
/// gates correctness, not speed — hence the safe default rather than an
/// optimistic one.
///
/// `ParallelPipelineStats::two_phase_filters` is what makes such a fallback
/// visible; both paths produce identical output.
[[nodiscard]] auto filter_gather_is_thread_safe(const Table& input,
                                                const std::vector<std::size_t>& src_of_dst) -> bool;
/// `filter_table_range` with a fused projection — the ranged form of
/// `filter_project_table`, which is what `filter …, select …` canonicalizes to.
[[nodiscard]] auto filter_project_table_range(const Table& input, const ir::Expr& predicate,
                                              const std::vector<ir::ColumnRef>& columns,
                                              RowRange rows, const ScalarRegistry* scalars)
    -> std::expected<Table, std::string>;
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

[[nodiscard]] auto group_barrier_worker_count(const ExecutionContext& exec, std::size_t rows)
    -> std::size_t;

/// Reusable buffers for `sort_key_index_slice`. One per worker: the slice sort
/// is called once per group, and reallocating its ping-pong buffers per group
/// would cost more than the sort of a small group.
struct RadixSliceScratch {
    std::vector<std::uint64_t> keys;
    std::vector<std::size_t> idx;
    std::vector<std::pair<std::uint64_t, std::size_t>> pairs;
};

/// Stably sort one contiguous run of `n` (key, row) pairs by key, ascending,
/// in place. Equal keys keep their incoming order, so a caller that fills the
/// run in ascending row order gets ties broken by row — the same total order a
/// global stable sort produces.
void sort_key_index_slice(std::uint64_t* keys, std::size_t* idx, std::size_t n,
                          RadixSliceScratch& scratch);

// Map an IEEE-754 double to a uint64 whose unsigned order matches ascending
// double order, so radix_sort_u64_asc can sort doubles directly. For positive
// values flip the sign bit; for negatives flip all bits. NaNs (sign bit clear)
// sort to the end. The transform is a bijection, so radix stays stable.
inline auto double_to_sortable_u64(double value) -> std::uint64_t {
    const auto bits = std::bit_cast<std::uint64_t>(value);
    return bits ^ ((static_cast<std::uint64_t>(-static_cast<std::int64_t>(bits >> 63))) |
                   (std::uint64_t{1} << 63));
}

[[nodiscard]] auto order_table(const Table& input, const std::vector<ir::OrderKey>& keys,
                               const ExecutionContext& exec) -> std::expected<Table, std::string>;

/// Rewrite every column of `input` through `perm` -- output row `i` takes input
/// row `perm[i]` -- and record `ordering` on the result.
///
/// This is the second half of `order_table` without the first: the sort exists
/// to PRODUCE a permutation, and a caller that already holds one (a grouped
/// operator's CSR bucketing, say -- a flat row buffer plus per-group offsets)
/// should not pay a radix pass to rediscover it.
/// The movement itself is the expensive part and is threaded by column x row
/// range; a hand-rolled serial gather in its place measured 0.85x, i.e. slower
/// than not permuting at all.
///
/// `ordering` is asserted, not derived: `perm` is opaque here, so the caller
/// states what order it puts the rows in. Getting it wrong is invisible in the
/// values and wrong in the metadata.
[[nodiscard]] auto permute_table_rows(const Table& input, const std::vector<std::size_t>& perm,
                                      std::vector<ir::OrderKey> ordering,
                                      const ExecutionContext& exec) -> Table;
[[nodiscard]] auto head_table(const Table& input, std::size_t count,
                              const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto tail_table(const Table& input, std::size_t count,
                              const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string>;

// aggregate.cpp — grouped/global aggregation.
/// `exec` is optional and only the GROUPED path can use it: the collect
/// aggregates (median/quantile/skew/kurtosis) reduce disjoint per-group slices,
/// which splits across workers. Callers that aggregate the whole table into one
/// group have nothing to split and pass nothing.
[[nodiscard]] auto aggregate_table(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                                   const std::vector<ir::AggSpec>& aggregations,
                                   const ExecutionContext* exec = nullptr)
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

[[nodiscard]] auto apply_rolling_func(const ir::CallExpr& call, const Table& table, WindowSpec spec,
                                      bool aligned = false)
    -> std::expected<ComputedColumn, std::string>;

/// Index of the aligned window bucket containing time `t`, in `unit` steps —
/// `floor(t / unit)`, rounding toward -inf so negative timestamps bucket the
/// same way positive ones do.
///
/// This is the ONE definition of the bucket grid. `apply_rolling_func` uses it
/// to bound an aligned window, and the grouped windowed update uses it to pick
/// the boundaries it may split a group at; if those two disagreed by one row
/// the split would silently cut a window in half.
[[nodiscard]] constexpr auto window_bucket_index(std::int64_t t, std::int64_t unit) noexcept
    -> std::int64_t {
    std::int64_t q = t / unit;
    if (t < 0 && t % unit != 0) {
        --q;
    }
    return q;
}
/// Column of nominal window bounds for the enclosing `window` clause: for each
/// row, the start (`want_end=false`) or end (`want_end=true`) of the window
/// containing its timestamp. `aligned` selects grid boundaries vs a trailing
/// `[t-dur, t]`. Returns the time index's type (Timestamp or Date).
[[nodiscard]] auto window_bound_column(const Table& table, ir::Duration duration, bool aligned,
                                       bool want_end) -> std::expected<ComputedColumn, std::string>;
[[nodiscard]] auto resample_table(const Table& input, ir::Duration bucket_dur,
                                  const std::vector<ir::ColumnRef>& extra_group_by,
                                  const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string>;

// update.cpp — update/select field application (incl. fast numeric paths).
[[nodiscard]] auto update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                const ScalarRegistry* scalars, const ExternRegistry* externs,
                                const ExecutionContext& exec) -> std::expected<Table, std::string>;
[[nodiscard]] auto grouped_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                        const std::vector<ir::ColumnRef>& group_by,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto windowed_update_table(Table input, const std::vector<ir::FieldSpec>& fields,
                                         ir::Duration duration, const ScalarRegistry* scalars,
                                         const ExternRegistry* externs,
                                         const ExecutionContext& exec, bool aligned = false)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto grouped_windowed_update_table(
    Table input, const std::vector<ir::FieldSpec>& fields, ir::Duration duration,
    const std::vector<ir::ColumnRef>& group_by, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec, bool aligned = false)
    -> std::expected<Table, std::string>;
[[nodiscard]] auto apply_guarded_update(Table input, const ir::UpdateNode& update,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<Table, std::string>;
/// How many whole-table column-kernel leaves the numeric-tree compiler has
/// spliced (`Int64(like(col, "pat"))` and friends) since the last reset.
///
/// Exists because "this leaf declines under a partial range" is not observable
/// in the output: with a range beginning at zero the spliced whole-table column
/// and the range agree on every value, so the decline can only be seen by
/// asking whether it happened. What it costs when it does not decline is a
/// whole-table kernel evaluation for a morsel-sized answer, plus that morsel
/// taking the fused-tree path while its siblings take the per-row one — and
/// those two disagree about the payload of a null cell.
///
/// The counter also lets a test prove the *positive* case, that a whole range
/// really does splice; without that, asserting zero under a partial range would
/// pass just as well if the expression never reached the splice at all.
///
/// Process-wide and relaxed: it is incremented once per compiled leaf, never
/// per row. The state lives in a host TU rather than an inline variable in this
/// header, because bundled plugins statically link runtime code and would
/// otherwise each get their own copy (the RTLD_LOCAL trap).
[[nodiscard]] auto column_kernel_splice_count() -> std::uint64_t;
void reset_column_kernel_splice_count();

/// The fused numeric fast path, evaluated over `range`. Input columns are read
/// from `range.begin`; the result is dense. A leaf that would need whole-table
/// evaluation (a spliced `like`/cast kernel) declines under a partial range
/// rather than re-running over the whole column per call.
[[nodiscard]] auto try_fast_update_numeric_expr(const ir::Expr& expr, const Table& input,
                                                RowRange range, ExprType output_kind,
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
//
// `rows` selects which rows of `input` to evaluate; the result is always dense
// (row 0 of the output is row `rows.begin` of the input). Not every sub-path
// honours a partial range yet — see `is_range_native_expr`, which is the
// authority on which expressions may be given one, and `evaluate_field`'s own
// asserts for what happens if that gate and this function disagree.
[[nodiscard]] auto evaluate_field(const ir::Expr& expr, const Table& input, RowRange range,
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
                                  const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string>;
[[nodiscard]] auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string>;
[[nodiscard]] auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                                        const std::vector<ir::ColumnRef>& group_by,
                                        const ExecutionContext& exec)
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
