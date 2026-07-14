#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <functional>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "interpreter_internal.hpp"
#include "join_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

// Sentinel: SIZE_MAX means "emit a default/null value for this position".
constexpr std::size_t kNull = SIZE_MAX;

// Sentinel group id: "this row's key is not in the index".
constexpr std::uint32_t kNoGroup = UINT32_MAX;

/// CSR view over the build side: matches(gid) is the ascending list of build
/// rows whose key belongs to group gid. Replaces one heap-allocated
/// vector<size_t> per distinct key — on a PK build side (every TPC-H join)
/// that was one single-element heap vector per build row — with two flat
/// arrays.
struct GroupedRows {
    std::vector<std::size_t> offsets;  ///< n_groups + 1
    std::vector<std::size_t> rows;     ///< grouped build rows, ascending per group

    [[nodiscard]] auto matches(std::uint32_t gid) const -> std::span<const std::size_t> {
        return {rows.data() + offsets[gid], offsets[gid + 1] - offsets[gid]};
    }
    /// Every group holds exactly one build row (unique build keys).
    [[nodiscard]] auto unique() const noexcept -> bool { return rows.size() + 1 == offsets.size(); }
};

/// Scatter build rows into CSR order by group id. `kNoGroup` rows (null keys,
/// which match nothing) are left out of the index entirely.
auto group_rows_csr(const std::vector<std::uint32_t>& row_gid, std::uint32_t n_groups)
    -> GroupedRows {
    GroupedRows out;
    out.offsets.assign(static_cast<std::size_t>(n_groups) + 1, 0);
    std::size_t indexed = 0;
    for (const std::uint32_t gid : row_gid) {
        if (gid != kNoGroup) {
            ++out.offsets[gid + 1];
            ++indexed;
        }
    }
    for (std::uint32_t g = 0; g < n_groups; ++g) {
        out.offsets[g + 1] += out.offsets[g];
    }
    out.rows.resize(indexed);
    std::vector<std::size_t> cursor(out.offsets.begin(), out.offsets.end() - 1);
    for (std::size_t row = 0; row < row_gid.size(); ++row) {
        const std::uint32_t gid = row_gid[row];
        if (gid != kNoGroup) {
            out.rows[cursor[gid]++] = row;
        }
    }
    return out;
}

/// Do two rows carry equal key values? `a` and `b` are the resolved key
/// columns of the two sides; they may differ in representation (a String
/// column joins against a Categorical one — both compare as text) but never
/// in ExprType, which key validation already enforced. Null-keyed rows never
/// reach this (they match nothing and are skipped before probing), so
/// validity is not consulted.
auto key_rows_equal(const std::vector<KeyCol>& a, std::size_t ra, const std::vector<KeyCol>& b,
                    std::size_t rb) -> bool {
    for (std::size_t i = 0; i < a.size(); ++i) {
        const KeyCol& ca = a[i];
        const KeyCol& cb = b[i];
        switch (ca.kind) {
            case KeyCol::Kind::Int64:
                if (ca.i64[ra] != cb.i64[rb]) {
                    return false;
                }
                break;
            case KeyCol::Kind::Double:
                if (!(ca.f64[ra] == cb.f64[rb])) {
                    return false;
                }
                break;
            case KeyCol::Kind::Bool:
                if ((*ca.boolean)[ra] != (*cb.boolean)[rb]) {
                    return false;
                }
                break;
            case KeyCol::Kind::Date:
                if (ca.date[ra].days != cb.date[rb].days) {
                    return false;
                }
                break;
            case KeyCol::Kind::Ts:
                if (ca.ts[ra].nanos != cb.ts[rb].nanos) {
                    return false;
                }
                break;
            case KeyCol::Kind::Str:
            case KeyCol::Kind::Cat:
                if (ca.text(ra) != cb.text(rb)) {
                    return false;
                }
                break;
        }
    }
    return true;
}

/// Open-addressed key→gid index for the generic multi-key join, hashing and
/// comparing the key columns in place. Each group is represented by its first
/// build row (rep_rows) — no boxed Key is ever built, which is the fix the
/// grouped aggregate already got (KeyRowIndex). Unlike KeyRowIndex it keeps
/// no keys at all: a join's build side stays resident, so the representative
/// row IS the key.
struct RowKeyIndex {
    std::vector<std::uint32_t> slots;   ///< 0 = empty, else gid + 1
    std::vector<std::uint64_t> hashes;  ///< per gid
    std::vector<std::size_t> rep_rows;  ///< per gid: first build row with this key

    [[nodiscard]] auto size() const noexcept -> std::uint32_t {
        return static_cast<std::uint32_t>(rep_rows.size());
    }

    void reserve(std::size_t n_rows) {
        hashes.reserve(n_rows);
        rep_rows.reserve(n_rows);
        rehash(std::bit_ceil(std::max<std::size_t>(n_rows * 10 / 7, 1024)));
    }

    void rehash(std::size_t capacity) {
        slots.assign(capacity, 0U);
        const std::size_t mask = capacity - 1;
        for (std::size_t gid = 0; gid < hashes.size(); ++gid) {
            std::size_t probe = static_cast<std::size_t>(hashes[gid]) & mask;
            while (slots[probe] != 0) {
                probe = (probe + 1) & mask;
            }
            slots[probe] = static_cast<std::uint32_t>(gid) + 1;
        }
    }

    auto find_or_insert(const std::vector<KeyCol>& cols, std::size_t row) -> std::uint32_t {
        if (slots.empty()) {
            rehash(1024);
        }
        const std::uint64_t hash = hash_key_row(cols, row);
        const std::size_t mask = slots.size() - 1;
        std::size_t probe = static_cast<std::size_t>(hash) & mask;
        while (true) {
            const std::uint32_t slot = slots[probe];
            if (slot == 0) {
                const auto gid = static_cast<std::uint32_t>(rep_rows.size());
                rep_rows.push_back(row);
                hashes.push_back(hash);
                slots[probe] = gid + 1;
                if ((hashes.size() * 10) > (slots.size() * 7)) {
                    rehash(slots.size() * 2);
                }
                return gid;
            }
            const std::uint32_t gid = slot - 1;
            if (hashes[gid] == hash && key_rows_equal(cols, row, cols, rep_rows[gid])) {
                return gid;
            }
            probe = (probe + 1) & mask;
        }
    }

    /// Probe with a row of the other side's key columns.
    [[nodiscard]] auto find(const std::vector<KeyCol>& probe_cols, std::size_t row,
                            const std::vector<KeyCol>& build_cols) const -> std::uint32_t {
        if (slots.empty()) {
            return kNoGroup;
        }
        const std::uint64_t hash = hash_key_row(probe_cols, row);
        const std::size_t mask = slots.size() - 1;
        std::size_t probe = static_cast<std::size_t>(hash) & mask;
        while (true) {
            const std::uint32_t slot = slots[probe];
            if (slot == 0) {
                return kNoGroup;
            }
            const std::uint32_t gid = slot - 1;
            if (hashes[gid] == hash && key_rows_equal(probe_cols, row, build_cols, rep_rows[gid])) {
                return gid;
            }
            probe = (probe + 1) & mask;
        }
    }
};

/// Pick a column on `side` that's a plausible time index — first preference
/// `Timestamp`, then `Date`, then `Int`. Used to make the asof "not a
/// TimeFrame" error actionable (suggest `as_timeframe(side, "<col>")`).
auto find_candidate_time_column(const Table& side) -> std::optional<std::string> {
    std::optional<std::string> ts_match;
    std::optional<std::string> date_match;
    std::optional<std::string> int_match;
    for (const auto& entry : side.columns) {
        const auto kind = column_kind(*entry.column);
        if (kind == ExprType::Timestamp && !ts_match.has_value()) {
            ts_match = entry.name;
        } else if (kind == ExprType::Date && !date_match.has_value()) {
            date_match = entry.name;
        } else if (kind == ExprType::Int && !int_match.has_value()) {
            int_match = entry.name;
        }
    }
    if (ts_match.has_value()) {
        return ts_match;
    }
    if (date_match.has_value()) {
        return date_match;
    }
    return int_match;
}

auto format_expr_type(ExprType kind) -> std::string {
    switch (kind) {
        case ExprType::Int:
            return "Int";
        case ExprType::Double:
            return "Double";
        case ExprType::Bool:
            return "Bool";
        case ExprType::Date:
            return "Date";
        case ExprType::Timestamp:
            return "Timestamp";
        case ExprType::String:
            return "String";
    }
    return "?";
}

auto format_keys(const std::vector<std::string>& keys) -> std::string {
    if (keys.empty()) {
        return "<none>";
    }
    if (keys.size() == 1) {
        return keys.front();
    }
    std::string out = "{";
    for (std::size_t i = 0; i < keys.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(keys[i]);
    }
    out.push_back('}');
    return out;
}

/// Build the "not a TimeFrame" diagnostic. Names which side(s) are bare
/// DataFrames, lists their columns, and — when there's an obvious time-like
/// column on a failing side — suggests the precise `as_timeframe(...)` call
/// that would fix the call site.
auto asof_not_timeframe_error(const Table& left, const Table& right) -> std::string {
    const bool left_bad = !left.time_index.has_value();
    const bool right_bad = !right.time_index.has_value();

    std::string msg = "asof join requires both sides to be TimeFrame";
    if (left_bad && right_bad) {
        msg.append("; neither side has been promoted with as_timeframe()");
    } else if (left_bad) {
        msg.append("; left side is a DataFrame (right is TimeFrame on '" + *right.time_index +
                   "')");
    } else {
        msg.append("; right side is a DataFrame (left is TimeFrame on '" + *left.time_index + "')");
    }

    auto add_side_hint = [&](const Table& side, const char* label) {
        if (side.time_index.has_value()) {
            return;
        }
        msg.append("\n  ");
        msg.append(label);
        msg.append(" columns: ");
        msg.append(format_columns(side));
        if (auto cand = find_candidate_time_column(side); cand.has_value()) {
            msg.append("\n  hint: promote it first — let ");
            msg.append(label);
            msg.append("_tf = as_timeframe(");
            msg.append(label);
            msg.append(", \"" + *cand + "\");");
        }
    };
    add_side_hint(left, "left");
    add_side_hint(right, "right");
    return msg;
}

}  // namespace

auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                     const std::vector<std::string>& keys, const ir::Expr* predicate,
                     const ScalarRegistry* scalars, PredicateMaskEvaluator mask_evaluator)
    -> std::expected<Table, std::string> {
    if (predicate == nullptr && kind != ir::JoinKind::Cross && keys.empty()) {
        return std::unexpected("join requires at least one key");
    }

    // Asof preconditions run before per-key column validation: a typical
    // "forgot as_timeframe" mistake produces a Timestamp-vs-Int mismatch on
    // the time key, but the actionable diagnosis is "promote the other side",
    // not "your types don't match".
    std::optional<std::size_t> asof_time_key_pos;
    if (kind == ir::JoinKind::Asof) {
        if (!left.time_index.has_value() || !right.time_index.has_value()) {
            return std::unexpected(asof_not_timeframe_error(left, right));
        }
        if (*left.time_index != *right.time_index) {
            return std::unexpected(
                "asof join requires both TimeFrames to share the same time index column"
                "\n  left  time index: '" +
                *left.time_index + "'\n  right time index: '" + *right.time_index +
                "'\n  hint: re-promote one side with the matching name, e.g. "
                "as_timeframe(<table>, \"" +
                *left.time_index + "\")");
        }
        const std::string& time_key = *left.time_index;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == time_key) {
                asof_time_key_pos = i;
                break;
            }
        }
        if (!asof_time_key_pos.has_value()) {
            std::string suggested;
            if (keys.empty()) {
                suggested = time_key;
            } else {
                suggested = "{" + time_key;
                for (const auto& k : keys) {
                    suggested.append(", ");
                    suggested.append(k);
                }
                suggested.push_back('}');
            }
            return std::unexpected("asof join: the 'on' keys must include the time index '" +
                                   time_key + "'\n  got:  on " + format_keys(keys) +
                                   "\n  hint: did you mean `... asof join ... on " + suggested +
                                   "`?");
        }
    }

    std::vector<const ColumnValue*> left_keys;
    std::vector<const ColumnValue*> right_keys;
    left_keys.reserve(keys.size());
    right_keys.reserve(keys.size());
    for (const auto& key : keys) {
        const auto* left_col = left.find(key);
        if (left_col == nullptr) {
            return std::unexpected("join key not found in left: " + key +
                                   " (available: " + format_columns(left) + ")");
        }
        const auto* right_col = right.find(key);
        if (right_col == nullptr) {
            return std::unexpected("join key not found in right: " + key +
                                   " (available: " + format_columns(right) + ")");
        }
        if (column_kind(*left_col) != column_kind(*right_col)) {
            return std::unexpected("join key type mismatch for " + key);
        }
        left_keys.push_back(left_col);
        right_keys.push_back(right_col);
    }

    // A null key matches nothing — not even another null (SQL: `NULL = NULL` is
    // not true; Polars' join_nulls=False). This is deliberately unlike group-by,
    // where nulls DO group together.
    //
    // Enforcing it takes both halves: a null-keyed row is never inserted into the
    // hash index, and a null-keyed probe row is never looked up. The second half
    // is not redundant — a null cell carries the type's zero value, so a null
    // probe key would otherwise hit a genuine `0` on the other side.
    std::vector<const ValidityBitmap*> left_key_validity;
    std::vector<const ValidityBitmap*> right_key_validity;
    bool has_null_keys = false;
    for (const auto& key : keys) {
        const auto* left_entry = left.find_entry(key);
        const auto* right_entry = right.find_entry(key);
        const auto* lv = left_entry != nullptr && left_entry->validity.has_value()
                             ? &*left_entry->validity
                             : nullptr;
        const auto* rv = right_entry != nullptr && right_entry->validity.has_value()
                             ? &*right_entry->validity
                             : nullptr;
        left_key_validity.push_back(lv);
        right_key_validity.push_back(rv);
        has_null_keys = has_null_keys || lv != nullptr || rv != nullptr;
    }
    const auto row_key_is_null = [](const std::vector<const ValidityBitmap*>& validity,
                                    std::size_t row) {
        for (const auto* bitmap : validity) {
            if (bitmap != nullptr && !(*bitmap)[row]) {
                return true;
            }
        }
        return false;
    };
    robin_hood::unordered_set<std::string> key_set(keys.begin(), keys.end());

    const std::size_t n_left = left.rows();
    const std::size_t n_right = right.rows();

    const bool preserve_left_rows =
        (kind == ir::JoinKind::Left || kind == ir::JoinKind::Outer || kind == ir::JoinKind::Asof);
    const bool preserve_right_rows = (kind == ir::JoinKind::Right || kind == ir::JoinKind::Outer);
    const bool semi_join = (kind == ir::JoinKind::Semi);
    const bool anti_join = (kind == ir::JoinKind::Anti);

    Table output;
    output.columns.reserve(left.columns.size() + right.columns.size());

    robin_hood::unordered_set<std::string> out_names;
    out_names.reserve(left.columns.size() + right.columns.size());

    for (const auto& entry : left.columns) {
        out_names.insert(entry.name);
        output.add_column(entry.name, make_empty_like(*entry.column));
    }

    struct RightOut {
        const ColumnValue* column = nullptr;
        std::size_t out_index = 0;
    };
    std::vector<RightOut> right_out;
    right_out.reserve(right.columns.size());
    for (const auto& entry : right.columns) {
        if (semi_join || anti_join || key_set.contains(entry.name)) {
            continue;
        }
        std::string name = entry.name;
        while (out_names.contains(name)) {
            name += "_right";
        }
        out_names.insert(name);
        output.add_column(name, make_empty_like(*entry.column));
        right_out.push_back(
            RightOut{.column = entry.column.get(), .out_index = output.columns.size() - 1});
    }

    const bool preserve_left_only =
        preserve_left_rows && !preserve_right_rows && kind != ir::JoinKind::Asof;

    auto materialize_left_identity = [&](const std::vector<std::size_t>& right_idx) {
        for (std::size_t c = 0; c < left.columns.size(); ++c) {
            output.replace_column(c, *left.columns[c].column, left.columns[c].validity);
        }

        if (!right_out.empty()) {
            bool has_right_nulls = false;
            for (std::size_t i = 0; i < right_idx.size() && !has_right_nulls; ++i) {
                has_right_nulls = (right_idx[i] == kNull);
            }

            if (!has_right_nulls) {
                for (const auto& item : right_out) {
                    output.replace_column(
                        item.out_index,
                        gather_column(*item.column, right_idx.data(), right_idx.size()));
                }
            } else {
                for (const auto& item : right_out) {
                    auto [col_out, validity] = gather_column_with_nulls(
                        *item.column, right_idx.data(), right_idx.size(), kNull);
                    output.replace_column(item.out_index, std::move(col_out), std::move(validity));
                }
            }
        }

        normalize_time_index(output);
    };

    // ── Materialize output columns from index arrays ─────────────────────
    // left_idx[i]  = left  row for output row i, or kNull for default.
    // right_idx[i] = right row for output row i, or kNull for default.
    // key_right_idx: for outer/right join unmatched right rows, the right row
    //   to fill key columns from; kNull elsewhere. Empty if not needed.
    auto materialize = [&](const std::vector<std::size_t>& left_idx,
                           const std::vector<std::size_t>& right_idx,
                           const std::vector<std::size_t>& key_right_idx) {
        const std::size_t total = left_idx.size();

        // ── Left columns ──
        // Check whether any left rows are null (outer/right join unmatched).
        bool has_left_nulls = false;
        for (std::size_t i = 0; i < total && !has_left_nulls; ++i) {
            has_left_nulls = (left_idx[i] == kNull);
        }

        if (!has_left_nulls) {
            for (std::size_t c = 0; c < left.columns.size(); ++c) {
                output.replace_column(
                    c, gather_column(*left.columns[c].column, left_idx.data(), total));
            }
        } else {
            // Build a "safe" index: null positions get row 0 (overwritten below).
            std::vector<std::size_t> safe_idx(total);
            std::vector<std::size_t> null_pos;
            for (std::size_t i = 0; i < total; ++i) {
                if (left_idx[i] != kNull) {
                    safe_idx[i] = left_idx[i];
                } else {
                    safe_idx[i] = 0;
                    null_pos.push_back(i);
                }
            }

            for (std::size_t c = 0; c < left.columns.size(); ++c) {
                const bool is_key = key_set.contains(left.columns[c].name);
                if (is_key && !key_right_idx.empty()) {
                    // Key columns for unmatched right rows: fill from the right table.
                    const auto* right_key_col = right.find(left.columns[c].name);
                    // Build per-row: pick from left or right depending on null.
                    // Key columns in outer joins are rare & small, so per-row is fine.
                    ColumnValue out_col = make_empty_like(*left.columns[c].column);
                    std::visit([total](auto& cc) { cc.reserve(total); }, out_col);
                    for (std::size_t i = 0; i < total; ++i) {
                        if (left_idx[i] != kNull) {
                            append_value(out_col, *left.columns[c].column, left_idx[i]);
                        } else {
                            append_value(out_col, *right_key_col, key_right_idx[i]);
                        }
                    }
                    output.replace_column(c, std::move(out_col));
                } else {
                    auto [col_out, validity] = gather_column_with_nulls(
                        *left.columns[c].column, left_idx.data(), total, kNull);
                    output.replace_column(c, std::move(col_out), std::move(validity));
                }
            }
        }

        // ── Right columns ──
        if (right_out.empty()) {
            // semi/anti — no right columns to emit.
        } else {
            bool has_right_nulls = false;
            for (std::size_t i = 0; i < total && !has_right_nulls; ++i) {
                has_right_nulls = (right_idx[i] == kNull);
            }

            if (!has_right_nulls) {
                for (const auto& item : right_out) {
                    output.replace_column(item.out_index,
                                          gather_column(*item.column, right_idx.data(), total));
                }
            } else {
                for (const auto& item : right_out) {
                    auto [col_out, validity] =
                        gather_column_with_nulls(*item.column, right_idx.data(), total, kNull);
                    output.replace_column(item.out_index, std::move(col_out), std::move(validity));
                }
            }
        }

        normalize_time_index(output);
    };

    // ── Cross join ───────────────────────────────────────────────────────
    if (kind == ir::JoinKind::Cross) {
        const std::size_t total = n_left * n_right;
        std::vector<std::size_t> li(total);
        std::vector<std::size_t> ri(total);
        std::size_t pos = 0;
        for (std::size_t l = 0; l < n_left; ++l) {
            for (std::size_t r = 0; r < n_right; ++r) {
                li[pos] = l;
                ri[pos] = r;
                ++pos;
            }
        }
        static const std::vector<std::size_t> empty_key_idx;
        materialize(li, ri, empty_key_idx);
        return output;
    }

    // ── Predicate (nested-loop) join ─────────────────────────────────────
    if (predicate != nullptr) {
        if (mask_evaluator == nullptr) {
            return std::unexpected("join predicate evaluation callback is not available");
        }

        struct NLJRightCol {
            const ColumnValue* column = nullptr;
            std::string batch_name;
        };
        std::vector<NLJRightCol> nlj_right;
        nlj_right.reserve(right.columns.size());
        {
            robin_hood::unordered_set<std::string> batch_left_names;
            for (const auto& entry : left.columns) {
                batch_left_names.insert(entry.name);
            }
            for (const auto& entry : right.columns) {
                std::string name = entry.name;
                while (batch_left_names.contains(name)) {
                    name += "_right";
                }
                batch_left_names.insert(name);
                nlj_right.push_back({.column = entry.column.get(), .batch_name = std::move(name)});
            }
        }

        std::vector<std::size_t> left_idx;
        std::vector<std::size_t> right_idx;
        std::vector<std::size_t> key_right_idx;
        std::vector<std::uint8_t> right_matched_pred;
        if (preserve_right_rows) {
            right_matched_pred.assign(n_right, 0U);
        }

        for (std::size_t l = 0; l < n_left; ++l) {
            Table batch;
            batch.columns.reserve(left.columns.size() + nlj_right.size());

            for (std::size_t ci = 0; ci < left.columns.size(); ++ci) {
                auto col = make_empty_like(*left.columns[ci].column);
                for (std::size_t j = 0; j < n_right; ++j) {
                    append_value(col, *left.columns[ci].column, l);
                }
                batch.add_column(output.columns[ci].name, std::move(col));
            }
            for (const auto& item : nlj_right) {
                batch.add_column(item.batch_name, *item.column);
            }

            auto mask_res = mask_evaluator(*predicate, batch, scalars, n_right);
            if (!mask_res) {
                return std::unexpected(mask_res.error());
            }

            bool left_had_match = false;
            for (std::size_t j = 0; j < n_right; ++j) {
                const bool match =
                    mask_res->value[j] != 0 && (!mask_res->valid || (*mask_res->valid)[j] != 0);
                if (!match) {
                    continue;
                }
                left_had_match = true;
                if (semi_join) {
                    left_idx.push_back(l);
                    right_idx.push_back(kNull);  // semi: no right columns
                    break;
                }
                if (anti_join) {
                    break;
                }
                left_idx.push_back(l);
                right_idx.push_back(j);
                if (preserve_right_rows) {
                    right_matched_pred[j] = 1U;
                }
            }

            if (!left_had_match) {
                if (anti_join || preserve_left_rows) {
                    left_idx.push_back(l);
                    right_idx.push_back(kNull);
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched_pred[r] == 0U) {
                    left_idx.push_back(kNull);
                    right_idx.push_back(r);
                    key_right_idx.resize(left_idx.size(), kNull);
                    key_right_idx.back() = r;
                }
            }
        }

        materialize(left_idx, right_idx, key_right_idx);
        return output;
    }

    // ── Helper: build index arrays from a right-row → left-matches lookup ─
    // Used by the small-left/large-right paths. Probes the index ONCE per
    // right row, recording each hit's match span, then replays the hits to
    // scatter — the previous shape re-ran the lookup for the fill pass,
    // paying a second full round of hash probes over the large side (the
    // same double-probe the chunked swapped join shed for q03).
    auto build_indices_from_right_scan = [&](auto&& left_matches_for_right_row)
        -> std::tuple<std::vector<std::size_t>, std::vector<std::size_t>,
                      std::vector<std::size_t>> {
        struct Hit {
            std::size_t right_row;
            std::span<const std::size_t> matches;
        };
        std::vector<Hit> hits;

        // Phase 1: probe once per right row; count matches per left row.
        std::vector<std::size_t> match_counts(n_left, 0);
        std::size_t total_matches = 0;
        std::vector<std::uint8_t> right_matched_flags;
        if (preserve_right_rows) {
            right_matched_flags.assign(n_right, 0U);
        }

        for (std::size_t r = 0; r < n_right; ++r) {
            const std::span<const std::size_t> matches = left_matches_for_right_row(r);
            if (matches.empty()) {
                continue;
            }
            hits.push_back(Hit{.right_row = r, .matches = matches});
            for (const std::size_t l : matches) {
                ++match_counts[l];
            }
            total_matches += matches.size();
            if (preserve_right_rows) {
                right_matched_flags[r] = 1U;
            }
        }

        // Phase 2: replay the hits into a right-match array indexed by left
        // offsets. Hits come in ascending right-row order, so each left row's
        // matches stay ascending — the order the probe-twice version produced.
        std::vector<std::size_t> match_offsets(n_left + 1, 0);
        for (std::size_t l = 0; l < n_left; ++l) {
            match_offsets[l + 1] = match_offsets[l] + match_counts[l];
        }
        std::vector<std::size_t> right_matches(total_matches);
        std::vector<std::size_t> next_off = match_offsets;
        for (const Hit& hit : hits) {
            for (const std::size_t l : hit.matches) {
                right_matches[next_off[l]++] = hit.right_row;
            }
        }
        hits.clear();
        hits.shrink_to_fit();

        // Phase 3: build left_idx / right_idx.
        std::size_t unmatched_right = 0;
        if (preserve_right_rows) {
            unmatched_right = static_cast<std::size_t>(std::count(
                right_matched_flags.begin(), right_matched_flags.end(), std::uint8_t{0}));
        }

        if (semi_join || anti_join) {
            // Membership join: left-only output.
            std::vector<std::size_t> li;
            li.reserve(n_left);
            std::vector<std::size_t> ri;  // always kNull for semi/anti
            for (std::size_t l = 0; l < n_left; ++l) {
                const bool has_match = (match_counts[l] > 0);
                if ((semi_join && has_match) || (anti_join && !has_match)) {
                    li.push_back(l);
                }
            }
            ri.assign(li.size(), kNull);
            return std::make_tuple(std::move(li), std::move(ri), std::vector<std::size_t>{});
        }

        // Preserving join.
        std::size_t out_rows = 0;
        for (std::size_t l = 0; l < n_left; ++l) {
            auto preserve = preserve_left_rows ? 1U : 0U;
            out_rows += match_counts[l] == 0 ? preserve : match_counts[l];
        }
        out_rows += unmatched_right;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        std::vector<std::size_t> kri;
        li.reserve(out_rows);
        ri.reserve(out_rows);

        for (std::size_t l = 0; l < n_left; ++l) {
            if (match_counts[l] == 0) {
                if (preserve_left_rows) {
                    li.push_back(l);
                    ri.push_back(kNull);
                }
                continue;
            }
            for (std::size_t idx = match_offsets[l]; idx < match_offsets[l + 1]; ++idx) {
                li.push_back(l);
                ri.push_back(right_matches[idx]);
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched_flags[r] == 0U) {
                    li.push_back(kNull);
                    ri.push_back(r);
                    kri.resize(li.size(), kNull);
                    kri.back() = r;
                }
            }
        }

        return std::make_tuple(std::move(li), std::move(ri), std::move(kri));
    };

    // ── Helper: build index arrays from a left-row → right-matches lookup ─
    // Used by the large-left/small-right paths. An empty span means "no
    // match".
    auto build_indices_from_left_scan = [&](auto&& right_matches_for_left_row)
        -> std::tuple<std::vector<std::size_t>, std::vector<std::size_t>,
                      std::vector<std::size_t>> {
        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        std::vector<std::size_t> kri;
        li.reserve(n_left);
        ri.reserve(n_left);

        std::vector<std::uint8_t> right_matched_flags;
        if (preserve_right_rows) {
            right_matched_flags.assign(n_right, 0U);
        }

        for (std::size_t l = 0; l < n_left; ++l) {
            const std::span<const std::size_t> matches = right_matches_for_left_row(l);
            const bool has_match = !matches.empty();
            if (semi_join) {
                if (has_match) {
                    li.push_back(l);
                    ri.push_back(kNull);
                }
                continue;
            }
            if (anti_join) {
                if (!has_match) {
                    li.push_back(l);
                    ri.push_back(kNull);
                }
                continue;
            }
            if (!has_match) {
                if (preserve_left_rows) {
                    li.push_back(l);
                    ri.push_back(kNull);
                }
                continue;
            }
            for (const std::size_t r : matches) {
                li.push_back(l);
                ri.push_back(r);
                if (preserve_right_rows) {
                    right_matched_flags[r] = 1U;
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched_flags[r] == 0U) {
                    li.push_back(kNull);
                    ri.push_back(r);
                    kri.resize(li.size(), kNull);
                    kri.back() = r;
                }
            }
        }

        return std::make_tuple(std::move(li), std::move(ri), std::move(kri));
    };

    // ── Single string/categorical key fast path ──────────────────────────
    if (kind != ir::JoinKind::Asof && !has_null_keys && keys.size() == 1 &&
        (std::holds_alternative<Column<std::string>>(*left_keys[0]) ||
         std::holds_alternative<Column<Categorical>>(*left_keys[0]))) {
        // Index the smaller side: key → dense gid, rows per gid in a CSR.
        const bool build_left = n_left < n_right;
        const ColumnValue& build_col = build_left ? *left_keys[0] : *right_keys[0];
        const std::size_t n_build = build_left ? n_left : n_right;

        robin_hood::unordered_map<std::string_view, std::uint32_t, StringViewHash, std::equal_to<>>
            key_gid;
        key_gid.reserve(n_build);
        std::vector<std::uint32_t> row_gid(n_build);
        std::uint32_t n_groups = 0;
        auto assign_gids = [&](const auto& col) {
            for (std::size_t i = 0; i < n_build; ++i) {
                auto [it, inserted] = key_gid.try_emplace(col[i], n_groups);
                n_groups += inserted ? 1U : 0U;
                row_gid[i] = it->second;
            }
        };
        if (const auto* bs = std::get_if<Column<std::string>>(&build_col)) {
            assign_gids(*bs);
        } else {
            assign_gids(std::get<Column<Categorical>>(build_col));
        }
        const GroupedRows grouped = group_rows_csr(row_gid, n_groups);

        if (build_left) {
            // Sides may mix String and Categorical (both are ExprType::String).
            const auto* rs = std::get_if<Column<std::string>>(right_keys[0]);
            const auto* rc = std::get_if<Column<Categorical>>(right_keys[0]);
            auto lookup = [&](std::size_t r) -> std::span<const std::size_t> {
                const std::string_view sv = rs != nullptr ? (*rs)[r] : (*rc)[r];
                auto it = key_gid.find(sv);
                return it == key_gid.end() ? std::span<const std::size_t>{}
                                           : grouped.matches(it->second);
            };
            auto [li, ri, kri] = build_indices_from_right_scan(lookup);
            materialize(li, ri, kri);
            return output;
        }

        if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
            // Resolve each left dictionary code to a build-side gid once, then
            // probe by code — no per-row hashing at all.
            const auto& dict = *lc->dictionary_ptr();
            std::vector<std::uint32_t> code_gid(dict.size(), kNoGroup);
            for (std::size_t code = 0; code < dict.size(); ++code) {
                auto it = key_gid.find(std::string_view{dict[code]});
                if (it != key_gid.end()) {
                    code_gid[code] = it->second;
                }
            }
            const auto* codes = lc->codes_data();
            if (preserve_left_only && grouped.unique()) {
                std::vector<std::size_t> ri(n_left, kNull);
                for (std::size_t l = 0; l < n_left; ++l) {
                    const std::uint32_t gid = code_gid[static_cast<std::size_t>(codes[l])];
                    if (gid != kNoGroup) {
                        ri[l] = grouped.rows[grouped.offsets[gid]];
                    }
                }
                materialize_left_identity(ri);
                return output;
            }
            auto lookup = [&](std::size_t l) -> std::span<const std::size_t> {
                const std::uint32_t gid = code_gid[static_cast<std::size_t>(codes[l])];
                return gid == kNoGroup ? std::span<const std::size_t>{} : grouped.matches(gid);
            };
            auto [li, ri, kri] = build_indices_from_left_scan(lookup);
            materialize(li, ri, kri);
        } else {
            const auto& ls = std::get<Column<std::string>>(*left_keys[0]);
            if (preserve_left_only && grouped.unique()) {
                std::vector<std::size_t> ri(n_left, kNull);
                for (std::size_t l = 0; l < n_left; ++l) {
                    auto it = key_gid.find(ls[l]);
                    if (it != key_gid.end()) {
                        ri[l] = grouped.rows[grouped.offsets[it->second]];
                    }
                }
                materialize_left_identity(ri);
                return output;
            }
            auto lookup = [&](std::size_t l) -> std::span<const std::size_t> {
                auto it = key_gid.find(ls[l]);
                return it == key_gid.end() ? std::span<const std::size_t>{}
                                           : grouped.matches(it->second);
            };
            auto [li, ri, kri] = build_indices_from_left_scan(lookup);
            materialize(li, ri, kri);
        }
        return output;
    }

    // ── Single int64 key fast path ───────────────────────────────────────
    if (kind != ir::JoinKind::Asof && !has_null_keys && keys.size() == 1 &&
        std::holds_alternative<Column<std::int64_t>>(*left_keys[0])) {
        const auto& left_ints = std::get<Column<std::int64_t>>(*left_keys[0]);
        const auto& right_ints = std::get<Column<std::int64_t>>(*right_keys[0]);

        // Index the smaller side: key → dense gid, rows per gid in a CSR.
        const bool build_left = n_left < n_right;
        const auto* build_data = build_left ? left_ints.data() : right_ints.data();
        const std::size_t n_build = build_left ? n_left : n_right;

        robin_hood::unordered_map<std::int64_t, std::uint32_t> key_gid;
        key_gid.reserve(n_build);
        std::vector<std::uint32_t> row_gid(n_build);
        std::uint32_t n_groups = 0;
        for (std::size_t i = 0; i < n_build; ++i) {
            auto [it, inserted] = key_gid.try_emplace(build_data[i], n_groups);
            n_groups += inserted ? 1U : 0U;
            row_gid[i] = it->second;
        }
        const GroupedRows grouped = group_rows_csr(row_gid, n_groups);

        if (build_left) {
            const auto* probe_data = right_ints.data();
            auto lookup = [&](std::size_t r) -> std::span<const std::size_t> {
                auto it = key_gid.find(probe_data[r]);
                return it == key_gid.end() ? std::span<const std::size_t>{}
                                           : grouped.matches(it->second);
            };
            auto [li, ri, kri] = build_indices_from_right_scan(lookup);
            materialize(li, ri, kri);
            return output;
        }

        const auto* probe_data = left_ints.data();
        if (preserve_left_only && grouped.unique()) {
            std::vector<std::size_t> ri(n_left, kNull);
            for (std::size_t l = 0; l < n_left; ++l) {
                auto it = key_gid.find(probe_data[l]);
                if (it != key_gid.end()) {
                    ri[l] = grouped.rows[grouped.offsets[it->second]];
                }
            }
            materialize_left_identity(ri);
            return output;
        }

        auto lookup = [&](std::size_t l) -> std::span<const std::size_t> {
            auto it = key_gid.find(probe_data[l]);
            return it == key_gid.end() ? std::span<const std::size_t>{}
                                       : grouped.matches(it->second);
        };
        auto [li, ri, kri] = build_indices_from_left_scan(lookup);
        materialize(li, ri, kri);
        return output;
    }

    // ── Asof join ────────────────────────────────────────────────────────
    if (kind == ir::JoinKind::Asof) {
        const std::size_t time_pos = *asof_time_key_pos;
        const auto* left_time_col = left_keys[time_pos];
        const auto* right_time_col = right_keys[time_pos];

        auto time_value = [](const ColumnValue& col,
                             std::size_t row) -> std::optional<std::int64_t> {
            if (const auto* ts = std::get_if<Column<Timestamp>>(&col)) {
                return (*ts)[row].nanos;
            }
            if (const auto* day = std::get_if<Column<Date>>(&col)) {
                return static_cast<std::int64_t>((*day)[row].days);
            }
            if (const auto* ints = std::get_if<Column<std::int64_t>>(&col)) {
                return (*ints)[row];
            }
            return std::nullopt;
        };

        const std::string& time_key = keys[time_pos];

        auto wrong_type_error = [&](const char* label, const ColumnValue& col) {
            return "asof join: time index column '" + time_key + "' on " + label + " has type " +
                   format_expr_type(column_kind(col)) +
                   ", but asof requires Timestamp, Date, or Int";
        };

        // The two-pointer merge only needs the time keys as int64. Timestamp is
        // layout-compatible with int64_t (a single `nanos` member), so for the
        // common Timestamp case read the column storage directly and skip
        // materialising two int64 arrays — the right one spans the whole right
        // table (128 MB at 16M rows). Date/Int convert into a backing buffer.
        std::vector<std::int64_t> left_times_buf;
        std::vector<std::int64_t> right_times_buf;
        auto as_int64_view =
            [&](const ColumnValue& col, std::size_t n, std::vector<std::int64_t>& buf,
                const char* label) -> std::expected<const std::int64_t*, std::string> {
            if (const auto* ts = std::get_if<Column<Timestamp>>(&col)) {
                static_assert(sizeof(Timestamp) == sizeof(std::int64_t));
                return reinterpret_cast<const std::int64_t*>(ts->data());
            }
            buf.reserve(n);
            for (std::size_t i = 0; i < n; ++i) {
                auto v = time_value(col, i);
                if (!v.has_value()) {
                    return std::unexpected(wrong_type_error(label, col));
                }
                buf.push_back(*v);
            }
            return buf.data();
        };

        auto left_times_v = as_int64_view(*left_time_col, n_left, left_times_buf, "left");
        if (!left_times_v) {
            return std::unexpected(left_times_v.error());
        }
        auto right_times_v = as_int64_view(*right_time_col, n_right, right_times_buf, "right");
        if (!right_times_v) {
            return std::unexpected(right_times_v.error());
        }
        const std::int64_t* left_times = *left_times_v;
        const std::int64_t* right_times = *right_times_v;

        const bool left_sorted = std::is_sorted(left_times, left_times + n_left);
        const bool right_sorted = std::is_sorted(right_times, right_times + n_right);
        if (!left_sorted || !right_sorted) {
            const char* which = (!left_sorted && !right_sorted)
                                    ? "both sides are"
                                    : (!left_sorted ? "left is" : "right is");
            return std::unexpected(
                std::string("asof join: ") + which + " not sorted ascending by time index '" +
                time_key +
                "' — silent look-ahead bias would be the result if this were allowed"
                "\n  hint: order the offending side first, e.g. `<table>[order " +
                time_key + "]` before promoting with as_timeframe()");
        }

        std::vector<const ColumnValue*> left_eq_keys;
        std::vector<const ColumnValue*> right_eq_keys;
        left_eq_keys.reserve(keys.size() - 1);
        right_eq_keys.reserve(keys.size() - 1);
        std::vector<const ValidityBitmap*> left_eq_validity;
        std::vector<const ValidityBitmap*> right_eq_validity;
        bool has_null_eq_keys = false;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (i == time_pos) {
                continue;
            }
            left_eq_keys.push_back(left_keys[i]);
            right_eq_keys.push_back(right_keys[i]);
            left_eq_validity.push_back(left_key_validity[i]);
            right_eq_validity.push_back(right_key_validity[i]);
            has_null_eq_keys = has_null_eq_keys || left_key_validity[i] != nullptr ||
                               right_key_validity[i] != nullptr;
        }
        // As in an equi-join, a null equality key matches nothing — not even
        // another null. A null-keyed right row is never a candidate, and a
        // null-keyed left row is left unmatched.
        const auto left_eq_is_null = [&](std::size_t l) {
            return has_null_eq_keys && row_key_is_null(left_eq_validity, l);
        };
        const auto right_eq_is_null = [&](std::size_t r) {
            return has_null_eq_keys && row_key_is_null(right_eq_validity, r);
        };

        // Asof keeps every left row exactly once in input order, so the left
        // side is the identity permutation — only the matched right row per left
        // row varies. We therefore build just right_idx and materialise the left
        // columns wholesale (no identity gather, no n_left index array).
        std::vector<std::size_t> right_idx(n_left);
        bool grouped_done = false;

        if (left_eq_keys.empty()) {
            // Time-only asof (the canonical case): with no equality keys every
            // right row is a candidate, so a single two-pointer merge over the
            // already-sorted time arrays finds the latest right row at-or-before
            // each left time in O(n_left + n_right). Skips the per-row Key
            // construction + hashing that otherwise builds one giant group over
            // the entire right table and dominates the cost for large rights.
            std::size_t pos = 0;  // # right rows with time <= current left time
            for (std::size_t l = 0; l < n_left; ++l) {
                while (pos < n_right && right_times[pos] <= left_times[l]) {
                    ++pos;
                }
                right_idx[l] = (pos == 0) ? kNull : pos - 1;
            }
            grouped_done = true;
        } else if (left_eq_keys.size() == 1 && !has_null_eq_keys) {
            // Single equality key (the common asof-by case, e.g. by symbol):
            // factorise the key column into dense codes by hashing its native
            // values into a small dictionary (one entry per distinct key), bucket
            // the right rows by code in ascending-time order, then two-pointer
            // merge per bucket. No per-row Key/ScalarValue heap allocation or
            // string copy — just value hashing into a dictionary sized to the key
            // cardinality, and one cursor per group instead of a second hash map.
            // Falls through to the generic path for key column types without a
            // usable hash (Timestamp/Date) or a left/right column type mismatch.
            const ColumnValue& rcol = *right_eq_keys[0];
            const ColumnValue& lcol = *left_eq_keys[0];
            std::visit(
                [&](const auto& rc) {
                    using ColT = std::decay_t<decltype(rc)>;
                    using KeyV = std::decay_t<decltype(rc[std::size_t{0}])>;
                    if constexpr (std::is_same_v<KeyV, std::string_view> ||
                                  std::is_arithmetic_v<KeyV>) {
                        const auto* lcp = std::get_if<ColT>(&lcol);
                        if (lcp == nullptr) {
                            return;  // left/right key types differ -> generic path
                        }
                        const auto& lc = *lcp;
                        robin_hood::unordered_map<KeyV, std::size_t> dict;
                        std::vector<std::vector<std::size_t>> buckets;
                        for (std::size_t r = 0; r < n_right; ++r) {
                            auto [it, inserted] = dict.try_emplace(rc[r], buckets.size());
                            if (inserted) {
                                buckets.emplace_back();
                            }
                            buckets[it->second].push_back(r);
                        }
                        std::vector<std::size_t> cursor(buckets.size(), 0);
                        for (std::size_t l = 0; l < n_left; ++l) {
                            auto it = dict.find(lc[l]);
                            if (it == dict.end()) {
                                right_idx[l] = kNull;
                                continue;
                            }
                            const auto& rows = buckets[it->second];
                            std::size_t& pos = cursor[it->second];
                            while (pos < rows.size() && right_times[rows[pos]] <= left_times[l]) {
                                ++pos;
                            }
                            right_idx[l] = (pos == 0) ? kNull : rows[pos - 1];
                        }
                        grouped_done = true;
                    }
                },
                rcol);
        }

        if (!grouped_done) {
            robin_hood::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_groups;
            right_groups.reserve(n_right);
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_eq_is_null(r)) {
                    continue;  // never a candidate for any left row
                }
                Key group;
                group.values.reserve(right_eq_keys.size());
                for (const auto* col : right_eq_keys) {
                    group.values.push_back(scalar_from_column(*col, r));
                }
                right_groups[group].push_back(r);
            }

            robin_hood::unordered_map<Key, std::size_t, KeyHash, KeyEq> right_pos;
            right_pos.reserve(right_groups.size());

            for (std::size_t l = 0; l < n_left; ++l) {
                if (left_eq_is_null(l)) {
                    right_idx[l] = kNull;  // matches nothing, including another null
                    continue;
                }
                Key group;
                group.values.reserve(left_eq_keys.size());
                for (const auto* col : left_eq_keys) {
                    group.values.push_back(scalar_from_column(*col, l));
                }

                auto it = right_groups.find(group);
                if (it == right_groups.end()) {
                    right_idx[l] = kNull;
                    continue;
                }

                auto [pos_it, inserted] = right_pos.try_emplace(group, 0);
                (void)inserted;
                std::size_t& pos = pos_it->second;
                const auto& rows = it->second;
                while (pos < rows.size() && right_times[rows[pos]] <= left_times[l]) {
                    ++pos;
                }

                right_idx[l] = (pos == 0) ? kNull : rows[pos - 1];
            }
        }

        materialize_left_identity(right_idx);
        output.time_index = left.time_index;
        return output;
    }

    // ── Generic multi-key fallback ───────────────────────────────────────
    // Hash and compare the key columns in place. The old shape boxed a Key —
    // a heap-allocated vector<ScalarValue>, one std::visit hash per component,
    // a string copy per string key — for EVERY row on both sides; that was
    // the exact pattern already removed from group-by (see KeyRowIndex).
    std::vector<KeyCol> left_cols;
    std::vector<KeyCol> right_cols;
    left_cols.reserve(keys.size());
    right_cols.reserve(keys.size());
    for (std::size_t i = 0; i < keys.size(); ++i) {
        // Null-keyed rows are skipped before hashing (they match nothing), so
        // the KeyCols carry no validity.
        auto lc = make_key_col(*left_keys[i], nullptr);
        auto rc = make_key_col(*right_keys[i], nullptr);
        if (!lc.has_value() || !rc.has_value()) {
            return std::unexpected("join: unsupported key column type for " + keys[i]);
        }
        left_cols.push_back(*lc);
        right_cols.push_back(*rc);
    }

    const bool build_left = n_left < n_right;
    const auto& build_cols = build_left ? left_cols : right_cols;
    const auto& probe_cols = build_left ? right_cols : left_cols;
    const auto& build_key_validity = build_left ? left_key_validity : right_key_validity;
    const auto& probe_key_validity = build_left ? right_key_validity : left_key_validity;
    const std::size_t n_build = build_left ? n_left : n_right;

    RowKeyIndex index;
    index.reserve(n_build);
    std::vector<std::uint32_t> row_gid(n_build, kNoGroup);
    for (std::size_t i = 0; i < n_build; ++i) {
        if (has_null_keys && row_key_is_null(build_key_validity, i)) {
            continue;  // never matchable, so never indexed
        }
        row_gid[i] = index.find_or_insert(build_cols, i);
    }
    const GroupedRows grouped = group_rows_csr(row_gid, index.size());

    auto lookup = [&](std::size_t row) -> std::span<const std::size_t> {
        if (has_null_keys && row_key_is_null(probe_key_validity, row)) {
            return {};  // matches nothing, including another null
        }
        const std::uint32_t gid = index.find(probe_cols, row, build_cols);
        return gid == kNoGroup ? std::span<const std::size_t>{} : grouped.matches(gid);
    };

    if (build_left) {
        auto [li, ri, kri] = build_indices_from_right_scan(lookup);
        materialize(li, ri, kri);
        return output;
    }

    if (preserve_left_only && grouped.unique()) {
        std::vector<std::size_t> ri(n_left, kNull);
        for (std::size_t l = 0; l < n_left; ++l) {
            const std::span<const std::size_t> matches = lookup(l);
            if (!matches.empty()) {
                ri[l] = matches.front();
            }
        }
        materialize_left_identity(ri);
        return output;
    }

    auto [li, ri, kri] = build_indices_from_left_scan(lookup);
    materialize(li, ri, kri);
    return output;
}

}  // namespace ibex::runtime
