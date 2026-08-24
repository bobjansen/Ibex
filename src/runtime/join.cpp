// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <algorithm>
#include <atomic>
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

/// Mark a join row as matched while multiple probe ranges may reach it.
inline void set_matched_flag(std::uint8_t& flag) noexcept {
#ifdef __cpp_lib_atomic_ref
    std::atomic_ref<std::uint8_t>(flag).store(1U, std::memory_order_relaxed);
#else
    // Apple's libc++ (macOS clang-werror leg) doesn't ship std::atomic_ref yet.
    // A byte atomic is lock-free on the supported targets, so this view gives
    // the same relaxed store without needing atomic_ref.
    static_assert(std::atomic<std::uint8_t>::is_always_lock_free);
    reinterpret_cast<std::atomic<std::uint8_t>*>(&flag)->store(1U, std::memory_order_relaxed);
#endif
}

/// CSR view over the build side: matches(gid) is the ascending list of build
/// rows whose key belongs to group gid.
///
/// CSR ("compressed sparse row", after the sparse-matrix layout) means one
/// flat buffer holding every row index once, plus an offsets array marking
/// where each group's run starts — so group `g` is `rows[offsets[g] ..
/// offsets[g + 1])` and `offsets` carries one entry more than there are
/// groups. Replaces one heap-allocated
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
/// in ExprType, which key validation already enforced.
///
/// Null-ness is compared before value, and this is not optional even though
/// under `NullMatch::Never` no null-keyed row reaches here: a null cell carries
/// its type's zero, so comparing values alone would make a null equal a genuine
/// zero. Under `Equal` that is exactly the pair that must NOT match, while two
/// nulls must — which is the same rule stated once, for both.
auto key_rows_equal(const std::vector<KeyCol>& a, std::size_t ra, const std::vector<KeyCol>& b,
                    std::size_t rb) -> bool {
    for (std::size_t i = 0; i < a.size(); ++i) {
        const KeyCol& ca = a[i];
        const KeyCol& cb = b[i];
        const bool a_null = ca.is_null(ra);
        if (a_null != cb.is_null(rb)) {
            return false;
        }
        if (a_null) {
            continue;  // both null in this component: equal, and no value to read
        }
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

/// Gather `entry` through `idx`, keeping BOTH kinds of null: the sentinel
/// positions (`kNull` — a row with no counterpart) and the rows that were
/// already null in the source. `gather_column_with_nulls` produces only the
/// first, so a source null came out as the type's zero — the very conflation
/// the join's key rules go to such lengths to avoid, one step later.
///
/// Takes no `ExecutionContext`: this is a whole-column body, and whether it runs
/// on a worker is `gather_entries`' decision, made once for all the columns.
/// Passing a context here would re-open that decision per column, which is the
/// per-column fan-out this batching exists to remove.
auto gather_entry(const ColumnEntry& entry, const std::size_t* idx, std::size_t total)
    -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
    auto [column, validity] = gather_column_with_nulls(*entry.column, idx, total, kNull, nullptr);
    if (!entry.validity.has_value()) {
        return {std::move(column), std::move(validity)};
    }
    ValidityBitmap bitmap =
        validity.has_value() ? std::move(*validity) : ValidityBitmap(total, true);
    for (std::size_t i = 0; i < total; ++i) {
        if (idx[i] != kNull && !(*entry.validity)[idx[i]]) {
            bitmap.set(i, false);
        }
    }
    return {std::move(column), std::move(bitmap)};
}

/// True when `idx` holds at least one `kNull`, i.e. an output row with no
/// counterpart on this side.
auto index_has_sentinel(const std::size_t* idx, std::size_t total) -> bool {
    for (std::size_t i = 0; i < total; ++i) {
        if (idx[i] == kNull) {
            return true;
        }
    }
    return false;
}

/// One output column the join still has to gather.
struct GatherJob {
    const ColumnEntry* entry = nullptr;  ///< source column
    const std::size_t* idx = nullptr;    ///< output row -> source row
    std::size_t out_index = 0;           ///< destination column in the output table
    bool has_sentinel = false;           ///< `idx` contains kNull
};

/// Gather every job in ONE worker batch, delegating the task shape to
/// `gather_columns_batched`.
///
/// The join used to call `gather_entry` in a loop and let each call fan its own
/// rows out, which submitted and waited a batch per column. See that helper for
/// the measurement that motivated batching. The join-specific part is only
/// which columns are indivisible (a side whose index carries `kNull`) and what
/// a whole-column gather means here (`gather_entry`, which merges the sentinel
/// nulls with the source's own).
auto gather_entries(std::span<const GatherJob> jobs, std::size_t total,
                    const ExecutionContext* exec) -> std::vector<GatheredColumn> {
    std::vector<ColumnGatherJob> column_jobs;
    column_jobs.reserve(jobs.size());
    for (const auto& job : jobs) {
        column_jobs.push_back({
            .column = job.entry->column.get(),
            .validity = job.entry->validity.has_value() ? &*job.entry->validity : nullptr,
            .idx = job.idx,
            .indivisible = job.has_sentinel,
        });
    }
    return gather_columns_batched(column_jobs, total, exec, [&](std::size_t j) -> GatheredColumn {
        return gather_entry(*jobs[j].entry, jobs[j].idx, total);
    });
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
        case ExprType::Categorical:
            return "Categorical";
    }
    return "?";
}

/// `expect n:1` as written, for the diagnostic.
auto format_expect(const ir::JoinExpect& expect) -> std::string {
    const auto side = [](ir::JoinMultiplicity m) {
        return m == ir::JoinMultiplicity::One ? "1" : "n";
    };
    return std::string(side(expect.left)) + ":" + side(expect.right);
}

auto format_key(const ir::JoinKey& key) -> std::string {
    return key.left == key.right ? key.left : key.left + " = " + key.right;
}

auto format_keys(const std::vector<ir::JoinKey>& keys) -> std::string {
    if (keys.empty()) {
        return "<none>";
    }
    if (keys.size() == 1) {
        return format_key(keys.front());
    }
    std::string out = "{";
    for (std::size_t i = 0; i < keys.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        out.append(format_key(keys[i]));
    }
    out.push_back('}');
    return out;
}

/// Build the "not a TimeFrame" diagnostic. Names which side(s) are bare
/// DataFrames, lists their columns, and — when there's an obvious time-like
/// column on a failing side — suggests the precise `as_timeframe(...)` call
/// that would fix the call site.
auto asof_not_timeframe_error(const Table& left, const Table& right) -> std::string {
    const bool left_bad = !left.time_index().has_value();
    const bool right_bad = !right.time_index().has_value();

    std::string msg = "asof join requires both sides to be TimeFrame";
    if (left_bad && right_bad) {
        msg.append("; neither side has been promoted with as_timeframe()");
    } else if (left_bad) {
        msg.append("; left side is a DataFrame (right is TimeFrame on '" + *right.time_index() +
                   "')");
    } else {
        msg.append("; right side is a DataFrame (left is TimeFrame on '" + *left.time_index() +
                   "')");
    }

    auto add_side_hint = [&](const Table& side, const char* label) {
        if (side.time_index().has_value()) {
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

/// Batch-table name for a right column in a nested-loop join predicate.
/// `#` cannot appear in an Ibex identifier, so this name is unreachable from
/// source and cannot shadow a left column.
auto nlj_right_batch_name(std::string_view name) -> std::string {
    return "#right#" + std::string(name);
}

/// Rewrites a join predicate's column references to the names the batch table
/// uses, resolving which side each one means.
///
/// `left(x)` and `right(x)` say it outright. A bare name resolves to whichever
/// input has the column; a name both inputs have is ambiguous and is rejected
/// rather than silently picking one. A bare name neither input has is left
/// alone — it may still resolve as a scalar binding (SPEC.md Section 6.2).
auto resolve_predicate_sides(const ir::Expr& predicate, const Table& left, const Table& right)
    -> std::expected<ir::Expr, std::string> {
    // `ExprPtr` deep-copies, so this is an independent tree to rewrite in place.
    ir::Expr rewritten = predicate;
    std::optional<std::string> failure;

    const auto resolve_ref = [&](ir::ColumnRef& ref) {
        if (ref.lexical) {
            return;  // `^name` never names a column
        }
        const bool in_left = left.find(ref.name) != nullptr;
        const bool in_right = right.find(ref.name) != nullptr;
        switch (ref.side) {
            case ir::JoinSide::Left:
                if (!in_left) {
                    failure = "join predicate: left(" + ref.name +
                              ") but the left input has no column \"" + ref.name + "\"";
                }
                break;
            case ir::JoinSide::Right:
                if (!in_right) {
                    failure = "join predicate: right(" + ref.name +
                              ") but the right input has no column \"" + ref.name + "\"";
                    break;
                }
                ref.name = nlj_right_batch_name(ref.name);
                break;
            case ir::JoinSide::Any:
                if (in_left && in_right) {
                    failure = "join predicate: \"" + ref.name +
                              "\" exists in both inputs; write left(" + ref.name + ") or right(" +
                              ref.name + ")";
                    break;
                }
                if (in_right) {
                    ref.name = nlj_right_batch_name(ref.name);
                }
                break;
        }
        ref.side = ir::JoinSide::Any;  // resolved: the batch is one namespace
    };

    const auto walk = [&](ir::Expr& expr, const auto& self) -> void {
        std::visit(
            [&](auto& node) {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, ir::ColumnRef>) {
                    resolve_ref(node);
                } else if constexpr (std::is_same_v<T, ir::BinaryExpr> ||
                                     std::is_same_v<T, ir::CompareExpr> ||
                                     std::is_same_v<T, ir::LogicalExpr>) {
                    self(*node.left, self);
                    if (node.right != nullptr) {  // unary `not` has no right
                        self(*node.right, self);
                    }
                } else if constexpr (std::is_same_v<T, ir::IsNullExpr>) {
                    self(*node.operand, self);
                } else if constexpr (std::is_same_v<T, ir::CallExpr>) {
                    for (auto& arg : node.args) {
                        self(*arg, self);
                    }
                } else {
                    // Literal and RankExpr hold no column references.
                    static_assert(std::is_same_v<T, ir::Literal> || std::is_same_v<T, ir::RankExpr>,
                                  "join predicate walk is missing an Expr alternative");
                }
            },
            expr.node);
    };

    walk(rewritten, walk);
    if (failure.has_value()) {
        return std::unexpected(std::move(*failure));
    }
    return rewritten;
}

}  // namespace

auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                     const std::vector<ir::JoinKey>& keys, const ir::Expr* predicate,
                     const ScalarRegistry* scalars, PredicateMaskEvaluator mask_evaluator,
                     const ir::JoinSuffixPolicy& suffix,
                     const std::vector<ir::OrderKey>& pending_order, ir::NullMatch null_match,
                     const ir::JoinExpect& expect, ir::MatchSelection take,
                     const ExecutionContext* exec) -> std::expected<Table, std::string> {
    if (predicate == nullptr && kind != ir::JoinKind::Cross && keys.empty()) {
        return std::unexpected("join requires at least one key");
    }

    // Asof preconditions run before per-key column validation: a typical
    // "forgot as_timeframe" mistake produces a Timestamp-vs-Int mismatch on
    // the time key, but the actionable diagnosis is "promote the other side",
    // not "your types don't match".
    std::optional<std::size_t> asof_time_key_pos;
    if (kind == ir::JoinKind::Asof) {
        if (!left.time_index().has_value() || !right.time_index().has_value()) {
            return std::unexpected(asof_not_timeframe_error(left, right));
        }
        const std::string& left_time_key = *left.time_index();
        const std::string& right_time_key = *right.time_index();
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i].left == left_time_key && keys[i].right == right_time_key) {
                asof_time_key_pos = i;
                break;
            }
        }
        if (!asof_time_key_pos.has_value()) {
            std::string suggested;
            if (keys.empty()) {
                suggested = left_time_key == right_time_key
                                ? left_time_key
                                : left_time_key + " = " + right_time_key;
            } else {
                suggested = "{" + (left_time_key == right_time_key
                                       ? left_time_key
                                       : left_time_key + " = " + right_time_key);
                for (const auto& k : keys) {
                    suggested.append(", ");
                    suggested.append(format_key(k));
                }
                suggested.push_back('}');
            }
            return std::unexpected(
                "asof join: the 'on' keys must include the time index '" + left_time_key +
                "' (left) and '" + right_time_key + "' (right)\n  got:  on " + format_keys(keys) +
                "\n  hint: did you mean `... asof join ... on " + suggested + "`?");
        }
    }

    std::vector<const ColumnValue*> left_keys;
    std::vector<const ColumnValue*> right_keys;
    left_keys.reserve(keys.size());
    right_keys.reserve(keys.size());
    for (const auto& key : keys) {
        const auto* left_col = left.find(key.left);
        if (left_col == nullptr) {
            return std::unexpected("join key not found in left: " + key.left +
                                   " (available: " + format_columns(left) + ")");
        }
        const auto* right_col = right.find(key.right);
        if (right_col == nullptr) {
            return std::unexpected("join key not found in right: " + key.right +
                                   " (available: " + format_columns(right) + ")");
        }
        if (column_kind(*left_col) != column_kind(*right_col)) {
            // Same wording as the static `ir::check_joins` diagnostic: this is
            // the same failure, reached only when a schema was not known ahead
            // of execution.
            return std::unexpected("join key type mismatch: left '" + key.left + "' is " +
                                   format_expr_type(column_kind(*left_col)) + " but right '" +
                                   key.right + "' is " + format_expr_type(column_kind(*right_col)));
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
        const auto* left_entry = left.find_entry(key.left);
        const auto* right_entry = right.find_entry(key.right);
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
    // Under `Never` a null-keyed row is kept out of the index and never looked
    // up -- the pair of half-measures that makes it match nothing. Under
    // `Equal` it takes part like any other key, and the null-aware hashing and
    // equality below pair it with the nulls on the other side and nothing else.
    const bool skip_null_keys = null_match == ir::NullMatch::Never;

    const std::size_t n_left = left.rows();
    const std::size_t n_right = right.rows();

    const bool preserve_left_rows =
        (kind == ir::JoinKind::Left || kind == ir::JoinKind::Outer || kind == ir::JoinKind::Asof);
    const bool preserve_right_rows = (kind == ir::JoinKind::Right || kind == ir::JoinKind::Outer);
    const bool semi_join = (kind == ir::JoinKind::Semi);
    const bool anti_join = (kind == ir::JoinKind::Anti);

    // The output column list and its collision names come from the shared
    // planner (see ir/join_output.hpp), the same one IR schema inference uses.
    // Resolve the predicate before planning the output. Both an ambiguous
    // predicate name and an output collision can be true of the same join, and
    // the predicate is the more actionable of the two: a suffix clause renames
    // output columns but leaves `on v < v` just as ambiguous, so reporting the
    // collision first would send the reader to the wrong fix.
    std::optional<ir::Expr> resolved_predicate;
    if (predicate != nullptr) {
        auto resolved = resolve_predicate_sides(*predicate, left, right);
        if (!resolved.has_value()) {
            return std::unexpected(std::move(resolved.error()));
        }
        resolved_predicate = std::move(*resolved);
    }

    auto planned = ir::plan_join_output(kind, keys, table_column_names(left),
                                        table_column_names(right), suffix);
    if (!planned.has_value()) {
        return std::unexpected(std::move(planned.error()));
    }
    const std::vector<ir::JoinOutputColumn>& plan = *planned;

    Table output;
    output.columns.reserve(plan.size());

    struct RightOut {
        const ColumnEntry* entry = nullptr;  ///< source column, with its validity
        std::size_t out_index = 0;
    };
    std::vector<RightOut> right_out;
    right_out.reserve(plan.size() - left.columns.size());
    for (const auto& column : plan) {
        const Table& source = column.side == ir::JoinOutputSide::Left ? left : right;
        const ColumnValue* src = source.columns[column.source_index].column.get();
        output.add_column(column.name, make_empty_like(*src));
        if (column.side == ir::JoinOutputSide::Right) {
            right_out.push_back(RightOut{.entry = &source.columns[column.source_index],
                                         .out_index = output.columns.size() - 1});
        }
    }

    const bool preserve_left_only =
        preserve_left_rows && !preserve_right_rows && kind != ir::JoinKind::Asof;

    // ── Row order carried over from the left input ────────────────────────
    // A join promises no row order (SPEC.md), and this changes nothing about
    // that. But a path that happens to emit the left rows in their own order
    // still *produces* one, and saying so lets a following `order` skip a sort
    // instead of re-establishing what the rows already satisfy. Which path runs
    // is a build-side decision made from the two row counts, so this is knowable
    // here and nowhere earlier.
    //
    // Duplicate matches for one left row are emitted adjacently, which a
    // non-strict ordering tolerates: (1, 1, 3) is still ascending.
    std::vector<ir::OrderKey> carried_ordering;
    if (left.properties().ordering().has_value()) {
        // The output planner renames a colliding left column and drops every
        // left column from neither semi nor anti, so a claim has to be restated
        // in the output's own names -- or dropped when a key cannot be named.
        robin_hood::unordered_map<std::string, std::string> left_output_name;
        for (const auto& column : plan) {
            if (column.side == ir::JoinOutputSide::Left) {
                left_output_name.emplace(left.columns[column.source_index].name, column.name);
            }
        }
        for (const auto& key : *left.properties().ordering()) {
            const auto it = left_output_name.find(key.name);
            if (it == left_output_name.end()) {
                carried_ordering.clear();
                break;
            }
            carried_ordering.push_back(
                ir::OrderKey{.name = it->second, .ascending = key.ascending});
        }
    }

    auto claim_carried_ordering = [&] {
        if (!carried_ordering.empty()) {
            output.set_properties(output.properties().with_ordering(carried_ordering));
        }
    };

    // ── Build side ────────────────────────────────────────────────────────
    // The default is to index the smaller side, which is right whenever there
    // is nothing else to weigh: the probe side is scanned once either way, and
    // a smaller index is the one that stays in cache.
    //
    // The exception is a pending `order` the left already satisfies. Indexing
    // the right instead scans the left, which emits in left-row order, which
    // that `order` then finds it has nothing to do. Trading a bigger index for
    // a whole sort is worth it -- but only while "bigger" stays modest, since
    // the index is probed once per row of the other side and a large one misses
    // cache on nearly every probe. Beyond the ratio the sort is the cheaper of
    // the two costs and the default wins.
    //
    // Semi and anti joins are excluded because they emit in left-row order
    // whichever side is indexed (see `build_indices_from_right_scan`), so there
    // is no trade to make -- forcing the side would pay the cost for nothing.
    constexpr std::size_t kMaxOrderPreservingBuildRatio = 4;
    const bool order_preserving_pays =
        !pending_order.empty() && !carried_ordering.empty() && !semi_join && !anti_join &&
        TableProperties::sorted_by(carried_ordering).satisfies(pending_order) &&
        n_right <= kMaxOrderPreservingBuildRatio * n_left;
    // True means "index the left, scan the right", which does NOT preserve the
    // left's order. Hence the negation: preserving it means indexing the right.
    const bool build_left = n_left < n_right && !order_preserving_pays;

    // ── `expect L:R` ──────────────────────────────────────────────────────
    // Checked against the pairs actually emitted, which is what makes one
    // check serve every join kind and every path through this function: a
    // violation is a row index appearing twice in the emitted array, whatever
    // produced it.
    //
    // Nothing is checked when the clause is absent, so a join that does not
    // declare a shape pays nothing.
    std::string expect_error;
    auto check_expect = [&](const std::vector<std::size_t>* left_idx,
                            const std::vector<std::size_t>* right_idx, std::size_t total) {
        const auto first_repeat = [&](const std::vector<std::size_t>* idx,
                                      std::size_t side_rows) -> std::optional<std::size_t> {
            if (idx == nullptr) {
                return std::nullopt;  // identity: each row of that side appears once
            }
            std::vector<std::uint8_t> seen(side_rows, 0U);
            for (std::size_t i = 0; i < total && i < idx->size(); ++i) {
                const std::size_t row = (*idx)[i];
                if (row == kNull) {
                    continue;  // an unmatched row matched nothing, so it repeats nothing
                }
                if (seen[row] != 0U) {
                    return row;
                }
                seen[row] = 1U;
            }
            return std::nullopt;
        };
        if (expect.right_at_most_one()) {
            if (auto row = first_repeat(left_idx, n_left)) {
                expect_error = "join declared `expect " + format_expect(expect) +
                               "`, but left row " + std::to_string(*row) +
                               " matches more than one row on the right";
                return;
            }
        }
        if (expect.left_at_most_one()) {
            if (auto row = first_repeat(right_idx, n_right)) {
                expect_error = "join declared `expect " + format_expect(expect) +
                               "`, but right row " + std::to_string(*row) +
                               " matches more than one row on the left";
            }
        }
    };

    // ── `take first` / `last` / `any` ─────────────────────────────────────
    // Collapse a left row's matches to one. `first` and `last` are meaningful
    // only against a stated order, so they read the RIGHT value's own ordering
    // claim -- which describes its physical row layout, so "first in that
    // order" is its lowest row index and "last" its highest. A right value
    // carrying no claim is an error rather than a silent pick.
    //
    // `any` needs no claim: the caller has said the choice does not matter.
    // Taking the lowest index makes it stable for the same input rows, so a
    // golden test still works -- an implementation property, not a promise.
    // Either check may have failed by the time a path returns; both are raised
    // through here so no exit forgets one.
    std::string take_error;
    if (take == ir::MatchSelection::First || take == ir::MatchSelection::Last) {
        if (!right.ordering().has_value() || right.ordering()->empty()) {
            take_error =
                "`take " + std::string(take == ir::MatchSelection::First ? "first" : "last") +
                "` needs the right input to state an order, and this one carries none; add "
                "`order` to it, or use `take any` if the choice does not matter";
        }
    }
    const auto keep_lowest_index =
        take == ir::MatchSelection::First || take == ir::MatchSelection::Any;

    struct MutableJoinIndices {
        std::vector<std::size_t>* left;
        std::vector<std::size_t>* right;
        std::vector<std::size_t>* key_right;
    };

    /// Rewrite the emitted pairs so each left row keeps one match. Rows with no
    /// match (an outer join's padding) are left alone: there is nothing to
    /// choose between.
    auto apply_take = [&](MutableJoinIndices indices) {
        auto& left_idx = *indices.left;
        auto& right_idx = *indices.right;
        auto& key_right_idx = *indices.key_right;
        const std::size_t total = left_idx.size();
        std::vector<std::size_t> chosen(n_left, kNull);
        bool any_choice = false;
        for (std::size_t i = 0; i < total; ++i) {
            const std::size_t l = left_idx[i];
            const std::size_t r = right_idx[i];
            if (l == kNull || r == kNull) {
                continue;
            }
            std::size_t& best = chosen[l];
            if (best == kNull) {
                best = r;
                continue;
            }
            any_choice = true;
            best = keep_lowest_index ? std::min(best, r) : std::max(best, r);
        }
        if (!any_choice) {
            return;  // nothing had a second match
        }
        std::vector<std::uint8_t> emitted(n_left, 0U);
        std::vector<std::size_t> out_left;
        std::vector<std::size_t> out_right;
        std::vector<std::size_t> out_key;
        out_left.reserve(total);
        out_right.reserve(total);
        for (std::size_t i = 0; i < total; ++i) {
            const std::size_t l = left_idx[i];
            const std::size_t r = right_idx[i];
            if (l != kNull && r != kNull) {
                if (r != chosen[l] || emitted[l] != 0U) {
                    continue;
                }
                emitted[l] = 1U;
            }
            out_left.push_back(l);
            out_right.push_back(r);
            if (!key_right_idx.empty()) {
                out_key.push_back(key_right_idx[i]);
            }
        }
        left_idx = std::move(out_left);
        right_idx = std::move(out_right);
        key_right_idx = std::move(out_key);
    };

    auto join_failure = [&](Table& out) -> std::expected<Table, std::string> {
        if (!take_error.empty()) {
            return std::unexpected(take_error);
        }
        if (!expect_error.empty()) {
            return std::unexpected(expect_error);
        }
        return std::move(out);
    };

    // Prove the claim from the emitted index array rather than from which path
    // produced it. The paths are many and change for performance reasons; the
    // indices are what actually determine the row order.
    auto claim_if_left_ordered = [&](const std::vector<std::size_t>& left_idx) {
        if (carried_ordering.empty()) {
            return;
        }
        for (std::size_t i = 0; i < left_idx.size(); ++i) {
            // kNull is an unmatched right row, whose left columns are null: the
            // left's ordering says nothing about where such a row belongs.
            if (left_idx[i] == kNull || (i > 0 && left_idx[i] < left_idx[i - 1])) {
                return;
            }
        }
        claim_carried_ordering();
    };

    // Run one batch of gather jobs and land the results on the output. Every
    // assembly path funnels through here, so they all get the single fan-out
    // rather than one per column.
    auto run_gather_jobs = [&](const std::vector<GatherJob>& jobs, std::size_t rows) {
        if (jobs.empty()) {
            return;
        }
        auto gathered = gather_entries(jobs, rows, exec);
        for (std::size_t j = 0; j < jobs.size(); ++j) {
            output.replace_column(jobs[j].out_index, std::move(gathered[j].first),
                                  std::move(gathered[j].second));
        }
    };

    auto materialize_left_identity = [&](const std::vector<std::size_t>& right_idx) {
        for (std::size_t c = 0; c < left.columns.size(); ++c) {
            output.replace_column(c, *left.columns[c].column, left.columns[c].validity);
        }

        const bool right_sentinel = index_has_sentinel(right_idx.data(), right_idx.size());
        std::vector<GatherJob> jobs;
        jobs.reserve(right_out.size());
        for (const auto& item : right_out) {
            jobs.push_back({.entry = item.entry,
                            .idx = right_idx.data(),
                            .out_index = item.out_index,
                            .has_sentinel = right_sentinel});
        }
        run_gather_jobs(jobs, right_idx.size());

        normalize_time_index(output);
        // Every left row, once, in its own order: the strongest form of the
        // claim, and it needs no index array to prove it.
        claim_carried_ordering();
        check_expect(nullptr, &right_idx, right_idx.size());
    };

    // ── Materialize output columns from index arrays ─────────────────────
    // left_idx[i]  = left  row for output row i, or kNull for default.
    // right_idx[i] = right row for output row i, or kNull for default.
    // key_right_idx: for outer/right join unmatched right rows, the right row
    //   to fill key columns from; kNull elsewhere. Empty if not needed.
    auto materialize = [&](const std::vector<std::size_t>& in_left_idx,
                           const std::vector<std::size_t>& in_right_idx,
                           const std::vector<std::size_t>& in_key_right_idx) {
        // `expect` describes how the inputs actually match, so it is checked
        // against the pairs BEFORE `take` drops any of them -- otherwise
        // `take first` would satisfy every `expect n:1` by construction and the
        // declaration would assert nothing.
        check_expect(&in_left_idx, &in_right_idx, in_left_idx.size());

        std::vector<std::size_t> taken_left;
        std::vector<std::size_t> taken_right;
        std::vector<std::size_t> taken_key;
        const std::vector<std::size_t>* left_idx_p = &in_left_idx;
        const std::vector<std::size_t>* right_idx_p = &in_right_idx;
        const std::vector<std::size_t>* key_right_idx_p = &in_key_right_idx;
        if (take != ir::MatchSelection::All && take_error.empty()) {
            taken_left = in_left_idx;
            taken_right = in_right_idx;
            taken_key = in_key_right_idx;
            apply_take(MutableJoinIndices{
                .left = &taken_left, .right = &taken_right, .key_right = &taken_key});
            left_idx_p = &taken_left;
            right_idx_p = &taken_right;
            key_right_idx_p = &taken_key;
        }
        const std::vector<std::size_t>& left_idx = *left_idx_p;
        const std::vector<std::size_t>& right_idx = *right_idx_p;
        const std::vector<std::size_t>& key_right_idx = *key_right_idx_p;

        const std::size_t total = left_idx.size();

        // ── Left columns ──
        // Check whether any left rows are null (outer/right join unmatched).
        bool has_left_nulls = false;
        for (std::size_t i = 0; i < total && !has_left_nulls; ++i) {
            has_left_nulls = (left_idx[i] == kNull);
        }

        if (!has_left_nulls) {
            std::vector<GatherJob> jobs;
            jobs.reserve(left.columns.size());
            for (std::size_t c = 0; c < left.columns.size(); ++c) {
                jobs.push_back({.entry = &left.columns[c],
                                .idx = left_idx.data(),
                                .out_index = c,
                                .has_sentinel = false});
            }
            run_gather_jobs(jobs, total);
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

            // The plan's left columns are its first `left.columns.size()`
            // entries, in input order, so `plan[c]` describes `left.columns[c]`.
            std::vector<GatherJob> plain_jobs;
            plain_jobs.reserve(left.columns.size());
            for (std::size_t c = 0; c < left.columns.size(); ++c) {
                // A folded key is the one output column two inputs feed, so it
                // is the only one an unmatched right row can fill. The planner
                // recorded which right column that is; recovering it from the
                // key list here would restate folding rules it owns.
                const auto peer = plan[c].folded_peer_index;
                if (peer.has_value() && !key_right_idx.empty()) {
                    const ColumnEntry& right_key = right.columns[*peer];
                    // Build per-row: pick from left or right depending on null.
                    // Key columns in outer joins are rare & small, so per-row is fine.
                    ColumnValue out_col = make_empty_like(*left.columns[c].column);
                    std::visit([total](auto& cc) { cc.reserve(total); }, out_col);
                    // A key can be null in a row this join emitted: an
                    // unmatched row keeps its own null key under `nulls never`,
                    // and under `nulls equal` a null key even matches. Copying
                    // the value alone would land the type's default here -- an
                    // empty string, a zero -- so the source row's validity has
                    // to come across with it.
                    std::optional<ValidityBitmap> out_validity;
                    const auto carry_null = [&](std::size_t row) {
                        if (!out_validity.has_value()) {
                            out_validity = ValidityBitmap(total, true);
                        }
                        out_validity->set(row, false);
                    };
                    for (std::size_t i = 0; i < total; ++i) {
                        const bool from_left = left_idx[i] != kNull;
                        const ColumnEntry& source = from_left ? left.columns[c] : right_key;
                        const std::size_t row = from_left ? left_idx[i] : key_right_idx[i];
                        append_value(out_col, *source.column, row);
                        if (source.validity.has_value() && !(*source.validity)[row]) {
                            carry_null(i);
                        }
                    }
                    output.replace_column(c, std::move(out_col), std::move(out_validity));
                } else {
                    // Collected rather than gathered here, so the plain columns
                    // still share one fan-out. A folded key is built per row
                    // just above and is not one of them.
                    plain_jobs.push_back({.entry = &left.columns[c],
                                          .idx = left_idx.data(),
                                          .out_index = c,
                                          .has_sentinel = true});
                }
            }
            run_gather_jobs(plain_jobs, total);
        }

        // ── Right columns ──
        if (right_out.empty()) {
            // semi/anti — no right columns to emit.
        } else {
            // Scanned once for the whole side rather than once per column:
            // every right column reads the same index array, so the answer
            // cannot differ between them. (This was already computed here and
            // then dropped on the floor, while `gather_column_with_nulls`
            // rescanned inside every column's gather.)
            const bool has_right_nulls = index_has_sentinel(right_idx.data(), total);

            std::vector<GatherJob> jobs;
            jobs.reserve(right_out.size());
            for (const auto& item : right_out) {
                jobs.push_back({.entry = item.entry,
                                .idx = right_idx.data(),
                                .out_index = item.out_index,
                                .has_sentinel = has_right_nulls});
            }
            run_gather_jobs(jobs, total);
        }

        normalize_time_index(output);
        claim_if_left_ordered(left_idx);
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
        return join_failure(output);
    }

    // ── Predicate (nested-loop) join ─────────────────────────────────────
    if (predicate != nullptr) {
        if (mask_evaluator == nullptr) {
            return std::unexpected("join predicate evaluation callback is not available");
        }

        // The predicate is evaluated over a batch table holding one left row
        // broadcast against every right row. Right columns go in under an
        // internal name no identifier can spell, so a right column can never
        // capture a reference meant for the left one; the predicate's column
        // references are rewritten to match, once, before the row loop.
        struct NLJRightCol {
            const ColumnValue* column = nullptr;
            std::string batch_name;
        };
        std::vector<NLJRightCol> nlj_right;
        nlj_right.reserve(right.columns.size());
        for (const auto& entry : right.columns) {
            nlj_right.push_back(
                {.column = entry.column.get(), .batch_name = nlj_right_batch_name(entry.name)});
        }

        // Resolved once, above, so its diagnostics precede the output plan's.
        const ir::Expr& batch_predicate = *resolved_predicate;

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
                // The predicate reads the *inputs* (SPEC.md Section 5.6), so
                // the batch carries the left column's own name. Using the
                // output name here was invisible until a suffix clause made
                // the two differ, and then `left(v)` could not find `v`.
                batch.add_column(left.columns[ci].name, std::move(col));
            }
            for (const auto& item : nlj_right) {
                batch.add_column(item.batch_name, *item.column);
            }

            auto mask_res =
                mask_evaluator(batch_predicate, batch, scalars, RowRange::whole(n_right));
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
        return join_failure(output);
    }

    // ── Parallel probe-scan fan-out ──────────────────────────────────────
    // The probe scan is the largest serial block left in this function: on
    // PDS-H (SF-2, 8 cores) it is 120ms of q13's left join, 33ms of q05, 21ms
    // of q09 and 18ms of q20 — the joins that never reach
    // `ChunkedInnerJoinOperator`, which is where W1's parallel probe lives.
    // Those are exactly the shapes the chunked operator excludes: a LEFT join
    // (q13) and multi-key joins (q05/q09/q20).
    //
    // Same contract as `probe_ranges_parallel` there, for the same reason:
    // ranges are contiguous and their parts concatenate in range order, and
    // each range appends in row order, so the result is byte-identical to the
    // serial loop however the workers interleave. The build index is complete
    // and read-only by now — `RowKeyIndex::find` is const and the fast paths
    // probe a `robin_hood` map they no longer write — so no locking is needed.
    const auto probe_workers = [&](std::size_t n) -> std::size_t {
        // Below this the fan-out and the concatenation cost more than the
        // probes they spread; the same threshold the chunked probe uses.
        constexpr std::size_t kMinProbeRows = 1U << 14U;
        if (exec == nullptr || !exec->parallel || !exec->parallel_join_probe ||
            on_worker_pool_thread() || n < kMinProbeRows) {
            return 0;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec->compute_budget();
        const std::size_t workers = std::min({budget, pool.size(), std::size_t{64}});
        return workers < 2 ? 0 : workers;
    };

    // Run `body(begin, end, out_li, out_ri)` over contiguous ranges of [0, n)
    // and append the parts to `li`/`ri` in range order. Returns false when the
    // shape does not qualify and the caller should run `body(0, n, li, ri)`.
    //
    // Per-worker output rather than count-then-fill: counting first would probe
    // the hash table twice per row, and a second cache-missing lookup per probe
    // row costs more than the one concatenating memcpy it would save.
    const auto scan_ranges_parallel = [&](std::size_t n, std::vector<std::size_t>& li,
                                          std::vector<std::size_t>& ri, const auto& body) -> bool {
        const std::size_t workers = probe_workers(n);
        if (workers == 0) {
            return false;
        }
        struct Part {
            std::vector<std::size_t> li;
            std::vector<std::size_t> ri;
        };
        std::vector<Part> parts(workers);
        auto& pool = process_worker_pool();
        const std::size_t grain = (n + workers - 1) / workers;
        {
            auto batch = pool.submit(workers, [&](std::size_t w) {
                const std::size_t begin = w * grain;
                const std::size_t end = std::min(n, begin + grain);
                if (begin >= end) {
                    return;
                }
                // One match per row is the common case; a fan-out join grows
                // past it, which is what a vector is for.
                parts[w].li.reserve(end - begin);
                parts[w].ri.reserve(end - begin);
                body(begin, end, parts[w].li, parts[w].ri);
            });
            batch.wait();
        }
        std::size_t total = 0;
        for (const auto& part : parts) {
            total += part.li.size();
        }
        li.reserve(li.size() + total);
        ri.reserve(ri.size() + total);
        for (auto& part : parts) {
            li.insert(li.end(), part.li.begin(), part.li.end());
            ri.insert(ri.end(), part.ri.begin(), part.ri.end());
        }
        if (exec->parallel_stats != nullptr) {
            exec->parallel_stats->parallel_probes.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    };

    // ── Helper: build index arrays from a right-row → left-matches lookup ─
    // Used by the small-left/large-right paths. Probes the index ONCE per
    // right row, in scan order, and emits matches immediately in that same
    // order — the probe side's natural row order (parquet/CSV scan order,
    // or an upstream join's own probe order). An earlier shape reassembled
    // hits grouped by LEFT row instead, which reads as "preserve left
    // order" but SPEC.md §5.6 puts row order outside the join contract on
    // purpose — there is no language-level guarantee here to preserve. Grouping by left row
    // silently permutes the output away from the probe side's scan order,
    // which then hurts cache locality on any DOWNSTREAM join that probes
    // this join's output (measured ~10-15% on a chained TPC-H-style join
    // sequence). Preserving scan order costs nothing extra: this phase
    // already visits right rows in order once.
    auto build_indices_from_right_scan = [&](auto&& left_matches_for_right_row)
        -> std::tuple<std::vector<std::size_t>, std::vector<std::size_t>,
                      std::vector<std::size_t>> {
        if (semi_join || anti_join) {
            // Membership join: left-only output. No right columns are
            // emitted, so there's no probe-side order to preserve here —
            // keep the simple left-row-order scan.
            std::vector<std::uint8_t> left_matched(n_left, 0U);
            for (std::size_t r = 0; r < n_right; ++r) {
                for (const std::size_t l : left_matches_for_right_row(r)) {
                    left_matched[l] = 1U;
                }
            }
            std::vector<std::size_t> li;
            li.reserve(n_left);
            for (std::size_t l = 0; l < n_left; ++l) {
                const bool has_match = left_matched[l] != 0U;
                if ((semi_join && has_match) || (anti_join && !has_match)) {
                    li.push_back(l);
                }
            }
            std::vector<std::size_t> ri(li.size(), kNull);
            return std::make_tuple(std::move(li), std::move(ri), std::vector<std::size_t>{});
        }

        // Preserving join: probe once per right row, in scan order, and
        // emit matches immediately in that same order.
        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        li.reserve(n_right);
        ri.reserve(n_right);
        std::vector<std::uint8_t> left_matched_flags;
        if (preserve_left_rows) {
            left_matched_flags.assign(n_left, 0U);
        }
        std::vector<std::uint8_t> right_matched_flags;
        if (preserve_right_rows) {
            right_matched_flags.assign(n_right, 0U);
        }

        // One body for both paths, so the parallel and serial results cannot
        // drift: the parallel one runs it per range, the serial one once.
        //
        // `left_matched_flags` is the only cross-range write — a left row can
        // be matched from any right row — so it goes through an atomic store. The
        // store is relaxed and always the same value, which on every target
        // here is the plain byte store the serial path already did; it is
        // present to make the race well defined, not to order anything.
        // `right_matched_flags[r]` needs none of that: `r` is the range's own.
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<std::size_t>& out_l,
                              std::vector<std::size_t>& out_r) {
            for (std::size_t r = begin; r < end; ++r) {
                const std::span<const std::size_t> matches = left_matches_for_right_row(r);
                if (matches.empty()) {
                    continue;
                }
                for (const std::size_t l : matches) {
                    out_l.push_back(l);
                    out_r.push_back(r);
                    if (preserve_left_rows) {
                        set_matched_flag(left_matched_flags[l]);
                    }
                }
                if (preserve_right_rows) {
                    right_matched_flags[r] = 1U;
                }
            }
        };
        if (!scan_ranges_parallel(n_right, li, ri, scan)) {
            scan(0, n_right, li, ri);
        }

        if (preserve_left_rows) {
            for (std::size_t l = 0; l < n_left; ++l) {
                if (left_matched_flags[l] == 0U) {
                    li.push_back(l);
                    ri.push_back(kNull);
                }
            }
        }

        std::vector<std::size_t> kri;
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

        // As in the right scan: one body, run per range in parallel or once
        // serially. Here the cross-range write is `right_matched_flags` —
        // a right row can be matched from any left row.
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<std::size_t>& out_l,
                              std::vector<std::size_t>& out_r) {
            for (std::size_t l = begin; l < end; ++l) {
                const std::span<const std::size_t> matches = right_matches_for_left_row(l);
                const bool has_match = !matches.empty();
                if (semi_join) {
                    if (has_match) {
                        out_l.push_back(l);
                        out_r.push_back(kNull);
                    }
                    continue;
                }
                if (anti_join) {
                    if (!has_match) {
                        out_l.push_back(l);
                        out_r.push_back(kNull);
                    }
                    continue;
                }
                if (!has_match) {
                    if (preserve_left_rows) {
                        out_l.push_back(l);
                        out_r.push_back(kNull);
                    }
                    continue;
                }
                for (const std::size_t r : matches) {
                    out_l.push_back(l);
                    out_r.push_back(r);
                    if (preserve_right_rows) {
                        set_matched_flag(right_matched_flags[r]);
                    }
                }
            }
        };
        if (!scan_ranges_parallel(n_left, li, ri, scan)) {
            scan(0, n_left, li, ri);
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
        // Which side is indexed was decided once, above.
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
            return join_failure(output);
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
                return join_failure(output);
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
                return join_failure(output);
            }
            auto lookup = [&](std::size_t l) -> std::span<const std::size_t> {
                auto it = key_gid.find(ls[l]);
                return it == key_gid.end() ? std::span<const std::size_t>{}
                                           : grouped.matches(it->second);
            };
            auto [li, ri, kri] = build_indices_from_left_scan(lookup);
            materialize(li, ri, kri);
        }
        return join_failure(output);
    }

    // ── Single int64 key fast path ───────────────────────────────────────
    if (kind != ir::JoinKind::Asof && !has_null_keys && keys.size() == 1 &&
        std::holds_alternative<Column<std::int64_t>>(*left_keys[0])) {
        const auto& left_ints = std::get<Column<std::int64_t>>(*left_keys[0]);
        const auto& right_ints = std::get<Column<std::int64_t>>(*right_keys[0]);

        // Which side is indexed was decided once, above.
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
            return join_failure(output);
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
            return join_failure(output);
        }

        auto lookup = [&](std::size_t l) -> std::span<const std::size_t> {
            auto it = key_gid.find(probe_data[l]);
            return it == key_gid.end() ? std::span<const std::size_t>{}
                                       : grouped.matches(it->second);
        };
        auto [li, ri, kri] = build_indices_from_left_scan(lookup);
        materialize(li, ri, kri);
        return join_failure(output);
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

        auto wrong_type_error = [&](const char* label, const ColumnValue& col) {
            const std::string& time_key =
                std::string_view{label} == "left" ? keys[time_pos].left : keys[time_pos].right;
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
            const std::string sort_key = !left_sorted ? keys[time_pos].left : keys[time_pos].right;
            return std::unexpected(
                std::string("asof join: ") + which + " not sorted ascending by time index '" +
                sort_key +
                "' — silent look-ahead bias would be the result if this were allowed"
                "\n  hint: order the offending side first, e.g. `<table>[order " +
                sort_key + "]` before promoting with as_timeframe()");
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
            return has_null_eq_keys && skip_null_keys && row_key_is_null(left_eq_validity, l);
        };
        const auto right_eq_is_null = [&](std::size_t r) {
            return has_null_eq_keys && skip_null_keys && row_key_is_null(right_eq_validity, r);
        };
        // `Key` carries one null bit per column, so a null equality key groups
        // with the nulls rather than with the zero its cell physically holds.
        // Past 64 columns the bit is dropped and nulls would silently rejoin the
        // zero group, so that case is refused instead of answered wrongly.
        if (!skip_null_keys && has_null_eq_keys && left_eq_keys.size() > kMaxKeyColumns) {
            return std::unexpected("asof join: `nulls equal` supports at most " +
                                   std::to_string(kMaxKeyColumns) + " equality keys");
        }
        const auto mark_nulls = [&](Key& key, const std::vector<const ValidityBitmap*>& validity,
                                    std::size_t row) {
            if (skip_null_keys) {
                return;
            }
            for (std::size_t i = 0; i < validity.size(); ++i) {
                if (validity[i] != nullptr && !(*validity[i])[row]) {
                    key.set_null(i);
                }
            }
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
                mark_nulls(group, right_eq_validity, r);
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
                mark_nulls(group, left_eq_validity, l);

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
        // One output row per left row, in left order: a `Preserve` with respect
        // to the left input. So the left's ordering and group-major claim carry
        // over too, not just its time index -- each surviving only if the column
        // naming it is still present after the join's column merge.
        apply_table_properties(output, TableProperties::derive(
                                           table_properties_of(left),
                                           [&](const std::string& name) -> KeyFate {
                                               return output.index.contains(name)
                                                          ? KeyFate::kept(name)
                                                          : KeyFate::dropped();
                                           },
                                           RowTransform::Preserve));
        return join_failure(output);
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
        // The KeyCols carry validity because under `Equal` a null key is hashed
        // and compared like any other, and a null cell holds its type's zero --
        // so without it a null would match a genuine zero. Under `Never` no
        // null-keyed row reaches the comparison and the pointers go unread.
        auto lc = make_key_col(*left_keys[i], left_key_validity[i]);
        auto rc = make_key_col(*right_keys[i], right_key_validity[i]);
        if (!lc.has_value() || !rc.has_value()) {
            return std::unexpected("join: unsupported key column type for " + format_key(keys[i]));
        }
        left_cols.push_back(*lc);
        right_cols.push_back(*rc);
    }

    const auto& build_cols = build_left ? left_cols : right_cols;
    const auto& probe_cols = build_left ? right_cols : left_cols;
    const auto& build_key_validity = build_left ? left_key_validity : right_key_validity;
    const auto& probe_key_validity = build_left ? right_key_validity : left_key_validity;
    const std::size_t n_build = build_left ? n_left : n_right;

    RowKeyIndex index;
    index.reserve(n_build);
    std::vector<std::uint32_t> row_gid(n_build, kNoGroup);
    // `key_rows_equal` and `hash_key_row` tag a null by position rather than
    // substituting a sentinel, which could collide with real data.
    for (std::size_t i = 0; i < n_build; ++i) {
        if (has_null_keys && skip_null_keys && row_key_is_null(build_key_validity, i)) {
            continue;  // never matchable, so never indexed
        }
        row_gid[i] = index.find_or_insert(build_cols, i);
    }
    const GroupedRows grouped = group_rows_csr(row_gid, index.size());

    auto lookup = [&](std::size_t row) -> std::span<const std::size_t> {
        if (has_null_keys && skip_null_keys && row_key_is_null(probe_key_validity, row)) {
            return {};  // matches nothing, including another null
        }
        const std::uint32_t gid = index.find(probe_cols, row, build_cols);
        return gid == kNoGroup ? std::span<const std::size_t>{} : grouped.matches(gid);
    };

    if (build_left) {
        auto [li, ri, kri] = build_indices_from_right_scan(lookup);
        materialize(li, ri, kri);
        return join_failure(output);
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
        return join_failure(output);
    }

    auto [li, ri, kri] = build_indices_from_left_scan(lookup);
    materialize(li, ri, kri);
    return join_failure(output);
}

namespace {

/// The column a field counts, when the field is exactly what `count(col)`
/// lowers to: `alias = Int64(col is not null)` (see `make_count_flag_field` in
/// the lowerer). Matched structurally rather than by name, because the alias is
/// generated and the shape is the actual contract.
auto count_flag_source_column(const ir::Expr& expr) -> std::optional<std::string> {
    const auto* cast = std::get_if<ir::CallExpr>(&expr.node);
    if (cast == nullptr || cast->callee != "Int64" || cast->args.size() != 1 ||
        !cast->named_args.empty() || cast->args.front() == nullptr) {
        return std::nullopt;
    }
    const auto* is_null = std::get_if<ir::IsNullExpr>(&cast->args.front()->node);
    if (is_null == nullptr || !is_null->negated || is_null->operand == nullptr) {
        return std::nullopt;
    }
    const auto* column = std::get_if<ir::ColumnRef>(&is_null->operand->node);
    return column != nullptr ? std::optional{column->name} : std::nullopt;
}

}  // namespace

auto fused_left_join_counted_column(const ir::AggregateNode& aggregate,
                                    std::span<const ir::UpdateNode* const> skipped_updates)
    -> std::optional<std::string> {
    if (aggregate.aggregations().size() != 1 || aggregate.group_by().size() != 1) {
        return std::nullopt;
    }
    std::string counted = aggregate.aggregations().front().column.name;
    const std::string& group_key = aggregate.group_by().front().name;
    if (counted.empty()) {
        return std::nullopt;
    }
    // Aggregate-first: each update's aliases name columns produced below it, so
    // resolving in this order follows the same direction the data flows back up.
    for (const ir::UpdateNode* update : skipped_updates) {
        for (const ir::FieldSpec& field : update->fields()) {
            if (field.alias == group_key) {
                // The group key would be computed here, not read from the join.
                return std::nullopt;
            }
            if (field.alias != counted) {
                continue;
            }
            auto source = count_flag_source_column(field.expr);
            if (!source.has_value()) {
                return std::nullopt;
            }
            counted = std::move(*source);
        }
    }
    return counted;
}

auto left_join_count_table(const ir::JoinNode& join, const ir::AggregateNode& aggregate,
                           const Table& left, const Table& right, std::string_view counted_column)
    -> std::optional<Table> {
    if (join.kind() != ir::JoinKind::Left || join.keys().size() != 1 ||
        join.predicate().has_value() || join.null_match() != ir::NullMatch::Never ||
        join.take() != ir::MatchSelection::All || join.expect().asserts_anything() ||
        !join.pending_order().empty() || aggregate.group_by().size() != 1 ||
        aggregate.aggregations().size() != 1 ||
        (aggregate.aggregations().front().func != ir::AggFunc::Count &&
         !(aggregate.aggregations().front().func == ir::AggFunc::Sum &&
           counted_column != aggregate.aggregations().front().column.name)) ||
        aggregate.aggregations().front().column.name.empty() ||
        aggregate.group_by().front().name != join.keys().front().left) {
        return std::nullopt;
    }
    const auto& key = join.keys().front();
    const auto& spec = aggregate.aggregations().front();
    const auto* left_key = left.find_entry(key.left);
    const auto* right_key = right.find_entry(key.right);
    const auto* counted = right.find_entry(std::string(counted_column));
    if (left_key == nullptr || right_key == nullptr || counted == nullptr ||
        !std::holds_alternative<Column<std::int64_t>>(*left_key->column) ||
        !std::holds_alternative<Column<std::int64_t>>(*right_key->column) ||
        left_key->validity.has_value() || right_key->validity.has_value()) {
        return std::nullopt;
    }
    const auto& left_values = std::get<Column<std::int64_t>>(*left_key->column);
    const auto& right_values = std::get<Column<std::int64_t>>(*right_key->column);
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> counts;
    counts.reserve(right.rows());
    const auto* counted_validity = counted->validity.has_value() ? &*counted->validity : nullptr;
    for (std::size_t row = 0; row < right.rows(); ++row) {
        if (counted_validity == nullptr || (*counted_validity)[row]) {
            ++counts[right_values[row]];
        }
    }
    robin_hood::unordered_flat_set<std::int64_t> seen;
    seen.reserve(left.rows());
    for (std::size_t row = 0; row < left.rows(); ++row) {
        if (!seen.emplace(left_values[row]).second) {
            return std::nullopt;
        }
    }
    Column<std::int64_t> out_counts;
    out_counts.resize_for_overwrite(left.rows());
    for (std::size_t row = 0; row < left.rows(); ++row) {
        const auto it = counts.find(left_values[row]);
        out_counts[row] = it == counts.end() ? 0 : static_cast<std::int64_t>(it->second);
    }
    Table out;
    out.add_column(aggregate.group_by().front().name, *left_key->column);
    out.add_column(spec.alias, std::move(out_counts));
    return out;
}

}  // namespace ibex::runtime
