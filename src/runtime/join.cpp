#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "join_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

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
            std::size_t h = std::visit(
                [](const auto& v) { return std::hash<std::decay_t<decltype(v)>>{}(v); }, value);
            hash_combine(h);
        }
        return seed;
    }
};

struct KeyEq {
    auto operator()(const Key& a, const Key& b) const -> bool { return a.values == b.values; }
};

// Sentinel: SIZE_MAX means "emit a default/null value for this position".
constexpr std::size_t kNull = SIZE_MAX;

}  // namespace

auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                     const std::vector<std::string>& keys, const ir::FilterExpr* predicate,
                     const ScalarRegistry* scalars, PredicateMaskEvaluator mask_evaluator)
    -> std::expected<Table, std::string> {
    if (predicate == nullptr && kind != ir::JoinKind::Cross && keys.empty()) {
        return std::unexpected("join requires at least one key");
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

    std::optional<std::size_t> asof_time_key_pos;
    if (kind == ir::JoinKind::Asof) {
        if (!left.time_index.has_value() || !right.time_index.has_value()) {
            return std::unexpected("asof join requires both operands to be TimeFrame");
        }
        if (*left.time_index != *right.time_index) {
            return std::unexpected(
                "asof join requires both TimeFrames to share the same time index column");
        }
        const std::string& time_key = *left.time_index;
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (keys[i] == time_key) {
                asof_time_key_pos = i;
                break;
            }
        }
        if (!asof_time_key_pos.has_value()) {
            return std::unexpected("asof join requires the 'on' keys to include the time index '" +
                                   time_key + "'");
        }
    }

    std::unordered_set<std::string> key_set(keys.begin(), keys.end());

    const std::size_t n_left = left.rows();
    const std::size_t n_right = right.rows();

    const bool preserve_left_rows =
        (kind == ir::JoinKind::Left || kind == ir::JoinKind::Outer || kind == ir::JoinKind::Asof);
    const bool preserve_right_rows = (kind == ir::JoinKind::Right || kind == ir::JoinKind::Outer);
    const bool semi_join = (kind == ir::JoinKind::Semi);
    const bool anti_join = (kind == ir::JoinKind::Anti);

    Table output;
    output.columns.reserve(left.columns.size() + right.columns.size());

    std::unordered_set<std::string> out_names;
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
        right_out.push_back(RightOut{entry.column.get(), output.columns.size() - 1});
    }

    const bool preserve_left_only =
        preserve_left_rows && !preserve_right_rows && kind != ir::JoinKind::Asof;

    auto materialize_left_identity = [&](const std::vector<std::size_t>& right_idx) {
        for (std::size_t c = 0; c < left.columns.size(); ++c) {
            *output.columns[c].column = *left.columns[c].column;
            output.columns[c].validity = left.columns[c].validity;
        }

        if (!right_out.empty()) {
            bool has_right_nulls = false;
            for (std::size_t i = 0; i < right_idx.size() && !has_right_nulls; ++i) {
                has_right_nulls = (right_idx[i] == kNull);
            }

            if (!has_right_nulls) {
                for (const auto& item : right_out) {
                    *output.columns[item.out_index].column =
                        gather_column(*item.column, right_idx.data(), right_idx.size());
                }
            } else {
                for (const auto& item : right_out) {
                    auto [col_out, validity] = gather_column_with_nulls(
                        *item.column, right_idx.data(), right_idx.size(), kNull);
                    *output.columns[item.out_index].column = std::move(col_out);
                    output.columns[item.out_index].validity = std::move(validity);
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
                *output.columns[c].column =
                    gather_column(*left.columns[c].column, left_idx.data(), total);
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
                bool is_key = key_set.contains(left.columns[c].name);
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
                    *output.columns[c].column = std::move(out_col);
                } else {
                    auto [col_out, validity] = gather_column_with_nulls(
                        *left.columns[c].column, left_idx.data(), total, kNull);
                    *output.columns[c].column = std::move(col_out);
                    output.columns[c].validity = std::move(validity);
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
                    *output.columns[item.out_index].column =
                        gather_column(*item.column, right_idx.data(), total);
                }
            } else {
                for (const auto& item : right_out) {
                    auto [col_out, validity] =
                        gather_column_with_nulls(*item.column, right_idx.data(), total, kNull);
                    *output.columns[item.out_index].column = std::move(col_out);
                    output.columns[item.out_index].validity = std::move(validity);
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
            std::unordered_set<std::string> batch_left_names;
            for (const auto& entry : left.columns) {
                batch_left_names.insert(entry.name);
            }
            for (const auto& entry : right.columns) {
                std::string name = entry.name;
                while (batch_left_names.contains(name)) {
                    name += "_right";
                }
                batch_left_names.insert(name);
                nlj_right.push_back({entry.column.get(), std::move(name)});
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

    // ── Helper: build index arrays from right-scan match iterator ────────
    // Used by the small-left/large-right paths.
    auto build_indices_from_right_scan = [&](auto&& for_each_left_match_for_right_row)
        -> std::tuple<std::vector<std::size_t>, std::vector<std::size_t>,
                      std::vector<std::size_t>> {
        // Phase 1: count matches per left row.
        std::vector<std::size_t> match_counts(n_left, 0);
        std::size_t total_matches = 0;
        std::vector<std::uint8_t> right_matched_flags;
        if (preserve_right_rows) {
            right_matched_flags.assign(n_right, 0U);
        }

        for (std::size_t r = 0; r < n_right; ++r) {
            bool any = false;
            for_each_left_match_for_right_row(r, [&](std::size_t l) {
                ++match_counts[l];
                ++total_matches;
                any = true;
            });
            if (preserve_right_rows && any) {
                right_matched_flags[r] = 1U;
            }
        }

        // Phase 2: build right-match array indexed by left offsets.
        std::vector<std::size_t> match_offsets(n_left + 1, 0);
        for (std::size_t l = 0; l < n_left; ++l) {
            match_offsets[l + 1] = match_offsets[l] + match_counts[l];
        }
        std::vector<std::size_t> right_matches(total_matches);
        std::vector<std::size_t> next_off = match_offsets;
        for (std::size_t r = 0; r < n_right; ++r) {
            for_each_left_match_for_right_row(
                r, [&](std::size_t l) { right_matches[next_off[l]++] = r; });
        }

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
                bool has_match = (match_counts[l] > 0);
                if ((semi_join && has_match) || (anti_join && !has_match)) {
                    li.push_back(l);
                }
            }
            ri.assign(li.size(), kNull);
            return {std::move(li), std::move(ri), {}};
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

        return {std::move(li), std::move(ri), std::move(kri)};
    };

    // ── Helper: build index arrays from left-scan match ──────────────────
    // Used by the large-left/small-right paths.
    auto build_indices_from_left_scan = [&](auto&& lookup_right_matches)
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
            const auto* matches = lookup_right_matches(l);
            const bool has_match = (matches != nullptr && !matches->empty());
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
            for (auto r : *matches) {
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

        return {std::move(li), std::move(ri), std::move(kri)};
    };

    // ── Single string/categorical key fast path ──────────────────────────
    if (kind != ir::JoinKind::Asof && keys.size() == 1 &&
        (std::holds_alternative<Column<std::string>>(*left_keys[0]) ||
         std::holds_alternative<Column<Categorical>>(*left_keys[0]))) {
        if (n_left < n_right) {
            std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                               std::equal_to<>>
                left_sv_index;
            left_sv_index.reserve(n_left);
            if (const auto* ls = std::get_if<Column<std::string>>(left_keys[0])) {
                for (std::size_t l = 0; l < n_left; ++l) {
                    left_sv_index[(*ls)[l]].push_back(l);
                }
            } else if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
                for (std::size_t l = 0; l < n_left; ++l) {
                    left_sv_index[(*lc)[l]].push_back(l);
                }
            }

            auto for_each = [&](std::size_t r, auto&& emit_left_row) {
                std::string_view sv;
                if (const auto* rs = std::get_if<Column<std::string>>(right_keys[0])) {
                    sv = (*rs)[r];
                } else {
                    sv = std::get<Column<Categorical>>(*right_keys[0])[r];
                }
                auto it = left_sv_index.find(sv);
                if (it == left_sv_index.end()) {
                    return;
                }
                for (auto l : it->second) {
                    emit_left_row(l);
                }
            };

            auto [li, ri, kri] = build_indices_from_right_scan(for_each);
            materialize(li, ri, kri);
            return output;
        }

        std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                           std::equal_to<>>
            right_sv_index;
        right_sv_index.reserve(n_right);
        if (const auto* rs = std::get_if<Column<std::string>>(right_keys[0])) {
            for (std::size_t r = 0; r < n_right; ++r) {
                right_sv_index[(*rs)[r]].push_back(r);
            }
        } else if (const auto* rc = std::get_if<Column<Categorical>>(right_keys[0])) {
            for (std::size_t r = 0; r < n_right; ++r) {
                right_sv_index[(*rc)[r]].push_back(r);
            }
        }

        if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
            const auto& dict = *lc->dictionary_ptr();
            std::vector<const std::vector<std::size_t>*> code_to_right(dict.size(), nullptr);
            bool unique_right = preserve_left_only;
            for (std::size_t code = 0; code < dict.size(); ++code) {
                auto it = right_sv_index.find(std::string_view{dict[code]});
                if (it != right_sv_index.end()) {
                    code_to_right[code] = &it->second;
                    if (unique_right && it->second.size() != 1) {
                        unique_right = false;
                    }
                }
            }
            const auto* codes = lc->codes_data();
            if (unique_right) {
                std::vector<std::size_t> ri(n_left, kNull);
                for (std::size_t l = 0; l < n_left; ++l) {
                    const auto* matches = code_to_right[static_cast<std::size_t>(codes[l])];
                    if (matches != nullptr) {
                        ri[l] = (*matches)[0];
                    }
                }
                materialize_left_identity(ri);
                return output;
            }
            auto lookup = [&](std::size_t l) -> const std::vector<std::size_t>* {
                return code_to_right[static_cast<std::size_t>(codes[l])];
            };
            auto [li, ri, kri] = build_indices_from_left_scan(lookup);
            materialize(li, ri, kri);
        } else {
            const auto& ls = std::get<Column<std::string>>(*left_keys[0]);
            if (preserve_left_only) {
                bool unique_right = true;
                for (const auto& [key, rows] : right_sv_index) {
                    (void)key;
                    if (rows.size() != 1) {
                        unique_right = false;
                        break;
                    }
                }
                if (unique_right) {
                    std::vector<std::size_t> ri(n_left, kNull);
                    for (std::size_t l = 0; l < n_left; ++l) {
                        auto it = right_sv_index.find(ls[l]);
                        if (it != right_sv_index.end()) {
                            ri[l] = it->second[0];
                        }
                    }
                    materialize_left_identity(ri);
                    return output;
                }
            }
            auto lookup = [&](std::size_t l) -> const std::vector<std::size_t>* {
                auto it = right_sv_index.find(ls[l]);
                return it != right_sv_index.end() ? &it->second : nullptr;
            };
            auto [li, ri, kri] = build_indices_from_left_scan(lookup);
            materialize(li, ri, kri);
        }
        return output;
    }

    // ── Single int64 key fast path ───────────────────────────────────────
    if (kind != ir::JoinKind::Asof && keys.size() == 1 &&
        std::holds_alternative<Column<std::int64_t>>(*left_keys[0])) {
        const auto& left_ints = std::get<Column<std::int64_t>>(*left_keys[0]);
        const auto& right_ints = std::get<Column<std::int64_t>>(*right_keys[0]);

        if (n_left < n_right) {
            std::unordered_map<std::int64_t, std::vector<std::size_t>> left_int_index;
            left_int_index.reserve(n_left);
            for (std::size_t l = 0; l < n_left; ++l) {
                left_int_index[left_ints[l]].push_back(l);
            }

            auto for_each = [&](std::size_t r, auto&& emit_left_row) {
                auto it = left_int_index.find(right_ints[r]);
                if (it == left_int_index.end()) {
                    return;
                }
                for (auto l : it->second) {
                    emit_left_row(l);
                }
            };

            auto [li, ri, kri] = build_indices_from_right_scan(for_each);
            materialize(li, ri, kri);
            return output;
        }

        std::unordered_map<std::int64_t, std::vector<std::size_t>> right_int_index;
        right_int_index.reserve(n_right);
        bool unique_right = preserve_left_only;
        for (std::size_t r = 0; r < n_right; ++r) {
            auto& rows = right_int_index[right_ints[r]];
            rows.push_back(r);
            if (unique_right && rows.size() != 1) {
                unique_right = false;
            }
        }

        if (unique_right) {
            std::vector<std::size_t> ri(n_left, kNull);
            for (std::size_t l = 0; l < n_left; ++l) {
                auto it = right_int_index.find(left_ints[l]);
                if (it != right_int_index.end()) {
                    ri[l] = it->second[0];
                }
            }
            materialize_left_identity(ri);
            return output;
        }

        auto lookup = [&](std::size_t l) -> const std::vector<std::size_t>* {
            auto it = right_int_index.find(left_ints[l]);
            return it != right_int_index.end() ? &it->second : nullptr;
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

        std::vector<std::int64_t> left_times;
        left_times.reserve(n_left);
        for (std::size_t l = 0; l < n_left; ++l) {
            auto v = time_value(*left_time_col, l);
            if (!v.has_value()) {
                return std::unexpected(
                    "asof join requires a Timestamp/Date/Int time index column in left operand");
            }
            left_times.push_back(*v);
        }

        std::vector<std::int64_t> right_times;
        right_times.reserve(n_right);
        for (std::size_t r = 0; r < n_right; ++r) {
            auto v = time_value(*right_time_col, r);
            if (!v.has_value()) {
                return std::unexpected(
                    "asof join requires a Timestamp/Date/Int time index column in right operand");
            }
            right_times.push_back(*v);
        }

        if (!std::is_sorted(left_times.begin(), left_times.end()) ||
            !std::is_sorted(right_times.begin(), right_times.end())) {
            return std::unexpected(
                "asof join requires both TimeFrames to be sorted ascending by their time index");
        }

        std::vector<const ColumnValue*> left_eq_keys;
        std::vector<const ColumnValue*> right_eq_keys;
        left_eq_keys.reserve(keys.size() - 1);
        right_eq_keys.reserve(keys.size() - 1);
        for (std::size_t i = 0; i < keys.size(); ++i) {
            if (i == time_pos) {
                continue;
            }
            left_eq_keys.push_back(left_keys[i]);
            right_eq_keys.push_back(right_keys[i]);
        }

        std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_groups;
        right_groups.reserve(n_right);
        for (std::size_t r = 0; r < n_right; ++r) {
            Key group;
            group.values.reserve(right_eq_keys.size());
            for (const auto* col : right_eq_keys) {
                group.values.push_back(scalar_from_column(*col, r));
            }
            right_groups[group].push_back(r);
        }

        std::unordered_map<Key, std::size_t, KeyHash, KeyEq> right_pos;
        right_pos.reserve(right_groups.size());

        std::vector<std::size_t> left_idx(n_left);
        std::vector<std::size_t> right_idx(n_left);
        for (std::size_t l = 0; l < n_left; ++l) {
            left_idx[l] = l;

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

        static const std::vector<std::size_t> empty_key_idx;
        materialize(left_idx, right_idx, empty_key_idx);
        output.time_index = left.time_index;
        return output;
    }

    // ── Generic multi-key fallback ───────────────────────────────────────
    std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> right_index;
    if (n_left < n_right) {
        std::unordered_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> left_index;
        left_index.reserve(n_left);
        for (std::size_t l = 0; l < n_left; ++l) {
            Key key;
            key.values.reserve(keys.size());
            for (const auto* col : left_keys) {
                key.values.push_back(scalar_from_column(*col, l));
            }
            left_index[key].push_back(l);
        }

        auto for_each = [&](std::size_t r, auto&& emit_left_row) {
            Key key;
            key.values.reserve(keys.size());
            for (const auto* col : right_keys) {
                key.values.push_back(scalar_from_column(*col, r));
            }
            auto it = left_index.find(key);
            if (it == left_index.end()) {
                return;
            }
            for (auto l : it->second) {
                emit_left_row(l);
            }
        };

        auto [li, ri, kri] = build_indices_from_right_scan(for_each);
        materialize(li, ri, kri);
        return output;
    }

    right_index.reserve(n_right);
    bool unique_right = preserve_left_only;
    for (std::size_t r = 0; r < n_right; ++r) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : right_keys) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        auto& rows = right_index[key];
        rows.push_back(r);
        if (unique_right && rows.size() != 1) {
            unique_right = false;
        }
    }

    if (unique_right) {
        std::vector<std::size_t> ri(n_left, kNull);
        for (std::size_t l = 0; l < n_left; ++l) {
            Key key;
            key.values.reserve(keys.size());
            for (const auto* col : left_keys) {
                key.values.push_back(scalar_from_column(*col, l));
            }
            auto it = right_index.find(key);
            if (it != right_index.end()) {
                ri[l] = it->second[0];
            }
        }
        materialize_left_identity(ri);
        return output;
    }

    auto lookup = [&](std::size_t l) -> const std::vector<std::size_t>* {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : left_keys) {
            key.values.push_back(scalar_from_column(*col, l));
        }
        auto it = right_index.find(key);
        return it != right_index.end() ? &it->second : nullptr;
    };
    auto [li, ri, kri] = build_indices_from_left_scan(lookup);
    materialize(li, ri, kri);
    return output;
}

}  // namespace ibex::runtime
