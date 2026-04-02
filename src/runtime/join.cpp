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

    if (kind == ir::JoinKind::Cross) {
        for (std::size_t l = 0; l < n_left; ++l) {
            for (std::size_t r = 0; r < n_right; ++r) {
                for (std::size_t i = 0; i < left.columns.size(); ++i) {
                    append_value(*output.columns[i].column, *left.columns[i].column, l);
                }
                for (const auto& item : right_out) {
                    append_value(*output.columns[item.out_index].column, *item.column, r);
                }
            }
        }
        normalize_time_index(output);
        return output;
    }

    const bool track_left_nulls = preserve_right_rows;
    bool left_had_nulls = false;
    std::vector<ValidityBitmap> left_validity;
    if (track_left_nulls) {
        left_validity.resize(left.columns.size());
    }

    const bool track_right_nulls = preserve_left_rows;
    bool right_had_nulls = false;
    std::vector<ValidityBitmap> right_validity;
    if (track_right_nulls) {
        right_validity.resize(right_out.size());
    }

    struct JoinEmitter {
        const Table& left;
        const Table& right;
        Table& output;
        const std::unordered_set<std::string>& key_set;
        const std::vector<RightOut>& right_out;
        bool track_left_nulls;
        bool& left_had_nulls;
        std::vector<ValidityBitmap>& left_validity;
        bool track_right_nulls;
        bool& right_had_nulls;
        std::vector<ValidityBitmap>& right_validity;

        auto append_left_row(std::size_t row) -> void {
            for (std::size_t i = 0; i < left.columns.size(); ++i) {
                append_value(*output.columns[i].column, *left.columns[i].column, row);
            }
            if (track_left_nulls) {
                for (auto& bm : left_validity) {
                    bm.push_back(true);
                }
            }
        }

        auto append_right_row(std::size_t row) -> void {
            for (const auto& item : right_out) {
                append_value(*output.columns[item.out_index].column, *item.column, row);
            }
            if (track_right_nulls) {
                for (auto& bm : right_validity) {
                    bm.push_back(true);
                }
            }
        }

        auto append_right_defaults() -> void {
            for (const auto& item : right_out) {
                std::visit(
                    [](auto& col) {
                        using ColType = std::decay_t<decltype(col)>;
                        if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                            col.push_back(std::int64_t{0});
                        } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                            col.push_back(0.0);
                        } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                            col.push_back(std::string_view{});
                        } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                            col.push_code(0);
                        } else {
                            col.push_back({});
                        }
                    },
                    *output.columns[item.out_index].column);
            }
            if (track_right_nulls) {
                right_had_nulls = true;
                for (auto& bm : right_validity) {
                    bm.push_back(false);
                }
            }
        }

        auto append_left_defaults_from_right(std::size_t right_row) -> void {
            for (std::size_t i = 0; i < left.columns.size(); ++i) {
                const auto& left_entry = left.columns[i];
                if (key_set.contains(left_entry.name)) {
                    const auto* right_key_col = right.find(left_entry.name);
                    if (right_key_col == nullptr) {
                        throw std::runtime_error(
                            "join key not found in right during row emission: " + left_entry.name);
                    }
                    append_value(*output.columns[i].column, *right_key_col, right_row);
                    if (track_left_nulls) {
                        left_validity[i].push_back(true);
                    }
                    continue;
                }

                std::visit(
                    [](auto& col) {
                        using ColType = std::decay_t<decltype(col)>;
                        if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                            col.push_back(std::int64_t{0});
                        } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                            col.push_back(0.0);
                        } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                            col.push_back(std::string_view{});
                        } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                            col.push_code(0);
                        } else {
                            col.push_back({});
                        }
                    },
                    *output.columns[i].column);
                if (track_left_nulls) {
                    left_validity[i].push_back(false);
                }
            }
            if (track_left_nulls) {
                left_had_nulls = true;
            }
        }

        auto finalize_left_validity() -> void {
            if (left_had_nulls) {
                for (std::size_t i = 0; i < left.columns.size(); ++i) {
                    output.columns[i].validity = std::move(left_validity[i]);
                }
            }
        }

        auto finalize_right_validity() -> void {
            if (right_had_nulls) {
                for (std::size_t i = 0; i < right_out.size(); ++i) {
                    output.columns[right_out[i].out_index].validity = std::move(right_validity[i]);
                }
            }
        }
    };

    JoinEmitter emitter{left,
                        right,
                        output,
                        key_set,
                        right_out,
                        track_left_nulls,
                        left_had_nulls,
                        left_validity,
                        track_right_nulls,
                        right_had_nulls,
                        right_validity};

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
                    emitter.append_left_row(l);
                    break;
                }
                if (anti_join) {
                    break;
                }
                emitter.append_left_row(l);
                emitter.append_right_row(j);
                if (preserve_right_rows) {
                    right_matched_pred[j] = 1U;
                }
            }

            if (!left_had_match) {
                if (anti_join) {
                    emitter.append_left_row(l);
                } else if (preserve_left_rows) {
                    emitter.append_left_row(l);
                    emitter.append_right_defaults();
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched_pred[r] == 0U) {
                    emitter.append_left_defaults_from_right(r);
                    emitter.append_right_row(r);
                }
            }
        }

        emitter.finalize_left_validity();
        emitter.finalize_right_validity();
        normalize_time_index(output);
        return output;
    }

    auto emit_preserving_rows_from_right_scan = [&](auto&& for_each_left_match_for_right_row) {
        std::vector<std::size_t> match_counts(n_left, 0);
        std::size_t total_matches = 0;
        std::vector<std::uint8_t> right_matched_build_left;
        if (preserve_right_rows) {
            right_matched_build_left.assign(n_right, 0U);
        }

        for (std::size_t r = 0; r < n_right; ++r) {
            bool right_has_match = false;
            for_each_left_match_for_right_row(r, [&](std::size_t l) {
                ++match_counts[l];
                ++total_matches;
                right_has_match = true;
            });
            if (preserve_right_rows && right_has_match) {
                right_matched_build_left[r] = 1U;
            }
        }

        std::vector<std::size_t> match_offsets(n_left + 1, 0);
        std::size_t left_phase_rows = 0;
        for (std::size_t l = 0; l < n_left; ++l) {
            match_offsets[l + 1] = match_offsets[l] + match_counts[l];
            left_phase_rows +=
                match_counts[l] == 0 ? (preserve_left_rows ? 1U : 0U) : match_counts[l];
        }

        std::vector<std::size_t> right_matches(total_matches);
        std::vector<std::size_t> next_offsets = match_offsets;
        for (std::size_t r = 0; r < n_right; ++r) {
            for_each_left_match_for_right_row(
                r, [&](std::size_t l) { right_matches[next_offsets[l]++] = r; });
        }

        std::size_t unmatched_right_rows = 0;
        if (preserve_right_rows) {
            unmatched_right_rows = static_cast<std::size_t>(std::count(
                right_matched_build_left.begin(), right_matched_build_left.end(), std::uint8_t{0}));
        }
        const std::size_t total_output_rows = left_phase_rows + unmatched_right_rows;
        for (auto& ce : output.columns) {
            std::visit([total_output_rows](auto& c) { c.reserve(total_output_rows); }, *ce.column);
        }

        for (std::size_t l = 0; l < n_left; ++l) {
            if (match_counts[l] == 0) {
                if (preserve_left_rows) {
                    emitter.append_left_row(l);
                    emitter.append_right_defaults();
                }
                continue;
            }
            for (std::size_t idx = match_offsets[l]; idx < match_offsets[l + 1]; ++idx) {
                emitter.append_left_row(l);
                emitter.append_right_row(right_matches[idx]);
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched_build_left[r] == 0U) {
                    emitter.append_left_defaults_from_right(r);
                    emitter.append_right_row(r);
                }
            }
        }
    };

    auto emit_membership_rows_from_right_scan = [&](auto&& for_each_left_match_for_right_row) {
        std::vector<std::uint8_t> left_matched(n_left, 0U);
        for (std::size_t r = 0; r < n_right; ++r) {
            for_each_left_match_for_right_row(r, [&](std::size_t l) { left_matched[l] = 1U; });
        }

        for (auto& ce : output.columns) {
            std::visit([n = n_left](auto& c) { c.reserve(n); }, *ce.column);
        }

        for (std::size_t l = 0; l < n_left; ++l) {
            const bool has_match = left_matched[l] != 0U;
            if ((semi_join && has_match) || (anti_join && !has_match)) {
                emitter.append_left_row(l);
            }
        }
    };

    if (preserve_left_rows || preserve_right_rows) {
        const auto n = std::max(n_left, n_right);
        for (auto& ce : output.columns) {
            std::visit([n](auto& c) { c.reserve(n); }, *ce.column);
        }
        for (auto& bm : right_validity) {
            bm.reserve(n);
        }
    }

    if (kind != ir::JoinKind::Asof && keys.size() == 1 &&
        (std::holds_alternative<Column<std::string>>(*left_keys[0]) ||
         std::holds_alternative<Column<Categorical>>(*left_keys[0]))) {
        if (n_left < n_right) {
            std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                               std::equal_to<std::string_view>>
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

            auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
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

            if (semi_join || anti_join) {
                emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
            } else {
                emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
            }
            emitter.finalize_left_validity();
            emitter.finalize_right_validity();
            return output;
        }

        std::unordered_map<std::string_view, std::vector<std::size_t>, StringViewHash,
                           std::equal_to<std::string_view>>
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

        std::vector<std::uint8_t> right_matched;
        if (preserve_right_rows) {
            right_matched.assign(n_right, 0U);
        }

        auto run_match = [&](std::string_view sv, std::size_t l) {
            auto it = right_sv_index.find(sv);
            if (it == right_sv_index.end()) {
                if (preserve_left_rows) {
                    emitter.append_left_row(l);
                    emitter.append_right_defaults();
                }
                return;
            }
            for (auto r : it->second) {
                emitter.append_left_row(l);
                emitter.append_right_row(r);
                if (preserve_right_rows) {
                    right_matched[r] = 1U;
                }
            }
        };

        if (const auto* lc = std::get_if<Column<Categorical>>(left_keys[0])) {
            const auto& dict = *lc->dictionary_ptr();
            std::vector<const std::vector<std::size_t>*> code_to_right(dict.size(), nullptr);
            for (std::size_t code = 0; code < dict.size(); ++code) {
                auto it = right_sv_index.find(std::string_view{dict[code]});
                if (it != right_sv_index.end()) {
                    code_to_right[code] = &it->second;
                }
            }
            const auto* codes = lc->codes_data();
            for (std::size_t l = 0; l < n_left; ++l) {
                const auto* matches = code_to_right[static_cast<std::size_t>(codes[l])];
                if (matches == nullptr) {
                    if (preserve_left_rows) {
                        emitter.append_left_row(l);
                        emitter.append_right_defaults();
                    }
                    continue;
                }
                for (auto r : *matches) {
                    emitter.append_left_row(l);
                    emitter.append_right_row(r);
                    if (preserve_right_rows) {
                        right_matched[r] = 1U;
                    }
                }
            }
        } else {
            const auto& ls = std::get<Column<std::string>>(*left_keys[0]);
            for (std::size_t l = 0; l < n_left; ++l) {
                run_match(ls[l], l);
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched[r] == 0U) {
                    emitter.append_left_defaults_from_right(r);
                    emitter.append_right_row(r);
                }
            }
        }

        emitter.finalize_left_validity();
        emitter.finalize_right_validity();
        return output;
    }

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

            auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
                auto it = left_int_index.find(right_ints[r]);
                if (it == left_int_index.end()) {
                    return;
                }
                for (auto l : it->second) {
                    emit_left_row(l);
                }
            };

            if (semi_join || anti_join) {
                emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
            } else {
                emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
            }
            emitter.finalize_left_validity();
            emitter.finalize_right_validity();
            return output;
        }

        std::unordered_map<std::int64_t, std::vector<std::size_t>> right_int_index;
        right_int_index.reserve(n_right);
        for (std::size_t r = 0; r < n_right; ++r) {
            right_int_index[right_ints[r]].push_back(r);
        }

        std::vector<std::uint8_t> right_matched;
        if (preserve_right_rows) {
            right_matched.assign(n_right, 0U);
        }

        for (std::size_t l = 0; l < n_left; ++l) {
            auto it = right_int_index.find(left_ints[l]);
            const bool has_match = (it != right_int_index.end());
            if (semi_join) {
                if (has_match) {
                    emitter.append_left_row(l);
                }
                continue;
            }
            if (anti_join) {
                if (!has_match) {
                    emitter.append_left_row(l);
                }
                continue;
            }
            if (!has_match) {
                if (preserve_left_rows) {
                    emitter.append_left_row(l);
                    emitter.append_right_defaults();
                }
                continue;
            }
            for (auto r : it->second) {
                emitter.append_left_row(l);
                emitter.append_right_row(r);
                if (preserve_right_rows) {
                    right_matched[r] = 1U;
                }
            }
        }

        if (preserve_right_rows) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (right_matched[r] == 0U) {
                    emitter.append_left_defaults_from_right(r);
                    emitter.append_right_row(r);
                }
            }
        }

        emitter.finalize_left_validity();
        emitter.finalize_right_validity();
        return output;
    }

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

        for (std::size_t l = 0; l < n_left; ++l) {
            emitter.append_left_row(l);

            Key group;
            group.values.reserve(left_eq_keys.size());
            for (const auto* col : left_eq_keys) {
                group.values.push_back(scalar_from_column(*col, l));
            }

            auto it = right_groups.find(group);
            if (it == right_groups.end()) {
                emitter.append_right_defaults();
                continue;
            }

            auto [pos_it, inserted] = right_pos.try_emplace(group, 0);
            (void)inserted;
            std::size_t& pos = pos_it->second;
            const auto& rows = it->second;
            while (pos < rows.size() && right_times[rows[pos]] <= left_times[l]) {
                ++pos;
            }

            if (pos == 0) {
                emitter.append_right_defaults();
            } else {
                emitter.append_right_row(rows[pos - 1]);
            }
        }

        output.time_index = left.time_index;
        normalize_time_index(output);
        emitter.finalize_right_validity();
        return output;
    }

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

        auto emit_left_matches_for_right_row = [&](std::size_t r, auto&& emit_left_row) {
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

        if (semi_join || anti_join) {
            emit_membership_rows_from_right_scan(emit_left_matches_for_right_row);
        } else {
            emit_preserving_rows_from_right_scan(emit_left_matches_for_right_row);
        }
        emitter.finalize_left_validity();
        emitter.finalize_right_validity();
        return output;
    }

    right_index.reserve(n_right);
    for (std::size_t r = 0; r < n_right; ++r) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : right_keys) {
            key.values.push_back(scalar_from_column(*col, r));
        }
        right_index[key].push_back(r);
    }

    std::vector<std::uint8_t> right_matched;
    if (preserve_right_rows) {
        right_matched.assign(n_right, 0U);
    }

    for (std::size_t l = 0; l < n_left; ++l) {
        Key key;
        key.values.reserve(keys.size());
        for (const auto* col : left_keys) {
            key.values.push_back(scalar_from_column(*col, l));
        }
        auto it = right_index.find(key);
        const bool has_match = (it != right_index.end());
        if (semi_join) {
            if (has_match) {
                emitter.append_left_row(l);
            }
            continue;
        }
        if (anti_join) {
            if (!has_match) {
                emitter.append_left_row(l);
            }
            continue;
        }
        if (!has_match) {
            if (preserve_left_rows) {
                emitter.append_left_row(l);
                emitter.append_right_defaults();
            }
            continue;
        }
        for (auto r : it->second) {
            emitter.append_left_row(l);
            emitter.append_right_row(r);
            if (preserve_right_rows) {
                right_matched[r] = 1U;
            }
        }
    }

    if (preserve_right_rows) {
        for (std::size_t r = 0; r < n_right; ++r) {
            if (right_matched[r] == 0U) {
                emitter.append_left_defaults_from_right(r);
                emitter.append_right_row(r);
            }
        }
    }

    emitter.finalize_left_validity();
    emitter.finalize_right_validity();
    return output;
}

}  // namespace ibex::runtime
