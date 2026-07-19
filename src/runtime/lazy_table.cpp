#include <ibex/ir/expr_predicates.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

LazyTable::LazyTable(Table schema, std::size_t rows, ColumnDecodeFn decode, SourceColumnStats stats,
                     KeyFilterScanFn key_filter_scan)
    : schema_(std::move(schema)),
      rows_(rows),
      decode_(std::move(decode)),
      stats_(std::move(stats)),
      key_filter_scan_(std::move(key_filter_scan)) {}

auto LazyTable::project(const std::set<std::string>& names) -> std::expected<Table, std::string> {
    std::vector<std::string> missing;
    for (const auto& entry : schema_.columns) {
        if (names.contains(entry.name) && !cache_.contains(entry.name)) {
            missing.push_back(entry.name);
        }
    }

    if (!missing.empty()) {
        auto decoded = decode_(missing, nullptr);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        for (auto& entry : decoded->columns) {
            auto name = entry.name;
            cache_.insert_or_assign(std::move(name), std::move(entry));
        }
        for (const auto& name : missing) {
            if (!cache_.contains(name)) {
                return std::unexpected("lazy source did not produce requested column '" + name +
                                       "'");
            }
        }
    }

    // Emit in schema order, so a projected table's column order matches the
    // source's regardless of the order columns happened to be decoded in.
    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        const auto& entry = cache_.at(field.name);
        out.add_column_shared(entry.name, entry.column, entry.validity);
    }
    // A plan may need the row count without needing any column — `count()` over
    // an unfiltered scan, say. Carry it so such a projection stays empty rather
    // than forcing a column to be decoded for its length alone.
    out.logical_rows = rows_;
    return out;
}

namespace {

/// Sampled pass rate above which a membership filter is not worth applying:
/// a near-full selection pushes every non-predicate column onto the
/// gather-decode path, slower than the dense decode it replaces (the same
/// lesson as the build-bounds selectivity gate).
constexpr double kMembershipPassRateCutoff = 0.75;
constexpr std::size_t kMembershipSampleMax = 65536;

struct KeyColumn {
    const std::int64_t* data = nullptr;
    const ValidityBitmap* validity = nullptr;
};

/// The membership filter only understands int64 keys; anything else means
/// "no filter", which is always sound.
auto int64_key_column(const Table& predicates, const std::string& key_name)
    -> std::optional<KeyColumn> {
    const auto* entry = predicates.find_entry(key_name);
    if (entry == nullptr) {
        return std::nullopt;
    }
    const auto* keys = std::get_if<Column<std::int64_t>>(&*entry->column);
    if (keys == nullptr) {
        return std::nullopt;
    }
    return KeyColumn{
        .data = keys->data(),
        .validity = entry->validity.has_value() ? &*entry->validity : nullptr,
    };
}

/// Rows with a null key are rejected too: a deferred scan feeds exactly one
/// inner join (eligibility proof), and null keys never match.
auto key_passes(const KeyColumn& key, const DynamicScanFilter& filter, std::size_t row) -> bool {
    return (key.validity == nullptr || (*key.validity)[row]) && filter.passes(key.data[row]);
}

/// Estimate the filter's pass rate over the candidate rows `rows(i)`,
/// i < n. Strided, not a prefix: fact tables are often ordered by the join
/// key, so a prefix sample would see one narrow key range and lie. Returns
/// 0.0 for small n — a useless pass over few rows costs nothing, so it never
/// needs vetoing.
template <typename RowAt>
auto membership_pass_rate(const KeyColumn& key, const DynamicScanFilter& filter, std::size_t n,
                          RowAt rows) -> double {
    if (n <= kMembershipSampleMax) {
        return 0.0;
    }
    const std::size_t stride = n / kMembershipSampleMax;
    std::size_t sampled = 0;
    std::size_t passed = 0;
    for (std::size_t i = 0; i < n; i += stride) {
        ++sampled;
        passed += key_passes(key, filter, rows(i)) ? 1 : 0;
    }
    return static_cast<double>(passed) / static_cast<double>(sampled);
}

/// AND the membership filter into an existing selection, in place. Skipped
/// (selection untouched) when the sample says it barely rejects.
void apply_membership_filter(const KeyColumn& key, const DynamicScanFilter& filter,
                             std::vector<std::size_t>& selected) {
    if (membership_pass_rate(key, filter, selected.size(), [&](std::size_t i) {
            return selected[i];
        }) > kMembershipPassRateCutoff) {
        return;
    }
    auto end = std::remove_if(selected.begin(), selected.end(),
                              [&](std::size_t row) { return !key_passes(key, filter, row); });
    selected.erase(end, selected.end());
}

/// Build a selection straight from the membership filter (no static
/// conjuncts). nullopt = the filter barely rejects; caller should decode
/// densely instead.
auto membership_selection(const KeyColumn& key, const DynamicScanFilter& filter, std::size_t rows)
    -> std::optional<std::vector<std::size_t>> {
    const auto sampled_rate =
        membership_pass_rate(key, filter, rows, [](std::size_t i) { return i; });
    if (sampled_rate > kMembershipPassRateCutoff) {
        return std::nullopt;
    }
    // One filter pass, not count-then-fill: a Bloom probe per key is the
    // expensive part here, and the sampled rate gives a good enough reserve
    // that push_back growth is rare.
    std::vector<std::size_t> selected;
    selected.reserve(
        std::min(rows, static_cast<std::size_t>(sampled_rate * 1.2 * static_cast<double>(rows)) +
                           kMembershipSampleMax));
    for (std::size_t row = 0; row < rows; ++row) {
        if (key_passes(key, filter, row)) {
            selected.push_back(row);
        }
    }
    return selected;
}

}  // namespace

auto LazyTable::project_where(const std::set<std::string>& names,
                              const std::vector<ir::Expr>& conjuncts, const ScalarRegistry* scalars,
                              const DynamicScanFilter* dynamic, const std::string* dynamic_key)
    -> std::expected<Table, std::string> {
    const bool membership =
        dynamic != nullptr && dynamic_key != nullptr && dynamic->has_membership();
    if (conjuncts.empty() && !membership) {
        return project(names);
    }

    // Fused path: the source evaluates the key filter inside its own decoder,
    // so the key column is never materialized whole-file. Only worth taking
    // when nothing else needs that column densely — a cached key means the
    // in-memory filter pass below is cheaper than re-reading pages.
    if (membership && conjuncts.empty() && key_filter_scan_ != nullptr &&
        !cache_.contains(*dynamic_key)) {
        auto scan = key_filter_scan_(*dynamic_key, *dynamic);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        if (scan->has_value()) {
            Selection selected = std::move(**scan);
            const bool all_rows = selected.size() == rows_;
            std::vector<std::string> wanted;
            for (const auto& field : schema_.columns) {
                if (names.contains(field.name)) {
                    wanted.push_back(field.name);
                }
            }
            if (!wanted.empty()) {
                auto decoded = decode_(wanted, all_rows ? nullptr : &selected);
                if (!decoded) {
                    return std::unexpected(decoded.error());
                }
                if (decoded->rows() != selected.size()) {
                    return std::unexpected(
                        "lazy source produced selected columns with the wrong row count");
                }
                Table out;
                for (const auto& name : wanted) {
                    const auto* entry = decoded->find_entry(name);
                    if (entry == nullptr) {
                        return std::unexpected("lazy source did not produce requested column '" +
                                               name + "'");
                    }
                    out.add_column_shared(entry->name, entry->column, entry->validity);
                }
                out.logical_rows = selected.size();
                return out;
            }
        }
        // No fused answer (unsupported type, or the filter stopped
        // rejecting): the ordinary decode-then-filter path below stands.
    }

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : conjuncts) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }
    if (membership) {
        referenced.insert(*dynamic_key);
    }

    auto predicates_res = decode_whole_columns(referenced);
    if (!predicates_res) {
        return std::unexpected(predicates_res.error());
    }
    Table& predicates = *predicates_res;

    const auto key =
        membership ? int64_key_column(predicates, *dynamic_key) : std::optional<KeyColumn>{};

    std::expected<std::vector<std::size_t>, std::string> selected;
    if (!conjuncts.empty()) {
        selected = filter_selection(predicates, conjuncts, scalars);
        if (!selected) {
            return std::unexpected(selected.error());
        }
        if (key.has_value()) {
            apply_membership_filter(*key, *dynamic, *selected);
        }
    } else {
        if (!key.has_value()) {
            return project(names);  // key missing or non-int64: no filter to apply
        }
        auto from_membership = membership_selection(*key, *dynamic, rows_);
        if (!from_membership.has_value()) {
            return project(names);  // escape hatch: the filter barely rejects
        }
        selected = std::move(*from_membership);
    }
    const bool all_rows = selected->size() == rows_;

    std::vector<std::string> remaining;
    for (const auto& field : schema_.columns) {
        if (names.contains(field.name) && !referenced.contains(field.name)) {
            remaining.push_back(field.name);
        }
    }

    Table decoded_remaining;
    if (!remaining.empty()) {
        auto decoded = decode_(remaining, all_rows ? nullptr : &*selected);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        decoded_remaining = std::move(*decoded);
        for (const auto& name : remaining) {
            if (decoded_remaining.find_entry(name) == nullptr) {
                return std::unexpected("lazy source did not produce requested column '" + name +
                                       "'");
            }
        }
        if (decoded_remaining.rows() != selected->size()) {
            return std::unexpected(
                "lazy source produced selected columns with the wrong row count");
        }
    }

    robin_hood::unordered_map<std::string, ColumnEntry> selected_columns;
    selected_columns.reserve(names.size());
    for (const auto& entry : predicates.columns) {
        if (!names.contains(entry.name)) {
            continue;
        }
        ColumnEntry gathered;
        gathered.name = entry.name;
        if (all_rows) {
            gathered.column = entry.column;
            gathered.validity = entry.validity;
        } else {
            gathered.column = std::make_shared<ColumnValue>(
                gather_column(*entry.column, selected->data(), selected->size()));
            if (entry.validity.has_value()) {
                ValidityBitmap validity(selected->size(), true);
                for (std::size_t row = 0; row < selected->size(); ++row) {
                    validity.set(row, (*entry.validity)[(*selected)[row]]);
                }
                gathered.validity = std::move(validity);
            }
        }
        selected_columns.insert_or_assign(gathered.name, std::move(gathered));
    }
    for (auto& entry : decoded_remaining.columns) {
        auto name = entry.name;
        selected_columns.insert_or_assign(std::move(name), std::move(entry));
    }

    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        auto it = selected_columns.find(field.name);
        if (it == selected_columns.end()) {
            return std::unexpected("lazy source did not produce requested column '" + field.name +
                                   "'");
        }
        out.add_column_shared(it->second.name, it->second.column, it->second.validity);
    }
    out.logical_rows = selected->size();
    return out;
}

auto LazyTable::decode_whole_columns(const robin_hood::unordered_set<std::string>& referenced)
    -> std::expected<Table, std::string> {
    // Predicate columns are decoded whole-file (the selection needs every
    // row), so they are legitimate cache entries: reuse any already cached,
    // and cache the ones decoded here for later projections.
    std::vector<std::string> missing;
    missing.reserve(referenced.size());
    for (const auto& field : schema_.columns) {
        if (referenced.contains(field.name) && !cache_.contains(field.name)) {
            missing.push_back(field.name);
        }
    }
    if (!missing.empty()) {
        auto decoded = decode_(missing, nullptr);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        for (auto& entry : decoded->columns) {
            auto name = entry.name;
            cache_.insert_or_assign(std::move(name), std::move(entry));
        }
        for (const auto& name : missing) {
            if (!cache_.contains(name)) {
                return std::unexpected("lazy source did not produce predicate column '" + name +
                                       "'");
            }
        }
    }

    Table out;
    for (const auto& field : schema_.columns) {
        if (!referenced.contains(field.name)) {
            continue;
        }
        const auto& entry = cache_.at(field.name);
        out.add_column_shared(entry.name, entry.column, entry.validity);
    }
    out.logical_rows = rows_;
    if (!out.columns.empty() && out.rows() != rows_) {
        return std::unexpected("lazy source produced predicate columns with the wrong row count");
    }
    return out;
}

auto LazyTable::project_rows(const std::set<std::string>& names, const Selection& selected)
    -> std::expected<Table, std::string> {
    const bool all_rows = selected.size() == rows_;
    // Columns already cached whole-file (predicate columns, or another scan
    // instance's decode) are gathered in memory — re-reading their pages
    // through the selection would repeat work already paid for.
    std::vector<std::string> missing;
    for (const auto& field : schema_.columns) {
        if (names.contains(field.name) && !cache_.contains(field.name)) {
            missing.push_back(field.name);
        }
    }
    Table decoded;
    if (!missing.empty()) {
        auto res = decode_(missing, all_rows ? nullptr : &selected);
        if (!res) {
            return std::unexpected(res.error());
        }
        decoded = std::move(*res);
        if (decoded.rows() != selected.size()) {
            return std::unexpected(
                "lazy source produced selected columns with the wrong row count");
        }
    }
    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        if (const auto cached = cache_.find(field.name); cached != cache_.end()) {
            const auto& entry = cached->second;
            if (all_rows) {
                out.add_column_shared(entry.name, entry.column, entry.validity);
            } else {
                auto column = std::make_shared<ColumnValue>(
                    gather_column(*entry.column, selected.data(), selected.size()));
                std::optional<ValidityBitmap> validity;
                if (entry.validity.has_value()) {
                    ValidityBitmap bits(selected.size(), true);
                    for (std::size_t row = 0; row < selected.size(); ++row) {
                        bits.set(row, (*entry.validity)[selected[row]]);
                    }
                    validity = std::move(bits);
                }
                out.add_column_shared(entry.name, std::move(column), std::move(validity));
            }
            continue;
        }
        const auto* entry = decoded.find_entry(field.name);
        if (entry == nullptr) {
            return std::unexpected("lazy source did not produce requested column '" + field.name +
                                   "'");
        }
        out.add_column_shared(entry->name, entry->column, entry->validity);
    }
    out.logical_rows = selected.size();
    return out;
}

auto LazyTable::join_key_selection(const std::vector<ir::Expr>& conjuncts,
                                   const ScalarRegistry* scalars, const DynamicScanFilter& dynamic,
                                   const std::string& key_name)
    -> std::expected<std::optional<JoinKeySelection>, std::string> {
    if (!dynamic.has_membership()) {
        return std::optional<JoinKeySelection>{};
    }

    // Fused path, same conditions as project_where: the source computes the
    // selection during the key column's decode, then only the surviving key
    // values are decoded at all.
    if (conjuncts.empty() && key_filter_scan_ != nullptr && !cache_.contains(key_name)) {
        auto scan = key_filter_scan_(key_name, dynamic);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        if (!scan->has_value()) {
            return std::optional<JoinKeySelection>{};  // no fused answer
        }
        JoinKeySelection out;
        out.selected = std::move(**scan);
        auto keys = project_rows({key_name}, out.selected);
        if (!keys) {
            return std::unexpected(keys.error());
        }
        auto* entry = keys->find_entry(key_name);
        if (entry == nullptr || !std::holds_alternative<Column<std::int64_t>>(*entry->column)) {
            return std::optional<JoinKeySelection>{};
        }
        out.keys = std::move(*entry);
        return std::optional{std::move(out)};
    }

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : conjuncts) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }
    referenced.insert(key_name);

    auto predicates_res = decode_whole_columns(referenced);
    if (!predicates_res) {
        return std::unexpected(predicates_res.error());
    }
    Table& predicates = *predicates_res;

    const auto key = int64_key_column(predicates, key_name);
    if (!key.has_value()) {
        return std::optional<JoinKeySelection>{};
    }

    JoinKeySelection out;
    if (!conjuncts.empty()) {
        auto selected = filter_selection(predicates, conjuncts, scalars);
        if (!selected) {
            return std::unexpected(selected.error());
        }
        apply_membership_filter(*key, dynamic, *selected);
        out.selected = std::move(*selected);
    } else {
        auto from_membership = membership_selection(*key, dynamic, rows_);
        if (!from_membership.has_value()) {
            return std::optional<JoinKeySelection>{};  // escape hatch
        }
        out.selected = std::move(*from_membership);
    }

    // Gather the key values for the selected rows from the cached whole
    // column.
    const auto* entry = predicates.find_entry(key_name);
    out.keys.name = key_name;
    if (out.selected.size() == rows_) {
        out.keys.column = entry->column;
        out.keys.validity = entry->validity;
    } else {
        out.keys.column = std::make_shared<ColumnValue>(
            gather_column(*entry->column, out.selected.data(), out.selected.size()));
        if (entry->validity.has_value()) {
            ValidityBitmap validity(out.selected.size(), true);
            for (std::size_t row = 0; row < out.selected.size(); ++row) {
                validity.set(row, (*entry->validity)[out.selected[row]]);
            }
            out.keys.validity = std::move(validity);
        }
    }
    return std::optional{std::move(out)};
}

auto LazyTable::materialize() -> std::expected<Table, std::string> {
    std::set<std::string> names;
    for (const auto& entry : schema_.columns) {
        names.insert(entry.name);
    }
    return project(names);
}

}  // namespace ibex::runtime
