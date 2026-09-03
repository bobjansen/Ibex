// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/like.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "execution_profile_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

class LazyTable::ReaderPool {
   public:
    std::mutex mutex;
    std::vector<LazySourceReaderPtr> available;
};

LazyTable::LazyTable(Table schema, std::size_t rows, ColumnDecodeFn decode, SourceColumnStats stats,
                     KeyFilterScanFn key_filter_scan)
    : schema_(std::move(schema)),
      rows_(rows),
      decode_(std::move(decode)),
      stats_(std::move(stats)),
      key_filter_scan_(std::move(key_filter_scan)) {}

LazyTable::LazyTable(Table schema, std::size_t rows, LazySourceReaderFactory reader_factory,
                     SourceColumnStats stats)
    : schema_(std::move(schema)),
      rows_(rows),
      reader_factory_(std::move(reader_factory)),
      reader_pool_(std::make_shared<ReaderPool>()),
      stats_(std::move(stats)) {}

auto LazyTable::decode_columns(const std::vector<std::string>& names, const Selection* selection,
                               const SourceUnit* unit, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    auto* profile_entry =
        exec.execution_profile == nullptr
            ? nullptr
            : exec.execution_profile->stage(selection == nullptr ? "source decode whole"
                                                                 : "source decode selected");
    const ExecutionProfileScope profile_scope(profile_entry, ProfilePhase::Source);
    if (reader_factory_) {
        auto reader = acquire_reader();
        if (!reader) {
            return std::unexpected(reader.error());
        }
        auto result = (*reader)->decode(names, selection, unit, exec);
        if (result) {
            release_reader(std::move(*reader));
        }
        return result;
    }
    // Only a factory-backed source can report units, so a decode function
    // source is never asked for one.
    return decode_(names, selection);
}

auto LazyTable::scan_units() -> std::vector<SourceUnit> {
    if (!reader_factory_) {
        return {};
    }
    auto reader = acquire_reader();
    if (!reader) {
        return {};  // the failure resurfaces at the first real decode
    }
    auto units = (*reader)->decode_units();
    release_reader(std::move(*reader));
    return units;
}

auto LazyTable::scan_key_filter(const std::string& key, const DynamicScanFilter& filter,
                                const SourceUnit* unit, const ExecutionContext& exec)
    -> std::expected<std::optional<Selection>, std::string> {
    auto* profile_entry = exec.execution_profile == nullptr
                              ? nullptr
                              : exec.execution_profile->stage("source dynamic key scan");
    const ExecutionProfileScope profile_scope(profile_entry, ProfilePhase::Source);
    if (reader_factory_) {
        auto reader = acquire_reader();
        if (!reader) {
            return std::unexpected(reader.error());
        }
        auto result = (*reader)->key_filter_scan(key, filter, unit, exec);
        if (result) {
            release_reader(std::move(*reader));
        }
        return result;
    }
    return key_filter_scan_(key, filter, exec);
}

auto LazyTable::acquire_reader() -> std::expected<LazySourceReaderPtr, std::string> {
    {
        const std::lock_guard lock(reader_pool_->mutex);
        if (!reader_pool_->available.empty()) {
            auto reader = std::move(reader_pool_->available.back());
            reader_pool_->available.pop_back();
            return reader;
        }
    }
    auto reader = reader_factory_();
    if (!reader) {
        return std::unexpected(reader.error());
    }
    if (*reader == nullptr) {
        return std::unexpected("lazy source reader factory returned null");
    }
    return reader;
}

void LazyTable::release_reader(LazySourceReaderPtr reader) {
    const std::lock_guard lock(reader_pool_->mutex);
    reader_pool_->available.push_back(std::move(reader));
}

auto LazyTable::project_uncached(const std::set<std::string>& names, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    std::vector<std::string> wanted;
    for (const auto& entry : schema_.columns) {
        if (names.contains(entry.name)) {
            wanted.push_back(entry.name);
        }
    }
    if (wanted.empty()) {
        Table empty;
        empty.logical_rows = rows_;
        return empty;
    }
    // Reuse anything already cached — reading it costs nothing and does not
    // make the cache any more poisoned than it already is — but never insert.
    std::vector<std::string> missing;
    for (const auto& name : wanted) {
        if (!cache_.contains(name)) {
            missing.push_back(name);
        }
    }
    Table out;
    if (!missing.empty()) {
        auto decoded = decode_columns(missing, nullptr, nullptr, exec);
        if (!decoded) {
            return std::unexpected(decoded.error());
        }
        for (auto& entry : decoded->columns) {
            out.add_column_from(entry.name, entry);
        }
    }
    for (const auto& name : wanted) {
        if (out.find(name) != nullptr) {
            continue;
        }
        const auto it = cache_.find(name);
        if (it == cache_.end()) {
            return std::unexpected("lazy source did not produce requested column '" + name + "'");
        }
        out.add_column_from(it->second.name, it->second);
    }
    out.logical_rows = rows_;
    return out;
}

auto LazyTable::project(const std::set<std::string>& names, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    std::vector<std::string> missing;
    for (const auto& entry : schema_.columns) {
        if (names.contains(entry.name) && !cache_.contains(entry.name)) {
            missing.push_back(entry.name);
        }
    }

    if (!missing.empty()) {
        auto decoded = decode_columns(missing, nullptr, nullptr, exec);
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
        out.add_column_from(entry.name, entry);
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

/// How many ranges a membership pass over `n` candidates may fan out to (1 =
/// run serial). The filter is a read-only Bloom/IN-list probe per row, so
/// ranges share nothing; each builds its own kept list and the lists are
/// concatenated in range order, which is exactly the serial order. Gated the
/// same way as `for_row_ranges` — that helper itself wants a pre-sized
/// output, which a filtered selection cannot supply.
auto membership_ranges(const ExecutionContext& exec, std::size_t n) -> std::size_t {
    constexpr std::size_t kMaxRanges = 64;
    if (!exec.can_fan_out() || !exec.parallel_join_probe || n < exec.parallel_min_rows ||
        on_worker_pool_thread()) {
        return 1;
    }
    const std::size_t min_rows = std::max<std::size_t>(exec.parallel_min_rows, 1);
    auto& pool = process_worker_pool();
    const std::size_t budget = exec.compute_budget();
    return std::min({std::clamp<std::size_t>(n / min_rows, 1, kMaxRanges), budget, pool.size()});
}

/// Fan the row-keep predicate out over `ranges` contiguous ranges of
/// `[0, n)`, appending each range's surviving `row_at(i)` values, then stitch
/// the per-range lists back in order.
template <typename RowAt>
auto keep_rows_parallel(std::size_t n, std::size_t ranges, const KeyColumn& key,
                        const DynamicScanFilter& filter, const RowAt& row_at)
    -> std::vector<std::size_t> {
    std::vector<std::vector<std::size_t>> parts(ranges);
    const std::size_t grain = (n + ranges - 1) / ranges;
    auto batch = process_worker_pool().submit(ranges, [&](std::size_t r) {
        const std::size_t begin = r * grain;
        const std::size_t end = std::min(n, begin + grain);
        if (begin >= end) {
            return;
        }
        auto& part = parts[r];
        part.reserve(end - begin);
        for (std::size_t i = begin; i < end; ++i) {
            const std::size_t row = row_at(i);
            if (key_passes(key, filter, row)) {
                part.push_back(row);
            }
        }
    });
    batch.wait();
    std::size_t total = 0;
    for (const auto& part : parts) {
        total += part.size();
    }
    std::vector<std::size_t> kept;
    kept.reserve(total);
    for (const auto& part : parts) {
        kept.insert(kept.end(), part.begin(), part.end());
    }
    return kept;
}

/// AND the membership filter into an existing selection, in place. Skipped
/// (selection untouched) when the sample says it barely rejects.
void apply_membership_filter(const KeyColumn& key, const DynamicScanFilter& filter,
                             std::vector<std::size_t>& selected, const ExecutionContext& exec) {
    if (membership_pass_rate(key, filter, selected.size(), [&](std::size_t i) {
            return selected[i];
        }) > kMembershipPassRateCutoff) {
        return;
    }
    if (const std::size_t ranges = membership_ranges(exec, selected.size()); ranges >= 2) {
        selected = keep_rows_parallel(selected.size(), ranges, key, filter,
                                      [&](std::size_t i) { return selected[i]; });
        return;
    }
    auto end = std::remove_if(selected.begin(), selected.end(),
                              [&](std::size_t row) { return !key_passes(key, filter, row); });
    selected.erase(end, selected.end());
}

/// Build a selection straight from the membership filter (no static
/// conjuncts). nullopt = the filter barely rejects; caller should decode
/// densely instead.
auto membership_selection(const KeyColumn& key, const DynamicScanFilter& filter, std::size_t rows,
                          const ExecutionContext& exec) -> std::optional<std::vector<std::size_t>> {
    const auto sampled_rate =
        membership_pass_rate(key, filter, rows, [](std::size_t i) { return i; });
    if (sampled_rate > kMembershipPassRateCutoff) {
        return std::nullopt;
    }
    if (const std::size_t ranges = membership_ranges(exec, rows); ranges >= 2) {
        return keep_rows_parallel(rows, ranges, key, filter, [](std::size_t i) { return i; });
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

/// `like(col, "pattern")`, possibly under any number of `not`s. Returns the
/// column and the compiled filter, or nullopt when the expression is any other
/// shape — including a `like` whose pattern is not a literal (it would have to
/// be evaluated per row) or does not compile (the ordinary path reports that
/// error, and reporting it from here would change nothing but the blame).
auto as_like_predicate(const ir::Expr& expr, bool negated)
    -> std::optional<std::pair<std::string, StringScanFilter>> {
    if (const auto* logical = std::get_if<ir::LogicalExpr>(&expr.node)) {
        if (logical->op != ir::LogicalOp::Not || logical->left == nullptr) {
            return std::nullopt;
        }
        return as_like_predicate(*logical->left, !negated);
    }
    const auto* call = std::get_if<ir::CallExpr>(&expr.node);
    if (call == nullptr || call->callee != "like" || call->args.size() != 2 ||
        !call->named_args.empty()) {
        return std::nullopt;
    }
    const auto* column = call->args[0] == nullptr ? nullptr : ir::as_column_ref(*call->args[0]);
    const auto* pattern_expr =
        call->args[1] == nullptr ? nullptr : std::get_if<ir::Literal>(&call->args[1]->node);
    if (column == nullptr || pattern_expr == nullptr) {
        return std::nullopt;
    }
    const auto* pattern = std::get_if<std::string>(&pattern_expr->value);
    if (pattern == nullptr) {
        return std::nullopt;
    }
    auto compiled = compile_like_pattern(*pattern);
    if (!compiled) {
        return std::nullopt;
    }
    return std::pair{column->name,
                     StringScanFilter{.pattern = std::move(*compiled), .negated = negated}};
}

auto inverted_compare(ir::CompareOp op) -> ir::CompareOp {
    switch (op) {
        case ir::CompareOp::Lt:
            return ir::CompareOp::Gt;
        case ir::CompareOp::Le:
            return ir::CompareOp::Ge;
        case ir::CompareOp::Gt:
            return ir::CompareOp::Lt;
        case ir::CompareOp::Ge:
            return ir::CompareOp::Le;
        default:
            return op;
    }
}

auto integer_literal(const ir::Expr& expr) -> std::optional<std::int64_t> {
    const auto* literal = std::get_if<ir::Literal>(&expr.node);
    if (literal == nullptr)
        return std::nullopt;
    if (const auto* integer = std::get_if<std::int64_t>(&literal->value))
        return *integer;
    if (const auto* date = std::get_if<Date>(&literal->value))
        return date->days;
    return std::nullopt;
}

/// A conjunction made solely of literal comparisons on one integer-like source
/// column. The result is an inclusive interval accepted by DynamicScanFilter.
auto static_range_filter(const std::vector<ir::Expr>& conjuncts)
    -> std::optional<std::pair<std::string, DynamicScanFilter>> {
    if (conjuncts.empty())
        return std::nullopt;
    std::optional<std::string> name;
    DynamicScanFilter filter;
    for (const auto& expr : conjuncts) {
        const auto* comparison = std::get_if<ir::CompareExpr>(&expr.node);
        if (comparison == nullptr || comparison->left == nullptr || comparison->right == nullptr ||
            comparison->op == ir::CompareOp::Ne)
            return std::nullopt;
        const auto* column = ir::as_column_ref(*comparison->left);
        auto value = integer_literal(*comparison->right);
        auto op = comparison->op;
        if (column == nullptr || !value.has_value()) {
            column = ir::as_column_ref(*comparison->right);
            value = integer_literal(*comparison->left);
            op = inverted_compare(op);
        }
        if (column == nullptr || column->lexical || !value.has_value())
            return std::nullopt;
        if (name.has_value() && *name != column->name)
            return std::nullopt;
        name = column->name;
        switch (op) {
            case ir::CompareOp::Eq:
                filter.min = filter.min.has_value() ? std::max(*filter.min, *value) : *value;
                filter.max = filter.max.has_value() ? std::min(*filter.max, *value) : *value;
                break;
            case ir::CompareOp::Le:
                filter.max = filter.max.has_value() ? std::min(*filter.max, *value) : *value;
                break;
            case ir::CompareOp::Ge:
                filter.min = filter.min.has_value() ? std::max(*filter.min, *value) : *value;
                break;
            case ir::CompareOp::Lt:
                if (*value == std::numeric_limits<std::int64_t>::min())
                    return std::nullopt;
                --*value;
                filter.max = filter.max.has_value() ? std::min(*filter.max, *value) : *value;
                break;
            case ir::CompareOp::Gt:
                if (*value == std::numeric_limits<std::int64_t>::max())
                    return std::nullopt;
                ++*value;
                filter.min = filter.min.has_value() ? std::max(*filter.min, *value) : *value;
                break;
            case ir::CompareOp::Ne:
                return std::nullopt;
        }
    }
    if (!name.has_value())
        return std::nullopt;
    return std::pair{std::move(*name), std::move(filter)};
}

}  // namespace

auto LazyTable::fusable_string_conjuncts(const std::vector<ir::Expr>& conjuncts,
                                         const std::set<std::string>& names,
                                         std::vector<FusedStringConjunct>& fused,
                                         std::vector<ir::Expr>& remaining) const -> bool {
    // Only the reader-backed sources implement the fused scan; the plain
    // `ColumnDecodeFn` constructor has no seam to offer it through.
    if (!reader_factory_) {
        return false;
    }

    // How many conjuncts read each column, so "read by nothing else" is a
    // lookup rather than a rescan per candidate.
    robin_hood::unordered_map<std::string, std::size_t> readers;
    std::vector<robin_hood::unordered_set<std::string>> refs(conjuncts.size());
    for (std::size_t i = 0; i < conjuncts.size(); ++i) {
        ir::collect_expr_column_refs(conjuncts[i], refs[i]);
        for (const auto& name : refs[i]) {
            ++readers[name];
        }
    }

    for (std::size_t i = 0; i < conjuncts.size(); ++i) {
        auto predicate = as_like_predicate(conjuncts[i], false);
        // The conjunct must read its column and nothing else, that column must
        // be a plain String in the source (a dictionary column decodes to
        // codes, which is already cheap and would be *slower* value by value),
        // and no one else may want it: not the projection, not another
        // conjunct, and not a past query that left it in the cache.
        if (predicate.has_value() && refs[i].size() == 1 && refs[i].contains(predicate->first) &&
            readers[predicate->first] == 1 && !names.contains(predicate->first) &&
            !cache_.contains(predicate->first)) {
            const auto* entry = schema_.find_entry(predicate->first);
            if (entry != nullptr && entry->column != nullptr &&
                std::holds_alternative<Column<std::string>>(*entry->column)) {
                fused.push_back(FusedStringConjunct{.column = std::move(predicate->first),
                                                    .filter = std::move(predicate->second)});
                continue;
            }
        }
        remaining.push_back(conjuncts[i]);
    }
    return !fused.empty();
}

auto LazyTable::scan_string_filters(const std::vector<FusedStringConjunct>& fused,
                                    const SourceUnit* unit, const ExecutionContext& exec)
    -> std::expected<std::optional<Selection>, std::string> {
    auto* profile_entry = exec.execution_profile == nullptr
                              ? nullptr
                              : exec.execution_profile->stage("source string filter scan");
    const ExecutionProfileScope profile_scope(profile_entry, ProfilePhase::Source);

    auto reader = acquire_reader();
    if (!reader) {
        return std::unexpected(reader.error());
    }
    std::optional<Selection> selected;
    for (const auto& conjunct : fused) {
        auto part = (*reader)->string_filter_scan(conjunct.column, conjunct.filter, unit, exec);
        if (!part) {
            return std::unexpected(part.error());
        }
        if (!part->has_value()) {
            // No fused answer: all or nothing, since a partial one would drop
            // the conjuncts already consumed. The reader goes back to the pool
            // because the fallback is about to decode through it.
            release_reader(std::move(*reader));
            return std::optional<Selection>{};
        }
        if (!selected.has_value()) {
            selected = std::move(**part);
            continue;
        }
        // Both are ascending source-row indices, so ANDing them is a merge.
        Selection both;
        both.reserve(std::min(selected->size(), (*part)->size()));
        std::ranges::set_intersection(*selected, **part, std::back_inserter(both));
        selected = std::move(both);
    }
    release_reader(std::move(*reader));
    return selected;
}

auto LazyTable::selection_for(const std::set<std::string>& output_names,
                              const std::vector<ir::Expr>& conjuncts, const ExecutionContext& exec,
                              const ScalarRegistry* scalars)
    -> std::expected<std::optional<Selection>, std::string> {
    if (conjuncts.empty()) {
        return std::optional<Selection>{};  // every row
    }

    // Conjuncts over a column nothing else reads are answered inside the page
    // decoder, so that column is never built. This is the mechanism a repeated
    // source currently loses entirely.
    std::vector<FusedStringConjunct> fused;
    std::vector<ir::Expr> unfused;
    std::optional<Selection> fused_selection;
    if (fusable_string_conjuncts(conjuncts, output_names, fused, unfused)) {
        auto scan = scan_string_filters(fused, nullptr, exec);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        fused_selection = std::move(*scan);
    }
    // A source that declined the fused scan answered nothing, so every conjunct
    // takes the ordinary path -- matching `project_where`.
    const std::vector<ir::Expr>& applied = fused_selection.has_value() ? unfused : conjuncts;

    std::optional<Selection> selected;
    if (!applied.empty()) {
        robin_hood::unordered_set<std::string> referenced;
        for (const auto& conjunct : applied) {
            ir::collect_expr_column_refs(conjunct, referenced);
        }
        auto predicates = decode_whole_columns(referenced, exec);
        if (!predicates) {
            return std::unexpected(predicates.error());
        }
        auto from_conjuncts = filter_selection(*predicates, applied, exec, scalars);
        if (!from_conjuncts) {
            return std::unexpected(from_conjuncts.error());
        }
        selected = std::move(*from_conjuncts);
    }

    if (fused_selection.has_value()) {
        if (!selected.has_value()) {
            selected = std::move(*fused_selection);
        } else {
            // Both are ascending source-row indices, so ANDing them is a merge.
            Selection both;
            both.reserve(std::min(selected->size(), fused_selection->size()));
            std::ranges::set_intersection(*selected, *fused_selection, std::back_inserter(both));
            selected = std::move(both);
        }
    }
    return selected;
}

auto LazyTable::project_where(const std::set<std::string>& names,
                              const std::vector<ir::Expr>& conjuncts, const ExecutionContext& exec,
                              const ScalarRegistry* scalars, const DynamicScanFilter* dynamic,
                              const std::string* dynamic_key) -> std::expected<Table, std::string> {
    const bool membership =
        dynamic != nullptr && dynamic_key != nullptr && dynamic->has_membership();
    if (conjuncts.empty() && !membership) {
        return project(names, exec);
    }

    // A predicate-only literal range can be decided by a reader while it
    // decodes the key, leaving only the selected payload columns to decode.
    // This is intentionally all-or-nothing: a source decline falls through to
    // the established decode-and-filter path unchanged.
    if (reader_factory_ && !conjuncts.empty()) {
        if (auto range = static_range_filter(conjuncts);
            range.has_value() && !names.contains(range->first) && !cache_.contains(range->first)) {
            auto scan = scan_key_filter(range->first, range->second, nullptr, exec);
            if (!scan)
                return std::unexpected(scan.error());
            if (scan->has_value()) {
                const Selection& selected = **scan;
                const bool all_rows = selected.size() == rows_;
                std::vector<std::string> wanted;
                for (const auto& field : schema_.columns)
                    if (names.contains(field.name))
                        wanted.push_back(field.name);
                auto decoded =
                    decode_columns(wanted, all_rows ? nullptr : &selected, nullptr, exec);
                if (!decoded)
                    return std::unexpected(decoded.error());
                return std::move(*decoded);
            }
        }
    }

    // Fused path: the source evaluates the key filter inside its own decoder,
    // so the key column is never materialized whole-file. Only worth taking
    // when nothing else needs that column densely — a cached key means the
    // in-memory filter pass below is cheaper than re-reading pages.
    if (membership && conjuncts.empty() && (key_filter_scan_ != nullptr || reader_factory_) &&
        !cache_.contains(*dynamic_key)) {
        auto scan = scan_key_filter(*dynamic_key, *dynamic, nullptr, exec);
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
                auto decoded =
                    decode_columns(wanted, all_rows ? nullptr : &selected, nullptr, exec);
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
                    out.add_column_from(entry->name, *entry);
                }
                out.logical_rows = selected.size();
                return out;
            }
        }
        // No fused answer (unsupported type, or the filter stopped
        // rejecting): the ordinary decode-then-filter path below stands.
    }

    // A conjunct whose column the rest of the query never looks at does not
    // need that column at all — only its answer. Hand those to the source to
    // evaluate inside its decoder, and let the conjuncts they did not claim go
    // on being evaluated the ordinary way below. A source that declines gives
    // back no selection at all, and then every conjunct takes that path.
    std::vector<FusedStringConjunct> fused;
    std::vector<ir::Expr> unfused;
    std::optional<Selection> fused_selection;
    if (fusable_string_conjuncts(conjuncts, names, fused, unfused)) {
        auto scan = scan_string_filters(fused, nullptr, exec);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        fused_selection = std::move(*scan);
    }
    const std::vector<ir::Expr>& applied = fused_selection.has_value() ? unfused : conjuncts;

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : applied) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }
    if (membership) {
        referenced.insert(*dynamic_key);
    }

    auto predicates_res = decode_whole_columns(referenced, exec);
    if (!predicates_res) {
        return std::unexpected(predicates_res.error());
    }
    const Table& predicates = *predicates_res;

    const auto key =
        membership ? int64_key_column(predicates, *dynamic_key) : std::optional<KeyColumn>{};

    std::optional<std::vector<std::size_t>> selected;
    if (!applied.empty()) {
        auto from_conjuncts = filter_selection(predicates, applied, exec, scalars);
        if (!from_conjuncts) {
            return std::unexpected(from_conjuncts.error());
        }
        selected = std::move(*from_conjuncts);
        if (key.has_value()) {
            apply_membership_filter(*key, *dynamic, *selected, exec);
        }
    } else if (membership && key.has_value()) {
        // nullopt here is the escape hatch (the filter barely rejects) or a
        // key that is missing/non-int64; either way membership contributes
        // nothing and `selected` stays empty.
        selected = membership_selection(*key, *dynamic, rows_, exec);
    }

    if (fused_selection.has_value()) {
        if (!selected.has_value()) {
            selected = std::move(*fused_selection);
        } else {
            // Both are ascending source-row indices, so ANDing them is a merge.
            Selection both;
            both.reserve(std::min(selected->size(), fused_selection->size()));
            std::ranges::set_intersection(*selected, *fused_selection, std::back_inserter(both));
            selected = std::move(both);
        }
    }
    if (!selected.has_value()) {
        return project(names, exec);  // nothing left to filter by
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
        auto decoded = decode_columns(remaining, all_rows ? nullptr : &*selected, nullptr, exec);
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
                gather_column(*entry.column, selected->data(), selected->size(), &exec));
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

auto LazyTable::project_unit(const std::set<std::string>& names, const SourceUnit& unit,
                             const ExecutionContext& exec) -> std::expected<Table, std::string> {
    std::vector<std::string> missing;
    for (const auto& field : schema_.columns) {
        if (names.contains(field.name) && !cache_.contains(field.name)) {
            missing.push_back(field.name);
        }
    }

    Table decoded;
    if (!missing.empty()) {
        auto res = decode_columns(missing, nullptr, &unit, exec);
        if (!res) {
            return std::unexpected(res.error());
        }
        decoded = std::move(*res);
        if (decoded.rows() != unit.rows) {
            return std::unexpected("lazy source produced a unit with the wrong row count");
        }
    }

    // A column another query already decoded whole is sliced rather than
    // re-read: the pages are gone from the equation, and the copy is exactly
    // this unit's rows.
    std::vector<std::size_t> unit_rows;
    const auto slice_of = [&](const ColumnEntry& entry) -> ColumnEntry {
        if (unit_rows.empty() && unit.rows != 0) {
            unit_rows.resize(unit.rows);
            for (std::size_t row = 0; row < unit.rows; ++row) {
                unit_rows[row] = unit.start + row;
            }
        }
        ColumnEntry sliced;
        sliced.name = entry.name;
        sliced.column = std::make_shared<ColumnValue>(
            gather_column(*entry.column, unit_rows.data(), unit_rows.size(), &exec));
        if (entry.validity.has_value()) {
            ValidityBitmap bits(unit.rows, true);
            for (std::size_t row = 0; row < unit.rows; ++row) {
                bits.set(row, (*entry.validity)[unit.start + row]);
            }
            sliced.validity = std::move(bits);
        }
        return sliced;
    };

    Table out;
    for (const auto& field : schema_.columns) {
        if (!names.contains(field.name)) {
            continue;
        }
        if (const auto cached = cache_.find(field.name); cached != cache_.end()) {
            auto sliced = slice_of(cached->second);
            out.add_column_shared(sliced.name, std::move(sliced.column),
                                  std::move(sliced.validity));
            continue;
        }
        const auto* entry = decoded.find_entry(field.name);
        if (entry == nullptr) {
            return std::unexpected("lazy source did not produce requested column '" + field.name +
                                   "'");
        }
        out.add_column_from(entry->name, *entry);
    }
    out.logical_rows = unit.rows;
    return out;
}

auto LazyTable::decode_unit_predicate_columns(
    const robin_hood::unordered_set<std::string>& referenced, const SourceUnit& unit,
    const ExecutionContext& exec) -> std::expected<Table, std::string> {
    // Deliberately NOT `decode_whole_columns`. That one caches what it decodes,
    // which is correct for a whole-file predicate column and wrong here: this
    // holds one unit's rows, and a fragment sitting in `cache_` would be
    // indistinguishable from a whole column to every later reader of it.
    std::set<std::string> names;
    for (const auto& name : referenced) {
        names.insert(name);
    }
    return project_unit(names, unit, exec);
}

auto LazyTable::project_where_unit(const std::set<std::string>& names,
                                   const std::vector<ir::Expr>& conjuncts, const SourceUnit& unit,
                                   const ExecutionContext& exec, const ScalarRegistry* scalars,
                                   const DynamicScanFilter* dynamic, const std::string* dynamic_key)
    -> std::expected<Table, std::string> {
    const bool membership =
        dynamic != nullptr && dynamic_key != nullptr && dynamic->has_membership();
    if (conjuncts.empty() && !membership) {
        return project_unit(names, unit, exec);
    }

    if (reader_factory_ && !conjuncts.empty()) {
        if (auto range = static_range_filter(conjuncts);
            range.has_value() && !names.contains(range->first) && !cache_.contains(range->first)) {
            auto scan = scan_key_filter(range->first, range->second, &unit, exec);
            if (!scan)
                return std::unexpected(scan.error());
            if (scan->has_value()) {
                const Selection& selected = **scan;
                const bool all_rows = selected.size() == unit.rows;
                std::vector<std::string> wanted;
                for (const auto& field : schema_.columns)
                    if (names.contains(field.name))
                        wanted.push_back(field.name);
                auto decoded = decode_columns(wanted, all_rows ? nullptr : &selected, &unit, exec);
                if (!decoded)
                    return std::unexpected(decoded.error());
                return std::move(*decoded);
            }
        }
    }

    // Everything below mirrors `project_where` step for step, with two
    // differences and no third: decodes carry `&unit`, and the selections the
    // source hands back (which are source-global) are rebased to unit-local
    // indices so the in-memory filtering and gathering code is untouched.
    const auto to_local = [&](Selection& selection) {
        if (unit.start == 0) {
            return;
        }
        for (auto& row : selection) {
            row -= unit.start;
        }
    };

    // Fused path: the source evaluates the key filter inside its own decoder,
    // for this unit's row groups only.
    if (membership && conjuncts.empty() && (key_filter_scan_ != nullptr || reader_factory_) &&
        !cache_.contains(*dynamic_key)) {
        auto scan = scan_key_filter(*dynamic_key, *dynamic, &unit, exec);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        if (scan->has_value()) {
            const Selection selected = std::move(**scan);  // source-global
            const bool all_rows = selected.size() == unit.rows;
            std::vector<std::string> wanted;
            for (const auto& field : schema_.columns) {
                if (names.contains(field.name)) {
                    wanted.push_back(field.name);
                }
            }
            if (!wanted.empty()) {
                auto decoded = decode_columns(wanted, all_rows ? nullptr : &selected, &unit, exec);
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
                    out.add_column_from(entry->name, *entry);
                }
                out.logical_rows = selected.size();
                return out;
            }
        }
    }

    std::vector<FusedStringConjunct> fused;
    std::vector<ir::Expr> unfused;
    std::optional<Selection> fused_selection;
    if (fusable_string_conjuncts(conjuncts, names, fused, unfused)) {
        auto scan = scan_string_filters(fused, &unit, exec);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        fused_selection = std::move(*scan);
        if (fused_selection.has_value()) {
            to_local(*fused_selection);
        }
    }
    const std::vector<ir::Expr>& applied = fused_selection.has_value() ? unfused : conjuncts;

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : applied) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }
    if (membership) {
        referenced.insert(*dynamic_key);
    }

    auto predicates_res = decode_unit_predicate_columns(referenced, unit, exec);
    if (!predicates_res) {
        return std::unexpected(predicates_res.error());
    }
    const Table& predicates = *predicates_res;

    const auto key =
        membership ? int64_key_column(predicates, *dynamic_key) : std::optional<KeyColumn>{};

    std::optional<Selection> selected;
    if (!applied.empty()) {
        auto from_conjuncts = filter_selection(predicates, applied, exec, scalars);
        if (!from_conjuncts) {
            return std::unexpected(from_conjuncts.error());
        }
        selected = std::move(*from_conjuncts);
        if (key.has_value()) {
            apply_membership_filter(*key, *dynamic, *selected, exec);
        }
    } else if (membership && key.has_value()) {
        selected = membership_selection(*key, *dynamic, unit.rows, exec);
    }

    if (fused_selection.has_value()) {
        if (!selected.has_value()) {
            selected = std::move(*fused_selection);
        } else {
            Selection both;
            both.reserve(std::min(selected->size(), fused_selection->size()));
            std::ranges::set_intersection(*selected, *fused_selection, std::back_inserter(both));
            selected = std::move(both);
        }
    }
    if (!selected.has_value()) {
        return project_unit(names, unit, exec);  // nothing left to filter by
    }
    const bool all_rows = selected->size() == unit.rows;

    std::vector<std::string> remaining;
    for (const auto& field : schema_.columns) {
        if (names.contains(field.name) && !referenced.contains(field.name)) {
            remaining.push_back(field.name);
        }
    }

    Table decoded_remaining;
    if (!remaining.empty()) {
        // The source indexes rows file-globally, so hand it the selection
        // rebased back out of unit-local space.
        Selection global;
        if (!all_rows && unit.start != 0) {
            global.reserve(selected->size());
            for (const auto row : *selected) {
                global.push_back(row + unit.start);
            }
        }
        const Selection* pass = all_rows ? nullptr : (unit.start == 0 ? &*selected : &global);
        auto decoded = decode_columns(remaining, pass, &unit, exec);
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
                gather_column(*entry.column, selected->data(), selected->size(), &exec));
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

auto LazyTable::decode_whole_columns(const robin_hood::unordered_set<std::string>& referenced,
                                     const ExecutionContext& exec)
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
        auto decoded = decode_columns(missing, nullptr, nullptr, exec);
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
        out.add_column_from(entry.name, entry);
    }
    out.logical_rows = rows_;
    if (!out.columns.empty() && out.rows() != rows_) {
        return std::unexpected("lazy source produced predicate columns with the wrong row count");
    }
    return out;
}

auto LazyTable::project_rows(const std::set<std::string>& names, const Selection& selected,
                             const ExecutionContext& exec) -> std::expected<Table, std::string> {
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
        auto res = decode_columns(missing, all_rows ? nullptr : &selected, nullptr, exec);
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
                out.add_column_from(entry.name, entry);
            } else {
                auto column = std::make_shared<ColumnValue>(
                    gather_column(*entry.column, selected.data(), selected.size(), &exec));
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
        out.add_column_from(entry->name, *entry);
    }
    out.logical_rows = selected.size();
    return out;
}

/// The columns `conjuncts` read from this source, when every one of them can be
/// staged through a selection cheaply -- nullopt when any cannot, and the
/// caller must not take the staged path at all.
///
/// The test is the column's WIDTH, not its selectivity. Reading a variable-width
/// column through a sparse selection is not proportionally cheaper: the reader
/// still walks the pages the selection skips through, and a dictionary-encoded
/// string still builds its dictionary per row group. So a staged read of one
/// costs about what the whole-file read costs, and the membership scan that
/// bought the selection goes unrepaid. q10 (`l_returnflag == "R"`) is exactly
/// that shape and measured +9.7% before this declined it.
///
/// Checked BEFORE the fused key scan runs, never after: deciding late would
/// mean paying for the scan and then falling back to the path that does not use
/// its answer.
auto LazyTable::stageable_conjunct_columns(const std::vector<ir::Expr>& conjuncts) const
    -> std::optional<std::set<std::string>> {
    robin_hood::unordered_set<std::string> named;
    for (const auto& conjunct : conjuncts) {
        ir::collect_expr_column_refs(conjunct, named);
    }
    std::set<std::string> names;
    for (const auto& field : schema_.columns) {
        if (!named.contains(field.name)) {
            continue;
        }
        if (std::holds_alternative<Column<std::string>>(*field.column) ||
            std::holds_alternative<Column<Categorical>>(*field.column)) {
            return std::nullopt;
        }
        names.insert(field.name);
    }
    if (names.empty()) {
        return std::nullopt;
    }
    return names;
}

/// AND `conjuncts` into a selection the fused key scan already produced,
/// decoding their columns THROUGH it rather than whole-file.
///
/// Late materialization applied to the predicate columns themselves: the
/// selection handed in came from a scan that abandons itself when it stops
/// rejecting, so it is already small, and reading a predicate column for those
/// rows beats reading it for the whole file to throw most of it away.
///
/// `filter_selection` answers in the staged table's own row numbers, so the
/// survivors are mapped back through `selected`. Both are ascending and the map
/// is monotone, so the result is ascending too -- which every consumer of a
/// Selection requires.
///
/// nullopt = the conjuncts reference no column of this source, which this shape
/// cannot stage; the caller keeps its whole-column path.
auto LazyTable::narrow_selection(Selection selected, const std::vector<ir::Expr>& conjuncts,
                                 const ExecutionContext& exec, const ScalarRegistry* scalars)
    -> std::expected<std::optional<Selection>, std::string> {
    const auto names = stageable_conjunct_columns(conjuncts);
    if (!names.has_value()) {
        return std::optional<Selection>{};
    }
    if (selected.empty()) {
        return std::optional{std::move(selected)};
    }

    auto stage = project_rows(*names, selected, exec);
    if (!stage) {
        return std::unexpected(stage.error());
    }
    auto local = filter_selection(*stage, conjuncts, exec, scalars);
    if (!local) {
        return std::unexpected(local.error());
    }
    Selection out;
    out.reserve(local->size());
    for (const std::size_t row : *local) {
        out.push_back(selected[row]);
    }
    return std::optional{std::move(out)};
}

auto LazyTable::join_key_selection(const std::vector<ir::Expr>& conjuncts,
                                   const ExecutionContext& exec, const ScalarRegistry* scalars,
                                   const DynamicScanFilter& dynamic, const std::string& key_name)
    -> std::expected<std::optional<JoinKeySelection>, std::string> {
    if (!dynamic.has_membership()) {
        return std::optional<JoinKeySelection>{};
    }

    // Fused path: the source evaluates the key filter inside its own decoder,
    // one row group per worker, and the key column is never materialized
    // whole-file. Only the surviving key values are decoded at all.
    //
    // Static conjuncts do NOT disqualify this. They used to -- the condition
    // read `conjuncts.empty()`, copied from `project_where`, where a conjunct
    // really does need a different shape -- and the cost of that copy was paid
    // by exactly the queries whose probe scan is most selective. q03
    // (`l_shipdate > ...`) and q10 (`l_returnflag == "R"`) fell to the path
    // below, which decodes the whole 12M-row key column AND the whole predicate
    // column and then walks all 12M rows twice, serially, to reject most of
    // them. Here the fused scan answers membership first and the conjuncts are
    // evaluated through its selection, so every later step is sized by what
    // survived rather than by the file.
    if ((key_filter_scan_ != nullptr || reader_factory_) && !cache_.contains(key_name) &&
        (conjuncts.empty() || stageable_conjunct_columns(conjuncts).has_value())) {
        auto scan = scan_key_filter(key_name, dynamic, nullptr, exec);
        if (!scan) {
            return std::unexpected(scan.error());
        }
        if (scan->has_value()) {
            Selection selected = std::move(**scan);
            bool narrowed = true;
            if (!conjuncts.empty()) {
                auto rest = narrow_selection(std::move(selected), conjuncts, exec, scalars);
                if (!rest) {
                    return std::unexpected(rest.error());
                }
                if (rest->has_value()) {
                    selected = std::move(**rest);
                } else {
                    narrowed = false;  // conjuncts not evaluable here
                }
            }
            if (narrowed) {
                JoinKeySelection out;
                out.selected = std::move(selected);
                auto keys = project_rows({key_name}, out.selected, exec);
                if (!keys) {
                    return std::unexpected(keys.error());
                }
                auto* entry = keys->find_entry(key_name);
                if (entry == nullptr ||
                    !std::holds_alternative<Column<std::int64_t>>(*entry->column)) {
                    return std::optional<JoinKeySelection>{};
                }
                out.keys = std::move(*entry);
                return std::optional{std::move(out)};
            }
        }
        // No fused answer (unsupported key type, or the filter stopped
        // rejecting and the scan abandoned): the whole-column path stands.
    }

    robin_hood::unordered_set<std::string> referenced;
    for (const auto& conjunct : conjuncts) {
        ir::collect_expr_column_refs(conjunct, referenced);
    }
    referenced.insert(key_name);

    auto predicates_res = decode_whole_columns(referenced, exec);
    if (!predicates_res) {
        return std::unexpected(predicates_res.error());
    }
    const Table& predicates = *predicates_res;

    const auto key = int64_key_column(predicates, key_name);
    if (!key.has_value()) {
        return std::optional<JoinKeySelection>{};
    }

    JoinKeySelection out;
    if (!conjuncts.empty()) {
        auto selected = filter_selection(predicates, conjuncts, exec, scalars);
        if (!selected) {
            return std::unexpected(selected.error());
        }
        apply_membership_filter(*key, dynamic, *selected, exec);
        out.selected = std::move(*selected);
    } else {
        auto from_membership = membership_selection(*key, dynamic, rows_, exec);
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
            gather_column(*entry->column, out.selected.data(), out.selected.size(), &exec));
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

auto LazyTable::materialize(const ExecutionContext& exec) -> std::expected<Table, std::string> {
    std::set<std::string> names;
    for (const auto& entry : schema_.columns) {
        names.insert(entry.name);
    }
    return project(names, exec);
}

}  // namespace ibex::runtime
