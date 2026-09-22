// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// decimal_aggregate.cpp — grouped aggregation when an input column is Decimal.
//
// The streaming aggregate keeps fixed-width numeric slots; a Decimal sum needs
// an int128 accumulator whose every add is checked against 38 digits. Rather
// than thread that through every fast path, a query with a Decimal aggregate
// input runs here: group ids once (the shared Key/KeyRowIndex machinery, which
// already groups Decimal keys), Decimal accumulators here, and every other
// aggregate in the same query delegated to `aggregate_table` keyed on those
// group ids -- so a non-Decimal aggregate keeps exactly its usual semantics.
// Semantics: plans/decimal-plan.md.

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "decimal_ops.hpp"
#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

constexpr const char* kGidColumn = "__ibex_decimal_gid";

auto agg_name(ir::AggFunc func) -> std::string_view {
    switch (func) {
        case ir::AggFunc::Sum:
            return "sum";
        case ir::AggFunc::Mean:
            return "mean";
        case ir::AggFunc::Min:
            return "min";
        case ir::AggFunc::Max:
            return "max";
        case ir::AggFunc::Count:
            return "count";
        case ir::AggFunc::CountDistinct:
            return "count_distinct";
        case ir::AggFunc::First:
            return "first";
        case ir::AggFunc::Last:
            return "last";
        case ir::AggFunc::Median:
            return "median";
        case ir::AggFunc::Stddev:
            return "std";
        case ir::AggFunc::Ewma:
            return "ewma";
        case ir::AggFunc::Quantile:
            return "quantile";
        case ir::AggFunc::Skew:
            return "skew";
        case ir::AggFunc::Kurtosis:
            return "kurtosis";
    }
    return "aggregate";
}

/// Group id per row and the first row of each group, in first-occurrence
/// order. With no group keys every row is group 0 and there is exactly one
/// group, even over an empty input (an ungrouped aggregate always has a row).
struct Grouping {
    std::vector<std::uint32_t> gids;
    std::vector<std::uint32_t> first_row;
    std::size_t groups = 0;
};

auto box_key_value(const ColumnValue& column, std::size_t row) -> ScalarValue {
    return std::visit(
        [row](const auto& col) -> ScalarValue {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<std::string>> ||
                          std::is_same_v<ColT, Column<Categorical>>) {
                return std::string(col[row]);
            } else if constexpr (std::is_same_v<ColT, Column<Decimal>>) {
                return DecimalValue{.units = col[row].units, .type = decimal_type_of(col)};
            } else {
                return ScalarValue{col[row]};
            }
        },
        column);
}

auto group_rows(const Table& input, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Grouping, std::string> {
    const std::size_t rows = input.rows();
    Grouping out;
    out.gids.assign(rows, 0U);
    if (group_by.empty()) {
        out.groups = 1;
        return out;
    }
    if (group_by.size() > kMaxKeyColumns) {
        return std::unexpected("group-by supports at most 64 key columns");
    }
    std::vector<KeyCol> cols;
    std::vector<const ColumnEntry*> entries;
    cols.reserve(group_by.size());
    entries.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("group-by column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        auto key_col = make_key_col(*entry);
        if (!key_col.has_value()) {
            return std::unexpected("unsupported group-by column type: " + key.name);
        }
        cols.push_back(*key_col);
        entries.push_back(entry);
    }
    std::vector<Key> keys;
    KeyRowIndex index;
    for (std::size_t row = 0; row < rows; ++row) {
        out.gids[row] = index.find_or_insert(keys, cols, row, [&]() -> std::uint32_t {
            Key key;
            key.values.reserve(cols.size());
            for (std::size_t i = 0; i < cols.size(); ++i) {
                if (cols[i].is_null(row)) {
                    key.values.emplace_back();
                    key.set_null(i);
                } else {
                    key.values.push_back(box_key_value(*entries[i]->column, row));
                }
            }
            keys.push_back(std::move(key));
            out.first_row.push_back(static_cast<std::uint32_t>(row));
            return static_cast<std::uint32_t>(keys.size() - 1);
        });
    }
    out.groups = keys.size();
    return out;
}

/// A Decimal result column from per-group units, null where `seen` is 0.
auto decimal_result(DecimalType type, const std::vector<Int128>& values,
                    const std::vector<std::uint8_t>& seen) -> ComputedColumn {
    Column<Decimal> col = make_decimal_column(type);
    col.resize(values.size());
    Decimal* out = col.data();
    bool any_null = false;
    for (std::size_t g = 0; g < values.size(); ++g) {
        out[g] = Decimal{seen[g] != 0 ? values[g] : Int128{0}};
        any_null = any_null || seen[g] == 0;
    }
    ComputedColumn result{.column = ColumnValue{std::move(col)}, .validity = std::nullopt};
    if (any_null) {
        ValidityBitmap validity(values.size(), true);
        for (std::size_t g = 0; g < values.size(); ++g) {
            if (seen[g] == 0) {
                validity.set(g, false);
            }
        }
        result.validity = std::move(validity);
    }
    return result;
}

struct GidUnits {
    std::uint32_t gid = 0;
    Int128 units = 0;
};
struct GidUnitsHash {
    auto operator()(const GidUnits& k) const noexcept -> std::size_t {
        return static_cast<std::size_t>(
            decimal::hash_units(k.units) ^
            (static_cast<std::uint64_t>(k.gid) * 0x9E3779B97F4A7C15ULL));
    }
};
struct GidUnitsEq {
    auto operator()(const GidUnits& a, const GidUnits& b) const noexcept -> bool {
        return a.gid == b.gid && a.units == b.units;
    }
};

auto aggregate_decimal_column(const ir::AggSpec& agg, const ColumnEntry& entry,
                              const Grouping& grouping)
    -> std::expected<ComputedColumn, std::string> {
    const auto& col = std::get<Column<Decimal>>(*entry.column);
    const DecimalType in_type = decimal_type_of(col);
    const ValidityBitmap* valid = entry.validity.has_value() ? &*entry.validity : nullptr;
    const Decimal* data = col.data();
    const std::size_t rows = col.size();
    const std::size_t groups = grouping.groups;
    const auto* gids = grouping.gids.data();
    const auto present = [valid](std::size_t row) { return valid == nullptr || (*valid)[row]; };

    switch (agg.func) {
        case ir::AggFunc::Sum:
        case ir::AggFunc::Mean: {
            // The sum is exact; its type is Decimal(38, s), so the only
            // overflow is past 38 digits -- an error, never a wrap.
            std::vector<Int128> sums(groups, 0);
            std::vector<std::int64_t> counts(groups, 0);
            constexpr std::size_t kMinRowsPerWorker = 65536;
            const bool parallel = rows >= kMinRowsPerWorker && !on_worker_pool_thread();
            WorkerPool* pool = parallel ? &process_worker_pool() : nullptr;
            const std::size_t workers =
                parallel ? std::min(pool->size(), rows / kMinRowsPerWorker) : 1;
            // A private group array per worker avoids atomics in the row loop.
            // Bound the scratch size; very high-cardinality inputs keep the
            // serial path rather than multiplying an already large state table.
            const bool use_parallel = workers > 1 && groups <= (1U << 20) / workers;
            if (use_parallel) {
                std::vector<std::vector<Int128>> partial_sums(workers,
                                                              std::vector<Int128>(groups, 0));
                std::vector<std::vector<std::int64_t>> partial_counts(
                    workers, std::vector<std::int64_t>(groups, 0));
                std::vector<std::uint8_t> overflow(workers, 0);
                auto batch = pool->submit(workers, [&](std::size_t worker) {
                    const std::size_t begin = rows * worker / workers;
                    const std::size_t end = rows * (worker + 1) / workers;
                    auto& local_sums = partial_sums[worker];
                    auto& local_counts = partial_counts[worker];
                    for (std::size_t row = begin; row < end; ++row) {
                        if (!present(row)) {
                            continue;
                        }
                        const std::uint32_t g = gids[row];
                        if (!decimal::checked_add(local_sums[g], data[row].units, local_sums[g])) {
                            overflow[worker] = 1;
                            return;
                        }
                        ++local_counts[g];
                    }
                });
                batch.wait();
                for (std::size_t worker = 0; worker < workers; ++worker) {
                    if (overflow[worker] != 0) {
                        return std::unexpected(
                            std::string(agg_name(agg.func)) + "(" + agg.column.name +
                            "): " + decimal_overflow(decimal::sum_result_type(in_type)));
                    }
                }
                for (std::size_t worker = 0; worker < workers; ++worker) {
                    for (std::size_t g = 0; g < groups; ++g) {
                        if (!decimal::checked_add(sums[g], partial_sums[worker][g], sums[g])) {
                            return std::unexpected(
                                std::string(agg_name(agg.func)) + "(" + agg.column.name +
                                "): " + decimal_overflow(decimal::sum_result_type(in_type)));
                        }
                        counts[g] += partial_counts[worker][g];
                    }
                }
            } else {
                for (std::size_t row = 0; row < rows; ++row) {
                    if (!present(row)) {
                        continue;
                    }
                    const std::uint32_t g = gids[row];
                    if (!decimal::checked_add(sums[g], data[row].units, sums[g])) {
                        return std::unexpected(
                            std::string(agg_name(agg.func)) + "(" + agg.column.name +
                            "): " + decimal_overflow(decimal::sum_result_type(in_type)));
                    }
                    ++counts[g];
                }
            }
            std::vector<std::uint8_t> seen(groups, 0);
            for (std::size_t g = 0; g < groups; ++g) {
                seen[g] = counts[g] > 0 ? 1 : 0;
            }
            if (agg.func == ir::AggFunc::Sum) {
                return decimal_result(decimal::sum_result_type(in_type), sums, seen);
            }
            // Mean: the quotient of the exact sum, formed in decimal and only
            // then converted, so 0.10, 0.20, 0.30 average to exactly 0.2.
            Column<double> out;
            out.resize(groups);
            bool any_null = false;
            for (std::size_t g = 0; g < groups; ++g) {
                out.data()[g] = counts[g] > 0
                                    ? decimal::divide_to_double(sums[g], in_type.scale, counts[g])
                                    : 0.0;
                any_null = any_null || counts[g] == 0;
            }
            ComputedColumn result{.column = ColumnValue{std::move(out)}, .validity = std::nullopt};
            if (any_null) {
                ValidityBitmap validity(groups, true);
                for (std::size_t g = 0; g < groups; ++g) {
                    if (counts[g] == 0) {
                        validity.set(g, false);
                    }
                }
                result.validity = std::move(validity);
            }
            return result;
        }
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::First:
        case ir::AggFunc::Last: {
            std::vector<Int128> values(groups, 0);
            std::vector<std::uint8_t> seen(groups, 0);
            for (std::size_t row = 0; row < rows; ++row) {
                if (!present(row)) {
                    continue;
                }
                const std::uint32_t g = gids[row];
                const Int128 v = data[row].units;
                bool take = seen[g] == 0;
                if (!take) {
                    switch (agg.func) {
                        case ir::AggFunc::Min:
                            take = v < values[g];
                            break;
                        case ir::AggFunc::Max:
                            take = v > values[g];
                            break;
                        case ir::AggFunc::Last:
                            take = true;
                            break;
                        default:
                            break;  // First keeps the first non-null value
                    }
                }
                if (take) {
                    values[g] = v;
                    seen[g] = 1;
                }
            }
            return decimal_result(in_type, values, seen);
        }
        case ir::AggFunc::CountDistinct: {
            robin_hood::unordered_flat_set<GidUnits, GidUnitsHash, GidUnitsEq> distinct;
            for (std::size_t row = 0; row < rows; ++row) {
                if (present(row)) {
                    distinct.insert(GidUnits{.gid = gids[row], .units = data[row].units});
                }
            }
            Column<std::int64_t> out;
            out.resize(groups, 0);
            for (const auto& pair : distinct) {
                ++out.data()[pair.gid];
            }
            return ComputedColumn{.column = ColumnValue{std::move(out)}, .validity = std::nullopt};
        }
        default:
            return std::unexpected(std::string(agg_name(agg.func)) +
                                   "() is not supported for Decimal column '" + agg.column.name +
                                   "'; convert with Float64(x) first");
    }
}

}  // namespace

auto aggregate_table_decimal(const Table& input, const std::vector<ir::ColumnRef>& group_by,
                             const std::vector<ir::AggSpec>& aggregations,
                             const ExecutionContext* exec) -> std::expected<Table, std::string> {
    auto grouping = group_rows(input, group_by);
    if (!grouping) {
        return std::unexpected(grouping.error());
    }
    const std::size_t groups = grouping->groups;

    std::vector<std::optional<ComputedColumn>> outputs(aggregations.size());
    std::vector<ir::AggSpec> delegated;
    std::vector<std::size_t> delegated_at;
    for (std::size_t i = 0; i < aggregations.size(); ++i) {
        const auto& agg = aggregations[i];
        if (agg.func == ir::AggFunc::Count) {
            Column<std::int64_t> counts;
            counts.resize(groups, 0);
            for (const std::uint32_t g : grouping->gids) {
                ++counts.data()[g];
            }
            outputs[i] =
                ComputedColumn{.column = ColumnValue{std::move(counts)}, .validity = std::nullopt};
            continue;
        }
        const auto* entry = input.find_entry(agg.column.name);
        if (entry == nullptr) {
            return std::unexpected("aggregate column not found: " + agg.column.name +
                                   " (available: " + format_columns(input) + ")");
        }
        if (!std::holds_alternative<Column<Decimal>>(*entry->column)) {
            delegated.push_back(agg);
            delegated_at.push_back(i);
            continue;
        }
        auto computed = aggregate_decimal_column(agg, *entry, *grouping);
        if (!computed) {
            return std::unexpected(computed.error());
        }
        outputs[i] = std::move(*computed);
    }

    if (!delegated.empty()) {
        // Same groups, same order: aggregate the rest keyed on our group ids,
        // then line its rows up with ours.
        std::expected<Table, std::string> rest = std::unexpected("");
        std::vector<std::uint32_t> order(groups, 0);
        if (group_by.empty()) {
            rest = aggregate_table(input, group_by, delegated, exec);
        } else {
            Table working = input;
            Column<std::int64_t> gid_col;
            gid_col.resize(grouping->gids.size());
            for (std::size_t r = 0; r < grouping->gids.size(); ++r) {
                gid_col.data()[r] = static_cast<std::int64_t>(grouping->gids[r]);
            }
            working.add_column(kGidColumn, std::move(gid_col));
            rest = aggregate_table(working, {ir::ColumnRef{.name = kGidColumn}}, delegated, exec);
            if (rest) {
                const auto* gid_out = std::get_if<Column<std::int64_t>>(rest->find(kGidColumn));
                if (gid_out == nullptr || gid_out->size() != groups) {
                    return std::unexpected("internal: decimal aggregate lost its group ids");
                }
                for (std::size_t row = 0; row < groups; ++row) {
                    order[static_cast<std::size_t>((*gid_out)[row])] =
                        static_cast<std::uint32_t>(row);
                }
            }
        }
        if (!rest) {
            return std::unexpected(rest.error());
        }
        if (rest->rows() != groups) {
            return std::unexpected("internal: decimal aggregate group count mismatch");
        }
        Table picked;
        for (const auto& agg : delegated) {
            const auto* entry = rest->find_entry(agg.alias);
            if (entry == nullptr) {
                return std::unexpected("internal: delegated aggregate '" + agg.alias + "' missing");
            }
            picked.add_column_shared(agg.alias, entry->column, entry->validity);
        }
        const Table aligned = gather_rows(picked, order);
        for (std::size_t k = 0; k < delegated.size(); ++k) {
            const auto& entry = aligned.columns[k];
            outputs[delegated_at[k]] =
                ComputedColumn{.column = *entry.column, .validity = entry.validity};
        }
    }

    Table out;
    if (!group_by.empty()) {
        Table keys;
        for (const auto& key : group_by) {
            const auto* entry = input.find_entry(key.name);
            keys.add_column_shared(key.name, entry->column, entry->validity);
        }
        const Table key_rows = gather_rows(keys, grouping->first_row);
        for (const auto& entry : key_rows.columns) {
            out.add_column_shared(entry.name, entry.column, entry.validity);
        }
    }
    for (std::size_t i = 0; i < aggregations.size(); ++i) {
        auto& slot = outputs[i];
        if (!slot.has_value()) {
            return std::unexpected("internal: decimal aggregate '" + aggregations[i].alias +
                                   "' produced no column");
        }
        auto& computed = *slot;
        if (computed.validity.has_value()) {
            out.add_column(aggregations[i].alias, std::move(computed.column),
                           std::move(*computed.validity));
        } else {
            out.add_column(aggregations[i].alias, std::move(computed.column));
        }
    }
    return out;
}

}  // namespace ibex::runtime
