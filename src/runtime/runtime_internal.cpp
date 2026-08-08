// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "runtime_internal.hpp"

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace ibex::runtime {

auto is_simple_identifier(std::string_view name) -> bool {
    if (name.empty()) {
        return false;
    }
    auto is_alpha = [](unsigned char ch) -> bool { return std::isalpha(ch) != 0; };
    auto is_alnum = [](unsigned char ch) -> bool { return std::isalnum(ch) != 0; };
    auto first = static_cast<unsigned char>(name.front());
    if (!is_alpha(first) && first != '_') {
        return false;
    }
    for (std::size_t i = 1; i < name.size(); ++i) {
        auto ch = static_cast<unsigned char>(name[i]);
        if (!is_alnum(ch) && ch != '_') {
            return false;
        }
    }
    return true;
}

auto format_columns(const Table& table) -> std::string {
    if (table.columns.empty()) {
        return "<none>";
    }
    std::string out;
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        if (i > 0) {
            out.append(", ");
        }
        const auto& name = table.columns[i].name;
        if (is_simple_identifier(name)) {
            out.append(name);
        } else {
            out.push_back('`');
            out.append(name);
            out.push_back('`');
        }
    }
    return out;
}

auto normalize_time_index(Table& table) -> void {
    // The rule itself lives in `TableProperties::normalized()`, which
    // `set_properties` applies. Re-seating the table's own properties is
    // therefore all this needs to do -- and the grouping is necessarily already
    // known, so the old hazard of normalizing before it was set is gone.
    table.set_properties(table.properties());
}

auto TableProperties::derive(const TableProperties& input, const KeyColumnFate& fate,
                             RowTransform transform) -> TableProperties {
    TableProperties out;
    if (transform == RowTransform::Recombine) {
        // Rows built from groups or several inputs: no claim about the input's
        // layout carries over. The operator states what it establishes itself.
        return out;
    }
    // Time index first: whether it survives gates the ordering rule below. An
    // overwritten time index counts as lost -- a column whose timestamps were
    // rewritten no longer indexes anything, even though it is still there.
    bool time_index_lost = false;
    if (input.time_index().has_value()) {
        if (const KeyFate result = fate(*input.time_index()); result.is_kept()) {
            out.time_index_ = result.name();
        } else {
            time_index_lost = true;
        }
    }
    // A reorder voids the input's ordering outright: the columns are all still
    // there, so `fate` would happily map every key, but the rows they described
    // have moved. The operator sets the new ordering after this returns.
    if (input.ordering().has_value() && transform != RowTransform::Reorder) {
        std::vector<ir::OrderKey> kept;
        kept.reserve(input.ordering()->size());
        bool preserved = !time_index_lost;  // lose the time index -> lose ordering
        if (preserved) {
            for (const auto& key : *input.ordering()) {
                // Dropped and overwritten are the same to an ordering: a key
                // whose values were rewritten no longer describes the row order
                // any more than a key that is gone.
                const KeyFate result = fate(key.name);
                if (!result.is_kept()) {
                    preserved = false;
                    break;
                }
                kept.push_back({.name = result.name(), .ascending = key.ascending});
            }
        }
        if (preserved) {
            out.ordering_ = std::move(kept);
        }
    }
    // The grouping outlives anything row-preserving or row-removing: dropping
    // rows cannot merge two partitions.
    //
    // Here is where `Overwritten` parts company with `Dropped`. A DROPPED key
    // cannot be named, so the claim goes with it rather than naming a column
    // that is not there. An OVERWRITTEN key is still a column, and the rows have
    // not moved -- they sit in the same runs, so the boundary an unpartitioned
    // lag/rolling would read across is still there. Voiding the claim would
    // disarm `check_row_order` over a live hazard, so the claim stands under the
    // old name. The name is then a slightly stale label on a real hazard, which
    // is the better of the two failures.
    if (!input.grouped_by().empty()) {
        std::vector<std::string> kept;
        kept.reserve(input.grouped_by().size());
        for (const auto& key : input.grouped_by()) {
            const KeyFate result = fate(key);
            if (result.is_overwritten()) {
                kept.push_back(key);
                continue;
            }
            if (result.is_dropped()) {
                kept.clear();
                break;
            }
            kept.push_back(result.name());
        }
        out.grouped_by_ = std::move(kept);
    }
    return out;
}

auto table_properties_of(const Table& table) -> TableProperties {
    return table.properties();
}

auto apply_table_properties(Table& table, const TableProperties& props) -> void {
    table.set_properties(props);
}

auto int64_to_date_checked(std::int64_t value) -> Date {
    if (value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
        throw std::runtime_error("date out of range");
    }
    return Date{static_cast<std::int32_t>(value)};
}

auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue {
    return std::visit(
        [&](const auto& col) -> ScalarValue {
            using ColType = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColType, Column<Categorical>> ||
                          std::is_same_v<ColType, Column<std::string>>) {
                return std::string(col[row]);
            } else {
                return col[row];
            }
        },
        column);
}

auto column_kind(const ColumnValue& column) -> ExprType {
    if (std::holds_alternative<Column<std::int64_t>>(column)) {
        return ExprType::Int;
    }
    if (std::holds_alternative<Column<double>>(column)) {
        return ExprType::Double;
    }
    if (std::holds_alternative<Column<bool>>(column)) {
        return ExprType::Bool;
    }
    if (std::holds_alternative<Column<Date>>(column)) {
        return ExprType::Date;
    }
    if (std::holds_alternative<Column<Timestamp>>(column)) {
        return ExprType::Timestamp;
    }
    return ExprType::String;
}

}  // namespace ibex::runtime
