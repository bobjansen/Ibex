// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "table_narrow.hpp"

#include <ibex/runtime/interpreter.hpp>

#include <atomic>
#include <cstdlib>
#include <optional>
#include <set>
#include <string>

namespace ibex::runtime {

namespace {
std::atomic<std::size_t> dropped_columns{0};
}  // namespace

auto narrowed_column_count() -> std::size_t {
    return dropped_columns.load(std::memory_order_relaxed);
}

auto narrow_to_columns(const Table& table, const std::set<std::string>& demanded)
    -> std::optional<Table> {
    static const bool disabled = std::getenv("IBEX_DISABLE_EAGER_PROJECTION") != nullptr;
    if (disabled) {
        return std::nullopt;
    }
    std::set<std::string> keep = demanded;
    const auto& props = table.properties();
    if (props.ordering().has_value()) {
        for (const auto& key : *props.ordering()) {
            keep.insert(key.name);
        }
    }
    if (props.time_index().has_value()) {
        keep.insert(*props.time_index());
    }
    keep.insert(props.grouped_by().begin(), props.grouped_by().end());
    std::size_t kept = 0;
    for (const auto& entry : table.columns) {
        kept += keep.contains(entry.name) ? 1U : 0U;
    }
    if (kept == table.columns.size() || kept == 0) {
        return std::nullopt;
    }
    Table narrowed;
    narrowed.columns.reserve(kept);
    for (const auto& entry : table.columns) {
        if (keep.contains(entry.name)) {
            narrowed.add_column_from(entry.name, entry);
        }
    }
    narrowed.set_properties(props);
    dropped_columns.fetch_add(table.columns.size() - kept, std::memory_order_relaxed);
    return narrowed;
}

}  // namespace ibex::runtime
