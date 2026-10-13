// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "eager_projection.hpp"

#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <set>

namespace ibex::repl::detail {

namespace {
std::atomic<std::size_t> dropped_columns{0};
}  // namespace

auto eager_projection_dropped_columns() -> std::size_t {
    return dropped_columns.load(std::memory_order_relaxed);
}

auto narrow_demanded_tables(const std::map<std::string, ir::ColumnDemand>& demand,
                            const runtime::TableRegistry& tables)
    -> std::vector<std::pair<std::string, runtime::Table>> {
    static const bool disabled = std::getenv("IBEX_DISABLE_EAGER_PROJECTION") != nullptr;
    std::vector<std::pair<std::string, runtime::Table>> out;
    if (disabled) {
        return out;
    }
    for (const auto& [name, needed] : demand) {
        if (needed.all) {
            continue;
        }
        const auto it = tables.find(name);
        if (it == tables.end()) {
            continue;
        }
        const runtime::Table& table = it->second;
        std::set<std::string> keep = needed.names;
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
        // Nothing to drop. A plan reading no column at all (`x[select {n =
        // count()}]`) still needs the row count, which a column-less table
        // would lose, so it keeps the table whole too.
        if (kept == table.columns.size() || kept == 0) {
            continue;
        }
        runtime::Table narrowed;
        narrowed.columns.reserve(kept);
        for (const auto& entry : table.columns) {
            if (keep.contains(entry.name)) {
                narrowed.add_column_from(entry.name, entry);
            }
        }
        narrowed.set_properties(props);
        dropped_columns.fetch_add(table.columns.size() - kept, std::memory_order_relaxed);
        out.emplace_back(name, std::move(narrowed));
    }
    return out;
}

}  // namespace ibex::repl::detail
