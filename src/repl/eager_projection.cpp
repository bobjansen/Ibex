// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "eager_projection.hpp"

#include <ibex/ir/required_columns.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "runtime/table_narrow.hpp"

namespace ibex::repl::detail {

auto narrow_demanded_tables(const std::map<std::string, ir::ColumnDemand>& demand,
                            const runtime::TableRegistry& tables)
    -> std::vector<std::pair<std::string, runtime::Table>> {
    std::vector<std::pair<std::string, runtime::Table>> out;
    for (const auto& [name, needed] : demand) {
        if (needed.all) {
            continue;
        }
        const auto it = tables.find(name);
        if (it == tables.end()) {
            continue;
        }
        if (auto narrowed = runtime::narrow_to_columns(it->second, needed.names)) {
            out.emplace_back(name, std::move(*narrowed));
        }
    }
    return out;
}

}  // namespace ibex::repl::detail
