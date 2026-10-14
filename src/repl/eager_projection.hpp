// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/required_columns.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace ibex::repl::detail {

/// The in-memory tables in `demand` that the plan reads only part of, each cut
/// down to its demanded columns by `runtime::narrow_to_columns`. The lazy
/// sources get the same narrowing from `decode_demanded_lazy_sources`; both
/// REPL drivers call this for their in-memory tables.
[[nodiscard]] auto narrow_demanded_tables(const std::map<std::string, ir::ColumnDemand>& demand,
                                          const runtime::TableRegistry& tables)
    -> std::vector<std::pair<std::string, runtime::Table>>;

}  // namespace ibex::repl::detail
