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
/// down to its demanded columns. The cut shares column storage, so it copies
/// nothing; what it saves is every operator above the scan carrying, and
/// gathering, columns nobody reads: `x[order {v3 desc}, head 2, by id6]` over
/// a nine-column table moved nine payload columns through the top-k heaps.
/// The lazy sources get the same narrowing from `decode_demanded_lazy_sources`.
///
/// A column the table's own properties name (its ordering, time index or
/// grouping) is kept even when undemanded, so the properties stay true of the
/// narrowed table. `IBEX_DISABLE_EAGER_PROJECTION` turns this off.
[[nodiscard]] auto narrow_demanded_tables(const std::map<std::string, ir::ColumnDemand>& demand,
                                          const runtime::TableRegistry& tables)
    -> std::vector<std::pair<std::string, runtime::Table>>;

}  // namespace ibex::repl::detail
