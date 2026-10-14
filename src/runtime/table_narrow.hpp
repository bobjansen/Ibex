// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <optional>
#include <set>
#include <string>

namespace ibex::runtime {

/// `table` cut down to the columns in `demanded`, or nullopt when the cut
/// would drop nothing and the table should be used whole. The cut shares
/// column storage, so it copies nothing; what it saves is every operator above
/// carrying, and gathering, columns nobody reads: `x[order {v3 desc}, head 2,
/// by id6]` over a nine-column table moved nine payload columns through the
/// top-k heaps. Every execution path narrows through this one function: both
/// REPL drivers for their in-memory tables, and `ibex_compile` output through
/// `ops::narrow`.
///
/// Names `table` lacks are ignored. A column the table's own properties name
/// (its ordering, time index or grouping) is kept even when undemanded, so the
/// properties stay true of the cut. A demand naming no column keeps the table
/// whole: a plan reading none (`x[select { n = count() }]`) still needs the
/// row count, which a column-less table would lose.
/// `IBEX_DISABLE_EAGER_PROJECTION` turns narrowing off.
[[nodiscard]] auto narrow_to_columns(const Table& table, const std::set<std::string>& demanded)
    -> std::optional<Table>;

/// Columns `narrow_to_columns` has dropped in this process. Lets a test see
/// that a path narrowed a table, which its answers cannot show.
[[nodiscard]] auto narrowed_column_count() -> std::size_t;

}  // namespace ibex::runtime
