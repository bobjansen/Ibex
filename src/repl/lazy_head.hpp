// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <cstddef>
#include <expected>
#include <string>

namespace ibex::repl::detail {

/// `:head` of a lazy binding: its first `count` rows of every column, rendered
/// as the REPL renders a table, with the header reporting the source's own row
/// count. Only the units those rows lie in are decoded (one row group of a
/// Parquet file), not the whole source, and the source's column cache is left
/// alone.
[[nodiscard]] auto format_lazy_head(runtime::LazyTable& lazy, std::size_t count,
                                    const runtime::ExecutionContext& exec)
    -> std::expected<std::string, std::string>;

}  // namespace ibex::repl::detail
