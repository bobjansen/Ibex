// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include "lazy_head.hpp"

#include <ibex/format.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/table_format.hpp>

#include <algorithm>
#include <cstddef>
#include <expected>
#include <numeric>
#include <set>
#include <sstream>
#include <string>
#include <utility>

namespace ibex::repl::detail {

auto format_lazy_head(runtime::LazyTable& lazy, std::size_t count,
                      const runtime::ExecutionContext& exec)
    -> std::expected<std::string, std::string> {
    std::set<std::string> names;
    for (const auto& field : lazy.schema().columns) {
        names.insert(field.name);
    }
    runtime::Selection leading(std::min(count, lazy.rows()));
    // NOLINTNEXTLINE(modernize-use-ranges): Because Apple libc++
    std::iota(leading.begin(), leading.end(), std::size_t{0});
    auto head = lazy.project_rows(names, leading, exec);
    if (!head.has_value()) {
        return std::unexpected(std::move(head.error()));
    }
    std::ostringstream rendered;
    runtime::format_table(*head, rendered, count);
    std::string text = std::move(rendered).str();
    // The rendering counts the rows decoded; the source has more.
    const std::string shown = ibex::formatting::format("rows: {}\n", head->rows());
    if (text.starts_with(shown)) {
        text.replace(0, shown.size(), ibex::formatting::format("rows: {}\n", lazy.rows()));
    }
    return text;
}

}  // namespace ibex::repl::detail
