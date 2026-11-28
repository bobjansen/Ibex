// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once
#include "interpreter_internal.hpp"

namespace ibex::runtime {
[[nodiscard]] auto polars_rolling_column(const ir::CallExpr& call, const Table& input)
    -> std::expected<ComputedColumn, std::string>;
}  // namespace ibex::runtime
