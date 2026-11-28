// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once
#include "interpreter_internal.hpp"

namespace ibex::runtime {
struct RollingPartitions {
    std::span<const std::size_t> rows;
    std::span<const std::size_t> offsets;
    std::size_t workers;
};
[[nodiscard]] auto polars_rolling_column(const ir::CallExpr& call, const Table& input,
                                         RollingPartitions partitions = {})
    -> std::expected<ComputedColumn, std::string>;
}  // namespace ibex::runtime
