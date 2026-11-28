// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once
#include "interpreter_internal.hpp"

namespace ibex::runtime {
inline auto is_polars_rolling(std::string_view name) -> bool {
    return name == "__ibex_rolling_rank" || name == "__ibex_rolling_rank_by" ||
           name == "__ibex_rolling_avg_rank" || name == "__ibex_rolling_avg_rank_by" ||
           name == "__ibex_rolling_sum" || name == "__ibex_rolling_mean" ||
           name == "__ibex_rolling_sum_by" || name == "__ibex_rolling_mean_by";
}

struct RollingPartitions {
    std::span<const std::size_t> rows;
    std::span<const std::size_t> offsets;
    std::size_t workers;
};
[[nodiscard]] auto polars_rolling_column(const ir::CallExpr& call, const Table& input,
                                         RollingPartitions partitions = {})
    -> std::expected<ComputedColumn, std::string>;
}  // namespace ibex::runtime
