// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/operator.hpp>

#include <expected>
#include <string>
#include <vector>

namespace ibex::runtime::kernel {

/// Chunk-level entry points for the established vectorized filter evaluator.
/// They own the temporary Table bridge and preserve morsel transport identity,
/// leaving chunked operators and the physical executor one shared seam.
[[nodiscard]] auto filter_chunk(Chunk input, const ir::Expr& predicate,
                                const ScalarRegistry* scalars) -> std::expected<Chunk, std::string>;
[[nodiscard]] auto filter_project_chunk(Chunk input, const ir::Expr& predicate,
                                        const std::vector<ir::ColumnRef>& columns,
                                        const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string>;
[[nodiscard]] auto filter_limit_chunk(Chunk input, const ir::Expr& predicate, std::size_t row_limit,
                                      const ScalarRegistry* scalars)
    -> std::expected<Chunk, std::string>;
[[nodiscard]] auto project_chunk(Chunk input, const std::vector<ir::ColumnRef>& columns)
    -> std::expected<Chunk, std::string>;
[[nodiscard]] auto filter_update_project_chunk(Chunk input, const ir::Expr& predicate,
                                               const std::vector<ir::FieldSpec>& fields,
                                               const std::vector<ir::ColumnRef>& project_columns,
                                               const std::vector<ir::ColumnRef>& gather_columns,
                                               const ScalarRegistry* scalars,
                                               const ExternRegistry* externs,
                                               const ExecutionContext& exec)
    -> std::expected<Chunk, std::string>;

}  // namespace ibex::runtime::kernel
