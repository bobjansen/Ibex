// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <expected>
#include <string>
#include <vector>

namespace ibex::runtime::kernel {

/// Execute one row-local update over a chunk while retaining its transport
/// identity.  The existing table-level field evaluator remains the semantic
/// authority until its representation-specific loops are ported one by one;
/// this entry point is the shared chunk-kernel seam both executors can use.
[[nodiscard]] auto update_row_local_chunk(Chunk input, const std::vector<ir::FieldSpec>& fields,
                                          const ScalarRegistry* scalars,
                                          const ExternRegistry* externs,
                                          const ExecutionContext& exec)
    -> std::expected<Chunk, std::string>;

}  // namespace ibex::runtime::kernel
