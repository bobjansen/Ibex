// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/operator.hpp>

namespace ibex::runtime {

/// Move a streamed chunk through the established table evaluator boundary.
/// Transport identity is deliberately not table metadata and is restored by
/// the caller that owns the one-input/one-output mapping contract.
[[nodiscard]] auto chunk_to_table(Chunk chunk) -> Table;
[[nodiscard]] auto table_to_chunk(Table table) -> Chunk;

}  // namespace ibex::runtime
