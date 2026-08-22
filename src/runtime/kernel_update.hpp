// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <vector>

namespace ibex::runtime {
class PredicateInput;
struct RowRange;
}  // namespace ibex::runtime

namespace ibex::runtime::kernel {

/// A caller-owned dense window for a fixed-width numeric expression. Exactly
/// one pointer is non-null, selected by the expression's inferred result kind.
struct NumericOutputSpan {
    std::int64_t* ints = nullptr;
    double* doubles = nullptr;
};

enum class FixedWidthNumericKind : std::uint8_t { Int, Double };

/// Classify and write the simple binary numeric family shared by the ChunkView
/// update kernel and table-level parallel field windows.  It declines anything
/// outside column/scalar arithmetic so the caller's established evaluator
/// remains the semantic fallback.
[[nodiscard]] auto fixed_width_numeric_binary_kind(const ir::Expr& expr,
                                                   const PredicateInput& input,
                                                   const ScalarRegistry* scalars)
    -> std::optional<FixedWidthNumericKind>;
[[nodiscard]] auto write_fixed_width_numeric_binary(const ir::Expr& expr,
                                                    const PredicateInput& input,
                                                    ::ibex::runtime::RowRange range,
                                                    const ScalarRegistry* scalars,
                                                    FixedWidthNumericKind output_kind,
                                                    NumericOutputSpan output) -> bool;

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
