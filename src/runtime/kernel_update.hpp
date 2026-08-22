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

#include "kernel_gather.hpp"

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

/// A resolved `__interp` expression whose leaves can be copied without
/// formatting or allocation.  It is deliberately internal to the update
/// kernels, but shared by chunk and table writers so their two-pass string
/// contracts cannot drift.
struct StringInterpolationOperand {
    std::optional<StringView> column;
    /// Categorical values stay dictionary-coded until the interpolation writer
    /// needs their byte slab; resolving the code directly avoids a per-row
    /// Column variant visit and makes the dictionary ownership explicit.
    struct CategoricalView {
        const Column<Categorical>::code_type* codes = nullptr;
        const std::vector<std::string>* dictionary = nullptr;
        std::size_t rows = 0;

        [[nodiscard]] auto value(std::size_t row) const noexcept -> std::string_view {
            return (*dictionary)[static_cast<std::size_t>(codes[row])];
        }
    } categorical;
    const Date* dates = nullptr;
    const Timestamp* timestamps = nullptr;
    std::string_view literal;
};

struct StringInterpolationPlan {
    std::vector<StringInterpolationOperand> operands;
};

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

[[nodiscard]] auto make_string_interpolation_plan(const ir::Expr& expr, const PredicateInput& input,
                                                  const ScalarRegistry* scalars)
    -> std::optional<StringInterpolationPlan>;
/// Count the byte window and write it after the caller has assigned its
/// prefix-summed `StringOutputSpan`. `validity`, when present, is dense in
/// `range`; invalid rows remain empty and are never read.
[[nodiscard]] auto string_interpolation_bytes(const StringInterpolationPlan& plan,
                                              ::ibex::runtime::RowRange range,
                                              const ValidityBitmap* validity)
    -> std::optional<std::uint32_t>;
[[nodiscard]] auto write_string_interpolation(const StringInterpolationPlan& plan,
                                              ::ibex::runtime::RowRange range,
                                              const ValidityBitmap* validity,
                                              StringOutputSpan output) -> bool;

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
