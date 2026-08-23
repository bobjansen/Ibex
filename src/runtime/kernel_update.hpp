// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstdint>
#include <expected>
#include <memory>
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
enum class DirectFieldKind : std::uint8_t { NumericBinary, TemporalPart, StringLength };
enum class TemporalPart : std::uint8_t { Year, Month, Day, Hour, Minute, Second };

/// Immutable direct-field plan for the first parallel-safe output shape.  The
/// plan borrows the IR expression; its owner must outlive all scheduled range
/// writes.  It owns no destination or metadata, so workers can share it.
struct DirectFieldPlan {
    DirectFieldKind kind = DirectFieldKind::NumericBinary;
    const ir::Expr* expression = nullptr;
    FixedWidthNumericKind numeric_kind = FixedWidthNumericKind::Int;
    TemporalPart temporal_part = TemporalPart::Year;
    const Date* dates = nullptr;
    const Timestamp* timestamps = nullptr;
    bool byte_length = false;
    const Column<std::string>* strings = nullptr;
    const Column<Categorical>* categoricals = nullptr;
    std::shared_ptr<const std::vector<std::int64_t>> categorical_lengths;
};

/// A caller-owned output window for one direct-field range.  Validity is
/// intentionally absent: every worker returns its own bitmap and the table
/// evaluator merges those after the write barrier.
struct DirectOutputWindow {
    NumericOutputSpan numeric;
};

/// Immutable plan for a native boolean predicate.  The output remains packed
/// in the evaluator-owned Column<bool>; writers only set true bits and return
/// their dense validity window.
struct DirectPredicatePlan {
    const ir::Expr* expression = nullptr;
};

enum class DirectValidityKind : std::uint8_t { FillNull, Coalesce, Case };

/// One value arm for a fixed-width null-handling expression.  Plans borrow
/// source entries and IR condition expressions; only the evaluator owns the
/// destination and the final merged validity bitmap.
struct DirectValidityValue {
    const ColumnEntry* column = nullptr;
    ScalarValue literal{};
    bool is_literal = false;
    bool is_null = false;
};

struct DirectValidityPlan {
    DirectValidityKind kind = DirectValidityKind::FillNull;
    FixedWidthNumericKind numeric_kind = FixedWidthNumericKind::Int;
    std::vector<const ir::Expr*> conditions;
    std::vector<DirectValidityValue> values;
};

struct CategoricalOutputSpan {
    Column<Categorical>::code_type* codes = nullptr;
    std::size_t begin = 0;
    std::size_t count = 0;
};

/// Owns the deterministic output dictionary and read-only source-code remaps.
/// Worker writes are restricted to `CategoricalOutputSpan::codes`.
struct DirectCategoricalPlan {
    DirectValidityKind kind = DirectValidityKind::Coalesce;
    std::vector<const ir::Expr*> conditions;
    std::vector<DirectValidityValue> values;
    std::shared_ptr<std::vector<std::string>> dictionary;
    std::shared_ptr<Column<Categorical>::index_map> index;
    std::vector<std::vector<Column<Categorical>::code_type>> remaps;
    std::vector<Column<Categorical>::code_type> literal_codes;
};

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
[[nodiscard]] auto try_plan_direct_numeric_field(const ir::Expr& expr, const PredicateInput& input,
                                                 const ScalarRegistry* scalars)
    -> std::optional<DirectFieldPlan>;
[[nodiscard]] auto try_plan_direct_temporal_field(const ir::Expr& expr, const PredicateInput& input)
    -> std::optional<DirectFieldPlan>;
[[nodiscard]] auto try_plan_direct_string_length_field(const ir::Expr& expr,
                                                       const PredicateInput& input)
    -> std::optional<DirectFieldPlan>;
/// The fixed-width entry point shared by table field splitting and serial
/// ChunkView updates. It keeps evaluator-side dispatch independent of the
/// individual family while preserving a closed set of output representations.
[[nodiscard]] auto try_plan_direct_fixed_width_field(const ir::Expr& expr,
                                                     const PredicateInput& input,
                                                     const ScalarRegistry* scalars)
    -> std::optional<DirectFieldPlan>;
[[nodiscard]] auto write_direct_field_range(const DirectFieldPlan& plan,
                                            const PredicateInput& input,
                                            ::ibex::runtime::RowRange range,
                                            const ScalarRegistry* scalars,
                                            DirectOutputWindow output) -> bool;
[[nodiscard]] auto try_plan_direct_predicate_field(const ir::Expr& expr)
    -> std::optional<DirectPredicatePlan>;
[[nodiscard]] auto write_direct_predicate_range(const DirectPredicatePlan& plan,
                                                const PredicateInput& input,
                                                ::ibex::runtime::RowRange range,
                                                const ScalarRegistry* scalars,
                                                BoolOutputSpan output)
    -> std::expected<std::optional<ValidityBitmap>, std::string>;
[[nodiscard]] auto try_plan_direct_validity_field(const ir::Expr& expr, const PredicateInput& input,
                                                  const ScalarRegistry* scalars)
    -> std::optional<DirectValidityPlan>;
[[nodiscard]] auto write_direct_validity_field_range(const DirectValidityPlan& plan,
                                                     const PredicateInput& input,
                                                     ::ibex::runtime::RowRange range,
                                                     const ScalarRegistry* scalars,
                                                     DirectOutputWindow output)
    -> std::expected<std::optional<ValidityBitmap>, std::string>;
[[nodiscard]] auto try_plan_direct_categorical_field(const ir::Expr& expr,
                                                     const PredicateInput& input,
                                                     const ScalarRegistry* scalars)
    -> std::optional<DirectCategoricalPlan>;
[[nodiscard]] auto write_direct_categorical_field_range(const DirectCategoricalPlan& plan,
                                                        const PredicateInput& input,
                                                        ::ibex::runtime::RowRange range,
                                                        const ScalarRegistry* scalars,
                                                        CategoricalOutputSpan output)
    -> std::expected<std::optional<ValidityBitmap>, std::string>;
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
