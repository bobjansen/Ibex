// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/operator.hpp>

#include <atomic>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "interpreter_internal.hpp"
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

/// One node of a compiled numeric expression tree, in post-order: `left` and
/// `right` index earlier entries of the same vector, so a plan is position
/// independent and can be copied. Column pointers are absolute-row, which is
/// what lets one plan serve any `RowRange` of the input it was planned against.
struct NumericTreeNode {
    enum class Kind : std::uint8_t {
        IntColumn,
        DoubleColumn,
        IntScalar,
        DoubleScalar,
        Binary,
        Min,
        Max,
        Unary
    };

    Kind kind = Kind::IntScalar;
    ExprType type = ExprType::Int;
    ir::ArithmeticOp op = ir::ArithmeticOp::Add;
    std::uint32_t left = 0;
    std::uint32_t right = 0;
    const std::int64_t* ints = nullptr;
    const double* doubles = nullptr;
    std::int64_t int_scalar = 0;
    double double_scalar = 0.0;
    double (*unary)(double) = nullptr;
};

/// A compiled arithmetic tree over columns and scalars: the general numeric
/// shape the single-operation `DirectFieldPlan` family cannot name. The root's
/// type is the output kind, so a caller sizes its destination from the plan.
struct DirectNumericTreePlan {
    std::vector<NumericTreeNode> nodes;
    std::uint32_t root = 0;
    ExprType type = ExprType::Int;
};

/// A resolved `__interp` expression whose leaves can be copied without
/// formatting or allocation. It is deliberately internal to the update
/// kernels, but shared by chunk and table writers so their two-pass string
/// contracts cannot drift. Interpolation and substring use the same operand
/// representation and count/prefix/write protocol.
struct DirectStringOperand {
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

struct DirectStringPlan {
    enum class Kind : std::uint8_t { Interpolation, Substring };

    Kind kind = Kind::Interpolation;
    std::vector<DirectStringOperand> operands;
    DirectStringOperand substring_source;
    std::int64_t substring_start = 0;
    std::optional<std::int64_t> substring_length;
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

[[nodiscard]] auto try_plan_direct_numeric_tree(const ir::Expr& expr, const PredicateInput& input,
                                                const ScalarRegistry* scalars)
    -> std::optional<DirectNumericTreePlan>;
[[nodiscard]] auto write_direct_numeric_tree_range(const DirectNumericTreePlan& plan,
                                                   ::ibex::runtime::RowRange range,
                                                   NumericOutputSpan output) -> bool;

[[nodiscard]] auto make_direct_string_plan(const ir::Expr& expr, const PredicateInput& input,
                                           const ScalarRegistry* scalars)
    -> std::optional<DirectStringPlan>;
/// Count the byte window and write it after the caller has assigned its
/// prefix-summed `StringOutputSpan`. `validity`, when present, is dense in
/// `range`; invalid rows remain empty and are never read.
[[nodiscard]] auto direct_string_bytes(const DirectStringPlan& plan,
                                       ::ibex::runtime::RowRange range,
                                       const ValidityBitmap* validity)
    -> std::optional<std::uint32_t>;
[[nodiscard]] auto write_direct_string(const DirectStringPlan& plan,
                                       ::ibex::runtime::RowRange range,
                                       const ValidityBitmap* validity, StringOutputSpan output)
    -> bool;

/// The direct-plan vocabulary for one field, resolved once. At most one member
/// is engaged; `plan_direct_field` picks in the order the writers below
/// dispatch in, so a caller cannot select a different one than the executor
/// would. Planning is not free (the categorical arm builds an output dictionary
/// and per-source code remaps), which is why callers pass a resolved route
/// around instead of re-planning per execution mode.
struct DirectFieldRoute {
    std::optional<DirectStringPlan> string;
    std::optional<DirectCategoricalPlan> categorical;
    std::optional<DirectPredicatePlan> predicate;
    std::optional<DirectValidityPlan> validity;
    std::optional<DirectFieldPlan> fixed_width;
    std::optional<DirectNumericTreePlan> numeric_tree;

    [[nodiscard]] auto has_plan() const noexcept -> bool {
        return string.has_value() || categorical.has_value() || predicate.has_value() ||
               validity.has_value() || fixed_width.has_value() || numeric_tree.has_value();
    }
};

[[nodiscard]] auto plan_direct_field(const ir::Expr& expr, const PredicateInput& input,
                                     const ScalarRegistry* scalars) -> DirectFieldRoute;

/// A caller's own writer for one range of an expression the direct vocabulary
/// does not cover. It fills the numeric window and returns that range's
/// validity. A caller that has no such writer -- one holding a chunk rather
/// than a table, whose evaluator is the table path it is trying not to enter --
/// passes none, and `evaluate_field_windows` then requires a direct plan.
using DirectFieldRangeWriter =
    std::function<std::expected<std::optional<ValidityBitmap>, std::string>(
        ::ibex::runtime::RowRange, NumericOutputSpan)>;

/// Evaluate one update field by splitting its rows across worker ranges, each
/// writing its own disjoint window of one pre-sized output column, then merging
/// the per-range validity.
///
/// Returns nullopt -- "not this shape" -- when the field has neither a direct
/// plan nor a `fallback`, or when the size gates make one whole-range
/// evaluation the better shape. The caller keeps its own serial route for both,
/// so this never has to invent one.
///
/// `inferred` is the update's authority on the output type when the caller has
/// one; pass nullopt to let the selected plan's own output kind decide, which
/// is what a caller without a table to infer from does.
///
/// `direct_numeric_writer` is a scratch flag shared with `fallback`: either may
/// set it to say a range was written by a direct numeric kernel, and this
/// reports it to `parallel_direct_numeric_fields` once, after the barrier. It
/// is one cell rather than two counters so a field whose ranges split between
/// the two writers is still counted exactly once.
[[nodiscard]] auto evaluate_field_windows(
    const ir::Expr& expr, const DirectFieldRoute& route, const PredicateInput& input,
    std::optional<ExprType> inferred, const ScalarRegistry* scalars, const ExecutionContext& exec,
    const DirectFieldRangeWriter* fallback, std::atomic<bool>* direct_numeric_writer = nullptr)
    -> std::expected<std::optional<ComputedColumn>, std::string>;

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
