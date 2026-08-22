// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/operator.hpp>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <type_traits>
#include <variant>
#include <vector>

namespace ibex::runtime::kernel {

/// Phase 2, item 1 of plans/kernel-pipeline-execution-plan.md: the non-owning
/// vocabulary kernels exchange data through, per the ownership model that plan
/// prescribes and CONTRACTS.md states. Views carry length, validity, and
/// position explicitly and own nothing; the referenced storage must outlive
/// the view (the one-query lease and the operator's chunk lifetime provide
/// that, as they already do for `TableRangeMorsel`).
///
/// Scope of this first slice: the fixed-width representations only
/// (`Int64`, `Double`, `Date`, `Timestamp`). `bool` (bit-packed words) and
/// `std::string` (cumulative offsets) are deliberately not viewable yet —
/// they are the next representations in the plan's "one at a time" order and
/// need their own access shapes rather than a lie that pretends they are
/// arrays.

/// Non-owning view of one fixed-width value array plus its optional
/// validity. Constrained to trivially-copyable value types: that is the
/// type-level claim "a flat array a kernel may copy by `sizeof(T)`" — it
/// covers the fixed-width `Column` alternatives AND raw code arrays
/// (`Column<Categorical>::codes_data()` is `const uint32_t*`, with no
/// `Column<uint32_t>` existing).
/// `validity == nullptr` means every row is valid — the zero-overhead
/// common case CONTRACTS.md §1 defines; `is_valid` never branches on a
/// `nullopt` per row.
///
/// Validity is a raw pointer, never `const std::optional<ValidityBitmap>&`:
/// an optional-by-const-ref parameter silently materializes a temporary
/// from a bare bitmap argument, the temporary dies at the end of the full
/// expression, and the view is left dangling — the plan-borrowing trap in
/// miniature. The borrowing rule is the view's contract.
template <typename T>
    requires std::is_trivially_copyable_v<T>
class ColumnView {
   public:
    ColumnView(const T* data, std::size_t rows, const ValidityBitmap* validity) noexcept
        : data_(data), rows_(rows), validity_(validity) {}

    ColumnView(const Column<T>& column, const ValidityBitmap* validity) noexcept
        : ColumnView(column.data(), column.size(), validity) {}

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_; }
    [[nodiscard]] auto data() const noexcept -> const T* { return data_; }
    [[nodiscard]] auto values() const noexcept -> std::span<const T> { return {data_, rows_}; }
    [[nodiscard]] auto validity() const noexcept -> const ValidityBitmap* { return validity_; }

    /// Row `row` is non-null. A null `validity` view answers true for every
    /// row; a present one is consulted per row.
    [[nodiscard]] auto is_valid(std::size_t row) const noexcept -> bool {
        return validity_ == nullptr || (*validity_)[row];
    }

    [[nodiscard]] auto value(std::size_t row) const noexcept -> const T& { return data_[row]; }

   private:
    const T* data_;
    std::size_t rows_;
    const ValidityBitmap* validity_;
};

/// Non-owning view of a chunk's columns, addressed by absolute position.
/// Column identity is by position, not name: a kernel is told positions by
/// the pipeline that resolved names once at construction. Carries the
/// chunk's position in the source index space (`row_offset`, `sequence`)
/// so range kernels can address the shared input the way
/// `TableRangeMorsel` does.
class ChunkView {
   public:
    ChunkView(const Chunk& chunk) : chunk_(&chunk) {}

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return chunk_->rows(); }
    [[nodiscard]] auto columns() const noexcept -> std::size_t { return chunk_->columns.size(); }
    [[nodiscard]] auto column(std::size_t pos) const noexcept -> const ColumnValue& {
        return *chunk_->columns.at(pos).column;
    }
    [[nodiscard]] auto validity(std::size_t pos) const noexcept
        -> const std::optional<ValidityBitmap>& {
        return chunk_->columns.at(pos).validity;
    }
    [[nodiscard]] auto row_offset() const noexcept -> std::size_t { return chunk_->row_offset; }
    [[nodiscard]] auto sequence() const noexcept -> std::uint64_t { return chunk_->sequence; }

    /// Typed view of column `pos`; terminates on a representation mismatch,
    /// which is a pipeline-construction bug (dispatch chose the wrong
    /// kernel), never a data condition.
    template <typename T>
    [[nodiscard]] auto view(std::size_t pos) const noexcept -> ColumnView<T> {
        const auto& valid = validity(pos);
        return ColumnView<T>(std::get<Column<T>>(column(pos)),
                             valid.has_value() ? &*valid : nullptr);
    }

   private:
    const Chunk* chunk_;
};

/// The explicit selection shapes a kernel may be handed (CONTRACTS.md §2):
/// never a hidden `std::vector` convention. All three address the SOURCE
/// row space; a contiguous `RowRange` additionally promises that survivor
/// rows are adjacent in the input, which is what lets a gather skip the
/// indirection entirely.
struct RowRange {
    std::size_t begin = 0;
    std::size_t end = 0;  // exclusive

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return end - begin; }
};

struct RowIndices {
    const std::size_t* data = nullptr;
    std::size_t count = 0;  // ascending; duplicates are a caller bug

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return count; }
    [[nodiscard]] auto operator[](std::size_t i) const noexcept -> std::size_t { return data[i]; }
};

struct RowBitmap {
    const ValidityBitmap* bits = nullptr;  // one entry per SOURCE row

    [[nodiscard]] auto test(std::size_t row) const noexcept -> bool { return (*bits)[row]; }
};

/// The engine's native mask shape: 64-row keep-word blocks, as produced by
/// `compute_filter_selection` and consumed word-at-a-time everywhere a
/// filter's survivors are compacted. Words are relative to `row_base`
/// (word `w`, bit `b` denotes source row `row_base + w*64 + b`); iteration
/// yields absolute source rows, ascending — the convention
/// `for_each_selected_row` established.
struct RowWordBlocks {
    const std::uint64_t* words = nullptr;
    std::size_t word_count = 0;
    std::size_t row_base = 0;
};

using Selection = std::variant<RowRange, RowIndices, RowBitmap, RowWordBlocks>;

/// Number of surviving rows a selection denotes. `RowBitmap` cannot answer
/// without the source length, which its caller supplies.
[[nodiscard]] inline auto selection_rows(const Selection& selection, std::size_t source_rows)
    -> std::size_t {
    return std::visit(
        [&](const auto& sel) -> std::size_t {
            using S = std::decay_t<decltype(sel)>;
            if constexpr (std::is_same_v<S, RowBitmap>) {
                std::size_t n = 0;
                for (std::size_t r = 0; r < source_rows; ++r) {
                    n += sel.test(r) ? 1U : 0U;
                }
                return n;
            } else if constexpr (std::is_same_v<S, RowWordBlocks>) {
                std::size_t n = 0;
                for (std::size_t w = 0; w < sel.word_count; ++w) {
                    n += static_cast<std::size_t>(std::popcount(sel.words[w]));
                }
                return n;
            } else {
                return sel.rows();
            }
        },
        selection);
}

}  // namespace ibex::runtime::kernel
