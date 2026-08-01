#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace ibex::runtime {

/// A contiguous row range `[begin_row, end_row)` of an immutable source table,
/// tagged with its position in the partition sequence.
///
/// The morsel holds the input pointer plus the range rather than a
/// materialized slice. That is deliberate: Phase 1's range-aware kernels read
/// the shared immutable input directly by absolute index, so keeping the
/// pointer+range here now means that zero-copy path can replace the Phase 0
/// gather (see `PartitionedTableSource`) without reshaping this type. The
/// pointed-to `Table` must outlive every morsel and must not be mutated while
/// morsels over it are live — the one-query-at-a-time invariant (Phase 0
/// item 6) is what makes that safe.
struct TableRangeMorsel {
    const Table* input = nullptr;
    std::size_t begin_row = 0;
    std::size_t end_row = 0;     // exclusive
    std::uint64_t sequence = 0;  // 0-based emission order, for the ordered merger

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return end_row - begin_row; }
};

/// Gather rows `[begin, end)` of a type-erased column into a fresh column.
///
/// This is the Phase 0 materializing path: it copies the range so existing
/// chunk operators (which own their input) can consume it unchanged. A
/// categorical range shares the source's dictionary and index (codes are
/// copied verbatim, exactly as `MaterializeOperator` requires), so no
/// dictionary rebuild happens and the codes stay valid.
[[nodiscard]] inline auto gather_range(const ColumnValue& src, std::size_t begin, std::size_t end)
    -> ColumnValue {
    return std::visit(
        [&](const auto& col) -> ColumnValue {
            using Col = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                Column<Categorical> out(col.dictionary_ptr(), col.index_ptr());
                out.reserve(end - begin);
                for (std::size_t r = begin; r < end; ++r) {
                    out.push_code(col.code_at(r));
                }
                return out;
            } else {
                Col out;
                out.reserve(end - begin);
                for (std::size_t r = begin; r < end; ++r) {
                    out.push_back(col[r]);
                }
                return out;
            }
        },
        src);
}

/// Gather rows `[begin, end)` of an optional validity bitmap. A `nullopt`
/// source (all rows valid) yields `nullopt`, preserving the zero-overhead
/// bitmap-free common case.
[[nodiscard]] inline auto gather_validity(const std::optional<ValidityBitmap>& src,
                                          std::size_t begin, std::size_t end)
    -> std::optional<ValidityBitmap> {
    if (!src.has_value()) {
        return std::nullopt;
    }
    ValidityBitmap out;
    out.reserve(end - begin);
    for (std::size_t r = begin; r < end; ++r) {
        out.push_back((*src)[r]);
    }
    return out;
}

/// Materialize rows `[begin, end)` of `input` as a chunk stamped with its
/// morsel identity (`sequence`, and `row_offset = begin`).
///
/// One construction point for every morsel, whatever pulls it: the serial
/// `PartitionedTableSource` below and the parallel island's workers must
/// produce byte-identical chunks for the same range, so they must not each
/// build one. A column-less frame (e.g. a `Table(n)` scaffold) carries only its
/// `logical_rows`; `begin == end` yields the zero-row schema/metadata carrier.
[[nodiscard]] inline auto make_morsel_chunk(const Table& input, std::size_t begin, std::size_t end,
                                            std::uint64_t sequence) -> Chunk {
    Chunk chunk;
    chunk.columns.reserve(input.columns.size());
    for (const auto& entry : input.columns) {
        chunk.columns.push_back(ColumnEntry{
            .name = entry.name,
            .column = std::make_shared<ColumnValue>(gather_range(*entry.column, begin, end)),
            .validity = gather_validity(entry.validity, begin, end),
            // A morsel is a row range of the same column: same data, same zone.
            .timezone = entry.timezone});
    }
    if (input.columns.empty()) {
        chunk.logical_rows = end - begin;
    }
    chunk.ordering = input.ordering;
    chunk.time_index = input.time_index;
    chunk.sequence = sequence;
    chunk.row_offset = begin;
    return chunk;
}

/// Number of morsels `PartitionedTableSource` (or a parallel island over the
/// same table) produces for `input` at `grain`. A zero-row input still yields
/// exactly one carrier morsel.
[[nodiscard]] inline auto partitioned_morsel_count(const Table& input, std::size_t grain)
    -> std::uint64_t {
    const std::size_t normalized_grain = grain == 0 ? 1 : grain;
    const std::size_t rows = input.rows();
    if (rows == 0) {
        return 1;
    }
    return static_cast<std::uint64_t>((rows + normalized_grain - 1) / normalized_grain);
}

/// The row range of morsel `sequence` over `rows` total rows at `grain`.
[[nodiscard]] inline auto morsel_row_range(std::size_t rows, std::size_t grain,
                                           std::uint64_t sequence)
    -> std::pair<std::size_t, std::size_t> {
    const std::size_t normalized_grain = grain == 0 ? 1 : grain;
    const std::size_t begin = std::min(static_cast<std::size_t>(sequence) * normalized_grain, rows);
    return {begin, std::min(begin + normalized_grain, rows)};
}

/// Source operator that partitions one immutable `Table` into contiguous row
/// ranges and emits each as a chunk stamped with its `sequence` and
/// `row_offset`. Ranges are produced in ascending row order, so the serial
/// (single-consumer) pull order already matches the source order — the
/// `sequence` tag becomes load-bearing only once Phase 1 lets workers pull
/// ranges out of order and an ordered merger reassembles them.
///
/// Contract:
///   * The pointed-to table must outlive the operator and must not be mutated
///     while it is being drained.
///   * A grain of 0 is treated as 1 (never an infinite loop / empty range).
///   * A column-less frame (e.g. a `Table(n)` scaffold) is partitioned by
///     `grain` too, carrying each range's `logical_rows`. A zero-row input —
///     with or without columns — emits exactly one zero-row carrier so its
///     schema/metadata reach `MaterializeOperator`.
///
/// This does NOT reuse `TableSourceOperator` (which emits the whole table as
/// one chunk) and it does NOT introduce column slicing/views — the ranges are
/// materialized by `gather_range`. Zero-copy range kernels are Phase 1.
class PartitionedTableSource final : public Operator {
   public:
    PartitionedTableSource(const Table& input, std::size_t grain)
        : input_(&input), grain_(grain == 0 ? 1 : grain) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        const std::size_t total = input_->rows();
        if (cursor_ >= total) {
            // Zero-row input (with or without columns): emit exactly one
            // carrier so the schema, logical rows, and metadata still reach the
            // sink (matches TableSourceOperator).
            if (total == 0 && !emitted_empty_) {
                emitted_empty_ = true;
                return std::optional<Chunk>{make_morsel_chunk(*input_, 0, 0, sequence_++)};
            }
            return std::optional<Chunk>{};
        }

        const std::size_t begin = cursor_;
        const std::size_t end = std::min(begin + grain_, total);
        cursor_ = end;
        return std::optional<Chunk>{make_morsel_chunk(*input_, begin, end, sequence_++)};
    }

   private:
    const Table* input_;
    std::size_t grain_;
    std::size_t cursor_ = 0;
    std::uint64_t sequence_ = 0;
    bool emitted_empty_ = false;
};

}  // namespace ibex::runtime
