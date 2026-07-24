#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// A horizontal slice of a table flowing through an operator pipeline.
///
/// Carries the same `ColumnEntry` shape as `Table` — columns with
/// optional per-row validity bitmaps. Table-level metadata (`ordering`,
/// `time_index`) is only meaningful on a fully materialized table, so
/// it is not part of `Chunk` by default. During the chunked-execution
/// migration, `TableSourceOperator` wraps a pre-built `Table` as a
/// single chunk and stashes that table's metadata on the chunk so
/// `MaterializeOperator` can restore it on the receiving side. Sources
/// that are natively chunked leave the metadata fields empty.
struct Chunk {
    std::vector<ColumnEntry> columns;
    std::optional<std::vector<ir::OrderKey>> ordering;
    std::optional<std::string> time_index;
    /// Logical row count for a column-less chunk (e.g. from `Table(n)`); see
    /// the matching field on `Table`. Only consulted by `rows()` when `columns`
    /// is empty.
    std::optional<std::size_t> logical_rows;

    /// Position of this chunk in its source's emission order. When a chunked
    /// source partitions one immutable table into row ranges (see
    /// `PartitionedTableSource` / `TableRangeMorsel`), workers may produce those
    /// ranges out of order; `sequence` lets an ordered merger reassemble the
    /// original row order. Zero for the single-source and chunk-at-a-time
    /// paths that do not partition. Chunk-preserving operators must copy it
    /// through unchanged (including empty schema-carrier chunks).
    std::uint64_t sequence = 0;

    /// Absolute starting row of this chunk within its source table, i.e. the
    /// index in the original (un-partitioned) table of this chunk's first row.
    /// Zero for non-partitioned sources. Like `sequence`, it must survive
    /// chunk-preserving operators so range-aware kernels can address the shared
    /// input by absolute index.
    std::size_t row_offset = 0;

    void add_column(std::string name, ColumnValue column,
                    std::optional<ValidityBitmap> validity = std::nullopt) {
        columns.push_back(ColumnEntry{.name = std::move(name),
                                      .column = std::make_shared<ColumnValue>(std::move(column)),
                                      .validity = std::move(validity)});
    }

    void replace_column(std::size_t pos, ColumnValue column) {
        columns.at(pos).column = std::make_shared<ColumnValue>(std::move(column));
    }

    void replace_column(std::size_t pos, ColumnValue column,
                        std::optional<ValidityBitmap> validity) {
        auto& entry = columns.at(pos);
        entry.column = std::make_shared<ColumnValue>(std::move(column));
        entry.validity = std::move(validity);
    }

    [[nodiscard]] auto mutable_column(std::size_t pos) -> ColumnValue& {
        auto& column = columns.at(pos).column;
        if (column.use_count() != 1) {
            column = std::make_shared<ColumnValue>(*column);
        }
        return *column;
    }

    [[nodiscard]] auto rows() const noexcept -> std::size_t {
        if (columns.empty()) {
            return logical_rows.value_or(0);
        }
        return column_size(*columns.front().column);
    }
};

/// Pull-based operator interface. `next()` returns the operator's next
/// chunk, or `std::nullopt` when the stream is exhausted. Errors
/// propagate as `std::unexpected`.
class Operator {
   public:
    Operator() = default;
    Operator(const Operator&) = delete;
    Operator(Operator&&) = delete;
    auto operator=(const Operator&) -> Operator& = delete;
    auto operator=(Operator&&) -> Operator& = delete;
    virtual ~Operator() = default;

    [[nodiscard]] virtual auto next() -> std::expected<std::optional<Chunk>, std::string> = 0;
};

using OperatorPtr = std::unique_ptr<Operator>;

/// Source operator that wraps an already-materialized `Table` and emits
/// it as a single chunk. Used as an adapter during the chunked-execution
/// migration: any operator that still produces a full `Table` can be
/// wrapped in a `TableSourceOperator` to plug into the chunk pipeline.
class TableSourceOperator final : public Operator {
   public:
    explicit TableSourceOperator(Table table) : table_(std::move(table)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        emitted_ = true;
        Chunk chunk;
        chunk.columns = std::move(table_.columns);
        chunk.ordering = std::move(table_.ordering);
        chunk.time_index = std::move(table_.time_index);
        // logical_rows is only meaningful for a column-less frame.
        if (chunk.columns.empty()) {
            chunk.logical_rows = table_.logical_rows;
        }
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    Table table_;
    bool emitted_ = false;
};

/// Sink that drains a child operator into a `Table`. Chunks are
/// consumed one at a time: the first chunk's columns are moved into
/// the result, and every subsequent chunk is appended and released
/// before the next is pulled, so peak memory is bounded by
/// `result + 1 chunk` rather than the full chunk list.
///
/// Concat assumes all chunks agree on schema (column count, names, and
/// variant alternatives) and that any `Column<Categorical>` values across
/// chunks share the same backing dictionary — which is the contract the
/// chunked csv source provides. Per-chunk validity bitmaps are concatenated
/// across chunks: a missing bitmap on any chunk means "every row valid", so
/// a nullable column that carries validity on only some chunks is widened to
/// a full-length bitmap on demand.
///
/// Append a chunk column's optional validity onto the result column's optional
/// validity. `prev_rows` is the result column's length before this chunk and
/// `src_rows` the chunk column's length. A `nullopt` bitmap means every row is
/// valid; the result stays `nullopt` (zero overhead) until some chunk actually
/// carries nulls, at which point the previously-accumulated rows are backfilled
/// as valid.
inline void materialize_append_validity(std::optional<ValidityBitmap>& dst_valid,
                                        std::size_t prev_rows,
                                        const std::optional<ValidityBitmap>& src_valid,
                                        std::size_t src_rows) {
    if (!dst_valid.has_value() && !src_valid.has_value()) {
        return;  // both all-valid: keep the column bitmap-free.
    }
    if (!dst_valid.has_value()) {
        // The result was implicitly all-valid so far; materialize that prefix.
        dst_valid.emplace(prev_rows, true);
    }
    dst_valid->reserve(prev_rows + src_rows);
    if (src_valid.has_value()) {
        for (std::size_t r = 0; r < src_rows; ++r) {
            dst_valid->push_back((*src_valid)[r]);
        }
    } else {
        for (std::size_t r = 0; r < src_rows; ++r) {
            dst_valid->push_back(true);
        }
    }
}

class MaterializeOperator {
   public:
    explicit MaterializeOperator(OperatorPtr child) : child_(std::move(child)) {}

    [[nodiscard]] auto run() -> std::expected<Table, std::string> {
        Table result;

        auto first_res = child_->next();
        if (!first_res.has_value()) {
            return std::unexpected(std::move(first_res.error()));
        }
        if (!first_res.value().has_value()) {
            return result;
        }

        Chunk first = std::move(*first_res.value());
        result.columns = std::move(first.columns);
        for (std::size_t i = 0; i < result.columns.size(); ++i) {
            result.index[result.columns[i].name] = i;
        }
        result.ordering = std::move(first.ordering);
        result.time_index = std::move(first.time_index);
        // logical_rows is only meaningful for a column-less frame.
        if (result.columns.empty()) {
            result.logical_rows = first.logical_rows;
        }

        const std::size_t n_cols = result.columns.size();

        while (true) {
            // Per-chunk interruption boundary for streamed pipelines.
            if (interrupt_requested()) {
                return std::unexpected(interrupt_message());
            }
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());

            if (chunk.columns.size() != n_cols) {
                return std::unexpected("MaterializeOperator: chunk schema mismatch (column count)");
            }

            if (n_cols == 0) {
                // Column-less frames (e.g. `Table(n)` row scaffolds) carry their
                // row count in `logical_rows`, not in any column; accumulate it
                // across chunks so multi-chunk zero-column output reports the
                // total rather than only the first chunk's count.
                result.logical_rows =
                    result.logical_rows.value_or(0) + chunk.logical_rows.value_or(0);
                continue;
            }

            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != result.columns[i].name) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != result.columns[i].column->index()) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column type)");
                }
            }

            // Every result column has the same length before this chunk lands;
            // capture it once for validity backfill. (`n_cols == 0` returned
            // above, so there is always a column 0 here.)
            const std::size_t prev_rows = column_size(*result.columns[0].column);
            for (std::size_t i = 0; i < n_cols; ++i) {
                const std::size_t src_rows = column_size(*chunk.columns[i].column);
                auto& dst_col = result.mutable_column(i);
                std::visit(
                    [&](auto& dst) {
                        using Col = std::decay_t<decltype(dst)>;
                        auto& src = std::get<Col>(*chunk.columns[i].column);
                        dst.reserve(dst.size() + src.size());
                        if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_code(src.code_at(r));
                            }
                        } else {
                            for (std::size_t r = 0; r < src.size(); ++r) {
                                dst.push_back(src[r]);
                            }
                        }
                    },
                    dst_col);
                materialize_append_validity(result.columns[i].validity, prev_rows,
                                            chunk.columns[i].validity, src_rows);
            }
            // `chunk` goes out of scope here, releasing its memory before
            // the next `child_->next()` call.
        }

        return result;
    }

   private:
    OperatorPtr child_;
};

}  // namespace ibex::runtime
