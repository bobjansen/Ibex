#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
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

    [[nodiscard]] auto rows() const noexcept -> std::size_t {
        if (columns.empty()) {
            return 0;
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
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    Table table_;
    bool emitted_ = false;
};

/// Sink that drains a child operator into a `Table`. For the
/// single-chunk case produced by `TableSourceOperator`, the chunk's
/// columns move directly into the result, preserving `ordering` and
/// `time_index`. Multi-chunk concatenation is deferred until a source
/// that actually emits more than one chunk is introduced (step 3 of
/// the chunked-execution migration).
class MaterializeOperator {
   public:
    explicit MaterializeOperator(OperatorPtr child) : child_(std::move(child)) {}

    [[nodiscard]] auto run() -> std::expected<Table, std::string> {
        Table result;
        std::optional<Chunk> first;

        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(chunk_res.error());
            }
            auto& maybe_chunk = chunk_res.value();
            if (!maybe_chunk.has_value()) {
                break;
            }
            if (!first.has_value()) {
                first = std::move(*maybe_chunk);
                continue;
            }
            return std::unexpected(
                "MaterializeOperator: multi-chunk concatenation not yet implemented");
        }

        if (first.has_value()) {
            result.columns = std::move(first->columns);
            for (std::size_t i = 0; i < result.columns.size(); ++i) {
                result.index[result.columns[i].name] = i;
            }
            result.ordering = std::move(first->ordering);
            result.time_index = std::move(first->time_index);
        }
        return result;
    }

   private:
    OperatorPtr child_;
};

}  // namespace ibex::runtime
