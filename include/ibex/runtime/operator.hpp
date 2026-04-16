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

/// Sink that drains a child operator into a `Table`. Chunks are
/// consumed one at a time: the first chunk's columns are moved into
/// the result, and every subsequent chunk is appended and released
/// before the next is pulled, so peak memory is bounded by
/// `result + 1 chunk` rather than the full chunk list.
///
/// Concat assumes all chunks agree on schema (column count, names, and
/// variant alternatives) and that any `Column<Categorical>` values across
/// chunks share the same backing dictionary — which is the contract the
/// chunked csv source provides. Validity bitmaps are not yet supported
/// on multi-chunk streams; any non-null validity on any chunk is
/// rejected until step 4+ wants to stream nulls.
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

        const std::size_t n_cols = result.columns.size();

        while (true) {
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
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != result.columns[i].name) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != result.columns[i].column->index()) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column type)");
                }
                if (chunk.columns[i].validity.has_value() ||
                    result.columns[i].validity.has_value()) {
                    return std::unexpected(
                        "MaterializeOperator: validity bitmaps not yet supported on multi-chunk "
                        "streams");
                }
            }

            for (std::size_t i = 0; i < n_cols; ++i) {
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
                    *result.columns[i].column);
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
