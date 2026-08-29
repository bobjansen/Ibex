// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// chunked.cpp — residual streaming operators, rank evaluation, extern-call
// execution, and concrete factories used by the physical and pipeline
// executors. Planning, migrated-plan dispatch, and generic pipeline execution
// live in their respective translation units. Split out of interpreter.cpp;
// shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/format.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <ratio>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "physical_plan.hpp"

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "chunk_conversion_internal.hpp"
#include "aggregate_chunked_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_chunked_internal.hpp"
#include "join_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "model_internal.hpp"
#include "packed_key_encoder_internal.hpp"
#include "physical_executor_internal.hpp"
#include "pipeline_executor_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

/// Append `src`'s validity for `src_rows` rows onto `dst`, which currently
/// describes `dst_rows` rows.
///
/// Concatenating chunks cannot leave this implicit. A validity bitmap is not
/// carried by the column, so appending values alone leaves a bitmap describing
/// only the FIRST chunk while the column grows past it — and every row beyond
/// it then reads as null. A chunk with no bitmap is all-valid, so a column that
/// only gains nulls in a later chunk still needs one backfilled for the rows
/// already appended.
///
/// Latent until `IBEX_CHUNK_ROWS` existed: production never emitted a second
/// chunk, so no concat ever ran with one. It surfaced as an `order` over a
/// null-bearing column disagreeing with itself between the serial and parallel
/// gather, both of which were faithfully reading a bitmap that had run out.
void append_validity(std::optional<ValidityBitmap>& dst, std::size_t dst_rows,
                     const std::optional<ValidityBitmap>& src, std::size_t src_rows) {
    if (!dst.has_value() && !src.has_value()) {
        return;  // both all-valid — the common case needs no bitmap at all
    }
    if (!dst.has_value()) {
        ValidityBitmap filled;
        filled.reserve(dst_rows + src_rows);
        for (std::size_t r = 0; r < dst_rows; ++r) {
            filled.push_back(true);
        }
        dst = std::move(filled);
    }
    dst->reserve(dst_rows + src_rows);
    for (std::size_t r = 0; r < src_rows; ++r) {
        dst->push_back(src.has_value() ? (*src)[r] : true);
    }
}

/// Morsel identity survives every one-input/one-output parallel-map operator.
/// It is intentionally separate from Table metadata: sequence/row offset are
/// executor transport state, never user-visible table properties.
struct ChunkIdentity {
    std::uint64_t sequence = 0;
    std::size_t row_offset = 0;
};

[[nodiscard]] auto chunk_identity_of(const Chunk& chunk) -> ChunkIdentity {
    return ChunkIdentity{.sequence = chunk.sequence, .row_offset = chunk.row_offset};
}

[[nodiscard]] auto table_to_chunk(Table table, ChunkIdentity identity) -> Chunk {
    auto chunk = table_to_chunk(std::move(table));
    chunk.sequence = identity.sequence;
    chunk.row_offset = identity.row_offset;
    return chunk;
}


}  // namespace

/// Per-chunk filter: pulls a chunk from the child, wraps it as a `Table`,
/// reuses the existing `filter_table` predicate evaluator, and emits the
/// filtered columns as the next chunk. Chunks that filter to zero rows
/// are skipped — the operator loops until it has a non-empty chunk or
/// the child stream ends.
// Streaming operator classes (internal linkage, see note below).
namespace {

/// Keeps one zero-row chunk back so an operator that rejects every row still
/// emits its schema.
///
/// A stream carries its schema in its chunks, so an operator that emits no chunk
/// emits no schema either: the result materializes as a table with no columns at
/// all, and anything downstream that names a column — a join looking for its key,
/// a filter for the value it compares — fails with "unknown column" on what is
/// really just an empty input.
///
/// Row filters are where that bites, since they are what can reject everything.
/// Each skips its empty chunks (forwarding them would be pure overhead), so this
/// holds the first one back and releases it at end of stream if nothing else was
/// ever emitted.
class SchemaCarrier {
   public:
    /// Offer a zero-row result as the schema of last resort.
    void hold(Table&& empty, ChunkIdentity identity = {}) {
        if (!held_.has_value() && !empty.columns.empty()) {
            held_ = Held{.table = std::move(empty), .identity = identity};
        }
    }
    void emitted() { emitted_ = true; }
    /// The held chunk — once, and only if nothing else was ever emitted.
    [[nodiscard]] auto release() -> std::optional<Chunk> {
        if (emitted_ || !held_.has_value()) {
            return std::nullopt;
        }
        emitted_ = true;
        return table_to_chunk(std::move(held_->table), held_->identity);
    }

   private:
    struct Held {
        Table table;
        ChunkIdentity identity;
    };
    std::optional<Held> held_;
    bool emitted_ = false;
};

class ChunkedFilterOperator final : public Operator {
   public:
    ChunkedFilterOperator(OperatorPtr child, const ir::Expr* predicate,
                          const ScalarRegistry* scalars,
                          kernel::FilterChunkRoute route = kernel::FilterChunkRoute::Auto,
                          bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          scalars_(scalars),
          route_(route),
          preserve_empty_morsels_(preserve_empty_morsels) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return schema_.release();
            }
            Chunk input = std::move(*chunk_res.value());
            const auto identity = chunk_identity_of(input);
            auto filtered = kernel::filter_chunk(std::move(input), *predicate_, scalars_, route_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->columns.empty() && filtered->rows() == 0) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{std::move(filtered.value())};
                }
                schema_.hold(chunk_to_table(std::move(filtered.value())), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{std::move(filtered.value())};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const ScalarRegistry* scalars_;
    kernel::FilterChunkRoute route_;
    bool preserve_empty_morsels_ = false;
    SchemaCarrier schema_;
};

/// Per-chunk project: resolves its metadata map over a ChunkView, shares the
/// selected column entries, and forwards it. Stateless and order preserving;
/// no inter-chunk coordination or Table round-trip is needed.
class ChunkedProjectOperator final : public Operator {
   public:
    ChunkedProjectOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* columns)
        : child_(std::move(child)), columns_(columns) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        auto projected = kernel::project_chunk(*chunk_res.value(), *columns_);
        if (!projected.has_value()) {
            return std::unexpected(std::move(projected.error()));
        }
        return std::optional<Chunk>{std::move(projected.value())};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* columns_;
};

/// Fused filter→project: computes the filter mask once per chunk and gathers
/// only the projected columns. Skips materializing columns that the surrounding
/// `select` would discard, which is the main win over running `Filter` then
/// `Project` as independent chunked operators.
class ChunkedFilterProjectOperator final : public Operator {
   public:
    ChunkedFilterProjectOperator(OperatorPtr child, const ir::Expr* predicate,
                                 const std::vector<ir::ColumnRef>* columns,
                                 const ScalarRegistry* scalars,
                                 kernel::FilterChunkRoute route = kernel::FilterChunkRoute::Auto,
                                 bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          columns_(columns),
          scalars_(scalars),
          route_(route),
          preserve_empty_morsels_(preserve_empty_morsels) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return schema_.release();
            }
            Chunk input = std::move(*chunk_res.value());
            const auto identity = chunk_identity_of(input);
            auto out = kernel::filter_project_chunk(std::move(input), *predicate_, *columns_,
                                                    scalars_, route_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (!out->columns.empty() && out->rows() == 0) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{std::move(out.value())};
                }
                schema_.hold(chunk_to_table(std::move(out.value())), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{std::move(out.value())};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::ColumnRef>* columns_;
    const ScalarRegistry* scalars_;
    kernel::FilterChunkRoute route_;
    bool preserve_empty_morsels_ = false;
    SchemaCarrier schema_;
};

/// Fused filter→head(n): pushes the row limit into the per-chunk filter so
/// gather stops as soon as `n` surviving rows are produced, and short-circuits
/// pulling from the child once the limit is reached. Only used for global
/// `head` (no group_by); grouped head still uses ChunkedHeadOperator.
class ChunkedFilterHeadOperator final : public Operator {
   public:
    ChunkedFilterHeadOperator(OperatorPtr child, const ir::Expr* predicate, std::size_t count,
                              const ScalarRegistry* scalars)
        : child_(std::move(child)),
          predicate_(predicate),
          count_(count),
          remaining_(count),
          scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            if (remaining_ == 0) {
                done_ = true;
                return std::optional<Chunk>{};
            }
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                done_ = true;
                return schema_.release();
            }
            auto out = kernel::filter_limit_chunk(std::move(*chunk_res.value()), *predicate_,
                                                  remaining_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            const std::size_t produced = out->rows();
            if (!out->columns.empty() && produced == 0) {
                schema_.hold(chunk_to_table(std::move(out.value())));
                continue;
            }
            remaining_ -= produced;
            if (remaining_ == 0) {
                done_ = true;
            }
            (void)count_;
            schema_.emitted();
            return std::optional<Chunk>{std::move(out.value())};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    std::size_t count_;
    std::size_t remaining_;
    const ScalarRegistry* scalars_;
    bool done_ = false;
    SchemaCarrier schema_;
};

/// Fused `Tail(Filter(x))`: filters each incoming chunk, then keeps only the
/// last `n` matching rows in a rolling buffer so we never hold the full
/// filtered result in memory (the prior materializing path built the entire
/// filter output and sliced the last `n`). We must still drain the child —
/// `tail` is inherently a read-all operator — but peak memory is O(n) rather
/// than O(matches). Only wired for global `tail` (empty group_by); grouped
/// tail still goes through the materializing path.
class ChunkedFilterTailOperator final : public Operator {
   public:
    ChunkedFilterTailOperator(OperatorPtr child, const ir::Expr* predicate, std::size_t count,
                              const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), count_(count), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            auto filtered =
                kernel::filter_chunk(std::move(*chunk_res.value()), *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (filtered->columns.empty()) {
                continue;
            }
            if (filtered->rows() == 0) {
                schema_.hold(chunk_to_table(std::move(filtered.value())));
                continue;
            }
            buffered_rows_ += filtered->rows();
            buffered_.push_back(chunk_to_table(std::move(filtered.value())));
            trim_to_limit();
        }
        done_ = true;
        if (buffered_.empty()) {
            return schema_.release();
        }
        schema_.emitted();
        if (buffered_.size() == 1) {
            return std::optional<Chunk>{table_to_chunk(std::move(buffered_.front()))};
        }
        auto concat = concat_buffered();
        if (!concat.has_value()) {
            return std::unexpected(std::move(concat.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(concat.value()))};
    }

   private:
    // Drop or slice from the front of `buffered_` until its combined row count
    // is ≤ count_. Full-chunk drops are cheap (pointer-level pop); only one
    // partial slice (gather_rows on the front) is ever needed per trim.
    auto trim_to_limit() -> void {
        while (buffered_rows_ > count_ && !buffered_.empty()) {
            const std::size_t front_rows = buffered_.front().rows();
            if (buffered_rows_ - front_rows >= count_) {
                buffered_rows_ -= front_rows;
                buffered_.pop_front();
                continue;
            }
            const std::size_t excess = buffered_rows_ - count_;
            const std::size_t keep = front_rows - excess;
            std::vector<std::size_t> idx;
            idx.reserve(keep);
            for (std::size_t i = excess; i < front_rows; ++i) {
                idx.push_back(i);
            }
            buffered_.front() = gather_rows(buffered_.front(), idx);
            buffered_rows_ = count_;
            break;
        }
    }

    auto concat_buffered() -> std::expected<Table, std::string> {
        Table out = std::move(buffered_.front());
        buffered_.pop_front();
        const std::size_t n_cols = out.columns.size();
        while (!buffered_.empty()) {
            Table& src_t = buffered_.front();
            if (src_t.columns.size() != n_cols) {
                return std::unexpected("tail: chunk schema mismatch (column count)");
            }
            // Taken BEFORE any column is appended: `rows()` reads column 0, so
            // reading it inside the loop would report the new length to every
            // column after the first.
            const std::size_t dst_rows = out.rows();
            const std::size_t src_rows = src_t.rows();
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (src_t.columns[i].name != out.columns[i].name) {
                    return std::unexpected("tail: chunk schema mismatch (column name)");
                }
                if (src_t.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("tail: chunk schema mismatch (column type)");
                }
                append_validity(out.columns[i].validity, dst_rows, src_t.columns[i].validity,
                                src_rows);
                append_column_values(out.mutable_column(i), *src_t.columns[i].column);
            }
            buffered_.pop_front();
        }
        return out;
    }

    OperatorPtr child_;
    const ir::Expr* predicate_;
    std::size_t count_;
    const ScalarRegistry* scalars_;
    std::deque<Table> buffered_;
    std::size_t buffered_rows_ = 0;
    bool done_ = false;
    SchemaCarrier schema_;
};

/// Fused filter→update→project: evaluates the predicate per chunk, gathers
/// only the columns needed (referenced by any update expression, or in the
/// final projection but not produced by the update), then runs the row-local
/// update and final projection. Skips materializing columns the surrounding
/// select would discard — the same win as ChunkedFilterProjectOperator, but
/// allowing computed fields in the select.
class ChunkedFilterUpdateProjectOperator final : public Operator {
   public:
    ChunkedFilterUpdateProjectOperator(
        OperatorPtr child, const ir::Expr* predicate, const std::vector<ir::FieldSpec>* fields,
        const std::vector<ir::ColumnRef>* project_columns,
        std::vector<ir::ColumnRef> gather_columns, const ScalarRegistry* scalars,
        const ExternRegistry* externs, const ExecutionContext& exec,
        kernel::FilterChunkRoute route = kernel::FilterChunkRoute::Auto,
        bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          fields_(fields),
          project_columns_(project_columns),
          gather_columns_(std::move(gather_columns)),
          scalars_(scalars),
          externs_(externs),
          exec_(&exec),
          route_(route),
          preserve_empty_morsels_(preserve_empty_morsels) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return schema_.release();
            }
            Chunk input = std::move(*chunk_res.value());
            const auto identity = chunk_identity_of(input);
            auto projected = kernel::filter_update_project_chunk(
                std::move(input), *predicate_, *fields_, *project_columns_, gather_columns_,
                scalars_, externs_, *exec_, route_);
            if (!projected.has_value()) {
                return std::unexpected(std::move(projected.error()));
            }
            const bool empty = !projected->columns.empty() && projected->rows() == 0;
            // An empty chunk still runs the update and the projection, cheaply,
            // because the schema it has to carry is the one they produce.
            if (empty) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{std::move(projected.value())};
                }
                schema_.hold(chunk_to_table(std::move(projected.value())), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{std::move(projected.value())};
        }
    }

   private:
    SchemaCarrier schema_;
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::FieldSpec>* fields_;
    const std::vector<ir::ColumnRef>* project_columns_;
    std::vector<ir::ColumnRef> gather_columns_;
    const ScalarRegistry* scalars_;
    const ExternRegistry* externs_;
    const ExecutionContext* exec_;
    kernel::FilterChunkRoute route_;
    bool preserve_empty_morsels_ = false;
};

class ChunkedRenameOperator final : public Operator {
   public:
    ChunkedRenameOperator(OperatorPtr child, const std::vector<ir::RenameSpec>* renames)
        : child_(std::move(child)), renames_(renames) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        const Chunk input = std::move(*chunk_res.value());
        const kernel::ChunkView view(input);
        const ir::ColumnNameMap names(*renames_);
        std::vector<std::string_view> input_names;
        input_names.reserve(view.columns());
        for (std::size_t pos = 0; pos < view.columns(); ++pos) {
            input_names.push_back(view.entry(pos).name);
        }
        if (auto valid = names.validate_input(input_names); !valid.has_value()) {
            return std::unexpected(valid.error());
        }
        std::vector<kernel::MappedChunkColumn> map;
        map.reserve(view.columns());
        for (std::size_t pos = 0; pos < view.columns(); ++pos) {
            std::string name(names.output_name(view.entry(pos).name));
            map.push_back({.source_position = pos, .name = std::move(name)});
        }
        const auto props = TableProperties::derive(
            view.properties(),
            [&](const std::string& name) -> KeyFate {
                return KeyFate::kept(std::string(names.output_name(name)));
            },
            RowTransform::Preserve);
        return std::optional<Chunk>{kernel::map_chunk(view, map, props)};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::RenameSpec>* renames_;
};

using ir::collect_expr_column_refs;

/// Per-chunk update for row-local field expressions. `build_operator()` only
/// routes here when all of the UpdateNode's field expressions are row-local
/// (per `is_row_local_update_expr`) and there are no tuple_fields or
/// group_by clauses — the subset where running `update_table` per chunk is
/// equivalent to running it on the materialized table.
class ChunkedUpdateOperator final : public Operator {
   public:
    ChunkedUpdateOperator(OperatorPtr child, const std::vector<ir::FieldSpec>* fields,
                          const ScalarRegistry* scalars, const ExternRegistry* externs,
                          const ExecutionContext& exec)
        : child_(std::move(child)),
          fields_(fields),
          scalars_(scalars),
          externs_(externs),
          exec_(&exec) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        auto out = kernel::update_row_local_chunk(std::move(*chunk_res.value()), *fields_, scalars_,
                                                  externs_, *exec_);
        if (!out.has_value()) {
            return std::unexpected(std::move(out.error()));
        }
        return std::optional<Chunk>{std::move(out.value())};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::FieldSpec>* fields_;
    const ScalarRegistry* scalars_;
    const ExternRegistry* externs_;
    const ExecutionContext* exec_;
};

class ChunkedHeadOperator final : public Operator {
   public:
    ChunkedHeadOperator(OperatorPtr child, std::size_t count,
                        const std::vector<ir::ColumnRef>* group_by)
        : child_(std::move(child)), count_(count), group_by_(group_by), remaining_(count) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        if (count_ == 0 && group_by_->empty()) {
            done_ = true;
            return std::optional<Chunk>{};
        }

        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                done_ = true;
                return std::optional<Chunk>{};
            }

            Chunk chunk = std::move(*chunk_res.value());
            if (count_ == 0) {
                done_ = true;
                const Table t = chunk_to_table(std::move(chunk));
                const std::vector<std::size_t> idx;
                return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
            }

            if (group_by_->empty()) {
                return take_global_rows(std::move(chunk));
            }

            auto filtered = take_grouped_rows(std::move(chunk));
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->has_value()) {
                continue;
            }
            return filtered;
        }
    }

   private:
    auto take_global_rows(Chunk chunk) -> std::expected<std::optional<Chunk>, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows <= remaining_) {
            remaining_ -= rows;
            if (remaining_ == 0) {
                done_ = true;
            }
            return std::optional<Chunk>{std::move(chunk)};
        }

        const Table t = chunk_to_table(std::move(chunk));
        std::vector<std::size_t> idx(remaining_);
        std::iota(idx.begin(), idx.end(), std::size_t{0});
        remaining_ = 0;
        done_ = true;
        return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
    }

    auto take_grouped_rows(Chunk chunk) -> std::expected<std::optional<Chunk>, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return std::optional<Chunk>{std::move(chunk)};
        }

        Table t = chunk_to_table(std::move(chunk));
        std::vector<std::size_t> idx;
        idx.reserve(std::min(rows, count_ * std::max<std::size_t>(1, group_by_->size())));

        for (std::size_t row = 0; row < rows; ++row) {
            Key key;
            key.values.reserve(group_by_->size());
            for (const auto& ref : *group_by_) {
                const auto* entry = t.find_entry(ref.name);
                if (entry == nullptr) {
                    return std::unexpected("head group-by column not found: " + ref.name +
                                           " (available: " + format_columns(t) + ")");
                }
                push_key_value(key, *entry, row);
            }
            auto& seen = seen_counts_[key];
            if (seen >= count_) {
                continue;
            }
            ++seen;
            idx.push_back(row);
        }

        if (idx.empty()) {
            return std::optional<Chunk>{};
        }
        if (idx.size() == rows) {
            return std::optional<Chunk>{table_to_chunk(std::move(t))};
        }
        return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
    }

    OperatorPtr child_;
    std::size_t count_;
    const std::vector<ir::ColumnRef>* group_by_;
    std::size_t remaining_;
    bool done_ = false;
    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> seen_counts_;
};

}  // namespace
auto compare_scalar_for_order(const ScalarValue& lhs, const ScalarValue& rhs) -> int {
    return std::visit(
        [](const auto& l, const auto& r) -> int {
            using L = std::decay_t<decltype(l)>;
            using R = std::decay_t<decltype(r)>;
            if constexpr (std::is_same_v<L, R>) {
                if (l < r) {
                    return -1;
                }
                if (r < l) {
                    return 1;
                }
                return 0;
            } else {
                invariant_violation("compare_scalar_for_order: mismatched scalar types");
            }
        },
        lhs, rhs);
}

auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                          const std::vector<ir::ColumnRef>& group_by, const ExecutionContext& exec)
    -> std::expected<ComputedColumn, std::string> {
    const std::size_t rows = input.rows();
    auto order_keys = ordering_keys_for_table(input, rank.order_keys);
    if (order_keys.empty()) {
        return std::unexpected("rank(): expected at least one order key");
    }

    struct ResolvedKey {
        const ColumnEntry* entry = nullptr;
        bool ascending = true;
    };
    std::vector<ResolvedKey> resolved_keys;
    resolved_keys.reserve(order_keys.size());
    for (const auto& key : order_keys) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        resolved_keys.push_back(ResolvedKey{.entry = entry, .ascending = key.ascending});
    }

    std::vector<const ColumnEntry*> group_entries;
    group_entries.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): group column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        group_entries.push_back(entry);
    }

    // Pre-flatten every group/order key into a typed array so the hot sort
    // comparator does plain vector indexing instead of per-comparison variant
    // dispatch. Crucially, string keys are flattened to string_view (views into
    // the column's storage) rather than the std::string that scalar_at_for_order
    // allocates on every access — without this, sorting 4M rows by a string key
    // performs hundreds of millions of heap allocations.
    constexpr std::uint64_t kSignFlip = std::uint64_t{1} << 63U;
    enum class FlatKind : std::uint8_t { I64, F64, Str };
    struct FlatCol {
        FlatKind kind = FlatKind::I64;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos / bool, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        const ValidityBitmap* validity = nullptr;
        bool ascending = true;
    };

    auto flatten = [&](const ColumnEntry* entry, bool ascending) -> FlatCol {
        FlatCol fc;
        fc.ascending = ascending;
        if (entry->validity.has_value()) {
            fc.validity = &*entry->validity;
        }
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (auto v : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(v) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                    fc.kind = FlatKind::F64;
                    fc.f64.assign(col.begin(), col.end());
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (const auto& d : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(d.days) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (const auto& ts : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(ts.nanos) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fc.u64.push_back(static_cast<std::uint64_t>(col[i] ? 1 : 0) ^ kSignFlip);
                } else {
                    // Column<std::string> or categorical: view, no allocation.
                    fc.kind = FlatKind::Str;
                    fc.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fc.str.push_back(col[i]);
                }
            },
            *entry->column);
        return fc;
    };

    // Built on demand. The radix fast path below resolves a Categorical or
    // numeric group key without ever comparing group values, and flattening a
    // string group key eagerly costs a 128MB array of string_views at 8M rows
    // — built only to be thrown away. Only the comparison-sort fallback and
    // the single-string-key group id loop actually read it.
    std::vector<FlatCol> group_flat;
    auto ensure_group_flat = [&] {
        if (!group_flat.empty() || group_entries.empty()) {
            return;
        }
        group_flat.reserve(group_entries.size());
        for (const auto* entry : group_entries) {
            group_flat.push_back(flatten(entry, /*ascending=*/true));
        }
    };

    std::vector<FlatCol> order_flat;
    order_flat.reserve(resolved_keys.size());
    for (const auto& key : resolved_keys)
        order_flat.push_back(flatten(key.entry, key.ascending));

    auto flat_is_null = [](const FlatCol& fc, std::size_t row) -> bool {
        return fc.validity != nullptr && !(*fc.validity)[row];
    };
    // Three-way compare of a single flat key; sign-flipped u64 compares as signed,
    // string_view as lexicographic — both match compare_scalar_for_order.
    auto flat_cmp = [](const FlatCol& fc, std::size_t lhs, std::size_t rhs) -> int {
        switch (fc.kind) {
            case FlatKind::I64: {
                auto l = fc.u64[lhs];
                auto r = fc.u64[rhs];
                return (l > r) - (l < r);
            }
            case FlatKind::F64: {
                auto l = fc.f64[lhs];
                auto r = fc.f64[rhs];
                return (l > r) - (l < r);
            }
            case FlatKind::Str: {
                const auto& l = fc.str[lhs];
                const auto& r = fc.str[rhs];
                return (l > r) - (l < r);
            }
        }
        return 0;
    };

    auto is_null_row_for_keys = [&](std::size_t row) -> bool {
        return std::ranges::any_of(order_flat,
                                   [&](const FlatCol& fc) { return flat_is_null(fc, row); });
    };

    auto same_group = [&](std::size_t lhs, std::size_t rhs) -> bool {
        return std::ranges::all_of(group_flat, [&](const FlatCol& fc) {
            const bool ln = flat_is_null(fc, lhs);
            const bool rn = flat_is_null(fc, rhs);
            if (ln != rn) {
                return false;
            }
            if (ln) {
                return true;
            }
            return flat_cmp(fc, lhs, rhs) == 0;
        });
    };

    auto equal_rank_keys = [&](std::size_t lhs, std::size_t rhs) -> bool {
        const bool lhs_null = is_null_row_for_keys(lhs);
        const bool rhs_null = is_null_row_for_keys(rhs);
        if (lhs_null || rhs_null) {
            return lhs_null == rhs_null;
        }
        return std::ranges::all_of(order_flat,
                                   [&](const FlatCol& fc) { return flat_cmp(fc, lhs, rhs) == 0; });
    };

    std::vector<std::size_t> idx;

    // Populated by the radix fast path when group_entries is non-empty: group g's
    // rows are idx[radix_group_starts[g]..radix_group_starts[g+1]). Used by the
    // rank sweep to avoid O(n) same_group calls.
    std::vector<std::size_t> radix_group_starts;

    // Fast path: a single non-null numeric order key with non-null group keys.
    // Radix-argsort by the order value (no O(n log n) string/comparison sort),
    // then a stable counting-sort by hashed group id makes each group contiguous
    // while preserving the within-group order from the radix pass. Falls back to
    // the comparison sort below for string order keys, multiple order keys, or
    // any nullable key (where na_option / null-group semantics need the general
    // path). This is the hot path for `rank(x) by g` over large frames.
    const bool radix_order = order_flat.size() == 1 && order_flat[0].kind != FlatKind::Str &&
                             order_flat[0].validity == nullptr &&
                             std::ranges::all_of(group_entries, [](const ColumnEntry* e) {
                                 return !e->validity.has_value();
                             });
    if (radix_order) {
        const FlatCol& ok = order_flat[0];
        std::vector<std::uint64_t> codes;
        if (ok.kind == FlatKind::F64) {
            codes.resize(rows);
            for (std::size_t i = 0; i < rows; ++i)
                codes[i] = double_to_sortable_u64(ok.f64[i]);
        } else {
            codes = ok.u64;  // already sign-flipped to order-preserving u64
        }
        // Invert the order-preserving codes for a descending key so an ascending
        // radix sort yields descending order.
        if (!ok.ascending) {
            for (auto& c : codes)
                c = ~c;
        }
        if (group_entries.empty()) {
            // Ungrouped: one run, so there is nothing to bucket and the whole
            // table is the slice.
            auto sort_result = radix_sort_u64_asc(std::move(codes), rows);
            idx.resize(rows);
            std::visit(
                [&](const auto& sorted) {
                    for (std::size_t i = 0; i < rows; ++i)
                        idx[i] = sorted[i];
                },
                sort_result);
        } else {
            // Assign group IDs using the already-flattened group_flat arrays (string_view,
            // no per-row allocation) instead of calling scalar_from_column (which
            // heap-allocates std::string for string columns on every row).
            std::vector<std::uint32_t> group_id(rows);
            std::uint32_t ngroups = 0;
            const Column<Categorical>* cat_group =
                group_entries.size() == 1
                    ? std::get_if<Column<Categorical>>(group_entries[0]->column.get())
                    : nullptr;
            if (cat_group != nullptr) {
                // A Categorical group key carries a dense code per row, so
                // resolve each CODE to a group id once instead of hashing the
                // dictionary string it stands for on every row. `flatten` gives
                // group keys string_views because order keys need lexicographic
                // comparison; grouping only needs equality, which the codes
                // already answer.
                //
                // Still resolved through the dictionary text rather than using
                // the code directly: nothing forbids a dictionary from carrying
                // the same string under two codes (a remap can produce one), and
                // treating those as different groups would split a group in two.
                constexpr std::uint32_t kUnset = std::numeric_limits<std::uint32_t>::max();
                const auto& dict = cat_group->dictionary();
                std::vector<std::uint32_t> code_gid(dict.size(), kUnset);
                robin_hood::unordered_flat_map<std::string_view, std::uint32_t> by_text;
                for (std::size_t r = 0; r < rows; ++r) {
                    const auto code = static_cast<std::size_t>(cat_group->code_at(r));
                    std::uint32_t gid = code_gid[code];
                    if (gid == kUnset) {
                        // Lazily, in row order, so group ids stay in order of
                        // first appearance exactly as the hashing loop assigns
                        // them.
                        auto [it, inserted] =
                            by_text.emplace(std::string_view{dict[code]}, ngroups);
                        if (inserted) {
                            ++ngroups;
                        }
                        gid = it->second;
                        code_gid[code] = gid;
                    }
                    group_id[r] = gid;
                }
            } else if (ensure_group_flat();
                       group_flat.size() == 1 && group_flat[0].kind == FlatKind::Str) {
                // Single string group key: hash string_views directly.
                robin_hood::unordered_flat_map<std::string_view, std::uint32_t> group_index;
                const auto& sv = group_flat[0].str;
                for (std::size_t r = 0; r < rows; ++r) {
                    auto [it, inserted] = group_index.emplace(sv[r], ngroups);
                    if (inserted)
                        ++ngroups;
                    group_id[r] = it->second;
                }
            } else {
                // General: build a flat key from each group_flat column without going
                // through ScalarValue. I64/F64 columns use their numeric values directly;
                // string columns still hash as string_view (the Key uses std::string only
                // for the fallback path which doesn't reach here).
                robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
                for (std::size_t r = 0; r < rows; ++r) {
                    Key key;
                    key.values.reserve(group_entries.size());
                    for (const auto* entry : group_entries)
                        push_key_value(key, *entry, r);
                    auto [it, inserted] = group_index.emplace(std::move(key), ngroups);
                    if (inserted)
                        ++ngroups;
                    group_id[r] = it->second;
                }
            }
            // Bucket rows by group, then sort each group's run where it sits.
            //
            // The obvious structure — sort all `rows` globally, then stable
            // counting-sort the result by group — walks a 64MB permutation
            // twice and cannot be split. Bucketing first makes each run
            // cache-resident and independent, so the runs sort concurrently
            // and the counting pass over the sorted order disappears. Measured
            // on a standalone harness at 8M rows, prices-shaped keys:
            //
            //   groups     global+count   bucket+per-group   ...threaded
            //        1          322ms            251ms            247ms
            //      252          346ms            150ms             50ms
            //   100000          344ms            190ms            100ms
            //
            // The key travels with the row so each run holds its own keys
            // contiguously; rows enter a run in ascending row order and the
            // slice sort is stable, so ties break by row exactly as the global
            // stable sort broke them.
            std::vector<std::size_t> cnt(static_cast<std::size_t>(ngroups) + 1, 0);
            for (std::size_t r = 0; r < rows; ++r)
                ++cnt[static_cast<std::size_t>(group_id[r]) + 1];
            for (std::size_t g = 0; g < ngroups; ++g)
                cnt[g + 1] += cnt[g];
            radix_group_starts =
                cnt;  // group g spans [radix_group_starts[g], radix_group_starts[g+1])

            idx.resize(rows);
            std::vector<std::uint64_t> run_keys(rows);
            {
                std::vector<std::size_t> cursor(cnt.begin(), cnt.end() - 1);
                for (std::size_t r = 0; r < rows; ++r) {
                    const std::size_t at = cursor[group_id[r]]++;
                    idx[at] = r;
                    run_keys[at] = codes[r];
                }
            }

            const std::size_t sort_workers =
                ngroups >= 2 ? group_barrier_worker_count(exec, rows) : 0;
            if (sort_workers >= 2) {
                std::atomic<std::size_t> next{0};
                auto batch = process_worker_pool().submit(sort_workers, [&](std::size_t) noexcept {
                    RadixSliceScratch scratch;
                    while (true) {
                        const std::size_t g = next.fetch_add(1, std::memory_order_relaxed);
                        if (g >= ngroups) {
                            return;
                        }
                        const std::size_t lo = radix_group_starts[g];
                        sort_key_index_slice(run_keys.data() + lo, idx.data() + lo,
                                             radix_group_starts[g + 1] - lo, scratch);
                    }
                });
                batch.wait();
            } else {
                RadixSliceScratch scratch;
                for (std::size_t g = 0; g < ngroups; ++g) {
                    const std::size_t lo = radix_group_starts[g];
                    sort_key_index_slice(run_keys.data() + lo, idx.data() + lo,
                                         radix_group_starts[g + 1] - lo, scratch);
                }
            }
        }
    } else {
        ensure_group_flat();
        idx.resize(rows);
        std::iota(idx.begin(), idx.end(), std::size_t{0});
        // pdqsort is unstable, but the comparator's `lhs < rhs` tiebreak makes the
        // order total, so the result matches a stable sort.
        pdqsort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
            // Order groups first (nulls sort first, ascending by value) so that rows
            // of the same group are contiguous for the sweep below.
            for (const auto& fc : group_flat) {
                const bool ln = flat_is_null(fc, lhs);
                const bool rn = flat_is_null(fc, rhs);
                if (ln != rn) {
                    return ln;  // null sorts first
                }
                if (ln) {
                    continue;
                }
                const int cmp = flat_cmp(fc, lhs, rhs);
                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            // Within a group, order by the rank keys (honouring na_option).
            const bool lhs_null = is_null_row_for_keys(lhs);
            const bool rhs_null = is_null_row_for_keys(rhs);
            if (lhs_null || rhs_null) {
                if (lhs_null != rhs_null) {
                    if (rank.na_option == ir::RankNaOption::Top) {
                        return lhs_null;
                    }
                    // `Bottom` and `Keep` both put nulls after the values.
                    // `Keep` overwrites their ranks with null further down, so
                    // where they sit does not show — but it has to be SOMEWHERE
                    // consistent. Ordering them by row index instead made the
                    // comparator intransitive: with rows 7, null, 3 it said
                    // 7 < null and null < 3 and 3 < 7, and a sort given a
                    // comparator like that produces an arbitrary permutation,
                    // which is how every rank came out in row order.
                    //
                    // Putting them last also keeps the ordinals right: nulls
                    // consume rank positions as they are walked, so they must
                    // be walked after every value.
                    return !lhs_null;
                }
                return lhs < rhs;
            }
            for (const auto& fc : order_flat) {
                const int cmp = flat_cmp(fc, lhs, rhs);
                if (cmp != 0) {
                    return fc.ascending ? (cmp < 0) : (cmp > 0);
                }
            }
            return lhs < rhs;
        });
    }

    // The tie scan below is the hot loop of the whole function: one call per
    // row. `equal_rank_keys` answers it in full generality — an any_of for
    // nulls, then an all_of of three-way compares each dispatching on the
    // key's kind — none of which a single non-null numeric key needs. That is
    // exactly the shape the radix path above requires, so when it ran the
    // comparison is one array read per side.
    const FlatCol* solo_key = radix_order ? order_flat.data() : nullptr;
    auto same_rank_keys = [&](std::size_t lhs, std::size_t rhs) -> bool {
        if (solo_key != nullptr) {
            return solo_key->kind == FlatKind::F64 ? solo_key->f64[lhs] == solo_key->f64[rhs]
                                                   : solo_key->u64[lhs] == solo_key->u64[rhs];
        }
        return equal_rank_keys(lhs, rhs);
    };

    std::vector<double> rank_values(rows, 0.0);
    // Only `na_option = keep` ever writes this, and only a null key makes it
    // write. Allocating it regardless cost a bitmap per call for every rank
    // that has no nulls to keep.
    ValidityBitmap validity;
    if (rank.na_option == ir::RankNaOption::Keep) {
        validity.resize(rows, true);
    }

    // When the radix fast path ran, group boundaries are already known from the
    // counting sort. Iterate the groups directly: walk radix_group_starts as a
    // cursor so each group_end lookup is O(1) with no scanning. The pdqsort
    // fallback leaves radix_group_starts empty and uses the same_group per-row
    // scan (needed for nulls / string order keys / multi-key cases).
    std::size_t gs_cursor = 0;  // index into radix_group_starts for the fast path

    // One group's ranks. Groups share no rows, and every write below is to
    // `rank_values[idx[k]]` — a row this group owns — so groups are
    // independent and the loop over them can be split.
    auto rank_group = [&](std::size_t pos, std::size_t group_end) {
        std::size_t dense_rank = 1;
        std::size_t ordinal = 1;
        std::size_t i = pos;
        while (i < group_end) {
            std::size_t tie_end = i + 1;
            while (tie_end < group_end && same_rank_keys(idx[i], idx[tie_end])) {
                ++tie_end;
            }

            // The radix path admits no nullable order key, so there is nothing
            // to test when it ran.
            const bool null_tie = solo_key == nullptr && is_null_row_for_keys(idx[i]);
            double assigned = 0.0;
            if (null_tie && rank.na_option == ir::RankNaOption::Keep) {
                for (std::size_t k = i; k < tie_end; ++k) {
                    validity.set(idx[k], false);
                }
            } else {
                switch (rank.method) {
                    case ir::RankMethod::Average: {
                        const auto first_rank = static_cast<double>(ordinal);
                        const auto last_rank = static_cast<double>(ordinal + (tie_end - i) - 1);
                        assigned = (first_rank + last_rank) / 2.0;
                        break;
                    }
                    case ir::RankMethod::Min:
                    case ir::RankMethod::Dense:
                        assigned = static_cast<double>(
                            rank.method == ir::RankMethod::Dense ? dense_rank : ordinal);
                        break;
                    case ir::RankMethod::Max:
                        assigned = static_cast<double>(ordinal + (tie_end - i) - 1);
                        break;
                    case ir::RankMethod::First:
                        break;
                }
                if (rank.method == ir::RankMethod::First) {
                    for (std::size_t k = i; k < tie_end; ++k) {
                        auto value = static_cast<double>(ordinal + (k - i));
                        rank_values[idx[k]] =
                            rank.pct ? value / static_cast<double>(group_end - pos) : value;
                    }
                } else {
                    if (rank.pct) {
                        assigned /= static_cast<double>(group_end - pos);
                    }
                    for (std::size_t k = i; k < tie_end; ++k) {
                        rank_values[idx[k]] = assigned;
                    }
                }
            }

            ordinal += (tie_end - i);
            if (!null_tie || rank.na_option != ir::RankNaOption::Keep) {
                ++dense_rank;
            }
            i = tie_end;
        }
    };

    // Split the groups when the radix path ran. Two conditions make that safe
    // and are exactly what the path already guarantees: the group boundaries
    // are known up front (no serial scan to find them), and there is no
    // nullable order key — so nothing writes the shared validity bitmap, whose
    // neighbouring bits share a word and could not be written concurrently.
    // The result does not depend on the split: each group's ranks are computed
    // from its own rows and scattered to their own row positions.
    const std::size_t group_count = radix_group_starts.empty() ? 0 : radix_group_starts.size() - 1;
    const std::size_t sweep_workers =
        (solo_key != nullptr && group_count >= 2) ? group_barrier_worker_count(exec, rows) : 0;
    if (sweep_workers >= 2) {
        std::atomic<std::size_t> cursor{0};
        auto batch = process_worker_pool().submit(sweep_workers, [&](std::size_t) noexcept {
            while (true) {
                const std::size_t g = cursor.fetch_add(1, std::memory_order_relaxed);
                if (g >= group_count) {
                    return;
                }
                rank_group(radix_group_starts[g], radix_group_starts[g + 1]);
            }
        });
        batch.wait();
    } else {
        std::size_t pos = 0;
        while (pos < rows) {
            std::size_t group_end = 0;
            if (!radix_group_starts.empty()) {
                ++gs_cursor;  // advance past the current group's start
                group_end = radix_group_starts[gs_cursor];
            } else {
                group_end = pos + 1;
                while (group_end < rows && same_group(idx[pos], idx[group_end]))
                    ++group_end;
            }
            rank_group(pos, group_end);
            pos = group_end;
        }
    }

    const bool integral = !rank.pct && rank.method != ir::RankMethod::Average;
    if (integral) {
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        std::int64_t* dst = out.data();
        for (std::size_t r = 0; r < rows; ++r) {
            dst[r] = static_cast<std::int64_t>(rank_values[r]);
        }
        ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
        if (rank.na_option == ir::RankNaOption::Keep) {
            result.validity = std::move(validity);
        }
        return result;
    }

    Column<double> out;
    out.resize_for_overwrite(rows);
    std::memcpy(out.data(), rank_values.data(), rows * sizeof(double));
    ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
    if (rank.na_option == ir::RankNaOption::Keep) {
        result.validity = std::move(validity);
    }
    return result;
}

/// Chunk-preserving `Order`: buffers incoming chunks, validates sortedness
/// on-the-fly, and at EOF either emits the buffered chunks unchanged (with
/// `ordering` stamped) or falls back to `order_table` on the concatenated
/// input. Downstream operators see a chunked stream either way — the win
/// over the materializing path is avoiding the final big concat+sort when
/// the input is already ordered, plus preserving chunk shape for whatever
/// runs next.
// linkage so LLVM can devirtualize/inline the final operator classes).
// Streaming operator classes (internal: keeps the pre-split anonymous-namespace
namespace {
class ChunkedOrderOperator final : public Operator {
   public:
    ChunkedOrderOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys,
                         const ExecutionContext& exec)
        : child_(std::move(child)), keys_(keys), exec_(&exec) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (mode_ == Mode::Ingest) {
            auto drained = drain_and_check();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
        }
        if (mode_ == Mode::EmitSorted) {
            if (emit_idx_ >= buffered_.size()) {
                mode_ = Mode::Done;
                return std::optional<Chunk>{};
            }
            Chunk out = std::move(buffered_[emit_idx_++]);
            out.set_properties(out.properties().with_ordering(resolved_keys_));
            return std::optional<Chunk>{std::move(out)};
        }
        if (mode_ == Mode::EmitUnsorted) {
            mode_ = Mode::Done;
            if (!sorted_result_.has_value()) {
                return std::optional<Chunk>{};
            }
            Chunk out = table_to_chunk(std::move(*sorted_result_));
            sorted_result_.reset();
            return std::optional<Chunk>{std::move(out)};
        }
        return std::optional<Chunk>{};
    }

   private:
    enum class Mode : std::uint8_t { Ingest, EmitSorted, EmitUnsorted, Done };

    auto drain_and_check() -> std::expected<void, std::string> {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());
            if (chunk.rows() == 0) {
                continue;
            }
            if (resolved_keys_.empty()) {
                // A bare `order` (no keys) on a TimeFrame is rejected — the
                // time-sorted invariant makes an all-column sort meaningless.
                // Ordering by explicit non-time keys is allowed and reshuffles
                // the rows (the still-sorted check below then fails, so the EOF
                // fallback routes through `order_table`, which appends the time
                // index as an implicit tiebreaker and keeps the TimeFrame).
                if (chunk.time_index().has_value() && keys_->empty()) {
                    return std::unexpected("order on TimeFrame must be by time index ascending");
                }
                auto resolved = resolve_keys(chunk);
                if (!resolved.has_value()) {
                    return std::unexpected(std::move(resolved.error()));
                }
                resolved_keys_ = std::move(*resolved);
            }
            if (still_sorted_) {
                // A chunk that already claims this ordering needs no checking:
                // the claim was made by the operator that laid the rows out.
                // This is what carries an upstream `order`, or a join that
                // emitted its left rows in order, past a second sort -- and it
                // covers multi-key, descending and string orderings, which the
                // data check below deliberately does not.
                if (chunk.properties().satisfies(resolved_keys_)) {
                    // The boundary snapshot still has to advance: a later chunk
                    // that makes no claim is compared against the last row seen,
                    // and a stale one would compare it against the wrong rows.
                    if (auto ok = record_chunk_boundary(chunk); !ok.has_value()) {
                        return std::unexpected(std::move(ok.error()));
                    }
                } else {
                    auto ok = validate_chunk(chunk);
                    if (!ok.has_value()) {
                        return std::unexpected(std::move(ok.error()));
                    }
                    if (!*ok) {
                        still_sorted_ = false;
                    }
                }
            }
            buffered_.push_back(std::move(chunk));
        }

        if (buffered_.empty()) {
            mode_ = Mode::Done;
            return {};
        }
        if (still_sorted_) {
            mode_ = Mode::EmitSorted;
            return {};
        }
        // Fallback: concat everything into one Table and sort.
        Table concat;
        auto concatenated = concat_buffered(concat);
        buffered_.clear();
        if (!concatenated.has_value()) {
            return std::unexpected(std::move(concatenated.error()));
        }
        auto sorted = order_table(concat, *keys_, *exec_);
        if (!sorted.has_value()) {
            return std::unexpected(std::move(sorted.error()));
        }
        sorted_result_ = std::move(*sorted);
        mode_ = Mode::EmitUnsorted;
        return {};
    }

    auto resolve_keys(const Chunk& chunk) -> std::expected<std::vector<ir::OrderKey>, std::string> {
        if (!keys_->empty()) {
            return *keys_;
        }
        std::vector<ir::OrderKey> resolved;
        resolved.reserve(chunk.columns.size());
        for (const auto& entry : chunk.columns) {
            resolved.push_back(ir::OrderKey{.name = entry.name, .ascending = true});
        }
        return resolved;
    }

    // Returns true if the chunk is internally sorted on the resolved keys and
    // its first row is ordered correctly relative to the last row of the
    // previously buffered chunk (if any).
    auto validate_chunk(const Chunk& chunk) -> std::expected<bool, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return true;
        }
        // Every comparison below reads the key's VALUE, and a null cell holds
        // its type's zero — so a null would be judged against a real zero and,
        // worse, judged in the wrong place: nulls sort last, and a column whose
        // values happen to ascend would be declared already sorted with its
        // nulls still at the front. The real sort knows where nulls go, so a
        // nullable key gives up the shortcut rather than answer wrongly. This
        // is the same guard the materialized `order_table_resolved` applies to
        // its own pre-sorted check.
        for (const auto& key : resolved_keys_) {
            for (const auto& column : chunk.columns) {
                if (column.name == key.name && column.validity.has_value()) {
                    return false;
                }
            }
        }
        // Index of each key within this chunk's column list.
        std::vector<std::size_t> key_idx;
        key_idx.reserve(resolved_keys_.size());
        for (const auto& key : resolved_keys_) {
            std::size_t found = chunk.columns.size();
            for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
                if (chunk.columns[i].name == key.name) {
                    found = i;
                    break;
                }
            }
            if (found == chunk.columns.size()) {
                return std::unexpected("order column not found in chunk: " + key.name);
            }
            key_idx.push_back(found);
        }

        // Boundary check against last row of previous chunk.
        if (!prev_last_.empty()) {
            auto cmp = compare_keys_cross(prev_last_, chunk, 0, key_idx);
            if (cmp > 0) {
                return false;
            }
        }

        // Internal sort check. Single-key fast path uses typed column access
        // to avoid the per-row scalar_from_column + variant dispatch cost
        // (which dominates pre-sorted runs: a 2M-row scan goes from ~10 ms
        // with scalars to ~0.5 ms with typed compare).
        if (resolved_keys_.size() == 1) {
            const bool asc = resolved_keys_[0].ascending;
            const auto& col_var = *chunk.columns[key_idx[0]].column;
            bool sorted = true;
            bool handled = false;
            std::visit(
                [&](const auto& col) {
                    using ColT = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            const bool bad = asc ? (col[i].nanos < col[i - 1].nanos)
                                                 : (col[i].nanos > col[i - 1].nanos);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            const bool bad = asc ? (col[i].days < col[i - 1].days)
                                                 : (col[i].days > col[i - 1].days);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>> ||
                                         std::is_same_v<ColT, Column<double>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            const bool bad = asc ? (col[i] < col[i - 1]) : (col[i] > col[i - 1]);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    }
                },
                col_var);
            if (handled) {
                if (!sorted) {
                    return false;
                }
            } else {
                for (std::size_t r = 1; r < rows; ++r) {
                    if (compare_keys_within(chunk, r - 1, r, key_idx) > 0) {
                        return false;
                    }
                }
            }
        } else {
            for (std::size_t r = 1; r < rows; ++r) {
                if (compare_keys_within(chunk, r - 1, r, key_idx) > 0) {
                    return false;
                }
            }
        }

        if (auto ok = record_chunk_boundary(chunk); !ok.has_value()) {
            return std::unexpected(std::move(ok.error()));
        }
        return true;
    }

    /// Snapshot this chunk's last row as the boundary the next chunk's first
    /// row is compared against. Separate from `validate_chunk` because a chunk
    /// whose ordering claim was believed still has to advance the boundary.
    auto record_chunk_boundary(const Chunk& chunk) -> std::expected<void, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return {};
        }
        prev_last_.clear();
        prev_last_.reserve(resolved_keys_.size());
        for (const auto& key : resolved_keys_) {
            std::size_t found = chunk.columns.size();
            for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
                if (chunk.columns[i].name == key.name) {
                    found = i;
                    break;
                }
            }
            if (found == chunk.columns.size()) {
                return std::unexpected("order column not found in chunk: " + key.name);
            }
            prev_last_.push_back(scalar_from_column(*chunk.columns[found].column, rows - 1));
        }
        return {};
    }

    // Lexicographic comparison of two rows within the same chunk, honoring
    // per-key `ascending`. Returns >0 if lhs > rhs in the chosen order
    // (i.e. out-of-order), 0 if equal, <0 otherwise.
    auto compare_keys_within(const Chunk& chunk, std::size_t a, std::size_t b,
                             const std::vector<std::size_t>& key_idx) -> int {
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            const auto& col = *chunk.columns[key_idx[i]].column;
            auto sa = scalar_from_column(col, a);
            auto sb = scalar_from_column(col, b);
            const int c = compare_scalar_for_order(sa, sb);
            if (c != 0) {
                return resolved_keys_[i].ascending ? c : -c;
            }
        }
        return 0;
    }

    // Compare a cached row of scalars (previous chunk's last row) to a row of
    // the current chunk. Returns >0 iff cached > current (i.e. boundary
    // violates sort order).
    auto compare_keys_cross(const std::vector<ScalarValue>& cached, const Chunk& chunk,
                            std::size_t row, const std::vector<std::size_t>& key_idx) -> int {
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            const auto& col = *chunk.columns[key_idx[i]].column;
            auto sb = scalar_from_column(col, row);
            const int c = compare_scalar_for_order(cached[i], sb);
            if (c != 0) {
                return resolved_keys_[i].ascending ? c : -c;
            }
        }
        return 0;
    }

    auto concat_buffered(Table& out) -> std::expected<void, std::string> {
        Chunk first = std::move(buffered_.front());
        // Carry the TimeFrame designation into the concatenated table so the
        // `order_table` fallback can honor the TimeFrame ordering policy
        // (keeping the index, appending it as an implicit tiebreaker).
        out.set_properties(first.properties());
        out.columns = std::move(first.columns);
        for (std::size_t i = 0; i < out.columns.size(); ++i) {
            out.index[out.columns[i].name] = i;
        }
        const std::size_t n_cols = out.columns.size();
        for (std::size_t bi = 1; bi < buffered_.size(); ++bi) {
            Chunk& chunk = buffered_[bi];
            if (chunk.columns.size() != n_cols) {
                return std::unexpected("order: chunk schema mismatch (column count)");
            }
            // Before any column is appended — see the matching note above.
            const std::size_t dst_rows = out.rows();
            const std::size_t src_rows = chunk.rows();
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != out.columns[i].name) {
                    return std::unexpected("order: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("order: chunk schema mismatch (column type)");
                }
                append_validity(out.columns[i].validity, dst_rows, chunk.columns[i].validity,
                                src_rows);
                append_column_values(out.mutable_column(i), *chunk.columns[i].column);
            }
        }
        return {};
    }

    OperatorPtr child_;
    const std::vector<ir::OrderKey>* keys_;
    const ExecutionContext* exec_;
    Mode mode_ = Mode::Ingest;
    std::vector<Chunk> buffered_;
    std::vector<ir::OrderKey> resolved_keys_;
    std::vector<ScalarValue> prev_last_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool still_sorted_ = true;
};

/// Chunk-preserving `as_timeframe`: buffers incoming chunks, promotes an
/// `Int` time column to `Timestamp` per chunk, validates ascending sortedness
/// on the fly, and either re-emits the buffered chunks with `time_index`
/// stamped (fast path: no sort) or falls back to concat + `order_table`
/// (slow path: SPEC §9.1 says as_timeframe must sort if unsorted, so the
/// full table materialization is unavoidable for that branch).
///
/// The win is real only when the input is already sorted on the time column,
/// which is the overwhelmingly common TimeFrame shape (CSV/parquet ingest,
/// streaming sources). For those we skip the sort entirely and let downstream
/// operators see a chunked TimeFrame.
class ChunkedAsTimeframeOperator final : public Operator {
   public:
    ChunkedAsTimeframeOperator(OperatorPtr child, std::string column, const ExecutionContext& exec)
        : child_(std::move(child)), column_(std::move(column)), exec_(&exec) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (mode_ == Mode::Ingest) {
            auto drained = drain();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
        }
        if (mode_ == Mode::EmitBuffered) {
            if (emit_idx_ >= buffered_.size()) {
                mode_ = Mode::Done;
                return std::optional<Chunk>{};
            }
            Chunk out = std::move(buffered_[emit_idx_++]);
            // as_timeframe: the index and the ascending order it implies.
            out.set_properties(TableProperties::time_frame(column_));
            return std::optional<Chunk>{std::move(out)};
        }
        if (mode_ == Mode::EmitSorted) {
            mode_ = Mode::Done;
            if (!sorted_result_.has_value()) {
                return std::optional<Chunk>{};
            }
            Chunk out = table_to_chunk(std::move(*sorted_result_));
            sorted_result_.reset();
            return std::optional<Chunk>{std::move(out)};
        }
        return std::optional<Chunk>{};
    }

   private:
    enum class Mode : std::uint8_t { Ingest, EmitBuffered, EmitSorted, Done };

    auto drain() -> std::expected<void, std::string> {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());
            if (chunk.rows() == 0) {
                continue;
            }

            // Locate the time column in this chunk.
            std::size_t col_idx = chunk.columns.size();
            for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
                if (chunk.columns[i].name == column_) {
                    col_idx = i;
                    break;
                }
            }
            if (col_idx == chunk.columns.size()) {
                return std::unexpected("as_timeframe: column '" + column_ + "' not found");
            }

            // On the first chunk, decide whether Int promotion is needed and
            // reject invalid types.
            if (!type_checked_) {
                const auto& col = *chunk.columns[col_idx].column;
                if (std::holds_alternative<Column<std::int64_t>>(col)) {
                    needs_promotion_ = true;
                } else if (!std::holds_alternative<Column<Timestamp>>(col) &&
                           !std::holds_alternative<Column<Date>>(col)) {
                    return std::unexpected("as_timeframe: column '" + column_ +
                                           "' must be Timestamp, Date, or Int");
                }
                type_checked_ = true;
            }

            // A null has no position in time, so it cannot be an index (see
            // the same check in the materialized as_timeframe). Chunk-local row
            // numbers would be misleading, so the message counts from the start
            // of the input.
            if (chunk.columns[col_idx].validity.has_value()) {
                const auto& validity = *chunk.columns[col_idx].validity;
                for (std::size_t r = 0; r < chunk.rows(); ++r) {
                    if (!validity[r]) {
                        return std::unexpected(
                            "as_timeframe: time index '" + column_ + "' is null at row " +
                            std::to_string(rows_seen_ + r) +
                            "; a TimeFrame's index must have no nulls (drop or fill them first)");
                    }
                }
            }
            rows_seen_ += chunk.rows();

            // Promote Int → Timestamp per chunk (cheap — same row count, same
            // layout — and keeps downstream operators seeing Timestamp).
            if (needs_promotion_) {
                const auto& ints = std::get<Column<std::int64_t>>(*chunk.columns[col_idx].column);
                Column<Timestamp> ts_col;
                ts_col.reserve(ints.size());
                for (auto v : ints) {
                    ts_col.push_back(Timestamp{v});
                }
                chunk.replace_column(col_idx, ColumnValue{std::move(ts_col)});
            }

            if (still_sorted_) {
                auto ok = validate_chunk(chunk, col_idx);
                if (!ok.has_value()) {
                    return std::unexpected(std::move(ok.error()));
                }
                if (!*ok) {
                    still_sorted_ = false;
                }
            }

            buffered_.push_back(std::move(chunk));
        }

        if (buffered_.empty()) {
            mode_ = Mode::Done;
            return {};
        }
        if (still_sorted_) {
            mode_ = Mode::EmitBuffered;
            return {};
        }

        // Fallback: concat all buffered chunks and run the full sort. SPEC
        // §9.1 requires as_timeframe to sort its input when unsorted, so this
        // materialization is intentional.
        Table concat;
        Chunk first = std::move(buffered_.front());
        concat.columns = std::move(first.columns);
        for (std::size_t i = 0; i < concat.columns.size(); ++i) {
            concat.index[concat.columns[i].name] = i;
        }
        const std::size_t n_cols = concat.columns.size();
        for (std::size_t bi = 1; bi < buffered_.size(); ++bi) {
            Chunk& chunk = buffered_[bi];
            if (chunk.columns.size() != n_cols) {
                return std::unexpected("as_timeframe: chunk schema mismatch (column count)");
            }
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != concat.columns[i].name) {
                    return std::unexpected("as_timeframe: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != concat.columns[i].column->index()) {
                    return std::unexpected("as_timeframe: chunk schema mismatch (column type)");
                }
                auto& dst_col = concat.mutable_column(i);
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
            }
        }
        buffered_.clear();

        auto sorted = order_table(concat, {{.name = column_, .ascending = true}}, *exec_);
        if (!sorted.has_value()) {
            return std::unexpected(std::move(sorted.error()));
        }
        sorted->set_properties(TableProperties::time_frame(column_));
        sorted_result_ = std::move(*sorted);
        mode_ = Mode::EmitSorted;
        return {};
    }

    // Returns true if the chunk's time column is ascending internally and its
    // first row is ≥ the last row of the previously buffered chunk. Typed
    // dispatch mirrors ChunkedOrderOperator's single-key fast path — the whole
    // point of this operator is to avoid a big sort, so the validation must
    // itself not be expensive.
    auto validate_chunk(const Chunk& chunk, std::size_t col_idx)
        -> std::expected<bool, std::string> {
        const std::size_t rows = chunk.rows();
        if (rows == 0) {
            return true;
        }
        const auto& col_var = *chunk.columns[col_idx].column;
        bool sorted = true;
        bool handled = false;
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    handled = true;
                    if (prev_last_nanos_.has_value() && col[0].nanos < *prev_last_nanos_) {
                        sorted = false;
                        return;
                    }
                    for (std::size_t i = 1; i < rows; ++i) {
                        if (col[i].nanos < col[i - 1].nanos) {
                            sorted = false;
                            return;
                        }
                    }
                    prev_last_nanos_ = col[rows - 1].nanos;
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    handled = true;
                    if (prev_last_days_.has_value() && col[0].days < *prev_last_days_) {
                        sorted = false;
                        return;
                    }
                    for (std::size_t i = 1; i < rows; ++i) {
                        if (col[i].days < col[i - 1].days) {
                            sorted = false;
                            return;
                        }
                    }
                    prev_last_days_ = col[rows - 1].days;
                }
            },
            col_var);
        if (!handled) {
            // Type already validated on first chunk; downstream schema
            // guarantees stability. Reaching here means inconsistent schema.
            return std::unexpected("as_timeframe: unexpected time column type");
        }
        return sorted;
    }

    OperatorPtr child_;
    std::string column_;
    const ExecutionContext* exec_;
    Mode mode_ = Mode::Ingest;
    std::vector<Chunk> buffered_;
    std::optional<std::int64_t> prev_last_nanos_;
    std::optional<std::int32_t> prev_last_days_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool type_checked_ = false;
    std::size_t rows_seen_ = 0;  ///< input rows drained, for null-index messages
    bool needs_promotion_ = false;
    bool still_sorted_ = true;
};

class ChunkedOrderedLimitOperator final : public Operator {
   public:
    enum class KeepMode : std::uint8_t { First, Last };

    ChunkedOrderedLimitOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys,
                                std::size_t count, const std::vector<ir::ColumnRef>* group_by,
                                KeepMode keep_mode)
        : child_(std::move(child)),
          keys_(keys),
          count_(count),
          group_by_(group_by),
          keep_mode_(keep_mode) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }

        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }

            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            if (!empty_template_.has_value()) {
                const std::vector<std::size_t> idx;
                empty_template_ = gather_rows(t, idx);
            }
            auto err = process_chunk(t);
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
        }

        emitted_ = true;
        if (!empty_template_.has_value()) {
            return std::optional<Chunk>{};
        }
        return std::optional<Chunk>{table_to_chunk(build_output())};
    }

   private:
    struct RowSnapshot {
        std::vector<ScalarValue> values;
        std::vector<uint8_t> valid;
    };

    struct Entry {
        Key key;
        std::size_t sequence = 0;
        RowSnapshot row;
    };

    struct GroupState {
        std::vector<Entry> heap;
    };

    static auto snapshot_row(const Table& chunk, std::size_t row) -> RowSnapshot {
        RowSnapshot snapshot;
        snapshot.values.reserve(chunk.columns.size());
        snapshot.valid.reserve(chunk.columns.size());
        for (const auto& column : chunk.columns) {
            snapshot.values.push_back(scalar_from_column(*column.column, row));
            snapshot.valid.push_back(
                column.validity.has_value() ? static_cast<uint8_t>((*column.validity)[row]) : 1U);
        }
        return snapshot;
    }

    [[nodiscard]] auto row_comes_first(const Entry& lhs, const Entry& rhs) const -> bool {
        for (std::size_t i = 0; i < lhs.key.values.size(); ++i) {
            const int cmp = compare_scalar_for_order(lhs.key.values[i], rhs.key.values[i]);
            if (cmp == 0) {
                continue;
            }
            return (*keys_)[i].ascending ? (cmp < 0) : (cmp > 0);
        }
        return lhs.sequence < rhs.sequence;
    }

    [[nodiscard]] auto entry_preferred(const Entry& lhs, const Entry& rhs) const -> bool {
        return keep_mode_ == KeepMode::First ? row_comes_first(lhs, rhs)
                                             // NOLINTNEXTLINE(readability-suspicious-call-argument)
                                             : row_comes_first(rhs, lhs);
    }

    template <typename T>
    [[nodiscard]] auto single_key_better(const T& lhs, std::size_t lhs_sequence, const Entry& rhs,
                                         bool ascending) const -> bool {
        const auto* rhs_key = std::get_if<T>(&rhs.key.values.front());
        if (rhs_key == nullptr) {
            invariant_violation("ChunkedOrderedLimitOperator: single-key type mismatch");
        }
        if (lhs == *rhs_key) {
            return keep_mode_ == KeepMode::First ? (lhs_sequence < rhs.sequence)
                                                 : (rhs.sequence < lhs_sequence);
        }
        const bool lhs_first = ascending ? (lhs < *rhs_key) : (lhs > *rhs_key);
        return keep_mode_ == KeepMode::First ? lhs_first : !lhs_first;
    }

    auto push_entry(Entry entry) -> void { push_entry(heap_, std::move(entry)); }

    auto push_entry(std::vector<Entry>& heap, Entry entry) const -> void {
        if (heap.size() < count_) {
            heap.push_back(std::move(entry));
            std::ranges::push_heap(
                heap, [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
            return;
        }

        std::ranges::pop_heap(
            heap, [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
        heap.back() = std::move(entry);
        std::ranges::push_heap(
            heap, [&](const Entry& a, const Entry& b) { return entry_preferred(a, b); });
    }

    // Resolves this row's group heap without boxing a Key (a heap-allocated
    // vector of ScalarValue) on every row: group_key_cols reads column
    // storage in place, and group_index_ probes a dense gid by hash, only
    // building a boxed Key the first time a group is seen. With a few hundred
    // distinct groups over millions of rows (e.g. `by symbol`), that turns the
    // per-row cost from an allocation + string hash into a hash-and-compare
    // over existing memory. Only called when group_by_ is non-empty — the
    // ungrouped case stays a direct `&heap_` at the call site so it never pays
    // for a function call it doesn't need.
    auto resolve_group_heap(const Table& chunk, const std::vector<KeyCol>& group_key_cols,
                            std::size_t row) -> std::vector<Entry>* {
        // Fast path: single Categorical key, non-null row. A null carries no
        // meaningful code, and the generic index already folds every null into
        // one group, so nulls stay on the slow path rather than being memoed.
        if (!cat_gid_memo_.empty() && !group_key_cols.front().is_null(row)) {
            const auto code = static_cast<std::size_t>(group_key_cols.front().cat->code_at(row));
            if (code < cat_gid_memo_.size()) {
                std::uint32_t& slot = cat_gid_memo_[code];
                if (slot == kNoCatGid) {
                    slot = resolve_group_slow(chunk, group_key_cols, row);
                }
                return &group_states_[slot].heap;
            }
        }
        return &group_states_[resolve_group_slow(chunk, group_key_cols, row)].heap;
    }

    auto resolve_group_slow(const Table& chunk, const std::vector<KeyCol>& group_key_cols,
                            std::size_t row) -> std::uint32_t {
        return group_index_.find_or_insert(group_keys_, group_key_cols, row, [&] {
            Key key;
            key.values.reserve(group_by_->size());
            for (const auto& ref : *group_by_) {
                const auto* entry = chunk.find_entry(ref.name);
                push_key_value(key, *entry, row);
            }
            group_keys_.push_back(std::move(key));
            group_states_.push_back(GroupState{});
            return static_cast<std::uint32_t>(group_states_.size() - 1);
        });
    }

    template <typename T>
    auto process_single_key_chunk(const Table& chunk, const Column<T>& key_column, bool ascending,
                                  const std::vector<KeyCol>& group_key_cols)
        -> std::optional<std::string> {
        // `chunk.rows()` is a std::visit over the column variant, and this loop
        // runs once per input row — it measured as ~15% of a top-k query while
        // sitting in the loop CONDITION. `key_column[row]` likewise re-tests
        // for adopted (Arrow) storage on every element. Resolve both once.
        const std::size_t rows = chunk.rows();
        const T* key_values = nullptr;
        if constexpr (is_dense_column_v<Column<T>>) {
            key_values = key_column.data();
        }
        // `Column<bool>` is bit-packed and has no dense buffer, so it keeps the
        // indexed read. Every T reaching here is trivially copyable.
        auto key_at = [&](std::size_t r) -> T {
            if constexpr (is_dense_column_v<Column<T>>) {
                return key_values[r];
            } else {
                return key_column[r];
            }
        };
        for (std::size_t row = 0; row < rows; ++row) {
            const std::size_t sequence = next_sequence_++;
            const T key = key_at(row);

            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                heap = resolve_group_heap(chunk, group_key_cols, row);
            }

            if (heap->size() == count_ &&
                !single_key_better(key, sequence, heap->front(), ascending)) {
                continue;
            }

            Entry entry;
            entry.key.values.reserve(1);
            entry.key.values.emplace_back(key);
            entry.sequence = sequence;
            entry.row = snapshot_row(chunk, row);
            push_entry(*heap, std::move(entry));
        }
        return std::nullopt;
    }

    auto process_chunk(const Table& chunk) -> std::optional<std::string> {
        if (count_ == 0 || chunk.rows() == 0) {
            return std::nullopt;
        }

        std::vector<KeyCol> group_key_cols;
        group_key_cols.reserve(group_by_->size());
        for (const auto& ref : *group_by_) {
            const auto* entry = chunk.find_entry(ref.name);
            if (entry == nullptr) {
                return "topk group-by column not found: " + ref.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            auto col = make_key_col(*entry);
            if (!col.has_value()) {
                return "topk group-by column has unsupported type: " + ref.name;
            }
            group_key_cols.push_back(*col);
        }

        // `by <categorical>` is the common shape (`by symbol`), and the generic
        // index answers it the expensive way: resolve the code to its
        // dictionary string, hash the bytes, then memcmp on every probe — once
        // per ROW, to rediscover one of a handful of groups. Within one chunk
        // the dictionary is fixed, so code -> gid is a function; memo it.
        cat_gid_memo_.clear();
        if (group_key_cols.size() == 1 && group_key_cols.front().kind == KeyCol::Kind::Cat) {
            cat_gid_memo_.assign(group_key_cols.front().cat->dictionary().size(), kNoCatGid);
        }

        std::vector<const ColumnValue*> key_columns;
        key_columns.reserve(keys_->size());
        for (const auto& key : *keys_) {
            const auto* column = chunk.find(key.name);
            if (column == nullptr) {
                return "order column not found: " + key.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            key_columns.push_back(column);
        }

        if (keys_->size() == 1) {
            const bool ascending = keys_->front().ascending;
            const ColumnValue& key_column = *key_columns.front();
            if (const auto* col = std::get_if<Column<std::int64_t>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols);
            }
            if (const auto* col = std::get_if<Column<double>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols);
            }
            if (const auto* col = std::get_if<Column<bool>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols);
            }
            if (const auto* col = std::get_if<Column<Date>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols);
            }
            if (const auto* col = std::get_if<Column<Timestamp>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols);
            }
        }

        // As above: `rows()` visits the column variant, so it does not belong
        // in a per-row loop condition.
        const std::size_t rows = chunk.rows();
        for (std::size_t row = 0; row < rows; ++row) {
            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                heap = resolve_group_heap(chunk, group_key_cols, row);
            }

            Entry entry;
            entry.key.values.reserve(keys_->size());
            for (const auto* column : key_columns) {
                entry.key.values.push_back(scalar_from_column(*column, row));
            }
            entry.sequence = next_sequence_++;
            if (heap->size() == count_ && !entry_preferred(entry, heap->front())) {
                continue;
            }
            entry.row = snapshot_row(chunk, row);
            push_entry(*heap, std::move(entry));
        }
        return std::nullopt;
    }

    auto build_output() -> Table {
        std::vector<Entry> winners;
        if (group_by_->empty()) {
            winners = heap_;
        } else {
            for (auto& state : group_states_) {
                for (auto& entry : state.heap) {
                    winners.push_back(std::move(entry));
                }
            }
        }

        if (count_ == 0 || winners.empty()) {
            return empty_template_.value_or(Table{});
        }

        std::ranges::sort(winners,
                          [&](const Entry& a, const Entry& b) { return row_comes_first(a, b); });

        Table out = empty_template_.value_or(Table{});
        for (const auto& entry : winners) {
            for (std::size_t col = 0; col < out.columns.size(); ++col) {
                auto& out_col = out.mutable_column(col);
                append_scalar(out_col, entry.row.values[col]);
                auto& out_entry = out.columns[col];
                if (entry.row.valid[col] == 0U) {
                    if (!out_entry.validity.has_value()) {
                        out_entry.validity = ValidityBitmap(column_size(out_col) - 1, true);
                    }
                    ValidityBitmap* const validity = &*out_entry.validity;
                    validity->push_back(false);
                } else if (out_entry.validity.has_value()) {
                    ValidityBitmap* const validity = &*out_entry.validity;
                    validity->push_back(true);
                }
            }
        }

        out.set_properties(out.properties().with_ordering(*keys_));
        return out;
    }

    OperatorPtr child_;
    const std::vector<ir::OrderKey>* keys_;
    std::size_t count_;
    const std::vector<ir::ColumnRef>* group_by_;
    KeepMode keep_mode_;
    bool emitted_ = false;
    std::size_t next_sequence_ = 0;
    std::vector<Entry> heap_;
    std::vector<Key> group_keys_;
    std::vector<GroupState> group_states_;
    KeyRowIndex group_index_;
    std::optional<Table> empty_template_;
    /// Per-chunk memo for a single Categorical group key: dictionary code ->
    /// gid. Empty when the fast path does not apply. Rebuilt every chunk
    /// because a later chunk may carry a DIFFERENT dictionary, which would
    /// make a retained code->gid mapping silently wrong.
    std::vector<std::uint32_t> cat_gid_memo_;
    static constexpr std::uint32_t kNoCatGid = std::numeric_limits<std::uint32_t>::max();
};

/// Encodes a fixed-width multi-column key into one flat integer.
///
/// Shared by `ChunkedDistinctOperator` and the hash aggregate state: both
/// want the same thing from a multi-column key — a POD that hashes and compares
/// in one shot, with no per-row allocation and no per-row string hashing — and
/// both then hand it to a partitioned parallel discovery pass.
///
/// **The axis is WIDTH, not column count.** A key of any arity lands in one of
/// three buckets (≤8, ≤16, ≤32 bytes) or declines, so adding key columns never
/// adds a code path. Column TYPE is likewise a runtime `PackCol::Kind` switch
/// rather than a template parameter. That is what keeps this from multiplying
/// out: the three existing hand-written shapes (one int, two ints, one
/// categorical) are all points inside it.
class ChunkedDistinctOperator final : public Operator {
   public:
    ChunkedDistinctOperator(OperatorPtr child, physical::BreakerParallelism dedup_plan)
        : child_(std::move(child)), dedup_plan_(dedup_plan) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }

            Table t = chunk_to_table(std::move(*chunk_res.value()));
            if (t.columns.empty()) {
                // `distinct` keeps the first occurrence of each row in input order and
                // this operator is stateful, so its chunks arrive in order too: a
                // `RowTransform::Subset`, under which every claim the input made still
                // holds. The metadata therefore rides through untouched.
                return std::optional<Chunk>{table_to_chunk(std::move(t))};
            }

            // The single-column fast paths hash the raw value and cannot express
            // "null", so a null would dedupe against a genuine 0 / "". A
            // null-bearing column falls through to the Key path below, which
            // carries the null bits.
            if (!generic_dedup_seen_ && t.columns.size() == 1 &&
                !t.columns.front().validity.has_value()) {
                fast_dedup_seen_ = true;
                auto out = process_single_column(std::move(t));
                if (!out.has_value()) {
                    continue;
                }
                return std::optional<Chunk>{table_to_chunk(std::move(*out))};
            }

            // Fixed-width integral keys with no nulls pack into a single integer,
            // so a multi-column distinct dedups through a flat typed set with no
            // per-row Key allocation — the dominant cost on high-cardinality
            // input, where nearly every row is a new value and the KeyRowIndex
            // path still heap-builds one owned Key each. Doubles are excluded
            // (byte equality would split -0.0 from 0.0 and merge NaNs) and so are
            // categoricals (a code names different values across chunks).
            key_entries_.clear();
            key_entries_.reserve(t.columns.size());
            for (const auto& entry : t.columns) {
                key_entries_.push_back(&entry);
            }
            if (!generic_dedup_seen_) {
                if (auto plan = encoder_.build_packed_key(key_entries_); plan.has_value()) {
                    fast_dedup_seen_ = true;
                    std::optional<Table> out;
                    if (plan->width <= sizeof(std::uint64_t)) {
                        out = process_packed(std::move(t), plan->cols, packed64_);
                    } else if (plan->width <= sizeof(PackedKeyEncoder::Packed128)) {
                        out = process_packed(std::move(t), plan->cols, packed128_);
                    } else {
                        out = process_packed(std::move(t), plan->cols, packed256_);
                    }
                    if (!out.has_value()) {
                        continue;
                    }
                    return std::optional<Chunk>{table_to_chunk(std::move(*out))};
                }
            }

            // The typed and packed stores contain raw values only, whereas the
            // generic Key index includes validity. They cannot deduplicate
            // against each other. A later Parquet row group may be the first
            // one to carry a bitmap, so do not switch stores and re-emit every
            // non-null key the fast path already accepted.
            //
            // A single-column typed store (int64/double/bool/Date/Timestamp/
            // string) migrates: it is a plain seen-set with no order to
            // preserve, so its values seed the generic index directly. The
            // packed multi-key stores and the categorical store still fail
            // explicitly -- see `migrate_single_column_dedup_to_generic`.
            if (fast_dedup_seen_ && std::ranges::any_of(t.columns, [](const ColumnEntry& entry) {
                    return entry.validity.has_value();
                })) {
                if (t.columns.size() != 1 ||
                    !migrate_single_column_dedup_to_generic(*t.columns.front().column)) {
                    return std::unexpected(
                        "ChunkedDistinctOperator: key column gained nulls across chunks");
                }
            }
            generic_dedup_seen_ = true;

            const std::size_t rows = t.rows();

            // Resolve each key column once for the whole chunk, so the row loop
            // hashes and compares values where they sit instead of boxing a Key
            // (a heap-allocated vector of variants) per row. make_key_col covers
            // every column type; the boxed fallback below is for anything it
            // cannot, and never runs for the built-in types.
            std::vector<KeyCol> cols;
            cols.reserve(t.columns.size());
            bool all_key_cols = true;
            for (const auto& entry : t.columns) {
                auto col = make_key_col(entry);
                if (!col.has_value()) {
                    all_key_cols = false;
                    break;
                }
                cols.push_back(*col);
            }

            std::vector<std::size_t> idx;
            idx.reserve(rows);
            if (all_key_cols) {
                for (std::size_t row = 0; row < rows; ++row) {
                    // find_or_insert calls the maker only for a genuinely new
                    // value; a duplicate hashes and compares in place, no alloc.
                    bool is_new = false;
                    key_index_.find_or_insert(group_order_, cols, row, [&] {
                        is_new = true;
                        Key key;
                        key.values.reserve(t.columns.size());
                        for (const auto& entry : t.columns) {
                            push_key_value(key, entry, row);
                        }
                        group_order_.push_back(std::move(key));
                        return static_cast<std::uint32_t>(group_order_.size() - 1);
                    });
                    if (is_new) {
                        idx.push_back(row);
                    }
                }
            } else {
                for (std::size_t row = 0; row < rows; ++row) {
                    Key key;
                    key.values.reserve(t.columns.size());
                    for (const auto& entry : t.columns) {
                        push_key_value(key, entry, row);
                    }
                    if (!seen_.insert(std::move(key)).second) {
                        continue;
                    }
                    idx.push_back(row);
                }
            }

            if (idx.empty()) {
                continue;
            }

            // `distinct` keeps the first occurrence of each row in input order and
            // this operator is stateful, so its chunks arrive in order too: a
            // `RowTransform::Subset`, under which every claim the input made still
            // holds. The metadata therefore rides through untouched.
            if (idx.size() == rows) {
                return std::optional<Chunk>{table_to_chunk(std::move(t))};
            }
            return std::optional<Chunk>{table_to_chunk(gather_rows(t, idx))};
        }
    }

   private:
    /// The dedup fan-out policy, resolved from `dedup_plan_` (which the plan
    /// owns -- src/runtime/PARALLELISM.md). Returns 0 to stay serial, otherwise
    /// the partition count. Both dedup paths call this; the two conditions that
    /// stay here are the ones only the operator can judge -- whether it is
    /// nested under another fan-out, and whether *this* chunk cleared the floor
    /// (a streaming source's row count is not known until it arrives).
    [[nodiscard]] auto dedup_partition_count(std::size_t rows) const -> std::size_t {
        if (dedup_plan_.decline != physical::FanOutDecline::None || dedup_plan_.worker_cap < 2 ||
            on_worker_pool_thread() || rows < dedup_plan_.row_floor) {
            return 0;
        }
        std::size_t count = 1;
        while (count * 2 <= dedup_plan_.worker_cap) {
            count *= 2;  // a power of two, so the partition is a mask
        }
        return count;
    }

    /// A single-column typed dedup store: the serial set, and -- once the
    /// parallel path has run -- one set per partition. Exactly `PackedDedup`'s
    /// split, minus the key buffer, because for one column the column already
    /// is the key buffer.
    template <typename T>
    struct TypedDedup {
        robin_hood::unordered_flat_set<T> seen;
        std::vector<robin_hood::unordered_flat_set<T>> parts;
    };

    template <typename T>
    auto gather_distinct_rows(Table t, TypedDedup<T>& state, const Column<T>& col)
        -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        // Exact-identity value types only -- see `try_typed_parallel` for why a
        // float must not be partitioned by its hash. Bool is left serial too:
        // two values fit in any cache, so there is nothing for partitioning to
        // win.
        constexpr bool kExactIdentity = std::is_same_v<T, std::int64_t> ||
                                        std::is_same_v<T, Date> || std::is_same_v<T, Timestamp>;
        // NOLINTNEXTLINE(misc-const-correctness) // threaded changes behind constexpr
        bool threaded = false;
        if constexpr (kExactIdentity) {
            threaded = try_typed_parallel(col, rows, state, keep_);
        }
        if (threaded) {
            for (std::size_t row = 0; row < rows; ++row) {
                if (keep_[row] != 0) {
                    idx.push_back(row);
                }
            }
        } else {
            for (std::size_t row = 0; row < rows; ++row) {
                if (!state.seen.insert(col[row]).second) {
                    continue;
                }
                idx.push_back(row);
            }
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        // `distinct` keeps the first occurrence of each row in input order and
        // this operator is stateful, so its chunks arrive in order too: a
        // `RowTransform::Subset`, under which every claim the input made still
        // holds. The metadata therefore rides through untouched.
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto gather_distinct_string_rows(Table t, const Column<std::string>& col)
        -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            const std::string_view value = col[row];
            if (seen_strings_.contains(value)) {
                continue;
            }
            owned_strings_.emplace_back(value);
            seen_strings_.insert(std::string_view{owned_strings_.back()});
            idx.push_back(row);
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        // `distinct` keeps the first occurrence of each row in input order and
        // this operator is stateful, so its chunks arrive in order too: a
        // `RowTransform::Subset`, under which every claim the input made still
        // holds. The metadata therefore rides through untouched.
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto gather_distinct_categorical_rows(Table t, const Column<Categorical>& col)
        -> std::optional<Table> {
        const void* dict_id = static_cast<const void*>(col.dictionary_ptr().get());
        if (cat_dictionary_id_ == nullptr || cat_dictionary_id_ == dict_id) {
            cat_dictionary_id_ = dict_id;
            const std::size_t rows = t.rows();
            const std::size_t dict_size = col.dictionary().size();
            // A Categorical code is a dense index into the dictionary, so
            // membership is an array read — hashing it was redundant work by
            // construction, one probe per ROW to discover at most `dict_size`
            // values. The flags stay across chunks (the dictionary only grows,
            // and this branch already requires the same dictionary), so first
            // occurrence is still decided over the whole input.
            if (seen_cat_flags_.size() < dict_size) {
                seen_cat_flags_.resize(dict_size, 0);
            }
            std::vector<std::size_t> idx;
            // At most one row per dictionary entry can be a first occurrence,
            // so this is an exact bound — `rows` reserved 8MB to hold a few
            // hundred indices.
            idx.reserve(std::min(rows, dict_size));
            const auto* codes = col.codes_data();
            for (std::size_t row = 0; row < rows; ++row) {
                auto& flag = seen_cat_flags_[static_cast<std::size_t>(codes[row])];
                if (flag != 0) {
                    continue;
                }
                flag = 1;
                idx.push_back(row);
            }
            if (idx.empty()) {
                return std::nullopt;
            }
            // `distinct` keeps the first occurrence of each row in input order and
            // this operator is stateful, so its chunks arrive in order too: a
            // `RowTransform::Subset`, under which every claim the input made still
            // holds. The metadata therefore rides through untouched.
            if (idx.size() == rows) {
                return t;
            }
            return gather_rows(t, idx);
        }

        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            const std::string_view value = col[row];
            if (seen_strings_.contains(value)) {
                continue;
            }
            owned_strings_.emplace_back(value);
            seen_strings_.insert(std::string_view{owned_strings_.back()});
            idx.push_back(row);
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        // `distinct` keeps the first occurrence of each row in input order and
        // this operator is stateful, so its chunks arrive in order too: a
        // `RowTransform::Subset`, under which every claim the input made still
        // holds. The metadata therefore rides through untouched.
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    /// Everything one packed width needs: the serial set, the per-partition
    /// sets the parallel path owns, and the per-chunk key buffer.
    ///
    /// The two set forms are alternatives, never both: a value deduped into
    /// `seen` is invisible to `parts` and vice versa, so once a chunk has taken
    /// the parallel path every later chunk must too (`dedup_part_count_`).
    template <typename Packed, typename Hash>
    struct PackedDedup {
        robin_hood::unordered_flat_set<Packed, Hash> seen;
        std::vector<robin_hood::unordered_flat_set<Packed, Hash>> parts;
        std::vector<Packed> keys;
    };

    /// Dedup a chunk across workers by hash-partitioning its keys. Returns false
    /// when the parallel path declines, leaving `keep` untouched.
    ///
    /// Distinct's serial cost is one key build plus one probe per row, and on
    /// the input this path exists for -- high cardinality, nearly every probe a
    /// miss -- the probes are a stream of cache misses that no amount of
    /// single-thread tuning removes. Partitioning gives each worker a set that
    /// no other worker touches, so the probes run concurrently with no locking,
    /// and each table is 1/P the size and correspondingly likelier to stay
    /// resident.
    ///
    /// **No scatter pass.** The aggregate's partitioned discovery
    /// (`try_discover_partitioned`) histograms and scatters row indices so each
    /// worker gets its partition's rows contiguously, because it must then
    /// number groups. Distinct numbers nothing: each worker can simply scan the
    /// whole key buffer and skip rows that are not its own. That reads the
    /// buffer P times instead of once, but sequentially and from a copy every
    /// worker shares in cache -- cheaper here than a scatter that turns the
    /// key reads random.
    ///
    /// **Determinism.** What a worker records is a KEEP FLAG at a row, not a
    /// position, and the output index list is rebuilt afterwards by scanning
    /// the flags in row order. Each partition is also scanned ascending, so the
    /// row kept for a value is the first one, exactly as the serial path picks.
    /// The output is byte-identical however the workers interleave.
    template <typename Packed, typename Hash>
    auto try_packed_parallel(const std::vector<PackedKeyEncoder::PackCol>& cols, std::size_t rows,
                             PackedDedup<Packed, Hash>& state, std::vector<std::uint8_t>& keep)
        -> bool {
        // Below this the key buffer and the fan-out cost more than the serial
        // probe they replace. Cardinality is not checkable up front -- it is
        // what the pass is about to find out -- so row count is the only gate
        // available.
        //
        // Once the parallel path HAS run, the values it deduped live in
        // `state.parts` and the serial `state.seen` is empty, so a small
        // trailing chunk that fell back would not find them and would re-emit
        // rows already emitted. Every condition below therefore either holds
        // for the whole query (the context and the pool are fixed) or, like the
        // row gate, guards only the first use.
        if (dedup_part_count_ == 0) {
            // `count <= pool.size()` by construction (the plan's worker cap was
            // clamped to it) and the pool never shrinks, so `submit(part_count,
            // ...)` below is never clamped -- which it must not be, or a
            // partition's rows would go unvisited.
            const std::size_t count = dedup_partition_count(rows);
            if (count == 0) {
                return false;
            }
            dedup_part_count_ = count;
            state.parts.resize(count);
            // Anything an earlier chunk deduped serially lives in `state.seen`,
            // which no worker will ever probe, so it has to move into the
            // partitions before the first parallel chunk runs. Without this a
            // value already emitted is inserted afresh and emitted a SECOND
            // time: the row gate is per chunk, so a chunk under it falls back
            // to serial while leaving `dedup_part_count_` at 0, and the next
            // chunk over the gate is then the first parallel use, against empty
            // partitions. A 5000-row chunk ahead of a 40000-row one is enough,
            // which any filter with uneven selectivity produces.
            //
            // Same hasher and mask the worker uses below, or a seeded key would
            // land in a partition nobody probes for it.
            Hash seed_hasher;
            for (const Packed& key : state.seen) {
                state.parts[seed_hasher(key) & (count - 1)].insert(key);
            }
            state.seen.clear();
        }
        auto& pool = process_worker_pool();
        const std::size_t part_count = dedup_part_count_;
        const std::size_t workers = part_count;
        const std::uint64_t part_mask = part_count - 1;

        // Pass 1: build every row's key and note its partition. Ranges are
        // contiguous, so both writes are sequential.
        state.keys.resize(rows);
        part_of_row_.resize(rows);
        const std::size_t ranges = std::max<std::size_t>(1, std::min(workers, rows));
        const std::size_t grain = (rows + ranges - 1) / ranges;
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                if (begin >= end) {
                    return;
                }
                PackedKeyEncoder::build_keys<Packed>(cols, begin, end, state.keys.data());
                Hash hasher;
                for (std::size_t row = begin; row < end; ++row) {
                    part_of_row_[row] =
                        static_cast<std::uint8_t>(hasher(state.keys[row]) & part_mask);
                }
            });
            batch.wait();
        }

        // Pass 2: one worker per partition, each scanning the whole chunk and
        // touching only its own rows and its own set.
        keep.assign(rows, 0);
        {
            auto batch = pool.submit(part_count, [&](std::size_t p) {
                auto& seen = state.parts[p];
                const auto tag = static_cast<std::uint8_t>(p);
                for (std::size_t row = 0; row < rows; ++row) {
                    if (part_of_row_[row] != tag) {
                        continue;
                    }
                    if (seen.insert(state.keys[row]).second) {
                        // Distinct partitions never share a row, so concurrent
                        // writes here never target the same byte.
                        keep[row] = 1;
                    }
                }
            });
            batch.wait();
        }
        return true;
    }

    /// The single-column twin of `try_packed_parallel`, for a store whose key
    /// IS the column's value. Same partitioned discovery, same determinism
    /// argument: keep flags are recorded at a row rather than a position, every
    /// partition is scanned ascending, so the row kept for a value is the first
    /// one however the workers interleave.
    ///
    /// A one-column `distinct` was the only shape with no parallel path at all,
    /// and it is the most common one. The asymmetry was visible from outside
    /// the engine: over the same 3M int64 keys, `distinct {k}` ran 70ms on one
    /// core and 68ms on eight, while `distinct {k, k}` -- strictly more work,
    /// but wide enough to reach the packed path -- ran 52ms on eight.
    /// Deduplicating on one key must not cost more than deduplicating on two.
    ///
    /// Only exact-identity value types reach here (`gather_distinct_rows`
    /// gates it): partitioning by hash is sound only where every pair the set
    /// calls equal also hashes alike. Integers give that. Floats do not -- -0.0
    /// and 0.0 compare equal and nothing promises they hash the same, so a
    /// partitioned run could emit both where the serial run emits one.
    template <typename T>
    auto try_typed_parallel(const Column<T>& col, std::size_t rows, TypedDedup<T>& state,
                            std::vector<std::uint8_t>& keep) -> bool {
        if (dedup_part_count_ == 0) {
            const std::size_t count = dedup_partition_count(rows);
            if (count == 0) {
                return false;
            }
            dedup_part_count_ = count;
        }
        if (state.parts.size() != dedup_part_count_) {
            // Whatever earlier chunks deduped serially lives in `seen`, which no
            // worker will ever probe, so it has to move into the partitions
            // before the first parallel chunk runs -- otherwise a value already
            // emitted is inserted afresh and emitted a SECOND time. The row gate
            // is per chunk, so a small chunk ahead of a large one produces
            // exactly that. Same hasher and mask the workers use below, or a
            // seeded value lands in a partition nobody probes for it.
            state.parts.resize(dedup_part_count_);
            const robin_hood::hash<T> seed_hasher;
            for (const T& value : state.seen) {
                state.parts[seed_hasher(value) & (dedup_part_count_ - 1)].insert(value);
            }
            state.seen.clear();
        }
        auto& pool = process_worker_pool();
        const std::size_t part_count = dedup_part_count_;
        const std::uint64_t part_mask = part_count - 1;

        // Pass 1: note each row's partition. No key buffer to fill -- the
        // column is one already -- so this is a read of `col` and a byte write.
        part_of_row_.resize(rows);
        const std::size_t ranges = std::max<std::size_t>(1, std::min(part_count, rows));
        const std::size_t grain = (rows + ranges - 1) / ranges;
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                const robin_hood::hash<T> hasher;
                for (std::size_t row = begin; row < end; ++row) {
                    part_of_row_[row] = static_cast<std::uint8_t>(hasher(col[row]) & part_mask);
                }
            });
            batch.wait();
        }

        // Pass 2: one worker per partition, each scanning the whole chunk and
        // touching only its own rows and its own set.
        keep.assign(rows, 0);
        {
            auto batch = pool.submit(part_count, [&](std::size_t p) {
                auto& seen = state.parts[p];
                const auto tag = static_cast<std::uint8_t>(p);
                for (std::size_t row = 0; row < rows; ++row) {
                    if (part_of_row_[row] != tag) {
                        continue;
                    }
                    if (seen.insert(col[row]).second) {
                        // Distinct partitions never share a row, so concurrent
                        // writes here never target the same byte.
                        keep[row] = 1;
                    }
                }
            });
            batch.wait();
        }
        return true;
    }

    template <typename Packed, typename Hash>
    auto process_packed(Table t, const std::vector<PackedKeyEncoder::PackCol>& cols,
                        PackedDedup<Packed, Hash>& state) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        if (try_packed_parallel(cols, rows, state, keep_)) {
            std::vector<std::size_t> idx;
            idx.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                if (keep_[row] != 0) {
                    idx.push_back(row);
                }
            }
            if (idx.empty()) {
                return std::nullopt;
            }
            if (idx.size() == rows) {
                return t;
            }
            // Once discovery is threaded the gather is what is left, and here
            // it is nearly a whole-table copy: q16 drops 58 of 236958 rows, so
            // 236900 rows of every column are rewritten just to close the gaps.
            //
            // Threading it anyway is a MEASURED DEAD END. `sort.cpp`'s
            // `gather_rows_parallel` was lifted into a shared header and called
            // from here; the profiler duly showed this operator's serial block
            // fall from 11.3ms to 8.7ms, and the wall did not move (q16 +0.7%
            // min, q21 +0.9% min, 8 interleaved rounds). The gather is memory
            // bound, so fanning it out over the same workers that just ran
            // discovery buys bandwidth that is already spent.
            return gather_rows(t, idx);
        }
        auto& seen = state.seen;
        // Growing to 118k entries one doubling at a time costs more than the
        // probing does: every rehash re-inserts everything already there, and a
        // packed key is wide enough that the table leaves cache early.
        //
        // Sizing for `rows` is exactly right when the input is all-distinct
        // (the table reaches that size anyway; only the rehashes are saved) and
        // wasteful in proportion to how duplicated the input is. Nothing here
        // knows the cardinality yet -- that is what the pass is about to find
        // out -- so the speculative part is capped by BYTES. A 50M-row chunk of
        // one repeated value would otherwise reserve well over a gigabyte.
        constexpr std::size_t kMaxSpeculativeBytes = 64UL << 20U;
        const std::size_t cap = kMaxSpeculativeBytes / sizeof(Packed);
        seen.reserve(seen.size() + std::min(rows, cap));
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            Packed key{};
            for (const auto& col : cols) {
                std::uint64_t cell = 0;
                switch (col.kind) {
                    case PackedKeyEncoder::PackCol::Kind::Int64:
                        cell = static_cast<std::uint64_t>(col.i64[row]);
                        break;
                    case PackedKeyEncoder::PackCol::Kind::Date:
                        cell = static_cast<std::uint32_t>(col.date[row].days);
                        break;
                    case PackedKeyEncoder::PackCol::Kind::Ts:
                        cell = static_cast<std::uint64_t>(col.ts[row].nanos);
                        break;
                    case PackedKeyEncoder::PackCol::Kind::Bool:
                        cell = (*col.boolean)[row] ? 1U : 0U;
                        break;
                    case PackedKeyEncoder::PackCol::Kind::Cat:
                        cell = col.remap[static_cast<std::size_t>(col.cat->code_at(row))];
                        break;
                }
                if constexpr (std::is_same_v<Packed, std::uint64_t>) {
                    key |= cell << col.shift;
                } else {
                    PackedKeyEncoder::splice(key, cell, col.shift);
                }
            }
            if (seen.insert(key).second) {
                idx.push_back(row);
            }
        }
        if (idx.empty()) {
            return std::nullopt;
        }
        // `distinct` keeps the first occurrence of each row in input order and
        // this operator is stateful, so its chunks arrive in order too: a
        // `RowTransform::Subset`, under which every claim the input made still
        // holds. The metadata therefore rides through untouched.
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto process_single_column(Table t) -> std::optional<Table> {
        const ColumnValue& column = *t.columns.front().column;
        if (const auto* col = std::get_if<Column<std::int64_t>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_i64_, *col);
        }
        if (const auto* col = std::get_if<Column<double>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_f64_, *col);
        }
        if (const auto* col = std::get_if<Column<bool>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_bool_, *col);
        }
        if (const auto* col = std::get_if<Column<Date>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_date_, *col);
        }
        if (const auto* col = std::get_if<Column<Timestamp>>(&column)) {
            return gather_distinct_rows(std::move(t), seen_timestamp_, *col);
        }
        if (const auto* col = std::get_if<Column<std::string>>(&column)) {
            return gather_distinct_string_rows(std::move(t), *col);
        }
        if (const auto* col = std::get_if<Column<Categorical>>(&column)) {
            return gather_distinct_categorical_rows(std::move(t), *col);
        }
        return std::nullopt;
    }

    /// One column's raw value hashed exactly as `hash_key_row` hashes a
    /// non-null single-column row -- what every hash `key_index_` stores must
    /// use, or a later chunk's probe would miss a migrated group's slot.
    ///
    /// Built from the SAME two helpers `hash_key_row` uses rather than
    /// open-coding the arithmetic, because open-coding it is what broke: adding
    /// a final avalanche to the two functions in `interpreter_internal.hpp` left
    /// this third copy behind, and the migrated groups promptly duplicated
    /// themselves. Expressed this way the three cannot disagree again.
    static auto mix_one(std::uint64_t value) -> std::uint64_t {
        std::uint64_t seed = 0;
        key_hash_mix(seed, value);
        return key_hash_finalize(seed);
    }

    /// Seed `group_order_`/`key_index_` with every value a single-column typed
    /// store (`seen_i64_`, `seen_strings_`, ...) already accepted, so a later
    /// chunk's generic probe treats them as already-emitted instead of
    /// re-emitting them. Distinct has no group order to preserve -- unlike the
    /// aggregate operator's gid-indexed slots, nothing downstream is keyed by
    /// the position a value lands at here -- so the values can be seeded in
    /// whatever order the typed set iterates them.
    template <typename Container, typename ToScalar, typename ToHash>
    void seed_generic_dedup_values(const Container& seen, const ToScalar& to_scalar,
                                   const ToHash& to_hash) {
        group_order_.reserve(group_order_.size() + seen.size());
        key_index_.hashes.reserve(key_index_.hashes.size() + seen.size());
        for (const auto& v : seen) {
            Key key;
            key.values.push_back(to_scalar(v));
            group_order_.push_back(std::move(key));
            key_index_.hashes.push_back(to_hash(v));
        }
    }

    void rehash_generic_dedup() {
        std::size_t capacity = 1024;
        while (capacity * 7 < key_index_.hashes.size() * 10) {
            capacity *= 2;
        }
        key_index_.rehash(capacity);
    }

    template <typename Container, typename ToScalar, typename ToHash>
    void seed_generic_dedup_from(const Container& seen, const ToScalar& to_scalar,
                                 const ToHash& to_hash) {
        seed_generic_dedup_values(seen, to_scalar, to_hash);
        rehash_generic_dedup();
    }

    /// A typed store holds its values in `seen` before the parallel path has run
    /// and in `parts` after it -- never both, but seeding from each in turn is
    /// correct either way, and missing the partitions would silently re-emit
    /// every value the threaded chunks had already accepted.
    template <typename T, typename ToScalar, typename ToHash>
    void seed_generic_dedup_from(const TypedDedup<T>& state, const ToScalar& to_scalar,
                                 const ToHash& to_hash) {
        seed_generic_dedup_values(state.seen, to_scalar, to_hash);
        for (const auto& part : state.parts) {
            seed_generic_dedup_values(part, to_scalar, to_hash);
        }
        rehash_generic_dedup();
    }

    /// Migrate whichever single-column typed store `process_single_column`
    /// used into the generic validity-aware index, so the transition to a
    /// nullable chunk keeps every row already emitted instead of erroring or
    /// re-emitting them. Categorical is excluded: `seen_cat_flags_` is keyed
    /// by dictionary-relative code, not a portable value, so (like the packed
    /// multi-key stores) it is left to the explicit-failure path.
    auto migrate_single_column_dedup_to_generic(const ColumnValue& column) -> bool {
        if (std::holds_alternative<Column<std::int64_t>>(column)) {
            seed_generic_dedup_from(
                seen_i64_, [](std::int64_t v) -> ScalarValue { return v; },
                [](std::int64_t v) { return mix_one(std::hash<std::int64_t>{}(v)); });
            return true;
        }
        if (std::holds_alternative<Column<double>>(column)) {
            seed_generic_dedup_from(
                seen_f64_, [](double v) -> ScalarValue { return v; },
                [](double v) { return mix_one(std::hash<double>{}(v)); });
            return true;
        }
        if (std::holds_alternative<Column<bool>>(column)) {
            seed_generic_dedup_from(
                seen_bool_, [](bool v) -> ScalarValue { return v; },
                [](bool v) { return mix_one(std::hash<bool>{}(v)); });
            return true;
        }
        if (std::holds_alternative<Column<Date>>(column)) {
            seed_generic_dedup_from(
                seen_date_, [](Date v) -> ScalarValue { return v; },
                [](Date v) { return mix_one(std::hash<Date>{}(v)); });
            return true;
        }
        if (std::holds_alternative<Column<Timestamp>>(column)) {
            seed_generic_dedup_from(
                seen_timestamp_, [](Timestamp v) -> ScalarValue { return v; },
                [](Timestamp v) { return mix_one(std::hash<Timestamp>{}(v)); });
            return true;
        }
        if (std::holds_alternative<Column<std::string>>(column)) {
            seed_generic_dedup_from(
                seen_strings_, [](std::string_view v) -> ScalarValue { return std::string(v); },
                [](std::string_view v) { return mix_one(std::hash<std::string_view>{}(v)); });
            return true;
        }
        return false;
    }

    OperatorPtr child_;
    // Multi-column dedup: `key_index_` hashes and compares each row in place and
    // holds one owned Key per distinct value in `group_order_` (the group-by hot
    // loop's mechanism). `seen_` is the fallback for a column type make_key_col
    // can't resolve — it boxes a Key per row, which is what this replaced.
    KeyRowIndex key_index_;
    std::vector<Key> group_order_;
    PackedDedup<std::uint64_t, robin_hood::hash<std::uint64_t>> packed64_;
    PackedDedup<PackedKeyEncoder::Packed128, PackedKeyEncoder::PackedWordsHash<2>> packed128_;
    PackedDedup<PackedKeyEncoder::Packed256, PackedKeyEncoder::PackedWordsHash<4>> packed256_;
    /// Scratch shared by every packed width, reused across chunks: the
    /// partition each row's key landed in, and whether the row is a first
    /// occurrence. Both are indexed by row and rewritten per chunk.
    std::vector<std::uint8_t> part_of_row_;
    std::vector<std::uint8_t> keep_;
    /// Pinned on first parallel use, and shared by the packed and typed stores
    /// -- an operator deduplicates on a fixed column count, so only one of them
    /// is ever the live store. A key's partition is `hash & (count-1)`, so a
    /// later chunk that partitioned differently would probe the wrong worker's
    /// set and re-emit a value already seen.
    std::size_t dedup_part_count_ = 0;
    PackedKeyEncoder encoder_;
    /// `t.columns` as pointers, rebuilt per chunk for the encoder. A member so
    /// the reserve is paid once rather than per chunk.
    std::vector<const ColumnEntry*> key_entries_;
    /// A validity-aware Key index and the typed/packed stores have incompatible
    /// identities. Once either kind has recorded a row, later chunks must not
    /// silently move to the other.
    bool fast_dedup_seen_ = false;
    bool generic_dedup_seen_ = false;
    /// The `dedup` phase's parallelism, resolved by the caller
    /// (`build_physical_distinct` from a footer estimate, `distinct_table` from
    /// the input's exact row count). The operator reads it -- see
    /// `dedup_partition_count` -- rather than deciding for itself.
    /// src/runtime/PARALLELISM.md, "Target: parallelism as a plan decision".
    physical::BreakerParallelism dedup_plan_{};
    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen_;
    TypedDedup<std::int64_t> seen_i64_;
    TypedDedup<double> seen_f64_;
    TypedDedup<bool> seen_bool_;
    TypedDedup<Date> seen_date_;
    TypedDedup<Timestamp> seen_timestamp_;
    std::vector<std::uint8_t> seen_cat_flags_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> seen_strings_;
    std::deque<std::string> owned_strings_;
    const void* cat_dictionary_id_ = nullptr;
};

class ChunkedSemiAntiJoinOperator final : public Operator {
   public:
    ChunkedSemiAntiJoinOperator(OperatorPtr left, Table right, ir::JoinKind kind,
                                const std::vector<ir::JoinKey>* keys, const ExecutionContext* exec)
        : left_(std::move(left)), right_(std::move(right)), kind_(kind), keys_(keys), exec_(exec) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
        }

        // Swapped mode: the left side was buffered during `initialize` (the
        // right was too large to set-ify cheaply), and the right-key set now
        // holds only the intersection of the two key columns, so a pass of
        // `filter_chunk` over the buffered left produces the result.
        //
        // Buffered as a LIST of chunks, not concatenated into one table. The
        // swap needs the left twice — once for its keys, once for its rows —
        // which is why it buffers at all, but it never needs the pieces glued
        // together. Gluing them cost a full copy of the left the moment sources
        // started arriving in more than one chunk: on q21 the semi join's own
        // time went 113ms -> 211ms, which was that copy and nothing else.
        if (swapped_) {
            while (swapped_next_ < left_buffered_.size()) {
                auto filtered = filter_chunk(std::move(left_buffered_[swapped_next_++]));
                if (!filtered.has_value()) {
                    continue;
                }
                return std::optional<Chunk>{table_to_chunk(std::move(*filtered))};
            }
            left_buffered_.clear();
            return std::optional<Chunk>{};
        }

        while (true) {
            auto chunk_res = left_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }

            Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_chunk(std::move(t));
            if (!filtered.has_value()) {
                continue;
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*filtered))};
        }
    }

   private:
    // Above this many right rows, building a hash set of every right key is the
    // dominant cost of the whole operator (q04: 3.8M inserts into a robin_hood
    // set, ~40% of the query). Past it, materialize the left and swap.
    static constexpr std::size_t kSemiSwapThreshold = 65536;

    /// Workers for the swapped build's intersection scan, or 0 to run it here.
    ///
    /// The scan is one hash lookup per right row against a map that stopped
    /// changing before it started, so it splits with no coordination — the same
    /// shape as `select_rows` below, and gated on the same row floor for the
    /// same reason: below it the fan-out costs more than the lookups it spreads.
    ///
    /// Capped additionally by a BYTE budget, which `select_rows` needs no
    /// equivalent of: each worker owns a private byte per left key, so the cost
    /// scales with the left's cardinality as well as the right's row count. A
    /// 57k-key left (q04) is 57KB per worker and free; a multi-million-key left
    /// would not be, and would rather have fewer workers than a large private
    /// allocation each.
    [[nodiscard]] auto intersect_worker_count(std::size_t right_rows, std::size_t slots) const
        -> std::size_t {
        constexpr std::size_t kSlotBudgetBytes = 8UL << 20;
        if (exec_ == nullptr || !exec_->can_fan_out() || on_worker_pool_thread() || slots == 0 ||
            right_rows < kMinParallelPredicateRows) {
            return 0;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec_->compute_budget();
        std::size_t workers = std::min({budget, pool.size(), std::size_t{64}});
        workers = std::min(workers, std::max<std::size_t>(kSlotBudgetBytes / slots, 1));
        return workers < 2 ? 0 : workers;
    }

    // Build the right-key set as the INTERSECTION of the two key columns, by
    // probing the large right against a map of the small left keys rather than
    // inserting every right key. `filter_chunk` then works unchanged: a left row
    // is in the intersection iff it has a right match (semi keeps those; anti
    // keeps the rest). Restricted to integer keys, which every TPC-H join uses
    // and where the win is; other key types keep the streaming build-on-right.
    auto init_int_swapped(const Column<std::int64_t>& rcol) -> std::optional<std::string> {
        // Drain the left into a list of chunks. Deliberately NOT
        // `MaterializeOperator`: see the note in `next()` — concatenating them
        // is a full copy of the left that nothing here needs.
        while (true) {
            auto chunk = left_->next();
            if (!chunk.has_value()) {
                return std::move(chunk.error());
            }
            if (!chunk->has_value()) {
                break;
            }
            left_buffered_.push_back(chunk_to_table(std::move(**chunk)));
        }
        swapped_ = true;

        const auto* rentry = right_.find_entry(keys_->front().right);
        const ValidityBitmap* rvalidity =
            rentry != nullptr && rentry->validity.has_value() ? &*rentry->validity : nullptr;
        const auto rnull = [rvalidity](std::size_t row) {
            return rvalidity != nullptr && !(*rvalidity)[row];
        };
        // The left key column, chunk by chunk. Every chunk must carry it as an
        // int64 for the intersection build to be worth taking; one that does not
        // falls back to the plain right set below, exactly as a missing key
        // column did when the left was one table.
        std::vector<const Column<std::int64_t>*> lcols;
        lcols.reserve(left_buffered_.size());
        std::size_t left_rows = 0;
        for (const auto& part : left_buffered_) {
            const ColumnValue* lkey = part.find(keys_->front().left);
            const auto* lcol = lkey != nullptr ? std::get_if<Column<std::int64_t>>(lkey) : nullptr;
            if (lcol == nullptr) {
                lcols.clear();
                break;
            }
            lcols.push_back(lcol);
            left_rows += lcol->size();
        }
        if (!lcols.empty() && left_rows < rcol.size()) {
            // A moderately dense integer key range does not need a hash lookup
            // for every row on the large side. q22's 84k in-scope customer
            // keys span only ~300k values: a byte-addressed candidate table is
            // smaller than the hash map and turns the 3M-order intersection
            // pass into two bounds checks plus array reads.
            //
            // Keep this a proved representation choice, not an assumption
            // about TPC-H keys. Sparse or very wide ranges decline to the hash
            // path below. Workers retain private hit arrays, so repeated right
            // keys never race and the final OR is deterministic.
            const auto try_dense_intersection = [&]() -> bool {
                if (left_rows == 0) {
                    return false;
                }
                std::int64_t min_key = std::numeric_limits<std::int64_t>::max();
                std::int64_t max_key = std::numeric_limits<std::int64_t>::min();
                for (const auto* lcol : lcols) {
                    for (const std::int64_t value : *lcol) {
                        min_key = std::min(min_key, value);
                        max_key = std::max(max_key, value);
                    }
                }
                const std::uint64_t difference =
                    static_cast<std::uint64_t>(max_key) - static_cast<std::uint64_t>(min_key);
                if (difference == std::numeric_limits<std::uint64_t>::max()) {
                    return false;  // the inclusive span is not representable
                }
                const std::uint64_t span64 = difference + 1;
                constexpr std::size_t kMaxDenseSlots = 2UL << 20U;
                constexpr std::size_t kMaxRangePerKey = 4;
                const std::size_t density_limit =
                    left_rows > std::numeric_limits<std::size_t>::max() / kMaxRangePerKey
                        ? std::numeric_limits<std::size_t>::max()
                        : left_rows * kMaxRangePerKey;
                if (span64 > kMaxDenseSlots || span64 > density_limit) {
                    return false;
                }
                const auto slots = static_cast<std::size_t>(span64);
                const auto slot_of = [min_key](std::int64_t value) {
                    return static_cast<std::size_t>(static_cast<std::uint64_t>(value) -
                                                    static_cast<std::uint64_t>(min_key));
                };
                std::vector<char> candidates(slots, char{0});
                for (const auto* lcol : lcols) {
                    for (const std::int64_t value : *lcol) {
                        candidates[slot_of(value)] = char{1};
                    }
                }
                const auto scan_range = [&](std::size_t lo, std::size_t hi, char* hits) {
                    for (std::size_t row = lo; row < hi; ++row) {
                        if (rnull(row)) {
                            continue;
                        }
                        const std::int64_t value = rcol[row];
                        if (value < min_key || value > max_key) {
                            continue;
                        }
                        const std::size_t slot = slot_of(value);
                        if (candidates[slot] != char{0}) {
                            hits[slot] = char{1};
                        }
                    }
                };

                std::vector<char> hits(slots, char{0});
                const std::size_t workers = intersect_worker_count(rcol.size(), slots);
                if (workers < 2) {
                    scan_range(0, rcol.size(), hits.data());
                } else {
                    std::vector<std::vector<char>> parts(workers,
                                                         std::vector<char>(slots, char{0}));
                    const std::size_t grain = (rcol.size() + workers - 1) / workers;
                    auto batch = process_worker_pool().submit(workers, [&](std::size_t worker) {
                        const std::size_t begin = worker * grain;
                        if (begin < rcol.size()) {
                            scan_range(begin, std::min(rcol.size(), begin + grain),
                                       parts[worker].data());
                        }
                    });
                    batch.wait();
                    for (const auto& part : parts) {
                        for (std::size_t slot = 0; slot < slots; ++slot) {
                            hits[slot] = static_cast<char>(hits[slot] | part[slot]);
                        }
                    }
                }
                // Retain the dense representation for the buffered-left probe
                // too. Converting the hits back into a hash set would throw
                // away the representation win just before probing q22's 84k
                // customer rows.
                dense_i64_min_ = min_key;
                dense_i64_hits_ = std::move(hits);
                return true;
            };
            if (try_dense_intersection()) {
                return std::nullopt;
            }

            // 57k inserts + 3.8M finds, versus 3.8M inserts the other way.
            //
            // The map stores a DENSE INDEX rather than a matched flag, which is
            // what lets the 3.8M-row scan below be split. Ranges of the right
            // column are independent — the scan only ever marks an existing
            // slot, never inserts — so each worker marks into its own byte
            // vector over the left keys and they are ORed afterwards. No
            // atomics, and the answer cannot depend on the split: `right_i64_`
            // is a set, so only WHICH keys were hit matters, not the order they
            // were found in.
            robin_hood::unordered_flat_map<std::int64_t, std::uint32_t> seen;
            seen.reserve(left_rows);
            std::uint32_t next_slot = 0;
            for (const auto* lcol : lcols) {
                for (const std::int64_t v : *lcol) {
                    if (seen.try_emplace(v, next_slot).second) {
                        ++next_slot;
                    }
                }
            }
            const std::size_t n_slots = next_slot;

            const auto scan_range = [&](std::size_t lo, std::size_t hi, char* hits) {
                for (std::size_t i = lo; i < hi; ++i) {
                    if (rnull(i)) {
                        continue;  // a null right key puts nothing in the set
                    }
                    if (auto it = seen.find(rcol[i]); it != seen.end()) {
                        hits[it->second] = char{1};
                    }
                }
            };

            std::vector<char> hits(n_slots, char{0});
            const std::size_t workers = intersect_worker_count(rcol.size(), n_slots);
            if (workers < 2) {
                scan_range(0, rcol.size(), hits.data());
            } else {
                // One private vector per worker, ORed below. `n_slots` bytes
                // each, which is why the worker count is capped by a byte
                // budget rather than by the pool size alone.
                std::vector<std::vector<char>> parts(workers, std::vector<char>(n_slots, char{0}));
                const std::size_t grain = (rcol.size() + workers - 1) / workers;
                auto batch = process_worker_pool().submit(workers, [&](std::size_t w) {
                    const std::size_t lo = w * grain;
                    if (lo < rcol.size()) {
                        scan_range(lo, std::min(rcol.size(), lo + grain), parts[w].data());
                    }
                });
                batch.wait();
                for (const auto& part : parts) {
                    for (std::size_t slot = 0; slot < n_slots; ++slot) {
                        hits[slot] = static_cast<char>(hits[slot] | part[slot]);
                    }
                }
            }

            for (const auto& [k, slot] : seen) {
                if (hits[slot] != char{0}) {
                    right_i64_.insert(k);
                }
            }
        } else {
            // Left is not the smaller side (or its key vanished); the plain
            // right set is as good, and the buffered left still emits.
            right_i64_.reserve(rcol.size());
            for (std::size_t i = 0; i < rcol.size(); ++i) {
                if (!rnull(i)) {
                    right_i64_.insert(rcol[i]);
                }
            }
        }
        return std::nullopt;
    }

    auto initialize() -> std::optional<std::string> {
        if (keys_->size() != 1) {
            return "ChunkedSemiAntiJoinOperator only supports single-key joins";
        }
        if (right_.columns.empty()) {
            return std::nullopt;
        }
        const ColumnValue* key = right_.find(keys_->front().right);
        if (key == nullptr) {
            return "join key not found in right table: " + keys_->front().right;
        }
        // The other half of "a null matches nothing": a null-keyed right row is
        // never put in the set, so nothing can find it. Skipping it on the probe
        // side alone would still let a null here be found by a genuine zero.
        const auto* right_entry = right_.find_entry(keys_->front().right);
        const ValidityBitmap* build_validity =
            right_entry != nullptr && right_entry->validity.has_value() ? &*right_entry->validity
                                                                        : nullptr;
        const auto build_is_null = [build_validity](std::size_t row) {
            return build_validity != nullptr && !(*build_validity)[row];
        };

        if (const auto* col = std::get_if<Column<std::int64_t>>(key)) {
            right_kind_ = ExprType::Int;
            if (col->size() > kSemiSwapThreshold) {
                return init_int_swapped(*col);
            }
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (!build_is_null(i)) {
                    right_i64_.insert((*col)[i]);
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<double>>(key)) {
            right_kind_ = ExprType::Double;
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (!build_is_null(i)) {
                    right_f64_.insert((*col)[i]);
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<bool>>(key)) {
            right_kind_ = ExprType::Bool;
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (!build_is_null(i)) {
                    right_bool_.insert((*col)[i]);
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Date>>(key)) {
            right_kind_ = ExprType::Date;
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (!build_is_null(i)) {
                    right_date_.insert((*col)[i]);
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Timestamp>>(key)) {
            right_kind_ = ExprType::Timestamp;
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (!build_is_null(i)) {
                    right_timestamp_.insert((*col)[i]);
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Categorical>>(key)) {
            right_kind_ = ExprType::String;
            right_cat_dictionary_id_ = static_cast<const void*>(col->dictionary_ptr().get());
            for (std::size_t row = 0; row < col->size(); ++row) {
                if (!build_is_null(row)) {
                    right_cat_codes_.insert(col->code_at(row));
                }
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<std::string>>(key)) {
            right_kind_ = ExprType::String;
            for (std::size_t i = 0; i < col->size(); ++i) {
                if (build_is_null(i)) {
                    continue;
                }
                owned_strings_.emplace_back((*col)[i]);
                right_strings_.insert(std::string_view{owned_strings_.back()});
            }
            return std::nullopt;
        }
        return "ChunkedSemiAntiJoinOperator: unsupported key type";
    }

    // Below this the fan-out costs more than the probes it splits: a probe runs
    // at ~10ns/row, so a 1<<16 chunk is a ~0.6ms pass, and there is nothing
    // there for eight threads to divide. The queries this split exists for are
    // two orders of magnitude past the gate either way -- q21 probes 21.5M rows
    // -- so it is set where a mistake is cheap rather than where it is tight.
    static constexpr std::size_t kMinParallelPredicateRows = 1U << 18U;

    /// The surviving row indices, ascending, evaluated across the worker pool.
    ///
    /// Every predicate this operator builds probes ONE key cell against a set
    /// that stopped changing before the first left chunk arrived: it reads the
    /// key column, the set, and a validity bitmap, and writes nothing. So the
    /// rows split with no coordination at all, and each range can build its own
    /// index list -- one memcpy per range to concatenate, rather than a second
    /// full pass over a keep-flag array.
    ///
    /// Ranges are contiguous and appended in order, so the result stays
    /// ascending, which both `gather_rows` and every consumer of the chunk
    /// require.
    template <typename Pred>
    auto select_rows(std::size_t rows, Pred pred) -> std::vector<std::size_t> {
        auto serial_select = [&] {
            std::vector<std::size_t> idx;
            idx.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                if (pred(row)) {
                    idx.push_back(row);
                }
            }
            return idx;
        };
        // The context checks come before the pool binding for the same reason
        // as everywhere else: constructing the pool spawns its threads
        // eagerly, and a serial query must not pay for them.
        if (exec_ == nullptr || !exec_->can_fan_out() || on_worker_pool_thread() ||
            rows < kMinParallelPredicateRows) {
            return serial_select();
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec_->compute_budget();
        const std::size_t workers = std::min(budget, pool.size());
        // `submit` CLAMPS its worker count to the pool size, so a range count
        // above it would leave those ranges unvisited and silently drop rows.
        const std::size_t ranges = std::max<std::size_t>(1, std::min(workers, rows));
        if (ranges < 2) {
            return serial_select();
        }

        const std::size_t grain = (rows + ranges - 1) / ranges;
        std::vector<std::vector<std::size_t>> parts(ranges);
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                if (begin >= end) {
                    return;
                }
                auto& out = parts[r];
                out.reserve(end - begin);
                for (std::size_t row = begin; row < end; ++row) {
                    if (pred(row)) {
                        out.push_back(row);
                    }
                }
            });
            batch.wait();
        }

        std::size_t total = 0;
        for (const auto& part : parts) {
            total += part.size();
        }
        std::vector<std::size_t> idx;
        idx.reserve(total);
        for (const auto& part : parts) {
            idx.insert(idx.end(), part.begin(), part.end());
        }
        return idx;
    }

    template <typename Pred>
    auto filter_rows(Table t, Pred pred) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        const std::vector<std::size_t> idx = select_rows(rows, pred);
        if (idx.empty()) {
            return std::nullopt;
        }
        if (idx.size() == rows) {
            return t;
        }
        return gather_rows(t, idx);
    }

    auto filter_chunk(Table t) -> std::optional<Table> {
        const ColumnValue* key = t.find(keys_->front().left);
        if (key == nullptr) {
            return std::nullopt;
        }
        const bool keep_matches = (kind_ == ir::JoinKind::Semi);
        // A null key matches nothing, not even another null. The set below is
        // keyed by VALUE and a null cell holds its type's zero, so without this
        // a null-keyed row would match a genuine zero on the other side --
        // silently, and in the direction that keeps rows a semi join should
        // drop and drops rows an anti join should keep.
        const auto* probe_entry = t.find_entry(keys_->front().left);
        const ValidityBitmap* probe_validity =
            probe_entry != nullptr && probe_entry->validity.has_value() ? &*probe_entry->validity
                                                                        : nullptr;
        const auto probe_is_null = [probe_validity](std::size_t row) {
            return probe_validity != nullptr && !(*probe_validity)[row];
        };

        if (right_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                bool match = false;
                if (!probe_is_null(row)) {
                    if (dense_i64_min_.has_value()) {
                        const std::uint64_t slot = static_cast<std::uint64_t>((*col)[row]) -
                                                   static_cast<std::uint64_t>(*dense_i64_min_);
                        match = slot < dense_i64_hits_.size() &&
                                dense_i64_hits_[static_cast<std::size_t>(slot)] != char{0};
                    } else {
                        match = right_i64_.contains((*col)[row]);
                    }
                }
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = !probe_is_null(row) && right_f64_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = !probe_is_null(row) && right_bool_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = !probe_is_null(row) && right_date_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = !probe_is_null(row) && right_timestamp_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }

        if (const auto* col = std::get_if<Column<Categorical>>(key);
            col != nullptr &&
            static_cast<const void*>(col->dictionary_ptr().get()) == right_cat_dictionary_id_) {
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match =
                    !probe_is_null(row) && right_cat_codes_.contains(col->code_at(row));
                return keep_matches ? match : !match;
            });
        }

        if (const auto* col = std::get_if<Column<Categorical>>(key)) {
            const void* left_dict_id = static_cast<const void*>(col->dictionary_ptr().get());
            if (left_cat_dictionary_id_ != left_dict_id) {
                left_cat_dictionary_id_ = left_dict_id;
                left_cat_matches_.assign(col->dictionary().size(), uint8_t{0});
                const auto& dict = col->dictionary();
                for (std::size_t i = 0; i < dict.size(); ++i) {
                    left_cat_matches_[i] =
                        static_cast<uint8_t>(right_strings_.contains(std::string_view{dict[i]}));
                }
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const auto code = static_cast<std::size_t>(col->code_at(row));
                const bool match = left_cat_matches_[code] != 0U;
                return keep_matches ? match : !match;
            });
        }
        if (const auto* col = std::get_if<Column<std::string>>(key)) {
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = !probe_is_null(row) && right_strings_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        return std::nullopt;
    }

    OperatorPtr left_;
    Table right_;
    ir::JoinKind kind_;
    const std::vector<ir::JoinKey>* keys_;
    bool initialized_ = false;
    bool swapped_ = false;
    /// The left side, buffered as chunks rather than concatenated.
    std::vector<Table> left_buffered_;
    std::size_t swapped_next_ = 0;
    ExprType right_kind_ = ExprType::Int;
    const ExecutionContext* exec_ = nullptr;

    robin_hood::unordered_flat_set<std::int64_t> right_i64_;
    std::optional<std::int64_t> dense_i64_min_;
    std::vector<char> dense_i64_hits_;
    robin_hood::unordered_flat_set<double> right_f64_;
    robin_hood::unordered_flat_set<bool> right_bool_;
    robin_hood::unordered_flat_set<Date> right_date_;
    robin_hood::unordered_flat_set<Timestamp> right_timestamp_;
    robin_hood::unordered_flat_set<Column<Categorical>::code_type> right_cat_codes_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> right_strings_;
    std::deque<std::string> owned_strings_;
    const void* right_cat_dictionary_id_ = nullptr;
    const void* left_cat_dictionary_id_ = nullptr;
    std::vector<uint8_t> left_cat_matches_;
};



}  // namespace

auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string> {
    MaterializeOperator sink{std::move(op)};
    return sink.run();
}

/// Defined next to `build_physical_join`; the join construction sites above it
/// (`inner_join_table`, the `IBEX_PROBE_MORSELS` probe POC) need it too.
namespace physical_executor_detail {
auto resolved_join_parallelism(const ExecutionContext& exec) -> physical::JoinParallelism;
}  // namespace physical_executor_detail

auto distinct_table(const Table& input, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    // I4 convergence: one implementation, reached through both shapes. The
    // whole-table signature survives; the whole-table dedup loop does not.
    //
    // The source copy is shallow — a `Table` holds shared column handles — so
    // this costs a vector of names and pointers, not the data. `distinct` on an
    // empty column list still works: the operator passes such a chunk straight
    // through, which is what the old `columns.empty()` special case did.
    auto source = make_table_source(input);
    // The whole table is one chunk, so its row count is exact -- the dedup
    // phase's fan-out is fully decided here rather than on the first chunk.
    physical::BreakerParallelism dedup_plan = physical::distinct_dedup_parallelism(
        {.rows = input.rows(), .source = physical::RowEstimate::Source::ChildExact});
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(dedup_plan, exec, pool_size);
    return materialize_operator(
        std::make_unique<ChunkedDistinctOperator>(std::move(source), dedup_plan));
}

auto is_streamable_inner_join(const ir::JoinNode& join) -> bool {
    // `nulls equal` goes to the materialized join, which implements the policy.
    // The streaming operators hash and probe on their own and would each need
    // the same null tagging; sending the opt-in case to the one implementation
    // that has it keeps a single definition of the semantics.
    return join.kind() == ir::JoinKind::Inner && !join.predicate().has_value() &&
           join.keys().size() == 1 && join.null_match() == ir::NullMatch::Never &&
           !join.expect().asserts_anything() && join.take() == ir::MatchSelection::All;
}

/// Two-fixed-width-int-key inner join (plans/parallelism-overview.md's
/// "stream multi-key joins" item; TPC-H q09's `lineitem` join is the
/// motivating case). Same structural gate as `is_streamable_inner_join`
/// plus a static schema check -- both keys, on both sides, must be provably
/// `Int64` -- so an ineligible pair (a string key, an unascribed/Unknown
/// schema) falls through to the materialized-call fallback (`interpret_node`'s
/// `Join` branch) exactly as it does today, never into a code path that could
/// fail at runtime.
/// `ChunkedInnerJoinOperator::initialize_pair` re-checks the actual runtime
/// column type regardless -- this is a routing optimization, not the sole
/// guarantee of correctness.
/// Whether every aggregation in `agg` can be computed incrementally, and so
/// streamed rather than materialized.
///
/// Named and shared for the same reason the join gates were: the physical
/// planner has to relay this rather than restate it. Reimplementing a
/// multi-clause eligibility test in the planner is what made it wrong about
/// two-key joins within an hour of being written.
auto aggregate_is_streamable(const ir::AggregateNode& agg) -> bool {
    return std::ranges::all_of(agg.aggregations(), [](const ir::AggSpec& spec) {
        switch (spec.func) {
            case ir::AggFunc::Count:
            case ir::AggFunc::Sum:
            case ir::AggFunc::Min:
            case ir::AggFunc::Max:
            case ir::AggFunc::Mean:
            case ir::AggFunc::Stddev:
            case ir::AggFunc::Skew:
            case ir::AggFunc::Kurtosis:
            case ir::AggFunc::First:
            case ir::AggFunc::Last:
                // First/Last: the operators themselves gate by column type
                // (numeric, string, categorical stream; Date/Timestamp fall to
                // the hash operator's error path -- unreachable in practice
                // since aggregation on those types is rejected upstream of the
                // chunked path entirely, same as every other agg func).
                return true;
            default:
                // Median/Quantile need all values; Ewma is row-order coupled --
                // these stay on the materializing path.
                return false;
        }
    });
}

auto is_streamable_semi_anti_join(const ir::JoinNode& join) -> bool {
    return (join.kind() == ir::JoinKind::Semi || join.kind() == ir::JoinKind::Anti) &&
           !join.predicate().has_value() && join.keys().size() == 1 &&
           join.null_match() == ir::NullMatch::Never && !join.expect().asserts_anything() &&
           join.take() == ir::MatchSelection::All;
}

auto is_streamable_pair_int_join(const ir::JoinNode& join) -> bool {
    if (join.kind() != ir::JoinKind::Inner || join.predicate().has_value() ||
        join.keys().size() != 2 || join.null_match() != ir::NullMatch::Never ||
        join.expect().asserts_anything() || join.take() != ir::MatchSelection::All) {
        return false;
    }
    const ir::SchemaInfo left_schema = ir::infer_schema(*join.children()[0]);
    const ir::SchemaInfo right_schema = ir::infer_schema(*join.children()[1]);
    if (!left_schema.is_known() || !right_schema.is_known()) {
        return false;
    }
    // NOLINTNEXTLINE(readability-use-anyofallof)
    for (const ir::JoinKey& key : join.keys()) {
        const ir::SchemaField* lf = left_schema.find(key.left);
        const ir::SchemaField* rf = right_schema.find(key.right);
        if (lf == nullptr || rf == nullptr || !lf->type.has_value() || !rf->type.has_value() ||
            *lf->type != ir::ColumnType::Int64 || *rf->type != ir::ColumnType::Int64) {
            return false;
        }
    }
    return true;
}

auto inner_join_table(const Table& left, const Table& right, const std::vector<ir::JoinKey>& keys,
                      const ir::JoinSuffixPolicy& suffix,
                      const std::vector<ir::OrderKey>& pending_order, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    // I4 convergence: keep the materialized signature used by interpret_node,
    // but run the same hash join that build_operator selects for this exact
    // semantic subset. Table sources copy only their column handles.
    auto source = make_table_source(left);
    return materialize_operator(make_chunked_inner_join_operator(
        std::move(source), right, &keys, exec, suffix, &pending_order,
        physical_executor_detail::resolved_join_parallelism(exec)));
}

namespace {

// The single choke point for "build this subtree, then immediately drain it
// to a whole Table" — every call site in this file that needs a materialized
// side (a join's build/probe side, an update's input, an aggregate's fused
// join operand, ...) with no downstream consumer to overlap the build with
// routes through here, rather than each hand-rolling its own
// `build_operator` + `materialize_operator` pair. One place to reason about
// this pattern instead of N independently-drifting copies.
auto materialize_row_local(const ir::Node& node, const TableRegistry& registry,
                           const ScalarRegistry* scalars, const ExternRegistry* externs,
                           const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    auto op = build_operator(node, registry, scalars, externs, exec, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    return materialize_operator(std::move(op.value()));
}

template <typename Fn>

auto build_unary_materializing_operator(const ir::Node& child_node, const TableRegistry& registry,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec,
                                        ModelResult* model_out, Fn fn)
    -> std::expected<OperatorPtr, std::string> {
    auto materialized =
        materialize_row_local(child_node, registry, scalars, externs, exec, model_out);
    if (!materialized.has_value()) {
        return std::unexpected(std::move(materialized.error()));
    }
    auto result = fn(std::move(materialized.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return make_table_source(std::move(result.value()));
}

}  // namespace

auto make_join_probe_operator(OperatorPtr source, std::optional<Table> materialized_source,
                              JoinProbeFactory probe)
    -> std::expected<OperatorPtr, std::string> {
    const ExecutionContext* exec = probe.execution_context();
    if (materialized_source.has_value() && exec != nullptr) {
        if (const std::size_t workers =
                pipeline_executor_detail::probe_morsel_workers(*materialized_source, *exec);
            workers >= 2) {
            return pipeline_executor_detail::build_probe_morsel_pipeline(
                std::move(*materialized_source), probe, workers, *exec);
        }
    }
    OperatorPtr probe_source = materialized_source.has_value()
                                   ? make_table_source(std::move(*materialized_source))
                                   : std::move(source);
    if (probe_source == nullptr) {
        return std::unexpected("join probe has no probe-side source");
    }
    return probe.attach_move(std::move(probe_source));
}

namespace {

// A materializing binary breaker (non-streamable join, matmul) now resolves in
// `interpret_node`, which drains both sides whole-table and serially.
// Overlapping the two materializations on a raw std::thread was tried twice
// (once unbudgeted, once under a since-removed helper-thread budget) and
// regressed the PDS-H suite both times (q09 +57% / +47.5%): the cost is
// structural -- both sides are already-expensive full materializations
// contending for the same cores/bandwidth -- not a concurrency count a budget
// could bound. A future attempt needs a cost-aware gate (skip when both sides
// are large), and belongs wherever that breaker is lifted onto the physical plan.

auto eval_extern_args(const std::vector<ir::Expr>& exprs, const ScalarRegistry* scalars,
                      const ExternRegistry* externs) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size());
    for (const auto& arg : exprs) {
        auto val = eval_expr(arg, Table{}, 0, scalars, externs);
        if (!val.has_value()) {
            return std::unexpected(std::move(val.error()));
        }
        // Externs take null-free ScalarValue arguments (see the null-arm plan).
        auto scalar = scalar_from_expr(val.value());
        if (!scalar.has_value()) {
            return std::unexpected("null argument in extern function call");
        }
        args.push_back(std::move(*scalar));
    }
    return args;
}

}  // namespace

auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                        const ExternRegistry* externs) -> std::expected<ExternValue, std::string> {
    if (externs == nullptr) {
        return std::unexpected("extern call with no registry: " + ec.callee());
    }
    const auto* fn = externs->find(ec.callee());
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + ec.callee());
    }
    if (fn->first_arg_is_table) {
        return std::unexpected("extern function requires a table input: " + ec.callee());
    }
    auto args = eval_extern_args(ec.args(), scalars, externs);
    if (!args.has_value()) {
        return std::unexpected(std::move(args.error()));
    }
    if (fn->kind == ExternReturnKind::Table && fn->chunked_table_func) {
        auto source = fn->chunked_table_func(args.value());
        if (source.has_value()) {
            auto materialized = materialize_operator(std::move(source.value()));
            if (!materialized.has_value()) {
                return std::unexpected(std::move(materialized.error()));
            }
            return ExternValue{std::move(materialized.value())};
        }
    }
    auto result = fn->func(args.value());
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return result;
}

auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                              const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<void, std::string> {
    for (const auto& node : preamble) {
        if (node->kind() != ir::NodeKind::ExternCall) {
            return std::unexpected("program preamble only supports extern calls");
        }
        const auto& ec = ir::node_cast<ir::ExternCallNode>(*node);
        auto result = invoke_extern_call(ec, scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
    }
    return {};
}

/// Planner seam: returns a pull-based operator that, when drained,
/// produces the logical result of `node`. Chunked operators exist
/// today for node kinds that are safe and useful to stream; any other
/// node kind falls back to the full-table `interpret_node` path and
/// is wrapped in a `TableSourceOperator` so downstream chunked
/// operators see a uniform pull-based interface.
// Order-delay past Filter/Project/Rename, and Head/Tail pushdown past
// Project/Rename, are handled by the IR canonicalize pass
// (src/ir/canonicalize.cpp). IR arrives here in canonical form, so
// build_operator only needs one branch per NodeKind and the shapes it
// matches are the post-canonicalization shapes (e.g. Project(Filter(x))
// for the fused operator, not Project(Filter(Order(x)))).

auto morsel_grain(const ExecutionContext& exec, std::size_t rows) -> std::size_t {
    if (exec.parallel_grain != 0) {
        return exec.parallel_grain;  // explicit override, used as given
    }
    // Aim for this many morsels per worker, so one slow morsel cannot strand
    // the others. Below ~2 per thread the sweep shows real imbalance loss.
    constexpr std::size_t kMorselsPerThread = 4;
    // The measured good band was 16k-256k; clamp inside it. The upper bound is
    // what stops the formula from choosing a grain worse than the old constant
    // on a large input — see the declaration.
    constexpr std::size_t kMinGrain = 4096;
    constexpr std::size_t kMaxGrain = 65536;

    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t threads = std::max<std::size_t>(std::min(budget, pool_size), 1);
    return std::clamp(rows / (threads * kMorselsPerThread), kMinGrain, kMaxGrain);
}

auto process_pipeline_stats() -> ParallelPipelineStats* {
    // File-local, like the worker pool and the query lease: a bundled plugin
    // statically links runtime code, so an inline header variable would give
    // each plugin its own counter (the RTLD_LOCAL trap).
    //
    // The reporter is a separate static whose destructor runs at exit. It holds
    // no reference to the counters it prints beyond the function-local statics
    // above it, which outlive it by declaration order.
    static const bool enabled = std::getenv("IBEX_PARALLEL_STATS") != nullptr;
    static ParallelPipelineStats stats;
    // Function-local exit reporter, instantiated once below; never copied or moved.
    // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
    struct Reporter {
        ~Reporter() {
            if (!enabled) {
                return;
            }
            ibex::formatting::print(
                stderr,
                "pipeline stats: parallel={} serial={} morsels={} "
                "pipelined_scans={} pipelined_stages={} range_heads={} two_phase={} "
                "parallel_fields={} parallel_direct_numeric_fields={} parallel_probes={} "
                "parallel_hash_builds={} parallel_aggregate_partitions={} "
                "parallel_aggregate_finalizes={} "
                "grouped_lifted_group_state={} chunk_direct_updates={}\n",
                stats.parallel_pipelines.load(), stats.serial_pipelines.load(),
                stats.morsels.load(), stats.pipelined_scans.load(), stats.pipelined_stages.load(),
                stats.range_heads.load(), stats.two_phase_filters.load(),
                stats.parallel_fields.load(), stats.parallel_direct_numeric_fields.load(),
                stats.parallel_probes.load(), stats.parallel_hash_builds.load(),
                stats.parallel_aggregate_partitions.load(),
                stats.parallel_aggregate_finalizes.load(), stats.grouped_lifted_group_state.load(),
                stats.chunk_direct_updates.load());
        }
    };
    static const Reporter reporter;
    return enabled ? &stats : nullptr;
}

void configure_parallel_from_env(ExecutionContext& exec) {
    // The other two execution switches, applied the same way and for the same
    // reason: one authority per setting. Both were `getenv` at their use sites
    // until the seams that share them started to outnumber the settings —
    // `stream_scans` is read at three build seams that must agree, and
    // `parallel_join_probe` at three probe gates. An unset variable leaves the
    // caller's choice alone, so a context built by hand still means "ignore the
    // environment".
    if (const auto want = stream_scans_from_env(); want.has_value()) {
        exec.stream_scans = *want;
    }
    if (const auto want = parallel_join_probe_from_env(); want.has_value()) {
        exec.parallel_join_probe = *want;
    }
    // Pin the COMPUTE budget to the core count. Every compute gate sizes itself
    // from `parallel_threads`, falling back to the pool size when it is 0 — and
    // the pool is now sized for decode, which wants more threads than cores
    // (`decode_oversubscribe`). Without this, growing the pool would
    // oversubscribe the compute paths too, and those measurably do not want it
    // (q01 +4.6%, q17 +3.2%). With a multiplier of 1 this is the same number the
    // fallback produced, so the default configuration is unchanged.
    if (exec.parallel_threads == 0) {
        exec.parallel_threads = compute_thread_count();
    }
    if (exec.parallel_stats == nullptr) {
        exec.parallel_stats = process_pipeline_stats();
    }
    if (exec.execution_profile == nullptr && execution_profile_requested()) {
        // The budget occupancy is measured against. Read from the context or
        // the environment rather than `process_worker_pool().size()`, so that
        // asking for a profile never constructs a pool a serial query would
        // otherwise never have built.
        const std::size_t budget = exec.compute_budget();
        exec.execution_profile = std::make_shared<ExecutionProfileState>(budget);
    }
    if (const std::size_t grain = morsel_rows_from_env(); grain > 0) {
        exec.parallel_grain = grain;
        // An explicit grain is an explicit request to partition at that size,
        // so it also lowers the serial threshold — otherwise the default
        // threshold would silently override the knob it was asked to honor.
        exec.parallel_min_rows = std::min(exec.parallel_min_rows, grain);
    }
    // `parallel_threads` is set at the top of this function, not here: it is
    // the COMPUTE budget and must track the core count, because the pool it
    // used to default to is now sized for decode instead.
}

namespace {

// One construction point for every row-local map operator that can live in a
// morsel pipeline. The serial planner uses the same factory: only the pipeline
// asks maps to retain zero-row morsels, because its ordered merger needs one
// output identity for every input morsel. Keeping the construction (especially
// FUP's gather set) here prevents the two planners from drifting as range-aware
// kernels replace these chunked implementations.
auto physical_filter_route(const ir::Expr& predicate,
                           const std::vector<ColumnKernelSignature>* source_signature)
    -> kernel::FilterChunkRoute {
    // A populated signature is the physical planner's proof that this is a
    // registered table scan with one of the representations the Chunk gather
    // family owns. Lazy/extern sources intentionally arrive without that
    // proof and retain the compatibility route.
    if (source_signature == nullptr || source_signature->empty() ||
        !kernel::supports_native_chunk_predicate(predicate)) {
        return kernel::FilterChunkRoute::Auto;
    }
    for (const auto& signature : *source_signature) {
        switch (signature.representation) {
            case ColumnRepresentation::FixedWidth:
            case ColumnRepresentation::PackedBool:
            case ColumnRepresentation::StringSlabs:
            case ColumnRepresentation::CategoricalCodes:
                break;
            default:
                return kernel::FilterChunkRoute::Auto;
        }
        switch (signature.null_policy) {
            case KernelNullPolicy::AllValid:
            case KernelNullPolicy::Nullable:
                break;
            default:
                return kernel::FilterChunkRoute::Auto;
        }
    }
    return kernel::FilterChunkRoute::NativePredicate;
}

auto build_filter_gather_map(const MapStep& step, OperatorPtr child, const ScalarRegistry* scalars,
                             const ExternRegistry* /*unused*/, const ExecutionContext& /*unused*/,
                             const std::vector<ColumnKernelSignature>* source_signature,
                             bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const auto& filter = ir::node_cast<ir::FilterNode>(*step.node);
    return std::make_unique<ChunkedFilterOperator>(
        std::move(child), &filter.predicate(), scalars,
        physical_filter_route(filter.predicate(), source_signature), preserve_empty_morsels);
}

auto build_metadata_map(const MapStep& step, OperatorPtr child, const ScalarRegistry* /*unused*/,
                        const ExternRegistry* /*unused*/, const ExecutionContext& /*unused*/,
                        const std::vector<ColumnKernelSignature>* /*unused*/, bool /*unused*/)
    -> std::expected<OperatorPtr, std::string> {
    if (step.node->kind() == ir::NodeKind::Project) {
        const auto& project = ir::node_cast<ir::ProjectNode>(*step.node);
        return std::make_unique<ChunkedProjectOperator>(std::move(child), &project.columns());
    }
    const auto& rename = ir::node_cast<ir::RenameNode>(*step.node);
    return std::make_unique<ChunkedRenameOperator>(std::move(child), &rename.renames());
}

auto build_row_local_update_map(const MapStep& step, OperatorPtr child,
                                const ScalarRegistry* scalars, const ExternRegistry* externs,
                                const ExecutionContext& exec,
                                const std::vector<ColumnKernelSignature>* /*unused*/,
                                bool /*unused*/) -> std::expected<OperatorPtr, std::string> {
    const auto& update = ir::node_cast<ir::UpdateNode>(*step.node);
    return std::make_unique<ChunkedUpdateOperator>(std::move(child), &update.fields(), scalars,
                                                   externs, exec);
}

auto build_filter_project_gather_map(const MapStep& step, OperatorPtr child,
                                     const ScalarRegistry* scalars,
                                     const ExternRegistry* /*unused*/,
                                     const ExecutionContext& /*unused*/,
                                     const std::vector<ColumnKernelSignature>* source_signature,
                                     bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    if (step.filter_predicate == nullptr || step.project_columns == nullptr) {
        return std::unexpected("filter-project map step missing normalized operands");
    }
    return std::make_unique<ChunkedFilterProjectOperator>(
        std::move(child), step.filter_predicate, step.project_columns, scalars,
        physical_filter_route(*step.filter_predicate, source_signature), preserve_empty_morsels);
}

auto build_filter_update_project_gather_map(
    const MapStep& step, OperatorPtr child, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec,
    const std::vector<ColumnKernelSignature>* source_signature, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    if (step.filter_predicate == nullptr || step.update_fields == nullptr ||
        step.project_columns == nullptr) {
        return std::unexpected("filter-update-project map step missing normalized operands");
    }
    robin_hood::unordered_set<std::string> update_outputs;
    robin_hood::unordered_set<std::string> needed;
    for (const auto& field : *step.update_fields) {
        update_outputs.insert(field.alias);
        collect_expr_column_refs(field.expr, needed);
    }
    for (const auto& column : *step.project_columns) {
        if (!update_outputs.contains(column.name)) {
            needed.insert(column.name);
        }
    }
    std::vector<ir::ColumnRef> gather_columns;
    gather_columns.reserve(needed.size());
    for (const auto& name : needed) {
        gather_columns.push_back(ir::ColumnRef{.name = name});
    }
    return std::make_unique<ChunkedFilterUpdateProjectOperator>(
        std::move(child), step.filter_predicate, step.update_fields, step.project_columns,
        std::move(gather_columns), scalars, externs, exec,
        physical_filter_route(*step.filter_predicate, source_signature), preserve_empty_morsels);
}

}  // namespace

auto map_kernel_factory(MapKernelCapability capability) noexcept -> MapKernelFactory {
    static constexpr std::array<MapKernelFactory, 5> factories{
        &build_filter_gather_map,
        &build_metadata_map,
        &build_row_local_update_map,
        &build_filter_project_gather_map,
        &build_filter_update_project_gather_map,
    };
    const auto index = static_cast<std::size_t>(capability);
    return index < factories.size() ? factories[index] : nullptr;
}

namespace physical_executor_detail {

/// Compose one step of a migrated physical map pipeline (Phase 1 of
/// plans/kernel-pipeline-execution-plan.md). Walks the plan top-down so the
/// per-step profile scopes nest exactly the way the per-kind switch's
/// recursion did, wraps each constructed operator with
/// `profile_operator`, and builds the source through the *public*
/// `build_operator` so every Scan/ExternCall streaming decision below stays
/// in its existing branch. The operator tree is therefore identical to the
/// pre-planner construction; the plan is the decision record, not a second
/// semantics.
auto build_physical_map_step(const physical::Plan& plan, std::size_t index,
                             const TableRegistry& registry, const ScalarRegistry* scalars,
                             const ExternRegistry* externs, const ExecutionContext& exec,
                             ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    // The morsel run, when the plan has one and a parallel executor was asked
    // for, is built as a unit: it owns the steps in
    // [parallel_begin, parallel_end) *and* the construction of its own input,
    // which it materializes before fanning out. Everything above it is composed
    // here, serially, exactly as it would be over any other child.
    //
    // `exec.can_fan_out()` is part of the condition, not an assumption: the plan's
    // mode says what the pipeline *may* do, and a serial run of the same plan
    // must compose every step here instead.
    //
    // So is the compute budget, and for the same reason. "Materializes before
    // fanning out" is a trade: the run pays for its whole input up front to buy
    // several workers over it. With a budget of one there is no second worker
    // to buy, and the payment is pure loss -- the serial composer streams the
    // same steps chunk by chunk instead. Measured at one core on PDS-H SF-1
    // against `IBEX_CORES=1`, byte-identical output on all 22 queries:
    // q14 +150%, q19 +110%, q12 +80%, q03/q10 +50%.
    //
    // It is the materialize that costs, not the split. Declining only the split
    // (`morsel_worker_count` below) was tried first and moved nothing: it took
    // `morsels` from 92 to 0 on q12/q14 and left the time where it was, because
    // every branch of the run builder had already materialized its input.
    // Deciding here, before the run is entered, is what skips it.
    //
    // The budget alone is consulted, never the pool: constructing it spawns
    // threads a declining query would never use. `can_fan_out()` answers
    // without touching it, and answers for an unconfigured context too -- on a
    // single-core box that is a budget of one just as surely as an explicit
    // `IBEX_CORES=1` is.
    if (exec.can_fan_out() && plan.mode == physical::PipelineMode::MorselParallel &&
        index == plan.parallel_begin) {
        physical::note_map_pipeline_executed();
        return pipeline_executor_detail::build_map_pipeline_parallel(
            plan, registry, scalars, externs, exec, model_out);
    }
    const auto build_child = [&] -> std::expected<OperatorPtr, std::string> {
        if (index + 1 == plan.steps.size()) {
            return build_operator(*plan.source_node, registry, scalars, externs, exec, model_out);
        }
        return build_physical_map_step(plan, index + 1, registry, scalars, externs, exec,
                                       model_out);
    };
    const MapStep& step = plan.steps[index];
    const ir::Node& node = *step.node;
    if (exec.execution_profile == nullptr) {
        auto child = build_child();
        if (!child.has_value()) {
            return child;
        }
        return step.factory(step, std::move(child.value()), scalars, externs, exec,
                            &plan.source_signature, false);
    }
    auto* entry = execution_profile_entry(exec.execution_profile, node);
    std::expected<OperatorPtr, std::string> result;
    {
        const ExecutionProfileScope scope(entry, ProfilePhase::Build);
        result = build_child();
        if (result.has_value()) {
            result = step.factory(step, std::move(result.value()), scalars, externs, exec,
                                  &plan.source_signature, false);
        }
    }
    if (!result.has_value()) {
        return result;
    }
    return profile_operator(std::move(result.value()), exec.execution_profile, node);
}

/// Build a join the plan migrated: `HashBuild` on one side, `HashProbe` on the
/// other. The family-owned join executor implements both phases and exposes a
/// narrow construction boundary here. The kernel-pipeline plan's Phase 4 item
/// 1.
///
/// The three branches are the ones that used to sit in `build_operator_impl`'s
/// per-kind switch. Construction lives with the plan, the decisions are the
/// same ones `plan_join` relayed, and execution is now owned by the join
/// family. HashBuild and HashProbe are explicit structural nodes connected by
/// a typed runtime-orientation edge; an eligible probe can also become a step
/// inside a map pipeline.

/// Both of a streaming join's fan-out phases, resolved for this query. The
/// capability halves are `physical::join_hash_build_parallelism` /
/// `join_probe_parallelism` (floor + worker ceiling); the resolved half needs
/// the `ExecutionContext` and the pool size, both in hand at build time. One
/// definition, shared by every join construction site, the way `distinct_table`
/// and `build_physical_distinct` share the dedup policy.
auto resolve_join_parallelism(physical::JoinParallelism par, const ExecutionContext& exec)
    -> physical::JoinParallelism {
    // The pool is sized for decode and spawns its threads on first touch, so a
    // serial query must not construct it just to learn it is serial.
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(par.build, exec, pool_size);
    physical::resolve_breaker_parallelism(par.probe, exec, pool_size);
    return par;
}

/// Compatibility construction sites that do not own a physical Plan still use
/// the same policy factories. Migrated joins take the overload below instead.
auto resolved_join_parallelism(const ExecutionContext& exec) -> physical::JoinParallelism {
    return resolve_join_parallelism({.build = physical::join_hash_build_parallelism(),
                                     .probe = physical::join_probe_parallelism()},
                                    exec);
}

/// Resolve the policies carried by the explicit HashBuild and HashProbe nodes.
/// Taking copies is intentional: resolution is execution-context state and the
/// data-only physical plan remains reusable and inspectable.
auto resolved_join_parallelism(const physical::StreamingJoinNodes& nodes,
                               const ExecutionContext& exec) -> physical::JoinParallelism {
    return resolve_join_parallelism(
        {.build = nodes.build.parallelism, .probe = nodes.probe.parallelism}, exec);
}

/// As `resolved_join_parallelism`, for the hash aggregate. The capability half
/// (floors, worker ceiling, strategy) comes from the four aggregate node
/// policy factories; the resolved half needs the
/// `ExecutionContext` and pool size, both in hand here at build time. One
/// definition, shared by every aggregate construction site.
auto resolved_aggregate_parallelism(const physical::HashAggregateNodes& nodes,
                                    const ExecutionContext& exec)
    -> std::expected<physical::AggregateParallelism, std::string> {
    physical::AggregateParallelism par{
        .discovery = nodes.discovery.parallelism,
        .accumulation = nodes.accumulation.parallelism,
        .final_ordering = nodes.final_ordering.parallelism,
        .emission = nodes.emission.parallelism,
    };
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(par.discovery, exec, pool_size);
    physical::resolve_breaker_parallelism(par.accumulation, exec, pool_size);
    physical::resolve_breaker_parallelism(par.final_ordering, exec, pool_size);
    physical::resolve_breaker_parallelism(par.emission, exec, pool_size);
    return par;
}

auto build_physical_join(const physical::Plan& plan, const ir::Node& node,
                         const TableRegistry& registry, const ScalarRegistry* scalars,
                         const ExternRegistry* externs, const ExecutionContext& exec,
                         ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    const auto& join = ir::node_cast<ir::JoinNode>(node);
    const physical::JoinPlan& jp = plan.join;
    if (jp.branch == physical::JoinBranch::SemiAnti) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        // Multiple producers: tried the same overlap the inner-join site
        // once had here too, twice. First attempt (unbudgeted): q04
        // regressed +19%. Second attempt, under a since-removed
        // helper-thread budget: q04 and q18 both STILL regressed, and in
        // both cases the overlap is entered exactly once per query (verified with a temporary
        // entry-count trace) -- there is no recursive pile-up here for a
        // budget to bound, so the budget was never going to help. The
        // cost is inherent to overlapping this specific pair of sides,
        // not to how many raw threads accumulate. Reverted a second
        // time; see plans/parallelism-overview.md's "generalize
        // multiple producers" section before trying again here.
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            std::make_unique<ChunkedSemiAntiJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), join.kind(), &join.keys(),
                &exec),
            stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
    }
    if (!plan.streaming_join.has_value()) {
        return std::unexpected("physical join: streaming plan has no HashBuild/HashProbe nodes");
    }
    const physical::StreamingJoinNodes& nodes = *plan.streaming_join;
    if (auto edge_error = physical::validate_streaming_join_edge(nodes)) {
        return std::unexpected(std::move(*edge_error));
    }
    // `nulls equal` goes to the materialized join, which implements the
    // policy. These streaming operators hash and probe on their own and
    // would each need the same null tagging; sending the opt-in case to the
    // one implementation that has it keeps a single definition of the
    // semantics -- and leaves this hot path bit-for-bit unchanged for every
    // join that does not ask for it.
    if (jp.branch == physical::JoinBranch::SingleKeyInner) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        // A deferred probe scan must not be interpreted here — the join
        // publishes build-side bounds into its filter slot first, then
        // interprets the right subtree itself (resolve_deferred_probe).
        const auto probe = deferred_probe_scan_of(*join.children()[1], exec);

        // Multiple producers (plans/parallelism-overview.md): the left
        // build and the right materialize were overlapped on a raw
        // std::thread here for a time (q10 ~-3% in-suite), but every
        // widening of the idea measured worse and was reverted, and the
        // site was removed ahead of the kernel-pipeline restructure —
        // branch concurrency needs a cost-aware gate, not a thread-count
        // one. Left builds first, then the right materializes.
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        if (probe.scan != nullptr) {
            auto built = make_scheduled_deferred_inner_join_operator(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns);
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return pipeline_executor_detail::make_pipelined_stage_if(
                std::move(*built), stage_probe, exec,
                execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = make_scheduled_chunked_inner_join_operator(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns);
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            std::move(*built), stage_probe, exec,
            execution_profile_entry(exec.execution_profile, node));
    }
    // Streaming two-Int64-key inner join (plans/parallelism-overview.md's
    // "stream multi-key joins" item): same shape as the single-key
    // streamable path just above, minus the multiple-producers-overlap
    // machinery -- this builds the hash index on the smaller of the two
    // sides and streams/scans the other through
    // `ChunkedInnerJoinOperator`'s pair-key path, replacing
    // `join_table_impl`'s whole-table hash join for exactly this shape.
    // A deferred-probe right side (e.g. TPC-H q09's lineitem) is honored
    // exactly like the single-key branch above; see
    // `ChunkedInnerJoinOperator::resolve_deferred_probe_pair` for the
    // one-component filter this POC pushes into the scan.
    if (jp.branch == physical::JoinBranch::PairIntInner) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        const auto probe = deferred_probe_scan_of(*join.children()[1], exec);
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        if (probe.scan != nullptr) {
            auto built = make_scheduled_deferred_inner_join_operator(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns);
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return pipeline_executor_detail::make_pipelined_stage_if(
                std::move(*built), stage_probe, exec,
                execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = make_scheduled_chunked_inner_join_operator(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns);
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            std::move(*built), stage_probe, exec,
            execution_profile_entry(exec.execution_profile, node));
    }

    return std::unexpected("physical join: plan named no streaming branch");
}

/// Build an aggregate the plan migrated: the streaming operator, or the
/// Join+Aggregate fusion. Phase 4 item 2.
///
/// Moved from `build_operator_impl`'s per-kind switch rather than rewritten.
/// `MaterializeAll` never arrives here -- it is not migrated, still falls back,
/// and is still counted in the backlog, which is what keeps that number honest.
auto build_physical_aggregate(const physical::Plan& plan, const ir::Node& node,
                              const TableRegistry& registry, const ScalarRegistry* scalars,
                              const ExternRegistry* externs, const ExecutionContext& exec,
                              ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    const auto& agg = ir::node_cast<ir::AggregateNode>(node);
    if (agg.children().empty()) {
        return std::unexpected("aggregate node missing child");
    }
    // The plan decides; this reads its decision. `plan_aggregate` relays the
    // same two predicates this code used to call, so the routing is
    // identical by construction.
    const physical::AggregatePlan& ap = plan.aggregate;
    if (ap.strategy == physical::AggregateStrategy::FusedLeftJoinCount) {
        // Two logical nodes, one physical step: the join named here is
        // consumed, never handed to `build_operator`, so it is neither
        // planned nor counted separately. The skip is a consequence of who
        // calls whom -- the same way a fused `MapStep` partner is skipped
        // by the plan's walk simply never descending to it.
        const auto& join = ir::node_cast<ir::JoinNode>(*ap.fused_join);
        const std::string& counted_column = ap.counted_column;
        auto left =
            materialize_row_local(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left.has_value()) {
            return std::unexpected(std::move(left.error()));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        if (auto fused = left_join_count_table(join, agg, *left, *right, counted_column, &exec);
            fused.has_value()) {
            return make_table_source(std::move(*fused));
        }
        auto joined = join_table_impl(*left, *right, join.kind(), join.keys(), nullptr, scalars,
                                      compute_mask, join.suffix(), join.pending_order(),
                                      join.null_match(), join.expect(), join.take(), &exec);
        if (!joined.has_value()) {
            return std::unexpected(std::move(joined.error()));
        }
        auto result = aggregate_table(*joined, agg.group_by(), agg.aggregations(), &exec);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        return make_table_source(std::move(*result));
    }
    if (ap.strategy == physical::AggregateStrategy::StreamingSorted) {
        if (!plan.hash_aggregate.has_value()) {
            return std::unexpected(
                "physical aggregate: adaptive strategy has no hash-fallback phase chain");
        }
        if (auto edge_error = physical::validate_hash_aggregate_edges(*plan.hash_aggregate)) {
            return std::unexpected(std::move(*edge_error));
        }
        if (agg.children().empty() ||
            plan.hash_aggregate->discovery.source != agg.children().front().get()) {
            return std::unexpected(
                "physical aggregate: Discovery input does not match the aggregate child");
        }
        auto parallelism = resolved_aggregate_parallelism(*plan.hash_aggregate, exec);
        if (!parallelism.has_value()) {
            return std::unexpected(std::move(parallelism.error()));
        }
        auto child_op =
            build_operator(*agg.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        // The sorted operator streams group-at-a-time when the child's
        // chunks arrive sorted on the group keys, and otherwise replays the
        // first chunk into the hash aggregate phase operator — so it is safe
        // to route the whole streamable subset here.
        // Aggregates are often the terminal breaker and hash aggregation
        // emits only after consuming all input. Scheduling one in its own
        // stage in that shape buys no overlap and only creates a thread.
        // A join below it is staged instead: its probe stream can fill the
        // aggregate while it keeps pulling the next probe chunk.
        // Resolve the hash fallback's four structural-node policies here, where
        // the ExecutionContext is in hand, and hand them down. The operator
        // retains only data-dependent gates such as actual row counts and
        // strategy-specific usefulness thresholds.
        return make_chunked_aggregate_operator(std::move(child_op.value()), &agg.group_by(),
                                               &agg.aggregations(), exec,
                                               std::move(*parallelism), ap.columns);
    }

    return std::unexpected("physical aggregate: plan named no executable strategy");
}

/// Build an ordering breaker. Phase 4 item 3.
///
/// No strategy vocabulary here on purpose: `ChunkedOrderOperator` handles every
/// Order there is, so unlike `plan_join` and `plan_aggregate` there is no
/// decision to describe. Adding a one-valued enum would be the ceremony the
/// plan document's risk table warns about -- what this buys is that the plan
/// owns construction, not that it makes a choice.
auto build_physical_order(const ir::Node& node, const TableRegistry& registry,
                          const ScalarRegistry* scalars, const ExternRegistry* externs,
                          const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const auto& order = ir::node_cast<ir::OrderNode>(node);
    if (order.children().empty()) {
        return std::unexpected("order node missing child");
    }
    auto child_op =
        build_operator(*order.children().front(), registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    return std::make_unique<ChunkedOrderOperator>(std::move(child_op.value()), &order.keys(), exec);
}

/// Build a row-limit breaker. Phase 4 item 3, same shape as the ordering one:
/// `ChunkedHeadOperator` handles every Head, so there is no decision to
/// describe and no `Plan` to consult.
auto build_physical_head(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const auto& head = ir::node_cast<ir::HeadNode>(node);
    if (head.children().empty()) {
        return std::unexpected("head node missing child");
    }
    // Evaluated before the child is built, as it always was: a bad count is the
    // caller's error and should not be preceded by constructing a subtree.
    auto count = evaluate_row_count_expr_impl(head.count_expr(), scalars, externs);
    if (!count.has_value()) {
        return std::unexpected(count.error());
    }
    // Head(Order(x)) is rewritten by canonicalize R16 into TopK(x);
    // Head(Filter(x)) with no group_by is rewritten by R7 into FilterHead(x);
    // Head past Project/Rename is handled by R4.
    auto child_op =
        build_operator(*head.children().front(), registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    return std::make_unique<ChunkedHeadOperator>(std::move(child_op.value()), *count,
                                                 &head.group_by());
}

/// Build a `Tail` breaker. Moved verbatim from the per-kind switch: `Tail`
/// needs every row before it can keep the last N, so it materializes the child
/// and calls `tail_table` rather than streaming. Same single-operator shape as
/// Head -- no `Plan` to consult.
auto build_physical_tail(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const auto& tail = ir::node_cast<ir::TailNode>(node);
    if (tail.children().empty()) {
        return std::unexpected("tail node missing child");
    }
    auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
    if (!count.has_value()) {
        return std::unexpected(count.error());
    }
    return build_unary_materializing_operator(
        *tail.children().front(), registry, scalars, externs, exec, model_out,
        [&](const Table& input) { return tail_table(input, *count, tail.group_by()); });
}

/// Build a `TopK` breaker (fused `Head(Order(x))` / `Tail(Order(x))`, canonicalize
/// R16). `ChunkedOrderedLimitOperator` is a serial bounded-heap select
/// (O(n log k), one pass) -- deliberately not a fan-out point, see
/// src/runtime/PARALLELISM.md. Moved verbatim from the per-kind switch.
auto build_physical_topk(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const auto& topk = ir::node_cast<ir::TopKNode>(node);
    if (topk.children().empty()) {
        return std::unexpected("topk node missing child");
    }
    auto child_op =
        build_operator(*topk.children().front(), registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    const auto keep = (topk.keep_mode() == ir::TopKNode::KeepMode::First)
                          ? ChunkedOrderedLimitOperator::KeepMode::First
                          : ChunkedOrderedLimitOperator::KeepMode::Last;
    return std::make_unique<ChunkedOrderedLimitOperator>(std::move(child_op.value()), &topk.keys(),
                                                         topk.count(), &topk.group_by(), keep);
}

/// Build a `FilterHead` / `FilterTail` breaker (fused `Head(Filter(x))` /
/// `Tail(Filter(x))`, canonicalize R7/R8). One streaming operator each. Moved
/// verbatim from the per-kind switch.
auto build_physical_filter_head_tail(const ir::Node& node, const TableRegistry& registry,
                                     const ScalarRegistry* scalars, const ExternRegistry* externs,
                                     const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    if (node.children().empty() || node.children().front() == nullptr) {
        return std::unexpected("filter_head/filter_tail node missing child");
    }
    auto child_op =
        build_operator(*node.children().front(), registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    if (node.kind() == ir::NodeKind::FilterHead) {
        const auto& fh = ir::node_cast<ir::FilterHeadNode>(node);
        return std::make_unique<ChunkedFilterHeadOperator>(std::move(child_op.value()),
                                                           &fh.predicate(), fh.count(), scalars);
    }
    const auto& ft = ir::node_cast<ir::FilterTailNode>(node);
    return std::make_unique<ChunkedFilterTailOperator>(std::move(child_op.value()), &ft.predicate(),
                                                       ft.count(), scalars);
}

/// Build a distinct breaker. The plan describes the `dedup` fan-out phase
/// (src/runtime/PARALLELISM.md); this resolves its worker cap here, where the
/// `ExecutionContext` is in hand, and hands it to the operator, which reads it
/// rather than deciding for itself.
auto build_physical_distinct(const physical::Plan& plan, const ir::Node& node,
                             const TableRegistry& registry, const ScalarRegistry* scalars,
                             const ExternRegistry* externs, const ExecutionContext& exec,
                             ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    if (node.children().empty()) {
        return std::unexpected("distinct node missing child");
    }
    auto child_op =
        build_operator(*node.children().front(), registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    // Distinct always carries exactly one phase (`plan_physical`); guard anyway
    // so a future planner change cannot silently hand the operator an
    // unresolved plan (worker_cap 0), which would pin it serial.
    physical::BreakerParallelism dedup_plan = plan.breaker_phases.empty()
                                                  ? physical::distinct_dedup_parallelism({})
                                                  : plan.breaker_phases.front().parallelism;
    // The pool is sized for decode and its threads spawn on first touch, so a
    // serial query must not construct it just to learn it is serial.
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(dedup_plan, exec, pool_size);
    return std::make_unique<ChunkedDistinctOperator>(std::move(child_op.value()), dedup_plan);
}

}  // namespace physical_executor_detail

namespace {

auto build_operator_impl(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    // Physical-plan seam (plans/kernel-pipeline-execution-plan.md). One plan
    // per node, and it describes the whole map chain or migrated breaker. The
    // executor below is also callable with an already-built plan, which makes
    // plan-edge mutation tests exercise the same consumer production uses.
    const physical::Plan plan = physical::plan_physical(node, registry, externs);
    if (plan.migrated) {
        return build_migrated_physical_operator(plan, node, registry, scalars, externs, exec,
                                                model_out);
    }
    // Counted in every mode. It used to fire only when the query could not fan
    // out, so at two cores or more the backlog read as empty -- a migration
    // counter that reports nothing on the configuration everything is measured
    // on. The seam visits each node once, so there is nothing to double count;
    // the test asserts the two modes agree.
    physical::note_materialized_call(plan.reason, node.kind());

    // A deferred lazy scan can be streamed instead of materialized. Everything
    // else — a registered table, a source with no unit decomposition — falls
    // through to the whole-table path at the bottom of this function, so
    // declining here costs nothing but the whole-table behaviour.
    if (node.kind() == ir::NodeKind::Scan && exec.stream_scans) {
        const auto& scan = ir::node_cast<ir::ScanNode>(node);
        if (!registry.contains(scan.source_name())) {
            // A null filter slot is what distinguishes a scan registered for
            // streaming from a deferred *probe* scan. A probe's decode belongs
            // to the join above it, which publishes build-side key bounds into
            // that slot first; streaming it here would decode it before those
            // bounds exist. See `deferred_probe_scan_of`, which draws the same
            // line from the other side.
            if (const auto* deferred = exec.deferred_scan(scan.source_name());
                deferred != nullptr && deferred->filter == nullptr) {
                auto units = deferred_scan_units(*deferred);
                if (units.size() > 1) {
                    if (exec.can_fan_out() &&
                        pipeline_executor_detail::scan_pipeline_worker_count(units.size()) > 0) {
                        return pipeline_executor_detail::build_pipelined_scan(
                            {}, false, *deferred, std::move(units), scalars, externs, exec);
                    }
                    return pipeline_executor_detail::make_deferred_scan_source(
                        *deferred, std::move(units), exec);
                }
            }
        }
    }

    if (node.kind() == ir::NodeKind::Filter) {
        const auto& filter = ir::node_cast<ir::FilterNode>(node);
        if (filter.children().empty()) {
            return std::unexpected("filter node missing child");
        }
        auto child_op =
            build_operator(*filter.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    if (node.kind() == ir::NodeKind::Project) {
        const auto& project = ir::node_cast<ir::ProjectNode>(node);
        if (project.children().empty()) {
            return std::unexpected("project node missing child");
        }
        auto child_op = build_operator(*project.children().front(), registry, scalars, externs,
                                       exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    // No FilterHead / FilterTail branch: fused Head(Filter(x)) / Tail(Filter(x))
    // (canonicalize R7/R8) is a migrated plan built by
    // `build_physical_filter_head_tail` at the seam above.

    if (node.kind() == ir::NodeKind::Rename) {
        const auto& rename = ir::node_cast<ir::RenameNode>(node);
        if (rename.children().empty()) {
            return std::unexpected("rename node missing child");
        }
        auto child_op =
            build_operator(*rename.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    if (node.kind() == ir::NodeKind::ExternCall && externs != nullptr) {
        const auto& ec = ir::node_cast<ir::ExternCallNode>(node);
        const auto* fn = externs->find(ec.callee());
        if (fn != nullptr && fn->chunked_table_func) {
            ExternArgs args;
            args.reserve(ec.args().size());
            bool args_ok = true;
            for (const auto& arg : ec.args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                auto scalar = val.has_value() ? scalar_from_expr(val.value()) : std::nullopt;
                if (!scalar.has_value()) {
                    args_ok = false;
                    break;
                }
                args.push_back(std::move(*scalar));
            }
            if (args_ok) {
                auto op = fn->chunked_table_func(args);
                if (op.has_value()) {
                    return std::move(op.value());
                }
            }
        }
    }

    // No Distinct branch: every Distinct is a migrated plan, built by
    // `build_physical_distinct` at the seam above.

    // No Order branch: every Order is a migrated plan, built by
    // `build_physical_order` at the seam above. Unlike the join and the
    // aggregate there is no eligibility gate to relay -- one operator handles
    // every Order -- so the plan has nothing to decide and says only that this
    // node is an ordering breaker.

    // No Aggregate branch: `MaterializeAll` (Median, Quantile, Ewma) falls
    // through to the whole-table path below exactly as it always did, and every
    // other aggregate is a migrated plan built by `build_physical_aggregate` at
    // the seam above.

    // No TopK / Head / Tail branch: each is a migrated plan built at the seam
    // above (`build_physical_topk` / `build_physical_head` / `build_physical_tail`).
    // TopK stays a serial bounded-heap select (O(n log k)); Tail materializes
    // and calls `tail_table`; the plan just records that they are breakers.

    // Every other node kind is a materialized-call fallback: not migrated by
    // `plan_physical`, counted just above, and executed by one whole-table
    // `interpret_node` call rather than a per-kind branch that re-enters
    // `build_operator` for each child. `interpret_node` recurses through
    // itself, so a fallback subtree is planned once, here, not per node.
    // Construct / Stream / Program (preamble), Model (`model_out`), and the
    // reshape / stat / window / update / matmul / materializing-join kinds all
    // resolve there. The input side of these breakers is no longer built
    // through the fused physical path; `physical_fallbacks_for(kind)` buckets
    // the backlog so a kind can later be lifted to a migrated breaker-over-
    // pipeline the way Join / Aggregate / Order were. Scan is handled as a
    // source by the caller.
    auto table = interpret_node(node, registry, scalars, externs, exec, model_out);
    if (!table.has_value()) {
        return std::unexpected(std::move(table.error()));
    }
    return make_table_source(std::move(table.value()));
}

}  // namespace

auto build_operator(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    if (exec.execution_profile == nullptr) {
        return build_operator_impl(node, registry, scalars, externs, exec, model_out);
    }
    auto* entry = execution_profile_entry(exec.execution_profile, node);
    std::expected<OperatorPtr, std::string> result;
    {
        const ExecutionProfileScope scope(entry, ProfilePhase::Build);
        result = build_operator_impl(node, registry, scalars, externs, exec, model_out);
    }
    if (!result.has_value()) {
        return result;
    }
    return profile_operator(std::move(result.value()), exec.execution_profile, node);
}

}  // namespace ibex::runtime
