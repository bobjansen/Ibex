// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// chunked.cpp — streaming (chunked) operator pipeline: per-chunk operators,
// rank evaluation, extern-call execution, and build_operator plan construction.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

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
#include "join_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "model_internal.hpp"
#include "packed_key_encoder_internal.hpp"
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
using ir::is_row_local_update_expr;

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

/// The base Scan under `node`, peeled through a chain of Project/Rename/Update
/// wrappers — null when `node` is not (a simple wrapper around) one scan.
/// Deliberately not past Filter: `deferred_probe_scan_of` reuses this peel,
/// and the driver only ever registers a probe scan for exactly the
/// Project/Rename/Update shape it proved eligible, so widening the peel here
/// would silently widen what counts as a deferred probe too.
auto base_scan_of(const ir::Node& node) -> const ir::ScanNode* {
    const ir::Node* cur = &node;
    while (cur->kind() == ir::NodeKind::Project || cur->kind() == ir::NodeKind::Rename ||
           cur->kind() == ir::NodeKind::Update) {
        if (cur->children().size() != 1 || cur->children().front() == nullptr) {
            return nullptr;
        }
        cur = cur->children().front().get();
    }
    if (cur->kind() != ir::NodeKind::Scan) {
        return nullptr;
    }
    return &ir::node_cast<ir::ScanNode>(*cur);
}

/// If `right` is a chain of Project/Rename nodes over a Scan whose name the
/// driver registered as a deferred probe scan, return its registration. The
/// driver only registers scans it proved eligible (ir::deferrable_probe_scans:
/// occurs once, feeds exactly this shape), so a hit here IS the eligible
/// position.
struct DeferredProbeScan {
    const DeferredScan* scan = nullptr;
    const std::string* name = nullptr;  ///< scan (instance) name in the plan
};

auto deferred_probe_scan_of(const ir::Node& right, const ExecutionContext& exec)
    -> DeferredProbeScan {
    if (exec.deferred_scans == nullptr) {
        return {};
    }
    const auto* scan_node = base_scan_of(right);
    if (scan_node == nullptr) {
        return {};
    }
    const auto& name = scan_node->source_name();
    const auto* scan = exec.deferred_scan(name);
    // A probe scan is one with a filter slot to publish build-side bounds
    // into. The registry also holds streaming registrations (Phase 1), which
    // have no slot and are not this join's to decode.
    if (scan == nullptr || scan->filter == nullptr) {
        return {};
    }
    // Recover the stored key iterator to expose the registry's own name string.
    const auto it = exec.deferred_scans->find(name);
    return DeferredProbeScan{.scan = scan, .name = &it->first};
}

/// The end of a hash chain. At namespace scope because the index and the
/// operator that probes it are no longer the same type, so the sentinel belongs
/// to neither alone; `ChunkedInnerJoinOperator::kNil` aliases it.
inline constexpr std::size_t kJoinNil = std::numeric_limits<std::size_t>::max();

/// A hash-index head table split into partitions by key hash.
///
/// Every key belongs to exactly one partition, so P workers can fill P
/// partitions with no shared writes, no locks, and -- unlike per-worker maps --
/// no merge afterwards. That is what makes a hash build morsel-parallel, and
/// it is the reason this type exists: `build_join_hash_index` is one serial
/// loop, and on TPC-H q21 it spends 40 ms hashing 1.29M rows inside a 75 ms
/// query (measured 2026-08-25, see plans/kernel-pipeline-execution-plan.md,
/// "Where join time actually goes").
///
/// `partition_count == 1` is exactly the single-map behaviour this replaced,
/// bit for bit: one partition, mask 0, every key landing in `parts[0]`.
/// Partitioning the TYPE and filling it in parallel are deliberately separate
/// steps -- the first cannot change a result, so anything the second breaks is
/// unambiguously the second's fault.
template <class Key, class Hash = robin_hood::hash<Key>, class Eq = std::equal_to<Key>>
struct PartitionedHeads {
    using Map = robin_hood::unordered_flat_map<Key, std::size_t, Hash, Eq>;
    /// Always a power of two, so `part_of` is a mask rather than a modulo.
    std::vector<Map> parts{1};
    std::size_t mask = 0;

    /// Size to `count` partitions (rounded down to a power of two, at least 1).
    void partition(std::size_t count) {
        std::size_t p = 1;
        while (p * 2 <= count) {
            p *= 2;
        }
        parts.assign(p, Map{});
        mask = p - 1;
    }

    [[nodiscard]] auto partition_count() const noexcept -> std::size_t { return parts.size(); }

    [[nodiscard]] auto part_of(const Key& key) const noexcept -> std::size_t {
        return mask == 0 ? 0 : (Hash{}(key)&mask);
    }

    /// Reserve for `n` build rows. Split across partitions, since a key can
    /// only land in one of them.
    void reserve(std::size_t n) {
        const std::size_t per = (n / parts.size()) + 1;
        for (auto& part : parts) {
            part.reserve(per);
        }
    }

    /// Insert `row` as the head for `key` if absent. Returns a pointer to the
    /// stored head (never null) and whether it was newly inserted, so a caller
    /// that loses the race to an earlier row can chain onto what is there.
    auto try_emplace(const Key& key, std::size_t row) -> std::pair<std::size_t*, bool> {
        auto [it, inserted] = parts[part_of(key)].try_emplace(key, row);
        return {&it->second, inserted};
    }

    /// The head row for `key`, or `kJoinNil` when the build side has none.
    [[nodiscard]] auto find_head(const Key& key) const -> std::size_t {
        const auto& part = parts[part_of(key)];
        const auto it = part.find(key);
        return it == part.end() ? kJoinNil : it->second;
    }
};

/// Everything a hash build produces and a hash probe consumes: the chained
/// index over one side's key column, plus what the probe needs to interpret it.
///
/// This is the barrier between Phase 4's `HashBuild` and `HashProbe`, stated as
/// a type. A probe holds it as `shared_ptr<const JoinHashIndex>`: it cannot
/// write to what a build produced, and one build can feed several probes.
///
/// What is deliberately NOT here: the categorical code -> head table. It is
/// derived from the PROBE chunk's dictionary and rebuilt per chunk, so it is
/// the probing operator's state (`probe_code_heads_`). Holding it here is what
/// made the build state mutable during probing.
struct JoinHashIndex {
    ExprType key_kind = ExprType::Int;
    /// False once any key repeats: the probe can skip chain walking when a
    /// build side is unique.
    bool unique = true;
    /// Row -> next row with the same key, `kJoinNil` at the end of a chain.
    std::vector<std::size_t> chain_next;
    /// Head row per key, one map per key representation.
    PartitionedHeads<std::int64_t> i64_heads;
    PartitionedHeads<double> f64_heads;
    PartitionedHeads<bool> bool_heads;
    PartitionedHeads<Date> date_heads;
    PartitionedHeads<Timestamp> ts_heads;
    PartitionedHeads<std::string_view, StringViewHash, StringViewEq> string_heads;

    /// Two-fixed-width-int-key path: both key values pack into one struct,
    /// injective with no knowledge of their domains -- same trick as the
    /// aggregate's own `PairIntKey`.
    struct PairKey {
        std::uint64_t a = 0;
        std::uint64_t b = 0;
        [[nodiscard]] friend auto operator==(const PairKey&, const PairKey&) -> bool = default;
    };
    struct PairKeyHash {
        auto operator()(const PairKey& key) const noexcept -> std::size_t {
            std::uint64_t h = key.a * 0x9e3779b97f4a7c15ULL;
            h ^= key.b + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            return static_cast<std::size_t>(h);
        }
    };
    PartitionedHeads<PairKey, PairKeyHash> pair_heads;

    /// Borrowed from the build table; null when the key column has no nulls. A
    /// null key matches nothing, so null build rows are never indexed and null
    /// probe rows are never looked up.
    const ValidityBitmap* validity = nullptr;
};

auto detect_join_key_kind(const ColumnValue& col, ExprType& out) -> std::optional<std::string> {
    if (std::holds_alternative<Column<std::int64_t>>(col)) {
        out = ExprType::Int;
    } else if (std::holds_alternative<Column<double>>(col)) {
        out = ExprType::Double;
    } else if (std::holds_alternative<Column<bool>>(col)) {
        out = ExprType::Bool;
    } else if (std::holds_alternative<Column<Date>>(col)) {
        out = ExprType::Date;
    } else if (std::holds_alternative<Column<Timestamp>>(col)) {
        out = ExprType::Timestamp;
    } else if (std::holds_alternative<Column<Categorical>>(col) ||
               std::holds_alternative<Column<std::string>>(col)) {
        out = ExprType::String;
    } else {
        return "ChunkedInnerJoinOperator: unsupported key type";
    }
    return std::nullopt;
}

/// The hash build. Chains each build row to the next row carrying the same key,
/// iterating in reverse so a chain walks forward during the probe -- which is
/// what makes the streamed output match the nested-loop inner join's ordering.
///
/// A null key matches nothing, not even another null (SQL / Polars). So a
/// null-keyed build row is never indexed, and a null-keyed probe row is never
/// looked up. Both halves are needed: a null cell holds the type's zero value,
/// so a null probe key would otherwise find a genuine `0`.
/// Fill one `PartitionedHeads` from `n` build rows, serially when it has one
/// partition and over the worker pool when it has more.
///
/// The two paths must produce a bit-identical index, and the reason they do is
/// worth stating rather than trusting. The serial build walks rows from `n-1`
/// down to 0, so a key's head is its LOWEST row and its chain ascends. The
/// parallel build scatters rows into partitions keeping them ascending within
/// each, then walks each partition's slice in reverse -- and because every row
/// carrying a given key lands in that key's one partition, reverse order
/// within the partition is reverse order within the key. Same head, same
/// chain, in any partition count. Output row order is a join contract
/// (SPEC.md 5.6 leaves it open, but `emit_swapped` and the chained probe both
/// depend on the chain's direction), so this is not a detail.
///
/// Writes are disjoint by construction: `chain_next[row]` is written only by
/// the worker owning that row's partition, and each partition's map is its
/// own. No locks, and no merge afterwards -- which is the whole reason the
/// head table is partitioned rather than built per-worker and combined.
template <class Heads, class KeyAt, class IsNull>
void fill_partitioned_heads(Heads& heads, std::size_t n, const KeyAt& key_at, const IsNull& is_null,
                            std::vector<std::size_t>& chain_next, bool& unique) {
    const auto insert = [&](std::size_t row, bool& dup) {
        auto [head, inserted] = heads.try_emplace(key_at(row), row);
        if (!inserted) {
            chain_next[row] = *head;
            *head = row;
            dup = true;
        }
    };
    if (heads.partition_count() == 1) {
        heads.reserve(n);
        bool dup = false;
        for (std::size_t r = n; r-- > 0;) {
            if (is_null(r)) {
                continue;
            }
            insert(r, dup);
        }
        unique = unique && !dup;
        return;
    }

    const std::size_t parts = heads.partition_count();
    auto& pool = process_worker_pool();
    const std::size_t ranges = std::min(pool.size(), parts);
    const std::size_t grain = (n + ranges - 1) / ranges;
    heads.reserve(n);

    // Pass 1: which partition each row belongs to, and how many rows each
    // (range, partition) pair contributes. Null-keyed rows are never indexed,
    // so they are given no partition and never counted.
    std::vector<std::uint8_t> part_of_row(n, 0);
    std::vector<char> indexed(n, 0);
    std::vector<std::size_t> counts(ranges * parts, 0);
    {
        auto batch = pool.submit(ranges, [&](std::size_t r) {
            const std::size_t begin = r * grain;
            const std::size_t end = std::min(n, begin + grain);
            std::size_t* row_counts = counts.data() + (r * parts);
            for (std::size_t row = begin; row < end; ++row) {
                if (is_null(row)) {
                    continue;
                }
                const std::size_t part = heads.part_of(key_at(row));
                part_of_row[row] = static_cast<std::uint8_t>(part);
                indexed[row] = 1;
                ++row_counts[part];
            }
        });
        batch.wait();
    }

    std::vector<std::size_t> offsets(ranges * parts, 0);
    std::vector<std::size_t> part_begin(parts + 1, 0);
    {
        std::size_t running = 0;
        for (std::size_t p = 0; p < parts; ++p) {
            part_begin[p] = running;
            for (std::size_t r = 0; r < ranges; ++r) {
                offsets[(r * parts) + p] = running;
                running += counts[(r * parts) + p];
            }
        }
        part_begin[parts] = running;
    }

    // Pass 2: scatter. Ranges are laid out in ascending order within each
    // partition and each range walks ascending, so a partition's slice is
    // ascending in row index -- which is what pass 3 reverses.
    std::vector<std::size_t> scatter_rows(part_begin[parts]);
    {
        auto batch = pool.submit(ranges, [&](std::size_t r) {
            const std::size_t begin = r * grain;
            const std::size_t end = std::min(n, begin + grain);
            std::size_t* cursor = offsets.data() + (r * parts);
            for (std::size_t row = begin; row < end; ++row) {
                if (indexed[row] == 0) {
                    continue;
                }
                scatter_rows[cursor[part_of_row[row]]++] = row;
            }
        });
        batch.wait();
    }

    // Pass 3: one worker per partition, claimed dynamically.
    std::vector<char> dup(parts, 0);
    {
        std::atomic<std::size_t> cursor{0};
        auto batch = pool.submit(std::min(pool.size(), parts), [&](std::size_t) {
            for (std::size_t p = cursor.fetch_add(1, std::memory_order_relaxed); p < parts;
                 p = cursor.fetch_add(1, std::memory_order_relaxed)) {
                bool part_dup = false;
                for (std::size_t i = part_begin[p + 1]; i-- > part_begin[p];) {
                    insert(scatter_rows[i], part_dup);
                }
                dup[p] = part_dup ? 1 : 0;
            }
        });
        batch.wait();
    }
    for (const char d : dup) {
        unique = unique && d == 0;
    }
}

auto build_join_hash_index(const Table& build_side, const std::string& key_name, ExprType key_kind,
                           std::size_t partitions) -> std::expected<JoinHashIndex, std::string> {
    const ColumnValue* key = build_side.find(key_name);
    if (key == nullptr) {
        return std::unexpected("join key not found in build side: " + key_name);
    }
    JoinHashIndex index;
    index.key_kind = key_kind;
    const auto* build_entry = build_side.find_entry(key_name);
    index.validity = build_entry != nullptr && build_entry->validity.has_value()
                         ? &*build_entry->validity
                         : nullptr;
    const std::size_t n = build_side.rows();
    index.chain_next.assign(n, kJoinNil);

    const auto is_null = [&index](std::size_t row) noexcept {
        return index.validity != nullptr && !(*index.validity)[row];
    };
    const auto build_scalar = [&]<typename ColT, typename Heads>(const ColT& col, Heads& heads) {
        const auto* data = col.data();
        heads.partition(partitions);
        fill_partitioned_heads(
            heads, col.size(), [data](std::size_t r) { return data[r]; }, is_null, index.chain_next,
            index.unique);
    };

    if (key_kind == ExprType::Int) {
        const auto* col = std::get_if<Column<std::int64_t>>(key);
        if (col == nullptr)
            return std::unexpected("inner join: build-side key type mismatch");
        build_scalar(*col, index.i64_heads);
    } else if (key_kind == ExprType::Double) {
        const auto* col = std::get_if<Column<double>>(key);
        if (col == nullptr)
            return std::unexpected("inner join: build-side key type mismatch");
        build_scalar(*col, index.f64_heads);
    } else if (key_kind == ExprType::Bool) {
        const auto* col = std::get_if<Column<bool>>(key);
        if (col == nullptr)
            return std::unexpected("inner join: build-side key type mismatch");
        // No `data()` on a packed bool column, so this is the one
        // representation `build_scalar` cannot serve. Left unpartitioned: a
        // bool key has two values, so partitioning can only leave every
        // partition but two empty.
        fill_partitioned_heads(
            index.bool_heads, n, [col](std::size_t r) { return (*col)[r]; }, is_null,
            index.chain_next, index.unique);
    } else if (key_kind == ExprType::Date) {
        const auto* col = std::get_if<Column<Date>>(key);
        if (col == nullptr)
            return std::unexpected("inner join: build-side key type mismatch");
        build_scalar(*col, index.date_heads);
    } else if (key_kind == ExprType::Timestamp) {
        const auto* col = std::get_if<Column<Timestamp>>(key);
        if (col == nullptr)
            return std::unexpected("inner join: build-side key type mismatch");
        build_scalar(*col, index.ts_heads);
    } else if (key_kind == ExprType::String) {
        index.string_heads.partition(partitions);
        if (const auto* c_cat = std::get_if<Column<Categorical>>(key)) {
            const auto& dict = c_cat->dictionary();
            fill_partitioned_heads(
                index.string_heads, n,
                [c_cat, &dict](std::size_t r) {
                    return std::string_view{dict[static_cast<std::size_t>(c_cat->code_at(r))]};
                },
                is_null, index.chain_next, index.unique);
        } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
            fill_partitioned_heads(
                index.string_heads, n,
                [c_str](std::size_t r) { return std::string_view{(*c_str)[r]}; }, is_null,
                index.chain_next, index.unique);
        } else {
            return std::unexpected("inner join: build-side key type mismatch");
        }
    }
    return index;
}

/// The two-key hash build: same chain-of-equal-rows convention as
/// `build_join_hash_index`, over a packed pair of Int64 keys. A row with either
/// key null is never indexed -- null never matches, not even another null.
auto build_join_pair_index(const Column<std::int64_t>& col0, const Column<std::int64_t>& col1,
                           const ValidityBitmap* v0, const ValidityBitmap* v1,
                           std::size_t partitions) -> JoinHashIndex {
    JoinHashIndex index;
    index.key_kind = ExprType::Int;
    const std::size_t n = col0.size();
    index.chain_next.assign(n, kJoinNil);
    const auto* d0 = col0.data();
    const auto* d1 = col1.data();
    index.pair_heads.partition(partitions);
    fill_partitioned_heads(
        index.pair_heads, n,
        [d0, d1](std::size_t r) {
            return JoinHashIndex::PairKey{.a = static_cast<std::uint64_t>(d0[r]),
                                          .b = static_cast<std::uint64_t>(d1[r])};
        },
        [v0, v1](std::size_t r) {
            return (v0 != nullptr && !(*v0)[r]) || (v1 != nullptr && !(*v1)[r]);
        },
        index.chain_next, index.unique);
    return index;
}

/// Which side of a hash join carries the index.
///
/// The choice is made at RUN time, from measured row counts, and this enum is
/// what makes it a VALUE rather than a shape encoded across three operator
/// members (`mode_`, `probe_op_`, `left_table_`). A build phase
/// can decide it without knowing who will probe, which is what a separately
/// scheduled `HashBuild` needs; see plans/kernel-pipeline-execution-plan.md,
/// "The build-side choice does not block the split" -- the physical plan does
/// not have to name the side statically, because the pipeline that scans the
/// other side is constructed after this phase has already run.
enum class JoinOrientation : std::uint8_t {
    BuildRight,  ///< index the right side, stream left chunks through it
    BuildLeft,   ///< index the left side, scan the right once in probe order
};

/// Everything a hash build phase decides and produces. Which table to stream
/// and which mode to run in are the caller's derivations from these two, not
/// the build's business.
struct JoinBuildOutcome {
    std::shared_ptr<const JoinHashIndex> index;
    JoinOrientation orientation = JoinOrientation::BuildRight;
};

/// Build the index over one named side. The primitive both orientation
/// decisions below reduce to, and the only place a `JoinHashIndex` becomes
/// shared and const.
auto build_join_side(const Table& side, const std::string& key_name, ExprType key_kind,
                     JoinOrientation orientation, std::size_t partitions)
    -> std::expected<JoinBuildOutcome, std::string> {
    auto built = build_join_hash_index(side, key_name, key_kind, partitions);
    if (!built.has_value()) {
        return std::unexpected(std::move(built.error()));
    }
    return JoinBuildOutcome{.index = std::make_shared<const JoinHashIndex>(std::move(*built)),
                            .orientation = orientation};
}

/// The single-key build phase over two already-materialized sides: choose an
/// orientation, build that side's index, return both. Reads and writes no
/// operator state, so the same call serves a join operator and a `HashBuild`
/// that has no probe attached yet.
///
/// `order_preservation_pays` arrives as a decided bool because it answers a
/// question about the join's OUTPUT plan -- would declining to swap deliver a
/// pending `order` for free -- which is the caller's to answer, not the
/// build's. It is only ever consulted when swapping was otherwise preferred.
auto choose_and_build_single_key(const Table& left, const Table& right, const std::string& left_key,
                                 const std::string& right_key, ExprType key_kind,
                                 bool order_preservation_pays, std::size_t partitions)
    -> std::expected<JoinBuildOutcome, std::string> {
    // Swapping indexes the smaller (left) side and scans the right, which
    // gives up left-row order. When an `order` above this join wants exactly
    // the order the left already carries, declining to swap delivers it and
    // that whole sort disappears -- worth a larger index, but only while
    // "larger" stays modest, since the index is probed once per row of the
    // other side. The same trade is made in join.cpp.
    if (left.rows() < right.rows() && !order_preservation_pays) {
        return build_join_side(left, left_key, key_kind, JoinOrientation::BuildLeft, partitions);
    }
    return build_join_side(right, right_key, key_kind, JoinOrientation::BuildRight, partitions);
}

/// One side's two Int64 key columns and their validity, or the error a join
/// reports for them. `side_name` is "left" or "right" only so the message
/// keeps naming the side the caller was asking about.
struct PairKeyColumns {
    const Column<std::int64_t>* col0 = nullptr;
    const Column<std::int64_t>* col1 = nullptr;
    const ValidityBitmap* v0 = nullptr;
    const ValidityBitmap* v1 = nullptr;
};

auto pair_key_columns(const Table& side, const std::string& name0, const std::string& name1,
                      std::string_view side_name) -> std::expected<PairKeyColumns, std::string> {
    const ColumnValue* key0 = side.find(name0);
    if (key0 == nullptr) {
        return std::unexpected("join key not found in " + std::string(side_name) +
                               " table: " + name0);
    }
    const ColumnValue* key1 = side.find(name1);
    if (key1 == nullptr) {
        return std::unexpected("join key not found in " + std::string(side_name) +
                               " table: " + name1);
    }
    PairKeyColumns out;
    out.col0 = std::get_if<Column<std::int64_t>>(key0);
    out.col1 = std::get_if<Column<std::int64_t>>(key1);
    if (out.col0 == nullptr || out.col1 == nullptr) {
        return std::unexpected(
            "ChunkedInnerJoinOperator: two-key join currently requires both keys to be Int64");
    }
    const auto* entry0 = side.find_entry(name0);
    const auto* entry1 = side.find_entry(name1);
    out.v0 = entry0 != nullptr && entry0->validity.has_value() ? &*entry0->validity : nullptr;
    out.v1 = entry1 != nullptr && entry1->validity.has_value() ? &*entry1->validity : nullptr;
    return out;
}

/// The two-Int64-key build phase. Same contract as the single-key one, with a
/// simpler decision: no pending-order trade exists on this path, so the
/// smaller side is indexed outright. Validates the right side first, which is
/// the order the operator already checked in, so consolidating the two
/// previously separate blocks cannot change which error a caller sees.
auto choose_and_build_pair(const Table& left, const Table& right, const ir::JoinKey& k0,
                           const ir::JoinKey& k1, std::size_t partitions)
    -> std::expected<JoinBuildOutcome, std::string> {
    auto right_keys = pair_key_columns(right, k0.right, k1.right, "right");
    if (!right_keys.has_value()) {
        return std::unexpected(std::move(right_keys.error()));
    }
    if (left.rows() <= right.rows()) {
        auto left_keys = pair_key_columns(left, k0.left, k1.left, "left");
        if (!left_keys.has_value()) {
            return std::unexpected(std::move(left_keys.error()));
        }
        return JoinBuildOutcome{
            .index = std::make_shared<const JoinHashIndex>(build_join_pair_index(
                *left_keys->col0, *left_keys->col1, left_keys->v0, left_keys->v1, partitions)),
            .orientation = JoinOrientation::BuildLeft};
    }
    return JoinBuildOutcome{
        .index = std::make_shared<const JoinHashIndex>(build_join_pair_index(
            *right_keys->col0, *right_keys->col1, right_keys->v0, right_keys->v1, partitions)),
        .orientation = JoinOrientation::BuildRight};
}

/// The probe half of a hash join: everything that consumes a `JoinHashIndex`
/// and turns probe-side rows into join output rows. It owns no build, which is
/// the point -- Phase 4's `HashProbe` has to be able to exist next to a build
/// it did not run, and an operator class that also decides which side to index
/// cannot be that.
///
/// Held by the operator today rather than scheduled on its own. What the
/// extraction buys now is that the probe's state is enumerable: an index, the
/// join's two output-name plans, the per-worker scratch, and the probe chunk's
/// own validity/dictionary. Nothing else in the join can reach it, and it can
/// reach nothing else in the join.
struct JoinProbe {
    const std::vector<ir::JoinKey>* keys_ = nullptr;
    const ExecutionContext* exec_ = nullptr;
    /// The `probe` phase's parallelism, resolved by the caller and set through
    /// `ChunkedInnerJoinOperator::bind_probe`. `probe_parallel_workers` reads it
    /// rather than re-deriving the floor and the cap. Travels with every copy of
    /// this struct (per-worker probes, `JoinProbeOperator`).
    physical::BreakerParallelism probe_plan_{};
    ir::JoinSuffixPolicy suffix_;
    /// The join's right table, owned jointly and immutably.
    ///
    /// Shared rather than borrowed because a probe has to be able to outlive
    /// the operator that ran the build, and because several probes -- one per
    /// morsel worker -- have to be able to read one build side at once. A raw
    /// pointer into a member could do neither, and depended on the operator
    /// never reallocating the table it points at. Null until the build phase
    /// resolves it, which on the deferred path is only after the scan runs.
    std::shared_ptr<const Table> right_;
    /// What the build phase produced. Immutable, and shareable: this is the
    /// only thing the probe needs from a build.
    std::shared_ptr<const JoinHashIndex> index_;
    /// True when the join keys are the two-Int64 pair shape.
    bool pair_mode_ = false;

    /// Reset per probe chunk.
    const ValidityBitmap* probe_validity_ = nullptr;
    /// Probe-side, not build-side: the probe chunk's dictionary code -> build
    /// chain head (`kJoinNil` = no match), rebuilt per chunk by
    /// `resolve_categorical_heads`.
    std::vector<std::size_t> probe_code_heads_;

    /// One worker's slice of a parallel probe. Members so the vectors keep
    /// their capacity across chunks instead of reallocating per probe.
    struct ProbePart {
        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
    };
    /// One matching probe row in swapped mode: the right row and the head of
    /// the left chain it hit. Phase 2 replays these instead of re-probing.
    struct SwappedHit {
        std::size_t rrow;
        std::size_t head;  ///< first left row in the chain for this key
    };
    /// One worker's slice of a swapped-mode phase 1. A member for the same
    /// reason as `ProbePart`: capacity survives across chunks.
    struct SwappedPart {
        std::vector<SwappedHit> hits;
        std::size_t total = 0;  ///< output rows this part's chains expand to
    };
    std::vector<ProbePart> probe_parts_;
    std::vector<SwappedPart> swapped_parts_;
    std::vector<std::size_t> part_offsets_;
    std::vector<std::size_t> right_emit_idx_;
    std::vector<std::string> right_emit_names_;
    std::vector<std::string> left_emit_names_;
    std::optional<ir::JoinColumnMapping> columns_;
    bool right_emit_ready_ = false;

    static constexpr std::size_t kNil = kJoinNil;

    /// The build this probe reads. Never null once a build side has been
    /// chosen; the `Precomputed` mode returns before one exists.
    [[nodiscard]] auto index() const noexcept -> const JoinHashIndex& { return *index_; }

    [[nodiscard]] auto probe_is_null(std::size_t row) const noexcept -> bool {
        return probe_validity_ != nullptr && !(*probe_validity_)[row];
    }

    // Which right columns this join emits, and under which names. Both come
    // from the shared planner (ir/join_output.hpp), so the chunked route lands
    // on the same output schema as the materialized route and IR inference.
    // The left column names are identical for every chunk, so the plan is
    // computed once from the first assembled chunk.

    auto setup_right_emit_schema(const Table& left_side) -> std::expected<void, std::string> {
        if (right_emit_ready_) {
            return {};
        }
        const auto left_names = table_column_names(left_side);
        const auto right_names = table_column_names(*right_);
        auto concrete = ir::resolve_join_columns(ir::JoinKind::Inner, *keys_, left_names,
                                                 right_names, suffix_);
        if (!concrete.has_value()) {
            return std::unexpected(std::move(concrete.error()));
        }
        const bool concrete_layout_matches_plan =
            columns_.has_value() && columns_->left_input_names.size() == left_names.size() &&
            columns_->right_input_names.size() == right_names.size() &&
            std::ranges::equal(columns_->left_input_names, left_names) &&
            std::ranges::equal(columns_->right_input_names, right_names);
        if (!concrete_layout_matches_plan) {
            // A pushed-down predicate may consume a column inside a lazy child
            // and omit it from the join input. Rebind the complete key/output
            // mapping at that concrete boundary once; probe and gather loops
            // remain positional.
            columns_ = std::move(*concrete);
        } else if (*columns_ != *concrete) {
            return std::unexpected(
                "physical join column mapping does not match its concrete inputs");
        }
        if (columns_->keys.size() != keys_->size()) {
            return std::unexpected("physical join column mapping has the wrong key count");
        }
        for (std::size_t i = 0; i < keys_->size(); ++i) {
            const ir::JoinKeyColumns& mapped = columns_->keys[i];
            if (mapped.left_index >= left_side.columns.size() ||
                mapped.right_index >= right_->columns.size() ||
                left_side.columns[mapped.left_index].name != keys_->at(i).left ||
                right_->columns[mapped.right_index].name != keys_->at(i).right) {
                return std::unexpected(
                    "physical join column mapping does not match its concrete inputs");
            }
        }
        const std::vector<ir::JoinOutputColumn>& plan = columns_->output;
        // A suffix clause renames the *left* side of a collision too, so the
        // left names come from the plan as well; taking them from the chunk
        // would keep the pre-rename spelling.
        left_emit_names_.reserve(left_side.columns.size());
        for (const auto& column : plan) {
            if (column.side == ir::JoinOutputSide::Left) {
                left_emit_names_.push_back(column.name);
            }
        }
        right_emit_idx_.reserve(plan.size() - left_side.columns.size());
        right_emit_names_.reserve(plan.size() - left_side.columns.size());
        for (const auto& column : plan) {
            if (column.side != ir::JoinOutputSide::Right) {
                continue;
            }
            right_emit_idx_.push_back(column.source_index);
            right_emit_names_.push_back(column.name);
        }
        right_emit_ready_ = true;
        return {};
    }

    /// Run `body(begin, end, li, ri)` over the probe rows across workers,
    /// concatenating each range's output in range order. Returns false when the
    /// parallel path declines and the caller should run its serial loop.
    ///
    /// **This is the join's only parallel axis, and it is the whole of it.**
    /// The build side is a shared read-only hash index — `heads`, `index().chain_next`
    /// — so probing it concurrently needs no locking at all, and the build
    /// itself is not worth threading: it is 1.5% of q10 against the probe and
    /// output assembly's ~15%.
    ///
    /// **Per-worker output rather than count-then-fill.** The obvious shape is
    /// to count matches per range, prefix-sum, then have each worker write its
    /// slice — but that probes the hash table TWICE per row, and a redundant
    /// cache-missing lookup per probe row is exactly the cost `emit_swapped`
    /// was restructured to avoid (q03 probes 3.2M lineitems to emit ~30K rows).
    /// Each worker appends to its own vectors instead and they are concatenated
    /// afterwards: one memcpy of two size_t arrays, against one hash probe per
    /// row saved.
    ///
    /// Order is exactly the serial order — ranges are contiguous and visited in
    /// order, and each range appends in row order — so the output is
    /// byte-identical however the workers interleave.
    /// The shared admission gate for every parallel probe axis: how many
    /// workers a probe over `n` rows may fan out to, or 0 to decline and run
    /// the caller's serial loop.
    ///
    /// The floor (`1U << 14U`) and the worker cap (`min(compute_budget, pool,
    /// 64)`) are the `probe` phase of the physical plan
    /// (`physical::join_probe_parallelism`), read from `probe_plan_`.
    /// src/runtime/PARALLELISM.md, "Target: parallelism as a plan decision".
    /// The checks that stay here are the ones only the operator can make:
    /// `parallel_join_probe` (a feature toggle, kept operator-side like the hash
    /// build's `IBEX_JOIN_BUILD_SERIAL`), nesting (`on_worker_pool_thread` -- no
    /// nested pool submissions), and whether *this chunk* cleared the floor (a
    /// streamed probe side's per-chunk row count is not a plan-time fact).
    [[nodiscard]] auto probe_parallel_workers(std::size_t n) const -> std::size_t {
        if (exec_ == nullptr || !exec_->parallel_join_probe || on_worker_pool_thread() ||
            probe_plan_.decline != physical::FanOutDecline::None || probe_plan_.worker_cap < 2 ||
            n < probe_plan_.row_floor) {
            return 0;
        }
        return probe_plan_.worker_cap;
    }

    template <typename Body>
    auto probe_ranges_parallel(std::size_t n, std::vector<std::size_t>& li,
                               std::vector<std::size_t>& ri, const Body& body) -> bool {
        const std::size_t workers = probe_parallel_workers(n);
        if (workers == 0) {
            return false;
        }
        auto& pool = process_worker_pool();
        const std::size_t grain = (n + workers - 1) / workers;
        probe_parts_.resize(workers);
        {
            auto batch = pool.submit(workers, [&](std::size_t w) {
                auto& part = probe_parts_[w];
                part.li.clear();
                part.ri.clear();
                const std::size_t begin = w * grain;
                const std::size_t end = std::min(n, begin + grain);
                if (begin >= end) {
                    return;
                }
                // Reserve for the common case of roughly one match per row;
                // a fan-out join grows past it, which is what a vector is for.
                part.li.reserve(end - begin);
                part.ri.reserve(end - begin);
                body(begin, end, part.li, part.ri);
            });
            batch.wait();
        }
        std::size_t total = 0;
        for (const auto& part : probe_parts_) {
            total += part.li.size();
        }
        li.resize(total);
        ri.resize(total);
        // The concat is the price the fan-out pays that the serial probe does
        // not, and on a high-match join it is the whole regression: every row
        // matching means `total == n`, i.e. two full index arrays copied
        // again. Each part's destination slice is disjoint and known, so the
        // copies go back to the workers; below the threshold the batch costs
        // more than the memcpy it spreads.
        constexpr std::size_t kMinParallelConcatRows = 1U << 16U;
        const auto copy_part = [&](std::size_t w, std::size_t at) {
            const auto& part = probe_parts_[w];
            std::ranges::copy(part.li, li.begin() + static_cast<std::ptrdiff_t>(at));
            std::ranges::copy(part.ri, ri.begin() + static_cast<std::ptrdiff_t>(at));
        };
        part_offsets_.resize(workers);
        std::size_t at = 0;
        for (std::size_t w = 0; w < workers; ++w) {
            part_offsets_[w] = at;
            at += probe_parts_[w].li.size();
        }
        if (total >= kMinParallelConcatRows) {
            auto batch =
                pool.submit(workers, [&](std::size_t w) { copy_part(w, part_offsets_[w]); });
            batch.wait();
        } else {
            for (std::size_t w = 0; w < workers; ++w) {
                copy_part(w, part_offsets_[w]);
            }
        }
        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_probes.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    /// Swapped-mode probe: phase 1 walks right rows `head_of` resolves against
    /// the left index, phase 2 expands the recorded chains into (li, ri).
    /// The parallel path fans phase 1 out over contiguous right-row ranges and
    /// phase 2 out over the per-range hit lists — each part's output slice
    /// starts at the prefix sum of the parts before it, so workers write
    /// disjoint slices and the result is byte-identical to the serial replay
    /// (parts are visited in range order, ranges in row order).
    template <typename HeadOf>
    void probe_swapped(std::size_t n_right, const HeadOf& head_of, std::vector<std::size_t>& li,
                       std::vector<std::size_t>& ri) {
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<SwappedHit>& hits,
                              std::size_t& total) {
            for (std::size_t r = begin; r < end; ++r) {
                if (probe_is_null(r)) {
                    continue;
                }
                const std::size_t head = head_of(r);
                if (head == kNil) {
                    continue;
                }
                hits.push_back(SwappedHit{.rrow = r, .head = head});
                for (std::size_t cur = head; cur != kNil; cur = index().chain_next[cur]) {
                    ++total;
                }
            }
        };
        const auto replay = [&](const std::vector<SwappedHit>& hits, std::size_t pos) {
            for (const SwappedHit& hit : hits) {
                for (std::size_t cur = hit.head; cur != kNil; cur = index().chain_next[cur]) {
                    li[pos] = cur;
                    ri[pos] = hit.rrow;
                    ++pos;
                }
            }
        };

        const std::size_t workers = probe_parallel_workers(n_right);
        if (workers == 0) {
            std::vector<SwappedHit> hits;
            std::size_t total = 0;
            scan(0, n_right, hits, total);
            li.assign(total, 0);
            ri.assign(total, 0);
            replay(hits, 0);
            return;
        }
        auto& pool = process_worker_pool();
        const std::size_t grain = (n_right + workers - 1) / workers;
        swapped_parts_.resize(workers);
        {
            auto batch = pool.submit(workers, [&](std::size_t w) {
                auto& part = swapped_parts_[w];
                part.hits.clear();
                part.total = 0;
                const std::size_t begin = w * grain;
                const std::size_t end = std::min(n_right, begin + grain);
                if (begin < end) {
                    scan(begin, end, part.hits, part.total);
                }
            });
            batch.wait();
        }
        part_offsets_.resize(workers);
        std::size_t total = 0;
        for (std::size_t w = 0; w < workers; ++w) {
            part_offsets_[w] = total;
            total += swapped_parts_[w].total;
        }
        li.assign(total, 0);
        ri.assign(total, 0);
        {
            auto batch = pool.submit(
                workers, [&](std::size_t w) { replay(swapped_parts_[w].hits, part_offsets_[w]); });
            batch.wait();
        }
        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_probes.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Stream mode: walk the probe side (a left chunk), for each row look
    // up the right-keyed chain and append (li, ri) in probe-scan order.
    // Returns true if every probe row matched exactly once (li == 0..n-1).
    // Only possible when the build side was unique; otherwise falls back
    // to the chained walk.
    template <typename Map, typename GetKey>
    auto probe_scalar(const Map& heads, std::size_t n, GetKey get, std::vector<std::size_t>& li,
                      std::vector<std::size_t>& ri) -> bool {
        // One body for both paths, so the parallel and serial results cannot
        // drift: the parallel one runs it per range, the serial one once.
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<std::size_t>& out_l,
                              std::vector<std::size_t>& out_r) {
            for (std::size_t l = begin; l < end; ++l) {
                if (probe_is_null(l)) {
                    continue;
                }
                const std::size_t head = heads.find_head(get(l));
                if (head == kNil) {
                    continue;
                }
                for (std::size_t cur = head; cur != kNil; cur = index().chain_next[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            // `li_identity` means li == 0..n-1, which for a unique build side
            // is exactly "every row matched" — the same test the serial path
            // makes, just recovered from the totals.
            return index().unique && li.size() == n;
        }
        if (index().unique) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                if (probe_is_null(l)) {
                    continue;
                }
                const std::size_t head = heads.find_head(get(l));
                if (head == kNil) {
                    continue;
                }
                lp[out] = l;
                rp[out] = head;
                ++out;
            }
            li.resize(out);
            ri.resize(out);
            return out == n;
        }
        for (std::size_t l = 0; l < n; ++l) {
            if (probe_is_null(l)) {
                continue;
            }
            const std::size_t head = heads.find_head(get(l));
            if (head == kNil) {
                continue;
            }
            std::size_t cur = head;
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = index().chain_next[cur];
            }
        }
        return false;
    }

    // Probe with the build-side chain head already resolved per row. Same two
    // shapes as `probe_scalar`, but the caller supplies the head instead of a
    // key to hash — see `resolve_categorical_heads`.
    template <typename GetHead>
    auto probe_resolved(std::size_t n, GetHead head_of, std::vector<std::size_t>& li,
                        std::vector<std::size_t>& ri) -> bool {
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<std::size_t>& out_l,
                              std::vector<std::size_t>& out_r) {
            for (std::size_t l = begin; l < end; ++l) {
                if (probe_is_null(l)) {
                    continue;
                }
                for (std::size_t cur = head_of(l); cur != kNil; cur = index().chain_next[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            return index().unique && li.size() == n;
        }
        if (index().unique) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                if (probe_is_null(l)) {
                    continue;
                }
                const std::size_t head = head_of(l);
                if (head == kNil) {
                    continue;
                }
                lp[out] = l;
                rp[out] = head;
                ++out;
            }
            li.resize(out);
            ri.resize(out);
            return out == n;
        }
        for (std::size_t l = 0; l < n; ++l) {
            if (probe_is_null(l)) {
                continue;
            }
            std::size_t cur = head_of(l);
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = index().chain_next[cur];
            }
        }
        return false;
    }

    // A Categorical probe column is a dictionary plus one code per row, so
    // every row sharing a code resolves to the same build chain. Resolve the
    // DICTIONARY against the build index once — 252 entries for the symbol
    // join — instead of rebuilding a string_view and hashing plus memcmp'ing
    // it per row across 8M rows. That lookup was the single largest cost in
    // the join profile (robin_hood string probe + __memcmp_avx2 + _Hash_bytes
    // together ~57%).
    //
    // Rebuilt per chunk rather than cached on the operator: chunks of one scan
    // usually share a dictionary, but nothing in the type guarantees it, and a
    // stale memo would silently join against the wrong rows. |dict| lookups
    // per chunk is noise next to |chunk rows|.
    void resolve_categorical_heads(const std::vector<std::string>& dict) {
        probe_code_heads_.assign(dict.size(), kNil);
        for (std::size_t c = 0; c < dict.size(); ++c) {
            probe_code_heads_[c] = index().string_heads.find_head(std::string_view{dict[c]});
        }
    }

    auto probe_chunk_against_right(Table left_chunk) -> std::expected<Table, std::string> {
        if (auto mapped = setup_right_emit_schema(left_chunk); !mapped.has_value()) {
            return std::unexpected(std::move(mapped.error()));
        }
        if (pair_mode_) {
            return probe_chunk_pair(std::move(left_chunk));
        }
        const ir::JoinKeyColumns& key_columns = columns_->keys.front();
        const ColumnEntry& probe_entry = left_chunk.columns[key_columns.left_index];
        const ColumnValue* key = probe_entry.column.get();
        probe_validity_ = probe_entry.validity.has_value() ? &*probe_entry.validity : nullptr;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        const std::size_t n = left_chunk.rows();
        li.reserve(n);
        ri.reserve(n);
        bool li_identity = false;

        if (index().key_kind == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(index().i64_heads, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (index().key_kind == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(index().f64_heads, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (index().key_kind == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            li_identity = probe_scalar(
                index().bool_heads, n, [&](std::size_t i) { return (*col)[i]; }, li, ri);
        } else if (index().key_kind == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(index().date_heads, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (index().key_kind == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(index().ts_heads, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (index().key_kind == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(key)) {
                const auto& dict = c_cat->dictionary();
                // Resolving the dictionary costs |dict| hash lookups and saves
                // one per row, so it pays exactly when the dictionary is
                // smaller than the chunk. A dictionary larger than the chunk
                // (a narrow slice of a high-cardinality column) would hash more
                // keys than there are rows to answer.
                if (dict.size() < n) {
                    resolve_categorical_heads(dict);
                    li_identity = probe_resolved(
                        n,
                        [&](std::size_t i) {
                            return probe_code_heads_[static_cast<std::size_t>(c_cat->code_at(i))];
                        },
                        li, ri);
                } else {
                    li_identity = probe_scalar(
                        index().string_heads, n,
                        [&](std::size_t i) {
                            return std::string_view{
                                dict[static_cast<std::size_t>(c_cat->code_at(i))]};
                        },
                        li, ri);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
                li_identity = probe_scalar(
                    index().string_heads, n, [&](std::size_t i) { return (*c_str)[i]; }, li, ri);
            } else {
                return std::unexpected("inner join: left key type mismatch");
            }
        }

        const std::size_t total = li_identity ? ri.size() : li.size();
        return assemble_output(std::move(left_chunk), li.data(), ri.data(), total, li_identity);
    }

    auto probe_chunk_pair(Table left_chunk) -> std::expected<Table, std::string> {
        const ir::JoinKeyColumns& k0 = columns_->keys[0];
        const ir::JoinKeyColumns& k1 = columns_->keys[1];
        const ColumnEntry& e0 = left_chunk.columns[k0.left_index];
        const ColumnEntry& e1 = left_chunk.columns[k1.left_index];
        const ColumnValue* key0 = e0.column.get();
        const ColumnValue* key1 = e1.column.get();
        const auto* col0 = std::get_if<Column<std::int64_t>>(key0);
        const auto* col1 = std::get_if<Column<std::int64_t>>(key1);
        if (col0 == nullptr || col1 == nullptr) {
            return std::unexpected(
                "inner join: left key type mismatch (two-key join expects "
                "Int64)");
        }
        const ValidityBitmap* v0 = e0.validity.has_value() ? &*e0.validity : nullptr;
        const ValidityBitmap* v1 = e1.validity.has_value() ? &*e1.validity : nullptr;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        const std::size_t n = left_chunk.rows();
        li.reserve(n);
        ri.reserve(n);

        const auto* d0 = col0->data();
        const auto* d1 = col1->data();
        const auto is_null = [&](std::size_t r) {
            return (v0 != nullptr && !(*v0)[r]) || (v1 != nullptr && !(*v1)[r]);
        };
        const auto get_key = [&](std::size_t r) {
            return JoinHashIndex::PairKey{.a = static_cast<std::uint64_t>(d0[r]),
                                          .b = static_cast<std::uint64_t>(d1[r])};
        };
        const bool li_identity = probe_pair(n, is_null, get_key, li, ri);

        const std::size_t total = li_identity ? ri.size() : li.size();
        return assemble_output(std::move(left_chunk), li.data(), ri.data(), total, li_identity);
    }

    // Same two shapes as `probe_scalar` (parallel fan-out via
    // `probe_ranges_parallel`, then a unique-build fast path, then the
    // general chained walk) but with an explicit null check instead of the
    // single-bitmap `probe_is_null` member, since a probe row here is null
    // when EITHER key is.
    template <typename IsNull, typename GetKey>
    auto probe_pair(std::size_t n, IsNull is_null, GetKey get_key, std::vector<std::size_t>& li,
                    std::vector<std::size_t>& ri) -> bool {
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<std::size_t>& out_l,
                              std::vector<std::size_t>& out_r) {
            for (std::size_t l = begin; l < end; ++l) {
                if (is_null(l)) {
                    continue;
                }
                const std::size_t head = index().pair_heads.find_head(get_key(l));
                if (head == kNil) {
                    continue;
                }
                for (std::size_t cur = head; cur != kNil; cur = index().chain_next[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            return index().unique && li.size() == n;
        }
        if (index().unique) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                if (is_null(l)) {
                    continue;
                }
                const std::size_t head = index().pair_heads.find_head(get_key(l));
                if (head == kNil) {
                    continue;
                }
                lp[out] = l;
                rp[out] = head;
                ++out;
            }
            li.resize(out);
            ri.resize(out);
            return out == n;
        }
        for (std::size_t l = 0; l < n; ++l) {
            if (is_null(l)) {
                continue;
            }
            const std::size_t head = index().pair_heads.find_head(get_key(l));
            if (head == kNil) {
                continue;
            }
            std::size_t cur = head;
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = index().chain_next[cur];
            }
        }
        return false;
    }

    // Swapped mode: the hash index is on the left table, so the right table
    // is the probe side, and output must still come out in left-row order.
    //
    // Phase 1 probes each right row once and remembers the head of every left
    // chain it hit; phase 2 replays just those hits to fill (li, ri). The hash
    // table is therefore probed once per right row for the whole join, not
    // once per phase: a selective join over a large right side (q03 probes
    // 3.2M lineitems to emit ~30K rows) no longer pays for 3.2M redundant
    // cache-missing lookups. `hits` costs one entry per *matching* right row,
    // so it is bounded by the output row count.
    auto emit_swapped(const Table& left_table) -> std::expected<Table, std::string> {
        if (auto mapped = setup_right_emit_schema(left_table); !mapped.has_value()) {
            return std::unexpected(std::move(mapped.error()));
        }
        if (pair_mode_) {
            return emit_swapped_pair(left_table);
        }
        const ir::JoinKeyColumns& key_columns = columns_->keys.front();
        const ColumnEntry& right_entry = right_->columns[key_columns.right_index];
        const ColumnValue* rkey = right_entry.column.get();
        const std::size_t n_right = right_->rows();

        // In swapped mode the index is on the left, so the right table is the
        // probe side. Its null-keyed rows match nothing (see build_join_hash_index).
        probe_validity_ = right_entry.validity.has_value() ? &*right_entry.validity : nullptr;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;

        // Every key kind reduces to "resolve right row r to a left chain head
        // or kNil"; the map branches wrap the hash lookup, the categorical
        // fast path hands the pre-resolved head straight through. One shape
        // means `probe_swapped` is the single scan/replay implementation for
        // both the serial and the parallel path.
        auto do_phase1 = [&](auto&& key_at, const auto& heads) {
            probe_swapped(
                n_right, [&](std::size_t r) { return heads.find_head(key_at(r)); }, li, ri);
        };
        // Same shape with the chain head already resolved — see
        // `resolve_categorical_heads`.
        auto do_phase1_resolved = [&](auto&& head_at) { probe_swapped(n_right, head_at, li, ri); };

        if (index().key_kind == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, index().i64_heads);
        } else if (index().key_kind == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, index().f64_heads);
        } else if (index().key_kind == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            do_phase1([&](std::size_t r) { return (*col)[r]; }, index().bool_heads);
        } else if (index().key_kind == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, index().date_heads);
        } else if (index().key_kind == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, index().ts_heads);
        } else if (index().key_kind == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(rkey)) {
                const auto& dict = c_cat->dictionary();
                if (dict.size() < n_right) {
                    resolve_categorical_heads(dict);
                    do_phase1_resolved([&](std::size_t r) {
                        return probe_code_heads_[static_cast<std::size_t>(c_cat->code_at(r))];
                    });
                } else {
                    do_phase1(
                        [&](std::size_t r) {
                            return std::string_view{
                                dict[static_cast<std::size_t>(c_cat->code_at(r))]};
                        },
                        index().string_heads);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(rkey)) {
                do_phase1([&](std::size_t r) { return (*c_str)[r]; }, index().string_heads);
            } else {
                return std::unexpected("inner join: right key type mismatch");
            }
        }

        // Output order is the order phase 1 visited the hits — right-scan
        // (probe) order. Row order is outside the join contract (SPEC.md
        // §5.6), so there's no correctness reason to reassemble by left row
        // instead; doing so was actively harmful, permuting the output away
        // from the probe side's natural scan order and hurting cache locality
        // on any downstream join that probes this join's output.
        Table left_copy;
        left_copy.columns.reserve(left_table.columns.size());
        for (const auto& c : left_table.columns) {
            left_copy.add_column(c.name, *c.column);
            left_copy.columns.back().validity = c.validity;
        }
        return assemble_output(std::move(left_copy), li.data(), ri.data(), li.size());
    }

    // Swapped pair-mode counterpart of `emit_swapped`: the pair index is on
    // the build-side left table, so the right table's rows are the probe
    // side. Reuses
    // `probe_swapped` unchanged -- it is already generic over a
    // `head_of(row)` callback -- with the null check folded into `head_of`
    // itself (returning `kNil`) instead of the single-bitmap `probe_is_null`
    // member, since a row here is null when EITHER key is.
    auto emit_swapped_pair(const Table& left_table) -> std::expected<Table, std::string> {
        const ir::JoinKeyColumns& k0 = columns_->keys[0];
        const ir::JoinKeyColumns& k1 = columns_->keys[1];
        const ColumnEntry& e0 = right_->columns[k0.right_index];
        const ColumnEntry& e1 = right_->columns[k1.right_index];
        const ColumnValue* rkey0 = e0.column.get();
        const ColumnValue* rkey1 = e1.column.get();
        const auto* col0 = std::get_if<Column<std::int64_t>>(rkey0);
        const auto* col1 = std::get_if<Column<std::int64_t>>(rkey1);
        if (col0 == nullptr || col1 == nullptr) {
            return std::unexpected(
                "inner join: right key type mismatch (two-key join expects Int64)");
        }
        const ValidityBitmap* v0 = e0.validity.has_value() ? &*e0.validity : nullptr;
        const ValidityBitmap* v1 = e1.validity.has_value() ? &*e1.validity : nullptr;
        const auto* d0 = col0->data();
        const auto* d1 = col1->data();
        const std::size_t n_right = right_->rows();

        const auto head_of = [&](std::size_t r) -> std::size_t {
            if ((v0 != nullptr && !(*v0)[r]) || (v1 != nullptr && !(*v1)[r])) {
                return kNil;
            }
            const JoinHashIndex::PairKey key{.a = static_cast<std::uint64_t>(d0[r]),
                                             .b = static_cast<std::uint64_t>(d1[r])};
            return index().pair_heads.find_head(key);
        };

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        probe_swapped(n_right, head_of, li, ri);

        Table left_copy;
        left_copy.columns.reserve(left_table.columns.size());
        for (const auto& c : left_table.columns) {
            left_copy.add_column(c.name, *c.column);
            left_copy.columns.back().validity = c.validity;
        }
        return assemble_output(std::move(left_copy), li.data(), ri.data(), li.size());
    }

    auto assemble_output(Table left_side, const std::size_t* li, const std::size_t* ri,
                         std::size_t total, bool li_identity = false, bool ri_identity = false)
        -> std::expected<Table, std::string> {
        Table output;
        if (!right_emit_ready_) {
            if (auto ready = setup_right_emit_schema(left_side); !ready.has_value()) {
                return std::unexpected(std::move(ready.error()));
            }
        }
        output.columns.reserve(left_side.columns.size() + right_emit_idx_.size());

        // A stream with no matches still has a schema. Returning a bare empty
        // table here used to be harmless only because the regular chunked path
        // normally has another node to provide one; the whole-table adapter
        // must be equivalent to join_table_impl even for an empty result.
        if (total == 0) {
            for (std::size_t i = 0; i < left_side.columns.size(); ++i) {
                output.add_column(std::string(left_emit_names_[i]),
                                  make_empty_like(*left_side.columns[i].column));
            }
            for (std::size_t e = 0; e < right_emit_idx_.size(); ++e) {
                output.add_column(std::string(right_emit_names_[e]),
                                  make_empty_like(*right_->columns[right_emit_idx_[e]].column));
            }
            return output;
        }

        // Gather a batch of columns in ONE fan-out. Calling `gather_column` per
        // column instead lets each call fan out its own rows, which submits and
        // waits a batch PER COLUMN — see `gather_columns_batched` for the
        // measurement that ruled that out. This is an inner join, so no index
        // carries a `kNull` sentinel and no job needs `indivisible`.
        const auto gather_batch =
            [&](std::span<const ColumnGatherJob> jobs) -> std::vector<GatheredColumn> {
            return gather_columns_batched(jobs, total, exec_, [&](std::size_t j) -> GatheredColumn {
                const auto& job = jobs[j];
                ColumnValue gathered = gather_column(*job.column, job.idx, total, nullptr);
                std::optional<ValidityBitmap> val;
                if (job.validity != nullptr) {
                    ValidityBitmap dst(total, false);
                    gather_validity_range(dst, *job.validity,
                                          std::span<const std::size_t>{job.idx, total}, 0, total);
                    val = std::move(dst);
                }
                return {std::move(gathered), std::move(val)};
            });
        };

        // li_identity: every probe row matched exactly once, so left columns
        // can be passed through directly (shared_ptr share) instead of
        // gathered. Do NOT move the underlying ColumnValue — the shared_ptr
        // may be aliased by upstream state (e.g., re-runnable source).
        const auto left_name = [&](std::size_t i, const ColumnEntry& lc) -> const std::string& {
            return i < left_emit_names_.size() ? left_emit_names_[i] : lc.name;
        };

        // The left's ordering, restated in the output's names, when this batch
        // emitted the left rows in their own order. Same rule and reasoning as
        // the materialized join in join.cpp -- a join promises no order, but a
        // path that produces one should say so, and the claim is proved from
        // the emitted index array rather than from which mode ran. Computed
        // before the identity branch below, which renames left columns in place.
        const auto carried_ordering = [&]() -> std::vector<ir::OrderKey> {
            if (!left_side.properties().ordering().has_value()) {
                return {};
            }
            if (!li_identity) {
                for (std::size_t i = 1; i < total; ++i) {
                    if (li[i] < li[i - 1]) {
                        return {};
                    }
                }
            }
            std::vector<ir::OrderKey> out;
            for (const auto& key : *left_side.properties().ordering()) {
                std::optional<std::string> emitted;
                for (std::size_t i = 0; i < left_side.columns.size(); ++i) {
                    if (left_side.columns[i].name == key.name) {
                        emitted = left_name(i, left_side.columns[i]);
                        break;
                    }
                }
                if (!emitted.has_value()) {
                    return {};  // a key the output cannot name
                }
                out.push_back(
                    ir::OrderKey{.name = std::move(*emitted), .ascending = key.ascending});
            }
            return out;
        }();
        if (li_identity && total == left_side.rows()) {
            for (std::size_t i = 0; i < left_side.columns.size(); ++i) {
                auto& lc = left_side.columns[i];
                std::string name = left_name(i, lc);
                lc.name = name;
                output.index[std::move(name)] = output.columns.size();
                output.columns.push_back(std::move(lc));
            }
        } else {
            std::vector<ColumnGatherJob> jobs;
            jobs.reserve(left_side.columns.size());
            for (const auto& lc : left_side.columns) {
                jobs.push_back({.column = lc.column.get(),
                                .validity = lc.validity.has_value() ? &*lc.validity : nullptr,
                                .idx = li,
                                .indivisible = false});
            }
            auto gathered = gather_batch(jobs);
            for (std::size_t i = 0; i < left_side.columns.size(); ++i) {
                const auto& lc = left_side.columns[i];
                if (gathered[i].second.has_value()) {
                    output.add_column(left_name(i, lc), std::move(gathered[i].first),
                                      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
                                      std::move(*gathered[i].second));
                } else {
                    output.add_column(left_name(i, lc), std::move(gathered[i].first));
                }
            }
        }

        // ri_identity: every emitted row consumes the next probe-side row
        // exactly once (two-phase deferred probe with a unique build side),
        // so probe columns are shared rather than gathered — the same
        // reasoning as li_identity above.
        const bool share_right = ri_identity && total == right_->rows();
        if (share_right) {
            for (std::size_t e = 0; e < right_emit_idx_.size(); ++e) {
                output.add_column_from(std::string(right_emit_names_[e]),
                                       right_->columns[right_emit_idx_[e]]);
            }
        } else {
            std::vector<ColumnGatherJob> jobs;
            jobs.reserve(right_emit_idx_.size());
            for (const auto index : right_emit_idx_) {
                const auto& rc = right_->columns[index];
                jobs.push_back({.column = rc.column.get(),
                                .validity = rc.validity.has_value() ? &*rc.validity : nullptr,
                                .idx = ri,
                                .indivisible = false});
            }
            auto gathered = gather_batch(jobs);
            for (std::size_t e = 0; e < right_emit_idx_.size(); ++e) {
                std::string name = right_emit_names_[e];
                if (gathered[e].second.has_value()) {
                    output.add_column(std::move(name), std::move(gathered[e].first),
                                      // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
                                      std::move(*gathered[e].second));
                } else {
                    output.add_column(std::move(name), std::move(gathered[e].first));
                }
            }
        }
        if (!carried_ordering.empty()) {
            output.set_properties(output.properties().with_ordering(carried_ordering));
        }
        return output;
    }
};

/// The streaming probe as an operator: a source of probe-side chunks, one
/// completed build, and nothing else.
///
/// This is Phase 4's `HashProbe`. It owns no build -- it reads one through
/// `JoinProbe`'s `shared_ptr<const>` handles -- and it does not know which
/// side of the join was hashed, because by the time it exists that is settled.
/// Two things follow, and they are the reason it is a type rather than a loop
/// inside the join: it can be constructed next to a build it did not run, and
/// several of it can read one build at once, which is what a per-worker morsel
/// chain needs.
///
/// The empty-schema carrier travels with it, because "this join produced no
/// rows but still has a schema" is a property of probing, not of the operator
/// that decided the orientation.
class JoinProbeOperator final : public Operator {
   public:
    /// `preserve_empty_morsels` is the same contract every map kernel in a
    /// morsel chain honours: one input morsel yields exactly one identified
    /// output morsel, because the ordered ring indexes by sequence and a
    /// coalesced empty result would be a lost slot rather than a smaller
    /// answer. A probe needs it more than a filter does -- a morsel of
    /// probe-side rows that matches nothing is entirely ordinary.
    JoinProbeOperator(OperatorPtr child, JoinProbe probe, bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          probe_(std::move(probe)),
          preserve_empty_(preserve_empty_morsels) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                if (!emitted_nonempty_ && empty_schema_.has_value()) {
                    auto schema = std::move(*empty_schema_);
                    empty_schema_.reset();
                    return std::optional<Chunk>{table_to_chunk(std::move(schema))};
                }
                return std::optional<Chunk>{};
            }
            Chunk input = std::move(*chunk_res.value());
            // A morsel's identity travels with it. `sequence` and `row_offset`
            // identify which morsel this is, not which rows it holds, so a
            // probe propagates them unchanged exactly as a filter does --
            // both change the row count, and neither changes which morsel it
            // is answering for. The ordered ring rejects a chunk that arrives
            // without them.
            const std::uint64_t sequence = input.sequence;
            const std::size_t row_offset = input.row_offset;
            auto out = probe_.probe_chunk_against_right(chunk_to_table(std::move(input)));
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0 && !preserve_empty_) {
                // Keep the planned empty table as a schema carrier. A join
                // with no matches still has its left and right output columns;
                // without this, a materializing sink sees no chunks at all.
                empty_schema_ = std::move(*out);
                continue;
            }
            emitted_nonempty_ = true;
            Chunk result = table_to_chunk(std::move(*out));
            result.sequence = sequence;
            result.row_offset = row_offset;
            return std::optional<Chunk>{std::move(result)};
        }
    }

   private:
    OperatorPtr child_;
    JoinProbe probe_;
    bool preserve_empty_ = false;
    std::optional<Table> empty_schema_;
    bool emitted_nonempty_ = false;
};

/// The runtime value carried by the physical HashBuild -> HashProbe edge.
/// Orientation is represented by the variant alternative, so HashProbe never
/// re-decides which side was indexed.
struct StreamingHashProbeInput {
    OperatorPtr source;
    std::optional<Table> materialized_source;
    JoinProbe probe;
};

struct SwappedHashProbeInput {
    Table left;
    JoinProbe probe;
};

struct PrecomputedHashProbeInput {
    Table output;
};

using HashProbeInput =
    std::variant<StreamingHashProbeInput, SwappedHashProbeInput, PrecomputedHashProbeInput>;

/// How many workers a probe over an already-materialized probe side may fan
/// out to, or 0 to decline. Defined with the morsel machinery below.
[[nodiscard]] auto probe_morsel_workers(const Table& input, const ExecutionContext& exec)
    -> std::size_t;

/// Run a probe over the morsels of an already-materialized probe side: one
/// `JoinProbeOperator` per worker, all reading the same build, results merged
/// in morsel order. Defined below, next to the morsel executor it uses.
[[nodiscard]] auto build_probe_morsel_pipeline(Table input, const JoinProbe& probe,
                                               std::size_t workers, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string>;

/// Inner hash join for single-key no-predicate joins.
///
/// Two execution modes:
/// - Stream: right is small (<= kStreamRightThreshold). Build a chained
///   hash index on the materialized right, then probe each left chunk
///   streamed from the child. Matches the classic star-join shape.
/// - Swapped: right is large and n_left < n_right. Materialize left,
///   build the hash index on left, iterate right rows once and emit output
///   in that same right-scan (probe) order (baseline's
///   `build_indices_from_right_scan` equivalent) — row order is outside the
///   join contract (SPEC.md §5.6), and preserving the probe side's scan order
///   instead of reassembling by left row keeps cache locality for any
///   downstream join that probes this join's output. Much better cache
///   behavior overall when the smaller side fits.
///
/// Name conflicts are resolved with the same `_right` suffix rule as
/// `join_table_impl`.
class ChunkedInnerJoinOperator final : public Operator {
   public:
    ChunkedInnerJoinOperator(OperatorPtr left, Table right, const std::vector<ir::JoinKey>* keys,
                             const ExecutionContext& exec, ir::JoinSuffixPolicy suffix = {},
                             const std::vector<ir::OrderKey>* pending_order = nullptr,
                             physical::JoinParallelism par = {},
                             std::optional<ir::JoinColumnMapping> columns = std::nullopt)
        : left_(std::move(left)),
          right_(std::make_shared<Table>(std::move(right))),
          keys_(keys),
          par_(par),
          pending_order_(pending_order) {
        bind_probe(keys, std::move(suffix), exec, std::move(columns));
    }

    /// Deferred-probe variant: the right side is an undecoded lazy scan (plus
    /// its Project/Rename wrappers), interpreted only after this join has
    /// published build-side key bounds into the scan's filter slot. The
    /// registry/scalars/externs pointers are the interpret context and outlive
    /// the operator.
    ChunkedInnerJoinOperator(OperatorPtr left, const ir::Node* right_node,
                             const TableRegistry* registry, const ScalarRegistry* scalars,
                             const ExternRegistry* externs, const ExecutionContext& exec,
                             const std::vector<ir::JoinKey>* keys, const DeferredScan* probe,
                             std::string probe_name, ir::JoinSuffixPolicy suffix = {},
                             const std::vector<ir::OrderKey>* pending_order = nullptr,
                             physical::JoinParallelism par = {},
                             std::optional<ir::JoinColumnMapping> columns = std::nullopt)
        : left_(std::move(left)),
          keys_(keys),
          deferred_probe_(probe),
          deferred_probe_name_(std::move(probe_name)),
          deferred_right_node_(right_node),
          deferred_registry_(registry),
          deferred_scalars_(scalars),
          deferred_externs_(externs),
          deferred_exec_(&exec),
          par_(par),
          pending_order_(pending_order) {
        bind_probe(keys, std::move(suffix), exec, std::move(columns));
    }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (auto err = run_build()) {
            return std::unexpected(std::move(*err));
        }

        if (mode_ == Mode::Precomputed) {
            if (swapped_emitted_) {
                return std::optional<Chunk>{};
            }
            swapped_emitted_ = true;
            if (precomputed_output_.rows() == 0) {
                return std::optional<Chunk>{};
            }
            return std::optional<Chunk>{table_to_chunk(std::move(precomputed_output_))};
        }

        if (mode_ == Mode::Swapped) {
            if (swapped_emitted_) {
                return std::optional<Chunk>{};
            }
            swapped_emitted_ = true;
            if (!left_table_.has_value()) {
                return std::unexpected(
                    "ChunkedInnerJoinOperator: swapped mode without a materialized left table");
            }
            auto out = probe_.emit_swapped(*left_table_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                return std::optional<Chunk>{};
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*out))};
        }

        // Stream mode is the probe, and the probe is its own operator. The
        // join constructs it on first use and delegates from here on:
        // everything left in this class is build-side.
        if (auto err = ensure_probe_op()) {
            return std::unexpected(std::move(*err));
        }
        return probe_op_->next();
    }

    /// Run this join's build phase to completion.
    ///
    /// The build is a phase with an explicit caller now, not a side effect of
    /// whoever happens to pull the first chunk. `build_physical_join` runs it
    /// at plan-execution time; `next()` still calls it, because a join reached
    /// by any other path must work and because idempotence is what makes both
    /// callers safe. After it returns the index is immutable and the probe
    /// side can stream through it -- that is the barrier, stated as a call
    /// rather than as a comment about `initialized_`.
    ///
    /// Deliberately NOT what this changes: it does not overlap the build with
    /// anything. Overlapping a join's two sides has been tried twice and
    /// regressed both times (`32889afd`, `27cb4a27`); this makes the build
    /// schedulable, and what to schedule it against stays an open, measured
    /// question.
    auto run_build() -> std::optional<std::string> {
        if (initialized_) {
            return std::nullopt;
        }
        if (auto err = initialize()) {
            return err;
        }
        initialized_ = true;
        return std::nullopt;
    }

    /// Move the completed HashBuild result across the physical edge. This is
    /// deliberately unavailable before `run_build`: HashProbe receives a
    /// runtime-oriented value, not the mutable coordinator that produced it.
    [[nodiscard]] auto take_hash_probe_input() -> std::expected<HashProbeInput, std::string> {
        if (!initialized_) {
            return std::unexpected("physical HashBuild output requested before the build ran");
        }
        if (mode_ == Mode::Precomputed) {
            return HashProbeInput{
                PrecomputedHashProbeInput{.output = std::move(precomputed_output_)}};
        }
        if (mode_ == Mode::Swapped) {
            if (!left_table_.has_value()) {
                return std::unexpected(
                    "ChunkedInnerJoinOperator: swapped mode without a materialized left table");
            }
            SwappedHashProbeInput input{.left = std::move(*left_table_),
                                        .probe = std::move(probe_)};
            left_table_.reset();
            return HashProbeInput{std::move(input)};
        }

        StreamingHashProbeInput input{.source = std::move(left_),
                                      .materialized_source = std::move(probe_side_),
                                      .probe = std::move(probe_)};
        probe_side_.reset();
        if (!input.materialized_source.has_value() && input.source == nullptr) {
            return std::unexpected("physical HashProbe has no probe-side source");
        }
        return HashProbeInput{std::move(input)};
    }

   private:
    enum class Mode : std::uint8_t { Stream, Swapped, Precomputed };

    /// The chain terminator, shared with the index this operator probes.
    static constexpr std::size_t kNil = kJoinNil;

    // Build-on-right is preferred when right is small enough that probing
    // it from streaming left chunks is cache-friendly. Above this, we
    // materialize left to pick the smaller build side.
    static constexpr std::size_t kStreamRightThreshold = 65536;

    /// Hand the probe what it needs before anything runs, including joint
    /// ownership of the right side. The `shared_ptr`'s identity is stable from
    /// construction even where the table it points at is filled later (the
    /// deferred path fills it once the scan resolves), so this binds once and
    /// the build phase writes through it.
    void bind_probe(const std::vector<ir::JoinKey>* keys, ir::JoinSuffixPolicy suffix,
                    const ExecutionContext& exec, std::optional<ir::JoinColumnMapping> columns) {
        probe_.keys_ = keys;
        probe_.suffix_ = std::move(suffix);
        probe_.exec_ = &exec;
        probe_.right_ = right_;
        probe_.probe_plan_ = par_.probe;
        probe_.columns_ = std::move(columns);
    }

    auto initialize() -> std::optional<std::string> {
        if (keys_->size() == 2) {
            return initialize_pair();
        }
        if (keys_->size() != 1) {
            return "ChunkedInnerJoinOperator only supports single-key or two-Int64-key joins";
        }
        if (deferred_probe_ != nullptr) {
            if (auto err = resolve_deferred_probe()) {
                return err;
            }
            if (mode_ == Mode::Precomputed) {
                return std::nullopt;
            }
        }
        const std::string& left_key_name = keys_->front().left;
        const std::string& right_key_name = keys_->front().right;
        const ColumnValue* rkey = right_->find(right_key_name);
        if (rkey == nullptr) {
            return "join key not found in right table: " + right_key_name;
        }
        ExprType key_kind = ExprType::Int;
        if (auto err = detect_join_key_kind(*rkey, key_kind)) {
            return err;
        }

        const std::size_t n_right = right_->rows();

        // Small right: index it without ever measuring the left, which is the
        // one orientation this join can choose without draining a child.
        if (n_right <= kStreamRightThreshold) {
            return adopt_build(build_join_side(*right_, right_key_name, key_kind,
                                               JoinOrientation::BuildRight,
                                               build_partitions(n_right)));
        }

        Table left_table;
        if (use_materialized_left_ && left_materialized_.has_value()) {
            // The deferred-probe path already drained the left child.
            left_table = std::move(*left_materialized_);
            left_materialized_.reset();
            use_materialized_left_ = false;
        } else {
            auto left_res = MaterializeOperator(std::move(left_)).run();
            if (!left_res.has_value()) {
                return std::move(left_res.error());
            }
            left_table = std::move(*left_res);
        }
        const std::size_t n_left = left_table.rows();

        // Evaluated only when swapping was otherwise preferred: it is not a
        // pure predicate (it can set up the probe's right-emit schema), so
        // asking it unconditionally would move work the short-circuit used to
        // skip.
        const bool order_pays =
            n_left < n_right && order_preserving_pays(left_table, n_left, n_right);
        auto outcome = choose_and_build_single_key(left_table, *right_, left_key_name,
                                                   right_key_name, key_kind, order_pays,
                                                   build_partitions(std::min(n_left, n_right)));
        return adopt_build(std::move(outcome), std::move(left_table));
    }

    /// How many partitions this join's hash build may fill concurrently, or 1
    /// to build it serially.
    ///
    /// The floor (`1U << 17U`) and the worker cap (`min(compute_budget, pool,
    /// 64)`) used to be computed here; they are now the `hash-build` phase of
    /// the physical plan (`physical::join_hash_build_parallelism`), resolved by
    /// the builder and read from `par_.build`. src/runtime/PARALLELISM.md,
    /// "Target: parallelism as a plan decision". The conditions that stay here
    /// are the ones only the operator can judge: the `IBEX_JOIN_BUILD_SERIAL`
    /// kill switch, whether it is nested under another fan-out
    /// (`on_worker_pool_thread` -- no nested pool submissions), and whether
    /// *this* build side cleared the floor (its row count is not known until
    /// the side is materialized). The floor is higher than the probe's because
    /// a partitioned build makes three passes over the keys where a serial
    /// build makes one.
    [[nodiscard]] auto build_partitions(std::size_t n) const -> std::size_t {
        // Kill switch, and the A/B handle: with it set, the same binary runs
        // the serial build, so the two can be interleaved without rebuilding.
        if (std::getenv("IBEX_JOIN_BUILD_SERIAL") != nullptr) {
            return 1;
        }
        if (par_.build.decline != physical::FanOutDecline::None || par_.build.worker_cap < 2 ||
            on_worker_pool_thread() || n < par_.build.row_floor) {
            return 1;
        }
        // Telemetry only (see `parallel_hash_builds`): the fan-out is byte-identical
        // to the serial build, so a gate that silently stopped matching would lose
        // the parallelism with every test still green. Counted here rather than at
        // the four call sites, which all route the result straight into the build.
        if (probe_.exec_ != nullptr && probe_.exec_->parallel_stats != nullptr) {
            probe_.exec_->parallel_stats->parallel_hash_builds.fetch_add(1,
                                                                         std::memory_order_relaxed);
        }
        return par_.build.worker_cap;
    }

    /// Publish what a build produced, and hand back the orientation it chose.
    /// The single place `probe_.index_` is written: `probe_.index()` gives the
    /// probe a `const` reference, so the barrier stays a compile error to
    /// cross rather than a convention.
    auto publish_build(std::expected<JoinBuildOutcome, std::string> outcome)
        -> std::expected<JoinOrientation, std::string> {
        if (!outcome.has_value()) {
            return std::unexpected(std::move(outcome.error()));
        }
        probe_.index_ = std::move(outcome->index);
        return outcome->orientation;
    }

    /// Apply what a build phase decided: publish the index and put this
    /// operator into the shape that orientation implies. The build returns a
    /// value; every member write that follows from it happens here and
    /// nowhere else.
    ///
    /// `left_table` is the materialized left when the decision needed one --
    /// it becomes either the scanned-once swapped side or the single chunk
    /// the stream path replays, depending on which way the build went.
    auto adopt_build(std::expected<JoinBuildOutcome, std::string> outcome,
                     std::optional<Table> left_table = std::nullopt) -> std::optional<std::string> {
        auto orientation = publish_build(std::move(outcome));
        if (!orientation.has_value()) {
            return std::move(orientation.error());
        }
        if (*orientation == JoinOrientation::BuildLeft) {
            left_table_ = std::move(left_table);
            mode_ = Mode::Swapped;
            return std::nullopt;
        }
        // BuildRight: the other side streams through the index, which is what
        // `JoinProbeOperator` does. A left that has already been drained --
        // either by the orientation decision above, or earlier by the
        // deferred-probe path publishing its key bounds -- is replayed as one
        // chunk rather than re-wrapped in its original operator, exactly as
        // `use_materialized_left_` used to do inline.
        //
        // The second case is not hypothetical and cost a segfault to find:
        // the deferred path drains `left_` to publish a filter, and if the
        // resolved right then lands under `kStreamRightThreshold` the fast
        // path arrives here with no `left_table` and a moved-from `left_`.
        std::optional<Table> materialized_probe_side;
        if (left_table.has_value()) {
            materialized_probe_side = std::move(*left_table);
        } else if (use_materialized_left_ && left_materialized_.has_value()) {
            materialized_probe_side = std::move(*left_materialized_);
            left_materialized_.reset();
            use_materialized_left_ = false;
        }

        // The probe side is kept rather than consumed. Building the probe
        // operator here would settle a question a caller above may want to
        // answer differently: a map pipeline over this join can take the probe
        // and run it at the head of its own worker chains, which is the Umbra
        // shape -- one build pipeline, then a probe pipeline whose maps run in
        // the same worker as the probe. `take_fusible_probe` is that handoff,
        // and it has to happen before `ensure_probe_op` commits.
        probe_side_ = std::move(materialized_probe_side);
        if (!probe_side_.has_value() && left_ == nullptr) {
            // Every way of losing the probe side is a bug in the branches
            // above, and one of them was. Aborting with a name beats
            // dereferencing null inside a pool thread, which is what the
            // deferred fast-path case actually did.
            invariant_violation("join probe: no probe-side source after the build phase");
        }
        return std::nullopt;
    }

    /// Build Stream mode's probe operator, once, on first use.
    ///
    /// Split from `adopt_build` so a caller that wants to fuse this probe into
    /// its own pipeline has a window in which the decision is still open --
    /// see `take_fusible_probe`.
    auto ensure_probe_op() -> std::optional<std::string> {
        if (probe_op_ != nullptr) {
            return std::nullopt;
        }
        // A materialized probe side can be morselized: one probe per worker,
        // each over its own morsel, all reading this one immutable build.
        // Opt-in, and `probe_morsel_workers` records the measurement that
        // explains why.
        if (probe_side_.has_value() && probe_.exec_ != nullptr) {
            if (const std::size_t workers = probe_morsel_workers(*probe_side_, *probe_.exec_);
                workers >= 2) {
                auto pipeline = build_probe_morsel_pipeline(std::move(*probe_side_), probe_,
                                                            workers, *probe_.exec_);
                if (!pipeline.has_value()) {
                    return std::move(pipeline.error());
                }
                probe_side_.reset();
                probe_op_ = std::move(*pipeline);
                return std::nullopt;
            }
        }
        OperatorPtr probe_source =
            probe_side_.has_value() ? make_table_source(std::move(*probe_side_)) : std::move(left_);
        probe_side_.reset();
        probe_op_ = std::make_unique<JoinProbeOperator>(std::move(probe_source), std::move(probe_));
        return std::nullopt;
    }

   public:
    /// One build, one probe side, ready to be run by someone else.
    struct FusibleProbe {
        Table probe_side;
        JoinProbe probe;
    };

    /// Hand the probe and its input to a caller that will run them itself.
    /// Empty unless this join settled on `BuildRight` and nothing has pulled
    /// from it yet: a swapped or precomputed join emits one table and has no
    /// probe pipeline to give.
    ///
    /// A probe side that is still streaming gets materialized here, which
    /// sounds like a cost added and is not: the caller is a parallel map
    /// pipeline, which was going to materialize the join's OUTPUT and
    /// morselize that. This materializes the probe side instead, and the
    /// join's output is then never assembled at all -- it is produced a morsel
    /// at a time inside the workers. Which of the two tables is larger is a
    /// real question and a measured one; it is not a question of whether a
    /// materialization exists.
    ///
    /// The operator is left without a probe and must be discarded.
    [[nodiscard]] auto take_fusible_probe()
        -> std::expected<std::optional<FusibleProbe>, std::string> {
        if (mode_ != Mode::Stream || probe_op_ != nullptr) {
            return std::optional<FusibleProbe>{};
        }
        if (!probe_side_.has_value()) {
            if (left_ == nullptr) {
                return std::optional<FusibleProbe>{};
            }
            auto drained = MaterializeOperator(std::move(left_)).run();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
            probe_side_ = std::move(*drained);
        }
        FusibleProbe out{.probe_side = std::move(*probe_side_), .probe = std::move(probe_)};
        probe_side_.reset();
        return std::optional<FusibleProbe>{std::move(out)};
    }

   private:
    /// Two-fixed-width-int-key path: narrow first cut of the streaming
    /// two-key join (plans/parallelism-overview.md's "stream multi-key
    /// joins" item). Non-deferred case: `right_` is already a whole `Table`
    /// by construction (the call site materializes it, same as the
    /// single-key path), so the only real decision left is which side to
    /// index: this materializes `left_` too and builds on whichever side is
    /// smaller -- the same motivation as `initialize()`'s single-key swap
    /// decision, and necessary here because the call site cannot know in
    /// advance which side a two-key join chain puts on which name (TPC-H
    /// q09's `lineitem` join has the multi-million-row side as `right_`;
    /// indexing it unconditionally was measured a >2x regression before this
    /// fix). Deferred case: see `resolve_deferred_probe_pair`.
    auto initialize_pair() -> std::optional<std::string> {
        if (deferred_probe_ != nullptr) {
            return resolve_deferred_probe_pair();
        }
        const ir::JoinKey& k0 = keys_->at(0);
        const ir::JoinKey& k1 = keys_->at(1);
        // Checked before the left child is drained, so an unusable right key
        // still costs nothing to report.
        if (auto right_keys = pair_key_columns(*right_, k0.right, k1.right, "right");
            !right_keys.has_value()) {
            return std::move(right_keys.error());
        }
        Table left_table;
        if (use_materialized_left_ && left_materialized_.has_value()) {
            // `resolve_deferred_probe_pair` already drained the left child
            // while deciding whether a scan filter was worth publishing.
            left_table = std::move(*left_materialized_);
            left_materialized_.reset();
            use_materialized_left_ = false;
        } else {
            auto left_res = MaterializeOperator(std::move(left_)).run();
            if (!left_res.has_value()) {
                return std::move(left_res.error());
            }
            left_table = std::move(*left_res);
        }
        probe_.pair_mode_ = true;
        // On the BuildRight side of this decision the left is already fully
        // materialized, so `adopt_build` drains it as a single chunk through
        // the existing `use_materialized_left_` mechanism rather than
        // re-wrapping it in an operator.
        auto outcome =
            choose_and_build_pair(left_table, *right_, k0, k1,
                                  build_partitions(std::min(left_table.rows(), right_->rows())));
        return adopt_build(std::move(outcome), std::move(left_table));
    }

    /// Deferred-probe POC for the two-key path (plans/parallelism-overview.md
    /// "deferred scan filtering for two-key joins", TPC-H q09's `lineitem`
    /// join). Reuses the existing single-key deferred-scan machinery
    /// unchanged: builds the (small) left side first, publishes a
    /// `DynamicScanFilter` over `keys_->at(0)` ONLY -- one component, not
    /// both -- into the scan's filter slot, then lets the source's normal
    /// decode-time pruning narrow the right side before it is ever
    /// materialized. Membership in one component is necessary but not
    /// sufficient for the pair match, so this can only produce harmless
    /// false positives (rows sharing q09's l_partkey but not l_suppkey);
    /// `choose_and_build_pair`'s exact pair probe afterward is what
    /// actually enforces both keys, unchanged from the non-deferred path.
    ///
    /// Same `kStreamRightThreshold` gate as the single-key
    /// `resolve_deferred_probe`: below it, publishing a filter (a Bloom plus
    /// a sort/unique pass over the whole build side) can only add cost, not
    /// recover it, since the deferred side was never going to be expensive to
    /// decode in the first place. First cut of this POC always materialized
    /// left and published a filter regardless of size -- measured a clean,
    /// unanimous +8.4% regression on q05 (its `join supplier on
    /// {l_suppkey=s_suppkey, n_nationkey=s_nationkey}` is exactly this
    /// shape, `supplier` fitting in one row group with nothing to prune).
    /// Below the threshold this falls through to `initialize_pair`'s
    /// ordinary side-picking, reusing the already-drained left side via
    /// `left_materialized_`/`use_materialized_left_` rather than draining it
    /// twice.
    ///
    /// No two-phase probe (`try_two_phase_probe`'s candidate-selection
    /// optimization) here -- that is a further, separable lever on top of
    /// scan-altitude pruning, and this POC is scoped to answering whether
    /// pruning the scan itself is worth it at all before adding more on top.
    auto resolve_deferred_probe_pair() -> std::optional<std::string> {
        DynamicScanFilter& slot = *deferred_probe_->filter;
        if (deferred_probe_->lazy->rows() > kStreamRightThreshold) {
            auto left_res = MaterializeOperator(std::move(left_)).run();
            if (!left_res.has_value()) {
                return std::move(left_res.error());
            }
            publish_build_filter_column(*left_res, keys_->at(0).left, slot);
            left_materialized_ = std::move(*left_res);
            use_materialized_left_ = true;
        }
        slot.ready = true;
        auto right = interpret_node(*deferred_right_node_, *deferred_registry_, deferred_scalars_,
                                    deferred_externs_, *deferred_exec_);
        if (!right.has_value()) {
            return std::move(right.error());
        }
        *right_ = std::move(*right);
        deferred_probe_ = nullptr;
        if (std::getenv("IBEX_DEBUG_PAIR_DEFER") != nullptr) {
            ibex::formatting::print(stderr,
                                    "[pair_defer] filter_published={} right_rows_after_filter={}\n",
                                    static_cast<int>(use_materialized_left_), right_->rows());
        }
        return initialize_pair();
    }

    /// The probe side is an undecoded lazy scan. When it is worth it, drain
    /// the build (left) side first and publish its key filter (membership +
    /// bounds) into the scan's filter slot, so the scan skips materializing
    /// rows that cannot match. Every path marks the slot ready before the
    /// scan is interpreted; the filter is an optimization the slot may
    /// simply not carry.
    auto resolve_deferred_probe() -> std::optional<std::string> {
        DynamicScanFilter& slot = *deferred_probe_->filter;
        // Pre-filter row count: an upper bound on the decoded size, good
        // enough to decide whether the probe side is large enough to bother.
        if (deferred_probe_->lazy->rows() > kStreamRightThreshold) {
            auto left_res = MaterializeOperator(std::move(left_)).run();
            if (!left_res.has_value()) {
                return std::move(left_res.error());
            }
            publish_build_filter(*left_res, slot);
            left_materialized_ = std::move(*left_res);
            use_materialized_left_ = true;
        }
        slot.ready = true;
        if (use_materialized_left_) {
            TwoPhase outcome = TwoPhase::NotApplicable;
            if (auto err = try_two_phase_probe(slot, outcome)) {
                return err;
            }
            if (outcome == TwoPhase::Precomputed) {
                deferred_probe_ = nullptr;
                mode_ = Mode::Precomputed;
                return std::nullopt;
            }
            if (outcome == TwoPhase::RightMaterialized) {
                // Phase A ran but full two-phase declined; its selection was
                // reused to materialize right_, so fall through to the
                // ordinary side-picking in initialize().
                deferred_probe_ = nullptr;
                return std::nullopt;
            }
        }
        auto right = interpret_node(*deferred_right_node_, *deferred_registry_, deferred_scalars_,
                                    deferred_externs_, *deferred_exec_);
        if (!right.has_value()) {
            return std::move(right.error());
        }
        *right_ = std::move(right.value());
        deferred_probe_ = nullptr;
        return std::nullopt;
    }

    enum class TwoPhase : std::uint8_t { NotApplicable, RightMaterialized, Precomputed };

    /// Interpret the Project/Rename/Update wrappers over an already
    /// materialized scan table by shadowing the scan name in a registry
    /// copy — the Scan case hits the registry before the deferred fallback.
    auto interpret_wrapped_right(Table scan_table) -> std::optional<std::string> {
        TableRegistry local = *deferred_registry_;
        local.insert_or_assign(deferred_probe_name_, std::move(scan_table));
        auto right = interpret_node(*deferred_right_node_, local, deferred_scalars_,
                                    deferred_externs_, *deferred_exec_);
        if (!right.has_value()) {
            return std::move(right.error());
        }
        *right_ = std::move(right.value());
        return std::nullopt;
    }

    /// Late materialization across the join (decode-fusion stage 5): probe
    /// with just the scan's key column, then decode the payload columns only
    /// for the rows that actually matched. When every survivor matched
    /// exactly one build row (unique build keys — the common star shape),
    /// the probe-side columns pass into the output without a gather.
    ///
    /// NotApplicable (nothing ran — no membership filter, or phase A had no
    /// selective answer): the caller interprets the subtree as before. When
    /// phase A DID run but full two-phase declines — the build side is
    /// larger than the candidate set (two-phase forces build-on-left; the
    /// ordinary side-picking may do better) or a key type surprise — its
    /// selection is reused to materialize `right_` (RightMaterialized)
    /// rather than thrown away: recomputing it from scratch was measured at
    /// +12% on q03.
    auto try_two_phase_probe(const DynamicScanFilter& slot, TwoPhase& outcome)
        -> std::optional<std::string> {
        outcome = TwoPhase::NotApplicable;
        if (!slot.has_membership() || !left_materialized_.has_value()) {
            return std::nullopt;
        }
        const Table& build = *left_materialized_;
        const auto* build_entry = build.find_entry(keys_->front().left);
        if (build_entry == nullptr ||
            !std::holds_alternative<Column<std::int64_t>>(*build_entry->column)) {
            return std::nullopt;
        }

        auto phase = deferred_scan_key_selection(*deferred_probe_, *deferred_exec_);
        if (!phase.has_value()) {
            return std::move(phase.error());
        }
        if (!phase->has_value()) {
            return std::nullopt;
        }
        auto sel = std::move(**phase);
        const auto* keys_col = std::get_if<Column<std::int64_t>>(&*sel.keys.column);
        if (build.rows() > sel.selected.size() || keys_col == nullptr) {
            auto right_rows = materialize_deferred_scan_rows(*deferred_probe_, sel.selected,
                                                             *deferred_exec_, std::move(sel.keys));
            if (!right_rows.has_value()) {
                return std::move(right_rows.error());
            }
            if (auto err = interpret_wrapped_right(std::move(*right_rows))) {
                return err;
            }
            outcome = TwoPhase::RightMaterialized;
            return std::nullopt;
        }

        // Publish only: this path builds on the left and scans the right, but
        // it emits one precomputed table rather than running either streaming
        // shape, so it takes no operator mode from the orientation.
        if (auto published = publish_build(
                build_join_side(build, keys_->front().left, ExprType::Int,
                                JoinOrientation::BuildLeft, build_partitions(build.rows())));
            !published.has_value()) {
            return std::move(published.error());
        }

        // Probe the candidate keys in scan order; record one hit per
        // surviving row plus the expanded (build row, survivor) pairs — the
        // same probe-order-major layout emit_swapped produces.
        const auto* key_data = keys_col->data();
        const ValidityBitmap* key_validity =
            sel.keys.validity.has_value() ? &*sel.keys.validity : nullptr;
        const std::size_t n = keys_col->size();

        // Same scan/replay shape as `probe_swapped`, with a twist: `ri` here
        // indexes HITS (the survivor list), not probe rows, so each part
        // needs two prefix offsets — its first hit index and its first output
        // pair — before the replays can write disjoint slices. One part when
        // the gate declines, so the serial path is the same code.
        const auto scan = [&](std::size_t begin, std::size_t end,
                              std::vector<JoinProbe::SwappedHit>& hits, std::size_t& total) {
            for (std::size_t i = begin; i < end; ++i) {
                if (key_validity != nullptr && !(*key_validity)[i]) {
                    continue;
                }
                const std::size_t head = probe_.index().i64_heads.find_head(key_data[i]);
                if (head == kNil) {
                    continue;
                }
                hits.push_back(JoinProbe::SwappedHit{.rrow = i, .head = head});
                for (std::size_t cur = head; cur != kNil; cur = probe_.index().chain_next[cur]) {
                    ++total;
                }
            }
        };
        const std::size_t workers = probe_.probe_parallel_workers(n);
        if (workers == 0) {
            probe_.swapped_parts_.resize(1);
            probe_.swapped_parts_[0].hits.clear();
            probe_.swapped_parts_[0].total = 0;
            scan(0, n, probe_.swapped_parts_[0].hits, probe_.swapped_parts_[0].total);
        } else {
            auto& pool = process_worker_pool();
            const std::size_t grain = (n + workers - 1) / workers;
            probe_.swapped_parts_.resize(workers);
            auto batch = pool.submit(workers, [&](std::size_t w) {
                auto& part = probe_.swapped_parts_[w];
                part.hits.clear();
                part.total = 0;
                const std::size_t begin = w * grain;
                const std::size_t end = std::min(n, begin + grain);
                if (begin < end) {
                    scan(begin, end, part.hits, part.total);
                }
            });
            batch.wait();
        }
        const std::size_t n_parts = probe_.swapped_parts_.size();
        std::vector<std::size_t> hit_offsets(n_parts);
        probe_.part_offsets_.resize(n_parts);
        std::size_t n_hits = 0;
        std::size_t total = 0;
        for (std::size_t w = 0; w < n_parts; ++w) {
            hit_offsets[w] = n_hits;
            probe_.part_offsets_[w] = total;
            n_hits += probe_.swapped_parts_[w].hits.size();
            total += probe_.swapped_parts_[w].total;
        }

        Selection survivors(n_hits);
        std::vector<std::size_t> li(total, 0);
        std::vector<std::size_t> ri(total, 0);
        Column<std::int64_t> gathered_keys;
        const bool gather_keys = n_hits != n;
        if (gather_keys) {
            gathered_keys.resize_for_overwrite(n_hits);
        }
        // Detach once here, not per element inside the replay: the mutable
        // `operator[]` pays a CoW check every call, and on a worker the
        // detach itself would race.
        std::int64_t* gathered_out = gather_keys ? gathered_keys.data() : nullptr;
        const auto replay = [&](std::size_t w) {
            const auto& part = probe_.swapped_parts_[w];
            std::size_t h = hit_offsets[w];
            std::size_t pos = probe_.part_offsets_[w];
            for (const JoinProbe::SwappedHit& hit : part.hits) {
                survivors[h] = sel.selected[hit.rrow];
                if (gathered_out != nullptr) {
                    gathered_out[h] = key_data[hit.rrow];
                }
                for (std::size_t cur = hit.head; cur != kNil;
                     cur = probe_.index().chain_next[cur]) {
                    li[pos] = cur;
                    ri[pos] = h;
                    ++pos;
                }
                ++h;
            }
        };
        if (workers == 0) {
            replay(0);
        } else {
            auto batch = process_worker_pool().submit(n_parts, replay);
            batch.wait();
            if (deferred_exec_->parallel_stats != nullptr) {
                deferred_exec_->parallel_stats->parallel_probes.fetch_add(
                    1, std::memory_order_relaxed);
            }
        }
        const bool ri_identity = total == n_hits;

        // Survivors' key values, gathered in memory from phase A's keys.
        ColumnEntry key_entry;
        key_entry.name = sel.keys.name;
        if (!gather_keys) {
            key_entry.column = sel.keys.column;
            key_entry.validity = sel.keys.validity;
        } else {
            key_entry.column = std::make_shared<ColumnValue>(std::move(gathered_keys));
            // Null keys never match, so every survivor's key is valid.
        }

        auto right_rows = materialize_deferred_scan_rows(*deferred_probe_, survivors,
                                                         *deferred_exec_, std::move(key_entry));
        if (!right_rows.has_value()) {
            return std::move(right_rows.error());
        }
        if (auto err = interpret_wrapped_right(std::move(*right_rows))) {
            return err;
        }

        Table left_copy;
        left_copy.columns.reserve(build.columns.size());
        for (const auto& c : build.columns) {
            left_copy.add_column(c.name, *c.column);
            left_copy.columns.back().validity = c.validity;
        }
        auto out = probe_.assemble_output(std::move(left_copy), li.data(), ri.data(), total,
                                          /*li_identity=*/false, ri_identity);
        if (!out.has_value()) {
            return std::move(out.error());
        }
        precomputed_output_ = std::move(*out);
        outcome = TwoPhase::Precomputed;
        return std::nullopt;
    }

    // Derive the probe scan's dynamic filter from the build side's valid key
    // values (int keys only; other key types publish nothing). Sound for any
    // inner join regardless of which side ends up as the build: a probe row
    // whose key fails the filter cannot match.
    //
    // Everything here is published ungated — membership because a range
    // estimate cannot predict set selectivity (the scan decides with a
    // sampled pass rate), and min/max because the consumer owns the policy:
    // materialize_deferred_scan gates conjunct synthesis on estimated
    // pruning, the fused key scan uses the raw bounds for row-group
    // skipping.
    void publish_build_filter(const Table& build, DynamicScanFilter& slot) const {
        publish_build_filter_column(build, keys_->front().left, slot);
    }

    // Component-selecting variant for the two-key deferred-probe POC
    // (`resolve_deferred_probe_pair`): publishes a filter over exactly one
    // named build-side column instead of always `keys_->front().left`, since
    // the pair join's scan filter only ever covers one of the two keys.
    static void publish_build_filter_column(const Table& build, const std::string& key_name,
                                            DynamicScanFilter& slot) {
        const auto* entry = build.find_entry(key_name);
        if (entry == nullptr) {
            return;
        }
        const auto* col = std::get_if<Column<std::int64_t>>(&*entry->column);
        if (col == nullptr || col->empty()) {
            return;
        }
        const ValidityBitmap* validity = entry->validity.has_value() ? &*entry->validity : nullptr;
        const auto* data = col->data();
        const std::size_t n = col->size();
        std::int64_t mn = std::numeric_limits<std::int64_t>::max();
        std::int64_t mx = std::numeric_limits<std::int64_t>::min();
        std::size_t valid_rows = 0;
        for (std::size_t r = 0; r < n; ++r) {
            if (validity != nullptr && !(*validity)[r]) {
                continue;
            }
            mn = std::min(mn, data[r]);
            mx = std::max(mx, data[r]);
            ++valid_rows;
        }
        if (valid_rows == 0) {
            return;
        }

        // Every build side gets a Bloom — even alongside an exact list, the
        // Bloom is the probe fast path (see DynamicScanFilter::passes).
        // Duplicate inserts are harmless. A small build side (dimension
        // chains: nation, region, filtered part) additionally dedups cheaply
        // into an exact list, cancelling the Bloom's false positives.
        constexpr std::size_t kInListBuildMax = 4096;
        constexpr std::size_t kInListMax = 1024;
        JoinBloomFilter bloom(valid_rows);
        for (std::size_t r = 0; r < n; ++r) {
            if (validity != nullptr && !(*validity)[r]) {
                continue;
            }
            bloom.insert(data[r]);
        }
        slot.bloom = std::move(bloom);
        if (valid_rows <= kInListBuildMax) {
            std::vector<std::int64_t> keys;
            keys.reserve(valid_rows);
            for (std::size_t r = 0; r < n; ++r) {
                if (validity != nullptr && !(*validity)[r]) {
                    continue;
                }
                keys.push_back(data[r]);
            }
            std::ranges::sort(keys);
            keys.erase(std::ranges::unique(keys).begin(), keys.end());
            if (keys.size() <= kInListMax) {
                slot.in_list = std::move(keys);
            }
        }
        // Raw facts, not policy: whether these bounds are worth acting on is
        // the consumer's call — materialize_deferred_scan gates synthesized
        // conjuncts on estimated pruning, while the fused key scan uses them
        // ungated to skip whole row groups (which has no gather downside).
        slot.min = mn;
        slot.max = mx;
    }

    /// Would indexing the right instead of the left buy the pending `order`,
    /// and is the index small enough that it is worth buying?
    ///
    /// The pending keys are in the join's output names and the left's claim is
    /// in the left's own, so the claim is restated through the output plan
    /// before they are compared -- a suffixed key is renamed and a key the
    /// output drops takes the claim with it.
    auto order_preserving_pays(const Table& left_table, std::size_t n_left, std::size_t n_right)
        -> bool {
        constexpr std::size_t kMaxOrderPreservingBuildRatio = 4;
        const auto& left_ordering = left_table.properties().ordering();
        if (pending_order_ == nullptr || pending_order_->empty() || !left_ordering.has_value() ||
            n_right > kMaxOrderPreservingBuildRatio * n_left) {
            return false;
        }
        if (!probe_.right_emit_ready_) {
            if (auto ready = probe_.setup_right_emit_schema(left_table); !ready.has_value()) {
                return false;  // the join is about to fail on this anyway
            }
        }
        std::vector<ir::OrderKey> carried;
        for (const auto& key : *left_ordering) {
            std::size_t idx = left_table.columns.size();
            for (std::size_t i = 0; i < left_table.columns.size(); ++i) {
                if (left_table.columns[i].name == key.name) {
                    idx = i;
                    break;
                }
            }
            if (idx == left_table.columns.size() || idx >= probe_.left_emit_names_.size()) {
                return false;
            }
            carried.push_back(
                ir::OrderKey{.name = probe_.left_emit_names_[idx], .ascending = key.ascending});
        }
        return TableProperties::sorted_by(std::move(carried)).satisfies(*pending_order_);
    }

    OperatorPtr left_;
    /// The right side, owned jointly with every probe reading it. A
    /// `shared_ptr` rather than a member `Table` because a probe must be able
    /// to outlive this operator and several probes must be able to read one
    /// build side at once -- see `JoinProbe::right_`. Written only by the
    /// build phase, which finishes before any probe runs.
    std::shared_ptr<Table> right_ = std::make_shared<Table>();
    const std::vector<ir::JoinKey>* keys_;
    /// The probe half. The operator runs the build and decides which side to
    /// index; everything after that belongs here. Moved into `probe_op_` when
    /// the orientation is BuildRight, since from then on the probe is an
    /// operator of its own and this class is build-side only.
    JoinProbe probe_;
    /// The probe side, drained by the build phase and held until either
    /// `ensure_probe_op` turns it into a source or `take_fusible_probe` hands
    /// it to a pipeline above. Empty when the probe side streams from `left_`.
    std::optional<Table> probe_side_;
    /// Stream mode's probe, constructed on first use: one
    /// `JoinProbeOperator` over the probe-side child, or -- when that child
    /// was already materialized and is big enough -- a morsel pipeline of
    /// several of them over its morsels. Null in the Swapped and Precomputed
    /// modes, which emit one table rather than streaming.
    OperatorPtr probe_op_;

    // Deferred-probe context (see the second constructor). `deferred_probe_`
    // doubles as the mode flag: non-null until the probe scan is resolved.
    const DeferredScan* deferred_probe_ = nullptr;
    std::string deferred_probe_name_;
    const ir::Node* deferred_right_node_ = nullptr;
    const TableRegistry* deferred_registry_ = nullptr;
    const ScalarRegistry* deferred_scalars_ = nullptr;
    const ExternRegistry* deferred_externs_ = nullptr;
    const ExecutionContext* deferred_exec_ = nullptr;
    bool initialized_ = false;
    Mode mode_ = Mode::Stream;

    /// Both fan-out phases' parallelism, resolved by the caller
    /// (`resolved_join_parallelism`, shared by every construction site).
    /// `build_partitions` reads `par_.build`; `bind_probe` copies `par_.probe`
    /// into the probe. src/runtime/PARALLELISM.md. Default-constructed
    /// (`worker_cap == 0`) only on a hand-built operator, where both phases then
    /// stay serial.
    physical::JoinParallelism par_{};

    // What an `order` above this join will ask for, or null. Only ever shifts
    // which side is indexed; see `initialize`.
    const std::vector<ir::OrderKey>* pending_order_ = nullptr;

    // Stream mode: when right > threshold and left >= right, left was
    // materialized to measure but not swapped; replay as a single chunk.
    std::optional<Table> left_materialized_;
    bool use_materialized_left_ = false;
    std::optional<Table> empty_schema_;

    // Swapped mode: materialized left held for later gather.
    std::optional<Table> left_table_;
    bool swapped_emitted_ = false;

    // Precomputed mode: the two-phase deferred probe assembled the whole
    // join output during initialization.
    Table precomputed_output_;
};

/// HashProbe for the runtime BuildLeft orientation. The build has already
/// produced the immutable index and retained the materialized left side; this
/// operator only scans the right side through that index and emits once.
class SwappedHashProbeOperator final : public Operator {
   public:
    explicit SwappedHashProbeOperator(SwappedHashProbeInput input)
        : left_(std::move(input.left)), probe_(std::move(input.probe)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        emitted_ = true;
        auto out = probe_.emit_swapped(left_);
        if (!out.has_value()) {
            return std::unexpected(std::move(out.error()));
        }
        if (out->rows() == 0) {
            return std::optional<Chunk>{};
        }
        return std::optional<Chunk>{table_to_chunk(std::move(*out))};
    }

   private:
    Table left_;
    JoinProbe probe_;
    bool emitted_ = false;
};

/// Deferred joins can finish during HashBuild after their dynamic filter has
/// resolved the probe source. They still cross the same typed edge; HashProbe
/// simply emits the already-computed result rather than re-running work.
class PrecomputedHashProbeOperator final : public Operator {
   public:
    explicit PrecomputedHashProbeOperator(Table output) : output_(std::move(output)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        emitted_ = true;
        if (output_.rows() == 0) {
            return std::optional<Chunk>{};
        }
        return std::optional<Chunk>{table_to_chunk(std::move(output_))};
    }

   private:
    Table output_;
    bool emitted_ = false;
};

/// Construct the physical HashProbe from exactly one completed HashBuild
/// output. There is no orientation branch after this point: the variant chosen
/// by the build owns the only legal probe implementation for that orientation.
auto build_hash_probe_operator(HashProbeInput input) -> std::expected<OperatorPtr, std::string> {
    if (auto* stream = std::get_if<StreamingHashProbeInput>(&input)) {
        if (stream->materialized_source.has_value() && stream->probe.exec_ != nullptr) {
            if (const std::size_t workers =
                    probe_morsel_workers(*stream->materialized_source, *stream->probe.exec_);
                workers >= 2) {
                return build_probe_morsel_pipeline(std::move(*stream->materialized_source),
                                                   stream->probe, workers, *stream->probe.exec_);
            }
        }
        OperatorPtr source = stream->materialized_source.has_value()
                                 ? make_table_source(std::move(*stream->materialized_source))
                                 : std::move(stream->source);
        return OperatorPtr{
            std::make_unique<JoinProbeOperator>(std::move(source), std::move(stream->probe))};
    }
    if (auto* swapped = std::get_if<SwappedHashProbeInput>(&input)) {
        return OperatorPtr{std::make_unique<SwappedHashProbeOperator>(std::move(*swapped))};
    }
    auto& precomputed = std::get<PrecomputedHashProbeInput>(input);
    return OperatorPtr{
        std::make_unique<PrecomputedHashProbeOperator>(std::move(precomputed.output))};
}


}  // namespace

auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string> {
    MaterializeOperator sink{std::move(op)};
    return sink.run();
}

namespace {

/// Defined next to `build_physical_join`; the join construction sites above it
/// (`inner_join_table`, the `IBEX_PROBE_MORSELS` probe POC) need it too.
auto resolved_join_parallelism(const ExecutionContext& exec) -> physical::JoinParallelism;

/// The hash aggregate's four structural-node policies, resolved together.
/// Defined next to `build_physical_aggregate`.
auto resolved_aggregate_parallelism(const physical::HashAggregateNodes& nodes,
                                    const ExecutionContext& exec)
    -> std::expected<physical::AggregateParallelism, std::string>;

}  // namespace

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
/// schema) falls through to `build_binary_materializing_operator` exactly
/// as it does today, never into a code path that could fail at runtime.
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
    return materialize_operator(std::make_unique<ChunkedInnerJoinOperator>(
        std::move(source), right, &keys, exec, suffix, &pending_order,
        resolved_join_parallelism(exec)));
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

namespace {

template <typename Fn>

auto build_binary_materializing_operator(const ir::Node& left_node, const ir::Node& right_node,
                                         const TableRegistry& registry,
                                         const ScalarRegistry* scalars,
                                         const ExternRegistry* externs,
                                         const ExecutionContext& exec, ModelResult* model_out,
                                         Fn fn) -> std::expected<OperatorPtr, std::string> {
    // Multiple producers (plans/parallelism-overview.md): tried overlapping
    // `left_node`/`right_node`'s materializations on a raw std::thread here,
    // twice. First attempt (unbudgeted): net regression on the full PDS-H
    // suite (q09 +57%, q04/q06/q07/q08/q19 also worse). Second attempt,
    // under a since-removed helper-thread budget: q09 STILL regressed
    // (+47.5%), and the budget provably wasn't the reason -- q09 hits this
    // function exactly once (verified with a temporary entry-count trace),
    // so there was never a recursive pile-up here for a budget to bound in
    // the first place, and re-testing at budget=1/2/8 all gave the same
    // ~230-250ms. The actual cost is structural, not concurrency-count: this
    // function materializes BOTH sides fully (unlike is_streamable_inner_join,
    // which builds the left as a cheap lazy operator and only materializes
    // the right), so overlapping two already-expensive full materializations
    // contends for the same cores/memory bandwidth rather than filling idle
    // ones. Reverted a second time; a future attempt here needs a
    // cost-aware gate (e.g. skip when both sides are large), not a
    // thread-count budget.
    auto left = materialize_row_local(left_node, registry, scalars, externs, exec, model_out);
    if (!left.has_value()) {
        return std::unexpected(std::move(left.error()));
    }
    auto right = materialize_row_local(right_node, registry, scalars, externs, exec, model_out);
    if (!right.has_value()) {
        return std::unexpected(std::move(right.error()));
    }
    auto result = fn(std::move(left.value()), std::move(right.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return make_table_source(std::move(result.value()));
}

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

namespace {

// Runtime-multithreading Phase 1, serial morsel slice. Owns the materialized
// input `Table` that the pipeline's `PartitionedTableSource` reads by pointer.
// `input_` is declared before `chain_` so the chain — which holds a raw
// pointer into `input_` — is destroyed first.
class OwningMorselPipelineOperator final : public Operator {
   public:
    OwningMorselPipelineOperator(std::unique_ptr<Table> input, OperatorPtr chain)
        : input_(std::move(input)), chain_(std::move(chain)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        return chain_->next();
    }

   private:
    std::unique_ptr<Table> input_;
    OperatorPtr chain_;
};

/// Serial-slice stand-in for the Phase 1 ordered merger. The current source
/// emits in sequence order, so validating the stream is enough to make a lost,
/// duplicated, or provenance-stripped morsel an immediate error. A later
/// concurrent merger replaces this with sequence-indexed buffering/release.
class SerialMorselOrderValidator final : public Operator {
   public:
    SerialMorselOrderValidator(OperatorPtr child, std::uint64_t expected_morsels, std::size_t grain)
        : child_(std::move(child)),
          expected_morsels_(expected_morsels),
          grain_(grain == 0 ? 1 : grain) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto result = child_->next();
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        if (result->has_value()) {
            const auto& chunk = result->value();
            const auto expected_offset = static_cast<std::size_t>(next_sequence_) * grain_;
            if (chunk.sequence != next_sequence_ || chunk.row_offset != expected_offset) {
                return std::unexpected("morsel pipeline: morsel identity gap or reordering");
            }
            ++next_sequence_;
            return result;
        }
        if (next_sequence_ != expected_morsels_) {
            return std::unexpected("morsel pipeline: missing output morsel");
        }
        return result;
    }

   private:
    OperatorPtr child_;
    std::uint64_t expected_morsels_ = 0;
    std::uint64_t next_sequence_ = 0;
    std::size_t grain_ = 1;
};

}  // namespace

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

namespace {

auto build_row_local_map_operator(const MapStep& step, OperatorPtr child,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const MapKernelFactory factory =
        step.factory != nullptr ? step.factory : map_kernel_factory(step.capability);
    if (factory == nullptr) {
        return std::unexpected("row-local map factory: unknown kernel capability");
    }
    return factory(step, std::move(child), scalars, externs, exec, nullptr, preserve_empty_morsels);
}

/// One unfused node as a step. The compatibility entry point for callers
/// outside a physical plan, which have a node and no fusion to express.
auto build_row_local_map_operator(const ir::Node& node, OperatorPtr child,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const auto capability = map_kernel_capability(node);
    if (!capability.has_value()) {
        return std::unexpected("row-local map factory: unsupported kernel capability");
    }
    const MapStep step{
        .node = &node, .capability = *capability, .factory = map_kernel_factory(*capability)};
    return build_row_local_map_operator(step, std::move(child), scalars, externs, exec,
                                        preserve_empty_morsels);
}

// The base of one worker's worker chain: a source the worker points at the
// morsel it just claimed. Two implementations, differing only in whether the
// morsel's rows are copied out of the shared input before the chain sees them.
class MorselSource : public Operator {
   public:
    /// Aim the source at rows [begin, end) of the pipeline's input. The next
    /// `next()` produces exactly that morsel and then reports exhaustion, so
    /// one call feeds one turn of the worker loop.
    virtual void set_morsel(std::size_t begin, std::size_t end, std::uint64_t sequence) = 0;
};

// Gathering source: materializes the morsel, then the chain above runs over it
// exactly as the serial path does. The fallback for any pipeline whose head this
// file cannot evaluate by range.
class GatherMorselSource final : public MorselSource {
   public:
    explicit GatherMorselSource(const Table& input) : input_(&input) {}

    void set_morsel(std::size_t begin, std::size_t end, std::uint64_t sequence) override {
        pending_ = make_morsel_chunk(*input_, begin, end, sequence);
    }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!pending_.has_value()) {
            return std::optional<Chunk>{};
        }
        auto chunk = std::move(*pending_);
        pending_.reset();
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    const Table* input_;
    std::optional<Chunk> pending_;
};

// Range-filtering source: absorbs the pipeline's head `Filter` and evaluates its
// predicate directly over the input's rows [begin, end), so the morsel is never
// materialized. Only surviving rows are ever copied — the gather the serial
// path pays for every row is gone.
//
// This is one operator doing the work of two, so it owes both their contracts:
// the morsel identity a gathering source would have stamped (`sequence` and
// `row_offset`, which the worker loop re-checks), and the head filter's
// `preserve_empty_morsels` behaviour — an empty result is still emitted,
// because the merger indexes by sequence and a skipped morsel is a lost slot
// rather than a smaller answer.
class RangeFilterMorselSource final : public MorselSource {
   public:
    RangeFilterMorselSource(const Table& input, const ir::Expr* predicate,
                            const std::vector<ir::ColumnRef>* project,
                            const ScalarRegistry* scalars)
        : input_(&input), predicate_(predicate), project_(project), scalars_(scalars) {}

    void set_morsel(std::size_t begin, std::size_t end, std::uint64_t sequence) override {
        pending_ = ChunkIdentity{.sequence = sequence, .row_offset = begin};
        begin_ = begin;
        end_ = end;
    }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!pending_.has_value()) {
            return std::optional<Chunk>{};
        }
        const auto identity = *pending_;
        pending_.reset();
        const RowRange rows{.begin = begin_, .count = end_ - begin_};
        auto filtered =
            project_ == nullptr
                ? filter_table_range(*input_, *predicate_, rows, scalars_)
                : filter_project_table_range(*input_, *predicate_, *project_, rows, scalars_);
        if (!filtered.has_value()) {
            return std::unexpected(std::move(filtered.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(filtered.value()), identity)};
    }

   private:
    const Table* input_;
    const ir::Expr* predicate_;
    const std::vector<ir::ColumnRef>* project_;
    const ScalarRegistry* scalars_;
    std::optional<ChunkIdentity> pending_;
    std::size_t begin_ = 0;
    std::size_t end_ = 0;
};

/// How a head operator is evaluated by range, when it can be. A filter head is
/// where gathering costs most — it copies rows the predicate is about to throw
/// away — and the physically fused filter/project form is included as well.
struct RangeHead {
    const ir::Expr* predicate = nullptr;
    const std::vector<ir::ColumnRef>* project = nullptr;  ///< null when unfused
};

/// The head's range form, or nullopt when it has to be built above a gathered
/// morsel instead.
///
/// Two things disqualify a head:
///
/// - A predicate that is not `is_range_native_expr`. Pipeline eligibility admits
///   Scalar calls, but every call still evaluates whole-table-and-slice, so
///   absorbing `abs(a) > 50` would re-run `abs` over the entire input once per
///   morsel — measured at 10x slower than serial on 20M rows. Gathering is the
///   correct choice there: the morsel is materialized once and the predicate
///   then runs over morsel-sized data.
/// - A column-less table, whose row count lives in the chunk's `logical_rows`
///   rather than in any column; only the gathering source carries that over.
[[nodiscard]] auto range_filter_head(const MapStep& step, const Table& input)
    -> std::optional<RangeHead> {
    if (input.columns.empty()) {
        return std::nullopt;
    }
    if (step.capability == MapKernelCapability::FilterGather) {
        // A head absorbs a filter and, at most, the projection directly above
        // it. A fused step that also carries an Update computes columns between
        // the two, and absorbing the projection here would skip that
        // computation -- the projection would then name a column nothing
        // produced. Such a step keeps its operator chain.
        if (step.fused_update != nullptr) {
            return std::nullopt;
        }
        const auto& predicate = ir::node_cast<ir::FilterNode>(*step.node).predicate();
        if (!is_range_native_expr(predicate)) {
            return std::nullopt;
        }
        // A planner-fused Project rides along exactly as the fused IR kind's
        // column list does below: same head, same absorbed projection.
        const std::vector<ir::ColumnRef>* project =
            step.fused_project != nullptr
                ? &ir::node_cast<ir::ProjectNode>(*step.fused_project).columns()
                : nullptr;
        return RangeHead{.predicate = &predicate, .project = project};
    }
    if (step.capability == MapKernelCapability::FilterProjectGather &&
        step.update_fields == nullptr && step.filter_predicate != nullptr &&
        step.project_columns != nullptr) {
        if (!is_range_native_expr(*step.filter_predicate)) {
            return std::nullopt;
        }
        return RangeHead{.predicate = step.filter_predicate, .project = step.project_columns};
    }
    return std::nullopt;
}

// One worker's private copy of the pipeline's map chain. The operators are
// per-worker (they carry mutable per-chunk state); the IR nodes, registries,
// and the input table they read are shared and immutable for the pipeline's
// lifetime.
struct MorselWorkerChain {
    MorselSource* source = nullptr;  // owned by `chain`, re-aimed per morsel
    OperatorPtr chain;
};

[[nodiscard]] auto build_morsel_worker_chain(const std::vector<MapStep>& operators,
                                             const Table& input, const ScalarRegistry* scalars,
                                             const ExternRegistry* externs,
                                             const ExecutionContext& exec,
                                             const JoinProbe* probe_head = nullptr)
    -> std::expected<MorselWorkerChain, std::string> {
    // A qualifying head is absorbed into the source rather than built as an
    // operator above it — same output, without materializing the morsel first.
    // Not available under a probe head: that optimization reads the first
    // operator as a filter over the SOURCE's rows, and with a probe between
    // them the source's rows are the probe side, not the filter's input.
    std::size_t first_op = 0;
    std::unique_ptr<MorselSource> source;
    if (!operators.empty() && probe_head == nullptr) {
        if (auto head = range_filter_head(operators.front(), input); head.has_value()) {
            source = std::make_unique<RangeFilterMorselSource>(input, head->predicate,
                                                               head->project, scalars);
            first_op = 1;
        }
    }
    if (source == nullptr) {
        source = std::make_unique<GatherMorselSource>(input);
    }

    MorselWorkerChain worker{.source = source.get(), .chain = std::move(source)};
    // The probe runs first, on this worker's own morsel of the probe side, and
    // every map above it runs in the same worker on the probe's output. That
    // is the whole point of the fused shape: the join's output is never
    // assembled as a table between the probe and the maps.
    if (probe_head != nullptr) {
        worker.chain = std::make_unique<JoinProbeOperator>(std::move(worker.chain), *probe_head,
                                                           /*preserve_empty_morsels=*/true);
    }
    for (std::size_t i = first_op; i < operators.size(); ++i) {
        const MapStep& op_node = operators[i];
        // `preserve_empty_morsels` is what makes one input morsel yield exactly
        // one identified output morsel — the merger indexes by sequence, so a
        // silently coalesced empty result would be a lost slot, not a smaller
        // answer.
        auto next = build_row_local_map_operator(op_node, std::move(worker.chain), scalars, externs,
                                                 exec, true);
        if (!next.has_value()) {
            // The plan's step vocabulary only admits row-local map kinds.
            return std::unexpected("morsel pipeline: " + next.error());
        }
        worker.chain = std::move(next.value());
    }
    return worker;
}
/// A bounded, sequence-ordered handoff between several producers and one
/// consumer — the one implementation of that shape in the runtime.
///
/// Slots are addressed `sequence % window`, so a producer may run at most
/// `window` sequences ahead of the consumer and then parks; the consumer parks
/// on the slot it needs next. Both waits are wrapped in `RingWaitScope`,
/// because produced-ahead and waiting-on-workers are idle rather than work —
/// counting them as work makes a blocked worker read as a busy one and
/// overstates occupancy.
///
/// Failure is ordered by sequence, not by arrival: the lowest-sequence failure
/// is the one reported, so the message a query returns never depends on which
/// thread lost a race. `record_fault` takes a static string and allocates
/// nothing, which is the only reporting path still available when allocation is
/// what failed.
///
/// Producer liveness is tracked so the consumer cannot wait for a sequence that
/// is never coming: a producer that leaves for any reason — exhaustion, error,
/// exception — must call `producer_exited`, which is what turns "a worker died"
/// into an error rather than a hang.
class OrderedChunkRing {
   public:
    OrderedChunkRing(std::size_t window, std::size_t producers)
        : window_(window == 0 ? 1 : window),
          ring_(window == 0 ? 1 : window),
          active_producers_(producers) {}

    OrderedChunkRing(const OrderedChunkRing&) = delete;
    auto operator=(const OrderedChunkRing&) -> OrderedChunkRing& = delete;
    OrderedChunkRing(OrderedChunkRing&&) = delete;
    auto operator=(OrderedChunkRing&&) -> OrderedChunkRing& = delete;
    ~OrderedChunkRing() = default;

    /// What a producer should do with the sequence it just claimed.
    enum class Acquire : std::uint8_t {
        Proceed,  ///< the slot is free; produce into it
        Abandon,  ///< cancelled, or a lower sequence already failed
    };

    /// Park until this sequence's slot is free. Called with no lock held.
    [[nodiscard]] auto acquire(std::uint64_t sequence) -> Acquire {
        std::unique_lock lock(mutex_);
        const RingWaitScope ring_wait;
        space_.wait(lock, [&] {
            return cancelled_ || sequence < released_ + window_ ||
                   (has_error_ && error_sequence_ < sequence);
        });
        // Only ever abandons sequences above the reported failure, so the
        // consumer still receives everything below it.
        return (cancelled_ || (has_error_ && error_sequence_ < sequence)) ? Acquire::Abandon
                                                                          : Acquire::Proceed;
    }

    void publish(std::uint64_t sequence, Chunk chunk) {
        {
            const std::scoped_lock lock(mutex_);
            ring_[static_cast<std::size_t>(sequence % window_)] = std::move(chunk);
        }
        ready_.notify_one();
    }

    /// Record an owned message. The caller has already built the string, so
    /// taking it by value and moving it under the lock never allocates here.
    void record_error(std::uint64_t sequence, std::string message) noexcept {
        {
            const std::scoped_lock lock(mutex_);
            if (claim_failure(sequence)) {
                error_owned_ = std::move(message);
                error_fixed_ = nullptr;
            }
        }
        wake_all();
    }

    /// Record a message in static storage. Allocates nothing at all, so it is
    /// the only reporting path available once allocation is what failed.
    void record_fault(std::uint64_t sequence, const char* message) noexcept {
        {
            const std::scoped_lock lock(mutex_);
            if (claim_failure(sequence)) {
                error_owned_.clear();  // frees, never allocates
                error_fixed_ = message;
            }
        }
        wake_all();
    }

    void producer_exited() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            --active_producers_;
        }
        ready_.notify_all();
    }

    /// Take the chunk at `sequence`, or nullopt when the run stopped before
    /// producing it — cancelled, failed, or out of producers. The caller asks
    /// `failure()` for why.
    [[nodiscard]] auto take(std::uint64_t sequence) -> std::optional<Chunk> {
        std::optional<Chunk> chunk;
        {
            std::unique_lock lock(mutex_);
            const auto slot = static_cast<std::size_t>(sequence % window_);
            {
                const RingWaitScope ring_wait;
                ready_.wait(lock, [&] {
                    return ring_[slot].has_value() || cancelled_ || active_producers_ == 0 ||
                           (has_error_ && error_sequence_ <= sequence);
                });
            }
            if (ring_[slot].has_value()) {
                chunk = std::move(ring_[slot]);
                ring_[slot].reset();
                ++released_;
            }
        }
        if (chunk.has_value()) {
            space_.notify_all();
        }
        return chunk;
    }

    [[nodiscard]] auto failure() const -> std::optional<std::string> {
        const std::scoped_lock lock(mutex_);
        if (!has_error_) {
            return std::nullopt;
        }
        return error_fixed_ != nullptr ? std::string(error_fixed_) : error_owned_;
    }

    void cancel() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        wake_all();
    }

   private:
    /// True if `sequence` becomes the reported failure. Lowest sequence wins,
    /// so the error a query reports never depends on thread timing.
    [[nodiscard]] auto claim_failure(std::uint64_t sequence) noexcept -> bool {
        if (has_error_ && sequence >= error_sequence_) {
            return false;
        }
        has_error_ = true;
        error_sequence_ = sequence;
        return true;
    }

    void wake_all() noexcept {
        ready_.notify_all();
        space_.notify_all();
    }

    std::size_t window_;
    mutable std::mutex mutex_;
    std::condition_variable ready_;  // consumer waits for the next sequence
    std::condition_variable space_;  // producers wait for ring space
    std::vector<std::optional<Chunk>> ring_;
    std::uint64_t released_ = 0;
    std::size_t active_producers_ = 0;
    bool cancelled_ = false;
    // The failure channel is split so it can be written without allocating.
    // `error_owned_` carries a message moved in from a producer; `error_fixed_`
    // points at static storage.
    bool has_error_ = false;
    std::uint64_t error_sequence_ = 0;
    std::string error_owned_;
    const char* error_fixed_ = nullptr;
};

// Runtime-multithreading Phase 1: the morsel pipeline executor.
//
// Workers pull numbered morsels from one shared cursor over the immutable
// materialized input, run their own chain over each, and deposit the result in
// a bounded ring indexed by `sequence`. `next()` is the ordered merger: it
// releases results strictly in sequence order, so the operator's output is
// byte-identical to the serial chain's regardless of completion order. The ring
// is the plan's bounded in-flight queue — a worker that runs ahead of the
// consumer by a full window blocks instead of buffering the whole pipeline.
//
// Output ownership (the plan's Phase-1 allocator variable): each task owns the
// chunk it produces, and the merger's consumer moves it straight into the
// downstream `MaterializeOperator` concat. Nothing escapes into task-local
// scratch storage, so no arena ownership has to be transferred. That is the
// simplest of the strategies the plan allows and the one whose allocation
// behavior the acceptance benchmarks measure; a presized query-owned buffer
// pool is the next option if allocation shows up in those numbers.
//
// Error and cancellation determinism: a failing morsel records its error under
// the lock, keeping the *lowest* sequence, and workers abandon only morsels
// above it — so every morsel below the reported failure is still produced, and
// the error a query reports does not depend on thread timing.
class MorselPipelineOperator final : public Operator {
   public:
    MorselPipelineOperator(std::unique_ptr<Table> input, std::vector<MorselWorkerChain> workers,
                           std::size_t grain, std::uint64_t morsel_count, WorkerPool& pool)
        : input_(std::move(input)),
          workers_(std::move(workers)),
          grain_(grain == 0 ? 1 : grain),
          morsel_count_(morsel_count),
          pool_(&pool),
          ring_(std::max<std::size_t>(workers_.size() * 2, 2), workers_.size()) {}

    ~MorselPipelineOperator() override { cancel_and_join(); }

    MorselPipelineOperator(const MorselPipelineOperator&) = delete;
    auto operator=(const MorselPipelineOperator&) -> MorselPipelineOperator& = delete;
    MorselPipelineOperator(MorselPipelineOperator&&) = delete;
    auto operator=(MorselPipelineOperator&&) -> MorselPipelineOperator& = delete;

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (finished_) {
            return std::optional<Chunk>{};
        }
        if (!started_) {
            started_ = true;
            batch_ = pool_->submit(workers_.size(), [this](std::size_t id) { run_worker(id); });
        }
        if (next_sequence_ >= morsel_count_) {
            return finish();
        }
        if (interrupt_requested()) {
            return fail(interrupt_message());
        }

        std::optional<Chunk> chunk = ring_.take(next_sequence_);
        if (chunk.has_value()) {
            ++next_sequence_;
            return std::optional<Chunk>{std::move(*chunk)};
        }

        // No chunk: the pipeline stopped early. Report why, deterministically.
        //
        // An interrupt outranks a recorded data error. A worker that fails at
        // the moment the user hits Ctrl+C is a race, and reporting its message
        // would make cancellation surface as an arbitrary query error depending
        // on which thread won. The cancellation contract says such a query
        // reports "interrupted", so the interrupt is checked first.
        if (interrupt_requested()) {
            return fail(interrupt_message());
        }
        if (auto failure = ring_.failure(); failure.has_value()) {
            return fail(std::move(*failure));
        }
        return fail("morsel pipeline: missing output morsel");
    }

   private:
    // Whatever happens to a worker — normal exhaustion, a recorded error, or an
    // exception — it must stop counting as active and must wake the merger.
    // Skipping this on any path leaves the consumer waiting for a sequence that
    // is never coming, which is a hang rather than an error.
    void worker_exited() noexcept { ring_.producer_exited(); }

    void run_worker(std::size_t worker_id) noexcept {
        // Cleanup runs however this scope is left, so no path can leave the
        // consumer waiting on a worker that is gone.
        // One-shot scope guard: aggregate-initialised, never copied or moved.
        // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
        struct ExitGuard {
            MorselPipelineOperator* self;
            ~ExitGuard() { self->worker_exited(); }
        } const guard{this};

        std::uint64_t sequence = 0;
        try {
            run_worker_loop(worker_id, sequence);
        } catch (const std::exception& error) {
            // An exception is not part of the operator protocol (evaluation
            // reports failure through `expected`), so it is something
            // unplanned — an allocation failure while materializing a morsel,
            // say. Convert it to a sequence-tagged pipeline error so it obeys the
            // same lowest-sequence determinism as any other failure, rather
            // than unwinding through a pool thread.
            //
            // Composing that message allocates, and the exception this handler
            // most expects is `bad_alloc` — so the detailed message is
            // best-effort, with an allocation-free fallback underneath it.
            // Throwing from here would terminate the process, since this
            // function is noexcept precisely so a worker cannot unwind into the
            // pool. `what()` cannot be stored: it dies with the exception.
            try {
                ring_.record_error(
                    sequence, "morsel pipeline: worker exception: " + std::string(error.what()));
            } catch (...) {
                ring_.record_fault(sequence,
                                   "morsel pipeline: worker exception (no memory to report it)");
            }
        } catch (...) {
            ring_.record_fault(sequence, "morsel pipeline: worker threw a non-standard exception");
        }
    }

    void run_worker_loop(std::size_t worker_id, std::uint64_t& claimed) {
        auto& worker = workers_[worker_id];
        const std::size_t rows = input_->rows();
        while (true) {
            const std::uint64_t sequence = cursor_.fetch_add(1, std::memory_order_relaxed);
            if (sequence >= morsel_count_) {
                break;
            }
            // Published so an exception thrown below is attributed to the
            // morsel that was in flight, not to sequence 0.
            claimed = sequence;
            // Backpressure: this morsel's slot is only free once the consumer
            // has released the morsel `window` ahead of it.
            if (ring_.acquire(sequence) == OrderedChunkRing::Acquire::Abandon) {
                break;
            }
            if (interrupt_requested()) {
                ring_.cancel();
                break;
            }

            const auto [begin, end] = morsel_row_range(rows, grain_, sequence);
            worker.source->set_morsel(begin, end, sequence);
            auto produced = worker.chain->next();

            if (!produced.has_value()) {
                ring_.record_error(sequence, std::move(produced.error()));
                break;
            }
            if (!produced->has_value()) {
                ring_.record_fault(sequence, "morsel pipeline: worker produced no output morsel");
                break;
            }
            Chunk out = std::move(**produced);
            if (out.sequence != sequence || out.row_offset != begin) {
                ring_.record_fault(sequence, "morsel pipeline: morsel identity gap or reordering");
                break;
            }
            ring_.publish(sequence, std::move(out));
        }
    }

    // Called from the destructor, so nothing here may throw: an escaping
    // exception during destruction terminates the process. Worker bodies are
    // already noexcept and convert failures into pipeline errors, so there is
    // nothing for `wait()` to rethrow — this guards the path regardless.
    void cancel_and_join() noexcept {
        try {
            ring_.cancel();
            batch_.wait();
        } catch (...) {  // NOLINT(bugprone-empty-catch)
            // Nothing left to report: the caller is either unwinding or has
            // already chosen the message it will return.
        }
    }

    // Drain the pipeline cleanly at EOF, then check the per-worker chains really
    // are exhausted: a chain still holding a suppressed schema carrier would
    // mean a morsel was coalesced away rather than emitted.
    [[nodiscard]] auto finish() -> std::expected<std::optional<Chunk>, std::string> {
        finished_ = true;
        batch_.wait();
        // Same precedence as `next()`: a cancelled run reports cancellation
        // even if a worker also failed on its way out.
        if (interrupt_requested()) {
            return std::unexpected(interrupt_message());
        }
        if (auto failure = ring_.failure(); failure.has_value()) {
            return std::unexpected(std::move(*failure));
        }
        for (auto& worker : workers_) {
            auto trailing = worker.chain->next();
            if (!trailing.has_value()) {
                return std::unexpected(std::move(trailing.error()));
            }
            if (trailing->has_value()) {
                return std::unexpected("morsel pipeline: unexpected trailing morsel");
            }
        }
        return std::optional<Chunk>{};
    }

    [[nodiscard]] auto fail(std::string message)
        -> std::expected<std::optional<Chunk>, std::string> {
        finished_ = true;
        cancel_and_join();
        return std::unexpected(std::move(message));
    }

    // `input_` is declared first so it outlives `workers_`: the chains read it
    // through raw pointers, and the batch is joined before any member is
    // destroyed.
    std::unique_ptr<Table> input_;
    std::vector<MorselWorkerChain> workers_;
    std::size_t grain_ = 1;
    std::uint64_t morsel_count_ = 0;
    WorkerPool* pool_;

    std::atomic<std::uint64_t> cursor_{0};

    // The ordered handoff between the workers and this operator's `next()`.
    OrderedChunkRing ring_;

    std::uint64_t next_sequence_ = 0;
    bool started_ = false;
    bool finished_ = false;
    WorkerPool::Batch batch_;
};

// Runtime-multithreading Phase 2: the two-phase parallel filter.
//
// What the ordered merger above cannot remove is the merge itself. Each worker
// materializes its morsel's surviving rows, and `MaterializeOperator` then
// copies all of them again into one table — so a filter pipeline copies its
// output twice where the serial path copies it once. That is why morsel parallelism wins
// track OUTPUT size rather than input size: a selective predicate wins easily,
// and a bulk one loses no matter how much input work is parallelized.
//
// A filter cannot simply presize its output and skip the merge, because its
// cardinality is data-dependent — nobody knows where morsel 7's rows belong
// until morsels 0-6 have been counted. So run the filter in two passes:
//
//   Phase A   every morsel evaluates the predicate and packs its surviving
//             rows into keep words, in parallel. Only the counts matter after.
//   (serial)  an exclusive prefix sum over those counts gives each morsel the
//             row — and, for string columns, the byte — where its output
//             begins. The output is then allocated ONCE, at exactly the
//             final size.
//   Phase B   every morsel gathers its rows straight into that shared output
//             at its own offset, in parallel. The slices are disjoint, so no
//             locking is needed and nothing is copied twice.
//
// The result is emitted as ONE chunk, which `MaterializeOperator` moves instead
// of concatenating. Ordering is structural — a morsel's rows land at its
// prefix-sum offset — so there is no ring, no merger, and the output is
// byte-identical to the serial filter's.
//
// What it costs: phase A's keep words are held for every morsel at once, which
// is one bit per input row (2.5MB for 20M rows), and phase B re-walks them.
// Neither re-evaluates the predicate.
//
// Writing into disjoint output ROWS is only disjoint in MEMORY for columns
// storing at least one addressable unit per row. `Column<bool>` and validity
// bitmaps pack 64 rows to a word, so two morsels meeting mid-word touch the
// same word; `gather_selection_into` resolves that with the shared-word rule
// (see `SharedBitWords` in filter.cpp) rather than excluding those columns.
// `filter_gather_is_thread_safe` remains as the allowlist that keeps a future
// column kind out until someone has checked it.
//
// Note a 64-row-aligned grain would NOT have made those columns safe, which is
// the tempting shortcut: an output offset is the prefix sum of POPCOUNTS, not
// of morsel sizes, so a morsel keeping 37 of its 64 rows already leaves the
// next one starting mid-word. Grain only aligns the SOURCE read, and reads
// never race.
class TwoPhaseFilterOperator final : public Operator {
   public:
    TwoPhaseFilterOperator(std::unique_ptr<Table> input, const ir::Expr& predicate,
                           bool fused_project, std::vector<const ir::Node*> tail,
                           const ScalarRegistry* scalars, FilterOutputLayout layout,
                           std::size_t grain, std::uint64_t morsel_count, std::size_t workers,
                           WorkerPool& pool)
        : input_(std::move(input)),
          predicate_(&predicate),
          fused_project_(fused_project),
          tail_(std::move(tail)),
          scalars_(scalars),
          layout_(std::move(layout)),
          grain_(grain == 0 ? 1 : grain),
          morsel_count_(morsel_count),
          workers_(workers),
          pool_(&pool) {
        selections_.resize(static_cast<std::size_t>(morsel_count_));
        row_at_.assign(static_cast<std::size_t>(morsel_count_), 0);
    }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (done_) {
            return std::optional<Chunk>{};
        }
        done_ = true;
        auto table = run();
        if (!table.has_value()) {
            return std::unexpected(std::move(table.error()));
        }
        // Sequence 0 / row_offset 0: this operator emits the pipeline's whole
        // output at once, so it is trivially the first and only morsel.
        return std::optional<Chunk>{table_to_chunk(std::move(table.value()), ChunkIdentity{})};
    }

   private:
    [[nodiscard]] auto run() -> std::expected<Table, std::string> {
        const std::size_t n_cols = layout_.output.columns.size();
        const bool has_strings = std::ranges::any_of(layout_.src_of_dst, [&](std::size_t src) {
            return std::holds_alternative<Column<std::string>>(*input_->columns[src].column);
        });
        if (has_strings) {
            chars_at_.assign(static_cast<std::size_t>(morsel_count_),
                             std::vector<std::size_t>(n_cols, 0));
        }

        if (auto failure = run_over_morsels([this](std::uint64_t sequence) { phase_a(sequence); });
            failure.has_value()) {
            return std::unexpected(std::move(*failure));
        }

        // Exclusive prefix sums: each morsel's counts become its offsets, and
        // the running totals become the output's exact size. Serial on purpose
        // — it is O(morsels), not O(rows).
        std::size_t rows_total = 0;
        for (std::size_t m = 0; m < selections_.size(); ++m) {
            row_at_[m] = rows_total;
            rows_total += selections_[m].kept;
        }
        std::vector<std::size_t> chars_total(n_cols, 0);
        if (has_strings) {
            for (std::size_t d = 0; d < n_cols; ++d) {
                for (auto& per_morsel : chars_at_) {
                    const std::size_t count = per_morsel[d];
                    per_morsel[d] = chars_total[d];
                    chars_total[d] += count;
                }
            }
        }

        presize_filter_output(layout_.output, *input_, layout_.src_of_dst, rows_total, chars_total);

        if (auto failure = run_over_morsels([this](std::uint64_t sequence) { phase_b(sequence); });
            failure.has_value()) {
            return std::unexpected(std::move(*failure));
        }

        // Identical rule to the serial filter: a row-local filter preserves
        // order and time index; a fused projection keeps each only when its
        // column survives.
        apply_table_properties(
            layout_.output, TableProperties::derive(
                                table_properties_of(*input_),
                                [&](const std::string& name) -> KeyFate {
                                    return (!fused_project_ || layout_.output.index.contains(name))
                                               ? KeyFate::kept(name)
                                               : KeyFate::dropped();
                                },
                                RowTransform::Subset));

        // Metadata-only operators above the filter run ONCE over the finished
        // output rather than per morsel. `project_table` / `rename_table` build
        // their result with `add_column_shared` — zero rows copied, O(columns)
        // — so running them serially here costs nothing, while routing the
        // chain through the ordered merger to parallelize them costs the whole
        // merge copy. Measured: `filter … rename` at 93% selectivity was 2.3x
        // SLOWER than serial on the merger.
        //
        // Applying them to the concatenated output is equivalent to applying
        // them per morsel because neither reads a row; and because these are
        // the same two functions the serial path calls, the ordering and
        // time-index rules cannot diverge from it either.
        Table result = std::move(layout_.output);
        for (const ir::Node* node : tail_) {
            auto next = apply_metadata_only(*node, result);
            if (!next.has_value()) {
                return std::unexpected(std::move(next.error()));
            }
            result = std::move(next.value());
        }
        return result;
    }

    [[nodiscard]] static auto apply_metadata_only(const ir::Node& node, const Table& input)
        -> std::expected<Table, std::string> {
        switch (node.kind()) {
            case ir::NodeKind::Project:
                return project_table(input, ir::node_cast<ir::ProjectNode>(node).columns());
            case ir::NodeKind::Rename:
                return rename_table(input, ir::node_cast<ir::RenameNode>(node).renames());
            default:
                // The pipeline builder only admits `is_metadata_only_node` kinds
                // into `tail_`, so reaching this means the two have drifted.
                invariant_violation("two-phase filter: non-metadata operator in the tail");
        }
    }

    void phase_a(std::uint64_t sequence) {
        const auto rows = morsel_range(sequence);
        auto selection =
            compute_filter_selection(*input_, *predicate_, scalars_, rows, /*row_limit=*/0);
        if (!selection.has_value()) {
            record_error(sequence, std::move(selection.error()));
            return;
        }
        auto& slot = selections_[static_cast<std::size_t>(sequence)];
        slot = std::move(selection.value());
        if (!chars_at_.empty()) {
            count_selected_chars(*input_, layout_.src_of_dst, slot, rows,
                                 chars_at_[static_cast<std::size_t>(sequence)]);
        }
    }

    void phase_b(std::uint64_t sequence) {
        const auto index = static_cast<std::size_t>(sequence);
        gather_selection_into(
            layout_.output, *input_, layout_.src_of_dst, selections_[index], morsel_range(sequence),
            GatherDest{.row = row_at_[index],
                       .char_base = chars_at_.empty() ? nullptr : &chars_at_[index]});
    }

    [[nodiscard]] auto morsel_range(std::uint64_t sequence) const -> RowRange {
        const auto [begin, end] = morsel_row_range(input_->rows(), grain_, sequence);
        return RowRange{.begin = begin, .count = end - begin};
    }

    // Run `body` over every morsel across the pool and join. A failure is
    // reported with the LOWEST morsel sequence, the same determinism rule the
    // ordered merger uses, so which morsel's error a query reports never
    // depends on thread timing. Workers abandon only morsels ABOVE a recorded
    // failure, so no morsel below it is skipped.
    template <typename Body>
    [[nodiscard]] auto run_over_morsels(const Body& body) -> std::optional<std::string> {
        reset_failure();
        std::atomic<std::uint64_t> cursor{0};
        {
            auto batch = pool_->submit(workers_, [&](std::size_t) noexcept {
                while (true) {
                    const std::uint64_t sequence = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (sequence >= morsel_count_) {
                        return;
                    }
                    if (failure_below(sequence) || interrupt_requested()) {
                        return;
                    }
                    // A worker may not unwind into the pool, so an unplanned
                    // exception becomes a sequence-tagged error like any other.
                    // The message itself allocates and the exception most
                    // expected here is bad_alloc, so there is an
                    // allocation-free fallback underneath it.
                    try {
                        body(sequence);
                    } catch (const std::exception& error) {
                        try {
                            record_error(sequence, "parallel filter: worker exception: " +
                                                       std::string(error.what()));
                        } catch (...) {
                            record_fault(sequence,
                                         "parallel filter: worker exception (no memory to "
                                         "report it)");
                        }
                    } catch (...) {
                        record_fault(sequence,
                                     "parallel filter: worker threw a non-standard exception");
                    }
                }
            });
            batch.wait();
        }
        // Same precedence as the ordered merger: an interrupt outranks a
        // recorded data error, so a worker failing as Ctrl+C arrives still
        // reports cancellation rather than an arbitrary error.
        if (interrupt_requested()) {
            return interrupt_message();
        }
        const std::scoped_lock lock(mutex_);
        if (!has_error_) {
            return std::nullopt;
        }
        return error_fixed_ != nullptr ? std::string(error_fixed_) : error_owned_;
    }

    void reset_failure() noexcept {
        const std::scoped_lock lock(mutex_);
        has_error_ = false;
        error_fixed_ = nullptr;
        error_owned_.clear();
    }

    [[nodiscard]] auto failure_below(std::uint64_t sequence) noexcept -> bool {
        const std::scoped_lock lock(mutex_);
        return has_error_ && error_sequence_ < sequence;
    }

    void record_error(std::uint64_t sequence, std::string message) {
        const std::scoped_lock lock(mutex_);
        if (claim_failure(sequence)) {
            error_owned_ = std::move(message);
            error_fixed_ = nullptr;
        }
    }

    void record_fault(std::uint64_t sequence, const char* message) noexcept {
        const std::scoped_lock lock(mutex_);
        if (claim_failure(sequence)) {
            error_owned_.clear();  // frees, never allocates
            error_fixed_ = message;
        }
    }

    [[nodiscard]] auto claim_failure(std::uint64_t sequence) noexcept -> bool {
        if (has_error_ && sequence >= error_sequence_) {
            return false;
        }
        has_error_ = true;
        error_sequence_ = sequence;
        return true;
    }

    // `input_` is declared first so it outlives everything reading it.
    std::unique_ptr<Table> input_;
    const ir::Expr* predicate_;
    bool fused_project_ = false;
    /// Metadata-only operators above the filter, source-to-sink, applied once
    /// to the finished output. Every element is `is_metadata_only_node`.
    std::vector<const ir::Node*> tail_;
    const ScalarRegistry* scalars_;
    FilterOutputLayout layout_;
    std::size_t grain_ = 1;
    std::uint64_t morsel_count_ = 0;
    std::size_t workers_ = 0;
    WorkerPool* pool_;

    // Written by phase A, read by phase B. Every element is touched by exactly
    // one worker (indexed by its own morsel sequence), so these need no lock —
    // the join between the phases is the synchronization.
    std::vector<FilterSelection> selections_;
    std::vector<std::size_t> row_at_;
    std::vector<std::vector<std::size_t>> chars_at_;  // empty when no string column

    std::mutex mutex_;
    bool has_error_ = false;
    std::string error_owned_;
    const char* error_fixed_ = nullptr;
    std::uint64_t error_sequence_ = 0;

    bool done_ = false;
};

// How many workers a pipeline of `morsel_count` morsels over `rows` rows should
// run on: 0 means "stay on the serial morsel chain".
//
// This is the plan's grain-size serial threshold. Below it, task dispatch,
// ring synchronization, and the merge cost more than the map they parallelize —
// cache-resident work should not pay for threads. A single morsel is serial by
// definition, and a one-thread budget means the caller asked for serial.
/// Whether this input is worth morselizing at all — a *different* question from
/// how many workers it deserves, and conflating the two is a trap worth naming.
///
/// A "refused" pipeline used to mean a serial sweep of morsels, which still pays
/// per-morsel materialization and the merge concat. So refusing by dropping the
/// worker count made a small query **slower than never forming a pipeline**:
/// measured 100ms against 36ms for the plain serial path, and it got worse once
/// the grain was derived, because that turned 2 morsels into 32. When the
/// answer is no, the input has to run as ONE whole-table chunk.
///
/// Two thresholds, because a pipeline's cost has two dimensions. Rows alone
/// cannot express it: 131,072 rows won at 6 columns and lost at 2 on the very
/// same predicate, and every row threshold puts those on the same side.
[[nodiscard]] auto is_worth_morselizing(const ExecutionContext& exec, std::size_t rows,
                                        std::size_t columns) -> bool {
    if (rows < exec.parallel_min_rows) {
        return false;
    }
    return exec.parallel_min_cells == 0 || columns == 0 ||
           rows * columns >= exec.parallel_min_cells;
}

[[nodiscard]] auto morsel_worker_count(const ExecutionContext& exec, std::uint64_t morsel_count)
    -> std::size_t {
    if (morsel_count < 2 || !exec.can_fan_out()) {
        return 0;
    }
    // Past the parallel gate: consulting the pool here is free of the
    // construct-before-declining hazard because a parallel query has already
    // built it (or is about to, on its first fan-out).
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t workers =
        std::min({budget, pool_size, static_cast<std::size_t>(morsel_count)});
    return workers < 2 ? 0 : workers;
}

// Build one eligible row-local parallel-map chain as a morsel pipeline: materialize its
// input subtree once, then run the chain over morsels of that table instead of
// one whole-table chunk. The operators are ordered source-to-sink.
//
// Two executors, one morsel model. A large input fans out across the worker
// pool and is reassembled by `MorselPipelineOperator`'s ordered merger; a small
// one (or a single-threaded budget) runs the same morsels serially through a
// `PartitionedTableSource`, where `MaterializeOperator`'s in-order concat is
// the trivially ordered merger. Both stamp and check the same morsel identity,
// so both are byte-identical to the plain serial chain — which is exactly what
// lets the threshold move without changing an answer.
//
// LOAD-BEARING INVARIANT — materialize before fan-out. The input subtree is
// executed to a `Table` here, on this thread, and every morsel source below
// takes that finished table by reference. That is what makes a deferred/lazy
// source safe in a pipeline: its decode runs exactly once, serially, before any
// worker exists, so neither `LazyTable::cache_` nor a plugin's `decode_`
// closure is ever touched concurrently. It is why `build_operator`'s seam no
// longer screens pipelines for deferred sources.
//
// The morsel sources all take `const Table&`, so the invariant is enforced by
// their signatures rather than by a check. Streaming a source's morsels
// directly into workers would mean handing them something other than a
// finished table — at which point the LazyTable synchronization contract
// applies in full and eligibility has to be re-established.
/// Defined below; both are consulted by the run builder, which decides its own
/// source strategy.
[[nodiscard]] auto scan_pipeline_worker_count(std::size_t unit_count) -> std::size_t;

/// Defined below; the run builder chooses between this streaming source and
/// materialize-then-morselize, so the choice lives with the run rather than at
/// the construction seam.
[[nodiscard]] auto build_pipelined_scan(const std::vector<MapStep>& operators,
                                        bool count_as_pipeline, const DeferredScan& scan,
                                        std::vector<SourceUnit> units,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string>;

}  // namespace

// Internal linkage to match the forward declarations beside the join, which
// sit inside this TU's anonymous namespace; the definitions must live down
// here because they use the morsel executor, which is defined below the join.
namespace {

/// Build a streaming join here and take its probe, so a map pipeline above it
/// can run that probe at the head of its own worker chains.
///
/// Returns an empty optional -- not an error -- for every join this cannot
/// fuse: a materializing one, a semi/anti one, a deferred probe scan (whose
/// right subtree must be interpreted by the join itself, after it publishes
/// build-side bounds), and any orientation that leaves no materialized probe
/// side. The caller then materializes the join's output as it always did.
///
/// Narrow on purpose. The point is to establish that two pipelines meeting at
/// a barrier can be built and can produce the right answer; widening the
/// shapes is cheap once that is true, and pointless before.
auto try_take_join_probe(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<std::optional<ChunkedInnerJoinOperator::FusibleProbe>, std::string> {
    if (std::getenv("IBEX_PROBE_MORSELS") == nullptr) {
        return std::optional<ChunkedInnerJoinOperator::FusibleProbe>{};
    }
    const physical::Plan plan = physical::plan_physical(node, registry, externs);
    const physical::JoinPlan& jp = plan.join;
    if (!jp.describes || jp.strategy != physical::JoinStrategy::StreamingProbe ||
        (jp.branch != physical::JoinBranch::SingleKeyInner &&
         jp.branch != physical::JoinBranch::PairIntInner)) {
        return std::optional<ChunkedInnerJoinOperator::FusibleProbe>{};
    }
    const auto& join = ir::node_cast<ir::JoinNode>(node);
    if (deferred_probe_scan_of(*join.children()[1], exec).scan != nullptr) {
        return std::optional<ChunkedInnerJoinOperator::FusibleProbe>{};
    }

    auto left_op = build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
    if (!left_op.has_value()) {
        return std::unexpected(std::move(left_op.error()));
    }
    auto right =
        materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
    if (!right.has_value()) {
        return std::unexpected(std::move(right.error()));
    }
    auto op = std::make_unique<ChunkedInnerJoinOperator>(
        std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
        &join.pending_order(), resolved_join_parallelism(exec));
    if (auto err = op->run_build()) {
        return std::unexpected(std::move(*err));
    }
    return op->take_fusible_probe();
}

auto probe_morsel_workers(const Table& input, const ExecutionContext& exec) -> std::size_t {
    // OPT-IN, and the measurement is why. On its own this is a LOSS: the
    // probe already fans out inside one chunk (`probe_ranges_parallel`) with
    // no copy, and morselizing replaces that with a per-morsel gather plus an
    // ordered merge. Measured at 8 cores, interleaved, median of 15:
    // q05 +1.7%, q09 +6.8% -- the only two PDS-H queries where it fires.
    //
    // It is wired anyway because the gather is not what this shape is for.
    // A probe morsel becomes worth its gather when the filters and
    // projections above the join run in the SAME chain, so the join's output
    // is never materialized between them. That needs the probe admitted into
    // the plan's step vocabulary, which is the next piece; this is the half
    // that had to work first, and it does -- 22/22 answers at 1, 2 and 8
    // cores with `IBEX_PROBE_MORSELS=1`.
    if (std::getenv("IBEX_PROBE_MORSELS") == nullptr) {
        return 0;
    }
    if (!exec.can_fan_out() || on_worker_pool_thread()) {
        return 0;
    }
    if (!is_worth_morselizing(exec, input.rows(), input.columns.size())) {
        return 0;
    }
    const std::size_t grain = morsel_grain(exec, input.rows());
    return morsel_worker_count(exec, partitioned_morsel_count(input, grain));
}

auto build_probe_morsel_pipeline(Table input, const JoinProbe& probe, std::size_t workers,
                                 const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string> {
    auto owned = std::make_unique<Table>(std::move(input));
    const std::size_t grain = morsel_grain(exec, owned->rows());
    const auto expected_morsels = partitioned_morsel_count(*owned, grain);
    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->parallel_pipelines.fetch_add(1, std::memory_order_relaxed);
        exec.parallel_stats->morsels.fetch_add(expected_morsels, std::memory_order_relaxed);
    }

    static const std::vector<MapStep> no_steps;
    std::vector<MorselWorkerChain> chains;
    chains.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
        // The chain is source + probe. Each worker gets its OWN `JoinProbe` --
        // its scratch vectors and its per-chunk categorical head table are
        // per-worker state -- and they share one build, because the index and
        // the build side are both `shared_ptr<const>` and have been since
        // `f6a1a632` and `6df9a966`. Copying the probe is what that ownership
        // was for.
        auto worker = build_morsel_worker_chain(no_steps, *owned, nullptr, nullptr, exec, &probe);
        if (!worker.has_value()) {
            return std::unexpected(std::move(worker.error()));
        }
        chains.push_back(std::move(worker.value()));
    }
    return std::make_unique<MorselPipelineOperator>(std::move(owned), std::move(chains), grain,
                                                    expected_morsels, process_worker_pool());
}

}  // namespace

namespace {

/// The steps of a plan's parallel prefix, ordered source-to-sink. A plan
/// records steps sink-first; every executor here composes bottom-up.
auto parallel_pipeline_operators(const physical::Plan& plan) -> std::vector<MapStep> {
    std::vector<MapStep> operators;
    operators.reserve(plan.parallel_step_count());
    for (std::size_t i = plan.parallel_end; i > plan.parallel_begin; --i) {
        operators.push_back(plan.steps[i - 1]);
    }
    return operators;
}

/// Run a physical map pipeline over morsels. The plan says which steps may run
/// in parallel (`parallel_steps`) and what feeds them (`parallel_input_node`);
/// this builds that input, materializes it, and executes the prefix over its
/// morsels. It is the pipeline's parallel *mode*, not a separate executor with
/// its own idea of what is eligible.
///
/// `steps` inside a plan are sink-first; the operators here run source-to-sink,
/// so the prefix is reversed once, on the build thread.
auto build_map_pipeline_parallel(const physical::Plan& plan, const TableRegistry& registry,
                                 const ScalarRegistry* scalars, const ExternRegistry* externs,
                                 const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const std::vector<MapStep> operators = parallel_pipeline_operators(plan);
    const ir::Node* input_node = physical::parallel_input_node(plan);
    if (input_node == nullptr) {
        return std::unexpected("map pipeline: parallel mode without an input");
    }

    // Source strategy, decided here because it is a property of this run's
    // input rather than of the query's root. A decomposable deferred scan can
    // feed the run one unit at a time -- decode and maps in the same worker
    // task, the ordered ring feeding whatever is above -- instead of being
    // decoded whole and morselized. Probe scans keep their join-owned dynamic
    // filter timing (a null filter slot is what distinguishes them) and so do
    // not stream here.
    if (exec.stream_scans && input_node->kind() == ir::NodeKind::Scan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(*input_node);
        if (!registry.contains(scan.source_name())) {
            if (const auto* deferred = exec.deferred_scan(scan.source_name());
                deferred != nullptr && deferred->filter == nullptr) {
                auto units = deferred_scan_units(*deferred);
                if (units.size() > 1 && scan_pipeline_worker_count(units.size()) >= 2) {
                    return build_pipelined_scan(operators, true, *deferred, std::move(units),
                                                scalars, externs, exec);
                }
            }
        }
    }

    // Fused probe: when this pipeline's input is a streaming join, take its
    // build and its probe side and run the probe at the head of every worker
    // chain, instead of materializing the join's OUTPUT and morselizing that.
    // The probe side becomes the morsel source, so the join's output is never
    // assembled as a table at all -- it is produced a morsel at a time by the
    // same worker that then runs the maps over it. Two pipelines meeting at a
    // barrier, which is the shape this plan has been working toward.
    std::optional<JoinProbe> fused_probe;
    std::unique_ptr<Table> owned;
    if (input_node->kind() == ir::NodeKind::Join) {
        auto fused = try_take_join_probe(*input_node, registry, scalars, externs, exec, model_out);
        if (!fused.has_value()) {
            return std::unexpected(std::move(fused.error()));
        }
        if (fused->has_value()) {
            owned = std::make_unique<Table>(std::move((*fused)->probe_side));
            fused_probe = std::move((*fused)->probe);
        }
    }
    if (owned == nullptr) {
        auto input_op = build_operator(*input_node, registry, scalars, externs, exec, model_out);
        if (!input_op.has_value()) {
            return std::unexpected(std::move(input_op.error()));
        }
        auto input_tbl = materialize_operator(std::move(input_op.value()));
        if (!input_tbl.has_value()) {
            return std::unexpected(std::move(input_tbl.error()));
        }
        owned = std::make_unique<Table>(std::move(input_tbl.value()));
    }
    const std::size_t grain = morsel_grain(exec, owned->rows());
    const auto expected_morsels = partitioned_morsel_count(*owned, grain);
    // Morselize only when the work would actually fan out. Splitting earns its
    // cost by having several workers share it; with fewer than two the split,
    // the per-morsel gather and the merge concat are all paid for parallelism
    // that was already ruled out, so the whole-table chunk below is the right
    // shape.
    //
    // No time claim is attached to this: measured on its own it moved nothing
    // (`morsels` 92 -> 0 on q12/q14, wall time unchanged), because by the time
    // this runs the input has already been materialized. The run's real cost at
    // a budget of one is that materialize, and it is declined at the
    // construction seam in `build_physical_map_step` instead. What this does
    // earn is an honest counter -- see below -- and not doing work whose only
    // consumer is a worker that will not exist.
    const std::size_t worker_count =
        is_worth_morselizing(exec, owned->rows(), owned->columns.size())
            ? morsel_worker_count(exec, expected_morsels)
            : 0;
    const bool morselize = worker_count >= 2;
    if (exec.parallel_stats != nullptr) {
        auto& stats = *exec.parallel_stats;
        (morselize ? stats.parallel_pipelines : stats.serial_pipelines)
            .fetch_add(1, std::memory_order_relaxed);
        if (morselize) {
            // Only count morsels that are actually formed. Reporting the
            // would-be split for a run that takes the whole-table chunk is what
            // made this cost invisible: `morsels=115` on a query with
            // `parallel=0` reads as work done, not as work paid for and thrown
            // away.
            stats.morsels.fetch_add(expected_morsels, std::memory_order_relaxed);
        }
    }

    if (worker_count >= 2) {
        const auto head = (operators.empty() || fused_probe.has_value())
                              ? std::nullopt
                              : range_filter_head(operators.front(), *owned);
        if (exec.parallel_stats != nullptr && head.has_value()) {
            exec.parallel_stats->range_heads.fetch_add(1, std::memory_order_relaxed);
        }

        // A range-native filter can skip the merger entirely by presizing its
        // output — see TwoPhaseFilterOperator. Anything above it must be
        // metadata-only: a row-touching operator would need the per-morsel
        // chunks the two-phase form does not produce, but Project and Rename
        // copy no rows and so are simply run once over the finished output.
        const auto tail = std::span{operators}.subspan(head.has_value() ? 1 : 0);
        if (head.has_value() && std::ranges::all_of(tail, [](const MapStep& step) {
                return is_metadata_only_node(step.node->kind());
            })) {
            auto layout = build_filter_output_layout(*owned, head->project);
            // A missing projected column is left to the ordered merger below,
            // which reports it through the normal evaluation path.
            if (layout.has_value() && filter_gather_is_thread_safe(*owned, layout->src_of_dst)) {
                if (exec.parallel_stats != nullptr) {
                    exec.parallel_stats->two_phase_filters.fetch_add(1, std::memory_order_relaxed);
                }
                // The tail is metadata-only by the check above, so no step in
                // it carries a fused partner; its nodes are the whole story.
                std::vector<const ir::Node*> tail_nodes;
                tail_nodes.reserve(tail.size());
                for (const MapStep& step : tail) {
                    tail_nodes.push_back(step.node);
                }
                return std::make_unique<TwoPhaseFilterOperator>(
                    std::move(owned), *head->predicate, head->project != nullptr,
                    std::move(tail_nodes), scalars, std::move(layout.value()), grain,
                    expected_morsels, worker_count, process_worker_pool());
            }
        }

        std::vector<MorselWorkerChain> workers;
        workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            auto worker =
                build_morsel_worker_chain(operators, *owned, scalars, externs, exec,
                                          fused_probe.has_value() ? &*fused_probe : nullptr);
            if (!worker.has_value()) {
                return std::unexpected(std::move(worker.error()));
            }
            workers.push_back(std::move(worker.value()));
        }
        return std::make_unique<MorselPipelineOperator>(std::move(owned), std::move(workers), grain,
                                                        expected_morsels, process_worker_pool());
    }

    if (!morselize) {
        // Too little work to be worth splitting: run the chain over one
        // whole-table chunk. This is the plain serial path — same map
        // operators, same `preserve_empty_morsels = false`, one chunk in and
        // one chunk out — so it costs exactly what not forming a pipeline costs.
        // Morselizing here instead would add a per-morsel gather and a merge
        // concat to buy parallelism that was already judged not worth having.
        OperatorPtr serial = make_table_source(std::move(*owned));
        if (fused_probe.has_value()) {
            // Not worth morselizing, but the probe was already taken from the
            // join and there is nothing to give it back to: run it here, over
            // the whole probe side, with the maps above it as before.
            serial = std::make_unique<JoinProbeOperator>(std::move(serial), *fused_probe);
        }
        for (const MapStep& op_node : operators) {
            auto next = build_row_local_map_operator(op_node, std::move(serial), scalars, externs,
                                                     exec, false);
            if (!next.has_value()) {
                return std::unexpected("morsel pipeline: " + next.error());
            }
            serial = std::move(next.value());
        }
        return serial;
    }

    OperatorPtr chain = std::make_unique<PartitionedTableSource>(*owned, grain);
    for (const MapStep& op_node : operators) {
        auto next =
            build_row_local_map_operator(op_node, std::move(chain), scalars, externs, exec, true);
        if (!next.has_value()) {
            // The plan's step vocabulary only admits row-local map kinds.
            return std::unexpected("morsel pipeline: " + next.error());
        }
        chain = std::move(next.value());
    }

    chain = std::make_unique<SerialMorselOrderValidator>(std::move(chain), expected_morsels, grain);
    return std::make_unique<OwningMorselPipelineOperator>(std::move(owned), std::move(chain));
}

/// Streams a deferred lazy scan one source unit at a time instead of decoding
/// the whole source and handing it over as a single chunk.
///
/// This is Phase 1 of `plans/pipelined-execution-plan.md`. The decode it
/// performs is the same decode `materialize_deferred_scan` performs, with the
/// same pushdowns — projection, static conjuncts, the dynamic key membership
/// filter, and both fused scans, all restricted to the unit rather than
/// declined (see `LazyTable::project_where_unit`). What changes is only that
/// the rows arrive in pieces, which is the precondition for anything above the
/// scan ever running concurrently with it.
///
/// The plan is fixed once, at construction, for the reason `DeferredScanPlan`
/// documents: re-reading the shared filter slot per unit could apply to unit 3
/// a bound that units 0-2 never saw.
///
/// **Phase 2 (concurrent units).** Units are decoded a WINDOW at a time on
/// worker threads rather than one after another, and the window after the one
/// being served is already decoding. Phase 1 measured why: decoding units
/// serially cut total work (pool work on q01 fell 234ms -> 125ms) but raised
/// 8-core wall, because the pool saw one short burst per unit with a serial
/// phase between and occupancy fell to 0.14. Nothing was too small to
/// parallelize — there was just never more than one unit's worth of work
/// available at a time.
///
/// Decoding a unit on a worker is safe, and specifically so:
///
///   * `LazyTable::acquire_reader` hands each concurrent acquisition its OWN
///     reader product, under a mutex, and a product owns all the mutable
///     decoder state. That is exactly what the reader pool was built for.
///   * `project_where_unit` never WRITES `cache_` — a unit holds a fragment of
///     a column, so it must not — and concurrent reads of it are fine. This is
///     load-bearing now, not just a correctness nicety: routing any part of the
///     unit path back through `project()`, which does cache, would turn this
///     into a data race.
///   * Every inner parallel path (`parallel_readers`, `for_row_ranges`,
///     `filter_selection`) checks `on_worker_pool_thread()` and runs serial
///     inside a task, so the outer window is the only level of parallelism and
///     nothing submits from a worker into a saturated pool.
///
/// Ordering is preserved exactly: workers claim units from a shared cursor and
/// write only their own slot, and chunks are served in unit order with
/// `sequence` / `row_offset` assigned on the calling thread. The categorical
/// remap also stays on the calling thread, in unit order, because it folds each
/// chunk into a dictionary shared with every earlier one.
class DeferredScanSourceOperator final : public Operator {
   public:
    DeferredScanSourceOperator(const DeferredScan& scan, std::vector<SourceUnit> units,
                               const ExecutionContext& exec)
        : scan_(&scan),
          plan_(plan_deferred_scan(scan)),
          units_(std::move(units)),
          exec_(&exec),
          window_(unit_window(exec)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            if (served_ < ready_.size()) {
                auto& slot = ready_[served_++];
                if (!slot.has_value()) {
                    return std::unexpected(std::move(slot.error()));
                }
                auto chunk = emit(std::move(*slot));
                if (!chunk.has_value()) {
                    continue;  // the unit's rows were all filtered out
                }
                return std::optional<Chunk>{std::move(*chunk)};
            }
            // Out of decoded units: put the next window in flight and wait for
            // it.
            //
            // Dispatching the window AFTER this one here too, so the consumer's
            // work would overlap the next decode, is a MEASURED DEAD END. It
            // doubles how much of the source is decoded at once — peak RSS on a
            // 25-row-group scan went 161MB -> 244MB — and returns nothing,
            // because the consumer is a blocking operator that eats chunks
            // faster than they decode, so there is no consumer work to overlap
            // with. Real overlap needs a pipeline that keeps running while the
            // scan decodes, which is the rest of Phase 2, not a deeper queue.
            //
            // `inflight_pending_`, not `batch_`: a one-unit window decodes on
            // this thread and never submits, so a `batch_`-based test reports
            // "nothing in flight" and drops a window that has already been
            // decoded. That silently lost every unit after the first whenever
            // the query was not parallel, and every trailing single-unit window
            // when it was.
            if (!inflight_pending_) {
                if (dispatched_ >= units_.size()) {
                    // A stream must still carry its schema when every unit was
                    // empty after scan pushdown.  Without this carrier,
                    // MaterializeOperator sees end-of-stream as its first
                    // result and returns a column-less Table, unlike the
                    // equivalent eager scan (and unlike a filter above an
                    // ordinary table source).  Keep the first empty unit out
                    // of the normal pipeline -- several streaming operators
                    // intentionally do not consume empty chunks -- and emit it
                    // only when it is the sole result.
                    if (empty_schema_carrier_.has_value()) {
                        auto carrier = std::move(*empty_schema_carrier_);
                        empty_schema_carrier_.reset();
                        return std::optional<Chunk>{emit_schema_carrier(std::move(carrier))};
                    }
                    return std::optional<Chunk>{};
                }
                dispatch();
            }
            harvest();
        }
    }

   private:
    /// How many units to decode at once. One means serial, which is what a
    /// non-parallel query, a single-thread budget, and a call already running
    /// inside a pool task all get — the last because submitting from a worker
    /// deadlocks against a saturated pool.
    static auto unit_window(const ExecutionContext& exec) -> std::size_t {
        if (!exec.can_fan_out() || on_worker_pool_thread()) {
            return 1;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec.compute_budget();
        return std::max<std::size_t>(1, std::min(budget, pool.size()));
    }

    /// Put the next window of units in flight. Returns without blocking:
    /// `WorkerPool::submit` is asynchronous, which is what makes the overlap
    /// possible — and which is also why everything the body touches is a
    /// member rather than a local.
    void dispatch() {
        window_begin_ = dispatched_;
        window_count_ = std::min(window_, units_.size() - dispatched_);
        dispatched_ += window_count_;
        inflight_.clear();
        inflight_.resize(window_count_);
        inflight_pending_ = true;
        if (window_count_ == 1) {
            // Nothing to overlap and no reason to pay for a pool round trip.
            inflight_[0] = decode_unit(0);
            return;
        }
        cursor_.store(0, std::memory_order_relaxed);
        batch_ = process_worker_pool().submit(window_count_, [this](std::size_t /*worker*/) {
            for (std::size_t i = cursor_.fetch_add(1, std::memory_order_relaxed); i < window_count_;
                 i = cursor_.fetch_add(1, std::memory_order_relaxed)) {
                inflight_[i] = decode_unit(i);
            }
        });
    }

    /// One unit's decode, as run by a worker. Never throws out of the body: a
    /// `Batch` rethrows the first escaped exception at `wait()`, which would
    /// lose the other units' errors and unwind through the pool.
    auto decode_unit(std::size_t slot) -> std::expected<Table, std::string> {
        try {
            auto table =
                materialize_deferred_scan_unit(*scan_, plan_, units_[window_begin_ + slot], *exec_);
            if (table.has_value()) {
                normalize_time_index(*table);
            }
            return table;
        } catch (const std::exception& e) {
            return std::unexpected(std::string("streamed scan: ") + e.what());
        }
    }

    void harvest() {
        if (batch_.has_value()) {
            batch_->wait();
            batch_.reset();
        }
        ready_ = std::move(inflight_);
        inflight_.clear();
        inflight_pending_ = false;
        served_ = 0;
    }

    /// Turn a decoded unit into the chunk to hand upward, or nullopt when it
    /// carries no rows.
    ///
    /// A unit whose every row the scan's predicates rejected carries nothing.
    /// Skipping it is not just an optimization: an empty chunk with columns is
    /// a shape some operators would rather not meet, and dropping it changes no
    /// result. A column-less chunk is a different thing — it carries a row
    /// count for `count()` — and is kept.
    auto emit(Table table) -> std::optional<Chunk> {
        if (!table.columns.empty() && table.rows() == 0) {
            if (!empty_schema_carrier_.has_value()) {
                empty_schema_carrier_ = std::move(table);
            }
            return std::nullopt;
        }
        // A non-empty result makes a deferred carrier unnecessary.
        empty_schema_carrier_.reset();
        unify_categorical_dictionaries(table);
        Chunk chunk;
        const std::size_t rows = table.rows();
        chunk.set_properties(table.properties());
        chunk.columns = std::move(table.columns);
        if (chunk.columns.empty()) {
            chunk.logical_rows = table.logical_rows;
        }
        chunk.sequence = sequence_++;
        chunk.row_offset = emitted_rows_;
        emitted_rows_ += rows;
        return chunk;
    }

    /// Materialize the one schema carrier retained when scan pushdown rejected
    /// every row.  It follows the ordinary source identity convention: first
    /// chunk, at row zero.  Dictionary unification remains necessary because a
    /// zero-row categorical still carries dictionary identity as part of its
    /// schema.
    auto emit_schema_carrier(Table table) -> Chunk {
        unify_categorical_dictionaries(table);
        Chunk chunk;
        chunk.set_properties(table.properties());
        chunk.columns = std::move(table.columns);
        if (chunk.columns.empty()) {
            chunk.logical_rows = table.logical_rows;
        }
        chunk.sequence = sequence_++;
        chunk.row_offset = emitted_rows_;
        return chunk;
    }

    /// Remap every Categorical column onto a dictionary shared by all this
    /// source's chunks.
    ///
    /// Parquet writes one dictionary PER ROW GROUP, and a unit is one row
    /// group, so without this each chunk's codes would mean something different
    /// from the last one's — and the operators that compare dictionary identity
    /// to take a fast path (grouping, joins, the packed key encoder) would be
    /// comparing codes across dictionaries that disagree. The whole-file decode
    /// never had this problem because it merged the groups' dictionaries
    /// itself. `ChunkedParquetSourceOperator` solves it the same way.
    ///
    /// **One lookup per dictionary ENTRY, never per row.** Interning row by row
    /// is a string hash per row, and it does not announce itself: TPC-H's
    /// `l_returnflag` and `l_linestatus` are plain `string` in the Arrow schema
    /// and only become Categorical because the writer dictionary-encoded them,
    /// so a query that never mentions a categorical type still pays. Measured
    /// on q01, per-row interning cost 114ms of the scan's 160ms — the entire
    /// regression against the materialized path, on the calling thread where
    /// nothing could overlap it. A dictionary has a handful of entries and a
    /// unit has a million rows; the difference is the whole cost.
    void unify_categorical_dictionaries(Table& table) {
        using code_type = Column<Categorical>::code_type;
        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            auto* local = std::get_if<Column<Categorical>>(table.columns[i].column.get());
            if (local == nullptr) {
                continue;
            }
            if (cat_states_.size() <= i) {
                cat_states_.resize(table.columns.size());
            }
            auto& state = cat_states_[i];
            if (!state.has_value()) {
                state.emplace();
            }
            if (state->dictionary_ptr() == local->dictionary_ptr()) {
                continue;  // already speaks the shared dictionary
            }
            // Intern this chunk's dictionary into the shared one, reading back
            // the code each entry landed on. `clear()` drops the codes and
            // keeps the dictionary, which is exactly what an accumulator wants.
            const auto& dictionary = local->dictionary();
            state->clear();
            for (const auto& value : dictionary) {
                state->push_back(value);
            }
            std::vector<code_type> remap(dictionary.size());
            for (std::size_t entry = 0; entry < dictionary.size(); ++entry) {
                remap[entry] = state->code_at(entry);
            }
            state->clear();

            const auto& local_codes = local->codes();
            std::vector<code_type> codes(local_codes.size());
            for (std::size_t row = 0; row < local_codes.size(); ++row) {
                codes[row] = remap[static_cast<std::size_t>(local_codes[row])];
            }
            table.columns[i].column = std::make_shared<ColumnValue>(
                Column<Categorical>{state->dictionary_ptr(), state->index_ptr(), std::move(codes)});
        }
    }

    const DeferredScan* scan_;
    DeferredScanPlan plan_;
    std::vector<SourceUnit> units_;
    const ExecutionContext* exec_;
    std::vector<std::optional<Column<Categorical>>> cat_states_;

    /// Units decoded and waiting to be served, in unit order, and how many of
    /// them have been.
    std::vector<std::expected<Table, std::string>> ready_;
    std::size_t served_ = 0;
    /// First empty, column-bearing unit.  It becomes a schema carrier only if
    /// every unit was empty; otherwise empty units stay invisible to the
    /// streaming operators above this source.
    std::optional<Table> empty_schema_carrier_;
    /// The window currently being decoded. Workers write disjoint slots of
    /// this, so it must not be resized while `batch_` is live.
    std::vector<std::expected<Table, std::string>> inflight_;
    std::optional<WorkerPool::Batch> batch_;
    /// Whether `inflight_` holds a dispatched window awaiting harvest. Not
    /// derivable from `batch_`: a one-unit window is decoded inline.
    bool inflight_pending_ = false;
    std::atomic<std::size_t> cursor_{0};
    std::size_t window_begin_ = 0;
    std::size_t window_count_ = 0;

    std::size_t dispatched_ = 0;
    std::size_t emitted_rows_ = 0;
    std::uint64_t sequence_ = 0;
    std::size_t window_ = 1;
};

// A one-chunk source owned by one scan-pipeline worker. The worker replaces
// the pending chunk for every source unit it claims, then pulls the private
// row-local chain exactly once. Keeping the chain private is what makes its
// mutable per-operator state safe without locks.
class ScanPipelineSource final : public Operator {
   public:
    void set(Chunk chunk) { pending_ = std::move(chunk); }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!pending_.has_value()) {
            return std::optional<Chunk>{};
        }
        auto chunk = std::move(*pending_);
        pending_.reset();
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    std::optional<Chunk> pending_;
};

struct ScanPipelineWorker {
    ScanPipelineSource* source = nullptr;
    OperatorPtr chain;
};

[[nodiscard]] auto build_scan_pipeline_worker(const std::vector<MapStep>& operators,
                                              const ScalarRegistry* scalars,
                                              const ExternRegistry* externs,
                                              const ExecutionContext& exec)
    -> std::expected<ScanPipelineWorker, std::string> {
    auto source = std::make_unique<ScanPipelineSource>();
    ScanPipelineWorker worker{.source = source.get(), .chain = std::move(source)};
    for (const MapStep& op_node : operators) {
        auto next = build_row_local_map_operator(op_node, std::move(worker.chain), scalars, externs,
                                                 exec, true);
        if (!next.has_value()) {
            return std::unexpected("scan pipeline: " + next.error());
        }
        worker.chain = std::move(next.value());
    }
    return worker;
}

/// A bounded source-to-map pipeline.
///
/// Each worker claims one source unit, decodes it with all globally planned
/// pushdowns intact, immediately runs the row-local operator chain, and
/// publishes the result into a bounded ordered ring. The caller drains that
/// ring into the next blocking operator. Thus decode of unit N+1, row-local
/// work on unit N, and consumption of an earlier unit can all be live at once;
/// there is no materialized table or whole-window wait between those stages.
class PipelinedScanOperator final : public Operator {
   public:
    PipelinedScanOperator(const DeferredScan& scan, std::vector<SourceUnit> units,
                          std::vector<ScanPipelineWorker> workers, const ExecutionContext& exec,
                          WorkerPool& pool)
        : scan_(&scan),
          plan_(plan_deferred_scan(scan)),
          units_(std::move(units)),
          workers_(std::move(workers)),
          exec_(&exec),
          pool_(&pool),
          window_(std::max<std::size_t>(workers_.size() * 2, 2)),
          ring_(window_, workers_.size()) {}

    ~PipelinedScanOperator() override { cancel_and_join(); }

    PipelinedScanOperator(const PipelinedScanOperator&) = delete;
    auto operator=(const PipelinedScanOperator&) -> PipelinedScanOperator& = delete;
    PipelinedScanOperator(PipelinedScanOperator&&) = delete;
    auto operator=(PipelinedScanOperator&&) -> PipelinedScanOperator& = delete;

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (finished_) {
            return std::optional<Chunk>{};
        }
        start();

        while (next_sequence_ < units_.size()) {
            std::optional<Chunk> produced = ring_.take(next_sequence_);
            if (!produced.has_value()) {
                // Stopped before this unit: cancelled, failed, or out of
                // workers. The ring reports the lowest-sequence failure, so the
                // message does not depend on which worker lost a race.
                if (auto failure = ring_.failure(); failure.has_value()) {
                    return fail(std::move(*failure));
                }
                return fail(interrupt_requested() ? interrupt_message()
                                                  : "scan pipeline: missing output unit");
            }
            ++next_sequence_;
            Chunk chunk = std::move(*produced);
            if (!chunk.columns.empty() && chunk.rows() == 0) {
                if (!empty_schema_carrier_.has_value()) {
                    empty_schema_carrier_ = std::move(chunk);
                }
                continue;
            }

            empty_schema_carrier_.reset();
            normalize_categorical_dictionaries(chunk);
            restamp(chunk);
            return std::optional<Chunk>{std::move(chunk)};
        }

        finish_workers();
        finished_ = true;
        if (empty_schema_carrier_.has_value()) {
            auto carrier = std::move(*empty_schema_carrier_);
            empty_schema_carrier_.reset();
            normalize_categorical_dictionaries(carrier);
            restamp(carrier);
            return std::optional<Chunk>{std::move(carrier)};
        }
        return std::optional<Chunk>{};
    }

   private:
    void start() {
        if (started_) {
            return;
        }
        started_ = true;
        batch_ = pool_->submit(workers_.size(), [this](std::size_t id) { run_worker(id); });
    }

    void run_worker(std::size_t worker_id) noexcept {
        // However this worker leaves, it must stop counting as a producer, or
        // the consumer waits for a unit that is never coming.
        // One-shot scope guard: aggregate-initialised, never copied or moved.
        // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
        struct ExitGuard {
            OrderedChunkRing* ring;
            ~ExitGuard() { ring->producer_exited(); }
        } const guard{&ring_};

        std::size_t claimed = 0;
        try {
            auto& worker = workers_[worker_id];
            while (true) {
                const std::size_t sequence = cursor_.fetch_add(1, std::memory_order_relaxed);
                if (sequence >= units_.size()) {
                    return;
                }
                claimed = sequence;
                if (ring_.acquire(sequence) == OrderedChunkRing::Acquire::Abandon) {
                    return;
                }
                if (interrupt_requested()) {
                    ring_.cancel();
                    return;
                }

                auto result = run_unit(worker, sequence);
                if (!result.has_value()) {
                    ring_.record_error(sequence, std::move(result.error()));
                    return;
                }
                ring_.publish(sequence, std::move(*result));
            }
        } catch (const std::exception& error) {
            // Sequence-tagged like any other failure, so the reported message
            // obeys the same lowest-sequence rule. Composing it allocates and
            // the likeliest exception here is bad_alloc, hence the
            // allocation-free fallback underneath.
            try {
                ring_.record_error(claimed,
                                   "scan pipeline: worker exception: " + std::string(error.what()));
            } catch (...) {
                ring_.record_fault(claimed,
                                   "scan pipeline: worker exception (no memory to report it)");
            }
        } catch (...) {
            ring_.record_fault(claimed, "scan pipeline: worker threw a non-standard exception");
        }
    }

    [[nodiscard]] auto run_unit(ScanPipelineWorker& worker, std::size_t sequence)
        -> std::expected<Chunk, std::string> {
        auto decoded = materialize_deferred_scan_unit(*scan_, plan_, units_[sequence], *exec_);
        if (!decoded.has_value()) {
            return std::unexpected(std::move(decoded.error()));
        }
        normalize_time_index(*decoded);
        worker.source->set(table_to_chunk(
            std::move(*decoded),
            ChunkIdentity{.sequence = sequence, .row_offset = units_[sequence].start}));
        auto produced = worker.chain->next();
        if (!produced.has_value()) {
            return std::unexpected(std::move(produced.error()));
        }
        if (!produced->has_value()) {
            return std::unexpected("scan pipeline: row-local chain dropped a source unit");
        }
        Chunk chunk = std::move(**produced);
        if (chunk.sequence != sequence) {
            return std::unexpected("scan pipeline: row-local chain reordered a source unit");
        }
        return chunk;
    }

    void restamp(Chunk& chunk) noexcept {
        chunk.sequence = emitted_sequence_++;
        chunk.row_offset = emitted_rows_;
        emitted_rows_ += chunk.rows();
    }

    // Parquet dictionaries are local to row groups. Ordered publication is the
    // one serial point where chunks are remapped onto one shared dictionary,
    // preserving the existing streamed-source contract for downstream keys.
    void normalize_categorical_dictionaries(Chunk& chunk) {
        using code_type = Column<Categorical>::code_type;
        if (cat_states_.size() < chunk.columns.size()) {
            cat_states_.resize(chunk.columns.size());
        }
        for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
            auto* local = std::get_if<Column<Categorical>>(chunk.columns[i].column.get());
            if (local == nullptr) {
                continue;
            }
            auto& state = cat_states_[i];
            if (!state.has_value()) {
                state.emplace();
            }
            if (state->dictionary_ptr() == local->dictionary_ptr()) {
                continue;
            }
            const auto& dictionary = local->dictionary();
            state->clear();
            for (const auto& value : dictionary) {
                state->push_back(value);
            }
            std::vector<code_type> remap(dictionary.size());
            for (std::size_t entry = 0; entry < dictionary.size(); ++entry) {
                remap[entry] = state->code_at(entry);
            }
            state->clear();
            const auto& local_codes = local->codes();
            std::vector<code_type> codes(local_codes.size());
            for (std::size_t row = 0; row < local_codes.size(); ++row) {
                codes[row] = remap[static_cast<std::size_t>(local_codes[row])];
            }
            chunk.columns[i].column = std::make_shared<ColumnValue>(
                Column<Categorical>{state->dictionary_ptr(), state->index_ptr(), std::move(codes)});
        }
    }

    void finish_workers() {
        if (batch_.has_value()) {
            batch_->wait();
            batch_.reset();
        }
        if (validated_) {
            return;
        }
        validated_ = true;
        for (auto& worker : workers_) {
            auto trailing = worker.chain->next();
            if (!trailing.has_value()) {
                throw std::runtime_error(std::move(trailing.error()));
            }
            if (trailing->has_value()) {
                throw std::runtime_error("scan pipeline: unexpected trailing output");
            }
        }
    }

    void cancel_and_join() noexcept {
        ring_.cancel();
        try {
            finish_workers();
        } catch (...) {  // NOLINT(bugprone-empty-catch)
        }
    }

    [[nodiscard]] auto fail(std::string message)
        -> std::expected<std::optional<Chunk>, std::string> {
        finished_ = true;
        cancel_and_join();
        return std::unexpected(std::move(message));
    }

    const DeferredScan* scan_;
    DeferredScanPlan plan_;
    std::vector<SourceUnit> units_;
    std::vector<ScanPipelineWorker> workers_;
    const ExecutionContext* exec_;
    WorkerPool* pool_;
    std::size_t window_ = 2;
    // The same ordered handoff the morsel executor uses: one implementation of
    // the bounded, sequence-ordered producer/consumer shape.
    OrderedChunkRing ring_;
    std::vector<std::optional<Column<Categorical>>> cat_states_;
    std::optional<Chunk> empty_schema_carrier_;
    std::optional<WorkerPool::Batch> batch_;
    std::atomic<std::size_t> cursor_{0};
    std::size_t next_sequence_ = 0;
    std::size_t emitted_rows_ = 0;
    std::uint64_t emitted_sequence_ = 0;
    bool started_ = false;
    bool finished_ = false;
    bool validated_ = false;
};

/// A bounded asynchronous boundary between two pipeline segments.
///
/// The existing executor is pull-based, which is ideal for operator-local
/// state but normally means a parent cannot start its work until its child has
/// returned from `next()`. This stage retains that contract at both ends while
/// driving its child on a dedicated scheduler thread and holding at most two
/// ordered chunks between them. A breaker below the stage may therefore build
/// or probe the next chunk while a row-local parent (or the next breaker) is
/// working on the preceding one.
///
/// This deliberately does not borrow a WorkerPool thread. A streamed scan
/// already owns pool tasks, and putting the stage on that same fixed pool
/// reintroduces the saturated-pool deadlock that the scan producer's worker
/// reservation avoids. The query lease limits this to one query, while the
/// builder only inserts stages at breaker boundaries, so this is bounded by
/// plan depth rather than morsel count.
class PipelinedStageOperator final : public Operator {
   public:
    PipelinedStageOperator(OperatorPtr child, ExecutionProfileEntry* entry)
        : child_(std::move(child)), entry_(entry) {}

    ~PipelinedStageOperator() override { cancel_and_join(); }

    PipelinedStageOperator(const PipelinedStageOperator&) = delete;
    auto operator=(const PipelinedStageOperator&) -> PipelinedStageOperator& = delete;
    PipelinedStageOperator(PipelinedStageOperator&&) = delete;
    auto operator=(PipelinedStageOperator&&) -> PipelinedStageOperator& = delete;

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        start();
        std::expected<std::optional<Chunk>, std::string> result = std::optional<Chunk>{};
        {
            std::unique_lock lock(mutex_);
            {
                // Idle, not serial work: waiting on the stage's producer thread.
                const RingWaitScope ring_wait;
                ready_.wait(lock, [this] {
                    return !ready_chunks_.empty() || producer_done_ || failure_.has_value();
                });
            }
            if (failure_.has_value()) {
                result = std::unexpected(std::move(*failure_));
                failure_.reset();
            } else if (!ready_chunks_.empty()) {
                result = std::optional<Chunk>{std::move(ready_chunks_.front())};
                ready_chunks_.pop_front();
            } else {
                done_ = true;
            }
        }
        space_.notify_one();
        if (!result.has_value()) {
            cancel_and_join();
            return result;
        }
        if (done_) {
            join();
        }
        return result;
    }

   private:
    static constexpr std::size_t kCapacity = 2;

    void start() {
        if (started_) {
            return;
        }
        started_ = true;
        producer_ = std::thread([this] {
            // Declare what kind of thread this is. It is deliberately NOT a pool
            // worker — it is long-lived and parks on the consumer's ring
            // backpressure, which a fixed-size pool cannot host — but it must
            // still be countable, and the profiler must not charge its work to
            // the calling thread's self time.
            const StageThreadScope stage_thread;
            produce();
        });
    }

    void produce() noexcept {
        try {
            while (true) {
                {
                    std::unique_lock lock(mutex_);
                    // Backpressure: the producer has filled the ring and the
                    // consumer has not drained it. Idle time on a runtime-owned
                    // thread, and the mirror image of the consumer's park at the
                    // other end of the same ring — a large value here means the
                    // CONSUMER is the bottleneck. `RingWaitScope` routes it to
                    // this thread's stage ledger.
                    const RingWaitScope ring_wait;
                    space_.wait(lock,
                                [this] { return cancelled_ || ready_chunks_.size() < kCapacity; });
                    if (cancelled_) {
                        return;
                    }
                }

                auto next = [&] {
                    // Scoped per pull, mirroring ProfiledOperator, so the
                    // backpressure wait above stays outside it.
                    const ExecutionProfileScope scope(entry_, ProfilePhase::Next);
                    return child_->next();
                }();
                if (!next.has_value()) {
                    fail(std::move(next.error()));
                    return;
                }
                if (!next->has_value()) {
                    {
                        const std::scoped_lock lock(mutex_);
                        producer_done_ = true;
                    }
                    ready_.notify_all();
                    return;
                }

                {
                    const std::scoped_lock lock(mutex_);
                    if (cancelled_) {
                        return;
                    }
                    // There is one producer. The capacity check immediately
                    // before `child_->next()` therefore reserves this slot:
                    // only the consumer can change the queue size meanwhile.
                    ready_chunks_.push_back(std::move(**next));
                }
                ready_.notify_one();
            }
        } catch (const std::exception& error) {
            fail("pipeline stage: producer exception: " + std::string(error.what()));
        } catch (...) {
            fail("pipeline stage: producer threw a non-standard exception");
        }
    }

    void fail(std::string message) noexcept {
        {
            const std::scoped_lock lock(mutex_);
            if (!failure_.has_value()) {
                failure_ = std::move(message);
            }
            producer_done_ = true;
        }
        ready_.notify_all();
    }

    void join() noexcept {
        if (producer_.joinable()) {
            producer_.join();
        }
    }

    void cancel_and_join() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        ready_.notify_all();
        space_.notify_all();
        join();
    }

    OperatorPtr child_;
    // The operator this stage was built for. `profile_operator` wraps the
    // STAGE, not the child, so the producer thread runs unwrapped code and had
    // no profile frame at all: anything it submitted to the pool was attributed
    // to no operator, and a fully parallel scan read as zero pool work. The
    // producer pushes a scope for this entry so its work has an owner.
    ExecutionProfileEntry* entry_ = nullptr;
    std::thread producer_;
    std::deque<Chunk> ready_chunks_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::condition_variable space_;
    std::optional<std::string> failure_;
    bool started_ = false;
    bool producer_done_ = false;
    bool cancelled_ = false;
    bool done_ = false;
};

[[nodiscard]] auto make_pipelined_stage(OperatorPtr child, const ExecutionContext& exec,
                                        ExecutionProfileEntry* entry) -> OperatorPtr {
    if (!exec.can_fan_out() || on_worker_pool_thread() || process_worker_pool().size() < 2) {
        return child;
    }
    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->pipelined_stages.fetch_add(1, std::memory_order_relaxed);
    }
    return std::make_unique<PipelinedStageOperator>(std::move(child), entry);
}

[[nodiscard]] auto make_pipelined_stage_if(OperatorPtr child, bool eligible,
                                           const ExecutionContext& exec,
                                           ExecutionProfileEntry* entry) -> OperatorPtr {
    return eligible ? make_pipelined_stage(std::move(child), exec, entry) : std::move(child);
}

[[nodiscard]] auto scan_pipeline_worker_count(std::size_t unit_count) -> std::size_t {
    auto& pool = process_worker_pool();
    if (pool.size() < 2) {
        // With one pool thread there is no worker to reserve for a downstream
        // operator batch. Running the producer there can deadlock as soon as a
        // breaker submits work and waits, so keep the serial window source.
        return 0;
    }
    // The decode pipeline is the one consumer sized against the POOL rather
    // than the compute budget: it is what the extra threads were added for.
    // `ExecutionContext::parallel_threads` deliberately does not cap it;
    // configure_parallel_from_env uses that field for compute only.
    const std::size_t budget = pool.size();
    std::size_t workers = std::min({budget, pool.size(), unit_count});
    // A spare thread is only necessary when every pool thread could remain
    // parked behind ring backpressure. The ring holds 2W results and workers
    // have already claimed at most another W units, so a source of at most 3W
    // units necessarily lets one worker exit after the first chunk is released.
    // Smaller sources (the common Parquet shape) keep the full decode budget;
    // longer sources reserve one thread for downstream batches.
    if (workers == pool.size() && unit_count > workers * 3) {
        --workers;
    }
    return workers;
}

[[nodiscard]] auto build_pipelined_scan(const std::vector<MapStep>& operators,
                                        bool count_as_pipeline, const DeferredScan& scan,
                                        std::vector<SourceUnit> units,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string> {
    const std::size_t worker_count = scan_pipeline_worker_count(units.size());
    if (worker_count == 0) {
        return std::unexpected("scan pipeline requires a worker");
    }
    std::vector<ScanPipelineWorker> workers;
    workers.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        auto worker = build_scan_pipeline_worker(operators, scalars, externs, exec);
        if (!worker.has_value()) {
            return std::unexpected(std::move(worker.error()));
        }
        workers.push_back(std::move(*worker));
    }
    if (exec.parallel_stats != nullptr) {
        if (count_as_pipeline) {
            exec.parallel_stats->parallel_pipelines.fetch_add(1, std::memory_order_relaxed);
        }
        exec.parallel_stats->morsels.fetch_add(units.size(), std::memory_order_relaxed);
        exec.parallel_stats->pipelined_scans.fetch_add(1, std::memory_order_relaxed);
    }
    return std::make_unique<PipelinedScanOperator>(scan, std::move(units), std::move(workers), exec,
                                                   process_worker_pool());
}

/// A breaker only earns a scheduler thread when its probe input can actually
/// publish more than one source unit. Registered tables and one-unit readers
/// return a single chunk, so staging them merely moves the same serial call to
/// another thread. Keep this structural test at build time: it avoids putting
/// a speculative thread on the hot path and makes the queue capacity an
/// overlap buffer rather than an accidental materialization boundary.
[[nodiscard]] auto has_multi_unit_deferred_scan(const ir::Node& node, const TableRegistry& registry,
                                                const ExecutionContext& exec) -> bool {
    if (node.kind() == ir::NodeKind::Scan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(node);
        if (registry.contains(scan.source_name())) {
            return false;
        }
        const auto* deferred = exec.deferred_scan(scan.source_name());
        return deferred != nullptr && deferred->filter == nullptr &&
               deferred_scan_units(*deferred).size() > 1;
    }
    return std::ranges::any_of(node.children(), [&](const ir::NodePtr& child) {
        return has_multi_unit_deferred_scan(*child, registry, exec);
    });
}

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
        return build_map_pipeline_parallel(plan, registry, scalars, externs, exec, model_out);
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
/// other, expressed for now as the streaming operators that already implement
/// exactly that. The kernel-pipeline plan's Phase 4 item 1.
///
/// The three branches are the ones that used to sit in `build_operator_impl`'s
/// per-kind switch, moved rather than rewritten -- which is the whole point of
/// this slice. Construction lives with the plan, the decisions are the same
/// ones `plan_join` already relayed, and the operators are untouched. The
/// backlog moves because a join is now executed *by* the physical plan, the
/// same sense in which a migrated map chain is: through a plan-owned builder
/// rather than the per-kind switch. What is still ahead is decomposing the
/// build and the probe into separate pipeline stages with a barrier between
/// them, so a probe can be a step inside a map pipeline.

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

/// Run a join's build phase here, at plan-execution time, instead of leaving it
/// to fire inside the probe's first `next()`.
///
/// This is what makes the build *scheduled*: it has a caller that is not the
/// probe, and its completion is a point in the plan rather than a flag the probe
/// checks. `IBEX_JOIN_BUILD_LAZY=1` restores the old timing, which is both a kill
/// switch and the A/B handle for the one thing this changes that is not free --
/// when the build side's child is drained.
auto scheduled_join_build(std::unique_ptr<ChunkedInnerJoinOperator> op)
    -> std::expected<OperatorPtr, std::string> {
    static const bool lazy = std::getenv("IBEX_JOIN_BUILD_LAZY") != nullptr;
    if (lazy) {
        return OperatorPtr{std::move(op)};
    }
    if (auto err = op->run_build()) {
        return std::unexpected(std::move(*err));
    }
    auto probe_input = op->take_hash_probe_input();
    if (!probe_input.has_value()) {
        return std::unexpected(std::move(probe_input.error()));
    }
    return build_hash_probe_operator(std::move(*probe_input));
}

auto build_physical_join(const physical::Plan& plan, const ir::Node& node,
                         const TableRegistry& registry, const ScalarRegistry* scalars,
                         const ExternRegistry* externs, const ExecutionContext& exec,
                         ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    const auto& join = ir::node_cast<ir::JoinNode>(node);
    const physical::JoinPlan& jp = plan.join;
    if (jp.branch == physical::JoinBranch::SemiAnti) {
        const bool stage_probe = has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
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
        return make_pipelined_stage_if(std::make_unique<ChunkedSemiAntiJoinOperator>(
                                           std::move(left_op.value()), std::move(right.value()),
                                           join.kind(), &join.keys(), &exec),
                                       stage_probe, exec,
                                       execution_profile_entry(exec.execution_profile, node));
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
        const bool stage_probe = has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
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
            auto built = scheduled_join_build(std::make_unique<ChunkedInnerJoinOperator>(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns));
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return make_pipelined_stage_if(std::move(*built), stage_probe, exec,
                                           execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = scheduled_join_build(std::make_unique<ChunkedInnerJoinOperator>(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns));
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return make_pipelined_stage_if(std::move(*built), stage_probe, exec,
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
        const bool stage_probe = has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
        const auto probe = deferred_probe_scan_of(*join.children()[1], exec);
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        if (probe.scan != nullptr) {
            auto built = scheduled_join_build(std::make_unique<ChunkedInnerJoinOperator>(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns));
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return make_pipelined_stage_if(std::move(*built), stage_probe, exec,
                                           execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = scheduled_join_build(std::make_unique<ChunkedInnerJoinOperator>(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns));
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return make_pipelined_stage_if(std::move(*built), stage_probe, exec,
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

auto build_migrated_physical_operator(const physical::Plan& plan, const ir::Node& node,
                                      const TableRegistry& registry, const ScalarRegistry* scalars,
                                      const ExternRegistry* externs, const ExecutionContext& exec,
                                      ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    if (!plan.migrated) {
        return std::unexpected("physical executor: plan does not migrate its root");
    }
    if (plan.root != &node) {
        return std::unexpected("physical executor: plan root does not match execution root");
    }
    if (node.kind() == ir::NodeKind::Head) {
        physical::note_map_pipeline_executed();
        return build_physical_head(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Tail) {
        physical::note_map_pipeline_executed();
        return build_physical_tail(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::TopK) {
        physical::note_map_pipeline_executed();
        return build_physical_topk(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::FilterHead || node.kind() == ir::NodeKind::FilterTail) {
        physical::note_map_pipeline_executed();
        return build_physical_filter_head_tail(node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Distinct) {
        physical::note_map_pipeline_executed();
        return build_physical_distinct(plan, node, registry, scalars, externs, exec, model_out);
    }
    if (node.kind() == ir::NodeKind::Order) {
        physical::note_map_pipeline_executed();
        return build_physical_order(node, registry, scalars, externs, exec, model_out);
    }
    if (plan.aggregate.describes) {
        physical::note_map_pipeline_executed();
        return build_physical_aggregate(plan, node, registry, scalars, externs, exec, model_out);
    }
    if (plan.join.describes) {
        physical::note_map_pipeline_executed();
        return build_physical_join(plan, node, registry, scalars, externs, exec, model_out);
    }
    // Every migrated map plan, both modes: the composer walks the chain and
    // hands the morsel run off at its boundary, and that run picks its own
    // source strategy.
    if (plan.mode != physical::PipelineMode::MorselParallel || !exec.can_fan_out()) {
        physical::note_map_pipeline_executed();
    }
    return build_physical_map_step(plan, 0, registry, scalars, externs, exec, model_out);
}

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
                    if (exec.can_fan_out() && scan_pipeline_worker_count(units.size()) > 0) {
                        return build_pipelined_scan({}, false, *deferred, std::move(units), scalars,
                                                    externs, exec);
                    }
                    return std::make_unique<DeferredScanSourceOperator>(*deferred, std::move(units),
                                                                        exec);
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
        return build_row_local_map_operator(node, std::move(child_op.value()), scalars, externs,
                                            exec, false);
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
        return build_row_local_map_operator(node, std::move(child_op.value()), scalars, externs,
                                            exec, false);
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
        return build_row_local_map_operator(node, std::move(child_op.value()), scalars, externs,
                                            exec, false);
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

    if (node.kind() == ir::NodeKind::Columns) {
        if (node.children().empty()) {
            return std::unexpected("columns node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, exec, model_out,
                                                  [](Table input) { return columns_table(input); });
    }

    if (node.kind() == ir::NodeKind::Melt) {
        const auto& mn = ir::node_cast<ir::MeltNode>(node);
        if (mn.children().empty()) {
            return std::unexpected("melt node missing child");
        }
        return build_unary_materializing_operator(
            *mn.children().front(), registry, scalars, externs, exec, model_out,
            [&](Table input) { return melt_table(input, mn.id_columns(), mn.measure_columns()); });
    }

    if (node.kind() == ir::NodeKind::Dcast) {
        const auto& dn = ir::node_cast<ir::DcastNode>(node);
        if (dn.children().empty()) {
            return std::unexpected("dcast node missing child");
        }
        return build_unary_materializing_operator(
            *dn.children().front(), registry, scalars, externs, exec, model_out, [&](Table input) {
                return dcast_table(input, dn.pivot_column(), dn.value_column(), dn.row_keys());
            });
    }

    if (node.kind() == ir::NodeKind::Cov) {
        if (node.children().empty()) {
            return std::unexpected("cov node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, exec, model_out,
                                                  [](Table input) { return cov_table(input); });
    }

    if (node.kind() == ir::NodeKind::Corr) {
        if (node.children().empty()) {
            return std::unexpected("corr node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, exec, model_out,
                                                  [](Table input) { return corr_table(input); });
    }

    if (node.kind() == ir::NodeKind::Transpose) {
        if (node.children().empty()) {
            return std::unexpected("transpose node missing child");
        }
        return build_unary_materializing_operator(
            *node.children().front(), registry, scalars, externs, exec, model_out,
            [](Table input) { return transpose_table(input); });
    }

    if (node.kind() == ir::NodeKind::Join) {
        const auto& join = ir::node_cast<ir::JoinNode>(node);
        if (join.children().size() != 2) {
            return std::unexpected("join node expects exactly two children");
        }
        // Only the materializing join reaches here now. A streaming one is a
        // migrated plan and was built by `build_physical_join` at the seam
        // above, the same way a migrated map chain never reaches this switch.
        const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
        return build_binary_materializing_operator(
            *join.children()[0], *join.children()[1], registry, scalars, externs, exec, model_out,
            [&](Table left, Table right) {
                return join_table_impl(left, right, join.kind(), join.keys(), pred, scalars,
                                       compute_mask, join.suffix(), join.pending_order(),
                                       join.null_match(), join.expect(), join.take(), &exec);
            });
    }

    if (node.kind() == ir::NodeKind::Matmul) {
        if (node.children().size() != 2) {
            return std::unexpected("matmul node expects exactly two children");
        }
        return build_binary_materializing_operator(
            *node.children()[0], *node.children()[1], registry, scalars, externs, exec, model_out,
            [](Table left, Table right) { return matmul_table(left, right); });
    }

    if (node.kind() == ir::NodeKind::Update) {
        const auto& update = ir::node_cast<ir::UpdateNode>(node);
        if (update.children().empty()) {
            return std::unexpected("update node missing child");
        }
        if (update.guard() != nullptr) {
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, exec, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    return apply_guarded_update(std::move(input), update, scalars, externs, exec);
                });
        }
        if (!update.group_by().empty()) {
            const bool all_rank = std::all_of(
                update.fields().begin(), update.fields().end(), [](const ir::FieldSpec& f) {
                    return std::holds_alternative<ir::RankExpr>(f.expr.node);
                });
            if (!all_rank && update.tuple_fields().empty()) {
                return build_unary_materializing_operator(
                    *update.children().front(), registry, scalars, externs, exec, model_out,
                    [&](Table input) -> std::expected<Table, std::string> {
                        return grouped_update_table(std::move(input), update.fields(),
                                                    update.group_by(), scalars, externs, exec);
                    });
            }
            if (!all_rank || !update.tuple_fields().empty()) {
                return std::unexpected(
                    "update + by: tuple-bound fields are not yet supported in grouped updates");
            }
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, exec, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    Table result = std::move(input);
                    for (const auto& field : update.fields()) {
                        const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node);
                        auto res = evaluate_rank_column(result, *rank, update.group_by(), exec);
                        if (!res) {
                            return std::unexpected(res.error());
                        }
                        if (res->validity.has_value()) {
                            result.add_column(field.alias, std::move(res->column),
                                              std::move(*res->validity));
                        } else {
                            result.add_column(field.alias, std::move(res->column));
                        }
                    }
                    return std::expected<Table, std::string>{std::move(result)};
                });
        }
        // Route to a streaming ChunkedUpdateOperator when every field is
        // row-local and there are no table-valued tuple assignments.
        const bool all_row_local =
            std::all_of(update.fields().begin(), update.fields().end(),
                        [](const ir::FieldSpec& f) { return is_row_local_update_expr(f.expr); });
        if (all_row_local && update.tuple_fields().empty()) {
            auto child_op = build_operator(*update.children().front(), registry, scalars, externs,
                                           exec, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            return std::make_unique<ChunkedUpdateOperator>(
                std::move(child_op.value()), &update.fields(), scalars, externs, exec);
        }
        auto child = build_unary_materializing_operator(
            *update.children().front(), registry, scalars, externs, exec, model_out,
            [&](Table input) {
                return update_table(std::move(input), update.fields(), scalars, externs, exec);
            });
        if (!child.has_value()) {
            return std::unexpected(std::move(child.error()));
        }
        auto result = materialize_operator(std::move(child.value()));
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        for (const auto& tspec : update.tuple_fields()) {
            auto src = interpret_node(*tspec.source, registry, scalars, externs, exec);
            if (!src.has_value()) {
                return std::unexpected(std::move(src.error()));
            }
            if (tspec.aliases.empty()) {
                for (const auto& entry : src->columns) {
                    if (entry.validity) {
                        result->add_column(entry.name, *entry.column, *entry.validity);
                    } else {
                        result->add_column(entry.name, *entry.column);
                    }
                }
            } else {
                if (src->columns.size() != tspec.aliases.size()) {
                    return std::unexpected(
                        "tuple assignment: expected " + std::to_string(tspec.aliases.size()) +
                        " column(s), got " + std::to_string(src->columns.size()));
                }
                for (std::size_t i = 0; i < tspec.aliases.size(); ++i) {
                    const auto& entry = src->columns[i];
                    if (entry.validity) {
                        result->add_column(tspec.aliases[i], *entry.column, *entry.validity);
                    } else {
                        result->add_column(tspec.aliases[i], *entry.column);
                    }
                }
            }
        }
        return make_table_source(std::move(result.value()));
    }

    if (node.kind() == ir::NodeKind::Resample) {
        const auto& rs = ir::node_cast<ir::ResampleNode>(node);
        if (node.children().empty()) {
            return std::unexpected("resample node missing child");
        }
        return build_unary_materializing_operator(
            *node.children().front(), registry, scalars, externs, exec, model_out,
            [&](Table input) {
                return resample_table(input, rs.duration(), rs.group_by(), rs.aggregations());
            });
    }

    if (node.kind() == ir::NodeKind::Window) {
        const auto& win = ir::node_cast<ir::WindowNode>(node);
        if (node.children().empty()) {
            return std::unexpected("window node missing child");
        }
        const ir::Node& child_node = *node.children().front();
        if (child_node.kind() != ir::NodeKind::Update) {
            return std::unexpected(
                "window: only 'update' is currently supported inside a window block");
        }
        const auto& update_node = ir::node_cast<ir::UpdateNode>(child_node);
        if (child_node.children().empty()) {
            return std::unexpected("window: update node missing child");
        }
        auto source_op = build_operator(*child_node.children().front(), registry, scalars, externs,
                                        exec, model_out);
        if (!source_op.has_value()) {
            return std::unexpected(std::move(source_op.error()));
        }
        auto source = materialize_operator(std::move(source_op.value()));
        if (!source.has_value()) {
            return std::unexpected(std::move(source.error()));
        }
        if (!source->time_index().has_value()) {
            return std::unexpected(
                "window requires a TimeFrame — use as_timeframe() to designate a timestamp column");
        }
        auto result =
            update_node.group_by().empty()
                ? windowed_update_table(std::move(source.value()), update_node.fields(),
                                        win.duration(), scalars, externs, exec, win.aligned())
                : grouped_windowed_update_table(std::move(source.value()), update_node.fields(),
                                                win.duration(), update_node.group_by(), scalars,
                                                externs, exec, win.aligned());
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        if (win.select_only()) {
            // `window` + `select`: keep only the time index, group keys, and the
            // listed fields (time index first so the result stays a TimeFrame).
            std::vector<ir::ColumnRef> keep;
            robin_hood::unordered_set<std::string> seen;
            auto keep_col = [&](const std::string& name) {
                if (seen.insert(name).second) {
                    keep.push_back(ir::ColumnRef{.name = name});
                }
            };
            if (result->time_index().has_value()) {
                keep_col(*result->time_index());
            }
            for (const auto& key : update_node.group_by()) {
                keep_col(key.name);
            }
            for (const auto& field : update_node.fields()) {
                keep_col(field.alias);
            }
            // A grouped window leaves the rows group-major, and `project_table`
            // preserves that: it derives with `RowTransform::Preserve`, which
            // carries `grouped_by` through and drops the ordering only if the
            // projection removes one of its keys, and the TimeFrame invariant
            // leaves a group-major ordering alone rather than rewriting it to
            // the (false) "time index ascending".
            auto projected = project_table(result.value(), keep);
            if (!projected.has_value()) {
                return std::unexpected(std::move(projected.error()));
            }
            result = std::move(projected);
        }
        return make_table_source(std::move(result.value()));
    }

    if (node.kind() == ir::NodeKind::AsTimeframe) {
        const auto& atf = ir::node_cast<ir::AsTimeframeNode>(node);
        if (node.children().empty()) {
            return std::unexpected("as_timeframe node missing child");
        }
        auto child_op =
            build_operator(*node.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedAsTimeframeOperator>(std::move(child_op.value()),
                                                            atf.column(), exec);
    }

    if (node.kind() == ir::NodeKind::Model) {
        const auto& mn = ir::node_cast<ir::ModelNode>(node);
        if (mn.children().empty()) {
            return std::unexpected("model node missing child");
        }
        auto child_op =
            build_operator(*mn.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        auto input = materialize_operator(std::move(child_op.value()));
        if (!input.has_value()) {
            return std::unexpected(std::move(input.error()));
        }
        auto result =
            fit_model(input.value(), mn.formula(), mn.method(), mn.params(), scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        // Linear methods expose coefficients; tree models expose importance;
        // unsupervised models (e.g. kmeans) have neither, so fall back to the
        // per-row fitted output (e.g. cluster ids).
        Table primary = !result.value().coefficients.columns.empty() ? result.value().coefficients
                        : !result.value().importance.columns.empty() ? result.value().importance
                                                                     : result.value().fitted_values;
        if (model_out != nullptr) {
            *model_out = std::move(result.value());
        }
        return make_table_source(std::move(primary));
    }

    if (node.kind() == ir::NodeKind::Construct || node.kind() == ir::NodeKind::Stream) {
        auto table = interpret_node(node, registry, scalars, externs, exec, model_out);
        if (!table.has_value()) {
            return std::unexpected(std::move(table.error()));
        }
        return make_table_source(std::move(table.value()));
    }

    if (node.kind() == ir::NodeKind::Program) {
        const auto& program = ir::node_cast<ir::ProgramNode>(node);
        auto preamble = execute_program_preamble(program.preamble(), scalars, externs);
        if (!preamble.has_value()) {
            return std::unexpected(std::move(preamble.error()));
        }
        return build_operator(program.main_node(), registry, scalars, externs, exec, model_out);
    }

    // Remaining node kinds fall through to interpret_node. Scan is already
    // handled as a source by the caller.
    auto table = interpret_node(node, registry, scalars, externs, exec, model_out);
    if (!table.has_value()) {
        return std::unexpected(std::move(table.error()));
    }
    return make_table_source(std::move(table.value()));
}

}  // namespace

auto build_operator_from_physical_plan(const physical::Plan& plan, const ir::Node& node,
                                       const TableRegistry& registry, const ScalarRegistry* scalars,
                                       const ExternRegistry* externs, const ExecutionContext& exec,
                                       ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    return build_migrated_physical_operator(plan, node, registry, scalars, externs, exec,
                                            model_out);
}

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
