// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// chunked.cpp — streaming (chunked) operator pipeline: per-chunk operators,
// rank evaluation, extern-call execution, and build_operator plan construction.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/format.hpp>
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
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <pdqsort.h>
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
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "model_internal.hpp"
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

// Whether a streamed aggregate slot has enough observations to be non-null.
// Mirrors the materializing aggregate's `agg_result_is_valid`.
auto chunked_agg_valid(ir::AggFunc func, const AggSlotCore& slot) -> bool {
    switch (func) {
        case ir::AggFunc::Mean:
            return slot.count > 0;
        case ir::AggFunc::Sum:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::First:
        case ir::AggFunc::Last:
            return slot.present();
        case ir::AggFunc::Stddev:
            return slot.count >= 2;
        case ir::AggFunc::Skew:
            return slot.count >= 3;
        case ir::AggFunc::Kurtosis:
            return slot.count >= 4;
        default:  // Count
            return true;
    }
}

// Whether a streamed aggregate carries a validity bitmap at all (Count never
// produces nulls; the value-bearing aggs may).
auto chunked_agg_tracks_validity(ir::AggFunc func) -> bool {
    switch (func) {
        case ir::AggFunc::Sum:
        case ir::AggFunc::Mean:
        case ir::AggFunc::Min:
        case ir::AggFunc::Max:
        case ir::AggFunc::First:
        case ir::AggFunc::Last:
        case ir::AggFunc::Stddev:
        case ir::AggFunc::Skew:
        case ir::AggFunc::Kurtosis:
            return true;
        default:
            return false;
    }
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
        auto projected = kernel::project_chunk(std::move(*chunk_res.value()), *columns_);
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
        Chunk input = std::move(*chunk_res.value());
        const kernel::ChunkView view(input);
        std::vector<kernel::MappedChunkColumn> map;
        map.reserve(view.columns());
        for (const auto& spec : *renames_) {
            if (!view.find_column(spec.old_name).has_value()) {
                return std::unexpected("rename: column not found: " + spec.old_name);
            }
        }
        for (std::size_t pos = 0; pos < view.columns(); ++pos) {
            std::string name = view.entry(pos).name;
            for (const auto& spec : *renames_) {
                if (spec.old_name == name) {
                    name = spec.new_name;
                    break;
                }
            }
            map.push_back({.source_position = pos, .name = std::move(name)});
        }
        const auto props = TableProperties::derive(
            view.properties(),
            [&](const std::string& name) -> KeyFate {
                for (const auto& spec : *renames_) {
                    if (spec.old_name == name) {
                        return KeyFate::kept(spec.new_name);
                    }
                }
                return KeyFate::kept(name);
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
    const FlatCol* solo_key = radix_order ? &order_flat[0] : nullptr;
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
/// Shared by `ChunkedDistinctOperator` and `ChunkedAggregateOperator`: both
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
struct PackedKeyEncoder {
    // MSVC has no __uint128_t. This is only a packed identity key, so an array
    // of words is both portable and avoids pulling a compiler-specific integer
    // type into the packed key path.
    template <std::size_t Words>
    struct PackedWords {
        std::array<std::uint64_t, Words> w{};

        [[nodiscard]] friend auto operator==(const PackedWords&, const PackedWords&)
            -> bool = default;
    };
    template <std::size_t Words>
    struct PackedWordsHash {
        auto operator()(const PackedWords<Words>& value) const noexcept -> std::size_t {
            std::uint64_t acc = 0;
            for (const auto word : value.w) {
                acc ^= word + 0x9e3779b97f4a7c15ULL + (acc << 6U) + (acc >> 2U);
            }
            return static_cast<std::size_t>(acc);
        }
    };
    using Packed128 = PackedWords<2>;
    using Packed256 = PackedWords<4>;

    /// OR `cell` into the packed key at bit offset `shift`. A cell never spans
    /// more than two words because no cell is wider than 64 bits.
    template <std::size_t Words>
    static void splice(PackedWords<Words>& key, std::uint64_t cell, unsigned shift) {
        const unsigned word = shift / 64U;
        const unsigned off = shift % 64U;
        key.w[word] |= cell << off;
        // `cell >> 64` is UB, so the carry into the next word is only taken when
        // the cell actually straddles the boundary.
        if (off != 0 && word + 1 < Words) {
            key.w[word + 1] |= cell >> (64U - off);
        }
    }

    /// The exact inverse of `splice`/the single-word shift in `pack_row`:
    /// recover the `width_bits`-wide cell that was spliced in at bit offset
    /// `shift`. Only ever needed to decode an ALREADY-PACKED key back into its
    /// per-column values (fast-path migration); packing itself never reads a
    /// cell back out, so this has no hot-path cost.
    template <typename Packed>
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] static auto extract_cell(const Packed& key, unsigned shift, unsigned width_bits)
        -> std::uint64_t {
        const std::uint64_t mask =
            width_bits >= 64U ? ~std::uint64_t{0} : ((std::uint64_t{1} << width_bits) - 1);
        if constexpr (std::is_same_v<Packed, std::uint64_t>) {
            return (key >> shift) & mask;
        } else {
            const unsigned word = shift / 64U;
            const unsigned off = shift % 64U;
            std::uint64_t value = key.w[word] >> off;
            if (off != 0 && word + 1 < key.w.size()) {
                value |= key.w[word + 1] << (64U - off);
            }
            return value & mask;
        }
    }

    /// One fixed-width integral key column, resolved to its raw storage and the
    /// bit offset it occupies in the packed key.
    struct PackCol {
        enum class Kind : std::uint8_t { Int64, Date, Ts, Bool, Cat } kind{Kind::Int64};
        const std::int64_t* i64 = nullptr;
        const Date* date = nullptr;
        const Timestamp* ts = nullptr;
        const Column<bool>* boolean = nullptr;
        const Column<Categorical>* cat = nullptr;
        const std::uint32_t* remap = nullptr;  ///< local code -> operator-global id
        unsigned shift = 0;  ///< bit offset of this column's cell in the packed key
    };
    struct PackedPlan {
        std::vector<PackCol> cols;
        unsigned width = 0;  ///< total packed width in bytes
    };

    /// Bit width of one packed cell, matching the byte counts
    /// `build_packed_layout` accumulates per `PackCol::Kind`.
    [[nodiscard]] static auto width_bits_of(PackCol::Kind kind) -> unsigned {
        switch (kind) {
            case PackCol::Kind::Int64:
            case PackCol::Kind::Ts:
                return 64U;
            case PackCol::Kind::Date:
            case PackCol::Kind::Cat:
                return 32U;
            case PackCol::Kind::Bool:
                return 8U;
        }
        return 0U;
    }

    /// Per-key-column interning state for Categorical columns.
    ///
    /// A categorical code is only meaningful against ITS OWN chunk's dictionary,
    /// so packing the raw code would merge two different values that happen to
    /// share a code in different chunks. Resolving each dictionary entry to an
    /// operator-global id fixes that, and costs one lookup per DICTIONARY ENTRY
    /// per chunk rather than one per row: the row loop then reads `remap[code]`,
    /// a single array index with no hashing and no allocation at all.
    struct CatIntern {
        /// Views point into `arena`, whose deque never invalidates references.
        robin_hood::unordered_flat_map<std::string_view, std::uint32_t> ids;
        std::deque<std::string> arena;
        std::vector<std::uint32_t> remap;  ///< rebuilt per chunk, indexed by local code
    };

    /// A key is packable iff every column reduces to a fixed-width INTEGRAL cell
    /// whose byte equality equals value equality, with no nulls, and the columns
    /// together fit in 32 bytes.
    ///
    /// Doubles are excluded (-0.0/NaN break byte equality). Strings are excluded
    /// because interning one per row would cost the hash lookup this path exists
    /// to avoid. Categoricals ARE included: their dictionary is interned once per
    /// chunk into operator-global ids (see `CatIntern`), which is what makes a
    /// code comparable across chunks.
    auto build_packed_key(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        for (const auto* entry : entries) {
            if (entry->validity.has_value()) {
                return std::nullopt;
            }
        }
        return build_packed_layout(entries);
    }

    /// Same as `build_packed_key`, minus the "no column may carry nulls" check.
    ///
    /// Used to recover a stable fast path's (kind, shift) layout when a LATER
    /// chunk's nulls are exactly what disqualifies `build_packed_key` -- the
    /// layout itself does not depend on nullability, only on each column's
    /// type and position, which stay fixed for the life of the query once the
    /// packed path has been selected. The migration path calls this to learn
    /// how to decode the packed keys a prior, null-free chunk already built.
    auto build_packed_layout(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        // Size the interning state ONCE, before any of it is pointed at.
        //
        // `intern_categorical` hands back `remap.data()`, and `PackCol` holds
        // that pointer for the rest of the chunk. Growing `cat_interns_` while
        // those pointers are live reallocates the vector, and `CatIntern` holds
        // a robin_hood map whose move constructor is not noexcept — so
        // `move_if_noexcept` COPIES, `remap` gets a fresh buffer, and column
        // 0's pointer is left dangling the moment column 1 is interned.
        //
        // The symptom was a second categorical key column silently reading
        // freed memory: PDS-H q7's `by { supp_nation, cust_nation, l_year }`
        // emitted 8 groups instead of 4, the first chunk's four separated from
        // the rest, because only the first chunk paid a reallocation. It needed
        // multi-chunk input to show at all (`IBEX_CHUNK_ROWS`).
        if (cat_interns_.size() < entries.size()) {
            cat_interns_.resize(entries.size());
        }
        PackedPlan plan;
        plan.cols.reserve(entries.size());
        unsigned bytes = 0;
        for (std::size_t k = 0; k < entries.size(); ++k) {
            const auto& entry = *entries[k];
            PackCol col;
            col.shift = bytes * 8;
            const ColumnValue& column = *entry.column;
            if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
                col.kind = PackCol::Kind::Int64;
                col.i64 = c_int->data();
                bytes += 8;
            } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
                col.kind = PackCol::Kind::Date;
                col.date = c_date->data();
                bytes += 4;
            } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
                col.kind = PackCol::Kind::Ts;
                col.ts = c_ts->data();
                bytes += 8;
            } else if (const auto* c_bool = std::get_if<Column<bool>>(&column)) {
                col.kind = PackCol::Kind::Bool;
                col.boolean = c_bool;
                bytes += 1;
            } else if (const auto* c_cat = std::get_if<Column<Categorical>>(&column)) {
                col.remap = intern_categorical(k, *c_cat);
                // An empty dictionary would leave the row loop indexing a remap
                // that has no entry for any code. Declining here keeps the hot
                // loop free of a per-row range check.
                if (col.remap == nullptr) {
                    return std::nullopt;
                }
                col.kind = PackCol::Kind::Cat;
                col.cat = c_cat;
                bytes += 4;
            } else {
                return std::nullopt;
            }
            if (bytes > sizeof(Packed256)) {
                return std::nullopt;
            }
            plan.cols.push_back(col);
        }
        plan.width = bytes;
        return plan;
    }

    /// Resolve chunk-local codes of key column `k` to operator-global ids,
    /// returning the remap, or nullptr when the dictionary is empty. Runs once
    /// per chunk per categorical key column.
    auto intern_categorical(std::size_t k, const Column<Categorical>& cat) -> const std::uint32_t* {
        // Never grows `cat_interns_` — `build_packed_key` sized it before any
        // caller took a pointer into it, and growing here would dangle those.
        auto& state = cat_interns_[k];
        const auto& dict = cat.dictionary();
        const std::size_t dict_size = dict.size();
        if (dict_size == 0) {
            return nullptr;
        }
        state.remap.resize(dict_size);
        for (std::size_t code = 0; code < dict_size; ++code) {
            const std::string_view value = dict[code];
            if (const auto it = state.ids.find(value); it != state.ids.end()) {
                state.remap[code] = it->second;
                continue;
            }
            const auto id = static_cast<std::uint32_t>(state.ids.size());
            // The view must outlive the chunk's dictionary, so the map keys are
            // views into this deque rather than into the column.
            state.arena.emplace_back(value);
            state.ids.emplace(std::string_view{state.arena.back()}, id);
            state.remap[code] = id;
        }
        return state.remap.data();
    }

    /// Pack one row's key. Cheap enough — a handful of array reads and shifts,
    /// no hashing and no branch on width — that callers which need the key
    /// twice recompute it rather than materialize a buffer.
    template <typename Packed>
    [[nodiscard]] static auto pack_row(const std::vector<PackCol>& cols, std::size_t row)
        -> Packed {
        Packed key{};
        for (const auto& col : cols) {
            std::uint64_t cell = 0;
            switch (col.kind) {
                case PackCol::Kind::Int64:
                    cell = static_cast<std::uint64_t>(col.i64[row]);
                    break;
                case PackCol::Kind::Date:
                    cell = static_cast<std::uint32_t>(col.date[row].days);
                    break;
                case PackCol::Kind::Ts:
                    cell = static_cast<std::uint64_t>(col.ts[row].nanos);
                    break;
                case PackCol::Kind::Bool:
                    cell = (*col.boolean)[row] ? 1U : 0U;
                    break;
                case PackCol::Kind::Cat:
                    cell = col.remap[static_cast<std::size_t>(col.cat->code_at(row))];
                    break;
            }
            if constexpr (std::is_same_v<Packed, std::uint64_t>) {
                key |= cell << col.shift;
            } else {
                splice(key, cell, col.shift);
            }
        }
        return key;
    }

    /// Materialize this chunk's packed key for every row in `[begin, end)`.
    template <typename Packed>
    static void build_keys(const std::vector<PackCol>& cols, std::size_t begin, std::size_t end,
                           Packed* out) {
        for (std::size_t row = begin; row < end; ++row) {
            out[row] = pack_row<Packed>(cols, row);
        }
    }

    /// Interning state, indexed by key column position (see `CatIntern`).
    std::vector<CatIntern> cat_interns_;
};

class ChunkedDistinctOperator final : public Operator {
   public:
    ChunkedDistinctOperator(OperatorPtr child, const ExecutionContext& exec)
        : child_(std::move(child)), exec_(&exec) {}

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
    template <typename T>
    auto gather_distinct_rows(Table t, robin_hood::unordered_flat_set<T>& seen,
                              const Column<T>& col) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            if (!seen.insert(col[row]).second) {
                continue;
            }
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
    /// the parallel path every later chunk must too (`packed_part_count_`).
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
        if (packed_part_count_ == 0) {
            constexpr std::size_t kMinRows = 1U << 15U;
            if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread() ||
                rows < kMinRows) {
                return false;
            }
            // Bound to the eligibility check above: constructing the pool
            // spawns its threads eagerly, and a `IBEX_PARALLEL=0` query must
            // not pay for them just to be told it is serial.
            const std::size_t pool_size = process_worker_pool().size();
            const std::size_t budget =
                exec_->parallel_threads != 0 ? exec_->parallel_threads : pool_size;
            const std::size_t workers = std::min({budget, pool_size, std::size_t{64}});
            if (workers < 2) {
                return false;
            }
            std::size_t count = 1;
            while (count * 2 <= workers) {
                count *= 2;  // a power of two, so the partition is a mask
            }
            // `count <= pool.size()` by construction and the pool never
            // shrinks, so `submit(part_count, ...)` below is never clamped —
            // which it must not be, or a partition's rows would go unvisited.
            packed_part_count_ = count;
            state.parts.resize(count);
            // Anything an earlier chunk deduped serially lives in `state.seen`,
            // which no worker will ever probe, so it has to move into the
            // partitions before the first parallel chunk runs. Without this a
            // value already emitted is inserted afresh and emitted a SECOND
            // time: the row gate is per chunk, so a chunk under it falls back
            // to serial while leaving `packed_part_count_` at 0, and the next
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
        const std::size_t part_count = packed_part_count_;
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
    void seed_generic_dedup_from(const Container& seen, const ToScalar& to_scalar,
                                 const ToHash& to_hash) {
        group_order_.reserve(group_order_.size() + seen.size());
        key_index_.hashes.reserve(key_index_.hashes.size() + seen.size());
        for (const auto& v : seen) {
            Key key;
            key.values.push_back(to_scalar(v));
            group_order_.push_back(std::move(key));
            key_index_.hashes.push_back(to_hash(v));
        }
        std::size_t capacity = 1024;
        while (capacity * 7 < key_index_.hashes.size() * 10) {
            capacity *= 2;
        }
        key_index_.rehash(capacity);
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
    /// Pinned on first parallel use. A key's partition is `hash & (count-1)`,
    /// so a later chunk that partitioned differently would probe the wrong
    /// worker's set and re-emit a value already seen.
    std::size_t packed_part_count_ = 0;
    PackedKeyEncoder encoder_;
    /// `t.columns` as pointers, rebuilt per chunk for the encoder. A member so
    /// the reserve is paid once rather than per chunk.
    std::vector<const ColumnEntry*> key_entries_;
    /// A validity-aware Key index and the typed/packed stores have incompatible
    /// identities. Once either kind has recorded a row, later chunks must not
    /// silently move to the other.
    bool fast_dedup_seen_ = false;
    bool generic_dedup_seen_ = false;
    robin_hood::unordered_flat_set<Key, KeyHash, KeyEq> seen_;
    robin_hood::unordered_flat_set<std::int64_t> seen_i64_;
    robin_hood::unordered_flat_set<double> seen_f64_;
    robin_hood::unordered_flat_set<bool> seen_bool_;
    robin_hood::unordered_flat_set<Date> seen_date_;
    robin_hood::unordered_flat_set<Timestamp> seen_timestamp_;
    std::vector<std::uint8_t> seen_cat_flags_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> seen_strings_;
    std::deque<std::string> owned_strings_;
    const void* cat_dictionary_id_ = nullptr;
    const ExecutionContext* exec_ = nullptr;
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
        if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread() || slots == 0 ||
            right_rows < kMinParallelPredicateRows) {
            return 0;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget =
            exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size();
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
        if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread() ||
            rows < kMinParallelPredicateRows) {
            return serial_select();
        }
        auto& pool = process_worker_pool();
        const std::size_t budget =
            (exec_->parallel_threads != 0) ? exec_->parallel_threads : pool.size();
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
                const bool match = !probe_is_null(row) && right_i64_.contains((*col)[row]);
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
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    return &static_cast<const ir::ScanNode&>(*cur);
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
                             const std::vector<ir::OrderKey>* pending_order = nullptr)
        : left_(std::move(left)),
          right_(std::move(right)),
          keys_(keys),
          exec_(&exec),
          suffix_(std::move(suffix)),
          pending_order_(pending_order) {}

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
                             const std::vector<ir::OrderKey>* pending_order = nullptr)
        : left_(std::move(left)),
          keys_(keys),
          deferred_probe_(probe),
          deferred_probe_name_(std::move(probe_name)),
          deferred_right_node_(right_node),
          deferred_registry_(registry),
          deferred_scalars_(scalars),
          deferred_externs_(externs),
          deferred_exec_(&exec),
          exec_(&exec),
          suffix_(std::move(suffix)),
          pending_order_(pending_order) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
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
            auto out = emit_swapped();
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                return std::optional<Chunk>{};
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*out))};
        }

        while (true) {
            Table left_chunk;
            if (use_materialized_left_) {
                if (left_materialized_drained_) {
                    if (!emitted_nonempty_ && empty_schema_.has_value()) {
                        auto schema = std::move(*empty_schema_);
                        empty_schema_.reset();
                        return std::optional<Chunk>{table_to_chunk(std::move(schema))};
                    }
                    return std::optional<Chunk>{};
                }
                left_materialized_drained_ = true;
                left_chunk = std::move(left_materialized_).value_or(Table{});
                left_materialized_.reset();
            } else {
                auto chunk_res = left_->next();
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
                left_chunk = chunk_to_table(std::move(*chunk_res.value()));
            }
            auto out = probe_chunk_against_right(std::move(left_chunk));
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                // Keep the planned empty table as a schema carrier. A join
                // with no matches still has its left and right output columns;
                // without this, a materializing sink sees no chunks at all.
                empty_schema_ = std::move(*out);
                continue;
            }
            emitted_nonempty_ = true;
            return std::optional<Chunk>{table_to_chunk(std::move(*out))};
        }
    }

   private:
    enum class Mode : std::uint8_t { Stream, Swapped, Precomputed };

    static constexpr std::size_t kNil = std::numeric_limits<std::size_t>::max();

    const ValidityBitmap* build_validity_ = nullptr;  // null → build key has no nulls
    const ValidityBitmap* probe_validity_ = nullptr;  // reset per probe chunk
    // Build-on-right is preferred when right is small enough that probing
    // it from streaming left chunks is cache-friendly. Above this, we
    // materialize left to pick the smaller build side.
    static constexpr std::size_t kStreamRightThreshold = 65536;

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
        const ColumnValue* rkey = right_.find(right_key_name);
        if (rkey == nullptr) {
            return "join key not found in right table: " + right_key_name;
        }
        if (auto err = detect_key_kind(*rkey, key_kind_)) {
            return err;
        }

        const std::size_t n_right = right_.rows();

        if (n_right <= kStreamRightThreshold) {
            if (auto err = build_index(right_, right_key_name)) {
                return err;
            }
            return std::nullopt;
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

        // Swapping indexes the smaller (left) side and scans the right, which
        // gives up left-row order. When an `order` above this join wants
        // exactly the order the left already carries, declining to swap
        // delivers it and that whole sort disappears -- worth a larger index,
        // but only while "larger" stays modest, since the index is probed once
        // per row of the other side. The same trade is made in join.cpp.
        if (n_left < n_right && !order_preserving_pays(left_table, n_left, n_right)) {
            left_table_ = std::move(left_table);
            if (auto err = build_index(*left_table_, left_key_name)) {
                return err;
            }
            mode_ = Mode::Swapped;
            return std::nullopt;
        }

        left_materialized_ = std::move(left_table);
        use_materialized_left_ = true;
        if (auto err = build_index(right_, right_key_name)) {
            return err;
        }
        return std::nullopt;
    }

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
        const ColumnValue* rkey0 = right_.find(k0.right);
        if (rkey0 == nullptr) {
            return "join key not found in right table: " + k0.right;
        }
        const ColumnValue* rkey1 = right_.find(k1.right);
        if (rkey1 == nullptr) {
            return "join key not found in right table: " + k1.right;
        }
        const auto* rcol0 = std::get_if<Column<std::int64_t>>(rkey0);
        const auto* rcol1 = std::get_if<Column<std::int64_t>>(rkey1);
        if (rcol0 == nullptr || rcol1 == nullptr) {
            return "ChunkedInnerJoinOperator: two-key join currently requires both keys to be "
                   "Int64";
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
        const std::size_t n_left = left_table.rows();
        const std::size_t n_right = right_.rows();
        pair_mode_ = true;

        if (n_left <= n_right) {
            return build_left_pair_index_and_swap(std::move(left_table));
        }

        // Build on the (smaller) right; left is already fully materialized,
        // so it is drained as a single chunk through the existing
        // `use_materialized_left_` mechanism rather than re-wrapped in an
        // operator.
        const auto* re0 = right_.find_entry(k0.right);
        const auto* re1 = right_.find_entry(k1.right);
        const ValidityBitmap* rv0 =
            re0 != nullptr && re0->validity.has_value() ? &*re0->validity : nullptr;
        const ValidityBitmap* rv1 =
            re1 != nullptr && re1->validity.has_value() ? &*re1->validity : nullptr;
        build_pair_index(*rcol0, *rcol1, rv0, rv1);
        left_materialized_ = std::move(left_table);
        use_materialized_left_ = true;
        return std::nullopt;
    }

    // Build on the (smaller) left, scan right row-by-row: same shape as
    // single-key `Mode::Swapped` / `emit_swapped`. Factored out of
    // `initialize_pair` so `resolve_deferred_probe_pair` (which always
    // builds on left -- the deferred side is definitionally the one too
    // large to materialize first) can reuse it.
    auto build_left_pair_index_and_swap(Table left_table) -> std::optional<std::string> {
        const ir::JoinKey& k0 = keys_->at(0);
        const ir::JoinKey& k1 = keys_->at(1);
        const ColumnValue* lkey0 = left_table.find(k0.left);
        if (lkey0 == nullptr) {
            return "join key not found in left table: " + k0.left;
        }
        const ColumnValue* lkey1 = left_table.find(k1.left);
        if (lkey1 == nullptr) {
            return "join key not found in left table: " + k1.left;
        }
        const auto* lcol0 = std::get_if<Column<std::int64_t>>(lkey0);
        const auto* lcol1 = std::get_if<Column<std::int64_t>>(lkey1);
        if (lcol0 == nullptr || lcol1 == nullptr) {
            return "ChunkedInnerJoinOperator: two-key join currently requires both keys to "
                   "be Int64";
        }
        const auto* le0 = left_table.find_entry(k0.left);
        const auto* le1 = left_table.find_entry(k1.left);
        const ValidityBitmap* lv0 =
            le0 != nullptr && le0->validity.has_value() ? &*le0->validity : nullptr;
        const ValidityBitmap* lv1 =
            le1 != nullptr && le1->validity.has_value() ? &*le1->validity : nullptr;
        build_pair_index(*lcol0, *lcol1, lv0, lv1);
        left_table_ = std::move(left_table);
        mode_ = Mode::Swapped;
        return std::nullopt;
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
    /// `build_left_pair_index_and_swap`'s exact pair probe afterward is what
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
        right_ = std::move(*right);
        deferred_probe_ = nullptr;
        if (std::getenv("IBEX_DEBUG_PAIR_DEFER") != nullptr) {
            std::fprintf(stderr, "[pair_defer] filter_published=%d right_rows_after_filter=%zu\n",
                         static_cast<int>(use_materialized_left_), right_.rows());
        }
        return initialize_pair();
    }

    // Swapped pair-mode counterpart of `emit_swapped`: the pair index is on
    // `left_table_`, so `right_`'s rows are the probe side. Reuses
    // `probe_swapped` unchanged -- it is already generic over a
    // `head_of(row)` callback -- with the null check folded into `head_of`
    // itself (returning `kNil`) instead of the single-bitmap `probe_is_null`
    // member, since a row here is null when EITHER key is.
    auto emit_swapped_pair() -> std::expected<Table, std::string> {
        const ir::JoinKey& k0 = keys_->at(0);
        const ir::JoinKey& k1 = keys_->at(1);
        const ColumnValue* rkey0 = right_.find(k0.right);
        if (rkey0 == nullptr) {
            return std::unexpected("join key not found in right table: " + k0.right);
        }
        const ColumnValue* rkey1 = right_.find(k1.right);
        if (rkey1 == nullptr) {
            return std::unexpected("join key not found in right table: " + k1.right);
        }
        const auto* col0 = std::get_if<Column<std::int64_t>>(rkey0);
        const auto* col1 = std::get_if<Column<std::int64_t>>(rkey1);
        if (col0 == nullptr || col1 == nullptr) {
            return std::unexpected(
                "inner join: right key type mismatch (two-key join expects Int64)");
        }
        if (!left_table_.has_value()) {
            return std::unexpected(
                "ChunkedInnerJoinOperator: swapped mode without a materialized left table");
        }
        const auto* e0 = right_.find_entry(k0.right);
        const auto* e1 = right_.find_entry(k1.right);
        const ValidityBitmap* v0 =
            e0 != nullptr && e0->validity.has_value() ? &*e0->validity : nullptr;
        const ValidityBitmap* v1 =
            e1 != nullptr && e1->validity.has_value() ? &*e1->validity : nullptr;
        const auto* d0 = col0->data();
        const auto* d1 = col1->data();
        const std::size_t n_right = right_.rows();

        const auto head_of = [&](std::size_t r) -> std::size_t {
            if ((v0 != nullptr && !(*v0)[r]) || (v1 != nullptr && !(*v1)[r])) {
                return kNil;
            }
            PairKey key{static_cast<std::uint64_t>(d0[r]), static_cast<std::uint64_t>(d1[r])};
            auto it = pair_heads_.find(key);
            return it == pair_heads_.end() ? kNil : it->second;
        };

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        probe_swapped(n_right, head_of, li, ri);

        const Table& left_table = *left_table_;
        Table left_copy;
        left_copy.columns.reserve(left_table.columns.size());
        for (const auto& c : left_table.columns) {
            left_copy.add_column(c.name, *c.column);
            left_copy.columns.back().validity = c.validity;
        }
        return assemble_output(std::move(left_copy), li.data(), ri.data(), li.size());
    }

    // Build the chained hash index for the two-key path, same chain-of-equal
    // rows convention as `build_scalar` (reverse iteration so the chain walks
    // forward during probe). A row with either key null is never indexed --
    // null never matches, not even another null (same policy as the
    // single-key path).
    void build_pair_index(const Column<std::int64_t>& col0, const Column<std::int64_t>& col1,
                          const ValidityBitmap* v0, const ValidityBitmap* v1) {
        const std::size_t n = col0.size();
        chain_next_.assign(n, kNil);
        pair_heads_.reserve(n);
        const auto* d0 = col0.data();
        const auto* d1 = col1.data();
        for (std::size_t r = n; r-- > 0;) {
            if ((v0 != nullptr && !(*v0)[r]) || (v1 != nullptr && !(*v1)[r])) {
                continue;
            }
            PairKey key{static_cast<std::uint64_t>(d0[r]), static_cast<std::uint64_t>(d1[r])};
            auto [it, inserted] = pair_heads_.try_emplace(key, r);
            if (!inserted) {
                chain_next_[r] = it->second;
                it->second = r;
                build_unique_ = false;
            }
        }
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
                auto it = pair_heads_.find(get_key(l));
                if (it == pair_heads_.end()) {
                    continue;
                }
                for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            return build_unique_ && li.size() == n;
        }
        if (build_unique_) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                if (is_null(l)) {
                    continue;
                }
                auto it = pair_heads_.find(get_key(l));
                if (it == pair_heads_.end()) {
                    continue;
                }
                lp[out] = l;
                rp[out] = it->second;
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
            auto it = pair_heads_.find(get_key(l));
            if (it == pair_heads_.end()) {
                continue;
            }
            std::size_t cur = it->second;
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = chain_next_[cur];
            }
        }
        return false;
    }

    auto probe_chunk_pair(Table left_chunk) -> std::expected<Table, std::string> {
        const ir::JoinKey& k0 = keys_->at(0);
        const ir::JoinKey& k1 = keys_->at(1);
        const ColumnValue* key0 = left_chunk.find(k0.left);
        if (key0 == nullptr) {
            return std::unexpected("join key not found in left chunk: " + k0.left);
        }
        const ColumnValue* key1 = left_chunk.find(k1.left);
        if (key1 == nullptr) {
            return std::unexpected("join key not found in left chunk: " + k1.left);
        }
        const auto* col0 = std::get_if<Column<std::int64_t>>(key0);
        const auto* col1 = std::get_if<Column<std::int64_t>>(key1);
        if (col0 == nullptr || col1 == nullptr) {
            return std::unexpected(
                "inner join: left key type mismatch (two-key join expects "
                "Int64)");
        }
        const auto* e0 = left_chunk.find_entry(k0.left);
        const auto* e1 = left_chunk.find_entry(k1.left);
        const ValidityBitmap* v0 =
            e0 != nullptr && e0->validity.has_value() ? &*e0->validity : nullptr;
        const ValidityBitmap* v1 =
            e1 != nullptr && e1->validity.has_value() ? &*e1->validity : nullptr;

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
            return PairKey{static_cast<std::uint64_t>(d0[r]), static_cast<std::uint64_t>(d1[r])};
        };
        const bool li_identity = probe_pair(n, is_null, get_key, li, ri);

        const std::size_t total = li_identity ? ri.size() : li.size();
        return assemble_output(std::move(left_chunk), li.data(), ri.data(), total, li_identity);
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
        right_ = std::move(right.value());
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
        right_ = std::move(right.value());
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

        key_kind_ = ExprType::Int;
        if (auto err = build_index(build, keys_->front().left)) {
            return err;
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
        const auto scan = [&](std::size_t begin, std::size_t end, std::vector<SwappedHit>& hits,
                              std::size_t& total) {
            for (std::size_t i = begin; i < end; ++i) {
                if (key_validity != nullptr && !(*key_validity)[i]) {
                    continue;
                }
                const auto it = i64_heads_.find(key_data[i]);
                if (it == i64_heads_.end()) {
                    continue;
                }
                hits.push_back(SwappedHit{.rrow = i, .head = it->second});
                for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                    ++total;
                }
            }
        };
        const std::size_t workers = probe_parallel_workers(n);
        if (workers == 0) {
            swapped_parts_.resize(1);
            swapped_parts_[0].hits.clear();
            swapped_parts_[0].total = 0;
            scan(0, n, swapped_parts_[0].hits, swapped_parts_[0].total);
        } else {
            auto& pool = process_worker_pool();
            const std::size_t grain = (n + workers - 1) / workers;
            swapped_parts_.resize(workers);
            auto batch = pool.submit(workers, [&](std::size_t w) {
                auto& part = swapped_parts_[w];
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
        const std::size_t n_parts = swapped_parts_.size();
        std::vector<std::size_t> hit_offsets(n_parts);
        part_offsets_.resize(n_parts);
        std::size_t n_hits = 0;
        std::size_t total = 0;
        for (std::size_t w = 0; w < n_parts; ++w) {
            hit_offsets[w] = n_hits;
            part_offsets_[w] = total;
            n_hits += swapped_parts_[w].hits.size();
            total += swapped_parts_[w].total;
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
            const auto& part = swapped_parts_[w];
            std::size_t h = hit_offsets[w];
            std::size_t pos = part_offsets_[w];
            for (const SwappedHit& hit : part.hits) {
                survivors[h] = sel.selected[hit.rrow];
                if (gathered_out != nullptr) {
                    gathered_out[h] = key_data[hit.rrow];
                }
                for (std::size_t cur = hit.head; cur != kNil; cur = chain_next_[cur]) {
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
        auto out = assemble_output(std::move(left_copy), li.data(), ri.data(), total,
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
    void publish_build_filter_column(const Table& build, const std::string& key_name,
                                     DynamicScanFilter& slot) const {
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
            std::sort(keys.begin(), keys.end());
            keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
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

    static auto detect_key_kind(const ColumnValue& col, ExprType& out)
        -> std::optional<std::string> {
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

    // Build the chained hash index on `build_side` using column `key_name`.
    // `build_idx_` maps each build-row index to the next row with the same
    // key (kNil sentinel terminates the chain).
    auto build_index(const Table& build_side, const std::string& key_name)
        -> std::optional<std::string> {
        const ColumnValue* key = build_side.find(key_name);
        if (key == nullptr) {
            return "join key not found in build side: " + key_name;
        }
        // A null key matches nothing, not even another null (SQL / Polars). So a
        // null-keyed build row is never indexed, and a null-keyed probe row is
        // never looked up. Both halves are needed: a null cell holds the type's
        // zero value, so a null probe key would otherwise find a genuine `0`.
        const auto* build_entry = build_side.find_entry(key_name);
        build_validity_ = build_entry != nullptr && build_entry->validity.has_value()
                              ? &*build_entry->validity
                              : nullptr;
        const std::size_t n = build_side.rows();
        chain_next_.assign(n, kNil);

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, i64_heads_);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, f64_heads_);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            bool_heads_.reserve(n);
            for (std::size_t r = n; r-- > 0;) {
                const bool v = (*col)[r];
                auto [it, inserted] = bool_heads_.try_emplace(v, r);
                if (!inserted) {
                    chain_next_[r] = it->second;
                    it->second = r;
                    build_unique_ = false;
                }
            }
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, date_heads_);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr)
                return "inner join: build-side key type mismatch";
            build_scalar(*col, ts_heads_);
        } else if (key_kind_ == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(key)) {
                const auto& dict = c_cat->dictionary();
                string_heads_.reserve(n);
                for (std::size_t r = n; r-- > 0;) {
                    auto code = static_cast<std::size_t>(c_cat->code_at(r));
                    insert_chain_sv(std::string_view{dict[code]}, r);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
                string_heads_.reserve(n);
                for (std::size_t r = n; r-- > 0;) {
                    insert_chain_sv((*c_str)[r], r);
                }
            } else {
                return "inner join: build-side key type mismatch";
            }
        }
        return std::nullopt;
    }

    // Iterate the build side in reverse so the chain walks forward during
    // probe, matching the nested-loop inner join's output ordering.
    template <typename ColT, typename Map>
    void build_scalar(const ColT& col, Map& heads) {
        const std::size_t n = col.size();
        heads.reserve(n);
        const auto* data = col.data();
        for (std::size_t r = n; r-- > 0;) {
            if (build_is_null(r)) {
                continue;
            }
            auto [it, inserted] = heads.try_emplace(data[r], r);
            if (!inserted) {
                chain_next_[r] = it->second;
                it->second = r;
                build_unique_ = false;
            }
        }
    }

    [[nodiscard]] auto build_is_null(std::size_t row) const noexcept -> bool {
        return build_validity_ != nullptr && !(*build_validity_)[row];
    }
    [[nodiscard]] auto probe_is_null(std::size_t row) const noexcept -> bool {
        return probe_validity_ != nullptr && !(*probe_validity_)[row];
    }

    void insert_chain_sv(std::string_view sv, std::size_t r) {
        if (build_is_null(r)) {
            return;
        }
        auto [it, inserted] = string_heads_.try_emplace(sv, r);
        if (!inserted) {
            chain_next_[r] = it->second;
            it->second = r;
            build_unique_ = false;
        }
    }

    // Which right columns this join emits, and under which names. Both come
    // from the shared planner (ir/join_output.hpp), so the chunked route lands
    // on the same output schema as the materialized route and IR inference.
    // The left column names are identical for every chunk, so the plan is
    // computed once from the first assembled chunk.
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
        if (!right_emit_ready_) {
            if (auto ready = setup_right_emit_schema(left_table); !ready.has_value()) {
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
            if (idx == left_table.columns.size() || idx >= left_emit_names_.size()) {
                return false;
            }
            carried.push_back(
                ir::OrderKey{.name = left_emit_names_[idx], .ascending = key.ascending});
        }
        return TableProperties::sorted_by(std::move(carried)).satisfies(*pending_order_);
    }

    auto setup_right_emit_schema(const Table& left_side) -> std::expected<void, std::string> {
        auto planned =
            ir::plan_join_output(ir::JoinKind::Inner, *keys_, table_column_names(left_side),
                                 table_column_names(right_), suffix_);
        if (!planned.has_value()) {
            return std::unexpected(std::move(planned.error()));
        }
        const std::vector<ir::JoinOutputColumn>& plan = *planned;
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
    /// The build side is a shared read-only hash index — `heads`, `chain_next_`
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
    /// the caller's serial loop. Below `kMinProbeRows` the fan-out and the
    /// concatenation cost more than the probes they spread — a probe is a hash
    /// lookup plus a chain walk, so the per-row work is real, but so is
    /// submitting a batch.
    [[nodiscard]] auto probe_parallel_workers(std::size_t n) const -> std::size_t {
        constexpr std::size_t kMinProbeRows = 1U << 14U;
        if (exec_ == nullptr || !exec_->parallel || !exec_->parallel_join_probe ||
            on_worker_pool_thread() || n < kMinProbeRows) {
            return 0;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget =
            exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size();
        const std::size_t workers = std::min({budget, pool.size(), std::size_t{64}});
        return workers < 2 ? 0 : workers;
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
                for (std::size_t cur = head; cur != kNil; cur = chain_next_[cur]) {
                    ++total;
                }
            }
        };
        const auto replay = [&](const std::vector<SwappedHit>& hits, std::size_t pos) {
            for (const SwappedHit& hit : hits) {
                for (std::size_t cur = hit.head; cur != kNil; cur = chain_next_[cur]) {
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
                auto it = heads.find(get(l));
                if (it == heads.end()) {
                    continue;
                }
                for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            // `li_identity` means li == 0..n-1, which for a unique build side
            // is exactly "every row matched" — the same test the serial path
            // makes, just recovered from the totals.
            return build_unique_ && li.size() == n;
        }
        if (build_unique_) {
            li.resize(n);
            ri.resize(n);
            std::size_t* lp = li.data();
            std::size_t* rp = ri.data();
            std::size_t out = 0;
            for (std::size_t l = 0; l < n; ++l) {
                if (probe_is_null(l)) {
                    continue;
                }
                auto it = heads.find(get(l));
                if (it == heads.end()) {
                    continue;
                }
                lp[out] = l;
                rp[out] = it->second;
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
            auto it = heads.find(get(l));
            if (it == heads.end()) {
                continue;
            }
            std::size_t cur = it->second;
            while (cur != kNil) {
                li.push_back(l);
                ri.push_back(cur);
                cur = chain_next_[cur];
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
                for (std::size_t cur = head_of(l); cur != kNil; cur = chain_next_[cur]) {
                    out_l.push_back(l);
                    out_r.push_back(cur);
                }
            }
        };
        if (probe_ranges_parallel(n, li, ri, scan)) {
            return build_unique_ && li.size() == n;
        }
        if (build_unique_) {
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
                cur = chain_next_[cur];
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
        code_heads_.assign(dict.size(), kNil);
        for (std::size_t c = 0; c < dict.size(); ++c) {
            if (auto it = string_heads_.find(std::string_view{dict[c]});
                it != string_heads_.end()) {
                code_heads_[c] = it->second;
            }
        }
    }

    auto probe_chunk_against_right(Table left_chunk) -> std::expected<Table, std::string> {
        if (pair_mode_) {
            return probe_chunk_pair(std::move(left_chunk));
        }
        const ColumnValue* key = left_chunk.find(keys_->front().left);
        if (key == nullptr) {
            return std::unexpected("join key not found in left chunk: " + keys_->front().left);
        }
        const auto* probe_entry = left_chunk.find_entry(keys_->front().left);
        probe_validity_ = probe_entry != nullptr && probe_entry->validity.has_value()
                              ? &*probe_entry->validity
                              : nullptr;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
        const std::size_t n = left_chunk.rows();
        li.reserve(n);
        ri.reserve(n);
        bool li_identity = false;

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(i64_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(f64_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            li_identity =
                probe_scalar(bool_heads_, n, [&](std::size_t i) { return (*col)[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(date_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::unexpected("inner join: left key type mismatch");
            }
            const auto* data = col->data();
            li_identity =
                probe_scalar(ts_heads_, n, [&](std::size_t i) { return data[i]; }, li, ri);
        } else if (key_kind_ == ExprType::String) {
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
                            return code_heads_[static_cast<std::size_t>(c_cat->code_at(i))];
                        },
                        li, ri);
                } else {
                    li_identity = probe_scalar(
                        string_heads_, n,
                        [&](std::size_t i) {
                            return std::string_view{
                                dict[static_cast<std::size_t>(c_cat->code_at(i))]};
                        },
                        li, ri);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(key)) {
                li_identity = probe_scalar(
                    string_heads_, n, [&](std::size_t i) { return (*c_str)[i]; }, li, ri);
            } else {
                return std::unexpected("inner join: left key type mismatch");
            }
        }

        const std::size_t total = li_identity ? ri.size() : li.size();
        return assemble_output(std::move(left_chunk), li.data(), ri.data(), total, li_identity);
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
    auto emit_swapped() -> std::expected<Table, std::string> {
        if (pair_mode_) {
            return emit_swapped_pair();
        }
        const ColumnValue* rkey = right_.find(keys_->front().right);
        if (rkey == nullptr) {
            return std::unexpected("join key not found in right table: " + keys_->front().right);
        }
        if (!left_table_.has_value()) {
            return std::unexpected(
                "ChunkedInnerJoinOperator: swapped mode without a materialized left table");
        }
        const Table& left_table = *left_table_;
        const std::size_t n_right = right_.rows();

        // In swapped mode the index is on the left, so the right table is the
        // probe side. Its null-keyed rows match nothing (see build_index).
        const auto* right_entry = right_.find_entry(keys_->front().right);
        probe_validity_ = right_entry != nullptr && right_entry->validity.has_value()
                              ? &*right_entry->validity
                              : nullptr;

        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;

        // Every key kind reduces to "resolve right row r to a left chain head
        // or kNil"; the map branches wrap the hash lookup, the categorical
        // fast path hands the pre-resolved head straight through. One shape
        // means `probe_swapped` is the single scan/replay implementation for
        // both the serial and the parallel path.
        auto do_phase1 = [&](auto&& key_at, const auto& heads) {
            probe_swapped(
                n_right,
                [&](std::size_t r) {
                    auto it = heads.find(key_at(r));
                    return it == heads.end() ? kNil : it->second;
                },
                li, ri);
        };
        // Same shape with the chain head already resolved — see
        // `resolve_categorical_heads`.
        auto do_phase1_resolved = [&](auto&& head_at) { probe_swapped(n_right, head_at, li, ri); };

        if (key_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, i64_heads_);
        } else if (key_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, f64_heads_);
        } else if (key_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            do_phase1([&](std::size_t r) { return (*col)[r]; }, bool_heads_);
        } else if (key_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, date_heads_);
        } else if (key_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(rkey);
            if (col == nullptr)
                return std::unexpected("inner join: right key type mismatch");
            const auto* data = col->data();
            do_phase1([&](std::size_t r) { return data[r]; }, ts_heads_);
        } else if (key_kind_ == ExprType::String) {
            if (const auto* c_cat = std::get_if<Column<Categorical>>(rkey)) {
                const auto& dict = c_cat->dictionary();
                if (dict.size() < n_right) {
                    resolve_categorical_heads(dict);
                    do_phase1_resolved([&](std::size_t r) {
                        return code_heads_[static_cast<std::size_t>(c_cat->code_at(r))];
                    });
                } else {
                    do_phase1(
                        [&](std::size_t r) {
                            return std::string_view{
                                dict[static_cast<std::size_t>(c_cat->code_at(r))]};
                        },
                        string_heads_);
                }
            } else if (const auto* c_str = std::get_if<Column<std::string>>(rkey)) {
                do_phase1([&](std::size_t r) { return (*c_str)[r]; }, string_heads_);
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
                                  make_empty_like(*right_.columns[right_emit_idx_[e]].column));
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
        const bool share_right = ri_identity && total == right_.rows();
        if (share_right) {
            for (std::size_t e = 0; e < right_emit_idx_.size(); ++e) {
                output.add_column_from(std::string(right_emit_names_[e]),
                                       right_.columns[right_emit_idx_[e]]);
            }
        } else {
            std::vector<ColumnGatherJob> jobs;
            jobs.reserve(right_emit_idx_.size());
            for (const auto index : right_emit_idx_) {
                const auto& rc = right_.columns[index];
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

    OperatorPtr left_;
    Table right_;
    const std::vector<ir::JoinKey>* keys_;

    // Deferred-probe context (see the second constructor). `deferred_probe_`
    // doubles as the mode flag: non-null until the probe scan is resolved.
    const DeferredScan* deferred_probe_ = nullptr;
    std::string deferred_probe_name_;
    const ir::Node* deferred_right_node_ = nullptr;
    const TableRegistry* deferred_registry_ = nullptr;
    const ScalarRegistry* deferred_scalars_ = nullptr;
    const ExternRegistry* deferred_externs_ = nullptr;
    const ExecutionContext* deferred_exec_ = nullptr;
    /// Set by both constructors; the deferred one aliases `deferred_exec_`.
    const ExecutionContext* exec_ = nullptr;

    bool initialized_ = false;
    Mode mode_ = Mode::Stream;
    ExprType key_kind_ = ExprType::Int;

    // Hash index on the build side (right in Stream, left in Swapped).
    bool build_unique_ = true;
    std::vector<std::size_t> chain_next_;
    // Probe-chunk dictionary code -> build chain head (kNil = no match).
    // Rebuilt per chunk by resolve_categorical_heads.
    std::vector<std::size_t> code_heads_;
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> i64_heads_;
    robin_hood::unordered_flat_map<double, std::size_t> f64_heads_;
    robin_hood::unordered_flat_map<bool, std::size_t> bool_heads_;
    robin_hood::unordered_flat_map<Date, std::size_t> date_heads_;
    robin_hood::unordered_flat_map<Timestamp, std::size_t> ts_heads_;
    robin_hood::unordered_flat_map<std::string_view, std::size_t, StringViewHash, StringViewEq>
        string_heads_;
    // Two-fixed-width-int-key path (see `initialize_pair`): both key values
    // pack into one 128-bit-ish struct, injective with no knowledge of their
    // domains -- same trick as the aggregate's own `PairIntKey`, but this is a
    // separate type since the two classes don't share member scope.
    bool pair_mode_ = false;
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
    robin_hood::unordered_flat_map<PairKey, std::size_t, PairKeyHash> pair_heads_;
    /// One worker's slice of a parallel probe. Members so the vectors keep
    /// their capacity across chunks instead of reallocating per probe.
    struct ProbePart {
        std::vector<std::size_t> li;
        std::vector<std::size_t> ri;
    };
    std::vector<ProbePart> probe_parts_;
    std::vector<SwappedPart> swapped_parts_;
    std::vector<std::size_t> part_offsets_;
    std::vector<std::size_t> right_emit_idx_;
    std::vector<std::string> right_emit_names_;
    std::vector<std::string> left_emit_names_;
    bool right_emit_ready_ = false;
    ir::JoinSuffixPolicy suffix_;
    // What an `order` above this join will ask for, or null. Only ever shifts
    // which side is indexed; see `initialize`.
    const std::vector<ir::OrderKey>* pending_order_ = nullptr;

    // Stream mode: when right > threshold and left >= right, left was
    // materialized to measure but not swapped; replay as a single chunk.
    std::optional<Table> left_materialized_;
    bool left_materialized_drained_ = false;
    bool use_materialized_left_ = false;
    std::optional<Table> empty_schema_;
    bool emitted_nonempty_ = false;

    // Swapped mode: materialized left held for later gather.
    std::optional<Table> left_table_;
    bool swapped_emitted_ = false;

    // Precomputed mode: the two-phase deferred probe assembled the whole
    // join output during initialization.
    Table precomputed_output_;
};

/// Streaming hash aggregate. Maintains a `robin_hood` group index and
/// per-group `AggState` across chunks: each incoming chunk updates the
/// state per row, the chunk is released, and the final result is
/// emitted as a single output chunk on EOF.
///
/// Eligibility is gated at `build_operator` time to the common subset
/// that streams cleanly: `Count`, `Sum`, `Min`, `Max`, `Mean` on
/// numeric (int/double) inputs. Nullable agg inputs are handled — null
/// rows skip the update, and an all-null group emits a null result.
/// Nullable group-by columns are not supported yet; they fall back to
/// `aggregate_table` via `interpret_node`. Complex aggs (Median, etc.)
/// and string aggs also fall back.
///
/// The first chunk's group-by column types are snapshotted (including
/// the Categorical dictionary pointer when applicable) and reused when
/// building output; the chunked csv source shares dictionaries across
/// chunks, matching MaterializeOperator's existing assumption.
/// A growable array of trivially-copyable slots that grows through `realloc`.
///
/// `std::vector` cannot use `realloc`: it must allocate, copy, and free, and on
/// this array that copy IS the cost. A group-by discovers its groups a chunk at
/// a time, so the slot array is resized once per chunk and never shrinks; by
/// the last chunk the copies dominate. Measured on q18 (3M groups over 6
/// chunks): `size_group_arrays` cost 79ms, and pre-reserving the final size --
/// which a real query cannot do, since the group count is what it is about to
/// find out -- removed 49ms of it. That removed cost is all copying.
///
/// At these sizes the block is served by `mmap`, and `realloc` extends it with
/// `mremap`: page-table work, no bytes moved. The elements it must still touch
/// are only the NEW ones, which is the irreducible part.
///
/// Deliberately minimal: no shrink, no insert, no iterators. It is a slot array
/// indexed by group id, and every use it has is `resize` / `data` / `[]`.
template <typename T>
class SlotArray {
   public:
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::is_trivially_destructible_v<T>);

    SlotArray() = default;
    SlotArray(const SlotArray&) = delete;
    auto operator=(const SlotArray&) -> SlotArray& = delete;
    SlotArray(SlotArray&& other) noexcept
        : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
        other.data_ = nullptr;
        other.size_ = 0;
        other.capacity_ = 0;
    }
    auto operator=(SlotArray&& other) noexcept -> SlotArray& {
        if (this != &other) {
            std::free(data_);
            data_ = other.data_;
            size_ = other.size_;
            capacity_ = other.capacity_;
            other.data_ = nullptr;
            other.size_ = 0;
            other.capacity_ = 0;
        }
        return *this;
    }
    ~SlotArray() { std::free(data_); }

    [[nodiscard]] auto size() const noexcept -> std::size_t { return size_; }
    [[nodiscard]] auto data() noexcept -> T* { return data_; }
    [[nodiscard]] auto data() const noexcept -> const T* { return data_; }
    auto operator[](std::size_t i) noexcept -> T& { return data_[i]; }
    auto operator[](std::size_t i) const noexcept -> const T& { return data_[i]; }

    /// Grow to `n` WITHOUT initializing the new tail, which is returned for the
    /// caller to fill. Never shrinks the allocation: a group-by only ever adds
    /// groups.
    ///
    /// Split out of `resize` because on a large array the fill is the expensive
    /// half and it is not serial by nature. `mremap` hands back pages the kernel
    /// has yet to materialize, so writing them is 72MB of first-touch page
    /// faults on q18's 3M slots — 44ms in a cold process, several times what the
    /// bytes alone cost, and page faults are what scales with threads. A caller
    /// holding a worker pool fans this out; one without it calls `resize` and
    /// pays the serial fill.
    ///
    /// How much this is worth depends on whether the pages are fresh, so read
    /// the two numbers separately. Cold (`ibex query.ibex`, the path a script
    /// takes) the fan-out takes q18's fill 44ms -> 34ms and the whole query
    /// -4.3%. Warm — the PDS-H harness, which reuses one process, so the
    /// allocator hands back pages already faulted — the fill is plain bandwidth
    /// and the suite geomean does not move.
    [[nodiscard]] auto grow_uninitialized(std::size_t n) -> std::span<T> {
        if (n <= size_) {
            size_ = n;
            return {};
        }
        if (n > capacity_) {
            // Geometric, so a per-chunk resize does not call realloc once per
            // chunk on a stream of many small chunks.
            const std::size_t want = std::max(n, capacity_ + (capacity_ / 2));
            auto* grown = static_cast<T*>(std::realloc(data_, want * sizeof(T)));
            if (grown == nullptr) {
                throw std::bad_alloc();
            }
            data_ = grown;
            capacity_ = want;
        }
        const std::size_t old = size_;
        size_ = n;
        return {data_ + old, n - old};
    }

    /// Value-initialize `tail`, a range `grow_uninitialized` just handed back.
    ///
    /// One `memset` when `T`'s value-initialized form is all-zero bytes, which
    /// every slot type here is (`AggSlotCore`'s two enums both start at 0). The
    /// per-element copy this replaces cost q18's fill 12ms of its 55: three
    /// million 24-byte `memcpy`s the compiler will not fuse, because it cannot
    /// see that the prototype is zeros.
    ///
    /// Padding is why the test is a run-time `memcmp` rather than a
    /// `static_assert`: value-initialization zeroes `T`'s padding too, so the
    /// comparison is well defined here, but no constant expression can state
    /// that for a type with padding. The loop keeps the class honest for a
    /// future slot type whose default is not all zeros.
    static void fill_default(std::span<T> tail) noexcept {
        if (tail.empty()) {
            return;
        }
        const T prototype{};
        alignas(T) std::array<unsigned char, sizeof(T)> zero{};
        // NOLINTNEXTLINE(cert-exp42-c,bugprone-suspicious-memory-comparison) -- padding note above.
        if (std::memcmp(&prototype, zero.data(), sizeof(T)) == 0) {
            // Through `void*`: `T` has default member initializers, so it is not
            // trivially default-constructible and -Wclass-memaccess objects to
            // memset-ing it directly. The memcmp above is what licenses this.
            std::memset(static_cast<void*>(tail.data()), 0, tail.size() * sizeof(T));
            return;
        }
        for (auto& slot : tail) {
            std::memcpy(&slot, &prototype, sizeof(T));
        }
    }

    /// Grow to `n`, value-initializing the new tail on the calling thread.
    void resize(std::size_t n) { fill_default(grow_uninitialized(n)); }

   private:
    T* data_ = nullptr;
    std::size_t size_ = 0;
    std::size_t capacity_ = 0;
};

class ChunkedAggregateOperator final : public Operator {
   public:
    /// `Cat` carries a Categorical's *code*, which the pair path may treat as
    /// an integer for the same reason `process_rows_cat` may index an array
    /// with it: within one operator a dictionary only ever grows and never
    /// reorders, so a code identifies the same value in every chunk.
    enum class IntKeyKind : std::uint8_t { Int64, Date, Ts, Cat };

    ChunkedAggregateOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                             const std::vector<ir::AggSpec>* aggregations,
                             const ExecutionContext& exec)
        : child_(std::move(child)),
          group_by_(group_by),
          aggregations_(aggregations),
          exec_(&exec) {}

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
            const Chunk chunk = std::move(*chunk_res.value());
            if (auto err = process_chunk(chunk)) {
                return std::unexpected(*err);
            }
            // `chunk` goes out of scope here, releasing its memory
            // before we pull the next one from the child.
        }
        emitted_ = true;
        return build_output_chunk();
    }

   private:
    auto process_chunk(const Chunk& chunk) -> std::optional<std::string> {
        if (std::getenv("IBEX_AGG_PARTITION_DEBUG") != nullptr) {
            std::fprintf(stderr, "[agg_process_chunk] rows=%zu group_by_size=%zu\n", chunk.rows(),
                         group_by_->size());
        }
        // Counted here, once per chunk, because the partition gate below asks
        // how much input this OPERATOR has — a question the per-call row count
        // stopped answering the moment sources began arriving in pieces.
        rows_offered_ += chunk.rows();
        std::vector<const ColumnEntry*> group_entries;
        group_entries.reserve(group_by_->size());
        for (const auto& key : *group_by_) {
            const ColumnEntry* entry = nullptr;
            for (const auto& e : chunk.columns) {
                if (e.name == key.name) {
                    entry = &e;
                    break;
                }
            }
            if (entry == nullptr) {
                return "group-by column not found: " + key.name;
            }
            group_entries.push_back(entry);
        }

        std::vector<const ColumnEntry*> agg_entries(aggregations_->size(), nullptr);
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const auto& agg = (*aggregations_)[i];
            if (agg.func == ir::AggFunc::Count) {
                continue;
            }
            const ColumnEntry* entry = nullptr;
            for (const auto& e : chunk.columns) {
                if (e.name == agg.column.name) {
                    entry = &e;
                    break;
                }
            }
            if (entry == nullptr) {
                return "aggregate column not found: " + agg.column.name;
            }
            const ExprType kind = expr_type_for_column(*entry->column);
            const bool first_or_last =
                agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last;
            // First/Last also accept String (which covers Column<std::string> and
            // Column<Categorical> — expr_type_for_column collapses both to
            // String); every other function stays numeric-only.
            const bool supported = kind == ExprType::Int || kind == ExprType::Double ||
                                   (first_or_last && kind == ExprType::String);
            if (!supported) {
                return "ChunkedAggregateOperator: non-numeric aggregation not supported";
            }
            agg_entries[i] = entry;
        }

        if (!initialized_) {
            n_aggs_ = aggregations_->size();
            plan_.reserve(n_aggs_);
            for (std::size_t i = 0; i < n_aggs_; ++i) {
                SlotPlan p;
                p.func = (*aggregations_)[i].func;
                if (p.func == ir::AggFunc::Count) {
                    p.kind = ExprType::Int;
                } else {
                    p.kind = expr_type_for_column(*agg_entries[i]->column);
                    p.categorical =
                        std::holds_alternative<Column<Categorical>>(*agg_entries[i]->column);
                }
                plan_.push_back(p);
            }
            // Lay the scratch out once the plan is known. Skew/Kurtosis share
            // one online recurrence that updates both higher moments, so each
            // asks for the pair.
            scratch_offset_.assign(n_aggs_, 0);
            scratch_stride_ = 0;
            for (std::size_t i = 0; i < n_aggs_; ++i) {
                // Scratch layout is [m2, m3, m4]. Stddev needs only the
                // first; the higher moments imply it, since their recurrence
                // reads m2 on every update.
                if (plan_[i].func == ir::AggFunc::Stddev) {
                    plan_[i].scratch_doubles = 1;
                } else if (plan_[i].func == ir::AggFunc::Skew ||
                           plan_[i].func == ir::AggFunc::Kurtosis) {
                    plan_[i].scratch_doubles = 3;
                }
                scratch_offset_[i] = static_cast<std::uint32_t>(scratch_stride_);
                scratch_stride_ += plan_[i].scratch_doubles;
            }
            group_templates_.reserve(group_entries.size());
            bool all_cat = true;
            for (const auto* e : group_entries) {
                group_templates_.push_back(make_empty_like(*e->column));
                if (!std::holds_alternative<Column<Categorical>>(*e->column) ||
                    e->validity.has_value()) {
                    all_cat = false;
                }
            }
            cat_fast_path_ = all_cat && !group_entries.empty();
            // Single-string-key fast path: avoids the generic `Key`/ScalarValue
            // variant path used by `process_rows_generic`. High-cardinality
            // `sum by user_id` (~100K distinct strings in 2M rows) was spending
            // most of its time constructing per-row ScalarValue variants and
            // hashing them; the string path uses a string_view map keyed against
            // an owned char/offset dictionary instead.
            str_fast_path_ =
                group_entries.size() == 1 &&
                std::holds_alternative<Column<std::string>>(*group_entries[0]->column) &&
                !group_entries[0]->validity.has_value();
            // Single fixed-width-integer key: a direct value map, no owned Key.
            const auto int_kind_of = [](const ColumnValue& col) -> std::optional<IntKeyKind> {
                if (std::holds_alternative<Column<std::int64_t>>(col)) {
                    return IntKeyKind::Int64;
                }
                if (std::holds_alternative<Column<Date>>(col)) {
                    return IntKeyKind::Date;
                }
                if (std::holds_alternative<Column<Timestamp>>(col)) {
                    return IntKeyKind::Ts;
                }
                return std::nullopt;
            };
            if (group_entries.size() == 1 && !group_entries[0]->validity.has_value()) {
                if (auto kind = int_kind_of(*group_entries[0]->column)) {
                    int_fast_path_ = true;
                    int_key_kind_ = *kind;
                }
            } else if (!cat_fast_path_ && group_entries.size() == 2 &&
                       !group_entries[0]->validity.has_value() &&
                       !group_entries[1]->validity.has_value()) {
                // A Categorical joins the pair path as its code. `cat_fast_path_`
                // already owns the all-Categorical case and is dispatched first,
                // so this is reached only by a *mixed* pair — `by { symbol, day }`
                // over a Categorical and a Date, which otherwise fell to the
                // generic path and hashed the symbol as text once per row.
                const auto pair_kind_of = [&](const ColumnValue& col) -> std::optional<IntKeyKind> {
                    if (std::holds_alternative<Column<Categorical>>(col)) {
                        return IntKeyKind::Cat;
                    }
                    return int_kind_of(col);
                };
                auto ka = pair_kind_of(*group_entries[0]->column);
                auto kb = pair_kind_of(*group_entries[1]->column);
                if (ka.has_value() && kb.has_value()) {
                    pair_int_fast_path_ = true;
                    int_key_kind_ = *ka;
                    int_key_kind_b_ = *kb;
                    const auto is_32_bit = [](IntKeyKind k) {
                        return k == IntKeyKind::Cat || k == IntKeyKind::Date;
                    };
                    pair_packs_u64_ = is_32_bit(*ka) && is_32_bit(*kb);
                }
            } else if (group_entries.size() >= 3) {
                // Three or more keys had no fast path at all: the branches above
                // only recognise one key or two, so everything wider fell to
                // `process_rows_generic`, which hashes a KeyCol tuple per row
                // and hashes a Categorical as TEXT while doing it. If the whole
                // key packs into a flat integer, `process_rows_packed` replaces
                // that with one hash of a POD — and, because a packed key is
                // something `try_discover_partitioned` can carry, threads the
                // discovery too.
                //
                // The probe is discarded; the real plan is rebuilt per chunk,
                // since a Categorical's remap is only valid for its own chunk.
                packed_fast_path_ = encoder_.build_packed_key(group_entries).has_value();
            }
            initialized_ = true;
        } else {
            for (std::size_t i = 0; i < n_aggs_; ++i) {
                if (plan_[i].func == ir::AggFunc::Count) {
                    continue;
                }
                const ExprType kind = expr_type_for_column(*agg_entries[i]->column);
                if (kind != plan_[i].kind) {
                    return "ChunkedAggregateOperator: aggregate column type changed across chunks";
                }
            }
            for (std::size_t i = 0; i < group_entries.size(); ++i) {
                if (group_entries[i]->column->index() != group_templates_[i].index()) {
                    return "ChunkedAggregateOperator: group-by column type changed across chunks";
                }
            }
        }

        const std::size_t rows = chunk.rows();

        // Global aggregate (`select { … }` with no `by`). Every row belongs to
        // group 0, so the generic path below was running a hash probe per row
        // against an EMPTY key just to rediscover that. Accumulate straight
        // into the single group, and — since the groups are independent of row
        // order — fan the row range out across workers.
        if (group_entries.empty()) {
            return process_rows_ungrouped(agg_entries, rows);
        }
        // A fast-path index records only raw values/codes. It therefore cannot
        // distinguish a later null from that value's zero/code representation.
        // Parquet commonly omits an all-valid row group's bitmap, so this is a
        // real streaming transition rather than a schema change visible in the
        // first chunk.
        //
        // Every fast path stores its groups' raw values in a form the generic
        // `KeyRowIndex` can be reseeded from -- `int_order_`/`str_order_`
        // directly, `cat_order_`/`multi_cat_codes_flat_` via the dictionary
        // `group_templates_` still holds, `pair_order_` via both, and the
        // packed path's `group_order_` is already boxed `Key`s (see
        // `migrate_packed_fast_path_to_generic`). Migrating only rebuilds the
        // key->gid lookup; the accumulated `flat_slots_`/`scratch_` those gids
        // already own are untouched, and this chunk then runs the generic path
        // below like any other.
        if ((cat_fast_path_ || str_fast_path_ || int_fast_path_ || pair_int_fast_path_ ||
             packed_fast_path_) &&
            std::ranges::any_of(group_entries, [](const ColumnEntry* entry) {
                return entry->validity.has_value();
            })) {
            if (cat_fast_path_) {
                migrate_cat_fast_path_to_generic(group_entries.size());
            } else if (int_fast_path_) {
                migrate_int_fast_path_to_generic();
            } else if (str_fast_path_) {
                migrate_str_fast_path_to_generic();
            } else if (pair_int_fast_path_) {
                migrate_pair_int_fast_path_to_generic();
            } else if (packed_fast_path_) {
                migrate_packed_fast_path_to_generic();
            }
        }
        if (cat_fast_path_) {
            return process_rows_cat(group_entries, agg_entries, rows);
        }
        if (str_fast_path_) {
            return process_rows_str(group_entries, agg_entries, rows);
        }
        if (int_fast_path_) {
            return process_rows_int(group_entries, agg_entries, rows);
        }
        if (pair_int_fast_path_) {
            return process_rows_int_pair(group_entries, agg_entries, rows);
        }
        if (packed_fast_path_ && rows != 0) {
            auto plan = encoder_.build_packed_key(group_entries);
            if (!plan.has_value()) {
                // The shape was packable when the first chunk fixed the path,
                // so this is an unsupported mid-stream key-layout transition.
                return "ChunkedAggregateOperator: group-by key column gained nulls across chunks";
            }
            if (plan->width <= sizeof(std::uint64_t)) {
                return process_rows_packed(group_entries, agg_entries, plan->cols, rows, packed64_);
            }
            if (plan->width <= sizeof(PackedKeyEncoder::Packed128)) {
                return process_rows_packed(group_entries, agg_entries, plan->cols, rows,
                                           packed128_);
            }
            return process_rows_packed(group_entries, agg_entries, plan->cols, rows, packed256_);
        }
        return process_rows_generic(group_entries, agg_entries, rows);
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_str(const std::vector<const ColumnEntry*>& group_entries,
                          const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        const auto& col = std::get<Column<std::string>>(*group_entries[0]->column);
        const char* src_chars = col.chars_data();
        const std::uint32_t* src_off = col.offsets_data();

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        // High-cardinality string keys are where this path pays: discovery is
        // the serial half, and a string group-by has nothing else to hide it
        // behind. Probing with a view keeps the owning copy per GROUP, as the
        // serial loop below does.
        const auto key_at = [&](std::size_t row) -> std::string_view {
            return std::string_view{src_chars + src_off[row], src_off[row + 1] - src_off[row]};
        };
        if (try_discover_partitioned<std::string, StrViewHash, StrViewEq>(
                key_at, rows, gids, str_partitions_, [&](std::size_t n) { str_order_.resize(n); },
                [&](const std::string& key, std::uint32_t gid, std::size_t) {
                    str_order_[gid] = key;
                },
                kDefaultPartitionMinRows,
                [&](std::uint32_t gid) -> std::string_view { return str_order_[gid]; })) {
            accumulate_gids(gids, agg_entries, rows);
            return std::nullopt;
        }

        // Run-length shortcut: sorted or chunked CSV often has adjacent
        // repeats; skip the hash lookup when the key matches the previous row.
        std::string_view prev_key;
        std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
        for (std::size_t row = 0; row < rows; ++row) {
            const std::string_view key{src_chars + src_off[row], src_off[row + 1] - src_off[row]};
            std::uint32_t gid{};
            if (key == prev_key) {
                gid = prev_gid;
            } else {
                // Transparent lookup on string_view avoids constructing a
                // std::string per probe. Insertions pay one std::string
                // construction per novel key — with libstdc++'s 15-char SSO,
                // 11-char user_id strings stay inline (no heap alloc).
                auto it = str_index_.find(key);
                if (it == str_index_.end()) {
                    gid = static_cast<std::uint32_t>(n_groups_);
                    str_index_.emplace(std::string(key), gid);
                    str_order_.emplace_back(key);
                    ++n_groups_;
                    size_group_arrays();
                } else {
                    gid = static_cast<std::uint32_t>(it->second);
                }
                prev_key = key;
                prev_gid = gid;
            }
            gids[row] = gid;
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    // Single fixed-width-integer key: probe a value -> gid map directly, the way
    // process_rows_str does for strings. Date/Timestamp are read as their raw
    // integer (days / nanos), which is order- and equality-faithful.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_int(const std::vector<const ColumnEntry*>& group_entries,
                          const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        const ColumnValue& key_col = *group_entries[0]->column;
        const std::int64_t* i64 = nullptr;
        const Date* dates = nullptr;
        const Timestamp* stamps = nullptr;
        switch (int_key_kind_) {
            case IntKeyKind::Int64:
                i64 = std::get<Column<std::int64_t>>(key_col).data();
                break;
            case IntKeyKind::Date:
                dates = std::get<Column<Date>>(key_col).data();
                break;
            case IntKeyKind::Ts:
                stamps = std::get<Column<Timestamp>>(key_col).data();
                break;
            case IntKeyKind::Cat:
                // A lone Categorical key never selects this path: it is
                // all-Categorical by definition, so `cat_fast_path_` claims it
                // and dispatches first. Only the pair path admits `Cat`.
                return "ChunkedAggregateOperator: categorical key on the single-int path";
        }
        const auto key_at = [&](std::size_t row) -> std::int64_t {
            switch (int_key_kind_) {
                case IntKeyKind::Int64:
                    return i64[row];
                case IntKeyKind::Date:
                    return dates[row].days;
                case IntKeyKind::Ts:
                    return stamps[row].nanos;
                case IntKeyKind::Cat:
                    break;
            }
            return 0;
        };

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        if (try_discover_partitioned<std::int64_t, robin_hood::hash<std::int64_t>>(
                key_at, rows, gids, int_partitions_, [&](std::size_t n) { int_order_.resize(n); },
                [&](std::int64_t key, std::uint32_t gid, std::size_t) { int_order_[gid] = key; },
                kDefaultPartitionMinRows, [&](std::uint32_t gid) { return int_order_[gid]; })) {
            accumulate_gids(gids, agg_entries, rows);
            return std::nullopt;
        }

        // Run-length shortcut, as in the string path: sorted/chunked input often
        // repeats the key, so skip the map lookup when it matches the last row.
        std::int64_t prev_key = 0;
        std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
        bool have_prev = false;
        for (std::size_t row = 0; row < rows; ++row) {
            const std::int64_t key = key_at(row);
            std::uint32_t gid{};
            if (have_prev && key == prev_key) {
                gid = prev_gid;
            } else {
                auto it = int_index_.find(key);
                if (it == int_index_.end()) {
                    gid = static_cast<std::uint32_t>(n_groups_);
                    int_index_.emplace(key, gid);
                    int_order_.push_back(key);
                    ++n_groups_;
                    size_group_arrays();
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    /// Production ownership threshold for the narrow PairIntKey path below,
    /// backed by a synthetic row/cardinality sweep (32k/64k/128k/262144 rows
    /// x low/high cardinality, 8 cores, 6 interleaved rounds): 32k showed no
    /// reliable win (3/6 wins, ~0%), 64k was the first point with a
    /// consistent, real one (6/6 wins, -6% to -10%), and 128k/262144 stayed
    /// positive. Deliberately NOT `kDefaultPartitionMinRows` (262144, the
    /// threshold `try_discover_partitioned` uses): that value was tuned for a
    /// different mechanism (discovery only, no fused accumulation, no
    /// deferred merge) and is not evidence for where THIS path's overhead
    /// breaks even -- q20's own chunks (~150k rows) sit between the two.
    static constexpr std::size_t kPairOwnedMinRows = 1U << 16U;  // 65536

    /// Production PairIntKey ownership path (TPC-H q20's
    /// `by { l_partkey, l_suppkey }` is the motivating shape; validated
    /// there at -16.5%, 8/8 paired wins, 8 cores, vs. a q18/Int64 prototype
    /// that measured only -7.6%, was never promoted, and has since been
    /// removed -- see plans/parallelism-overview.md). Deliberately narrow:
    ///
    /// - Exactly one aggregate, Sum(Double) or Count. q18 and q20 both only
    ///   ever exercise one, so nothing measures whether row-wise fusion beats
    ///   partition-outer/aggregate-outer accumulation once a query carries
    ///   several -- widen only after that shape is actually benchmarked.
    /// - Row-wise fusion only: with exactly one aggregate a second full row
    ///   scan can only add cost, never locality it does not already have.
    /// - No env-var mode selector: this runs whenever eligible, the same way
    ///   `try_discover_partitioned` has no toggle either. `IBEX_DISABLE_
    ///   OWNED_PAIR_AGG=1` is a kill switch for the unusual case that needs
    ///   one, not a normal control surface.
    template <typename KeyAt>
    auto try_owned_pair(const KeyAt& key_at, std::size_t rows, std::uint32_t* gids,
                        const std::vector<const ColumnEntry*>& agg_entries) -> bool {
        if (std::getenv("IBEX_DISABLE_OWNED_PAIR_AGG") != nullptr) {
            return false;
        }
        if (!owned_mode_) {
            if (n_groups_ > 0 || partitioned_active_) {
                return false;
            }
            if (n_aggs_ != 1) {
                return false;
            }
            if (plan_[0].func != ir::AggFunc::Sum && plan_[0].func != ir::AggFunc::Count) {
                return false;
            }
            if (plan_[0].func == ir::AggFunc::Sum && plan_[0].kind != ExprType::Double) {
                return false;
            }
            if (scratch_stride_ != 0) {
                return false;
            }
            if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread()) {
                return false;
            }
            if (std::max(rows_offered_, rows) < kPairOwnedMinRows) {
                return false;
            }
            auto& pool0 = process_worker_pool();
            const std::size_t budget0 =
                exec_->parallel_threads != 0 ? exec_->parallel_threads : pool0.size();
            if (std::min(budget0, pool0.size()) < 2) {
                return false;
            }
        }

        auto& pool = process_worker_pool();
        const std::size_t budget =
            exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size();
        const std::size_t workers = std::min({budget, pool.size(), std::size_t{64}});
        std::size_t part_count = 1;
        while (part_count * 2 <= workers) {
            part_count *= 2;
        }
        const std::uint64_t part_mask = part_count - 1;
        if (owned_pair_partitions_.size() < part_count) {
            owned_pair_partitions_.resize(part_count);
        }

        const std::size_t ranges = workers;
        const std::size_t grain = (rows + ranges - 1) / ranges;
        part_of_row_.resize(rows);
        std::vector<std::size_t> counts(ranges * part_count, 0);
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* row_counts = counts.data() + (r * part_count);
                PairIntKeyHash hasher;
                for (std::size_t row = begin; row < end; ++row) {
                    const auto part = static_cast<std::uint8_t>(hasher(key_at(row)) & part_mask);
                    part_of_row_[row] = part;
                    ++row_counts[part];
                }
            });
            batch.wait();
        }
        std::vector<std::size_t> offsets(ranges * part_count, 0);
        std::vector<std::size_t> part_begin(part_count + 1, 0);
        {
            std::size_t running = 0;
            for (std::size_t p = 0; p < part_count; ++p) {
                part_begin[p] = running;
                for (std::size_t r = 0; r < ranges; ++r) {
                    offsets[(r * part_count) + p] = running;
                    running += counts[(r * part_count) + p];
                }
            }
            part_begin[part_count] = running;
        }
        scatter_rows_.resize(rows);
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* cursor = offsets.data() + (r * part_count);
                for (std::size_t row = begin; row < end; ++row) {
                    scatter_rows_[cursor[part_of_row_[row]]++] = row;
                }
            });
            batch.wait();
        }

        std::vector<const double*> sum_cols(n_aggs_, nullptr);
        std::vector<const ValidityBitmap*> sum_validity(n_aggs_, nullptr);
        std::vector<std::uint8_t> is_count(n_aggs_, 0);
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (plan_[a].func == ir::AggFunc::Count) {
                is_count[a] = 1;
                continue;
            }
            sum_cols[a] = std::get<Column<double>>(*agg_entries[a]->column).data();
            sum_validity[a] =
                agg_entries[a]->validity.has_value() ? &*agg_entries[a]->validity : nullptr;
        }

        const std::uint64_t row_base = owned_rows_seen_;
        {
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(std::min(workers, part_count), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1, std::memory_order_relaxed); p < part_count;
                     p = cursor.fetch_add(1, std::memory_order_relaxed)) {
                    auto& partition = owned_pair_partitions_[p];
                    for (std::size_t i = part_begin[p]; i < part_begin[p + 1]; ++i) {
                        const std::size_t row = scatter_rows_[i];
                        const PairIntKey key = key_at(row);
                        auto it = partition.index.find(key);
                        std::uint32_t local{};
                        if (it == partition.index.end()) {
                            local = static_cast<std::uint32_t>(partition.keys.size());
                            partition.index.emplace(key, local);
                            partition.keys.push_back(key);
                            partition.first_rows.push_back(row_base + row);
                            partition.slots.resize((local + 1) * n_aggs_);
                        } else {
                            local = it->second;
                        }
                        gids[row] = local;
                        // n_aggs_ == 1 is enforced above -- this is a single
                        // slot update, not a loop over aggregates. Written as
                        // one, not unrolled, so a future widening to >1
                        // aggregate (once actually measured, per the
                        // class-level comment) is a small diff here.
                        AggSlotCore& slot = partition.slots[local];
                        if (is_count[0] != 0) {
                            ++slot.count;
                        } else if (sum_validity[0] == nullptr || (*sum_validity[0])[row]) {
                            slot.double_value += sum_cols[0][row];
                            slot.mark_present();
                        }
                    }
                }
            });
            batch.wait();
        }

        owned_rows_seen_ += rows;
        owned_mode_ = true;
        if (std::getenv("IBEX_AGG_PARTITION_DEBUG") != nullptr) {
            std::fprintf(stderr, "[agg_owned_pair] chunk rows=%zu part_count=%zu total_rows=%llu\n",
                         rows, owned_pair_partitions_.size(),
                         static_cast<unsigned long long>(owned_rows_seen_));
        }
        return true;
    }

    /// Walk every `owned_pair_partitions_` entry once, in first-occurrence
    /// order (a P-way merge over `first_rows`, run once at final emission),
    /// and populate `pair_order_`/`flat_slots_` -- the arrays
    /// `build_output_chunk`'s `pair_int_fast_path_` branch already reads.
    void finalize_owned_pair() {
        if (owned_finalized_) {
            return;
        }
        owned_finalized_ = true;
        const std::size_t part_count = owned_pair_partitions_.size();
        std::vector<std::size_t> cursors(part_count, 0);
        std::size_t total = 0;
        for (const auto& partition : owned_pair_partitions_) {
            total += partition.keys.size();
        }
        n_groups_ = total;
        pair_order_.resize(total);
        AggSlotCore* fs = flat_slots_.grow_uninitialized(total * n_aggs_).data();
        std::size_t g = 0;
        while (true) {
            std::size_t best = part_count;
            std::uint64_t best_row = std::numeric_limits<std::uint64_t>::max();
            for (std::size_t p = 0; p < part_count; ++p) {
                if (cursors[p] >= owned_pair_partitions_[p].keys.size()) {
                    continue;
                }
                if (owned_pair_partitions_[p].first_rows[cursors[p]] < best_row) {
                    best_row = owned_pair_partitions_[p].first_rows[cursors[p]];
                    best = p;
                }
            }
            if (best == part_count) {
                break;
            }
            const auto& partition = owned_pair_partitions_[best];
            const std::size_t local = cursors[best];
            const PairIntKey& key = partition.keys[local];
            pair_order_[g] = {static_cast<std::int64_t>(key.first),
                              static_cast<std::int64_t>(key.second)};
            for (std::size_t a = 0; a < n_aggs_; ++a) {
                fs[(g * n_aggs_) + a] = partition.slots[(local * n_aggs_) + a];
            }
            ++cursors[best];
            ++g;
        }
    }

    // Two fixed-width-integer keys, grouped as one composite. Mirrors
    // process_rows_int exactly, packing (key_a, key_b) into a two-word key so
    // a single hash probe replaces the generic path's per-key Key comparison.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_int_pair(const std::vector<const ColumnEntry*>& group_entries,
                               const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        // Bind the key column's buffer once, the way `process_rows_int` does.
        // Reading it through `std::get` per row costs a variant index check per
        // key per row and re-derives the pointer every time, which on 8M rows
        // over two keys was most of this loop.
        struct RawKeyReader {
            const std::int64_t* i64 = nullptr;
            const Date* dates = nullptr;
            const Timestamp* stamps = nullptr;
            const Column<Categorical>::code_type* codes = nullptr;
            IntKeyKind kind = IntKeyKind::Int64;

            [[nodiscard]] auto operator()(std::size_t row) const -> std::int64_t {
                switch (kind) {
                    case IntKeyKind::Int64:
                        return i64[row];
                    case IntKeyKind::Date:
                        return dates[row].days;
                    case IntKeyKind::Ts:
                        return stamps[row].nanos;
                    case IntKeyKind::Cat:
                        return codes[row];
                }
                return 0;
            }
        };
        const auto bind_reader = [](const ColumnValue& col, IntKeyKind kind) -> RawKeyReader {
            RawKeyReader reader;
            reader.kind = kind;
            switch (kind) {
                case IntKeyKind::Int64:
                    reader.i64 = std::get<Column<std::int64_t>>(col).data();
                    break;
                case IntKeyKind::Date:
                    reader.dates = std::get<Column<Date>>(col).data();
                    break;
                case IntKeyKind::Ts:
                    reader.stamps = std::get<Column<Timestamp>>(col).data();
                    break;
                case IntKeyKind::Cat:
                    reader.codes = std::get<Column<Categorical>>(col).codes_data();
                    break;
            }
            return reader;
        };
        const auto key_a_at = bind_reader(*group_entries[0]->column, int_key_kind_);
        const auto key_b_at = bind_reader(*group_entries[1]->column, int_key_kind_b_);
        const auto pack = [](std::int64_t a, std::int64_t b) -> PairIntKey {
            return {.first = static_cast<std::uint64_t>(a),
                    .second = static_cast<std::uint64_t>(b)};
        };

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        // A Categorical code and a Date are both 32 bits wide, so when both
        // keys are one of those the composite fits in 64 bits exactly and can
        // be probed in the same flat int map the single-int path uses. That is
        // the common shape of `by { symbol, day }`, and it halves the key
        // width, the hash and the stored entry against the 128-bit form.
        // Both key domains are 32 bits wide here, so the composite is exact.
        const auto pack_u64 = [](std::int64_t a, std::int64_t b) -> std::int64_t {
            return static_cast<std::int64_t>(
                (static_cast<std::uint64_t>(static_cast<std::uint32_t>(a)) << 32U) |
                static_cast<std::uint64_t>(static_cast<std::uint32_t>(b)));
        };
        if (pair_packs_u64_) {
            // Both key domains are narrow enough to enumerate: a Categorical
            // spans its dictionary, and a Date column's span is measured. When
            // the product fits, index a flat cell -> gid array and the per-row
            // hash disappears entirely -- the same trick, and the same reason,
            // as the all-Categorical Cartesian path. `by { day }` over 4
            // distinct days was costing 34ms on 8M rows purely in hash probes.
            if (try_process_rows_pair_dense(key_a_at, key_b_at, group_entries, agg_entries, rows)) {
                return std::nullopt;
            }
            // Discovery across workers, for the case the dense array cannot
            // hold: the cell budget is a product, so a wide symbol domain times
            // a wide day domain overflows it long before either alone is
            // remarkable, and the u64 key that falls out is the CHEAPEST key in
            // this file to partition. Until now this branch returned before ever
            // reaching `try_discover_partitioned` — a `by { symbol, day }` over
            // 5000 symbols and 1000 days ran wholly serially.
            //
            // Only while the dense path has never run. Dense numbers groups in
            // its own array and rebuilds that array from `pair_order_`, so it
            // can safely take over from partitioned discovery; the reverse is
            // not true, because the partitions would not know the groups dense
            // had already numbered and would issue second ids for them.
            if (!pair_dense_active_ &&
                try_discover_partitioned<std::int64_t, robin_hood::hash<std::int64_t>>(
                    [&](std::size_t row) { return pack_u64(key_a_at(row), key_b_at(row)); }, rows,
                    gids, int_partitions_, [&](std::size_t n) { pair_order_.resize(n); },
                    [&](std::int64_t, std::uint32_t gid, std::size_t row) {
                        // From the row, not by unpacking: `pair_order_` holds the
                        // reader's own values, and the pack truncates to 32 bits.
                        pair_order_[gid] = {key_a_at(row), key_b_at(row)};
                    },
                    kDefaultPartitionMinRows,
                    [&](std::uint32_t gid) {
                        // The pack is a pure function of the pair, so a group's
                        // key is recoverable even though the pack is lossy.
                        return pack_u64(pair_order_[gid].first, pair_order_[gid].second);
                    })) {
                accumulate_gids(gids, agg_entries, rows);
                return std::nullopt;
            }

            // Falling here with groups already numbered means the dense path ran
            // on an earlier chunk and this chunk's domains overflowed its budget.
            // `int_index_` has no record of those groups, so without this it
            // would issue a second id for each and the output would carry two
            // rows per key. `pair_order_` is the pair path's source of truth —
            // this is the same rebuild dense itself does when its bounds move.
            if (int_index_.size() < pair_order_.size()) {
                int_index_.reserve(pair_order_.size());
                for (std::size_t g = 0; g < pair_order_.size(); ++g) {
                    int_index_.emplace(pack_u64(pair_order_[g].first, pair_order_[g].second),
                                       static_cast<std::uint32_t>(g));
                }
            }

            std::int64_t prev_packed = 0;
            std::uint32_t prev_gid_u64 = std::numeric_limits<std::uint32_t>::max();
            bool have_prev_u64 = false;
            for (std::size_t row = 0; row < rows; ++row) {
                const std::int64_t a = key_a_at(row);
                const std::int64_t b = key_b_at(row);
                const std::int64_t key = pack_u64(a, b);
                std::uint32_t gid{};
                if (have_prev_u64 && key == prev_packed) {
                    gid = prev_gid_u64;
                } else {
                    auto it = int_index_.find(key);
                    if (it == int_index_.end()) {
                        gid = static_cast<std::uint32_t>(n_groups_);
                        int_index_.emplace(key, gid);
                        pair_order_.emplace_back(a, b);
                        ++n_groups_;
                        size_group_arrays();
                    } else {
                        gid = it->second;
                    }
                    prev_packed = key;
                    prev_gid_u64 = gid;
                    have_prev_u64 = true;
                }
                gids[row] = gid;
            }
            accumulate_gids(gids, agg_entries, rows);
            return std::nullopt;
        }

        if (try_owned_pair([&](std::size_t row) { return pack(key_a_at(row), key_b_at(row)); },
                           rows, gids, agg_entries)) {
            return std::nullopt;
        }

        if (try_discover_partitioned<PairIntKey, PairIntKeyHash>(
                [&](std::size_t row) { return pack(key_a_at(row), key_b_at(row)); }, rows, gids,
                pair_partitions_, [&](std::size_t n) { pair_order_.resize(n); },
                [&](const PairIntKey& key, std::uint32_t gid, std::size_t) {
                    pair_order_[gid] = {static_cast<std::int64_t>(key.first),
                                        static_cast<std::int64_t>(key.second)};
                },
                kDefaultPartitionMinRows,
                [&](std::uint32_t gid) {
                    return pack(pair_order_[gid].first, pair_order_[gid].second);
                })) {
            accumulate_gids(gids, agg_entries, rows);
            return std::nullopt;
        }

        PairIntKey prev_key{};
        std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
        bool have_prev = false;
        for (std::size_t row = 0; row < rows; ++row) {
            const std::int64_t a = key_a_at(row);
            const std::int64_t b = key_b_at(row);
            const PairIntKey key = pack(a, b);
            std::uint32_t gid{};
            if (have_prev && key == prev_key) {
                gid = prev_gid;
            } else {
                auto it = pair_index_.find(key);
                if (it == pair_index_.end()) {
                    gid = static_cast<std::uint32_t>(n_groups_);
                    pair_index_.emplace(key, gid);
                    pair_order_.emplace_back(a, b);
                    ++n_groups_;
                    size_group_arrays();
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    /// Bounds of one key column over a chunk, as the dense cell numbering needs
    /// them. A Categorical answers from its dictionary without reading a row;
    /// anything else is measured.
    template <typename Reader>
    auto key_bounds(const Reader& read, const ColumnValue& col, IntKeyKind kind, std::size_t rows)
        -> std::pair<std::int64_t, std::int64_t> {
        if (kind == IntKeyKind::Cat) {
            const auto size = std::get<Column<Categorical>>(col).dictionary().size();
            return {0, size == 0 ? 0 : static_cast<std::int64_t>(size) - 1};
        }
        std::int64_t lo = read(0);
        std::int64_t hi = lo;
        for (std::size_t row = 1; row < rows; ++row) {
            const std::int64_t v = read(row);
            lo = std::min(lo, v);
            hi = std::max(hi, v);
        }
        return {lo, hi};
    }

    /// Group a packed 32-bit key pair through a flat cell array. Returns false
    /// when the key domains are too large to enumerate, leaving the caller on
    /// the hash path.
    ///
    /// The cell numbering is a function of the bounds, so a later chunk that
    /// widens them invalidates every cell already handed out. That is handled
    /// the way the multi-key Categorical path handles a stride change: widen to
    /// the union and rebuild the array from `pair_order_`, which holds each
    /// group's key pair. Group ids themselves never move.
    template <typename ReaderA, typename ReaderB>
    auto try_process_rows_pair_dense(const ReaderA& key_a_at, const ReaderB& key_b_at,
                                     // One caller
                                     // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                                     const std::vector<const ColumnEntry*>& group_entries,
                                     const std::vector<const ColumnEntry*>& agg_entries,
                                     std::size_t rows) -> bool {
        if (rows == 0) {
            return false;
        }
        const auto [a_lo, a_hi] =
            key_bounds(key_a_at, *group_entries[0]->column, int_key_kind_, rows);
        const auto [b_lo, b_hi] =
            key_bounds(key_b_at, *group_entries[1]->column, int_key_kind_b_, rows);

        std::int64_t a_min = a_lo;
        std::int64_t b_min = b_lo;
        std::int64_t a_max = a_hi;
        std::int64_t b_max = b_hi;
        if (pair_dense_active_) {
            a_min = std::min(a_min, pair_dense_a_min_);
            b_min = std::min(b_min, pair_dense_b_min_);
            a_max = std::max(a_max, pair_dense_a_max_);
            b_max = std::max(b_max, pair_dense_b_max_);
        }

        // Spans are computed in unsigned arithmetic so a domain that legitimately
        // straddles zero cannot overflow the subtraction.
        const auto a_span = static_cast<std::uint64_t>(a_max - a_min) + 1;
        const auto b_span = static_cast<std::uint64_t>(b_max - b_min) + 1;
        if (b_span != 0 && a_span > kDenseCellLimit / b_span) {
            return false;  // product overflows the dense budget
        }
        const std::uint64_t cells = a_span * b_span;
        if (cells > kDenseCellLimit) {
            return false;
        }

        const bool bounds_changed = !pair_dense_active_ || a_min != pair_dense_a_min_ ||
                                    b_min != pair_dense_b_min_ || b_span != pair_dense_b_span_;
        if (bounds_changed) {
            pair_dense_gid_.assign(static_cast<std::size_t>(cells), kNoGid);
            for (std::size_t g = 0; g < pair_order_.size(); ++g) {
                const auto cell =
                    (static_cast<std::uint64_t>(pair_order_[g].first - a_min) * b_span) +
                    static_cast<std::uint64_t>(pair_order_[g].second - b_min);
                pair_dense_gid_[static_cast<std::size_t>(cell)] = static_cast<std::uint32_t>(g);
            }
            pair_dense_a_min_ = a_min;
            pair_dense_b_min_ = b_min;
            pair_dense_a_max_ = a_max;
            pair_dense_b_max_ = b_max;
            pair_dense_b_span_ = b_span;
            pair_dense_active_ = true;
        } else if (pair_dense_gid_.size() < cells) {
            pair_dense_gid_.resize(static_cast<std::size_t>(cells), kNoGid);
            pair_dense_a_max_ = a_max;
        }

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();
        std::uint32_t* dense = pair_dense_gid_.data();
        for (std::size_t row = 0; row < rows; ++row) {
            const std::int64_t a = key_a_at(row);
            const std::int64_t b = key_b_at(row);
            const auto cell = (static_cast<std::uint64_t>(a - a_min) * b_span) +
                              static_cast<std::uint64_t>(b - b_min);
            std::uint32_t gid = dense[cell];
            if (gid == kNoGid) {
                gid = static_cast<std::uint32_t>(n_groups_);
                dense[cell] = gid;
                pair_order_.emplace_back(a, b);
                ++n_groups_;
                size_group_arrays();
                dense = pair_dense_gid_.data();
            }
            gids[row] = gid;
        }

        accumulate_gids(gids, agg_entries, rows);
        return true;
    }

    /// Slot-indexed boxed value, grown on first use. `slot_index` is the same
    /// `gid * n_aggs_ + agg_i` that indexes `flat_slots_`.
    auto text_at(std::size_t slot_index) -> ScalarValue& {
        if (text_store_.size() < flat_slots_.size()) {
            text_store_.resize(flat_slots_.size());
        }
        return text_store_[slot_index];
    }

    /// Scratch for one (group, aggregate). Only valid when that aggregate
    /// declared scratch_doubles > 0.
    [[nodiscard]] auto scratch_for(std::size_t gid, std::size_t agg_i) -> double* {
        return scratch_.data() + (gid * scratch_stride_) + scratch_offset_[agg_i];
    }

    /// Size every per-group array to `n_groups_`. THE ONLY PLACE THAT RESIZES
    /// THEM — the grouping fast paths used to call `flat_slots_.resize()`
    /// directly, and adding a second per-group array (scratch) to just one of
    /// those call sites left the others reading a null pointer.
    /// One hash partition's share of the group index. Partitions are disjoint by
    /// construction — a key's partition is a function of its hash — so a worker
    /// owning a partition owns every row and every group in it, and needs no
    /// lock and no merge against the others.
    /// `Eq` is spelled out so a transparent hash/equal pair can be used: the
    /// string path stores owning `std::string` keys but probes with
    /// `std::string_view`, and only pays the copy on a genuinely new group —
    /// exactly what the serial `str_index_` does.
    template <typename Key, typename Hash, typename Eq = std::equal_to<Key>>
    struct KeyPartition {
        robin_hood::unordered_flat_map<Key, std::uint32_t, Hash, Eq> index;
        /// This partition's groups, in the order they were first seen — which,
        /// because rows are scattered in row order, is ascending by first row.
        /// That is what makes the final ordering a merge of already-sorted
        /// lists rather than a sort.
        std::vector<std::uint32_t> gids;
        std::vector<std::uint64_t> first_rows;
        std::vector<Key> keys;
        /// How many of `keys` have already been written back to the caller's
        /// gid-indexed key vector; the rest were added by the current chunk.
        std::size_t stored = 0;
    };

    /// Discover groups across workers by hash-partitioning the rows.
    ///
    /// **Group DISCOVERY is the serial half of a high-cardinality group-by, and
    /// it is the half that could not be threaded before.** `accumulate_gids`
    /// already fans out the summing, but only once gids exist, and its gate
    /// declines exactly when groups are numerous — `morsels * n_groups > rows/4`
    /// — because merging per-morsel partial tables then costs more than the scan
    /// it saved. PDS-H q20 (543k groups) and q13 (150k) both land there and run
    /// wholly serially.
    ///
    /// Partitioning removes the merge instead of paying it. Each key belongs to
    /// exactly one partition, so per-partition tables never have to be
    /// reconciled: there is no partial to combine, only a concatenation. It also
    /// shrinks each table to a P-th of the rows, which is most of the win at
    /// this cardinality — the serial probe is cache-miss bound on a table far
    /// larger than L2.
    ///
    /// Returns false when the shape does not justify it and the caller should
    /// run its own serial loop.
    ///
    /// **Ordering.** Ibex reports groups in first-occurrence order, and gids
    /// here are handed out by an atomic, so gid order is a race. The order is
    /// recovered from data instead: every group records the row it was first
    /// seen at, and `build_output_chunk` walks the groups by that. Rows scatter
    /// into partitions in row order, so each partition's list is already
    /// ascending and the global order is a P-way merge, not a sort.
    ///
    /// **Determinism.** A group's rows all live in one partition and are visited
    /// in row order, so each group's values accumulate in exactly the order the
    /// serial path would use. The output is byte-identical, not merely
    /// equivalent — including the float sums.
    /// Passed as `key_of_group` by a caller that cannot reconstruct a
    /// partition key from a group id, which is what decides whether this path
    /// may start part-way through a stream. Only the packed path is in that
    /// position: its key is built from a ROW and is not invertible.
    struct NoGroupKeys {};

    template <typename Key, typename Hash, typename Eq = std::equal_to<Key>, typename KeyAt,
              typename ResizeKeys, typename StoreKey, typename KeyOfGroup = NoGroupKeys>
    auto try_discover_partitioned(const KeyAt& key_at, std::size_t rows, std::uint32_t* gids,
                                  std::vector<KeyPartition<Key, Hash, Eq>>& partitions,
                                  const ResizeKeys& resize_keys, const StoreKey& store_key,
                                  std::size_t min_rows = kDefaultPartitionMinRows,
                                  const KeyOfGroup& key_of_group = {}) -> bool {
        // Below `min_rows` the partition and scatter passes cost more than the
        // serial probe they replace. High cardinality is not checkable up front
        // — it is what discovery is about to find out — so row count is the only
        // gate available, and a low-cardinality run of this size still wins from
        // the smaller per-partition tables.
        //
        // It is a parameter because the break-even is a property of the KEY, not
        // of partitioning: the serial probe a packed key replaces is far more
        // expensive per row than the one an int key replaces, so it pays off
        // sooner. Callers that do not pass it keep the original threshold.
        //
        // Once this path HAS run, every later chunk must take it too, however
        // small. The groups it discovered live in `partitions`, and the serial
        // loops probe `int_index_` / `str_index_`, which this path never
        // populates — so a small trailing chunk falling back would not find the
        // existing groups and would allocate second ids for them. The row gate
        // therefore only guards the first use.
        //
        // The gate counts every row this operator has been OFFERED, not the
        // rows in this call. They were the same number while a source produced
        // one chunk; once it produces six, a per-call gate sees a sixth of the
        // input and declines on a query that plainly qualifies. PDS-H q20 is
        // exactly that: 909k rows over 543k groups, which activated this path
        // as one chunk and lost it entirely as six, taking the aggregate from
        // 50ms to 79ms and the query +23%. The threshold itself is unchanged —
        // lowering it is a measured dead end, because the break-even is set by
        // group CARDINALITY and a low-cardinality run of this size loses.
        constexpr bool can_seed = !std::is_same_v<KeyOfGroup, NoGroupKeys>;
        if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread()) {
            return false;
        }
        if (!partitioned_active_) {
            if (std::max(rows_offered_, rows) < min_rows) {
                return false;
            }
            // Starting part-way through means groups already exist, and they
            // live in the serial index this path neither reads nor writes.
            // They have to be moved across (below) or they would be issued
            // second ids; a caller that cannot hand back their keys cannot
            // start late at all.
            if (n_groups_ > 0 && !can_seed) {
                return false;
            }
        }
        auto& pool = process_worker_pool();
        const std::size_t budget =
            exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size();
        std::size_t workers = std::min({budget, pool.size(), std::size_t{64}});
        if (workers < 2) {
            return false;
        }
        std::size_t part_count = 1;
        while (part_count * 2 <= workers) {
            part_count *= 2;  // a power of two, so the partition is a mask
        }
        const std::uint64_t part_mask = part_count - 1;
        if (partitions.size() < part_count) {
            partitions.resize(part_count);
        }

        // Adopt the groups the serial path already discovered, so this path can
        // start on any chunk rather than only the first. Each keeps its
        // existing global id, and `stored` is set past them all: the ordering
        // merge below only visits entries added by the current chunk, so their
        // `first_rows` are never read and the ids handed out here continue
        // after them — which is the same invariant that lets one partitioned
        // chunk follow another.
        if constexpr (can_seed) {
            if (!partitioned_active_ && n_groups_ > 0) {
                Hash hasher;
                for (std::uint32_t gid = 0; gid < static_cast<std::uint32_t>(n_groups_); ++gid) {
                    auto key = key_of_group(gid);
                    auto& partition = partitions[static_cast<std::size_t>(hasher(key) & part_mask)];
                    partition.index.emplace(Key(key),
                                            static_cast<std::uint32_t>(partition.gids.size()));
                    partition.gids.push_back(gid);
                    partition.first_rows.push_back(0);
                    partition.keys.emplace_back(key);
                }
                for (auto& partition : partitions) {
                    partition.stored = partition.gids.size();
                }
            }
        }

        // Pass 1: partition of every row, and a per-range histogram. Ranges are
        // contiguous so that the scatter below keeps rows in row order within a
        // partition, which is what the ordering argument above depends on.
        const std::size_t ranges = workers;
        const std::size_t grain = (rows + ranges - 1) / ranges;
        part_of_row_.resize(rows);
        std::vector<std::size_t> counts(ranges * part_count, 0);
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* row_counts = counts.data() + (r * part_count);
                Hash hasher;
                for (std::size_t row = begin; row < end; ++row) {
                    const auto part = static_cast<std::uint8_t>(hasher(key_at(row)) & part_mask);
                    part_of_row_[row] = part;
                    ++row_counts[part];
                }
            });
            batch.wait();
        }

        // Exclusive prefix sum, partition-major then range-major, so each range
        // writes its own slice of each partition without touching a shared
        // cursor.
        std::vector<std::size_t> offsets(ranges * part_count, 0);
        std::vector<std::size_t> part_begin(part_count + 1, 0);
        {
            std::size_t running = 0;
            for (std::size_t p = 0; p < part_count; ++p) {
                part_begin[p] = running;
                for (std::size_t r = 0; r < ranges; ++r) {
                    offsets[(r * part_count) + p] = running;
                    running += counts[(r * part_count) + p];
                }
            }
            part_begin[part_count] = running;
        }

        // Pass 2: scatter row indices into their partition's slice.
        scatter_rows_.resize(rows);
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* cursor = offsets.data() + (r * part_count);
                for (std::size_t row = begin; row < end; ++row) {
                    scatter_rows_[cursor[part_of_row_[row]]++] = row;
                }
            });
            batch.wait();
        }

        // Pass 3: each worker owns whole partitions. Ids assigned here are
        // partition-LOCAL — a plain counter, no atomic — because a global id
        // cannot be handed out in first-occurrence order until every partition
        // has been seen. `gids[row]` therefore holds a local id until pass 4.
        const std::uint64_t row_base = rows_seen_;
        {
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(std::min(workers, part_count), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1, std::memory_order_relaxed); p < part_count;
                     p = cursor.fetch_add(1, std::memory_order_relaxed)) {
                    auto& partition = partitions[p];
                    for (std::size_t i = part_begin[p]; i < part_begin[p + 1]; ++i) {
                        const std::size_t row = scatter_rows_[i];
                        // `auto`, not `Key`: the probe type may be a view onto
                        // the key column (strings), and materializing an owning
                        // key per ROW rather than per GROUP is the whole cost
                        // this path exists to avoid.
                        const auto key = key_at(row);
                        auto it = partition.index.find(key);
                        std::uint32_t local{};
                        if (it == partition.index.end()) {
                            local = static_cast<std::uint32_t>(partition.gids.size());
                            partition.index.emplace(Key(key), local);
                            partition.gids.push_back(0);  // filled below, in order
                            partition.first_rows.push_back(row_base + row);
                            partition.keys.emplace_back(key);
                        } else {
                            local = it->second;
                        }
                        gids[row] = local;
                    }
                }
            });
            batch.wait();
        }

        // Number this chunk's new groups in first-occurrence order, so that gid
        // order IS that order and nothing downstream has to compensate.
        //
        // Emitting in discovery order and permuting at the end was tried first
        // and is a trap: the permutation turns the emit's sequential walk of the
        // slot array into a random gather, which on q18's 1.5m-group aggregate
        // cost more than the parallel discovery saved (+25% median). Paying one
        // ordered pass here instead keeps every later pass sequential.
        //
        // Each partition's new entries are already ascending by first row, so
        // this is a P-way merge. Groups carried over from earlier chunks keep
        // their ids: a group first seen in a later chunk necessarily has a later
        // first row, so appending after them preserves the global order.
        //
        // Rescanning every partition per group is O(groups x partitions), and
        // replacing it with the textbook heap (replace-top, one sift per group,
        // O(groups log partitions)) is a MEASURED DEAD END: q18's merge went
        // 25.6ms -> 27.1ms and q20's 7.9ms -> 13.1ms, suite +0.15% over 12
        // interleaved rounds. P is `part_count`, a power of two capped by the
        // worker count -- 8 here. Eight predictable compares over an array that
        // never leaves L1 beat three sift levels of data-dependent branching and
        // struct moves. A heap would need dozens of runs before it paid.
        const std::size_t base = n_groups_;
        {
            std::vector<std::size_t> cursors(part_count);
            for (std::size_t p = 0; p < part_count; ++p) {
                cursors[p] = partitions[p].stored;
            }
            std::uint32_t next = static_cast<std::uint32_t>(base);
            while (true) {
                std::size_t best = part_count;
                std::uint64_t best_row = std::numeric_limits<std::uint64_t>::max();
                for (std::size_t p = 0; p < part_count; ++p) {
                    if (cursors[p] >= partitions[p].first_rows.size()) {
                        continue;
                    }
                    if (partitions[p].first_rows[cursors[p]] < best_row) {
                        best_row = partitions[p].first_rows[cursors[p]];
                        best = p;
                    }
                }
                if (best == part_count) {
                    break;
                }
                partitions[best].gids[cursors[best]] = next++;
                ++cursors[best];
            }
            n_groups_ = next;
        }
        size_group_arrays();
        // The caller's gid-indexed key vector has to cover the ids just handed
        // out before any of them is written back.
        resize_keys(n_groups_);
        for (auto& partition : partitions) {
            for (std::size_t i = partition.stored; i < partition.gids.size(); ++i) {
                // The third argument is the group's first row WITHIN THIS
                // CHUNK. Only entries from `stored` on are visited, and those
                // are exactly the groups this call discovered, so their first
                // row is always local and `row_base` recovers it. A packed key
                // is not invertible on its own — the packed path uses this row
                // to read the original column values back for the output.
                store_key(partition.keys[i], partition.gids[i],
                          static_cast<std::size_t>(partition.first_rows[i] - row_base));
            }
            partition.stored = partition.gids.size();
        }

        // Pass 4: local id -> global gid. The partition a row belongs to is
        // already recorded, so this is a lookup, not a re-probe.
        {
            auto batch = pool.submit(ranges, [&](std::size_t r) {
                const std::size_t begin = r * grain;
                const std::size_t end = std::min(rows, begin + grain);
                for (std::size_t row = begin; row < end; ++row) {
                    gids[row] = partitions[part_of_row_[row]].gids[gids[row]];
                }
            });
            batch.wait();
        }
        rows_seen_ += rows;
        partitioned_active_ = true;
        return true;
    }

    void size_group_arrays() {
        auto tail = flat_slots_.grow_uninitialized(n_groups_ * n_aggs_);
        if (!fill_slots_parallel(tail)) {
            SlotArray<AggSlotCore>::fill_default(tail);
        }
        if (scratch_stride_ != 0) {
            scratch_.resize(n_groups_ * scratch_stride_, 0.0);
        }
    }

    /// Zero a freshly grown slot tail across workers. Returns false when the
    /// tail is too small to be worth a batch — which is every call from
    /// `alloc_group`, where the tail is one slot — and the caller fills it
    /// serially.
    ///
    /// Worth threading at all only because the cost is page faults rather than
    /// bytes: the kernel materializes a page per fault, and eight threads
    /// faulting disjoint pages fault in parallel. Eight threads buy about 1.3x,
    /// not 8x — the fault path serializes on the kernel's own locks — so this is
    /// a small win, not a lever. Sizing it against the rest of q18's aggregate
    /// (3M groups): 80ms parallel discovery probe, 44ms this fill, 26ms serial
    /// first-occurrence merge, 12ms key-array growth, 10ms accumulate. It is the
    /// largest SERIAL block, which is why it is threaded first, and it is still
    /// only a sixth of the operator.
    auto fill_slots_parallel(std::span<AggSlotCore> tail) -> bool {
        // Sized so the batch (a submit plus a join) stays small against the
        // work. Below a few megabytes the serial memset is already
        // bandwidth-bound and has no faults left to hide.
        constexpr std::size_t kMinTailBytes = 4UL << 20;
        if (tail.size() * sizeof(AggSlotCore) < kMinTailBytes) {
            return false;
        }
        if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread()) {
            return false;
        }
        auto& pool = process_worker_pool();
        const std::size_t threads = std::min(
            std::size_t{16}, exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size());
        if (threads < 2) {
            return false;
        }
        const std::size_t grain = (tail.size() + threads - 1) / threads;
        auto batch = pool.submit(threads, [&](std::size_t t) {
            const std::size_t begin = t * grain;
            if (begin >= tail.size()) {
                return;
            }
            const std::size_t end = std::min(tail.size(), begin + grain);
            SlotArray<AggSlotCore>::fill_default(tail.subspan(begin, end - begin));
        });
        batch.wait();
        return true;
    }

    auto alloc_group() -> std::uint32_t {
        auto gid = static_cast<std::uint32_t>(n_groups_);
        ++n_groups_;
        size_group_arrays();
        return gid;
    }

    /// Seed `group_order_`/`key_index_` (the generic path's state) with `n`
    /// groups a fast path already discovered, in the same first-seen order
    /// the fast path used -- so gid `i` here matches the gid `flat_slots_`/
    /// `scratch_` already hold data for at index `i`. `key_at` builds the
    /// full `Key` (every fast path here stores raw values only, never a
    /// null, so every migrated `Key` has an empty null mask).
    ///
    /// Hashing goes through `hash_key_value`, which is defined to agree with
    /// `hash_key_row` on every value both can express -- the invariant this
    /// whole migration rests on: a later chunk's row-based probe and a
    /// migrated group's stored hash must land the same value in the same
    /// slot. `KeyRowIndex::rehash` reproduces the exact open-address
    /// placement `find_or_insert` would have made one row at a time, so a
    /// batch reseed and an incremental build agree on where every group ends
    /// up.
    template <typename KeyAt>
    void seed_generic_index_from_keys(std::size_t n, const KeyAt& key_at) {
        group_order_.reserve(group_order_.size() + n);
        key_index_.hashes.reserve(key_index_.hashes.size() + n);
        for (std::size_t i = 0; i < n; ++i) {
            Key key = key_at(i);
            key_index_.hashes.push_back(hash_key_value(key));
            group_order_.push_back(std::move(key));
        }
        std::size_t capacity = 1024;
        while (capacity * 7 < key_index_.hashes.size() * 10) {
            capacity *= 2;
        }
        key_index_.rehash(capacity);
    }

    /// A single `int_key_kind_`-typed raw value, as a `ScalarValue` matching
    /// what `push_key_value` would have built for the equivalent column.
    static auto scalar_of_int_key(IntKeyKind kind, std::int64_t raw) -> ScalarValue {
        switch (kind) {
            case IntKeyKind::Date:
                return Date{.days = static_cast<std::int32_t>(raw)};
            case IntKeyKind::Ts:
                return Timestamp{.nanos = raw};
            case IntKeyKind::Int64:
            case IntKeyKind::Cat:
                break;
        }
        return raw;
    }

    /// Fold the single-int fast path's raw values (int64 / Date / Timestamp,
    /// as `process_rows_int` stores them) into the generic grouping path when
    /// a later chunk brings a validity bitmap the fast path cannot express.
    /// The accumulated slots stay put -- only the key->gid lookup is rebuilt.
    void migrate_int_fast_path_to_generic() {
        seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
            Key key;
            key.values.push_back(scalar_of_int_key(int_key_kind_, int_order_[i]));
            return key;
        });
        int_fast_path_ = false;
    }

    /// Same migration as `migrate_int_fast_path_to_generic`, for the
    /// single-string fast path's `str_order_`.
    void migrate_str_fast_path_to_generic() {
        seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
            Key key;
            key.values.push_back(str_order_[i]);
            return key;
        });
        str_fast_path_ = false;
    }

    /// Fold the categorical fast path -- single-key (`cat_order_`, code ==
    /// dictionary index) or multi-key (`multi_cat_codes_flat_`, `n_keys`
    /// codes per group) -- into the generic grouping path. A code only means
    /// something against ITS column's dictionary, which `group_templates_`
    /// still holds (the empty `make_empty_like` template built at
    /// `initialized_` time shares the dictionary every chunk's column uses),
    /// so decoding a migrated group's code to the same string `push_key_value`
    /// would have read off the live column is just a dictionary lookup.
    void migrate_cat_fast_path_to_generic(std::size_t n_keys) {
        const auto decode = [&](std::size_t c, Column<Categorical>::code_type code) {
            return std::get<Column<Categorical>>(group_templates_[c])
                .dictionary()[static_cast<std::size_t>(code)];
        };
        if (n_keys == 1) {
            seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
                Key key;
                key.values.push_back(std::string(decode(0, cat_order_[i])));
                return key;
            });
        } else {
            seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
                Key key;
                key.values.reserve(n_keys);
                for (std::size_t c = 0; c < n_keys; ++c) {
                    key.values.push_back(
                        std::string(decode(c, multi_cat_codes_flat_[(i * n_keys) + c])));
                }
                return key;
            });
        }
        cat_fast_path_ = false;
    }

    /// Fold the paired-int fast path's raw values (`pair_order_`, one
    /// `int64`/`Date`/`Timestamp`/categorical-code pair per group) into the
    /// generic grouping path. `int_key_kind_`/`int_key_kind_b_` name each
    /// column's type; a `Cat` column's code decodes through the matching
    /// `group_templates_` entry exactly as the single/multi categorical
    /// migration does.
    void migrate_pair_int_fast_path_to_generic() {
        const auto scalar_at = [&](IntKeyKind kind, std::size_t col_index,
                                   std::int64_t raw) -> ScalarValue {
            if (kind == IntKeyKind::Cat) {
                const auto& dict =
                    std::get<Column<Categorical>>(group_templates_[col_index]).dictionary();
                return std::string(dict[static_cast<std::size_t>(raw)]);
            }
            return scalar_of_int_key(kind, raw);
        };
        seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
            Key key;
            key.values.reserve(2);
            key.values.push_back(scalar_at(int_key_kind_, 0, pair_order_[i].first));
            key.values.push_back(scalar_at(int_key_kind_b_, 1, pair_order_[i].second));
            return key;
        });
        pair_int_fast_path_ = false;
    }

    /// Fold the packed (3+ key) fast path into the generic grouping path.
    ///
    /// Unlike every other fast path here, this one needs no decode at all:
    /// `process_rows_packed` already builds a full boxed `Key` per group into
    /// `group_order_` from the ROW (see its comment -- the packed word is
    /// used only for the FAST lookup, never as the group's stored identity),
    /// so `group_order_` is already exactly what the generic path expects.
    /// Only `key_index_`, which hashes packed words rather than `Key`s, needs
    /// rebuilding from what is already there.
    void migrate_packed_fast_path_to_generic() {
        key_index_.hashes.clear();
        key_index_.hashes.reserve(group_order_.size());
        for (const auto& key : group_order_) {
            key_index_.hashes.push_back(hash_key_value(key));
        }
        std::size_t capacity = 1024;
        while (capacity * 7 < key_index_.hashes.size() * 10) {
            capacity *= 2;
        }
        key_index_.rehash(capacity);
        packed_fast_path_ = false;
    }

    // ── Multi-key categorical index, keyed on the code tuple ──────────────────
    //
    // Groups are identified by the codes themselves (stored in
    // multi_cat_codes_flat_), verified on every hit. The Cartesian cell is only
    // a usable identity while the stride product fits in 64 bits; this does not
    // care, and it survives dictionary growth without a rebuild.
    static auto hash_codes(const Column<Categorical>::code_type* codes, std::size_t n)
        -> std::uint64_t {
        std::uint64_t seed = 0;
        for (std::size_t i = 0; i < n; ++i) {
            key_hash_mix(seed, std::hash<std::int64_t>{}(static_cast<std::int64_t>(codes[i])));
        }
        // Finalized for the same reason `hash_key_row` is: this index masks the
        // LOW bits to pick a slot and probes linearly, and the combine above
        // never diffuses into them. Categorical codes are small dense integers,
        // which is precisely the input that makes the unfinalized combine a
        // near-linear function of the key and turns the probe into one long
        // cluster. Nothing outside this index consumes the value, so unlike the
        // three in `interpreter_internal.hpp` it has no agreement to maintain.
        return key_hash_finalize(seed);
    }

    [[nodiscard]] auto codes_of_group(std::size_t group, std::size_t n_keys) const
        -> const Column<Categorical>::code_type* {
        return multi_cat_codes_flat_.data() + (group * n_keys);
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    void multi_cat_rehash_slots(std::size_t capacity, std::size_t n_keys) {
        multi_cat_slots_.assign(capacity, 0U);
        const std::size_t mask = capacity - 1;
        for (std::size_t group = 0; group < n_groups_; ++group) {
            std::size_t probe =
                static_cast<std::size_t>(hash_codes(codes_of_group(group, n_keys), n_keys)) & mask;
            while (multi_cat_slots_[probe] != 0) {
                probe = (probe + 1) & mask;
            }
            multi_cat_slots_[probe] = static_cast<std::uint32_t>(group) + 1;
        }
    }

    /// Rebuild the index from the groups already collected — used when the
    /// dense array gives up, and to seed the table on first use.
    void multi_cat_rehash_groups() {
        const std::size_t n_keys = n_groups_ == 0 ? 0 : multi_cat_codes_flat_.size() / n_groups_;
        std::size_t capacity = 1024;
        while ((n_groups_ * 10) > (capacity * 7)) {
            capacity *= 2;
        }
        multi_cat_rehash_slots(capacity, n_keys);
    }

    template <typename NewGroup>
    auto multi_cat_find_or_insert(const Column<Categorical>::code_type* codes, std::size_t n_keys,
                                  NewGroup&& new_group) -> std::uint32_t {
        const std::uint64_t hash = hash_codes(codes, n_keys);
        std::size_t mask = multi_cat_slots_.size() - 1;
        std::size_t probe = static_cast<std::size_t>(hash) & mask;
        while (true) {
            const std::uint32_t slot = multi_cat_slots_[probe];
            if (slot == 0) {
                const std::uint32_t gid = new_group();
                multi_cat_slots_[probe] = gid + 1;
                if ((n_groups_ * 10) > (multi_cat_slots_.size() * 7)) {
                    multi_cat_rehash_slots(multi_cat_slots_.size() * 2, n_keys);
                }
                return gid;
            }
            const std::uint32_t gid = slot - 1;
            if (std::equal(codes, codes + n_keys, codes_of_group(gid, n_keys))) {
                return gid;
            }
            probe = (probe + 1) & mask;
        }
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto process_rows_cat(const std::vector<const ColumnEntry*>& group_entries,
                          const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        std::vector<const Column<Categorical>*> cat_cols;
        cat_cols.reserve(group_entries.size());
        for (const auto* e : group_entries) {
            cat_cols.push_back(&std::get<Column<Categorical>>(*e->column));
        }
        const std::size_t n_keys = cat_cols.size();
        const bool single_key = n_keys == 1;

        if (single_key && rows > 0 &&
            try_process_rows_cat_parallel(*cat_cols[0], agg_entries, rows)) {
            return std::nullopt;
        }

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();
        if (single_key) {
            // A Categorical code is already a dense index into [0, dict_size),
            // so map code → gid with a direct array instead of hashing. Dicts
            // only grow and never reorder across chunks, so existing gids stay
            // valid and new dict entries just extend the array with sentinels.
            const auto* codes = cat_cols[0]->codes_data();
            const std::size_t dict_size = cat_cols[0]->dictionary().size();
            if (cat_dense_gid_.size() < dict_size) {
                cat_dense_gid_.resize(dict_size, kNoGid);
            }
            std::uint32_t* dense = cat_dense_gid_.data();
            for (std::size_t row = 0; row < rows; ++row) {
                const auto code = codes[row];
                std::uint32_t gid = dense[code];
                if (gid == kNoGid) {
                    gid = alloc_group();
                    dense[code] = gid;
                    cat_order_.push_back(code);
                }
                gids[row] = gid;
            }
        } else {
            // Multi-key: encode each row as a uint64_t Cartesian cell.
            // Strides may grow across chunks if a chunk introduces new dict
            // entries; we recompute per chunk and rebuild the index when that
            // happens (rare — Categorical dicts are usually stable).
            std::vector<std::uint64_t> dict_sizes(n_keys);
            for (std::size_t c = 0; c < n_keys; ++c) {
                dict_sizes[c] = static_cast<std::uint64_t>(cat_cols[c]->dictionary().size());
                if (dict_sizes[c] == 0)
                    dict_sizes[c] = 1;  // avoid stride collapse
            }
            // Strides: cell = c0*s0 + c1*s1 + … with s_{n-1} = 1.
            //
            // The cell only identifies a key tuple while the stride product
            // fits in 64 bits. Past that the multiply wraps, distinct tuples
            // collide, and `total_cells` itself wraps — a product of exactly
            // 2^64 (16 keys of 16 values, say) lands on 0, which would pass the
            // dense-array bound and index a zero-length array. Detect the
            // overflow and let the hash path, which identifies groups by their
            // codes rather than by a cell, take over.
            std::vector<std::uint64_t> strides(n_keys);
            std::uint64_t total_cells = 1;
            bool cells_overflow = false;
            {
                std::uint64_t s = 1;
                for (int ci = static_cast<int>(n_keys) - 1; ci >= 0; --ci) {
                    strides[static_cast<std::size_t>(ci)] = s;
                    const std::uint64_t size = dict_sizes[static_cast<std::size_t>(ci)];
                    if (s > std::numeric_limits<std::uint64_t>::max() / size) {
                        cells_overflow = true;
                        break;
                    }
                    s *= size;
                }
                total_cells = s;
            }
            const bool dense_possible = !cells_overflow && total_cells <= kDenseCellLimit;

            // Hoist raw code pointers out of the row loop.
            std::vector<const Column<Categorical>::code_type*> raws(n_keys);
            for (std::size_t c = 0; c < n_keys; ++c)
                raws[c] = cat_cols[c]->codes_data();

            const auto cell_of_group = [&](std::size_t g) -> std::uint64_t {
                std::uint64_t cell = 0;
                for (std::size_t c = 0; c < n_keys; ++c) {
                    cell += static_cast<std::uint64_t>(multi_cat_codes_flat_[(g * n_keys) + c]) *
                            strides[c];
                }
                return cell;
            };
            const auto new_group = [&](std::size_t row) -> std::uint32_t {
                for (std::size_t c = 0; c < n_keys; ++c)
                    multi_cat_codes_flat_.push_back(raws[c][row]);
                return alloc_group();
            };

            // When the Cartesian cell space is bounded, index a dense array
            // (one load per row, no hashing). If a later chunk grows the dicts
            // past the limit — or past what 64 bits can encode — migrate the
            // existing groups into the hash index once and stay there; dicts
            // only grow, so the cell space never shrinks back.
            if (multi_dense_ && !dense_possible) {
                multi_cat_rehash_groups();
                std::vector<std::uint32_t>().swap(multi_cat_cell_dense_);
                multi_dense_ = false;
            }

            if (multi_dense_) {
                // Rebuild the dense array when strides change (new dict entries).
                if (multi_cat_strides_ != strides) {
                    multi_cat_cell_dense_.assign(static_cast<std::size_t>(total_cells), kNoGid);
                    for (std::size_t g = 0; g < n_groups_; ++g)
                        multi_cat_cell_dense_[cell_of_group(g)] = static_cast<std::uint32_t>(g);
                    multi_cat_strides_ = strides;
                }
                std::uint32_t* dense = multi_cat_cell_dense_.data();
                if (n_keys == 2) {
                    const auto* k0 = raws[0];
                    const auto* k1 = raws[1];
                    const std::uint64_t s0 = strides[0];
                    const std::uint64_t s1 = strides[1];
                    for (std::size_t row = 0; row < rows; ++row) {
                        const std::uint64_t cell = (static_cast<std::uint64_t>(k0[row]) * s0) +
                                                   (static_cast<std::uint64_t>(k1[row]) * s1);
                        std::uint32_t gid = dense[cell];
                        if (gid == kNoGid) {
                            gid = new_group(row);
                            dense[cell] = gid;
                        }
                        gids[row] = gid;
                    }
                } else {
                    for (std::size_t row = 0; row < rows; ++row) {
                        std::uint64_t cell = 0;
                        for (std::size_t c = 0; c < n_keys; ++c)
                            cell += static_cast<std::uint64_t>(raws[c][row]) * strides[c];
                        std::uint32_t gid = dense[cell];
                        if (gid == kNoGid) {
                            gid = new_group(row);
                            dense[cell] = gid;
                        }
                        gids[row] = gid;
                    }
                }
            } else {
                // Hash fallback for cell spaces that are unbounded, or that no
                // longer fit in 64 bits. It identifies a group by its codes, not
                // by a cell: correct however the strides behave, and it needs no
                // rebuild when a new dictionary entry changes them.
                if (multi_cat_slots_.empty()) {
                    multi_cat_rehash_groups();
                }
                std::vector<Column<Categorical>::code_type> row_codes(n_keys);
                for (std::size_t row = 0; row < rows; ++row) {
                    for (std::size_t c = 0; c < n_keys; ++c) {
                        row_codes[c] = raws[c][row];
                    }
                    gids[row] = multi_cat_find_or_insert(row_codes.data(), n_keys,
                                                         [&] { return new_group(row); });
                }
            }
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    /// Row count below which partitioned discovery is not worth its fan-out,
    /// for the int/string keys it was originally tuned against.
    static constexpr std::size_t kDefaultPartitionMinRows = 1U << 18U;
    /// The packed key's break-even is lower: its serial probe hashes and
    /// compares a multi-column key, so there is more per-row work to move onto
    /// the workers than a single int key offers.
    static constexpr std::size_t kPackedPartitionMinRows = 1U << 15U;

    /// Everything one packed width needs on the group-by side.
    template <typename Packed, typename Hash>
    struct PackedGroups {
        robin_hood::unordered_flat_map<Packed, std::uint32_t, Hash> index;
        std::vector<KeyPartition<Packed, Hash>> partitions;
    };

    /// Three or more fixed-width key columns, grouped through one packed key.
    ///
    /// **The output key store is deliberately unchanged.** `group_order_` still
    /// holds one boxed `Key` per group, built once per GROUP, so
    /// `build_output_chunk` needs no packed case: this path sets none of the
    /// `*_fast_path_` flags and lands in the same branch the generic path uses.
    /// That is also why the key is rebuilt from the ROW rather than unpacked —
    /// a packed key is not invertible on its own, since a Categorical cell
    /// holds an operator-global interned id rather than the column's own code.
    template <typename Packed, typename Hash>
    auto process_rows_packed(const std::vector<const ColumnEntry*>& group_entries,
                             const std::vector<const ColumnEntry*>& agg_entries,
                             const std::vector<PackedKeyEncoder::PackCol>& cols, std::size_t rows,
                             PackedGroups<Packed, Hash>& state) -> std::optional<std::string> {
        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        // The one place a Key gets built: once per group, never per row.
        const auto build_key_at = [&](std::size_t row) {
            Key key;
            key.values.reserve(group_entries.size());
            for (const auto* entry : group_entries) {
                push_key_value(key, *entry, row);
            }
            return key;
        };
        const auto key_at = [&](std::size_t row) {
            return PackedKeyEncoder::pack_row<Packed>(cols, row);
        };

        if (try_discover_partitioned<Packed, Hash>(
                key_at, rows, gids, state.partitions,
                [&](std::size_t n) { group_order_.resize(n); },
                [&](const Packed&, std::uint32_t gid, std::size_t row) {
                    group_order_[gid] = build_key_at(row);
                },
                kPackedPartitionMinRows)) {
            accumulate_gids(gids, agg_entries, rows);
            return std::nullopt;
        }

        // Run-length shortcut, as in the string and int paths: sorted or chunked
        // input often repeats the key, so skip the map lookup when it matches
        // the previous row.
        Packed prev_key{};
        std::uint32_t prev_gid = std::numeric_limits<std::uint32_t>::max();
        bool have_prev = false;
        for (std::size_t row = 0; row < rows; ++row) {
            const Packed key = key_at(row);
            std::uint32_t gid{};
            if (have_prev && key == prev_key) {
                gid = prev_gid;
            } else {
                auto it = state.index.find(key);
                if (it == state.index.end()) {
                    group_order_.push_back(build_key_at(row));
                    gid = alloc_group();
                    state.index.emplace(key, gid);
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    auto process_rows_generic(const std::vector<const ColumnEntry*>& group_entries,
                              const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        std::vector<KeyCol> cols;
        cols.reserve(group_entries.size());
        for (const auto* entry : group_entries) {
            auto col = make_key_col(*entry);
            if (!col.has_value()) {
                return "group-by: unsupported key column type";
            }
            cols.push_back(*col);
        }

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();
        for (std::size_t row = 0; row < rows; ++row) {
            gids[row] = key_index_.find_or_insert(group_order_, cols, row, [&] {
                // The one place a Key gets built: once per group, not per row.
                Key key;
                key.values.reserve(group_entries.size());
                for (const auto* entry : group_entries) {
                    push_key_value(key, *entry, row);
                }
                group_order_.push_back(std::move(key));
                return alloc_group();
            });
        }

        accumulate_gids(gids, agg_entries, rows);
        return std::nullopt;
    }

    /// Parallel scatter-accumulate of an already-assigned gid array — the
    /// shared back half of every hash group-by fast path (string, int,
    /// int-pair, generic). Returns false when the shape is not worth it and
    /// the caller should accumulate serially into `flat_slots_`.
    ///
    /// **The gid pass stays serial on purpose.** It mutates the group index
    /// and it is what defines group ORDER — Ibex reports groups in observed
    /// first-occurrence order, so assigning gids concurrently would either
    /// change the answer or need a reconciliation pass costing more than the
    /// scan. Once every row carries a gid the rest is a pure scatter-reduce,
    /// which is the part worth threading: for an 8-aggregate query like q01 it
    /// is the dominant cost (24% of the whole query by profile), while the gid
    /// probe is a single packed-integer lookup per row.
    ///
    /// Reproducibility, stated exactly, because the two halves get confused:
    /// the partition is derived from row count and group count alone — both
    /// properties of the DATA — and morsels merge in ascending order, so the
    /// result does not depend on the machine, the thread count, or the
    /// schedule (verified byte-identical across 2/3/5/8/16 threads). It DOES
    /// differ from the serial path in the last ulp, because summing per morsel
    /// and merging is not the same order as summing down the rows — q01's
    /// sum_disc_price moves at the 11th significant digit. That is inherent to
    /// any partitioned float reduction, it matches what
    /// `try_process_rows_cat_parallel` has always done, and if anything the
    /// partitioned sum is the more accurate of the two.
    ///
    /// This mirrors `try_process_rows_cat_parallel`, which can skip the gid
    /// pass entirely because a Categorical code is already a dense index.
    auto try_accumulate_parallel(const std::uint32_t* gids,
                                 const std::vector<const ColumnEntry*>& agg_entries,
                                 std::size_t rows) -> bool {
        if (on_worker_pool_thread() || !exec_->parallel || rows < exec_->parallel_min_rows ||
            n_groups_ == 0 || n_aggs_ == 0) {
            return false;
        }
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (!agg_is_combinable(plan_[a].func)) {
                return false;
            }
            // A boxed First/Last value lives outside the slot array, so a
            // private copy would not capture it.
            if (plan_[a].kind != ExprType::Int && plan_[a].kind != ExprType::Double) {
                return false;
            }
        }

        // Same budget and morsel shape as the Categorical path, for the same
        // reasons: partial state is bounded by GROUP COUNT, and the merge costs
        // one agg_combine per (morsel, group) while the scan it replaces costs
        // one update per row. Fanning out only pays when the merge stays small
        // against the scan — a high-cardinality group-by would merge more slots
        // than it saved row updates.
        constexpr std::size_t kMinRowsPerMorsel = 65536;
        constexpr std::size_t kMaxMorsels = 64;
        constexpr std::size_t kPartialBudgetBytes = 32UL << 20;
        constexpr std::size_t kMergeToScanRatio = 4;
        const std::size_t per_morsel_bytes =
            n_groups_ * ((n_aggs_ * sizeof(AggSlotCore)) + (scratch_stride_ * sizeof(double)));
        if (per_morsel_bytes == 0 || per_morsel_bytes > kPartialBudgetBytes) {
            return false;
        }
        std::size_t morsels = std::clamp<std::size_t>(rows / kMinRowsPerMorsel, 1, kMaxMorsels);
        morsels = std::min(morsels, kPartialBudgetBytes / per_morsel_bytes);
        if (morsels < 2 || morsels * n_groups_ > rows / kMergeToScanRatio) {
            return false;
        }

        const std::size_t stride = n_groups_ * n_aggs_;
        const std::size_t scratch_span = n_groups_ * scratch_stride_;
        const std::size_t grain = (rows + morsels - 1) / morsels;
        std::vector<AggSlotCore> partials(morsels * stride);
        std::vector<double> partial_scratch(morsels * scratch_span, 0.0);

        auto& pool = process_worker_pool();
        const std::size_t threads =
            std::min(morsels, exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size());
        std::atomic<std::size_t> cursor{0};
        auto batch = pool.submit(threads, [&](std::size_t) {
            while (true) {
                const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                if (m >= morsels) {
                    return;
                }
                const std::size_t begin = m * grain;
                const std::size_t end = std::min(rows, begin + grain);
                if (begin >= end) {
                    continue;
                }
                accumulate_columns_into(gids, agg_entries, begin, end, &partials[m * stride],
                                        partial_scratch.data() + (m * scratch_span));
            }
        });
        batch.wait();

        for (std::size_t m = 0; m < morsels; ++m) {
            const AggSlotCore* src = &partials[m * stride];
            const double* src_scratch = partial_scratch.data() + (m * scratch_span);
            for (std::size_t g = 0; g < n_groups_; ++g) {
                AggSlotCore* dst = &flat_slots_[g * n_aggs_];
                for (std::size_t a = 0; a < n_aggs_; ++a) {
                    const std::size_t off = (g * scratch_stride_) + scratch_offset_[a];
                    agg_combine(dst[a], src[(g * n_aggs_) + a], plan_[a].func, plan_[a].kind,
                                scratch_stride_ == 0 ? nullptr : scratch_.data() + off,
                                scratch_stride_ == 0 ? nullptr : src_scratch + off);
                }
            }
        }
        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    /// Accumulate `gids` either across workers or, when that is not worth it,
    /// serially — the one call every gid-assigning fast path ends with.
    void accumulate_gids(const std::uint32_t* gids,
                         const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows) {
        if (!try_accumulate_parallel(gids, agg_entries, rows)) {
            accumulate_columns_into(gids, agg_entries, 0, rows, flat_slots_.data(),
                                    scratch_.data());
        }
    }

    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access)
    // We need AggSlotCore to be a POD

    /// Scatter-accumulate rows [begin, end) into `base`, indexed by
    /// `gids[row] * n_aggs_ + agg_i`. `base` is the caller's slot array —
    /// `flat_slots_` for the serial path, a worker-private array for the
    /// parallel one — and `GidT` covers both assigned gids (uint32_t) and raw
    /// Categorical codes (int32_t), which are already dense indices.
    ///
    template <typename GidT>
    void accumulate_columns_into(const GidT* gids,
                                 const std::vector<const ColumnEntry*>& agg_entries,
                                 std::size_t begin, std::size_t end, AggSlotCore* base,
                                 double* scratch_base) {
        AggSlotCore* fs = base;
        const std::size_t rows = end;
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            // Takes GidT so a signed Categorical code indexes without an
            // implicit narrowing conversion at each of the ~19 call sites.
            const auto slot_for = [&](GidT g) -> AggSlotCore& {
                return fs[(static_cast<std::size_t>(g) * n_aggs_) + agg_i];
            };
            // Moment accumulators, laid out beside `base` and indexed the same
            // way. A worker-private slot array needs a worker-private scratch
            // to match, or two morsels would accumulate one group's variance
            // into the same doubles.
            const auto scratch_at = [&](GidT g) -> double* {
                return scratch_base + (static_cast<std::size_t>(g) * scratch_stride_) +
                       scratch_offset_[agg_i];
            };

            if (plan_[agg_i].func == ir::AggFunc::Count) {
                for (std::size_t row = begin; row < rows; ++row) {
                    slot_for(gids[row]).count++;
                }
                continue;
            }

            const auto& entry = *agg_entries[agg_i];
            const ValidityBitmap* validity =
                entry.validity.has_value() ? &*entry.validity : nullptr;
            const bool has_nulls = validity != nullptr;

            if (plan_[agg_i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                switch (plan_[agg_i].func) {
                    case ir::AggFunc::Sum:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += data[row];
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += data[row];
                            slot.count++;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.present() ? std::min(slot.double_value, v) : v;
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.present() ? std::max(slot.double_value, v) : v;
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), scratch_at(gids[row])[0],
                                              data[row]);
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            double* scr = scratch_at(gids[row]);
                            agg_update_moments(slot_for(gids[row]), scr[0], scr[1], scr[2],
                                               data[row]);
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            if (!slot.present()) {
                                slot.double_value = data[row];
                                slot.mark_present();
                            }
                        }
                        break;
                    case ir::AggFunc::Last:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value = data[row];
                            slot.mark_present();
                        }
                        break;
                    default:
                        break;
                }
            } else if (plan_[agg_i].kind == ExprType::Int) {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                switch (plan_[agg_i].func) {
                    case ir::AggFunc::Sum:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.int_value += data[row];
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += static_cast<double>(data[row]);
                            slot.count++;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.present() ? std::min(slot.int_value, v) : v;
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.present() ? std::max(slot.int_value, v) : v;
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), scratch_at(gids[row])[0],
                                              static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            double* scr = scratch_at(gids[row]);
                            agg_update_moments(slot_for(gids[row]), scr[0], scr[1], scr[2],
                                               static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            if (!slot.present()) {
                                slot.int_value = data[row];
                                slot.mark_present();
                            }
                        }
                        break;
                    case ir::AggFunc::Last:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.int_value = data[row];
                            slot.mark_present();
                        }
                        break;
                    default:
                        break;
                }
            } else {
                // ExprType::String — First/Last only (the type gate in
                // process_chunk rejects every other function here). Covers
                // both Column<std::string> and Column<Categorical>; the two
                // share ScalarValue{std::string} as the wire format via
                // append_scalar, which pushes into a Categorical dictionary
                // when the target column is Categorical.
                const bool categorical = plan_[agg_i].categorical;
                const auto value_at = [&](std::size_t row) -> std::string {
                    if (categorical) {
                        return std::string(std::get<Column<Categorical>>(*entry.column)[row]);
                    }
                    return std::string(std::get<Column<std::string>>(*entry.column)[row]);
                };
                if (plan_[agg_i].func == ir::AggFunc::First) {
                    for (std::size_t row = begin; row < rows; ++row) {
                        if (has_nulls && !(*validity)[row])
                            continue;
                        auto& slot = slot_for(gids[row]);
                        if (!slot.present()) {
                            text_at((static_cast<std::size_t>(gids[row]) * n_aggs_) + agg_i) =
                                value_at(row);
                            slot.mark_present();
                        }
                    }
                } else {
                    for (std::size_t row = begin; row < rows; ++row) {
                        if (has_nulls && !(*validity)[row])
                            continue;
                        auto& slot = slot_for(gids[row]);
                        text_at((static_cast<std::size_t>(gids[row]) * n_aggs_) + agg_i) =
                            value_at(row);
                        slot.mark_present();
                    }
                }
            }
        }
    }

    /// Accumulate rows [begin, end) of a global aggregate into `slots`
    /// (n_aggs_ entries, caller-owned). No gid indirection: the compiler sees
    /// a plain reduction over a contiguous range, which is also what lets a
    /// worker own a private copy.
    /// Parallel single-key Categorical group-by. Returns false when the shape
    /// is not eligible and the caller should run the serial path.
    ///
    /// A Categorical code is already a dense index into the dictionary, so a
    /// worker needs no hash table at all: it accumulates into a private slot
    /// array indexed by code. That bounds the partial state by DICTIONARY
    /// SIZE, which is what makes per-worker group state affordable here and
    /// keeps `by symbol` (a few hundred groups) cheap.
    ///
    /// Group order is Ibex's observed first-occurrence order, and the merge
    /// preserves it exactly: morsels are contiguous ascending row ranges, and
    /// merging them in ascending order while walking each morsel's own
    /// first-seen code list visits codes in precisely the order a serial scan
    /// would have met them.
    auto try_process_rows_cat_parallel(const Column<Categorical>& cat,
                                       const std::vector<const ColumnEntry*>& agg_entries,
                                       std::size_t rows) -> bool {
        if (on_worker_pool_thread() || !exec_->parallel || rows < exec_->parallel_min_rows) {
            return false;
        }
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (!agg_is_combinable(plan_[a].func)) {
                return false;
            }
            // As above: a boxed First/Last value lives outside the slot.
            if (plan_[a].kind != ExprType::Int && plan_[a].kind != ExprType::Double) {
                return false;
            }
        }
        const std::size_t dict_size = cat.dictionary().size();
        if (dict_size == 0) {
            return false;
        }

        // Same row-derived partition as the global aggregate, then bounded by
        // what the per-worker slot arrays cost. Both inputs (row count and
        // dictionary size) are properties of the DATA, so the partition — and
        // therefore the float reduction order — is still independent of the
        // machine and the schedule.
        constexpr std::size_t kMinRowsPerMorsel = 65536;
        constexpr std::size_t kMaxMorsels = 64;
        constexpr std::size_t kPartialBudgetBytes = 32UL << 20;
        const std::size_t per_morsel_bytes =
            dict_size * ((n_aggs_ * sizeof(AggSlotCore)) + (scratch_stride_ * sizeof(double)));
        if (per_morsel_bytes == 0 || per_morsel_bytes > kPartialBudgetBytes) {
            return false;  // one worker's state alone blows the budget
        }
        std::size_t morsels = std::clamp<std::size_t>(rows / kMinRowsPerMorsel, 1, kMaxMorsels);
        morsels = std::min(morsels, kPartialBudgetBytes / per_morsel_bytes);
        if (morsels < 2) {
            return false;
        }
        // The merge costs one agg_combine per (morsel, dictionary entry), so it
        // scales with GROUP COUNT while the scan it replaces scales with rows.
        // Fanning out only pays when the merge stays small against the scan:
        // `by symbol` (252 groups) merges ~4k slots against 1M rows, but
        // `by user_id` (100k groups) would merge ~1M — more work than it saves,
        // and measured as a 17% REGRESSION when a smaller slot let it through
        // the memory gate.
        constexpr std::size_t kMergeToScanRatio = 4;
        if (morsels * dict_size > rows / kMergeToScanRatio) {
            return false;
        }

        const auto* codes = cat.codes_data();
        const std::size_t grain = (rows + morsels - 1) / morsels;
        std::vector<AggSlotCore> partials(morsels * dict_size * n_aggs_);
        std::vector<double> cat_partial_scratch(morsels * dict_size * scratch_stride_, 0.0);
        // Per morsel, the codes it saw in first-occurrence order.
        std::vector<std::vector<Column<Categorical>::code_type>> seen(morsels);

        auto& pool = process_worker_pool();
        const std::size_t threads =
            std::min(morsels, exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size());
        std::atomic<std::size_t> cursor{0};
        auto batch = pool.submit(threads, [&](std::size_t) {
            std::vector<std::uint8_t> local_seen(dict_size, 0);
            while (true) {
                const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                if (m >= morsels) {
                    return;
                }
                const std::size_t begin = m * grain;
                const std::size_t end = std::min(rows, begin + grain);
                if (begin >= end) {
                    continue;
                }
                std::ranges::fill(local_seen, std::uint8_t{0});
                auto& order = seen[m];
                for (std::size_t row = begin; row < end; ++row) {
                    const auto code = codes[row];
                    if (local_seen[static_cast<std::size_t>(code)] == 0) {
                        local_seen[static_cast<std::size_t>(code)] = 1;
                        order.push_back(code);
                    }
                }
                accumulate_columns_into(
                    codes, agg_entries, begin, end, &partials[m * dict_size * n_aggs_],
                    cat_partial_scratch.data() + (m * dict_size * scratch_stride_));
            }
        });
        batch.wait();

        if (cat_dense_gid_.size() < dict_size) {
            cat_dense_gid_.resize(dict_size, kNoGid);
        }
        for (std::size_t m = 0; m < morsels; ++m) {
            const AggSlotCore* src = &partials[m * dict_size * n_aggs_];
            const double* src_scratch =
                cat_partial_scratch.data() + (m * dict_size * scratch_stride_);
            for (const auto code : seen[m]) {
                const auto idx = static_cast<std::size_t>(code);
                std::uint32_t gid = cat_dense_gid_[idx];
                if (gid == kNoGid) {
                    gid = alloc_group();
                    cat_dense_gid_[idx] = gid;
                    cat_order_.push_back(code);
                }
                AggSlotCore* dst = &flat_slots_[(static_cast<std::size_t>(gid) * n_aggs_)];
                for (std::size_t a = 0; a < n_aggs_; ++a) {
                    agg_combine(dst[a], src[(idx * n_aggs_) + a], plan_[a].func, plan_[a].kind,
                                scratch_stride_ == 0 ? nullptr : scratch_for(gid, a),
                                scratch_stride_ == 0
                                    ? nullptr
                                    : src_scratch + (idx * scratch_stride_) + scratch_offset_[a]);
                }
            }
        }
        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    /// `scratch_base` is this caller's moment region for the single group —
    /// `scratch_` serially, a worker-private slice per morsel in parallel.
    void accumulate_ungrouped_range_impl(const std::vector<const ColumnEntry*>& agg_entries,
                                         std::size_t begin, std::size_t end, AggSlotCore* slots,
                                         double* scratch_base) {
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            AggSlotCore& slot = slots[agg_i];
            const auto func = plan_[agg_i].func;
            if (func == ir::AggFunc::Count) {
                slot.count += static_cast<std::int64_t>(end - begin);
                continue;
            }
            const auto& entry = *agg_entries[agg_i];
            const ValidityBitmap* validity =
                entry.validity.has_value() ? &*entry.validity : nullptr;
            const bool has_nulls = validity != nullptr;

            // One generic driver per storage kind; `step` is the per-row body.
            const auto each = [&](auto&& step) {
                if (has_nulls) {
                    for (std::size_t row = begin; row < end; ++row) {
                        if (!(*validity)[row]) {
                            continue;
                        }
                        step(row);
                    }
                } else {
                    for (std::size_t row = begin; row < end; ++row) {
                        step(row);
                    }
                }
            };

            if (plan_[agg_i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                switch (func) {
                    case ir::AggFunc::Sum:
                        // has_value must track "saw a non-null value", not
                        // "the range was non-empty": sum over an all-null
                        // column is NULL, not 0.
                        each([&](std::size_t r) {
                            slot.double_value += data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Mean:
                        each([&](std::size_t r) {
                            slot.double_value += data[r];
                            slot.count++;
                        });
                        break;
                    case ir::AggFunc::Min:
                        each([&](std::size_t r) {
                            slot.double_value =
                                slot.present() ? std::min(slot.double_value, data[r]) : data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Max:
                        each([&](std::size_t r) {
                            slot.double_value =
                                slot.present() ? std::max(slot.double_value, data[r]) : data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Stddev:
                        each([&](std::size_t r) {
                            agg_update_stddev(slot, scratch_base[scratch_offset_[agg_i]], data[r]);
                        });
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        each([&](std::size_t r) {
                            double* scr = scratch_base + scratch_offset_[agg_i];
                            agg_update_moments(slot, scr[0], scr[1], scr[2], data[r]);
                        });
                        break;
                    case ir::AggFunc::First:
                        each([&](std::size_t r) {
                            if (!slot.present()) {
                                slot.double_value = data[r];
                                slot.mark_present();
                            }
                        });
                        break;
                    case ir::AggFunc::Last:
                        each([&](std::size_t r) {
                            slot.double_value = data[r];
                            slot.mark_present();
                        });
                        break;
                    default:
                        break;
                }
            } else if (plan_[agg_i].kind == ExprType::Int) {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                switch (func) {
                    case ir::AggFunc::Sum:
                        each([&](std::size_t r) {
                            slot.int_value += data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Mean:
                        each([&](std::size_t r) {
                            slot.double_value += static_cast<double>(data[r]);
                            slot.count++;
                        });
                        break;
                    case ir::AggFunc::Min:
                        each([&](std::size_t r) {
                            slot.int_value =
                                slot.present() ? std::min(slot.int_value, data[r]) : data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Max:
                        each([&](std::size_t r) {
                            slot.int_value =
                                slot.present() ? std::max(slot.int_value, data[r]) : data[r];
                            slot.mark_present();
                        });
                        break;
                    case ir::AggFunc::Stddev:
                        each([&](std::size_t r) {
                            agg_update_stddev(slot, scratch_base[scratch_offset_[agg_i]],
                                              static_cast<double>(data[r]));
                        });
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        each([&](std::size_t r) {
                            double* scr = scratch_base + scratch_offset_[agg_i];
                            agg_update_moments(slot, scr[0], scr[1], scr[2],
                                               static_cast<double>(data[r]));
                        });
                        break;
                    case ir::AggFunc::First:
                        each([&](std::size_t r) {
                            if (!slot.present()) {
                                slot.int_value = data[r];
                                slot.mark_present();
                            }
                        });
                        break;
                    case ir::AggFunc::Last:
                        each([&](std::size_t r) {
                            slot.int_value = data[r];
                            slot.mark_present();
                        });
                        break;
                    default:
                        break;
                }
            } else {
                // ExprType::String — First/Last only, same wire format as the
                // grouped path (ScalarValue{std::string}).
                const bool categorical = plan_[agg_i].categorical;
                const auto value_at = [&](std::size_t row) -> std::string {
                    if (categorical) {
                        return std::string(std::get<Column<Categorical>>(*entry.column)[row]);
                    }
                    return std::string(std::get<Column<std::string>>(*entry.column)[row]);
                };
                if (func == ir::AggFunc::First) {
                    each([&](std::size_t r) {
                        if (!slot.present()) {
                            text_at(agg_i) = value_at(r);
                            slot.mark_present();
                        }
                    });
                } else {
                    each([&](std::size_t r) {
                        text_at(agg_i) = value_at(r);
                        slot.mark_present();
                    });
                }
            }
        }
    }

    /// Global aggregate over `rows`, optionally fanned out across workers.
    auto process_rows_ungrouped(const std::vector<const ColumnEntry*>& agg_entries,
                                std::size_t rows) -> std::optional<std::string> {
        // An empty input must produce NO group, hence no output row — the
        // generic path got that for free by only creating a group when a row
        // arrived. Creating it up front turned `count()` over an empty table
        // into a 1-row answer.
        if (rows == 0) {
            return std::nullopt;
        }
        if (n_groups_ == 0) {
            // build_output_chunk() reads group_order_[g] for the generic key
            // layout, so the single group still needs its (empty) Key — the
            // generic path used to push one from its make_group lambda.
            group_order_.emplace_back();
            alloc_group();
        }
        AggSlotCore* dst = flat_slots_.data();

        const std::size_t morsels = ungrouped_morsels(rows);
        if (morsels < 2) {
            accumulate_ungrouped_range_impl(agg_entries, 0, rows, dst, scratch_.data());
            return std::nullopt;
        }

        // One private slot array per morsel, each written by exactly one
        // worker. Merging them in ascending morsel order — never completion
        // order — is what keeps First/Last correct and the float reduction
        // reproducible run to run.
        const std::size_t grain = (rows + morsels - 1) / morsels;
        std::vector<AggSlotCore> partials(morsels * n_aggs_);
        std::vector<double> ung_scratch(morsels * scratch_stride_, 0.0);

        auto& pool = process_worker_pool();
        const std::size_t threads =
            std::min(morsels, exec_->parallel_threads != 0 ? exec_->parallel_threads : pool.size());
        std::atomic<std::size_t> cursor{0};
        auto batch = pool.submit(threads, [&](std::size_t) {
            while (true) {
                const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                if (m >= morsels) {
                    return;
                }
                const std::size_t begin = m * grain;
                const std::size_t end = std::min(rows, begin + grain);
                if (begin < end) {
                    accumulate_ungrouped_range_impl(agg_entries, begin, end,
                                                    &partials[(m * n_aggs_)],
                                                    ung_scratch.data() + (m * scratch_stride_));
                }
            }
        });
        batch.wait();

        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        for (std::size_t m = 0; m < morsels; ++m) {
            for (std::size_t a = 0; a < n_aggs_; ++a) {
                agg_combine(dst[a], partials[(m * n_aggs_) + a], plan_[a].func, plan_[a].kind,
                            scratch_stride_ == 0 ? nullptr : scratch_for(0, a),
                            scratch_stride_ == 0
                                ? nullptr
                                : ung_scratch.data() + (m * scratch_stride_) + scratch_offset_[a]);
            }
        }
        return std::nullopt;
    }

    /// How many row-morsels to split a global aggregate into; 1 = stay serial.
    [[nodiscard]] auto ungrouped_morsels(std::size_t rows) const -> std::size_t {
        // Submitting from a pool thread would deadlock (WorkerPool::submit
        // aborts rather than allow it), and a morsel is already one worker's
        // share.
        if (on_worker_pool_thread() || !exec_->parallel || rows < exec_->parallel_min_rows) {
            return 1;
        }
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (!agg_is_combinable(plan_[a].func)) {
                return 1;  // Skew/Kurtosis: no partial merge, stay serial.
            }
            // A non-numeric First/Last keeps its value in `text_store_`, which
            // agg_combine cannot reach and workers must not write concurrently.
            if (plan_[a].kind != ExprType::Int && plan_[a].kind != ExprType::Double) {
                return 1;
            }
        }
        // The partition is a function of the ROW COUNT ALONE — deliberately
        // not of the thread count. A float reduction's result depends on where
        // the range is cut, so deriving morsels from the pool size would make
        // `sum`/`std` answers differ between a 4-core box and a 24-core one,
        // and differ again under `--threads`. Keyed on rows, the answer depends
        // only on the data: same input, same result, any machine, any schedule.
        //
        // Morsels are large because a reduction's per-row cost is constant —
        // equal ranges finish together, so unlike a filter there is no
        // imbalance to hedge against and every extra morsel is pure dispatch
        // and merge overhead. The cap bounds the partial array.
        constexpr std::size_t kMinRowsPerMorsel = 65536;
        constexpr std::size_t kMaxMorsels = 64;
        return std::clamp<std::size_t>(rows / kMinRowsPerMorsel, 1, kMaxMorsels);
    }

    auto build_output_chunk() -> std::expected<std::optional<Chunk>, std::string> {
        // Owned-partition mode: the deferred first-occurrence merge runs
        // exactly once, here, right before the ordinary emission logic below
        // -- which then runs completely unmodified, reading the same
        // pair_order_/flat_slots_/n_groups_ it always has.
        if (owned_mode_) {
            finalize_owned_pair();
        }

        Chunk out;
        out.columns.reserve(group_by_->size() + aggregations_->size());

        if (!initialized_) {
            // No input rows at all — emit a chunk with empty columns of
            // the expected schema where possible. Without any chunk we
            // have no types to build group columns; return an empty
            // optional so the sink finalizes an empty table.
            return std::optional<Chunk>{};
        }

        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            out.add_column((*group_by_)[i].name, make_empty_like(group_templates_[i]));
        }
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const auto& agg = (*aggregations_)[i];
            ColumnValue column;
            switch (agg.func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    column = Column<double>{};
                    break;
                case ir::AggFunc::Sum:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                    if (plan_[i].kind == ExprType::Double) {
                        column = Column<double>{};
                    } else {
                        column = Column<std::int64_t>{};
                    }
                    break;
                case ir::AggFunc::First:
                case ir::AggFunc::Last:
                    if (plan_[i].kind == ExprType::Double) {
                        column = Column<double>{};
                    } else if (plan_[i].kind == ExprType::Int) {
                        column = Column<std::int64_t>{};
                    } else if (plan_[i].categorical) {
                        column = Column<Categorical>{};
                    } else {
                        column = Column<std::string>{};
                    }
                    break;
                default:
                    return std::unexpected(
                        "ChunkedAggregateOperator: unsupported agg in build_output");
            }
            out.add_column(agg.alias, std::move(column));
        }

        for (std::size_t i = 0; i < out.columns.size(); ++i) {
            std::visit([&](auto& c) { c.reserve(n_groups_); }, out.mutable_column(i));
        }

        std::vector<ValidityBitmap> agg_validity(aggregations_->size());
        std::vector<std::uint8_t> track_validity(aggregations_->size(), 0U);
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            if (chunked_agg_tracks_validity(plan_[i].func)) {
                track_validity[i] = 1U;
                agg_validity[i].reserve(n_groups_);
            }
        }

        // The null group's key cell carries the type's zero value plus a clear
        // validity bit. Only the generic Key path can produce one — the cat/str
        // fast paths above are only taken for key columns with no nulls.
        std::vector<ValidityBitmap> key_validity(group_by_->size());
        std::uint64_t any_null_keys = 0;
        if (!cat_fast_path_ && !str_fast_path_ && !int_fast_path_ && !pair_int_fast_path_) {
            for (const auto& key : group_order_) {
                any_null_keys |= key.null_mask;
            }
            if (any_null_keys != 0) {
                for (auto& bitmap : key_validity) {
                    bitmap.assign(n_groups_, true);
                }
            }
        }

        const auto push_int_key = [](ColumnValue& col, IntKeyKind kind, std::int64_t raw) {
            switch (kind) {
                case IntKeyKind::Int64:
                    std::get<Column<std::int64_t>>(col).push_back(raw);
                    return;
                case IntKeyKind::Date:
                    std::get<Column<Date>>(col).push_back(Date{static_cast<std::int32_t>(raw)});
                    return;
                case IntKeyKind::Ts:
                    std::get<Column<Timestamp>>(col).push_back(Timestamp{raw});
                    return;
                case IntKeyKind::Cat:
                    // The output column is `make_empty_like` of the input, so it
                    // shares the input's dictionary and the stored code resolves
                    // against it.
                    std::get<Column<Categorical>>(col).push_code(
                        static_cast<Column<Categorical>::code_type>(raw));
                    return;
            }
        };

        const AggSlotCore* fs = flat_slots_.data();

        // Emission is column-major, one output column per task: every column is
        // a separate buffer written by exactly one worker, so no two tasks touch
        // the same bytes and the emitted order is the group order regardless of
        // which worker got which column.
        const auto emit_key_column = [&](std::size_t ci) {
            ColumnValue& col = out.mutable_column(ci);
            if (cat_fast_path_) {
                auto& cat_col = std::get<Column<Categorical>>(col);
                const std::size_t n_keys = group_by_->size();
                if (n_keys == 1) {
                    for (std::size_t g = 0; g < n_groups_; ++g) {
                        cat_col.push_code(cat_order_[g]);
                    }
                } else {
                    for (std::size_t g = 0; g < n_groups_; ++g) {
                        cat_col.push_code(multi_cat_codes_flat_[(g * n_keys) + ci]);
                    }
                }
            } else if (str_fast_path_) {
                auto& str_col = std::get<Column<std::string>>(col);
                for (std::size_t g = 0; g < n_groups_; ++g) {
                    str_col.push_back(str_order_[g]);
                }
            } else if (int_fast_path_) {
                for (std::size_t g = 0; g < n_groups_; ++g) {
                    push_int_key(col, int_key_kind_, int_order_[g]);
                }
            } else if (pair_int_fast_path_) {
                const IntKeyKind kind = ci == 0 ? int_key_kind_ : int_key_kind_b_;
                for (std::size_t g = 0; g < n_groups_; ++g) {
                    push_int_key(col, kind, ci == 0 ? pair_order_[g].first : pair_order_[g].second);
                }
            } else {
                for (std::size_t g = 0; g < n_groups_; ++g) {
                    const Key& key = group_order_[g];
                    if (ci >= key.values.size()) {
                        continue;
                    }
                    append_scalar(col, key.values[ci]);
                    if (any_null_keys != 0 && ci < kMaxKeyColumns &&
                        (key.null_mask & (std::uint64_t{1} << ci)) != 0) {
                        key_validity[ci].set(g, false);
                    }
                }
            }
        };

        const auto emit_agg_column = [&](std::size_t i) {
            ColumnValue& column = out.mutable_column(group_by_->size() + i);
            const bool tracks_validity = track_validity[i] != 0U;
            for (std::size_t g = 0; g < n_groups_; ++g) {
                const AggSlotCore& slot = fs[(g * n_aggs_) + i];
                if (tracks_validity) {
                    agg_validity[i].push_back(chunked_agg_valid(plan_[i].func, slot));
                }
                switch (plan_[i].func) {
                    case ir::AggFunc::Count:
                        append_scalar(column, slot.count);
                        break;
                    case ir::AggFunc::Mean:
                        append_scalar(column,
                                      slot.count == 0
                                          ? 0.0
                                          : slot.double_value / static_cast<double>(slot.count));
                        break;
                    case ir::AggFunc::Sum:
                    case ir::AggFunc::Min:
                    case ir::AggFunc::Max:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else {
                            append_scalar(column, slot.int_value);
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        append_scalar(column, agg_finalize_stddev(slot, scratch_for(g, i)[0]));
                        break;
                    case ir::AggFunc::Skew:
                        append_scalar(column, agg_finalize_skew(slot, scratch_for(g, i)[0],
                                                                scratch_for(g, i)[1]));
                        break;
                    case ir::AggFunc::Kurtosis:
                        append_scalar(column, agg_finalize_kurtosis(slot, scratch_for(g, i)[0],
                                                                    scratch_for(g, i)[2]));
                        break;
                    case ir::AggFunc::First:
                    case ir::AggFunc::Last:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else if (plan_[i].kind == ExprType::Int) {
                            append_scalar(column, slot.int_value);
                        } else {
                            append_scalar(column, text_store_[(g * n_aggs_) + i]);
                        }
                        break;
                    default:
                        break;
                }
            }
        };

        const std::size_t n_out_columns = out.columns.size();
        const auto emit_column = [&](std::size_t c) {
            if (c < group_by_->size()) {
                emit_key_column(c);
            } else {
                emit_agg_column(c - group_by_->size());
            }
        };

        // A one-task budget would pay the pool round trip for work the calling
        // thread is about to do anyway, so it stays serial.
        auto& pool = process_worker_pool();
        const std::size_t threads =
            std::min(n_out_columns, (exec_ != nullptr && exec_->parallel_threads != 0)
                                        ? exec_->parallel_threads
                                        : pool.size());
        if (exec_ != nullptr && exec_->parallel && !on_worker_pool_thread() && threads > 1 &&
            n_groups_ >= exec_->parallel_min_rows) {
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(threads, [&](std::size_t) {
                while (true) {
                    const std::size_t c = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (c >= n_out_columns) {
                        return;
                    }
                    emit_column(c);
                }
            });
            batch.wait();
            if (exec_->parallel_stats != nullptr) {
                exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            for (std::size_t c = 0; c < n_out_columns; ++c) {
                emit_column(c);
            }
        }

        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            if (track_validity[i] == 0U || agg_validity[i].empty()) {
                continue;
            }
            bool has_null = false;
            for (std::size_t r = 0; r < agg_validity[i].size(); ++r) {
                if (!agg_validity[i][r]) {
                    has_null = true;
                    break;
                }
            }
            if (has_null) {
                out.columns[group_by_->size() + i].validity = std::move(agg_validity[i]);
            }
        }

        for (std::size_t ci = 0; ci < group_by_->size() && ci < kMaxKeyColumns; ++ci) {
            if ((any_null_keys & (std::uint64_t{1} << ci)) != 0) {
                out.columns[ci].validity = std::move(key_validity[ci]);
            }
        }

        return std::optional<Chunk>{std::move(out)};
    }

    // NOLINTEND(cppcoreguidelines-pro-type-union-access)

    struct SlotPlan {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
        // Only meaningful when kind == String: disambiguates Column<Categorical>
        // from Column<std::string> for First/Last output construction, since
        // expr_type_for_column collapses both to ExprType::String.
        bool categorical = false;
        /// Extra per-GROUP state this aggregate needs, in doubles. Zero for
        /// almost everything, which is the point: state that only one
        /// aggregate wants must not sit in AggSlotCore, where it would cost
        /// every group of every query. Skew/Kurtosis declare 2 (the third and
        /// fourth central moments); a future aggregate declares whatever it
        /// needs without touching the slot.
        std::uint32_t scratch_doubles = 0;
    };

    struct CatKey {
        std::vector<Column<Categorical>::code_type> codes;
        auto operator==(const CatKey& o) const noexcept -> bool { return codes == o.codes; }
    };

    // Transparent hash/eq: lets `str_index_.find(string_view)` skip the
    // allocation of a temporary std::string on every probe.
    struct StrViewHash {
        using is_transparent = void;
        auto operator()(std::string_view s) const noexcept -> std::size_t {
            return robin_hood::hash_bytes(s.data(), s.size());
        }
        auto operator()(const std::string& s) const noexcept -> std::size_t {
            return robin_hood::hash_bytes(s.data(), s.size());
        }
    };
    struct StrViewEq {
        using is_transparent = void;
        auto operator()(const std::string& a, const std::string& b) const noexcept -> bool {
            return a == b;
        }
        auto operator()(const std::string& a, std::string_view b) const noexcept -> bool {
            return std::string_view(a) == b;
        }
        auto operator()(std::string_view a, const std::string& b) const noexcept -> bool {
            return a == std::string_view(b);
        }
    };
    struct CatKeyHash {
        auto operator()(const CatKey& k) const noexcept -> std::size_t {
            std::size_t h = 0;
            for (auto c : k.codes) {
                h ^= robin_hood::hash<Column<Categorical>::code_type>{}(c) + 0x9e3779b9 +
                     (h << 6U) + (h >> 2U);
            }
            return h;
        }
    };

    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* group_by_;
    const std::vector<ir::AggSpec>* aggregations_;
    const ExecutionContext* exec_;
    bool emitted_ = false;

    bool initialized_ = false;
    bool cat_fast_path_ = false;
    bool str_fast_path_ = false;
    std::size_t n_aggs_ = 0;
    std::size_t n_groups_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> group_templates_;

    /// Per-group scratch for aggregates that declared `scratch_doubles`,
    /// laid out group-major: `scratch_[gid * scratch_stride_ + offset[agg]]`.
    /// Group-major so several small consumers in one group share a cache line.
    /// Stays EMPTY when no aggregate asks for any — the same "pay only if used"
    /// rule as text_store_, and `double` keeps it trivially copyable so growth
    /// is a memcpy.
    std::vector<double> scratch_;
    std::size_t scratch_stride_ = 0;
    std::vector<std::uint32_t> scratch_offset_;

    /// Boxed First/Last values for non-numeric columns, parallel to
    /// `flat_slots_` and indexed identically. Stays EMPTY — no allocation, no
    /// ScalarValue construction — for an all-numeric query, which is why the
    /// slot itself can be a POD.
    std::vector<ScalarValue> text_store_;

    // Flat accumulator storage: n_groups_ × n_aggs_ contiguous AggSlotCores.
    SlotArray<AggSlotCore> flat_slots_;

    // Reusable per-chunk gids buffer to avoid repeated heap allocations.
    std::vector<std::uint32_t> gids_buf_;

    // Generic path (non-Categorical group keys).
    KeyRowIndex key_index_;
    std::vector<Key> group_order_;

    // Sentinel for "no group assigned yet" in the dense index arrays.
    static constexpr std::uint32_t kNoGid = std::numeric_limits<std::uint32_t>::max();
    // Cartesian cell-space size below which multi-key grouping uses a dense
    // array (one load per row) instead of hashing. 4M cells = 16 MB of u32.
    static constexpr std::uint64_t kDenseCellLimit = 4'000'000ULL;

    // Single-Categorical fast path: code → gid via direct array (codes are a
    // dense [0, dict_size) index, so no hashing is needed).
    using cat_code = Column<Categorical>::code_type;
    std::vector<std::uint32_t> cat_dense_gid_;
    std::vector<cat_code> cat_order_;

    // Multi-Categorical fast path: cell-encoded. Dense array while the cell
    // space stays under kDenseCellLimit; spills to the hash map otherwise.
    bool multi_dense_ = true;
    std::vector<std::uint32_t> multi_cat_cell_dense_;
    std::vector<std::uint32_t> multi_cat_slots_;  // open addressing on the code tuple: gid + 1
    std::vector<Column<Categorical>::code_type> multi_cat_codes_flat_;  // n_groups_ × n_keys
    std::vector<std::uint64_t> multi_cat_strides_;  // last-seen strides for rebuild detection

    // Single-string-key fast path.
    robin_hood::unordered_flat_map<std::string, std::size_t, StrViewHash, StrViewEq> str_index_;
    std::vector<std::string> str_order_;

    // Single fixed-width-integer-key fast path (int64 / Date / Timestamp, no
    // nulls): a direct value -> gid map, no owned Key per group. `group by <int
    // id>` is one of the most common shapes, and the generic path was building a
    // heap-allocated Key per group for it (117k allocations on TPC-H q02's
    // 117k-group min).
    bool int_fast_path_ = false;
    IntKeyKind int_key_kind_ = IntKeyKind::Int64;
    robin_hood::unordered_flat_map<std::int64_t, std::uint32_t> int_index_;
    std::vector<std::int64_t> int_order_;  ///< group keys, as raw integers, in first-seen order

    // Two fixed-width-integer keys are packed into a two-word composite key
    // and grouped exactly as one integer key: `(l_partkey, l_suppkey)` on
    // TPC-H q20's ~800k groups otherwise falls to the generic `Key` path, which
    // boxes a ScalarValue-vector Key per group and compares it field-by-field on
    // every probe. Keeping two 64-bit values is injective with no knowledge of
    // their domains, so this is always exact and portable to MSVC.
    bool pair_int_fast_path_ = false;
    /// Three or more key columns, all fixed-width and packable. Sets no other
    /// fast-path flag, so the output path treats it as the generic key case.
    bool packed_fast_path_ = false;
    PackedKeyEncoder encoder_;
    PackedGroups<std::uint64_t, robin_hood::hash<std::uint64_t>> packed64_;
    PackedGroups<PackedKeyEncoder::Packed128, PackedKeyEncoder::PackedWordsHash<2>> packed128_;
    PackedGroups<PackedKeyEncoder::Packed256, PackedKeyEncoder::PackedWordsHash<4>> packed256_;
    IntKeyKind int_key_kind_b_ = IntKeyKind::Int64;
    struct PairIntKey {
        std::uint64_t first = 0;
        std::uint64_t second = 0;

        [[nodiscard]] friend auto operator==(const PairIntKey&, const PairIntKey&)
            -> bool = default;
    };
    struct PairIntKeyHash {
        auto operator()(const PairIntKey& key) const noexcept -> std::size_t {
            std::uint64_t h = key.first * 0x9e3779b97f4a7c15ULL;
            h ^= key.second + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
            return static_cast<std::size_t>(h);
        }
    };
    robin_hood::unordered_flat_map<PairIntKey, std::uint32_t, PairIntKeyHash> pair_index_;
    std::vector<std::pair<std::int64_t, std::int64_t>> pair_order_;
    /// Parallel group discovery (see `try_discover_partitioned`). `rows_seen_`
    /// makes a group's first-row index global across chunks, which is what the
    /// first-occurrence numbering is merged on.
    std::vector<KeyPartition<PairIntKey, PairIntKeyHash>> pair_partitions_;
    std::vector<KeyPartition<std::int64_t, robin_hood::hash<std::int64_t>>> int_partitions_;
    std::vector<KeyPartition<std::string, StrViewHash, StrViewEq>> str_partitions_;
    std::vector<std::uint8_t> part_of_row_;
    std::vector<std::size_t> scatter_rows_;
    std::uint64_t rows_seen_ = 0;

    // --- Partition-owned aggregation (plans/parallelism-overview.md "stream
    // multi-key joins" successor, step 2): the PairIntKey path only, admitted
    // by `try_owned_pair`'s gates. `IBEX_DISABLE_OWNED_PAIR_AGG=1` is its kill
    // switch.

    /// A partition's group discovery AND its own final aggregate state --
    /// no global gid, no global `flat_slots_` entry, until
    /// `finalize_owned_pair` walks every partition once at
    /// final emission to restore first-occurrence order (plans doc: "moves
    /// that merge... does not eliminate the ordered merge itself").
    template <typename Key, typename Hash, typename Eq = std::equal_to<Key>>
    struct OwnedPartition {
        robin_hood::unordered_flat_map<Key, std::uint32_t, Hash, Eq> index;
        std::vector<Key> keys;
        std::vector<std::uint64_t> first_rows;
        std::vector<AggSlotCore> slots;  ///< n_local_groups * n_aggs_
    };
    std::vector<OwnedPartition<PairIntKey, PairIntKeyHash>> owned_pair_partitions_;
    /// Set once this operator has committed to owned-partition mode. Per the
    /// plan's safety note, only ever ADMITTED before any other discovery path
    /// (serial or `try_discover_partitioned`) has created a group -- widening
    /// to seed/migrate an in-progress run is out of scope for this prototype.
    bool owned_mode_ = false;
    bool owned_finalized_ = false;
    std::uint64_t owned_rows_seen_ = 0;
    /// Set once `try_discover_partitioned` has run; see the gate there for why
    /// a later chunk may then never fall back to the serial loop.
    bool partitioned_active_ = false;
    /// Input rows this operator has been offered across every chunk, which is
    /// what the partition gate measures. Distinct from `rows_seen_`, which
    /// counts only rows the partitioned path itself consumed and exists to give
    /// group first-rows a global base.
    std::size_t rows_offered_ = 0;
    /// Both keys are 32 bits wide (Categorical code / Date), so the composite
    /// packs into 64 bits and probes `int_index_` instead of `pair_index_`.
    /// The two paths are mutually exclusive, so sharing that map is safe.
    bool pair_packs_u64_ = false;
    /// Flat cell -> gid for a packed pair whose domains are small enough to
    /// enumerate. Cells are numbered from the mins below, so widening them
    /// rebuilds the array.
    std::vector<std::uint32_t> pair_dense_gid_;
    std::int64_t pair_dense_a_min_ = 0;
    std::int64_t pair_dense_a_max_ = 0;
    std::int64_t pair_dense_b_min_ = 0;
    std::int64_t pair_dense_b_max_ = 0;
    std::uint64_t pair_dense_b_span_ = 0;
    bool pair_dense_active_ = false;
};

/// Replays one buffered chunk ahead of the rest of a child stream. Used by
/// ChunkedSortedAggregateOperator to hand the already-pulled first chunk back
/// to a fallback operator without losing it.
class PrependChunkOperator final : public Operator {
   public:
    PrependChunkOperator(Chunk first, OperatorPtr rest)
        : first_(std::move(first)), rest_(std::move(rest)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!emitted_first_) {
            emitted_first_ = true;
            return std::optional<Chunk>{std::move(first_)};
        }
        return rest_->next();
    }

   private:
    Chunk first_;
    OperatorPtr rest_;
    bool emitted_first_ = false;
};

/// Streaming aggregate for input already sorted on the group-by keys.
///
/// When the child's chunks declare an `ordering` whose leading keys cover the
/// group_by columns, every group's rows are contiguous in the stream. We then
/// keep accumulators for only the *current* group, emit each group as soon as
/// its run ends, and produce output already sorted by the group keys. Peak
/// memory is O(one group + one output chunk) instead of O(all groups), and
/// there is no hashing — group changes are detected by a typed equality scan.
///
/// Eligibility is decided from the first non-empty chunk. If the input is not
/// sorted on the group_by keys (no `ordering`, or it doesn't cover them, or a
/// group key is nullable), the operator transparently falls back to the
/// hash-based ChunkedAggregateOperator by replaying the already-pulled chunk
/// ahead of the remaining child. The supported agg subset matches
/// ChunkedAggregateOperator (Count/Sum/Min/Max/Mean on numeric columns);
/// build_operator only routes that subset here.
class ChunkedSortedAggregateOperator final : public Operator {
   public:
    ChunkedSortedAggregateOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                                   const std::vector<ir::AggSpec>* aggregations,
                                   const ExecutionContext& exec)
        : child_(std::move(child)),
          group_by_(group_by),
          aggregations_(aggregations),
          exec_(&exec) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (fallback_) {
            return fallback_->next();
        }
        if (!decided_) {
            auto decided = decide_strategy();
            if (!decided.has_value()) {
                return std::unexpected(std::move(decided.error()));
            }
            if (fallback_) {
                return fallback_->next();
            }
        }
        return next_sorted();
    }

   private:
    struct SlotPlan {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
    };

    // Pull chunks until the first non-empty one, then choose sorted vs fallback.
    auto decide_strategy() -> std::expected<void, std::string> {
        decided_ = true;
        Chunk first;
        bool have = false;
        std::optional<Chunk> schema_only;
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;  // EOF before any rows
            }
            if (chunk_res.value()->rows() == 0) {
                // Empty, but it still carries the input's columns and their
                // types. Keep the first one in case no chunk ever has rows.
                if (!schema_only.has_value() && !chunk_res.value()->columns.empty()) {
                    schema_only = std::move(*chunk_res.value());
                }
                continue;
            }
            first = std::move(*chunk_res.value());
            have = true;
            break;
        }
        if (!have) {
            // Every row was filtered away upstream. Emitting nothing would emit
            // no schema either, and the result would materialize with no columns
            // at all — so a downstream join looking for its key, or a filter for
            // the value it compares, would fail with "unknown column" on what is
            // really just an empty input. The hash operator derives its output
            // columns from the input's types, so hand it the empty chunk and let
            // it produce a properly-shaped empty result.
            if (schema_only.has_value()) {
                fallback_ = std::make_unique<ChunkedAggregateOperator>(
                    std::make_unique<PrependChunkOperator>(std::move(*schema_only),
                                                           std::move(child_)),
                    group_by_, aggregations_, *exec_);
                return {};
            }
            done_ = true;
            input_eof_ = true;
            return {};
        }
        if (!sorted_on_group_by(first) || needs_hash_fallback(first)) {
            fallback_ = std::make_unique<ChunkedAggregateOperator>(
                std::make_unique<PrependChunkOperator>(std::move(first), std::move(child_)),
                group_by_, aggregations_, *exec_);
            return {};
        }
        if (auto err = init_plan(first)) {
            return std::unexpected(*err);
        }
        if (auto err = consume(first)) {
            return std::unexpected(*err);
        }
        return {};
    }

    // The input is grouped-contiguous iff the first |group_by| ordering keys
    // are exactly the group_by columns (as a set; direction and intra-prefix
    // order don't matter for contiguity). Nullable group keys fall back, since
    // the streaming key compare ignores validity.
    [[nodiscard]] auto sorted_on_group_by(const Chunk& chunk) const -> bool {
        if (group_by_->empty()) {
            return false;  // global aggregate: let the hash path handle it
        }
        if (!chunk.ordering().has_value() || chunk.ordering()->size() < group_by_->size()) {
            return false;
        }
        const auto& ordering = *chunk.ordering();
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            bool in_group = false;
            for (const auto& g : *group_by_) {
                if (g.name == ordering[i].name) {
                    in_group = true;
                    break;
                }
            }
            if (!in_group) {
                return false;
            }
        }
        return std::ranges::all_of(*group_by_, [&chunk](const auto& g) {
            const ColumnEntry* entry = find_entry(chunk, g.name);
            return entry != nullptr && !entry->validity.has_value();
        });
    }

    static auto find_entry(const Chunk& chunk, const std::string& name) -> const ColumnEntry* {
        for (const auto& e : chunk.columns) {
            if (e.name == name) {
                return &e;
            }
        }
        return nullptr;
    }

    // Non-numeric First/Last (string/categorical) has no group-at-a-time
    // implementation here — route it to the hash operator, which handles any
    // type. Numeric First/Last streams natively (see accumulate_typed).
    [[nodiscard]] auto needs_hash_fallback(const Chunk& first) const -> bool {
        return std::ranges::any_of(*aggregations_, [&](const ir::AggSpec& agg) {
            if (agg.func != ir::AggFunc::First && agg.func != ir::AggFunc::Last) {
                return false;
            }
            const ColumnEntry* entry = find_entry(first, agg.column.name);
            if (entry == nullptr) {
                return false;  // reported as a proper error by init_plan
            }
            const ExprType kind = expr_type_for_column(*entry->column);
            return kind != ExprType::Int && kind != ExprType::Double;
        });
    }

    auto init_plan(const Chunk& first) -> std::optional<std::string> {
        n_aggs_ = aggregations_->size();
        plan_.resize(n_aggs_);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            const auto& agg = (*aggregations_)[i];
            plan_[i].func = agg.func;
            if (agg.func == ir::AggFunc::Count) {
                plan_[i].kind = ExprType::Int;
                continue;
            }
            const ColumnEntry* entry = find_entry(first, agg.column.name);
            if (entry == nullptr) {
                return "aggregate column not found: " + agg.column.name;
            }
            const ExprType kind = expr_type_for_column(*entry->column);
            if (kind != ExprType::Int && kind != ExprType::Double) {
                return "ChunkedSortedAggregateOperator: non-numeric aggregation not supported";
            }
            plan_[i].kind = kind;
        }
        key_templates_.clear();
        key_templates_.reserve(group_by_->size());
        for (const auto& g : *group_by_) {
            key_templates_.push_back(make_empty_like(*find_entry(first, g.name)->column));
        }
        track_validity_.assign(n_aggs_, 0U);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            track_validity_[i] = chunked_agg_tracks_validity(plan_[i].func) ? 1U : 0U;
        }
        // Capture the leading ordering keys so emitted chunks can advertise the
        // group-sorted order they preserve (lets a downstream `order` skip work).
        if (first.ordering().has_value()) {
            out_ordering_.assign(
                first.ordering()->begin(),
                first.ordering()->begin() + static_cast<std::ptrdiff_t>(group_by_->size()));
        }
        cur_slots_.assign(n_aggs_, AggSlotCore{});
        cur_scratch_.assign(n_aggs_ * kMomentScratch, 0.0);
        reset_output();
        return std::nullopt;
    }

    void reset_output() {
        out_columns_.clear();
        out_columns_.reserve(group_by_->size() + n_aggs_);
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            ColumnEntry entry;
            entry.name = (*group_by_)[i].name;
            entry.column = std::make_shared<ColumnValue>(make_empty_like(key_templates_[i]));
            std::visit([&](auto& c) { c.reserve(kEmitThreshold); }, *entry.column);
            out_columns_.push_back(std::move(entry));
        }
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            ColumnValue column;
            switch (plan_[i].func) {
                case ir::AggFunc::Count:
                    column = Column<std::int64_t>{};
                    break;
                case ir::AggFunc::Mean:
                case ir::AggFunc::Stddev:
                case ir::AggFunc::Skew:
                case ir::AggFunc::Kurtosis:
                    column = Column<double>{};
                    break;
                default:  // Sum / Min / Max
                    column = plan_[i].kind == ExprType::Double
                                 ? ColumnValue{Column<double>{}}
                                 : ColumnValue{Column<std::int64_t>{}};
                    break;
            }
            std::visit([&](auto& c) { c.reserve(kEmitThreshold); }, column);
            ColumnEntry entry;
            entry.name = (*aggregations_)[i].alias;
            entry.column = std::make_shared<ColumnValue>(std::move(column));
            out_columns_.push_back(std::move(entry));
        }
        out_validity_.assign(n_aggs_, ValidityBitmap{});
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (track_validity_[i] != 0U) {
                out_validity_[i].reserve(kEmitThreshold);
            }
        }
        pending_rows_ = 0;
    }

    // Drive input until we have a full output batch or hit EOF, then emit.
    auto next_sorted() -> std::expected<std::optional<Chunk>, std::string> {
        if (done_) {
            return std::optional<Chunk>{};
        }
        while (!input_eof_ && pending_rows_ < kEmitThreshold) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                input_eof_ = true;
                break;
            }
            if (chunk_res.value()->rows() == 0) {
                continue;
            }
            if (auto err = consume(*chunk_res.value())) {
                return std::unexpected(*err);
            }
        }
        if (input_eof_ && open_) {
            close_group();
            open_ = false;
        }
        if (pending_rows_ == 0) {
            done_ = true;
            return std::optional<Chunk>{};
        }
        Chunk out = take_pending();
        if (input_eof_) {
            done_ = true;
        }
        return std::optional<Chunk>{std::move(out)};
    }

    // Fold one chunk into the streaming state. Rows are scanned as runs of
    // equal group keys; each run is accumulated columnwise into the open
    // group's slots, and a group-key change closes the open group.
    auto consume(const Chunk& chunk) -> std::optional<std::string> {
        std::vector<const ColumnValue*> key_cols;
        key_cols.reserve(group_by_->size());
        for (const auto& g : *group_by_) {
            const ColumnEntry* entry = find_entry(chunk, g.name);
            if (entry == nullptr) {
                return "group-by column not found: " + g.name;
            }
            key_cols.push_back(entry->column.get());
        }
        std::vector<const ColumnEntry*> agg_entries(n_aggs_, nullptr);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (plan_[i].func == ir::AggFunc::Count) {
                continue;
            }
            const ColumnEntry* entry = find_entry(chunk, (*aggregations_)[i].column.name);
            if (entry == nullptr) {
                return "aggregate column not found: " + (*aggregations_)[i].column.name;
            }
            if (expr_type_for_column(*entry->column) != plan_[i].kind) {
                return "ChunkedSortedAggregateOperator: aggregate column type changed across "
                       "chunks";
            }
            agg_entries[i] = entry;
        }

        const std::size_t rows = chunk.rows();
        std::size_t r = 0;
        while (r < rows) {
            if (!open_) {
                start_group(key_cols, r);
            } else if (!row_matches_open(key_cols, r)) {
                close_group();
                start_group(key_cols, r);
            }
            std::size_t e = r + 1;
            while (e < rows && cells_equal(key_cols, r, e)) {
                ++e;
            }
            accumulate_range(agg_entries, r, e);
            r = e;
        }
        return std::nullopt;
    }

    void start_group(const std::vector<const ColumnValue*>& key_cols, std::size_t row) {
        open_key_.clear();
        open_key_.reserve(key_cols.size());
        for (const auto* col : key_cols) {
            open_key_.push_back(scalar_from_column(*col, row));
        }
        std::ranges::fill(cur_slots_, AggSlotCore{});
        std::ranges::fill(cur_scratch_, 0.0);
        open_ = true;
    }

    // Whether `row` continues the currently open group. Only called at run
    // anchors (group boundaries and chunk starts), so the scalar build is
    // paid per group, not per row.
    [[nodiscard]] auto row_matches_open(const std::vector<const ColumnValue*>& key_cols,
                                        std::size_t row) const -> bool {
        for (std::size_t i = 0; i < key_cols.size(); ++i) {
            if (scalar_from_column(*key_cols[i], row) != open_key_[i]) {
                return false;
            }
        }
        return true;
    }

    static auto cell_equal(const ColumnValue& col, std::size_t a, std::size_t b) -> bool {
        return std::visit(
            [&](const auto& c) -> bool {
                using ColT = std::decay_t<decltype(c)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    return c.code_at(a) == c.code_at(b);
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    return c[a].days == c[b].days;
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    return c[a].nanos == c[b].nanos;
                } else {
                    return c[a] == c[b];
                }
            },
            col);
    }

    [[nodiscard]] static auto cells_equal(const std::vector<const ColumnValue*>& key_cols,
                                          std::size_t a, std::size_t b) -> bool {
        return std::ranges::all_of(key_cols,
                                   [a, b](const auto* col) { return cell_equal(*col, a, b); });
    }

    // Accumulate the contiguous row range [start, end) — all one group — into
    // the open group's slots, branch-hoisted per aggregation.
    void accumulate_range(const std::vector<const ColumnEntry*>& agg_entries, std::size_t start,
                          std::size_t end) {
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            AggSlotCore& slot = cur_slots_[i];
            if (plan_[i].func == ir::AggFunc::Count) {
                slot.count += static_cast<std::int64_t>(end - start);
                continue;
            }
            const auto& entry = *agg_entries[i];
            const bool has_nulls = entry.validity.has_value();
            if (plan_[i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                accumulate_typed(slot, &cur_scratch_[i * kMomentScratch], plan_[i].func, data,
                                 entry, has_nulls, start, end);
            } else {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                accumulate_typed(slot, &cur_scratch_[i * kMomentScratch], plan_[i].func, data,
                                 entry, has_nulls, start, end);
            }
        }
    }

    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access)
    // We need AggSlotCore to be a POD

    template <typename T>
    /// `scratch` is this aggregate's per-group scratch (2 doubles for the
    /// higher moments); it stays a parameter so this helper remains static and
    /// has no reach into operator state.
    static void accumulate_typed(AggSlotCore& slot, double* scratch, ir::AggFunc func,
                                 const T* data, const ColumnEntry& entry, bool has_nulls,
                                 std::size_t start, std::size_t end) {
        const auto valid = [&](std::size_t row) { return !has_nulls || (*entry.validity)[row]; };
        switch (func) {
            case ir::AggFunc::Sum:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value += data[row];
                    } else {
                        slot.int_value += data[row];
                    }
                    slot.mark_present();
                }
                break;
            case ir::AggFunc::Mean:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    slot.double_value += static_cast<double>(data[row]);
                    slot.count++;
                }
                break;
            case ir::AggFunc::Min:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.present() ? std::min(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.present() ? std::min(slot.int_value, data[row]) : data[row];
                    }
                    slot.mark_present();
                }
                break;
            case ir::AggFunc::Max:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.present() ? std::max(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.present() ? std::max(slot.int_value, data[row]) : data[row];
                    }
                    slot.mark_present();
                }
                break;
            case ir::AggFunc::Stddev:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_stddev(slot, scratch[0], static_cast<double>(data[row]));
                }
                break;
            case ir::AggFunc::Skew:
            case ir::AggFunc::Kurtosis:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_moments(slot, scratch[0], scratch[1], scratch[2],
                                       static_cast<double>(data[row]));
                }
                break;
            case ir::AggFunc::First:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row) || slot.present()) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value = data[row];
                    } else {
                        slot.int_value = data[row];
                    }
                    slot.mark_present();
                }
                break;
            case ir::AggFunc::Last:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value = data[row];
                    } else {
                        slot.int_value = data[row];
                    }
                    slot.mark_present();
                }
                break;
            default:
                break;
        }
    }

    // Flush the open group's key + aggregate values into the output buffers.
    void close_group() {
        for (std::size_t i = 0; i < group_by_->size(); ++i) {
            append_scalar(*out_columns_[i].column, open_key_[i]);
        }
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            ColumnValue& column = *out_columns_[group_by_->size() + i].column;
            const AggSlotCore& slot = cur_slots_[i];
            if (track_validity_[i] != 0U) {
                out_validity_[i].push_back(chunked_agg_valid(plan_[i].func, slot));
            }
            switch (plan_[i].func) {
                case ir::AggFunc::Count:
                    append_scalar(column, ScalarValue{slot.count});
                    break;
                case ir::AggFunc::Mean:
                    append_scalar(
                        column, ScalarValue{slot.count == 0 ? 0.0
                                                            : slot.double_value /
                                                                  static_cast<double>(slot.count)});
                    break;
                case ir::AggFunc::Stddev:
                    append_scalar(column, ScalarValue{agg_finalize_stddev(
                                              slot, cur_scratch_[i * kMomentScratch])});
                    break;
                case ir::AggFunc::Skew:
                    append_scalar(column, ScalarValue{agg_finalize_skew(
                                              slot, cur_scratch_[i * kMomentScratch],
                                              cur_scratch_[(i * kMomentScratch) + 1])});
                    break;
                case ir::AggFunc::Kurtosis:
                    append_scalar(column, ScalarValue{agg_finalize_kurtosis(
                                              slot, cur_scratch_[i * kMomentScratch],
                                              cur_scratch_[(i * kMomentScratch) + 2])});
                    break;
                default:  // Sum / Min / Max
                    if (plan_[i].kind == ExprType::Double) {
                        append_scalar(column, ScalarValue{slot.double_value});
                    } else {
                        append_scalar(column, ScalarValue{slot.int_value});
                    }
                    break;
            }
        }
        ++pending_rows_;
    }

    // NOLINTEND(cppcoreguidelines-pro-type-union-access)

    auto take_pending() -> Chunk {
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            if (track_validity_[i] == 0U || out_validity_[i].empty()) {
                continue;
            }
            bool has_null = false;
            for (std::size_t r = 0; r < out_validity_[i].size(); ++r) {
                if (!out_validity_[i][r]) {
                    has_null = true;
                    break;
                }
            }
            if (has_null) {
                out_columns_[group_by_->size() + i].validity = std::move(out_validity_[i]);
            }
        }
        Chunk out;
        out.columns = std::move(out_columns_);
        if (!out_ordering_.empty()) {
            out.set_properties(TableProperties::sorted_by(out_ordering_));
        }
        reset_output();
        return out;
    }

    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* group_by_;
    const std::vector<ir::AggSpec>* aggregations_;
    const ExecutionContext* exec_;

    bool decided_ = false;
    bool done_ = false;
    bool input_eof_ = false;
    bool open_ = false;
    OperatorPtr fallback_;

    static constexpr std::size_t kEmitThreshold = 8192;

    std::size_t n_aggs_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> key_templates_;
    std::vector<std::uint8_t> track_validity_;
    std::vector<ir::OrderKey> out_ordering_;

    // Open-group state.
    std::vector<AggSlotCore> cur_slots_;
    /// Scratch for the group currently being streamed — 2 doubles per
    /// aggregate that declared any (see SlotPlan::scratch_doubles). This
    /// operator holds one group at a time, so it needs one group's worth.
    /// [m2, m3, m4] per aggregate. Every moment aggregate needs m2 -- the
    /// higher ones read it on each update -- so the stride is uniform rather
    /// than per-function; this operator keeps one group's worth, not millions.
    static constexpr std::size_t kMomentScratch = 3;
    std::vector<double> cur_scratch_;
    std::vector<ScalarValue> open_key_;

    // Output buffers for closed groups awaiting emission.
    std::vector<ColumnEntry> out_columns_;
    std::vector<ValidityBitmap> out_validity_;
    std::size_t pending_rows_ = 0;
};

}  // namespace

auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string> {
    MaterializeOperator sink{std::move(op)};
    return sink.run();
}

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
    return materialize_operator(std::make_unique<ChunkedDistinctOperator>(std::move(source), exec));
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
        std::move(source), right, &keys, exec, suffix, &pending_order));
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
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& ec = static_cast<const ir::ExternCallNode&>(*node);
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
// NOLINTBEGIN cppcoreguidelines-pro-type-static-cast-downcast
// Order-delay past Filter/Project/Rename, and Head/Tail pushdown past
// Project/Rename, are handled by the IR canonicalize pass
// (src/ir/canonicalize.cpp). IR arrives here in canonical form, so
// build_operator only needs one branch per NodeKind and the shapes it
// matches are the post-canonicalization shapes (e.g. Project(Filter(x))
// for the fused operator, not Project(Filter(Order(x)))).

// Runtime-multithreading Phase 1, serial-island slice. Owns the materialized
// input `Table` that the island's `PartitionedTableSource` reads by pointer.
// `input_` is declared before `chain_` so the chain — which holds a raw
// pointer into `input_` — is destroyed first.
class OwningIslandOperator final : public Operator {
   public:
    OwningIslandOperator(std::unique_ptr<Table> input, OperatorPtr chain)
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
class SerialIslandOrderValidator final : public Operator {
   public:
    SerialIslandOrderValidator(OperatorPtr child, std::uint64_t expected_morsels, std::size_t grain)
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
                return std::unexpected("parallel island: morsel identity gap or reordering");
            }
            ++next_sequence_;
            return result;
        }
        if (next_sequence_ != expected_morsels_) {
            return std::unexpected("parallel island: missing output morsel");
        }
        return result;
    }

   private:
    OperatorPtr child_;
    std::uint64_t expected_morsels_ = 0;
    std::uint64_t next_sequence_ = 0;
    std::size_t grain_ = 1;
};

auto island_grain(const ExecutionContext& exec, std::size_t rows) -> std::size_t {
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
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t threads = std::max<std::size_t>(std::min(budget, pool_size), 1);
    return std::clamp(rows / (threads * kMorselsPerThread), kMinGrain, kMaxGrain);
}

auto process_island_stats() -> ParallelIslandStats* {
    // File-local, like the worker pool and the query lease: a bundled plugin
    // statically links runtime code, so an inline header variable would give
    // each plugin its own counter (the RTLD_LOCAL trap).
    //
    // The reporter is a separate static whose destructor runs at exit. It holds
    // no reference to the counters it prints beyond the function-local statics
    // above it, which outlive it by declaration order.
    static const bool enabled = std::getenv("IBEX_PARALLEL_STATS") != nullptr;
    static ParallelIslandStats stats;
    struct Reporter {
        ~Reporter() {
            if (!enabled) {
                return;
            }
            ibex::formatting::print(
                stderr,
                "island stats: parallel={} serial={} morsels={} "
                "pipelined_scans={} pipelined_stages={} range_heads={} two_phase={} "
                "parallel_fields={} parallel_direct_numeric_fields={} parallel_probes={} "
                "grouped_lifted_group_state={} chunk_direct_updates={}\n",
                stats.parallel_islands.load(), stats.serial_islands.load(), stats.morsels.load(),
                stats.pipelined_scans.load(), stats.pipelined_stages.load(),
                stats.range_heads.load(), stats.two_phase_filters.load(),
                stats.parallel_fields.load(), stats.parallel_direct_numeric_fields.load(),
                stats.parallel_probes.load(), stats.grouped_lifted_group_state.load(),
                stats.chunk_direct_updates.load());
        }
    };
    static const Reporter reporter;
    return enabled ? &stats : nullptr;
}

void configure_parallel_from_env(ExecutionContext& exec) {
    if (const auto want = parallel_enabled_from_env(); want.has_value()) {
        exec.parallel = *want;
    }
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
        exec.parallel_stats = process_island_stats();
    }
    if (exec.execution_profile == nullptr && execution_profile_requested()) {
        // The budget occupancy is measured against. Read from the context or
        // the environment rather than `process_worker_pool().size()`, so that
        // asking for a profile never constructs a pool a serial query would
        // otherwise never have built.
        const std::size_t budget =
            exec.parallel_threads != 0 ? exec.parallel_threads : compute_thread_count();
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

// One construction point for every row-local map operator that can live in a
// parallel island. The serial planner uses the same factory: only the island
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
                             const ExternRegistry*, const ExecutionContext&,
                             const std::vector<ColumnKernelSignature>* source_signature,
                             bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const auto& filter = static_cast<const ir::FilterNode&>(*step.node);
    return std::make_unique<ChunkedFilterOperator>(
        std::move(child), &filter.predicate(), scalars,
        physical_filter_route(filter.predicate(), source_signature), preserve_empty_morsels);
}

auto build_metadata_map(const MapStep& step, OperatorPtr child, const ScalarRegistry*,
                        const ExternRegistry*, const ExecutionContext&,
                        const std::vector<ColumnKernelSignature>*, bool)
    -> std::expected<OperatorPtr, std::string> {
    if (step.node->kind() == ir::NodeKind::Project) {
        const auto& project = static_cast<const ir::ProjectNode&>(*step.node);
        return std::make_unique<ChunkedProjectOperator>(std::move(child), &project.columns());
    }
    const auto& rename = static_cast<const ir::RenameNode&>(*step.node);
    return std::make_unique<ChunkedRenameOperator>(std::move(child), &rename.renames());
}

auto build_row_local_update_map(const MapStep& step, OperatorPtr child,
                                const ScalarRegistry* scalars, const ExternRegistry* externs,
                                const ExecutionContext& exec,
                                const std::vector<ColumnKernelSignature>*, bool)
    -> std::expected<OperatorPtr, std::string> {
    const auto& update = static_cast<const ir::UpdateNode&>(*step.node);
    return std::make_unique<ChunkedUpdateOperator>(std::move(child), &update.fields(), scalars,
                                                   externs, exec);
}

auto build_filter_project_gather_map(const MapStep& step, OperatorPtr child,
                                     const ScalarRegistry* scalars, const ExternRegistry*,
                                     const ExecutionContext&,
                                     const std::vector<ColumnKernelSignature>* source_signature,
                                     bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    // Two shapes reach one kernel: canonicalize's fused node, and a Filter the
    // planner fused with the Project above it. The operator only ever wanted a
    // predicate and a column list, which is why physical fusion needs no new
    // kernel -- only a step able to name both nodes.
    const ir::Expr* predicate = nullptr;
    const std::vector<ir::ColumnRef>* columns = nullptr;
    if (step.fused_project != nullptr) {
        predicate = &static_cast<const ir::FilterNode&>(*step.node).predicate();
        columns = &static_cast<const ir::ProjectNode&>(*step.fused_project).columns();
    } else {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(*step.node);
        predicate = &fp.predicate();
        columns = &fp.columns();
    }
    return std::make_unique<ChunkedFilterProjectOperator>(
        std::move(child), predicate, columns, scalars,
        physical_filter_route(*predicate, source_signature), preserve_empty_morsels);
}

auto build_filter_update_project_gather_map(
    const MapStep& step, OperatorPtr child, const ScalarRegistry* scalars,
    const ExternRegistry* externs, const ExecutionContext& exec,
    const std::vector<ColumnKernelSignature>* source_signature, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    // Two shapes, one kernel: canonicalize's fused node, and a Filter the
    // planner fused with the Update and Project above it. The operator wants a
    // predicate, the update fields, and a column list wherever they come from.
    const ir::Expr* predicate = nullptr;
    const std::vector<ir::FieldSpec>* fields = nullptr;
    const std::vector<ir::ColumnRef>* project_columns = nullptr;
    if (step.fused_project != nullptr) {
        predicate = &static_cast<const ir::FilterNode&>(*step.node).predicate();
        fields = &static_cast<const ir::UpdateNode&>(*step.fused_update).fields();
        project_columns = &static_cast<const ir::ProjectNode&>(*step.fused_project).columns();
    } else {
        const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(*step.node);
        predicate = &fup.predicate();
        fields = &fup.fields();
        project_columns = &fup.project_columns();
    }
    robin_hood::unordered_set<std::string> update_outputs;
    robin_hood::unordered_set<std::string> needed;
    for (const auto& field : *fields) {
        update_outputs.insert(field.alias);
        collect_expr_column_refs(field.expr, needed);
    }
    for (const auto& column : *project_columns) {
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
        std::move(child), predicate, fields, project_columns, std::move(gather_columns), scalars,
        externs, exec, physical_filter_route(*predicate, source_signature), preserve_empty_morsels);
}

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

// The base of one worker's island chain: a source the worker points at the
// morsel it just claimed. Two implementations, differing only in whether the
// morsel's rows are copied out of the shared input before the chain sees them.
class MorselSource : public Operator {
   public:
    /// Aim the source at rows [begin, end) of the island's input. The next
    /// `next()` produces exactly that morsel and then reports exhaustion, so
    /// one call feeds one turn of the worker loop.
    virtual void set_morsel(std::size_t begin, std::size_t end, std::uint64_t sequence) = 0;
};

// Gathering source: materializes the morsel, then the chain above runs over it
// exactly as the serial path does. The fallback for any island whose head this
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

// Range-filtering source: absorbs the island's head `Filter` and evaluates its
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
/// away — and `FilterProject` is included because that is what `filter …,
/// select …` canonicalizes to, so restricting this to a bare `Filter` would
/// miss the shape most real queries take.
struct RangeHead {
    const ir::Expr* predicate = nullptr;
    const std::vector<ir::ColumnRef>* project = nullptr;  ///< null when unfused
};

/// The head's range form, or nullopt when it has to be built above a gathered
/// morsel instead.
///
/// Two things disqualify a head:
///
/// - A predicate that is not `is_range_native_expr`. Island eligibility admits
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
    if (step.node->kind() == ir::NodeKind::Filter) {
        // A head absorbs a filter and, at most, the projection directly above
        // it. A fused step that also carries an Update computes columns between
        // the two, and absorbing the projection here would skip that
        // computation -- the projection would then name a column nothing
        // produced. Such a step keeps its operator chain.
        if (step.fused_update != nullptr) {
            return std::nullopt;
        }
        const auto& predicate = static_cast<const ir::FilterNode&>(*step.node).predicate();
        if (!is_range_native_expr(predicate)) {
            return std::nullopt;
        }
        // A planner-fused Project rides along exactly as the fused IR kind's
        // column list does below: same head, same absorbed projection.
        const std::vector<ir::ColumnRef>* project =
            step.fused_project != nullptr
                ? &static_cast<const ir::ProjectNode&>(*step.fused_project).columns()
                : nullptr;
        return RangeHead{.predicate = &predicate, .project = project};
    }
    if (step.node->kind() == ir::NodeKind::FilterProject) {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(*step.node);
        if (!is_range_native_expr(fp.predicate())) {
            return std::nullopt;
        }
        return RangeHead{.predicate = &fp.predicate(), .project = &fp.columns()};
    }
    return std::nullopt;
}

// One worker's private copy of the island's map chain. The operators are
// per-worker (they carry mutable per-chunk state); the IR nodes, registries,
// and the input table they read are shared and immutable for the island's
// lifetime.
struct IslandWorkerChain {
    MorselSource* source = nullptr;  // owned by `chain`, re-aimed per morsel
    OperatorPtr chain;
};

[[nodiscard]] auto build_island_worker_chain(const std::vector<MapStep>& operators,
                                             const Table& input, const ScalarRegistry* scalars,
                                             const ExternRegistry* externs,
                                             const ExecutionContext& exec)
    -> std::expected<IslandWorkerChain, std::string> {
    // A qualifying head is absorbed into the source rather than built as an
    // operator above it — same output, without materializing the morsel first.
    std::size_t first_op = 0;
    std::unique_ptr<MorselSource> source;
    if (!operators.empty()) {
        if (auto head = range_filter_head(operators.front(), input); head.has_value()) {
            source = std::make_unique<RangeFilterMorselSource>(input, head->predicate,
                                                               head->project, scalars);
            first_op = 1;
        }
    }
    if (source == nullptr) {
        source = std::make_unique<GatherMorselSource>(input);
    }

    IslandWorkerChain worker{.source = source.get(), .chain = std::move(source)};
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
            return std::unexpected("parallel island: " + next.error());
        }
        worker.chain = std::move(next.value());
    }
    return worker;
}

// Runtime-multithreading Phase 1: the parallel island executor.
//
// Workers pull numbered morsels from one shared cursor over the immutable
// materialized input, run their own chain over each, and deposit the result in
// a bounded ring indexed by `sequence`. `next()` is the ordered merger: it
// releases results strictly in sequence order, so the operator's output is
// byte-identical to the serial chain's regardless of completion order. The ring
// is the plan's bounded in-flight queue — a worker that runs ahead of the
// consumer by a full window blocks instead of buffering the whole island.
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
class ParallelIslandOperator final : public Operator {
   public:
    ParallelIslandOperator(std::unique_ptr<Table> input, std::vector<IslandWorkerChain> workers,
                           std::size_t grain, std::uint64_t morsel_count, WorkerPool& pool)
        : input_(std::move(input)),
          workers_(std::move(workers)),
          grain_(grain == 0 ? 1 : grain),
          morsel_count_(morsel_count),
          window_(std::max<std::size_t>(workers_.size() * 2, 2)),
          pool_(&pool),
          active_workers_(workers_.size()) {
        ring_.resize(window_);
        ring_ready_.assign(window_, false);
    }

    ~ParallelIslandOperator() override { cancel_and_join(); }

    ParallelIslandOperator(const ParallelIslandOperator&) = delete;
    auto operator=(const ParallelIslandOperator&) -> ParallelIslandOperator& = delete;
    ParallelIslandOperator(ParallelIslandOperator&&) = delete;
    auto operator=(ParallelIslandOperator&&) -> ParallelIslandOperator& = delete;

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

        std::optional<Chunk> chunk;
        {
            std::unique_lock lock(mutex_);
            const auto slot = static_cast<std::size_t>(next_sequence_ % window_);
            {
                // Idle, not serial work: the merger is waiting on its workers.
                const RingWaitScope ring_wait;
                ready_.wait(lock, [&] {
                    return ring_ready_[slot] || cancelled_ || active_workers_ == 0 ||
                           (has_error_ && error_sequence_ <= next_sequence_);
                });
            }
            if (ring_ready_[slot]) {
                chunk = std::move(ring_[slot]);
                ring_[slot].reset();
                ring_ready_[slot] = false;
                ++next_sequence_;
                ++released_;
            }
        }
        if (chunk.has_value()) {
            space_.notify_all();
            return std::optional<Chunk>{std::move(*chunk)};
        }

        // No chunk: the island stopped early. Report why, deterministically.
        //
        // An interrupt outranks a recorded data error. A worker that fails at
        // the moment the user hits Ctrl+C is a race, and reporting its message
        // would make cancellation surface as an arbitrary query error depending
        // on which thread won. The cancellation contract says such a query
        // reports "interrupted", so the interrupt is checked first.
        if (interrupt_requested()) {
            return fail(interrupt_message());
        }
        // Compose the message out here, before `fail()` takes the same lock.
        std::optional<std::string> failure;
        {
            const std::scoped_lock lock(mutex_);
            if (has_error_) {
                failure = error_fixed_ != nullptr ? std::string(error_fixed_) : error_owned_;
            }
        }
        if (failure.has_value()) {
            return fail(std::move(*failure));
        }
        return fail("parallel island: missing output morsel");
    }

   private:
    // Whatever happens to a worker — normal exhaustion, a recorded error, or an
    // exception — it must stop counting as active and must wake the merger.
    // Skipping this on any path leaves the consumer waiting for a sequence that
    // is never coming, which is a hang rather than an error.
    void worker_exited() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            --active_workers_;
        }
        ready_.notify_all();
    }

    void run_worker(std::size_t worker_id) noexcept {
        // Cleanup runs however this scope is left, so no path can leave the
        // consumer waiting on a worker that is gone.
        struct ExitGuard {
            ParallelIslandOperator* self;
            ~ExitGuard() { self->worker_exited(); }
        } const guard{this};

        std::uint64_t sequence = 0;
        try {
            run_worker_loop(worker_id, sequence);
        } catch (const std::exception& error) {
            // An exception is not part of the operator protocol (evaluation
            // reports failure through `expected`), so it is something
            // unplanned — an allocation failure while materializing a morsel,
            // say. Convert it to a sequence-tagged island error so it obeys the
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
                record_error(sequence,
                             "parallel island: worker exception: " + std::string(error.what()));
            } catch (...) {
                record_fault(sequence,
                             "parallel island: worker exception (no memory to report it)");
            }
        } catch (...) {
            record_fault(sequence, "parallel island: worker threw a non-standard exception");
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
            {
                std::unique_lock lock(mutex_);
                // Backpressure: this morsel's ring slot is only free once the
                // consumer has released the morsel `window_` ahead of it.
                // Produced-ahead is idle, not work: `run_task` subtracts this
                // from the worker time it records, or a blocked worker reads as
                // a busy one and `occupancy` overstates the machine.
                const RingWaitScope ring_wait;
                space_.wait(lock, [&] {
                    return cancelled_ || sequence < released_ + window_ ||
                           (has_error_ && error_sequence_ < sequence);
                });
                if (cancelled_ || (has_error_ && error_sequence_ < sequence)) {
                    break;  // only ever abandons morsels above the reported failure
                }
            }
            if (interrupt_requested()) {
                cancel();
                break;
            }

            const auto [begin, end] = morsel_row_range(rows, grain_, sequence);
            worker.source->set_morsel(begin, end, sequence);
            auto produced = worker.chain->next();

            if (!produced.has_value()) {
                record_error(sequence, std::move(produced.error()));
                break;
            }
            if (!produced->has_value()) {
                record_fault(sequence, "parallel island: worker produced no output morsel");
                break;
            }
            Chunk out = std::move(**produced);
            if (out.sequence != sequence || out.row_offset != begin) {
                record_fault(sequence, "parallel island: morsel identity gap or reordering");
                break;
            }
            {
                const std::scoped_lock lock(mutex_);
                const auto slot = static_cast<std::size_t>(sequence % window_);
                ring_[slot] = std::move(out);
                ring_ready_[slot] = true;
            }
            ready_.notify_one();
        }
    }

    // Record an owned message. The caller has already built the string, so
    // taking it by value and moving it under the lock never allocates here.
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

    // Record a message in static storage. Allocates nothing at all, so it is
    // the only reporting path available once allocation is what failed.
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

    // True if `sequence` becomes the reported failure. Lowest sequence wins, so
    // the error a query reports never depends on thread timing.
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

    void cancel() {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        ready_.notify_all();
        space_.notify_all();
    }

    // Called from the destructor, so nothing here may throw: an escaping
    // exception during destruction terminates the process. Worker bodies are
    // already noexcept and convert failures into island errors, so there is
    // nothing for `wait()` to rethrow — this guards the path regardless.
    void cancel_and_join() noexcept {
        try {
            cancel();
            batch_.wait();
        } catch (...) {  // NOLINT(bugprone-empty-catch)
            // Nothing left to report: the caller is either unwinding or has
            // already chosen the message it will return.
        }
    }

    // Drain the island cleanly at EOF, then check the per-worker chains really
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
        if (has_error_) {
            return std::unexpected(error_fixed_ != nullptr ? std::string(error_fixed_)
                                                           : error_owned_);
        }
        for (auto& worker : workers_) {
            auto trailing = worker.chain->next();
            if (!trailing.has_value()) {
                return std::unexpected(std::move(trailing.error()));
            }
            if (trailing->has_value()) {
                return std::unexpected("parallel island: unexpected trailing morsel");
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
    std::vector<IslandWorkerChain> workers_;
    std::size_t grain_ = 1;
    std::uint64_t morsel_count_ = 0;
    std::size_t window_ = 2;
    WorkerPool* pool_;

    std::atomic<std::uint64_t> cursor_{0};

    std::mutex mutex_;
    std::condition_variable ready_;  // consumer waits for the next sequence
    std::condition_variable space_;  // workers wait for ring space
    std::vector<std::optional<Chunk>> ring_;
    std::vector<bool> ring_ready_;
    std::uint64_t released_ = 0;
    std::size_t active_workers_ = 0;
    bool cancelled_ = false;
    // The failure channel is split so it can be written without allocating.
    // `error_owned_` carries a message moved in from a worker (moving a string
    // never allocates); `error_fixed_` points at static storage and is the only
    // path usable when the failure *is* an allocation failure.
    bool has_error_ = false;
    std::string error_owned_;
    const char* error_fixed_ = nullptr;
    std::uint64_t error_sequence_ = 0;

    std::uint64_t next_sequence_ = 0;
    bool started_ = false;
    bool finished_ = false;
    WorkerPool::Batch batch_;
};

// Runtime-multithreading Phase 2: the two-phase parallel filter.
//
// What the ordered merger above cannot remove is the merge itself. Each worker
// materializes its morsel's surviving rows, and `MaterializeOperator` then
// copies all of them again into one table — so a filter island copies its
// output twice where the serial path copies it once. That is why island wins
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
        // Sequence 0 / row_offset 0: this operator emits the island's whole
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
                return project_table(input, static_cast<const ir::ProjectNode&>(node).columns());
            case ir::NodeKind::Rename:
                return rename_table(input, static_cast<const ir::RenameNode&>(node).renames());
            default:
                // The island builder only admits `is_metadata_only_node` kinds
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

// How many workers an island of `morsel_count` morsels over `rows` rows should
// run on: 0 means "stay on the serial morsel chain".
//
// This is the plan's grain-size serial threshold. Below it, task dispatch,
// ring synchronization, and the merge cost more than the map they parallelize —
// cache-resident work should not pay for threads. A single morsel is serial by
// definition, and a one-thread budget means the caller asked for serial.
/// Whether this input is worth morselizing at all — a *different* question from
/// how many workers it deserves, and conflating the two is a trap worth naming.
///
/// A "refused" island used to mean a serial sweep of morsels, which still pays
/// per-morsel materialization and the merge concat. So refusing by dropping the
/// worker count made a small query **slower than never forming an island**:
/// measured 100ms against 36ms for the plain serial path, and it got worse once
/// the grain was derived, because that turned 2 morsels into 32. When the
/// answer is no, the input has to run as ONE whole-table chunk.
///
/// Two thresholds, because an island's cost has two dimensions. Rows alone
/// cannot express it: 131,072 rows won at 6 columns and lost at 2 on the very
/// same predicate, and every row threshold puts those on the same side.
[[nodiscard]] auto island_is_worth_morselizing(const ExecutionContext& exec, std::size_t rows,
                                               std::size_t columns) -> bool {
    if (rows < exec.parallel_min_rows) {
        return false;
    }
    return exec.parallel_min_cells == 0 || columns == 0 ||
           rows * columns >= exec.parallel_min_cells;
}

[[nodiscard]] auto island_worker_count(const ExecutionContext& exec, std::uint64_t morsel_count)
    -> std::size_t {
    if (morsel_count < 2 || !exec.parallel) {
        return 0;
    }
    // Past the parallel gate: consulting the pool here is free of the
    // construct-before-declining hazard because a parallel query has already
    // built it (or is about to, on its first fan-out).
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t workers =
        std::min({budget, pool_size, static_cast<std::size_t>(morsel_count)});
    return workers < 2 ? 0 : workers;
}

// Build one eligible row-local parallel-map chain as an island: materialize its
// input subtree once, then run the chain over morsels of that table instead of
// one whole-table chunk. The operators are ordered source-to-sink.
//
// Two executors, one morsel model. A large input fans out across the worker
// pool and is reassembled by `ParallelIslandOperator`'s ordered merger; a small
// one (or a single-threaded budget) runs the same morsels serially through a
// `PartitionedTableSource`, where `MaterializeOperator`'s in-order concat is
// the trivially ordered merger. Both stamp and check the same morsel identity,
// so both are byte-identical to the plain serial chain — which is exactly what
// lets the threshold move without changing an answer.
//
// LOAD-BEARING INVARIANT — materialize before fan-out. The input subtree is
// executed to a `Table` here, on this thread, and every morsel source below
// takes that finished table by reference. That is what makes a deferred/lazy
// source safe in an island: its decode runs exactly once, serially, before any
// worker exists, so neither `LazyTable::cache_` nor a plugin's `decode_`
// closure is ever touched concurrently. It is why `build_operator`'s seam no
// longer screens islands for deferred sources.
//
// The morsel sources all take `const Table&`, so the invariant is enforced by
// their signatures rather than by a check. Streaming a source's morsels
// directly into workers would mean handing them something other than a
// finished table — at which point the LazyTable synchronization contract
// applies in full and eligibility has to be re-established.
/// The steps of a plan's parallel prefix, ordered source-to-sink. A plan
/// records steps sink-first; every executor here composes bottom-up.
auto parallel_pipeline_operators(const physical::Plan& plan) -> std::vector<MapStep> {
    std::vector<MapStep> operators;
    operators.reserve(plan.parallel_steps);
    for (std::size_t i = plan.parallel_steps; i > 0; --i) {
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
    auto input_op = build_operator(*input_node, registry, scalars, externs, exec, model_out);
    if (!input_op.has_value()) {
        return std::unexpected(std::move(input_op.error()));
    }
    auto input_tbl = materialize_operator(std::move(input_op.value()));
    if (!input_tbl.has_value()) {
        return std::unexpected(std::move(input_tbl.error()));
    }
    auto owned = std::make_unique<Table>(std::move(input_tbl.value()));
    const std::size_t grain = island_grain(exec, owned->rows());
    const auto expected_morsels = partitioned_morsel_count(*owned, grain);
    const bool morselize = island_is_worth_morselizing(exec, owned->rows(), owned->columns.size());
    const std::size_t worker_count = morselize ? island_worker_count(exec, expected_morsels) : 0;
    if (exec.parallel_stats != nullptr) {
        auto& stats = *exec.parallel_stats;
        (worker_count >= 2 ? stats.parallel_islands : stats.serial_islands)
            .fetch_add(1, std::memory_order_relaxed);
        stats.morsels.fetch_add(expected_morsels, std::memory_order_relaxed);
    }

    if (worker_count >= 2) {
        const auto head =
            operators.empty() ? std::nullopt : range_filter_head(operators.front(), *owned);
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

        std::vector<IslandWorkerChain> workers;
        workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            auto worker = build_island_worker_chain(operators, *owned, scalars, externs, exec);
            if (!worker.has_value()) {
                return std::unexpected(std::move(worker.error()));
            }
            workers.push_back(std::move(worker.value()));
        }
        return std::make_unique<ParallelIslandOperator>(std::move(owned), std::move(workers), grain,
                                                        expected_morsels, process_worker_pool());
    }

    if (!morselize) {
        // Too little work to be worth splitting: run the chain over one
        // whole-table chunk. This is the plain serial path — same map
        // operators, same `preserve_empty_morsels = false`, one chunk in and
        // one chunk out — so it costs exactly what not forming an island costs.
        // Morselizing here instead would add a per-morsel gather and a merge
        // concat to buy parallelism that was already judged not worth having.
        OperatorPtr serial = make_table_source(std::move(*owned));
        for (const MapStep& op_node : operators) {
            auto next = build_row_local_map_operator(op_node, std::move(serial), scalars, externs,
                                                     exec, false);
            if (!next.has_value()) {
                return std::unexpected("parallel island: " + next.error());
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
            return std::unexpected("parallel island: " + next.error());
        }
        chain = std::move(next.value());
    }

    chain = std::make_unique<SerialIslandOrderValidator>(std::move(chain), expected_morsels, grain);
    return std::make_unique<OwningIslandOperator>(std::move(owned), std::move(chain));
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
        if (!exec.parallel || on_worker_pool_thread()) {
            return 1;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec.parallel_threads != 0 ? exec.parallel_threads : pool.size();
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
          ring_(window_) {}

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
            std::expected<Chunk, std::string> produced = std::unexpected("missing pipeline unit");
            {
                std::unique_lock lock(mutex_);
                const std::size_t slot = next_sequence_ % window_;
                {
                    // Idle, not serial work: the consumer is waiting on its
                    // decode workers. Counting this as self time reported a
                    // streaming scan's wait as the query's serial residue.
                    const RingWaitScope ring_wait;
                    ready_.wait(lock, [&] {
                        return ring_[slot].has_value() || cancelled_ || worker_failure_.has_value();
                    });
                }
                if (worker_failure_.has_value()) {
                    auto message = std::move(*worker_failure_);
                    lock.unlock();
                    return fail(std::move(message));
                }
                if (!ring_[slot].has_value()) {
                    lock.unlock();
                    return fail(interrupt_requested() ? interrupt_message()
                                                      : "scan pipeline: missing output unit");
                }
                produced = std::move(**ring_[slot]);
                ring_[slot].reset();
                ++next_sequence_;
                ++released_;
            }
            space_.notify_all();

            if (!produced.has_value()) {
                return fail(std::move(produced.error()));
            }
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
        try {
            auto& worker = workers_[worker_id];
            while (true) {
                const std::size_t sequence = cursor_.fetch_add(1, std::memory_order_relaxed);
                if (sequence >= units_.size()) {
                    return;
                }
                {
                    std::unique_lock lock(mutex_);
                    const RingWaitScope ring_wait;  // produced-ahead: idle, not work
                    space_.wait(lock, [&] { return cancelled_ || sequence < released_ + window_; });
                    if (cancelled_) {
                        return;
                    }
                }
                if (interrupt_requested()) {
                    cancel();
                    return;
                }

                auto result = run_unit(worker, sequence);
                {
                    const std::scoped_lock lock(mutex_);
                    ring_[sequence % window_] = std::move(result);
                }
                ready_.notify_one();
            }
        } catch (const std::exception& error) {
            record_worker_failure("scan pipeline: worker exception: " + std::string(error.what()));
        } catch (...) {
            record_worker_failure("scan pipeline: worker threw a non-standard exception");
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

    void record_worker_failure(std::string message) noexcept {
        {
            const std::scoped_lock lock(mutex_);
            if (!worker_failure_.has_value()) {
                worker_failure_ = std::move(message);
            }
            cancelled_ = true;
        }
        ready_.notify_all();
        space_.notify_all();
    }

    void cancel() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        ready_.notify_all();
        space_.notify_all();
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
        cancel();
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
    std::vector<std::optional<std::expected<Chunk, std::string>>> ring_;
    std::vector<std::optional<Column<Categorical>>> cat_states_;
    std::optional<Chunk> empty_schema_carrier_;
    std::optional<WorkerPool::Batch> batch_;
    std::atomic<std::size_t> cursor_{0};
    std::mutex mutex_;
    std::condition_variable ready_;
    std::condition_variable space_;
    std::optional<std::string> worker_failure_;
    std::size_t released_ = 0;
    std::size_t next_sequence_ = 0;
    std::size_t emitted_rows_ = 0;
    std::uint64_t emitted_sequence_ = 0;
    bool started_ = false;
    bool cancelled_ = false;
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
    if (!exec.parallel || on_worker_pool_thread() || process_worker_pool().size() < 2) {
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

[[nodiscard]] auto build_pipelined_scan(const std::vector<MapStep>& operators, bool count_as_island,
                                        const DeferredScan& scan, std::vector<SourceUnit> units,
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
        if (count_as_island) {
            exec.parallel_stats->parallel_islands.fetch_add(1, std::memory_order_relaxed);
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
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& scan = static_cast<const ir::ScanNode&>(node);
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
        ExecutionProfileScope scope(entry, ProfilePhase::Build);
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

auto build_operator_impl(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    // Runtime-multithreading Phase 1 seam. Only consult the island analysis
    // when a parallel executor is actually requested — build_operator() is a
    // hot query-construction path, so the serial default must not pay for
    // analysis it would discard. When eligible, the whole row-local chain is
    // built as one island here and its inner nodes are not recursed into
    // separately (only the island's input subtree is), so there is no
    // re-analysis of the chain and no infinite recursion.
    //
    // A lazy/deferred source in the island's input subtree used to disqualify
    // it, per the LazyTable synchronization contract's interim gate. That gate
    // is LIFTED (Phase 3b): `build_parallel_island` materializes its input
    // subtree into an owned Table *before* constructing any morsel source, so
    // every deferred decode happens on the single build thread and no worker
    // ever reaches a `LazyTable`. The contract's hazards — concurrent `cache_`
    // writes and concurrent `decode_` calls — need a worker to touch the source
    // to arise, and none does.
    //
    // That is a claim about `build_parallel_island`'s structure, so it is
    // asserted there rather than restated here. A future slice that streams a
    // source's morsels straight into workers, instead of materializing first,
    // reintroduces both hazards and must re-establish eligibility (per-worker
    // readers + a frozen cache) before it removes that assertion.
    // Physical-plan seam (plans/kernel-pipeline-execution-plan.md Phase 2,
    // item 4). One plan per node, consulted in both modes: it decides whether
    // this is a pipeline at all, and if so whether the pipeline may run over
    // morsels. There is no second analysis to disagree with it.
    const physical::Plan plan = physical::plan_physical(node, registry, externs);
    if (plan.migrated && plan.mode == physical::PipelineMode::MorselParallel && exec.parallel) {
        // A row-local chain rooted directly at a decomposable lazy scan is
        // one physical pipeline: decode and maps run in the same worker
        // task, and the ordered ring feeds the next breaker. This is the
        // production replacement for the old materialize-before-fan-out
        // island boundary. Probe scans keep their join-owned dynamic
        // filter timing and therefore do not enter here.
        const ir::Node* input_node = physical::parallel_input_node(plan);
        if (exec.stream_scans && input_node != nullptr &&
            input_node->kind() == ir::NodeKind::Scan) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
            const auto& scan = static_cast<const ir::ScanNode&>(*input_node);
            if (!registry.contains(scan.source_name())) {
                if (const auto* deferred = exec.deferred_scan(scan.source_name());
                    deferred != nullptr && deferred->filter == nullptr) {
                    auto units = deferred_scan_units(*deferred);
                    if (units.size() > 1 && scan_pipeline_worker_count(units.size()) >= 2) {
                        physical::note_map_pipeline_executed();
                        return build_pipelined_scan(parallel_pipeline_operators(plan), true,
                                                    *deferred, std::move(units), scalars, externs,
                                                    exec);
                    }
                }
            }
        }
        physical::note_map_pipeline_executed();
        return build_map_pipeline_parallel(plan, registry, scalars, externs, exec, model_out);
    }
    // Serial composition of a migrated pipeline: same constructors, same
    // per-node profile entries, same source construction (the source goes
    // through the public build_operator, so every Scan/ExternCall streaming
    // decision below is unchanged).
    //
    // In parallel mode this is deliberately NOT taken. A pipeline whose own
    // mode is serial may still contain a parallel one below a step that bounds
    // it — `df[filter ...][update ...]` is the shape — and the per-kind
    // recursion below finds it by re-planning at each node. Consuming the whole
    // chain here would swallow that inner pipeline. Modelling a serial tail
    // over a parallel prefix in one plan is Phase 3 work.
    if (plan.migrated && !exec.parallel) {
        physical::note_map_pipeline_executed();
        return build_physical_map_step(plan, 0, registry, scalars, externs, exec, model_out);
    }
    if (!plan.migrated && !exec.parallel) {
        physical::note_materialized_call(plan.reason);
    }

    // A deferred lazy scan can be streamed instead of materialized. Everything
    // else — a registered table, a source with no unit decomposition — falls
    // through to the whole-table path at the bottom of this function, so
    // declining here costs nothing but the whole-table behaviour.
    if (node.kind() == ir::NodeKind::Scan && exec.stream_scans) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
        const auto& scan = static_cast<const ir::ScanNode&>(node);
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
                    if (exec.parallel && scan_pipeline_worker_count(units.size()) > 0) {
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
        const auto& filter = static_cast<const ir::FilterNode&>(node);
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
        const auto& project = static_cast<const ir::ProjectNode&>(node);
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

    // Fused Project(Filter(x)) produced by canonicalize R5.
    if (node.kind() == ir::NodeKind::FilterProject) {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
        if (fp.children().empty()) {
            return std::unexpected("filter_project node missing child");
        }
        auto child_op =
            build_operator(*fp.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return build_row_local_map_operator(node, std::move(child_op.value()), scalars, externs,
                                            exec, false);
    }

    // Fused Head(Filter(x)) / Tail(Filter(x)) produced by canonicalize R7/R8.
    if (node.kind() == ir::NodeKind::FilterHead) {
        const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
        if (fh.children().empty()) {
            return std::unexpected("filter_head node missing child");
        }
        auto child_op =
            build_operator(*fh.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterHeadOperator>(std::move(child_op.value()),
                                                           &fh.predicate(), fh.count(), scalars);
    }
    if (node.kind() == ir::NodeKind::FilterTail) {
        const auto& ft = static_cast<const ir::FilterTailNode&>(node);
        if (ft.children().empty()) {
            return std::unexpected("filter_tail node missing child");
        }
        auto child_op =
            build_operator(*ft.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterTailOperator>(std::move(child_op.value()),
                                                           &ft.predicate(), ft.count(), scalars);
    }

    // Fused Project(Update(Filter(x))) produced by canonicalize R6.
    if (node.kind() == ir::NodeKind::FilterUpdateProject) {
        const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
        if (fup.children().empty()) {
            return std::unexpected("filter_update_project node missing child");
        }
        auto child_op =
            build_operator(*fup.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return build_row_local_map_operator(node, std::move(child_op.value()), scalars, externs,
                                            exec, false);
    }

    if (node.kind() == ir::NodeKind::Rename) {
        const auto& rename = static_cast<const ir::RenameNode&>(node);
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
        const auto& ec = static_cast<const ir::ExternCallNode&>(node);
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

    if (node.kind() == ir::NodeKind::Distinct) {
        if (node.children().empty()) {
            return std::unexpected("distinct node missing child");
        }
        auto child_op =
            build_operator(*node.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedDistinctOperator>(std::move(child_op.value()), exec);
    }

    if (node.kind() == ir::NodeKind::Order) {
        const auto& order = static_cast<const ir::OrderNode&>(node);
        if (order.children().empty()) {
            return std::unexpected("order node missing child");
        }
        auto child_op =
            build_operator(*order.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedOrderOperator>(std::move(child_op.value()), &order.keys(),
                                                      exec);
    }

    if (node.kind() == ir::NodeKind::Aggregate) {
        const auto& agg = static_cast<const ir::AggregateNode&>(node);
        if (agg.children().empty()) {
            return std::unexpected("aggregate node missing child");
        }
        bool streamable = true;
        for (const auto& spec : agg.aggregations()) {
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
                    // (numeric, string, categorical stream; Date/Timestamp fall
                    // to the hash operator's error path — unreachable in
                    // practice since aggregation on those types is rejected
                    // upstream of the chunked path entirely, same as every
                    // other agg func).
                    break;
                default:
                    // Median/Quantile need all values; Ewma is row-order
                    // coupled — these stay on the materializing path.
                    streamable = false;
                    break;
            }
            if (!streamable) {
                break;
            }
        }
        const ir::Node* aggregate_child = agg.children().front().get();
        // Only nodes this rewrite can account for may be walked past, because
        // what follows aggregates the JOIN's output directly and never builds
        // the skipped nodes. A Project selects columns the join output already
        // has; an Update has to be vetted by `fused_left_join_counted_column`,
        // which is why they are collected rather than merely counted.
        //
        // FilterProject and FilterUpdateProject used to be skipped here too,
        // and that silently dropped their predicate: `(l left join r on k)
        // [filter v > k][select {k, v}][select {n = sum(k)}, by k]` returned a
        // group per join key instead of a group per surviving row.
        std::vector<const ir::UpdateNode*> skipped_updates;
        while (aggregate_child != nullptr &&
               (aggregate_child->kind() == ir::NodeKind::Project ||
                aggregate_child->kind() == ir::NodeKind::Update) &&
               !aggregate_child->children().empty()) {
            if (aggregate_child->kind() == ir::NodeKind::Update) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
                skipped_updates.push_back(static_cast<const ir::UpdateNode*>(aggregate_child));
            }
            aggregate_child = aggregate_child->children().front().get();
        }
        if (streamable && aggregate_child != nullptr &&
            aggregate_child->kind() == ir::NodeKind::Join) {
            const auto& join = static_cast<const ir::JoinNode&>(*aggregate_child);
            // Null when an update between the aggregate and the join computes
            // something this rewrite cannot reproduce from the join output.
            auto counted = fused_left_join_counted_column(agg, skipped_updates);
            const bool candidate = join.kind() == ir::JoinKind::Left && join.keys().size() == 1 &&
                                   !join.predicate().has_value() && agg.group_by().size() == 1 &&
                                   agg.aggregations().size() == 1 &&
                                   (agg.aggregations().front().func == ir::AggFunc::Count ||
                                    agg.aggregations().front().func == ir::AggFunc::Sum) &&
                                   counted.has_value() &&
                                   agg.group_by().front().name == join.keys().front().left;
            if (candidate) {
                auto left = materialize_row_local(*join.children()[0], registry, scalars, externs,
                                                  exec, model_out);
                if (!left.has_value()) {
                    return std::unexpected(std::move(left.error()));
                }
                auto right = materialize_row_local(*join.children()[1], registry, scalars, externs,
                                                   exec, model_out);
                if (!right.has_value()) {
                    return std::unexpected(std::move(right.error()));
                }
                const std::string counted_column = std::move(*counted);
                if (auto fused = left_join_count_table(join, agg, *left, *right, counted_column);
                    fused.has_value()) {
                    return make_table_source(std::move(*fused));
                }
                auto joined =
                    join_table_impl(*left, *right, join.kind(), join.keys(), nullptr, scalars,
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
        }
        if (streamable) {
            auto child_op = build_operator(*agg.children().front(), registry, scalars, externs,
                                           exec, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            // The sorted operator streams group-at-a-time when the child's
            // chunks arrive sorted on the group keys, and otherwise replays the
            // first chunk into a hash ChunkedAggregateOperator — so it is safe
            // to route the whole streamable subset here.
            // Aggregates are often the terminal breaker and hash aggregation
            // emits only after consuming all input. Scheduling one in its own
            // stage in that shape buys no overlap and only creates a thread.
            // A join below it is staged instead: its probe stream can fill the
            // aggregate while it keeps pulling the next probe chunk.
            return std::make_unique<ChunkedSortedAggregateOperator>(
                std::move(child_op.value()), &agg.group_by(), &agg.aggregations(), exec);
        }
    }

    if (node.kind() == ir::NodeKind::TopK) {
        // Fused Head(Order(x)) / Tail(Order(x)) — canonicalize R16. The
        // chunked implementation uses a partial heap-select (O(n log k)).
        const auto& topk = static_cast<const ir::TopKNode&>(node);
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
        return std::make_unique<ChunkedOrderedLimitOperator>(
            std::move(child_op.value()), &topk.keys(), topk.count(), &topk.group_by(), keep);
    }

    if (node.kind() == ir::NodeKind::Head) {
        const auto& head = static_cast<const ir::HeadNode&>(node);
        if (head.children().empty()) {
            return std::unexpected("head node missing child");
        }
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

    if (node.kind() == ir::NodeKind::Tail) {
        const auto& tail = static_cast<const ir::TailNode&>(node);
        if (tail.children().empty()) {
            return std::unexpected("tail node missing child");
        }
        auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
        if (!count.has_value()) {
            return std::unexpected(count.error());
        }
        // Tail(Order(x)) → TopK via R16; Tail(Filter(x)) no-group_by → FilterTail via R8;
        // Tail past Project/Rename via R4.
        return build_unary_materializing_operator(
            *tail.children().front(), registry, scalars, externs, exec, model_out,
            [&](Table input) { return tail_table(input, *count, tail.group_by()); });
    }

    if (node.kind() == ir::NodeKind::Columns) {
        if (node.children().empty()) {
            return std::unexpected("columns node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, exec, model_out,
                                                  [](Table input) { return columns_table(input); });
    }

    if (node.kind() == ir::NodeKind::Melt) {
        const auto& mn = static_cast<const ir::MeltNode&>(node);
        if (mn.children().empty()) {
            return std::unexpected("melt node missing child");
        }
        return build_unary_materializing_operator(
            *mn.children().front(), registry, scalars, externs, exec, model_out,
            [&](Table input) { return melt_table(input, mn.id_columns(), mn.measure_columns()); });
    }

    if (node.kind() == ir::NodeKind::Dcast) {
        const auto& dn = static_cast<const ir::DcastNode&>(node);
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
        const auto& join = static_cast<const ir::JoinNode&>(node);
        if (join.children().size() != 2) {
            return std::unexpected("join node expects exactly two children");
        }
        const bool streamable_semi_anti =
            (join.kind() == ir::JoinKind::Semi || join.kind() == ir::JoinKind::Anti) &&
            !join.predicate().has_value() && join.keys().size() == 1 &&
            join.null_match() == ir::NullMatch::Never && !join.expect().asserts_anything() &&
            join.take() == ir::MatchSelection::All;
        if (streamable_semi_anti) {
            const bool stage_probe =
                has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
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
            auto right = materialize_row_local(*join.children()[1], registry, scalars, externs,
                                               exec, model_out);
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return make_pipelined_stage_if(std::make_unique<ChunkedSemiAntiJoinOperator>(
                                               std::move(left_op.value()), std::move(right.value()),
                                               join.kind(), &join.keys(), &exec),
                                           stage_probe, exec,
                                           execution_profile_entry(exec.execution_profile, node));
        }
        // `nulls equal` goes to the materialized join, which implements the
        // policy. These streaming operators hash and probe on their own and
        // would each need the same null tagging; sending the opt-in case to the
        // one implementation that has it keeps a single definition of the
        // semantics -- and leaves this hot path bit-for-bit unchanged for every
        // join that does not ask for it.
        if (is_streamable_inner_join(join)) {
            const bool stage_probe =
                has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
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
                return make_pipelined_stage_if(
                    std::make_unique<ChunkedInnerJoinOperator>(
                        std::move(left_op.value()), join.children()[1].get(), &registry, scalars,
                        externs, exec, &join.keys(), probe.scan, *probe.name, join.suffix(),
                        &join.pending_order()),
                    stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
            }
            auto right = materialize_row_local(*join.children()[1], registry, scalars, externs,
                                               exec, model_out);
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return make_pipelined_stage_if(
                std::make_unique<ChunkedInnerJoinOperator>(
                    std::move(left_op.value()), std::move(right.value()), &join.keys(), exec,
                    join.suffix(), &join.pending_order()),
                stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
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
        if (is_streamable_pair_int_join(join)) {
            const bool stage_probe =
                has_multi_unit_deferred_scan(*join.children()[0], registry, exec);
            const auto probe = deferred_probe_scan_of(*join.children()[1], exec);
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            if (probe.scan != nullptr) {
                return make_pipelined_stage_if(
                    std::make_unique<ChunkedInnerJoinOperator>(
                        std::move(left_op.value()), join.children()[1].get(), &registry, scalars,
                        externs, exec, &join.keys(), probe.scan, *probe.name, join.suffix(),
                        &join.pending_order()),
                    stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
            }
            auto right = materialize_row_local(*join.children()[1], registry, scalars, externs,
                                               exec, model_out);
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return make_pipelined_stage_if(
                std::make_unique<ChunkedInnerJoinOperator>(
                    std::move(left_op.value()), std::move(right.value()), &join.keys(), exec,
                    join.suffix(), &join.pending_order()),
                stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
        }
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
        const auto& update = static_cast<const ir::UpdateNode&>(node);
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
        const auto& rs = static_cast<const ir::ResampleNode&>(node);
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
        const auto& win = static_cast<const ir::WindowNode&>(node);
        if (node.children().empty()) {
            return std::unexpected("window node missing child");
        }
        const ir::Node& child_node = *node.children().front();
        if (child_node.kind() != ir::NodeKind::Update) {
            return std::unexpected(
                "window: only 'update' is currently supported inside a window block");
        }
        const auto& update_node = static_cast<const ir::UpdateNode&>(child_node);
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
        const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
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
        const auto& mn = static_cast<const ir::ModelNode&>(node);
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
        const auto& program = static_cast<const ir::ProgramNode&>(node);
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
        ExecutionProfileScope scope(entry, ProfilePhase::Build);
        result = build_operator_impl(node, registry, scalars, externs, exec, model_out);
    }
    if (!result.has_value()) {
        return result;
    }
    return profile_operator(std::move(result.value()), exec.execution_profile, node);
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace ibex::runtime
