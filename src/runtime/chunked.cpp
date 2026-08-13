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
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_internal.hpp"
#include "model_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

auto chunk_to_table(Chunk chunk) -> Table {
    Table t;
    t.columns = std::move(chunk.columns);
    for (std::size_t i = 0; i < t.columns.size(); ++i) {
        t.index[t.columns[i].name] = i;
    }
    t.set_properties(chunk.properties());
    if (t.columns.empty()) {  // logical_rows is only meaningful when column-less
        t.logical_rows = chunk.logical_rows;
    }
    return t;
}

auto table_to_chunk(Table table) -> Chunk {
    Chunk c;
    c.columns = std::move(table.columns);
    c.set_properties(table.properties());
    if (c.columns.empty()) {  // logical_rows is only meaningful when column-less
        c.logical_rows = table.logical_rows;
    }
    return c;
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
            return slot.has_value;
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
                          const ScalarRegistry* scalars, bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          scalars_(scalars),
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
            const Table t = chunk_to_table(std::move(input));
            auto filtered = filter_table(t, *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->columns.empty() && filtered->rows() == 0) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{
                        table_to_chunk(std::move(filtered.value()), identity)};
                }
                schema_.hold(std::move(filtered.value()), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(filtered.value()), identity)};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const ScalarRegistry* scalars_;
    bool preserve_empty_morsels_ = false;
    SchemaCarrier schema_;
};

/// Per-chunk project: pulls a chunk, reuses `project_table` to select
/// and rename columns, and forwards the result. Stateless and order
/// preserving; no inter-chunk coordination is needed.
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
        Chunk input = std::move(*chunk_res.value());
        const auto identity = chunk_identity_of(input);
        const Table t = chunk_to_table(std::move(input));
        auto projected = project_table(t, *columns_);
        if (!projected.has_value()) {
            return std::unexpected(std::move(projected.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(projected.value()), identity)};
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
                                 const ScalarRegistry* scalars, bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          columns_(columns),
          scalars_(scalars),
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
            const Table t = chunk_to_table(std::move(input));
            auto out = filter_project_table(t, *predicate_, *columns_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (!out->columns.empty() && out->rows() == 0) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{table_to_chunk(std::move(out.value()), identity)};
                }
                schema_.hold(std::move(out.value()), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(out.value()), identity)};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::ColumnRef>* columns_;
    const ScalarRegistry* scalars_;
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
            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto out = filter_table_limit(t, *predicate_, remaining_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            const std::size_t produced = out->rows();
            if (!out->columns.empty() && produced == 0) {
                schema_.hold(std::move(out.value()));
                continue;
            }
            remaining_ -= produced;
            if (remaining_ == 0) {
                done_ = true;
            }
            (void)count_;
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
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
            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_table(t, *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (filtered->columns.empty()) {
                continue;
            }
            if (filtered->rows() == 0) {
                schema_.hold(std::move(filtered.value()));
                continue;
            }
            buffered_rows_ += filtered->rows();
            buffered_.push_back(std::move(filtered.value()));
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
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (src_t.columns[i].name != out.columns[i].name) {
                    return std::unexpected("tail: chunk schema mismatch (column name)");
                }
                if (src_t.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("tail: chunk schema mismatch (column type)");
                }
                auto& dst_col = out.mutable_column(i);
                std::visit(
                    [&](auto& dst) {
                        using Col = std::decay_t<decltype(dst)>;
                        auto& src = std::get<Col>(*src_t.columns[i].column);
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
    ChunkedFilterUpdateProjectOperator(OperatorPtr child, const ir::Expr* predicate,
                                       const std::vector<ir::FieldSpec>* fields,
                                       const std::vector<ir::ColumnRef>* project_columns,
                                       std::vector<ir::ColumnRef> gather_columns,
                                       const ScalarRegistry* scalars, const ExternRegistry* externs,
                                       const ExecutionContext& exec,
                                       bool preserve_empty_morsels = false)
        : child_(std::move(child)),
          predicate_(predicate),
          fields_(fields),
          project_columns_(project_columns),
          gather_columns_(std::move(gather_columns)),
          scalars_(scalars),
          externs_(externs),
          exec_(&exec),
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
            const Table t = chunk_to_table(std::move(input));
            auto filtered = filter_project_table(t, *predicate_, gather_columns_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            const bool empty = !filtered->columns.empty() && filtered->rows() == 0;
            auto updated =
                update_table(std::move(filtered.value()), *fields_, scalars_, externs_, *exec_);
            if (!updated.has_value()) {
                return std::unexpected(std::move(updated.error()));
            }
            auto projected = project_table(updated.value(), *project_columns_);
            if (!projected.has_value()) {
                return std::unexpected(std::move(projected.error()));
            }
            // An empty chunk still runs the update and the projection, cheaply,
            // because the schema it has to carry is the one they produce.
            if (empty) {
                if (preserve_empty_morsels_) {
                    return std::optional<Chunk>{
                        table_to_chunk(std::move(projected.value()), identity)};
                }
                schema_.hold(std::move(projected.value()), identity);
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(projected.value()), identity)};
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
        Chunk chunk = std::move(*chunk_res.value());
        for (const auto& spec : *renames_) {
            bool found = false;
            for (auto& col : chunk.columns) {
                if (col.name == spec.old_name) {
                    col.name = spec.new_name;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return std::unexpected("rename: column not found: " + spec.old_name);
            }
        }
        // Rewrite the chunk's order-sensitive metadata through the same shared
        // rule as the serial `rename_table`, so a renamed sort key / time index
        // is relabeled here too rather than left carrying its old column name.
        if (chunk.ordering().has_value() || chunk.time_index().has_value() ||
            !chunk.grouped_by().empty()) {
            auto props = TableProperties::derive(
                TableProperties::recovered(chunk.ordering(), chunk.time_index(),
                                           chunk.grouped_by()),
                [&](const std::string& name) -> KeyFate {
                    for (const auto& spec : *renames_) {
                        if (spec.old_name == name) {
                            return KeyFate::kept(spec.new_name);
                        }
                    }
                    return KeyFate::kept(name);
                },
                RowTransform::Preserve);
            chunk.set_properties(props);
        }
        return std::optional<Chunk>{std::move(chunk)};
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
        Chunk input = std::move(*chunk_res.value());
        const auto identity = chunk_identity_of(input);
        Table t = chunk_to_table(std::move(input));
        auto out = update_table(std::move(t), *fields_, scalars_, externs_, *exec_);
        if (!out.has_value()) {
            return std::unexpected(std::move(out.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(out.value()), identity)};
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
            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != out.columns[i].name) {
                    return std::unexpected("order: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != out.columns[i].column->index()) {
                    return std::unexpected("order: chunk schema mismatch (column type)");
                }
                auto& dst_col = out.mutable_column(i);
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

class ChunkedDistinctOperator final : public Operator {
   public:
    explicit ChunkedDistinctOperator(OperatorPtr child) : child_(std::move(child)) {}

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
            if (t.columns.size() == 1 && !t.columns.front().validity.has_value()) {
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
            if (auto plan = build_packed_key(t); plan.has_value()) {
                std::optional<Table> out =
                    plan->width <= sizeof(std::uint64_t)
                        ? process_packed<std::uint64_t>(std::move(t), plan->cols, seen_packed64_)
                        : process_packed<Packed128>(std::move(t), plan->cols, seen_packed128_);
                if (!out.has_value()) {
                    continue;
                }
                return std::optional<Chunk>{table_to_chunk(std::move(*out))};
            }

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

    // MSVC has no __uint128_t. This is only a packed identity key, so a pair
    // of words is both portable and avoids pulling a compiler-specific integer
    // type into the distinct fast path.
    struct Packed128 {
        std::uint64_t lo = 0;
        std::uint64_t hi = 0;

        [[nodiscard]] friend auto operator==(const Packed128&, const Packed128&) -> bool = default;
    };
    struct Packed128Hash {
        auto operator()(const Packed128& value) const noexcept -> std::size_t {
            auto lo = value.lo;
            const auto hi = value.hi;
            lo ^= hi + 0x9e3779b97f4a7c15ULL + (lo << 6) + (lo >> 2);
            return static_cast<std::size_t>(lo);
        }
    };

    /// One fixed-width integral key column, resolved to its raw storage and the
    /// bit offset it occupies in the packed key.
    struct PackCol {
        enum class Kind : std::uint8_t { Int64, Date, Ts, Bool } kind{Kind::Int64};
        const std::int64_t* i64 = nullptr;
        const Date* date = nullptr;
        const Timestamp* ts = nullptr;
        const Column<bool>* boolean = nullptr;
        unsigned shift = 0;  ///< bit offset of this column's cell in the packed key
    };
    struct PackedPlan {
        std::vector<PackCol> cols;
        unsigned width = 0;  ///< total packed width in bytes
    };

    /// A key is packable iff every column is a fixed-width INTEGRAL type (byte
    /// equality equals value equality — so no double, whose -0.0/NaN break that,
    /// and no categorical, whose code names a different value in a later chunk)
    /// with no nulls, and the columns together fit in 16 bytes.
    static auto build_packed_key(const Table& t) -> std::optional<PackedPlan> {
        PackedPlan plan;
        plan.cols.reserve(t.columns.size());
        unsigned bytes = 0;
        for (const auto& entry : t.columns) {
            if (entry.validity.has_value()) {
                return std::nullopt;
            }
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
            } else {
                return std::nullopt;
            }
            if (bytes > sizeof(Packed128)) {
                return std::nullopt;
            }
            plan.cols.push_back(col);
        }
        plan.width = bytes;
        return plan;
    }

    template <typename Packed, typename Set>
    auto process_packed(Table t, const std::vector<PackCol>& cols, Set& seen)
        -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
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
                }
                if constexpr (std::is_same_v<Packed, Packed128>) {
                    if (col.shift < 64) {
                        key.lo |= cell << col.shift;
                        if (col.shift != 0) {
                            key.hi |= cell >> (64 - col.shift);
                        }
                    } else {
                        key.hi |= cell << (col.shift - 64);
                    }
                } else {
                    key |= static_cast<Packed>(cell) << col.shift;
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

    OperatorPtr child_;
    // Multi-column dedup: `key_index_` hashes and compares each row in place and
    // holds one owned Key per distinct value in `group_order_` (the group-by hot
    // loop's mechanism). `seen_` is the fallback for a column type make_key_col
    // can't resolve — it boxes a Key per row, which is what this replaced.
    KeyRowIndex key_index_;
    std::vector<Key> group_order_;
    robin_hood::unordered_flat_set<std::uint64_t> seen_packed64_;
    robin_hood::unordered_flat_set<Packed128, Packed128Hash> seen_packed128_;
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
};

class ChunkedSemiAntiJoinOperator final : public Operator {
   public:
    ChunkedSemiAntiJoinOperator(OperatorPtr left, Table right, ir::JoinKind kind,
                                const std::vector<ir::JoinKey>* keys)
        : left_(std::move(left)), right_(std::move(right)), kind_(kind), keys_(keys) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
        }

        // Swapped mode: the left side was materialized during `initialize` (the
        // right was too large to set-ify cheaply), and the right-key set now
        // holds only the intersection of the two key columns, so one pass of
        // `filter_chunk` over the whole materialized left produces the result.
        if (swapped_) {
            if (swapped_emitted_) {
                return std::optional<Chunk>{};
            }
            swapped_emitted_ = true;
            auto filtered = filter_chunk(std::move(*left_swapped_));
            left_swapped_.reset();
            if (!filtered.has_value()) {
                return std::optional<Chunk>{};
            }
            return std::optional<Chunk>{table_to_chunk(std::move(*filtered))};
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

    // Build the right-key set as the INTERSECTION of the two key columns, by
    // probing the large right against a map of the small left keys rather than
    // inserting every right key. `filter_chunk` then works unchanged: a left row
    // is in the intersection iff it has a right match (semi keeps those; anti
    // keeps the rest). Restricted to integer keys, which every TPC-H join uses
    // and where the win is; other key types keep the streaming build-on-right.
    auto init_int_swapped(const Column<std::int64_t>& rcol) -> std::optional<std::string> {
        auto left_res = MaterializeOperator(std::move(left_)).run();
        if (!left_res.has_value()) {
            return std::move(left_res.error());
        }
        left_swapped_ = std::move(*left_res);
        swapped_ = true;

        const auto* rentry = right_.find_entry(keys_->front().right);
        const ValidityBitmap* rvalidity =
            rentry != nullptr && rentry->validity.has_value() ? &*rentry->validity : nullptr;
        const auto rnull = [rvalidity](std::size_t row) {
            return rvalidity != nullptr && !(*rvalidity)[row];
        };
        const ColumnValue* lkey = left_swapped_->find(keys_->front().left);
        const auto* lcol = lkey != nullptr ? std::get_if<Column<std::int64_t>>(lkey) : nullptr;
        if (lcol != nullptr && lcol->size() < rcol.size()) {
            // 57k inserts + 3.8M finds, versus 3.8M inserts the other way.
            robin_hood::unordered_flat_map<std::int64_t, char> seen;
            seen.reserve(lcol->size());
            for (const std::int64_t v : *lcol) {
                seen.try_emplace(v, char{0});
            }
            for (std::size_t i = 0; i < rcol.size(); ++i) {
                if (rnull(i)) {
                    continue;  // a null right key puts nothing in the set
                }
                if (auto it = seen.find(rcol[i]); it != seen.end()) {
                    it->second = char{1};
                }
            }
            for (const auto& [k, matched] : seen) {
                if (matched != char{0}) {
                    right_i64_.insert(k);
                }
            }
        } else {
            // Left is not the smaller side (or its key vanished); the plain
            // right set is as good, and the materialized left still emits once.
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

    template <typename Pred>
    auto filter_rows(Table t, Pred pred) -> std::optional<Table> {
        const std::size_t rows = t.rows();
        std::vector<std::size_t> idx;
        idx.reserve(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            if (pred(row)) {
                idx.push_back(row);
            }
        }
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
    bool swapped_emitted_ = false;
    std::optional<Table> left_swapped_;
    ExprType right_kind_ = ExprType::Int;

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
    const ir::Node* cur = &right;
    while (cur->kind() == ir::NodeKind::Project || cur->kind() == ir::NodeKind::Rename ||
           cur->kind() == ir::NodeKind::Update) {
        if (cur->children().size() != 1 || cur->children().front() == nullptr) {
            return {};
        }
        cur = cur->children().front().get();
    }
    if (cur->kind() != ir::NodeKind::Scan) {
        return {};
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    const auto& name = static_cast<const ir::ScanNode&>(*cur).source_name();
    const auto* scan = exec.deferred_scan(name);
    if (scan == nullptr) {
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
                    return std::optional<Chunk>{};
                }
                left_chunk = chunk_to_table(std::move(*chunk_res.value()));
            }
            auto out = probe_chunk_against_right(std::move(left_chunk));
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (out->rows() == 0) {
                continue;
            }
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
        if (keys_->size() != 1) {
            return "ChunkedInnerJoinOperator only supports single-key joins";
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
        struct Hit {
            std::size_t pos;   // index into the candidate selection
            std::size_t head;  // first build row in the chain
        };
        std::vector<Hit> hits;
        hits.reserve(n);
        std::size_t total = 0;
        for (std::size_t i = 0; i < n; ++i) {
            if (key_validity != nullptr && !(*key_validity)[i]) {
                continue;
            }
            const auto it = i64_heads_.find(key_data[i]);
            if (it == i64_heads_.end()) {
                continue;
            }
            hits.push_back(Hit{.pos = i, .head = it->second});
            for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                ++total;
            }
        }

        Selection survivors;
        survivors.reserve(hits.size());
        for (const Hit& hit : hits) {
            survivors.push_back(sel.selected[hit.pos]);
        }

        std::vector<std::size_t> li(total, 0);
        std::vector<std::size_t> ri(total, 0);
        std::size_t pos = 0;
        for (std::size_t h = 0; h < hits.size(); ++h) {
            for (std::size_t cur = hits[h].head; cur != kNil; cur = chain_next_[cur]) {
                li[pos] = cur;
                ri[pos] = h;
                ++pos;
            }
        }
        const bool ri_identity = total == hits.size();

        // Survivors' key values, gathered in memory from phase A's keys.
        ColumnEntry key_entry;
        key_entry.name = sel.keys.name;
        if (hits.size() == n) {
            key_entry.column = sel.keys.column;
            key_entry.validity = sel.keys.validity;
        } else {
            Column<std::int64_t> gathered;
            gathered.reserve(hits.size());
            for (const Hit& hit : hits) {
                gathered.push_back(key_data[hit.pos]);
            }
            key_entry.column = std::make_shared<ColumnValue>(std::move(gathered));
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
        const auto* entry = build.find_entry(keys_->front().left);
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

    // Stream mode: walk the probe side (a left chunk), for each row look
    // up the right-keyed chain and append (li, ri) in probe-scan order.
    // Returns true if every probe row matched exactly once (li == 0..n-1).
    // Only possible when the build side was unique; otherwise falls back
    // to the chained walk.
    template <typename Map, typename GetKey>
    auto probe_scalar(const Map& heads, std::size_t n, GetKey get, std::vector<std::size_t>& li,
                      std::vector<std::size_t>& ri) -> bool {
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

        std::size_t total = 0;

        // In swapped mode the index is on the left, so the right table is the
        // probe side. Its null-keyed rows match nothing (see build_index).
        const auto* right_entry = right_.find_entry(keys_->front().right);
        probe_validity_ = right_entry != nullptr && right_entry->validity.has_value()
                              ? &*right_entry->validity
                              : nullptr;

        struct Hit {
            std::size_t rrow;
            std::size_t head;  // first left row in the chain for this key
        };
        std::vector<Hit> hits;

        auto do_phase1 = [&](auto&& key_at, const auto& heads) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (probe_is_null(r)) {
                    continue;
                }
                auto it = heads.find(key_at(r));
                if (it == heads.end()) {
                    continue;
                }
                hits.push_back(Hit{r, it->second});
                for (std::size_t cur = it->second; cur != kNil; cur = chain_next_[cur]) {
                    ++total;
                }
            }
        };

        // Same shape with the chain head already resolved — see
        // `resolve_categorical_heads`.
        auto do_phase1_resolved = [&](auto&& head_at) {
            for (std::size_t r = 0; r < n_right; ++r) {
                if (probe_is_null(r)) {
                    continue;
                }
                const std::size_t head = head_at(r);
                if (head == kNil) {
                    continue;
                }
                hits.push_back(Hit{.rrow = r, .head = head});
                for (std::size_t cur = head; cur != kNil; cur = chain_next_[cur]) {
                    ++total;
                }
            }
        };

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

        // Phase 2: replay the recorded hits in the same order Phase 1 visited
        // them — right-scan (probe) order. Row order is outside the join
        // contract (SPEC.md §5.6), so there's no correctness reason to
        // reassemble by left row instead; doing so was actively harmful,
        // permuting the output away from the probe side's natural scan
        // order and hurting cache locality on any downstream join that
        // probes this join's output.
        std::vector<std::size_t> li(total, 0);
        std::vector<std::size_t> ri(total, 0);
        std::size_t pos = 0;
        for (const Hit& hit : hits) {
            for (std::size_t cur = hit.head; cur != kNil; cur = chain_next_[cur]) {
                li[pos] = cur;
                ri[pos] = hit.rrow;
                ++pos;
            }
        }

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
        if (total == 0) {
            return output;
        }
        if (!right_emit_ready_) {
            if (auto ready = setup_right_emit_schema(left_side); !ready.has_value()) {
                return std::unexpected(std::move(ready.error()));
            }
        }
        output.columns.reserve(left_side.columns.size() + right_emit_idx_.size());

        auto gather_with_validity =
            [&](const ColumnValue& src_col, const std::optional<ValidityBitmap>& src_val,
                const std::size_t* idx) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
            ColumnValue gathered = gather_column(src_col, idx, total, exec_);
            std::optional<ValidityBitmap> val;
            if (src_val.has_value()) {
                const auto& src_bm = *src_val;
                ValidityBitmap dst(total, false);
                for (std::size_t i = 0; i < total; ++i) {
                    dst.set(i, src_bm[idx[i]]);
                }
                val = std::move(dst);
            }
            return {std::move(gathered), std::move(val)};
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
            for (std::size_t i = 0; i < left_side.columns.size(); ++i) {
                const auto& lc = left_side.columns[i];
                auto [gathered, val] = gather_with_validity(*lc.column, lc.validity, li);
                if (val.has_value()) {
                    output.add_column(left_name(i, lc), std::move(gathered), std::move(*val));
                } else {
                    output.add_column(left_name(i, lc), std::move(gathered));
                }
            }
        }

        // ri_identity: every emitted row consumes the next probe-side row
        // exactly once (two-phase deferred probe with a unique build side),
        // so probe columns are shared rather than gathered — the same
        // reasoning as li_identity above.
        const bool share_right = ri_identity && total == right_.rows();
        for (std::size_t e = 0; e < right_emit_idx_.size(); ++e) {
            const auto& rc = right_.columns[right_emit_idx_[e]];
            std::string name = right_emit_names_[e];
            if (share_right) {
                output.add_column_from(std::move(name), rc);
                continue;
            }
            auto [gathered, val] = gather_with_validity(*rc.column, rc.validity, ri);
            if (val.has_value()) {
                output.add_column(std::move(name), std::move(gathered), std::move(*val));
            } else {
                output.add_column(std::move(name), std::move(gathered));
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
                if (plan_[i].func == ir::AggFunc::Skew || plan_[i].func == ir::AggFunc::Kurtosis) {
                    plan_[i].scratch_doubles = 2;
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
            } else if (group_entries.size() == 2 && !group_entries[0]->validity.has_value() &&
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
                [&](const std::string& key, std::uint32_t gid) { str_order_[gid] = key; })) {
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
                [&](std::int64_t key, std::uint32_t gid) { int_order_[gid] = key; })) {
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
            const auto pack_u64 = [](std::int64_t a, std::int64_t b) -> std::int64_t {
                return static_cast<std::int64_t>(
                    (static_cast<std::uint64_t>(static_cast<std::uint32_t>(a)) << 32U) |
                    static_cast<std::uint64_t>(static_cast<std::uint32_t>(b)));
            };
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

        if (try_discover_partitioned<PairIntKey, PairIntKeyHash>(
                [&](std::size_t row) { return pack(key_a_at(row), key_b_at(row)); }, rows, gids,
                pair_partitions_, [&](std::size_t n) { pair_order_.resize(n); },
                [&](const PairIntKey& key, std::uint32_t gid) {
                    pair_order_[gid] = {static_cast<std::int64_t>(key.first),
                                        static_cast<std::int64_t>(key.second)};
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
    template <typename Key, typename Hash, typename Eq = std::equal_to<Key>, typename KeyAt,
              typename ResizeKeys, typename StoreKey>
    auto try_discover_partitioned(const KeyAt& key_at, std::size_t rows, std::uint32_t* gids,
                                  std::vector<KeyPartition<Key, Hash, Eq>>& partitions,
                                  const ResizeKeys& resize_keys, const StoreKey& store_key)
        -> bool {
        // Below this the partition and scatter passes cost more than the serial
        // probe they replace. High cardinality is not checkable up front — it is
        // what discovery is about to find out — so row count is the only gate
        // available, and a low-cardinality run of this size still wins from the
        // smaller per-partition tables.
        //
        // Once this path HAS run, every later chunk must take it too, however
        // small. The groups it discovered live in `partitions`, and the serial
        // loops probe `int_index_` / `str_index_`, which this path never
        // populates — so a small trailing chunk falling back would not find the
        // existing groups and would allocate second ids for them. The row gate
        // therefore only guards the first use.
        constexpr std::size_t kMinRows = 1U << 18U;
        if (exec_ == nullptr || !exec_->parallel || on_worker_pool_thread() ||
            (rows < kMinRows && !partitioned_active_)) {
            return false;
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
                store_key(partition.keys[i], partition.gids[i]);
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
        flat_slots_.resize(n_groups_ * n_aggs_);
        if (scratch_stride_ != 0) {
            scratch_.resize(n_groups_ * scratch_stride_, 0.0);
        }
    }

    auto alloc_group() -> std::uint32_t {
        auto gid = static_cast<std::uint32_t>(n_groups_);
        ++n_groups_;
        size_group_arrays();
        return gid;
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
            const auto value = std::hash<std::int64_t>{}(static_cast<std::int64_t>(codes[i]));
            seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
        }
        return seed;
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
        const std::size_t per_morsel_bytes = n_groups_ * n_aggs_ * sizeof(AggSlotCore);
        if (per_morsel_bytes == 0 || per_morsel_bytes > kPartialBudgetBytes) {
            return false;
        }
        std::size_t morsels = std::clamp<std::size_t>(rows / kMinRowsPerMorsel, 1, kMaxMorsels);
        morsels = std::min(morsels, kPartialBudgetBytes / per_morsel_bytes);
        if (morsels < 2 || morsels * n_groups_ > rows / kMergeToScanRatio) {
            return false;
        }

        const std::size_t stride = n_groups_ * n_aggs_;
        const std::size_t grain = (rows + morsels - 1) / morsels;
        std::vector<AggSlotCore> partials(morsels * stride);

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
                accumulate_columns_into(gids, agg_entries, begin, end, &partials[m * stride]);
            }
        });
        batch.wait();

        for (std::size_t m = 0; m < morsels; ++m) {
            const AggSlotCore* src = &partials[m * stride];
            for (std::size_t g = 0; g < n_groups_; ++g) {
                AggSlotCore* dst = &flat_slots_[g * n_aggs_];
                for (std::size_t a = 0; a < n_aggs_; ++a) {
                    agg_combine(dst[a], src[(g * n_aggs_) + a], plan_[a].func, plan_[a].kind);
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
            accumulate_columns_into(gids, agg_entries, 0, rows, flat_slots_.data());
        }
    }

    // NOLINTBEGIN(cppcoreguidelines-pro-type-union-access)
    // We need AggSlotCore to be a POD

    /// Scatter-accumulate rows [begin, end) into `base`, indexed by
    /// `gids[row] * n_aggs_ + agg_i`. `base` is the caller's slot array —
    /// `flat_slots_` for the serial path, a worker-private array for the
    /// parallel one — and `GidT` covers both assigned gids (uint32_t) and raw
    /// Categorical codes (int32_t), which are already dense indices.
    template <typename GidT>
    void accumulate_columns_into(const GidT* gids,
                                 const std::vector<const ColumnEntry*>& agg_entries,
                                 std::size_t begin, std::size_t end, AggSlotCore* base) {
        AggSlotCore* fs = base;
        const std::size_t rows = end;
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            // Takes GidT so a signed Categorical code indexes without an
            // implicit narrowing conversion at each of the ~19 call sites.
            const auto slot_for = [&](GidT g) -> AggSlotCore& {
                return fs[(static_cast<std::size_t>(g) * n_aggs_) + agg_i];
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
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += data[row];
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.has_value ? std::min(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.has_value ? std::max(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), data[row]);
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            double* scr = scratch_for(static_cast<std::size_t>(gids[row]), agg_i);
                            agg_update_moments(slot_for(gids[row]), scr[0], scr[1], data[row]);
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            if (!slot.has_value) {
                                slot.double_value = data[row];
                                slot.has_value = true;
                            }
                        }
                        break;
                    case ir::AggFunc::Last:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value = data[row];
                            slot.has_value = true;
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
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += static_cast<double>(data[row]);
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::min(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::max(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            double* scr = scratch_for(static_cast<std::size_t>(gids[row]), agg_i);
                            agg_update_moments(slot_for(gids[row]), scr[0], scr[1],
                                               static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            if (!slot.has_value) {
                                slot.int_value = data[row];
                                slot.has_value = true;
                            }
                        }
                        break;
                    case ir::AggFunc::Last:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.int_value = data[row];
                            slot.has_value = true;
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
                        if (!slot.has_value) {
                            text_at((static_cast<std::size_t>(gids[row]) * n_aggs_) + agg_i) =
                                value_at(row);
                            slot.has_value = true;
                        }
                    }
                } else {
                    for (std::size_t row = begin; row < rows; ++row) {
                        if (has_nulls && !(*validity)[row])
                            continue;
                        auto& slot = slot_for(gids[row]);
                        text_at((static_cast<std::size_t>(gids[row]) * n_aggs_) + agg_i) =
                            value_at(row);
                        slot.has_value = true;
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
        const std::size_t per_morsel_bytes = dict_size * n_aggs_ * sizeof(AggSlotCore);
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
                accumulate_columns_into(codes, agg_entries, begin, end,
                                        &partials[m * dict_size * n_aggs_]);
            }
        });
        batch.wait();

        if (cat_dense_gid_.size() < dict_size) {
            cat_dense_gid_.resize(dict_size, kNoGid);
        }
        for (std::size_t m = 0; m < morsels; ++m) {
            const AggSlotCore* src = &partials[m * dict_size * n_aggs_];
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
                    agg_combine(dst[a], src[(idx * n_aggs_) + a], plan_[a].func, plan_[a].kind);
                }
            }
        }
        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    void accumulate_ungrouped_range(const std::vector<const ColumnEntry*>& agg_entries,
                                    std::size_t begin, std::size_t end, AggSlotCore* slots) {
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
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Mean:
                        each([&](std::size_t r) {
                            slot.double_value += data[r];
                            slot.count++;
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Min:
                        each([&](std::size_t r) {
                            slot.double_value =
                                slot.has_value ? std::min(slot.double_value, data[r]) : data[r];
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Max:
                        each([&](std::size_t r) {
                            slot.double_value =
                                slot.has_value ? std::max(slot.double_value, data[r]) : data[r];
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Stddev:
                        each([&](std::size_t r) {
                            agg_update_stddev(slot, data[r]);
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        each([&](std::size_t r) {
                            double* scr = scratch_for(0, agg_i);
                            agg_update_moments(slot, scr[0], scr[1], data[r]);
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::First:
                        each([&](std::size_t r) {
                            if (!slot.has_value) {
                                slot.double_value = data[r];
                                slot.has_value = true;
                            }
                        });
                        break;
                    case ir::AggFunc::Last:
                        each([&](std::size_t r) {
                            slot.double_value = data[r];
                            slot.has_value = true;
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
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Mean:
                        each([&](std::size_t r) {
                            slot.double_value += static_cast<double>(data[r]);
                            slot.count++;
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Min:
                        each([&](std::size_t r) {
                            slot.int_value =
                                slot.has_value ? std::min(slot.int_value, data[r]) : data[r];
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Max:
                        each([&](std::size_t r) {
                            slot.int_value =
                                slot.has_value ? std::max(slot.int_value, data[r]) : data[r];
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Stddev:
                        each([&](std::size_t r) {
                            agg_update_stddev(slot, static_cast<double>(data[r]));
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        each([&](std::size_t r) {
                            double* scr = scratch_for(0, agg_i);
                            agg_update_moments(slot, scr[0], scr[1], static_cast<double>(data[r]));
                            slot.has_value = true;
                        });
                        break;
                    case ir::AggFunc::First:
                        each([&](std::size_t r) {
                            if (!slot.has_value) {
                                slot.int_value = data[r];
                                slot.has_value = true;
                            }
                        });
                        break;
                    case ir::AggFunc::Last:
                        each([&](std::size_t r) {
                            slot.int_value = data[r];
                            slot.has_value = true;
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
                        if (!slot.has_value) {
                            text_at(agg_i) = value_at(r);
                            slot.has_value = true;
                        }
                    });
                } else {
                    each([&](std::size_t r) {
                        text_at(agg_i) = value_at(r);
                        slot.has_value = true;
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
            accumulate_ungrouped_range(agg_entries, 0, rows, dst);
            return std::nullopt;
        }

        // One private slot array per morsel, each written by exactly one
        // worker. Merging them in ascending morsel order — never completion
        // order — is what keeps First/Last correct and the float reduction
        // reproducible run to run.
        const std::size_t grain = (rows + morsels - 1) / morsels;
        std::vector<AggSlotCore> partials(morsels * n_aggs_);

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
                    accumulate_ungrouped_range(agg_entries, begin, end, &partials[(m * n_aggs_)]);
                }
            }
        });
        batch.wait();

        if (exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        for (std::size_t m = 0; m < morsels; ++m) {
            for (std::size_t a = 0; a < n_aggs_; ++a) {
                agg_combine(dst[a], partials[(m * n_aggs_) + a], plan_[a].func, plan_[a].kind);
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
        for (std::size_t out_row = 0; out_row < n_groups_; ++out_row) {
            const std::size_t g = out_row;
            if (cat_fast_path_) {
                const bool single_key = group_by_->size() == 1;
                if (single_key) {
                    auto& cat_col = std::get<Column<Categorical>>(out.mutable_column(0));
                    cat_col.push_code(cat_order_[g]);
                } else {
                    const std::size_t n_keys = group_by_->size();
                    for (std::size_t ci = 0; ci < n_keys; ++ci) {
                        auto& cat_col = std::get<Column<Categorical>>(out.mutable_column(ci));
                        cat_col.push_code(multi_cat_codes_flat_[(g * n_keys) + ci]);
                    }
                }
            } else if (str_fast_path_) {
                auto& str_col = std::get<Column<std::string>>(out.mutable_column(0));
                str_col.push_back(str_order_[g]);
            } else if (int_fast_path_) {
                push_int_key(out.mutable_column(0), int_key_kind_, int_order_[g]);
            } else if (pair_int_fast_path_) {
                push_int_key(out.mutable_column(0), int_key_kind_, pair_order_[g].first);
                push_int_key(out.mutable_column(1), int_key_kind_b_, pair_order_[g].second);
            } else {
                const Key& key = group_order_[g];
                for (std::size_t ci = 0; ci < key.values.size(); ++ci) {
                    append_scalar(out.mutable_column(ci), key.values[ci]);
                    if (any_null_keys != 0 && ci < kMaxKeyColumns &&
                        (key.null_mask & (std::uint64_t{1} << ci)) != 0) {
                        key_validity[ci].set(out_row, false);
                    }
                }
            }
            for (std::size_t i = 0; i < aggregations_->size(); ++i) {
                auto& column = out.mutable_column(group_by_->size() + i);
                const AggSlotCore& slot = fs[(g * n_aggs_) + i];
                if (track_validity[i] != 0U) {
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
                        append_scalar(column, agg_finalize_stddev(slot));
                        break;
                    case ir::AggFunc::Skew:
                        append_scalar(column, agg_finalize_skew(slot, scratch_for(g, i)[0]));
                        break;
                    case ir::AggFunc::Kurtosis:
                        append_scalar(column, agg_finalize_kurtosis(slot, scratch_for(g, i)[1]));
                        break;
                    case ir::AggFunc::First:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else if (plan_[i].kind == ExprType::Int) {
                            append_scalar(column, slot.int_value);
                        } else {
                            append_scalar(column, text_store_[(g * n_aggs_) + i]);
                        }
                        break;
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
    std::vector<AggSlotCore> flat_slots_;

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
    /// Set once `try_discover_partitioned` has run; see the gate there for why
    /// a later chunk may then never fall back to the serial loop.
    bool partitioned_active_ = false;
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
        cur_scratch_.assign(n_aggs_ * 2, 0.0);
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
                accumulate_typed(slot, &cur_scratch_[i * 2], plan_[i].func, data, entry, has_nulls,
                                 start, end);
            } else {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                accumulate_typed(slot, &cur_scratch_[i * 2], plan_[i].func, data, entry, has_nulls,
                                 start, end);
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
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Mean:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    slot.double_value += static_cast<double>(data[row]);
                    slot.count++;
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Min:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.has_value ? std::min(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.has_value ? std::min(slot.int_value, data[row]) : data[row];
                    }
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Max:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value =
                            slot.has_value ? std::max(slot.double_value, data[row]) : data[row];
                    } else {
                        slot.int_value =
                            slot.has_value ? std::max(slot.int_value, data[row]) : data[row];
                    }
                    slot.has_value = true;
                }
                break;
            case ir::AggFunc::Stddev:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_stddev(slot, static_cast<double>(data[row]));
                }
                break;
            case ir::AggFunc::Skew:
            case ir::AggFunc::Kurtosis:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row)) {
                        continue;
                    }
                    agg_update_moments(slot, scratch[0], scratch[1],
                                       static_cast<double>(data[row]));
                }
                break;
            case ir::AggFunc::First:
                for (std::size_t row = start; row < end; ++row) {
                    if (!valid(row) || slot.has_value) {
                        continue;
                    }
                    if constexpr (std::is_same_v<T, double>) {
                        slot.double_value = data[row];
                    } else {
                        slot.int_value = data[row];
                    }
                    slot.has_value = true;
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
                    slot.has_value = true;
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
                    append_scalar(column, ScalarValue{agg_finalize_stddev(slot)});
                    break;
                case ir::AggFunc::Skew:
                    append_scalar(column,
                                  ScalarValue{agg_finalize_skew(slot, cur_scratch_[i * 2])});
                    break;
                case ir::AggFunc::Kurtosis:
                    append_scalar(column, ScalarValue{agg_finalize_kurtosis(
                                              slot, cur_scratch_[(i * 2) + 1])});
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

namespace {

template <typename Fn>

auto build_unary_materializing_operator(const ir::Node& child_node, const TableRegistry& registry,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec,
                                        ModelResult* model_out, Fn fn)
    -> std::expected<OperatorPtr, std::string> {
    auto child_op = build_operator(child_node, registry, scalars, externs, exec, model_out);
    if (!child_op.has_value()) {
        return std::unexpected(std::move(child_op.error()));
    }
    auto materialized = materialize_operator(std::move(child_op.value()));
    if (!materialized.has_value()) {
        return std::unexpected(std::move(materialized.error()));
    }
    auto result = fn(std::move(materialized.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(result.value()));
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
    auto left_op = build_operator(left_node, registry, scalars, externs, exec, model_out);
    if (!left_op.has_value()) {
        return std::unexpected(std::move(left_op.error()));
    }
    auto right_op = build_operator(right_node, registry, scalars, externs, exec, model_out);
    if (!right_op.has_value()) {
        return std::unexpected(std::move(right_op.error()));
    }
    auto left = materialize_operator(std::move(left_op.value()));
    if (!left.has_value()) {
        return std::unexpected(std::move(left.error()));
    }
    auto right = materialize_operator(std::move(right_op.value()));
    if (!right.has_value()) {
        return std::unexpected(std::move(right.error()));
    }
    auto result = fn(std::move(left.value()), std::move(right.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(result.value()));
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
            ibex::formatting::print(stderr,
                                    "island stats: parallel={} serial={} morsels={} range_heads={} "
                                    "two_phase={} parallel_fields={}\n",
                                    stats.parallel_islands.load(), stats.serial_islands.load(),
                                    stats.morsels.load(), stats.range_heads.load(),
                                    stats.two_phase_filters.load(), stats.parallel_fields.load());
        }
    };
    static const Reporter reporter;
    return enabled ? &stats : nullptr;
}

void configure_parallel_from_env(ExecutionContext& exec) {
    if (const auto want = parallel_enabled_from_env(); want.has_value()) {
        exec.parallel = *want;
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
            exec.parallel_threads != 0 ? exec.parallel_threads : default_thread_count();
        exec.execution_profile = std::make_shared<ExecutionProfileState>(budget);
    }
    if (const std::size_t grain = morsel_rows_from_env(); grain > 0) {
        exec.parallel_grain = grain;
        // An explicit grain is an explicit request to partition at that size,
        // so it also lowers the serial threshold — otherwise the default
        // threshold would silently override the knob it was asked to honor.
        exec.parallel_min_rows = std::min(exec.parallel_min_rows, grain);
    }
    // `parallel_threads` is deliberately left alone. `IBEX_THREADS` already
    // sizes the process pool, and a zero budget means "use the pool", so the
    // environment reaches the island either way — while a caller that set its
    // own budget before calling this keeps it.
}

// One construction point for every row-local map operator that can live in a
// parallel island. The serial planner uses the same factory: only the island
// asks maps to retain zero-row morsels, because its ordered merger needs one
// output identity for every input morsel. Keeping the construction (especially
// FUP's gather set) here prevents the two planners from drifting as range-aware
// kernels replace these chunked implementations.
auto build_row_local_map_operator(const ir::Node& node, OperatorPtr child,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    switch (node.kind()) {
        case ir::NodeKind::Filter: {
            const auto& filter = static_cast<const ir::FilterNode&>(node);
            return std::make_unique<ChunkedFilterOperator>(std::move(child), &filter.predicate(),
                                                           scalars, preserve_empty_morsels);
        }
        case ir::NodeKind::Project: {
            const auto& project = static_cast<const ir::ProjectNode&>(node);
            return std::make_unique<ChunkedProjectOperator>(std::move(child), &project.columns());
        }
        case ir::NodeKind::Rename: {
            const auto& rename = static_cast<const ir::RenameNode&>(node);
            return std::make_unique<ChunkedRenameOperator>(std::move(child), &rename.renames());
        }
        case ir::NodeKind::Update: {
            // Only reachable for an update `execution_capability(const Node&)`
            // proved row-local: unguarded, ungrouped, scalar-only fields, no
            // tuple assignment. An update never drops rows, so it needs no
            // empty-morsel handling to stay 1:1.
            const auto& update = static_cast<const ir::UpdateNode&>(node);
            return std::make_unique<ChunkedUpdateOperator>(std::move(child), &update.fields(),
                                                           scalars, externs, exec);
        }
        case ir::NodeKind::FilterProject: {
            const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
            return std::make_unique<ChunkedFilterProjectOperator>(
                std::move(child), &fp.predicate(), &fp.columns(), scalars, preserve_empty_morsels);
        }
        case ir::NodeKind::FilterUpdateProject: {
            const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
            robin_hood::unordered_set<std::string> update_outputs;
            robin_hood::unordered_set<std::string> needed;
            for (const auto& field : fup.fields()) {
                update_outputs.insert(field.alias);
                collect_expr_column_refs(field.expr, needed);
            }
            for (const auto& column : fup.project_columns()) {
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
                std::move(child), &fup.predicate(), &fup.fields(), &fup.project_columns(),
                std::move(gather_columns), scalars, externs, exec, preserve_empty_morsels);
        }
        default:
            return std::unexpected("row-local map factory: unsupported operator kind");
    }
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
[[nodiscard]] auto range_filter_head(const ir::Node& node, const Table& input)
    -> std::optional<RangeHead> {
    if (input.columns.empty()) {
        return std::nullopt;
    }
    if (node.kind() == ir::NodeKind::Filter) {
        const auto& predicate = static_cast<const ir::FilterNode&>(node).predicate();
        if (!is_range_native_expr(predicate)) {
            return std::nullopt;
        }
        return RangeHead{.predicate = &predicate};
    }
    if (node.kind() == ir::NodeKind::FilterProject) {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
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

[[nodiscard]] auto build_island_worker_chain(const std::vector<const ir::Node*>& operators,
                                             const Table& input, const ScalarRegistry* scalars,
                                             const ExternRegistry* externs,
                                             const ExecutionContext& exec)
    -> std::expected<IslandWorkerChain, std::string> {
    // A qualifying head is absorbed into the source rather than built as an
    // operator above it — same output, without materializing the morsel first.
    std::size_t first_op = 0;
    std::unique_ptr<MorselSource> source;
    if (!operators.empty()) {
        if (auto head = range_filter_head(*operators.front(), input); head.has_value()) {
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
        const ir::Node* op_node = operators[i];
        // `preserve_empty_morsels` is what makes one input morsel yield exactly
        // one identified output morsel — the merger indexes by sequence, so a
        // silently coalesced empty result would be a lost slot, not a smaller
        // answer.
        auto next = build_row_local_map_operator(*op_node, std::move(worker.chain), scalars,
                                                 externs, exec, true);
        if (!next.has_value()) {
            // analyze_parallel_island() only admits row-local map kinds.
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
            ready_.wait(lock, [&] {
                return ring_ready_[slot] || cancelled_ || active_workers_ == 0 ||
                       (has_error_ && error_sequence_ <= next_sequence_);
            });
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
    if (morsel_count < 2) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t workers =
        std::min({budget, pool_size, static_cast<std::size_t>(morsel_count)});
    return workers < 2 ? 0 : workers;
}

// Build one eligible row-local parallel-map chain as an island: materialize its
// input subtree once, then run the chain over morsels of that table instead of
// one whole-table chunk. `candidate.operators` is source-to-sink.
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
auto build_parallel_island(const ParallelIslandCandidate& candidate, const TableRegistry& registry,
                           const ScalarRegistry* scalars, const ExternRegistry* externs,
                           const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    auto input_op = build_operator(*candidate.input, registry, scalars, externs, exec, model_out);
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
        const auto head = candidate.operators.empty()
                              ? std::nullopt
                              : range_filter_head(*candidate.operators.front(), *owned);
        if (exec.parallel_stats != nullptr && head.has_value()) {
            exec.parallel_stats->range_heads.fetch_add(1, std::memory_order_relaxed);
        }

        // A range-native filter can skip the merger entirely by presizing its
        // output — see TwoPhaseFilterOperator. Anything above it must be
        // metadata-only: a row-touching operator would need the per-morsel
        // chunks the two-phase form does not produce, but Project and Rename
        // copy no rows and so are simply run once over the finished output.
        const auto tail = std::span{candidate.operators}.subspan(head.has_value() ? 1 : 0);
        if (head.has_value() && std::ranges::all_of(tail, [](const ir::Node* node) {
                return is_metadata_only_node(node->kind());
            })) {
            auto layout = build_filter_output_layout(*owned, head->project);
            // A missing projected column is left to the ordered merger below,
            // which reports it through the normal evaluation path.
            if (layout.has_value() && filter_gather_is_thread_safe(*owned, layout->src_of_dst)) {
                if (exec.parallel_stats != nullptr) {
                    exec.parallel_stats->two_phase_filters.fetch_add(1, std::memory_order_relaxed);
                }
                return std::make_unique<TwoPhaseFilterOperator>(
                    std::move(owned), *head->predicate, head->project != nullptr,
                    std::vector<const ir::Node*>(tail.begin(), tail.end()), scalars,
                    std::move(layout.value()), grain, expected_morsels, worker_count,
                    process_worker_pool());
            }
        }

        std::vector<IslandWorkerChain> workers;
        workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            auto worker =
                build_island_worker_chain(candidate.operators, *owned, scalars, externs, exec);
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
        OperatorPtr serial = std::make_unique<TableSourceOperator>(std::move(*owned));
        for (const ir::Node* op_node : candidate.operators) {
            auto next = build_row_local_map_operator(*op_node, std::move(serial), scalars, externs,
                                                     exec, false);
            if (!next.has_value()) {
                return std::unexpected("parallel island: " + next.error());
            }
            serial = std::move(next.value());
        }
        return serial;
    }

    OperatorPtr chain = std::make_unique<PartitionedTableSource>(*owned, grain);
    for (const ir::Node* op_node : candidate.operators) {
        auto next =
            build_row_local_map_operator(*op_node, std::move(chain), scalars, externs, exec, true);
        if (!next.has_value()) {
            // analyze_parallel_island() only admits row-local map kinds.
            return std::unexpected("parallel island: " + next.error());
        }
        chain = std::move(next.value());
    }

    chain = std::make_unique<SerialIslandOrderValidator>(std::move(chain), expected_morsels, grain);
    return std::make_unique<OwningIslandOperator>(std::move(owned), std::move(chain));
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
    if (exec.parallel) {
        const auto island = analyze_parallel_island(node);
        if (island.eligible()) {
            return build_parallel_island(island, registry, scalars, externs, exec, model_out);
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
        return std::make_unique<ChunkedDistinctOperator>(std::move(child_op.value()));
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
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, exec, model_out);
            if (!right_op.has_value()) {
                return std::unexpected(std::move(right_op.error()));
            }
            auto right = materialize_operator(std::move(right_op.value()));
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return std::make_unique<ChunkedSemiAntiJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), join.kind(), &join.keys());
        }
        // `nulls equal` goes to the materialized join, which implements the
        // policy. These streaming operators hash and probe on their own and
        // would each need the same null tagging; sending the opt-in case to the
        // one implementation that has it keeps a single definition of the
        // semantics -- and leaves this hot path bit-for-bit unchanged for every
        // join that does not ask for it.
        const bool streamable_inner =
            join.kind() == ir::JoinKind::Inner && !join.predicate().has_value() &&
            join.keys().size() == 1 && join.null_match() == ir::NullMatch::Never &&
            !join.expect().asserts_anything() && join.take() == ir::MatchSelection::All;
        if (streamable_inner) {
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            // A deferred probe scan must not be interpreted here — the join
            // publishes build-side bounds into its filter slot first, then
            // interprets the right subtree itself (resolve_deferred_probe).
            if (const auto probe = deferred_probe_scan_of(*join.children()[1], exec);
                probe.scan != nullptr) {
                return std::make_unique<ChunkedInnerJoinOperator>(
                    std::move(left_op.value()), join.children()[1].get(), &registry, scalars,
                    externs, exec, &join.keys(), probe.scan, *probe.name, join.suffix(),
                    &join.pending_order());
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, exec, model_out);
            if (!right_op.has_value()) {
                return std::unexpected(std::move(right_op.error()));
            }
            auto right = materialize_operator(std::move(right_op.value()));
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return std::make_unique<ChunkedInnerJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), &join.keys(), exec,
                join.suffix(), &join.pending_order());
        }
        const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
        return build_binary_materializing_operator(
            *join.children()[0], *join.children()[1], registry, scalars, externs, exec, model_out,
            [&](Table left, Table right) {
                return join_table_impl(left, right, join.kind(), join.keys(), pred, scalars,
                                       compute_mask, join.suffix(), join.pending_order(),
                                       join.null_match(), join.expect(), join.take());
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
        return std::make_unique<TableSourceOperator>(std::move(result.value()));
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
        return std::make_unique<TableSourceOperator>(std::move(result.value()));
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
        return std::make_unique<TableSourceOperator>(std::move(primary));
    }

    if (node.kind() == ir::NodeKind::Construct || node.kind() == ir::NodeKind::Stream) {
        auto table = interpret_node(node, registry, scalars, externs, exec, model_out);
        if (!table.has_value()) {
            return std::unexpected(std::move(table.error()));
        }
        return std::make_unique<TableSourceOperator>(std::move(table.value()));
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
    return std::make_unique<TableSourceOperator>(std::move(table.value()));
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
