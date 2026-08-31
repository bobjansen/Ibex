// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// map_chunked.cpp — the row-local streaming map operators (filter / project /
// rename / row-local update and their fused forms, plus the small head /
// filter-head / filter-tail limit operators that share the same per-chunk
// machinery), the map-step kernel factories, and the physical-plan construction
// sites that build them (`build_physical_map_step`, `build_physical_head`,
// `build_physical_filter_head_tail`). Split out of chunked.cpp; the operator
// classes are fully private to this translation unit.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "physical_plan.hpp"

#include "chunk_conversion_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "physical_executor_internal.hpp"
#include "pipeline_executor_internal.hpp"

namespace ibex::runtime {

namespace {

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

/// Per-chunk filter: pulls a chunk from the child, wraps it as a `Table`,
/// reuses the existing `filter_table` predicate evaluator, and emits the
/// filtered columns as the next chunk. Chunks that filter to zero rows
/// are skipped — the operator loops until it has a non-empty chunk or
/// the child stream ends.
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
        return pipeline_executor_detail::build_map_pipeline_parallel(plan, registry, scalars,
                                                                     externs, exec, model_out);
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

}  // namespace physical_executor_detail

}  // namespace ibex::runtime
