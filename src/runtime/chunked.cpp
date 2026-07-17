// chunked.cpp — streaming (chunked) operator pipeline: per-chunk operators,
// rank evaluation, extern-call execution, and build_operator plan construction.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <expected>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

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
    t.ordering = std::move(chunk.ordering);
    t.time_index = std::move(chunk.time_index);
    if (t.columns.empty()) {  // logical_rows is only meaningful when column-less
        t.logical_rows = chunk.logical_rows;
    }
    return t;
}

auto table_to_chunk(Table table) -> Chunk {
    Chunk c;
    c.columns = std::move(table.columns);
    c.ordering = std::move(table.ordering);
    c.time_index = std::move(table.time_index);
    if (c.columns.empty()) {  // logical_rows is only meaningful when column-less
        c.logical_rows = table.logical_rows;
    }
    return c;
}

// Whether a streamed aggregate slot has enough observations to be non-null.
// Mirrors the materializing aggregate's `agg_result_is_valid`.
auto chunked_agg_valid(ir::AggFunc func, const AggSlot& slot) -> bool {
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
    void hold(Table&& empty) {
        if (!held_.has_value() && !empty.columns.empty()) {
            held_ = std::move(empty);
        }
    }
    void emitted() { emitted_ = true; }
    /// The held chunk — once, and only if nothing else was ever emitted.
    [[nodiscard]] auto release() -> std::optional<Chunk> {
        if (emitted_ || !held_.has_value()) {
            return std::nullopt;
        }
        emitted_ = true;
        return table_to_chunk(std::move(*held_));
    }

   private:
    std::optional<Table> held_;
    bool emitted_ = false;
};

class ChunkedFilterOperator final : public Operator {
   public:
    ChunkedFilterOperator(OperatorPtr child, const ir::Expr* predicate,
                          const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return schema_.release();
            }
            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_table(t, *predicate_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            if (!filtered->columns.empty() && filtered->rows() == 0) {
                schema_.hold(std::move(filtered.value()));
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(filtered.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const ScalarRegistry* scalars_;
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
        const Table t = chunk_to_table(std::move(*chunk_res.value()));
        auto projected = project_table(t, *columns_);
        if (!projected.has_value()) {
            return std::unexpected(std::move(projected.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(projected.value()))};
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
                                 const ScalarRegistry* scalars)
        : child_(std::move(child)), predicate_(predicate), columns_(columns), scalars_(scalars) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return schema_.release();
            }
            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto out = filter_project_table(t, *predicate_, *columns_, scalars_);
            if (!out.has_value()) {
                return std::unexpected(std::move(out.error()));
            }
            if (!out->columns.empty() && out->rows() == 0) {
                schema_.hold(std::move(out.value()));
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
        }
    }

   private:
    OperatorPtr child_;
    const ir::Expr* predicate_;
    const std::vector<ir::ColumnRef>* columns_;
    const ScalarRegistry* scalars_;
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
                                       const ScalarRegistry* scalars, const ExternRegistry* externs)
        : child_(std::move(child)),
          predicate_(predicate),
          fields_(fields),
          project_columns_(project_columns),
          gather_columns_(std::move(gather_columns)),
          scalars_(scalars),
          externs_(externs) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                return std::optional<Chunk>{};
            }
            const Table t = chunk_to_table(std::move(*chunk_res.value()));
            auto filtered = filter_project_table(t, *predicate_, gather_columns_, scalars_);
            if (!filtered.has_value()) {
                return std::unexpected(std::move(filtered.error()));
            }
            const bool empty = !filtered->columns.empty() && filtered->rows() == 0;
            auto updated = update_table(std::move(filtered.value()), *fields_, scalars_, externs_);
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
                schema_.hold(std::move(projected.value()));
                continue;
            }
            schema_.emitted();
            return std::optional<Chunk>{table_to_chunk(std::move(projected.value()))};
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
                          const ScalarRegistry* scalars, const ExternRegistry* externs)
        : child_(std::move(child)), fields_(fields), scalars_(scalars), externs_(externs) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            return std::optional<Chunk>{};
        }
        Table t = chunk_to_table(std::move(*chunk_res.value()));
        auto out = update_table(std::move(t), *fields_, scalars_, externs_);
        if (!out.has_value()) {
            return std::unexpected(std::move(out.error()));
        }
        return std::optional<Chunk>{table_to_chunk(std::move(out.value()))};
    }

   private:
    OperatorPtr child_;
    const std::vector<ir::FieldSpec>* fields_;
    const ScalarRegistry* scalars_;
    const ExternRegistry* externs_;
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
                          const std::vector<ir::ColumnRef>& group_by)
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

    std::vector<FlatCol> group_flat;
    group_flat.reserve(group_entries.size());
    for (const auto* entry : group_entries)
        group_flat.push_back(flatten(entry, /*ascending=*/true));

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
    const bool radix_order =
        order_flat.size() == 1 && order_flat[0].kind != FlatKind::Str &&
        order_flat[0].validity == nullptr &&
        std::ranges::all_of(group_flat, [](const FlatCol& fc) { return fc.validity == nullptr; });
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
        auto sort_result = radix_sort_u64_asc(std::move(codes), rows);
        idx.resize(rows);
        std::visit(
            [&](const auto& sorted) {
                for (std::size_t i = 0; i < rows; ++i)
                    idx[i] = sorted[i];
            },
            sort_result);

        if (!group_entries.empty()) {
            // Assign group IDs using the already-flattened group_flat arrays (string_view,
            // no per-row allocation) instead of calling scalar_from_column (which
            // heap-allocates std::string for string columns on every row).
            std::vector<std::uint32_t> group_id(rows);
            std::uint32_t ngroups = 0;
            if (group_flat.size() == 1 && group_flat[0].kind == FlatKind::Str) {
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
            // Stable counting sort of idx by group id.
            // Save the prefix-sum as group_starts BEFORE the scatter so we have O(1)
            // group boundary lookup for the rank sweep (avoids O(n) same_group calls).
            std::vector<std::size_t> cnt(static_cast<std::size_t>(ngroups) + 1, 0);
            for (std::size_t r = 0; r < rows; ++r)
                ++cnt[static_cast<std::size_t>(group_id[r]) + 1];
            for (std::size_t g = 0; g < ngroups; ++g)
                cnt[g + 1] += cnt[g];
            radix_group_starts =
                cnt;  // group g spans [radix_group_starts[g], radix_group_starts[g+1])
            std::vector<std::size_t> grouped(rows);
            for (std::size_t i = 0; i < rows; ++i) {
                std::uint32_t g = group_id[idx[i]];
                grouped[cnt[g]++] = idx[i];
            }
            idx = std::move(grouped);
        }
    } else {
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
                    if (rank.na_option == ir::RankNaOption::Bottom) {
                        return !lhs_null;
                    }
                    return lhs < rhs;
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

    std::vector<double> rank_values(rows, 0.0);
    ValidityBitmap validity(rows, true);

    // When the radix fast path ran, group boundaries are already known from the
    // counting sort. Iterate the groups directly: walk radix_group_starts as a
    // cursor so each group_end lookup is O(1) with no scanning. The pdqsort
    // fallback leaves radix_group_starts empty and uses the same_group per-row
    // scan (needed for nulls / string order keys / multi-key cases).
    std::size_t gs_cursor = 0;  // index into radix_group_starts for the fast path

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

        std::size_t dense_rank = 1;
        std::size_t ordinal = 1;
        std::size_t i = pos;
        while (i < group_end) {
            std::size_t tie_end = i + 1;
            while (tie_end < group_end && equal_rank_keys(idx[i], idx[tie_end])) {
                ++tie_end;
            }

            const bool null_tie = is_null_row_for_keys(idx[i]);
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

        pos = group_end;
    }

    const bool integral = !rank.pct && rank.method != ir::RankMethod::Average;
    if (integral) {
        Column<std::int64_t> out;
        out.reserve(rows);
        for (double value : rank_values) {
            out.push_back(static_cast<std::int64_t>(value));
        }
        ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
        if (rank.na_option == ir::RankNaOption::Keep) {
            result.validity = std::move(validity);
        }
        return result;
    }

    Column<double> out;
    out.reserve(rows);
    for (const double value : rank_values) {
        out.push_back(value);
    }
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
    ChunkedOrderOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys)
        : child_(std::move(child)), keys_(keys) {}

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
            out.ordering = resolved_keys_;
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
                if (chunk.time_index.has_value()) {
                    if (keys_->size() != 1 || (*keys_)[0].name != *chunk.time_index ||
                        !(*keys_)[0].ascending) {
                        return std::unexpected(
                            "order on TimeFrame must be by time index ascending");
                    }
                }
                auto resolved = resolve_keys(chunk);
                if (!resolved.has_value()) {
                    return std::unexpected(std::move(resolved.error()));
                }
                resolved_keys_ = std::move(*resolved);
            }
            if (still_sorted_) {
                auto ok = validate_chunk(chunk);
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
        auto sorted = order_table(concat, *keys_);
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

        // Snapshot last row for next boundary check.
        prev_last_.clear();
        prev_last_.reserve(resolved_keys_.size());
        for (std::size_t i = 0; i < resolved_keys_.size(); ++i) {
            prev_last_.push_back(scalar_from_column(*chunk.columns[key_idx[i]].column, rows - 1));
        }
        return true;
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
    ChunkedAsTimeframeOperator(OperatorPtr child, std::string column)
        : child_(std::move(child)), column_(std::move(column)) {}

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
            out.time_index = column_;
            out.ordering = std::vector<ir::OrderKey>{{.name = column_, .ascending = true}};
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

        auto sorted = order_table(concat, {{.name = column_, .ascending = true}});
        if (!sorted.has_value()) {
            return std::unexpected(std::move(sorted.error()));
        }
        sorted->time_index = column_;
        normalize_time_index(*sorted);
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
    Mode mode_ = Mode::Ingest;
    std::vector<Chunk> buffered_;
    std::optional<std::int64_t> prev_last_nanos_;
    std::optional<std::int32_t> prev_last_days_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool type_checked_ = false;
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

    template <typename T>
    auto process_single_key_chunk(const Table& chunk, const Column<T>& key_column, bool ascending,
                                  const std::vector<const ColumnValue*>& group_columns)
        -> std::optional<std::string> {
        for (std::size_t row = 0; row < chunk.rows(); ++row) {
            const std::size_t sequence = next_sequence_++;
            const T& key = key_column[row];

            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                Key group_key;
                group_key.values.reserve(group_columns.size());
                for (const auto* column : group_columns) {
                    group_key.values.push_back(scalar_from_column(*column, row));
                }
                heap = &group_heaps_[std::move(group_key)].heap;
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

        std::vector<const ColumnValue*> group_columns;
        group_columns.reserve(group_by_->size());
        for (const auto& ref : *group_by_) {
            const auto* column = chunk.find(ref.name);
            if (column == nullptr) {
                return "head group-by column not found: " + ref.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            group_columns.push_back(column);
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
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<double>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<bool>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<Date>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
            if (const auto* col = std::get_if<Column<Timestamp>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_columns);
            }
        }

        for (std::size_t row = 0; row < chunk.rows(); ++row) {
            std::vector<Entry>* heap = &heap_;
            if (!group_by_->empty()) {
                Key group_key;
                group_key.values.reserve(group_columns.size());
                for (const auto* column : group_columns) {
                    group_key.values.push_back(scalar_from_column(*column, row));
                }
                heap = &group_heaps_[std::move(group_key)].heap;
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
            for (auto& [_, state] : group_heaps_) {
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

        out.ordering = *keys_;
        normalize_time_index(out);
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
    robin_hood::unordered_flat_map<Key, GroupState, KeyHash, KeyEq> group_heaps_;
    std::optional<Table> empty_template_;
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
                t.ordering.reset();
                t.time_index.reset();
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

            t.ordering.reset();
            t.time_index.reset();
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
        t.ordering.reset();
        t.time_index.reset();
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
        t.ordering.reset();
        t.time_index.reset();
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
            std::vector<std::size_t> idx;
            idx.reserve(rows);
            const auto* codes = col.codes_data();
            for (std::size_t row = 0; row < rows; ++row) {
                if (!seen_cat_codes_.insert(codes[row]).second) {
                    continue;
                }
                idx.push_back(row);
            }
            if (idx.empty()) {
                return std::nullopt;
            }
            t.ordering.reset();
            t.time_index.reset();
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
        t.ordering.reset();
        t.time_index.reset();
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
        t.ordering.reset();
        t.time_index.reset();
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
    robin_hood::unordered_flat_set<Column<Categorical>::code_type> seen_cat_codes_;
    robin_hood::unordered_flat_set<std::string_view, StringViewHash, StringViewEq> seen_strings_;
    std::deque<std::string> owned_strings_;
    const void* cat_dictionary_id_ = nullptr;
};

class ChunkedSemiAntiJoinOperator final : public Operator {
   public:
    ChunkedSemiAntiJoinOperator(OperatorPtr left, Table right, ir::JoinKind kind,
                                const std::vector<std::string>* keys)
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

        const ColumnValue* lkey = left_swapped_->find(keys_->front());
        const auto* lcol = lkey != nullptr ? std::get_if<Column<std::int64_t>>(lkey) : nullptr;
        if (lcol != nullptr && lcol->size() < rcol.size()) {
            // 57k inserts + 3.8M finds, versus 3.8M inserts the other way.
            robin_hood::unordered_flat_map<std::int64_t, char> seen;
            seen.reserve(lcol->size());
            for (const std::int64_t v : *lcol) {
                seen.try_emplace(v, char{0});
            }
            for (const std::int64_t v : rcol) {
                if (auto it = seen.find(v); it != seen.end()) {
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
            for (const std::int64_t v : rcol) {
                right_i64_.insert(v);
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
        const ColumnValue* key = right_.find(keys_->front());
        if (key == nullptr) {
            return "join key not found in right table: " + keys_->front();
        }

        if (const auto* col = std::get_if<Column<std::int64_t>>(key)) {
            right_kind_ = ExprType::Int;
            if (col->size() > kSemiSwapThreshold) {
                return init_int_swapped(*col);
            }
            for (const std::int64_t row : *col) {
                right_i64_.insert(row);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<double>>(key)) {
            right_kind_ = ExprType::Double;
            for (const double row : *col) {
                right_f64_.insert(row);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<bool>>(key)) {
            right_kind_ = ExprType::Bool;
            for (const bool row : *col) {
                right_bool_.insert(row);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Date>>(key)) {
            right_kind_ = ExprType::Date;
            for (auto row : *col) {
                right_date_.insert(row);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Timestamp>>(key)) {
            right_kind_ = ExprType::Timestamp;
            for (auto row : *col) {
                right_timestamp_.insert(row);
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<Categorical>>(key)) {
            right_kind_ = ExprType::String;
            right_cat_dictionary_id_ = static_cast<const void*>(col->dictionary_ptr().get());
            for (std::size_t row = 0; row < col->size(); ++row) {
                right_cat_codes_.insert(col->code_at(row));
            }
            return std::nullopt;
        }
        if (const auto* col = std::get_if<Column<std::string>>(key)) {
            right_kind_ = ExprType::String;
            for (auto row : *col) {
                owned_strings_.emplace_back(row);
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
        const ColumnValue* key = t.find(keys_->front());
        if (key == nullptr) {
            return std::nullopt;
        }
        const bool keep_matches = (kind_ == ir::JoinKind::Semi);

        if (right_kind_ == ExprType::Int) {
            const auto* col = std::get_if<Column<std::int64_t>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_i64_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Double) {
            const auto* col = std::get_if<Column<double>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_f64_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Bool) {
            const auto* col = std::get_if<Column<bool>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_bool_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Date) {
            const auto* col = std::get_if<Column<Date>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_date_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        if (right_kind_ == ExprType::Timestamp) {
            const auto* col = std::get_if<Column<Timestamp>>(key);
            if (col == nullptr) {
                return std::nullopt;
            }
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_timestamp_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }

        if (const auto* col = std::get_if<Column<Categorical>>(key);
            col != nullptr &&
            static_cast<const void*>(col->dictionary_ptr().get()) == right_cat_dictionary_id_) {
            return filter_rows(std::move(t), [&](std::size_t row) {
                const bool match = right_cat_codes_.contains(col->code_at(row));
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
                const bool match = right_strings_.contains((*col)[row]);
                return keep_matches ? match : !match;
            });
        }
        return std::nullopt;
    }

    OperatorPtr left_;
    Table right_;
    ir::JoinKind kind_;
    const std::vector<std::string>* keys_;
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

/// Inner hash join for single-key no-predicate joins.
///
/// Two execution modes:
/// - Stream: right is small (<= kStreamRightThreshold). Build a chained
///   hash index on the materialized right, then probe each left chunk
///   streamed from the child. Matches the classic star-join shape.
/// - Swapped: right is large and n_left < n_right. Materialize left,
///   build the hash index on left, iterate right rows in two phases to
///   emit output in left-row order (baseline's `build_indices_from_right_scan`
///   equivalent). Much better cache behavior when the smaller side fits.
///
/// Name conflicts are resolved with the same `_right` suffix rule as
/// `join_table_impl`.
class ChunkedInnerJoinOperator final : public Operator {
   public:
    ChunkedInnerJoinOperator(OperatorPtr left, Table right, const std::vector<std::string>* keys)
        : left_(std::move(left)), right_(std::move(right)), keys_(keys) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!initialized_) {
            auto err = initialize();
            if (err.has_value()) {
                return std::unexpected(std::move(*err));
            }
            initialized_ = true;
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
    enum class Mode : std::uint8_t { Stream, Swapped };

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
        const std::string& key_name = keys_->front();
        const ColumnValue* rkey = right_.find(key_name);
        if (rkey == nullptr) {
            return "join key not found in right table: " + key_name;
        }
        if (auto err = detect_key_kind(*rkey, key_kind_)) {
            return err;
        }

        const std::size_t n_right = right_.rows();

        if (n_right <= kStreamRightThreshold) {
            if (auto err = build_index(right_, key_name)) {
                return err;
            }
            setup_right_emit_schema(key_name);
            return std::nullopt;
        }

        auto left_res = MaterializeOperator(std::move(left_)).run();
        if (!left_res.has_value()) {
            return std::move(left_res.error());
        }
        Table left_table = std::move(*left_res);
        const std::size_t n_left = left_table.rows();

        if (n_left < n_right) {
            left_table_ = std::move(left_table);
            if (auto err = build_index(*left_table_, key_name)) {
                return err;
            }
            setup_right_emit_schema(key_name);
            mode_ = Mode::Swapped;
            return std::nullopt;
        }

        left_materialized_ = std::move(left_table);
        use_materialized_left_ = true;
        if (auto err = build_index(right_, key_name)) {
            return err;
        }
        setup_right_emit_schema(key_name);
        return std::nullopt;
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

    void setup_right_emit_schema(const std::string& key_name) {
        right_emit_idx_.reserve(right_.columns.size());
        for (std::size_t i = 0; i < right_.columns.size(); ++i) {
            if (right_.columns[i].name == key_name) {
                continue;
            }
            right_emit_idx_.push_back(i);
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

    auto probe_chunk_against_right(Table left_chunk) -> std::expected<Table, std::string> {
        const ColumnValue* key = left_chunk.find(keys_->front());
        if (key == nullptr) {
            return std::unexpected("join key not found in left chunk: " + keys_->front());
        }
        const auto* probe_entry = left_chunk.find_entry(keys_->front());
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
                li_identity = probe_scalar(
                    string_heads_, n,
                    [&](std::size_t i) {
                        return std::string_view{dict[static_cast<std::size_t>(c_cat->code_at(i))]};
                    },
                    li, ri);
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
        const ColumnValue* rkey = right_.find(keys_->front());
        if (rkey == nullptr) {
            return std::unexpected("join key not found in right table: " + keys_->front());
        }
        if (!left_table_.has_value()) {
            return std::unexpected(
                "ChunkedInnerJoinOperator: swapped mode without a materialized left table");
        }
        const Table& left_table = *left_table_;
        const std::size_t n_left = left_table.rows();
        const std::size_t n_right = right_.rows();

        std::vector<std::size_t> match_counts(n_left, 0);
        std::size_t total = 0;

        // In swapped mode the index is on the left, so the right table is the
        // probe side. Its null-keyed rows match nothing (see build_index).
        const auto* right_entry = right_.find_entry(keys_->front());
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
                    ++match_counts[cur];
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
                do_phase1(
                    [&](std::size_t r) {
                        return std::string_view{dict[static_cast<std::size_t>(c_cat->code_at(r))]};
                    },
                    string_heads_);
            } else if (const auto* c_str = std::get_if<Column<std::string>>(rkey)) {
                do_phase1([&](std::size_t r) { return (*c_str)[r]; }, string_heads_);
            } else {
                return std::unexpected("inner join: right key type mismatch");
            }
        }

        // Phase 2: replay the recorded hits. Right rows were visited in
        // ascending order, so writing each match at the running offset of its
        // left row yields (li, ri) sorted by left row, then by right row —
        // the same order the two-probe version produced.
        std::vector<std::size_t> li(total, 0);
        std::vector<std::size_t> ri(total, 0);
        std::vector<std::size_t> next_off(n_left, 0);
        std::size_t acc = 0;
        for (std::size_t l = 0; l < n_left; ++l) {
            next_off[l] = acc;
            acc += match_counts[l];
        }
        for (const Hit& hit : hits) {
            for (std::size_t cur = hit.head; cur != kNil; cur = chain_next_[cur]) {
                const std::size_t pos = next_off[cur]++;
                li[pos] = cur;
                ri[pos] = hit.rrow;
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
                         std::size_t total, bool li_identity = false)
        -> std::expected<Table, std::string> {
        Table output;
        if (total == 0) {
            return output;
        }
        output.columns.reserve(left_side.columns.size() + right_emit_idx_.size());

        robin_hood::unordered_set<std::string> out_names;
        out_names.reserve(left_side.columns.size() + right_emit_idx_.size());

        auto gather_with_validity =
            [&](const ColumnValue& src_col, const std::optional<ValidityBitmap>& src_val,
                const std::size_t* idx) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
            ColumnValue gathered = gather_column(src_col, idx, total);
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
        if (li_identity && total == left_side.rows()) {
            for (auto& lc : left_side.columns) {
                out_names.insert(lc.name);
                output.index[lc.name] = output.columns.size();
                output.columns.push_back(std::move(lc));
            }
        } else {
            for (const auto& lc : left_side.columns) {
                auto [gathered, val] = gather_with_validity(*lc.column, lc.validity, li);
                out_names.insert(lc.name);
                if (val.has_value()) {
                    output.add_column(lc.name, std::move(gathered), std::move(*val));
                } else {
                    output.add_column(lc.name, std::move(gathered));
                }
            }
        }

        for (const std::size_t idx : right_emit_idx_) {
            const auto& rc = right_.columns[idx];
            std::string name = rc.name;
            while (out_names.contains(name)) {
                name += "_right";
            }
            out_names.insert(name);
            auto [gathered, val] = gather_with_validity(*rc.column, rc.validity, ri);
            if (val.has_value()) {
                output.add_column(std::move(name), std::move(gathered), std::move(*val));
            } else {
                output.add_column(std::move(name), std::move(gathered));
            }
        }
        return output;
    }

    OperatorPtr left_;
    Table right_;
    const std::vector<std::string>* keys_;

    bool initialized_ = false;
    Mode mode_ = Mode::Stream;
    ExprType key_kind_ = ExprType::Int;

    // Hash index on the build side (right in Stream, left in Swapped).
    bool build_unique_ = true;
    std::vector<std::size_t> chain_next_;
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> i64_heads_;
    robin_hood::unordered_flat_map<double, std::size_t> f64_heads_;
    robin_hood::unordered_flat_map<bool, std::size_t> bool_heads_;
    robin_hood::unordered_flat_map<Date, std::size_t> date_heads_;
    robin_hood::unordered_flat_map<Timestamp, std::size_t> ts_heads_;
    robin_hood::unordered_flat_map<std::string_view, std::size_t, StringViewHash, StringViewEq>
        string_heads_;
    std::vector<std::size_t> right_emit_idx_;

    // Stream mode: when right > threshold and left >= right, left was
    // materialized to measure but not swapped; replay as a single chunk.
    std::optional<Table> left_materialized_;
    bool left_materialized_drained_ = false;
    bool use_materialized_left_ = false;

    // Swapped mode: materialized left held for later gather.
    std::optional<Table> left_table_;
    bool swapped_emitted_ = false;
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
    ChunkedAggregateOperator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                             const std::vector<ir::AggSpec>* aggregations)
        : child_(std::move(child)), group_by_(group_by), aggregations_(aggregations) {}

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
                auto ka = int_kind_of(*group_entries[0]->column);
                auto kb = int_kind_of(*group_entries[1]->column);
                if (ka.has_value() && kb.has_value()) {
                    pair_int_fast_path_ = true;
                    int_key_kind_ = *ka;
                    int_key_kind_b_ = *kb;
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
                    flat_slots_.resize(n_groups_ * n_aggs_);
                } else {
                    gid = static_cast<std::uint32_t>(it->second);
                }
                prev_key = key;
                prev_gid = gid;
            }
            gids[row] = gid;
        }

        accumulate_columns(gids, agg_entries, rows);
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
        }
        const auto key_at = [&](std::size_t row) -> std::int64_t {
            switch (int_key_kind_) {
                case IntKeyKind::Int64:
                    return i64[row];
                case IntKeyKind::Date:
                    return dates[row].days;
                case IntKeyKind::Ts:
                    return stamps[row].nanos;
            }
            return 0;
        };

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

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
                    flat_slots_.resize(n_groups_ * n_aggs_);
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    // Two fixed-width-integer keys, grouped as one composite. Mirrors
    // process_rows_int exactly, packing (key_a, key_b) into a two-word key so
    // a single hash probe replaces the generic path's per-key Key comparison.
    auto process_rows_int_pair(const std::vector<const ColumnEntry*>& group_entries,
                               const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> std::optional<std::string> {
        const auto raw_reader = [](const ColumnValue& col, IntKeyKind kind) {
            return [&col, kind](std::size_t row) -> std::int64_t {
                switch (kind) {
                    case IntKeyKind::Int64:
                        return std::get<Column<std::int64_t>>(col).data()[row];
                    case IntKeyKind::Date:
                        return std::get<Column<Date>>(col).data()[row].days;
                    case IntKeyKind::Ts:
                        return std::get<Column<Timestamp>>(col).data()[row].nanos;
                }
                return 0;
            };
        };
        const auto key_a_at = raw_reader(*group_entries[0]->column, int_key_kind_);
        const auto key_b_at = raw_reader(*group_entries[1]->column, int_key_kind_b_);
        const auto pack = [](std::int64_t a, std::int64_t b) -> PairIntKey {
            return {.first = static_cast<std::uint64_t>(a),
                    .second = static_cast<std::uint64_t>(b)};
        };

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

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
                    flat_slots_.resize(n_groups_ * n_aggs_);
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    auto alloc_group() -> std::uint32_t {
        auto gid = static_cast<std::uint32_t>(n_groups_);
        ++n_groups_;
        flat_slots_.resize(n_groups_ * n_aggs_);
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

        accumulate_columns(gids, agg_entries, rows);
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

        accumulate_columns(gids, agg_entries, rows);
        return std::nullopt;
    }

    void accumulate_columns(const std::uint32_t* gids,
                            const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows) {
        AggSlot* fs = flat_slots_.data();
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            const auto slot_for = [&](std::uint32_t g) -> AggSlot& {
                return fs[(static_cast<std::size_t>(g) * n_aggs_) + agg_i];
            };

            if (plan_[agg_i].func == ir::AggFunc::Count) {
                for (std::size_t row = 0; row < rows; ++row) {
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
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.double_value += data[row];
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.sum += data[row];
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.has_value ? std::min(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const double v = data[row];
                            slot.double_value = slot.has_value ? std::max(slot.double_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), data[row]);
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_moments(slot_for(gids[row]), data[row]);
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = 0; row < rows; ++row) {
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
                        for (std::size_t row = 0; row < rows; ++row) {
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
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.int_value += data[row];
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Mean:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            slot.sum += static_cast<double>(data[row]);
                            slot.count++;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Min:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::min(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            std::int64_t v = data[row];
                            slot.int_value = slot.has_value ? std::max(slot.int_value, v) : v;
                            slot.has_value = true;
                        }
                        break;
                    case ir::AggFunc::Stddev:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_stddev(slot_for(gids[row]), static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::Skew:
                    case ir::AggFunc::Kurtosis:
                        for (std::size_t row = 0; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            agg_update_moments(slot_for(gids[row]), static_cast<double>(data[row]));
                        }
                        break;
                    case ir::AggFunc::First:
                        for (std::size_t row = 0; row < rows; ++row) {
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
                        for (std::size_t row = 0; row < rows; ++row) {
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
                    for (std::size_t row = 0; row < rows; ++row) {
                        if (has_nulls && !(*validity)[row])
                            continue;
                        auto& slot = slot_for(gids[row]);
                        if (!slot.has_value) {
                            slot.first_value = value_at(row);
                            slot.has_value = true;
                        }
                    }
                } else {
                    for (std::size_t row = 0; row < rows; ++row) {
                        if (has_nulls && !(*validity)[row])
                            continue;
                        auto& slot = slot_for(gids[row]);
                        slot.last_value = value_at(row);
                        slot.has_value = true;
                    }
                }
            }
        }
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
            }
        };

        const AggSlot* fs = flat_slots_.data();
        for (std::size_t g = 0; g < n_groups_; ++g) {
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
                        key_validity[ci].set(g, false);
                    }
                }
            }
            for (std::size_t i = 0; i < aggregations_->size(); ++i) {
                auto& column = out.mutable_column(group_by_->size() + i);
                const AggSlot& slot = fs[(g * n_aggs_) + i];
                if (track_validity[i] != 0U) {
                    agg_validity[i].push_back(chunked_agg_valid(plan_[i].func, slot));
                }
                switch (plan_[i].func) {
                    case ir::AggFunc::Count:
                        append_scalar(column, slot.count);
                        break;
                    case ir::AggFunc::Mean:
                        append_scalar(column, slot.count == 0
                                                  ? 0.0
                                                  : slot.sum / static_cast<double>(slot.count));
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
                        append_scalar(column, agg_finalize_skew(slot));
                        break;
                    case ir::AggFunc::Kurtosis:
                        append_scalar(column, agg_finalize_kurtosis(slot));
                        break;
                    case ir::AggFunc::First:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else if (plan_[i].kind == ExprType::Int) {
                            append_scalar(column, slot.int_value);
                        } else {
                            append_scalar(column, slot.first_value);
                        }
                        break;
                    case ir::AggFunc::Last:
                        if (plan_[i].kind == ExprType::Double) {
                            append_scalar(column, slot.double_value);
                        } else if (plan_[i].kind == ExprType::Int) {
                            append_scalar(column, slot.int_value);
                        } else {
                            append_scalar(column, slot.last_value);
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

    struct SlotPlan {
        ir::AggFunc func = ir::AggFunc::Sum;
        ExprType kind = ExprType::Int;
        // Only meaningful when kind == String: disambiguates Column<Categorical>
        // from Column<std::string> for First/Last output construction, since
        // expr_type_for_column collapses both to ExprType::String.
        bool categorical = false;
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
    bool emitted_ = false;

    bool initialized_ = false;
    bool cat_fast_path_ = false;
    bool str_fast_path_ = false;
    std::size_t n_aggs_ = 0;
    std::size_t n_groups_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> group_templates_;

    // Flat accumulator storage: n_groups_ × n_aggs_ contiguous AggSlots.
    std::vector<AggSlot> flat_slots_;

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
    enum class IntKeyKind : std::uint8_t { Int64, Date, Ts };
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
                                   const std::vector<ir::AggSpec>* aggregations)
        : child_(std::move(child)), group_by_(group_by), aggregations_(aggregations) {}

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
                    group_by_, aggregations_);
                return {};
            }
            done_ = true;
            input_eof_ = true;
            return {};
        }
        if (!sorted_on_group_by(first) || needs_hash_fallback(first)) {
            fallback_ = std::make_unique<ChunkedAggregateOperator>(
                std::make_unique<PrependChunkOperator>(std::move(first), std::move(child_)),
                group_by_, aggregations_);
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
        if (!chunk.ordering.has_value() || chunk.ordering->size() < group_by_->size()) {
            return false;
        }
        const auto& ordering = *chunk.ordering;
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
        if (first.ordering.has_value()) {
            out_ordering_.assign(
                first.ordering->begin(),
                first.ordering->begin() + static_cast<std::ptrdiff_t>(group_by_->size()));
        }
        cur_slots_.assign(n_aggs_, AggSlot{});
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
        std::ranges::fill(cur_slots_, AggSlot{});
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
            AggSlot& slot = cur_slots_[i];
            if (plan_[i].func == ir::AggFunc::Count) {
                slot.count += static_cast<std::int64_t>(end - start);
                continue;
            }
            const auto& entry = *agg_entries[i];
            const bool has_nulls = entry.validity.has_value();
            if (plan_[i].kind == ExprType::Double) {
                const double* data = std::get<Column<double>>(*entry.column).data();
                accumulate_typed(slot, plan_[i].func, data, entry, has_nulls, start, end);
            } else {
                const std::int64_t* data = std::get<Column<std::int64_t>>(*entry.column).data();
                accumulate_typed(slot, plan_[i].func, data, entry, has_nulls, start, end);
            }
        }
    }

    template <typename T>
    static void accumulate_typed(AggSlot& slot, ir::AggFunc func, const T* data,
                                 const ColumnEntry& entry, bool has_nulls, std::size_t start,
                                 std::size_t end) {
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
                    slot.sum += static_cast<double>(data[row]);
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
                    agg_update_moments(slot, static_cast<double>(data[row]));
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
            const AggSlot& slot = cur_slots_[i];
            if (track_validity_[i] != 0U) {
                out_validity_[i].push_back(chunked_agg_valid(plan_[i].func, slot));
            }
            switch (plan_[i].func) {
                case ir::AggFunc::Count:
                    append_scalar(column, ScalarValue{slot.count});
                    break;
                case ir::AggFunc::Mean:
                    append_scalar(
                        column,
                        ScalarValue{slot.count == 0 ? 0.0
                                                    : slot.sum / static_cast<double>(slot.count)});
                    break;
                case ir::AggFunc::Stddev:
                    append_scalar(column, ScalarValue{agg_finalize_stddev(slot)});
                    break;
                case ir::AggFunc::Skew:
                    append_scalar(column, ScalarValue{agg_finalize_skew(slot)});
                    break;
                case ir::AggFunc::Kurtosis:
                    append_scalar(column, ScalarValue{agg_finalize_kurtosis(slot)});
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
            out.ordering = out_ordering_;
        }
        reset_output();
        return out;
    }

    OperatorPtr child_;
    const std::vector<ir::ColumnRef>* group_by_;
    const std::vector<ir::AggSpec>* aggregations_;

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
    std::vector<AggSlot> cur_slots_;
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
                                        const ExternRegistry* externs, ModelResult* model_out,
                                        Fn fn) -> std::expected<OperatorPtr, std::string> {
    auto child_op = build_operator(child_node, registry, scalars, externs, model_out);
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
                                         const ExternRegistry* externs, ModelResult* model_out,
                                         Fn fn) -> std::expected<OperatorPtr, std::string> {
    auto left_op = build_operator(left_node, registry, scalars, externs, model_out);
    if (!left_op.has_value()) {
        return std::unexpected(std::move(left_op.error()));
    }
    auto right_op = build_operator(right_node, registry, scalars, externs, model_out);
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

auto build_operator(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    if (node.kind() == ir::NodeKind::Filter) {
        const auto& filter = static_cast<const ir::FilterNode&>(node);
        if (filter.children().empty()) {
            return std::unexpected("filter node missing child");
        }
        auto child_op =
            build_operator(*filter.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterOperator>(std::move(child_op.value()),
                                                       &filter.predicate(), scalars);
    }

    if (node.kind() == ir::NodeKind::Project) {
        const auto& project = static_cast<const ir::ProjectNode&>(node);
        if (project.children().empty()) {
            return std::unexpected("project node missing child");
        }
        auto child_op =
            build_operator(*project.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedProjectOperator>(std::move(child_op.value()),
                                                        &project.columns());
    }

    // Fused Project(Filter(x)) produced by canonicalize R5.
    if (node.kind() == ir::NodeKind::FilterProject) {
        const auto& fp = static_cast<const ir::FilterProjectNode&>(node);
        if (fp.children().empty()) {
            return std::unexpected("filter_project node missing child");
        }
        auto child_op =
            build_operator(*fp.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterProjectOperator>(
            std::move(child_op.value()), &fp.predicate(), &fp.columns(), scalars);
    }

    // Fused Head(Filter(x)) / Tail(Filter(x)) produced by canonicalize R7/R8.
    if (node.kind() == ir::NodeKind::FilterHead) {
        const auto& fh = static_cast<const ir::FilterHeadNode&>(node);
        if (fh.children().empty()) {
            return std::unexpected("filter_head node missing child");
        }
        auto child_op =
            build_operator(*fh.children().front(), registry, scalars, externs, model_out);
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
            build_operator(*ft.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterTailOperator>(std::move(child_op.value()),
                                                           &ft.predicate(), ft.count(), scalars);
    }

    // Fused Project(Update(Filter(x))) produced by canonicalize R6. The
    // gather set (columns the update reads ∪ projected columns not produced
    // by the update) is recomputed here from the node payload.
    if (node.kind() == ir::NodeKind::FilterUpdateProject) {
        const auto& fup = static_cast<const ir::FilterUpdateProjectNode&>(node);
        if (fup.children().empty()) {
            return std::unexpected("filter_update_project node missing child");
        }
        robin_hood::unordered_set<std::string> update_outputs;
        robin_hood::unordered_set<std::string> needed;
        for (const auto& f : fup.fields()) {
            update_outputs.insert(f.alias);
            collect_expr_column_refs(f.expr, needed);
        }
        for (const auto& col : fup.project_columns()) {
            if (update_outputs.find(col.name) == update_outputs.end()) {
                needed.insert(col.name);
            }
        }
        std::vector<ir::ColumnRef> gather_cols;
        gather_cols.reserve(needed.size());
        for (const auto& name : needed) {
            gather_cols.push_back(ir::ColumnRef{.name = name});
        }
        auto child_op =
            build_operator(*fup.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedFilterUpdateProjectOperator>(
            std::move(child_op.value()), &fup.predicate(), &fup.fields(), &fup.project_columns(),
            std::move(gather_cols), scalars, externs);
    }

    if (node.kind() == ir::NodeKind::Rename) {
        const auto& rename = static_cast<const ir::RenameNode&>(node);
        if (rename.children().empty()) {
            return std::unexpected("rename node missing child");
        }
        auto child_op =
            build_operator(*rename.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedRenameOperator>(std::move(child_op.value()),
                                                       &rename.renames());
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
            build_operator(*node.children().front(), registry, scalars, externs, model_out);
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
            build_operator(*order.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedOrderOperator>(std::move(child_op.value()), &order.keys());
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
            auto child_op =
                build_operator(*agg.children().front(), registry, scalars, externs, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            // The sorted operator streams group-at-a-time when the child's
            // chunks arrive sorted on the group keys, and otherwise replays the
            // first chunk into a hash ChunkedAggregateOperator — so it is safe
            // to route the whole streamable subset here.
            return std::make_unique<ChunkedSortedAggregateOperator>(
                std::move(child_op.value()), &agg.group_by(), &agg.aggregations());
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
            build_operator(*topk.children().front(), registry, scalars, externs, model_out);
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
            build_operator(*head.children().front(), registry, scalars, externs, model_out);
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
            *tail.children().front(), registry, scalars, externs, model_out,
            [&](Table input) { return tail_table(input, *count, tail.group_by()); });
    }

    if (node.kind() == ir::NodeKind::Columns) {
        if (node.children().empty()) {
            return std::unexpected("columns node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return columns_table(input); });
    }

    if (node.kind() == ir::NodeKind::Melt) {
        const auto& mn = static_cast<const ir::MeltNode&>(node);
        if (mn.children().empty()) {
            return std::unexpected("melt node missing child");
        }
        return build_unary_materializing_operator(
            *mn.children().front(), registry, scalars, externs, model_out,
            [&](Table input) { return melt_table(input, mn.id_columns(), mn.measure_columns()); });
    }

    if (node.kind() == ir::NodeKind::Dcast) {
        const auto& dn = static_cast<const ir::DcastNode&>(node);
        if (dn.children().empty()) {
            return std::unexpected("dcast node missing child");
        }
        return build_unary_materializing_operator(
            *dn.children().front(), registry, scalars, externs, model_out, [&](Table input) {
                return dcast_table(input, dn.pivot_column(), dn.value_column(), dn.row_keys());
            });
    }

    if (node.kind() == ir::NodeKind::Cov) {
        if (node.children().empty()) {
            return std::unexpected("cov node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return cov_table(input); });
    }

    if (node.kind() == ir::NodeKind::Corr) {
        if (node.children().empty()) {
            return std::unexpected("corr node missing child");
        }
        return build_unary_materializing_operator(*node.children().front(), registry, scalars,
                                                  externs, model_out,
                                                  [](Table input) { return corr_table(input); });
    }

    if (node.kind() == ir::NodeKind::Transpose) {
        if (node.children().empty()) {
            return std::unexpected("transpose node missing child");
        }
        return build_unary_materializing_operator(
            *node.children().front(), registry, scalars, externs, model_out,
            [](Table input) { return transpose_table(input); });
    }

    if (node.kind() == ir::NodeKind::Join) {
        const auto& join = static_cast<const ir::JoinNode&>(node);
        if (join.children().size() != 2) {
            return std::unexpected("join node expects exactly two children");
        }
        const bool streamable_semi_anti =
            (join.kind() == ir::JoinKind::Semi || join.kind() == ir::JoinKind::Anti) &&
            !join.predicate().has_value() && join.keys().size() == 1;
        if (streamable_semi_anti) {
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, model_out);
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
        const bool streamable_inner = join.kind() == ir::JoinKind::Inner &&
                                      !join.predicate().has_value() && join.keys().size() == 1;
        if (streamable_inner) {
            auto left_op =
                build_operator(*join.children()[0], registry, scalars, externs, model_out);
            if (!left_op.has_value()) {
                return std::unexpected(std::move(left_op.error()));
            }
            auto right_op =
                build_operator(*join.children()[1], registry, scalars, externs, model_out);
            if (!right_op.has_value()) {
                return std::unexpected(std::move(right_op.error()));
            }
            auto right = materialize_operator(std::move(right_op.value()));
            if (!right.has_value()) {
                return std::unexpected(std::move(right.error()));
            }
            return std::make_unique<ChunkedInnerJoinOperator>(
                std::move(left_op.value()), std::move(right.value()), &join.keys());
        }
        const ir::Expr* pred = join.predicate().has_value() ? &*join.predicate() : nullptr;
        return build_binary_materializing_operator(
            *join.children()[0], *join.children()[1], registry, scalars, externs, model_out,
            [&](Table left, Table right) {
                return join_table_impl(left, right, join.kind(), join.keys(), pred, scalars,
                                       compute_mask);
            });
    }

    if (node.kind() == ir::NodeKind::Matmul) {
        if (node.children().size() != 2) {
            return std::unexpected("matmul node expects exactly two children");
        }
        return build_binary_materializing_operator(
            *node.children()[0], *node.children()[1], registry, scalars, externs, model_out,
            [](Table left, Table right) { return matmul_table(left, right); });
    }

    if (node.kind() == ir::NodeKind::Update) {
        const auto& update = static_cast<const ir::UpdateNode&>(node);
        if (update.children().empty()) {
            return std::unexpected("update node missing child");
        }
        if (update.guard() != nullptr) {
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    return apply_guarded_update(std::move(input), update, scalars, externs);
                });
        }
        if (!update.group_by().empty()) {
            const bool all_rank = std::all_of(
                update.fields().begin(), update.fields().end(), [](const ir::FieldSpec& f) {
                    return std::holds_alternative<ir::RankExpr>(f.expr.node);
                });
            if (!all_rank && update.tuple_fields().empty()) {
                return build_unary_materializing_operator(
                    *update.children().front(), registry, scalars, externs, model_out,
                    [&](Table input) -> std::expected<Table, std::string> {
                        return grouped_update_table(std::move(input), update.fields(),
                                                    update.group_by(), scalars, externs);
                    });
            }
            if (!all_rank || !update.tuple_fields().empty()) {
                return std::unexpected(
                    "update + by: tuple-bound fields are not yet supported in grouped updates");
            }
            return build_unary_materializing_operator(
                *update.children().front(), registry, scalars, externs, model_out,
                [&](Table input) -> std::expected<Table, std::string> {
                    Table result = std::move(input);
                    for (const auto& field : update.fields()) {
                        const auto* rank = std::get_if<ir::RankExpr>(&field.expr.node);
                        auto res = evaluate_rank_column(result, *rank, update.group_by());
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
            auto child_op =
                build_operator(*update.children().front(), registry, scalars, externs, model_out);
            if (!child_op.has_value()) {
                return std::unexpected(std::move(child_op.error()));
            }
            return std::make_unique<ChunkedUpdateOperator>(std::move(child_op.value()),
                                                           &update.fields(), scalars, externs);
        }
        auto child = build_unary_materializing_operator(
            *update.children().front(), registry, scalars, externs, model_out, [&](Table input) {
                return update_table(std::move(input), update.fields(), scalars, externs);
            });
        if (!child.has_value()) {
            return std::unexpected(std::move(child.error()));
        }
        auto result = materialize_operator(std::move(child.value()));
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        for (const auto& tspec : update.tuple_fields()) {
            auto src = interpret_node(*tspec.source, registry, scalars, externs);
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
            *node.children().front(), registry, scalars, externs, model_out, [&](Table input) {
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
        auto source_op =
            build_operator(*child_node.children().front(), registry, scalars, externs, model_out);
        if (!source_op.has_value()) {
            return std::unexpected(std::move(source_op.error()));
        }
        auto source = materialize_operator(std::move(source_op.value()));
        if (!source.has_value()) {
            return std::unexpected(std::move(source.error()));
        }
        if (!source->time_index.has_value()) {
            return std::unexpected(
                "window requires a TimeFrame — use as_timeframe() to designate a timestamp column");
        }
        auto result = update_node.group_by().empty()
                          ? windowed_update_table(std::move(source.value()), update_node.fields(),
                                                  win.duration(), scalars, externs)
                          : grouped_windowed_update_table(std::move(source.value()),
                                                          update_node.fields(), win.duration(),
                                                          update_node.group_by(), scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        return std::make_unique<TableSourceOperator>(std::move(result.value()));
    }

    if (node.kind() == ir::NodeKind::AsTimeframe) {
        const auto& atf = static_cast<const ir::AsTimeframeNode&>(node);
        if (node.children().empty()) {
            return std::unexpected("as_timeframe node missing child");
        }
        auto child_op =
            build_operator(*node.children().front(), registry, scalars, externs, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return std::make_unique<ChunkedAsTimeframeOperator>(std::move(child_op.value()),
                                                            atf.column());
    }

    if (node.kind() == ir::NodeKind::Model) {
        const auto& mn = static_cast<const ir::ModelNode&>(node);
        if (mn.children().empty()) {
            return std::unexpected("model node missing child");
        }
        auto child_op =
            build_operator(*mn.children().front(), registry, scalars, externs, model_out);
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
        auto table = interpret_node(node, registry, scalars, externs, model_out);
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
        return build_operator(program.main_node(), registry, scalars, externs, model_out);
    }

    // Remaining node kinds fall through to interpret_node. Scan is already
    // handled as a source by the caller.
    auto table = interpret_node(node, registry, scalars, externs, model_out);
    if (!table.has_value()) {
        return std::unexpected(std::move(table.error()));
    }
    return std::make_unique<TableSourceOperator>(std::move(table.value()));
}
// NOLINTEND cppcoreguidelines-pro-type-static-cast-downcast

}  // namespace ibex::runtime
