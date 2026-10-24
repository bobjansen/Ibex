// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// ordered_chunked.cpp — the ordering breaker family: chunk-preserving `order`
// and the bounded-heap `TopK` select, plus their physical-plan construction
// sites (`build_physical_order`, `build_physical_topk`). Split out of
// chunked.cpp; the operators are fully private to this translation unit.
// `build_physical_tail` stays in chunked.cpp (it materializes and calls
// `tail_table` rather than driving an ordering operator); `as_timeframe` is a
// whole-table `run_materialized_node` implementation with no streaming operator.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "physical_executor_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

/// Chunk-preserving `Order`: buffers incoming chunks, validates sortedness
/// on-the-fly, and at EOF either emits the buffered chunks unchanged (with
/// `ordering` stamped) or falls back to `order_table` on the concatenated
/// input. Downstream operators see a chunked stream either way — the win
/// over the materializing path is avoiding the final big concat+sort when
/// the input is already ordered, plus preserving chunk shape for whatever
/// runs next.
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
        return schema_.release();
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
                if (!schema_.holding()) {
                    const auto identity = chunk_identity_of(chunk);
                    auto empty = order_table(chunk_to_table(std::move(chunk)), *keys_, *exec_);
                    if (!empty.has_value()) {
                        return std::unexpected(std::move(empty.error()));
                    }
                    schema_.hold(std::move(*empty), identity);
                }
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
            schema_.emitted();
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
    SchemaCarrier schema_;
    std::vector<Chunk> buffered_;
    std::vector<ir::OrderKey> resolved_keys_;
    std::vector<ScalarValue> prev_last_;
    std::size_t emit_idx_ = 0;
    std::optional<Table> sorted_result_;
    bool still_sorted_ = true;
};

/// Yields prebuilt chunks in order, for concatenating them through
/// `materialize_operator`.
class ChunkListSource final : public Operator {
   public:
    explicit ChunkListSource(std::vector<Chunk> chunks) : chunks_(std::move(chunks)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (pos_ >= chunks_.size()) {
            return std::optional<Chunk>{};
        }
        return std::optional<Chunk>{std::move(chunks_[pos_++])};
    }

   private:
    std::vector<Chunk> chunks_;
    std::size_t pos_ = 0;
};

class ChunkedOrderedLimitOperator final : public Operator {
   public:
    enum class KeepMode : std::uint8_t { First, Last };

    ChunkedOrderedLimitOperator(OperatorPtr child, const std::vector<ir::OrderKey>* keys,
                                std::size_t count, const std::vector<ir::ColumnRef>* group_by,
                                KeepMode keep_mode, ExecutionContext exec)
        : child_(std::move(child)),
          keys_(keys),
          count_(count),
          group_by_(group_by),
          keep_mode_(keep_mode),
          exec_(std::move(exec)) {}

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
            group_states_.emplace_back();
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
        if constexpr (!std::is_same_v<T, bool>) {
            if (!group_by_->empty() && count_ <= kPodMaxCount) {
                using K = std::conditional_t<std::is_same_v<T, double>, double, std::int64_t>;
                return process_grouped_pod<K>(
                    chunk,
                    [&](std::size_t r) -> K {
                        if constexpr (std::is_same_v<T, Date>) {
                            return key_at(r).days;
                        } else if constexpr (std::is_same_v<T, Timestamp>) {
                            return key_at(r).nanos;
                        } else {
                            return key_at(r);
                        }
                    },
                    ascending, group_key_cols, rows);
            }
        }
        // Grouped by one Categorical key with no nulls, across workers: each
        // fixed row range keeps a heap per dictionary code, then per code the
        // range heaps merge into that group's heap. The output is sorted by
        // (key, sequence) at the end, so which group a winner sits in first
        // does not matter, and a group's winners -- its top k under that strict
        // order -- do not depend on the merge order.
        constexpr std::size_t kMaxRangeCodeHeaps = std::size_t{1} << 16;
        const std::size_t group_workers = group_by_->size() == 1 && !cat_gid_memo_.empty() &&
                                                  group_key_cols.front().validity == nullptr
                                              ? group_barrier_worker_count(exec_, rows)
                                              : 0;
        if (group_workers >= 2 && group_workers * cat_gid_memo_.size() <= kMaxRangeCodeHeaps) {
            const auto& cat = *group_key_cols.front().cat;
            const std::size_t dict = cat_gid_memo_.size();
            constexpr std::size_t kNoRow = std::numeric_limits<std::size_t>::max();
            const std::size_t base = next_sequence_;
            const std::size_t grain = (rows + group_workers - 1) / group_workers;
            std::vector<std::vector<std::vector<Entry>>> local(group_workers);
            std::vector<std::vector<std::size_t>> sample(group_workers);
            auto batch = process_worker_pool().submit(group_workers, [&](std::size_t r) {
                auto& heaps = local[r];
                auto& first_row = sample[r];
                heaps.resize(dict);
                first_row.assign(dict, kNoRow);
                const std::size_t begin = std::min(rows, r * grain);
                const std::size_t end = std::min(rows, begin + grain);
                for (std::size_t row = begin; row < end; ++row) {
                    const auto code = static_cast<std::size_t>(cat.code_at(row));
                    std::vector<Entry>& heap = heaps[code];
                    if (first_row[code] == kNoRow) {
                        first_row[code] = row;
                    }
                    const std::size_t sequence = base + row;
                    const T key = key_at(row);
                    if (heap.size() == count_ &&
                        !single_key_better(key, sequence, heap.front(), ascending)) {
                        continue;
                    }
                    Entry entry;
                    entry.key.values.reserve(1);
                    entry.key.values.emplace_back(key);
                    entry.sequence = sequence;
                    entry.row = snapshot_row(chunk, row);
                    push_entry(heap, std::move(entry));
                }
            });
            batch.wait();
            next_sequence_ += rows;
            for (std::size_t code = 0; code < dict; ++code) {
                std::size_t any_row = kNoRow;
                for (std::size_t r = 0; r < group_workers && any_row == kNoRow; ++r) {
                    any_row = sample[r][code];
                }
                if (any_row == kNoRow) {
                    continue;
                }
                std::vector<Entry>* heap = resolve_group_heap(chunk, group_key_cols, any_row);
                for (std::size_t r = 0; r < group_workers; ++r) {
                    for (auto& entry : local[r][code]) {
                        if (heap->size() == count_ && !entry_preferred(entry, heap->front())) {
                            continue;
                        }
                        push_entry(*heap, std::move(entry));
                    }
                }
            }
            return std::nullopt;
        }

        // Ungrouped, across workers: each fixed row range keeps its own heap,
        // then the range heaps merge into this one. A row's rank is (key,
        // sequence), a strict total order, so the winners do not depend on
        // which rows a heap saw first; sequences are the serial ones.
        const std::size_t workers =
            group_by_->empty() ? group_barrier_worker_count(exec_, rows) : 0;
        if (workers >= 2) {
            const std::size_t base = next_sequence_;
            const std::size_t grain = (rows + workers - 1) / workers;
            std::vector<std::vector<Entry>> local(workers);
            auto batch = process_worker_pool().submit(workers, [&](std::size_t r) {
                std::vector<Entry>& heap = local[r];
                const std::size_t begin = std::min(rows, r * grain);
                const std::size_t end = std::min(rows, begin + grain);
                for (std::size_t row = begin; row < end; ++row) {
                    const std::size_t sequence = base + row;
                    const T key = key_at(row);
                    if (heap.size() == count_ &&
                        !single_key_better(key, sequence, heap.front(), ascending)) {
                        continue;
                    }
                    Entry entry;
                    entry.key.values.reserve(1);
                    entry.key.values.emplace_back(key);
                    entry.sequence = sequence;
                    entry.row = snapshot_row(chunk, row);
                    push_entry(heap, std::move(entry));
                }
            });
            batch.wait();
            next_sequence_ += rows;
            for (auto& heap : local) {
                for (auto& entry : heap) {
                    if (heap_.size() == count_ && !entry_preferred(entry, heap_.front())) {
                        continue;
                    }
                    push_entry(heap_, std::move(entry));
                }
            }
            return std::nullopt;
        }
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

    // ── Grouped top-k over one order key, in flat POD heaps ─────────────────
    //
    // `order { v }, head k, by g` (h2o q8: two largest v3 per id6) used to
    // resolve every row's group through one serial boxed index and to box a
    // full-row snapshot for every heap candidate, so it ran on one core and
    // at 5M groups held gigabytes of `ScalarValue`s. Here the rows are hash
    // partitioned by group key and each partition, on its own worker, keeps
    // its groups' heaps as `k` flat (order key, sequence, ref) slots. A
    // group lives in exactly one partition and its rows are visited in row
    // order, so every heap sees what the serial loop would. Only rows still
    // in a heap when the chunk ends are copied, into a stored piece; the
    // winners are gathered from those pieces once, at the end, in the same
    // (key, sequence) order `build_output` sorts by.

    static constexpr std::size_t kPodMaxCount = 16;
    /// A ref of a row in the chunk being processed, not yet in a piece.
    static constexpr std::uint64_t kPodCurrentChunk = std::uint64_t{1} << 63;

    template <typename K>
    struct PodEntry {
        K key{};
        std::uint64_t sequence = 0;
        std::uint64_t ref = 0;  ///< (piece << 32) | row, or kPodCurrentChunk | row
    };

    template <typename K>
    struct PodPartition {
        KeyRowIndex index;
        std::vector<Key> keys;
        /// The index instead, when the group key is one integer column.
        robin_hood::unordered_flat_map<std::int64_t, std::uint32_t> int_index;
        std::uint32_t null_gid = std::numeric_limits<std::uint32_t>::max();
        std::vector<PodEntry<K>> heaps;  ///< gid * count_ .. + sizes[gid]
        std::vector<std::uint32_t> sizes;
        std::vector<std::uint32_t> stamp;    ///< chunk number + 1 that last changed gid
        std::vector<std::uint32_t> touched;  ///< gids changed by the current chunk
    };

    template <typename K>
    auto pod_partitions() -> std::vector<PodPartition<K>>& {
        if constexpr (std::is_same_v<K, double>) {
            return pod_double_;
        } else {
            return pod_int_;
        }
    }

    template <typename K>
    [[nodiscard]] auto pod_better(const PodEntry<K>& lhs, const PodEntry<K>& rhs,
                                  bool ascending) const -> bool {
        // `single_key_better`, on the flat entry.
        if (lhs.key == rhs.key) {
            return keep_mode_ == KeepMode::First ? (lhs.sequence < rhs.sequence)
                                                 : (rhs.sequence < lhs.sequence);
        }
        const bool lhs_first = ascending ? (lhs.key < rhs.key) : (lhs.key > rhs.key);
        return keep_mode_ == KeepMode::First ? lhs_first : !lhs_first;
    }

    template <typename K, typename KeyAt>
    auto process_grouped_pod(const Table& chunk, const KeyAt& key_at, bool ascending,
                             const std::vector<KeyCol>& group_key_cols, std::size_t rows)
        -> std::optional<std::string> {
        if (!pod_active_) {
            // One integer key, read as its raw value; fixed for the stream,
            // since a group must be found by the same index in every chunk.
            pod_int_key_ =
                group_key_cols.size() == 1 && (group_key_cols.front().kind == KeyCol::Kind::Int64 ||
                                               group_key_cols.front().kind == KeyCol::Kind::Date ||
                                               group_key_cols.front().kind == KeyCol::Kind::Ts);
        }
        pod_active_ = true;
        pod_ascending_ = ascending;
        const std::size_t workers = group_barrier_worker_count(exec_, rows);
        auto& parts = pod_partitions<K>();
        if (parts.empty()) {
            // Fixed by the first chunk: a group's partition is a function of
            // its hash and this count, for the life of the operator.
            std::size_t count = 1;
            while (count * 2 <= workers) {
                count *= 2;
            }
            parts.resize(count);
        }
        const std::size_t n_parts = parts.size();
        const std::uint64_t base = next_sequence_;
        next_sequence_ += rows;
        ++pod_chunks_;
        const auto chunk_stamp = static_cast<std::uint32_t>(pod_chunks_);

        // Every row's key hash, once. A lone non-null Categorical hashes per
        // dictionary entry, exactly as `hash_key_row` hashes its text.
        std::vector<std::uint64_t> hashes(rows);
        const auto run_ranges = [&](const auto& body) {
            if (workers < 2) {
                body(std::size_t{0}, rows);
                return;
            }
            const std::size_t grain = (rows + workers - 1) / workers;
            auto batch = process_worker_pool().submit(workers, [&](std::size_t r) {
                const std::size_t begin = std::min(rows, r * grain);
                body(begin, std::min(rows, begin + grain));
            });
            batch.wait();
        };
        const KeyCol& key0 = group_key_cols.front();
        const auto int_key = [&key0](std::size_t row) -> std::int64_t {
            switch (key0.kind) {
                case KeyCol::Kind::Date:
                    return key0.date[row].days;
                case KeyCol::Kind::Ts:
                    return key0.ts[row].nanos;
                default:
                    return key0.i64[row];
            }
        };
        // Any fixed value: a null's partition only has to be the same in
        // every chunk.
        constexpr std::uint64_t kNullHash = 0x9e3779b97f4a7c15ULL;
        if (pod_int_key_) {
            run_ranges([&](std::size_t begin, std::size_t end) {
                for (std::size_t row = begin; row < end; ++row) {
                    hashes[row] = key0.is_null(row)
                                      ? kNullHash
                                      : robin_hood::hash<std::int64_t>{}(int_key(row));
                }
            });
        } else if (group_key_cols.size() == 1 && group_key_cols.front().kind == KeyCol::Kind::Cat &&
                   group_key_cols.front().validity == nullptr) {
            const auto& cat = *group_key_cols.front().cat;
            const auto& dict = cat.dictionary();
            std::vector<std::uint64_t> code_hash(dict.size());
            for (std::size_t code = 0; code < dict.size(); ++code) {
                std::uint64_t seed = 0;
                key_hash_mix(seed, std::hash<std::string_view>{}(dict[code]));
                code_hash[code] = key_hash_finalize(seed);
            }
            const auto* codes = cat.codes_data();
            run_ranges([&](std::size_t begin, std::size_t end) {
                for (std::size_t row = begin; row < end; ++row) {
                    hashes[row] = code_hash[static_cast<std::size_t>(codes[row])];
                }
            });
        } else {
            run_ranges([&](std::size_t begin, std::size_t end) {
                for (std::size_t row = begin; row < end; ++row) {
                    hashes[row] = hash_key_row(group_key_cols, row);
                }
            });
        }

        std::vector<const ColumnEntry*> group_entries;
        group_entries.reserve(group_by_->size());
        for (const auto& ref : *group_by_) {
            group_entries.push_back(chunk.find_entry(ref.name));
        }
        const std::size_t k = count_;
        const auto worse_first = [&](const PodEntry<K>& a, const PodEntry<K>& b) {
            return pod_better(a, b, ascending);
        };
        // High hash bits pick the partition; the index probes with the low.
        const auto part_of = [n_parts](std::uint64_t hash) {
            return static_cast<std::size_t>(hash >> 40U) & (n_parts - 1);
        };
        const auto run_partition = [&](std::size_t p) {
            PodPartition<K>& part = parts[p];
            for (std::size_t row = 0; row < rows; ++row) {
                const std::uint64_t hash = hashes[row];
                if (part_of(hash) != p) {
                    continue;
                }
                const auto new_group = [&] {
                    const auto gid = static_cast<std::uint32_t>(part.sizes.size());
                    part.sizes.push_back(0);
                    part.stamp.push_back(0);
                    part.heaps.resize(part.sizes.size() * k);
                    return gid;
                };
                std::uint32_t gid = 0;
                if (pod_int_key_) {
                    if (key0.is_null(row)) {
                        if (part.null_gid == std::numeric_limits<std::uint32_t>::max()) {
                            part.null_gid = new_group();
                        }
                        gid = part.null_gid;
                    } else {
                        auto [it, fresh] = part.int_index.try_emplace(int_key(row), 0U);
                        if (fresh) {
                            it->second = new_group();
                        }
                        gid = it->second;
                    }
                } else {
                    gid = part.index.find_or_insert_hashed(
                        hash,
                        [&](std::uint32_t g) {
                            return key_equals_row(part.keys[g], group_key_cols, row);
                        },
                        [&] {
                            Key key;
                            key.values.reserve(group_entries.size());
                            for (const ColumnEntry* entry : group_entries) {
                                push_key_value(key, *entry, row);
                            }
                            part.keys.push_back(std::move(key));
                            return new_group();
                        });
                }
                const PodEntry<K> entry{key_at(row), base + row, kPodCurrentChunk | row};
                PodEntry<K>* heap = part.heaps.data() + (static_cast<std::size_t>(gid) * k);
                std::uint32_t& size = part.sizes[gid];
                if (size == k) {
                    if (!pod_better(entry, heap[0], ascending)) {
                        continue;
                    }
                    std::pop_heap(heap, heap + k, worse_first);
                    heap[k - 1] = entry;
                    std::push_heap(heap, heap + k, worse_first);
                } else {
                    heap[size] = entry;
                    ++size;
                    std::push_heap(heap, heap + size, worse_first);
                }
                if (part.stamp[gid] != chunk_stamp) {
                    part.stamp[gid] = chunk_stamp;
                    part.touched.push_back(gid);
                }
            }
        };
        if (workers >= 2 && n_parts >= 2) {
            std::atomic<std::size_t> cursor{0};
            auto batch = process_worker_pool().submit(std::min(workers, n_parts), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1); p < n_parts; p = cursor.fetch_add(1)) {
                    run_partition(p);
                }
            });
            batch.wait();
        } else {
            for (std::size_t p = 0; p < n_parts; ++p) {
                run_partition(p);
            }
        }

        // Copy the rows this chunk left in a heap into a piece, and point
        // their entries at it.
        // A row's place in the piece is its rank among the kept rows: mark,
        // then number in row order (no sort; at 5M groups nearly every row
        // is kept).
        std::vector<std::uint32_t> rank(rows, 0);
        for (auto& part : parts) {
            for (const std::uint32_t gid : part.touched) {
                const PodEntry<K>* heap = part.heaps.data() + (static_cast<std::size_t>(gid) * k);
                for (std::uint32_t i = 0; i < part.sizes[gid]; ++i) {
                    if ((heap[i].ref & kPodCurrentChunk) != 0) {
                        rank[static_cast<std::uint32_t>(heap[i].ref)] = 1;
                    }
                }
            }
        }
        std::vector<std::uint32_t> kept;
        for (std::size_t row = 0; row < rows; ++row) {
            if (rank[row] != 0) {
                rank[row] = static_cast<std::uint32_t>(kept.size());
                kept.push_back(static_cast<std::uint32_t>(row));
            }
        }
        // Mostly kept (many groups, few rows each): keep the chunk itself,
        // whose columns are shared, rather than copying nearly all of it.
        const bool whole_chunk = kept.size() * 2 >= rows;
        if (kept.empty()) {
            for (auto& part : parts) {
                part.touched.clear();
            }
            return std::nullopt;
        }
        const auto piece = static_cast<std::uint64_t>(pod_pieces_.size());
        for (auto& part : parts) {
            for (const std::uint32_t gid : part.touched) {
                PodEntry<K>* heap = part.heaps.data() + (static_cast<std::size_t>(gid) * k);
                for (std::uint32_t i = 0; i < part.sizes[gid]; ++i) {
                    if ((heap[i].ref & kPodCurrentChunk) != 0) {
                        const auto row = static_cast<std::uint32_t>(heap[i].ref);
                        heap[i].ref = (piece << 32U) | (whole_chunk ? row : rank[row]);
                    }
                }
            }
            part.touched.clear();
        }
        pod_piece_rows_ += whole_chunk ? rows : kept.size();
        pod_pieces_.push_back(whole_chunk ? chunk : gather_rows(chunk, kept));
        pod_compact_if_sparse<K>();
        return std::nullopt;
    }

    /// The live entries of every heap, in output order.
    template <typename K>
    auto pod_winners() -> std::vector<PodEntry<K>> {
        std::vector<PodEntry<K>> winners;
        for (const auto& part : pod_partitions<K>()) {
            for (std::size_t gid = 0; gid < part.sizes.size(); ++gid) {
                const PodEntry<K>* heap = part.heaps.data() + (gid * count_);
                winners.insert(winners.end(), heap, heap + part.sizes[gid]);
            }
        }
        // `row_comes_first`: the order key, then arrival. Two stable radix
        // passes, arrival first: equal keys then keep arrival order.
        const std::size_t n = winners.size();
        const std::size_t workers = group_barrier_worker_count(exec_, n);
        const auto permute = [&](const SortIdx& order) {
            std::vector<PodEntry<K>> sorted(n);
            std::visit(
                [&](const auto& idx) {
                    for (std::size_t i = 0; i < n; ++i) {
                        sorted[i] = winners[static_cast<std::size_t>(idx[i])];
                    }
                },
                order);
            winners = std::move(sorted);
        };
        std::vector<std::uint64_t> radix(n);
        for (std::size_t i = 0; i < n; ++i) {
            radix[i] = winners[i].sequence;
        }
        permute(radix_sort_u64_asc(std::move(radix), n, workers));
        radix.assign(n, 0);
        for (std::size_t i = 0; i < n; ++i) {
            std::uint64_t bits = 0;
            if constexpr (std::is_same_v<K, double>) {
                // -0.0 and 0.0 compare equal, so they must tie here too.
                const double key = winners[i].key == 0.0 ? 0.0 : winners[i].key;
                bits = double_to_sortable_u64(key);
            } else {
                bits = static_cast<std::uint64_t>(winners[i].key) ^ (std::uint64_t{1} << 63U);
            }
            radix[i] = pod_ascending_ ? bits : ~bits;
        }
        permute(radix_sort_u64_asc(std::move(radix), n, workers));
        return winners;
    }

    /// The stored pieces as one table, and each piece's first row in it.
    auto pod_concat_pieces() -> std::pair<Table, std::vector<std::size_t>> {
        std::vector<std::size_t> offsets;
        offsets.reserve(pod_pieces_.size());
        std::size_t total = 0;
        for (const auto& piece : pod_pieces_) {
            offsets.push_back(total);
            total += piece.rows();
        }
        if (pod_pieces_.size() == 1) {
            return {std::move(pod_pieces_.front()), std::move(offsets)};
        }
        std::vector<Chunk> chunks;
        chunks.reserve(pod_pieces_.size());
        for (auto& piece : pod_pieces_) {
            chunks.push_back(table_to_chunk(std::move(piece)));
        }
        auto table = materialize_operator(std::make_unique<ChunkListSource>(std::move(chunks)));
        if (!table.has_value()) {
            invariant_violation("grouped top-k: concatenating stored rows failed: " +
                                table.error());
        }
        return {std::move(*table), std::move(offsets)};
    }

    /// Once most stored rows have been displaced from every heap, keep only
    /// the live ones: worst case (each row better than the last) every row
    /// is stored once.
    template <typename K>
    void pod_compact_if_sparse() {
        std::size_t live = 0;
        for (const auto& part : pod_partitions<K>()) {
            for (const auto size : part.sizes) {
                live += size;
            }
        }
        constexpr std::size_t kMinGarbageRows = std::size_t{1} << 16;
        if (pod_pieces_.size() < 2 || pod_piece_rows_ < (2 * live) + kMinGarbageRows) {
            return;
        }
        auto [table, offsets] = pod_concat_pieces();
        std::vector<std::uint64_t> rows;
        rows.reserve(live);
        for (auto& part : pod_partitions<K>()) {
            for (std::size_t gid = 0; gid < part.sizes.size(); ++gid) {
                PodEntry<K>* heap = part.heaps.data() + (gid * count_);
                for (std::uint32_t i = 0; i < part.sizes[gid]; ++i) {
                    rows.push_back(offsets[heap[i].ref >> 32U] + (heap[i].ref & 0xffffffffU));
                    heap[i].ref = rows.size() - 1;
                }
            }
        }
        pod_pieces_.clear();
        pod_pieces_.push_back(gather_rows(table, rows));
        pod_piece_rows_ = rows.size();
    }

    template <typename K>
    auto build_pod_output() -> Table {
        const auto winners = pod_winners<K>();
        if (winners.empty()) {
            return empty_template_.value_or(Table{});
        }
        auto [table, offsets] = pod_concat_pieces();
        std::vector<std::uint64_t> rows;
        rows.reserve(winners.size());
        for (const auto& entry : winners) {
            rows.push_back(offsets[entry.ref >> 32U] + (entry.ref & 0xffffffffU));
        }
        Table out = gather_rows(table, rows);
        out.set_properties(out.properties().with_ordering(*keys_));
        return out;
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
        if (pod_active_) {
            return pod_double_.empty() ? build_pod_output<std::int64_t>()
                                       : build_pod_output<double>();
        }
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
    ExecutionContext exec_;
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
    /// `process_grouped_pod`'s state; one of the two partition sets is used,
    /// by the order key's storage.
    bool pod_active_ = false;
    bool pod_ascending_ = true;
    bool pod_int_key_ = false;
    std::size_t pod_chunks_ = 0;
    std::vector<PodPartition<std::int64_t>> pod_int_;
    std::vector<PodPartition<double>> pod_double_;
    std::vector<Table> pod_pieces_;
    std::size_t pod_piece_rows_ = 0;
};

}  // namespace

namespace physical_executor_detail {

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
    return std::make_unique<ChunkedOrderedLimitOperator>(
        std::move(child_op.value()), &topk.keys(), topk.count(), &topk.group_by(), keep, exec);
}

}  // namespace physical_executor_detail

}  // namespace ibex::runtime
