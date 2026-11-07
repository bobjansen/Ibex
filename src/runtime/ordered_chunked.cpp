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
#include <cstdlib>
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
                    } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                        handled = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            const bool bad = asc ? order_less_double(col[i], col[i - 1])
                                                 : order_less_double(col[i - 1], col[i]);
                            if (bad) {
                                sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
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
            // A null sorts after every value under asc and desc alike, as
            // `order` puts it; two nulls tie. A null slot's value is undefined.
            const bool lhs_null = lhs.key.is_null(i);
            const bool rhs_null = rhs.key.is_null(i);
            if (lhs_null != rhs_null) {
                return rhs_null;
            }
            if (lhs_null) {
                continue;
            }
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

    /// Three-way key compare for the typed heaps: the Float64 order for a
    /// double (every NaN ties and sorts last ascending), `<` otherwise. One
    /// call per input row, so it keeps the old `==`-then-`<` cost.
    template <typename T>
    [[nodiscard]] static auto key_compare(const T& lhs, const T& rhs) -> int {
        if constexpr (std::is_same_v<T, double>) {
            return order_compare_double(lhs, rhs);
        } else {
            return lhs < rhs ? -1 : (rhs < lhs ? 1 : 0);
        }
    }

    template <typename T>
    [[nodiscard]] auto single_key_better(const T& lhs, std::size_t lhs_sequence, const Entry& rhs,
                                         bool ascending) const -> bool {
        const auto* rhs_key = std::get_if<T>(&rhs.key.values.front());
        if (rhs_key == nullptr) {
            invariant_violation("ChunkedOrderedLimitOperator: single-key type mismatch");
        }
        const int cmp = key_compare(lhs, *rhs_key);
        if (cmp == 0) {
            return keep_mode_ == KeepMode::First ? (lhs_sequence < rhs.sequence)
                                                 : (rhs.sequence < lhs_sequence);
        }
        const bool lhs_first = ascending ? cmp < 0 : cmp > 0;
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
                                  const std::vector<KeyCol>& group_key_cols,
                                  const std::vector<const ColumnEntry*>& key_columns)
        -> std::optional<std::string> {
        // A null cell holds its type's zero, and the typed heaps below compare
        // raw values, so they would rank a null as a zero instead of last. The
        // POD heaps flag a null themselves (they hold their own storage, which
        // a later chunk must keep feeding); every other typed branch shares
        // the Entry heaps with the boxed path, which carries the null.
        const ValidityBitmap* key_validity =
            key_columns.front()->validity.has_value() ? &*key_columns.front()->validity : nullptr;
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
                    ascending, group_key_cols, rows, key_validity);
            }
        }
        if (key_validity != nullptr) {
            return process_boxed_chunk(chunk, key_columns, group_key_cols);
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
    static constexpr std::size_t kPodPartsPerWorker = 8;
    /// A ref of a row in the chunk being processed, not yet in a piece.
    static constexpr std::uint64_t kPodCurrentChunk = std::uint64_t{1} << 63;
    /// Set in `PodEntry::sequence` when the row's order key is null: a
    /// sequence never reaches 2^63, so the flag costs no space. A null sorts
    /// after every value under asc and desc alike, as `order` puts it.
    static constexpr std::uint64_t kPodNullKey = std::uint64_t{1} << 63;

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
        // `single_key_better`, on the flat entry, with a null key last. Either
        // flag set is rare: the common case pays one test for both.
        bool lhs_first = false;
        if (((lhs.sequence | rhs.sequence) & kPodNullKey) != 0) [[unlikely]] {
            const bool lhs_null = (lhs.sequence & kPodNullKey) != 0;
            const bool rhs_null = (rhs.sequence & kPodNullKey) != 0;
            if (lhs_null == rhs_null) {
                const std::uint64_t lhs_sequence = lhs.sequence & ~kPodNullKey;
                const std::uint64_t rhs_sequence = rhs.sequence & ~kPodNullKey;
                return keep_mode_ == KeepMode::First ? (lhs_sequence < rhs_sequence)
                                                     : (rhs_sequence < lhs_sequence);
            }
            lhs_first = rhs_null;
        } else {
            const int cmp = key_compare(lhs.key, rhs.key);
            if (cmp == 0) {
                return keep_mode_ == KeepMode::First ? (lhs.sequence < rhs.sequence)
                                                     : (rhs.sequence < lhs.sequence);
            }
            lhs_first = ascending ? cmp < 0 : cmp > 0;
        }
        return keep_mode_ == KeepMode::First ? lhs_first : !lhs_first;
    }

    static constexpr std::uint32_t kPodNoGid = std::numeric_limits<std::uint32_t>::max();

    /// Decide, for a chunk, whether its single partition finds groups by
    /// `key - lo` (`pod_dense_`). On one thread the probe was the whole of
    /// h2o q8 -- a hash-map lookup per row, most rows then rejected by one
    /// comparison with the group's worst kept entry -- and `id6`'s values
    /// fill their range. Only the first chunk may start it, since the map
    /// would not know its groups; a later chunk that widens the range
    /// rebuilds the array, and one that widens it too far moves the groups
    /// into `int_index` for good.
    template <typename K, typename IntKey>
    void prepare_pod_dense(PodPartition<K>& part, const KeyCol& key0, const IntKey& int_key,
                           std::size_t rows) {
        constexpr std::uint64_t kMaxCells = std::uint64_t{1} << 26;
        if (pod_dense_retired_ || (!pod_dense_ && !part.sizes.empty()) ||
            std::getenv("IBEX_DISABLE_DENSE_TOPK_KEY") != nullptr) {
            pod_dense_retired_ = true;
            return;
        }
        std::int64_t lo = std::numeric_limits<std::int64_t>::max();
        std::int64_t hi = std::numeric_limits<std::int64_t>::min();
        for (std::size_t row = 0; row < rows; ++row) {
            if (!key0.is_null(row)) {
                const std::int64_t v = int_key(row);
                lo = std::min(lo, v);
                hi = std::max(hi, v);
            }
        }
        if (lo > hi) {
            return;  // every key null: nothing to place yet
        }
        if (pod_dense_cells_are_gids_) {
            const std::int64_t old_hi =
                pod_dense_lo_ + static_cast<std::int64_t>(pod_dense_cells_) - 1;
            if (lo >= pod_dense_lo_ && hi <= old_hi) {
                return;
            }
            // The cells cannot move, so the groups move into the map, keeping
            // their ids; a cell no row reached stays an empty group.
            for (std::size_t cell = 0; cell < pod_dense_cells_; ++cell) {
                if (part.sizes[cell] != 0) {
                    part.int_index.emplace(pod_dense_lo_ + static_cast<std::int64_t>(cell),
                                           static_cast<std::uint32_t>(cell));
                }
            }
            pod_dense_ = false;
            pod_dense_cells_are_gids_ = false;
            pod_dense_retired_ = true;
            return;
        }
        if (pod_dense_) {
            const std::int64_t old_hi =
                pod_dense_lo_ + static_cast<std::int64_t>(pod_dense_gid_.size()) - 1;
            lo = std::min(lo, pod_dense_lo_);
            hi = std::max(hi, old_hi);
        }
        const std::uint64_t span =
            static_cast<std::uint64_t>(hi) - static_cast<std::uint64_t>(lo) + 1;
        const bool fits = span != 0 && span <= kMaxCells &&
                          (pod_dense_ || span <= (std::uint64_t{4} * rows) + 1024);
        if (!fits) {
            if (pod_dense_) {
                for (std::size_t cell = 0; cell < pod_dense_gid_.size(); ++cell) {
                    if (pod_dense_gid_[cell] != kPodNoGid) {
                        part.int_index.emplace(pod_dense_lo_ + static_cast<std::int64_t>(cell),
                                               pod_dense_gid_[cell]);
                    }
                }
                std::vector<std::uint32_t>().swap(pod_dense_gid_);
                pod_dense_ = false;
            }
            pod_dense_retired_ = true;
            return;
        }
        if (!pod_dense_ && span <= rows) {
            // At most a cell a row: make every cell's group now.
            pod_dense_lo_ = lo;
            pod_dense_ = true;
            pod_dense_cells_are_gids_ = true;
            pod_dense_cells_ = static_cast<std::size_t>(span);
            part.sizes.assign(pod_dense_cells_, 0);
            part.stamp.assign(pod_dense_cells_, 0);
            part.heaps.resize(pod_dense_cells_ * count_);
            return;
        }
        if (!pod_dense_) {
            pod_dense_gid_.assign(static_cast<std::size_t>(span), kPodNoGid);
            pod_dense_lo_ = lo;
            pod_dense_ = true;
            // The cells bound the groups, so grow the per-group arrays once:
            // at 5M groups their doubling copied the heaps (24 bytes an
            // entry) over and over. Capacity alone touches no pages.
            const std::size_t groups = std::min(static_cast<std::size_t>(span), rows);
            part.sizes.reserve(groups);
            part.stamp.reserve(groups);
            part.touched.reserve(groups);
            part.heaps.reserve(groups * count_);
            return;
        }
        if (lo == pod_dense_lo_ && span == pod_dense_gid_.size()) {
            return;
        }
        std::vector<std::uint32_t> grown(static_cast<std::size_t>(span), kPodNoGid);
        const auto shift = static_cast<std::size_t>(pod_dense_lo_ - lo);
        std::copy(pod_dense_gid_.begin(), pod_dense_gid_.end(),
                  grown.begin() + static_cast<std::ptrdiff_t>(shift));
        pod_dense_gid_ = std::move(grown);
        pod_dense_lo_ = lo;
    }

    template <typename K, typename KeyAt>
    auto process_grouped_pod(const Table& chunk, const KeyAt& key_at, bool ascending,
                             const std::vector<KeyCol>& group_key_cols, std::size_t rows,
                             const ValidityBitmap* key_validity) -> std::optional<std::string> {
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
            // Several per worker: each partition's index and heaps are a
            // fraction of the groups (h2o q8 at K=2: 5M), small enough to stay
            // in cache, and partitions are claimed dynamically.
            std::size_t count = 1;
            while (count * 2 <= workers) {
                count *= 2;
            }
            parts.resize(count == 1 ? 1 : count * kPodPartsPerWorker);
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
        // Every row to its partition, in row order within each, so a
        // partition's worker reads only its own rows: each scanning all of
        // them was 8x the hash reads, and at more partitions far worse.
        std::vector<std::uint32_t> part_rows(rows);
        std::vector<std::size_t> part_begin(n_parts + 1, 0);
        // With one integer group key, what the probe reads of a row travels
        // with it: the key, the order key and both null flags, so a partition
        // reads its own contiguous slice. Read through the row instead, every
        // partition touched a line per row of three row-indexed arrays that
        // its neighbours' rows share -- each line fetched up to once per
        // partition (h2o q8 at K=2: the probe was ~190 ms).
        const bool carry = pod_int_key_ && n_parts > 1;
        std::vector<std::int64_t> carried_key;
        std::vector<K> carried_value;
        std::vector<std::uint8_t> carried_nulls;  ///< bit 0 group key, bit 1 order key
        if (carry) {
            carried_key.resize(rows);
            carried_value.resize(rows);
            carried_nulls.resize(rows);
        }
        if (n_parts == 1) {
            for (std::size_t row = 0; row < rows; ++row) {
                part_rows[row] = static_cast<std::uint32_t>(row);
            }
            part_begin[1] = rows;
        } else {
            const std::size_t ranges = std::max<std::size_t>(workers, 1);
            const std::size_t grain = (rows + ranges - 1) / ranges;
            std::vector<std::size_t> counts(ranges * n_parts, 0);
            const auto each_range = [&](const auto& body) {
                if (ranges < 2) {
                    body(std::size_t{0});
                    return;
                }
                auto batch = process_worker_pool().submit(ranges, body);
                batch.wait();
            };
            each_range([&](std::size_t r) {
                const std::size_t begin = std::min(rows, r * grain);
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* local = counts.data() + (r * n_parts);
                for (std::size_t row = begin; row < end; ++row) {
                    ++local[part_of(hashes[row])];
                }
            });
            std::size_t total = 0;
            for (std::size_t p = 0; p < n_parts; ++p) {
                part_begin[p] = total;
                for (std::size_t r = 0; r < ranges; ++r) {
                    const std::size_t c = counts[(r * n_parts) + p];
                    counts[(r * n_parts) + p] = total;
                    total += c;
                }
            }
            part_begin[n_parts] = total;
            each_range([&](std::size_t r) {
                const std::size_t begin = std::min(rows, r * grain);
                const std::size_t end = std::min(rows, begin + grain);
                std::size_t* next = counts.data() + (r * n_parts);
                for (std::size_t row = begin; row < end; ++row) {
                    const std::size_t at = next[part_of(hashes[row])]++;
                    part_rows[at] = static_cast<std::uint32_t>(row);
                    if (carry) {
                        const bool group_null = key0.is_null(row);
                        const bool order_null = key_validity != nullptr && !(*key_validity)[row];
                        carried_key[at] = group_null ? 0 : int_key(row);
                        carried_value[at] = order_null ? K{} : key_at(row);
                        carried_nulls[at] = static_cast<std::uint8_t>((group_null ? 1U : 0U) |
                                                                      (order_null ? 2U : 0U));
                    }
                }
            });
        }
        if (pod_int_key_ && n_parts == 1) {
            prepare_pod_dense(parts.front(), key0, int_key, rows);
        } else {
            pod_dense_retired_ = true;
        }
        const bool dense = pod_dense_;
        const bool cells_are_gids = pod_dense_cells_are_gids_;
        // Which groups this chunk changed is tracked per row (`stamp`,
        // `touched`) only when there are many more groups than rows; else
        // the bookkeeping below visits every group, which costs less than a
        // random `stamp` access per row (h2o q8 at K=2 on one thread: one
        // chunk, 5M groups).
        std::size_t groups_before = 0;
        for (const auto& part : parts) {
            groups_before += part.sizes.size();
        }
        const bool track = groups_before > 3 * rows;
        const auto run_partition = [&](std::size_t p) {
            PodPartition<K>& part = parts[p];
            // Once a sample of this partition's rows shows most start groups
            // (h2o q8 at K=2: two rows a group), size its index and heaps for
            // the rest at that rate rather than regrow and rehash on the way.
            constexpr std::size_t kReserveSample = 1024;
            const std::size_t sample_end = part_begin[p] + kReserveSample;
            const std::size_t groups_at_start = part.sizes.size();
            for (std::size_t at = part_begin[p]; at < part_begin[p + 1]; ++at) {
                if (at == sample_end && part_begin[p + 1] - at > 4 * kReserveSample) {
                    const std::size_t fresh = part.sizes.size() - groups_at_start;
                    if (fresh * 2 > kReserveSample) {
                        const std::size_t total =
                            part.sizes.size() + ((part_begin[p + 1] - at) * fresh / kReserveSample);
                        if (pod_int_key_) {
                            part.int_index.reserve(total);
                        }
                        part.sizes.reserve(total);
                        part.stamp.reserve(total);
                        part.touched.reserve(total);
                        part.heaps.reserve(total * k);
                    }
                }
                const std::size_t row = part_rows[at];
                if (cells_are_gids) {
                    // A row's group is its key, so fetch the group's size and
                    // heap a few rows ahead: at 5M groups each is a cache
                    // miss, and this loop otherwise waits on them one by one.
                    constexpr std::size_t kAhead = 16;
                    if (at + kAhead < part_begin[p + 1]) {
                        const std::size_t ahead = part_rows[at + kAhead];
                        if (!key0.is_null(ahead)) {
                            const auto cell = static_cast<std::size_t>(
                                static_cast<std::uint64_t>(int_key(ahead)) -
                                static_cast<std::uint64_t>(pod_dense_lo_));
                            ::ibex::detail::prefetch(part.sizes.data() + cell);
                            ::ibex::detail::prefetch(part.heaps.data() + (cell * k));
                        }
                    }
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
                    if (carry ? (carried_nulls[at] & 1U) != 0 : key0.is_null(row)) {
                        if (part.null_gid == std::numeric_limits<std::uint32_t>::max()) {
                            part.null_gid = new_group();
                        }
                        gid = part.null_gid;
                    } else if (cells_are_gids) {
                        gid = static_cast<std::uint32_t>(static_cast<std::uint64_t>(int_key(row)) -
                                                         static_cast<std::uint64_t>(pod_dense_lo_));
                    } else if (dense) {
                        std::uint32_t& slot = pod_dense_gid_[static_cast<std::size_t>(
                            static_cast<std::uint64_t>(int_key(row)) -
                            static_cast<std::uint64_t>(pod_dense_lo_))];
                        if (slot == kPodNoGid) {
                            slot = new_group();
                        }
                        gid = slot;
                    } else {
                        auto [it, fresh] =
                            part.int_index.try_emplace(carry ? carried_key[at] : int_key(row), 0U);
                        if (fresh) {
                            it->second = new_group();
                        }
                        gid = it->second;
                    }
                } else {
                    gid = part.index.find_or_insert_hashed(
                        hashes[row],
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
                const bool null_key = carry ? (carried_nulls[at] & 2U) != 0
                                            : key_validity != nullptr && !(*key_validity)[row];
                const PodEntry<K> entry{null_key ? K{}
                                        : carry  ? carried_value[at]
                                                 : key_at(row),
                                        (base + row) | (null_key ? kPodNullKey : 0),
                                        kPodCurrentChunk | row};
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
                if (track && part.stamp[gid] != chunk_stamp) {
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

        if (!track) {
            // Every group, as `touched` would hold had each been stamped.
            const auto all_groups = [&](std::size_t p) {
                auto& part = parts[p];
                part.touched.resize(part.sizes.size());
                for (std::size_t gid = 0; gid < part.sizes.size(); ++gid) {
                    part.touched[gid] = static_cast<std::uint32_t>(gid);
                }
            };
            if (workers >= 2 && n_parts >= 2) {
                std::atomic<std::size_t> cursor{0};
                auto batch =
                    process_worker_pool().submit(std::min(workers, n_parts), [&](std::size_t) {
                        for (std::size_t p = cursor.fetch_add(1); p < n_parts;
                             p = cursor.fetch_add(1)) {
                            all_groups(p);
                        }
                    });
                batch.wait();
            } else {
                for (std::size_t p = 0; p < n_parts; ++p) {
                    all_groups(p);
                }
            }
        }
        // Copy the rows this chunk left in a heap into a piece, and point
        // their entries at it.
        // A row's place in the piece is its rank among the kept rows: mark,
        // then number in row order (no sort; at 5M groups nearly every row
        // is kept).
        // Each pass below is per partition or per row range: at 5M groups
        // nearly every row is kept, and serially these were ~100 ms.
        const auto each_part = [&](const auto& body) {
            if (workers < 2 || n_parts < 2) {
                for (std::size_t p = 0; p < n_parts; ++p) {
                    body(p);
                }
                return;
            }
            std::atomic<std::size_t> cursor{0};
            auto batch = process_worker_pool().submit(std::min(workers, n_parts), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1); p < n_parts; p = cursor.fetch_add(1)) {
                    body(p);
                }
            });
            batch.wait();
        };
        // How many of this chunk's rows a heap still holds. When that is
        // most of them the chunk is kept whole and a row is its own place,
        // so the ranks below are not needed.
        std::vector<std::size_t> part_kept(n_parts, 0);
        each_part([&](std::size_t p) {
            const auto& part = parts[p];
            std::size_t count = 0;
            for (const std::uint32_t gid : part.touched) {
                const PodEntry<K>* heap = part.heaps.data() + (static_cast<std::size_t>(gid) * k);
                for (std::uint32_t i = 0; i < part.sizes[gid]; ++i) {
                    count += (heap[i].ref & kPodCurrentChunk) != 0 ? 1 : 0;
                }
            }
            part_kept[p] = count;
        });
        std::size_t kept_rows = 0;
        for (const std::size_t count : part_kept) {
            kept_rows += count;
        }
        if (kept_rows == 0) {
            for (auto& part : parts) {
                part.touched.clear();
            }
            return std::nullopt;
        }
        // Mostly kept (many groups, few rows each): keep the chunk itself,
        // whose columns are shared, rather than copying nearly all of it.
        const bool whole_chunk = kept_rows * 2 >= rows;
        std::vector<std::uint32_t> rank(whole_chunk ? 0 : rows, 0);
        std::vector<std::uint32_t> kept;
        if (!whole_chunk) {
            each_part([&](std::size_t p) {
                const auto& part = parts[p];
                for (const std::uint32_t gid : part.touched) {
                    const PodEntry<K>* heap =
                        part.heaps.data() + (static_cast<std::size_t>(gid) * k);
                    for (std::uint32_t i = 0; i < part.sizes[gid]; ++i) {
                        if ((heap[i].ref & kPodCurrentChunk) != 0) {
                            rank[static_cast<std::uint32_t>(heap[i].ref)] = 1;
                        }
                    }
                }
            });
            // A kept row's rank is its place among the kept rows, in row order.
            {
                const std::size_t ranges = std::max<std::size_t>(workers, 1);
                const std::size_t grain = (rows + ranges - 1) / ranges;
                std::vector<std::size_t> first(ranges + 1, 0);
                const auto each_range = [&](const auto& body) {
                    if (ranges < 2) {
                        body(std::size_t{0});
                        return;
                    }
                    auto batch = process_worker_pool().submit(ranges, body);
                    batch.wait();
                };
                each_range([&](std::size_t r) {
                    const std::size_t begin = std::min(rows, r * grain);
                    const std::size_t end = std::min(rows, begin + grain);
                    std::size_t n = 0;
                    for (std::size_t row = begin; row < end; ++row) {
                        n += rank[row] != 0 ? 1 : 0;
                    }
                    first[r + 1] = n;
                });
                for (std::size_t r = 0; r < ranges; ++r) {
                    first[r + 1] += first[r];
                }
                kept.resize(first[ranges]);
                each_range([&](std::size_t r) {
                    const std::size_t begin = std::min(rows, r * grain);
                    const std::size_t end = std::min(rows, begin + grain);
                    auto at = static_cast<std::uint32_t>(first[r]);
                    for (std::size_t row = begin; row < end; ++row) {
                        if (rank[row] != 0) {
                            rank[row] = at;
                            kept[at++] = static_cast<std::uint32_t>(row);
                        }
                    }
                });
            }
        }
        const auto piece = static_cast<std::uint64_t>(pod_pieces_.size());
        each_part([&](std::size_t p) {
            auto& part = parts[p];
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
        });
        pod_piece_rows_ += whole_chunk ? rows : kept.size();
        pod_pieces_.push_back(whole_chunk ? chunk : gather_rows(chunk, kept));
        pod_compact_if_sparse<K>();
        return std::nullopt;
    }

    /// The live entries' refs, in output order: `row_comes_first`, the order
    /// key then arrival, with null keys last. Two stable radix passes over
    /// indexes (arrival first, then the key gathered in that order), composed
    /// at the end, rather than permuting the 24-byte entries after each; every
    /// pass is split across workers. At h2o q8's K=2 there are 7.3M winners,
    /// and the serial collection and permutes were ~300 ms.
    template <typename K>
    auto pod_winners() -> std::vector<std::uint64_t> {
        auto& parts = pod_partitions<K>();
        const std::size_t n_parts = parts.size();
        std::vector<std::size_t> part_first(n_parts + 1, 0);
        for (std::size_t p = 0; p < n_parts; ++p) {
            std::size_t live = 0;
            for (const auto size : parts[p].sizes) {
                live += size;
            }
            part_first[p + 1] = part_first[p] + live;
        }
        const std::size_t n = part_first[n_parts];
        const std::size_t workers = group_barrier_worker_count(exec_, n);
        std::vector<std::uint64_t> sequence(n);
        std::vector<std::uint64_t> key_bits(n);
        std::vector<std::uint64_t> refs(n);
        std::vector<std::uint8_t> null_key(n);
        std::atomic<bool> any_null{false};
        const auto each_part = [&](const auto& body) {
            if (workers < 2 || n_parts < 2) {
                for (std::size_t p = 0; p < n_parts; ++p) {
                    body(p);
                }
                return;
            }
            std::atomic<std::size_t> cursor{0};
            auto batch = process_worker_pool().submit(std::min(workers, n_parts), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1); p < n_parts; p = cursor.fetch_add(1)) {
                    body(p);
                }
            });
            batch.wait();
        };
        each_part([&](std::size_t p) {
            const auto& part = parts[p];
            std::size_t at = part_first[p];
            bool nulls = false;
            for (std::size_t gid = 0; gid < part.sizes.size(); ++gid) {
                const PodEntry<K>* heap = part.heaps.data() + (gid * count_);
                for (std::uint32_t i = 0; i < part.sizes[gid]; ++i, ++at) {
                    const PodEntry<K>& entry = heap[i];
                    sequence[at] = entry.sequence & ~kPodNullKey;
                    const bool null = (entry.sequence & kPodNullKey) != 0;
                    nulls = nulls || null;
                    null_key[at] = null ? 1 : 0;
                    std::uint64_t bits = 0;
                    if constexpr (std::is_same_v<K, double>) {
                        bits = double_to_sortable_u64(entry.key);
                    } else {
                        bits = static_cast<std::uint64_t>(entry.key) ^ (std::uint64_t{1} << 63U);
                    }
                    key_bits[at] = pod_ascending_ ? bits : ~bits;
                    refs[at] = entry.ref;
                }
            }
            if (nulls) {
                any_null.store(true, std::memory_order_relaxed);
            }
        });
        // `order[i]` indexes the entries; each pass sorts by one more
        // significant key, stably, so equal keys keep the order before it.
        std::vector<std::uint64_t> order(n);
        for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i) {
                order[i] = i;
            }
        });
        const auto pass = [&](const std::vector<std::uint64_t>& key) {
            std::vector<std::uint64_t> gathered(n);
            for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
                for (std::size_t i = begin; i < end; ++i) {
                    gathered[i] = key[order[i]];
                }
            });
            const SortIdx sorted = radix_sort_u64_asc(std::move(gathered), n, workers);
            std::vector<std::uint64_t> next(n);
            std::visit(
                [&](const auto& idx) {
                    for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
                        for (std::size_t i = begin; i < end; ++i) {
                            next[i] = order[static_cast<std::size_t>(idx[i])];
                        }
                    });
                },
                sorted);
            order = std::move(next);
        };
        // Arrival first. Sequences are distinct row numbers below
        // `next_sequence_`, so when the winners are a fair share of the rows
        // seen, placing each at its sequence and reading the places in order
        // sorts them outright, with no radix pass.
        const std::uint64_t span = next_sequence_;
        if (n != 0 && span <= (std::uint64_t{4} * n) + 4096 &&
            span < std::numeric_limits<std::uint32_t>::max()) {
            constexpr std::uint32_t kEmpty = std::numeric_limits<std::uint32_t>::max();
            const auto places = static_cast<std::size_t>(span);
            std::vector<std::uint32_t> at_sequence(places);
            for_row_ranges(&exec_, places, [&](std::size_t begin, std::size_t end) {
                std::fill(at_sequence.begin() + static_cast<std::ptrdiff_t>(begin),
                          at_sequence.begin() + static_cast<std::ptrdiff_t>(end), kEmpty);
            });
            for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
                for (std::size_t i = begin; i < end; ++i) {
                    at_sequence[static_cast<std::size_t>(sequence[i])] =
                        static_cast<std::uint32_t>(i);
                }
            });
            const std::size_t ranges = std::max<std::size_t>(workers, 1);
            const std::size_t grain = (places + ranges - 1) / ranges;
            std::vector<std::size_t> first(ranges + 1, 0);
            const auto each_range = [&](const auto& body) {
                if (ranges < 2) {
                    body(std::size_t{0});
                    return;
                }
                auto batch = process_worker_pool().submit(ranges, body);
                batch.wait();
            };
            each_range([&](std::size_t r) {
                const std::size_t begin = std::min(places, r * grain);
                const std::size_t end = std::min(places, begin + grain);
                std::size_t count = 0;
                for (std::size_t q = begin; q < end; ++q) {
                    count += at_sequence[q] != kEmpty ? 1 : 0;
                }
                first[r + 1] = count;
            });
            for (std::size_t r = 0; r < ranges; ++r) {
                first[r + 1] += first[r];
            }
            each_range([&](std::size_t r) {
                const std::size_t begin = std::min(places, r * grain);
                const std::size_t end = std::min(places, begin + grain);
                std::size_t out = first[r];
                for (std::size_t q = begin; q < end; ++q) {
                    if (at_sequence[q] != kEmpty) {
                        order[out++] = at_sequence[q];
                    }
                }
            });
        } else {
            pass(sequence);
        }
        pass(key_bits);
        if (any_null.load(std::memory_order_relaxed)) {
            // Most significant of all: null keys last, the passes above kept
            // them in (key, arrival) order and a null's key is a placeholder.
            std::vector<std::uint64_t> nulls(n);
            for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
                for (std::size_t i = begin; i < end; ++i) {
                    nulls[i] = null_key[i];
                }
            });
            pass(nulls);
        }
        std::vector<std::uint64_t> out(n);
        for_row_ranges(&exec_, n, [&](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i) {
                out[i] = refs[order[i]];
            }
        });
        return out;
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
        std::vector<std::size_t> rows(winners.size());
        for_row_ranges(&exec_, rows.size(), [&](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i) {
                rows[i] = offsets[winners[i] >> 32U] + (winners[i] & 0xffffffffU);
            }
        });
        // Across workers: the winners are in order-key order, so this is a
        // random gather of every output row (7.3M at h2o q8's K=2), bound by
        // latency rather than bandwidth -- serially it was ~90 ms.
        Table out = permute_table_rows(table, rows, *keys_, exec_);
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
            cat_gid_memo_.assign(group_key_cols.front().cat->dictionary_size(), kNoCatGid);
        }

        std::vector<const ColumnEntry*> key_columns;
        key_columns.reserve(keys_->size());
        for (const auto& key : *keys_) {
            const auto* column = chunk.find_entry(key.name);
            if (column == nullptr) {
                return "order column not found: " + key.name +
                       " (available: " + format_columns(chunk) + ")";
            }
            key_columns.push_back(column);
        }

        if (keys_->size() == 1) {
            const bool ascending = keys_->front().ascending;
            const ColumnValue& key_column = *key_columns.front()->column;
            if (const auto* col = std::get_if<Column<std::int64_t>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols,
                                                key_columns);
            }
            if (const auto* col = std::get_if<Column<double>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols,
                                                key_columns);
            }
            if (const auto* col = std::get_if<Column<bool>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols,
                                                key_columns);
            }
            if (const auto* col = std::get_if<Column<Date>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols,
                                                key_columns);
            }
            if (const auto* col = std::get_if<Column<Timestamp>>(&key_column)) {
                return process_single_key_chunk(chunk, *col, ascending, group_key_cols,
                                                key_columns);
            }
        }

        return process_boxed_chunk(chunk, key_columns, group_key_cols);
    }

    /// Every row boxed into a `Key` that carries its nulls: any key count or
    /// type, and the one path a null-bearing key takes into the Entry heaps.
    auto process_boxed_chunk(const Table& chunk, const std::vector<const ColumnEntry*>& key_columns,
                             const std::vector<KeyCol>& group_key_cols)
        -> std::optional<std::string> {
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
                push_key_value(entry.key, *column, row);
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
    /// One partition and one integer key whose values fill a small range: a
    /// group is found at `pod_dense_gid_[key - pod_dense_lo_]` rather than
    /// through `int_index` (see `prepare_pod_dense`). Off for good once a
    /// chunk's keys do not fit.
    bool pod_dense_ = false;
    /// The dense cells ARE the gids: group `key - lo` was made up front, so a
    /// row needs no gid lookup at all (`pod_dense_gid_` stays empty).
    bool pod_dense_cells_are_gids_ = false;
    std::size_t pod_dense_cells_ = 0;
    bool pod_dense_retired_ = false;
    std::int64_t pod_dense_lo_ = 0;
    std::vector<std::uint32_t> pod_dense_gid_;
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
