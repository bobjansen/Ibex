// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

/// Column names of `table`, in order — the input form expected by
/// `ir::plan_join_output()`.
[[nodiscard]] inline auto table_column_names(const Table& table) -> std::vector<std::string_view> {
    std::vector<std::string_view> names;
    names.reserve(table.columns.size());
    for (const auto& entry : table.columns) {
        names.emplace_back(entry.name);
    }
    return names;
}

using PredicateMaskEvaluator = std::expected<Mask, std::string> (*)(const ir::Expr& predicate,
                                                                    const Table& table,
                                                                    const ScalarRegistry* scalars,
                                                                    RowRange rows);

/// `pending_order` is what an `order` directly above this join will ask for
/// (`ir::JoinNode::pending_order`), or empty. It can only shift which side is
/// indexed, never the rows or their names -- see `choose_build_side` in
/// join.cpp for the trade.
[[nodiscard]] auto join_table_impl(
    const Table& left, const Table& right, ir::JoinKind kind, const std::vector<ir::JoinKey>& keys,
    const ir::Expr* predicate, const ScalarRegistry* scalars, PredicateMaskEvaluator mask_evaluator,
    const ir::JoinSuffixPolicy& suffix = {}, const std::vector<ir::OrderKey>& pending_order = {},
    ir::NullMatch null_match = ir::NullMatch::Never, const ir::JoinExpect& expect = {},
    ir::MatchSelection take = ir::MatchSelection::All, const ExecutionContext* exec = nullptr)
    -> std::expected<Table, std::string>;

/// Which column the fused left-join aggregate should read, given the `Update`
/// nodes between the aggregate and the join, ordered aggregate-first. Returns
/// nullopt when the rewrite must decline.
///
/// The rewrite aggregates the JOIN's output directly and never builds the
/// nodes between, so an update that *computes* a value the aggregate reads
/// cannot be waved through: its column does not exist down there, and the
/// column it was derived from holds different values. Exactly one update shape
/// is safe, and it is the one this fast path exists for -- `count(col)` lowers
/// to `Update{alias = Int64(col is not null)} + sum(alias)`, a 0/1 flag whose
/// grouped sum is what `left_join_count_table` computes from the join
/// structure itself. Anything else declines, and the ordinary path runs the
/// whole chain.
[[nodiscard]] auto fused_left_join_counted_column(
    const ir::AggregateNode& aggregate, std::span<const ir::UpdateNode* const> skipped_updates)
    -> std::optional<std::string>;

/// The Join+Aggregate fusion, resolved from the aggregate alone: walk past the
/// Project/Update nodes between them, find the join, and check every clause the
/// rewrite needs. `nullopt` when this is not that shape.
///
/// One definition for a seven-clause predicate that was written out twice --
/// chunked.cpp and interpreter.cpp -- along with the skip-walk feeding it. The
/// copies had already drifted: one gated on the aggregations being streamable
/// and the other did not. Harmless, because a single Count/Sum is streamable by
/// definition, which is exactly how this kind of divergence survives long
/// enough to matter.
struct FusedLeftJoinCount {
    /// The join whose output the aggregate consumes. Naming it is what makes
    /// this a fusion in the plan rather than a walk rediscovered in a builder:
    /// two logical nodes, one physical step.
    const ir::JoinNode* join = nullptr;
    /// The column the count/sum reads, resolved back through any updates
    /// between the aggregate and the join.
    std::string counted_column;
    /// The join-output column the group key copies, resolved the same way:
    /// the aggregate's own key name unless a plain copy renamed it (the
    /// Polars adapter renames every join column through a `select`).
    std::string group_source;
};

[[nodiscard]] auto plan_fused_left_join_count(const ir::AggregateNode& aggregate)
    -> std::optional<FusedLeftJoinCount>;

/// The counts behind the Join+Aggregate fusion: one per (unique) left key.
///
/// A left side whose keys cover a small, dense range -- TPC-H q13's 1.5M
/// `c_custkey` are 1..1.5M -- is addressed directly, `key - lo`: no map to
/// build, and a right row costs a range check and one add instead of a hash
/// probe. On q13 the serial map build alone was 48 ms. The density rule is
/// `densify_int_heads`': a range at most eight times the key count, plus slack
/// for small sides. Sparser keys keep the map, which names each key's left row.
class LeftKeyCounts {
   public:
    /// False when the left keys repeat: the fusion needs one output row per key.
    [[nodiscard]] auto prepare(const Column<std::int64_t>& keys, std::size_t rows) -> bool {
        if (rows == 0) {
            return true;
        }
        std::int64_t lo = keys[0];
        std::int64_t hi = keys[0];
        for (std::size_t row = 1; row < rows; ++row) {
            lo = std::min(lo, keys[row]);
            hi = std::max(hi, keys[row]);
        }
        const std::uint64_t span = static_cast<std::uint64_t>(hi) - static_cast<std::uint64_t>(lo);
        if (span < (std::uint64_t{8} * rows) + 1024) {
            dense_ = true;
            lo_ = lo;
            span_ = span;
            std::vector<std::uint8_t> seen(static_cast<std::size_t>(span) + 1, 0);
            for (std::size_t row = 0; row < rows; ++row) {
                auto& mark = seen[slot_of(keys[row])];
                if (mark != 0) {
                    return false;
                }
                mark = 1;
            }
            counts_ = std::vector<std::atomic<std::int64_t>>(static_cast<std::size_t>(span) + 1);
        } else {
            slots_.reserve(rows);
            for (std::size_t row = 0; row < rows; ++row) {
                if (!slots_.emplace(keys[row], row).second) {
                    return false;
                }
            }
            counts_ = std::vector<std::atomic<std::int64_t>>(rows);
        }
        for (auto& count : counts_) {
            count.store(0, std::memory_order_relaxed);
        }
        return true;
    }

    /// Count right rows `[begin, end)`: each whose key matches a left key adds
    /// one, unless the key is null (it matches nothing) or `counted_validity`
    /// says the counted column is null there. `Atomic` when other threads count
    /// at the same time; a lone counter adds without the locked instruction.
    template <bool Atomic>
    void count(const std::int64_t* keys, const ValidityBitmap* key_validity,
               const ValidityBitmap* counted_validity, std::size_t begin, std::size_t end) {
        for (std::size_t row = begin; row < end; ++row) {
            if ((key_validity != nullptr && !(*key_validity)[row]) ||
                (counted_validity != nullptr && !(*counted_validity)[row])) {
                continue;
            }
            if (dense_) {
                const std::uint64_t slot =
                    static_cast<std::uint64_t>(keys[row]) - static_cast<std::uint64_t>(lo_);
                if (slot <= span_) {
                    add<Atomic>(counts_[static_cast<std::size_t>(slot)]);
                }
            } else if (const auto it = slots_.find(keys[row]); it != slots_.end()) {
                add<Atomic>(counts_[it->second]);
            }
        }
    }

    /// Each left row's count, in left row order. After every `count` returned.
    [[nodiscard]] auto finish(const std::int64_t* keys, std::size_t rows) const
        -> Column<std::int64_t> {
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        for (std::size_t row = 0; row < rows; ++row) {
            const std::size_t slot = dense_ ? slot_of(keys[row]) : row;
            out[row] = counts_[slot].load(std::memory_order_relaxed);
        }
        return out;
    }

   private:
    template <bool Atomic>
    static void add(std::atomic<std::int64_t>& count) noexcept {
        if constexpr (Atomic) {
            count.fetch_add(1, std::memory_order_relaxed);
        } else {
            count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
        }
    }

    [[nodiscard]] auto slot_of(std::int64_t key) const noexcept -> std::size_t {
        return static_cast<std::size_t>(static_cast<std::uint64_t>(key) -
                                        static_cast<std::uint64_t>(lo_));
    }

    bool dense_ = false;
    std::int64_t lo_ = 0;
    std::uint64_t span_ = 0;
    robin_hood::unordered_flat_map<std::int64_t, std::size_t> slots_;
    std::vector<std::atomic<std::int64_t>> counts_;
};

/// Physical rewrite for a unique-left-key left join followed by
/// count(right_column) grouped by that key: built over the materialized left
/// side, then fed the right side a chunk at a time, so the right side is
/// never assembled into a table.
class LeftJoinCounter {
   public:
    /// `nullopt` when the shape or the left side rules the fusion out: a
    /// repeated left key, or a key that is not a null-free Int64. The ordinary
    /// join must be used then.
    [[nodiscard]] static auto prepare(const ir::JoinNode& join, const ir::AggregateNode& aggregate,
                                      const Table& left, std::string_view counted_column,
                                      std::string_view group_source)
        -> std::optional<LeftJoinCounter>;

    /// Whether a right chunk can be counted: the right key is Int64 and the
    /// counted column is present. A stream's schema is stable, so its first
    /// chunk answers for all of them.
    [[nodiscard]] auto accepts(const Chunk& right) const -> bool;

    /// Count a chunk on this thread alone. False when it is not accepted.
    [[nodiscard]] auto count(const Chunk& right) -> bool;

    /// Count a chunk across workers (one large chunk). False when it is not
    /// accepted.
    [[nodiscard]] auto count_parallel(const Chunk& right, const ExecutionContext* exec) -> bool;

    /// The aggregate's output: the left keys in left row order, and their counts.
    [[nodiscard]] auto finish() const -> Table;

   private:
    LeftJoinCounter() = default;

    auto columns_of(const Chunk& right, const ColumnEntry*& key, const ColumnEntry*& counted) const
        -> bool;

    LeftKeyCounts counts_;
    std::shared_ptr<const ColumnValue> left_keys_;
    std::string group_key_;
    std::string alias_;
    std::string right_key_;
    std::string counted_;
};

}  // namespace ibex::runtime
