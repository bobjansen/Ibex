// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// semi_anti_join.cpp — `ChunkedSemiAntiJoinOperator`: the streaming single-key
// `nulls never` semi/anti join. Split out of join_chunked.cpp (which was
// approaching the size chunked.cpp had before it was dismantled) so its own
// code no longer perturbs the inner-join operators' codegen — an earlier
// change here measured a repeatable q18 wall-time regression with no
// per-operator cause, see project_semi_anti_gather_parallel. Declared in
// join_chunked_internal.hpp (factory) and interpreter_internal.hpp
// (`is_streamable_semi_anti_join`).

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <limits>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "chunk_conversion_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_chunked_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

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

    // ── Dense bit-packed integer membership (see `dense_i64_hits_`) ──────────
    static constexpr auto dense_word(std::uint64_t slot) -> std::size_t {
        return static_cast<std::size_t>(slot >> 6U);
    }
    static constexpr auto dense_bit(std::uint64_t slot) -> std::uint64_t {
        return std::uint64_t{1} << (slot & 63U);
    }
    [[nodiscard]] auto dense_contains(std::int64_t value) const -> bool {
        const std::uint64_t slot =
            static_cast<std::uint64_t>(value) - static_cast<std::uint64_t>(*dense_i64_min_);
        return slot < dense_i64_nbits_ &&
               (dense_i64_hits_[dense_word(slot)] & dense_bit(slot)) != 0;
    }

    /// Build `dense_i64_hits_` directly from the right key column, for the
    /// swapped build whose buffered left is NOT the smaller side (so
    /// `try_dense_intersection` does not apply) but whose right key span is
    /// still small and dense enough that a cache-resident bitmap beats a
    /// multi-million-entry hash probe. q21: ~5M distinct order keys spanning
    /// ~24M values -> a 3MB bitmap probed 15M times instead of an ~80MB
    /// robin_hood set. Returns false to leave the hash build to the caller.
    template <typename RNull>
    auto try_build_dense_right(const Column<std::int64_t>& rcol, RNull rnull) -> bool {
        const std::size_t rows = rcol.size();
        if (rows == 0) {
            return false;
        }
        std::int64_t min_key = std::numeric_limits<std::int64_t>::max();
        std::int64_t max_key = std::numeric_limits<std::int64_t>::min();
        std::size_t live = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            if (rnull(i)) {
                continue;
            }
            const std::int64_t value = rcol[i];
            min_key = std::min(min_key, value);
            max_key = std::max(max_key, value);
            ++live;
        }
        if (live == 0) {
            return false;
        }
        const std::uint64_t difference =
            static_cast<std::uint64_t>(max_key) - static_cast<std::uint64_t>(min_key);
        if (difference == std::numeric_limits<std::uint64_t>::max()) {
            return false;
        }
        const std::uint64_t span = difference + 1;
        // 8MB ceiling (64M slots) keeps the bitmap inside a large L3; at most
        // 16 slots per live key keeps a sparse column on the hash path, where
        // the bitmap would be mostly empty cache lines.
        constexpr std::uint64_t kMaxDenseBits = 64ULL << 20U;
        if (span > kMaxDenseBits || span > static_cast<std::uint64_t>(live) * 16U) {
            return false;
        }
        dense_i64_min_ = min_key;
        dense_i64_nbits_ = static_cast<std::size_t>(span);
        dense_i64_hits_.assign((dense_i64_nbits_ + 63U) / 64U, std::uint64_t{0});
        for (std::size_t i = 0; i < rows; ++i) {
            if (rnull(i)) {
                continue;
            }
            const std::uint64_t slot =
                static_cast<std::uint64_t>(rcol[i]) - static_cast<std::uint64_t>(min_key);
            dense_i64_hits_[dense_word(slot)] |= dense_bit(slot);
        }
        return true;
    }

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
                // Bit-packed, so the ceiling and the per-key density gate are
                // both 8x looser than the old byte-addressed table: 64M slots =
                // 8MB, at most 16 slots per left key.
                constexpr std::uint64_t kMaxDenseSlots = 64ULL << 20U;
                constexpr std::size_t kMaxRangePerKey = 16;
                const std::size_t density_limit =
                    left_rows > std::numeric_limits<std::size_t>::max() / kMaxRangePerKey
                        ? std::numeric_limits<std::size_t>::max()
                        : left_rows * kMaxRangePerKey;
                if (span64 > kMaxDenseSlots || span64 > density_limit) {
                    return false;
                }
                const auto slots = static_cast<std::size_t>(span64);
                const std::size_t nwords = (slots + 63U) / 64U;
                const auto slot_of = [min_key](std::int64_t value) {
                    return static_cast<std::uint64_t>(value) - static_cast<std::uint64_t>(min_key);
                };
                std::vector<std::uint64_t> candidates(nwords, std::uint64_t{0});
                for (const auto* lcol : lcols) {
                    for (const std::int64_t value : *lcol) {
                        const std::uint64_t slot = slot_of(value);
                        candidates[dense_word(slot)] |= dense_bit(slot);
                    }
                }
                const auto scan_range = [&](std::size_t lo, std::size_t hi, std::uint64_t* hits) {
                    for (std::size_t row = lo; row < hi; ++row) {
                        if (rnull(row)) {
                            continue;
                        }
                        const std::int64_t value = rcol[row];
                        if (value < min_key || value > max_key) {
                            continue;
                        }
                        const std::uint64_t slot = slot_of(value);
                        if ((candidates[dense_word(slot)] & dense_bit(slot)) != 0) {
                            hits[dense_word(slot)] |= dense_bit(slot);
                        }
                    }
                };

                std::vector<std::uint64_t> hits(nwords, std::uint64_t{0});
                const std::size_t workers =
                    intersect_worker_count(rcol.size(), nwords * sizeof(std::uint64_t));
                if (workers < 2) {
                    scan_range(0, rcol.size(), hits.data());
                } else {
                    std::vector<std::vector<std::uint64_t>> parts(
                        workers, std::vector<std::uint64_t>(nwords, std::uint64_t{0}));
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
                        for (std::size_t w = 0; w < nwords; ++w) {
                            hits[w] |= part[w];
                        }
                    }
                }
                // Retain the dense representation for the buffered-left probe
                // too. Converting the hits back into a hash set would throw
                // away the representation win just before probing q22's 84k
                // customer rows.
                dense_i64_min_ = min_key;
                dense_i64_nbits_ = slots;
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
        } else if (!try_build_dense_right(rcol, rnull)) {
            // Left is not the smaller side (or its key vanished) and the right
            // key span is too wide/sparse for a bitmap; the plain right set is
            // as good, and the buffered left still emits.
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
        // For a small output the serial per-column gather is cheapest. For a
        // large one -- q21's semi join is one ~15M-row chunk, previously ~88%
        // serial -- fan the columns out over the pool in one batch. The floor
        // is the same `kMinParallelPredicateRows` (and the same reasoning) as
        // `select_rows`: `filter_chunk` runs once per left chunk, so a lower
        // floor forks a batch per ~150k-row chunk and the barriers cost more
        // than they buy.
        if (idx.size() >= kMinParallelPredicateRows) {
            return gather_rows_batched(t, idx);
        }
        return gather_rows(t, idx);
    }

    /// `gather_rows`, but the columns are gathered concurrently in ONE worker
    /// batch (`gather_columns_batched`) instead of a serial per-column loop.
    /// `idx` is an ascending subset with no `kNull` sentinel, so a subset keeps
    /// every group boundary and imposes no order and the source properties ride
    /// along unchanged -- the same rule `gather_rows` documents.
    auto gather_rows_batched(const Table& input, const std::vector<std::size_t>& idx) -> Table {
        const std::size_t total = idx.size();
        const std::span<const std::size_t> idx_span{idx.data(), total};

        std::vector<ColumnGatherJob> jobs;
        jobs.reserve(input.columns.size());
        for (const auto& entry : input.columns) {
            jobs.push_back({
                .column = entry.column.get(),
                .validity = entry.validity.has_value() ? &*entry.validity : nullptr,
                .idx = idx.data(),
                .indivisible = false,
            });
        }

        auto gathered =
            gather_columns_batched(jobs, total, exec_, [&](std::size_t j) -> GatheredColumn {
                const auto& entry = input.columns[j];
                ColumnValue col = make_gather_column(*entry.column, total);
                gather_range_into(col, *entry.column, idx_span, 0, total);
                std::optional<ValidityBitmap> val;
                if (entry.validity.has_value()) {
                    ValidityBitmap dst(total, false);
                    gather_validity_range(dst, *entry.validity, idx_span, 0, total);
                    val = std::move(dst);
                }
                return {std::move(col), std::move(val)};
            });

        Table output;
        output.columns.reserve(input.columns.size());
        for (std::size_t j = 0; j < input.columns.size(); ++j) {
            output.add_column(input.columns[j].name, std::move(gathered[j].first));
            if (gathered[j].second.has_value()) {
                output.columns.back().validity = std::move(gathered[j].second);
            }
        }
        output.set_properties(input.properties());
        return output;
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
                    match = dense_i64_min_.has_value() ? dense_contains((*col)[row])
                                                       : right_i64_.contains((*col)[row]);
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
    /// Dense bit-packed membership over `[dense_i64_min_, dense_i64_min_ +
    /// dense_i64_nbits_)`, used in place of `right_i64_` when the integer key
    /// span is boundable and dense enough for the bitmap to stay
    /// cache-resident. Populated by `try_build_dense_right` (probe the small
    /// right against a bitmap of itself) or `try_dense_intersection` (probe the
    /// large right against a bitmap of the smaller buffered left).
    std::optional<std::int64_t> dense_i64_min_;
    std::size_t dense_i64_nbits_ = 0;
    std::vector<std::uint64_t> dense_i64_hits_;
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

auto make_chunked_semi_anti_join_operator(OperatorPtr left, Table right, ir::JoinKind kind,
                                          const std::vector<ir::JoinKey>* keys,
                                          const ExecutionContext* exec) -> OperatorPtr {
    return std::make_unique<ChunkedSemiAntiJoinOperator>(std::move(left), std::move(right), kind,
                                                         keys, exec);
}

auto is_streamable_semi_anti_join(const ir::JoinNode& join) -> bool {
    return (join.kind() == ir::JoinKind::Semi || join.kind() == ir::JoinKind::Anti) &&
           !join.predicate().has_value() && join.keys().size() == 1 &&
           join.null_match() == ir::NullMatch::Never && !join.expect().asserts_anything() &&
           join.take() == ir::MatchSelection::All;
}

}  // namespace ibex::runtime
