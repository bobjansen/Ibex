// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// distinct_chunked.cpp — the streaming `distinct` breaker and its two
// construction sites (`distinct_table` for the whole-table adapter,
// `build_physical_distinct` for the physical-plan seam). Split out of
// chunked.cpp; the operator is fully private to this translation unit and the
// dedup fan-out policy is resolved by the caller (src/runtime/PARALLELISM.md).

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
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
#include "packed_key_encoder_internal.hpp"
#include "physical_executor_internal.hpp"
#include "physical_plan.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {
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

namespace physical_executor_detail {

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

}  // namespace ibex::runtime
