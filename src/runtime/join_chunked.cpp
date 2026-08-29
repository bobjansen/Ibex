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
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_chunked_internal.hpp"
#include "join_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "model_internal.hpp"
#include "reshape_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

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
auto deferred_probe_scan_impl(const ir::Node& right, const ExecutionContext& exec)
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
        auto concrete =
            ir::resolve_join_columns(ir::JoinKind::Inner, *keys_, left_names, right_names, suffix_);
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

auto make_probe_factory(JoinProbe probe) -> JoinProbeFactory {
    const ExecutionContext* exec = probe.exec_;
    auto state = std::make_shared<JoinProbe>(std::move(probe));
    return JoinProbeFactory{[state](OperatorPtr child, bool preserve_empty) -> OperatorPtr {
                                return std::make_unique<JoinProbeOperator>(std::move(child), *state,
                                                                           preserve_empty);
                            },
                            [state](OperatorPtr child, bool preserve_empty) -> OperatorPtr {
                                return std::make_unique<JoinProbeOperator>(
                                    std::move(child), std::move(*state), preserve_empty);
                            },
                            exec};
}

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
        auto built = make_join_probe_operator(std::move(left_), std::move(probe_side_),
                                              make_probe_factory(std::move(probe_)));
        if (!built.has_value()) {
            return std::move(built.error());
        }
        probe_side_.reset();
        probe_op_ = std::move(*built);
        return std::nullopt;
    }

   public:
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
        -> std::expected<std::optional<FusibleJoinProbe>, std::string> {
        if (mode_ != Mode::Stream || probe_op_ != nullptr) {
            return std::optional<FusibleJoinProbe>{};
        }
        if (!probe_side_.has_value()) {
            if (left_ == nullptr) {
                return std::optional<FusibleJoinProbe>{};
            }
            auto drained = MaterializeOperator(std::move(left_)).run();
            if (!drained.has_value()) {
                return std::unexpected(std::move(drained.error()));
            }
            probe_side_ = std::move(*drained);
        }
        FusibleJoinProbe out{.probe_side = std::move(*probe_side_),
                             .probe = make_probe_factory(std::move(probe_))};
        probe_side_.reset();
        return std::optional<FusibleJoinProbe>{std::move(out)};
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
        return make_join_probe_operator(std::move(stream->source),
                                        std::move(stream->materialized_source),
                                        make_probe_factory(std::move(stream->probe)));
    }
    if (auto* swapped = std::get_if<SwappedHashProbeInput>(&input)) {
        return OperatorPtr{std::make_unique<SwappedHashProbeOperator>(std::move(*swapped))};
    }
    auto& precomputed = std::get<PrecomputedHashProbeInput>(input);
    return OperatorPtr{
        std::make_unique<PrecomputedHashProbeOperator>(std::move(precomputed.output))};
}

}  // namespace

auto deferred_probe_scan_of(const ir::Node& right, const ExecutionContext& exec)
    -> DeferredProbeScan {
    return deferred_probe_scan_impl(right, exec);
}

auto make_chunked_inner_join_operator(OperatorPtr left, Table right,
                                      const std::vector<ir::JoinKey>* keys,
                                      const ExecutionContext& exec, ir::JoinSuffixPolicy suffix,
                                      const std::vector<ir::OrderKey>* pending_order,
                                      physical::JoinParallelism parallelism,
                                      std::optional<ir::JoinColumnMapping> columns) -> OperatorPtr {
    return std::make_unique<ChunkedInnerJoinOperator>(std::move(left), std::move(right), keys, exec,
                                                      std::move(suffix), pending_order, parallelism,
                                                      std::move(columns));
}

namespace {

auto finish_scheduled_join(std::unique_ptr<ChunkedInnerJoinOperator> op)
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

}  // namespace

auto make_scheduled_chunked_inner_join_operator(
    OperatorPtr left, Table right, const std::vector<ir::JoinKey>* keys,
    const ExecutionContext& exec, ir::JoinSuffixPolicy suffix,
    const std::vector<ir::OrderKey>* pending_order, physical::JoinParallelism parallelism,
    std::optional<ir::JoinColumnMapping> columns) -> std::expected<OperatorPtr, std::string> {
    return finish_scheduled_join(std::make_unique<ChunkedInnerJoinOperator>(
        std::move(left), std::move(right), keys, exec, std::move(suffix), pending_order,
        parallelism, std::move(columns)));
}

auto make_scheduled_deferred_inner_join_operator(
    OperatorPtr left, const ir::Node* right_node, const TableRegistry* registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    const std::vector<ir::JoinKey>* keys, const DeferredScan* probe, std::string probe_name,
    ir::JoinSuffixPolicy suffix, const std::vector<ir::OrderKey>* pending_order,
    physical::JoinParallelism parallelism, std::optional<ir::JoinColumnMapping> columns)
    -> std::expected<OperatorPtr, std::string> {
    return finish_scheduled_join(std::make_unique<ChunkedInnerJoinOperator>(
        std::move(left), right_node, registry, scalars, externs, exec, keys, probe,
        std::move(probe_name), std::move(suffix), pending_order, parallelism, std::move(columns)));
}

auto take_fusible_join_probe(OperatorPtr left, Table right, const std::vector<ir::JoinKey>* keys,
                             const ExecutionContext& exec, ir::JoinSuffixPolicy suffix,
                             const std::vector<ir::OrderKey>* pending_order,
                             physical::JoinParallelism parallelism)
    -> std::expected<std::optional<FusibleJoinProbe>, std::string> {
    auto op =
        std::make_unique<ChunkedInnerJoinOperator>(std::move(left), std::move(right), keys, exec,
                                                   std::move(suffix), pending_order, parallelism);
    if (auto err = op->run_build()) {
        return std::unexpected(std::move(*err));
    }
    return op->take_fusible_probe();
}

}  // namespace ibex::runtime
