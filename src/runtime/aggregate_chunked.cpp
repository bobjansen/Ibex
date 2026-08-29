// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Adaptive sorted/hash aggregate execution. The complete aggregate family is
// kept in this translation unit so extraction from the pipeline builder does
// not split hot templates or state across compilation boundaries.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/format.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
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
#include <new>
#include <numeric>
#include <optional>
#include <ratio>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "physical_plan.hpp"

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "aggregate_chunked_internal.hpp"
#include "chunk_conversion_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "packed_key_encoder_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

namespace {

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
///
/// The `realloc`/`free` calls are the whole reason this class exists (in-place
/// `mremap` growth, no copy) -- it is itself the RAII wrapper the check wants.
// NOLINTBEGIN(cppcoreguidelines-no-malloc)
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
        // prototype{} zeroes padding too (see the class comment), so this is well defined.
        // NOLINTNEXTLINE(cert-exp42-c,cert-flp37-c,bugprone-suspicious-memory-comparison)
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
// NOLINTEND(cppcoreguidelines-no-malloc)

auto bind_aggregate_columns(std::optional<physical::AggregateColumnMapping>& columns, bool& bound,
                            const std::vector<ir::ColumnRef>& group_by,
                            const std::vector<ir::AggSpec>& aggregations, const Chunk& chunk)
    -> std::optional<std::string> {
    if (bound) {
        return std::nullopt;
    }
    std::vector<std::string_view> names;
    names.reserve(chunk.columns.size());
    for (const ColumnEntry& column : chunk.columns) {
        names.push_back(column.name);
    }
    const bool concrete_layout_matches_plan = columns.has_value() &&
                                              columns->input_names.size() == names.size() &&
                                              std::ranges::equal(columns->input_names, names);
    if (!concrete_layout_matches_plan) {
        // Logical schema inference may know every source column while the
        // physical child emits a narrower layout. A pushed-down filter, for
        // example, can consume its predicate-only column inside a lazy scan and
        // omit it from the chunks delivered to this breaker. Bind that actual
        // boundary once; all row loops remain positional.
        auto resolved = physical::resolve_aggregate_columns(group_by, aggregations, names);
        if (!resolved.has_value()) {
            return std::move(resolved.error());
        }
        columns = std::move(*resolved);
    }
    if (columns->group_by.size() != group_by.size() ||
        columns->aggregate_inputs.size() != aggregations.size()) {
        return "aggregate column mapping does not match aggregate shape";
    }
    for (std::size_t i = 0; i < columns->group_by.size(); ++i) {
        const std::size_t index = columns->group_by[i];
        if (index >= chunk.columns.size() || chunk.columns[index].name != group_by[i].name) {
            return "aggregate group-by column mapping does not match concrete input";
        }
    }
    for (std::size_t i = 0; i < columns->aggregate_inputs.size(); ++i) {
        const auto index = columns->aggregate_inputs[i];
        if (aggregations[i].func == ir::AggFunc::Count) {
            if (index.has_value()) {
                return "count aggregate unexpectedly has an input column mapping";
            }
            continue;
        }
        if (!index.has_value() || *index >= chunk.columns.size() ||
            chunk.columns[*index].name != aggregations[i].column.name) {
            std::string detail =
                "aggregate input column mapping does not match concrete input: expected '" +
                aggregations[i].column.name + "'";
            if (index.has_value()) {
                detail += " at position " + std::to_string(*index);
                if (*index < chunk.columns.size()) {
                    detail += ", found '" + chunk.columns[*index].name + "'";
                } else {
                    detail += ", but the input has only " + std::to_string(chunk.columns.size()) +
                              " columns";
                }
            }
            return detail;
        }
    }
    bound = true;
    return std::nullopt;
}

class HashAggregateState final {
   public:
    /// `Cat` carries a Categorical's *code*, which the pair path may treat as
    /// an integer for the same reason `process_rows_cat` may index an array
    /// with it: within one operator a dictionary only ever grows and never
    /// reorders, so a code identifies the same value in every chunk.
    enum class IntKeyKind : std::uint8_t { Int64, Date, Ts, Cat };

    HashAggregateState(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                       const std::vector<ir::AggSpec>* aggregations, const ExecutionContext& exec,
                       physical::AggregateParallelism par = {},
                       std::optional<physical::AggregateColumnMapping> columns = std::nullopt)
        : child_(std::move(child)),
          group_by_(group_by),
          aggregations_(aggregations),
          exec_(&exec),
          columns_(std::move(columns)),
          par_(par),
          discovery_profile_(exec.execution_profile == nullptr
                                 ? nullptr
                                 : exec.execution_profile->stage("Aggregate.Discovery")),
          accumulation_profile_(exec.execution_profile == nullptr
                                    ? nullptr
                                    : exec.execution_profile->stage("Aggregate.Accumulation")),
          final_ordering_profile_(exec.execution_profile == nullptr
                                      ? nullptr
                                      : exec.execution_profile->stage("Aggregate.FinalOrdering")),
          emission_profile_(exec.execution_profile == nullptr
                                ? nullptr
                                : exec.execution_profile->stage("Aggregate.Emission")) {}

    /// Pull and run the structural Discovery node for one chunk. The chunk is
    /// retained until `accumulate_discovery` consumes its transfer, so the
    /// ColumnEntry pointers in that value remain valid without copying data.
    auto next_discovery() -> std::expected<bool, std::string> {
        if (input_consumed_) {
            return false;
        }
        if (active_chunk_.has_value()) {
            return std::unexpected(
                "physical aggregate: Discovery advanced before Accumulation consumed its input");
        }
        auto chunk_res = child_->next();
        if (!chunk_res.has_value()) {
            return std::unexpected(std::move(chunk_res.error()));
        }
        if (!chunk_res.value().has_value()) {
            input_consumed_ = true;
            return false;
        }
        active_chunk_ = std::move(*chunk_res.value());
        const ExecutionProfileScope scope(discovery_profile_, ProfilePhase::Next);
        if (auto err = discover_chunk(*active_chunk_)) {
            return std::unexpected(*err);
        }
        return true;
    }

    /// Consume the current Discovery output at the structural Accumulation
    /// node, then release the input chunk before the source advances.
    auto accumulate_discovery() -> std::expected<void, std::string> {
        if (!active_chunk_.has_value()) {
            return std::unexpected("physical aggregate: Accumulation has no discovered chunk");
        }
        const ExecutionProfileScope scope(accumulation_profile_, ProfilePhase::Next);
        if (auto err = accumulate_discovered_chunk()) {
            return std::unexpected(*err);
        }
        active_chunk_.reset();
        return {};
    }

    /// Structural FinalOrdering entry. Owned-partition strategies transfer
    /// their local group state into deterministic first-occurrence order here;
    /// already-global strategies have no deferred work at this boundary.
    auto finalize_ordering() -> std::optional<std::string> {
        const ExecutionProfileScope scope(final_ordering_profile_, ProfilePhase::Next);
        if (ordering_finalized_) {
            return std::nullopt;
        }
        if (owned_mode_) {
            finalize_owned_active();
            if (owned_async_error_.has_value()) {
                return owned_async_error_;
            }
        }
        ordering_finalized_ = true;
        return std::nullopt;
    }

    /// Structural Emission entry. It consumes only finalized, globally ordered
    /// group state and constructs the result columns.
    auto emit_output() -> std::expected<std::optional<Chunk>, std::string> {
        const ExecutionProfileScope scope(emission_profile_, ProfilePhase::Next);
        if (!input_consumed_) {
            return std::unexpected("physical aggregate: Emission ran before input consumption");
        }
        if (active_chunk_.has_value()) {
            return std::unexpected("physical aggregate: Emission ran with unconsumed discovery");
        }
        if (!ordering_finalized_) {
            return std::unexpected("physical aggregate: Emission ran before FinalOrdering");
        }
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        emitted_ = true;
        return build_output_chunk();
    }

   private:
    /// Telemetry for the breaker-parallelism slice (src/runtime/PARALLELISM.md).
    /// The plan owns each phase's worker cap and fan-out permission now; the
    /// fan-out output is byte-identical to serial, so without a counter a gate
    /// that silently stopped matching would lose the parallelism with every
    /// test green. Counted once per fan-out commit (per chunk for `partition`,
    /// once for `finalize`, which runs once).
    void note_partition_fanout() const {
        if (exec_ != nullptr && exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_aggregate_partitions.fetch_add(
                1, std::memory_order_relaxed);
        }
    }
    void note_finalize_fanout() const {
        if (exec_ != nullptr && exec_->parallel_stats != nullptr) {
            exec_->parallel_stats->parallel_aggregate_finalizes.fetch_add(
                1, std::memory_order_relaxed);
        }
    }

    enum class DiscoveryTransferKind : std::uint8_t {
        None,
        NeedsAccumulation,
        FusedAccumulation,
    };

    /// Per-chunk ownership transfer from Discovery to Accumulation. Column
    /// pointers remain valid because `consume_input` keeps the owning Chunk
    /// alive until `accumulate_discovered_chunk` consumes this value. The gid
    /// buffer is operator-owned and cannot be reused until the transfer resets.
    struct AggregateDiscoveryTransfer {
        DiscoveryTransferKind kind = DiscoveryTransferKind::None;
        std::vector<const ColumnEntry*> aggregate_entries;
        std::vector<std::uint8_t> skip_fields;
        std::size_t rows = 0;
    };

    void publish_discovered(const std::vector<const ColumnEntry*>& aggregate_entries,
                            std::size_t rows,
                            const std::vector<std::uint8_t>* skip_fields = nullptr) {
        discovery_transfer_.kind = DiscoveryTransferKind::NeedsAccumulation;
        discovery_transfer_.aggregate_entries = aggregate_entries;
        discovery_transfer_.rows = rows;
        discovery_transfer_.skip_fields =
            skip_fields == nullptr ? std::vector<std::uint8_t>{} : *skip_fields;
    }

    void publish_fused_accumulation() {
        discovery_transfer_ = {};
        discovery_transfer_.kind = DiscoveryTransferKind::FusedAccumulation;
    }

    auto accumulate_discovered_chunk() -> std::optional<std::string> {
        if (discovery_transfer_.kind == DiscoveryTransferKind::None) {
            return "physical aggregate: Discovery produced no accumulation transfer";
        }
        if (discovery_transfer_.kind == DiscoveryTransferKind::FusedAccumulation) {
            discovery_transfer_ = {};
            return std::nullopt;
        }
        if (gids_buf_.size() < discovery_transfer_.rows) {
            return "physical aggregate: Discovery produced a short group-id buffer";
        }
        const auto* skip =
            discovery_transfer_.skip_fields.empty() ? nullptr : &discovery_transfer_.skip_fields;
        accumulate_gids(gids_buf_.data(), discovery_transfer_.aggregate_entries,
                        discovery_transfer_.rows, skip);
        discovery_transfer_ = {};
        return std::nullopt;
    }

    auto discover_chunk(const Chunk& chunk) -> std::optional<std::string> {
        discovery_transfer_ = {};
        if (std::getenv("IBEX_AGG_PARTITION_DEBUG") != nullptr) {
            ibex::formatting::print(stderr, "[agg_process_chunk] rows={} group_by_size={}\n",
                                    chunk.rows(), group_by_->size());
        }
        // Counted here, once per chunk, because the partition gate below asks
        // how much input this OPERATOR has — a question the per-call row count
        // stopped answering the moment sources began arriving in pieces.
        rows_offered_ += chunk.rows();
        if (auto err = bind_aggregate_columns(columns_, columns_bound_, *group_by_, *aggregations_,
                                              chunk)) {
            return err;
        }
        // `bind_aggregate_columns` only returns nullopt once `columns_` is bound.
        if (!columns_.has_value()) {
            return "HashAggregateState: column mapping not bound";
        }
        const physical::AggregateColumnMapping& cols = *columns_;
        std::vector<const ColumnEntry*> group_entries;
        group_entries.reserve(group_by_->size());
        for (const std::size_t index : cols.group_by) {
            group_entries.push_back(&chunk.columns[index]);
        }

        std::vector<const ColumnEntry*> agg_entries(aggregations_->size(), nullptr);
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const auto& agg = (*aggregations_)[i];
            const std::optional<std::size_t>& input_idx = cols.aggregate_inputs[i];
            if (agg.func == ir::AggFunc::Count || !input_idx.has_value()) {
                continue;
            }
            const ColumnEntry* entry = &chunk.columns[*input_idx];
            const ExprType kind = expr_type_for_column(*entry->column);
            const bool first_or_last =
                agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last;
            // First/Last also accept String (which covers Column<std::string> and
            // Column<Categorical> — expr_type_for_column collapses both to
            // String); every other function stays numeric-only.
            const bool supported = kind == ExprType::Int || kind == ExprType::Double ||
                                   (first_or_last && kind == ExprType::String);
            if (!supported) {
                return "HashAggregateState: non-numeric aggregation not supported";
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
                    return "HashAggregateState: aggregate column type changed across chunks";
                }
            }
            for (std::size_t i = 0; i < group_entries.size(); ++i) {
                if (group_entries[i]->column->index() != group_templates_[i].index()) {
                    return "HashAggregateState: group-by column type changed across chunks";
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
            auto error = process_rows_ungrouped(agg_entries, rows);
            if (!error.has_value()) {
                publish_fused_accumulation();
            }
            return error;
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
                return "HashAggregateState: group-by key column gained nulls across chunks";
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
            publish_discovered(agg_entries, rows);
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

        publish_discovered(agg_entries, rows);
        return std::nullopt;
    }

    // Single fixed-width-integer key: probe a value -> gid map directly, the way
    // process_rows_str does for strings. Date/Timestamp are read as their raw
    // integer (days / nanos), which is order- and equality-faithful.
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
                return "HashAggregateState: categorical key on the single-int path";
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

        // The q18 shape (one Int64 key and one Double sum) is a streaming sink,
        // not a sequence of per-chunk fork/join pipelines. Each chunk becomes
        // one independent hot-table task; the caller immediately pulls the
        // next chunk, and all tasks join once at end-of-stream. Besides removing
        // the three barriers per chunk, this keeps the common clustered key in
        // a 4096-slot cache-resident reduction and sends only cold/pre-aggregated
        // records to the persistent partition maps.
        if (try_async_hot_int_sum(group_entries[0]->column, agg_entries, rows)) {
            publish_fused_accumulation();
            return std::nullopt;
        }

        gids_buf_.resize(rows);
        auto* gids = gids_buf_.data();

        // A non-null First value is fixed at the same row that creates its
        // group. Record it during discovery and omit the later all-row scan.
        // Group-key reduction turns q10's six descriptive keys into exactly
        // this shape; scanning 229k rows once per carried field was redundant.
        if (discovery_first_eligible_.empty()) {
            discovery_first_eligible_.resize(n_aggs_, 0U);
            for (std::size_t a = 0; a < n_aggs_; ++a) {
                discovery_first_eligible_[a] = plan_[a].func == ir::AggFunc::First ? 1U : 0U;
            }
        }
        std::vector<std::uint8_t> discovery_first(n_aggs_, 0U);
        bool has_discovery_first = false;
        if (std::getenv("IBEX_DISABLE_DISCOVERY_FIRST") == nullptr) {
            for (std::size_t a = 0; a < n_aggs_; ++a) {
                if (discovery_first_eligible_[a] == 0U) {
                    continue;
                }
                if (agg_entries[a]->validity.has_value()) {
                    // A group may still be waiting for its first non-null
                    // value. Keep this field on the ordinary scan for every
                    // later chunk too, even if that later chunk has no nulls.
                    discovery_first_eligible_[a] = 0U;
                    continue;
                }
                discovery_first[a] = 1U;
                has_discovery_first = true;
            }
        }
        const std::size_t groups_before = n_groups_;
        std::vector<std::size_t> first_rows;

        // Partition-owned accumulation, the same shape the PairIntKey path
        // above takes. It fuses discovery and the sum/count into one pass over
        // partition-local state, so the global first-occurrence numbering --
        // and with it the whole second `accumulate_gids` scan of every row --
        // is deferred to a single merge over GROUPS at emission.
        //
        // `int_order_` holds the raw key whatever `int_key_kind_` is; the emit
        // side reconstructs Date/Timestamp/Categorical from it exactly as it
        // does for the ordinary int path, so this needs no kind-specific arm.
        if (try_owned<std::int64_t, robin_hood::hash<std::int64_t>>(
                key_at, rows, gids, agg_entries, owned_int_partitions_, kIntOwnedMinRows)) {
            publish_fused_accumulation();
            return std::nullopt;
        }

        if (try_discover_partitioned<std::int64_t, robin_hood::hash<std::int64_t>>(
                key_at, rows, gids, int_partitions_,
                [&](std::size_t n) {
                    int_order_.resize(n);
                    if (has_discovery_first) {
                        first_rows.resize(n - groups_before);
                    }
                },
                [&](std::int64_t key, std::uint32_t gid, std::size_t row) {
                    int_order_[gid] = key;
                    if (has_discovery_first) {
                        first_rows[gid - groups_before] = row;
                    }
                },
                kDefaultPartitionMinRows, [&](std::uint32_t gid) { return int_order_[gid]; })) {
            if (has_discovery_first) {
                seed_discovery_first(groups_before, first_rows, agg_entries, discovery_first);
            }
            publish_discovered(agg_entries, rows, has_discovery_first ? &discovery_first : nullptr);
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
                    if (has_discovery_first) {
                        first_rows.push_back(row);
                    }
                } else {
                    gid = it->second;
                }
                prev_key = key;
                prev_gid = gid;
                have_prev = true;
            }
            gids[row] = gid;
        }

        if (has_discovery_first) {
            seed_discovery_first(groups_before, first_rows, agg_entries, discovery_first);
        }
        publish_discovered(agg_entries, rows, has_discovery_first ? &discovery_first : nullptr);
        return std::nullopt;
    }

    static constexpr std::size_t kOwnedHotSlots = 4096;

    struct OwnedHotRecord {
        std::int64_t key = 0;
        std::uint64_t first_row = 0;
        AggSlotCore slot;
    };

    struct OwnedHotChunk {
        std::shared_ptr<ColumnValue> key_column;
        std::shared_ptr<ColumnValue> sum_column;
        std::optional<ValidityBitmap> sum_validity;
        std::uint64_t row_base = 0;
        std::size_t rows = 0;
        std::size_t part_count = 0;
        std::vector<std::vector<OwnedHotRecord>> records_by_partition;
        std::optional<std::string> error;
    };

    struct OwnedHotSlot {
        std::uint32_t tag = std::numeric_limits<std::uint32_t>::max();
        std::uint32_t last_access_tag = std::numeric_limits<std::uint32_t>::max();
        std::uint32_t record = std::numeric_limits<std::uint32_t>::max();
    };

    [[nodiscard]] static auto owned_hot_hash(std::int64_t key) noexcept -> std::uint64_t {
        // robin_hood's Int64 hash preserves too much of a sequential key's bit
        // pattern for a high-bit fixed table. SplitMix's finalizer gives both
        // candidate slots and the tag independent-looking bits at tiny cost.
        auto x = static_cast<std::uint64_t>(key);
        x ^= x >> 30U;
        x *= 0xbf58476d1ce4e5b9ULL;
        x ^= x >> 27U;
        x *= 0x94d049bb133111ebULL;
        x ^= x >> 31U;
        return x;
    }

    static void process_owned_hot_chunk(OwnedHotChunk& job) noexcept {
        try {
            const auto* keys = std::get<Column<std::int64_t>>(*job.key_column).data();
            const auto* values = std::get<Column<double>>(*job.sum_column).data();
            const ValidityBitmap* validity =
                job.sum_validity.has_value() ? &*job.sum_validity : nullptr;
            constexpr std::uint32_t kEmpty = std::numeric_limits<std::uint32_t>::max();
            constexpr unsigned kShift = 64U - 12U;
            constexpr std::uint64_t kH2Mult = 0xf1357aea2e62a9c5ULL;

            std::array<OwnedHotSlot, kOwnedHotSlots> table{};
            std::vector<OwnedHotRecord> records;
            records.reserve(std::max<std::size_t>(kOwnedHotSlots, job.rows / 3));
            std::size_t filled = 0;
            std::uint64_t prng = 0;
            std::int64_t last_key = 0;
            std::uint32_t last_record = kEmpty;
            bool have_last = false;

            const auto append_record = [&](std::size_t row) -> std::uint32_t {
                const auto index = static_cast<std::uint32_t>(records.size());
                OwnedHotRecord record;
                record.key = keys[row];
                record.first_row = job.row_base + row;
                if (validity == nullptr || (*validity)[row]) {
                    record.slot.double_value = values[row];
                    record.slot.mark_present();
                }
                records.push_back(record);
                return index;
            };
            const auto update_record = [&](std::uint32_t record, std::size_t row) {
                if (validity == nullptr || (*validity)[row]) {
                    auto& slot = records[record].slot;
                    slot.double_value += values[row];
                    slot.mark_present();
                }
            };

            for (std::size_t row = 0; row < job.rows; ++row) {
                const std::int64_t key = keys[row];
                // HashKeys in Polars computes hashes as a column kernel before
                // probing. Here keys arrive as raw Int64s, and q18's useful
                // locality is specifically runs of the same key. Retain the
                // most recent hot/cold record so the other rows in a run avoid
                // both the SplitMix hash and the two fixed-table probes.
                if (have_last && key == last_key) {
                    update_record(last_record, row);
                    continue;
                }
                const std::uint64_t hash = owned_hot_hash(key);
                const auto tag = static_cast<std::uint32_t>(hash);
                const auto h1 = static_cast<std::size_t>(hash >> kShift);
                const auto h2 = static_cast<std::size_t>((hash * kH2Mult) >> kShift);
                auto& s1 = table[h1];
                auto& s2 = table[h2];

                if (s1.tag == tag && s1.record != kEmpty && records[s1.record].key == key) {
                    s1.last_access_tag = tag;
                    update_record(s1.record, row);
                    last_key = key;
                    last_record = s1.record;
                    have_last = true;
                    continue;
                }
                if (s2.tag == tag && s2.record != kEmpty && records[s2.record].key == key) {
                    s2.last_access_tag = tag;
                    update_record(s2.record, row);
                    last_key = key;
                    last_record = s2.record;
                    have_last = true;
                    continue;
                }

                if (filled < kOwnedHotSlots) {
                    OwnedHotSlot* empty = s1.record == kEmpty ? &s1 : nullptr;
                    if (empty == nullptr && s2.record == kEmpty) {
                        empty = &s2;
                    }
                    if (empty != nullptr) {
                        empty->tag = tag;
                        empty->last_access_tag = tag;
                        empty->record = append_record(row);
                        last_key = key;
                        last_record = empty->record;
                        have_last = true;
                        ++filled;
                        continue;
                    }
                }

                // Polars' second chance: a miss first marks one candidate as
                // recently considered and stays cold. Seeing the same tag
                // again earns admission, evicting that candidate's mapping;
                // its record already contains the complete pre-aggregate.
                OwnedHotSlot& chosen = (prng >> 63U) != 0 ? s1 : s2;
                prng += hash;
                if (chosen.last_access_tag == tag) {
                    chosen.tag = tag;
                    chosen.last_access_tag = tag;
                    chosen.record = append_record(row);
                    last_record = chosen.record;
                } else {
                    chosen.last_access_tag = tag;
                    last_record = append_record(row);
                }
                last_key = key;
                have_last = true;
            }

            // Records were appended at their first source row and only updated
            // in place, so this stable routing preserves global first-seen
            // order without a histogram/scatter phase or a sort.
            job.records_by_partition.resize(job.part_count);
            std::vector<std::size_t> counts(job.part_count, 0);
            const robin_hood::hash<std::int64_t> partition_hash;
            const std::size_t mask = job.part_count - 1;
            for (const auto& record : records) {
                ++counts[partition_hash(record.key) & mask];
            }
            for (std::size_t p = 0; p < job.part_count; ++p) {
                job.records_by_partition[p].reserve(counts[p]);
            }
            for (auto& record : records) {
                const std::size_t p = partition_hash(record.key) & mask;
                job.records_by_partition[p].push_back(record);
            }

            // Release decoded buffers as soon as this task is done. The job's
            // compact pre-aggregates remain until the one final cold merge.
            job.key_column.reset();
            job.sum_column.reset();
            job.sum_validity.reset();
        } catch (const std::exception& error) {
            job.error = "async hot aggregate: " + std::string(error.what());
        } catch (...) {
            job.error = "async hot aggregate: non-standard worker exception";
        }
    }

    auto try_async_hot_int_sum(const std::shared_ptr<ColumnValue>& key_column,
                               const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows)
        -> bool {
        if (!owned_async_hot_mode_) {
            if (std::getenv("IBEX_DISABLE_OWNED_PAIR_AGG") != nullptr ||
                std::getenv("IBEX_DISABLE_ASYNC_HOT_AGG") != nullptr || n_groups_ > 0 ||
                partitioned_active_ || owned_mode_ || n_aggs_ != 1 ||
                plan_[0].func != ir::AggFunc::Sum || plan_[0].kind != ExprType::Double ||
                int_key_kind_ != IntKeyKind::Int64 || scratch_stride_ != 0 || exec_ == nullptr ||
                on_worker_pool_thread() || std::max(rows_offered_, rows) < kIntOwnedMinRows) {
                return false;
            }
            // As `try_owned`: fan-out permission and the worker cap are the
            // plan's Discovery node (src/runtime/PARALLELISM.md);
            // `kIntOwnedMinRows` stays as this specialization's admission gate.
            if (par_.discovery.decline != physical::FanOutDecline::None ||
                par_.discovery.worker_cap < 2) {
                return false;
            }
            auto& pool = process_worker_pool();
            const std::size_t workers = par_.discovery.worker_cap;
            owned_async_part_count_ = 1;
            while (owned_async_part_count_ * 2 <= workers) {
                owned_async_part_count_ *= 2;
            }
            owned_int_partitions_.resize(owned_async_part_count_);
            owned_async_group_.emplace(pool.task_group());
            owned_async_hot_mode_ = true;
            owned_mode_ = true;
            note_partition_fanout();
        }

        const ColumnEntry& agg0 = *agg_entries[0];
        auto job = std::make_unique<OwnedHotChunk>();
        job->key_column = key_column;
        job->sum_column = agg0.column;
        if (agg0.validity.has_value()) {
            job->sum_validity = *agg0.validity;
        }
        job->row_base = owned_rows_seen_;
        job->rows = rows;
        job->part_count = owned_async_part_count_;
        auto* const raw_job = job.get();
        owned_async_jobs_.push_back(std::move(job));
        // Engaged since the block above either emplaced it or `owned_async_hot_mode_`
        // was already set (the two only ever change together).
        if (!owned_async_group_.has_value()) {
            invariant_violation("async hot aggregate: task group missing while accepting chunks");
        }
        owned_async_group_->submit([raw_job] { process_owned_hot_chunk(*raw_job); });
        owned_rows_seen_ += rows;
        return true;
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

    /// Same gate for the single-Int64-key slice. Held at the pair path's value
    /// until the sweep below says otherwise -- the mechanism is identical and
    /// its break-even has no reason to differ by more than the key's own probe
    /// cost, which is lower, not higher.
    static constexpr std::size_t kIntOwnedMinRows = 1U << 16U;  // 65536

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
    template <typename Key, typename Hash, typename KeyAt, typename Partitions>
    auto try_owned(const KeyAt& key_at, std::size_t rows, std::uint32_t* gids,
                   const std::vector<const ColumnEntry*>& agg_entries, Partitions& partitions,
                   std::size_t min_rows) -> bool {
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
            if (exec_ == nullptr || on_worker_pool_thread()) {
                return false;
            }
            // Fan-out permission and the worker cap are the plan's
            // (src/runtime/PARALLELISM.md); `decline != None` folds in
            // `!exec_->can_fan_out()`. `min_rows` stays here: it is the owned
            // strategy's own "is the specialization worth it" gate, lower than
            // Discovery's radix floor, and an operator-resolved choice like
            // the join's build orientation.
            if (par_.discovery.decline != physical::FanOutDecline::None ||
                par_.discovery.worker_cap < 2) {
                return false;
            }
            if (std::max(rows_offered_, rows) < min_rows) {
                return false;
            }
        }

        auto& pool = process_worker_pool();
        const std::size_t workers = par_.discovery.worker_cap;
        std::size_t part_count = 1;
        while (part_count * 2 <= workers) {
            part_count *= 2;
        }
        const std::uint64_t part_mask = part_count - 1;
        if (partitions.size() < part_count) {
            partitions.resize(part_count);
        }
        note_partition_fanout();

        // A clustered count key should not pay the partition/scatter/hash
        // pipeline once per ROW. Compress contiguous equal-key runs first and
        // carry their lengths as count weights. This is exact for arbitrary
        // input order: non-contiguous runs still meet in the exact hash
        // fallback, while sorted inputs such as TPC-H lineitem reduce the
        // expensive part of the pipeline by roughly their mean run length.
        // Sample before committing because all-unique keys would only add two
        // equality passes and retain the original item count.
        // NOLINTNEXTLINE(misc-const-correctness) -- mutated in the int64 instantiation below
        [[maybe_unused]] bool compress_runs = false;
        if constexpr (std::is_same_v<Key, std::int64_t>) {
            compress_runs = owned_ordered_run_mode_;
            if (!compress_runs && !owned_mode_ && plan_[0].func == ir::AggFunc::Count &&
                rows >= 64 && std::getenv("IBEX_DISABLE_ORDERED_RUN_AGG") == nullptr) {
                const std::size_t sampled = std::min<std::size_t>(rows, 8192);
                std::size_t repeats = 0;
                Key previous = key_at(0);
                for (std::size_t row = 1; row < sampled; ++row) {
                    const Key key = key_at(row);
                    repeats += key == previous ? 1 : 0;
                    previous = key;
                }
                compress_runs = repeats * 2 >= sampled - 1;
            }
        }

        const std::size_t source_ranges = workers;
        const std::size_t source_grain = (rows + source_ranges - 1) / source_ranges;

        // Run compression only ever engages for a clustered single-Int64 Count
        // (the `if constexpr` above is the only writer of `compress_runs`).
        // Two parallel passes, no scratch: pass 1 counts runs per range so the
        // per-range output offsets are contiguous, pass 2 emits (key, length)
        // straight into `owned_ordered_run_{keys,counts}_`. A third pass to
        // dereference anchor rows -- and the `owned_run_rows_/_lengths_` arrays
        // it read -- used to sit between them; folding it into pass 2 drops one
        // pool barrier per chunk, which is q21's `count() by l_orderkey` hot
        // path (~23 chunks, this was 3 barriers each).
        if constexpr (std::is_same_v<Key, std::int64_t>) {
            if (compress_runs) {
                std::vector<std::size_t> run_offsets(source_ranges + 1, 0);
                {
                    auto batch = pool.submit(source_ranges, [&](std::size_t r) {
                        const std::size_t begin = r * source_grain;
                        const std::size_t end = std::min(rows, begin + source_grain);
                        if (begin >= end) {
                            return;
                        }
                        std::size_t runs = 1;
                        Key previous = key_at(begin);
                        for (std::size_t row = begin + 1; row < end; ++row) {
                            const Key key = key_at(row);
                            runs += key == previous ? 0 : 1;
                            previous = key;
                        }
                        run_offsets[r + 1] = runs;
                    });
                    batch.wait();
                }
                for (std::size_t r = 0; r < source_ranges; ++r) {
                    run_offsets[r + 1] += run_offsets[r];
                }
                const std::size_t items = run_offsets.back();
                const std::size_t old_runs = owned_ordered_run_keys_.size();
                owned_ordered_run_keys_.resize(old_runs + items);
                owned_ordered_run_counts_.resize(old_runs + items);
                {
                    auto batch = pool.submit(source_ranges, [&](std::size_t r) {
                        const std::size_t begin = r * source_grain;
                        const std::size_t end = std::min(rows, begin + source_grain);
                        if (begin >= end) {
                            return;
                        }
                        std::size_t out = old_runs + run_offsets[r];
                        std::size_t anchor = begin;
                        Key previous = key_at(begin);
                        for (std::size_t row = begin + 1; row < end; ++row) {
                            const Key key = key_at(row);
                            if (key != previous) {
                                owned_ordered_run_keys_[out] = previous;
                                owned_ordered_run_counts_[out] = row - anchor;
                                ++out;
                                anchor = row;
                                previous = key;
                            }
                        }
                        owned_ordered_run_keys_[out] = previous;
                        owned_ordered_run_counts_[out] = end - anchor;
                    });
                    batch.wait();
                }
                if (owned_ordered_runs_nondecreasing_) {
                    const std::size_t from = old_runs == 0 ? 1 : old_runs;
                    for (std::size_t i = from; i < old_runs + items; ++i) {
                        if (owned_ordered_run_keys_[i] < owned_ordered_run_keys_[i - 1]) {
                            owned_ordered_runs_nondecreasing_ = false;
                            break;
                        }
                    }
                }
                owned_rows_seen_ += rows;
                owned_mode_ = true;
                owned_ordered_run_mode_ = true;
                return true;
            }
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
                Hash hasher;
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
            const ColumnEntry& entry = *agg_entries[a];
            sum_cols[a] = std::get<Column<double>>(*entry.column).data();
            sum_validity[a] = entry.validity.has_value() ? &*entry.validity : nullptr;
        }

        const std::uint64_t row_base = owned_rows_seen_;
        {
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(std::min(workers, part_count), [&](std::size_t) {
                for (std::size_t p = cursor.fetch_add(1, std::memory_order_relaxed); p < part_count;
                     p = cursor.fetch_add(1, std::memory_order_relaxed)) {
                    auto& partition = partitions[p];
                    for (std::size_t i = part_begin[p]; i < part_begin[p + 1]; ++i) {
                        const std::size_t row = scatter_rows_[i];
                        const Key key = key_at(row);
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
            ibex::formatting::print(
                stderr, "[agg_owned] chunk rows={} part_count={} total_rows={}\n", rows,
                partitions.size(), static_cast<unsigned long long>(owned_rows_seen_));
        }
        return true;
    }

    /// Walk every `owned_pair_partitions_` entry once, in first-occurrence
    /// order (a P-way merge over `first_rows`, run once at final emission),
    /// and populate `pair_order_`/`flat_slots_` -- the arrays
    /// `build_output_chunk`'s `pair_int_fast_path_` branch already reads.
    template <typename Partitions, typename ResizeOrder, typename StoreKey>
    void finalize_owned(Partitions& partitions, const ResizeOrder& resize_order,
                        const StoreKey& store_key) {
        if (owned_finalized_) {
            return;
        }
        owned_finalized_ = true;
        const std::size_t part_count = partitions.size();
        std::size_t total = 0;
        for (const auto& partition : partitions) {
            total += partition.keys.size();
        }
        n_groups_ = total;
        resize_order(total);
        AggSlotCore* fs = flat_slots_.grow_uninitialized(total * n_aggs_).data();
        if (total == 0) {
            return;
        }

        // K-way merge of the `part_count` partition group-lists by `first_rows`
        // into the output at ascending `g`. Every partition's `first_rows` is
        // strictly ascending and the values are globally unique (they are row
        // indices, and each row belongs to one partition), so this is a stable
        // merge over a total order -- byte-identical however the segments below
        // are split.
        const auto merge_segment = [&](std::vector<std::size_t> cur,
                                       const std::vector<std::size_t>& stop, std::size_t g) {
            for (;;) {
                std::size_t best = part_count;
                std::uint64_t best_row = std::numeric_limits<std::uint64_t>::max();
                for (std::size_t p = 0; p < part_count; ++p) {
                    if (cur[p] >= stop[p]) {
                        continue;
                    }
                    const std::uint64_t fr = partitions[p].first_rows[cur[p]];
                    if (fr < best_row) {
                        best_row = fr;
                        best = p;
                    }
                }
                if (best == part_count) {
                    break;
                }
                const auto& partition = partitions[best];
                const std::size_t local = cur[best];
                store_key(g, partition.keys[local]);
                for (std::size_t a = 0; a < n_aggs_; ++a) {
                    fs[(g * n_aggs_) + a] = partition.slots[(local * n_aggs_) + a];
                }
                ++cur[best];
                ++g;
            }
        };

        std::vector<std::size_t> part_end(part_count);
        for (std::size_t p = 0; p < part_count; ++p) {
            part_end[p] = partitions[p].keys.size();
        }

        // For q18's 3M groups the serial merge is ~24M comparisons plus 3M
        // slot copies -- tens of ms on the calling thread. Split the OUTPUT
        // into per-worker rank ranges via merge-path co-ranking: since every
        // `first_rows` value is unique, `sum_p lower_bound(first_rows_p, v)`
        // steps by exactly one at each value and so equals any target rank at
        // exactly one `v`. Each worker then merges the disjoint input slices
        // between two frontiers into its disjoint output slice.
        // The FinalOrdering node's worker ceiling and fan-out permission are the
        // plan's (src/runtime/PARALLELISM.md); `part_count` and `total / 4096`
        // stay here -- they need the group count discovery just produced. The
        // `1U << 17U` group floor is this merge's own threshold, beside it.
        std::size_t workers = 1;
        if (exec_ != nullptr && par_.final_ordering.decline == physical::FanOutDecline::None &&
            !on_worker_pool_thread() && part_count >= 2 && total >= std::size_t{1} << 17U) {
            workers = std::min({par_.final_ordering.worker_cap, part_count, total / 4096});
        }

        if (workers < 2) {
            merge_segment(std::vector<std::size_t>(part_count, 0), part_end, 0);
            return;
        }
        note_finalize_fanout();

        const std::uint64_t hi = owned_rows_seen_ + 1;
        const auto frontier = [&](std::size_t rank) {
            std::uint64_t lo = 0;
            std::uint64_t high = hi;
            while (lo < high) {
                const std::uint64_t mid = lo + ((high - lo) / 2);
                std::size_t sum = 0;
                for (std::size_t p = 0; p < part_count; ++p) {
                    const auto& fr = partitions[p].first_rows;
                    sum += static_cast<std::size_t>(std::lower_bound(fr.begin(), fr.end(), mid) -
                                                    fr.begin());
                }
                if (sum < rank) {
                    lo = mid + 1;
                } else {
                    high = mid;
                }
            }
            std::vector<std::size_t> off(part_count);
            for (std::size_t p = 0; p < part_count; ++p) {
                const auto& fr = partitions[p].first_rows;
                off[p] = static_cast<std::size_t>(std::lower_bound(fr.begin(), fr.end(), lo) -
                                                  fr.begin());
            }
            return off;
        };

        std::vector<std::vector<std::size_t>> bounds(workers + 1);
        bounds.front().assign(part_count, 0);
        bounds.back() = part_end;
        for (std::size_t w = 1; w < workers; ++w) {
            bounds[w] = frontier(w * total / workers);
        }

        auto batch = process_worker_pool().submit(workers, [&](std::size_t w) {
            std::size_t g = 0;
            for (std::size_t p = 0; p < part_count; ++p) {
                g += bounds[w][p];
            }
            merge_segment(bounds[w], bounds[w + 1], g);
        });
        batch.wait();
    }

    /// Dispatch the deferred merge to whichever key the owned run filled. Only
    /// one can be non-empty: the gate admits an owned run only before any group
    /// exists, so a single operator commits to one key and keeps it.
    void finalize_owned_active() {
        if (owned_async_hot_mode_) {
            finalize_owned_async_hot();
        } else if (owned_ordered_run_mode_) {
            finalize_owned_ordered_runs();
        } else if (!owned_pair_partitions_.empty()) {
            finalize_owned(
                owned_pair_partitions_, [&](std::size_t n) { pair_order_.resize(n); },
                [&](std::size_t g, const PairIntKey& key) {
                    pair_order_[g] = {static_cast<std::int64_t>(key.first),
                                      static_cast<std::int64_t>(key.second)};
                });
        } else if (!owned_int_partitions_.empty()) {
            finalize_owned(
                owned_int_partitions_, [&](std::size_t n) { int_order_.resize(n); },
                [&](std::size_t g, std::int64_t key) { int_order_[g] = key; });
        }
    }

    /// Join the streaming hot-table tasks once, then let one worker own each
    /// cold partition for its complete lifetime. There is no chunk-local
    /// histogram, scatter, or accumulate barrier: chunks only publish tasks;
    /// the two synchronization points here are whole-stream hot completion and
    /// whole-stream cold completion.
    void finalize_owned_async_hot() {
        if (owned_finalized_) {
            return;
        }
        owned_finalized_ = true;
        if (!owned_async_group_.has_value()) {
            invariant_violation("async hot aggregate join: task group already released");
        }
        try {
            owned_async_group_->wait();
        } catch (const std::exception& error) {
            owned_async_error_ = "async hot aggregate join: " + std::string(error.what());
            return;
        } catch (...) {
            owned_async_error_ = "async hot aggregate join: non-standard worker exception";
            return;
        }
        for (const auto& job : owned_async_jobs_) {
            if (job->error.has_value()) {
                owned_async_error_ = *job->error;
                return;
            }
        }

        auto& partitions = owned_int_partitions_;
        const std::size_t part_count = owned_async_part_count_;
        if (part_count >= 2) {
            note_finalize_fanout();
        }
        try {
            auto batch = process_worker_pool().submit(part_count, [&](std::size_t p) {
                auto& partition = partitions[p];
                std::size_t records = 0;
                for (const auto& job : owned_async_jobs_) {
                    records += job->records_by_partition[p].size();
                }

                // This reserve runs concurrently per owner. Unlike the failed
                // calling-thread reserve experiment, it neither serializes the
                // partitions nor guesses from total input rows; the exact
                // pre-aggregate count is a safe upper bound on distinct keys.
                partition.index.reserve(records);
                partition.keys.reserve(records);
                partition.first_rows.reserve(records);
                partition.slots.reserve(records);

                for (const auto& job : owned_async_jobs_) {
                    for (const auto& record : job->records_by_partition[p]) {
                        auto it = partition.index.find(record.key);
                        std::uint32_t local{};
                        if (it == partition.index.end()) {
                            local = static_cast<std::uint32_t>(partition.keys.size());
                            partition.index.emplace(record.key, local);
                            partition.keys.push_back(record.key);
                            partition.first_rows.push_back(record.first_row);
                            partition.slots.push_back(record.slot);
                        } else {
                            local = it->second;
                            if (record.slot.present()) {
                                auto& slot = partition.slots[local];
                                slot.double_value += record.slot.double_value;
                                slot.mark_present();
                            }
                        }
                    }
                }
            });
            batch.wait();
        } catch (const std::exception& error) {
            owned_async_error_ = "async cold aggregate: " + std::string(error.what());
            return;
        } catch (...) {
            owned_async_error_ = "async cold aggregate: non-standard worker exception";
            return;
        }

        // Pre-aggregate payloads are no longer needed. Release them before the
        // output arrays are allocated so peak memory is cold state + output,
        // rather than cold state + every streamed record + output.
        owned_async_jobs_.clear();
        owned_async_group_.reset();

        // Reuse the already-parallel first-occurrence merge. `first_rows` in
        // every cold partition is ascending because hot records are created at
        // their first source row and jobs are consumed in input order.
        owned_finalized_ = false;
        finalize_owned(
            partitions, [&](std::size_t n) { int_order_.resize(n); },
            [&](std::size_t g, std::int64_t key) { int_order_[g] = key; });
    }

    /// A clustered single-Int64 Count is summarized as contiguous runs while
    /// chunks arrive. If the complete stream is nondecreasing, adjacent runs
    /// are the final groups and no hash table is needed at all. If a later
    /// chunk disproves ordering, merge the run summaries through a hash map at
    /// emission; this preserves exact first-occurrence semantics without
    /// retaining or replaying the input rows.
    void finalize_owned_ordered_runs() {
        if (owned_finalized_) {
            return;
        }
        owned_finalized_ = true;
        if (owned_ordered_run_keys_.empty()) {
            n_groups_ = 0;
            return;
        }

        if (owned_ordered_runs_nondecreasing_) {
            const bool ord_timing = std::getenv("IBEX_ORDERED_RUN_TIMING") != nullptr;
            const auto ord_t0 = std::chrono::steady_clock::now();
            const std::size_t run_count = owned_ordered_run_keys_.size();
            const std::int64_t* const rk = owned_ordered_run_keys_.data();
            const std::size_t* const rc = owned_ordered_run_counts_.data();

            // How many workers can help. The build loop below is a segmented
            // reduction over `run_count` sorted (key, count) runs: keys are
            // nondecreasing so a group boundary is just `rk[i] != rk[i-1]`.
            // Each worker owns a contiguous run slice; a key straddling a slice
            // boundary has its leading partial count carried back to the group
            // the previous worker finished (at most `workers - 1` fixups).
            // Ceiling and permission from the plan; `run_count / 8192` and this
            // path's own `1U << 16U` run floor stay here (the ordered-run merge
            // is a strategy specialization, floor beside its code).
            std::size_t workers = 1;
            if (exec_ != nullptr && par_.final_ordering.decline == physical::FanOutDecline::None &&
                !on_worker_pool_thread() && run_count >= (std::size_t{1} << 16U) &&
                std::getenv("IBEX_DISABLE_PARALLEL_ORDERED_MERGE") == nullptr) {
                workers = std::min({par_.final_ordering.worker_cap, run_count / 8192});
            }

            std::size_t total = 0;
            if (workers < 2) {
                total = run_count == 0 ? 0 : 1;
                for (std::size_t i = 1; i < run_count; ++i) {
                    total += rk[i] == rk[i - 1] ? 0 : 1;
                }
                n_groups_ = total;
                int_order_.resize(total);
                AggSlotCore* slots = flat_slots_.grow_uninitialized(total).data();
                if (total != 0) {
                    std::size_t group = 0;
                    int_order_[0] = rk[0];
                    slots[0] = AggSlotCore{};
                    slots[0].count = static_cast<std::int64_t>(rc[0]);
                    for (std::size_t i = 1; i < run_count; ++i) {
                        if (rk[i] != int_order_[group]) {
                            ++group;
                            int_order_[group] = rk[i];
                            slots[group] = AggSlotCore{};
                        }
                        slots[group].count += static_cast<std::int64_t>(rc[i]);
                    }
                }
            } else {
                note_finalize_fanout();
                auto& pool = process_worker_pool();
                const std::size_t grain = (run_count + workers - 1) / workers;
                std::vector<std::size_t> local_groups(workers, 0);
                std::vector<char> boundary_open(workers, 0);
                {
                    auto batch = pool.submit(workers, [&](std::size_t w) {
                        const std::size_t begin = w * grain;
                        const std::size_t end = std::min(run_count, begin + grain);
                        if (begin >= end) {
                            return;
                        }
                        std::size_t g = 0;
                        for (std::size_t i = begin; i < end; ++i) {
                            if (i == 0 || rk[i] != rk[i - 1]) {
                                ++g;
                            }
                        }
                        local_groups[w] = g;
                        boundary_open[w] = (begin > 0 && rk[begin] == rk[begin - 1]) ? 1 : 0;
                    });
                    batch.wait();
                }
                std::vector<std::size_t> group_base(workers + 1, 0);
                for (std::size_t w = 0; w < workers; ++w) {
                    group_base[w + 1] = group_base[w] + local_groups[w];
                }
                total = group_base[workers];
                n_groups_ = total;
                int_order_.resize(total);
                AggSlotCore* slots = flat_slots_.grow_uninitialized(total).data();
                std::vector<std::int64_t> carry(workers, 0);
                {
                    auto batch = pool.submit(workers, [&](std::size_t w) {
                        const std::size_t begin = w * grain;
                        const std::size_t end = std::min(run_count, begin + grain);
                        if (begin >= end) {
                            return;
                        }
                        std::size_t i = begin;
                        if (boundary_open[w] != 0) {
                            const std::int64_t k0 = rk[begin];
                            std::int64_t c = 0;
                            while (i < end && rk[i] == k0) {
                                c += static_cast<std::int64_t>(rc[i]);
                                ++i;
                            }
                            carry[w] = c;
                        }
                        std::size_t g = group_base[w];
                        while (i < end) {
                            const std::int64_t k = rk[i];
                            std::int64_t c = 0;
                            while (i < end && rk[i] == k) {
                                c += static_cast<std::int64_t>(rc[i]);
                                ++i;
                            }
                            int_order_[g] = k;
                            slots[g] = AggSlotCore{};
                            slots[g].count = c;
                            ++g;
                        }
                    });
                    batch.wait();
                }
                for (std::size_t w = 0; w < workers; ++w) {
                    if (boundary_open[w] != 0) {
                        slots[group_base[w] - 1].count += carry[w];
                    }
                }
            }

            if (ord_timing) {
                const auto ms = std::chrono::duration<double, std::milli>(
                                    std::chrono::steady_clock::now() - ord_t0)
                                    .count();
                ibex::formatting::print(stderr,
                                        "[ord_run] finalize nondecreasing runs={} groups={} "
                                        "workers={} {}ms\n",
                                        run_count, total, workers, ms);
            }
            return;
        }

        robin_hood::unordered_flat_map<std::int64_t, std::uint32_t> index;
        std::vector<std::int64_t> counts;
        for (std::size_t i = 0; i < owned_ordered_run_keys_.size(); ++i) {
            const std::int64_t key = owned_ordered_run_keys_[i];
            auto it = index.find(key);
            std::uint32_t group{};
            if (it == index.end()) {
                group = static_cast<std::uint32_t>(int_order_.size());
                index.emplace(key, group);
                int_order_.push_back(key);
                counts.push_back(0);
            } else {
                group = it->second;
            }
            counts[group] += static_cast<std::int64_t>(owned_ordered_run_counts_[i]);
        }
        n_groups_ = int_order_.size();
        AggSlotCore* slots = flat_slots_.grow_uninitialized(n_groups_).data();
        for (std::size_t group = 0; group < n_groups_; ++group) {
            slots[group] = AggSlotCore{};
            slots[group].count = counts[group];
        }
    }

    // Two fixed-width-integer keys, grouped as one composite. Mirrors
    // process_rows_int exactly, packing (key_a, key_b) into a two-word key so
    // a single hash probe replaces the generic path's per-key Key comparison.
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
                publish_discovered(agg_entries, rows);
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
            publish_discovered(agg_entries, rows);
            return std::nullopt;
        }

        if (try_owned<PairIntKey, PairIntKeyHash>(
                [&](std::size_t row) { return pack(key_a_at(row), key_b_at(row)); }, rows, gids,
                agg_entries, owned_pair_partitions_, kPairOwnedMinRows)) {
            publish_fused_accumulation();
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
            publish_discovered(agg_entries, rows);
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

        publish_discovered(agg_entries, rows);
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

        publish_discovered(agg_entries, rows);
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
        // The Discovery node's worker cap and fan-out permission come from
        // the plan (src/runtime/PARALLELISM.md); the operator keeps only the two
        // checks it alone can make -- is it nested, did this operator's input so
        // far clear the floor. `decline != None` folds in `!exec_->can_fan_out()`
        // (the plan resolves `SingleCore` from it). `min_rows` is still the
        // operator's: it is the radix strategy's own admission gate, stricter
        // than `try_owned`'s, and lives beside the constant it names.
        if (exec_ == nullptr || on_worker_pool_thread()) {
            return false;
        }
        if (par_.discovery.decline != physical::FanOutDecline::None ||
            par_.discovery.worker_cap < 2) {
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
        const std::size_t workers = par_.discovery.worker_cap;
        std::size_t part_count = 1;
        while (part_count * 2 <= workers) {
            part_count *= 2;  // a power of two, so the partition is a mask
        }
        note_partition_fanout();
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
            auto next = static_cast<std::uint32_t>(base);
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
        if (exec_ == nullptr || par_.accumulation.decline != physical::FanOutDecline::None ||
            on_worker_pool_thread()) {
            return false;
        }
        auto& pool = process_worker_pool();
        const std::size_t threads = std::min(std::size_t{16}, par_.accumulation.worker_cap);
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
            key.values.emplace_back(str_order_[i]);
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
                key.values.emplace_back(std::string(decode(0, cat_order_[i])));
                return key;
            });
        } else {
            seed_generic_index_from_keys(n_groups_, [&](std::size_t i) {
                Key key;
                key.values.reserve(n_keys);
                for (std::size_t c = 0; c < n_keys; ++c) {
                    key.values.emplace_back(
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
        const std::size_t mask = multi_cat_slots_.size() - 1;
        std::size_t probe = static_cast<std::size_t>(hash) & mask;
        while (true) {
            const std::uint32_t slot = multi_cat_slots_[probe];
            if (slot == 0) {
                const std::uint32_t gid = std::forward<NewGroup>(new_group)();
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
            publish_fused_accumulation();
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

        publish_discovered(agg_entries, rows);
        return std::nullopt;
    }

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
            publish_discovered(agg_entries, rows);
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

        publish_discovered(agg_entries, rows);
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

        publish_discovered(agg_entries, rows);
        return std::nullopt;
    }

    /// Store non-null First aggregates at the row that creates each new group.
    /// `seeded[a]` is set only when the input column has no validity bitmap,
    /// so the discovery row is necessarily the first value under First's
    /// null-skipping semantics. The later accumulation pass may skip exactly
    /// these fields; nullable First and every Last still scan normally.
    void seed_discovery_first(std::size_t first_gid, const std::vector<std::size_t>& first_rows,
                              const std::vector<const ColumnEntry*>& agg_entries,
                              const std::vector<std::uint8_t>& seeded) {
        bool has_text = false;
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            has_text = has_text || (seeded[a] != 0U && plan_[a].kind == ExprType::String);
        }
        if (has_text && text_store_.size() < flat_slots_.size()) {
            text_store_.resize(flat_slots_.size());
        }
        const auto seed_one = [&](std::size_t gid, std::size_t row, std::size_t a) {
            const auto& entry = *agg_entries[a];
            auto& slot = flat_slots_[(gid * n_aggs_) + a];
            if (plan_[a].kind == ExprType::Double) {
                slot.double_value = std::get<Column<double>>(*entry.column)[row];
            } else if (plan_[a].kind == ExprType::Int) {
                slot.int_value = std::get<Column<std::int64_t>>(*entry.column)[row];
            } else {
                std::string value;
                if (plan_[a].categorical) {
                    value = std::string(std::get<Column<Categorical>>(*entry.column)[row]);
                } else {
                    value = std::string(std::get<Column<std::string>>(*entry.column)[row]);
                }
                text_store_[(gid * n_aggs_) + a] = std::move(value);
            }
            slot.mark_present();
        };

        // Ceiling and permission from the plan; the `parallel_min_rows` floor on
        // the first-occurrence count stays here (the shared knob, not a phase
        // constant).
        std::size_t threads = 1;
        if (exec_ != nullptr && par_.final_ordering.decline == physical::FanOutDecline::None &&
            !on_worker_pool_thread() && first_rows.size() >= exec_->parallel_min_rows) {
            threads = par_.final_ordering.worker_cap;
        }
        if (threads >= 2) {
            note_finalize_fanout();
            const std::size_t grain = (first_rows.size() + threads - 1) / threads;
            auto batch = process_worker_pool().submit(threads, [&](std::size_t worker) {
                const std::size_t begin = worker * grain;
                const std::size_t end = std::min(first_rows.size(), begin + grain);
                for (std::size_t local = begin; local < end; ++local) {
                    for (std::size_t a = 0; a < n_aggs_; ++a) {
                        if (seeded[a] != 0U) {
                            seed_one(first_gid + local, first_rows[local], a);
                        }
                    }
                }
            });
            batch.wait();
            if (exec_->parallel_stats != nullptr) {
                exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
            }
            return;
        }

        // Field-major serial order keeps each source column's reads together.
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (seeded[a] == 0U) {
                continue;
            }
            for (std::size_t local = 0; local < first_rows.size(); ++local) {
                seed_one(first_gid + local, first_rows[local], a);
            }
        }
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
                                 std::size_t rows, const std::vector<std::uint8_t>* skip = nullptr)
        -> bool {
        // Partition on the data alone -- not `exec_->can_fan_out()`, the thread
        // budget, or whether this runs on a pool thread. Those choose who
        // executes the morsels; the cut decides the arithmetic, and a cut that
        // varied with the schedule would let the same query answer differently
        // on two machines. Morsels run inline below when fan-out is not
        // available.
        if (n_groups_ == 0 || n_aggs_ == 0) {
            return false;
        }
        for (std::size_t a = 0; a < n_aggs_; ++a) {
            if (skip != nullptr && (*skip)[a] != 0U) {
                continue;
            }
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

        const auto run_morsel = [&](std::size_t m) {
            const std::size_t begin = m * grain;
            const std::size_t end = std::min(rows, begin + grain);
            if (begin < end) {
                accumulate_columns_into(gids, agg_entries, begin, end, &partials[m * stride],
                                        partial_scratch.data() + (m * scratch_span), skip);
            }
        };
        const std::size_t threads =
            exec_ != nullptr && par_.accumulation.decline == physical::FanOutDecline::None
                ? std::min(morsels, par_.accumulation.worker_cap)
                : std::size_t{1};
        const bool fanned_out = threads >= 2 && !on_worker_pool_thread();
        if (fanned_out) {
            auto& pool = process_worker_pool();
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(threads, [&](std::size_t) {
                while (true) {
                    const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (m >= morsels) {
                        return;
                    }
                    run_morsel(m);
                }
            });
            batch.wait();
        } else {
            for (std::size_t m = 0; m < morsels; ++m) {
                run_morsel(m);
            }
        }

        for (std::size_t m = 0; m < morsels; ++m) {
            const AggSlotCore* src = &partials[m * stride];
            const double* src_scratch = partial_scratch.data() + (m * scratch_span);
            for (std::size_t g = 0; g < n_groups_; ++g) {
                AggSlotCore* dst = &flat_slots_[g * n_aggs_];
                for (std::size_t a = 0; a < n_aggs_; ++a) {
                    if (skip != nullptr && (*skip)[a] != 0U) {
                        continue;
                    }
                    const std::size_t off = (g * scratch_stride_) + scratch_offset_[a];
                    agg_combine(dst[a], src[(g * n_aggs_) + a], plan_[a].func, plan_[a].kind,
                                scratch_stride_ == 0 ? nullptr : scratch_.data() + off,
                                scratch_stride_ == 0 ? nullptr : src_scratch + off);
                }
            }
        }
        if (fanned_out && exec_ != nullptr && exec_->parallel_stats != nullptr) {
            // Counts a fan-out, not a partition. The morsels are cut the same
            // way either way, so counting them when they ran inline would
            // report parallelism that never happened.
            exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    /// Accumulate `gids` either across workers or, when that is not worth it,
    /// serially — the one call every gid-assigning fast path ends with.
    void accumulate_gids(const std::uint32_t* gids,
                         const std::vector<const ColumnEntry*>& agg_entries, std::size_t rows,
                         const std::vector<std::uint8_t>* skip = nullptr) {
        if (!try_accumulate_parallel(gids, agg_entries, rows, skip)) {
            accumulate_columns_into(gids, agg_entries, 0, rows, flat_slots_.data(), scratch_.data(),
                                    skip);
        }
    }

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
                                 double* scratch_base,
                                 const std::vector<std::uint8_t>* skip = nullptr) {
        AggSlotCore* fs = base;
        const std::size_t rows = end;
        for (std::size_t agg_i = 0; agg_i < n_aggs_; ++agg_i) {
            if (skip != nullptr && (*skip)[agg_i] != 0U) {
                continue;
            }
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
                            const std::int64_t v = data[row];
                            slot.int_value = slot.present() ? std::min(slot.int_value, v) : v;
                            slot.mark_present();
                        }
                        break;
                    case ir::AggFunc::Max:
                        for (std::size_t row = begin; row < rows; ++row) {
                            if (has_nulls && !(*validity)[row])
                                continue;
                            auto& slot = slot_for(gids[row]);
                            const std::int64_t v = data[row];
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
        // Partition on the data alone -- not `exec_->can_fan_out()`, the thread
        // budget, or whether this runs on a pool thread. Those choose who
        // executes the morsels; the cut decides the arithmetic, and a cut that
        // varied with the schedule would let the same query answer differently
        // on two machines. Morsels run inline below when fan-out is not
        // available.
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

        const auto run_morsel = [&](std::size_t m, std::vector<std::uint8_t>& local_seen) {
            const std::size_t begin = m * grain;
            const std::size_t end = std::min(rows, begin + grain);
            if (begin >= end) {
                return;
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
                                    &partials[m * dict_size * n_aggs_],
                                    cat_partial_scratch.data() + (m * dict_size * scratch_stride_));
        };
        const std::size_t threads =
            exec_ != nullptr && par_.accumulation.decline == physical::FanOutDecline::None
                ? std::min(morsels, par_.accumulation.worker_cap)
                : std::size_t{1};
        const bool fanned_out = threads >= 2 && !on_worker_pool_thread();
        if (fanned_out) {
            auto& pool = process_worker_pool();
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(threads, [&](std::size_t) {
                std::vector<std::uint8_t> local_seen(dict_size, 0);
                while (true) {
                    const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (m >= morsels) {
                        return;
                    }
                    run_morsel(m, local_seen);
                }
            });
            batch.wait();
        } else {
            std::vector<std::uint8_t> local_seen(dict_size, 0);
            for (std::size_t m = 0; m < morsels; ++m) {
                run_morsel(m, local_seen);
            }
        }

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
        if (fanned_out && exec_ != nullptr && exec_->parallel_stats != nullptr) {
            // Counts a fan-out, not a partition. The morsels are cut the same
            // way either way, so counting them when they ran inline would
            // report parallelism that never happened.
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

        // One morsel's work, identical whoever runs it -- which is the point:
        // the partial it writes and the slot it lands in depend on `m` alone.
        const auto run_morsel = [&](std::size_t m) {
            const std::size_t begin = m * grain;
            const std::size_t end = std::min(rows, begin + grain);
            if (begin < end) {
                accumulate_ungrouped_range_impl(agg_entries, begin, end, &partials[(m * n_aggs_)],
                                                ung_scratch.data() + (m * scratch_stride_));
            }
        };
        const std::size_t threads =
            exec_ != nullptr && par_.accumulation.decline == physical::FanOutDecline::None
                ? std::min(morsels, par_.accumulation.worker_cap)
                : std::size_t{1};
        // Submitting from a pool thread would deadlock (`WorkerPool::submit`
        // aborts rather than allow it), and one worker gains nothing from a
        // round trip, so both run the morsels here. The arithmetic is unchanged
        // either way.
        if (threads >= 2 && !on_worker_pool_thread()) {
            auto& pool = process_worker_pool();
            std::atomic<std::size_t> cursor{0};
            auto batch = pool.submit(threads, [&](std::size_t) {
                while (true) {
                    const std::size_t m = cursor.fetch_add(1, std::memory_order_relaxed);
                    if (m >= morsels) {
                        return;
                    }
                    run_morsel(m);
                }
            });
            batch.wait();
            if (exec_ != nullptr && exec_->parallel_stats != nullptr) {
                exec_->parallel_stats->parallel_fields.fetch_add(1, std::memory_order_relaxed);
            }
        } else {
            for (std::size_t m = 0; m < morsels; ++m) {
                run_morsel(m);
            }
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
        // Deliberately NOT gated on `exec_->can_fan_out()`, the thread budget, or
        // whether this runs on a pool thread. Those decide who EXECUTES the
        // morsels, not how the range is cut, and a float reduction's result
        // depends on where it is cut. Keeping the cut a function of the data
        // alone is what makes one worker, eight workers, a serial run and a
        // nested run agree bit for bit. The caller runs the morsels inline
        // when it cannot fan out.
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
                    return std::unexpected("HashAggregateState: unsupported agg in build_output");
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
            exec_ != nullptr && par_.emission.decline == physical::FanOutDecline::None
                ? std::min(n_out_columns, par_.emission.worker_cap)
                : std::size_t{1};
        if (exec_ != nullptr && !on_worker_pool_thread() && threads > 1 &&
            n_groups_ >= par_.emission.row_floor) {
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
    std::optional<physical::AggregateColumnMapping> columns_;
    bool columns_bound_ = false;
    bool input_consumed_ = false;
    bool ordering_finalized_ = false;
    bool emitted_ = false;
    std::optional<Chunk> active_chunk_;

    bool initialized_ = false;
    bool cat_fast_path_ = false;
    bool str_fast_path_ = false;
    std::size_t n_aggs_ = 0;
    std::size_t n_groups_ = 0;
    std::vector<SlotPlan> plan_;
    std::vector<ColumnValue> group_templates_;
    std::vector<std::uint8_t> discovery_first_eligible_;

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
    AggregateDiscoveryTransfer discovery_transfer_;

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
    std::vector<OwnedPartition<std::int64_t, robin_hood::hash<std::int64_t>>> owned_int_partitions_;
    // q18's Polars-style sink: per-chunk 4096-slot hot reducers publish compact
    // pre-aggregates asynchronously; one final owner task per cold partition
    // builds the persistent maps. Jobs are declared before the task group so
    // reverse destruction joins every capture before releasing its storage.
    std::vector<std::unique_ptr<OwnedHotChunk>> owned_async_jobs_;
    std::optional<WorkerPool::TaskGroup> owned_async_group_;
    std::optional<std::string> owned_async_error_;
    std::size_t owned_async_part_count_ = 0;
    bool owned_async_hot_mode_ = false;
    std::vector<std::int64_t> owned_ordered_run_keys_;
    std::vector<std::size_t> owned_ordered_run_counts_;
    bool owned_ordered_run_mode_ = false;
    bool owned_ordered_runs_nondecreasing_ = true;
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
    /// The four structural nodes' fan-out policies, resolved by
    /// `build_physical_aggregate` (src/runtime/PARALLELISM.md). The operator
    /// reads `par_.<phase>.{decline, worker_cap}` for fan-out permission and the
    /// worker ceiling; it keeps only what the plan cannot know -- nesting
    /// (`on_worker_pool_thread`), the data-derived partition/run terms of each
    /// cap, and the strategy-specific admission floors (`kPairOwnedMinRows`, the
    /// ordered-run `1U << 16U`, `parallel_min_rows`).
    physical::AggregateParallelism par_{};
    ExecutionProfileEntry* discovery_profile_ = nullptr;
    ExecutionProfileEntry* accumulation_profile_ = nullptr;
    ExecutionProfileEntry* final_ordering_profile_ = nullptr;
    ExecutionProfileEntry* emission_profile_ = nullptr;
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

/// Serial executor for the hash fallback's typed structural chain. Discovery
/// and Accumulation exchange a bounded per-chunk transfer (or an explicit fused
/// marker); FinalOrdering and Emission are separate calls with enforceable
/// preconditions. Keeping this coordinator outside the state object prevents
/// output construction from silently triggering the ordering merge again.
class HashAggregatePhaseOperator final : public Operator {
   public:
    explicit HashAggregatePhaseOperator(std::unique_ptr<HashAggregateState> state)
        : state_(std::move(state)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        while (true) {
            auto discovered = state_->next_discovery();
            if (!discovered.has_value()) {
                return std::unexpected(std::move(discovered.error()));
            }
            if (!*discovered) {
                break;
            }
            if (auto accumulated = state_->accumulate_discovery(); !accumulated.has_value()) {
                return std::unexpected(std::move(accumulated.error()));
            }
        }
        if (auto error = state_->finalize_ordering()) {
            return std::unexpected(std::move(*error));
        }
        emitted_ = true;
        return state_->emit_output();
    }

   private:
    std::unique_ptr<HashAggregateState> state_;
    bool emitted_ = false;
};

auto make_hash_aggregate_operator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                                  const std::vector<ir::AggSpec>* aggregations,
                                  const ExecutionContext& exec, physical::AggregateParallelism par,
                                  std::optional<physical::AggregateColumnMapping> columns)
    -> OperatorPtr {
    auto state = std::make_unique<HashAggregateState>(std::move(child), group_by, aggregations,
                                                      exec, par, std::move(columns));
    return std::make_unique<HashAggregatePhaseOperator>(std::move(state));
}

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
/// hash aggregate phase operator by replaying the already-pulled chunk
/// ahead of the remaining child. The supported agg subset matches
/// HashAggregateState (Count/Sum/Min/Max/Mean on numeric columns);
/// build_operator only routes that subset here.
class ChunkedSortedAggregateOperator final : public Operator {
   public:
    ChunkedSortedAggregateOperator(
        OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
        const std::vector<ir::AggSpec>* aggregations, const ExecutionContext& exec,
        physical::AggregateParallelism par = {},
        std::optional<physical::AggregateColumnMapping> columns = std::nullopt)
        : child_(std::move(child)),
          group_by_(group_by),
          aggregations_(aggregations),
          exec_(&exec),
          par_(par),
          columns_(std::move(columns)) {}

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
                fallback_ =
                    make_hash_aggregate_operator(std::make_unique<PrependChunkOperator>(
                                                     std::move(*schema_only), std::move(child_)),
                                                 group_by_, aggregations_, *exec_, par_, columns_);
                return {};
            }
            done_ = true;
            input_eof_ = true;
            return {};
        }
        if (auto err = bind_aggregate_columns(columns_, columns_bound_, *group_by_, *aggregations_,
                                              first)) {
            return std::unexpected(std::move(*err));
        }
        if (!sorted_on_group_by(first) || needs_hash_fallback(first)) {
            fallback_ = make_hash_aggregate_operator(
                std::make_unique<PrependChunkOperator>(std::move(first), std::move(child_)),
                group_by_, aggregations_, *exec_, par_, columns_);
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
        if (!columns_.has_value() || group_by_->empty()) {
            return false;  // unbound, or a global aggregate: let the hash path handle it
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
        return std::ranges::all_of(columns_->group_by, [&chunk](const std::size_t index) {
            return !chunk.columns[index].validity.has_value();
        });
    }

    // Non-numeric First/Last (string/categorical) has no group-at-a-time
    // implementation here — route it to the hash operator, which handles any
    // type. Numeric First/Last streams natively (see accumulate_typed).
    [[nodiscard]] auto needs_hash_fallback(const Chunk& first) const -> bool {
        if (!columns_.has_value()) {
            return true;  // unbound: route to the hash operator, which rebinds
        }
        for (std::size_t i = 0; i < aggregations_->size(); ++i) {
            const ir::AggSpec& agg = (*aggregations_)[i];
            const std::optional<std::size_t>& input_idx = columns_->aggregate_inputs[i];
            if ((agg.func != ir::AggFunc::First && agg.func != ir::AggFunc::Last) ||
                !input_idx.has_value()) {
                continue;
            }
            const ColumnEntry* entry = &first.columns[*input_idx];
            const ExprType kind = expr_type_for_column(*entry->column);
            if (kind != ExprType::Int && kind != ExprType::Double) {
                return true;
            }
        }
        return false;
    }

    auto init_plan(const Chunk& first) -> std::optional<std::string> {
        if (!columns_.has_value()) {
            return "ChunkedSortedAggregateOperator: column mapping not bound";
        }
        n_aggs_ = aggregations_->size();
        plan_.resize(n_aggs_);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            const auto& agg = (*aggregations_)[i];
            plan_[i].func = agg.func;
            if (agg.func == ir::AggFunc::Count) {
                plan_[i].kind = ExprType::Int;
                continue;
            }
            const std::optional<std::size_t>& input_idx = columns_->aggregate_inputs[i];
            if (!input_idx.has_value()) {
                continue;
            }
            const ColumnEntry* entry = &first.columns[*input_idx];
            const ExprType kind = expr_type_for_column(*entry->column);
            if (kind != ExprType::Int && kind != ExprType::Double) {
                return "ChunkedSortedAggregateOperator: non-numeric aggregation not supported";
            }
            plan_[i].kind = kind;
        }
        key_templates_.clear();
        key_templates_.reserve(group_by_->size());
        for (const std::size_t index : columns_->group_by) {
            key_templates_.push_back(make_empty_like(*first.columns[index].column));
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
        if (!columns_.has_value()) {
            return "ChunkedSortedAggregateOperator: column mapping not bound";
        }
        std::vector<const ColumnValue*> key_cols;
        key_cols.reserve(group_by_->size());
        for (const std::size_t index : columns_->group_by) {
            key_cols.push_back(chunk.columns[index].column.get());
        }
        std::vector<const ColumnEntry*> agg_entries(n_aggs_, nullptr);
        for (std::size_t i = 0; i < n_aggs_; ++i) {
            const std::optional<std::size_t>& input_idx = columns_->aggregate_inputs[i];
            if (plan_[i].func == ir::AggFunc::Count || !input_idx.has_value()) {
                continue;
            }
            const ColumnEntry* entry = &chunk.columns[*input_idx];
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
    /// Forwarded verbatim to the hash aggregate fallback --
    /// the sorted stream itself has no fan-out point (it emits group-at-a-time).
    physical::AggregateParallelism par_{};
    std::optional<physical::AggregateColumnMapping> columns_;
    bool columns_bound_ = false;

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

auto make_chunked_aggregate_operator(OperatorPtr child, const std::vector<ir::ColumnRef>* group_by,
                                     const std::vector<ir::AggSpec>* aggregations,
                                     const ExecutionContext& exec,
                                     physical::AggregateParallelism parallelism,
                                     std::optional<physical::AggregateColumnMapping> columns)
    -> OperatorPtr {
    return std::make_unique<ChunkedSortedAggregateOperator>(
        std::move(child), group_by, aggregations, exec, parallelism, std::move(columns));
}

}  // namespace ibex::runtime
