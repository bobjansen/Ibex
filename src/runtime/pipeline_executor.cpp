// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/expr_predicates.hpp>
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
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <exception>
#include <expected>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "physical_plan.hpp"

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "chunk_conversion_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_chunked_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "physical_executor_internal.hpp"
#include "pipeline_executor_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime::pipeline_executor_detail {

struct ChunkIdentity {
    std::uint64_t sequence = 0;
    std::size_t row_offset = 0;
};

[[nodiscard]] auto table_to_chunk(Table table, ChunkIdentity identity) -> Chunk {
    auto chunk = runtime::table_to_chunk(std::move(table));
    chunk.sequence = identity.sequence;
    chunk.row_offset = identity.row_offset;
    return chunk;
}

auto materialize_row_local(const ir::Node& node, const TableRegistry& registry,
                           const ScalarRegistry* scalars, const ExternRegistry* externs,
                           const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    auto op = build_operator(node, registry, scalars, externs, exec, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    return materialize_operator(std::move(op.value()));
}

}  // namespace ibex::runtime::pipeline_executor_detail

namespace ibex::runtime {

namespace pipeline_executor_detail {

// Runtime-multithreading Phase 1, serial morsel slice. Owns the materialized
// input `Table` that the pipeline's `PartitionedTableSource` reads by pointer.
// `input_` is declared before `chain_` so the chain — which holds a raw
// pointer into `input_` — is destroyed first.
class OwningMorselPipelineOperator final : public Operator {
   public:
    OwningMorselPipelineOperator(std::unique_ptr<Table> input, OperatorPtr chain)
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
class SerialMorselOrderValidator final : public Operator {
   public:
    SerialMorselOrderValidator(OperatorPtr child, std::uint64_t expected_morsels, std::size_t grain)
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
                return std::unexpected("morsel pipeline: morsel identity gap or reordering");
            }
            ++next_sequence_;
            return result;
        }
        if (next_sequence_ != expected_morsels_) {
            return std::unexpected("morsel pipeline: missing output morsel");
        }
        return result;
    }

   private:
    OperatorPtr child_;
    std::uint64_t expected_morsels_ = 0;
    std::uint64_t next_sequence_ = 0;
    std::size_t grain_ = 1;
};

}  // namespace pipeline_executor_detail

namespace pipeline_executor_detail {

auto build_row_local_map_operator(const MapStep& step, OperatorPtr child,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const MapKernelFactory factory =
        step.factory != nullptr ? step.factory : map_kernel_factory(step.capability);
    if (factory == nullptr) {
        return std::unexpected("row-local map factory: unknown kernel capability");
    }
    return factory(step, std::move(child), scalars, externs, exec, nullptr, preserve_empty_morsels);
}

/// One unfused node as a step. The compatibility entry point for callers
/// outside a physical plan, which have a node and no fusion to express.
auto build_row_local_map_operator(const ir::Node& node, OperatorPtr child,
                                  const ScalarRegistry* scalars, const ExternRegistry* externs,
                                  const ExecutionContext& exec, bool preserve_empty_morsels)
    -> std::expected<OperatorPtr, std::string> {
    const auto capability = map_kernel_capability(node);
    if (!capability.has_value()) {
        return std::unexpected("row-local map factory: unsupported kernel capability");
    }
    const MapStep step{
        .node = &node, .capability = *capability, .factory = map_kernel_factory(*capability)};
    return build_row_local_map_operator(step, std::move(child), scalars, externs, exec,
                                        preserve_empty_morsels);
}

// The base of one worker's worker chain: a source the worker points at the
// morsel it just claimed. Two implementations, differing only in whether the
// morsel's rows are copied out of the shared input before the chain sees them.
class MorselSource : public Operator {
   public:
    /// Aim the source at rows [begin, end) of the pipeline's input. The next
    /// `next()` produces exactly that morsel and then reports exhaustion, so
    /// one call feeds one turn of the worker loop.
    virtual void set_morsel(std::size_t begin, std::size_t end, std::uint64_t sequence) = 0;
};

// Gathering source: materializes the morsel, then the chain above runs over it
// exactly as the serial path does. The fallback for any pipeline whose head this
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

// Range-filtering source: absorbs the pipeline's head `Filter` and evaluates its
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
/// away — and the physically fused filter/project form is included as well.
struct RangeHead {
    const ir::Expr* predicate = nullptr;
    const std::vector<ir::ColumnRef>* project = nullptr;  ///< null when unfused
};

/// The head's range form, or nullopt when it has to be built above a gathered
/// morsel instead.
///
/// Two things disqualify a head:
///
/// - A predicate that is not `is_range_native_expr`. Pipeline eligibility admits
///   Scalar calls, but every call still evaluates whole-table-and-slice, so
///   absorbing `abs(a) > 50` would re-run `abs` over the entire input once per
///   morsel — measured at 10x slower than serial on 20M rows. Gathering is the
///   correct choice there: the morsel is materialized once and the predicate
///   then runs over morsel-sized data.
/// - A column-less table, whose row count lives in the chunk's `logical_rows`
///   rather than in any column; only the gathering source carries that over.
[[nodiscard]] auto range_filter_head(const MapStep& step, const Table& input)
    -> std::optional<RangeHead> {
    if (input.columns.empty()) {
        return std::nullopt;
    }
    if (step.capability == MapKernelCapability::FilterGather) {
        // A head absorbs a filter and, at most, the projection directly above
        // it. A fused step that also carries an Update computes columns between
        // the two, and absorbing the projection here would skip that
        // computation -- the projection would then name a column nothing
        // produced. Such a step keeps its operator chain.
        if (step.fused_update != nullptr) {
            return std::nullopt;
        }
        const auto& predicate = ir::node_cast<ir::FilterNode>(*step.node).predicate();
        if (!is_range_native_expr(predicate)) {
            return std::nullopt;
        }
        // A planner-fused Project rides along exactly as the fused IR kind's
        // column list does below: same head, same absorbed projection.
        const std::vector<ir::ColumnRef>* project =
            step.fused_project != nullptr
                ? &ir::node_cast<ir::ProjectNode>(*step.fused_project).columns()
                : nullptr;
        return RangeHead{.predicate = &predicate, .project = project};
    }
    if (step.capability == MapKernelCapability::FilterProjectGather &&
        step.update_fields == nullptr && step.filter_predicate != nullptr &&
        step.project_columns != nullptr) {
        if (!is_range_native_expr(*step.filter_predicate)) {
            return std::nullopt;
        }
        return RangeHead{.predicate = step.filter_predicate, .project = step.project_columns};
    }
    return std::nullopt;
}

// One worker's private copy of the pipeline's map chain. The operators are
// per-worker (they carry mutable per-chunk state); the IR nodes, registries,
// and the input table they read are shared and immutable for the pipeline's
// lifetime.
struct MorselWorkerChain {
    MorselSource* source = nullptr;  // owned by `chain`, re-aimed per morsel
    OperatorPtr chain;
};

[[nodiscard]] auto build_morsel_worker_chain(const std::vector<MapStep>& operators,
                                             const Table& input, const ScalarRegistry* scalars,
                                             const ExternRegistry* externs,
                                             const ExecutionContext& exec,
                                             const JoinProbeFactory* probe_head = nullptr)
    -> std::expected<MorselWorkerChain, std::string> {
    // A qualifying head is absorbed into the source rather than built as an
    // operator above it — same output, without materializing the morsel first.
    // Not available under a probe head: that optimization reads the first
    // operator as a filter over the SOURCE's rows, and with a probe between
    // them the source's rows are the probe side, not the filter's input.
    std::size_t first_op = 0;
    std::unique_ptr<MorselSource> source;
    if (!operators.empty() && probe_head == nullptr) {
        if (auto head = range_filter_head(operators.front(), input); head.has_value()) {
            source = std::make_unique<RangeFilterMorselSource>(input, head->predicate,
                                                               head->project, scalars);
            first_op = 1;
        }
    }
    if (source == nullptr) {
        source = std::make_unique<GatherMorselSource>(input);
    }

    MorselWorkerChain worker{.source = source.get(), .chain = std::move(source)};
    // The probe runs first, on this worker's own morsel of the probe side, and
    // every map above it runs in the same worker on the probe's output. That
    // is the whole point of the fused shape: the join's output is never
    // assembled as a table between the probe and the maps.
    if (probe_head != nullptr) {
        worker.chain = probe_head->attach(std::move(worker.chain),
                                          /*preserve_empty_morsels=*/true);
    }
    for (std::size_t i = first_op; i < operators.size(); ++i) {
        const MapStep& op_node = operators[i];
        // `preserve_empty_morsels` is what makes one input morsel yield exactly
        // one identified output morsel — the merger indexes by sequence, so a
        // silently coalesced empty result would be a lost slot, not a smaller
        // answer.
        auto next = build_row_local_map_operator(op_node, std::move(worker.chain), scalars, externs,
                                                 exec, true);
        if (!next.has_value()) {
            // The plan's step vocabulary only admits row-local map kinds.
            return std::unexpected("morsel pipeline: " + next.error());
        }
        worker.chain = std::move(next.value());
    }
    return worker;
}
/// Wait on `cv` (holding `lock`) until `pred()`, cooperatively running queued
/// pool tasks while parked when the caller is a pool worker.
///
/// Without this a pool worker blocked on a pipeline ring strands any nested
/// `pool.submit` under it: its child tasks sit in the queue while every other
/// worker is parked in its own ring wait, none in the pool's dispatch loop. See
/// `plans/cooperative-pipeline-waits-plan.md`.
///
/// Assist only when this thread is a pool worker AND a fan-out it started is
/// still running — the sole case with queued work to run. A non-pool caller (the
/// merger thread, a stage producer) owns no pool task to strand; a pipeline
/// worker that has nested nothing has an empty-for-it queue. Both take the plain
/// `cv.wait`, so the common backpressure park costs exactly what it did before
/// this existed, and the stage-ledger accounting in `RingWaitScope` is unchanged.
template <typename Pred>
void cooperative_ring_wait(std::condition_variable& cv, std::unique_lock<std::mutex>& lock,
                           Pred pred) {
    if (!on_worker_pool_thread() || !this_thread_has_outstanding_nested_work()) {
        const RingWaitScope ring_wait;
        cv.wait(lock, pred);
        return;
    }
    auto& pool = process_worker_pool();
    while (!pred()) {
        lock.unlock();
        const bool helped = pool.try_run_one_pending();
        lock.lock();
        if (pred()) {
            break;
        }
        if (!helped) {
            // Genuine park — the only interval that is "ring wait". Bounded so a
            // task another worker runs while we sleep cannot leave fresh queue
            // work unnoticed (that wakeup lands on the pool queue, not `cv`).
            const RingWaitScope ring_wait;
            cv.wait_for(lock, kCoopPollInterval, [&pred] { return pred(); });
        }
    }
}

/// A bounded, sequence-ordered handoff between several producers and one
/// consumer — the one implementation of that shape in the runtime.
///
/// Slots are addressed `sequence % window`, so a producer may run at most
/// `window` sequences ahead of the consumer and then parks; the consumer parks
/// on the slot it needs next. Both waits are wrapped in `RingWaitScope`,
/// because produced-ahead and waiting-on-workers are idle rather than work —
/// counting them as work makes a blocked worker read as a busy one and
/// overstates occupancy.
///
/// Failure is ordered by sequence, not by arrival: the lowest-sequence failure
/// is the one reported, so the message a query returns never depends on which
/// thread lost a race. `record_fault` takes a static string and allocates
/// nothing, which is the only reporting path still available when allocation is
/// what failed.
///
/// Producer liveness is tracked so the consumer cannot wait for a sequence that
/// is never coming: a producer that leaves for any reason — exhaustion, error,
/// exception — must call `producer_exited`, which is what turns "a worker died"
/// into an error rather than a hang.
class OrderedChunkRing {
   public:
    OrderedChunkRing(std::size_t window, std::size_t producers)
        : window_(window == 0 ? 1 : window),
          ring_(window == 0 ? 1 : window),
          active_producers_(producers) {}

    OrderedChunkRing(const OrderedChunkRing&) = delete;
    auto operator=(const OrderedChunkRing&) -> OrderedChunkRing& = delete;
    OrderedChunkRing(OrderedChunkRing&&) = delete;
    auto operator=(OrderedChunkRing&&) -> OrderedChunkRing& = delete;
    ~OrderedChunkRing() = default;

    /// What a producer should do with the sequence it just claimed.
    enum class Acquire : std::uint8_t {
        Proceed,  ///< the slot is free; produce into it
        Abandon,  ///< cancelled, or a lower sequence already failed
    };

    /// Park until this sequence's slot is free. Called with no lock held.
    [[nodiscard]] auto acquire(std::uint64_t sequence) -> Acquire {
        std::unique_lock lock(mutex_);
        cooperative_ring_wait(space_, lock, [&] {
            return cancelled_ || sequence < released_ + window_ ||
                   (has_error_ && error_sequence_ < sequence);
        });
        // Only ever abandons sequences above the reported failure, so the
        // consumer still receives everything below it.
        return (cancelled_ || (has_error_ && error_sequence_ < sequence)) ? Acquire::Abandon
                                                                          : Acquire::Proceed;
    }

    void publish(std::uint64_t sequence, Chunk chunk) {
        {
            const std::scoped_lock lock(mutex_);
            ring_[static_cast<std::size_t>(sequence % window_)] = std::move(chunk);
        }
        ready_.notify_one();
    }

    /// Record an owned message. The caller has already built the string, so
    /// taking it by value and moving it under the lock never allocates here.
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

    /// Record a message in static storage. Allocates nothing at all, so it is
    /// the only reporting path available once allocation is what failed.
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

    void producer_exited() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            --active_producers_;
        }
        ready_.notify_all();
    }

    /// Take the chunk at `sequence`, or nullopt when the run stopped before
    /// producing it — cancelled, failed, or out of producers. The caller asks
    /// `failure()` for why.
    [[nodiscard]] auto take(std::uint64_t sequence) -> std::optional<Chunk> {
        std::optional<Chunk> chunk;
        {
            std::unique_lock lock(mutex_);
            const auto slot = static_cast<std::size_t>(sequence % window_);
            cooperative_ring_wait(ready_, lock, [&] {
                return ring_[slot].has_value() || cancelled_ || active_producers_ == 0 ||
                       (has_error_ && error_sequence_ <= sequence);
            });
            if (ring_[slot].has_value()) {
                chunk = std::move(ring_[slot]);
                ring_[slot].reset();
                ++released_;
            }
        }
        if (chunk.has_value()) {
            space_.notify_all();
        }
        return chunk;
    }

    [[nodiscard]] auto failure() const -> std::optional<std::string> {
        const std::scoped_lock lock(mutex_);
        if (!has_error_) {
            return std::nullopt;
        }
        return error_fixed_ != nullptr ? std::string(error_fixed_) : error_owned_;
    }

    void cancel() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        wake_all();
    }

   private:
    /// True if `sequence` becomes the reported failure. Lowest sequence wins,
    /// so the error a query reports never depends on thread timing.
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

    std::size_t window_;
    mutable std::mutex mutex_;
    std::condition_variable ready_;  // consumer waits for the next sequence
    std::condition_variable space_;  // producers wait for ring space
    std::vector<std::optional<Chunk>> ring_;
    std::uint64_t released_ = 0;
    std::size_t active_producers_ = 0;
    bool cancelled_ = false;
    // The failure channel is split so it can be written without allocating.
    // `error_owned_` carries a message moved in from a producer; `error_fixed_`
    // points at static storage.
    bool has_error_ = false;
    std::uint64_t error_sequence_ = 0;
    std::string error_owned_;
    const char* error_fixed_ = nullptr;
};

// Runtime-multithreading Phase 1: the morsel pipeline executor.
//
// Workers pull numbered morsels from one shared cursor over the immutable
// materialized input, run their own chain over each, and deposit the result in
// a bounded ring indexed by `sequence`. `next()` is the ordered merger: it
// releases results strictly in sequence order, so the operator's output is
// byte-identical to the serial chain's regardless of completion order. The ring
// is the plan's bounded in-flight queue — a worker that runs ahead of the
// consumer by a full window blocks instead of buffering the whole pipeline.
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
class MorselPipelineOperator final : public Operator {
   public:
    MorselPipelineOperator(std::unique_ptr<Table> input, std::vector<MorselWorkerChain> workers,
                           std::size_t grain, std::uint64_t morsel_count, WorkerPool& pool)
        : input_(std::move(input)),
          workers_(std::move(workers)),
          grain_(grain == 0 ? 1 : grain),
          morsel_count_(morsel_count),
          pool_(&pool),
          ring_(std::max<std::size_t>(workers_.size() * 2, 2), workers_.size()) {}

    ~MorselPipelineOperator() override { cancel_and_join(); }

    MorselPipelineOperator(const MorselPipelineOperator&) = delete;
    auto operator=(const MorselPipelineOperator&) -> MorselPipelineOperator& = delete;
    MorselPipelineOperator(MorselPipelineOperator&&) = delete;
    auto operator=(MorselPipelineOperator&&) -> MorselPipelineOperator& = delete;

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

        std::optional<Chunk> chunk = ring_.take(next_sequence_);
        if (chunk.has_value()) {
            ++next_sequence_;
            return chunk;
        }

        // No chunk: the pipeline stopped early. Report why, deterministically.
        //
        // An interrupt outranks a recorded data error. A worker that fails at
        // the moment the user hits Ctrl+C is a race, and reporting its message
        // would make cancellation surface as an arbitrary query error depending
        // on which thread won. The cancellation contract says such a query
        // reports "interrupted", so the interrupt is checked first.
        if (interrupt_requested()) {
            return fail(interrupt_message());
        }
        if (auto failure = ring_.failure(); failure.has_value()) {
            return fail(std::move(*failure));
        }
        return fail("morsel pipeline: missing output morsel");
    }

   private:
    // Whatever happens to a worker — normal exhaustion, a recorded error, or an
    // exception — it must stop counting as active and must wake the merger.
    // Skipping this on any path leaves the consumer waiting for a sequence that
    // is never coming, which is a hang rather than an error.
    void worker_exited() noexcept { ring_.producer_exited(); }

    void run_worker(std::size_t worker_id) noexcept {
        // Cleanup runs however this scope is left, so no path can leave the
        // consumer waiting on a worker that is gone.
        // One-shot scope guard: aggregate-initialised, never copied or moved.
        // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
        struct ExitGuard {
            MorselPipelineOperator* self;
            ~ExitGuard() { self->worker_exited(); }
        } const guard{this};

        std::uint64_t sequence = 0;
        try {
            run_worker_loop(worker_id, sequence);
        } catch (const std::exception& error) {
            // An exception is not part of the operator protocol (evaluation
            // reports failure through `expected`), so it is something
            // unplanned — an allocation failure while materializing a morsel,
            // say. Convert it to a sequence-tagged pipeline error so it obeys the
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
                ring_.record_error(
                    sequence, "morsel pipeline: worker exception: " + std::string(error.what()));
            } catch (...) {
                ring_.record_fault(sequence,
                                   "morsel pipeline: worker exception (no memory to report it)");
            }
        } catch (...) {
            ring_.record_fault(sequence, "morsel pipeline: worker threw a non-standard exception");
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
            // Backpressure: this morsel's slot is only free once the consumer
            // has released the morsel `window` ahead of it.
            if (ring_.acquire(sequence) == OrderedChunkRing::Acquire::Abandon) {
                break;
            }
            if (interrupt_requested()) {
                ring_.cancel();
                break;
            }

            const auto [begin, end] = morsel_row_range(rows, grain_, sequence);
            worker.source->set_morsel(begin, end, sequence);
            auto produced = worker.chain->next();

            if (!produced.has_value()) {
                ring_.record_error(sequence, std::move(produced.error()));
                break;
            }
            if (!produced->has_value()) {
                ring_.record_fault(sequence, "morsel pipeline: worker produced no output morsel");
                break;
            }
            Chunk out = std::move(**produced);
            if (out.sequence != sequence || out.row_offset != begin) {
                ring_.record_fault(sequence, "morsel pipeline: morsel identity gap or reordering");
                break;
            }
            ring_.publish(sequence, std::move(out));
        }
    }

    // Called from the destructor, so nothing here may throw: an escaping
    // exception during destruction terminates the process. Worker bodies are
    // already noexcept and convert failures into pipeline errors, so there is
    // nothing for `wait()` to rethrow — this guards the path regardless.
    void cancel_and_join() noexcept {
        try {
            ring_.cancel();
            batch_.wait();
        } catch (...) {  // NOLINT(bugprone-empty-catch)
            // Nothing left to report: the caller is either unwinding or has
            // already chosen the message it will return.
        }
    }

    // Drain the pipeline cleanly at EOF, then check the per-worker chains really
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
        if (auto failure = ring_.failure(); failure.has_value()) {
            return std::unexpected(std::move(*failure));
        }
        for (auto& worker : workers_) {
            auto trailing = worker.chain->next();
            if (!trailing.has_value()) {
                return std::unexpected(std::move(trailing.error()));
            }
            if (trailing->has_value()) {
                return std::unexpected("morsel pipeline: unexpected trailing morsel");
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
    std::vector<MorselWorkerChain> workers_;
    std::size_t grain_ = 1;
    std::uint64_t morsel_count_ = 0;
    WorkerPool* pool_;

    std::atomic<std::uint64_t> cursor_{0};

    // The ordered handoff between the workers and this operator's `next()`.
    OrderedChunkRing ring_;

    std::uint64_t next_sequence_ = 0;
    bool started_ = false;
    bool finished_ = false;
    WorkerPool::Batch batch_;
};

// Runtime-multithreading Phase 2: the two-phase parallel filter.
//
// What the ordered merger above cannot remove is the merge itself. Each worker
// materializes its morsel's surviving rows, and `MaterializeOperator` then
// copies all of them again into one table — so a filter pipeline copies its
// output twice where the serial path copies it once. That is why morsel parallelism wins
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
        // Sequence 0 / row_offset 0: this operator emits the pipeline's whole
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
                return project_table(input, ir::node_cast<ir::ProjectNode>(node).columns());
            case ir::NodeKind::Rename:
                return rename_table(input, ir::node_cast<ir::RenameNode>(node).renames());
            default:
                // The pipeline builder only admits `is_metadata_only_node` kinds
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

// How many workers a pipeline of `morsel_count` morsels over `rows` rows should
// run on: 0 means "stay on the serial morsel chain".
//
// This is the plan's grain-size serial threshold. Below it, task dispatch,
// ring synchronization, and the merge cost more than the map they parallelize —
// cache-resident work should not pay for threads. A single morsel is serial by
// definition, and a one-thread budget means the caller asked for serial.
/// Whether this input is worth morselizing at all — a *different* question from
/// how many workers it deserves, and conflating the two is a trap worth naming.
///
/// A "refused" pipeline used to mean a serial sweep of morsels, which still pays
/// per-morsel materialization and the merge concat. So refusing by dropping the
/// worker count made a small query **slower than never forming a pipeline**:
/// measured 100ms against 36ms for the plain serial path, and it got worse once
/// the grain was derived, because that turned 2 morsels into 32. When the
/// answer is no, the input has to run as ONE whole-table chunk.
///
/// Two thresholds, because a pipeline's cost has two dimensions. Rows alone
/// cannot express it: 131,072 rows won at 6 columns and lost at 2 on the very
/// same predicate, and every row threshold puts those on the same side.
[[nodiscard]] auto is_worth_morselizing(const ExecutionContext& exec, std::size_t rows,
                                        std::size_t columns) -> bool {
    if (rows < exec.parallel_min_rows) {
        return false;
    }
    return exec.parallel_min_cells == 0 || columns == 0 ||
           rows * columns >= exec.parallel_min_cells;
}

[[nodiscard]] auto morsel_worker_count(const ExecutionContext& exec, std::uint64_t morsel_count)
    -> std::size_t {
    if (morsel_count < 2 || !exec.can_fan_out()) {
        return 0;
    }
    // Past the parallel gate: consulting the pool here is free of the
    // construct-before-declining hazard because a parallel query has already
    // built it (or is about to, on its first fan-out).
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t workers =
        std::min({budget, pool_size, static_cast<std::size_t>(morsel_count)});
    return workers < 2 ? 0 : workers;
}

// Build one eligible row-local parallel-map chain as a morsel pipeline: materialize its
// input subtree once, then run the chain over morsels of that table instead of
// one whole-table chunk. The operators are ordered source-to-sink.
//
// Two executors, one morsel model. A large input fans out across the worker
// pool and is reassembled by `MorselPipelineOperator`'s ordered merger; a small
// one (or a single-threaded budget) runs the same morsels serially through a
// `PartitionedTableSource`, where `MaterializeOperator`'s in-order concat is
// the trivially ordered merger. Both stamp and check the same morsel identity,
// so both are byte-identical to the plain serial chain — which is exactly what
// lets the threshold move without changing an answer.
//
// LOAD-BEARING INVARIANT — materialize before fan-out. The input subtree is
// executed to a `Table` here, on this thread, and every morsel source below
// takes that finished table by reference. That is what makes a deferred/lazy
// source safe in a pipeline: its decode runs exactly once, serially, before any
// worker exists, so neither `LazyTable::cache_` nor a plugin's `decode_`
// closure is ever touched concurrently. It is why `build_operator`'s seam no
// longer screens pipelines for deferred sources.
//
// The morsel sources all take `const Table&`, so the invariant is enforced by
// their signatures rather than by a check. Streaming a source's morsels
// directly into workers would mean handing them something other than a
// finished table — at which point the LazyTable synchronization contract
// applies in full and eligibility has to be re-established.
//
// `scan_pipeline_worker_count` and `build_pipelined_scan` are defined below and
// declared in `pipeline_executor_internal.hpp`; both are consulted by the run
// builder, which decides its own source strategy.

}  // namespace pipeline_executor_detail

// Internal linkage to match the forward declarations beside the join, which
// sit inside this TU's anonymous namespace; the definitions must live down
// here because they use the morsel executor, which is defined below the join.
namespace pipeline_executor_detail {

/// Build a streaming join here and take its probe, so a map pipeline above it
/// can run that probe at the head of its own worker chains.
///
/// Returns an empty optional -- not an error -- for every join this cannot
/// fuse: a materializing one, a semi/anti one, a deferred probe scan (whose
/// right subtree must be interpreted by the join itself, after it publishes
/// build-side bounds), and any orientation that leaves no materialized probe
/// side. The caller then materializes the join's output as it always did.
///
/// Narrow on purpose. The point is to establish that two pipelines meeting at
/// a barrier can be built and can produce the right answer; widening the
/// shapes is cheap once that is true, and pointless before.
auto try_take_join_probe(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<std::optional<FusibleJoinProbe>, std::string> {
    if (std::getenv("IBEX_PROBE_MORSELS") == nullptr) {
        return std::optional<FusibleJoinProbe>{};
    }
    const physical::Plan plan = physical::plan_physical(node, registry, externs);
    const physical::JoinPlan& jp = plan.join;
    if (!jp.describes || jp.strategy != physical::JoinStrategy::StreamingProbe ||
        (jp.branch != physical::JoinBranch::SingleKeyInner &&
         jp.branch != physical::JoinBranch::PairIntInner)) {
        return std::optional<FusibleJoinProbe>{};
    }
    const auto& join = ir::node_cast<ir::JoinNode>(node);
    if (deferred_probe_scan_of(*join.children()[1], exec).scan != nullptr) {
        return std::optional<FusibleJoinProbe>{};
    }

    auto left_op = build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
    if (!left_op.has_value()) {
        return std::unexpected(std::move(left_op.error()));
    }
    auto right =
        materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
    if (!right.has_value()) {
        return std::unexpected(std::move(right.error()));
    }
    return take_fusible_join_probe(std::move(left_op.value()), std::move(right.value()),
                                   &join.keys(), exec, join.suffix(), &join.pending_order(),
                                   physical_executor_detail::resolved_join_parallelism(exec));
}

auto probe_morsel_workers(const Table& input, const ExecutionContext& exec) -> std::size_t {
    // OPT-IN, and the measurement is why. On its own this is a LOSS: the
    // probe already fans out inside one chunk (`probe_ranges_parallel`) with
    // no copy, and morselizing replaces that with a per-morsel gather plus an
    // ordered merge. Measured at 8 cores, interleaved, median of 15:
    // q05 +1.7%, q09 +6.8% -- the only two PDS-H queries where it fires.
    //
    // It is wired anyway because the gather is not what this shape is for.
    // A probe morsel becomes worth its gather when the filters and
    // projections above the join run in the SAME chain, so the join's output
    // is never materialized between them. That needs the probe admitted into
    // the plan's step vocabulary, which is the next piece; this is the half
    // that had to work first. This remains opt-in: the former "22/22" claim is
    // stale, and SF4 q09 currently stalls in both the pre-extraction baseline
    // and this tree when the knob is enabled. Resolve that independently
    // before admitting this shape by default.
    if (std::getenv("IBEX_PROBE_MORSELS") == nullptr) {
        return 0;
    }
    if (!exec.can_fan_out() || on_worker_pool_thread()) {
        return 0;
    }
    if (!is_worth_morselizing(exec, input.rows(), input.columns.size())) {
        return 0;
    }
    const std::size_t grain = morsel_grain(exec, input.rows());
    return morsel_worker_count(exec, partitioned_morsel_count(input, grain));
}

auto build_probe_morsel_pipeline(Table input, const JoinProbeFactory& probe, std::size_t workers,
                                 const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string> {
    auto owned = std::make_unique<Table>(std::move(input));
    const std::size_t grain = morsel_grain(exec, owned->rows());
    const auto expected_morsels = partitioned_morsel_count(*owned, grain);
    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->parallel_pipelines.fetch_add(1, std::memory_order_relaxed);
        exec.parallel_stats->morsels.fetch_add(expected_morsels, std::memory_order_relaxed);
    }

    static const std::vector<MapStep> no_steps;
    std::vector<MorselWorkerChain> chains;
    chains.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
        // The chain is source + probe. Each worker gets its OWN `JoinProbe` --
        // its scratch vectors and its per-chunk categorical head table are
        // per-worker state -- and they share one build, because the index and
        // the build side are both `shared_ptr<const>` and have been since
        // `f6a1a632` and `6df9a966`. Copying the probe is what that ownership
        // was for.
        auto worker = build_morsel_worker_chain(no_steps, *owned, nullptr, nullptr, exec, &probe);
        if (!worker.has_value()) {
            return std::unexpected(std::move(worker.error()));
        }
        chains.push_back(std::move(worker.value()));
    }
    return std::make_unique<MorselPipelineOperator>(std::move(owned), std::move(chains), grain,
                                                    expected_morsels, process_worker_pool());
}

}  // namespace pipeline_executor_detail

namespace pipeline_executor_detail {

/// The steps of a plan's parallel prefix, ordered source-to-sink. A plan
/// records steps sink-first; every executor here composes bottom-up.
auto parallel_pipeline_operators(const physical::Plan& plan) -> std::vector<MapStep> {
    std::vector<MapStep> operators;
    operators.reserve(plan.parallel_step_count());
    for (std::size_t i = plan.parallel_end; i > plan.parallel_begin; --i) {
        operators.push_back(plan.steps[i - 1]);
    }
    return operators;
}

/// Run a physical map pipeline over morsels. The plan says which steps may run
/// in parallel (`parallel_steps`) and what feeds them (`parallel_input_node`);
/// this builds that input, materializes it, and executes the prefix over its
/// morsels. It is the pipeline's parallel *mode*, not a separate executor with
/// its own idea of what is eligible.
///
/// `steps` inside a plan are sink-first; the operators here run source-to-sink,
/// so the prefix is reversed once, on the build thread.
auto build_map_pipeline_parallel(const physical::Plan& plan, const TableRegistry& registry,
                                 const ScalarRegistry* scalars, const ExternRegistry* externs,
                                 const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const std::vector<MapStep> operators = parallel_pipeline_operators(plan);
    const ir::Node* input_node = physical::parallel_input_node(plan);
    if (input_node == nullptr) {
        return std::unexpected("map pipeline: parallel mode without an input");
    }

    // Source strategy, decided here because it is a property of this run's
    // input rather than of the query's root. A decomposable deferred scan can
    // feed the run one unit at a time -- decode and maps in the same worker
    // task, the ordered ring feeding whatever is above -- instead of being
    // decoded whole and morselized. Probe scans keep their join-owned dynamic
    // filter timing (a null filter slot is what distinguishes them) and so do
    // not stream here.
    if (exec.stream_scans && input_node->kind() == ir::NodeKind::Scan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(*input_node);
        if (!registry.contains(scan.source_name())) {
            if (const auto* deferred = exec.deferred_scan(scan.source_name());
                deferred != nullptr && deferred->filter == nullptr) {
                auto units = deferred_scan_units(*deferred);
                if (units.size() > 1 && scan_pipeline_worker_count(units.size()) >= 2) {
                    return build_pipelined_scan(operators, true, *deferred, std::move(units),
                                                scalars, externs, exec);
                }
            }
        }
    }

    // Fused probe: when this pipeline's input is a streaming join, take its
    // build and its probe side and run the probe at the head of every worker
    // chain, instead of materializing the join's OUTPUT and morselizing that.
    // The probe side becomes the morsel source, so the join's output is never
    // assembled as a table at all -- it is produced a morsel at a time by the
    // same worker that then runs the maps over it. Two pipelines meeting at a
    // barrier, which is the shape this plan has been working toward.
    std::optional<JoinProbeFactory> fused_probe;
    std::unique_ptr<Table> owned;
    if (input_node->kind() == ir::NodeKind::Join) {
        auto fused = try_take_join_probe(*input_node, registry, scalars, externs, exec, model_out);
        if (!fused.has_value()) {
            return std::unexpected(std::move(fused.error()));
        }
        if (fused->has_value()) {
            owned = std::make_unique<Table>(std::move((*fused)->probe_side));
            fused_probe = std::move((*fused)->probe);
        }
    }
    if (owned == nullptr) {
        auto input_op = build_operator(*input_node, registry, scalars, externs, exec, model_out);
        if (!input_op.has_value()) {
            return std::unexpected(std::move(input_op.error()));
        }
        auto input_tbl = materialize_operator(std::move(input_op.value()));
        if (!input_tbl.has_value()) {
            return std::unexpected(std::move(input_tbl.error()));
        }
        owned = std::make_unique<Table>(std::move(input_tbl.value()));
    }
    const std::size_t grain = morsel_grain(exec, owned->rows());
    const auto expected_morsels = partitioned_morsel_count(*owned, grain);
    // Morselize only when the work would actually fan out. Splitting earns its
    // cost by having several workers share it; with fewer than two the split,
    // the per-morsel gather and the merge concat are all paid for parallelism
    // that was already ruled out, so the whole-table chunk below is the right
    // shape.
    //
    // No time claim is attached to this: measured on its own it moved nothing
    // (`morsels` 92 -> 0 on q12/q14, wall time unchanged), because by the time
    // this runs the input has already been materialized. The run's real cost at
    // a budget of one is that materialize, and it is declined at the
    // construction seam in `build_physical_map_step` instead. What this does
    // earn is an honest counter -- see below -- and not doing work whose only
    // consumer is a worker that will not exist.
    const std::size_t worker_count =
        is_worth_morselizing(exec, owned->rows(), owned->columns.size())
            ? morsel_worker_count(exec, expected_morsels)
            : 0;
    const bool morselize = worker_count >= 2;
    if (exec.parallel_stats != nullptr) {
        auto& stats = *exec.parallel_stats;
        (morselize ? stats.parallel_pipelines : stats.serial_pipelines)
            .fetch_add(1, std::memory_order_relaxed);
        if (morselize) {
            // Only count morsels that are actually formed. Reporting the
            // would-be split for a run that takes the whole-table chunk is what
            // made this cost invisible: `morsels=115` on a query with
            // `parallel=0` reads as work done, not as work paid for and thrown
            // away.
            stats.morsels.fetch_add(expected_morsels, std::memory_order_relaxed);
        }
    }

    if (worker_count >= 2) {
        const auto head = (operators.empty() || fused_probe.has_value())
                              ? std::nullopt
                              : range_filter_head(operators.front(), *owned);
        if (exec.parallel_stats != nullptr && head.has_value()) {
            exec.parallel_stats->range_heads.fetch_add(1, std::memory_order_relaxed);
        }

        // A range-native filter can skip the merger entirely by presizing its
        // output — see TwoPhaseFilterOperator. Anything above it must be
        // metadata-only: a row-touching operator would need the per-morsel
        // chunks the two-phase form does not produce, but Project and Rename
        // copy no rows and so are simply run once over the finished output.
        const auto tail = std::span{operators}.subspan(head.has_value() ? 1 : 0);
        if (head.has_value() && std::ranges::all_of(tail, [](const MapStep& step) {
                return is_metadata_only_node(step.node->kind());
            })) {
            auto layout = build_filter_output_layout(*owned, head->project);
            // A missing projected column is left to the ordered merger below,
            // which reports it through the normal evaluation path.
            if (layout.has_value() && filter_gather_is_thread_safe(*owned, layout->src_of_dst)) {
                if (exec.parallel_stats != nullptr) {
                    exec.parallel_stats->two_phase_filters.fetch_add(1, std::memory_order_relaxed);
                }
                // The tail is metadata-only by the check above, so no step in
                // it carries a fused partner; its nodes are the whole story.
                std::vector<const ir::Node*> tail_nodes;
                tail_nodes.reserve(tail.size());
                for (const MapStep& step : tail) {
                    tail_nodes.push_back(step.node);
                }
                return std::make_unique<TwoPhaseFilterOperator>(
                    std::move(owned), *head->predicate, head->project != nullptr,
                    std::move(tail_nodes), scalars, std::move(layout.value()), grain,
                    expected_morsels, worker_count, process_worker_pool());
            }
        }

        std::vector<MorselWorkerChain> workers;
        workers.reserve(worker_count);
        for (std::size_t i = 0; i < worker_count; ++i) {
            auto worker =
                build_morsel_worker_chain(operators, *owned, scalars, externs, exec,
                                          fused_probe.has_value() ? &*fused_probe : nullptr);
            if (!worker.has_value()) {
                return std::unexpected(std::move(worker.error()));
            }
            workers.push_back(std::move(worker.value()));
        }
        return std::make_unique<MorselPipelineOperator>(std::move(owned), std::move(workers), grain,
                                                        expected_morsels, process_worker_pool());
    }

    if (!morselize) {
        // Too little work to be worth splitting: run the chain over one
        // whole-table chunk. This is the plain serial path — same map
        // operators, same `preserve_empty_morsels = false`, one chunk in and
        // one chunk out — so it costs exactly what not forming a pipeline costs.
        // Morselizing here instead would add a per-morsel gather and a merge
        // concat to buy parallelism that was already judged not worth having.
        OperatorPtr serial = make_table_source(std::move(*owned));
        if (fused_probe.has_value()) {
            // Not worth morselizing, but the probe was already taken from the
            // join and there is nothing to give it back to: run it here, over
            // the whole probe side, with the maps above it as before.
            serial = fused_probe->attach_move(std::move(serial));
        }
        for (const MapStep& op_node : operators) {
            auto next = build_row_local_map_operator(op_node, std::move(serial), scalars, externs,
                                                     exec, false);
            if (!next.has_value()) {
                return std::unexpected("morsel pipeline: " + next.error());
            }
            serial = std::move(next.value());
        }
        return serial;
    }

    OperatorPtr chain = std::make_unique<PartitionedTableSource>(*owned, grain);
    for (const MapStep& op_node : operators) {
        auto next =
            build_row_local_map_operator(op_node, std::move(chain), scalars, externs, exec, true);
        if (!next.has_value()) {
            // The plan's step vocabulary only admits row-local map kinds.
            return std::unexpected("morsel pipeline: " + next.error());
        }
        chain = std::move(next.value());
    }

    chain = std::make_unique<SerialMorselOrderValidator>(std::move(chain), expected_morsels, grain);
    return std::make_unique<OwningMorselPipelineOperator>(std::move(owned), std::move(chain));
}

/// Streams a deferred lazy scan one source unit at a time instead of decoding
/// the whole source and handing it over as a single chunk.
///
/// This is Phase 1 of `plans/pipelined-execution-plan.md`. The decode it
/// performs is the same decode `materialize_deferred_scan` performs, with the
/// same pushdowns — projection, static conjuncts, the dynamic key membership
/// filter, and both fused scans, all restricted to the unit rather than
/// declined (see `LazyTable::project_where_unit`). What changes is only that
/// the rows arrive in pieces, which is the precondition for anything above the
/// scan ever running concurrently with it.
///
/// The plan is fixed once, at construction, for the reason `DeferredScanPlan`
/// documents: re-reading the shared filter slot per unit could apply to unit 3
/// a bound that units 0-2 never saw.
///
/// **Phase 2 (concurrent units).** Units are decoded a WINDOW at a time on
/// worker threads rather than one after another, and the window after the one
/// being served is already decoding. Phase 1 measured why: decoding units
/// serially cut total work (pool work on q01 fell 234ms -> 125ms) but raised
/// 8-core wall, because the pool saw one short burst per unit with a serial
/// phase between and occupancy fell to 0.14. Nothing was too small to
/// parallelize — there was just never more than one unit's worth of work
/// available at a time.
///
/// Decoding a unit on a worker is safe, and specifically so:
///
///   * `LazyTable::acquire_reader` hands each concurrent acquisition its OWN
///     reader product, under a mutex, and a product owns all the mutable
///     decoder state. That is exactly what the reader pool was built for.
///   * `project_where_unit` never WRITES `cache_` — a unit holds a fragment of
///     a column, so it must not — and concurrent reads of it are fine. This is
///     load-bearing now, not just a correctness nicety: routing any part of the
///     unit path back through `project()`, which does cache, would turn this
///     into a data race.
///   * Every inner parallel path (`parallel_readers`, `for_row_ranges`,
///     `filter_selection`) checks `on_worker_pool_thread()` and runs serial
///     inside a task, so the outer window is the only level of parallelism and
///     nothing submits from a worker into a saturated pool.
///
/// Ordering is preserved exactly: workers claim units from a shared cursor and
/// write only their own slot, and chunks are served in unit order with
/// `sequence` / `row_offset` assigned on the calling thread. The categorical
/// remap also stays on the calling thread, in unit order, because it folds each
/// chunk into a dictionary shared with every earlier one.
class DeferredScanSourceOperator final : public Operator {
   public:
    DeferredScanSourceOperator(const DeferredScan& scan, std::vector<SourceUnit> units,
                               const ExecutionContext& exec)
        : scan_(&scan),
          plan_(plan_deferred_scan(scan)),
          units_(std::move(units)),
          exec_(&exec),
          window_(unit_window(exec)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        while (true) {
            if (served_ < ready_.size()) {
                auto& slot = ready_[served_++];
                if (!slot.has_value()) {
                    return std::unexpected(std::move(slot.error()));
                }
                auto chunk = emit(std::move(*slot));
                if (!chunk.has_value()) {
                    continue;  // the unit's rows were all filtered out
                }
                return chunk;
            }
            // Out of decoded units: put the next window in flight and wait for
            // it.
            //
            // Dispatching the window AFTER this one here too, so the consumer's
            // work would overlap the next decode, is a MEASURED DEAD END. It
            // doubles how much of the source is decoded at once — peak RSS on a
            // 25-row-group scan went 161MB -> 244MB — and returns nothing,
            // because the consumer is a blocking operator that eats chunks
            // faster than they decode, so there is no consumer work to overlap
            // with. Real overlap needs a pipeline that keeps running while the
            // scan decodes, which is the rest of Phase 2, not a deeper queue.
            //
            // `inflight_pending_`, not `batch_`: a one-unit window decodes on
            // this thread and never submits, so a `batch_`-based test reports
            // "nothing in flight" and drops a window that has already been
            // decoded. That silently lost every unit after the first whenever
            // the query was not parallel, and every trailing single-unit window
            // when it was.
            if (!inflight_pending_) {
                if (dispatched_ >= units_.size()) {
                    // A stream must still carry its schema when every unit was
                    // empty after scan pushdown.  Without this carrier,
                    // MaterializeOperator sees end-of-stream as its first
                    // result and returns a column-less Table, unlike the
                    // equivalent eager scan (and unlike a filter above an
                    // ordinary table source).  Keep the first empty unit out
                    // of the normal pipeline -- several streaming operators
                    // intentionally do not consume empty chunks -- and emit it
                    // only when it is the sole result.
                    if (empty_schema_carrier_.has_value()) {
                        auto carrier = std::move(*empty_schema_carrier_);
                        empty_schema_carrier_.reset();
                        return std::optional<Chunk>{emit_schema_carrier(std::move(carrier))};
                    }
                    return std::optional<Chunk>{};
                }
                dispatch();
            }
            harvest();
        }
    }

   private:
    /// How many units to decode at once. One means serial, which is what a
    /// non-parallel query, a single-thread budget, and a call already running
    /// inside a pool task all get — the last because submitting from a worker
    /// deadlocks against a saturated pool.
    static auto unit_window(const ExecutionContext& exec) -> std::size_t {
        if (!exec.can_fan_out() || on_worker_pool_thread()) {
            return 1;
        }
        auto& pool = process_worker_pool();
        const std::size_t budget = exec.compute_budget();
        return std::max<std::size_t>(1, std::min(budget, pool.size()));
    }

    /// Put the next window of units in flight. Returns without blocking:
    /// `WorkerPool::submit` is asynchronous, which is what makes the overlap
    /// possible — and which is also why everything the body touches is a
    /// member rather than a local.
    void dispatch() {
        window_begin_ = dispatched_;
        window_count_ = std::min(window_, units_.size() - dispatched_);
        dispatched_ += window_count_;
        inflight_.clear();
        inflight_.resize(window_count_);
        inflight_pending_ = true;
        if (window_count_ == 1) {
            // Nothing to overlap and no reason to pay for a pool round trip.
            inflight_[0] = decode_unit(0);
            return;
        }
        cursor_.store(0, std::memory_order_relaxed);
        batch_ = process_worker_pool().submit(window_count_, [this](std::size_t /*worker*/) {
            for (std::size_t i = cursor_.fetch_add(1, std::memory_order_relaxed); i < window_count_;
                 i = cursor_.fetch_add(1, std::memory_order_relaxed)) {
                inflight_[i] = decode_unit(i);
            }
        });
    }

    /// One unit's decode, as run by a worker. Never throws out of the body: a
    /// `Batch` rethrows the first escaped exception at `wait()`, which would
    /// lose the other units' errors and unwind through the pool.
    auto decode_unit(std::size_t slot) -> std::expected<Table, std::string> {
        try {
            auto table =
                materialize_deferred_scan_unit(*scan_, plan_, units_[window_begin_ + slot], *exec_);
            if (table.has_value()) {
                normalize_time_index(*table);
            }
            return table;
        } catch (const std::exception& e) {
            return std::unexpected(std::string("streamed scan: ") + e.what());
        }
    }

    void harvest() {
        if (batch_.has_value()) {
            batch_->wait();
            batch_.reset();
        }
        ready_ = std::move(inflight_);
        inflight_.clear();
        inflight_pending_ = false;
        served_ = 0;
    }

    /// Turn a decoded unit into the chunk to hand upward, or nullopt when it
    /// carries no rows.
    ///
    /// A unit whose every row the scan's predicates rejected carries nothing.
    /// Skipping it is not just an optimization: an empty chunk with columns is
    /// a shape some operators would rather not meet, and dropping it changes no
    /// result. A column-less chunk is a different thing — it carries a row
    /// count for `count()` — and is kept.
    auto emit(Table table) -> std::optional<Chunk> {
        if (!table.columns.empty() && table.rows() == 0) {
            if (!empty_schema_carrier_.has_value()) {
                empty_schema_carrier_ = std::move(table);
            }
            return std::nullopt;
        }
        // A non-empty result makes a deferred carrier unnecessary.
        empty_schema_carrier_.reset();
        unify_categorical_dictionaries(table);
        Chunk chunk;
        const std::size_t rows = table.rows();
        chunk.set_properties(table.properties());
        chunk.columns = std::move(table.columns);
        if (chunk.columns.empty()) {
            chunk.logical_rows = table.logical_rows;
        }
        chunk.sequence = sequence_++;
        chunk.row_offset = emitted_rows_;
        emitted_rows_ += rows;
        return chunk;
    }

    /// Materialize the one schema carrier retained when scan pushdown rejected
    /// every row.  It follows the ordinary source identity convention: first
    /// chunk, at row zero.  Dictionary unification remains necessary because a
    /// zero-row categorical still carries dictionary identity as part of its
    /// schema.
    auto emit_schema_carrier(Table table) -> Chunk {
        unify_categorical_dictionaries(table);
        Chunk chunk;
        chunk.set_properties(table.properties());
        chunk.columns = std::move(table.columns);
        if (chunk.columns.empty()) {
            chunk.logical_rows = table.logical_rows;
        }
        chunk.sequence = sequence_++;
        chunk.row_offset = emitted_rows_;
        return chunk;
    }

    /// Remap every Categorical column onto a dictionary shared by all this
    /// source's chunks.
    ///
    /// Parquet writes one dictionary PER ROW GROUP, and a unit is one row
    /// group, so without this each chunk's codes would mean something different
    /// from the last one's — and the operators that compare dictionary identity
    /// to take a fast path (grouping, joins, the packed key encoder) would be
    /// comparing codes across dictionaries that disagree. The whole-file decode
    /// never had this problem because it merged the groups' dictionaries
    /// itself. `ChunkedParquetSourceOperator` solves it the same way.
    ///
    /// **One lookup per dictionary ENTRY, never per row.** Interning row by row
    /// is a string hash per row, and it does not announce itself: TPC-H's
    /// `l_returnflag` and `l_linestatus` are plain `string` in the Arrow schema
    /// and only become Categorical because the writer dictionary-encoded them,
    /// so a query that never mentions a categorical type still pays. Measured
    /// on q01, per-row interning cost 114ms of the scan's 160ms — the entire
    /// regression against the materialized path, on the calling thread where
    /// nothing could overlap it. A dictionary has a handful of entries and a
    /// unit has a million rows; the difference is the whole cost.
    void unify_categorical_dictionaries(Table& table) {
        using code_type = Column<Categorical>::code_type;
        for (std::size_t i = 0; i < table.columns.size(); ++i) {
            auto* local = std::get_if<Column<Categorical>>(table.columns[i].column.get());
            if (local == nullptr) {
                continue;
            }
            if (cat_states_.size() <= i) {
                cat_states_.resize(table.columns.size());
            }
            auto& state = cat_states_[i];
            if (!state.has_value()) {
                state.emplace();
            }
            if (state->dictionary_ptr() == local->dictionary_ptr()) {
                continue;  // already speaks the shared dictionary
            }
            // Intern this chunk's dictionary into the shared one, reading back
            // the code each entry landed on. `clear()` drops the codes and
            // keeps the dictionary, which is exactly what an accumulator wants.
            const auto& dictionary = local->dictionary();
            state->clear();
            for (const auto& value : dictionary) {
                state->push_back(value);
            }
            std::vector<code_type> remap(dictionary.size());
            for (std::size_t entry = 0; entry < dictionary.size(); ++entry) {
                remap[entry] = state->code_at(entry);
            }
            state->clear();

            const auto& local_codes = local->codes();
            std::vector<code_type> codes(local_codes.size());
            for (std::size_t row = 0; row < local_codes.size(); ++row) {
                codes[row] = remap[static_cast<std::size_t>(local_codes[row])];
            }
            table.columns[i].column = std::make_shared<ColumnValue>(
                Column<Categorical>{state->dictionary_ptr(), state->index_ptr(), std::move(codes)});
        }
    }

    const DeferredScan* scan_;
    DeferredScanPlan plan_;
    std::vector<SourceUnit> units_;
    const ExecutionContext* exec_;
    std::vector<std::optional<Column<Categorical>>> cat_states_;

    /// Units decoded and waiting to be served, in unit order, and how many of
    /// them have been.
    std::vector<std::expected<Table, std::string>> ready_;
    std::size_t served_ = 0;
    /// First empty, column-bearing unit.  It becomes a schema carrier only if
    /// every unit was empty; otherwise empty units stay invisible to the
    /// streaming operators above this source.
    std::optional<Table> empty_schema_carrier_;
    /// The window currently being decoded. Workers write disjoint slots of
    /// this, so it must not be resized while `batch_` is live.
    std::vector<std::expected<Table, std::string>> inflight_;
    std::optional<WorkerPool::Batch> batch_;
    /// Whether `inflight_` holds a dispatched window awaiting harvest. Not
    /// derivable from `batch_`: a one-unit window is decoded inline.
    bool inflight_pending_ = false;
    std::atomic<std::size_t> cursor_{0};
    std::size_t window_begin_ = 0;
    std::size_t window_count_ = 0;

    std::size_t dispatched_ = 0;
    std::size_t emitted_rows_ = 0;
    std::uint64_t sequence_ = 0;
    std::size_t window_ = 1;
};

// A one-chunk source owned by one scan-pipeline worker. The worker replaces
// the pending chunk for every source unit it claims, then pulls the private
// row-local chain exactly once. Keeping the chain private is what makes its
// mutable per-operator state safe without locks.
class ScanPipelineSource final : public Operator {
   public:
    void set(Chunk chunk) { pending_ = std::move(chunk); }

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (!pending_.has_value()) {
            return std::optional<Chunk>{};
        }
        auto chunk = std::move(*pending_);
        pending_.reset();
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    std::optional<Chunk> pending_;
};

struct ScanPipelineWorker {
    ScanPipelineSource* source = nullptr;
    OperatorPtr chain;
};

[[nodiscard]] auto build_scan_pipeline_worker(const std::vector<MapStep>& operators,
                                              const ScalarRegistry* scalars,
                                              const ExternRegistry* externs,
                                              const ExecutionContext& exec)
    -> std::expected<ScanPipelineWorker, std::string> {
    auto source = std::make_unique<ScanPipelineSource>();
    ScanPipelineWorker worker{.source = source.get(), .chain = std::move(source)};
    for (const MapStep& op_node : operators) {
        auto next = build_row_local_map_operator(op_node, std::move(worker.chain), scalars, externs,
                                                 exec, true);
        if (!next.has_value()) {
            return std::unexpected("scan pipeline: " + next.error());
        }
        worker.chain = std::move(next.value());
    }
    return worker;
}

/// A bounded source-to-map pipeline.
///
/// Each worker claims one source unit, decodes it with all globally planned
/// pushdowns intact, immediately runs the row-local operator chain, and
/// publishes the result into a bounded ordered ring. The caller drains that
/// ring into the next blocking operator. Thus decode of unit N+1, row-local
/// work on unit N, and consumption of an earlier unit can all be live at once;
/// there is no materialized table or whole-window wait between those stages.
class PipelinedScanOperator final : public Operator {
   public:
    PipelinedScanOperator(const DeferredScan& scan, std::vector<SourceUnit> units,
                          std::vector<ScanPipelineWorker> workers, const ExecutionContext& exec,
                          WorkerPool& pool)
        : scan_(&scan),
          plan_(plan_deferred_scan(scan)),
          units_(std::move(units)),
          workers_(std::move(workers)),
          exec_(&exec),
          pool_(&pool),
          window_(std::max<std::size_t>(workers_.size() * 2, 2)),
          ring_(window_, workers_.size()) {}

    ~PipelinedScanOperator() override { cancel_and_join(); }

    PipelinedScanOperator(const PipelinedScanOperator&) = delete;
    auto operator=(const PipelinedScanOperator&) -> PipelinedScanOperator& = delete;
    PipelinedScanOperator(PipelinedScanOperator&&) = delete;
    auto operator=(PipelinedScanOperator&&) -> PipelinedScanOperator& = delete;

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (finished_) {
            return std::optional<Chunk>{};
        }
        start();

        while (next_sequence_ < units_.size()) {
            std::optional<Chunk> produced = ring_.take(next_sequence_);
            if (!produced.has_value()) {
                // Stopped before this unit: cancelled, failed, or out of
                // workers. The ring reports the lowest-sequence failure, so the
                // message does not depend on which worker lost a race.
                if (auto failure = ring_.failure(); failure.has_value()) {
                    return fail(std::move(*failure));
                }
                return fail(interrupt_requested() ? interrupt_message()
                                                  : "scan pipeline: missing output unit");
            }
            ++next_sequence_;
            Chunk chunk = std::move(*produced);
            if (!chunk.columns.empty() && chunk.rows() == 0) {
                if (!empty_schema_carrier_.has_value()) {
                    empty_schema_carrier_ = std::move(chunk);
                }
                continue;
            }

            empty_schema_carrier_.reset();
            normalize_categorical_dictionaries(chunk);
            restamp(chunk);
            return std::optional<Chunk>{std::move(chunk)};
        }

        finish_workers();
        finished_ = true;
        if (empty_schema_carrier_.has_value()) {
            auto carrier = std::move(*empty_schema_carrier_);
            empty_schema_carrier_.reset();
            normalize_categorical_dictionaries(carrier);
            restamp(carrier);
            return std::optional<Chunk>{std::move(carrier)};
        }
        return std::optional<Chunk>{};
    }

   private:
    void start() {
        if (started_) {
            return;
        }
        started_ = true;
        batch_ = pool_->submit(workers_.size(), [this](std::size_t id) { run_worker(id); });
    }

    void run_worker(std::size_t worker_id) noexcept {
        // However this worker leaves, it must stop counting as a producer, or
        // the consumer waits for a unit that is never coming.
        // One-shot scope guard: aggregate-initialised, never copied or moved.
        // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
        struct ExitGuard {
            OrderedChunkRing* ring;
            ~ExitGuard() { ring->producer_exited(); }
        } const guard{&ring_};

        std::size_t claimed = 0;
        try {
            auto& worker = workers_[worker_id];
            while (true) {
                const std::size_t sequence = cursor_.fetch_add(1, std::memory_order_relaxed);
                if (sequence >= units_.size()) {
                    return;
                }
                claimed = sequence;
                if (ring_.acquire(sequence) == OrderedChunkRing::Acquire::Abandon) {
                    return;
                }
                if (interrupt_requested()) {
                    ring_.cancel();
                    return;
                }

                auto result = run_unit(worker, sequence);
                if (!result.has_value()) {
                    ring_.record_error(sequence, std::move(result.error()));
                    return;
                }
                ring_.publish(sequence, std::move(*result));
            }
        } catch (const std::exception& error) {
            // Sequence-tagged like any other failure, so the reported message
            // obeys the same lowest-sequence rule. Composing it allocates and
            // the likeliest exception here is bad_alloc, hence the
            // allocation-free fallback underneath.
            try {
                ring_.record_error(claimed,
                                   "scan pipeline: worker exception: " + std::string(error.what()));
            } catch (...) {
                ring_.record_fault(claimed,
                                   "scan pipeline: worker exception (no memory to report it)");
            }
        } catch (...) {
            ring_.record_fault(claimed, "scan pipeline: worker threw a non-standard exception");
        }
    }

    [[nodiscard]] auto run_unit(ScanPipelineWorker& worker, std::size_t sequence)
        -> std::expected<Chunk, std::string> {
        auto decoded = materialize_deferred_scan_unit(*scan_, plan_, units_[sequence], *exec_);
        if (!decoded.has_value()) {
            return std::unexpected(std::move(decoded.error()));
        }
        normalize_time_index(*decoded);
        worker.source->set(table_to_chunk(
            std::move(*decoded),
            ChunkIdentity{.sequence = sequence, .row_offset = units_[sequence].start}));
        auto produced = worker.chain->next();
        if (!produced.has_value()) {
            return std::unexpected(std::move(produced.error()));
        }
        if (!produced->has_value()) {
            return std::unexpected("scan pipeline: row-local chain dropped a source unit");
        }
        Chunk chunk = std::move(**produced);
        if (chunk.sequence != sequence) {
            return std::unexpected("scan pipeline: row-local chain reordered a source unit");
        }
        return chunk;
    }

    void restamp(Chunk& chunk) noexcept {
        chunk.sequence = emitted_sequence_++;
        chunk.row_offset = emitted_rows_;
        emitted_rows_ += chunk.rows();
    }

    // Parquet dictionaries are local to row groups. Ordered publication is the
    // one serial point where chunks are remapped onto one shared dictionary,
    // preserving the existing streamed-source contract for downstream keys.
    void normalize_categorical_dictionaries(Chunk& chunk) {
        using code_type = Column<Categorical>::code_type;
        if (cat_states_.size() < chunk.columns.size()) {
            cat_states_.resize(chunk.columns.size());
        }
        for (std::size_t i = 0; i < chunk.columns.size(); ++i) {
            auto* local = std::get_if<Column<Categorical>>(chunk.columns[i].column.get());
            if (local == nullptr) {
                continue;
            }
            auto& state = cat_states_[i];
            if (!state.has_value()) {
                state.emplace();
            }
            if (state->dictionary_ptr() == local->dictionary_ptr()) {
                continue;
            }
            const auto& dictionary = local->dictionary();
            state->clear();
            for (const auto& value : dictionary) {
                state->push_back(value);
            }
            std::vector<code_type> remap(dictionary.size());
            for (std::size_t entry = 0; entry < dictionary.size(); ++entry) {
                remap[entry] = state->code_at(entry);
            }
            state->clear();
            const auto& local_codes = local->codes();
            std::vector<code_type> codes(local_codes.size());
            for (std::size_t row = 0; row < local_codes.size(); ++row) {
                codes[row] = remap[static_cast<std::size_t>(local_codes[row])];
            }
            chunk.columns[i].column = std::make_shared<ColumnValue>(
                Column<Categorical>{state->dictionary_ptr(), state->index_ptr(), std::move(codes)});
        }
    }

    void finish_workers() {
        if (batch_.has_value()) {
            batch_->wait();
            batch_.reset();
        }
        if (validated_) {
            return;
        }
        validated_ = true;
        for (auto& worker : workers_) {
            auto trailing = worker.chain->next();
            if (!trailing.has_value()) {
                throw std::runtime_error(trailing.error());
            }
            if (trailing->has_value()) {
                throw std::runtime_error("scan pipeline: unexpected trailing output");
            }
        }
    }

    void cancel_and_join() noexcept {
        ring_.cancel();
        try {
            finish_workers();
        } catch (...) {  // NOLINT(bugprone-empty-catch)
        }
    }

    [[nodiscard]] auto fail(std::string message)
        -> std::expected<std::optional<Chunk>, std::string> {
        finished_ = true;
        cancel_and_join();
        return std::unexpected(std::move(message));
    }

    const DeferredScan* scan_;
    DeferredScanPlan plan_;
    std::vector<SourceUnit> units_;
    std::vector<ScanPipelineWorker> workers_;
    const ExecutionContext* exec_;
    WorkerPool* pool_;
    std::size_t window_ = 2;
    // The same ordered handoff the morsel executor uses: one implementation of
    // the bounded, sequence-ordered producer/consumer shape.
    OrderedChunkRing ring_;
    std::vector<std::optional<Column<Categorical>>> cat_states_;
    std::optional<Chunk> empty_schema_carrier_;
    std::optional<WorkerPool::Batch> batch_;
    std::atomic<std::size_t> cursor_{0};
    std::size_t next_sequence_ = 0;
    std::size_t emitted_rows_ = 0;
    std::uint64_t emitted_sequence_ = 0;
    bool started_ = false;
    bool finished_ = false;
    bool validated_ = false;
};

/// A bounded asynchronous boundary between two pipeline segments.
///
/// The existing executor is pull-based, which is ideal for operator-local
/// state but normally means a parent cannot start its work until its child has
/// returned from `next()`. This stage retains that contract at both ends while
/// driving its child on a dedicated scheduler thread and holding at most two
/// ordered chunks between them. A breaker below the stage may therefore build
/// or probe the next chunk while a row-local parent (or the next breaker) is
/// working on the preceding one.
///
/// This deliberately does not borrow a WorkerPool thread. A streamed scan
/// already owns pool tasks, and putting the stage on that same fixed pool
/// reintroduces the saturated-pool deadlock that the scan producer's worker
/// reservation avoids. The query lease limits this to one query, while the
/// builder only inserts stages at breaker boundaries, so this is bounded by
/// plan depth rather than morsel count.
class PipelinedStageOperator final : public Operator {
   public:
    PipelinedStageOperator(OperatorPtr child, ExecutionProfileEntry* entry)
        : child_(std::move(child)), entry_(entry) {}

    ~PipelinedStageOperator() override { cancel_and_join(); }

    PipelinedStageOperator(const PipelinedStageOperator&) = delete;
    auto operator=(const PipelinedStageOperator&) -> PipelinedStageOperator& = delete;
    PipelinedStageOperator(PipelinedStageOperator&&) = delete;
    auto operator=(PipelinedStageOperator&&) -> PipelinedStageOperator& = delete;

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        start();
        std::expected<std::optional<Chunk>, std::string> result = std::optional<Chunk>{};
        {
            std::unique_lock lock(mutex_);
            // Idle, not serial work: waiting on the stage's producer thread. When
            // this stage is nested under another pipeline, `next()` runs on a
            // pool worker, so the wait must keep the pool moving.
            cooperative_ring_wait(ready_, lock, [this] {
                return !ready_chunks_.empty() || producer_done_ || failure_.has_value();
            });
            if (failure_.has_value()) {
                result = std::unexpected(std::move(*failure_));
                failure_.reset();
            } else if (!ready_chunks_.empty()) {
                result = std::optional<Chunk>{std::move(ready_chunks_.front())};
                ready_chunks_.pop_front();
            } else {
                done_ = true;
            }
        }
        space_.notify_one();
        if (!result.has_value()) {
            cancel_and_join();
            return result;
        }
        if (done_) {
            join();
        }
        return result;
    }

   private:
    static constexpr std::size_t kCapacity = 2;

    void start() {
        if (started_) {
            return;
        }
        started_ = true;
        producer_ = std::thread([this] {
            // Declare what kind of thread this is. It is deliberately NOT a pool
            // worker — it is long-lived and parks on the consumer's ring
            // backpressure, which a fixed-size pool cannot host — but it must
            // still be countable, and the profiler must not charge its work to
            // the calling thread's self time.
            const StageThreadScope stage_thread;
            produce();
        });
    }

    void produce() noexcept {
        try {
            while (true) {
                {
                    std::unique_lock lock(mutex_);
                    // Backpressure: the producer has filled the ring and the
                    // consumer has not drained it. Idle time on a runtime-owned
                    // thread, and the mirror image of the consumer's park at the
                    // other end of the same ring — a large value here means the
                    // CONSUMER is the bottleneck. `RingWaitScope` routes it to
                    // this thread's stage ledger.
                    const RingWaitScope ring_wait;
                    space_.wait(lock,
                                [this] { return cancelled_ || ready_chunks_.size() < kCapacity; });
                    if (cancelled_) {
                        return;
                    }
                }

                auto next = [&] {
                    // Scoped per pull, mirroring ProfiledOperator, so the
                    // backpressure wait above stays outside it.
                    const ExecutionProfileScope scope(entry_, ProfilePhase::Next);
                    return child_->next();
                }();
                if (!next.has_value()) {
                    fail(std::move(next.error()));
                    return;
                }
                if (!next->has_value()) {
                    {
                        const std::scoped_lock lock(mutex_);
                        producer_done_ = true;
                    }
                    ready_.notify_all();
                    return;
                }

                {
                    const std::scoped_lock lock(mutex_);
                    if (cancelled_) {
                        return;
                    }
                    // There is one producer. The capacity check immediately
                    // before `child_->next()` therefore reserves this slot:
                    // only the consumer can change the queue size meanwhile.
                    ready_chunks_.push_back(std::move(**next));
                }
                ready_.notify_one();
            }
        } catch (const std::exception& error) {
            fail("pipeline stage: producer exception: " + std::string(error.what()));
        } catch (...) {
            fail("pipeline stage: producer threw a non-standard exception");
        }
    }

    void fail(std::string message) noexcept {
        {
            const std::scoped_lock lock(mutex_);
            if (!failure_.has_value()) {
                failure_ = std::move(message);
            }
            producer_done_ = true;
        }
        ready_.notify_all();
    }

    void join() noexcept {
        if (producer_.joinable()) {
            producer_.join();
        }
    }

    void cancel_and_join() noexcept {
        {
            const std::scoped_lock lock(mutex_);
            cancelled_ = true;
        }
        ready_.notify_all();
        space_.notify_all();
        join();
    }

    OperatorPtr child_;
    // The operator this stage was built for. `profile_operator` wraps the
    // STAGE, not the child, so the producer thread runs unwrapped code and had
    // no profile frame at all: anything it submitted to the pool was attributed
    // to no operator, and a fully parallel scan read as zero pool work. The
    // producer pushes a scope for this entry so its work has an owner.
    ExecutionProfileEntry* entry_ = nullptr;
    std::thread producer_;
    std::deque<Chunk> ready_chunks_;
    std::mutex mutex_;
    std::condition_variable ready_;
    std::condition_variable space_;
    std::optional<std::string> failure_;
    bool started_ = false;
    bool producer_done_ = false;
    bool cancelled_ = false;
    bool done_ = false;
};

[[nodiscard]] auto make_pipelined_stage(OperatorPtr child, const ExecutionContext& exec,
                                        ExecutionProfileEntry* entry) -> OperatorPtr {
    if (!exec.can_fan_out() || on_worker_pool_thread() || process_worker_pool().size() < 2) {
        return child;
    }
    if (exec.parallel_stats != nullptr) {
        exec.parallel_stats->pipelined_stages.fetch_add(1, std::memory_order_relaxed);
    }
    return std::make_unique<PipelinedStageOperator>(std::move(child), entry);
}

[[nodiscard]] auto make_pipelined_stage_if(OperatorPtr child, bool eligible,
                                           const ExecutionContext& exec,
                                           ExecutionProfileEntry* entry) -> OperatorPtr {
    return eligible ? make_pipelined_stage(std::move(child), exec, entry) : std::move(child);
}

[[nodiscard]] auto scan_pipeline_worker_count(std::size_t unit_count) -> std::size_t {
    auto& pool = process_worker_pool();
    if (pool.size() < 2) {
        // With one pool thread there is no worker to reserve for a downstream
        // operator batch. Running the producer there can deadlock as soon as a
        // breaker submits work and waits, so keep the serial window source.
        return 0;
    }
    // The decode pipeline is the one consumer sized against the POOL rather
    // than the compute budget: it is what the extra threads were added for.
    // `ExecutionContext::parallel_threads` deliberately does not cap it;
    // configure_parallel_from_env uses that field for compute only.
    const std::size_t budget = pool.size();
    std::size_t workers = std::min({budget, pool.size(), unit_count});
    // A spare thread is only necessary when every pool thread could remain
    // parked behind ring backpressure. The ring holds 2W results and workers
    // have already claimed at most another W units, so a source of at most 3W
    // units necessarily lets one worker exit after the first chunk is released.
    // Smaller sources (the common Parquet shape) keep the full decode budget;
    // longer sources reserve one thread for downstream batches.
    if (workers == pool.size() && unit_count > workers * 3) {
        --workers;
    }
    return workers;
}

auto make_deferred_scan_source(const DeferredScan& scan, std::vector<SourceUnit> units,
                               const ExecutionContext& exec) -> OperatorPtr {
    return std::make_unique<DeferredScanSourceOperator>(scan, std::move(units), exec);
}

[[nodiscard]] auto build_pipelined_scan(const std::vector<MapStep>& operators,
                                        bool count_as_pipeline, const DeferredScan& scan,
                                        std::vector<SourceUnit> units,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec)
    -> std::expected<OperatorPtr, std::string> {
    const std::size_t worker_count = scan_pipeline_worker_count(units.size());
    if (worker_count == 0) {
        return std::unexpected("scan pipeline requires a worker");
    }
    std::vector<ScanPipelineWorker> workers;
    workers.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        auto worker = build_scan_pipeline_worker(operators, scalars, externs, exec);
        if (!worker.has_value()) {
            return std::unexpected(std::move(worker.error()));
        }
        workers.push_back(std::move(*worker));
    }
    if (exec.parallel_stats != nullptr) {
        if (count_as_pipeline) {
            exec.parallel_stats->parallel_pipelines.fetch_add(1, std::memory_order_relaxed);
        }
        exec.parallel_stats->morsels.fetch_add(units.size(), std::memory_order_relaxed);
        exec.parallel_stats->pipelined_scans.fetch_add(1, std::memory_order_relaxed);
    }
    return std::make_unique<PipelinedScanOperator>(scan, std::move(units), std::move(workers), exec,
                                                   process_worker_pool());
}

/// A breaker only earns a scheduler thread when its probe input can actually
/// publish more than one source unit. Registered tables and one-unit readers
/// return a single chunk, so staging them merely moves the same serial call to
/// another thread. Keep this structural test at build time: it avoids putting
/// a speculative thread on the hot path and makes the queue capacity an
/// overlap buffer rather than an accidental materialization boundary.
[[nodiscard]] auto has_multi_unit_deferred_scan(const ir::Node& node, const TableRegistry& registry,
                                                const ExecutionContext& exec) -> bool {
    if (node.kind() == ir::NodeKind::Scan) {
        const auto& scan = ir::node_cast<ir::ScanNode>(node);
        if (registry.contains(scan.source_name())) {
            return false;
        }
        const auto* deferred = exec.deferred_scan(scan.source_name());
        return deferred != nullptr && deferred->filter == nullptr &&
               deferred_scan_units(*deferred).size() > 1;
    }
    return std::ranges::any_of(node.children(), [&](const ir::NodePtr& child) {
        return has_multi_unit_deferred_scan(*child, registry, exec);
    });
}

}  // namespace pipeline_executor_detail

}  // namespace ibex::runtime
