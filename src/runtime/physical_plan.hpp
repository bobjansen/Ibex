// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/pipeline.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::runtime::physical {

/// Phase 1 of plans/kernel-pipeline-execution-plan.md: an explicit, data-only
/// physical plan computed *beside* `build_operator`, before any of its
/// per-kind branches run. The plan is the decision record — which pipelines
/// exist, and why anything else materialized — not a second executor. Phase 2
/// replaces per-step execution with kernels; until then a migrated pipeline
/// constructs exactly the operators the per-kind switch would have.
///
/// Scope (deliberately tiny, per the plan's "first vocabulary"):
/// a `MapPipeline` is a non-empty top-down chain of row-local map nodes
/// (Filter, Project, Rename, and row-local Update) over a source. A source is
/// a Scan, a chunked extern call, or the materialized output of a subtree that
/// keeps the existing executor — a pipeline breaker feeding this pipeline.
/// Everything else is a `MaterializedCall` placeholder naming the logical
/// subtree that keeps the existing executor.
enum class SourceKind : std::uint8_t {
    TableScan,     ///< Scan of a table in the registry
    LazyScan,      ///< Scan resolved lazily (deferred/reader-backed)
    ExternSource,  ///< chunked extern call (read_csv, read_parquet, ...)
    /// The materialized output of a pipeline breaker (a join, an aggregate, an
    /// order — whatever the walk bottomed out in). The subtree keeps the
    /// existing executor and this pipeline consumes what it produces, which is
    /// exactly the relationship a breaker has to the pipeline above it. This is
    /// what lets one plan describe every map chain, not only the chains that
    /// happen to sit directly on a scan.
    MaterializedInput,
};

enum class FallbackReason : std::uint8_t {
    /// The root is not a row-local map kind (join, aggregate, order, ...).
    NotMapChain,
    /// The root is a bare source; there is no map work to migrate.
    EmptyChain,
    /// A map node with no single child, or one whose proven capability has no
    /// factory. Structurally broken rather than unsupported: the existing
    /// executor owns the diagnostic.
    MalformedMapNode,
};

/// How a migrated pipeline may execute. This is the plan's answer to "may the
/// morsel executor run this", decided once during lowering from the same rules
/// that used to live in the pipeline analysis — a pipeline that carries its own
/// mode is what lets parallel execution become a mode of a pipeline rather than
/// a second executor with its own analysis.
enum class PipelineMode : std::uint8_t {
    /// Every step runs on the single build thread, in order.
    Serial,
    /// A leading run of `parallel_steps` steps may run over morsels. The rest
    /// of the chain, if any, stays serial above it.
    MorselParallel,
};

/// Why a migrated pipeline is serial-only. `None` when it is not.
enum class SerialOnlyReason : std::uint8_t {
    None,
    /// No leading step is a `ParallelMap` — a bare row-local `Update` is the
    /// case that matters. It is a map step, and deliberately not a parallel
    /// one: an update is 1:1 and splits its field computation inside the
    /// operator, so a morsel pipeline would buy two whole-table copies. See
    /// `execution_capability(const ir::Node&)`.
    NotParallelMap,
    /// A step reads rows a morsel does not contain (rolling, rank, generator,
    /// unknown call), so it cannot be evaluated over a subset.
    UnsupportedExpression,
    /// Every step is metadata-only (Project/Rename), which costs O(columns);
    /// morselizing it would gather and concatenate the whole table to
    /// parallelize a pointer assignment.
    NoRowWork,
};

/// One lowered pipeline. Immutable by convention: built by `plan_physical`,
/// consumed read-only. Step nodes are `const ir::Node*` rather than copied
/// payloads so there is exactly one definition of each step's semantics; the
/// plan records *shape*, the IR remains the program. Like
/// every analysis over the IR, the plan **borrows** the IR it was lowered
/// from and must not outlive it.
/// How a join executes. Phase 4 item 1's vocabulary: the plan says which side
/// builds the hash table and which side streams through it, so that decision
/// stops being a branch inside `build_operator` and becomes something a test
/// and `explain physical` can both read.
enum class JoinStrategy : std::uint8_t {
    /// One side is hashed into a table and the other streams through it --
    /// `HashBuild` and `HashProbe`. Which side is which is a run-time
    /// decision, not this strategy's claim; see `JoinPlan::left_input`. This
    /// is what `is_streamable_inner_join` and the semi/anti gate select.
    StreamingProbe,
    /// Both sides are materialized before the join runs. Every semantics the
    /// streaming operators do not implement lands here.
    MaterializeBoth,
};

/// Why a join materializes both sides instead of streaming. `None` when it
/// streams. Each value is one clause of the eligibility gates, named rather
/// than left as a conjunction nobody can attribute a decision to.
enum class JoinDeclineReason : std::uint8_t {
    None,
    /// Not one of the kinds a streaming operator implements.
    UnsupportedKind,
    /// A non-equi predicate: the streaming operators probe on keys only.
    HasPredicate,
    /// More keys than any streaming path takes (the operators handle one, or a
    /// pair of Int64s).
    MultipleKeys,
    /// Exactly two keys, but not the all-Int64 shape the pair path requires --
    /// or a schema too unknown to prove they are. Schema-dependent, so this is
    /// the one decline that is not a property of the node alone.
    KeyTypesUnsupported,
    /// `nulls equal`: the materialized join is the one implementation that
    /// tags nulls, and keeping it single is deliberate.
    NullsEqual,
    /// `expect` asserts a cardinality the streaming path does not check.
    AssertsCardinality,
    /// `take first/last/any` rather than all matches.
    TakeSelection,
};

/// Which streaming operator runs a join, when one does.
///
/// Named rather than inferred. The seam used to pick between the two inner
/// branches by testing `key_count == 1` vs `== 2`, which is only sound while
/// `StreamingProbe` implies a full gate passed -- an invisible coupling, and a
/// gate that later admitted a one-key shape the single-key operator cannot run
/// would have routed wrong in silence. The planner knows which predicate said
/// yes; saying so costs nothing.
enum class JoinBranch : std::uint8_t {
    /// Not streaming: both sides materialize.
    None,
    /// `is_streamable_semi_anti_join`.
    SemiAnti,
    /// `is_streamable_inner_join` -- one key.
    SingleKeyInner,
    /// `is_streamable_pair_int_join` -- two provably-Int64 keys.
    PairIntInner,
};

/// What the plan knows about a `Join` node.
///
/// Describes only. Execution still goes through `build_operator`'s per-kind
/// switch, exactly as before -- this is the same order the island removal took
/// (describe, prove the description equals what the builder does, then move
/// execution), because a description that is wrong is much cheaper to find than
/// an executor that is wrong.
struct JoinPlan {
    /// False unless the planned node is a `Join`.
    bool describes = false;
    ir::JoinKind kind{};
    JoinStrategy strategy = JoinStrategy::MaterializeBoth;
    /// Which operator runs it. `None` exactly when `strategy` is
    /// `MaterializeBoth`.
    JoinBranch branch = JoinBranch::None;
    JoinDeclineReason decline = JoinDeclineReason::None;
    /// The join's two inputs, in textual order. Both null unless `strategy` is
    /// `StreamingProbe`.
    ///
    /// Deliberately NOT named build and probe. These fields used to be, and
    /// the claim was false whenever the operator swapped: which side is hashed
    /// is resolved when the build phase runs, from measured row counts
    /// (`JoinOrientation`, `choose_and_build_single_key` in chunked.cpp). A
    /// plan that names a build side is a plan a scheduler would believe.
    /// Naming the inputs and leaving the orientation to the build is what
    /// lets both children be scheduled as pipelines without the plan
    /// pretending to a decision it cannot make -- see
    /// plans/kernel-pipeline-execution-plan.md, "The build-side choice does
    /// not block the split".
    const ir::Node* left_input = nullptr;
    const ir::Node* right_input = nullptr;
    std::size_t key_count = 0;
};

/// Classify a join. Pure: it reads the node and nothing else.
[[nodiscard]] auto plan_join(const ir::JoinNode& join) -> JoinPlan;

/// `strategy=... decline=... keys=N build=<kind> probe=<kind>` for tests and
/// `explain physical`.
[[nodiscard]] auto explain_join(const JoinPlan& plan) -> std::string;

/// How an aggregate executes.
enum class AggregateStrategy : std::uint8_t {
    /// Fused with the join beneath it: two logical nodes, one physical step.
    /// Checked first, because it consumes the join rather than reading its
    /// output.
    FusedLeftJoinCount,
    /// Adaptive streamable aggregate: group-at-a-time when the child's chunks
    /// arrive sorted on the group keys, hash aggregation otherwise. The enum
    /// name predates the hash fallback becoming the common generated-data path.
    StreamingSorted,
    /// Median and Quantile need every value at once, Ewma is row-order
    /// coupled: the whole input is materialized.
    MaterializeAll,
};

/// One aggregate input resolved against the ordered physical columns of its
/// child. Logical names stop at this boundary; execution phases share these
/// positions.
struct AggregateColumnMapping {
    std::vector<std::size_t> group_by;
    /// Count consumes rows rather than an input column, so its entry is
    /// nullopt. Every other aggregate carries its input column position.
    std::vector<std::optional<std::size_t>> aggregate_inputs;

    auto operator==(const AggregateColumnMapping&) const -> bool = default;
};

/// Values crossing the structural phases of the hash-aggregate fallback.
/// These are ownership types, not table schemas: each value is internal state
/// that only the following phase may consume.
enum class AggregateDataKind : std::uint8_t {
    None,
    InputChunks,
    DiscoveredGroups,
    AccumulatedGroups,
    OrderedGroups,
    OutputChunks,
};

struct AggregateDiscoveryNode {
    const ir::Node* source = nullptr;
    AggregateDataKind input = AggregateDataKind::None;
    AggregateDataKind output = AggregateDataKind::None;
};

struct AggregateAccumulationNode {
    AggregateDataKind input = AggregateDataKind::None;
    AggregateDataKind output = AggregateDataKind::None;
};

struct AggregateFinalOrderingNode {
    AggregateDataKind input = AggregateDataKind::None;
    AggregateDataKind output = AggregateDataKind::None;
};

struct AggregateEmissionNode {
    AggregateDataKind input = AggregateDataKind::None;
    AggregateDataKind output = AggregateDataKind::None;
};

/// The hash fallback's structural chain. The current optimized kernels may
/// fuse work within an edge, but the plan and executor agree on these four
/// ownership boundaries. Discovery hands Accumulation a bounded per-chunk
/// transfer (or an explicit fused marker); final ordering and emission have
/// separate executor entries. The serial coordinator invokes all four nodes;
/// their current fan-out policies remain the coarser partition/finalize pair.
struct HashAggregateNodes {
    AggregateDiscoveryNode discovery;
    AggregateAccumulationNode accumulation;
    AggregateFinalOrderingNode final_ordering;
    AggregateEmissionNode emission;
};

/// Validate the typed Discovery -> Accumulation -> FinalOrdering -> Emission
/// chain. Execution calls the same predicate used by mutation tests.
[[nodiscard]] auto validate_hash_aggregate_edges(const HashAggregateNodes& nodes)
    -> std::optional<std::string>;

/// What the plan knows about an `Aggregate` node.
///
/// Every field is RELAYED from the predicates the builder itself calls --
/// `plan_fused_left_join_count` and `aggregate_is_streamable` -- rather than
/// restated here. Restating is what made `plan_join` disagree with the builder
/// about two-key joins, and the probe written from the same reading agreed with
/// the mistake.
struct AggregatePlan {
    /// False unless the planned node is an `Aggregate`.
    bool describes = false;
    AggregateStrategy strategy = AggregateStrategy::MaterializeAll;
    /// The `Join` this aggregate fuses with, or null. Naming it is what makes
    /// the fusion a property of the plan rather than a walk the builder redoes.
    const ir::Node* fused_join = nullptr;
    /// The column the fused count/sum reads, resolved back through any updates
    /// between the aggregate and the join. Empty unless fused.
    std::string counted_column;
    /// Positional bindings for the streamed/hash aggregate input. Closed known
    /// schemas fill this during planning; lazy/unknown inputs bind it once at
    /// the first concrete chunk.
    std::optional<AggregateColumnMapping> columns;
};

/// Resolve an aggregate's logical column references against one ordered input
/// schema. The same function serves physical planning and the one-time runtime
/// bind for inputs whose schema is not statically known.
[[nodiscard]] auto resolve_aggregate_columns(std::span<const ir::ColumnRef> group_by,
                                             std::span<const ir::AggSpec> aggregations,
                                             std::span<const std::string_view> input_names)
    -> std::expected<AggregateColumnMapping, std::string>;

/// Classify an aggregate. Pure, and decided entirely by relayed predicates.
[[nodiscard]] auto plan_aggregate(const ir::AggregateNode& agg) -> AggregatePlan;

/// `strategy=... fused=Join counted=<col>` for tests and `explain physical`.
[[nodiscard]] auto explain_aggregate(const AggregatePlan& plan) -> std::string;

// --- Breaker parallelism (src/runtime/PARALLELISM.md, "Target: parallelism as
// a plan decision"). A breaker's fan-out was private to its operator in
// `chunked.cpp` — invisible to `explain physical`, un-A/B-able except through
// `IBEX_CORES`. These types move the *decision* and the *tunable* onto the
// plan, one phase at a time; the determinism devices and the kernels stay in
// the operator. Distinct is done: the planner fills a `dedup` phase,
// `build_physical_distinct` resolves its worker cap, and
// `ChunkedDistinctOperator` reads it rather than deciding for itself. The two
// checks that stay in the operator — is it nested, did this chunk clear the
// floor — are the two only the operator can make.

enum class PartitionStrategy : std::uint8_t {
    PackedKey,  ///< hash-partition a packed key, one map per partition (Distinct, string/int
                ///< group-by)
    RadixHash,  ///< histogram → prefix-sum → scatter, then whole partitions
                ///< (`try_discover_partitioned`)
    Owned,      ///< partition-owned key maps + slots (`try_owned` / async hot table)
    RowRange,   ///< contiguous row ranges — the radix sort and `(column × range)` gather in
                ///< `sort.cpp` that `order_table` runs
    HeadTable,  ///< the join hash index's head map split into partitions by key hash
                ///< (`PartitionedHeads`), one worker filling each — no merge afterwards
    Range,      ///< a worker per contiguous slice of probe rows, output concatenated in
                ///< range order (`ChunkedInnerJoinOperator::probe_ranges_parallel`)
};

/// A row-count estimate available at plan time, and where it came from. The
/// planner never guesses: `Footer` is a registered scan's row count, `None`
/// means the operator will decide on its first chunk (today's behaviour).
struct RowEstimate {
    std::size_t rows = 0;
    enum class Source : std::uint8_t { None, Footer, TableExact, ChildExact } source = Source::None;

    [[nodiscard]] auto confident() const noexcept -> bool { return source != Source::None; }
};

enum class FanOutDecline : std::uint8_t {
    None,  ///< may fan out (the operator still checks `on_worker_pool_thread()` and the floor)
    SingleCore,  ///< `exec.can_fan_out()` is false
    BelowFloor,  ///< a confident estimate is under `row_floor`
};

/// One fan-out point's parallelism. The capability half (`row_floor`,
/// `breaker_max_workers`, `strategy`, `estimate`) is set by `plan_physical`,
/// which has no `ExecutionContext`. The resolved half (`decline`,
/// `worker_cap`) is filled by `resolve_breaker_parallelism` from
/// `build_physical_*`, which does. `worker_cap == 0` means "not resolved yet"
/// — the state a plan is in when `explain physical` runs straight off
/// `plan_physical` in a test.
struct BreakerParallelism {
    std::size_t row_floor = 0;
    std::size_t breaker_max_workers = 0;
    PartitionStrategy strategy = PartitionStrategy::PackedKey;
    RowEstimate estimate{};

    FanOutDecline decline = FanOutDecline::None;
    std::size_t worker_cap = 0;
};

/// A breaker is one or more named phases, each with a fan-out point. Distinct /
/// Order / TopK have one; Join has two (hash-build, probe); Aggregate currently
/// has two policy phases (partition, finalize), alongside four structural nodes
/// for discovery, accumulation, final ordering, and emission.
struct BreakerPhase {
    std::string_view name;
    BreakerParallelism parallelism;
};

/// The `dedup` phase's capability half: the row floor and worker ceiling that
/// used to be `1U << 15U` and `std::size_t{64}` inside `ChunkedDistinctOperator`
/// (twice, once per dedup path), plus the packed-key strategy. Both the planner
/// (from a footer estimate) and the whole-table `distinct_table` adapter (from
/// the input's exact row count) build the phase through here, so there is one
/// definition of the policy.
[[nodiscard]] auto distinct_dedup_parallelism(RowEstimate estimate) -> BreakerParallelism;

/// `order`'s single `sort` phase. Unlike distinct, the fan-out already lives in
/// `sort.cpp` (`radix_sort`, `gather_rows_parallel`) gated on the *shared*
/// `parallel_min_rows` / `parallel_min_cells` knobs, not a private constant --
/// so `row_floor` is left 0, which `resolve_breaker_parallelism` fills from
/// `exec.parallel_min_rows`. There is no per-breaker worker ceiling. The phase
/// is descriptive: `ChunkedOrderOperator` buffers and calls `order_table`,
/// which reads the knobs itself.
[[nodiscard]] auto order_sort_parallelism() -> BreakerParallelism;

/// A streaming join's two fan-out phases: the hash build's `1U << 17U` floor and
/// `min(budget, pool, 64)` cap, and the probe's `1U << 14U` floor and matching
/// cap. Both used to be private constants in `chunked.cpp`
/// (`ChunkedInnerJoinOperator::build_partitions`, `JoinProbe::probe_parallel_workers`);
/// the operator now reads its resolved copy of each from the plan. The two
/// checks that stay in the operator are the ones only it can make: a kill switch
/// (`IBEX_JOIN_BUILD_SERIAL` / the `parallel_join_probe` toggle), nesting
/// (`on_worker_pool_thread`), and whether this side/chunk cleared the floor.
[[nodiscard]] auto join_hash_build_parallelism() -> BreakerParallelism;
[[nodiscard]] auto join_probe_parallelism() -> BreakerParallelism;

/// Both fan-out phases of a streaming join, resolved together — what every join
/// construction site hands `ChunkedInnerJoinOperator`. Bundled so the two
/// `BreakerParallelism` values cannot be passed in the wrong order.
struct JoinParallelism {
    BreakerParallelism build;
    BreakerParallelism probe;
};

/// The typed edge between a streaming join's two physical nodes. The build
/// resolves which candidate input is indexed and returns that orientation with
/// the immutable hash index; the probe consumes both as one value.
enum class JoinDataKind : std::uint8_t {
    None,
    RuntimeOrientedBuildOutput,
};

/// Explicit physical hash-build node. Both logical inputs are candidates: the
/// smaller side is selected only after their actual row counts are known.
struct HashBuildNode {
    const ir::Node* left_input = nullptr;
    const ir::Node* right_input = nullptr;
    JoinDataKind output = JoinDataKind::None;
    BreakerParallelism parallelism;
};

/// Explicit physical hash-probe node. `build_input` names the build node's
/// output rather than smuggling the dependency through one monolithic join
/// operator. The runtime orientation selects which candidate input is probed.
struct HashProbeNode {
    JoinDataKind build_input = JoinDataKind::None;
    const ir::Node* left_input = nullptr;
    const ir::Node* right_input = nullptr;
    BreakerParallelism parallelism;
};

/// The two nodes and their barrier edge. Kept together so a plan cannot carry
/// a build without its consumer (or reverse their policies by accident).
struct StreamingJoinNodes {
    HashBuildNode build;
    HashProbeNode probe;
    /// Resolved during planning when both child schemas are closed and known;
    /// otherwise bound once from the first concrete inputs at execution.
    std::optional<ir::JoinColumnMapping> columns;
};

/// Validate the typed HashBuild -> HashProbe edge. The executor calls the same
/// predicate mutation tests use, so a malformed plan cannot silently fall back
/// to the coordinator's implicit handoff.
[[nodiscard]] auto validate_streaming_join_edge(const StreamingJoinNodes& nodes)
    -> std::optional<std::string>;

/// A hash aggregate's fan-out points. The operator has two today:
///
/// - `partition` — the histogram → prefix-sum → scatter → per-partition
///   accumulate region shared by `try_discover_partitioned` (radix-hash) and
///   `try_owned` (partition-owned key maps). Discovery and accumulation are one
///   `pool.submit` here, not two; the plan's separate "discovery" / "accumulate"
///   phases appear only once that region is actually decomposed (Phase 4).
///   `row_floor` is the *lower* admission floor (65536, `try_owned`'s
///   `kPairOwnedMinRows`); the radix path's stricter 262144
///   (`kDefaultPartitionMinRows`) is a strategy-internal detail slice 2 folds in.
/// - `finalize` — the K-way first-occurrence merge (`finalize_owned`'s
///   co-ranking merge, the ordered-run finalize, the non-owned first-row seed
///   pass). `row_floor` 131072 (`1U << 17U`).
[[nodiscard]] auto aggregate_partition_parallelism(RowEstimate estimate = {}) -> BreakerParallelism;
[[nodiscard]] auto aggregate_finalize_parallelism(RowEstimate estimate = {}) -> BreakerParallelism;

/// Both fan-out phases of a hash aggregate, resolved together — what
/// `build_physical_aggregate` hands the operator. Bundled like `JoinParallelism`
/// so the two cannot be passed in the wrong order.
struct AggregateParallelism {
    BreakerParallelism partition;
    BreakerParallelism finalize;
};

/// Fill `bp`'s resolved half. `pool_size` is `process_worker_pool().size()`, or
/// 0 when the caller declined to construct the pool for a serial query. The one
/// implementation of the worker-cap clamp that used to be open-coded per
/// operator. A phase with `row_floor == 0` inherits `exec.parallel_min_rows` --
/// the shared knob is the default, and a non-zero `row_floor` (distinct's
/// 32768) is a visible per-operator override of it.
void resolve_breaker_parallelism(BreakerParallelism& bp, const ExecutionContext& exec,
                                 std::size_t pool_size);

/// `Breaker(Distinct) keys={...}` plus one indented line per phase, for tests
/// and `explain physical`.
[[nodiscard]] auto explain_breaker(std::string_view kind, const std::vector<BreakerPhase>& phases)
    -> std::string;

struct Plan {
    bool migrated = false;
    FallbackReason reason = FallbackReason::NotMapChain;
    SourceKind source = SourceKind::TableScan;
    /// The node this pipeline's input is built from: the scan or extern call
    /// for the three scan-like kinds, and the breaker's root for
    /// `MaterializedInput`. Recorded for every plan whose walk completed,
    /// migrated or not.
    const ir::Node* source_node = nullptr;
    /// Ordered top-down (sink first), matching the direction the executor
    /// composes in. Each step carries its own kernel dispatch.
    std::vector<MapStep> steps;
    PipelineMode mode = PipelineMode::Serial;
    SerialOnlyReason serial_reason = SerialOnlyReason::NotParallelMap;
    /// The half-open range of steps that may execute over morsels, as indices
    /// into `steps` (sink-first). Empty unless `mode` is `MorselParallel`.
    ///
    /// The run need not start at the root. `df[filter ...][update ...]` lowers
    /// to Update over Filter: the update bounds the run from above, the filter
    /// is the run, and the source feeds it. Steps before `parallel_begin`
    /// execute serially on the pipeline's output; steps at and after
    /// `parallel_end` are the run's input, built and materialized beneath it.
    ///
    /// Modelling the run's position is what lets one plan describe the whole
    /// chain. Before it, a chain whose root was not parallel-eligible was
    /// declared serial and the per-kind recursion re-planned each node until it
    /// stumbled on the eligible sub-chain.
    std::size_t parallel_begin = 0;
    std::size_t parallel_end = 0;

    [[nodiscard]] auto parallel_step_count() const noexcept -> std::size_t {
        return parallel_end - parallel_begin;
    }
    std::vector<ColumnKernelSignature> source_signature;
    const ir::Node* root = nullptr;
    /// Set when `root` is a `Join`. The plan does not execute it yet, so this
    /// coexists with `migrated == false`: the plan describes more of the query
    /// than it runs, which is what makes the backlog shrinkable one kind at a
    /// time instead of in one jump.
    JoinPlan join;
    /// Present exactly for a streaming inner join. These are physical nodes,
    /// not generic labels in `breaker_phases`: their typed edge makes the
    /// build → probe dependency and its runtime-resolved orientation
    /// inspectable. Semi/anti retains its separate streaming operator.
    std::optional<StreamingJoinNodes> streaming_join;
    /// Set when `root` is an `Aggregate`.
    AggregatePlan aggregate;
    /// Present for the adaptive streamable aggregate's hash fallback. These
    /// are structural phase nodes; `breaker_phases` below continues to carry
    /// the two current fan-out policies until discovery and accumulation have
    /// independent scheduling policies.
    std::optional<HashAggregateNodes> hash_aggregate;
    /// Set when `root` is a breaker whose parallelism the plan describes.
    /// Empty otherwise. One entry per fan-out phase (see `BreakerPhase`).
    std::vector<BreakerPhase> breaker_phases;
};

/// Lower `root` into a Phase 1 plan. Read-only over the IR, the registry, and
/// the context; safe to call from tests and `explain` tooling with no intent
/// to execute. A step is admitted only when the per-kind switch would build
/// it as a map too (`is_map_step` mirrors that routing exactly), so the plan
/// can never claim a shape the executor would construct differently.
[[nodiscard]] auto plan_physical(const ir::Node& root, const TableRegistry& registry,
                                 const ExternRegistry* externs,
                                 const ir::SourceSchemas& source_schemas = {}) -> Plan;

/// Deterministic multi-line rendering for tests and debugging. A plan that
/// cannot explain why it materialized is not an acceptable plan, so every
/// fallback prints its reason.
[[nodiscard]] auto explain_physical(const Plan& plan) -> std::string;

/// The node feeding the parallel prefix: the first step below it when the
/// chain continues past what may run in parallel, else the pipeline's source.
/// Null for a serial pipeline, which has no such boundary.
[[nodiscard]] auto parallel_input_node(const Plan& plan) -> const ir::Node*;

/// Reason name as used by `explain_physical`.
[[nodiscard]] auto serial_only_reason_name(SerialOnlyReason reason) -> std::string_view;
[[nodiscard]] auto fallback_reason_name(FallbackReason reason) -> std::string_view;

// Process-wide counters. An executed pipeline and the pre-planner construction
// are indistinguishable from the outside, so — like ParallelPipelineStats — the
// path needs its own proof it fired. Read the deltas around a query in tests;
// report them from profiling tooling later.
[[nodiscard]] auto physical_plans_built() -> std::uint64_t;
[[nodiscard]] auto physical_map_pipelines() -> std::uint64_t;
[[nodiscard]] auto physical_materialized_calls() -> std::uint64_t;

/// Record that a pipeline is executing (called once per plan that migrates,
/// whether the executor is the Phase 1 serial composer or the parallel
/// pipeline, which is the same pipeline's parallel mode).
void note_map_pipeline_executed();

/// Record a fallback: one logical node the physical plan does not describe, so
/// `build_operator`'s per-kind switch decides it instead.
///
/// `kind` is what makes this a migration backlog rather than a bare total. The
/// port order for Phase 4 was written a priori ("hash join, then hash
/// aggregate, then distinct"); counting which kinds real queries actually fall
/// back on is how that guess gets replaced by evidence, and how a finished port
/// proves itself -- a migrated kind's count goes to zero and stays there.
void note_materialized_call(FallbackReason reason, ir::NodeKind kind);

/// Fallbacks recorded for one node kind, for tests that want to assert a port
/// removed them.
[[nodiscard]] auto physical_fallbacks_for(ir::NodeKind kind) -> std::uint64_t;

/// Printable name for any node kind, including the ones that are not map steps.
[[nodiscard]] auto node_kind_name(ir::NodeKind kind) -> std::string_view;

/// One `plan fallback:` line per node kind that fell back, descending by count.
/// Empty when nothing did. Printed at exit under `IBEX_PLAN_STATS`.
[[nodiscard]] auto physical_fallback_report() -> std::string;

}  // namespace ibex::runtime::physical
