// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/core/compiler.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::runtime {

class ExecutionProfileState;

enum class ScalarKind : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
};

using ColumnValue =
    std::variant<Column<std::int64_t>, Column<double>, Column<std::string>, Column<Categorical>,
                 Column<Date>, Column<Timestamp>, Column<bool>>;
using ScalarValue = std::variant<std::int64_t, double, bool, std::string, Date, Timestamp>;

/// Copy the value-metadata of `src` onto a column derived from it.
///
/// The single place a derived column inherits what its values mean. Every
/// helper that builds one column out of another routes through it, so an
/// operator that gathers, copies, or empties a column cannot silently strip its
/// time zone -- the alternative is remembering to carry it at ~40 call sites,
/// which is how the table-metadata bugs happened.
[[nodiscard]] inline auto with_meta_of(ColumnValue out, const ColumnValue& src) -> ColumnValue {
    const ColumnMeta meta = std::visit([](const auto& col) { return col.meta(); }, src);
    std::visit([&](auto& col) { col.set_meta(meta); }, out);
    return out;
}

/// Packed validity bitmap (1 bit per row): true = valid, false = null.
/// Designed for row-validity propagation where bulk bitwise ops dominate.
class ValidityBitmap {
   public:
    using word_type = std::uint64_t;
    using size_type = std::size_t;

   private:
    static constexpr size_type kBitsPerWord = sizeof(word_type) * 8;

    std::vector<word_type> words_;
    size_type size_bits_ = 0;
    std::shared_ptr<const void> external_owner_;
    const std::uint8_t* external_data_ = nullptr;
    size_type external_offset_ = 0;

    static constexpr auto word_index(size_type bit) noexcept -> size_type {
        return bit / kBitsPerWord;
    }
    static constexpr auto bit_offset(size_type bit) noexcept -> size_type {
        return bit % kBitsPerWord;
    }
    static constexpr auto bit_mask(size_type bit) noexcept -> word_type {
        return word_type{1} << bit_offset(bit);
    }
    static constexpr auto words_for_bits(size_type bits) noexcept -> size_type {
        return (bits + kBitsPerWord - 1) / kBitsPerWord;
    }
    static constexpr auto low_bits_mask(size_type bits) noexcept -> word_type {
        if (bits == 0) {
            return word_type{0};
        }
        if (bits >= kBitsPerWord) {
            return ~word_type{0};
        }
        return (word_type{1} << bits) - 1;
    }

    auto clear_unused_tail_bits() noexcept -> void {
        if (words_.empty()) {
            return;
        }
        const size_type rem = bit_offset(size_bits_);
        if (rem == 0) {
            return;
        }
        words_.back() &= low_bits_mask(rem);
    }

    auto drop_external() noexcept -> void {
        external_owner_.reset();
        external_data_ = nullptr;
        external_offset_ = 0;
    }

    // Split so the owned case — every row of every per-row validity build — is
    // a single predictable branch the caller can inline, with the copy kept
    // out of line. `set()`/`push_back()` are called once per row against ~2
    // instructions of real work, so an out-of-line call here is the whole
    // cost: before this split it measured as a ~15% regression on the
    // guarded-update benchmarks. See the identical fix on `Column::push_back`.
    IBEX_ALWAYS_INLINE auto detach_external() -> void {
        if (!external_owner_) [[likely]] {
            return;
        }
        detach_external_slow();
    }

    IBEX_NOINLINE auto detach_external_slow() -> void {
        std::vector<word_type> owned(words_for_bits(size_bits_), 0);
        for (size_type i = 0; i < size_bits_; ++i) {
            const size_type source_bit = external_offset_ + i;
            const auto byte = external_data_[source_bit / 8];
            if (((byte >> (source_bit % 8)) & 0x01U) != 0U) {
                owned[word_index(i)] |= bit_mask(i);
            }
        }
        words_ = std::move(owned);
        drop_external();
    }

   public:
    ValidityBitmap() = default;

    explicit ValidityBitmap(size_type count, bool value = false) { assign(count, value); }

    ValidityBitmap(const std::vector<bool>& values) {
        reserve(values.size());
        for (const bool v : values) {
            push_back(v);
        }
    }

    ValidityBitmap(std::initializer_list<bool> init) {
        reserve(init.size());
        for (const bool v : init) {
            push_back(v);
        }
    }

    /// Adopt an immutable Arrow-compatible bitmap slice. `owner` keeps
    /// `data` alive; `offset` and `count` are measured in bits.
    [[nodiscard]] static auto from_external(std::shared_ptr<const void> owner,
                                            const std::uint8_t* data, size_type offset,
                                            size_type count) -> ValidityBitmap {
        if (!owner) {
            throw std::invalid_argument("external validity requires a lifetime owner");
        }
        if (count != 0 && data == nullptr) {
            throw std::invalid_argument("non-empty external validity requires a bitmap buffer");
        }
        ValidityBitmap bitmap;
        bitmap.size_bits_ = count;
        bitmap.external_owner_ = std::move(owner);
        bitmap.external_data_ = data;
        bitmap.external_offset_ = offset;
        return bitmap;
    }

    [[nodiscard]] auto is_external() const noexcept -> bool {
        return static_cast<bool>(external_owner_);
    }

    [[nodiscard]] auto size() const noexcept -> size_type { return size_bits_; }
    [[nodiscard]] auto empty() const noexcept -> bool { return size_bits_ == 0; }
    [[nodiscard]] auto word_count() const noexcept -> size_type {
        return words_for_bits(size_bits_);
    }

    [[nodiscard]] auto operator[](size_type idx) const noexcept -> bool {
        if (is_external()) {
            const size_type source_bit = external_offset_ + idx;
            return ((external_data_[source_bit / 8] >> (source_bit % 8)) & 0x01U) != 0U;
        }
        return (words_[word_index(idx)] & bit_mask(idx)) != 0;
    }

    IBEX_ALWAYS_INLINE auto set(size_type idx, bool value) -> void {
        detach_external();
        auto& w = words_[word_index(idx)];
        const word_type m = bit_mask(idx);
        if (value) {
            w |= m;
        } else {
            w &= ~m;
        }
    }

    IBEX_ALWAYS_INLINE auto push_back(bool value) -> void {
        detach_external();
        const size_type idx = size_bits_;
        if (bit_offset(idx) == 0) {
            words_.push_back(0);
        }
        if (value) {
            words_.back() |= bit_mask(idx);
        }
        ++size_bits_;
    }

    auto reserve(size_type count_bits) -> void {
        detach_external();
        words_.reserve(words_for_bits(count_bits));
    }

    auto resize(size_type count_bits) -> void { resize(count_bits, false); }

    auto resize(size_type count_bits, bool value) -> void {
        detach_external();
        if (count_bits <= size_bits_) {
            size_bits_ = count_bits;
            words_.resize(words_for_bits(count_bits));
            clear_unused_tail_bits();
            return;
        }

        const size_type old_size = size_bits_;
        const size_type new_words = words_for_bits(count_bits);
        words_.resize(new_words, 0);
        size_bits_ = count_bits;
        if (value) {
            for (size_type i = old_size; i < count_bits; ++i) {
                set(i, true);
            }
        }
    }

    auto assign(size_type count_bits, bool value) -> void {
        drop_external();
        size_bits_ = count_bits;
        words_.assign(words_for_bits(count_bits), value ? ~word_type{0} : word_type{0});
        clear_unused_tail_bits();
    }

    /// Base Arrow-compatible bitmap pointer and logical bit offset.
    [[nodiscard]] auto buffer_data() const noexcept -> const std::uint8_t* {
        if (is_external()) {
            return external_data_;
        }
        return reinterpret_cast<const std::uint8_t*>(words_.data());
    }
    [[nodiscard]] auto buffer_offset() const noexcept -> size_type {
        return is_external() ? external_offset_ : 0;
    }

    /// Word access is zero-based. Mutable access detaches an external slice;
    /// const callers must consult `buffer_offset()` before bulk word reads.
    [[nodiscard]] auto words_data() -> word_type* {
        detach_external();
        return words_.data();
    }
    [[nodiscard]] auto words_data() const noexcept -> const word_type* {
        return reinterpret_cast<const word_type*>(buffer_data());
    }
};

struct ColumnEntry {
    std::string name;
    std::shared_ptr<ColumnValue> column;
    // Validity bitmap: true = valid (not null), false = null.
    // nullopt means every row is valid — the common case, with zero overhead.
    std::optional<ValidityBitmap> validity;
};

/// Returns true if row `row` of `entry` is null.
[[nodiscard]] inline auto is_null(const ColumnEntry& entry, std::size_t row) -> bool {
    return entry.validity.has_value() && !(*entry.validity)[row];
}

/// Returns the number of elements in a type-erased column.
[[nodiscard]] inline auto column_size(const ColumnValue& column) noexcept -> std::size_t {
    return std::visit([](const auto& col) { return col.size(); }, column);
}

struct Table {
    std::vector<ColumnEntry> columns;
    robin_hood::unordered_map<std::string, std::size_t> index;

    /// Order-sensitive metadata. Private because assembling it field by field is
    /// what produced the bugs `TableProperties` exists to prevent: an operator
    /// that copies `time_index` through and forgets that its rows moved has
    /// written something false, and nothing in a plain struct stops it. Read it
    /// here; replace it wholesale through `set_properties`.
    [[nodiscard]] auto properties() const noexcept -> const TableProperties& { return props_; }
    [[nodiscard]] auto ordering() const noexcept
        -> const std::optional<std::vector<ir::OrderKey>>& {
        return props_.ordering();
    }
    [[nodiscard]] auto time_index() const noexcept -> const std::optional<std::string>& {
        return props_.time_index();
    }
    [[nodiscard]] auto grouped_by() const noexcept -> const std::vector<std::string>& {
        return props_.grouped_by();
    }

    /// The single mutator. Applies the TimeFrame invariant on the way in, so a
    /// caller cannot land a half-stated claim.
    void set_properties(const TableProperties& props) { props_ = props.normalized(); }

    /// Logical row count for a column-less frame (e.g. produced by `Table(n)`).
    /// Only consulted by `rows()` when `columns` is empty; once any column is
    /// added the count is derived from the columns as usual.
    std::optional<std::size_t> logical_rows;

    void add_column(std::string name, ColumnValue column);
    /// Add a column with an explicit validity bitmap (true = valid, false = null).
    void add_column(std::string name, ColumnValue column, ValidityBitmap validity);
    /// Replace the storage for an existing column, preserving its name and validity.
    /// This keeps copy-on-write explicit: callers reseat the column handle rather
    /// than mutating a potentially shared ColumnValue in place.
    void replace_column(std::size_t pos, ColumnValue column);
    /// Replace the storage and validity for an existing column.
    void replace_column(std::size_t pos, ColumnValue column,
                        std::optional<ValidityBitmap> validity);
    /// Rename an existing column and keep the index map in sync.
    void rename_column(std::size_t pos, std::string name);

   private:
    TableProperties props_;

   public:
    /// Return a mutable column after detaching shared storage if necessary.
    [[nodiscard]] auto mutable_column(std::size_t pos) -> ColumnValue&;
    /// Share an existing column without copying its data. Safe under the
    /// copy-on-write invariant: shared columns are never mutated in place —
    /// any modification reseats a fresh shared_ptr (see add_column above).
    /// Used by zero-copy projection/rename to avoid deep-copying key columns.
    void add_column_shared(std::string name, std::shared_ptr<ColumnValue> column,
                           std::optional<ValidityBitmap> validity = std::nullopt);
    /// Share `source`'s storage and validity under `name`. Takes the entry
    /// whole rather than its parts, so the two cannot be mismatched at a call
    /// site. Column metadata needs no help here -- it lives on the column
    /// itself, so sharing the storage shares it.
    void add_column_from(std::string name, const ColumnEntry& source);
    [[nodiscard]] auto find(const std::string& name) -> ColumnValue*;
    [[nodiscard]] auto find(const std::string& name) const -> const ColumnValue*;
    [[nodiscard]] auto find_entry(const std::string& name) const -> const ColumnEntry*;
    [[nodiscard]] auto rows() const noexcept -> std::size_t {
        if (columns.empty()) {
            return logical_rows.value_or(0);
        }
        return column_size(*columns.front().column);
    }
};

using TableRegistry = robin_hood::unordered_map<std::string, Table>;
using ScalarRegistry = robin_hood::unordered_map<std::string, ScalarValue>;

class LazyTable;

/// Register-blocked Bloom filter over int64 join keys: each key sets two bits
/// inside one 64-bit word, so a membership probe touches a single cache line.
/// Sized at ~16 bits per expected key, which keeps the false-positive rate in
/// the low single-digit percent — false positives only cost wasted decode,
/// never wrong answers.
class JoinBloomFilter {
   public:
    explicit JoinBloomFilter(std::size_t expected_keys) {
        // 16 bits/key = 4 keys per 64-bit word, rounded up to a power of two
        // so the word index is a mask, not a modulo.
        std::size_t words = 8;
        while (words * 4 < expected_keys) {
            words *= 2;
        }
        words_.assign(words, 0);
        mask_ = words - 1;
    }

    void insert(std::int64_t key) noexcept {
        const auto [word, bits] = position(key);
        words_[word] |= bits;
    }

    [[nodiscard]] auto contains(std::int64_t key) const noexcept -> bool {
        const auto [word, bits] = position(key);
        return (words_[word] & bits) == bits;
    }

   private:
    // splitmix64 finalizer: cheap, and mixes well enough that the word index
    // (low bits) and the two bit choices (high bits) are independent.
    [[nodiscard]] auto position(std::int64_t key) const noexcept
        -> std::pair<std::size_t, std::uint64_t> {
        auto h = static_cast<std::uint64_t>(key) + 0x9e3779b97f4a7c15ULL;
        h = (h ^ (h >> 30U)) * 0xbf58476d1ce4e5b9ULL;
        h = (h ^ (h >> 27U)) * 0x94d049bb133111ebULL;
        h ^= h >> 31U;
        const std::uint64_t bits =
            (std::uint64_t{1} << ((h >> 32U) & 63U)) | (std::uint64_t{1} << ((h >> 38U) & 63U));
        return {static_cast<std::size_t>(h & mask_), bits};
    }

    std::vector<std::uint64_t> words_;
    std::uint64_t mask_ = 0;
};

/// Key filter a join derives from its build side for a deferred probe scan.
/// `ready` flips exactly once, before the scan is materialized, when the
/// owning join has decided (filter present or deliberately absent). A scan
/// materialized while `ready` is still false simply decodes without dynamic
/// filtering — absence is always sound, only slower.
struct DynamicScanFilter {
    bool ready = false;
    std::optional<std::int64_t> min;
    std::optional<std::int64_t> max;
    /// Exact membership: sorted distinct build keys, published when the build
    /// side is small (dimension chains). Empty means "not published". Always
    /// accompanied by `bloom`: the Bloom is the cheap reject path (a binary
    /// search per probe key costs ~8 mispredicting branches — measured 88% of
    /// a q17 run), the list only confirms the rare Bloom passes exactly.
    std::vector<std::int64_t> in_list;
    /// Approximate membership; false positives only.
    std::optional<JoinBloomFilter> bloom;

    [[nodiscard]] auto has_membership() const noexcept -> bool { return bloom.has_value(); }

    /// Only meaningful when `has_membership()`; false means "cannot match".
    [[nodiscard]] auto passes(std::int64_t key) const noexcept -> bool {
        if (!bloom->contains(key)) {
            return false;
        }
        return in_list.empty() || std::binary_search(in_list.begin(), in_list.end(), key);
    }
};

/// A lazy source whose decode the whole-script driver deferred so the join
/// probing it can narrow the scan with build-side bounds first (see
/// `ir::deferrable_probe_scans` for eligibility). The driver still owns the
/// static conjuncts and column demand it would have used to pre-decode.
struct DeferredScan {
    std::shared_ptr<LazyTable> lazy;
    std::vector<ir::Expr> conjuncts;  ///< static scan predicates proven for this source
    std::set<std::string> demand;     ///< columns the plan reads from this scan
    bool demand_all = false;
    std::string key_column;  ///< join key in the scan's own column names
    std::shared_ptr<DynamicScanFilter> filter;
};

/// A range of source rows a reader can decode without touching the rest.
///
/// This is the streaming unit: a Parquet row group or a run of them, an Arrow
/// batch, whatever decomposition the source has natively. `start` is a
/// **source-global** row index and every `Selection` stays in source-global
/// indices too, so a selection means the same thing whether it is handed to a
/// whole-source decode or restricted to one unit. That is what lets the
/// per-unit path reuse the whole-source pushdown machinery unchanged instead of
/// growing a second, unit-local index space.
struct SourceUnit {
    std::size_t start = 0;
    std::size_t rows = 0;
};

/// Keyed by scan (instance) name as it appears in the plan.
using DeferredScanRegistry = std::map<std::string, DeferredScan>;

/// Query-scoped execution context. Created once at the `interpret()` boundary
/// and threaded explicitly through operator construction (`build_operator`) and
/// the full-table interpreter (`interpret_node`) instead of an execution-scoped
/// thread-local. Making per-query state an explicit parameter is the
/// prerequisite for moving work onto worker threads (see the runtime
/// multithreading plan, Phase 0): a worker must be able to see this state
/// without relying on the thread it happens to run on.
///
/// Phase 0 owns only the deferred-scan registry; the query thread budget, RNG
/// seed, and worker-failure/cancellation state are added by later phases. A
/// default-constructed context (no deferred scans) reproduces the pre-context
/// serial behavior.
/// Per-query counters for the parallel-island executor. Optional: a query only
/// pays for them when an `ExecutionContext` points at one, and they are touched
/// once per island (never per row or per morsel), so they are free on the hot
/// path.
///
/// Their reason to exist is that the island decision is invisible from the
/// outside — a parallel island and a serial one produce identical output by
/// construction. Without a counter, a benchmark or a test cannot tell "ran in
/// parallel" from "silently fell back to serial", which is the failure mode
/// that makes a parallel test hollow.
struct ParallelIslandStats {
    std::atomic<std::uint64_t> parallel_islands{0};  ///< islands run on worker threads
    std::atomic<std::uint64_t> serial_islands{0};    ///< islands below the grain threshold
    std::atomic<std::uint64_t> morsels{0};           ///< morsels those islands partitioned into
    /// Islands whose head operator was absorbed into a range-evaluating source
    /// instead of being run above a gathered morsel. Observability for the
    /// zero-copy path: it is a silent optimization, so without a counter a
    /// regression to gathering everywhere would only show up as a slow
    /// benchmark.
    std::atomic<std::uint64_t> range_heads{0};
    /// Update fields whose evaluation was split across worker threads inside
    /// `update_table`, rather than run as one whole-table evaluation. Same
    /// reason as `range_heads`: the split is invisible in the output, so
    /// without a counter a regression to serial would leave every test green
    /// and only show up as a slow benchmark.
    std::atomic<std::uint64_t> parallel_fields{0};
    /// Islands run as a two-phase filter — output presized from per-morsel
    /// popcounts, then gathered into disjoint slices — instead of through the
    /// ordered merger. Same reason as `range_heads`: both produce identical
    /// output, so without a counter a silent fall back to the merger (a
    /// narrowed gate, a newly nullable column) would cost the merge copy again
    /// with every test still green.
    std::atomic<std::uint64_t> two_phase_filters{0};
    /// Grouped windowed updates whose groups were spread across worker threads.
    /// Same reason as the counters above: the output is identical either way,
    /// so without this a gate that quietly stopped matching — a newly nullable
    /// field, an extern call added to a query — would cost the parallelism with
    /// every test still green.
    std::atomic<std::uint64_t> parallel_group_windows{0};
};

struct ExecutionContext {
    /// Deferred lazy scans the whole-script driver installed for this query, or
    /// null when the query has none. Not owned: the driver keeps the registry
    /// alive for the whole `interpret()` call.
    const DeferredScanRegistry* deferred_scans = nullptr;

    /// Opt-in internal operator/source profile, installed by
    /// configure_parallel_from_env() when IBEX_PROFILE_OPERATORS is present.
    /// Shared ownership keeps the report alive across streamed operators and
    /// prints it when the query's last execution context/operator releases it.
    std::shared_ptr<ExecutionProfileState> execution_profile{};

    /// Runtime-multithreading Phase 1. When set, `build_operator()` consults
    /// `analyze_parallel_island()` at its seam and, for an eligible row-local
    /// parallel-map chain, executes it over morsels of the materialized island
    /// input instead of a single whole-table chunk. Whether those morsels run
    /// on worker threads or serially is decided per island by the size
    /// thresholds below; either way an ordered merger emits results in morsel
    /// `sequence` order, so output is byte-identical to the plain serial path.
    /// **On by default.** A 24-configuration sweep from 131k to 20M rows, at 2
    /// and 6 columns and both selectivities, measured 20 wins, 4 parity and 0
    /// regressions; the size gates below are what keep the small end at parity.
    /// `IBEX_PARALLEL=0` turns it off.
    bool parallel = true;

    /// Morsel row-grain for the island source when `parallel` is set. The input
    /// is partitioned into contiguous ranges of at most this many rows, and one
    /// range is one parallel task. Ignored when `parallel` is false.
    ///
    /// **0 means derive it from the input** (`island_grain`), which is the
    /// default: a measured sweep found no grain in a 1000x band that loses to
    /// serial, so there is nothing here worth asking a user to tune. A non-zero
    /// value is an explicit override and is used as given.
    std::size_t parallel_grain = 0;

    /// Island thread budget, or 0 to use the process pool's size
    /// (`IBEX_THREADS`). Clamped to the pool size and to the morsel count.
    std::size_t parallel_threads = 0;

    /// The plan's grain-size serial threshold: an island input smaller than
    /// this stays on the serial morsel chain rather than paying task,
    /// synchronization, and merge overhead to parallelize cache-resident work.
    /// Tests that need the worker path on a small table set this to 0.
    ///
    /// This is a floor on ROWS, which is the right unit for splitting one
    /// expression across ranges (`evaluate_field_maybe_parallel`, whose work is
    /// per row). An island also copies per *cell*, so it applies
    /// `parallel_min_cells` on top of this.
    std::size_t parallel_min_rows = 65536;

    /// Second island threshold, in cells (rows x output columns), or 0 to skip
    /// the check.
    ///
    /// An island's cost is dominated by copying rows out, which scales with
    /// table WIDTH — so a row count alone cannot say whether the work is worth
    /// a fan-out. Measured: 131,072 rows won at 6 columns and *lost* at 2, on
    /// the same predicate. Both clear any sane row threshold; only the cell
    /// count separates them.
    std::size_t parallel_min_cells = 512UL * 1024;

    /// Optional island counters, or null to record nothing. Not owned.
    ParallelIslandStats* parallel_stats = nullptr;

    /// Look up a deferred scan by its plan (instance) name, or null if there is
    /// no registry or no matching entry.
    [[nodiscard]] auto deferred_scan(const std::string& name) const -> const DeferredScan* {
        if (deferred_scans == nullptr) {
            return nullptr;
        }
        const auto it = deferred_scans->find(name);
        return it == deferred_scans->end() ? nullptr : &it->second;
    }
};

/// Apply the parallel-island environment switches to `exec`: `IBEX_PARALLEL`
/// enables islands, and `IBEX_MORSEL_ROWS` overrides the morsel grain (and,
/// when set explicitly, drops the serial threshold to that grain so a
/// deliberately small grain is honored). Unset variables leave `exec`
/// untouched, so this never overrides a budget the caller chose.
///
/// `IBEX_THREADS` is deliberately *not* applied here: it sizes the process
/// worker pool, which a zero `parallel_threads` defers to anyway.
///
/// This is how a benchmark run turns the executor on: parallel islands stay off
/// by default until Phase 1's acceptance measurements say otherwise.
void configure_parallel_from_env(ExecutionContext& exec);

/// A process-lifetime island counter when `IBEX_PARALLEL_STATS` is set in the
/// environment, else null. `configure_parallel_from_env` installs it, and the
/// accumulated totals are written to stderr at process exit.
///
/// This exists because the island decision is invisible from the outside: an
/// island and the serial chain produce identical output by construction, so a
/// benchmark cannot tell "ran in parallel" from "silently fell back to serial".
/// Without it an A/B showing no difference has two indistinguishable readings —
/// the parallelism did not pay, or it never happened. `ExecutionContext::
/// parallel_stats` already answers that for one query in a test; this answers
/// it for a whole process that a harness drives through stdin.
///
/// Accumulating across queries is the point rather than a limitation: a PDS-H
/// run is 22 queries in one warm process, and the question worth asking is how
/// many islands the *suite* formed.
[[nodiscard]] auto process_island_stats() -> ParallelIslandStats*;

/// Materialize a deferred scan now: static conjuncts plus whatever bounds its
/// filter slot carries (if `ready`). The single decode path for deferred
/// sources — both the chunked join and the interpret fallback use it.
[[nodiscard]] auto materialize_deferred_scan(const DeferredScan& scan, const ExecutionContext& exec)
    -> std::expected<Table, std::string>;

/// What a deferred scan decided to apply, worked out once from the filter
/// slot's published state.
///
/// `materialize_deferred_scan` does this and the decode in one call, which is
/// all a whole-table consumer needs. A streaming consumer decodes many times
/// and must not re-decide in between: the filter slot is shared mutable state,
/// so re-reading it per unit could apply a bound to unit 3 that units 0-2 never
/// saw, and the concatenation would be missing rows no predicate excluded.
struct DeferredScanPlan {
    std::set<std::string> names;      ///< columns to decode, already expanded for demand_all
    std::vector<ir::Expr> conjuncts;  ///< static predicates plus any synthesized bounds
    /// Non-null when a membership filter applies, in which case the key is
    /// `scan.key_column`. Borrowed from the scan's filter slot.
    const DynamicScanFilter* dynamic = nullptr;
};

[[nodiscard]] auto plan_deferred_scan(const DeferredScan& scan) -> DeferredScanPlan;

/// Whether a lazy source should be streamed through its scan operator rather
/// than decoded whole before the plan runs.
///
/// **On by default** (`plans/pipelined-execution-plan.md`, Phases 1-2):
/// streaming measures 7.8% faster than materializing on PDS-H at 8 cores and
/// 5.7% faster on one, and it bounds how much of a source is decoded at once.
/// `IBEX_STREAM_SCAN=0` (or `off`/`false`/`no`) turns it off, which is the
/// escape hatch for a bug in it or an A/B against it.
///
/// Read fresh each call so a test can flip it, and consulted in exactly two
/// places — the driver that decides whether to defer a source, and the scan
/// operator builder — which must agree, hence one function rather than two
/// `getenv`s.
[[nodiscard]] auto stream_scans_enabled() -> bool;

/// The scan's streaming decomposition, or empty when its source has none (a
/// non-lazy source, or a reader with no unit decode). Decodes nothing.
[[nodiscard]] auto deferred_scan_units(const DeferredScan& scan) -> std::vector<SourceUnit>;

/// One unit of a deferred scan, under a plan already fixed by
/// `plan_deferred_scan`. Concatenating every unit in order reproduces
/// `materialize_deferred_scan`.
[[nodiscard]] auto materialize_deferred_scan_unit(const DeferredScan& scan,
                                                  const DeferredScanPlan& plan,
                                                  const SourceUnit& unit,
                                                  const ExecutionContext& exec)
    -> std::expected<Table, std::string>;

/// Opaque model result produced by the `model { ... }` clause.
/// Accessor functions (`coef`, `residuals`, `fitted`, `summary`) extract
/// sub-tables; `predict` applies the stored formula to new data.
struct ModelResult {
    Table coefficients;   ///< term | estimate (empty for non-linear plugins)
    Table summary;        ///< term | estimate | std_error | t_stat | p_value
    Table fitted_values;  ///< single column: fitted
    Table residuals;      ///< single column: residual
    Table importance;     ///< term | gain (tree models; empty otherwise)
    /// Opaque, self-freeing handle to a plugin-owned native model (e.g. a
    /// LightGBM booster), set by model plugins. Reused by model_predict; null
    /// for built-in linear methods. See ExternRegistry::ModelOps.
    std::shared_ptr<void> native;
    ir::ModelFormula formula;
    std::string method;
    double r_squared = 0.0;
    double adj_r_squared = 0.0;
    std::size_t n_obs = 0;
    std::size_t n_params = 0;
};

using ModelRegistry = robin_hood::unordered_map<std::string, ModelResult>;

/// Interpret an IR node tree against a table registry. This is a top-level
/// runtime entry point: extern/plugin callbacks must not call it re-entrantly.
/// Plugins provide data/functions to the host query; they do not initiate
/// nested query execution.
class ExternRegistry;

/// Interpret `node` under an ExecutionContext built from the environment
/// (`IBEX_PARALLEL`, `IBEX_MORSEL_ROWS`, `IBEX_PARALLEL_STATS`) on top of the
/// defaults. Use the overload below to supply a context directly — that is
/// also how a caller opts OUT of the environment.
[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry,
                             const ScalarRegistry* scalars = nullptr,
                             const ExternRegistry* externs = nullptr,
                             ModelResult* model_out = nullptr) -> std::expected<Table, std::string>;

/// `interpret()` overload for callers that supply an explicit query
/// `ExecutionContext` (e.g. the whole-script driver installing deferred scans).
/// The context is borrowed for the duration of the call and must outlive it.
[[nodiscard]] auto interpret(const ir::Node& node, const TableRegistry& registry,
                             const ScalarRegistry* scalars, const ExternRegistry* externs,
                             ModelResult* model_out, const ExecutionContext& exec)
    -> std::expected<Table, std::string>;

/// Invoke an extern whose first argument is a table. The scalar result, if
/// any, is intentionally discarded: this API is the execution seam for
/// top-level script effects such as write_csv and write_parquet.
[[nodiscard]] auto invoke_table_consumer(const ExternRegistry& externs, const std::string& callee,
                                         const Table& input, const std::vector<ScalarValue>& args)
    -> std::expected<void, std::string>;

/// Evaluate row-local filter conjuncts and return the surviving row indices in
/// ascending order. Null predicate values do not survive (the same three-valued
/// logic used by a Filter node). This is the seam used by deferred file readers
/// to late-materialize non-predicate columns without duplicating expression
/// evaluation inside an I/O plugin. Later conjuncts compact their referenced
/// columns once earlier conjuncts have made the candidate set selective.
/// `exec` supplies the parallel settings: a fused-bounds scan splits into row
/// ranges under it. Row indices are exact, so a range split reproduces the
/// serial answer bit for bit.
[[nodiscard]] auto filter_selection(const Table& input, const std::vector<ir::Expr>& conjuncts,
                                    const ExecutionContext& exec,
                                    const ScalarRegistry* scalars = nullptr)
    -> std::expected<std::vector<std::size_t>, std::string>;

/// Predicts on new data with a previously fitted plugin model, reusing its
/// native handle. Rebuilds the design matrix from `newdata` using the model's
/// stored formula. Returns a single-column "prediction" table.
[[nodiscard]] auto predict_model(const ModelResult& model, const Table& newdata,
                                 const ExternRegistry& externs)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto join_tables(
    const Table& left, const Table& right, ir::JoinKind kind, const std::vector<ir::JoinKey>& keys,
    const ir::Expr* predicate = nullptr, const ScalarRegistry* scalars = nullptr,
    const ir::JoinSuffixPolicy& suffix = {}, ir::NullMatch null_match = ir::NullMatch::Never,
    const ir::JoinExpect& expect = {}, ir::MatchSelection take = ir::MatchSelection::All)
    -> std::expected<Table, std::string>;

[[nodiscard]] auto extract_scalar(const Table& table, const std::string& column)
    -> std::expected<ScalarValue, std::string>;

/// Row-wise scalar builtins (abs, sqrt, the transcendentals, ceil/floor/trunc,
/// date parts, pmin/pmax, is_nan, casts) live in one registry shared by the
/// table-expression evaluators. These expose that registry so a scalar-only
/// caller (the REPL's scalar evaluator) can route to it instead of maintaining
/// a parallel table. `is_scalar_builtin` reports membership; `eval_scalar_builtin`
/// applies the builtin to already-evaluated scalar arguments (validating arity).
[[nodiscard]] auto is_scalar_builtin(std::string_view name) -> bool;
[[nodiscard]] auto eval_scalar_builtin(std::string_view name, const std::vector<ScalarValue>& args)
    -> std::expected<ScalarValue, std::string>;

/// Reduce a whole series to a scalar with an aggregate function (sum, mean, min,
/// max, count, median, std, first, last, skew, kurtosis, and ewma/quantile with
/// `param`). Lets `max(series)` etc. work in scalar position — distinct from the
/// element-wise `pmax`/`pmin` (which are `is_scalar_builtin`).
[[nodiscard]] auto aggregate_series(std::string_view name, const ColumnValue& column,
                                    double param = 0.0) -> std::expected<ScalarValue, std::string>;

[[nodiscard]] auto evaluate_row_count_expr(const ir::Expr& expr,
                                           const ScalarRegistry* scalars = nullptr,
                                           const ExternRegistry* externs = nullptr)
    -> std::expected<std::size_t, std::string>;

/// Merge two validity bitmaps (`a && b`) for the first `n` rows.
/// Returns nullopt when both inputs are nullopt-equivalent (nullptr).
/// Exposed for micro-benchmarking and runtime-level utilities.
[[nodiscard]] auto merge_validity_bitmaps(const ValidityBitmap* a, const ValidityBitmap* b,
                                          std::size_t n) -> std::optional<ValidityBitmap>;

}  // namespace ibex::runtime
