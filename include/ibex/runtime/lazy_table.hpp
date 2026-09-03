// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/like.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::runtime {

/// Ascending, zero-based row indices over a deferred source.
using Selection = std::vector<std::size_t>;

/// Decodes exactly the named columns from an underlying source. Supplied by
/// whichever plugin backs the source (Parquet today); the names are always a
/// subset of the schema the `LazyTable` was built with. A null selection means
/// every source row; otherwise only the named source rows are materialized.
using ColumnDecodeFn = std::function<std::expected<Table, std::string>(
    const std::vector<std::string>&, const Selection*)>;

/// Optional fused scan a source may support: evaluate a join-derived key
/// filter against the named column *during* its decode and return the passing
/// rows, without materializing the column. The inner optional is nullopt when
/// the source has no fused answer — an unsupported column type, or the filter
/// stopped rejecting partway through — and the caller must fall back to the
/// ordinary decode-then-filter path, which is always correct.
using KeyFilterScanFn = std::function<std::expected<std::optional<Selection>, std::string>(
    const std::string&, const DynamicScanFilter&, const ExecutionContext&)>;

/// A `like` / `not like` predicate over one string column, in a form a source
/// can evaluate inside its own decoder.
///
/// This exists for the column a query references *only* from its filter. Such a
/// column is decoded whole, tested once, and thrown away — and for a wide text
/// column (TPC-H's `o_comment`) that materialization dominates the query. The
/// predicate's result is one bit per row, so evaluating it as values leave the
/// page decoder skips the materialization entirely.
///
/// Null is not a value the pattern is matched against: `like(null, p)` is null,
/// and a filter keeps only true, so a null row fails **both** polarities. That
/// is why `negated` sits here rather than being applied by the caller.
struct StringScanFilter {
    LikePattern pattern;
    bool negated = false;

    /// Only for non-null values; see the note on nulls above.
    [[nodiscard]] auto passes(std::string_view value) const -> bool {
        return like_match(pattern, value) != negated;
    }
};

/// One independently mutable reader/decoder for a lazy source.
///
/// Implementations may share immutable file handles and schema metadata, but
/// each product owns its decoder cursor and other mutable backend state.
class LazySourceReader {
   public:
    virtual ~LazySourceReader() = default;

    /// The source's streaming decomposition, in ascending row order and
    /// covering every row exactly once. Empty — the default — means the source
    /// has no decomposition and must be decoded whole; `LazyTable` then never
    /// passes a non-null `unit` to any of the methods below, so a reader that
    /// does not override this may ignore that parameter entirely.
    [[nodiscard]] virtual auto decode_units() -> std::vector<SourceUnit> { return {}; }

    /// `exec` carries the query's parallel settings. A decoder is free to use
    /// them (the Parquet one decodes columns across workers) or ignore them,
    /// but it must not consult the environment for the answer: the execution
    /// context is the single authority on whether a query runs parallel.
    ///
    /// `unit` restricts the decode to one range from `decode_units()`; null
    /// means the whole source. The result holds only that unit's rows, and
    /// `selection` — still source-global — is intersected with the unit rather
    /// than reinterpreted relative to it.
    [[nodiscard]] virtual auto decode(const std::vector<std::string>& names,
                                      const Selection* selection, const SourceUnit* unit,
                                      const ExecutionContext& exec)
        -> std::expected<Table, std::string> = 0;

    /// Passing rows for `filter`, in source-global indices; `unit` restricts
    /// the scan the same way it restricts `decode`.
    [[nodiscard]] virtual auto key_filter_scan(const std::string& /*key*/,
                                               const DynamicScanFilter& /*filter*/,
                                               const SourceUnit* /*unit*/,
                                               const ExecutionContext& /*exec*/)
        -> std::expected<std::optional<Selection>, std::string> {
        return std::optional<Selection>{};
    }

    /// Evaluate `filter` against `column` during its decode and return the
    /// passing rows, without materializing the column. nullopt means the
    /// source has no fused answer for this column (a nested or unsupported
    /// encoding), and the caller falls back to decode-then-filter, which is
    /// always correct.
    ///
    /// Unlike `key_filter_scan` this never gives up on a high pass rate. That
    /// escape hatch protects a *speculative* filter, which the caller could
    /// choose not to apply; this predicate is the query's own, so its answer is
    /// needed whatever fraction of rows survives.
    [[nodiscard]] virtual auto string_filter_scan(const std::string& /*column*/,
                                                  const StringScanFilter& /*filter*/,
                                                  const SourceUnit* /*unit*/,
                                                  const ExecutionContext& /*exec*/)
        -> std::expected<std::optional<Selection>, std::string> {
        return std::optional<Selection>{};
    }
};

using LazySourceReaderPtr = std::unique_ptr<LazySourceReader>;
using LazySourceReaderFactory = std::function<std::expected<LazySourceReaderPtr, std::string>()>;

/// What a source's metadata says about one column before anything is decoded.
///
/// Parquet footers carry min/max and a null count per column chunk. They do
/// **not** carry distinct counts — that field is optional in the spec and
/// Arrow's writer leaves it unset on every file we produce — so anything about
/// distinctness has to be *derived* from what is here, by the planner, which
/// owns that policy. This type is deliberately raw metadata and nothing else.
struct ColumnStats {
    /// Whole-source value range for an INTEGER column, merged across row groups.
    /// Absent for other types, and wherever any chunk lacks statistics — a
    /// partial range would be a lie, not a conservative answer.
    std::optional<std::int64_t> min;
    std::optional<std::int64_t> max;
    /// Nulls across the whole source. Absent when any chunk fails to report one.
    std::optional<std::size_t> null_count;
};

/// Column name -> what the source's metadata says about it. A column may be
/// absent, which means "nothing known" and never "nothing there".
using SourceColumnStats = robin_hood::unordered_map<std::string, ColumnStats>;

/// A table source whose schema is known up front but whose column data is
/// decoded only when a query asks for it, and cached once decoded.
///
/// This is what turns a `let t = read_parquet(p)` binding into a projection
/// pushdown: binding reads the file's metadata (schema + row count) and nothing
/// else, then each query decodes just the columns it references. A source that
/// cannot read columns selectively has no reason to use this — it should keep
/// returning a materialized `Table`.
///
/// Decoded columns accumulate in the cache, so a binding used by several
/// queries decodes each column at most once across all of them.
class LazyTable {
   public:
    /// `schema` is a zero-row Table carrying the source's column names and
    /// types; `rows` is its true row count. `stats` is optional metadata a
    /// source may know for free (Parquet's footer); an empty map costs only the
    /// planning that would have used it. `key_filter_scan` is the optional
    /// fused dynamic-filter scan; sources without one leave it empty.
    LazyTable(Table schema, std::size_t rows, ColumnDecodeFn decode, SourceColumnStats stats = {},
              KeyFilterScanFn key_filter_scan = {});
    /// Factory-backed sources keep successful products in a local idle pool.
    /// Sequential stages reuse a reader; simultaneous acquisitions create
    /// distinct products, so mutable decoder state is never shared by workers.
    LazyTable(Table schema, std::size_t rows, LazySourceReaderFactory reader_factory,
              SourceColumnStats stats = {});

    /// Column names and types, with no rows. Cheap: known from metadata alone.
    [[nodiscard]] auto schema() const noexcept -> const Table& { return schema_; }
    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_; }
    /// Per-column source metadata, for the planner. Read from the file's footer
    /// at bind time, so consulting it decodes nothing.
    [[nodiscard]] auto column_stats() const noexcept -> const SourceColumnStats& { return stats_; }

    /// Materialize `names`, in schema order. Names not in the schema are
    /// ignored, so a caller may pass the union of the columns demanded across
    /// several sources without first splitting it per source.
    [[nodiscard]] auto project(const std::set<std::string>& names, const ExecutionContext& exec)
        -> std::expected<Table, std::string>;

    /// Decode `names` WITHOUT populating the column cache, for a caller that
    /// wants to LOOK at a column without committing the source to eager
    /// decoding.
    ///
    /// A cached column is not free: `project_where` and the deferred probe both
    /// decline their fused / dynamic-key scan when the key column is already in
    /// `cache_` (deliberately — a whole-file column left by an earlier query
    /// must never masquerade as this query's selection). So plan-time key
    /// verification, which reads exactly the columns those scans care about,
    /// would silently disable them by using `project`.
    [[nodiscard]] auto project_uncached(const std::set<std::string>& names,
                                        const ExecutionContext& exec)
        -> std::expected<Table, std::string>;

    /// Materialize `names` after applying row-local scan conjuncts. Predicate
    /// columns are decoded first to compute a selection; all other columns are
    /// decoded with that selection. This deliberately bypasses `cache_`: a
    /// selected column must never masquerade as a cached whole-file column in a
    /// later query.
    ///
    /// `dynamic` + `dynamic_key` optionally add a join-derived key membership
    /// filter (Bloom or exact IN-list) over the named key column, ANDed into
    /// the selection. Ignored — soundly, membership only removes rows that
    /// cannot match — when the key column is missing or not int64, and skipped
    /// when a sampled pass rate says the filter barely rejects (a near-full
    /// selection would gather-decode every other column for nothing).
    ///
    /// `exec` carries the parallel settings the scan's filter runs under. It is
    /// required rather than defaulted: a scan that silently fell back to one
    /// thread because a caller forgot it is exactly the bug this seam had.
    [[nodiscard]] auto project_where(const std::set<std::string>& names,
                                     const std::vector<ir::Expr>& conjuncts,
                                     const ExecutionContext& exec,
                                     const ScalarRegistry* scalars = nullptr,
                                     const DynamicScanFilter* dynamic = nullptr,
                                     const std::string* dynamic_key = nullptr)
        -> std::expected<Table, std::string>;

    /// The rows `conjuncts` select, WITHOUT materializing any output column.
    ///
    /// `project_where` computes a selection and immediately spends it decoding
    /// that occurrence's own output columns, which is right when one occurrence
    /// owns the decode. It is exactly wrong when several occurrences of one
    /// source share a decode: each would re-read the file. This returns the
    /// selection alone, so a caller can decode the shared columns once and
    /// gather per occurrence.
    ///
    /// `nullopt` means "every row" -- no conjunct rejected anything -- and is
    /// distinct from an empty `Selection`, which means nothing survived.
    ///
    /// `output_names` is what the query will actually read from this source. It
    /// gates the fused scan exactly as `project_where` does: a predicate column
    /// that the projection also wants is decoded normally rather than answered
    /// inside the page decoder, because it has to be materialized anyway.
    ///
    /// Phase 2 seam of plans/per-occurrence-scan-selections-plan.md. Nothing
    /// calls it yet.
    [[nodiscard]] auto selection_for(const std::set<std::string>& output_names,
                                     const std::vector<ir::Expr>& conjuncts,
                                     const ExecutionContext& exec,
                                     const ScalarRegistry* scalars = nullptr)
        -> std::expected<std::optional<Selection>, std::string>;

    /// The source's streaming decomposition, or empty when it has none.
    ///
    /// A source with units can be scanned a piece at a time through
    /// `project_where_unit` instead of materialized whole by `project_where`.
    /// Consulting this decodes nothing: it is read from metadata the binding
    /// already holds.
    [[nodiscard]] auto scan_units() -> std::vector<SourceUnit>;

    /// `project_where` restricted to one unit from `scan_units()`.
    ///
    /// Every pushdown the whole-source call applies is applied here too, to
    /// that unit alone: projection, static conjuncts, the dynamic key
    /// membership filter, and both fused scans. Concatenating the units in
    /// order reproduces `project_where`'s table exactly — that equality is the
    /// contract, and `tests/test_lazy_table.cpp` asserts it directly rather
    /// than trusting the two code paths to stay in step.
    ///
    /// Unlike `project_where` this never reads or writes `cache_` for the
    /// columns it decodes: a unit holds a fragment of a column, and a fragment
    /// must never be able to masquerade as a whole one. A column another query
    /// already cached whole is sliced rather than re-read.
    [[nodiscard]] auto project_where_unit(const std::set<std::string>& names,
                                          const std::vector<ir::Expr>& conjuncts,
                                          const SourceUnit& unit, const ExecutionContext& exec,
                                          const ScalarRegistry* scalars = nullptr,
                                          const DynamicScanFilter* dynamic = nullptr,
                                          const std::string* dynamic_key = nullptr)
        -> std::expected<Table, std::string>;

    /// Materialize `names` through an explicit ascending row selection —
    /// late materialization for a caller (the deferred-probe join) that
    /// already knows exactly which rows survive. Bypasses `cache_` like
    /// `project_where` does, and for the same reason.
    [[nodiscard]] auto project_rows(const std::set<std::string>& names, const Selection& selected,
                                    const ExecutionContext& exec)
        -> std::expected<Table, std::string>;

    /// Phase A of a two-phase deferred probe: compute the scan's selection
    /// (static conjuncts + key membership, or the fused key scan) and the
    /// key column's values for those rows — without decoding anything else.
    /// The outer optional is nullopt when there is no selective answer
    /// (no membership, non-int64 key, or the escape hatch fired) and the
    /// caller must fall back to the ordinary full materialization.
    struct JoinKeySelection {
        Selection selected;  ///< ascending source-row indices
        ColumnEntry keys;    ///< key values for exactly those rows
    };
    [[nodiscard]] auto join_key_selection(const std::vector<ir::Expr>& conjuncts,
                                          const ExecutionContext& exec,
                                          const ScalarRegistry* scalars,
                                          const DynamicScanFilter& dynamic,
                                          const std::string& key_name)
        -> std::expected<std::optional<JoinKeySelection>, std::string>;

    /// Materialize every column — the fallback for anything that consumes the
    /// table whole rather than through a query plan.
    [[nodiscard]] auto materialize(const ExecutionContext& exec)
        -> std::expected<Table, std::string>;

   private:
    class ReaderPool;

    /// A conjunct this scan will hand to the source instead of evaluating it
    /// over a materialized column.
    struct FusedStringConjunct {
        std::string column;
        StringScanFilter filter;
    };

    /// The conjuncts eligible for the fused string scan, and the columns they
    /// consume. A conjunct qualifies only when it is `like`/`not like` over a
    /// String column against a literal pattern, and that column is read by
    /// nothing else: not by another conjunct, not by the projection, and not
    /// already sitting in `cache_` (where testing it in memory beats re-reading
    /// its pages). Returns nullopt when nothing qualifies, so the caller keeps
    /// its existing path unchanged.
    [[nodiscard]] auto fusable_string_conjuncts(const std::vector<ir::Expr>& conjuncts,
                                                const std::set<std::string>& names,
                                                std::vector<FusedStringConjunct>& fused,
                                                std::vector<ir::Expr>& remaining) const -> bool;

    /// Decode the referenced columns whole-file into `cache_` (they are
    /// legitimate whole-column entries) and return them as a table.
    [[nodiscard]] auto decode_whole_columns(
        const robin_hood::unordered_set<std::string>& referenced, const ExecutionContext& exec)
        -> std::expected<Table, std::string>;
    [[nodiscard]] auto decode_columns(const std::vector<std::string>& names,
                                      const Selection* selection, const SourceUnit* unit,
                                      const ExecutionContext& exec)
        -> std::expected<Table, std::string>;
    [[nodiscard]] auto scan_key_filter(const std::string& key, const DynamicScanFilter& filter,
                                       const SourceUnit* unit, const ExecutionContext& exec)
        -> std::expected<std::optional<Selection>, std::string>;
    /// The conjunct columns eligible for staging through a selection; nullopt
    /// when any of them is variable-width and the staged path must be declined.
    [[nodiscard]] auto stageable_conjunct_columns(const std::vector<ir::Expr>& conjuncts) const
        -> std::optional<std::set<std::string>>;
    /// AND static conjuncts into an existing selection, decoding their columns
    /// through it. nullopt = the conjuncts name no column to stage.
    [[nodiscard]] auto narrow_selection(Selection selected, const std::vector<ir::Expr>& conjuncts,
                                        const ExecutionContext& exec, const ScalarRegistry* scalars)
        -> std::expected<std::optional<Selection>, std::string>;
    /// The predicate columns for one unit, never cached and never read from
    /// the cache except to slice a column that is already there whole.
    [[nodiscard]] auto decode_unit_predicate_columns(
        const robin_hood::unordered_set<std::string>& referenced, const SourceUnit& unit,
        const ExecutionContext& exec) -> std::expected<Table, std::string>;
    /// Every demanded column of `unit`, with no filtering — the unit-scoped
    /// counterpart of `project` for a scan that has nothing to filter by.
    [[nodiscard]] auto project_unit(const std::set<std::string>& names, const SourceUnit& unit,
                                    const ExecutionContext& exec)
        -> std::expected<Table, std::string>;
    /// Run every conjunct that `fusable_string_conjuncts` claimed through the
    /// source's fused scan, intersecting their selections. nullopt = at least
    /// one had no fused answer, so the caller must evaluate them all the
    /// ordinary way (a partial answer would silently drop the rest).
    [[nodiscard]] auto scan_string_filters(const std::vector<FusedStringConjunct>& fused,
                                           const SourceUnit* unit, const ExecutionContext& exec)
        -> std::expected<std::optional<Selection>, std::string>;
    [[nodiscard]] auto acquire_reader() -> std::expected<LazySourceReaderPtr, std::string>;
    void release_reader(LazySourceReaderPtr reader);

    Table schema_;
    std::size_t rows_ = 0;
    ColumnDecodeFn decode_;
    LazySourceReaderFactory reader_factory_;
    std::shared_ptr<ReaderPool> reader_pool_;
    SourceColumnStats stats_;
    KeyFilterScanFn key_filter_scan_;
    robin_hood::unordered_map<std::string, ColumnEntry> cache_;
};

using LazyTablePtr = std::shared_ptr<LazyTable>;

/// Phase A of the two-phase deferred probe: the scan's selection (static
/// conjuncts + membership, or the fused key scan) plus the key values for
/// exactly those rows. nullopt = no selective answer; fall back to
/// `materialize_deferred_scan`.
[[nodiscard]] auto deferred_scan_key_selection(const DeferredScan& scan,
                                               const ExecutionContext& exec)
    -> std::expected<std::optional<LazyTable::JoinKeySelection>, std::string>;

/// Phase B: materialize the scan's demanded columns through the survivor
/// selection, splicing in `key_column` (the survivors' key values, already
/// in hand from phase A) at its schema position so column order matches the
/// ordinary path.
[[nodiscard]] auto materialize_deferred_scan_rows(const DeferredScan& scan, const Selection& rows,
                                                  const ExecutionContext& exec,
                                                  ColumnEntry key_column)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
