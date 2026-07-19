#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
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
    const std::string&, const DynamicScanFilter&)>;

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

    /// Column names and types, with no rows. Cheap: known from metadata alone.
    [[nodiscard]] auto schema() const noexcept -> const Table& { return schema_; }
    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_; }
    /// Per-column source metadata, for the planner. Read from the file's footer
    /// at bind time, so consulting it decodes nothing.
    [[nodiscard]] auto column_stats() const noexcept -> const SourceColumnStats& { return stats_; }

    /// Materialize `names`, in schema order. Names not in the schema are
    /// ignored, so a caller may pass the union of the columns demanded across
    /// several sources without first splitting it per source.
    [[nodiscard]] auto project(const std::set<std::string>& names)
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
    [[nodiscard]] auto project_where(const std::set<std::string>& names,
                                     const std::vector<ir::Expr>& conjuncts,
                                     const ScalarRegistry* scalars = nullptr,
                                     const DynamicScanFilter* dynamic = nullptr,
                                     const std::string* dynamic_key = nullptr)
        -> std::expected<Table, std::string>;

    /// Materialize `names` through an explicit ascending row selection —
    /// late materialization for a caller (the deferred-probe join) that
    /// already knows exactly which rows survive. Bypasses `cache_` like
    /// `project_where` does, and for the same reason.
    [[nodiscard]] auto project_rows(const std::set<std::string>& names, const Selection& selected)
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
                                          const ScalarRegistry* scalars,
                                          const DynamicScanFilter& dynamic,
                                          const std::string& key_name)
        -> std::expected<std::optional<JoinKeySelection>, std::string>;

    /// Materialize every column — the fallback for anything that consumes the
    /// table whole rather than through a query plan.
    [[nodiscard]] auto materialize() -> std::expected<Table, std::string>;

   private:
    /// Decode the referenced columns whole-file into `cache_` (they are
    /// legitimate whole-column entries) and return them as a table.
    [[nodiscard]] auto decode_whole_columns(
        const robin_hood::unordered_set<std::string>& referenced)
        -> std::expected<Table, std::string>;

    Table schema_;
    std::size_t rows_ = 0;
    ColumnDecodeFn decode_;
    SourceColumnStats stats_;
    KeyFilterScanFn key_filter_scan_;
    robin_hood::unordered_map<std::string, ColumnEntry> cache_;
};

using LazyTablePtr = std::shared_ptr<LazyTable>;

/// Phase A of the two-phase deferred probe: the scan's selection (static
/// conjuncts + membership, or the fused key scan) plus the key values for
/// exactly those rows. nullopt = no selective answer; fall back to
/// `materialize_deferred_scan`.
[[nodiscard]] auto deferred_scan_key_selection(const DeferredScan& scan)
    -> std::expected<std::optional<LazyTable::JoinKeySelection>, std::string>;

/// Phase B: materialize the scan's demanded columns through the survivor
/// selection, splicing in `key_column` (the survivors' key values, already
/// in hand from phase A) at its schema position so column order matches the
/// ordinary path.
[[nodiscard]] auto materialize_deferred_scan_rows(const DeferredScan& scan, const Selection& rows,
                                                  ColumnEntry key_column)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
