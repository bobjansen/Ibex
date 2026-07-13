#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <cstddef>
#include <expected>
#include <functional>
#include <memory>
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
    /// types; `rows` is its true row count.
    LazyTable(Table schema, std::size_t rows, ColumnDecodeFn decode);

    /// Column names and types, with no rows. Cheap: known from metadata alone.
    [[nodiscard]] auto schema() const noexcept -> const Table& { return schema_; }
    [[nodiscard]] auto rows() const noexcept -> std::size_t { return rows_; }

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
    [[nodiscard]] auto project_where(const std::set<std::string>& names,
                                     const std::vector<ir::Expr>& conjuncts,
                                     const ScalarRegistry* scalars = nullptr)
        -> std::expected<Table, std::string>;

    /// Materialize every column — the fallback for anything that consumes the
    /// table whole rather than through a query plan.
    [[nodiscard]] auto materialize() -> std::expected<Table, std::string>;

   private:
    Table schema_;
    std::size_t rows_ = 0;
    ColumnDecodeFn decode_;
    robin_hood::unordered_map<std::string, ColumnEntry> cache_;
};

using LazyTablePtr = std::shared_ptr<LazyTable>;

}  // namespace ibex::runtime
