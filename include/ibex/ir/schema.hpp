#pragma once

#include <ibex/ir/node.hpp>

#include <expected>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::ir {

// `ColumnType` and `SchemaField` are defined in node.hpp so that IR nodes
// (e.g. AscribeNode) can carry a schema without depending on this header.

/// Result of schema propagation for a node: either a `Known` ordered list of
/// columns, or `Unknown` (⊥) — the sound default that defeats static checking
/// downstream and falls back to runtime validation.
class SchemaInfo {
   public:
    SchemaInfo() = default;  ///< Unknown.

    [[nodiscard]] static auto unknown() -> SchemaInfo { return SchemaInfo{}; }
    [[nodiscard]] static auto known(std::vector<SchemaField> fields, bool open = false,
                                    std::optional<std::string> time_index = std::nullopt)
        -> SchemaInfo {
        SchemaInfo info;
        info.known_ = true;
        info.open_ = open;
        info.fields_ = std::move(fields);
        info.time_index_ = std::move(time_index);
        return info;
    }

    [[nodiscard]] auto is_known() const noexcept -> bool { return known_; }
    /// True for an "open" schema — the listed columns are present, but extra
    /// columns may also exist (a `*` wildcard). Missing-column checks are only
    /// sound on a closed (non-open) Known schema.
    [[nodiscard]] auto is_open() const noexcept -> bool { return open_; }
    [[nodiscard]] auto fields() const noexcept -> const std::vector<SchemaField>& {
        return fields_;
    }
    /// Name of the designated time-index column for a TimeFrame, if any. A
    /// `DataFrame` has none; `as_timeframe` and `resample` set it.
    [[nodiscard]] auto time_index() const noexcept -> const std::optional<std::string>& {
        return time_index_;
    }

    /// Returns the field named `name`, or nullptr if absent (or schema Unknown).
    [[nodiscard]] auto find(std::string_view name) const -> const SchemaField*;

   private:
    bool known_ = false;
    bool open_ = false;
    std::vector<SchemaField> fields_;
    std::optional<std::string> time_index_;
};

/// Maps a source/extern table name to its declared schema. An entry that is
/// absent (or maps to Unknown) leaves the corresponding `ScanNode` /
/// `ExternCallNode` Unknown. Empty by default — every source is Unknown.
using SourceSchemas = robin_hood::unordered_map<std::string, SchemaInfo>;

/// Propagate column schemas bottom-up through the IR and return the schema of
/// `node`'s result. Operators not yet modelled return `Unknown`, which is
/// always sound.
[[nodiscard]] auto infer_schema(const Node& node, const SourceSchemas& sources = {}) -> SchemaInfo;

/// Key identifying one reader *call site* in a `SourceSchemas`, e.g.
/// `read_parquet("data/lineitem.parquet")`.
///
/// A declared reader return schema is keyed by the bare callee, which assumes
/// one schema per function. That is false for a generic reader: `read_parquet`
/// yields a different schema for every path. So a caller that knows what a
/// specific call site returns -- the driver, which can read the footer -- keys
/// it by this instead, and `infer_schema` prefers it over the bare callee.
///
/// Returns nullopt when any argument is not a literal, since then the call site
/// is not identifiable without evaluating it.
[[nodiscard]] auto extern_call_site_key(const std::string& callee, const std::vector<Expr>& args)
    -> std::optional<std::string>;

/// Prove every ascription in `root` that sits over a statically known input
/// schema, marking it checked (see `AscribeNode::checked`). Returns the first
/// ascription that provably fails -- a fatal user error, and one worth raising
/// before a single page is decoded.
///
/// Ascriptions over an input whose schema is unknown or open are left alone for
/// the interpreter to check against real data.
[[nodiscard]] auto check_ascriptions(Node& root, const SourceSchemas& sources)
    -> std::expected<void, std::string>;

/// Validate the column references in `node` (and its subtree) against the
/// statically inferred input schemas, where the input schema is `Known`.
///
/// The unambiguously column-only positions — `select`/`order`/`rename` targets,
/// `by` group keys, and aggregate source columns — are always checked: a name
/// there can only be a column.
///
/// When `check_expressions` is true, `filter` predicates and computed
/// `select`/`update` expressions are also checked. A bare name in those
/// positions resolves to a column *or* a lexical binding, so `lexical_names`
/// must list every in-scope binding name; a reference is only flagged when it is
/// in neither the input schema nor `lexical_names`. Pass `check_expressions`
/// only when `lexical_names` is complete (e.g. the REPL's bindings); a superset
/// is safe (it can only under-report), an incomplete set is not.
///
/// Returns an error message for the first provably-absent reference, or
/// `nullopt`.
[[nodiscard]] auto check_column_refs(
    const Node& node, const SourceSchemas& sources = {},
    const robin_hood::unordered_set<std::string>& lexical_names = {},
    bool check_expressions = false) -> std::optional<std::string>;

}  // namespace ibex::ir
