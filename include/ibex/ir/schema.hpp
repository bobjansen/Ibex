#pragma once

#include <ibex/ir/node.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ibex::ir {

/// Column scalar type used by the schema-propagation pass. Mirrors
/// `parser::ScalarType`; kept independent so the `ir` layer does not depend on
/// `parser`. Conversion happens at the comparison boundary (e.g. when checking
/// a `DataFrame<Schema>` contract).
enum class ColumnType : std::uint8_t {
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    Date,
    Timestamp,
};

/// One column in a known schema. `type` is `nullopt` when the column is known
/// to exist but its scalar type has not been inferred yet (e.g. an
/// update-derived column before expression type inference exists).
struct SchemaField {
    std::string name;
    std::optional<ColumnType> type;
};

/// Result of schema propagation for a node: either a `Known` ordered list of
/// columns, or `Unknown` (⊥) — the sound default that defeats static checking
/// downstream and falls back to runtime validation.
class SchemaInfo {
   public:
    SchemaInfo() = default;  ///< Unknown.

    [[nodiscard]] static auto unknown() -> SchemaInfo { return SchemaInfo{}; }
    [[nodiscard]] static auto known(std::vector<SchemaField> fields) -> SchemaInfo {
        SchemaInfo info;
        info.known_ = true;
        info.fields_ = std::move(fields);
        return info;
    }

    [[nodiscard]] auto is_known() const noexcept -> bool { return known_; }
    [[nodiscard]] auto fields() const noexcept -> const std::vector<SchemaField>& {
        return fields_;
    }

    /// Returns the field named `name`, or nullptr if absent (or schema Unknown).
    [[nodiscard]] auto find(std::string_view name) const -> const SchemaField*;

   private:
    bool known_ = false;
    std::vector<SchemaField> fields_;
};

/// Maps a source/extern table name to its declared schema. An entry that is
/// absent (or maps to Unknown) leaves the corresponding `ScanNode` /
/// `ExternCallNode` Unknown. Empty by default — every source is Unknown.
using SourceSchemas = std::unordered_map<std::string, SchemaInfo>;

/// Propagate column schemas bottom-up through the IR and return the schema of
/// `node`'s result. Operators not yet modelled return `Unknown`, which is
/// always sound.
[[nodiscard]] auto infer_schema(const Node& node, const SourceSchemas& sources = {}) -> SchemaInfo;

}  // namespace ibex::ir
