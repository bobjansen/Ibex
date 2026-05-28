#pragma once

#include <ibex/ir/node.hpp>

#include <string>
#include <string_view>
#include <unordered_map>
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
