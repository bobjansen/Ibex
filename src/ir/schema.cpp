#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace ibex::ir {

auto SchemaInfo::find(std::string_view name) const -> const SchemaField* {
    if (!known_) {
        return nullptr;
    }
    for (const auto& field : fields_) {
        if (field.name == name) {
            return &field;
        }
    }
    return nullptr;
}

namespace {

auto literal_type(const Literal& lit) -> ColumnType {
    return std::visit(
        [](const auto& value) -> ColumnType {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return ColumnType::Int64;
            } else if constexpr (std::is_same_v<T, double>) {
                return ColumnType::Float64;
            } else if constexpr (std::is_same_v<T, bool>) {
                return ColumnType::Bool;
            } else if constexpr (std::is_same_v<T, std::string>) {
                return ColumnType::String;
            } else if constexpr (std::is_same_v<T, Date>) {
                return ColumnType::Date;
            } else {
                return ColumnType::Timestamp;
            }
        },
        lit.value);
}

/// Best-effort type of a computed-field expression given the input schema.
/// Only the cases that are certain are inferred; everything else is nullopt
/// (full expression type inference is a later stage).
auto expr_type(const Expr& expr, const SchemaInfo& input) -> std::optional<ColumnType> {
    if (const auto* col = std::get_if<ColumnRef>(&expr.node)) {
        if (const auto* field = input.find(col->name)) {
            return field->type;
        }
        return std::nullopt;
    }
    if (const auto* lit = std::get_if<Literal>(&expr.node)) {
        return literal_type(*lit);
    }
    return std::nullopt;
}

/// Output type of an aggregate, when known with certainty.
auto agg_result_type(const AggSpec& agg, const SchemaInfo& input) -> std::optional<ColumnType> {
    switch (agg.func) {
        case AggFunc::Count:
            return ColumnType::Int64;
        case AggFunc::Median:
        case AggFunc::Stddev:
        case AggFunc::Ewma:
        case AggFunc::Quantile:
        case AggFunc::Skew:
        case AggFunc::Kurtosis:
            return ColumnType::Float64;
        case AggFunc::Min:
        case AggFunc::Max:
        case AggFunc::First:
        case AggFunc::Last:
            // These preserve the input column's type.
            if (const auto* field = input.find(agg.column.name)) {
                return field->type;
            }
            return std::nullopt;
        case AggFunc::Sum:
        case AggFunc::Mean:
            // Sum may widen; Mean's promotion rules are deferred to expression
            // type inference. Leave the type unresolved but the column known.
            return std::nullopt;
    }
    return std::nullopt;
}

auto child_schema(const Node& node, const SourceSchemas& sources, std::size_t index = 0)
    -> SchemaInfo {
    if (index >= node.children().size()) {
        return SchemaInfo::unknown();
    }
    return infer_schema(*node.children()[index], sources);
}

}  // namespace

auto infer_schema(const Node& node, const SourceSchemas& sources) -> SchemaInfo {
    switch (node.kind()) {
        case NodeKind::Program:
            return infer_schema(static_cast<const ProgramNode&>(node).main_node(), sources);

        case NodeKind::Scan: {
            const auto& scan = static_cast<const ScanNode&>(node);
            if (auto it = sources.find(scan.source_name()); it != sources.end()) {
                return it->second;
            }
            return SchemaInfo::unknown();
        }
        case NodeKind::ExternCall: {
            const auto& call = static_cast<const ExternCallNode&>(node);
            if (auto it = sources.find(call.callee()); it != sources.end()) {
                return it->second;
            }
            return SchemaInfo::unknown();
        }

        // Pure row-shaping operators: schema passes through unchanged.
        case NodeKind::Filter:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::Distinct:
        case NodeKind::AsTimeframe:
        case NodeKind::Window:
            return child_schema(node, sources);

        case NodeKind::Project: {
            // Output is exactly the listed columns; carry types over from the
            // child when it is known. Known even from an Unknown child, since
            // the projection itself fixes the output column set.
            const auto& project = static_cast<const ProjectNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out;
            out.reserve(project.columns().size());
            for (const auto& ref : project.columns()) {
                std::optional<ColumnType> type;
                if (const auto* field = input.find(ref.name)) {
                    type = field->type;
                }
                out.push_back(SchemaField{.name = ref.name, .type = type});
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Rename: {
            const auto& rename = static_cast<const RenameNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            if (!input.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = input.fields();
            for (const auto& spec : rename.renames()) {
                for (auto& field : out) {
                    if (field.name == spec.old_name) {
                        field.name = spec.new_name;
                    }
                }
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Update: {
            // Existing columns are retained, so we must know them: Unknown child
            // -> Unknown. Adds or replaces the listed fields.
            const auto& update = static_cast<const UpdateNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            if (!input.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = input.fields();
            auto upsert = [&out](const std::string& name, std::optional<ColumnType> type) {
                for (auto& field : out) {
                    if (field.name == name) {
                        field.type = type;
                        return;
                    }
                }
                out.push_back(SchemaField{.name = name, .type = type});
            };
            for (const auto& field : update.fields()) {
                upsert(field.alias, expr_type(field.expr, input));
            }
            for (const auto& tuple : update.tuple_fields()) {
                for (const auto& alias : tuple.aliases) {
                    upsert(alias, std::nullopt);
                }
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Aggregate: {
            // Output: group-by key columns followed by one column per aggregate.
            // Known even from an Unknown child (the output column set is fixed),
            // though key/aggregate types may be unresolved.
            const auto& agg = static_cast<const AggregateNode&>(node);
            const SchemaInfo input = child_schema(node, sources);
            std::vector<SchemaField> out;
            out.reserve(agg.group_by().size() + agg.aggregations().size());
            for (const auto& key : agg.group_by()) {
                std::optional<ColumnType> type;
                if (const auto* field = input.find(key.name)) {
                    type = field->type;
                }
                out.push_back(SchemaField{.name = key.name, .type = type});
            }
            for (const auto& spec : agg.aggregations()) {
                out.push_back(
                    SchemaField{.name = spec.alias, .type = agg_result_type(spec, input)});
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Join: {
            // A ∪ B: left columns, then right columns whose names are not
            // already present (shared join keys appear once).
            const SchemaInfo left = child_schema(node, sources, 0);
            const SchemaInfo right = child_schema(node, sources, 1);
            if (!left.is_known() || !right.is_known()) {
                return SchemaInfo::unknown();
            }
            std::vector<SchemaField> out = left.fields();
            for (const auto& field : right.fields()) {
                const bool present = std::any_of(out.begin(), out.end(), [&](const SchemaField& f) {
                    return f.name == field.name;
                });
                if (!present) {
                    out.push_back(field);
                }
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Construct: {
            const auto& construct = static_cast<const ConstructNode&>(node);
            std::vector<SchemaField> out;
            out.reserve(construct.columns().size());
            for (const auto& col : construct.columns()) {
                std::optional<ColumnType> type;
                if (!col.elements.empty()) {
                    type = literal_type(col.elements.front());
                } else if (col.expr_node != nullptr) {
                    const SchemaInfo sub = infer_schema(*col.expr_node, sources);
                    if (sub.is_known() && sub.fields().size() == 1) {
                        type = sub.fields().front().type;
                    }
                }
                out.push_back(SchemaField{.name = col.name, .type = type});
            }
            return SchemaInfo::known(std::move(out));
        }

        case NodeKind::Ascribe:
            // The ascription fixes the result schema (validated at run time).
            return SchemaInfo::known(static_cast<const AscribeNode&>(node).schema());

        case NodeKind::Columns:
            // Exposes child column names as a single String column named "name".
            return SchemaInfo::known({SchemaField{.name = "name", .type = ColumnType::String}});

        // Data-dependent output columns or not yet modelled: Unknown is sound.
        case NodeKind::Resample:
        case NodeKind::Melt:
        case NodeKind::Dcast:
        case NodeKind::Cov:
        case NodeKind::Corr:
        case NodeKind::Transpose:
        case NodeKind::Matmul:
        case NodeKind::Model:
        case NodeKind::Stream:
        case NodeKind::FilterProject:
        case NodeKind::FilterUpdateProject:
        case NodeKind::FilterHead:
        case NodeKind::FilterTail:
        case NodeKind::TopK:
            return SchemaInfo::unknown();
    }
    return SchemaInfo::unknown();
}

}  // namespace ibex::ir
