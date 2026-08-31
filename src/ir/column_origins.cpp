// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/column_origins.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
namespace {

/// Origins of `node`'s only (or `index`-th) child.
auto child_origins(const Node& node, const SourceSchemas& sources, std::size_t index = 0)
    -> ColumnOriginMap {
    if (node.children().size() <= index || node.children()[index] == nullptr) {
        return {};
    }
    return column_origins(*node.children()[index], sources);
}

/// The origin an expression carries, which is one only when the expression IS a
/// column — anything computed is a new value, whatever it was computed from.
auto origin_of_expr(const Expr& expr, const ColumnOriginMap& input) -> std::optional<ColumnOrigin> {
    const auto* ref = as_column_ref(expr);
    if (ref == nullptr) {
        return std::nullopt;
    }
    const auto it = input.find(ref->name);
    if (it == input.end()) {
        return std::nullopt;
    }
    return it->second;
}

/// Keep only the named columns, in the order given. A projection neither
/// renames nor computes, so a kept column keeps its origin unchanged.
auto project_origins(const std::vector<ColumnRef>& columns, const ColumnOriginMap& input)
    -> ColumnOriginMap {
    ColumnOriginMap out;
    for (const auto& ref : columns) {
        if (const auto it = input.find(ref.name); it != input.end()) {
            out.emplace(ref.name, it->second);
        }
    }
    return out;
}

/// An update rewrites or adds the named fields. A field whose expression is a
/// bare column reference is a copy, and a copy of a base column still IS that
/// base column, so it keeps the origin; anything else drops it.
auto update_origins(const std::vector<FieldSpec>& fields,
                    const std::vector<TupleFieldSpec>& tuple_fields, ColumnOriginMap input)
    -> ColumnOriginMap {
    for (const auto& field : fields) {
        if (auto origin = origin_of_expr(field.expr, input)) {
            input.insert_or_assign(field.alias, std::move(*origin));
        } else {
            input.erase(field.alias);
        }
    }
    // A tuple field's values come from a sub-plan, not from this input.
    for (const auto& tuple : tuple_fields) {
        for (const auto& alias : tuple.aliases) {
            input.erase(alias);
        }
    }
    return input;
}

auto join_origins(const JoinNode& join, const SourceSchemas& sources) -> ColumnOriginMap {
    const SchemaInfo left_schema = infer_schema(*join.children()[0], sources);
    const SchemaInfo right_schema = infer_schema(*join.children()[1], sources);
    if (!left_schema.is_known() || !right_schema.is_known()) {
        return {};
    }
    std::vector<std::string_view> left_names;
    std::vector<std::string_view> right_names;
    left_names.reserve(left_schema.fields().size());
    right_names.reserve(right_schema.fields().size());
    for (const auto& field : left_schema.fields()) {
        left_names.emplace_back(field.name);
    }
    for (const auto& field : right_schema.fields()) {
        right_names.emplace_back(field.name);
    }
    // `plan_join_output` is the single authority on the output column list and
    // its collision renaming. Matching output names against the join keys to
    // rediscover the same thing is exactly the drift `JoinOutputColumn` was
    // introduced to stop.
    const auto plan =
        plan_join_output(join.kind(), join.keys(), left_names, right_names, join.suffix());
    if (!plan.has_value()) {
        return {};
    }

    const ColumnOriginMap left_origins = column_origins(*join.children()[0], sources);
    const ColumnOriginMap right_origins = column_origins(*join.children()[1], sources);

    ColumnOriginMap out;
    for (const auto& column : plan.value()) {
        const auto& side_schema = column.side == JoinOutputSide::Left ? left_schema : right_schema;
        const auto& side_origins =
            column.side == JoinOutputSide::Left ? left_origins : right_origins;
        if (column.source_index >= side_schema.fields().size()) {
            continue;
        }
        // A folded same-name key draws its value from EITHER side depending on
        // which row matched, so it has no single origin — the two sides are
        // equal on matched rows but an outer row takes the other side's value.
        if (column.folded_peer_index.has_value() && join.kind() != JoinKind::Inner) {
            continue;
        }
        const auto& native = side_schema.fields()[column.source_index].name;
        if (const auto it = side_origins.find(native); it != side_origins.end()) {
            out.emplace(column.name, it->second);
        }
    }
    return out;
}

}  // namespace

// NOLINTNEXTLINE(misc-no-recursion) -- mirrors the plan, like infer_schema.
auto column_origins(const Node& node, const SourceSchemas& sources) -> ColumnOriginMap {
    switch (node.kind()) {
        case NodeKind::Program:
            return column_origins(node_cast<ProgramNode>(node).main_node(), sources);

        case NodeKind::Scan: {
            // The seed: a scan's columns ARE its source's columns.
            const auto& scan = node_cast<ScanNode>(node);
            const auto it = sources.find(scan.source_name());
            if (it == sources.end() || !it->second.is_known()) {
                return {};
            }
            ColumnOriginMap out;
            for (const auto& field : it->second.fields()) {
                out.emplace(field.name, ColumnOrigin{.scan = scan.id(),
                                                     .source = scan.source_name(),
                                                     .column = field.name});
            }
            return out;
        }

        // Row-shaping only: they drop or reorder rows and touch no value.
        case NodeKind::Filter:
        case NodeKind::Order:
        case NodeKind::Head:
        case NodeKind::Tail:
        case NodeKind::Distinct:
        case NodeKind::Ascribe:
        case NodeKind::AsTimeframe:
            return child_origins(node, sources);

        case NodeKind::Project:
            return project_origins(node_cast<ProjectNode>(node).columns(),
                                   child_origins(node, sources));

        case NodeKind::Rename: {
            const auto& rename = node_cast<RenameNode>(node);
            const ColumnNameMap names(rename.renames());
            const ColumnOriginMap input = child_origins(node, sources);
            ColumnOriginMap out;
            out.reserve(input.size());
            for (const auto& [name, origin] : input) {
                out.insert_or_assign(std::string(names.output_name(name)), origin);
            }
            return out;
        }

        case NodeKind::Update: {
            const auto& update = node_cast<UpdateNode>(node);
            return update_origins(update.fields(), update.tuple_fields(),
                                  child_origins(node, sources));
        }

        case NodeKind::Aggregate: {
            // A group key column holds one of the values that grouped, so it is
            // still that base column. The aggregates are computed and are not.
            const auto& agg = node_cast<AggregateNode>(node);
            const ColumnOriginMap input = child_origins(node, sources);
            ColumnOriginMap out;
            for (const auto& key : agg.group_by()) {
                if (const auto it = input.find(key.name); it != input.end()) {
                    out.emplace(key.name, it->second);
                }
            }
            return out;
        }

        case NodeKind::Join: {
            const auto& join = node_cast<JoinNode>(node);
            if (join.children().size() < 2 || join.children()[0] == nullptr ||
                join.children()[1] == nullptr) {
                return {};
            }
            return join_origins(join, sources);
        }

        // Everything else — window, resample, melt, dcast, matmul, extern calls
        // and the rest — either computes its output or reshapes it in a way this
        // does not model. No origin is always the sound answer: a pass that gets
        // none simply does not fire.
        default:
            return {};
    }
}

auto column_origin_of(const Node& node, const std::string& column, const SourceSchemas& sources)
    -> std::optional<ColumnOrigin> {
    const ColumnOriginMap origins = column_origins(node, sources);
    const auto it = origins.find(column);
    if (it == origins.end()) {
        return std::nullopt;
    }
    return it->second;
}

}  // namespace ibex::ir
