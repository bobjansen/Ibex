// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/count_distinct_reduction.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace ibex::ir {
namespace {

void collect_max_id(const Node& node, std::uint64_t& max_id) {
    max_id = std::max(max_id, node.id().value);
    for (const auto& child : node.children()) {
        if (child) {
            collect_max_id(*child, max_id);
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& prog = node_cast<ProgramNode>(node);
        for (const auto& pre : prog.preamble()) {
            if (pre) {
                collect_max_id(*pre, max_id);
            }
        }
        collect_max_id(prog.main_node(), max_id);
    }
}

/// `node` is an Aggregate; return its fused replacement, or `node` unchanged.
auto try_fuse(NodePtr node, std::uint64_t& next_id, const SourceSchemas& sources) -> NodePtr {
    const auto& agg = node_cast<AggregateNode>(*node);
    if (agg.aggregations().size() != 1 || agg.children().size() != 1) {
        return node;
    }
    const AggSpec& spec = agg.aggregations().front();
    if (spec.func != AggFunc::Count || !spec.column.name.empty()) {
        return node;
    }
    Node& distinct = *node->mutable_children().front();
    if (distinct.kind() != NodeKind::Distinct || distinct.children().size() != 1) {
        return node;
    }
    Node& project = *distinct.mutable_children().front();
    if (project.kind() != NodeKind::Project) {
        return node;
    }
    const auto& keys = agg.group_by();
    const auto& cols = node_cast<ProjectNode>(project).columns();
    if (cols.size() != keys.size() + 1) {
        return node;
    }
    // Every projected column is a group key except exactly one -- the value the
    // count is over. A group key that is not projected would leave `cols` a row
    // short, so the size check plus "one non-key" is sufficient given the keys
    // are themselves distinct (the lowerer never repeats a `by` key).
    std::optional<std::string> value_column;
    for (const auto& col : cols) {
        const bool is_key =
            std::ranges::any_of(keys, [&](const ColumnRef& k) { return k.name == col.name; });
        if (is_key) {
            continue;
        }
        if (value_column.has_value()) {
            return node;
        }
        value_column = col.name;
    }
    if (!value_column.has_value()) {
        return node;
    }

    // `t[distinct { K, v }][count() by K]` keeps a null v as its own distinct
    // pair, so its count includes it; `count_distinct(v)` excludes nulls, the
    // SQL convention. The two therefore only agree when v cannot be null. An
    // unprovable schema declines rather than risk a silent result change.
    const SchemaInfo schema = infer_schema(project, sources);
    const auto* field = schema.find(*value_column);
    if (field == nullptr || !field->non_null()) {
        return node;
    }

    std::vector<AggSpec> fused{AggSpec{.func = AggFunc::CountDistinct,
                                      .column = ColumnRef{.name = std::move(*value_column)},
                                      .alias = spec.alias,
                                      .param = 0.0}};
    auto fused_agg = std::make_unique<AggregateNode>(NodeId{next_id++},
                                                     std::vector<ColumnRef>(keys), std::move(fused));
    fused_agg->add_child(std::move(distinct.mutable_children().front()));
    return fused_agg;
}

auto walk(NodePtr node, std::uint64_t& next_id, const SourceSchemas& sources) -> NodePtr {
    if (!node) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = walk(std::move(child), next_id, sources);
    }
    if (node->kind() == NodeKind::Program) {
        auto& prog = node_cast<ProgramNode>(*node);
        for (auto& pre : prog.mutable_preamble()) {
            pre = walk(std::move(pre), next_id, sources);
        }
        auto& main = prog.mutable_main_node();
        if (main) {
            main = walk(std::move(main), next_id, sources);
        }
    }
    if (node->kind() == NodeKind::Aggregate) {
        return try_fuse(std::move(node), next_id, sources);
    }
    return node;
}

}  // namespace

auto fuse_distinct_count_to_count_distinct(NodePtr root, const SourceSchemas& sources) -> NodePtr {
    if (!root) {
        return root;
    }
    std::uint64_t next_id = 0;
    collect_max_id(*root, next_id);
    ++next_id;
    return walk(std::move(root), next_id, sources);
}

}  // namespace ibex::ir
