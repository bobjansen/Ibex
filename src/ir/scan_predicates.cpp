// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/column_origins.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/scan_predicates.hpp>
#include <ibex/ir/schema.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::ir {
/// Above this fraction of its own base table, a build side covers too much of
/// the join key's domain for a published Bloom/bounds filter to reject
/// anything worth the apparatus. Calibrated below, not chosen blind.
constexpr double kMaxBuildDomainCoverage = 0.9;
// NOLINTBEGIN(cppcoreguidelines-pro-type-static-cast-downcast) -- every cast in this file
// is guarded by a node.kind() check (or switch) matching the target node type.
namespace {

auto append_conjuncts(const Expr& expr, std::vector<Expr>& out) -> bool {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node);
        logical != nullptr && logical->op == LogicalOp::And && logical->right != nullptr) {
        return append_conjuncts(*logical->left, out) && append_conjuncts(*logical->right, out);
    }
    if (!is_subset_evaluable_expr(expr)) {
        return false;
    }
    out.push_back(expr);
    return true;
}

/// The Scan a filter sits on, when every node between the two preserves rows
/// and lets its predicate be translated to the scan's own names.
auto projected_scan(const Node& node, std::vector<const RenameNode*>* renames = nullptr)
    -> const ScanNode* {
    if (node.children().size() != 1 || node.children().front() == nullptr) {
        return nullptr;
    }
    const Node* child = node.children().front().get();
    while (true) {
        if (child->kind() == NodeKind::Rename) {
            if (child->children().size() != 1 || child->children().front() == nullptr) {
                return nullptr;
            }
            if (renames != nullptr) {
                renames->push_back(static_cast<const RenameNode*>(child));
            }
            child = child->children().front().get();
            continue;
        }
        // A Project subsets columns without renaming, so a predicate's column
        // names still resolve against whatever is beneath it.
        if (child->kind() == NodeKind::Project) {
            if (child->children().size() != 1 || child->children().front() == nullptr) {
                return nullptr;
            }
            child = child->children().front().get();
            continue;
        }
        // A checked Ascribe is a proven identity (see AscribeNode::checked):
        // it asserts shape, never renames, and `check_ascriptions` has already
        // verified it against the real source schema before this pass runs.
        // An unchecked one has no such guarantee (its schema may not be the
        // physical scan's), so it stays opaque, same as any other node kind.
        if (child->kind() == NodeKind::Ascribe &&
            static_cast<const AscribeNode&>(*child).checked()) {
            if (child->children().size() != 1 || child->children().front() == nullptr) {
                return nullptr;
            }
            child = child->children().front().get();
            continue;
        }
        break;
    }
    if (child->kind() != NodeKind::Scan) {
        return nullptr;
    }
    return static_cast<const ScanNode*>(child);
}

auto filter_predicate(const Node& node) -> const Expr* {
    switch (node.kind()) {
        case NodeKind::Filter:
            return &static_cast<const FilterNode&>(node).predicate();
        case NodeKind::FilterHead:
            return &static_cast<const FilterHeadNode&>(node).predicate();
        case NodeKind::FilterTail:
            return &static_cast<const FilterTailNode&>(node).predicate();
        default:
            return nullptr;
    }
}

auto take_unique_child(Node& parent) -> NodePtr {
    auto& children = parent.mutable_children();
    NodePtr child = std::move(children.front());
    children.clear();
    return child;
}

auto is_applied_scan_filter(const Node& node, const std::set<std::string>& applied_sources)
    -> bool {
    const auto* predicate = filter_predicate(node);
    const auto* scan = projected_scan(node);
    if (predicate == nullptr || scan == nullptr || !applied_sources.contains(scan->source_name())) {
        return false;
    }
    std::vector<Expr> conjuncts;
    return append_conjuncts(*predicate, conjuncts);
}

auto remove_filter(NodePtr node) -> NodePtr {
    const auto id = node->id();
    switch (node->kind()) {
        case NodeKind::Filter:
            return take_unique_child(*node);

        case NodeKind::FilterHead: {
            const auto count = static_cast<const FilterHeadNode&>(*node).count();
            NodePtr child = take_unique_child(*node);
            auto head = std::make_unique<HeadNode>(id, count);
            head->add_child(std::move(child));
            return head;
        }

        case NodeKind::FilterTail: {
            const auto count = static_cast<const FilterTailNode&>(*node).count();
            NodePtr child = take_unique_child(*node);
            auto tail = std::make_unique<TailNode>(id, count);
            tail->add_child(std::move(child));
            return tail;
        }

        default:
            return node;
    }
}

auto remove_applied_filters(NodePtr node, const std::set<std::string>& applied_sources) -> NodePtr {
    if (node == nullptr) {
        return node;
    }
    for (auto& child : node->mutable_children()) {
        child = remove_applied_filters(std::move(child), applied_sources);
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            preamble = remove_applied_filters(std::move(preamble), applied_sources);
        }
        auto& main = program.mutable_main_node();
        main = remove_applied_filters(std::move(main), applied_sources);
    }
    if (is_applied_scan_filter(*node, applied_sources)) {
        return remove_filter(std::move(node));
    }
    return node;
}

void visit(const Node& node, std::map<std::string, std::size_t>& scan_counts,
           ScanPredicateMap& candidates) {
    if (node.kind() == NodeKind::Scan) {
        ++scan_counts[static_cast<const ScanNode&>(node).source_name()];
    }

    if (const auto* predicate = filter_predicate(node)) {
        std::vector<const RenameNode*> renames;
        if (const auto* scan = projected_scan(node, &renames); scan != nullptr) {
            Expr scan_predicate = *predicate;
            for (const auto* rename : renames) {
                ColumnNameMap(rename->renames()).remap_expr_to_input(scan_predicate);
            }
            std::vector<Expr> conjuncts;
            if (append_conjuncts(scan_predicate, conjuncts)) {
                auto& destination = candidates[scan->source_name()];
                destination.insert(destination.end(), conjuncts.begin(), conjuncts.end());
            }
        }
    }

    for (const auto& child : node.children()) {
        if (child != nullptr) {
            visit(*child, scan_counts, candidates);
        }
    }
}

void count_scans(const Node& node, const std::set<std::string>& sources,
                 std::map<std::string, std::size_t>& counts) {
    if (node.kind() == NodeKind::Scan) {
        const auto& name = static_cast<const ScanNode&>(node).source_name();
        if (sources.contains(name)) {
            ++counts[name];
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = static_cast<const ProgramNode&>(node);
        for (const auto& preamble : program.preamble()) {
            if (preamble != nullptr) {
                count_scans(*preamble, sources, counts);
            }
        }
        count_scans(program.main_node(), sources, counts);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            count_scans(*child, sources, counts);
        }
    }
}

void rename_scans(NodePtr& node, const std::map<std::string, std::size_t>& counts,
                  std::map<std::string, std::size_t>& next_instance,
                  std::map<std::string, std::string>& instances) {
    if (node == nullptr) {
        return;
    }
    if (node->kind() == NodeKind::Scan) {
        const auto& name = static_cast<const ScanNode&>(*node).source_name();
        if (const auto counted = counts.find(name);
            counted != counts.end() && counted->second > 1) {
            auto instance = name + "#" + std::to_string(++next_instance[name]);
            instances.emplace(instance, name);
            node = std::make_unique<ScanNode>(node->id(), std::move(instance));
        }
        return;
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            rename_scans(preamble, counts, next_instance, instances);
        }
        rename_scans(program.mutable_main_node(), counts, next_instance, instances);
    }
    for (auto& child : node->mutable_children()) {
        rename_scans(child, counts, next_instance, instances);
    }
}

}  // namespace

auto split_scan_instances(NodePtr root, const std::set<std::string>& sources) -> ScanInstanceSplit {
    ScanInstanceSplit split;
    if (root == nullptr || sources.empty()) {
        split.plan = std::move(root);
        return split;
    }
    std::map<std::string, std::size_t> counts;
    count_scans(*root, sources, counts);
    std::map<std::string, std::size_t> next_instance;
    rename_scans(root, counts, next_instance, split.instances);
    split.plan = std::move(root);
    return split;
}

auto scan_predicates(const Node& root) -> ScanPredicateMap {
    std::map<std::string, std::size_t> scan_counts;
    ScanPredicateMap candidates;
    visit(root, scan_counts, candidates);

    for (auto it = candidates.begin(); it != candidates.end();) {
        if (it->second.empty() || scan_counts[it->first] != 1) {
            it = candidates.erase(it);
        } else {
            ++it;
        }
    }
    return candidates;
}

namespace {

void count_scan_occurrences(const Node& node, std::map<std::string, std::size_t>& counts) {
    if (node.kind() == NodeKind::Scan) {
        ++counts[static_cast<const ScanNode&>(node).source_name()];
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            count_scan_occurrences(*child, counts);
        }
    }
}

/// Descend a chain of row-local Project / Rename / Update nodes to a Scan,
/// mapping `key` back through renames and bare-column aliases to the scan's
/// own column name. Any other node kind (a residual Filter, a Window, ...)
/// makes the chain ineligible: removing rows at the scan would not commute
/// with it. A renaming `select` lowers to Project(Update(Scan)) with
/// bare-ColumnRef field exprs, which is why Update belongs here.
auto match_probe_chain(const Node& node, std::string key)
    -> std::optional<std::pair<std::string, std::string>> {
    const Node* cur = &node;
    while (true) {
        switch (cur->kind()) {
            case NodeKind::Scan:
                return std::pair{static_cast<const ScanNode&>(*cur).source_name(), std::move(key)};
            case NodeKind::Project: {
                const auto& cols = static_cast<const ProjectNode&>(*cur).columns();
                const bool keeps_key =
                    std::any_of(cols.begin(), cols.end(),
                                [&](const ColumnRef& col) { return col.name == key; });
                if (!keeps_key) {
                    return std::nullopt;
                }
                break;
            }
            case NodeKind::Rename: {
                const ColumnNameMap names(static_cast<const RenameNode&>(*cur).renames());
                key = names.input_name(key);
                break;
            }
            case NodeKind::Update: {
                const auto& update = static_cast<const UpdateNode&>(*cur);
                if (!update.tuple_fields().empty() || !update.group_by().empty()) {
                    return std::nullopt;
                }
                for (const auto& field : update.fields()) {
                    // Every field must be row-local for row removal at the
                    // scan to commute with the update.
                    if (!is_subset_evaluable_expr(field.expr)) {
                        return std::nullopt;
                    }
                }
                // Fields all read the update's INPUT, so the key maps through
                // at most one alias — never chained within the same node.
                for (const auto& field : update.fields()) {
                    if (field.alias == key) {
                        const auto* ref = std::get_if<ColumnRef>(&field.expr.node);
                        if (ref == nullptr || ref->lexical) {
                            // The key is computed; no scan column to bound.
                            return std::nullopt;
                        }
                        key = ref->name;
                        break;
                    }
                }
                break;
            }
            default:
                return std::nullopt;
        }
        if (cur->children().size() != 1 || cur->children().front() == nullptr) {
            return std::nullopt;
        }
        cur = cur->children().front().get();
    }
}

// Whether the join's build side (LEFT) is worth deferring the probe (RIGHT)
// scan for: the build side must publish key bounds tight enough to actually
// prune the probe scan's decode. Compares the build side's ESTIMATED row
// count (`estimate_cardinality`, which sees through filters) against the
// probe source's own EXACT row count (footer metadata, `row_counts`). An
// unfiltered or otherwise-unshrunk build side yields a bound spanning the
// scan's whole key domain -- pruning nothing while still paying full eager
// materialization of the build side, a pure loss (see
// project_deferred_probe_no_cost_model.md). `row_counts` empty or the
// estimate unavailable means "no information": permissive by default,
// matching every existing caller (see the header doc).
/// How much of its own base table the build side still carries, as a fraction.
///
/// The row-count gate below asks "is the build side smaller than the probe
/// side". That is the wrong question when a table is small *by construction*
/// rather than *by filtering*: q12 joins an unfiltered `orders` (1.5M rows)
/// into `lineitem` (12M), passing the row-count gate three times over, yet
/// every `l_orderkey` references a real `o_orderkey`, so the published Bloom
/// rejects nothing and the whole apparatus is a pure loss. q14 is the same
/// shape with `part`.
///
/// What separates those from q03 is not size but COVERAGE of the join key's
/// value domain. Estimating that directly needs the probe key's distinct
/// count, which this codebase has already been burned by twice (a sampled
/// count regressed q10 5x; the footer span reads far above the truth for
/// `l_orderkey`, whose generator skips values). So do not ask the probe side
/// anything. Ask only about the BUILD side's own table: trace its key back to
/// the base table it structurally comes from, and compare that table's
/// unfiltered row count against the build side's estimated (filtered) rows.
/// A build side still carrying all of its own table covers all of the key
/// domain it can ever contribute, whatever the probe side looks like.
///
///   q12  origin orders, 1.5M of 1.5M   -> 1.00  decline
///   q14  origin part,   400k of 400k   -> 1.00  decline
///   q03  origin orders, filtered       -> well under 1, accept
///
/// A build side's own row count also caps its distinct keys ("no operator
/// invents values"), so this needs no uniqueness proof and cannot be inflated
/// by key skew the way a footer-derived distinct estimate can.
auto build_side_key_domain_ratio(const JoinNode& join, const SourceRowCounts& row_counts,
                                 const SourceSchemas& schemas, std::size_t build_rows)
    -> std::optional<double> {
    const auto origin = column_origin_of(*join.children()[0], join.keys().front().left, schemas);
    if (!origin.has_value()) {
        return std::nullopt;  // computed/derived key: no base table to size
    }
    const auto domain = row_counts.find(origin->source);
    if (domain == row_counts.end() || domain->second == 0) {
        return std::nullopt;
    }
    return static_cast<double>(build_rows) / static_cast<double>(domain->second);
}

auto build_side_worth_deferring(const JoinNode& join, const std::string& probe_source,
                                const SourceRowCounts& row_counts, const SourceSchemas& schemas,
                                const std::map<std::string, double>& absorbed) -> bool {
    if (row_counts.empty()) {
        return true;  // gate disabled: caller supplied no cardinality info
    }
    const auto probe_rows = row_counts.find(probe_source);
    if (probe_rows == row_counts.end()) {
        return true;  // probe source's own size unknown: nothing to compare against
    }
    const CardinalityEstimate build_est =
        estimate_cardinality(*join.children()[0], row_counts, schemas,
                             CardinalityOptions{.absorbed_scan_selectivity = absorbed});
    if (!build_est.rows.has_value()) {
        return true;  // build side unestimable: decline to guess, stay permissive
    }
    // Slack factor: estimates are approximate (heuristic filter selectivity,
    // footer-span distinct counts), so require the build side to be clearly
    // smaller rather than gating on any margin at all.
    if (*build_est.rows * 2 >= probe_rows->second) {
        return false;
    }
    // Cheap check first, so the origin trace is only paid where it can matter.
    const auto ratio = build_side_key_domain_ratio(join, row_counts, schemas, *build_est.rows);
    if (std::getenv("IBEX_PROBE_GATE_DEBUG") != nullptr) {
        const auto origin =
            column_origin_of(*join.children()[0], join.keys().front().left, schemas);
        std::fprintf(stderr, "[probe-gate] probe=%s key=%s build_rows=%zu origin=%s ratio=%s\n",
                     probe_source.c_str(), join.keys().front().left.c_str(), *build_est.rows,
                     origin.has_value() ? origin->source.c_str() : "<none>",
                     ratio.has_value() ? std::to_string(*ratio).c_str() : "<none>");
    }
    return !ratio.has_value() || *ratio < kMaxBuildDomainCoverage;
}

void collect_deferrable(const Node& node, const std::set<std::string>& sources,
                        const std::map<std::string, std::size_t>& counts,
                        const SourceRowCounts& row_counts, const SourceSchemas& schemas,
                        const std::map<std::string, double>& absorbed,
                        std::map<std::string, DeferrableProbeScan>& out) {
    if (node.kind() == NodeKind::Join) {
        const auto& join = static_cast<const JoinNode&>(node);
        // The build side publishes its key values under the LEFT name and the
        // deferred scan is bounded under the RIGHT one. The runtime handles
        // that mapping directly, so a mapped key is as eligible as a
        // same-named one; requiring normalization here turns an otherwise
        // selective mapped join into a whole-table probe scan.
        //
        // A two-key join is also eligible, filtering on its FIRST component
        // only (independent-component pruning: membership in that one column
        // is necessary but not sufficient for the pair match, so it can only
        // reject rows, never wrongly admit one -- the runtime still checks
        // both keys exactly once decoded). Deliberately not both components
        // yet: `DynamicScanFilter`/`DeferredScan` are single-column, and this
        // is the POC for whether scan-altitude pruning is the lever at all
        // before extending them (plans/parallelism-overview.md's "stream
        // multi-key joins" follow-up, TPC-H q09's lineitem join).
        if (join.kind() == JoinKind::Inner &&
            (join.keys().size() == 1 || join.keys().size() == 2) && !join.predicate().has_value() &&
            join.children().size() == 2 && join.children()[0] != nullptr &&
            join.children()[1] != nullptr) {
            if (auto match = match_probe_chain(*join.children()[1], join.keys().front().right);
                match.has_value() && sources.contains(match->first)) {
                if (const auto count = counts.find(match->first);
                    count != counts.end() && count->second == 1 &&
                    build_side_worth_deferring(join, match->first, row_counts, schemas, absorbed)) {
                    out.emplace(match->first,
                                DeferrableProbeScan{.key_column = std::move(match->second)});
                }
            }
        }
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_deferrable(*child, sources, counts, row_counts, schemas, absorbed, out);
        }
    }
}

}  // namespace

auto plan_join_key_origins(const Node& root, const SourceSchemas& sources)
    -> robin_hood::unordered_map<std::string, std::set<std::string>> {
    robin_hood::unordered_map<std::string, std::set<std::string>> out;
    const auto walk = [&](const Node& node, const auto& self) -> void {
        if (node.kind() == NodeKind::Join && node.children().size() >= 2) {
            const auto& join = static_cast<const JoinNode&>(node);
            // Each side's keys name columns of THAT side's output, so each is
            // resolved against that side's origins.
            for (std::size_t side = 0; side < 2; ++side) {
                if (node.children()[side] == nullptr) {
                    continue;
                }
                const ColumnOriginMap origins = column_origins(*node.children()[side], sources);
                for (const auto& key : join.keys()) {
                    const auto& name = side == 0 ? key.left : key.right;
                    if (const auto it = origins.find(name); it != origins.end()) {
                        out[it->second.source].insert(it->second.column);
                    }
                }
            }
        }
        for (const auto& child : node.children()) {
            if (child != nullptr) {
                self(*child, self);
            }
        }
    };
    walk(root, walk);
    return out;
}

auto deferrable_probe_scans(const Node& root, const std::set<std::string>& sources,
                            const SourceRowCounts& row_counts, const SourceSchemas& schemas,
                            const std::map<std::string, double>& absorbed_scan_selectivity)
    -> std::map<std::string, DeferrableProbeScan> {
    std::map<std::string, DeferrableProbeScan> out;
    if (sources.empty()) {
        return out;
    }
    std::map<std::string, std::size_t> counts;
    count_scan_occurrences(root, counts);
    collect_deferrable(root, sources, counts, row_counts, schemas, absorbed_scan_selectivity, out);
    return out;
}

auto remove_applied_scan_filters(NodePtr root, const std::set<std::string>& applied_sources)
    -> NodePtr {
    if (root == nullptr || applied_sources.empty()) {
        return root;
    }

    // Recheck the same proof used for selection pushdown before mutating the
    // plan. This keeps the API safe if its caller overstates what it applied.
    const auto candidates = scan_predicates(*root);
    std::set<std::string> safe_sources;
    for (const auto& source : applied_sources) {
        if (candidates.contains(source)) {
            safe_sources.insert(source);
        }
    }
    if (safe_sources.empty()) {
        return root;
    }

    return remove_applied_filters(std::move(root), safe_sources);
}
// NOLINTEND(cppcoreguidelines-pro-type-static-cast-downcast)

}  // namespace ibex::ir
