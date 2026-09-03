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

#include "ibex/format.hpp"

namespace ibex::ir {
/// Above this fraction of its own base table, a build side covers too much of
/// the join key's domain for a published Bloom/bounds filter to reject
/// anything worth the apparatus. Calibrated below, not chosen blind.
constexpr double kMaxBuildDomainCoverage = 0.9;
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
                renames->push_back(node_cast<RenameNode>(child));
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
        if (child->kind() == NodeKind::Ascribe && node_cast<AscribeNode>(*child).checked()) {
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
    return node_cast<ScanNode>(child);
}

auto filter_predicate(const Node& node) -> const Expr* {
    switch (node.kind()) {
        case NodeKind::Filter:
            return &node_cast<FilterNode>(node).predicate();
        case NodeKind::FilterHead:
            return &node_cast<FilterHeadNode>(node).predicate();
        case NodeKind::FilterTail:
            return &node_cast<FilterTailNode>(node).predicate();
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
            const auto count = node_cast<FilterHeadNode>(*node).count();
            NodePtr child = take_unique_child(*node);
            auto head = std::make_unique<HeadNode>(id, count);
            head->add_child(std::move(child));
            return head;
        }

        case NodeKind::FilterTail: {
            const auto count = node_cast<FilterTailNode>(*node).count();
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
        auto& program = node_cast<ProgramNode>(*node);
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

/// Collect each scan OCCURRENCE and the conjuncts sitting over it.
///
/// Keyed by the scan's own `NodeId` rather than its source name, so two
/// occurrences of one source stay apart. Unfiltered occurrences are recorded
/// too: an occurrence that wants the whole table is exactly the one a pushed
/// filter would corrupt, so the count has to see it.
void visit(const Node& node, std::map<NodeId, std::string>& occurrences,
           std::map<NodeId, std::vector<Expr>>& by_scan) {
    if (node.kind() == NodeKind::Scan) {
        occurrences.emplace(node.id(), node_cast<ScanNode>(node).source_name());
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
                auto& destination = by_scan[scan->id()];
                destination.insert(destination.end(), conjuncts.begin(), conjuncts.end());
            }
        }
    }

    for (const auto& child : node.children()) {
        if (child != nullptr) {
            visit(*child, occurrences, by_scan);
        }
    }
}

}  // namespace

auto scan_predicates_by_occurrence(const Node& root) -> std::vector<ScanOccurrence> {
    std::map<NodeId, std::string> occurrences;
    std::map<NodeId, std::vector<Expr>> by_scan;
    visit(root, occurrences, by_scan);

    std::vector<ScanOccurrence> out;
    out.reserve(occurrences.size());
    for (auto& [scan, source] : occurrences) {
        auto conjuncts = by_scan.find(scan);
        out.push_back(ScanOccurrence{
            .scan = scan,
            .source = source,
            .conjuncts = conjuncts == by_scan.end() ? std::vector<Expr>{} : conjuncts->second});
    }
    return out;  // `occurrences` is ordered by scan id, so `out` is too
}

auto scan_predicates(const Node& root) -> ScanPredicateMap {
    // Derived from the per-occurrence answer, and deliberately no more
    // permissive than before: a source is pushed only when it has exactly ONE
    // occurrence, because the table registry is keyed by source name and cannot
    // give two occurrences different rows. Widening this is Phase 3 of
    // plans/per-occurrence-scan-selections-plan.md, and needs the registry
    // change first -- lifting it alone is UNSOUND, not merely risky: on a
    // `like` / `!like` pair both conjuncts land in one decode, intersect to
    // nothing, and the query returns zero rows.
    auto occurrences = scan_predicates_by_occurrence(root);
    std::map<std::string, std::size_t> counts;
    for (const auto& occurrence : occurrences) {
        ++counts[occurrence.source];
    }

    ScanPredicateMap candidates;
    for (auto& occurrence : occurrences) {
        if (occurrence.conjuncts.empty() || counts[occurrence.source] != 1) {
            continue;
        }
        candidates.emplace(occurrence.source, std::move(occurrence.conjuncts));
    }
    return candidates;
}

namespace {

void count_scan_occurrences(const Node& node, std::map<std::string, std::size_t>& counts) {
    if (node.kind() == NodeKind::Scan) {
        ++counts[node_cast<ScanNode>(node).source_name()];
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = node_cast<ProgramNode>(node);
        for (const auto& preamble : program.preamble()) {
            if (preamble != nullptr) {
                count_scan_occurrences(*preamble, counts);
            }
        }
        count_scan_occurrences(program.main_node(), counts);
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
                return std::pair{node_cast<ScanNode>(*cur).source_name(), std::move(key)};
            case NodeKind::Project: {
                const auto& cols = node_cast<ProjectNode>(*cur).columns();
                const bool keeps_key =
                    std::any_of(cols.begin(), cols.end(),
                                [&](const ColumnRef& col) { return col.name == key; });
                if (!keeps_key) {
                    return std::nullopt;
                }
                break;
            }
            case NodeKind::Rename: {
                const ColumnNameMap names(node_cast<RenameNode>(*cur).renames());
                key = names.input_name(key);
                break;
            }
            case NodeKind::Update: {
                const auto& update = node_cast<UpdateNode>(*cur);
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

        ibex::formatting::print(
            stderr, "[probe-gate] probe={} key={} build_rows={} origin={} ratio={}\n",
            probe_source.c_str(), join.keys().front().left.c_str(), *build_est.rows,
            origin.has_value() ? origin->source.c_str() : "<none>",
            ratio.has_value() ? std::to_string(*ratio).c_str() : "<none>");
    }
    return !ratio.has_value() || *ratio < kMaxBuildDomainCoverage;
}

/// The inner-join / key-arity / no-predicate shape a deferrable probe needs.
auto is_probe_shaped_join(const JoinNode& join) -> bool {
    return join.kind() == JoinKind::Inner &&
           (join.keys().size() == 1 || join.keys().size() == 2) && !join.predicate().has_value() &&
           join.children().size() == 2 && join.children()[0] != nullptr &&
           join.children()[1] != nullptr;
}

/// Replace the single `Scan` at the bottom of a verified probe chain (only
/// row-local Project / Rename / Update above it) with one named `instance`.
void rename_chain_scan(NodePtr& node, const std::string& instance) {
    if (node->kind() == NodeKind::Scan) {
        node = std::make_unique<ScanNode>(node->id(), instance);
        return;
    }
    rename_chain_scan(node->mutable_children().front(), instance);
}

/// Is `expr` a `like(col, "literal")`, possibly negated? The column name if so.
///
/// Deliberately narrow. Splitting a source's occurrences is only worth its cost
/// when pushdown buys something a downstream filter cannot: a `like` over a
/// column the query reads from nowhere else is answered inside the page decoder
/// and the string column is never built at all. Every other predicate is merely
/// evaluated EARLIER by pushing, which is not worth a per-occurrence gather --
/// measured: splitting on q21's `l_receiptdate > l_commitdate` costs +10% there,
/// because each occurrence then materializes its own 48M-row gather instead of
/// sharing one table that downstream filters stream over.
// NOLINTNEXTLINE(misc-no-recursion)
auto like_predicate_column(const Expr& expr) -> const std::string* {
    if (const auto* logical = std::get_if<LogicalExpr>(&expr.node)) {
        if (logical->op == LogicalOp::Not && logical->left != nullptr) {
            return like_predicate_column(*logical->left);
        }
        return nullptr;
    }
    const auto* call = std::get_if<CallExpr>(&expr.node);
    if (call == nullptr || call->callee != "like" || call->args.size() != 2 ||
        !call->named_args.empty() || call->args[0] == nullptr || call->args[1] == nullptr) {
        return nullptr;
    }
    if (std::get_if<Literal>(&call->args[1]->node) == nullptr) {
        return nullptr;
    }
    const auto* column = as_column_ref(*call->args[0]);
    return column == nullptr ? nullptr : &column->name;
}

/// Sources with an occurrence whose filter is a fusable `like`.
// NOLINTNEXTLINE(misc-no-recursion)
void collect_filtered_sources(const Node& node, std::set<std::string>& out) {
    if (const auto* predicate = filter_predicate(node)) {
        std::vector<const RenameNode*> renames;
        if (const auto* scan = projected_scan(node, &renames); scan != nullptr) {
            Expr scan_predicate = *predicate;
            for (const auto* rename : renames) {
                ColumnNameMap(rename->renames()).remap_expr_to_input(scan_predicate);
            }
            std::vector<Expr> conjuncts;
            if (append_conjuncts(scan_predicate, conjuncts)) {
                for (const auto& conjunct : conjuncts) {
                    if (like_predicate_column(conjunct) != nullptr) {
                        out.insert(scan->source_name());
                        break;
                    }
                }
            }
        }
    }
    if (node.kind() == NodeKind::Program) {
        const auto& program = node_cast<ProgramNode>(node);
        for (const auto& preamble : program.preamble()) {
            if (preamble != nullptr) {
                collect_filtered_sources(*preamble, out);
            }
        }
        collect_filtered_sources(program.main_node(), out);
    }
    for (const auto& child : node.children()) {
        if (child != nullptr) {
            collect_filtered_sources(*child, out);
        }
    }
}

/// Rename every occurrence of a source in `split_these` to its own instance.
// NOLINTNEXTLINE(misc-no-recursion)
void isolate_filtered(NodePtr& node, const std::set<std::string>& split_these,
                      std::map<std::string, std::size_t>& next_instance,
                      std::map<std::string, std::string>& instances) {
    if (node->kind() == NodeKind::Scan) {
        const auto& source = node_cast<ScanNode>(*node).source_name();
        if (split_these.contains(source)) {
            auto instance = source + "#f" + std::to_string(++next_instance[source]);
            instances.emplace(instance, source);
            node = std::make_unique<ScanNode>(node->id(), instance);
        }
        return;
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = node_cast<ProgramNode>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            if (preamble != nullptr) {
                isolate_filtered(preamble, split_these, next_instance, instances);
            }
        }
        isolate_filtered(program.mutable_main_node(), split_these, next_instance, instances);
    }
    for (auto& child : node->mutable_children()) {
        if (child != nullptr) {
            isolate_filtered(child, split_these, next_instance, instances);
        }
    }
}

void isolate_probes(NodePtr& node, const std::set<std::string>& sources,
                    const std::map<std::string, std::size_t>& counts,
                    std::map<std::string, std::size_t>& next_instance,
                    std::map<std::string, std::string>& instances) {
    if (node->kind() == NodeKind::Join) {
        auto& join = node_cast<JoinNode>(*node);
        if (is_probe_shaped_join(join)) {
            if (auto match = match_probe_chain(*join.children()[1], join.keys().front().right);
                match.has_value() && sources.contains(match->first)) {
                if (const auto c = counts.find(match->first);
                    c != counts.end() && c->second > 1) {
                    auto instance = match->first + "#" + std::to_string(++next_instance[match->first]);
                    instances.emplace(instance, match->first);
                    rename_chain_scan(join.mutable_children()[1], instance);
                }
            }
        }
    }
    if (node->kind() == NodeKind::Program) {
        auto& program = node_cast<ProgramNode>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            if (preamble != nullptr) {
                isolate_probes(preamble, sources, counts, next_instance, instances);
            }
        }
        isolate_probes(program.mutable_main_node(), sources, counts, next_instance, instances);
    }
    for (auto& child : node->mutable_children()) {
        if (child != nullptr) {
            isolate_probes(child, sources, counts, next_instance, instances);
        }
    }
}

void collect_deferrable(const Node& node, const std::set<std::string>& sources,
                        const std::map<std::string, std::size_t>& counts,
                        const SourceRowCounts& row_counts, const SourceSchemas& schemas,
                        const std::map<std::string, double>& absorbed,
                        std::map<std::string, DeferrableProbeScan>& out) {
    if (node.kind() == NodeKind::Join) {
        const auto& join = node_cast<JoinNode>(node);
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

auto scan_source_counts(const Node& root) -> std::map<std::string, std::size_t> {
    std::map<std::string, std::size_t> counts;
    count_scan_occurrences(root, counts);
    return counts;
}

auto isolate_deferrable_probe_scans(NodePtr root, const std::set<std::string>& sources)
    -> ScanInstanceSplit {
    ScanInstanceSplit split;
    if (root == nullptr || sources.empty()) {
        split.plan = std::move(root);
        return split;
    }
    std::map<std::string, std::size_t> counts;
    count_scan_occurrences(*root, counts);
    std::map<std::string, std::size_t> next_instance;
    isolate_probes(root, sources, counts, next_instance, split.instances);
    split.plan = std::move(root);
    return split;
}

auto isolate_filtered_scan_instances(NodePtr root, const std::set<std::string>& sources)
    -> ScanInstanceSplit {
    ScanInstanceSplit split;
    if (root == nullptr || sources.empty()) {
        split.plan = std::move(root);
        return split;
    }
    std::map<std::string, std::size_t> counts;
    count_scan_occurrences(*root, counts);

    std::set<std::string> filtered;
    collect_filtered_sources(*root, filtered);

    std::set<std::string> split_these;
    for (const auto& source : filtered) {
        const auto count = counts.find(source);
        if (count != counts.end() && count->second > 1 && sources.contains(source)) {
            split_these.insert(source);
        }
    }
    if (!split_these.empty()) {
        std::map<std::string, std::size_t> next_instance;
        isolate_filtered(root, split_these, next_instance, split.instances);
    }
    split.plan = std::move(root);
    return split;
}

auto plan_join_key_origins(const Node& root, const SourceSchemas& sources)
    -> robin_hood::unordered_map<std::string, std::set<std::string>> {
    robin_hood::unordered_map<std::string, std::set<std::string>> out;
    const auto walk = [&](const Node& node, const auto& self) -> void {
        if (node.kind() == NodeKind::Join && node.children().size() >= 2) {
            const auto& join = node_cast<JoinNode>(node);
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

}  // namespace ibex::ir
