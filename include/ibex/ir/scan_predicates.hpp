// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/cardinality.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

#include <map>
#include <memory>
#include <robin_hood.h>
#include <set>
#include <string>
#include <vector>

namespace ibex::ir {

using ScanPredicateMap = std::map<std::string, std::vector<Expr>>;

/// Collect row-local filter conjuncts that can be evaluated over a named Scan,
/// optionally through column-only Project nodes. A source is returned only when
/// it occurs exactly once in the plan: the materialized table registry is keyed
/// by source name, so applying one occurrence's selection to a repeated /
/// self-join scan would be unsound. A repeated scan therefore keeps its
/// use-site `Filter` nodes and shares one whole-table decode.
///
[[nodiscard]] auto scan_predicates(const Node& root) -> ScanPredicateMap;

/// How many `Scan` nodes name each source. A count above one means a self-join
/// or repeated binding: the source keeps its use-site filters and is decoded
/// once, shared, rather than streamed per occurrence.
[[nodiscard]] auto scan_source_counts(const Node& root) -> std::map<std::string, std::size_t>;

/// A lazy scan eligible for deferred decode: it feeds — through nothing but
/// column-only Project and Rename nodes — the RIGHT side of an inner
/// single-key no-predicate join, and occurs nowhere else in the plan.
/// Deferring its decode lets the join derive bounds from its build side and
/// hand them to the scan before any probe column is materialized.
struct DeferrableProbeScan {
    /// The join key translated back through the rename chain to the scan's
    /// own column name.
    std::string key_column;
};

/// Scan (instance) name -> eligibility info. `sources` is the set of lazy
/// scan names the caller can actually defer; anything else is ignored.
/// For each source, its own names for the columns joined on anywhere in `root`.
///
/// Uniqueness of a base column is only worth proving where a pass can consume
/// it, and the consumer is `estimate_cardinality`'s `|PK join FK| <= |FK|`
/// bound, which reads join keys. Proving it costs a full column decode, so this
/// is what keeps that cost proportional to the plan rather than to the file.
///
/// Resolved through `column_origins`, not by matching names. A join key names a
/// column of the JOIN'S INPUT, which may be several renames removed from the
/// source column it came from -- `select { c_nationkey = n_nationkey }` is the
/// PDS-H idiom and defeats a name comparison outright.
[[nodiscard]] auto plan_join_key_origins(const Node& root, const SourceSchemas& sources)
    -> robin_hood::unordered_map<std::string, std::set<std::string>>;

/// `row_counts`/`schemas`, when supplied, gate eligibility on estimated size:
/// a source is only returned when the join's BUILD side (the side that will
/// publish key bounds into this scan's filter slot) is estimated to be
/// smaller than this scan's own (exact, footer-known) row count. An unfiltered
/// or otherwise unestimable build side means the published bound would span
/// the scan's whole key domain -- pruning nothing while still paying full
/// eager materialization of the build side, a pure loss (see
/// project_deferred_probe_no_cost_model.md / TPC-H q12). Omitting both
/// (leaving them default-empty) disables the gate entirely and keeps the
/// original, purely structural eligibility -- every existing caller does this
/// today, including every unit test below.
/// `absorbed_scan_selectivity` carries the selectivity of filters already fused
/// into a scan's decode, keyed by source. Callers that run AFTER
/// `remove_applied_scan_filters` must supply it: the gate below weighs a build
/// side's filtered row estimate against its own table's size, and without this
/// every build side reads back its full table, making a genuinely reduced one
/// indistinguishable from an unfiltered one.
[[nodiscard]] auto deferrable_probe_scans(
    const Node& root, const std::set<std::string>& sources, const SourceRowCounts& row_counts = {},
    const SourceSchemas& schemas = {},
    const std::map<std::string, double>& absorbed_scan_selectivity = {})
    -> std::map<std::string, DeferrableProbeScan>;

/// Remove row-local filters which have already been applied by a lazy source.
/// `applied_sources` must contain only sources for which the caller actually
/// materialized the selection. The implementation repeats the scan-predicate
/// proof, so a repeated source or a partial/non-local predicate remains in the
/// plan even if it is named in `applied_sources`.
///
/// Fused filter nodes are de-fused while retaining their project, update, or
/// limit operation.
[[nodiscard]] auto remove_applied_scan_filters(NodePtr root,
                                               const std::set<std::string>& applied_sources)
    -> NodePtr;

}  // namespace ibex::ir
