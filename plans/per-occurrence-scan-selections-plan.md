---
name: per_occurrence_scan_selections
description: "A source scanned more than once loses ALL filter pushdown, because the table registry is keyed by source name and cannot tell occurrences apart. Separate the SELECTION (per occurrence) from the DECODE (per source) so each occurrence keeps its predicate without paying a second decode."
metadata:
  type: project
---

# Per-occurrence scan selections over one shared decode

**Status: proposed, nothing built.** The regression that motivates it is
diagnosed and bisected; one candidate fix was built, measured, and rejected
(§ Rejected). The design below is what is left.

## The defect

`src/ir/scan_predicates.cpp:210`:

```cpp
if (it->second.empty() || scan_counts[it->first] != 1) {
    it = candidates.erase(it);   // a source scanned twice loses ALL its predicates
}
```

Filter pushdown is keyed by **source name**. Two occurrences of one source may
want different rows, so pushing either occurrence's predicate would wrongly
filter the other. The conservative answer is to erase every predicate for that
source.

**Consequence:** any query that binds a source and filters it more than once
loses filter pushdown *entirely* — every predicate kind, not just the fused
string scan. Answers stay correct, which is why nothing noticed.

Caught by `scripts/ibex-e2e.sh` at *"whole-script (parquet plugin, fused string
filter over a nullable column)"*. That check asserts on a **profile marker**
(`source string filter scan`), not only on the answer, which is the only reason
it was visible at all.

**Bisected to `afe55f25`** "Delete split_scan_instances; a repeated scan is
decoded once and shared" (2026-08-31):

| commit | `source string filter scan` |
|--------|------------------------------|
| `afe55f25~1` | 1 (answer correct) |
| `afe55f25` | 0 (answer still correct) |

`split_scan_instances` used to give each occurrence its own name, so
`scan_counts == 1` per occurrence. Removing it made occurrences share a name,
which switched the `!= 1` rule on for every multi-use binding. That removal was
itself a real win (q21 -5-10%, q03 -12.5%, [[project_scan_instance_split_no_cost_gate]])
and is not the thing to undo.

## Why the obvious fixes do not work

**Lifting the gate is unsound, not merely risky.** Verified: on the
`like` / `!like` shape it pushes both conjuncts into one shared decode, they
intersect to nothing, and the query returns **0 rows**.

**Re-splitting the occurrences costs a second decode.** Built as
`isolate_filtered_scan_instances` (rename each occurrence to `source#fN`, the
move `isolate_deferrable_probe_scans` already makes for the deferred probe). It
works — the fused scan fires, the answer is right — but
`tests/test_repl.cpp:1060` ("a source scanned twice is decoded once, filters
stay") fails: one decode of `{a, b}` becomes three, `{a}`, `{b}`, `{b}`. And
q21 binds `lineitem_raw` once and filters it, so the split fires there and
plausibly returns the q21 win `afe55f25` bought. Rejected, not shipped.

**Two separate `read_parquet` calls are not a workaround.** Two reads of the
same path dedupe to one source (`__ibex_source_0`), so the occurrence count is
still 2.

## What is actually lost, precisely

This matters, because it is smaller than "pushdown" and it sets the design.

For an ordinary predicate, today's behaviour — decode the source once, run each
occurrence's `Filter` above the shared table — is fine, arguably optimal. It
costs no extra decode and no extra gather.

What is lost is the case where pushdown is **qualitatively** different rather
than merely earlier: a predicate over a column the query references *only from
its filter*. There, the fused source scan (`LazyTable::scan_string_filters`,
`libs/parquet/parquet.hpp` "Fused string filter scan") matches the pattern
against the bytes the page decoder hands back and emits row indices, so a
`Column<std::string>` is **never built**. On TPC-H `o_comment` that is 79MB of
characters and 1.5M offsets not written, for an answer that is one bit per row
([[project_filter_only_column_fusion]], [[project_string_like_filter]]).

Today a repeated source pays the full materialization of that column and then
filters it. That is the regression, and it is the thing to fix.

## The design

Separate the two things the registry currently conflates:

- **decode** — per SOURCE. One pass over the file for the union of columns any
  occurrence needs as OUTPUT.
- **selection** — per OCCURRENCE. `Selection` is already
  `std::vector<std::size_t>` (`include/ibex/runtime/lazy_table.hpp:25`).

```
  source "orders"                      one decode of {o_orderkey, o_totalprice}
      |                                (o_comment never materialized)
      +-- occurrence #1  selection S1  <- fused scan of like(o_comment, ...)
      +-- occurrence #2  selection S2  <- fused scan of !like(...) / other
```

Each occurrence's table is `gather(shared columns, its selection)`. Versus
today, that trades a materialization of the predicate column for a gather —
strictly better whenever the predicate column is filter-only, which is exactly
the case the gate currently kills.

### Why the registry is the obstacle

`TableRegistry = robin_hood::unordered_map<std::string, Table>`
(`include/ibex/runtime/interpreter.hpp:367`) — one materialized `Table` per
source name. That single map is why occurrence identity currently forces a
second decode: the only way to give two occurrences different rows today is to
give them different names, and a different name means a different decode.

The runtime half of what is needed largely exists. `LazyTable::project_where`
already takes conjuncts as a parameter and deliberately bypasses `cache_` (a
selected column must never masquerade as a cached whole-file column), and
`scan_string_filters` already returns a `Selection` rather than a column. What
does not exist is a plan/registry representation of "same decode, different
rows".

### Phases

**Phase 1 — occurrence identity in the plan (no behaviour change).**
Key `ScanPredicateMap` on scan-node identity rather than source name.
`ColumnOrigin` already carries a `NodeId` scan identity for exactly this reason
(`f9b0866e`, the commit immediately before the deletion, added it so FD
reduction could tell a self-join's two sides apart). Keep declining to push when
occurrences disagree, so behaviour is byte-identical; this phase only makes the
disagreement *expressible*. Ship and verify separately — a pass that changes
nothing is the cheapest thing to prove correct.

**Phase 2 — per-occurrence registry entries over a shared decode.**
An occurrence resolves to `(source, selection)`. The source decodes its output
columns once; each occurrence gathers. The `instances` map
(`instance -> source`) already threads through `resolve_lazy` in `repl.cpp` and
is the natural place to carry the extra indirection. Behaviour still identical
when every selection is "all rows".

**Phase 3 — per-occurrence fused predicate evaluation.**
With 1 and 2 in place, an occurrence whose predicate is over a filter-only
column asks the source for a `Selection` instead of a column. This is the phase
that actually fixes the e2e check and recovers the mechanism.

**Acceptance:** the e2e check
(*"fused string filter over a nullable column"*) goes green at Phase 3, with
`source string filter scan` back in the profile and the answer still
400/300000/800/600000. `test_repl.cpp:1060` must stay green throughout —
one decode of `{a, b}`, not three — since that is the property the rejected fix
broke.

**Phase 4 — re-examine the `!= 1` gate.** It can then be narrowed from "any
repeated source" to "a predicate that cannot be answered per occurrence",
which should be nearly nothing.

## Gating and risk

Phase 3 is where a regression could enter, and the temptation will be a cost
estimate. Resist it: [[project_deferred_probe_no_cost_model]] is the standing
example of a pushdown gate guessing wrong (q12). The structural signal here is
strong and needs no estimate — **the predicate's column is referenced by nothing
but that filter**, which is already exactly what
`LazyTable::fusable_string_conjuncts` tests (`refs[i].size() == 1`,
`readers[col] == 1`, `!names.contains(col)`, `!cache_.contains(col)`).

The measurement to run before and after each phase is an **interleaved paired
A/B**, not a serial before/after. On this codebase a serial suite run reported
q10 -2.9% where the interleaved run on the same machine minutes later gave
-32.8% with 10/10 paired wins, and showed 10-12% "improvements" on queries whose
plans provably had not changed ([[project_bench_interleaved_methodology]]).

Watch q21 and q03 specifically: they are what `afe55f25` improved, and q21 is
the query whose repeated filtered `lineitem_raw` made the rejected fix
dangerous.

## Reproduce

```sh
uv run --project . python tests/data/gen_parquet_string_filter_nulls.py \
    tests/data/parquet_string_filter_nulls_out.parquet
IBEX_PROFILE_OPERATORS=1 ./build-release/tools/ibex_eval \
    --plugin-path build-release/tools \
    tests/data/parquet_string_filter_nulls_check.ibex | grep -c 'source string filter scan'
# 0 on HEAD; 1 before afe55f25. Delete the fixture afterwards.
```

The single-use control, which fuses normally and confirms the gate is the only
difference:

```sh
# same query with ONE use of the binding -> the fused scan fires
```

## Until this is built

`scripts/ibex-e2e.sh` fails at that check, and **Phase 3 is what turns it green
again** — restoring the fused scan for a repeatedly-scanned source is the whole
point of this plan, and that check is its acceptance test. Phases 1 and 2 change
no behaviour, so it stays red through them; that is expected, not a surprise.

In the meantime the check should not be deleted, skipped, or weakened to green
the script. It is a true failure reporting a real lost capability, and it is the
only thing that noticed — every answer stayed correct.

A **strictly safe interim** that needs none of the above: push the INTERSECTION
of the conjuncts common to every occurrence. Each occurrence's own filter still
runs above the shared decode, so nothing loses rows it needs, and today's
total-loss behaviour improves whenever occurrences agree. It does not fix the
e2e check — the intersection of `like` and `!like` is empty — so it is a
mitigation, not the fix.

Related: [[project_repeated_scan_drops_all_predicates]],
[[project_scan_instance_split_no_cost_gate]],
[[project_filter_only_column_fusion]], [[project_string_like_filter]],
`plans/dynamic-filter-pushdown-plan.md`, `plans/bigger-than-ram-plan.md`.
