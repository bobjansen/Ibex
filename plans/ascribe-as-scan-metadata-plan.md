# Fuse a proven Ascribe into its Scan instead of teaching passes to see through it

Status: landed 2026-08-20. `ScanNode::ascribed_schema()` added
(`node.hpp`), `fuse_checked_ascriptions` implemented next to
`check_ascriptions` (`schema.cpp`/`schema.hpp`) and wired into both REPL call
sites (`repl.cpp`), `infer_schema`'s `Scan` case mirrors the `Ascribe` case,
`required_columns.cpp` needed no change (confirmed by test), and
`lower.cpp`'s deep-clone `Scan` case now copies `ascribed_schema()`. 4 new
unit tests in `test_ir_schema.cpp`; full `ctest` (1641/1641) and 22/22
`check_answers.py` pass. q09's `part` island dropped from the 104-morsel
zero-row-work island Mechanism 5 chased to `morsels=8` for the whole query
(`IBEX_PARALLEL_STATS=1`), with no chunked-engine (`pipeline.cpp`/
`chunked.cpp`) changes at all — the plan's central thesis. q04's
`pipelined_scans=2` confirms `build_pipelined_scan` still recognizes a fused,
ascribed multi-unit `Scan` as pipelined. Written after five same-day
reverted attempts at [[project_ascribe_pipeline_barrier]] (Mechanism 5 of
[query-shape-conformance-plan.md](query-shape-conformance-plan.md)).

## The complaint this plan answers

An ascription (`read_parquet(...) as DataFrame<{...}>`) asserts a *type
fact* — these columns exist, with these types — about data that already
exists. It moves no row, drops no row, renames nothing, computes nothing.
Structurally, though, `AscribeNode` is a full peer of `FilterNode`/
`ProjectNode`/`JoinNode`: same `Node` base, same tree position, same
obligation on every pass that walks the IR to have an opinion about it.

`check_ascriptions` (`src/ir/schema.cpp:569`) proves an ascription against a
statically known input schema and sets `AscribeNode::checked()` — but leaves
the node in the tree, unchanged in shape, forever. From there, every
consumer that wants to treat a checked ascription as a no-op has had to be
taught that fact independently:

- the interpreter's identity elision (`src/runtime/interpreter.cpp:782`,
  "Mechanism 2" in the query-shape-conformance history)
- `scan_predicates.cpp`'s `match_probe_chain`/key-tracing walks (two
  separate call sites, `:59` and `:340`, "Mechanism 3")
- five same-day attempts to teach the chunked engine's `execution_capability`,
  `expressions_are_subset_evaluable`, `build_row_local_map_operator`, and
  (not yet tried) `analyze_parallel_island`'s `NoRowWork` demotion
  ("Mechanism 5")

Every one of those is the same fact — "a checked Ascribe is nothing" —
re-derived and re-encoded at a different call site, in a different vocabulary
(row-locality, streamability, key-tracing, cardinality). Mechanism 5's five
failures this session are not five different bugs; they are one pattern
(treating a metadata-only node as an operator with real eligibility rules)
surfacing five times in one subsystem because nothing upstream ever removes
the node once it has nothing left to prove.

**The fix:** once `check_ascriptions` proves `Ascribe(Scan(...))`, physically
replace the pair with a single `Scan` node carrying the ascribed schema as an
attribute. Not deletion-that-loses-information — the schema moves, it
doesn't vanish (this is exactly what
`[[feedback_ascribe_carries_info_fuse_not_elide]]` already establishes as the
right shape: fuse the information onto the surviving node, don't discard it).
After the fusion, no downstream pass — not the three that already special-case
it, not any future one — ever sees a bare checked `Ascribe` in a canonicalized
tree, so none of them need a case for it. An unchecked ascription (open
schema, unknown/dynamic source — the only case `check_ascriptions` cannot
prove) is untouched by this and keeps going through every existing code path
exactly as today; this plan does not change behavior for that case at all.

## Scope: what actually needs to change

### 1. `ScanNode` gets an optional ascribed schema

`include/ibex/ir/node.hpp:579` — `ScanNode` currently holds only
`source_name_`. Add an optional payload with the same shape `AscribeNode`
already carries:

```cpp
struct AscribedSchema {
    std::vector<SchemaField> fields;
    bool open;
};
// ScanNode:
[[nodiscard]] auto ascribed_schema() const noexcept -> const std::optional<AscribedSchema>&;
void set_ascribed_schema(std::vector<SchemaField> fields, bool open);
```

This is additive — every existing `ScanNode` construction leaves it
`std::nullopt`, so nothing that constructs a bare `Scan` today needs to
change.

### 2. The fusion pass itself

New function, `src/ir/schema.cpp` (next to `check_ascriptions`, which it
depends on having already run):

```cpp
auto fuse_checked_ascriptions(NodePtr root) -> NodePtr;
```

Bottom-up rewrite, same shape as `canonicalize.cpp`'s R-rules (`try_fuse_topk`
at `canonicalize.cpp:955` is the closest existing template — take ownership
of a `NodePtr`, optionally replace it, hand ownership back). The rule:

> If `node` is `Ascribe`, `checked()` is true, and its one child is a bare
> `Scan`: take ownership of the child `Scan`, call `set_ascribed_schema` on
> it with the Ascribe's `schema()`/`open()`, and return it in place of the
> `Ascribe` node. Otherwise return `node` unchanged.

Deliberately narrow: it only fires on the direct `Ascribe(Scan)` shape — the
one every ascribed reader actually produces (SPEC convention since
Mechanism 1). `Ascribe` wrapping anything else (a join, a filtered
subquery — the grammar permits `base as DataFrame<{...}>` for any `base`,
per `lower_ascribe` in `lower.cpp:1611`) is left exactly as-is, still needing
every existing checked()-aware code path. That's fine: those paths stay
correct and necessary, just less frequently exercised.

### 3. Call the fusion pass right after `check_ascriptions`

Two call sites in `src/repl/repl.cpp` currently call `check_ascriptions`
(`:3678`, `:4947`). Both already hold the tree as an owned `NodePtr` right
there. Add `lowered.value() = ir::fuse_checked_ascriptions(std::move(lowered.value()))`
(or the `rewritten` equivalent at the second site) immediately after each
`check_ascriptions` call succeeds. No reordering of anything else in the
pipeline — this slots in exactly where Mechanism 2's `checked()` flag already
becomes trustworthy, one line later.

### 4. Audit of the ten existing `NodeKind::Ascribe` call sites

Checked each this session; grouping by what they actually need:

**Already correct, no change needed** — these treat Ascribe as identity
*unconditionally* (checked or not), because the property they compute
(cardinality, sort order, column origin) is true of an ascription regardless
of proof state:
- `src/ir/cardinality.cpp:72,332`
- `src/ir/pending_order.cpp:39`
- `src/ir/column_origins.cpp:154`

These fire less often post-fusion (only on the now-rarer unchecked/non-Scan
Ascribe shapes) but don't need editing — a fused `Scan` just doesn't reach
their `case NodeKind::Ascribe` at all, and that's fine, they were never wrong.

**Stay exactly as-is, by design** — these already gate on `checked()` and are
correct for the unchecked case, which is the only case that can still reach
them after fusion:
- `src/ir/scan_predicates.cpp:59` (`match_probe_chain`'s Mechanism 3 fix)
- `src/ir/scan_predicates.cpp:340` (the twin key-tracing walk)
- `src/runtime/interpreter.cpp:749` (Mechanism 2's identity elision)

**Need a mirrored case on `Scan`** — the two places that must recognize an
*ascribed* Scan and treat it like the checked-Ascribe case used to:
- `src/ir/schema.cpp:770` (`infer_schema`'s `Scan` case) — when
  `scan.ascribed_schema()` is set, build the `SchemaInfo` from it exactly the
  way the existing `Ascribe` case at `schema.cpp:1053` already does, instead
  of (or in addition to, preferring the ascribed one) looking up
  `sources.find(source_name)`.
- `src/ir/required_columns.cpp:149` (`Scan`'s case) — likely needs *no*
  change: it already does `out.sources[source_name].merge(need)`, which is
  precisely what the current checked-Ascribe branch reduces to ("pass the
  parent's demand straight through"). Confirm with a test before assuming.

**Needs a decision, not just a mirror** — `src/parser/lower.cpp:4899`, the
generic IR deep-clone used for shared-binding/subquery duplication, has an
`Ascribe` case (`clone = builder_.ascribe(...)`). Its `Scan` case (wherever
that is — not yet located) needs to copy `ascribed_schema()` too, or a cloned
fused Scan silently loses its ascription.

**A found side-benefit, not required for this plan but worth flagging**:
`src/codegen/emitter.cpp:563` does *not* special-case `checked()` at all
today — it unconditionally emits `ibex::ops::ascribe(child, {...}, open)`,
paying the runtime check in every compiled query even when Mechanism 2 has
already proven it unnecessary at lower time. Fusion fixes this for free: a
fused Scan never reaches this codegen case, so a compiled query stops paying
for a check that was already proven redundant. No separate emitter change
needed — just note it in this plan so the improvement doesn't go
unrecognized as "why did codegen get faster."

### 5. What becomes deletable after this lands (not in this plan's first cut)

Once the fusion is landed, verified, and has run in production for a while,
the five Mechanism-5 attempts' worth of chunked-engine special-casing
(`execution_capability`'s would-be `Ascribe` case, `build_row_local_map_
operator`'s would-be `Ascribe` case) becomes unnecessary — a fused tree never
presents a bare Ascribe to `pipeline.cpp`/`chunked.cpp` at all, so the whole
Mechanism-5 problem this session chased five times *disappears without a
single line changed in either file*. Don't do this speculatively — verify
fusion lands correctly first, then confirm no Ascribe case is needed there,
as a follow-up cleanup, not part of the initial change.

## Verification plan

1. **Unit-level**: a direct test for `fuse_checked_ascriptions` — checked
   `Ascribe(Scan)` → bare `Scan` with `ascribed_schema()` set; unchecked
   Ascribe untouched; `Ascribe(Join(...))` or any non-Scan child untouched;
   double ascription `Ascribe(Ascribe(Scan))` (if the grammar even permits
   it — check) behaves sanely.
2. **`infer_schema` parity**: for a fused Scan, `infer_schema` must return
   byte-identical `SchemaInfo` to what the pre-fusion `Ascribe(Scan)` tree
   returned — this is the highest-risk correctness surface, since every
   schema-aware pass depends on it.
3. **22/22 `check_answers.py`** + full `ctest` at each step (matches this
   session's established methodology).
4. **The actual payoff check**: re-run q09's `IBEX_PARALLEL_STATS` probe from
   this session's 5th attempt (`part` forming a bare 104-morsel island for
   zero row-work) with fusion landed and *no* Mechanism-5 pipeline.cpp/
   chunked.cpp changes at all. If fusion is doing its job, `part` never
   reaches the chunked engine as an `Ascribe` node in the first place — it's
   a plain `Scan`, exactly as it was before Mechanism 1 ever existed — and
   the whole failure mode should be structurally impossible rather than
   gated by a new cardinality check.
5. **q04's actual original win** (Mechanism 5's whole motivation — a
   multi-row-group ascribed scan should still unlock `build_pipelined_scan`)
   needs re-verification post-fusion: a fused Scan carrying an ascribed
   schema must still be recognized by `has_multi_unit_deferred_scan` and
   `build_pipelined_scan` as an ordinary deferred multi-unit Scan — which it
   should be automatically, since after fusion it *is* one, with no wrapper
   node in the way at all. This is the plan's actual thesis: q04's win and
   today's five regressions were never in tension: the wrapper node was.

## Risk / effort honesty

This is a bigger change than anything attempted today — it touches
`ScanNode`'s shape (used everywhere a `Scan` is matched on) and the fusion
pass sits upstream of a fair amount of the pipeline (`required_columns`,
`scan_predicates`, `interpret`/`build_operator`, `emitter`, plus whatever
`lower.cpp:4899`'s clone path is used for). It is not a same-day fix the way
the five Mechanism-5 attempts were meant to be. But it removes a whole class
of bug at its source instead of patching another leaf: Mechanisms 2, 3, and
5 are three independently-discovered instances of the same missing rewrite,
and a fourth instance (whatever future pass next needs to reason about
row-locality, cost, or scheduling around a scan) is now a certainty, not a
risk, if the node keeps existing in checked form. Recommend treating this as
its own session, not a same-day follow-on to today's five attempts.
