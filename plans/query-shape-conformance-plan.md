# PDS-H query-shape conformance: what is left

**Compacted 2026-09-23.** The investigation is over. This file keeps the open
leftovers and the list of things not to try again. The full diary (mechanisms
1–5, the scan-fusion calibration tables and the q22 rewrite) is in git history:
`git show 81392162:plans/query-shape-conformance-plan.md`.

## Background

`482eb583` "Match query shapes" rewrote the PDS-H queries into their canonical
form and cost 13–16% across the suite. The hand-fused versions had bought
engine behaviour through how they were written (a `select {...}` after every
scan, per-order scalar aggregates in q21). Leaving out q21, the suite is back at
parity (0.995×). The scan-fusion cost gate that was once the top item is
**closed**: re-measured 2026-09-02, fusing wins or breaks even on 21 of 22
(memory: `project_scan_fusion_gate_closed`).

## Open items, ranked

1. **q13: the fused non-anchored LIKE scan does more work than it saves.**
   `string_filter_scan` over all of `o_comment` for
   `not like '%special%requests%'` uses 66% more CPU than a dense decode plus an
   in-memory filter (SF-8/8c: fused 398ms wall / 2614ms pool_work, unfused
   294ms / 1577ms). Better occupancy hides part of it (0.82 vs 0.67). Either
   make the fused string scan competitive, or decline fusion for a
   non-anchored LIKE over a large String column. This is one query and one
   mechanism, and it needs no cost model.
2. **Row-group task granularity in `direct_decode_table`** (old Mechanism
   4(a)). A single-column whole-file decode with fewer row-group tasks than
   workers leaves cores idle (q15: `l_shipdate` at SF-2). Split a row-group
   task further when `tasks.size() < workers`. See also memory
   `project_row_group_caps_parallel_width`.
3. **Deferred-probe eligibility gate on row-count ratio** (old Mechanism 3).
   Low priority. Re-survey first what still reaches `collect_deferrable`. If it
   is built, key it on the footer `rows()` ratio of the two sides, and
   calibrate on more than q08 (~3.3%) and q12 (~25%) at SF-2.

Old Mechanism 1 (filter-into-join pushdown could not see schemas at lowering
time) looks resolved: the REPL re-runs `push_filters_into_joins` with
footer-derived schemas (`repl.cpp`, after `check_and_fuse_ascriptions`). Check
that before reopening it.

## q21 is not a conformance gap

The canonical q21 self-joins lineitem at line granularity and then
aggregates, so its join output grows with the square of lines per order. The
old query computed per-order counts first. Pushing `o_orderstatus == 'F'`
through was tested and does nothing (it keeps ~50% of orders). Closing the gap
would need a rewrite that turns "self-join whose only consumer compares a
cardinality (`== 1`, `> 1`, `exists`)" into distinct-and-count. q21 is the only
query of the 22 with that shape, so this is closed as a known gap.
Runtime-level q21 work continues separately (memory:
`project_q21_is_occupancy_bound`).

## Tried and reverted: do not repeat without new evidence

- **`elide_checked_ascriptions`** (delete the `Ascribe` node once it is proven):
  net +25%, because it unblocked deferred probing for q09/q12. Replaced by
  `fuse_checked_ascriptions`, which folds the ascription into the `Scan`.
- **A structural "was this side filtered" gate for deferred probes**
  (`contains_row_reducing_node`): net −10.3% when it should have won. It cannot
  tell q08's `part` (small by construction) from q12's `orders` (large and
  unfiltered).
- **Classifying `Ascribe` as pipelineable in `execution_capability()`**, four
  attempts: +13% to +30% on the suite. Each newly eligible scan paid pipelining
  overhead for a consumer that drained it synchronously. Fixed at the root by
  `fuse_checked_ascriptions` (memory: `project_ascribe_as_scan_metadata`).
- **A numeric selective-decode fast path above the Arrow API**, twice: no win.
  The cost is in Arrow's page copy.
- **Swapping the old q21 query text back in**: ruled out. Join order and query
  shape are the engine's job (memory: `feedback_engine_owns_join_order`).
