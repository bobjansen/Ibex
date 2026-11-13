# Streaming join follow-ups

Status: open, 2026-11-13. Follows `c82bcfa0` (single-key left joins stream).
Numbers are SF-10 on the i7-13700 at 3.2 GHz, physical cores pinned
(`taskset -c 0,2,..`). Unless a section names another query, the test query is
lineitem (60M rows: `l_partkey`, `l_extendedprice`, `l_quantity`) joined to
part (2M rows: `p_partkey`, `p_size`), then summed by `p_size`. Every lineitem
row matches a part.

## 1. The inner join loses to the left join on the same query

| | inner | inner, left filter off | left |
|---|---:|---:|---:|
| 8 cores | 0.69 s | 0.50 s | 0.44 s |
| 1 core | 3.15 s | 2.02 s | 2.01 s |

The table has two causes.

**a. The streamed-left key filter retries, and is abandoned, on every unit.**
`ChunkedInnerJoinOperator::publish_left_stream_filter` publishes the part keys
into lineitem's scan slot whenever `n_right * 2 <= left rows` (2M × 2 ≤ 60M).
Here that is an exact bitmap over 1..2M that rejects nothing. Each of
lineitem's 62 decode units then works through `LazyTable::project_where_unit`
as follows:

1. The fused key scan (`key_filter_scan` → `filtered_key_selection`) decodes
   and tests the unit's whole `l_partkey`. That is ~983K rows; the source's
   abandon rule needs only 2^18 rows above a 75% pass rate, but a unit is one
   group.
2. The scan abandons: it returns nullopt.
3. The general path decodes `l_partkey` again (`decode_unit_predicate_columns`)
   and applies the same membership in memory.

A temporary trace counted 61 of 62 units abandoning. Nothing remembers the
abandonment, so every unit pays again. The cost is about 1.25 s of decode
work at 8 cores (scan CPU 2599 ms vs 1340 ms; zstd samples 1.8×), 0.19 s of
wall time at 8 cores and 1.13 s at 1 core.

**Fixed 2026-11-13 for the case measured here**
(`ChunkedInnerJoinOperator::drop_filter_rejecting_nothing`): a streamed slot
drops its bitmap when the bitmap is full and the scan's Int64 key statistics
lie inside `[min, max]`. That is when the filter provably rejects nothing.
- Inner join: 0.68 → 0.51 s at 8 cores, 3.08 → 2.02 s at 1 core; the answers
  are identical.
- Native PDS-H A/B: gm 0.999 (8 cores), 1.008 (1 core). All 22 outputs are
  identical, and q04's 1-core +3.9% did not reproduce over three alternating
  rounds.
- Without statistics (CSV, an in-memory lazy source) the bitmap stays: a full
  range can still prune a wider probe.
- The join keeps measuring orientation by draining the left. The no-drain
  shortcut left the same probe ~0.3 s slower at 1 core: same instructions,
  chunks, index and LLC misses, just more cycles. That is not explained yet.

**Per-unit footer estimate, 2026-11-13** (instead of a sticky abandon). A
cumulative tally that abandons for good would lose the filter on clustered keys:
early units pass everything and later ones would be pruned. So each unit is
decided on its own row groups' statistics. `footer_estimated_pass` weights each
group's rows by the bitmap's density over the group's footer [min, max]
(`JoinKeyBitmap::count_range`). When the estimate is above the source's own
abandon rate (75%), `key_filter_scan` answers "every row" without decoding, and
the unit decodes plainly. Otherwise it filters as before.
- part filtered to 90% of keys, lineitem joined to it: 8 cores 0.61 → 0.54 s,
  1 core 3.23 → 2.19 s.
- Selective `p_size < 5`: unchanged (0.24 s, 1.55 s).
- Clustered lineitem join orders[`o_orderkey <= 15M`]: unchanged (0.32 s,
  ~0.95 s), so the pruning is kept.
- Native PDS-H A/B: gm 0.998 (8 cores), 1.001 (1 core); all 22 outputs
  identical.

Limits:
- The estimate assumes keys spread evenly over a group's range. A sparse probe
  domain (TPC-H order keys use about a quarter of their range) reads low and
  keeps its filter, i.e. today's behaviour.
- Bloom filters (sparse build keys) and sources without statistics still retry
  per unit. A tally with periodic re-tests (so clustered data can switch the
  filter back on) would cover them; there is no measured case yet.

Fix options, cheapest first:

- **Treat a full bitmap as a range.** A bitmap whose population count equals
  its span (`distinct_keys == max - min + 1`, the usual 1..N dimension key)
  rejects nothing that `[min, max]` does not already reject. Publish only the
  bounds then, and the existing footer test (`footer_covered_fraction`,
  range-only filters) declines the scan before reading a page. This handles
  this query statically and changes nothing for a sparse build side.
- **Make the abandonment sticky across units.** Hold a shared flag (or running
  scanned/passing totals) beside the slot. Set it when a unit's fused scan
  abandons, or when the in-memory fallback measures a pass rate above
  `kAbandonPassRate` over at least `kAbandonMinRows`. Later units then take
  `project_unit` with no membership. This is always correct: the slot only
  prunes, and the join probes every row exactly anyway. The semi join's left
  slot shares the path and gains the same way.
  - `DynamicScanFilter` is in a public header and is copied by value (the
    static range filter), so the flag must be a `shared_ptr<atomic<bool>>`
    rather than an `atomic` member.
  - The streamed units run in parallel, so which units still filter depends
    on the schedule. The answer does not.
  - A file sorted by key would show a high pass rate first and a low one later.
    The rule is prefix-based, like the whole-source one.
- Before landing either, check which PDS-H queries publish a left slot that
  abandons. Use the same trace in `project_where_unit`. q03/q05/q09 publish
  and should reject; prove they still do, and A/B all 22.

**b. The inner probe makes two passes for a unique build.**
`JoinProbe::probe_unique_ranges` runs a count batch, then a write batch, and
writes both `li` and `ri`. The left join's `probe_left_outer` does one batch
and writes only `ri` (misses as `kNil`). With the filter off, the inner
operator spends 100 ms serial (`next_self_ms`) and 970 ms pool work in 1512
tasks; the left join spends 29 ms and 652 ms in 1016 tasks. Proposed: write
`ri` in one pass with `kNil` for misses, take `li` as the identity when there
is no `kNil`, and compact both only when there are misses (a second pass over
`ri`, paid only by joins that drop rows). Expected ~0.05 s at 8 cores here. A/B
PDS-H: q03/q05/q10 probe selective unique builds, where the compaction pass
is the new cost.

**Fixed 2026-11-13** (parallel path; the serial path already matched the left
join at 1 core). The first batch writes heads into `ri` and counts matches.
When every row matched, `li` is the identity and the second batch is skipped;
otherwise it compacts as before.
- lineitem x part inner at 8 cores: 0.50 → 0.44 s, equal to the left join.
  Join serial time 100 → 34 ms, tasks 1512 → 1016.
- Native PDS-H 8c A/B: gm 1.002, nothing beyond 2%; all 22 outputs identical.
- h2o joins at 8 threads: q1 1.23 → 1.23, q2 1.36 → 1.31, q4 1.26 → 1.23.

## 2. Two-key left join

**Now:** `plan_join` declines it (`MultipleKeys`), so `join_table_impl`
materializes both sides.

**What it needs:** the inner pair path exists: `is_streamable_pair_int_join`
(both keys provably Int64), `initialize_pair` / `choose_and_build_pair`,
`probe_chunk_pair` → `probe_pair` with its own `is_null` over both keys'
validity. The changes:

- Accept `Left` in a pair gate.
- Route `probe_pair` through a left-outer variant: a null in either key, or a
  miss, emits `(l, kNil)`. The single-key `probe_left_outer` assumes
  `probe_is_null` (one bitmap), so give it an `is_null` parameter instead of a
  copy.
- Force `BuildRight` in `initialize_pair`, falling back to `join_table_impl`
  when the left is smaller than a right above `kStreamRightThreshold`, as the
  single-key path does.
- Keep the deferred-probe paths out: `make_scheduled_deferred_inner_join_operator`
  and `try_stream_pair_deferred` index the left.

**Tests:** extend the streaming-left oracle test with pair keys: nulls in
either key, duplicates, misses, and the fallback.

**Value:** no current benchmark has a two-key left join. Do it when a query
needs it.

## 3. Outer, right, and small-left left joins: keep unmatched build rows

**Now:** outer and right joins decline (`UnsupportedKind`). A single-key left
join whose left side is smaller than a right above 64K rows runs
`join_table_impl` on the drained sides.

**One mechanism covers all three.** The streaming join needs two independent
switches:

- **keep unmatched probe rows:** emit a probe-side miss inline with `kNil`.
  This is what `left_outer_` does today.
- **keep unmatched build rows:** mark each build row a probe matches, and
  after the probe side ends emit every unmarked build row once, with null
  probe-side columns.

| join | build right (left streams) | build left (right streams) |
|---|---|---|
| left | probe rows (done) | build rows |
| right | build rows | probe rows |
| outer | both | both |

What the second switch needs:

- **A matched bitmap over the build rows.** Writing it from the probe workers
  races, so mark from each chunk's completed `ri` after the probe batch, on
  the operator thread or per-range into per-worker bitmaps OR'd at the end.
  This is the same trade `join_table_impl`'s unique outer path makes ("mark
  from the completed row indices rather than having all workers contend").
  For a unique build with a dense direct-address table it is one byte or bit
  per build row.
- **An end-of-stream emission:** after the probe source is exhausted,
  `JoinProbeOperator::next` must yield one more chunk built from the unmarked
  build rows. The probe's chunks may run as a pipelined stage
  (`make_pipelined_stage_if`), so the final chunk must come after every worker
  has finished. That is a barrier this operator does not have today; check
  `stage_probe` paths before assuming the plain operator.
- **Null build keys:** they never match and must still appear once as
  unmatched build rows (`fill_partitioned_heads` never indexes them; the
  emission walks rows, not the index).
- **Folded keys:** the output's key column takes the build row's key for a
  build-only row (`JoinOutputColumn::folded_peer_index`, which
  `join_table_impl`'s `materialize` handles). The streaming assembly gathers
  only right columns from `ri`, so the final chunk needs a per-column source
  choice. Only outer joins emit rows where the left key is null.
- **Orientation for right and outer joins:** with right as build, a right join
  is "left streams, build-side outer". With left as build (swapped mode),
  `emit_swapped` materializes the right side today, so streaming the right
  needs the probe operator over right chunks with left/right output order
  restored. Lower risk: a right join first only on the build-right
  orientation, falling back otherwise.
- **Row order:** unmatched build rows come last. That is outside the join
  contract (SPEC.md §5.6), but `join_table_impl` has the same layout for outer
  joins, so tests that pin it keep their meaning.

**Order of work:**

1. Outer, single key, build right, unique and duplicate builds. It exercises
   both switches.
2. Right join via the same operator.
3. Small-left left join (build left, keep unmatched build rows), which
   replaces the `join_table_impl` fallback in `initialize()`.

**Tests:** the streaming-left oracle test pattern (`[join][left][stream]`)
against `join_tables` per kind, with null keys on both sides, an empty side,
and all rows unmatched on either side. Add a check that the final chunk
arrives after a chunked (`IBEX_CHUNK_ROWS`), parallel probe. Mutation-check
the matched marking and the final emission.

**Value:** no benchmark query has an outer or right join that reaches a join
operator (PDS-H q13's left join fuses into a count). The gain is memory and
time on user joins over scanned tables, in the same ratio as `c82bcfa0`
measured (8 cores: 1.19 → 0.44 s, 3.55 → 0.88 GB).
