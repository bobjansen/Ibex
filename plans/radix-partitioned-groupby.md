---
name: radix_partitioned_groupby
description: "Note: high-cardinality group-by is memory-bound; radix partitioning is the fix. The remaining wall on PDS-H q18/q10 after the boxed-Key fast paths."
metadata:
  node_type: memory
  type: project
---

# Note: radix-partitioned group-by for high-cardinality grouping

Status: **noted, not built.** This is the group-by ceiling that remains after the
July-2026 fast-path work (single fixed-width-int key, packed multi-key distinct,
in-place multi-key hashing). Those removed the *allocation* cost (a boxed `Key`
per group). What is left is **memory-bandwidth** cost, which they cannot touch.

## The evidence

Instrumenting PDS-H q18: the dominant stage is `sum(l_quantity) by l_orderkey`
over 6M line-item rows producing **1.5M groups**. Breakdown (warm, isolated):

- `count() by l_orderkey`: ~198 ms — the pure grouping (6M hash probes into a
  1.5M-entry map).
- `sum(...) by l_orderkey`: ~300-400 ms — the extra is scattered `+=` into
  1.5M accumulator slots (60 MB of `AggSlot`).

Both are cache-miss-bound: the hash map (1.5M × 12 B ≈ 18 MB) and the slot array
(60 MB) both blow past L2, so almost every row pays a main-memory round trip. The
single-int fast path (`process_rows_int`) already avoids the per-group `Key`
allocation; it is a flat `robin_hood::unordered_flat_map<int64, gid>`. The
remaining time is the map and slot access pattern, not the key representation.

This is not a defect like the boxed-`Key` anti-patterns were — it is the
intrinsic cost of touching 78 MB of hash-table + accumulator state in random
order. q10 has the same shape at lower scale (114k rows → 38k groups).

## The fix: radix partitioning

Polars and DuckDB win high-cardinality group-by by **partitioning before
grouping**: hash each row's key, route it by the top bits into one of N
partitions (e.g. 256), then group each partition independently. Each partition's
hash table and slots are ~1/N the size, so they fit in cache, turning random
main-memory access into sequential-ish partitioned access. Partitions are also
trivially parallelizable — the natural point to introduce group-by threading.

Sketch:
1. Pass 1: hash each key, `partition = hash >> (64 - log2 N)`; count per
   partition (for exact sizing) or just scatter row indices into per-partition
   buffers.
2. Pass 2: for each partition, run the existing `ChunkedAggregateOperator`
   grouping over just that partition's rows. Each fits in cache.
3. Concatenate the per-partition group outputs.

## Why it is a real project, not a tweak

- It restructures the aggregate operator's core loop (a partitioning pass ahead
  of the existing group step), for every key type, not just one fast path.
- Choosing N and the partition-buffer strategy needs benchmarking across
  cardinalities — it must not regress the low-cardinality case (a handful of
  groups, where the current single-pass path is already optimal, and
  partitioning would be pure overhead). Gate on an estimated/observed group
  count or input size.
- It is the natural substrate for **multi-threaded group-by** (partitions are
  independent), which is itself on the execution roadmap. Doing radix first,
  single-threaded, then adding threads later is the sensible order.

## Where it matters

- **q18**: after semi-join pushdown (done, 668 → 246 ms) the big-orders group-by
  is the remaining wall.
- **q10**: the 7-key customer group-by (26 ms) is smaller but the same class;
  q10's larger cost is Parquet decode (see
  [[project_filtered_scan_groupby_gap]]).
- Any `sum/count by <high-cardinality id>` — a very common shape.

Related: [[project_multikey_groupby_no_boxed_key]] (the allocation fixes this
sits on top of), [[project_execution_roadmap]] (threading).
