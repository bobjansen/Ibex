---
name: late_materialize_fd_payload
description: "q10 spends 3.1x its operator budget decoding and carrying six FD-determined wide string columns for a 20-row output. The group-key reduction narrows the key but keeps the payload as First() aggregates, which pins it to the scan. Proposal: re-fetch the payload above the top-k instead."
metadata:
  type: project
---

# Late materialization of FD-determined payload columns

**Status: LANDED `568c4974` "Lift FD-determined payload columns above a top-k"
(2026-09-03).** `src/ir/group_key_reduction.cpp`
(`plan_lift` / `apply_lift` / `lift_walk`), 4 tests in
`tests/test_ir_group_key_reduction.cpp`.

**Result: q10 -32.8% (10/10 paired wins, interleaved), -38.5% in the suite-wide
interleaved run.** 1844/1844 tests pass, 22/22 TPC-H answers correct at SF-1.

The pass fires on **q10 only** — every other PDS-H query's plan is byte-identical
(verified by join count across all 22), so the suite total is q10's share and
nothing else moved. Two things the implementation taught that the design did not
predict are folded in below: the mandatory re-sort, and why q03 does not qualify.

Found 2026-09-03 while analysing why q10 is the worst query in PDS-H against
Polars' streaming engine.

## The observation

q10 groups by seven columns and outputs **twenty rows**:

```
by { c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment }
...
order { revenue desc }, head 20
```

`c_custkey` is the customer primary key, so it functionally determines the other
six. `group_key_reduction.cpp` already proves this and already fires —
`IBEX_UNIQUE_KEY_STATS=1` prints `[unique] c_custkey rows=1200000`, and the
operator profile reports `aggregate keys=1 aggs=7`. **The group key is correctly
narrowed to one Int64.**

The problem is what happens to the six dropped keys.
`group_key_reduction.cpp:216` rewrites each one into

```cpp
AggSpec{.func = AggFunc::First, .column = group_by[i], .alias = group_by[i].name}
```

which is exact — a dropped key is constant within its group — but it keeps every
one of those columns *referenced*. Projection pushdown therefore cannot drop
them from the scan, so all six are decoded from Parquet, gathered through three
joins, and written into ~304K aggregate slots, so that `head 20` can discard
99.99% of them.

`c_comment` alone averages ~72 bytes; with `c_address`, `c_name` and `c_phone`
the payload is roughly 130 bytes per customer row across 1.2M rows.

## What it costs (measured, SF-8, local)

A/B of the real query against the same query with `by { c_custkey }`. **Both
produce an identical top-20** (verified on custkey and revenue), so this is a
like-for-like measurement of carrying the payload:

| group key | 1 core | 8 cores |
|-----------|--------|---------|
| wide (7 columns, as written) | 1.00s | 0.56s |
| `c_custkey` only | 0.66s | 0.34s |
| | **-34%** | **-39%** |

The narrow variant omits the display columns entirely rather than re-fetching
them, so the true prize is somewhat under these figures — see *Risks*.

### The saving is decode, not the joins

This is the part that determines the design. Operator `self_ms`, 1 core,
`IBEX_PROFILE_OPERATORS=1`:

| operator | wide | narrow | delta |
|----------|------|--------|-------|
| `source decode selected` | 784.7 | 126.4 | **-658** |
| `source decode whole` | 823.6 | 285.3 | **-538** |
| `join inner keys=1` (both) | 398.9 | 221.1 | -178 |
| `Aggregate.Discovery` + `.Emission` | 123.1 | 23.1 | -100 |
| **total operator work** | **2201.9** | **700.5** | **3.1x** |

Two thirds of the win is simply **not decoding customer's wide string columns**
(189 MB of Parquet). The join and aggregate savings are real but secondary. So
the mechanism to aim for is "make the payload unreferenced in the main
pipeline", after which existing projection pushdown does the heavy lifting for
free. That is why the fix is a *plan shape* change, not an aggregation-kernel
change.

### This confirms an existing prediction

`plans/beat-polars-plan.md` (2026-08-27 update, line ~48) already concluded:

> A forced fact-first join order saved only about 2-3ms and is not the main
> lever. **The remaining q10 gap is upstream decode/join materialization**, not
> mixed-key grouping.

This plan supplies the mechanism behind that sentence and puts a number on it.
It does not reopen mixed-key grouping, and it does not lower the generic
partition gate — both remain closed as that update says.

The same file's dead-end list carries the fact that explains the decode cost:

> q10's expensive key columns (`c_name`, `c_address`, `c_phone`, `c_comment`)
> are `Column<std::string>`, **NOT Categorical** — their Parquet pages fall back
> to PLAIN when the dictionary overflows.

So these columns are decoded as plain bytes, with no dictionary to share, which
is why `source decode selected` alone is 784.7ms of a ~2.2s single-core budget.
Not touching them at all is worth far more than decoding them faster.

Note that the FD-reduction revert recorded in that dead-end list ("the discovery
point does not cover q10 — customer is never the build side, so `c_custkey`'s
uniqueness is never observed") is **stale**: on the current tree the uniqueness
*is* observed and the reduction *does* fire, as `[unique] c_custkey rows=1200000`
and `aggregate keys=1 aggs=7` show. The key half of that work is in and working;
this plan is about the payload half it left behind.

## Proposal

Replace the `First()` aggregates with a re-fetch join placed **above the
top-k**:

```
Project(original column order)
  |
  +- Join(inner, agg_key = source_key)
       |
       +- Head 20                          <- join sits ABOVE this
       |    +- Order { revenue desc }
       |         +- Aggregate(key=c_custkey, aggs=[revenue])   <- payload gone
       |
       +- Scan(customer, [c_custkey, c_name, c_address, c_phone, c_acctbal, ...])
```

`group_key_reduction.cpp` already inserts a `ProjectNode` above the rewritten
aggregate to restore the caller-visible column order; that projection is the
natural place to grow into this shape.

**Node placement is the whole proposal.** Putting the join immediately above the
`Aggregate` is the easy version and is probably not worth doing: it joins ~304K
aggregate rows against customer and still decodes most of those wide strings,
which eats the win. The prize exists only when the join runs against the 20 rows
that survive `head`. The rewrite is valid there whenever the sort keys do not
reference payload columns — in q10 the sort key is `revenue`, an aggregate.

Two existing mechanisms make this cheaper than it sounds:

- the uniqueness fact is already proved and already consumed by this same pass;
- dynamic filter pushdown (`plans/dynamic-filter-pushdown-plan.md`, Bloom/IN)
  can turn a 20-key re-fetch into a selective scan rather than a full customer
  decode.

## Gating

Do **not** gate this on a cost estimate. The deferred-probe join regressed q12
for exactly that reason ([[project_deferred_probe_no_cost_model]]), and a
payload re-fetch has the same failure shape: attractive on paper, negative when
the row count above the aggregate is large.

Gate **structurally** instead: apply the rewrite only when a `Head` with small
`n` sits above the aggregate and the intervening `Order` keys do not reference
any payload column. That is a pattern match rather than a guess, so it cannot
misfire on shapes it was never measured on. Widen only on measurement.

## Reach

Only two PDS-H queries have the shape (multi-column `by` with a proved FD, plus
a top-k):

| query | payload | expected |
|-------|---------|----------|
| q10 | 4 wide strings + Float64 (~130 B/row) | **-34 to -39%** (measured) |
| q03 | `o_orderdate` (Date) + `o_shippriority` (Int) | small — narrow fixed-width columns, little decode to save |

q03 is a correctness-of-generality check, not a justification. The prize is q10,
which at SF-10 on 16 cores is **4.87x** Polars streaming — the worst query in
the suite by a wide margin.

## What the implementation added to the design

**1. The re-sort above the join is mandatory, not optional.** A join's row order
is outside the contract (SPEC 5.6), so the re-fetch destroys the ordering the
top-k established: the right 20 rows come back in the wrong order. `apply_lift`
re-applies the `Order` keys above the join. It is k rows, so it costs nothing,
but without it the query is silently wrong in its ordering while its *set* of
rows stays correct — the kind of bug a set-comparison test would not catch.
`payload lift re-sorts after the re-fetch join` covers it.

**2. q03 does not qualify, for an upstream reason.** The plan predicted it
would. It does not, because the FD reduction never fires there: q03 profiles as
`aggregate keys=3 aggs=1`, and no uniqueness is proved for `o_orderkey`. So there
are no `first()` aggregates to lift. The lift is not the blocker; the uniqueness
proof is. Given q03's payload is a Date and an Int the prize was predicted small
anyway, but if someone wants it, the work is in the proof, not here.

**3. v1's single-source restriction costs ~3%.** q10's `n_name` is two joins
away (`customer` -> `nation`), so it stays a `first()` aggregate and the
aggregate ends at `keys=1 aggs=2` rather than `aggs=1`. Hand-measured, lifting it
too is worth about 3% more at eight cores. Not built.

**4. `fold_output` is what makes the join legal.** Both sides spell the key
`c_custkey`, which a plain join rejects as a collision. `JoinKey::fold_output`
folds it to one output column.

## Measurement note

The first suite A/B was run serially (`bench_ibex.py`, before then after) and
reported **q10 -2.9%**, with q18/q22/q16 apparently improving 10-12% and q04
regressing 7.4% — on queries whose plans the pass provably does not touch. All of
that was drift, exactly as [[project_bench_interleaved_methodology]] warns. The
interleaved paired A/B on the same machine minutes later gave -32.8% with 10/10
paired wins. **Do not trust a serial before/after here**; the effect is large and
the noise is larger than the effect on the queries that did not change.

## Risks and open questions

1. ~~**The re-fetch must not rebuild a big hash table.**~~ **RETIRED** — the
   built pass measures -32.8%, so whatever the re-fetch side costs, it is well
   inside the win. Worth revisiting only if a future plan shape makes the
   re-fetch scan non-selective.
2. ~~**The measured -34/-39% is an upper bound.**~~ **RETIRED** — the real
   rewrite, re-fetch included and output byte-identical, measures -29% at one
   core and -32.8% at eight. The upper bound was -34/-39%; the built thing lands
   just under it, as expected.
3. **Column order and nullability** must survive the new shape — the existing
   `ProjectNode` exists precisely because a positional caller (`write_csv`, an
   ascription) sees column order. A re-fetch join adds a second place that can
   permute or null-extend columns.
4. **Non-top-k shapes are out of scope** under the structural gate, which means
   a plain multi-key group-by over wide FD-determined columns keeps today's
   behaviour. That is deliberate; widening needs its own measurement.

## Refuted along the way — do not re-chase

`customer.parquet` has only **2 row groups** (row groups follow
`ceil(rows / 1048576)`, Arrow's default max row-group length; this explains all
four TPC-H tables). Decode units are row groups, so customer's decode caps at
two workers regardless of core count — a textbook match for
[[project_row_group_caps_parallel_width]] and for q10 getting *worse* on a
16-core box.

**It is not the binding constraint.** Rewriting `customer.parquet` with 16 row
groups moved 8-core wall from 0.57s to 0.56s, and made 1 core *worse*
(1.05 -> 1.19s) through per-group overhead. Do not re-open without new evidence.

## The other half of q10, deliberately not in this plan

q10 barely parallelises, and that is a separate and larger problem. Operator
`self_ms`, 1 core -> 8 cores:

| operator | 1c | 8c | speedup |
|----------|-----|-----|---------|
| `source decode whole` | 537.0 | 134.0 | 4.01x |
| `source decode selected` | 116.8 | 27.3 | 4.28x |
| **`join inner keys=1`** | **324.7** | 161.6 | **2.01x** |
| `join inner keys=1` (2nd) | 81.4 | 39.4 | 2.07x |
| **`Aggregate.Discovery`** | 70.6 | 40.9 | **1.73x** |
| `Aggregate.Emission` | 51.6 | 21.1 | 2.45x |
| TOTAL | 1277 | 568 | 2.25x |

Decode parallelises fine; the joins and the aggregate do not, and parallelism
*adds* 238ms of `barrier_wait` plus ~101ms of scan `ring_wait` that does not
exist at 1 core (`occupancy=0.334`, `serial_fraction=0.203`,
`amdahl_ceiling=4.93x`, 34 barriers).

This is why more cores made q10 worse on AWS: ibex's speedup went 2.69x (SF-8)
-> 2.04x (SF-10) while Polars streaming went 6.6x -> 9.2x. ibex single-threaded
is competitive on this query; the whole gap is scaling. See
[[project_scaling_is_the_whole_gap]] and `plans/breaker-map-plan.md`.

Kept out of this plan on purpose: it is diffuse, several attempts are already
recorded as reverted, and it needs a 16-core box to measure honestly. The
payload fix is local, verified, and independent of it.

## Reproduce

```sh
# both variants, identical top-20
sed 's/by { c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment }/by { c_custkey }/' \
    benchmarking/tpch/queries/q10.ibex > /tmp/q10_narrow.ibex

for f in benchmarking/tpch/queries/q10.ibex /tmp/q10_narrow.ibex; do
  for c in 1 8; do
    IBEX_CORES=$c taskset -c 0-$((c-1)) ./build-release/tools/ibex_eval \
        --plugin-path build-release/tools "$f"
  done
done

# where the time goes
IBEX_PROFILE_OPERATORS=1 IBEX_CORES=1 taskset -c 0 ./build-release/tools/ibex_eval \
    --plugin-path build-release/tools benchmarking/tpch/queries/q10.ibex
```

Related: [[project_q10_fd_payload_carried_through_pipeline]],
[[project_groupby_functional_dependency]],
[[project_high_cardinality_groupby_gap]], `plans/beat-polars-plan.md`,
`plans/breaker-map-plan.md`.
