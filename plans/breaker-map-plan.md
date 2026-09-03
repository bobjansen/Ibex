# The breaker map: where the cores go

Status: **measured, nothing built** (2026-09-03, SF-8, `IBEX_CORES=8`, branch
`better-plans`).

Companion to [`src/runtime/PARALLELISM.md`](../src/runtime/PARALLELISM.md) (the
model) and [parallelism-overview.md](parallelism-overview.md) (the inconsistency
catalogue). Those two describe *how* fan-out works and *where it disagrees with
itself*. This file answers a different question, the one q01 forced:

> Not "how many workers should this operator get?" but **"what is the next
> blocking boundary, and can it be streamed, fused, or made independently
> parallel?"**

Ranking by query elapsed time cannot answer it — a breaker that is internally
parallel yet starves behind its producer looks identical to one that is fast.
So the ranking here is by **idle core-milliseconds attributed to the operator
that left them idle**.

---

## 1. The metric, and why it is an identity rather than an estimate

`IBEX_PROFILE_OPERATORS=1` prints one `profile node=` row per operator with
`self_ms` (exclusive time on the calling thread — the row's own work, children
excluded) and `pool_work_ms` (work that row's own fan-out ran on pool threads).
During one operator's exclusive window the pool offers `self_ms × workers`
core-milliseconds, so

```
idle_core_ms = self_ms × workers − pool_work_ms
```

is the capacity that operator left on the floor. Because the self times
partition the wall clock, **these sum to the query's `pool_unqueued_ms`** — the
pool's own measurement of capacity nobody asked for. The tool prints that
closure per query; a query whose closure drifts is a query whose attribution is
not to be believed (§5).

Two readings need saying out loud:

* **A negative row is not an error — it is the overlap succeeding.** A streaming
  scan whose decode runs *during its consumer's* window shows
  `pool_work > self × workers`. It donated capacity to another operator's
  window. q13's `source string filter scan` donates 1152 core-ms; q04's
  `scan __ibex_source_1` donates 399. Layer A is doing its job there.
* **`occ = pool_work / (self × workers)`** is the same number as a fraction, and
  it is the column to read second. `occ ≈ 0.5` with `ring_wait ≈ self` is a
  specific, recurring signature (§4.5), not a generic "half parallel".

The tool is [`benchmarking/breaker_map.py`](../benchmarking/breaker_map.py).
Like `profile_suite.py` it runs every query twice and parses only the second
(the first pays cold page cache, which otherwise makes every I/O-heavy query
look serial). It also carries the static half of the map — the label → family →
boundary-kind table below — so the classification is one definition rather than
a judgement re-made per reading.

---

## 2. Layer 1 — the static map: families and boundary kinds

Read from `physical_plan.hpp`, `PARALLELISM.md` §"Layer C", and the operator
TUs. `boundary` is a claim about the **family**, not about today's code — it is
the column the plan exists to argue about.

| Family | Operators | Boundary | Can it disappear? |
|---|---|---|---|
| **map** | filter, project, rename, row-local update, fused forms | `pipeline` | Already gone. Idle here is *starvation* — the producer, not the operator. |
| **scan** | `scan`, `source decode whole/selected`, `source dynamic key scan`, `source string filter scan` | `pipeline` | No boundary at all; idle here is decode width (§4.5). |
| **aggregate** | `Aggregate.Discovery` → `.Accumulation` → `.FinalOrdering` → `.Emission`, `aggregate keys=N` | `partial` (Discovery/Accumulation), `hard` (FinalOrdering), `pipeline` (Emission) | Partly. Worker-local state + one deterministic merge removes the per-chunk barrier; the *final* merge cannot go. |
| **join** | `join inner/semi/anti/cross keys=N` | `partial` | Partly. The probe streams; the build is a real boundary. Output assembly is neither and is where the time is. |
| **distinct** | `distinct` | `partial` | Partition ownership + deterministic merge. Already migrated; 0.3% of idle. |
| **order** | `order`, `sort` | `hard` | No. Parallel sort/gather, then stream the output. |
| **topk** | `head`, `tail`, `topk`, `FilterHead/Tail` | `hard`, bounded | No, and it does not matter: bounded serial state, 0.9 core-ms across the suite. |
| **window** | `rank`, `window`, reshape, materializing joins | `hard` | Usually no. Explicit materialization, isolated scheduling. |

The point of the `boundary` column is that **`pipeline` and `hard` idle are not
the same debt.** Idle attributed to a `pipeline` operator is always somebody
else's fault (a producer that could not fill it); idle attributed to a `hard`
one is the cost of the semantics. Only `partial` idle is a boundary that
mechanism can remove — and it is 63% of the total.

---

## 3. Layer 2 — the measured map

PDS-H at SF-8, 8 cores, `build-release`, second of two runs, quiet box.
Capacity is `wall × 8 = 49,104` core-ms; **23,906 of it (48.7%) is idle.**
Packing the measured work perfectly onto 8 cores would put the suite at
**~3.15s against today's 6.14s — a 1.95× ceiling**, which is the whole prize
and the number every candidate below should be sized against.

### By family

| Family | idle core-ms | share |
|---|---:|---:|
| join | 10,642 | 44.5% |
| aggregate | 7,654 | 32.0% |
| scan | 4,102 | 17.2% |
| map | 1,290 | 5.4% |
| order | 147 | 0.6% |
| distinct | 71 | 0.3% |
| topk | 1 | 0.0% |

### By boundary kind

| Boundary | idle core-ms | share |
|---|---:|---:|
| `partial` — mechanism can remove it | 15,063 | 63.0% |
| `pipeline` — starvation, not a barrier | 7,234 | 30.3% |
| `hard` — the semantics cost this | 1,610 | 6.7% |

### The top boundaries, ranked

`occ` is that operator's own occupancy during its own window.

| idle | query | operator | self | pool | occ | barrier | ring |
|---:|:--|:--|---:|---:|---:|---:|---:|
| 1975 | q04 | `join semi keys=1` | 268.8 | 175.2 | **0.08** | 23.3 | 85.6 |
| 1683 | q13 | `aggregate keys=1 aggs=1` | 268.1 | 461.9 | 0.22 | 72.0 | 0.0 |
| 1303 | q21 | `join semi keys=1` | 271.9 | 871.9 | 0.40 | 116.7 | 0.0 |
| 1150 | q21 | `Aggregate.Discovery` | 164.4 | 165.8 | 0.13 | 27.3 | 0.0 |
| 847 | q10 | `join inner keys=1` | 146.2 | 322.4 | 0.28 | 85.1 | 0.0 |
| 804 | q22 | `join anti keys=1` | 107.0 | 52.2 | **0.06** | 8.0 | 0.0 |
| 798 | q21 | `source decode whole` | 192.4 | 740.4 | 0.48 | 93.3 | 0.0 |
| 743 | q18 | `Aggregate.FinalOrdering` | 252.5 | 1277.3 | 0.63 | 193.8 | 0.0 |
| 700 | q19 | `join inner keys=1` | 126.5 | 312.4 | 0.31 | 51.7 | 58.0 |
| 699 | q01 | `Aggregate.Accumulation` | 153.6 | 529.6 | 0.43 | 150.0 | 0.0 |
| 662 | q12 | `join inner keys=1` | 95.7 | 103.7 | 0.14 | 15.6 | 18.5 |
| 651 | q21 | `Aggregate.Emission` | 105.0 | 188.7 | 0.22 | 104.9 | 0.0 |
| 638 | q18 | `Aggregate.Emission` | 97.4 | 141.4 | 0.18 | 91.4 | 0.0 |
| 550 | q10 | `join inner keys=1` | 80.5 | 93.4 | 0.15 | 25.0 | 23.5 |
| 519 | q01 | `update` | 114.1 | 393.4 | 0.43 | 106.2 | 0.0 |
| 500 | q12 | `scan __ibex_source_1` | 125.9 | 507.3 | 0.50 | 0.0 | **125.4** |
| 481 | q10 | `scan __ibex_source_0` | 70.5 | 83.0 | 0.15 | 0.0 | **70.4** |
| 463 | q06 | `scan __ibex_source_0` | 115.1 | 458.1 | 0.50 | 0.0 | **115.0** |
| 422 | q20 | `Aggregate.FinalOrdering` | 173.5 | 966.0 | 0.70 | 145.4 | 0.0 |
| 411 | q21 | `filter` | 96.6 | 361.7 | 0.47 | 45.3 | 0.0 |
| 388 | q20 | `scan __ibex_source_2` | 96.8 | 386.6 | 0.50 | 0.0 | **96.5** |
| 363 | q01 | `Aggregate.Discovery` | 45.3 | 0.0 | **0.00** | 0.0 | 0.0 |

Full per-query detail: `python benchmarking/breaker_map.py 8`.

---

## 4. What the map says — five findings, in rank order

### 4.1 The semi/anti join build is essentially serial — 4,418 core-ms (18.5% of all idle)

q04 `join semi keys=1` at **occ 0.08**, q22 `join anti keys=1` at **occ 0.06**,
q21's two semi joins at 0.40 and 0.56. This is the single largest recoverable
block in the suite and it is one operator: `semi_anti_join.cpp`.

The probe half was parallelized (`a69d01d9` + `b07ebdd5` + `e7c7c2c6`, q21
−34%). The **build** half was not, and at SF-8 it is what is left:

* the left side is drained chunk-by-chunk into a chunk list on the calling
  thread (`init_int_swapped`);
* the dense path marks a `candidates` bitmap over **every left key**, serially,
  before the parallel right-side scan starts — 12M orders rows for q04;
* the hash path builds a `robin_hood` map of every left key serially
  (`seen.try_emplace`, one insert per left row);
* the hit merge and the final `right_i64_.insert` reconstruction are serial
  loops over the slot space.

**Measured, not inferred.** q04's `join semi keys=1` row reports
`build_self_ms=172.069` with `pool_work` attributable to the build of **zero**,
and `barriers=1 pool_tasks=8` for the *entire* operator — one fan-out, of eight
tasks, in a 261ms operator, and it happens during `next()`, not the build. The
build phase is 172ms of one thread while seven sit idle. (The `next()` half is a
different problem: `ring_wait_ms=89.153` against `next_self_ms=89.293` — the
probe is parked on its producer for 100% of its window, the §4.5 signature.)

The reason is a live footgun: `intersect_worker_count` caps workers by
`kSlotBudgetBytes / slots` (8 MB of private per-worker slot bytes). Its comment
reasons about a 57k-key left — a scale where the cap never binds. q04's left is
12M orders rows over a sparse orderkey span, so the dense bitmap alone is
megabytes and the cap collapses the one parallel phase to a **single worker**
(`workers < 2` → run it here) exactly when the left is largest, which is the
opposite of what is wanted. That is a break-even measured at one scale and
applied at another.

*Treatment:* hash-partition the left key space, so candidate marking, the right
scan, the hit merge, and the set reconstruction are all per-partition and
private — the same private-state-then-merge idiom the aggregate already uses,
with the byte budget becoming per-partition rather than per-worker.

### 4.2 Hash-aggregate `FinalOrdering` + `Emission` — 3,251 core-ms (13.6%) — Emission BUILT

**Status: Emission done (2026-09-03, uncommitted). `FinalOrdering` untouched.**

q18 (743 + 638), q21 (272 + 651 + 202 + 24), q20 (422 + 217), q10 (104). These
are the two phases *after* accumulation: `finalize_owned_active()` transfers
partition-local group state into deterministic first-occurrence order, and
`build_output_chunk()` materializes the result columns. Emission runs **once,
at the end, on the calling thread** — q18 spends 97ms and q21 105ms there with
occ 0.18–0.22, and the barrier wait next to it (91ms, 105ms) says the pool is
sitting still through it.

*Diagnosis, once built:* the emit was **already** fanned out — one task per
output **column** — and that was the whole problem. `threads =
min(n_out_columns, worker_cap)`, and the shapes it is slowest on are the
narrowest: an Int64 key plus one aggregate is **two columns, so two workers**,
at millions of groups. The measured ~1.8 effective workers was the cap, not
imbalance.

*Treatment (built):* `(output column × group range)` tasks over a pre-sized
destination — the same split, and the same 64-wide alignment for bit-packed
validity, that `gather_columns_batched` already uses. A range-writable column
is sized once on the building thread and its buffer address resolved there
(`data()`/`codes_data()` detach a shared buffer, which is not a thing to do
from a worker); each task then writes its groups by **index**, so the emitted
order is the group order whichever worker ran which range. Variable-width
output (strings), the generic `Key` path and `CountDistinct` keep the append
path as one whole-column task, exactly as `gather_columns_batched` keeps string
columns indivisible. The plan vocabulary gained `PartitionStrategy::ColumnRange`
so `explain physical` states what the phase actually does.

One serial tail came with it: the "did any group come out null" check ran
bit-at-a-time over every group, once per aggregate. Now word-wise.

*Measured*, SF-8 / 8 cores, `perf`-free interleaved min-wall, base and new
binaries built from the same tree:

| | self | pool | barrier wait | emission core-ms |
|---|---:|---:|---:|---:|
| q18 Emission before | 97.4 | 141.4 | 91.4 | 239 |
| q18 Emission after | 56.5 | 74.9 | **10.0** | 131 |
| q21 Emission before | 105.0 | 188.7 | 104.9 | 294 |
| q21 Emission after | 74.9 | 80.9 | **9.2** | 156 |

The barrier-wait collapse is the mechanism landing: the caller no longer waits
on a two-worker batch. Emission's absolute work also fell ~45% — indexed writes
into a pre-sized buffer skip the per-append capacity check and CoW probe.

Wall clock, two full-suite interleaved runs (9 and 7 reps): **suite −1.6% and
−2.3%** on summed per-query minimum. **q18 is the only query whose own move
clears the noise floor**, at −5.2% / −7.7% / −11.8% min across three runs.
q21, q09 and q12 each swung both directions between runs — ±13% per query is
the documented noise floor on this box and they are inside it.

Verification: all 22 query outputs **byte-identical** to the pre-change binary
at 8 cores; 1837 tests pass; a new test covers the shape the change is for
(one key + one aggregate, 70,003 groups, so tasks far outnumber columns) and
asserts a **hand-computed** answer rather than serial-vs-parallel agreement —
the two now share one task grid, so an index wrong in both would agree with
itself. Mutation-checked: an off-by-one within a range fails it.

### 4.3 `Aggregate.Discovery` is serial at low cardinality — 1,835 core-ms (7.7%)

q01 is the pure case and it is stark: **45.3ms of self time, `pool_work` exactly
0.000, occupancy 0.00** — to discover **four groups**. q15 (249), q11 (102) and
q21 (1150) are the same shape at other cardinalities.

This is the case from the previous session, and the map confirms the diagnosis
while re-ranking it third rather than first. q01's whole aggregate — Discovery
363 + Accumulation 699 — is 1,062 core-ms, and the `update` beneath it
contributes another 519 through the per-chunk barrier (46 chunks, 106ms of
barrier wait, `barriers=92`). The shape to build is the one already sketched:

```
scan + filter + derived columns
  → worker-local {returnflag, linestatus} aggregate state
  → one deterministic merge of ~4 groups at the end
  → order/output
```

i.e. the low-cardinality categorical analogue of the partition-owned path that
already exists for high-cardinality integer keys. It removes 46 cycles of "scan
workers occupy the pool → aggregate submits a batch → aggregate waits", which is
what the 150ms of Accumulation barrier wait *is*.

### 4.4 Inner-join probe and output assembly — ~3,100 core-ms

q10 (847 + 550), q19 (700), q12 (662), q09 (344), q07, q05, q03. Occupancies
cluster at 0.14–0.31 with substantial `barrier` **and** `ring` wait on the same
row (q19: 51.7 barrier + 58.0 ring; q12: 15.6 + 18.5) — the signature of a probe
that is internally parallel but fed by a producer that cannot keep up, *and*
then rejoins. `assemble_output` dominating is a known finding
(`project_join_parallelism`); the map now sizes it against everything else.

Deliberately ranked below §4.1–4.3 despite the family total: it is spread across
seven queries with no single dominant operator, so it is many small slices
rather than one mechanism.

### 4.5 The `occ ≈ 0.50, ring_wait ≈ self` cluster is decode width, not a breaker

q12 (self 125.9 / ring 125.4), q06 (115.1 / 115.0), q20 (96.8 / 96.5), q18
(85.2 / 84.9), q10 (70.5 / 70.4). The consumer spends **its entire self time
parked on the ring**, and the producer runs at exactly half the machine. This is
`project_row_group_caps_parallel_width` measured from the other side: decode
units are row groups, ~4 are in flight, and the lever is row-group **size at
write time**, not a worker count. 2,200-odd core-ms sits here and **no breaker
change can recover it** — which is precisely why it is worth having the map say
so before somebody spends a week on the scan.

---

## 5. What the map does not say

* **Small-query attribution is soft.** Per-query closure runs 95–104% on the
  fifteen queries over ~100ms, but q11 reads 341%, q22 125%, q15 120%, q02 118%
  and q13 64%. Sub-30ms queries have thread-startup latency measured against a
  near-zero window (the `pool_unqueued_ms` caveat already documented in
  `execution_profile.cpp`), and q13's overcorrection comes from its
  `string_filter_scan` donating 1,152 core-ms into other operators' windows.
  **Rank on the large queries; treat q11/q15/q02 as directional.**
* **It measures idle, not waste.** An operator doing 4.6× the necessary work at
  perfect occupancy is invisible here — that is what
  `project_task_clock_finds_multiplied_work` is for. The two are complementary
  and neither substitutes for the other.
* **It is one scale factor.** Every threshold in §4.1 exists because somebody
  measured a break-even at SF-1 or SF-2. Re-run at SF-2 before believing a
  ranking transfers; the tool takes a `--queries=` filter for exactly that.
* **It does not model edges between operators.** The producer/consumer
  relationship is inferred from `ring_wait` and from reading the query, not from
  a recorded graph. Making the physical plan emit its own edges (so the map
  reads the graph instead of guessing it) is the natural next slice, and is
  where "parallelism as a plan decision" would take it.

---

## 6. Order of attack

1. ~~**`Aggregate.Emission` parallel gather** (§4.2)~~ — **done**. Suite
   −1.6…−2.3%, q18 −5…−12%, byte-identical. Smaller than the 3.7%-of-capacity
   the map priced it at, which is the expected shape: recovering idle capacity
   at one boundary hands it to the next one down.
2. **Semi/anti build partitioning** (§4.1) — largest win outright, and fixes a
   threshold that currently misfires at scale. Check `intersect_worker_count`
   against SF-8 first: if it is collapsing to one worker on q04, that alone is
   a measurable slice before any restructuring.
3. **Worker-local low-cardinality aggregate** (§4.3) — q01 is the validation
   case, and the one where the mechanism is already fully sketched.
4. **`Aggregate.FinalOrdering`** (§4.2's other half, 1,461 core-ms) — untouched,
   and the harder half: it already runs at occupancy 0.63–0.70, so the headroom
   is real but thinner than Emission's was.
5. Re-measure the map. Every item above moves capacity between rows; the
   ranking after step 3 is not the ranking now.

The map is regenerated, not maintained by hand:

```
python benchmarking/breaker_map.py 8              # whole suite
python benchmarking/breaker_map.py 8 --queries=q01,q04,q21
```
