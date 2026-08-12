# PDS-H performance status

Where Ibex stands against Polars on the 22 implemented PDS-H queries, what the
last round of work changed, and which levers are left. Written 2026-08-11 at
commit `f916e13`.

## How these numbers were taken

Local box, WSL2. **Pinned to 8 cores** (`taskset -c 0-7`, `IBEX_THREADS=8` /
`POLARS_MAX_THREADS=8`) — never 24, because Polars thrashes at that width on
this machine and inflates Ibex's relative standing by ~1.7×. Min of 5
iterations in one warm process per engine, whole-script mode (the batch planner
runs, matching how `ibex file.ibex` executes and how Polars' lazy API optimises
a whole plan). SF-1 and SF-2 from `benchmarking/data/tpch/parquet_sf{1,2}`.

Two standing traps when reproducing this:

- `benchmarking/data/tpch/parquet` is a **symlink the bench script flips per
  scale factor**. Pin explicit `parquet_sf<N>` paths in any A/B, or a stale
  symlink silently doubles every number.
- Run-to-run drift on this box is ~10% on the full suite. A single round
  resolves nothing under ~10%; three rounds resolve ~5%; ~5% effects need 5+
  interleaved rounds and ~3% effects needed 18. Several apparent findings this
  round evaporated at higher round counts.

## Standing

### SF-1 (min of 5, 8 cores)

| query | ibex | ibex-st | polars | polars-st | ibex/polars | st/st |
|---|---|---|---|---|---|---|
| q01 | 70.8 ms | 202.4 ms | 68.7 ms | 296.5 ms | 1.03x | 0.68x |
| q02 | 27.8 ms | 27.5 ms | 39.0 ms | 56.7 ms | 0.71x | 0.48x |
| q03 | 49.6 ms | 71.5 ms | 31.4 ms | 86.6 ms | 1.58x | 0.82x |
| q04 | 69.8 ms | 93.6 ms | 31.9 ms | 87.6 ms | 2.19x | 1.07x |
| q05 | 73.8 ms | 122.9 ms | 53.0 ms | 144.6 ms | 1.39x | 0.85x |
| q06 | 15.5 ms | 43.7 ms | 12.7 ms | 28.2 ms | 1.23x | 1.55x |
| q07 | 47.4 ms | 80.7 ms | 110.1 ms | 265.8 ms | 0.43x | 0.30x |
| q08 | 32.3 ms | 73.1 ms | 43.5 ms | 96.4 ms | 0.74x | 0.76x |
| q09 | 98.9 ms | 168.1 ms | 83.0 ms | 225.8 ms | 1.19x | 0.74x |
| q10 | 92.3 ms | 113.5 ms | 43.3 ms | 92.3 ms | 2.13x | 1.23x |
| q11 | 24.1 ms | 24.1 ms | 22.5 ms | 22.8 ms | 1.07x | 1.06x |
| q12 | 42.3 ms | 70.7 ms | 37.4 ms | 124.9 ms | 1.13x | 0.57x |
| q13 | 193.9 ms | 185.7 ms | 108.5 ms | 209.4 ms | 1.79x | 0.89x |
| q14 | 18.8 ms | 38.5 ms | 10.7 ms | 29.6 ms | 1.75x | 1.30x |
| q15 | 31.0 ms | 47.7 ms | 23.0 ms | 56.3 ms | 1.35x | 0.85x |
| q16 | 61.4 ms | 54.2 ms | 26.2 ms | 33.3 ms | 2.34x | 1.63x |
| q17 | 17.8 ms | 41.6 ms | 81.9 ms | 350.0 ms | 0.22x | 0.12x |
| q18 | 184.7 ms | 192.0 ms | 176.2 ms | 641.0 ms | 1.05x | 0.30x |
| q19 | 35.8 ms | 59.6 ms | 16.0 ms | 50.4 ms | 2.23x | 1.18x |
| q20 | 114.1 ms | 131.5 ms | 50.2 ms | 179.8 ms | 2.27x | 0.73x |
| q21 | 194.6 ms | 235.0 ms | 285.9 ms | 1.02 s | 0.68x | 0.23x |
| q22 | 33.1 ms | 36.5 ms | 26.2 ms | 41.4 ms | 1.26x | 0.88x |
| **total** | 1.53 s | 2.11 s | 1.38 s | 4.14 s | 1.11x | 0.51x |
| **geomean** | 52.4 ms | 77.6 ms | 44.2 ms | 109.8 ms | **1.19x** | **0.71x** |

MT gain: ibex 1.48×, polars 2.48×.

### SF-2 (min of 5, 8 cores)

| query | ibex | ibex-st | polars | polars-st | ibex/polars | st/st |
|---|---|---|---|---|---|---|
| q01 | 137.5 ms | 419.9 ms | 134.3 ms | 609.8 ms | 1.02x | 0.69x |
| q02 | 43.3 ms | 44.9 ms | 52.8 ms | 114.6 ms | 0.82x | 0.39x |
| q03 | 94.5 ms | 147.2 ms | 51.3 ms | 174.3 ms | 1.84x | 0.84x |
| q04 | 130.5 ms | 190.4 ms | 59.4 ms | 194.6 ms | 2.20x | 0.98x |
| q05 | 165.3 ms | 267.9 ms | 100.2 ms | 322.7 ms | 1.65x | 0.83x |
| q06 | 28.4 ms | 90.5 ms | 25.4 ms | 55.1 ms | 1.12x | 1.64x |
| q07 | 93.9 ms | 157.3 ms | 217.6 ms | 652.6 ms | 0.43x | 0.24x |
| q08 | 63.9 ms | 144.2 ms | 62.9 ms | 197.8 ms | 1.01x | 0.73x |
| q09 | 214.4 ms | 354.9 ms | 166.1 ms | 495.1 ms | 1.29x | 0.72x |
| q10 | 211.5 ms | 235.0 ms | 71.7 ms | 196.1 ms | 2.95x | 1.20x |
| q11 | 41.4 ms | 49.6 ms | 28.6 ms | 47.9 ms | 1.45x | 1.04x |
| q12 | 66.7 ms | 132.3 ms | 70.2 ms | 258.3 ms | 0.95x | 0.51x |
| q13 | 417.0 ms | 426.2 ms | 174.0 ms | 481.8 ms | 2.40x | 0.88x |
| q14 | 35.3 ms | 77.8 ms | 18.7 ms | 61.3 ms | 1.88x | 1.27x |
| q15 | 58.7 ms | 110.5 ms | 35.9 ms | 110.9 ms | 1.64x | 1.00x |
| q16 | 97.3 ms | 102.9 ms | 29.6 ms | 61.3 ms | 3.28x | 1.68x |
| q17 | 31.3 ms | 83.7 ms | 166.5 ms | 779.4 ms | 0.19x | 0.11x |
| q18 | 369.2 ms | 418.1 ms | 312.6 ms | 1.53 s | 1.18x | 0.27x |
| q19 | 57.5 ms | 123.8 ms | 28.0 ms | 103.1 ms | 2.06x | 1.20x |
| q20 | 224.5 ms | 286.1 ms | 104.7 ms | 436.7 ms | 2.14x | 0.66x |
| q21 | 434.1 ms | 529.0 ms | 629.3 ms | 2.47 s | 0.69x | 0.21x |
| q22 | 64.5 ms | 76.9 ms | 49.3 ms | 96.7 ms | 1.31x | 0.79x |
| **total** | 3.08 s | 4.47 s | 2.59 s | 9.45 s | 1.19x | 0.47x |
| **geomean** | 99.6 ms | 159.8 ms | 76.8 ms | 236.5 ms | **1.30x** | **0.68x** |

MT gain: ibex 1.60×, polars 3.08×.

## The shape of the gap

**Single-threaded we are ahead; multi-threaded we are behind.** st/st is 0.71×
at SF-1 and 0.68× at SF-2 — Ibex does ~1.4× less work per core. The whole
deficit is scaling: Polars converts 8 cores into 2.5–3.1×, we convert them into
1.5–1.6×.

That framing matters for where effort goes. Per-core efficiency is not the
problem and further micro-optimisation of serial kernels will not close it. It
also means the gap widens with core count, which is the case that matters for
large machines: at 8 cores we are 1.19× behind, at 192 the same scaling
difference is far more costly.

Second-order: the gap grows with scale factor (1.19× → 1.30×), concentrated in
q10 (2.13× → 2.95×), q16 (2.34× → 3.28×), and q13 (1.79× → 2.40×). Operator
profiles below show that these do not share one cause: q10 and q13 contain
expensive high-cardinality group discovery, while q16 is led by `distinct`.
Everything else is roughly scale-stable.

Where we are comfortably ahead: **q17 (0.22×/0.19×)**, **q07 (0.43×)**, **q21
(0.68×)**, q02, q08. These are the dynamic-filter-pushdown and semi-join
queries — work that was done and paid off.

## What changed this round

Seven commits, all multithreading. Cumulative effect on the suite geomean was
roughly −3.7% then −14%; ibex's MT gain went from 1.01× at the start of the
round to 1.48×/1.60×.

| commit | change |
|---|---|
| `f86465c` | Decode Parquet columns in parallel |
| `ac5713c` | Fan the hash group-by accumulate across workers |
| `c838a35` | Split the fused-bounds filter scan across row ranges |
| `ec0bf8b` | Drive the Parquet decoder from the ExecutionContext |
| `ec62911` | Gather across row ranges, not one row at a time |
| `ac60c6d` | Scan the fused key filter one row group per worker (q17 −28%, geomean −3.7%) |
| `f916e13` | Split fixed-width Parquet decode by row group (geomean −14%, 18/22 improve) |

Two lessons from that work are worth carrying forward:

**Our own pushdown competes with the parallelizer.** Discovered twice. Projection
pushdown and late materialization strip the join emit down to a single column,
so a column-axis parallel gather has nothing to split. Decode fusion leaves no
operator for the island planner to thread. Anything that removes work also
removes the axis a parallel version wanted.

**Queue indivisible tasks first.** When decode work was split by row group, the
mixed queue put q01's two whole-column dictionary decodes behind ~30 shard
tasks; they started last and set the finish time, making q01 6.5% *slower*.
Queueing the indivisible tasks first turned that into 13.7% faster. This
generalises to any mixed-granularity work queue.

## Operator profile (step 1)

The runtime now has an opt-in operator/source profiler. It records exclusive
operator build and `next()` time, inclusive main-thread span, source decode
time, and worker-pool task occupancy. It is off by default:

```sh
IBEX_PROFILE_OPERATORS=1 IBEX_THREADS=8 IBEX_PARALLEL=1 \
  build-release/tools/ibex benchmarking/tpch/queries/ibex/q20.ibex
```

`span_ms` is elapsed main-thread time including children; `build_self_ms`,
`next_self_ms`, and `source_self_ms` exclude nested profiled work.
`pool_work_ms` is summed worker occupancy and may exceed wall time. The profile
clock starts when the execution context is configured, so it covers lazy
decode and execution but not earlier schema/footer planning.

Each row also carries `occupancy` — `pool_work_ms / (span_ms × workers)` — the
share of the machine that operator kept busy while it ran. 0 means it was handed
no worker at all; 1 means it filled every one for its whole span. It turns the
binary "no worker work" observation into a number, so an operator that got
*partial* help stops looking like one that got none.

The header line summarises the plan:

```
operator profile: wall_ms=… entries=… workers=8 self_ms=… serial_self_ms=…
                  serial_fraction=… amdahl_ceiling=…x pool_work_ms=… occupancy=…
```

`serial_fraction` is the share of profiled main-thread work that drew no worker
help, and `amdahl_ceiling` is `1 / serial_fraction` — the speedup this query can
*ever* reach on unbounded cores. Both are computed from **self** time, never
from `span_ms`: spans are inclusive and nest, so summing them across rows
double-counts every parent, while self times are exclusive and add up.

Two honest limits. The serial classifier is binary — an operator that drew even
a little worker help counts as fully parallel — so `amdahl_ceiling` is
optimistic. And `wall_ms` under instrumentation runs well above the benchmark
timings (q10 reads 205 ms against a 92 ms benchmark), so treat the ratios as the
signal and the absolute times as attribution only.

Measured at SF-1, 8 workers:

| query | serial_fraction | amdahl_ceiling | occupancy |
|---|---:|---:|---:|
| q06 | 0.003 | 363x | 0.51 |
| q10 | 0.190 | 5.3x | 0.20 |
| q20 | 0.621 | 1.6x | 0.16 |
| q16 | 0.794 | 1.3x | 0.03 |
| q13 | 0.897 | **1.1x** | 0.02 |

This sharpens the ranking below considerably. **q13 and q16 cannot be rescued by
more cores at all** — at ceilings of 1.1× and 1.3×, the only route is removing
serial work, which is what items 2 and 4 propose. q20 at 1.6× is the same story
one step less severe. q10 is a different problem: it has 5.3× of headroom and
uses only 20% of the machine, so its issue is that the parallel parts are
inefficient rather than that too much is serial. And q06 — the query this
round's decode work targeted — comes out at 0.3% serial and 51% occupancy,
which is what "done" looks like.

One diagnostic trace at each scale factor gave the following leading serial
stages. These numbers are for attribution under instrumentation, not replacement
benchmark timings:

| query | stage | SF-1 | SF-2 | interpretation |
|---|---|---:|---:|---|
| q20 | 2-key aggregate | 96 ms | 239 ms | 544k/1.09m groups; group discovery remains serial |
| q13 | left-join build | 63 ms | 159 ms | emits 1.53m/3.07m rows before aggregation |
| q13 | 1-key aggregate | 37 ms | 87 ms | 150k/300k groups; no worker work |
| q10 | final inner join | 34 ms | 70 ms | serial join work after parallel decode |
| q10 | 7-key aggregate | 27 ms | 58 ms | high-cardinality generic key path; no worker work |
| q16 | `distinct` | 26 ms | 50 ms | larger than its final aggregate |
| q16 | 3-key aggregate | 9 ms | 15 ms | not the query's leading stage |
| q04 | semi-join | 22 ms | 45 ms | serial probe dominates after decode |

The existing group-by parallelism fans out only accumulation after group IDs
exist, and its high-cardinality gate keeps q10, q13, and q20 on the serial path.
That makes group discovery—not another decode pass—the clearest reusable
scaling target. Decode already generated substantial worker activity in four
of the five profiles; q13's whole-column decode was the notable indivisible
exception.

The default-off cost was checked against `f916e13` with 11 interleaved repeats
of the `core,scalar,pipeline` release suites pinned to one CPU: all 26 cases
were classified as noise (total target time −1.15%).

## Re-measured after parallel group discovery (commits `1d8b1a1`, `00abeae`)

Group discovery now hash-partitions rows across workers for the one- and
two-integer-key paths. Measured, SF-1, 8 cores, 6 interleaved rounds:
**q18 −20.0%, q20 −12.5%, q13 −10.0%**, everything else neutral.

| query | serial_fraction | amdahl_ceiling |
|---|---:|---:|
| q20 | 0.621 → **0.099** | 1.6x → **10.1x** |
| q18 | 0.897 → **0.106** | 1.1x → **9.5x** |
| q13 | 0.897 → 0.786 | 1.1x → 1.3x |

**This reorders what is left.** q13's aggregate is fixed, and its top item is no
longer the join. Its profile now reads:

| stage | self | pool_work | note |
|---|---:|---:|---|
| `source decode whole` | 126 ms | **0.000** | `o_comment` for the LIKE filter |
| `join left keys=1` (build) | 89 ms | 0.000 | emits 1.53m rows |
| `aggregate keys=1` | 26 ms | 66.8 ms | occupancy 0.263 — now threaded |

`pool_work=0.000` on the decode is not a rounding artefact: the column is a
string, strings are excluded from row-group splitting, so that decode is one
indivisible task and gets no worker at all.

### DONE: the filter-only column is never materialised (`2975559`)

Shipped, and it is the second option below rather than the first. See
"Measured, and what it cost" at the end of this section for the numbers — the
prize was NOT where the estimate below put it.

### The two options, as they looked beforehand

The largest single serial item left in q13, and `source decode whole` carries
40–126 ms of self time in eight of the 22 queries. Strings were excluded from
the row-group split because a shard's destination offset depends on the total
length of every preceding row.

Two ways to lift it, both costing one extra pass over the character data
(~20 ms for `o_comment`, against a 126 ms serial decode — so roughly
126 ms → 16 ms parallel + 20 ms copy):

- decode each row-group range into a local `Column<std::string>` and concatenate;
- or give each shard a disjoint char region sized from the footer's
  `total_uncompressed_size` upper bound, then compact.

Either needs a bulk block-append on `Column<std::string>` — a core public
header, so plugins must be rebuilt (ABI).

**But probably neither.** In q13 `o_comment` appears ONLY in the filter: ~160MB
of characters are materialised into a flat `Column<std::string>`, tested with
`!like(...)`, and discarded. The predicate result is one bit per row.

That matters beyond the wasted stores. Strings resist row-group splitting solely
because a shard's destination offset depends on every preceding row's length —
and a predicate result has no offsets. It is fixed-width, so it shards exactly
like the numeric columns: no concat, no compaction, no extra pass over the
character data, and no bulk-append on `Column<std::string>`, hence no core
header change and no plugin ABI break. All three costs above disappear.

Nor is it a new mechanism: `filtered_key_selection` already evaluates a
join-key filter as values leave the page decoder and emits row indices instead
of a column, and it is parallel by row group as of `1d8b1a1`. This is the same
shape with the compile-once `like` kernel.

To check first: (a) the demand analysis — this applies only when a column is
referenced SOLELY by scan conjuncts, which must be a real query against the plan
rather than an assumption; (b) `LazyTable`'s column cache, where poisoning is
already a recorded trap. And size the prize honestly: q13's filter passes ~99%
of rows, so this saves the MATERIALISATION, not the decode — measure that split
before assuming it is the whole 126 ms.

### Measured, and what it cost

**q13 165.2 ms → 135.8 ms (−17.8%)**, SF-1, 8 cores, 6 interleaved rounds, the
two ranges disjoint (old 151–181, new 129–146). Nothing else moves outside
run-to-run noise, which is the right shape: q13 is the only PDS-H query whose
filter column is both large and unread. (q02 and q09 also fuse — `p_type`,
`p_name` — but `part` is 200k rows.)

`o_comment` is UNCOMPRESSED PLAIN, 79MB of characters over two row groups. The
stage breakdown, from a `%`-only pattern against the real one:

| | CPU | span |
|---|---:|---:|
| `ReadBatch` alone (`like(c, "%")`) | 62 ms | 46 ms |
| `+ %special%requests%` matching | 114 ms | 84 ms |
| old: decode whole + `like` + gather | ~166 ms | ~166 ms (serial) |

**The estimate above was wrong about where the cost was.** Materialisation was
assumed to dominate; it is about 50 ms of CPU, while `ByteArrayReader::ReadBatch`
is 62 ms and the pattern matching is 52 ms — and matching is work the old path
did too. So roughly half the win is removing the `Column<std::string>` build and
half is the two-way row-group split that removing it made possible. The lesson
is the recurring one: `begin_bulk_append` had already made the string decode a
straight-line copy, so the thing that looked wasteful was no longer the
expensive part.

**Where the remaining time is.** The scan is now 62 ms of `ReadBatch` plus 52 ms
of matching. The matcher runs at ~1.4 GB/s: `LikeKind::Fragments` walks
`std::string_view::find` per fragment per value, which is memchr-then-memcmp
rather than a tuned `memmem`. That is worth perhaps 25 ms of CPU and it would
help the ordinary `like` column kernel too, which is a wider blast radius than
this scan. Not attempted.

**Parallelism is capped at 2 here.** `orders.parquet` has two row groups
(1048576 + 451424), so the split is 2-way and uneven — a 1.43x ceiling, and the
span tracks the larger group. A file written with more row groups would do
better; this is a property of the data, not the scan.

**Traps hit while building it.**

- Mutation-testing the Parquet header requires rebuilding **`ibex_eval`**, not
  `ibex_parquet_plugin`. `libs/parquet/backend.cpp` compiles the same
  `parquet.hpp` into the binary, and that is the copy an
  `extern fn read_parquet ... from "parquet.hpp"` actually runs. Two mutations
  looked "benign" for exactly this reason before the third one exposed it.
- Dictionary string columns are excluded deliberately. They decode to codes plus
  one small dictionary, so materialising them is cheap and matching them value by
  value would be *slower*. The gate is free: a dictionary column arrives as
  `Column<Categorical>` in the source schema, so testing for `Column<std::string>`
  excludes it without asking the plugin anything.
- No abandon rule, unlike the key scan. That escape hatch protects a
  *speculative* filter the caller may decline; this predicate is the query's
  own, so its answer is needed at any pass rate.

### Item 2 below is now q13's SECOND item, not its first

Fusing the join into the aggregation still removes ~95 ms (the 89 ms build plus
the 1.53m-row materialisation and the 6 ms update). It is the classic eager
aggregation rewrite — Yan & Larson, *Eager Aggregation and Lazy Aggregation*
(VLDB 1995) — pushing the group-by below the join: count orders per `o_custkey`
first, then join the 150k counts to customer. Correctness conditions are the
usual ones (group key must contain the join key, aggregate must be decomposable,
left-join non-matches must still produce 0), which makes it a riskier change
than the decode work and narrower in reach — in PDS-H it is essentially q13
alone.

## Open levers, ranked

### 1. Parallel high-cardinality group discovery — DONE for integer keys

The largest measured reusable item. q20 spends 239 ms at SF-2 discovering and
aggregating 1.09m two-column groups; q13 spends 87 ms on 300k groups; q10
spends 58 ms on its seven-key groups. The current parallel accumulate path
cannot help these cases because group-ID discovery is serial and the
high-cardinality merge gate disables the fan-out. A useful design must shard
group discovery itself and merge local group tables without serial work
proportional to every output group.

### 2. Fuse q13's join into aggregation

q13's SF-2 trace spends 159 ms building/emitting the left join and another 87
ms consuming its 3.07m-row output into 300k groups. A join-aggregate path that
counts matches per customer directly could avoid materialising that large
intermediate. This is more query-specific than item 1, but the measured span is
large and grows nearly linearly with scale.

### 3. Complete mixed-key fast paths for q10

q10's seven-key aggregate remains expensive even after group discovery is
addressed. Two distinct causes were diagnosed previously:

- **Fast-path coverage.** `ChunkedAggregateOperator` has fast paths for
  ungrouped, single-Cat, single-Str, single-Int and Int-pair keys. Anything
  *mixed* falls to a fully generic boxed-`Key` path. q10 (Int64 + Double + 5
  string-ish) and q16 (2 Cat + Int) both land there. Measured decomposition of
  q10's group-by: 2.9 ms for one int key, 5.7 ms for two numeric keys via the
  generic path, 29.4 ms for all seven — so ~2.8 ms is generic-path overhead and
  ~24 ms is per-row hashing, comparison and boxing of the string columns.
- **Redundant group keys.** q10 groups by 7 columns and gets the same 37,967
  groups as grouping by `c_custkey` alone: the other six are functionally
  dependent on it. Eliminating them is worth ~26 ms, ~28% of q10.

### 4. Target q16 `distinct` and q04's semi-join

The new trace rules out treating q16 as primarily a group-by problem:
`distinct` costs 50 ms at SF-2 versus 15 ms for the final three-key aggregate.
Its composite categorical path should be profiled below the operator boundary
before designing a fix. q04's 45 ms serial semi-join probe is similarly a
specific candidate. These are meaningful but less reusable than items 1–3.

### 5. Multithreaded scheduling across operators

We still convert 8 cores into only 1.5–1.6× while Polars gets 2.5–3.1×. Work so
far is mostly parallel within operators. Once the measured serial kernels above
are reduced, operator islands and their merge/concat cost remain the general
route to keeping workers occupied across a whole plan.

### 6. The decode-threading tax on small queries

q02, q13 and q16 are ~3% *slower* with threads on. Confirmed at 18 interleaved
rounds; it predates this round's work (it was 6–8% before) and is not a defect.
The decode itself is faster under threads even there; the loss is downstream,
consistent with decoded columns scattering across eight cores' caches while the
consuming join/group-by runs serially. Low value — ~3% on 3 of 22 queries.

## Dead ends — do not redo without new information

Each of these was implemented and measured, not reasoned about.

**Column-axis parallel join gather.** Measured exactly zero. Instrumentation
showed the join emits `cols=1` every time — even a deliberately 24-column-wide
join collapsed to one column, because pushdown and late materialization strip
the emit before the gather sees it. No threshold change helps.

**Footer-bytes speedup gate for decode threading.** Intended to fix the ~3% tax
by threading only when `total_task_bytes >= 2 * largest_task_bytes`. Measured
**+1.5% geomean worse**: it fires on queries where threading genuinely helps
(q19 +16.4%, q08 +9.0%) and does not fix the targets. Compressed bytes rank
*decode* cost badly — a well-compressed column is cheap to read and expensive to
decode. A working gate would need a cost model over decoded work (rows × type
width × encoding).

**Categorical code hashing in the generic group-by path.** Hashing and comparing
Categorical keys by dictionary code instead of text bought only 8% (37.4 →
34.2 ms) and **produced wrong answers on q16**, whose group keys come from a
join, so different chunks carry different dictionaries and codes are not
comparable. The single-Cat fast path gets away with raw codes only because it
relies on "dictionaries only grow and never reorder", which stops holding once a
join sits between the source and the group-by. A sound version needs
per-column dictionary-pointer tracking plus a rebuild of the hash index whenever
a chunk brings a new dictionary — real machinery for 8%.

Also worth recording: **q10's expensive key columns are not Categorical.**
`c_name`, `c_address`, `c_phone` and `c_comment` are `Column<std::string>`,
because their Parquet pages fall back to PLAIN once the dictionary overflows.
Only low-cardinality columns (`n_name` 25 entries, `p_brand` 25, `p_type` 150)
arrive as dictionaries. Any plan that assumes TPC-H strings are Categorical is
wrong.

**Functional-dependency group-key reduction (built, reverted).** The full
mechanism was implemented — `FunctionalDependency` on `TableProperties` with a
transitive closure, a `derive()` rule retiring a dependency when either end is
dropped/renamed/overwritten, join-side emission from the existing
`build_unique_` flag, and aggregate-side key reduction rewriting the dropped
keys as `first()` aggregates. It compiled, passed 22/22 answers and the full
test suite.

It was reverted because **the discovery point does not cover q10.** Tracing every
`build_index` call shows the three joins build on `nation(c_nationkey)`,
`orders(c_custkey)` and `cust_orders(o_orderkey)`. `customer` is *never* the
build side — the planner correctly indexes the smaller filtered `orders` (57,069
rows) and probes with `customer` (150,000) — so `customer.c_custkey`'s
uniqueness is never observed. The dependency that *is* discoverable
(`o_orderkey -> everything`) has a determinant that is not a group key, and
`o_orderkey -> c_name` does not yield `c_custkey -> c_name`.

Two bugs found while building it, worth knowing if it is rebuilt:

- The indexed side's key column is usually **not** in the join output — an
  equi-join emits one copy, from the other side — so the determinant must be
  looked up under the counterpart key's name.
- Nulls break the claim: two rows null in the determinant need not come from the
  same source row, so a dependency may only be claimed for a column with no
  validity bitmap. An inner join's own key is safe, since a null key matches
  nothing.

To make it fire, uniqueness must be observed elsewhere: on the **probe** side
(customer's keys are already hashed during the probe, but detecting duplicates
among them needs a set — ~150k inserts of speculative work that only pays off if
a downstream group-by uses it), or at the scan. That is a design fork needing
its own measurement.

## Correctness bar

Every change in this round held to the same bar, and anything landed here should:

- **22/22 PDS-H answers** via `benchmarking/tpch/check_answers.py`.
- **Single- vs multi-threaded outputs compared.** All 22 result CSVs are diffed
  at 1 and 8 threads. Exactly three files legitimately differ — q01, q09, q15 —
  from the known float-ordering difference in the parallel group-by reduction,
  which is deterministic across thread counts but differs from the serial
  summation order in the last ulp. Every other output must be byte-identical.
- **Full test suite** (currently 1540 tests) and the e2e script, which now includes two
  Parquet checks that each run single- and multi-threaded and require identical
  bytes.
