# Beating Polars and DuckDB multi-core

Status: **ongoing umbrella plan, created 2026-10-04** by merging
`beat-polars-plan.md` and `beat-duckdb-plan.md`. Their full text, with the
measurement history this file leaves out, is at
`git show 7a32d537:plans/beat-polars-plan.md` and
`git show 7a32d537:plans/beat-duckdb-plan.md` (the August Polars plan is at
`git show e82f679d:plans/beat-polars-plan.md`). Mechanism lives in
`src/runtime/CONTRACTS.md` and `src/runtime/PARALLELISM.md`; the retired
owned-aggregate and runtime-multithreading plans are at
`git show 587bc2e4:plans/owned-agg-per-chunk-barrier-plan.md` and
`git show 587bc2e4:plans/runtime-multithreading-plan.md`.

**The benchmark write-up waits for this plan's first milestone.** Ibex is the
fastest of the three engines on 1–4 cores, ties at 8 and loses at 16 (§1.0); a
launch post would invite exactly the comparison it does not yet win.

## 0. Target

**References: Polars' streaming executor and DuckDB, never Polars in-memory.**
The harness defaulted to in-memory until `7a32d537`. The 2026-09-27 SF-10 run
took that default and read Ibex 0.64× Polars at 16 cores; against streaming
Ibex was 1.36× behind (2026-09-24), while Ibex/DuckDB barely moved between the
two runs (8c 1.09 → 1.02, 16c 1.22 → 1.17). The runs also differ in data and
scale (§1.1), so this is indicative, but nearly all of the swing is the
executor, not Ibex. §1.0 confirms it: Ibex/Polars-streaming 1.09 at 16 cores
where the in-memory column had read 0.64. DuckDB is in every run and is the control
for any large swing.

**Milestone 1 (the publication gate):** PDS-H SF-10 on AWS physical cores
(`r7i.8xlarge`, `--threads-per-core 1`), same sitting, all three engines:

- suite total and geomean at or below **1.0 against both** references at
  **8 cores**, and also without q21;
- still ahead of both on **1 core** (a soft floor: a small 1-core cost is fine
  for a clear 8-core win; report it, do not veto on it);
- **2 cores not a loss** against either.

**Milestone 2:** the same at 16 cores.

## 1. Baseline

### 1.0 The first clean three-engine run (2026-10-04)

`run-tpch.sh --on-demand --type r7i.8xlarge --threads-per-core 1 --sf 10
--cores 2,4,8,16 --no-polars-in-memory` on `1bfeceb5` (clean tree), Polars
1.42.1 streaming, DuckDB 1.5.4, polars-benchmark data; all 22 answers match
Polars at every core count. Artifact
`benchmarking/results/tpch_aws_20261004T100951.tar.gz`. Mean of 5 warm
iterations; the 1-core rows repeat in each pass and agree within 1%.

| cores | Ibex total | Polars-st | DuckDB | Ibex/Polars | Ibex/DuckDB | geomean I/P | geomean I/D |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 50.5 s | 67.2 s | 56.8 s | 0.75 | 0.89 | 0.78 | 0.86 |
| 2 | 24.8 s | 34.3 s | 30.4 s | 0.72 | 0.82 | 0.78 | 0.80 |
| 4 | 13.8 s | 17.2 s | 15.5 s | 0.80 | 0.89 | 0.86 | 0.87 |
| 8 | 8.35 s | 8.86 s | 8.14 s | **0.94** | **1.03** | **1.00** | **0.99** |
| 8, no q21 | 7.36 s | 7.18 s | 7.10 s | 1.03 | 1.04 | 1.02 | 1.00 |
| 16 | 5.39 s | 4.95 s | 4.49 s | 1.09 | 1.20 | 1.13 | 1.15 |

Speedup over 1 core at 8 / 16: Ibex 6.0× / 9.4×, Polars 7.6× / 13.6×, DuckDB
7.0× / 12.6×. Implied parallel fraction (8c and 16c agree): Ibex **95.3%**,
Polars 98.9%, DuckDB 98.1%. Up from Ibex ~88% on 2026-09-24.

**Milestone 1 status: 1 core and 2 cores pass; 8 cores is a near miss.** Ibex
beats Polars on the 8-core total and ties both on geomean, but trails DuckDB
by 3% on the total and both by 2–4% without q21. Tying needs ~260 ms off the
no-q21 total at 8 cores; a claim that survives sitting drift (§4) wants ~0.95,
about **600 ms**.

**Per query at 8 cores, against the faster reference** (ms, mean; "1c" is
Ibex against the faster reference on one core; "x8" the 8-core speedups
Ibex / Polars / DuckDB):

| query | Ibex | Polars | DuckDB | lost | ratio | 1c | x8 |
|---|---:|---:|---:|---:|---:|---:|---|
| q01 | 623 | 610 | 495 | +128 | 1.26 | 1.05 | 6.0 / 7.4 / 7.3 |
| q19 | 433 | 315 | 358 | +117 | 1.37 | 1.01 | 5.6 / 7.6 / 7.4 |
| q14 | 333 | 242 | 283 | +91 | 1.38 | 1.06 | 5.9 / 7.7 / 7.1 |
| q12 | 310 | 235 | 252 | +75 | 1.32 | 0.75 | **4.2** / 7.5 / 6.9 |
| q10 | 505 | 435 | 431 | +74 | 1.17 | 0.91 | 5.3 / 7.6 / 6.9 |
| q15 | 286 | 217 | 268 | +69 | 1.32 | 1.09 | 6.3 / 7.6 / 6.6 |
| q04 | 281 | 342 | 215 | +67 | 1.31 | 1.03 | 5.0 / 6.3 / 6.3 |
| q06 | 231 | 168 | 247 | +63 | 1.37 | 1.11 | 6.0 / 7.4 / 7.4 |
| q09 | 819 | 870 | 756 | +62 | 1.08 | 0.85 | 5.7 / 7.5 / 7.3 |
| q03 | 416 | 388 | 355 | +62 | 1.17 | 0.94 | 5.6 / 7.9 / 7.0 |
| q07 | 498 | 443 | 454 | +56 | 1.13 | 0.83 | 5.6 / 7.7 / 7.5 |
| q05 | 464 | 435 | 421 | +44 | 1.10 | 0.99 | 6.2 / 7.8 / 6.9 |
| q16 | 116 | 95 | 119 | +21 | 1.23 | 0.88 | **3.6** / 5.8 / 3.9 |
| q11 | 50 | 73 | 42 | +7 | 1.17 | 1.08 | 5.0 / 6.8 / 5.4 |

Ties or wins: q20 (1.00), q08, q02, q13, q17, q22 (0.76), q21 (0.95), q18
(0.75, −134 ms). Lost 936 ms on 14 queries, won 293 ms back on 8.

What the table says:

- **The lineitem scan-filter family is two thirds of the 8-core loss.** q01,
  q19, q14, q15, q04, q06 and q12 are a date-range or predicate filter over
  lineitem, then an aggregate or a small join: 610 of 936 ms. Six of them are
  also **per-core losers** (1c 1.01–1.11). Polars is the faster reference on
  q19/q14/q15/q12/q06, DuckDB on q01/q04. q06 (scan, filter, sum) is the
  purest probe of the family.
- **q12 and q16 are the scaling outliers**: q12 wins on one core (0.75) and
  scales 4.2× against 7.5×; q16 3.6× against 5.8×.
- **The deferred-probe joins (q03, q05, q07, q09) cost 224 ms** while winning
  on one core: a scaling gap, ~5.7× against ~7.5×.
- **At 16 cores the order changes:** q21 becomes the top loser (+149 ms, 1.25
  against DuckDB), then q01 +111, q10 +104, q19 +97, q14 +78, q12 +76, q03
  +76. Lost 1,175 ms on 18 queries. Milestone 2 needs q21 back.

### 1.0b After consumer-helps and the left-scan filter (2026-10-05)

Same command and box type on `4d628abe` (code as of `c4084d23` plus the tidy
fix `b43fea81`; the bench harness fix `4d628abe` restored the DuckDB column).
All 22 answers match. Artifact
`benchmarking/results/tpch_aws_20261005T071555.tar.gz`.

| cores | Ibex total | Polars-st | DuckDB | Ibex/Polars | Ibex/DuckDB | geomean I/P | geomean I/D |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 (8c pass) | 56.0 s | 78.0 s | 62.6 s | 0.72 | 0.89 | 0.75 | 0.86 |
| 2 | 25.3 s | 34.5 s | 30.8 s | 0.73 | 0.82 | 0.78 | 0.81 |
| 4 | 14.1 s | 17.8 s | 15.7 s | 0.79 | 0.90 | 0.84 | 0.88 |
| 8 | 8.95 s | 9.88 s | 9.05 s | **0.91** | **0.99** | **0.95** | **0.96** |
| 8, no q21 | 7.84 s | 8.00 s | 7.87 s | **0.98** | **1.00** (0.996) | **0.98** | **0.96** |
| 16 | 5.53 s | 5.10 s | 4.57 s | 1.08 | 1.21 | 1.11 | 1.14 |

**Milestone 1: met, confirmed — but against DuckDB without q21 it is a tie,
not a margin.** Every criterion holds at 8 cores, with and without q21; 1 core
is still ahead (0.72 / 0.89), 2 cores is no loss. The 8-core pass above ran on
a slow stretch (its own 1-core rows ~10% slower for all three engines), so a
second sitting ran two more 8-core passes (`--cores 8,8`, same commit, new box;
`benchmarking/results/tpch_aws_20261005T091728.tar.gz`):

| 8 cores | Ibex | Polars-st | DuckDB | I/P | I/D | gm I/P | gm I/D | no q21 I/P | no q21 I/D |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| run 1 pass (above) | 8.95 s | 9.88 s | 9.05 s | 0.91 | 0.99 | 0.95 | 0.96 | 0.98 | 0.996 |
| confirmation pass 1 | 7.85 s | 8.63 s | 7.91 s | 0.91 | 0.99 | 0.95 | 0.95 | 0.98 | 0.996 |
| confirmation pass 2 | 7.86 s | 8.61 s | 7.92 s | 0.91 | 0.99 | 0.96 | 0.95 | 0.99 | 0.997 |

Three passes on two boxes agree to the second decimal, so the ratios are
stable even though absolute times moved 12% between boxes. Against Polars the
milestone holds with room (0.91 total, 0.98 without q21). Against DuckDB the
total is 0.99 and the no-q21 total 0.996–0.997: under 1.0 on every pass, but
by about 25 ms of 6.9 s. §1.0 puts a claim that survives sitting drift (§4) at
~0.95: that is ~300 ms more off the 8-core no-q21 total, and §3's top losers
(q19, q01, q03, q10, q14) are where it is.

Moved at 8 cores (Ibex/Polars): q12 1.32 → **0.85** (310 → 224 ms, §3 item
2); the scan family from consumer-helps (§3 item 1), q06 1.37 → 1.22, q14
1.38 → 1.26, q15 1.32 → 1.23, q19 1.37 → 1.30, q01 1.02 → 0.96, q04 0.82 →
0.75. The rest within a few percent. Lost 756 ms on 13 queries against the
faster reference (was 936 on 14), won 364 back (was 293). Top losers now:
q19 +105, q01 +99, q03 +73, q10 +72, q14 +69, q04 +61, q07 +56, q15 +54.

At 16 cores (milestone 2) the totals barely moved (1.08 / 1.21, was 1.09 /
1.20): q12 and the scan family improved, q03, q16, q18 and q20 drifted the
other way. Lost 1,188 ms on 19 queries; q21 +192, q10 +113, q01 +102, q03
+98, q09 +91, q07 +83.

### 1.1 Earlier numbers, and what each is still good for

| run | data | Polars column | still valid for |
|---|---|---|---|
| 2026-09-24, SF-8, `461c0963`, 2–16 cores | **Ibex-written** Parquet (1Mi-row groups, uncompressed) | streaming | the shape of the curve; serial-time profile (§2) |
| 2026-09-27, SF-10, `546ce652`, 8 and 16 cores | polars-benchmark's own | **in-memory (invalid)** | Ibex/DuckDB only |

Data provenance matters: from `f69f26d4` (2026-09-25) every run reads the
tables polars-benchmark generates (tpchgen-cli, Polars' writer: 122,880-row row
groups, ZSTD, no page encoding stats, `l_quantity` Int64). That switch exposed
two single-core losses the old files hid, both fixed: the Int64-sum gate on
q18's async hot aggregate, and categorical recovery for stats-less dictionary
columns (−20.5% suite at 8 cores). Never compare across that line.

**2026-09-24 (SF-8, Ibex-written data), totals:**

| cores | Ibex/Polars-streaming | Ibex/DuckDB |
|---:|---:|---:|
| 1 | 0.61 | 0.82 |
| 2 | 0.67 | 0.77 |
| 4 | 0.78 | 0.86 |
| 8 | 1.08 | 1.09 |
| 12 | 1.26 | 1.22 |
| 16 | 1.36 | 1.22 |

Implied parallel fraction at 16 cores: Ibex ~88%, Polars ~98%; both follow
Amdahl from 8 to 16. Parity at 16 against Polars needed ~93%.

**2026-09-27 (SF-10), Ibex/DuckDB:** 1 core 0.86, 8 cores 1.02 (geomean 1.00),
16 cores 1.17 (geomean 1.13); implied fraction 95.0% against DuckDB's 98.1%.
At 16 cores Ibex lost 971 ms on 17 queries and won 138 ms back on 5. Largest
losses (ms): q01 118, q10 111, q21 91, q03 82, q04 73, q19 72, q12 67, q14 63,
q07 60, q09 55. Wins to keep: q17 (0.66), q02 (0.54), q22, q18, q16. **13 of
the 17 queries Ibex lost at 16 cores it won or tied on one core**; the
exceptions, slower per core too, were q01 (1.08), q04 (1.03) and q11 (1.04).

## 2. Where the gap is

- **Scaling, not per-core speed.** Both references scale 12–15× at 16 cores
  on most queries; Ibex 6–11×. The widest spreads are on queries Ibex wins on
  one core (q12, q10, q19, q14, q20, q07, q03).
- **Serial time is the Amdahl term, flat with cores** (AWS profile,
  2026-09-24, `run-tpch.sh --profile`): serial self time 1,833 ms at 8 cores,
  1,869 at 16, **39% of wall at 16**. Perfect scheduling of the parallel work
  that exists buys only 17%; parity needed ~26%. **Serial work has to become
  parallel; scheduling alone cannot close it.** By query: q21 708 ms (38% of
  all serial time), q13 182, q10 182, q18 113, q01 92, q07 84, q03 75, q20 72,
  q05 68, q09 59. The top five are 68%.
- **Idle core-ms by operator family** (breaker map, AWS 16c): join 41%, scan
  27%, aggregate 26%, map 5%. `benchmarking/breaker_map.py` regenerates it.
- **Scale cliffs.** q10's was a fixed row cap (the uniqueness proof) crossed
  between SF-8 and SF-10. Signature: **1-core time scales with the data,
  multi-core time does not**, because the fallback plan is serial.
- **Canonical plans.** Before `5c64cfcb` every PDS-H script ran an
  un-canonicalized plan (`write_csv(result, …); result;` executes the sink's
  input, and sink inputs were never optimized). Numbers before it measured a
  plan no sink-less script would run.

## 3. Work, ranked

Ranked by **ms lost at 8 cores against the faster reference** (§1.0). A
change counts when it moves the 8-core total; report its 1-core and 16-core
effect too. Milestone 1 needs ~260 ms, comfortably ~600 ms.

1. **The lineitem scan-filter family: q06 first, then q14, q15, q19, q01,
   q04** (~535 ms at 8c, and per-core losers 1.01–1.11). Profile q06 on one
   core against Polars and DuckDB: decode, filter and sum over lineitem with
   nothing else in the way. Whatever the per-core gap is (decode width, the
   filter kernel, selection materialization, the date-range test), it is
   likely shared by the rest; carry the fix through q14/q15 (Polars ~25%
   faster), q19 and q01/q04 (DuckDB 20–30% faster). Per query:
   - q01: the main thread's serial work is gone; what remains is worker-side
     (decode, filter, update, the fused dense aggregate). Profile the workers.
     Removing the scan/aggregate overlap is a measured no-go (§5).
   - q14 is contention, not bandwidth (§5).
   - q19 is parked: deriving OR-side predicates per join side (PostgreSQL's
     `extract_restriction_or_clauses`) is built and correct and drops the join
     from ~79 to 0.3 ms, but a small filtered build side then makes the join
     materialize lineitem instead of streaming it (q19 ~230 → 320–410 ms).
     Patches in `/home/brj/ibex-parked/`; resume at where the join decides to
     materialize its probe side when the build side is `Filter(Scan)`.
   **Landed `49da7a27`:** the consumer decodes units in place of the spare
   pool thread (q06 kept 7 of 8 cores busy; q01/q04/q06/q12/q14/q15/q19
   −4 to −7% at 8 cores, local). Per core this family is ~2/3 zstd for both
   Ibex and Polars, so the 1-core numbers are close to a floor.
2. **Filter the streamed LEFT scan by a built right side** (q12 first; q10,
   q14 next). **Built 2026-10-04** after the widening (Stages A/B below):
   q12 −35% at 8 cores SF-10 (11/11 pairs, local), all 22 answers
   byte-identical; it also fires on q10 (customer 1.5M → 427k rows) and q14
   (part 2M → 625k), whose costs are elsewhere, so those move ~1%. Taking the
   passing key values from the key scan instead of decoding the key again was
   worth −8.4% on q12 by itself. Rest of the suite: noise (q22 +2.8%, no
   change in its plan: layout).

   *Symptom.* q12 (`orders join lineitem`, lineitem filtered to 311k rows)
   is 58% occupied warm. The join correctly builds on the small right side,
   then streams all 15M `orders` rows (key and priority decoded in full)
   through it; 2.1% match. An 80 ms tail runs on ~1.75 threads plus 103 ms
   of join self time on the main thread. The only scan filter the inner join
   publishes is the build-LEFT/filter-RIGHT deferred probe
   (`deferred_probe_scan_of`); the mirror does not exist. A scratch copy with
   the operands swapped (which takes that path; sizing only, never landed --
   the engine owns join order) answers the same and runs **260 → 176 ms
   (−32%)**; join pool work 131 → 29 ms. On AWS, roughly q12 310 → ~200 ms
   against Polars 235.

   *Where else* (`IBEX_TRACE_JOIN` probe, SF-10, BuildRight joins streaming a
   base scan): q12 orders 15M → 2.1%; q10 customer 1.5M → 38% (wide string
   columns skipped for 62% of rows); q14 part 2M → 37.5%; q07 customer by
   a 2-row nation build → 8% (cheap columns, small gain). q09/q15/q21 stream
   at 100%, so the gate below must keep them out. q05's 1.8M → 4% probe goes
   through another join path (fused probe); check separately. q04 is a semi
   join, a different operator.

   *Order (2026-10-04, at review):* the filter was widened to every
   reasonable key type first, so this landed as a general mechanism rather
   than an Int64-only benchmark path. **Stage A** (`a1d8fd8d`): Date,
   Timestamp and Bool keys through `join_key_domain`, one definition shared by
   publisher, `LazyTable` and the Parquet fused scan; TIMESTAMP(NANO) fuses,
   other units filter after decode. **Stage B** (`b9cb2f9e`): String and
   Categorical keys (an exact string set; Categorical tested once per
   dictionary entry). **Stage C** (`c4084d23`): this item, gated on "the
   publisher produced a filter", not on key type. Found on the way and fixed:
   joins inside `rbind(...)` never got a deferred probe (`5b5ac0a0`).

   *As built* (`c4084d23`). `DeferredScan::stream_filter` is a second slot
   beside `filter` (three sites read `filter == nullptr` as "streaming, not a
   probe scan"). `ir::streamed_join_left_scans` names the left base scan of a
   single-key inner join, reached through Project/Rename and read once; the
   REPL registers it only when the scan has no filter of its own, so its row
   count is exact. The scan operators plan on first `next()`, after the right
   side exists. `ChunkedInnerJoinOperator::publish_left_stream_filter` runs
   before the left is drained: when `n_right * 2 <=` left rows it publishes the
   right keys, and a published filter goes straight to BuildRight (the side the
   drain would have chosen anyway, so output order is unchanged); no filter
   means the old drain. Inner join only. Tests: the lowered-IR matcher
   (`test_ir_required_columns.cpp`) and the StreamLeftReader runtime test
   (`test_interpreter.cpp`), which fails with the filter switched off.
   Not covered: two-key joins, semi/anti left inputs, Decimal and Double keys.
3. **q12 scaling** (+75 ms) is mostly item 2. q16 (3.6× against 5.8×) is
   the same symptom at a smaller size; check `occ`/`ring_wait` (item 8) first.
4. **q10** (+74 ms). −23% at 8 cores (`96d974dc`: dictionary-decided string
   conjuncts with the key scan's rows as candidates), −29% at SF-10 (`546ce652`: no row
   cap on the uniqueness proof when the key is read in full), plus
   `3005a3cf`/`0231282c` (columnar, partitioned generic group keys). Still a
   loser to both. Left: the serial hash build (predates the build/probe split;
   re-time first), `source decode whole`, and `Aggregate.Emission` appending
   text keys row by row (~10–15 ms at SF-10; bulk-append from
   `GroupKeyStore`).
5. **The deferred-probe joins: q03, q05, q07, q09** (+224 ms together, all
   winning or tied on one core). Their time is the
   lineitem scan feeding the join (`dynamic key scan`, then `decode
   selected`). Done: selection validation moved into the tasks (`08caeea4`,
   −2 to −4%); exact join-key bitmap for dense keys (`a925b814`, q03 −7.5% at
   8c / −18% at 1c; the design is on `DynamicScanFilter` in
   `include/ibex/runtime/interpreter.hpp`). Left: the
   key-scan merge (needs `Selection` not to zero-fill, or consuming the parts
   directly).
6. **q16 and q13.** q16 −11% at 8c (`4a96588c`: distinct partition sets
   pre-sized from a HyperLogLog); range-compressed packed keys measured a
   further −2 to −5% in a prototype (needs footer bounds through column
   origins and a fixed 16-bit categorical id). q13: 182 ms serial, the
   aggregate row, and the fused non-anchored LIKE scan: `string_filter_scan`
   over all of `o_comment` for `not like '%special%requests%'` uses 66% more
   CPU than a dense decode plus an in-memory filter (SF-8/8c: fused 398 ms
   wall / 2614 ms pool work, unfused 294 / 1577; better occupancy, 0.82 vs
   0.67, hides part of it). Make the fused string scan competitive, or decline
   fusion for a non-anchored LIKE over a large String column.
7. **Scale-cliff sweep.** SF 1, 2, 4, 8, 10, 16, 30 at 1 and 16 cores; flag any
   query whose time grows faster than its data between adjacent scales, then
   diff its plan and profile across that step. Fixed thresholds to suspect:
   `kMaxProofRows` (1M), `kDenseCellLimit` (4M cells), the 32 MiB dense
   partial budget in `dense_morsel_count`, `kDefaultPartitionMinRows` /
   `kPackedPartitionMinRows`, the deferred-probe gates.
8. **Decode width and scan starvation** (q04, q10, q12, q06, q20 show
   `occ ≈ 0.5, ring_wait ≈ self`; ties into items 1 and 3). Split a single-column decode's row-group
   task when there are fewer tasks than workers (`direct_decode_table`; q15's
   `l_shipdate` at SF-2 left cores idle); row-group size at write time is the
   other lever.
9. **Canonical-plan audit.** Diff every query's plan with and without its
   `write_csv(result…)` line and A/B the two, for any pass still tuned to the
   pre-`5c64cfcb` shapes.
10. **Constants calibrated at 8 cores on the dev box:**
   `IBEX_DECODE_SATURATION`, `kNestedDecodeFanout` and the
   `num_row_groups() < pool.size()` gate, `parallel_min_rows`,
   `kParallelDecodeMinRows`, per-operator partial-state budgets. Measure first,
   one constant per A/B, and only once items 1–4 have landed.
11. **Chunked `let` bindings** (structural, unbuilt): a binding is one
    contiguous `Table`, so streaming a large intermediate into it pays a serial
    concat. Needs its own design note; overlaps the binding GC work.
12. **q21: milestone 2.** A win at 8 cores (0.95) and the top loser at 16
    (+149 ms against DuckDB); largest serial term (708 ms on 2026-09-24).
    Rank by `ring_wait`/occupancy, not `pool_work`.

**Method for a loser** (found three q10 fixes in one day):
`IBEX_PROFILE_OPERATORS=1` at 8 and 16 cores and rank nodes by main-thread
self time (`serial_self_ms`, `barrier_wait_ms`, `ring_wait_ms`); then
`perf record --call-graph dwarf`, bucketed into 50 ms windows, and **check the
phase is on the critical path** (other threads idle while it runs) before
fixing it; when a fix does not move the wall, find what ate the gain.

## 4. Validation gates

- **Cross-engine claims: AWS only**, SMT off, all engines `taskset` to the
  same cores, one sitting. Sittings drift ~10%; read ratios within a sitting,
  never Ibex against an earlier Ibex run. The dev box (hybrid P/E cores under
  WSL2) held Polars to 4.7× at 8 cores where physical cores give 7.4×: never
  make a cross-engine claim from it above ~4 cores.
- **Answers:** `run_bench.sh` diffs every Ibex answer against upstream Polars
  at the benchmarked SF before timing (`check_against_polars.py`); never pass
  `--no-answer-check` for a published number. Serial vs parallel Ibex answers
  are byte-identical except q01, q09, q15 (last-ulp float reductions); a fourth
  difference is a bug.
- **Warm and fresh.** `run_bench.sh` times a warm in-process loop for every
  engine. Ibex gains far more from warmth than Polars (q21 ~38% against ~8%,
  first-touch page faults), so quote a large win fresh-process too (see
  `tune_allocator_once` in `src/runtime/interpreter.cpp`).
- **Ibex-only A/B: local**, `benchmarking/ab_queries.py`, interleaved, 8 cores,
  byte-identity on; re-run anything flagged at `--repeats 16`. For changes
  that add threads or spinning use `perf stat` elapsed plus standalone
  min-wall (memory `project_ab_queries_cpu_burn_bias`). Build the base from
  the exact parent commit.
- **Per-query canaries:** q02/q13/q16 (small-query tax; new per-query setup is
  built lazily or amortized), q06 (scan), q21 (carries the total), q14/q15/q19
  (geomean losers).
- **Data:** `benchmarking/data/tpch/parquet` points at
  `~/polars-benchmark/data/tables/scale-<sf>.0` and flips per scale factor;
  every harness prints `# dataset:`. Point it back at `scale-8.0` after a
  local SF-10 run.
- **`self_ms` is not serial time.** Confirm a target sized from the operator
  table with phase timers before building.

## 5. Measured dead ends: do not revisit without new evidence

Probe-operator Bloom; column-axis join gather; gid-sharded aggregate
accumulate (8× read amplification); the two-phase join `left_copy` branch; a
lookahead decode window (+52% RSS, no wall change); a static one-producer
admission gate; lowering the int-partition row gate; a footer-bytes
small-query gate; naive morsel islands around 1:1 operators; AVX2 filter
left-pack; one-chunk lookahead for first-chunk discovery; a work-stealing probe
cursor at 2 cores.

- **Categorical code hashing in the generic group-by:** wrong answers on q16
  (chunks after a join carry different dictionaries).
- **Selected-row gather straight from Parquet pages** (q14): ~20% slower; the
  dense path's 512 KB scratch batch is L2-resident.
- **q14 is not bandwidth-bound:** eight single-threaded copies run at a
  quarter of measured DRAM bandwidth. The contention is L3 capacity plus the
  join hash probe and the 600k-row gathers.
- **Moving a computed column across a join without a cost model:** −9 to −15%
  where it fits, q12 +40% where it does not.
- **Row-encoded group-by:** q10 +26% (decode bandwidth contention).
- **Worker-local low-cardinality aggregate:** −2%. **Aggregate inside the scan
  pipeline:** removing the overlap costs q01 44% and q15 61%.
- **Remembering the last key in the join-key probe:** repeated keys already
  hit L1.
- **jemalloc:** +3.7% fresh at 1 core. **Huge pages for column buffers
  only:** −0.6% / −1.8%; whole-heap huge pages are the lever (parked).
- **Owned aggregate:** more partitions than workers (`part_count`): q18 +13
  to +48%. A serial `reserve` of the partition maps on the calling thread:
  q18 +44%, q20 +30%. Small tweaks to the radix owned accumulate (a key/gid
  cache, run collapsing, a single fused scan with 8× redundant hashing):
  neutral or worse; it is latency-bound at ~30 ns/row.
- **From the query-shape conformance work** (`482eb583` rewrote the 22
  queries into their canonical form, −13–16%; the suite is back at parity
  without q21): `elide_checked_ascriptions` (net +25%, it unblocked deferred
  probes for q09/q12; `fuse_checked_ascriptions` replaced it); a structural
  "was this side filtered" deferred-probe gate (`contains_row_reducing_node`,
  net −10.3% where it should have won: cannot tell q08's small-by-construction
  `part` from q12's large unfiltered `orders`); classifying `Ascribe` as
  pipelineable, four attempts at +13–30% (every newly eligible scan paid
  pipelining overhead for a consumer that drained it synchronously); a
  numeric selective-decode path above the Arrow API, twice (the cost is in
  Arrow's page copy).
- **q21 is a shape gap, not a regression to undo.** Canonical q21 self-joins
  lineitem at line granularity and then aggregates, so its join output grows
  with the square of lines per order; the old query counted per order first.
  Pushing `o_orderstatus == 'F'` through does nothing (it keeps ~50%).
  Closing it needs a rewrite of "a self-join whose only consumer compares a
  cardinality (`== 1`, `> 1`, `exists`)" into distinct-and-count, and q21 is
  the only query with that shape. Swapping the old query text back in is
  ruled out: join order and shape are the engine's job.
- **A fix that does not move the wall** was usually off the critical path (the
  q10 build-side string concat: serial, fixed, 0%).

The full list with mechanisms is in memory `project_reverted_perf_dead_ends`.
