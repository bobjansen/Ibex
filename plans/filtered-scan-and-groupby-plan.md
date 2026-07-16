# q03/q10 gap: filtered scans and the string-keyed group-by

Status: analysis done (the join-perf plan's "item 4: re-profile"), no code changed
Date: 2026-07-14
Baseline: PDS-H SF-1, single-threaded, warm page cache, `build-release/` at
`ed21794` (join rework + NumericCast kernel committed)

## Standings after the join work

Full-suite run (user's `run_bench.sh`, 2026-07-14 16:24):

| query | ibex | polars-st | ratio |
|---|---:|---:|---:|
| q01 | 221.5 | 342.5 | **0.65× (ibex wins)** |
| q03 | 107.2 | 82.9 | 1.29× |
| q05 | 140.6 | 132.4 | 1.06× |
| q06 | 43.8 | 30.0 | **1.46× (worst ratio)** |
| q09 | 213.7 | 192.3 | 1.11× |
| q10 | 111.1 | 86.3 | 1.29× |
| q13 | 211.6 | 180.6 | 1.17× |
| q19 | 59.0 | 55.0 | 1.07× |

geomean 1.1×. q03 and q10 are the largest absolute gaps (~25ms each); q06 is
the worst ratio but only +14ms.

## Decomposition (min-per-statement over 10 iters, one warm REPL, quiet box)

**TRAP that burned this session:** the post-commit perf hook runs in the
background after every commit; a breakdown taken while it ran read 4–5× high
(q03 "693ms"). `pgrep -af perf|ibex|ninja` + `uptime` before trusting numbers.

q03 (95.5ms):

| stage | ms | share |
|---|---:|---:|
| lineitem filtered scan (`l_shipdate >`, 54% pass, 3 cols) | 57.0 | 60% |
| joins (customer⋈orders 8.5 + ⋈lineitem 14.3) | 22.8 | 24% |
| orders filtered scan | 12.5 | 13% |
| group-by + order + head | 2.0 | 2% |

q10 (101.6ms):

| stage | ms | share |
|---|---:|---:|
| lineitem filtered scan (`l_returnflag == "R"`, 25% pass, 3 cols) | 42.0 | 41% |
| 7-key group-by (115k rows → ~38k groups, 4 string keys) | 26.3 | 26% |
| joins (4.3 + 11.8 + 0.8) | 16.9 | 17% |
| customer scan (7 cols) + orders scan | 15.9 | 16% |

Direct stage A/B against polars-st (same box, min-of-7):

| stage | ibex | polars-st | ratio |
|---|---:|---:|---:|
| q03 lineitem scan (3.24M survivors) | 57.0 | 47.7 | 1.20× |
| q10 lineitem scan (1.48M survivors) | 42.0 | 26.3 | **1.60×** |
| q10 7-key aggregate (materialized input) | 26.3 | 14.9 | **1.76×** |

The joins are NOT the story anymore — both queries' join stages look
competitive post-rework, and the two hot stages above over-explain the total
gap (ibex is faster than polars-st elsewhere in these queries).

## Finding 1 — filtered scan: per-value glue dominates selected decode

Profile of the q03 lineitem statement (perf, self time):

| symbol | self |
|---|---:|
| `decode_physical_column<DOUBLE>` (selected decode, per-value lambda) | 21.4% |
| `decode_physical_column<INT64>` (same, l_orderkey) | 10.7% |
| `direct_column` + `direct_decode_table` glue | 11.5% |
| memmove — of which growing the selection vector | 15.3% / 9.0% |
| `filter_selection` self | 4.6% |
| Arrow dictionary/bit-unpack work (the honest part) | ~13% |

~44% of the statement is *our* per-value machinery (a callback lambda per
value over `ReadBatch`/`Skip`), not Arrow's decoding. The sparser the
selection, the worse the ratio (q10's 25% → 1.60× vs q03's 54% → 1.20×):
per-value overhead and Skip-gap churn grow relative to bytes delivered.

This is the already-diagnosed conclusion of
`plans/parquet-filtering-scan-observations.md`: rearranging Skip/ReadBatch
above Apache's public API was measured neutral-or-slower; the next step must
run **inside the encoding decoder** (dense-decode the block, gather survivors)
— "direct per-row-group decode + selection-aware late materialization are ONE
work item". It touches q03, q05, q06, q10, q13 (orders `!like` scan), q19 —
six of eight queries. Highest-leverage item, but also the invasive one
(ColumnDecodeFn is plugin ABI; rebuild the .so in lockstep).

**Quick win found — DONE 2026-07-16 (3e8a46d):** `select_bounds`
(`src/runtime/filter.cpp`) reserved `min(rows, 4096)` and block-inserted
survivors — q03's 3.24M survivors doubled the vector ~10 times, re-copying up
to 26MB each time. That was the 9% memmove. Fixed by extrapolating the first
block's hit rate across the remaining blocks and reserving once, capped at
`rows`. Measured q03 lineitem scan **47.1 → 43.3ms** (min-of-12, interleaved,
SF-1). The transient-memory worry was unfounded: an over-reserve costs address
space, not RSS — untouched pages are never faulted in. Peak RSS is identical
(125.7MB) even on a deliberately pathological shape (first block hits, other
12M rows miss).

## Finding 2 — q10's 7-key group-by: string hashing + per-group boxing

The aggregate runs in `ChunkedAggregateOperator` (in-place row hashing — the
per-ROW Key boxing is already gone). Profile of the aggregate statement:

| symbol | self |
|---|---:|
| `std::_Hash_bytes` (libstdc++, NOT inlined) | 18.8% |
| `ChunkedAggregateOperator::next` | 14.5% |
| malloc/free machinery (unlink_chunk, consolidate, _int_malloc, cfree) | ~12% |
| memmove + `Column<string>` range_insert (output materialization) | 13.4% |
| `scalar_from_column` + `push_key_value` (per-GROUP Key boxing) | 6.5% |
| `AggSlot` move ctor (states vector growth; AggSlot is fat) | 2.5% |

Costs, in order:
1. **String hashing — HYPOTHESIS REFUTED 2026-07-16, do not re-run.** Every
   row hashes 4 string keys (`c_comment` ~73B, `c_address`, `c_phone`,
   `c_name`) through `std::hash<string_view>` → libstdc++'s out-of-line
   `_Hash_bytes`, 18.8% self time. Swapping `hash_key_row`'s Str/Cat arm (and
   `KeyHash`'s string arm) to `robin_hood::hash_bytes` — inline, same family —
   measured **no change**: base 26.84ms vs new 26.91ms mean of 3 interleaved
   min-of-12 rounds, rounds disagreeing on sign. The path was confirmed live
   (`ChunkedAggregateOperator` holds a `KeyRowIndex`, which hashes via
   `hash_key_row`), so this is not a missed target. The 18.8% is *inherent
   work* — reading and mixing ~73 bytes per row — not call overhead. Only a
   hash that reads fewer bytes (e.g. prefix + length, with full compare on
   probe) or a scheme that hashes each key once per group rather than per row
   could move it. Reverted.
2. **Per-group boxing**: ~38k groups × (Key vector alloc + 4–5 heap string
   copies + fat AggState) each run — the malloc churn above. An arena for
   group-key strings would cut most of it. (The chunked operator must keep
   group keys — chunks die — so boxing per group is structural; its COST is not.)
3. `n_name` arrives categorical but is hashed/compared as text (KeyCol::text)
   because the all-categorical CatKey fast path doesn't apply to mixed keys.
   Minor here (25 distinct values), but code-hashing categoricals in mixed
   keys is available if needed.
4. Planner-level (an "exceed polars" lever, not a catch-up one): `c_custkey`
   is unique per customer, so grouping by it alone and carrying the other six
   columns as first() would shrink the key to one int64. Polars does not do
   this either (its 7-key hash is simply faster).

## Suggested order

1. ~~Faster string hash in `hash_key_row`~~ — **REFUTED, reverted** (see
   finding 2.1). No change; the cost is inherent byte-mixing.
2. ~~`select_bounds` reserve~~ — **DONE** (3e8a46d), q03 scan 47.1 → 43.3ms.
3. The in-decoder filtered decode (finding 1) — the big one, and now the only
   remaining lever on the scan side; plan it against
   `plans/parquet-filtering-scan-observations.md`'s rejected-experiments list
   so nothing is re-run. **Re-profile first**: findings here predate the join
   rework, null-free fast paths, and the reserve fix.
4. Group-key arena (finding 2.2) — now the leading q10 aggregate item, since
   the hash is a dead end. ~12% of the stage is malloc churn from boxing a Key
   (vector alloc + 4–5 heap string copies) per group.

## Methodology (same as join plan, plus)

- Check for the post-commit perf hook before ANY measurement.
- Stage timings: min-per-statement over ≥10 iters in ONE warm REPL
  (`scratchpad stage_breakdown.py` pattern; the REPL times each `let`).
- Stage-level polars-st comparisons: `POLARS_MAX_THREADS=1`, materialize the
  same inputs, min-of-7.
