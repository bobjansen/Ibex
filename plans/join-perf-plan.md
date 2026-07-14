# Join (and group-by) performance: findings and plan

Status: items 1–3 DONE (2026-07-14, uncommitted). q09 −23%, q13 −30%. See
"Results" at the bottom; item 4 (re-profile, then maybe group-by) remains.
Date: 2026-07-14
Baseline: PDS-H/TPC-H SF-1, single-threaded, warm page cache, `build-release/`

## Why this is next

As of today Ibex is at **geomean parity with polars-st** across the 8 implemented
PDS-H queries (polars-st 1.1x by geomean; Ibex *wins* q01 and q19). Scan/decode
work is finished: `plans/parquet-filtering-scan-observations.md` records that
dense string decode is now at Apache's floor (55ms microbenchmark vs Polars'
57ms), and that the three remaining scan ideas — a native encoding decoder, a
decode arena, and mmap — were each measured and **rejected**. Do not re-run them.

The remaining single-threaded gap is the **join and the group-by**. q13
decomposes as:

| stage | time |
|---|---:|
| orders scan + `!like` (decode ~60ms + fragment match ~30ms) | 90 ms |
| left join (150k x 1.5M) | 73 ms |
| group-by `c_custkey` (150k groups) + count | 74 ms |
| **total** | **~260 ms** (polars-st: 164 ms) |

Decode is no longer the dominant term. The join and the group-by are 147ms of it.

## Profiles

`perf record --call-graph=dwarf`, `ibex_eval`, 3-5 iterations of the query body,
self time (`--no-children`).

### q09 (5 joins, one of them two-key) — ~60% of the query is join machinery

| symbol | self |
|---|---:|
| `ChunkedInnerJoinOperator::next` | 30.1% |
| `robin_hood::Table<Key, std::vector<size_t>>` (find/insert) | 14.0% |
| `std::__do_visit` (variant visit) | 10.5% |
| `scalar_from_column` | 5.2% |
| `join_table_impl` | 6.1% |

### q13 (one left join, 150k x 1.5M)

| symbol | self |
|---|---:|
| `join_table_impl` | 29.8% |
| `eval_expr` (per-row scalar eval) | 7.7% |
| `__memcmp_avx2_movbe` | 6.3% |
| `ChunkedAggregateOperator::next` | 4.8% |
| robin_hood `findIdx`/`keyToIdx` on **string** keys | 6.8% |
| `gather_column_with_nulls` | 3.1% |

## Findings

### 1. The join still boxes a `Key` per row — the bug group-by already fixed

`src/runtime/join.cpp:29`:

```cpp
struct Key { std::vector<ScalarValue> values; };   // heap alloc PER ROW
```

The generic multi-key path (`join.cpp:1124` onwards) builds one of these for
every row on both sides, and hashes it through `KeyHash` (a `std::visit` per key
component). That is precisely the pattern removed from group-by in
"Multi-key group-by without a boxed key" (q10 group-by 71.6 -> 32.3ms). The join
never got the same treatment.

The q09 profile is the receipt: 14% in the `Key`-keyed hash table, 10.5% in
`std::__do_visit`, 5.2% in `scalar_from_column` — that is all boxing, not
joining. **Highest-value item.** Hash and compare the key columns in place, as
`aggregate.cpp` now does.

Any join on 2+ keys takes this path. q09's `partsupp` join is `on { l_partkey,
l_suppkey }`.

### 2. Single-int64 key path allocates a `std::vector<size_t>` per distinct key

`src/runtime/join.cpp:832`:

```cpp
robin_hood::unordered_map<std::int64_t, std::vector<std::size_t>> left_int_index;
for (std::size_t l = 0; l < n_left; ++l)
    left_int_index[left_ints[l]].push_back(l);      // one heap vector per key
```

For q13 that is ~150k heap-allocated vectors, each holding exactly **one**
element, because `c_custkey` is a primary key. **Every TPC-H join is PK-FK, so
the build side is essentially always unique.**

Two fixes, either of which removes the allocations:

- a CSR-style index (count per key -> prefix sum -> one flat row array), or
- a chain-head index (`head[]` + `next[]`), which is what the swapped-probe work
  already did elsewhere (see `project_swapped_join_probe_once`).

Note the non-swapped branch has a `unique_right` fast path (`join.cpp:860`)
that stores a single row index and skips the vectors entirely — the **swapped**
branch (`n_left < n_right`, which is the branch q13 and most PK-FK joins take)
has no `unique_left` equivalent. Adding one is a small, contained change.

### 3. TRAP: there are (at least) TWO join implementations

- `join_table_impl` (`src/runtime/join.cpp`) — eager; q13 goes here.
- `ChunkedInnerJoinOperator` (`src/runtime/chunked.cpp`) — streaming; q09 goes here.

They have **separate** key/hash paths. Fixing one and benchmarking the other
query will show nothing — exactly the trap already documented for grouping
("there are TWO grouping impls... fixing only one changes nothing observable")
and for null keys. Check which operator a query actually runs before and after.

**CORRECTION (found during implementation):** `ChunkedInnerJoinOperator` does
NOT box Keys — it is single-key only (`keys_->size() != 1` errors out at
initialize) with native-typed head maps + a chain array, and multi-key joins
route around it to `join_table_impl` via `build_binary_materializing_operator`
(chunked.cpp ~5096). So ALL the `Key`/`std::visit`/`scalar_from_column` cost in
the q09 profile was `join_table_impl`'s generic path; its 30% of
`ChunkedInnerJoinOperator::next` self time is genuine probe/assemble work on
the four single-key joins. The whole fix landed in join.cpp.

### 4. `count(col)` evaluates its 0/1 cast per row (a regression I introduced)

`count(col)` lowers to `sum(Int64(col is not null))` over a pre-aggregate column
(see `project_count_non_null`). The `Int64(<bool>)` cast currently falls into
`eval_scalar_over_columns` -> per-row `evaluate_field` -> `eval_expr`, which is
the 7.7% `eval_expr` in the q13 profile (plus part of the 6.8% in string-keyed
`findIdx`/`keyToIdx`: per-row column-name and builtin-registry lookups).

q09 has no `count(col)` and shows no `eval_expr` in its profile — consistent.

Fix: give `Int64`/`Float64` of a boolean-valued expression a vectorized path
(`eval_value_vec` already produces a `Column<bool>`; the cast is a widening loop).
Contained, and worth ~7% of q13 on its own.

## Suggested order

1. **In-place multi-key hashing in both join impls** (finding 1). Biggest, and it
   is a port of a fix that already exists in `aggregate.cpp`.
2. **Unique/flat build index for the single-key paths** (finding 2). Small,
   contained, hits q13/q05/q10 and every PK-FK join.
3. **Vectorize the Bool -> Int cast** (finding 4). Small; removes a regression.
4. Re-profile before touching the group-by. It is 74ms of q13, but items 1-2 may
   move the picture, and `ChunkedAggregateOperator::next` is only 4.8%.

## Methodology (do not skip — this box lies)

WSL2 wall-clock drifts 10%+ between runs; the same unchanged binary measured
113ms and 130ms an hour apart during the scan work. Two readings were nearly
mis-attributed because of it.

- Compare **interleaved** (A/B/A/B in one session), never A-then-B.
- **min-of-8** iterations inside ONE warm REPL process (`ibex` binary driven via
  stdin with `:timing on`), not repeated `ibex_eval` runs, which pay plugin and
  Arrow startup.
- `benchmarking/tpch/bench_ibex.py --warmup 3 --iters 11` for the headline table;
  `benchmarking/tpch/run_bench.sh` also runs polars and polars-st.
- Allocator note: jemalloc is linked into `ibex_bench` **only**, not into
  `ibex`/`ibex_eval`. Default jemalloc is *worse* than the tuned glibc the
  runtime already sets up (`tune_allocator_once`). Do not "just link jemalloc".

## Verification

- `cd build && ctest` (1097 tests)
- `scripts/ibex-e2e.sh`
- `uv run benchmarking/tpch/check_answers.py` — all 8 official SF-1 answers
  (q1 q3 q5 q6 q9 q10 q13 q19). **Joins have weak unit coverage**: the swapped
  probe path needed a right side >65,536 rows and was only ever caught by these
  answer checks. Run them after every join change.
- `tests/parity/run_parity.sh` (12 cases; interpreted vs transpiled)

## Key files

- `src/runtime/join.cpp` — `Key` (29), `KeyHash` (33), single string/categorical
  key (715), single int64 key (832), asof (893), generic multi-key (1124)
- `src/runtime/chunked.cpp` — `ChunkedInnerJoinOperator`
- `src/runtime/aggregate.cpp` — the in-place multi-key grouping to copy from
- `benchmarking/tpch/queries/q09.ibex`, `q13.ibex` — the two join-heavy queries

## Results (2026-07-14, implemented)

All three items landed in one pass; verified with ctest (1097), the 8 official
PDS-H answer checks, parity, and ibex-e2e; strict g++ (-Wpedantic/-Wconversion/
-Wshadow -Werror) clean on the changed TUs.

**What was built** (all in `join_table_impl` unless noted):

1. *(finding 1)* Generic multi-key path hashes/compares key rows **in place**
   via `KeyCol`/`hash_key_row` (interpreter_internal.hpp, now included by
   join.cpp) + a local `RowKeyIndex`. Unlike group-by's `KeyRowIndex` it stores
   NO boxed Key at all — each group's representative is its first build row
   (`rep_rows`), compared cross-side with `key_rows_equal` (Str and Cat compare
   as text, so mixed String↔Categorical joins still work). join.cpp's local
   `Key`/`KeyHash`/`KeyEq` were deleted; the asof generic fallback now uses the
   header's Key (not hot, left boxed).
2. *(finding 2)* Every build index (int64, string/cat, multi-key) is now
   key→dense-gid + a CSR (`GroupedRows`: offsets[] + rows[]) — zero per-key
   heap vectors. Unique-build detection is a free byproduct
   (`rows.size() == n_groups`), so the swapped branch gets uniqueness for free
   too. Bonus: `build_indices_from_right_scan` now probes the index **once**
   per right row (records hit spans, replays them for the fill pass) — the
   join.cpp copy of the double-probe the chunked swapped join already shed.
3. *(finding 4)* New `ScalarKernel::NumericCast` (expr.cpp) vectorizes
   Int64/Int32/Int/Float64/Float32 over a bare column/literal/lexical scalar,
   mirroring the per-row eval exactly (Bool→0/1, non-integer Float→Int errors,
   null payloads never read). `count(col)`'s `Int64(col is not null)` lands on
   it after eval_scalar_over_columns materializes the bool arm.

**Measured** (interleaved A/B/A/B, min of 6 iters × 2 rounds per side, one warm
REPL per run, same box/session):

| query | base | new | delta |
|---|---:|---:|---:|
| q09 | 254.2 ms | 195.4 ms | **−23.1%** |
| q13 | 276.7 ms | 194.9 ms | **−29.6%** |
| q10 | 107.2 ms | 103.0 ms | −3.9% |
| q05 | 135.0 ms | 131.6 ms | −2.6% |
| q01/q03/q06/q19 | — | — | ±2.6% (noise) |

The cast kernel was worth far more on q13 than the profile's 7.7% `eval_expr`
suggested — the per-row path also paid per-row builtin-registry and column
lookups (the string-keyed `findIdx` slice) and ExprValue boxing.

**Next (item 4):** re-profile q13/q09 before touching group-by; joins no longer
dominate q13 (~195ms vs polars-st 164ms; scan+like ~90ms and the 150k-group
aggregate are what's left).
