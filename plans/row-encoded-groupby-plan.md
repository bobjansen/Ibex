---
name: row_encoded_groupby
description: "A row-encoded key path for the mixed multi-column hash group-by, replacing process_rows_generic's boxed-Key/per-column-hash loop. What Polars and DuckDB do; needs no uniqueness proof."
metadata:
  node_type: plan
  type: project
---

# Row-encoded key path for the mixed multi-column group-by

**Status: BUILT (phase 1 + partitioned discovery), MEASURED, REVERTED (2026-09-01).
Does not pay on this workload. Keep this doc as the record.**

## Outcome

A working `RowKeyEncoder` + `process_rows_row_encoded` was built, wired as the
`>=2`-key / lone-double fallback, with `try_discover_partitioned` active
(`parallel_aggregate_partitions=1` on q10, where the generic path is `=0`).
7 byte-identical-vs-generic tests passed (mixed int/str/f64, nullable,
`-0.0`, string length-prefix, cross-chunk categorical dict, wide 6-col key);
NaN was a deliberate documented semantics fix (all-NaN one group, like
Polars/DuckDB). Full ctest + check_answers green.

**Interleaved SF-8 A/B (`IBEX_DISABLE_ROW_ENCODED_GROUPBY` toggle, 12 reps):**

| query | row-encoded | generic | |
|---|---|---|---|
| q10 | 678 ms | **538 ms** | −140 ms / **+26% slower** |
| q16 | 116 ms | 117 ms | wash |
| q18 | 417 ms | 418 ms | wash |
| q13 | 287 ms | 287 ms | wash |
| synthetic 40M-group `by {l_orderkey, l_returnflag}` | 2467 ms | 2451 ms | wash |

**Why it loses.** Per-node profile of q10 (`IBEX_PROFILE_OPERATORS`): the
aggregate's own `Aggregate.Discovery` span is unchanged (168 vs 167 ms — it was
never the bottleneck, that number is a wall-span dominated by the upstream join
chain). The +140 ms lands on the operators that run *concurrently* with the
row-encode on the worker pool: `join inner build_self` 159 vs 116 ms, `source
decode whole` 140 vs 112, `scan __ibex_source_0` 138 vs 105. The row-encode is a
second parallel byte-shuffling pass (encode ~185 B/row for q10's wide string
key, hash it, memcmp it) competing for L3 and memory bandwidth with the parquet
decode — on a box where the PDS-H suite already runs at `work_x ≈ 1.5` from
memory-bus contention ([[project_task_clock_finds_multiplied_work]]). Generic
hashes the key columns **in place**, does strictly less work, and its serial
discovery runs between/after decode rather than fighting it.

**Why the reference engines can and ibex cannot.** Polars/DuckDB built their
whole group-by around row-encoding from day one — SIMD per-column encoders, arena
maps, and no stable of type-specialised fast paths to fall back to. ibex settled
on specialised paths (`process_rows_int/str/cat/pair`, `PackedKeyEncoder` for
fixed-width composites) plus an in-place-hashing generic fallback. For the cases
that reach the fallback in PDS-H — low-cardinality mixed keys (q16), or a wide
key on a decode-bound query (q10) — the generic path is already competitive, and
adding a bandwidth-hungry parallel encode makes the co-running decode slower. The
`radix-partitioned-groupby.md` axis (partition a *single* high-cardinality int
key so its map fits cache) is a different, still-open lever; row-encoding the
composite key is not.

**Do not retry** without either (a) a genuinely bandwidth-cheap SIMD encoder
*and* a measurement showing headroom on the memory bus, or (b) a target workload
(ClickBench-style high-cardinality mixed-key group-by that is NOT decode-bound)
where the generic path's serial boxed-`Key` discovery actually dominates.

---

## Original scoping (2026-09-01) — kept for context

## Why

PDS-H q10's final aggregate groups on `{c_custkey:Int64, c_name:Str,
c_acctbal:Float64, c_phone:Str, n_name:Str, c_address:Str, c_comment:Str}`.
None of `aggregate_chunked.cpp`'s fast paths take it:

- `cat_fast_path_` / `str_fast_path_` / `int_fast_path_` — single key only.
- `pair_int_fast_path_` — two fixed-width-integral keys.
- `packed_fast_path_` (`PackedKeyEncoder`) — ≥3 keys, but **every column must be
  a fixed-width integral cell**: it excludes `Float64` (−0.0/NaN break byte
  equality) and `String` (interning one per row is the cost it exists to avoid).

So q10 falls to `process_rows_generic`: a boxed `Key` (`std::vector<ScalarValue>`)
built per group, `hash_key_row` hashing each column per row — including a
Categorical/String hashed as **text** (`std::_Hash_bytes`, 3.5% of the query by
`perf`) — and `key_equals_row` per probe. And its discovery is **fully serial**
(it never calls `try_discover_partitioned`; q10's `Aggregate.Discovery` measured
160 ms serial at SF-8/8c).

Reference engines (`~/polars` `crates/polars-expr/src/groups/row_encoded.rs`,
DuckDB `TupleDataCollection`) do not special-case this and do not do FD key
reduction. For any >1-key group-by Polars uses a **`RowEncodedHashGrouper`**:
encode the whole composite key into one variable-length byte string per row
(`polars-row`, unordered variant — strings inline, numerics fixed-width,
categoricals as codes), hash it once, look it up in a `BytesIndexMap` (an
arena of key bytes + a hashbrown table of group indices, zero per-key alloc),
hash-partitioned for parallelism. It is the known-good design — there is no
cleverer trick, and it is why the two FD-reduction attempts
([[project_groupby_functional_dependency]]) were the wrong lever: a fast
mixed-key group-by needs no uniqueness proof at all.

## What ibex already has (most of the back half)

- **`try_discover_partitioned<Key, Hash, Eq, KeyAt, …>`** — hash-partitioned
  parallel group discovery, already templated on an arbitrary `Key`, already
  used by the string path with `Key = std::string` + `StrViewHash/StrViewEq`.
  Handles first-occurrence ordering, mid-stream start (`key_of_group`), the
  `rows_offered_` gate.
- **`try_accumulate_parallel` / `accumulate_columns_into`** — parallel
  scatter-reduce over a gid array. Type-agnostic to the key. Unchanged.
- **`publish_discovered` / `build_output_chunk`** — materialize groups in
  first-occurrence order from `group_order_` (`std::vector<Key>` of boxed
  values). Unchanged.
- **`PackedKeyEncoder::CatIntern`** — interns each chunk's dictionary entries to
  operator-global ids once per chunk (`remap[code]`), the mechanism that makes a
  Categorical code comparable across chunks (the thing the 2026-08-11
  code-hashing attempt got wrong). Reuse verbatim.
- **`push_key_value` / `Key`** — builds the boxed per-group key for output. The
  row-encoded path calls this once per group, exactly as `process_rows_packed`
  does via `build_key_at`.

So the new code is the **front half only**: encode a row to injective bytes, and
a group state holding a bytes arena + map.

## Design

### 1. `RowKeyEncoder` (new, sibling of `PackedKeyEncoder`)

`src/runtime/row_key_encoder_internal.hpp`. Accepts **any** group-key column set
(Int64/Date/Timestamp/Bool/Float64/String/Categorical, nullable or not).

- `plan(group_entries)` → `RowKeyPlan{ columns, has_nulls }`. Always succeeds for
  scalar key columns; returns `nullopt` only for a key type nothing can group on
  (List/Struct — not reachable as a group key today, keep `process_rows_generic`
  as the ultimate fallback for it).
- `encode_row(plan, row, std::vector<char>& out)` — append, per column, in key
  order:
  - **null flag byte** (`0` null / `1` present) — only emitted for a column whose
    entry has a validity bitmap; a proven-non-null column emits none (its bytes
    can never collide with a sentinel because there is no sentinel).
  - **fixed-width** (Int64/Date/Ts 8B, Bool 1B): raw native-endian bytes. Order
    is irrelevant for a hash group-by, so no sign-flip / big-endian.
  - **Float64**: `bit_cast<uint64_t>`, then canonicalise — `x == 0.0 ? 0.0`
    (folds −0.0), `isnan(x) ? kCanonicalNaN`. This is the one rule
    `PackedKeyEncoder` refused to carry; it is three instructions per value.
  - **String**: `uint32` length prefix + raw bytes. The length prefix keeps the
    concatenation injective (`"a"+"bc"` ≠ `"ab"+"c"`).
  - **Categorical**: 4-byte operator-global id from `CatIntern::remap[code]`
    (interned once per chunk, as `PackedKeyEncoder` does).
- The encoder owns the per-chunk `CatIntern` state (one per categorical key
  column), rebuilt per chunk like the packed path.

### 2. `process_rows_row_encoded` (new, mirrors `process_rows_packed`)

- `BytesGroups` state: `std::vector<char> arena; robin_hood::unordered_flat_map<
  ArenaSlice, std::uint32_t, ArenaHash, ArenaEq>` where `ArenaSlice = {offset,
  len}` into `arena` (or a `std::string_view` rebased on lookup). Plus the
  `std::vector<KeyPartition<std::string, …>>` the partitioned path needs.
- Encode each row into a reused scratch buffer; run-length shortcut on the
  previous row's bytes (sorted / chunk-repeated input — same as the other paths).
- `try_discover_partitioned<std::string, StrHash, StrEq>(key_at, rows, gids,
  state.partitions, resize_keys, store_key, kRowEncodedPartitionMinRows,
  key_of_group)` — `key_at(row)` returns the encoded `std::string_view`;
  `store_key` records `group_order_[gid] = build_key_at(row)` (boxed, for
  output); `key_of_group(gid)` re-encodes from the boxed `Key` for mid-stream
  start. This is a near-copy of `process_rows_packed`'s call.
- Serial fallback: append to `arena`, `state.map.emplace`, `alloc_group()`.
- `publish_discovered(agg_entries, rows)` — unchanged.

### 3. Fast-path selection (`aggregate_chunked.cpp` ~line 700)

Add `row_encoded_fast_path_`, chosen when `group_entries.size() >= 2` (or a
single non-int/str/cat scalar such as a lone `Float64` key) and none of the
narrower paths applied. It **supersedes `process_rows_generic`** for every
all-scalar key set — including nullable ones, which it handles natively.

`process_rows_generic` stays only for a key column type the encoder returns
`nullopt` for.

### 4. Mid-stream nulls

`migrate_*_fast_path_to_generic` exists because the value-only fast paths cannot
represent a null that appears in a later chunk. The row-encoded path emits a null
flag, so **it needs no migration** — it is strictly more capable than generic.
Simplest: when a value-only fast path (`int`/`str`/`cat`/`pair`/`packed`) hits a
late null, migrate to **row-encoded** instead of generic (rename the helpers,
reseed `group_order_` the same way). Keeps one slow path, not two. *Can be
deferred* — phase 1 may leave the `→ generic` migration alone and only add the
new path for keys that are nullable from chunk 1.

## Phasing

1. **`RowKeyEncoder` + `process_rows_row_encoded`, serial discovery only.**
   Wire as the `>=2`-key fallback. Gate `try_discover_partitioned` off. Verify
   byte-identical vs `process_rows_generic` on: mixed int+str+f64 keys; nullable
   keys; `−0.0`/`NaN` f64 keys; a Categorical key across two chunks with
   different dictionaries; empty and single-row chunks. Bench q10/q16/q18 SF-2
   and SF-8 (expect the per-row win: no text hash, no boxed Key probe).
2. **Enable `try_discover_partitioned`** for the row-encoded path with its own
   `min_rows`. Verify parallel determinism (byte-identical across 1/2/4/8
   threads — the existing invariant) and the mid-stream-start path
   (`key_of_group`). Bench (expect q10/q18 discovery to stop being serial).
3. **Fold `→ generic` migrations into `→ row_encoded`.** Delete
   `process_rows_generic` if nothing reaches it (List/Struct group keys are not
   constructible today — confirm, then remove; otherwise keep it minimal).
4. **`try_accumulate_parallel` for text aggregates** is out of scope (it bails
   on non-Int/Double today); unrelated.

## Effort / risk

- ~250 lines encoder, ~140 lines `process_rows_row_encoded` + state, ~60 lines
  wiring, ~200 lines tests. One focused PR per phase.
- **Low risk on the back half** — discovery, accumulation, output, CatIntern all
  exist and are proven byte-identical + deterministic.
- **The risk is encoder injectivity.** Byte-equal MUST iff value-equal:
  - f64 `−0.0`/`NaN` — canonicalised (above).
  - string concatenation ambiguity — length-prefixed (above).
  - null vs a value that encodes to the sentinel byte — the null flag is a
    separate leading byte, never overlaps the value bytes.
  - a Categorical code meaning different things in different chunks — `CatIntern`.
  Each gets a dedicated test asserting `row_encoded == generic` output.
- Encoding adds one materialisation pass (columns → bytes) per chunk. For a
  low-cardinality all-int key the packed path is still cheaper (register-width
  key, no var-length) — row-encoded is the fallback, it does not replace the
  narrow fast paths.

## What this does NOT need

No uniqueness proof, no functional-dependency reduction, no Parquet footer
statistics, no catalog. It makes the group-by the query already asked for run
fast. FD reduction ([[project_groupby_functional_dependency]]) becomes a
second-order optimisation (fewer bytes in the key) rather than the thing
standing between q10 and parity.

Related: [[radix_partitioned_groupby]] (the single-int high-cardinality
bandwidth ceiling — composes with this, same `try_discover_partitioned`),
[[project_high_cardinality_groupby_gap]], [[project_multikey_groupby_no_boxed_key]].
