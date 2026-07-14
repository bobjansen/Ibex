# Parquet filtering scan: implementation and performance observations

Status: implementation landed; follow-up decoder experiments rejected  
Date: 2026-07-13  
Primary commits:

- `588d8e5 Add direct filtered Parquet scans`
- `400315c Compact selective scan predicates`

This note records the implementation results, profiles, Polars comparison,
failed experiments, and remaining optimization boundary after executing
`plans/parquet-filtering-scan-plan.md`. It is intentionally more detailed than
the commit messages so that the rejected paths do not have to be rediscovered.

## Executive summary

The planned structural change succeeded:

1. Parquet values now decode directly into Ibex columns instead of taking the
   `Parquet -> Arrow Table -> Ibex` path.
2. Predicate columns decode first, the runtime computes a row selection, and
   non-predicate payload columns materialize only selected rows.
3. Selective conjunct evaluation compacts the active rows once selectivity
   reaches 25% or lower.

Correctness is solid and the new interfaces create the right seam for future
scan work. The measured improvements are modest, however. Q06 improved about
3% in the clean paired comparison and Q19 remained within noise. Polars is not
avoiding predicate evaluation or row-group reads on this data; its native
Parquet decoder and integrated expression/scan pipeline simply perform the
same unavoidable work much faster.

Multiple attempts to reproduce Polars' prefiltered decoder above Apache
Parquet's public C++ APIs were neutral or slower. Apache's existing
`TypedColumnReader::Skip` already skips whole pages without decoding and uses
1,024-value scratch batches for partial pages. A meaningful next decoder step
therefore has to operate inside the encoding decoder, or replace it, rather
than rearranging `Skip` and `ReadBatch` calls above it.

## What landed

### Direct decode

The lazy Parquet callback now accepts:

```text
ColumnDecodeFn(names, Selection*) -> Table
```

`Selection == nullptr` means all source rows. A non-null selection is sorted,
unique, file-global row indices.

The direct decoder in `libs/parquet/parquet.hpp`:

- reads only demanded leaf columns;
- iterates row groups through Apache Parquet column readers;
- writes same-layout required numeric values directly into resized Ibex
  storage;
- uses bounded scratch buffers for converted numeric/date/time values;
- preserves null validity for optional columns;
- reads fully dictionary-encoded strings as `Column<Categorical>`;
- remaps each row group's local string dictionary into one Ibex dictionary;
- accepts the same file-global selection in whole-file and row-group-streaming
  reads.

There is no intermediate Arrow `Table` on this path. Arrow's schema and
Parquet reader remain in use, but decoded values go into final Ibex storage.

### Late materialization

`LazyTable::project_where` now:

1. collects every column referenced by the pushed scan conjuncts;
2. decodes those predicate columns densely;
3. calls `filter_selection` to obtain surviving source row indices;
4. decodes remaining demanded columns with that selection;
5. gathers any predicate columns also required by the result;
6. returns a table whose logical row count is the selection length.

Filtered reads deliberately bypass the ordinary lazy-column cache. Caching a
selected column as if it represented the whole source would make a later
unfiltered query silently incorrect.

The scan predicate analysis only pushes predicates to a source that occurs
exactly once in the plan. The materialized table registry is keyed by source
name, so applying one occurrence's selection to a repeated scan or self-join
would otherwise be unsound.

### Selective predicate compaction

`400315c` changed `filter_selection` from always computing dense masks over the
original table to a staged selection algorithm. Once 25% or fewer rows remain,
it gathers only the columns referenced by the next consecutive same-column
conjuncts, evaluates them over the compact table, and maps local survivors back
to original source indices.

This reduced comparison work on Q06, but introduced gather work. The net result
was a small improvement rather than a large one.

### Deliberately retained residual filter

Scan predicate extraction does not remove the `Filter` node from the IR. The
ordinary runtime therefore evaluates the predicate again over the already
selected table. This is idempotent and keeps partial pushdown sound, but it is
some residual work. It is relatively small because the second evaluation sees
only survivors.

## Correctness and implementation constraints discovered

- `TypedColumnReader::Skip` counts physical values, not logical rows. It is
  safe for flat required columns, but optional columns must stay on the
  definition-level-aware sequential path because nulls have no physical value
  to skip.
- A selected column must report exactly `selection.size()` rows and must never
  enter the whole-column cache.
- Predicate columns that also feed the result, such as Q06's `l_discount`,
  must be gathered after selection rather than decoded a second time.
- Parquet dictionaries are local to a column chunk/row group. Codes cannot be
  concatenated without remapping.
- A string column is treated as categorical only when every data page in every
  row group is dictionary encoded. Seeing a dictionary page alone is not
  enough because writers may fall back to PLAIN encoding.
- Selected row indices remain file-global when decoding a row-group subset.
  This allows one selection representation to serve lazy whole-file reads and
  row-group streaming.
- The `ColumnDecodeFn` signature is plugin ABI. Runtime/public-header changes
  require rebuilding the Parquet plugin in lockstep; stale plugins can fail in
  misleading ways.
- Empty projections and empty selections still need a correct logical row
  count even when no physical column is present.
- Keeping the original `Filter` above a filtered scan was valuable during the
  rollout: partial extraction could not silently change query semantics.

## Benchmark environment and methodology

- Host: WSL2, 13th Gen Intel Core i7-13700
- Logical CPUs: 24
- Build: `build-release/`, Clang, `-O3 -march=native`
- Apache Arrow/Parquet: 22.0.0
- Polars: 1.38.1
- Data: PDS-H/TPC-H SF-1, warm OS page cache
- Ibex execution: single-threaded
- Fair Polars comparison: `POLARS_MAX_THREADS=1`
- Polars default comparison: 24-thread pool

The benchmark must keep one Ibex REPL process warm. Repeated `ibex_eval`
processes include dynamic-library and plugin startup costs and are not useful
for these decoder comparisons.

WSL wall-clock results varied materially with scheduling and CPU state. Later
experiments therefore used:

- ABBA ordering for separate-process comparisons;
- 30-100 measured iterations after several warmups;
- `perf stat` task-clock where hardware counters were unavailable;
- finally, alternating candidate/baseline iterations inside one warm REPL.

The last method was decisive for rejecting the dictionary-index prototype.
Single short runs often indicated the opposite result.

## Dataset and encoding observations

`lineitem.parquet` contains 6,001,215 rows in six row groups: five groups of
1,048,576 rows and a final group of 758,335 rows. It was written by
`parquet-cpp-arrow 22.0.0` and the relevant chunks are uncompressed.

First-row-group metadata:

| column | logical/physical type | encodings | chunk bytes |
|---|---|---|---:|
| `l_quantity` | Float64 / DOUBLE | PLAIN, RLE, RLE_DICTIONARY | 788,963 |
| `l_discount` | Float64 / DOUBLE | PLAIN, RLE, RLE_DICTIONARY | 526,507 |
| `l_shipdate` | Date32 / INT32 | PLAIN, RLE, RLE_DICTIONARY | 1,585,132 |
| `l_extendedprice` | Float64 / DOUBLE | PLAIN, RLE, RLE_DICTIONARY | 8,620,368 |
| `l_shipinstruct` | String / BYTE_ARRAY | PLAIN, RLE, RLE_DICTIONARY | 264,344 |
| `l_shipmode` | String / BYTE_ARRAY | PLAIN, RLE, RLE_DICTIONARY | 395,403 |

`l_quantity`, `l_discount`, and the string predicates are low-cardinality and
fully dictionary encoded. High-cardinality columns such as
`l_extendedprice` start with a dictionary and fall back to PLAIN pages.

Q06 retains 114,160 of 6,001,215 rows, about 1.90%. Matches are scattered, so
selected runs are usually single rows and the mean gap is roughly 52 rows.
Q19's pushed lineitem predicate retains about 214k rows, roughly 3.6%.

All Q06 row groups have broad `l_shipdate` ranges spanning the benchmark
predicate. Row-group statistics therefore read 6/6 groups; statistics cannot
explain the Polars advantage on this file.

## Landed performance results

Clean paired end-to-end comparison for selective predicate compaction:

| query | before | after | change |
|---|---:|---:|---:|
| Q06 | 122.82 ms | 118.95 ms | -3.15% |
| Q19 | 175.91 ms | 175.29 ms | -0.35%, noise |

Later clean runs ranged around 114-119 ms for Q06 and 169-183 ms for Q19
without source changes. That variation is why paired results are more useful
than comparing isolated minima or means from different runs.

### Q06 profile after compaction

DSO self time:

| component | self time |
|---|---:|
| Ibex runtime/executable | 42.11% |
| `parquet.so` | 36.02% |
| `libparquet` | 11.77% |
| libc | 5.45% |
| `libarrow` | 3.03% |

Important symbols:

| symbol | self time |
|---|---:|
| Parquet `direct_column` | 35.90% |
| `filter_selection_impl` | 24.55% |
| `compare_col_scalar` | 6.20% |
| `gather_column` | 5.30% |
| public `filter_selection` wrapper | 2.80% |
| specialized double comparison | 0.83% |

Before compaction, comparison work was about 14.2% and there was no equivalent
5.3% compact-gather cost. Compaction traded comparison passes for gathers and
won only a few milliseconds overall.

### Q19 profile after compaction

DSO self time:

| component | self time |
|---|---:|
| `parquet.so` | 49.97% |
| Ibex runtime/executable | 30.61% |
| `libparquet` | 10.01% |
| libc | 6.12% |
| `libarrow` | 1.82% |

Important symbols:

| symbol | self time |
|---|---:|
| Parquet `direct_column` | 49.83% |
| `filter_selection_impl` | 15.35% |
| `compute_mask` | 3.15% |
| categorical gather | 2.56% |
| public `filter_selection` wrapper | 2.00% |
| comparisons | 1.58% |

Q19 remains even more clearly decoder-bound than Q06.

### Per-column Q06 timings

Environment-gated instrumentation around `direct_column` produced these warm
steady-state values:

| column | source rows | output rows | typical time |
|---|---:|---:|---:|
| `l_quantity` | 6,001,215 | 6,001,215 | 14-15 ms |
| `l_discount` | 6,001,215 | 6,001,215 | 12-13 ms |
| `l_shipdate` | 6,001,215 | 6,001,215 | 14-15 ms |
| `l_extendedprice` | 6,001,215 | 114,160 | 22-24 ms |

The surprising result is that sparse selected `l_extendedprice` costs more
than each dense predicate column. The high-cardinality payload has much more
encoded data, and scattered selection turns into many small survivor runs.
That observation motivated the page-level experiments below.

## What Polars actually does

The local verbose Q06 trace reported:

```text
[ParquetFileReader]: project: 4 / 16
[ParquetFileReader]: Pre-filtered decode enabled (3 live, 1 non-live)
[ParquetFileReader]: Predicate pushdown: reading 6 / 6 row groups
```

The three live columns are `l_shipdate`, `l_quantity`, and `l_discount`; the
non-live payload column is `l_extendedprice`.

Therefore Polars does not avoid the predicate work:

```text
decode live predicate columns -> evaluate combined predicate/mask
                              -> filtered decode of payload column
                              -> aggregate
```

It also does not win through row-group statistics on this query: all six row
groups are read. Projection pushdown limits the scan to 4/16 columns, and
`parallel="auto"` selects its prefiltered strategy.

Single-threaded Q06 strategy isolation:

| Polars strategy | median | mean | minimum |
|---|---:|---:|---:|
| `parallel="none"` | 38.03 ms | 37.95 ms | 33.90 ms |
| `parallel="prefiltered"` | 32.27 ms | 32.67 ms | 30.57 ms |

Prefiltering saves about 5-6 ms, but Polars without it is still far faster than
Ibex. Thus late materialization is only part of the gap. The primary difference
is Polars' native Parquet decoder and integrated scan/expression pipeline. It
does not create a separate PyArrow table; its Arrow-compatible buffers are its
native column representation.

Relevant upstream descriptions:

- [Polars `scan_parquet`](https://docs.pola.rs/api/python/dev/reference/api/polars.scan_parquet.html)
- [Polars lazy optimizer](https://docs.pola.rs/user-guide/lazy/optimizations/)

The 24-thread default pool is additional headroom, not the root cause. Polars
remains much faster with `POLARS_MAX_THREADS=1`.

## Follow-up experiments that were rejected

All experimental source changes below were removed. The release plugin was
rebuilt from `400315c` after each final rejection.

### 1. Sequential batch/masked selected numeric decode

Hypothesis: sparse `Skip`/`ReadBatch` survivor runs were dominated by virtual
call overhead, so reading 64K sequential values and gathering selected offsets
would be faster.

Two variants were tested:

- the existing level-aware sequential loop, checking every decoded row;
- a required-column loop that decoded 64K values and iterated only selection
  offsets falling in each batch.

Observed selected `l_extendedprice` remained about 22-27 ms and sometimes rose
above 30 ms. End-to-end:

- Q06: about 118.61 ms versus the 118.95 ms reference, tied;
- Q19: about 178.11 ms versus 175.29 ms, slightly worse.

Decoding rejected values traded away the only useful part of `Skip`. Rejected.

> **Re-run and re-rejected on 2026-07-14** — see the follow-up at the end of this
> document. If you are about to write a decoder that `ReadBatch`es a block into a
> scratch buffer and gathers the selection out of it, it has now been measured
> twice, and the second time it was **slower the denser the selection got**. Read
> that section first.

### 2. Direct numeric dictionary expansion

Hypothesis: for fully dictionary-encoded required numeric columns, call
`ReadBatchWithDictionary`, decode compact codes, and expand directly into the
final Ibex buffer instead of letting Apache produce physical values first.

Raw warm column timings sometimes improved:

- `l_quantity`: about 14 ms to 11.5 ms;
- `l_discount`: about 12.8 ms to 10.6 ms;
- `l_shipdate`: about 15 ms to 14 ms.

End-to-end results did not reproduce reliably. In an alternating Q06 run the
candidate averaged 114.0-114.8 ms while baseline averaged 113.2-113.3 ms.
Other queries occasionally regressed. The apparent per-column gain was lost in
code expansion, output writes, and run-to-run noise. Rejected.

### 3. Public page-reader selected decoder

Hypothesis: consume `RowGroupReader::GetColumnPageReader` directly:

- PLAIN required numeric pages: indexed little-endian loads only for selected
  rows;
- RLE_DICTIONARY pages: decode indices once, then perform dictionary lookups
  only for selected rows;
- other encodings: decode the page to scratch and gather;
- skip pages containing no selected rows.

The prototype correctly handled Q06 and Q19, dictionary pages, PLAIN fallback,
Data Page V1/V2 layout, and row-group-global selection indices.

Short wall-clock tests initially looked encouraging: Q19 appeared to fall from
about 174-177 ms to 168-169 ms. Longer task-clock tests reversed that result:

| variant | baseline CPU | candidate CPU | result |
|---|---:|---:|---:|
| all selected numeric pages | 19.152 s | 19.719 s | candidate +2.96% |
| only PLAIN/mixed chunks | 19.930 s | 20.424 s | candidate +2.48% |

The custom path lost Apache's decoder reuse and did not improve on its whole-
page skip behavior. Rejected.

### 4. Selection-preserving exposed dictionary indices

Hypothesis: keep Apache's column/page reader but use the supported
`ColumnWithExposeEncoding` plus `ReadBatchWithDictionary` only for fully
dictionary-encoded selected numeric columns. This avoided the custom page
reader and left mixed/PLAIN columns untouched.

Separate process measurements disagreed depending on ordering. A decisive test
then alternated candidate and baseline Q19 iterations inside one warm REPL. The
warmup count kept the strategy parity known, and each strategy received 100
measured iterations:

| strategy | mean | median | standard deviation |
|---|---:|---:|---:|
| dictionary-preserving candidate | 179.269 ms | 177.431 ms | 7.479 ms |
| baseline `Skip`/`ReadBatch` | 178.893 ms | 177.734 ms | 6.586 ms |

Mean paired delta was +0.376 ms for the candidate; it was faster in only 46 of
100 pairs. Rejected.

## Apache Parquet reader observations

Arrow 22's `TypedColumnReaderImpl::Skip` is better than its high-level shape
suggests:

- when the requested gap covers the rest of the current data page, it consumes
  the page without decoding its values;
- for a partial-page gap, it decodes into a reusable scratch buffer in batches
  of 1,024 values;
- it retains configured page and dictionary decoders across calls;
- `ReadBatch` itself stops at page boundaries, even when a larger batch was
  requested.

Consequences:

- replacing many sparse calls with dense decode is not automatically a win;
- rebuilding page decoding through the public `PageReader` loses internal
  state and optimizations;
- exposed dictionary indices still require decoding every index and writing a
  code buffer, which costs about as much as the dictionary expansions avoided
  at these sizes;
- a useful selection-aware decoder must skip within RLE/bit-packed runs and
  index PLAIN values without forcing rejected values through output scratch.

That capability is below the current public `TypedColumnReader` API.

## Current interpretation of the remaining gap

The original two-copy problem is fixed. The remaining gap is not another
obvious full-column copy. It is a combination of:

1. Apache Parquet encoding decode being slower than Polars' native reader for
   these files;
2. dense materialization of all predicate columns before evaluation;
3. selection and predicate evaluation living above the decoder rather than in
   the same batch loop;
4. repeated general-purpose mask, selection, and gather machinery;
5. residual predicate evaluation by the retained `Filter` node;
6. single-threaded decode and execution.

The first item is the largest. Q06's three dense predicate decodes plus one
sparse payload decode already consume substantially more time than the entire
single-threaded Polars query.

## Productive next directions

### 1. Selection-aware decoding inside the encoding decoder

The most direct decoder experiment now requires either an Arrow patch/fork or a
different decoder. The useful primitive would accept selected logical offsets
for a page and:

- advance over RLE runs without expanding rejected dictionary values;
- advance a bit-packed run without writing rejected indices;
- load selected PLAIN fixed-width values directly;
- preserve the configured dictionary and page decoder across pages;
- return selected values directly into final Ibex storage.

This must be microbenchmarked independently before changing the Ibex plugin.
Reimplementing it above `PageReader` has already been disproven.

### 2. Batch-local predicate evaluation

Instead of materializing all 6M rows of every predicate column, extend the lazy
source contract with a batch predicate callback:

```text
decode predicate batch -> runtime evaluates batch -> batch selection
                       -> decode/materialize payload survivors
```

This is closer to Polars' integrated prefiltered pipeline and can reduce peak
memory and full-column mask traffic. It is a larger interface change because
the plugin must call back into runtime expression evaluation without learning
Ibex expression semantics itself.

### 3. Mark or remove fully applied scan predicates

Once scan predicate application can prove complete, eliminate the residual
`Filter` evaluation. This is smaller than decoder work and will not close the
gap alone, but the current correctness-first duplication is measurable and
conceptually temporary.

### 4. Profile string dictionary remapping independently

Q19 spends about half its time in Parquet decoding and uses categorical string
predicates. The current global remap uses an ordered `std::map<std::string,
int32_t>`. The dictionaries are small, so this may not matter, but it should be
isolated before considering a hash-map or dictionary-identity fast path.

### 5. Page/row-group statistics for different data

Statistics are still structurally worthwhile, but they do nothing for the
current Q06 file because every row group overlaps the date predicate. Evaluate
them on clustered/sorted Parquet data rather than using Q06 as the acceptance
benchmark.

### 6. Parallel column decode

Predicate and payload columns are independent enough to decode concurrently,
and Polars' default reader uses 24 pipelines here. This belongs to the runtime
multithreading milestone. Single-threaded Polars remains the fair decoder
comparison until then.

## Paths not worth repeating without a lower-level change

- Dense 64K selected-value decoding above `TypedColumnReader`.
- Grouping sparse survivor rows into page-sized `ReadBatch` calls.
- Reimplementing mixed PLAIN/dictionary pages through public `PageReader`.
- Reading every numeric dictionary code and expanding it in Ibex.
- Using `ReadBatchWithDictionary` for selected numeric payloads without a way
  to skip rejected RLE/bit-packed indices.
- Comparing isolated short wall-clock runs on WSL2.

## Verification record

For the landed implementation:

- all 1,046 CTest cases passed;
- `scripts/ibex-e2e.sh` passed, including Parquet plugin smoke, null round-trip,
  batch boundaries, and cross-row-group categorical dictionary remapping;
- official SF-1 answers passed for Q01, Q03, Q05, Q06, Q10, and Q19;
- Q06/Q19 answers were rerun after every decoder prototype;
- all rejected prototype changes were removed;
- `build-release/tools/parquet.so` was rebuilt from the clean committed source.

At the end of the investigation, the tracked worktree matched `400315c`.
Only unrelated pre-existing untracked plan files remained.

## Follow-up 2026-07-13: the gap was mostly Ibex-side, not Apache decode

A standalone microbenchmark (bare `TypedColumnReader::ReadBatch` into a
preallocated buffer, outside Ibex entirely) overturned the "Apache decode is
the largest item" interpretation above:

| column | Ibex `direct_column` | bare Apache `ReadBatch` | custom RLE_DICT decoder | memcpy floor |
|---|---:|---:|---:|---:|
| `l_quantity` (dense) | 14-15 ms | 7.3 ms | 5.0 ms | 2.9 ms |
| `l_discount` (dense) | 12-13 ms | 6.2 ms | 4.5 ms | 2.4 ms |
| `l_extendedprice` (dense) | — | 10.4 ms | 10.5 ms | 2.4 ms |

Bare Apache decodes q06's four columns in ~29 ms — the same ballpark as
single-threaded Polars' whole 32-38 ms query. Ibex's decode layer was paying
2x on top of Apache, and the reason is:

**Every TPC-H column is optional.** parquet-cpp-arrow writes columns as
optional even when no value is null, so `decode_numeric_column`'s bulk
fast path (gated on `max_definition_level == 0`) was dead code on every real
file. All dense decodes fell through to `decode_physical_column`'s per-value
`emit` lambda: per-value `push_back`, per-value validity-bit append, per-row
selection branch.

### Fix: gate fast paths on chunk statistics `null_count == 0`

`chunk_has_no_nulls` (column-chunk stats) now unlocks, per row group:

1. **Dense bulk decode** for optional-but-null-free columns — `ReadBatch`
   straight into final Ibex storage (def levels consumed into scratch and
   checked via `levels_read == values_read`; a lying stat throws).
2. **Categorical bulk remap** — code loop without the validity/keep branches,
   def levels not read at all.
3. **Selected decode, scattered selections** — dense batch decode (no def
   levels) + gather of only the selected offsets, with early exit once the
   selection is exhausted. This replaced routing selections through `Skip`.
4. **Selected decode, needle-in-haystack** (mean gap > 64K rows) — the
   `Skip`-based run path.

### Two traps hit on the way

- **`Skip` decodes partial-page gaps.** Routing q19's scattered selection
  (214k rows, mean gap ~26) through `Skip`/`ReadBatch` run pairs cost +60 ms
  end-to-end (q19 190→234 ms) — `Skip` only avoids decode when a gap covers
  the rest of a page. Hence the sparsity heuristic in (3)/(4).
- **The old `Skip` path threw on page-boundary short reads.** `ReadBatch`
  stops at page boundaries; the run read needed a loop. Latent because the
  path was dead (required columns only) — it broke q01 the moment null-free
  optional columns started taking it.

### Paired results (same box, plugin-only A/B, 12 iters, avg)

| query | before | after | change |
|---|---:|---:|---:|
| q01 | 340 ms | 309 ms | -9% |
| q03 | 276 ms | 185 ms | -33% |
| q05 | 261 ms | 216 ms | -17% |
| q06 | 117 ms | 86 ms | -27% |
| q10 | 339 ms | 253 ms | -25% |
| q19 | 179 ms | 99 ms | -45% |

Geomean -27%. All six official SF-1 answers verified, full ctest (1046) and
`scripts/ibex-e2e.sh` pass (null round-trip unaffected: files with real nulls
have `null_count > 0` stats and keep the level-aware path).

### Remaining decoder headroom

The custom RLE_DICTIONARY decoder (hand bit-unpack + dict gather over raw
`PageReader` buffers) is 1.4-1.5x faster than Apache on fully dict-encoded
numeric columns (7.3→5.0 ms) and neutral on PLAIN, so "replace Apache's
encoding decoder" is real but now second-order (~5 ms/query on q06-shaped
scans). Dense predicate-column decode is now the largest single component
again; batch-local predicate evaluation (direction 2 above) is unchanged in
value.

## Follow-up 2026-07-14: dense string columns never got the null-free bulk path

The `null_count == 0` fast paths of the previous follow-up were added to
`decode_numeric_column` and to the categorical remap. **Non-dictionary string
columns never got one.** Their two bulk paths in `decode_physical_column` are
both gated on `selection != nullptr`, so a *full* read of a dense string column
always fell through to the level-aware per-value loop.

### Where a dense string column's time went

`l_comment`, 6,001,215 rows, ~160MB of text, warm cache, single-threaded:

| reader | time |
|---|---:|
| Polars (native decoder) | 50 ms |
| bare pyarrow `read_table`, 1 thread | 70 ms |
| **Ibex before** | **113 ms** |
| **Ibex after** | **76.6 ms** |

Ibex was paying ~40ms *on top of Apache* — larger than Apache's own gap to
Polars. Three causes, all removed by `decode_string_column`:

1. definition levels decoded for a chunk whose statistics prove it has no nulls;
2. a validity bit appended per row, building an all-true bitmap;
3. **the character buffer growing by doubling.** `out.reserve(output_rows)`
   reserves the *offsets* only — `Column<std::string>::reserve(n, chars_hint)`
   leaves `chars_` alone when `chars_hint` is 0, so 160MB of text was re-copied
   through the whole doubling chain. The column chunk's
   `total_uncompressed_size()` is a close upper bound on the decoded characters
   (it also carries a 4-byte length per value), so one reserve removes it.

Isolating (3): bulk decode without the chars reserve lands at 92ms; with it,
76.6ms. So (1)+(2) are worth ~20ms and (3) ~16ms.

### Measured effect

ABBA, min of 8 iterations in one warm REPL (the only method that survives WSL2
scheduling noise — the interleaved end-to-end query runs were ±10% and could not
resolve this):

| scan | before | after | change |
|---|---:|---:|---:|
| `l_comment` (6.0M rows, ~160MB) | 113.0 ms | 76.6 ms | **-32%** |
| `o_comment` (1.5M rows, ~73MB) | 101.5 ms | 89.0 ms | -12% |

End-to-end TPC-H moves less than that, and for a structural reason worth
recording: **most TPC-H string predicates are low-cardinality and therefore take
the categorical path**, which already had its bulk decode. Only `o_comment` (q13)
and `l_comment` are dense high-cardinality. q13 improves ~5-10%; the rest is
within noise. The fix matters for text-heavy data generally, not for this
benchmark specifically.

### Remaining gap after this

Ibex dense string decode (76.6ms) is now essentially at Apache's floor (70ms).
The residual 70 vs 50 is Apache's encoding decoder versus Polars' native one —
the same 1.4-1.5x this document already measured for RLE_DICTIONARY numerics.
Closing it means replacing the encoding decoder, not rearranging calls above it.
That is now the *only* remaining decode item; the Ibex-side overhead is gone.

## Follow-up 2026-07-14 (2): native decoding is a dead end; the cost is the append

Question asked: can a native encoding decoder close the remaining gap to Polars?
**Measured answer: no.** A standalone microbenchmark (`ParquetFileReader` +
`GetColumnPageReader`, hand-written page decode straight out of the raw page
buffers, compared against `TypedColumnReader::ReadBatch` in the same harness):

| column | Apache | native page decode | ratio |
|---|---:|---:|---:|
| `l_quantity` (RLE_DICT double) | 5.6 ms | 4.9 ms | 1.15x |
| `l_discount` | 5.1 ms | 4.6 ms | 1.11x |
| `l_extendedprice` (PLAIN/mixed) | 8.9 ms | 7.7 ms | 1.15x |
| `l_shipdate` (INT32) | 5.2 ms | 4.3 ms | 1.21x |
| `l_comment` (PLAIN BYTE_ARRAY) | 112 ms | **126 ms** | **0.93x — slower** |

Two things changed since this document claimed a 1.4-1.5x custom-decoder win:
Arrow's SIMD was enabled (`ARROW_SIMD_LEVEL`), so Apache's decoders got faster,
and the numeric headroom shrank to ~1.15x — about 3ms on a q06-shaped scan, not
worth the maintenance. For strings, hand-rolling is outright *slower*: Apache's
byte-array decoder is fine.

### Where the string time actually goes

Same harness, same Apache decode, only the *append* changed:

| | time |
|---|---:|
| Apache decode + `push_back` per value (vector::insert + offsets push) | 112 ms |
| Apache decode + raw cursor into presized storage | **55 ms** |

The decode was never the problem — **filling `Column<std::string>` was**. Hence
`Column<std::string>::begin_bulk_append` (sizes the flat buffers once from the
chunk's byte bound, then writes through cursors) and `NoInitAllocator` on
`offsets_`/`chars_` so that sizing them does not first zero-fill 160MB.

### The other half: page faults

`/usr/bin/time -v` on four `l_comment` decodes: **107,766 minor page faults**,
0.17s *system* time against 0.28s user. Every decode allocates a fresh ~174MB
character buffer and first-touches every 4KB page of it. That is ~40ms of the
~70ms decode, and it is why the bulk-append win (2x in a hot-buffer
microbenchmark) shows up as only ~5ms end to end.

The runtime's existing `tune_allocator_once()` (`M_MMAP_MAX=0`,
`M_TRIM_THRESHOLD=-1`) is doing real work here — disabling it takes the fault
count from 107k to 185k — but it cannot eliminate first-touch on a fresh buffer.

### Allocator results (asked: why not test under jemalloc?)

| allocator | faults | wall | sys |
|---|---:|---:|---:|
| glibc + existing mallopt tuning | 107.8k | 0.44 s | 0.20 s |
| jemalloc, **default** | 361.4k | 0.68 s | 0.39 s |
| jemalloc, `dirty_decay_ms:-1,muzzy_decay_ms:-1` | 56.1k | 0.38 s | 0.10 s |
| glibc + `GLIBC_TUNABLES=glibc.malloc.hugetlb=1` | 107.5k | 0.46 s | 0.19 s |

Note jemalloc **out of the box is far worse** than tuned glibc — its decay
madvises the pages straight back. Tuned, it halves the faults and is worth ~14%
on a one-shot `ibex_eval` run. It does **not** move the warm-REPL steady-state
minimum, which is what `bench_ibex.py` measures, so the published PDS-H numbers
are not distorted by the allocator. Worth knowing: jemalloc is linked into
`ibex_bench` only — **not** into `ibex`/`ibex_eval`, which is what the PDS-H
harness actually drives.

### Landed, interleaved, min-of-8 in one warm REPL

| variant | `l_comment` scan |
|---|---:|
| before | 116 ms |
| + null-free bulk string decode, chars reserve | 80 ms |
| + bulk append through cursors, no-init buffers | **75 ms** |

q13 end-to-end (interleaved): 277-284ms → 260-268ms. Modest because `o_comment`
is 1.5M rows and the join plus two group-bys dominate.

### The real remaining lever

Polars decodes `l_comment` in 50ms; Apache-into-hot-buffers is 55ms; our plugin
is ~70ms. **The gap is not the decoder — it is that we hand a fresh 174MB buffer
to the kernel and fault it in on every query.** The next lever is buffer reuse (a
decode arena / pooled column storage), not a new decoder. Do not re-run the
native-decoder experiment; it is measured and rejected twice now.

## Follow-up 2026-07-14 (3): buffer reuse is NOT the lever — and decode is now at Polars' floor

The "next lever is buffer reuse" conclusion from the previous section was drawn
from whole-process fault counts, which fold in startup. Measuring the *marginal*
per-iteration cost (faults with N=1 vs N=9, divided by 8) overturns it:

| query | faults/iter | first-touched/iter | sys/iter |
|---|---:|---:|---:|
| `l_comment` scan | 5,640 | 22 MB | 25.0 ms |
| q06 | 3,680 | 14 MB | 12.5 ms |
| pure aggregate | **0** | 0 MB | 1.3 ms |

The existing `tune_allocator_once()` mallopt already recycles buffers: a pure
aggregate faults **zero** times per iteration, and the string scan re-faults only
22MB of the 174MB it allocates. **A decode arena would be chasing ~5ms.**

### The system time is `pread`, and Polars pays it too

`strace -c`: **pread64 is 87% of syscall time** — 27 calls at ~3ms each, one per
column chunk, copying the chunk out of the page cache. ~20ms per iteration on the
string scan. That is the system time, not page faults.

It is not a lever either:

- **mmap is far worse.** Swapping `ReadableFile` for `MemoryMappedFile` took the
  scan from 75ms to **353ms** (user time 7x). Rejected.
- **Polars does not avoid the copy.** `read_parquet(memory_map=True)` = 57.3ms,
  `memory_map=False` = 56.8ms. No difference. They pay the same read.

### Where that leaves the decode

Apache decode + pread + raw-cursor append, in the microbenchmark: **55ms**.
Polars: **57ms**. That is parity — and the bulk append has now landed, so the
dense-string decode path is at Polars' floor. There is no decode work left worth
doing single-threaded.

### q13 decomposed (~260ms), the worst query vs polars-st

| stage | time |
|---|---:|
| orders scan + `!like` (decode ~60ms + fragment match ~30ms) | 90 ms |
| left join (150k x 1.5M) | 73 ms |
| group-by c_custkey (150k groups) + count | 74 ms |

Polars-st does all of q13 in 164ms. **Decode is no longer the dominant term** —
the join and the group-by together are 147ms. Single-threaded work should go
there next, not into the scan.

## Follow-up 2026-07-14 (4): rejected experiment 1, re-run and re-rejected

A working tree turned up carrying `decode_selected_numeric_column` — a selected
numeric decoder that `ReadBatch`es 64K values into a scratch buffer and gathers
the selection's offsets out of each batch, taking that path whenever the chunk
statistics prove the row group null-free and the selection is not extremely
sparse. That is **rejected experiment 1 above**, rebuilt: "a required-column loop
that decoded 64K values and iterated only selection offsets falling in each
batch." It was measured again, from scratch, and rejected again for the same
reason. Recording it here because the entry above evidently was not enough to
stop it being written a second time.

It was correct — 1097/1097 tests, all 9 official SF-1 answers — so what follows
is a performance verdict only.

### The dense path was taken by every PDS-H scan, not just dense ones

The guard was `group_rows / group_selected > kDirectDecodeBatchRows` (65536).
`lineitem.parquet` has 6 row groups of 1,048,576 rows, so that declines the
dense path only below **~16 selected rows per row group**. Every filtered scan
in the suite — down to a 0.009%-selective needle — took it. The guard reads like
a sparsity check and is not one.

### Whole-query A/B says nothing, and that is the first finding

Plugin-only A/B (base = the parent commit's `parquet.hpp`, identical REPL binary
and runtime; interleaved, `taskset`-pinned, min-of-5 x 3 repeats):

geomean **+0.8%** — and *every* query's delta was smaller than that query's own
repeat-to-repeat spread on the base side (q10: delta +1.6%, base spread 7.3%).
A whole-query harness on this box cannot see a decoder change at all; the join
and group-by noise buries it. Isolate the scan statement or do not measure.

### Isolated scan: the dense path never wins, and loses where it should win most

`lineitem[filter <pred>, select { r = sum(l_extendedprice) }]` — payload decoded
under a selection. Interleaved, pinned, min-of-10 per repeat, 3 repeats:

| predicate selectivity | Skip path | dense + gather | delta |
|---|---:|---:|---:|
| 72.3% (`l_shipdate >= 1994-01-01`) | 58.0 ms | 59.9 ms | **+3.2%** |
| 24.6% (`l_returnflag == "R"`) | 34.1 ms | 32.7 ms | -4.2% |
| 1.9% (q06's predicate) | 39.9 ms | 39.5 ms | -1.2% |
| 0.009% (558 rows) | 37.2 ms | 36.9 ms | -0.8% |

The middle three are inside the drift (a separate set put the same three at
-1.7%, +5.4%, +4.6%). The one delta that reproduced across both sets is the
**densest** selection, and it is a **loss** (+8.8% in the first set, +3.2% here).

The mechanism is the same one that sank the original experiment, and the shape of
the table explains why there is no threshold to tune:

- At every selectivity tested, the surviving rows still touch **every page**, so
  both paths decode essentially every value. Skip's whole-page skip — its only
  real saving — almost never fires on a scattered predicate.
- The dense path therefore adds a 64K-value scratch write plus a gather read on
  top of the same decode. That cost scales with the number of values decoded,
  which is why the loss is **largest at 72%** and vanishes into noise at 1.9%.

So the payoff curve runs the wrong way: the path is a wash exactly where it is
cheap and a loss exactly where it is exercised. A threshold that never loses is a
threshold that never fires. Reverted.

### The row-group batch decoder in the same tree, also a dead end

The tree also carried a `BatchColumnDecodeFn` on `LazyTable` (decode one row
group, apply the predicate to it, then decode that batch's payload with a
batch-local selection) with a `project_where` path to drive it. It was wired but
**disabled** — `read_parquet_lazy` passed `batches = 0` — with the author's note
that enabling it "reopens Arrow column readers and loses their page/decoder
state, which is materially slower on mixed pages." That is rejected experiment 3's
finding again ("the custom path lost Apache's decoder reuse"). Reverted with the
rest; batch-local predicate evaluation stays listed under productive directions,
but it only pays *inside* the decoder, not by re-entering it per row group.

### What this does not touch

"Selection-aware decoding inside the encoding decoder" (productive direction 1)
is still the open lever and is still unattempted. Every rejection so far,
including this one, has been an attempt to get selection-awareness by
rearranging Apache's **public** API — `Skip`/`ReadBatch`/page readers — from
above. Four attempts, four rejections, one mechanism: from above the decoder you
can only choose to decode values you do not want, or to pay per value you do.
