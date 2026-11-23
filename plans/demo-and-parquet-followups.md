# Demo and Parquet read/write follow-ups

Status: open, 2026-11-23. What is left after two arcs:

- the ibex-polars demo's short in-memory queries (10M rows, i7-13700,
  `taskset -c 0,2,..,14`, `IBEX_CORES=8`, `POLARS_MAX_THREADS=8`), which closed
  with the global aggregate level with Polars and every other query ahead;
- the Parquet read/write findings against Polars 2.1 on a 10M-row
  Polars-written trades file (int64 `trade_id`, String `symbol`, int32 `day`,
  f64 `price`, int64 `qty`, bool, Categorical `venue`).

The work that landed: `caf4b923` through `340aa284` (demo), then `95998530`,
`8401244c` (one-core global aggregate), `447aadfc`, `9c28c6dc` (bare reader
calls go through the planner), `3d4276db` (writer dictionary pages),
`0b7868f5` (Categorical written as a dictionary column) and `575f148d`
(`ibex.LazyFrame.collect` keeps native chunks). Each item below says what
reopens it.

## 1. Accepted for now

**a. Int32 is written as Int64.** Ibex has no Int32 type: an Int64 column
held as int32 (`is_compact_int32`) is a storage choice made from the values,
so writing those as Int32 would make the file schema depend on the data. A
native `parquet::write(parquet::read(p), q)` turns `day: Int32` into Int64.
The Polars sink is unaffected: it passes Polars' schema. Fixing it needs the
source's declared width carried with the column (a `ColumnMeta` field set by
the readers and kept through operators, as the time zone is). *Reopen* when a
user needs native copies to keep the schema.

**b. A String stored as codes is written as Categorical.** `0b7868f5` writes
every `Column<Categorical>` as `dictionary<int32, utf8>`. Readers store
plain strings as codes on their own, so a native copy turns `symbol: String`
into Categorical. Chosen over the alternative: a `ColumnMeta` flag for "came in
as a dictionary", set by the Parquet reader (from `ARROW:schema`) and the
Arrow import, and written only when set. *Reopen* together with 1a; the same
metadata carrier solves both.

## 2. Python result path

**a. `ibex_pyarrow.execute_pipeline` still joins its chunks on one thread.**
`MaterializeOperator` concatenates every chunk before export: ~60 ms of a
111 ms 10M-row read at 8 cores. `ibex.LazyFrame.collect` and the Polars engine
no longer call it for tables (`ibex._execute.execute_table` streams), but any
direct caller still pays. Fix: build the endpoint's result from the streamed
chunks as a chunked Arrow table (nullability unified as `join_chunks` does).

**b. A streamed query that produces no table is run twice.**
`execute_table` falls back to `execute_pipeline` when the stream delivered no
chunk, which happens only for a write returning its row count. A write
through that path would write its file twice. `collect` cannot produce one
and the engine's writes go through `sink_parquet`, so nothing reaches it
today. Fix: have `execute_pipeline_stream` return the statement's scalar
result instead of `None`.

**c. Importing an Arrow table is serial.** Binding the 10M-row trades table
from Python (`tables={"t": ...}`) took 64 ms on one core, mostly interning
strings. *Reopen* if a workload binds large Python tables per query.

**d. `scan_parquet(p).select(pl.len())` is refused.** The adapter raises
`NotImplementedError: empty expression lists are not supported yet` (Polars
pushes an empty projection into the scan). There is no fallback by design, so
the query fails rather than running on Polars.

## 3. Parquet writer

Native copy is now 174 ms against Polars' 185 at 8 cores (1195 ms against
1348 at one core); read → filter → derive → write 218 against 223. What is
left:

- **Nullable chunks keep Arrow's encoder.** `write_dictionary_numbers` and
  `write_dictionary_text` take null-free chunks only; a chunk with nulls goes
  through `ColumnWriter` with the per-value hash and RLE flush the new path
  removed. Needs definition levels per page (RLE runs of 0/1) and indices for
  the present values only.
- **High-cardinality doubles come out 3% larger than pyarrow's file.** Arrow's
  pessimistic page-size estimate cuts several pages per chunk, and each early
  page packs at the narrower width of the dictionary so far. Ours writes one
  page per chunk. Smaller pages would close it, at the cost of more headers and
  statistics.
- **Remaining hot spots:** zstd itself (the largest share), hashing every row
  of a plain `Column<std::string>` in `build_text_dictionary` (Python-imported
  text; native reads hand over codes), and the copy in `build_values` when a
  row group spans more than one input piece.

## 4. In-memory engine

**a. The few-groups aggregate materializes `price * qty`.** The demo's
`group_by(venue, is_buy).agg(vwap = sum(price * qty) / sum(qty))` computes the
product as a 10M-row update column (~3.9 ms, bandwidth-bound) before
aggregating it. Removing it needs expression inputs accumulated inside the
aggregate.

**b. The Int-output branch of `write_fixed_width_numeric_binary` reads through
`IntegerView`,** whose per-element width test keeps the loop scalar. The
Double branch reads physical values directly since `97bcaf5c`.

**c. Distinct over several chunks misses the dense path.** The dense
first-occurrence distinct (`try_dense_whole_input`) needs the whole input as
one chunk, so a distinct over a Parquet scan takes the hash path.

**d. `saw_null_key_` in the typed top-k is untested.** It routes chunks after
one with a null sort key to the boxed path. Only a single-threaded top-k over
several input chunks, some with null keys (a lazy scan), reaches it, and no
test builds that input; a deliberate break of it still passes.

**e. The fused conjunction's 1-2 leaf exclusion was measured at 2048-row
blocks.** `try_fused_conjunction` declines one leaf and two pair-kernel
leaves because they read 3-6% slower block by block. Blocks are now 64 rows,
which changed the trade-off for three leaves; re-measure before widening.

**f. `distinct_symbol` read +7-8% at one core** in two A/Bs of `8401244c`,
whose code it does not run (`prices[distinct { symbol }]`: no filter, no
aggregate). Taken as code layout; not verified.
