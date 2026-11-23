# Parallel Parquet writer

**Status: planned, 2026-11-23.** Replace `parquet::write` (one `parquet::arrow::WriteTable`
call, one thread, uncompressed) with a writer that encodes column chunks on the worker pool
and writes them in order, and let `IbexEngine.sink_parquet` use it instead of handing every
batch to Polars' writer.

## Why

`IbexEngine.sink_parquet` runs the query in Ibex and hands each result batch to Polars' own
streaming Parquet writer (`857f0709`). That writer is CPU-bound on every core, and so is the
query, so streaming overlaps nothing: on the demo's wide filter + derive (9.9M rows, 204 MB,
8 P-cores) the Ibex sink takes 273 ms against Polars' 228 ms, and the same as Ibex collect +
`write_parquet` (266 ms). Ibex loses every query that passes most rows through (1.2-1.3x) and
wins only where the output is small. The handing also has to cross into Python per batch,
convert to Polars frames, and serialize through the engine's lock (the 11-23 affinity
deadlock, `8a35f4c8`).

Writers measured on that 9.9M-row result, 8 P-cores (2026-11-22):

| Writer | Time | Cores busy |
|---|---:|---:|
| Polars (zstd, statistics, dictionary where it pays) | 165 ms | 7.4 |
| pyarrow (Arrow C++ `write_table`), uncompressed | 1,240 ms | ~1 |
| Ibex `parquet::write` (Arrow C++ `WriteTable`), uncompressed | 2,000 ms | ~1.2 |

Arrow C++'s writer encodes one row group at a time, a column at a time; that, not Parquet, is
the gap.

## Targets

- The writer alone, from a resident Ibex table: at or under Polars' 165 ms for the wide result
  at 8 cores, with zstd and statistics, within 5% of Polars' file size.
- `IbexEngine.sink_parquet` on the wide query at or under Polars' own sink (228 ms); the
  small-output queries keep their current lead.
- Output independent of thread count and chunking: the same input gives the same bytes.

## What Polars writes (the defaults to match)

From the sink above: row groups of about 122k rows (82 for 9.9M), zstd on every column,
statistics on every column, dictionary encoding chosen per column (`symbol`, `day`, `qty`,
`venue` dictionary; `trade_id`, `price`, `notional`, bools plain), and an `ARROW:schema`
key-value entry so readers recover Arrow types (Int32 widths, Categorical as dictionary,
timezones). Phase 0 pins its exact rules (row group sizing, data page size, zstd level,
dictionary fallback threshold) from its source and output.

## Design

**Unit of work: a column chunk.** One task encodes one (row group, column) into its own
buffer: an optional dictionary page, then data pages, each a Thrift `PageHeader` plus its
compressed payload, and the chunk's `ColumnMetaData` (encodings, sizes, statistics) with page
offsets *relative to the chunk's start*. Tasks share nothing; very wide chunks split further by
page.

**Sequencer.** Chunks are written to the sink in (row group, column) order as they finish,
each one's offsets rebased to where it lands, and its metadata appended to the row group's.
The footer is the Thrift `FileMetaData` (schema elements, row groups, `ARROW:schema`,
`created_by`), its length and `PAR1`. In-flight row groups are bounded (about two per worker)
so memory is set by the window, not the result.

**Input.** The writer takes Ibex chunks (the `TableChunkSink` that `interpret_chunks` drives,
`f389618e`) and cuts them into row groups of the target size; a resident table is just one
chunk. So a query streams into its file with no materialized result and no Python in the
path.

**Building blocks, all in Arrow 22 (pinned in `libs/parquet/CMakeLists.txt`):**

- Value encoding: `parquet::MakeTypedEncoder` (PLAIN, RLE_DICTIONARY with its dictionary
  encoder), and the RLE level encoder for definition levels.
- Compression: `arrow::util::Codec` (zstd through the GCC-built zstd, `be821bbe`; snappy, lz4,
  uncompressed).
- Statistics: `parquet::TypedStatistics`, encoded as Parquet writes them.
- Thrift: the generated `parquet::format` types (`generated/parquet_types.h`) and Arrow's
  compact-protocol serializer (`parquet/thrift_internal.h`). That header is internal: if it
  shifts under a later Arrow, the fallback is a small compact-protocol writer of our own
  (only `PageHeader`, `ColumnMetaData` and `FileMetaData` are needed).

**Types.** Int64 (compact Int32 storage written as INT32 when the declared type is Int32, as
the adapter knows), Double, Bool, String (dictionary when it pays, falling back to PLAIN per
chunk past a dictionary size limit, as parquet-cpp does), Categorical (its codes written
straight as RLE_DICTIONARY over its dictionary: no re-encoding, the cheapest column of all),
Date (INT32 DATE), Timestamp (INT64, ns, with its zone), Decimal (INT32/INT64/FLBA by
precision). Nullable columns write definition levels; non-null columns are declared
`required` only when the declared schema says so.

**Considered and set aside:** Arrow's `FileWriter` with `ArrowWriterProperties::use_threads`
encodes a row group's columns in parallel but row groups one after another, so it scales to
the column count (9 here) at best, and the 2,000 ms single-thread figure suggests that is not
enough. Phase 0 measures it anyway; if it reaches the target, this plan stops there.

## Phases

0. **Measure.** Arrow `FileWriter` + `use_threads` + zstd from an Ibex table, and Polars'
   writer split into encode and compress time, on the wide, filtered and group-by results.
   Pin Polars' defaults. Decide whether custom chunk assembly is needed (expected: yes).
1. **Fixed-width chunks.** Int64/Int32/Double/Bool/Date, no nulls, PLAIN, zstd; sequencer
   and footer. Round-trip tests read with Arrow C++, pyarrow and Polars.
2. **Everything else.** Nulls; String (PLAIN and dictionary with fallback); Categorical as
   dictionary; Timestamp and Decimal; statistics; `ARROW:schema`.
3. **Streaming and native use.** Chunk input with the bounded window; `parquet::write`
   switches to the new writer, with named arguments for compression, level, row group size
   and statistics; a lazy plan streams into it. SPEC.md, docs and an `.ibex` example.
4. **IbexEngine.** A bridge call that runs the query and writes the file in C++ with the GIL
   released. `sink_parquet` uses it for a local path with options the writer supports
   (compression and level, statistics, row group size, data page size); `PartitionBy`, cloud
   targets, file objects and anything else keep the current Polars-writer path. Benchmarks
   against Polars' sink on the demo queries and a PDS-H-sized write.

## Verification

- Round trip, property-style: random schemas and sizes (empty, one row, exactly and just past
  a row group, all null, no nulls, long strings, high and low cardinality), every type,
  written by Ibex and read back by Arrow C++, pyarrow, Polars and DuckDB, equal to the input.
- Same bytes at 1, 2 and 8 threads, and for one chunk or many.
- Statistics and encodings checked against what the readers report; files validated with
  Arrow's metadata reader.
- Performance on the 8 P-cores against Polars run in the same sitting: wall and CPU time,
  file size; A/Bs of the native suites where a script writes Parquet.
- Run on the GCC wheel build too, not only Clang (`project-gcc-wheel-codegen`).

## Open decisions

- **Default compression for `parquet::write`.** Today it writes uncompressed. Matching Polars
  (zstd) makes files several times smaller and changes existing scripts' output size.
- **How close the IbexEngine sink must match Polars' file.** Current tests require identical
  schema and metadata (`check_metadata=True`); a native writer gives the same logical schema
  and data, but its own `created_by` and possibly different row group and page boundaries.
- **Scope of `sink_parquet` options** served natively in phase 4, and whether the
  Polars-writer path stays as the fallback.
