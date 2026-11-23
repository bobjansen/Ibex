# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Running a native pipeline and returning its result as one Arrow table."""

import os
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa


def join_chunks(tables):
    """Ibex's result chunks as one Arrow table, sharing their buffers.

    A column's nullability can differ between chunks (one chunk has no nulls
    and says so), and Arrow joins only equal schemas: a field is nullable when
    it is in any chunk, and each chunk is relabelled, not copied.
    """
    first = tables[0].schema
    if all(table.schema.equals(first) for table in tables[1:]):
        return pa.concat_tables(tables)
    fields = [field.with_nullable(any(table.schema.field(index).nullable for table in tables))
              for index, field in enumerate(first)]
    schema = pa.schema(fields, metadata=first.metadata)
    return pa.concat_tables([table if table.schema.equals(schema)
                             else pa.Table.from_arrays(table.columns, schema=schema) for table in tables])


def execute_table(source, tables=None, scalars=None):
    """The table `source` produces, as the chunks Ibex made it in.

    `execute_pipeline` joins every chunk into one table on one thread before
    handing it over: 60 ms of a 10M-row Parquet read. Streamed, the chunks are
    kept as they are and joined without a copy. A pipeline that streams no
    chunk produced no table (a write returns its row count), and is run
    through `execute_pipeline` for that result.
    """
    import ibex_pyarrow
    chunks = []
    ibex_pyarrow.execute_pipeline_stream(source, chunks.append, tables=tables, scalars=scalars)
    if chunks:
        return join_chunks(chunks)
    return ibex_pyarrow.execute_pipeline(source, tables=tables, scalars=scalars)


def _worker_count():
    """The CPUs this process may use, capped by `IBEX_CORES` as the engine is."""
    try:
        available = len(os.sched_getaffinity(0))
    except AttributeError:  # macOS and Windows have no affinity mask
        available = os.cpu_count() or 1
    try:
        return max(1, min(available, int(os.environ.get("IBEX_CORES", available))))
    except ValueError:
        return available


def cast_chunks(column, target):
    """`column` cast to `target`, its chunks cast in parallel.

    Arrow casts a chunked array one chunk at a time on one thread; decoding a
    10M-row dictionary column to strings that way took 95 ms. Each chunk's cast
    releases the GIL, so a thread per chunk divides it.
    """
    workers = min(column.num_chunks, _worker_count())
    if workers < 2:
        return column.cast(target)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return pa.chunked_array(list(pool.map(lambda chunk: chunk.cast(target), column.chunks)),
                                type=target)
