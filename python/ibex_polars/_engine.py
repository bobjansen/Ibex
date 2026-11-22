# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine entry point and bounded, weakly owned prepared-plan cache."""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
import weakref

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np

from . import _categories
from ._adapter import Adapter, check_version, flag_key
from ._temporal import is_temporal, storage_type

check_version()

from polars.io.plugins import register_io_source
from polars.lazyframe.query_result import SingleNodeQueryResult


def _dictionary_codes(column, name):
    """(labels, codes): a dictionary column as one label array and its codes.

    A streamed result's chunks each carry the dictionary of the scan unit they
    came from, usually with the same labels. A chunk whose dictionary equals
    the one before it keeps its codes; another dictionary is appended to the
    labels and its chunk's codes shift by where it starts. `labels.gather(codes)`
    is then the column: one parallel Polars gather, where one call per chunk
    or Arrow's serial unification of 41 chunks of 9.9M rows cost 25 ms more.
    """
    dictionaries, shifted, size, start = [], [], 0, 0
    for chunk in column.chunks:
        dictionary = chunk.dictionary
        if not dictionaries or not dictionary.equals(dictionaries[-1]):
            start = size
            dictionaries.append(dictionary)
            size += len(dictionary)
        indices = chunk.indices
        # Codes are non-negative: an int32 view as UInt32 is exact and free.
        indices = indices.view(pa.uint32()) if indices.type == pa.int32() else indices.cast(pa.uint32())
        shifted.append(pc.add(indices, pa.scalar(start, pa.uint32())) if start else indices)
    if size >= 1 << 32:
        raise RuntimeError(f"the dictionaries of {name} have more than 2^32 labels")
    # One chunk: a gather from chunked labels costs five times as much.
    labels = pl.from_arrow(pa.chunked_array(dictionaries, type=column.type.value_type)).rechunk() \
        if dictionaries else pl.Series(name, [], dtype=pl.String)
    codes = pl.from_arrow(pa.chunked_array(shifted, type=pa.uint32())).alias(name)
    return labels, codes


def _categorical_from_arrow(column, dtype, name):
    """Translate the dictionary labels once, then gather Polars category ids.

    Arrow dictionaries are local to each chunk; their ordinals need not match
    the requested Polars category mapping, so the labels are cast to `dtype`
    and their ids gathered by code. One streaming query does the gather and
    `cat.to`, whose check of every id then runs on every thread.
    """
    labels, codes = _dictionary_codes(column, name)
    # `categorical` keeps a named mapping alive until the ids have their dtype.
    categorical = labels.cast(pl.String).cast(dtype, strict=True)
    result = pl.DataFrame([codes]).lazy().select(
        pl.lit(categorical.to_physical()).gather(pl.col(name)).cat.to(dtype).alias(name)
    ).collect(engine="streaming").to_series()
    del categorical
    return result


def _strings_from_dictionary(column, name):
    """A dictionary-encoded Arrow column as plain Polars strings.

    Ibex keeps a repetitive String column dictionary-encoded and exports it so.
    Arrow's cast to strings, then Polars' import of them, copied every row
    twice on one thread: 136 ms for 9.9M rows of 5000 labels. Polars instead
    gathers from the small label column by code, in parallel: 6 ms.
    """
    labels, codes = _dictionary_codes(column, name)
    return pl.DataFrame([codes]).lazy().select(
        pl.lit(labels.cast(pl.String)).gather(pl.col(name)).alias(name)).collect().to_series()


def _known_codes(column, dtype, name, categories):
    """(codes, Known) when every chunk carries the same dictionary Ibex was given.

    The column's Polars ids are then a function of its codes alone: the codes
    plus the Known's offset when its ids are consecutive, or a lookup in its
    ids otherwise. The caller applies that inside one Polars query, so the
    columns convert in parallel and no codes are copied here.
    """
    known, parts = None, []
    for chunk in column.chunks:
        found = _categories.known_for(chunk, categories, dtype)
        if found is None or (known is not None and found is not known):
            return None
        known = found
        # Codes are non-negative, so viewing them as UInt32 is exact and free.
        parts.append(pl.from_arrow(chunk.indices.view(pa.uint32())))
    if known is None:
        return None
    return pl.concat(parts, rechunk=False).alias(name), known


def _known_ids(name, known):
    """The Polars ids of column `name`'s codes, as an expression (nulls stay null).

    `known` is None when the codes are the ids.
    """
    if known is None:
        return pl.col(name)
    if known.offset is not None:
        return pl.col(name) + known.offset if known.offset else pl.col(name)
    return pl.lit(pl.Series(known.ids, dtype=pl.UInt32)).gather(pl.col(name))


_OVERLAP = None
_OVERLAP_LOCK = threading.Lock()


def _overlap():
    """One helper thread for work that overlaps a collect's own conversion."""
    global _OVERLAP
    with _OVERLAP_LOCK:
        if _OVERLAP is None:
            _OVERLAP = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ibex-polars-convert")
        return _OVERLAP


def _left_rows(rows, source):
    """The rows of `source` a join's left row numbers name, in their order."""
    import ibex_pyarrow

    numbers = rows.to_numpy(zero_copy_only=False) if rows.null_count == 0 else None
    if numbers is not None and len(numbers) and numbers.dtype == np.int64:
        # One parallel native pass checks that the numbers strictly increase
        # within the source and sets their bits: each row at most once, in
        # order, so the rows are a filter of the source.
        bits = ibex_pyarrow.left_row_mask(numbers, source.height)
        if bits is not None:
            # Strictly increasing and as many as the source: the identity.
            # Keep the original buffers and categories.
            if len(numbers) == source.height:
                return source
            # Streaming filters morsels on every thread, not one thread per
            # column; the flag's name is longer than any column's, so it
            # cannot collide.
            flag = "_ibex_keep" + "_" * max((len(n) for n in source.columns), default=0)
            keep = pl.from_arrow(pa.Array.from_buffers(pa.bool_(), source.height, [None, pa.py_buffer(bits)]))
            return source.with_columns(keep.alias(flag)).lazy().filter(pl.col(flag)).drop(flag).collect(
                engine="streaming")
    return source[pl.from_arrow(rows)]


def _to_polars(result, expected, output, categories=(), left_rows=None):
    """Export the declared result schema without decoding categorical keys."""
    if left_rows is not None:
        # A join's left columns, taken from the original frame by row number.
        row, source = left_rows
        column = result[row]
        # combine_chunks copies even a single chunk.
        rows = column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()
        # The left rebuild reads only the original frame and the right
        # conversion only Ibex's columns; both release the GIL, so with a
        # parallel Polars pool they overlap instead of running in sequence.
        pending = _overlap().submit(_left_rows, rows, source) if pl.thread_pool_size() > 1 else None
        left = None if pending is not None else _left_rows(rows, source)
        # Polars' output is the left columns, then the right ones; the native
        # result's right columns are those, in order, but named against a
        # left side of only keys and a row number, so they are renamed here.
        right_names = list(expected)[source.width:]
        left_count = list(result.column_names).index(row) + 1
        native = result.select(list(range(left_count, result.num_columns)))
        if native.num_columns != len(right_names):
            raise RuntimeError("native join output does not match the plan's right columns")
        native = native.rename_columns(right_names)
        rest = {name: expected[name] for name in right_names}
        right = _to_polars(native, rest, {**output, **dict(zip(right_names, (output[n] for n in result.column_names[left_count:])))},
                           categories) if rest else None
        if pending is not None:
            left = pending.result()
        return pl.DataFrame([*left.get_columns(), *(right.get_columns() if right is not None else [])])
    if not expected:
        # Arrow carries a row count for zero-column tables, but from_arrow
        # loses that height. Drop a null-only carrier after conversion; its
        # Arrow array needs no per-row value buffers or Python objects.
        return pl.from_arrow(pa.table({"_ibex_height": pa.nulls(result.num_rows)})).drop("_ibex_height")
    result = result.select(list(expected))
    metadata = dict(result.schema.metadata or {})
    metadata.pop(b"ibex.ordering", None)
    result = result.replace_schema_metadata(metadata or None)
    decoded = {}
    for index, name in enumerate(result.column_names):
        dtype = result.column(index).type
        if expected[name] == pl.String and pa.types.is_dictionary(dtype):
            decoded[name] = _strings_from_dictionary(result.column(index), name)
            continue
        if is_temporal(expected[name]):
            # Through Int64: compact storage can export Int32 ticks, and Arrow
            # casts only Int64 to timestamp/duration.
            ticks = result.column(index)
            if ticks.type != pa.int64():
                ticks = ticks.cast(pa.int64())
            result = result.set_column(index, name, ticks.cast(storage_type(expected[name])))
            continue
        # The shared lowerer's logical string type describes dictionary-backed
        # strings too. Preserve dictionaries for code-based categorical export.
        if expected[name].base_type() == pl.Categorical and pa.types.is_dictionary(dtype):
            continue
        if expected[name].base_type() == pl.Categorical and pa.types.is_integer(dtype):
            # Polars category ids carried through Ibex as an integer payload.
            continue
        if (pa.types.is_integer(dtype) and pa.types.is_integer(output[name])
                and expected[name].is_integer()):
            # Integer widths narrow in the final strict Polars cast, which
            # converts every column in one query instead of one Arrow pass each.
            continue
        if dtype != output[name]:
            result = result.set_column(index, name, result.column(index).cast(output[name]))
    dictionaries = {name: dtype for name, dtype in expected.items()
                    if dtype.base_type() == pl.Categorical and pa.types.is_dictionary(result[name].type)}
    known, mappings = {}, {}
    for name, dtype in expected.items():
        if dtype.base_type() == pl.Categorical and pa.types.is_integer(result[name].type):
            # The ids themselves (`Adapter._resident_table(ids=...)`): no
            # lookup, only `cat.to`, which still checks every id. Non-negative,
            # so viewing them as UInt32 is exact and free.
            column = result[name]
            if column.type != pa.int32():
                raise RuntimeError(f"category ids of {name} came back as {column.type}")
            known[name] = pl.from_arrow(pa.chunked_array([chunk.view(pa.uint32()) for chunk in column.chunks],
                                                         type=pa.uint32())).alias(name)
            mappings[name] = None
            dictionaries[name] = dtype
    for name, dtype in list(dictionaries.items()):
        if name in known:
            continue
        codes = _known_codes(result[name], dtype, name, categories)
        if codes is not None:
            known[name], mappings[name] = codes
    categorical = {name: _categorical_from_arrow(result[name], dtype, name)
                   for name, dtype in dictionaries.items() if name not in known}
    if known:
        # One streaming query converts every column of ids. `cat.to` checks
        # each id serially per chunk; streaming splits the rows into morsels,
        # so the check runs on every thread instead of one per column.
        converted = pl.DataFrame(list(known.values())).lazy().with_columns(
            [_known_ids(name, mappings[name]).cat.to(dictionaries[name]).alias(name) for name in known]
        ).collect(engine="streaming")
        categorical.update({name: converted.get_column(name) for name in known})
    prebuilt = {**categorical, **decoded}
    if prebuilt:
        frame = pl.DataFrame([prebuilt[name] if name in prebuilt
                              else pl.from_arrow(result[name]).alias(name)
                              for name in expected])
    else:
        frame = pl.from_arrow(result)
    assert isinstance(frame, pl.DataFrame)
    # Named category mappings still require conversion to the requested dtype.
    return frame if frame.schema == expected else frame.lazy().cast(expected, strict=True).collect(engine="streaming")


class _Stopped(Exception):
    """The consumer of a streamed result went away; the query stops."""


class _StreamedResult:
    """An Ibex query run on a helper thread whose chunks arrive as batches.

    The helper thread runs the query (without the GIL) and queues each chunk
    as Arrow; `batches()` converts and yields them on the consumer's thread.
    The queue is bounded, so a slow consumer pauses the query rather than
    buffering the whole result. Closing `batches()` early stops the query.
    """

    _DEPTH = 4

    def __init__(self, source, bindings, scalars):
        self._queue = queue.Queue(maxsize=self._DEPTH)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(source, bindings, scalars),
                                        name="ibex-polars-stream", daemon=True)

    def _put(self, item):
        while not self._stop.is_set():
            try:
                self._queue.put(item, timeout=0.05)
                return
            except queue.Full:
                pass
        raise _Stopped

    def _run(self, source, bindings, scalars):
        import ibex_pyarrow
        try:
            ibex_pyarrow.execute_pipeline_stream(source, lambda batch: self._put((True, batch)),
                                                 tables=bindings, scalars=scalars)
            self._put((False, None))
        except _Stopped:
            pass
        except BaseException as error:  # handed to the consumer, which raises it
            try:
                self._put((False, error))
            except _Stopped:
                pass

    def batches(self, min_rows=1 << 20):
        """The result as Arrow tables of at least `min_rows` rows (but the last).

        Ibex's chunks are a scan unit each, often a few thousand rows after a
        filter, and converting one to Polars has a fixed cost of about a
        millisecond; joined (without copying) up to `min_rows`, they convert
        at the whole result's per-row cost.
        """
        self._thread.start()
        pending, rows = [], 0
        try:
            while True:
                more, item = self._queue.get()
                if not more:
                    if item is not None:
                        raise item
                    if pending:
                        yield pa.concat_tables(pending)
                    return
                pending.append(item)
                rows += item.num_rows
                if rows >= min_rows:
                    yield pa.concat_tables(pending)
                    pending, rows = [], 0
        finally:
            self._stop.set()
            self._thread.join()


class IbexEngine(pl.Engine):
    def __init__(self, *, cache_size=32):
        check_version()
        if type(cache_size) is not int or cache_size < 0:
            raise ValueError("cache_size must be a non-negative integer")
        self._cache_size = cache_size
        self._cache = OrderedDict()
        self._lock = threading.RLock()

    @property
    def name(self):
        return "ibex"

    @property
    def plan_engine(self):
        # Polars explain() renders its own plan. engine.explain(lf) below
        # displays the actual source passed to the native Ibex evaluator.
        return "in-memory"

    def _prepare(self, lf, optimizations):
        check_version()
        key = (id(lf), flag_key(optimizations))
        entry = self._cache.get(key)
        if entry is not None and entry[0]() is lf:
            self._cache.move_to_end(key)
            return entry[1]
        try:
            plan = Adapter(lf, optimizations).compile()
        except NotImplementedError as error:
            if str(error) != "join with a fused predicate" or not optimizations.predicate_pushdown:
                raise
            # Visitor 15.2 cannot expose this optimized node. Revisit the same
            # query as an ordinary join plus filter; Ibex can push it down.
            # Copy the Rust flags so caller settings and cache keys stay intact.
            flags = pl.QueryOptFlags._from_pyoptflags(optimizations._pyoptflags.copy())
            flags.update(predicate_pushdown=False)
            plan = Adapter(lf, flags).compile()
        if self._cache_size:
            owner = weakref.ref(self)
            def discard(reference):
                engine = owner()
                if engine is not None:
                    with engine._lock:
                        cached = engine._cache.get(key)
                        if cached is not None and cached[0] is reference:
                            engine._cache.pop(key, None)
            self._cache[key] = (weakref.ref(lf, discard), plan)
            self._cache.move_to_end(key)
            while len(self._cache) > self._cache_size:
                self._cache.popitem(last=False)
        return plan

    def prepare(self, lf, *, optimizations=None):
        """Translate/prepare inputs without executing the analytical query."""
        with self._lock:
            self._prepare(lf, optimizations if optimizations is not None else pl.QueryOptFlags())
        return self

    def explain(self, lf, *, optimizations=None):
        with self._lock:
            return self._prepare(lf, optimizations if optimizations is not None else pl.QueryOptFlags())[0]

    def collect(self, lf, *, optimizations, background=False, post_opt_callback=None):
        if background or post_opt_callback is not None:
            raise NotImplementedError("IbexEngine does not support background collection or post-optimization callbacks")
        import ibex_pyarrow
        with self._lock:
            source, bindings, scalars, expected, output, categories, left_rows = self._prepare(lf, optimizations)
            result = ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        # Conversion/casting constructs the public Polars result; the query's
        # filters, arithmetic and reductions have already executed in Ibex.
        return _to_polars(result, expected, output, categories, left_rows)

    def execute(self, lf, *, optimizations):
        return SingleNodeQueryResult(self.collect(lf, optimizations=optimizations))

    def sink_parquet(self, lf, path, *, optimizations, **options):
        """`lf.sink_parquet(path, engine=IbexEngine())`: run `lf` in Ibex, write with Polars.

        The query runs in Ibex, and Polars' own Parquet writer writes its
        result batch by batch as Ibex produces it, so the encoding overlaps the
        query. Each batch reaches Polars as `collect`'s result would, mostly
        sharing Arrow buffers. The file therefore has exactly the schema and metadata
        Polars would write, and every `sink_parquet` option and target
        (compression, statistics, row groups, `PartitionBy`, cloud storage,
        file objects) behaves as it does in Polars.
        """
        with self._lock:
            source, bindings, scalars, expected, output, categories, left_rows = self._prepare(lf, optimizations)
            if left_rows is not None or not expected:
                # A join rebuilt from the original frame's rows, or a result
                # without columns, converts whole.
                frame = self.collect(lf, optimizations=optimizations)
                frame.lazy().sink_parquet(path, engine="streaming", **options)
                return
            # Polars' streaming sink writes each batch as Ibex produces it, so
            # encoding overlaps the query instead of following it.
            stream = _StreamedResult(source, bindings, scalars)
            schema = pl.Schema(expected)

            def batches(with_columns, predicate, n_rows, batch_size):
                remaining = n_rows
                for batch in stream.batches():
                    frame = _to_polars(batch, expected, output, categories)
                    if with_columns is not None:
                        frame = frame.select(with_columns)
                    if predicate is not None:
                        frame = frame.filter(predicate)
                    if remaining is not None:
                        frame = frame.head(remaining)
                        remaining -= frame.height
                    yield frame
                    if remaining == 0:
                        return

            register_io_source(batches, schema=schema).sink_parquet(path, engine="streaming", **options)
