# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine entry point and bounded, weakly owned prepared-plan cache."""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
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

from polars.lazyframe.query_result import SingleNodeQueryResult


def _categorical_from_arrow(column, dtype, name):
    """Translate dictionary entries once, then gather Polars category codes.

    Arrow dictionaries are local to each chunk; their ordinals need not match
    the requested Polars category mapping. Keep the mapping alive in `labels`
    until the physical codes have acquired their categorical dtype.
    """
    parts = []
    for chunk in column.chunks:
        labels = pl.from_arrow(chunk.dictionary).cast(dtype, strict=True)
        codes = pc.take(labels.to_physical().to_arrow(), chunk.indices)
        parts.append(pl.from_arrow(codes).cat.to(dtype).alias(name))
    return pl.concat(parts, rechunk=False) if parts else pl.Series(name, [], dtype=dtype)


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
    """The Polars ids of column `name`'s codes, as an expression (nulls stay null)."""
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
    for index, name in enumerate(result.column_names):
        dtype = result.column(index).type
        if is_temporal(expected[name]):
            result = result.set_column(index, name, result.column(index).cast(storage_type(expected[name])))
            continue
        # The shared lowerer's logical string type describes dictionary-backed
        # strings too. Preserve dictionaries for code-based categorical export.
        if expected[name].base_type() == pl.Categorical and pa.types.is_dictionary(dtype):
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
    for name, dtype in dictionaries.items():
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
    if categorical:
        frame = pl.DataFrame([categorical[name] if name in categorical
                              else pl.from_arrow(result[name]).alias(name)
                              for name in expected])
    else:
        frame = pl.from_arrow(result)
    assert isinstance(frame, pl.DataFrame)
    # Named category mappings still require conversion to the requested dtype.
    return frame if frame.schema == expected else frame.lazy().cast(expected, strict=True).collect(engine="streaming")


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
