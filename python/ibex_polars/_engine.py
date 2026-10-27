# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine entry point and bounded, weakly owned prepared-plan cache."""

from collections import OrderedDict
import threading
import weakref

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

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


def _known_categorical(column, dtype, name, categories):
    """The column's Polars ids when every chunk carries a dictionary Ibex was given."""
    parts = []
    for chunk in column.chunks:
        ids = _categories.physical(chunk, categories, dtype)
        if ids is None:
            return None
        parts.append(ids)
    return pl.concat(parts, rechunk=False).alias(name) if parts else None


def _to_polars(result, expected, output, categories=()):
    """Export the declared result schema without decoding categorical keys."""
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
        if dtype != output[name]:
            result = result.set_column(index, name, result.column(index).cast(output[name]))
    dictionaries = {name: dtype for name, dtype in expected.items()
                    if dtype.base_type() == pl.Categorical and pa.types.is_dictionary(result[name].type)}
    known = {}
    for name, dtype in dictionaries.items():
        ids = _known_categorical(result[name], dtype, name, categories)
        if ids is not None:
            known[name] = ids
    categorical = {name: _categorical_from_arrow(result[name], dtype, name)
                   for name, dtype in dictionaries.items() if name not in known}
    if known:
        # One query converts every column of ids, in parallel.
        converted = pl.DataFrame(list(known.values())).with_columns(
            [pl.col(name).cat.to(dictionaries[name]) for name in known])
        categorical.update({name: converted.get_column(name) for name in known})
    if categorical:
        frame = pl.DataFrame([categorical[name] if name in categorical
                              else pl.from_arrow(result[name]).alias(name)
                              for name in expected])
    else:
        frame = pl.from_arrow(result)
    assert isinstance(frame, pl.DataFrame)
    # Named category mappings still require conversion to the requested dtype.
    return frame if frame.schema == expected else frame.cast(expected, strict=True)


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
            source, bindings, scalars, expected, output, categories = self._prepare(lf, optimizations)
            result = ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        # Conversion/casting constructs the public Polars result; the query's
        # filters, arithmetic and reductions have already executed in Ibex.
        return _to_polars(result, expected, output, categories)

    def execute(self, lf, *, optimizations):
        return SingleNodeQueryResult(self.collect(lf, optimizations=optimizations))
