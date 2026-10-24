# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Engine entry point and bounded, weakly owned prepared-plan cache."""

from collections import OrderedDict
import threading
import weakref

import polars as pl
import pyarrow as pa

from ._adapter import Adapter, check_version, flag_key
from ._temporal import is_temporal, storage_type

check_version()

from polars.lazyframe.query_result import SingleNodeQueryResult


def _to_polars(result, expected, output):
    """Export the declared result schema without decoding categorical keys."""
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
        # strings too. For categorical output, let Polars import the dictionary
        # directly instead of decoding every row and encoding it again.
        if expected[name].base_type() == pl.Categorical and pa.types.is_dictionary(dtype):
            continue
        if dtype != output[name]:
            result = result.set_column(index, name, result.column(index).cast(output[name]))
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
        plan = Adapter(lf, optimizations).compile()
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
            source, bindings, scalars, expected, output = self._prepare(lf, optimizations)
            result = ibex_pyarrow.execute_pipeline(source, tables=bindings, scalars=scalars)
        # Conversion/casting constructs the public Polars result; the query's
        # filters, arithmetic and reductions have already executed in Ibex.
        return _to_polars(result, expected, output)

    def execute(self, lf, *, optimizations):
        return SingleNodeQueryResult(self.collect(lf, optimizations=optimizations))
