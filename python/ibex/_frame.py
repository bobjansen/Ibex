# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Lazy query construction and small Arrow-backed result containers."""

from dataclasses import dataclass
import os

import pyarrow as pa

from ._expr import _expressions, _identifier, _into_expr


@dataclass(frozen=True)
class _BoundSource:
    table: pa.Table
    capsule: object


@dataclass(frozen=True)
class LazyFrame:
    _source: tuple
    _steps: tuple = ()

    def _append(self, operation, expressions):
        return LazyFrame(self._source, self._steps + ((operation, expressions),))

    def filter(self, *predicates, **constraints):
        expressions = tuple(_into_expr(value) for value in predicates) + tuple(
            _into_expr(name) == value for name, value in constraints.items()
        )
        if not expressions:
            raise TypeError("filter requires at least one predicate")
        if any(expr._aggregate for expr in expressions):
            raise NotImplementedError("reductions inside filters are not supported yet")
        return self._append("filter", expressions)

    def select(self, *exprs, **named_exprs):
        expressions = _expressions(exprs, named_exprs)
        aggregate = any(expr._aggregate for expr in expressions)
        if aggregate and not all(expr._aggregate for expr in expressions):
            raise NotImplementedError("mixing reductions and row expressions is not supported yet")
        if any(expr._aggregate and expr._row_reference for expr in expressions):
            raise NotImplementedError("mixing reductions and row expressions is not supported yet")
        if any(not expr._aggregate and not expr._row_reference for expr in expressions):
            raise NotImplementedError("literal-only projections are not supported yet")
        return self._append("select", expressions)

    def with_columns(self, *exprs, **named_exprs):
        expressions = _expressions(exprs, named_exprs)
        # Ibex updates are sequential; do not accidentally claim simultaneous
        # Polars semantics for multiple assignments until staging is added.
        if len(expressions) != 1:
            raise NotImplementedError("with_columns currently supports one expression per call")
        if expressions[0]._aggregate:
            raise NotImplementedError("reduction broadcasting in with_columns is not supported yet")
        return self._append("update", expressions)

    def group_by(self, *by, maintain_order=False):
        if len(by) == 1 and isinstance(by[0], (list, tuple)):
            by = tuple(by[0])
        if not by:
            raise ValueError("group_by requires at least one column")
        if type(maintain_order) is not bool:
            raise TypeError("maintain_order must be a Boolean")
        for name in by:
            _identifier(name)
        if len(set(by)) != len(by):
            raise ValueError("duplicate grouping keys")
        return LazyGroupBy(self, tuple(by), maintain_order)

    def join(self, other, on=None, *, how="inner", left_on=None, right_on=None,
             suffix="_right", nulls_equal=False, coalesce=None, maintain_order="none", validate="m:m", _left_columns=None, _right_columns=None):
        if not isinstance(other, LazyFrame):
            raise TypeError("join requires another lazy frame")
        if how not in ("inner", "left", "right", "full", "semi", "anti", "cross"):
            raise NotImplementedError(f"unsupported join kind {how}")
        if validate != "m:m":
            raise NotImplementedError("join cardinality validation is not supported yet")
        if maintain_order is None:
            maintain_order = "none"
        if maintain_order not in ("none", "left", "right", "left_right", "right_left"):
            raise ValueError("unsupported join ordering policy")
        if how in ("semi", "anti") and maintain_order not in ("none", "left", "left_right"):
            raise NotImplementedError("right ordering for semi/anti joins is unsupported")
        if type(nulls_equal) is not bool or (coalesce is not None and type(coalesce) is not bool):
            raise TypeError("join nulls_equal/coalesce must be Boolean")
        if not isinstance(suffix, str):
            raise TypeError("join suffix must be a string")
        def keys(value):
            value = (value,) if isinstance(value, str) else tuple(value or ())
            for name in value:
                _identifier(name)
            if len(set(value)) != len(value):
                raise NotImplementedError("duplicate join keys are unsupported")
            return value
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("use on or left_on/right_on")
            left_on = right_on = on
        left_on, right_on = keys(left_on), keys(right_on)
        if how == "cross":
            if left_on or right_on:
                raise ValueError("cross joins do not accept keys")
        elif not left_on or len(left_on) != len(right_on):
            raise ValueError("join needs equally sized left/right key lists")
        coalesce = (how in ("inner", "left", "right")) if coalesce is None else coalesce
        if how in ("semi", "anti", "cross"):
            coalesce = False
        return self._append("join", (other, left_on, right_on, how, suffix, nulls_equal, coalesce, maintain_order, _left_columns, _right_columns))

    def slice(self, offset, length=None):
        if type(offset) is not int or (length is not None and type(length) is not int):
            raise TypeError("slice offset and length must be integers")
        if length is not None and length < 0:
            raise ValueError("slice length must be non-negative")
        return self._append("slice", (offset, length))

    def head(self, n=5):
        if type(n) is not int:
            raise TypeError("head length must be an integer")
        return self.slice(0, n)

    def tail(self, n=5):
        if type(n) is not int or n < 0:
            raise ValueError("tail length must be a non-negative integer")
        return self.slice(-n, n)

    def gather_every(self, n, offset=0):
        if type(n) is not int or n <= 0 or type(offset) is not int or offset < 0:
            raise ValueError("gather_every requires a positive step and non-negative offset")
        return self[slice(offset, None, n)]

    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise NotImplementedError("lazy indexing currently supports slices only")
        if any(value is not None and type(value) is not int for value in (key.start, key.stop, key.step)):
            raise TypeError("slice bounds and step must be integers")
        if key.step == 0:
            raise ValueError("slice step cannot be zero")
        return self._append("python_slice", (key.start, key.stop, key.step or 1))

    def unique(self, subset=None, *, keep="any", maintain_order=False):
        if keep not in ("any", "first", "last", "none"):
            raise ValueError("keep must be 'any', 'first', 'last', or 'none'")
        if type(maintain_order) is not bool:
            raise TypeError("maintain_order must be a Boolean")
        if subset is not None:
            if isinstance(subset, str):
                subset = (subset,)
            elif isinstance(subset, (list, tuple)):
                subset = tuple(subset)
            else:
                raise NotImplementedError("unique currently requires column-name subset keys")
            for name in subset:
                _identifier(name)
            if not subset:
                raise NotImplementedError("unique with an empty subset is unsupported")
            subset = tuple(dict.fromkeys(subset))
        return self._append("unique", (subset, keep, maintain_order))

    def sort(self, by, *more_by, descending=False, nulls_last=False, maintain_order=False):
        names = tuple(by) if isinstance(by, (list, tuple)) else (by,)
        names += more_by
        for name in names:
            _identifier(name)
        if not names:
            raise ValueError("sort requires at least one column")
        if type(maintain_order) is not bool:
            raise TypeError("maintain_order must be a Boolean")
        def flags(value, name):
            values = (value,) * len(names) if type(value) is bool else tuple(value)
            if len(values) != len(names) or any(type(flag) is not bool for flag in values):
                raise ValueError(f"{name} must have one Boolean per sort key")
            return values
        return self._append("sort", (names, flags(descending, "descending"), flags(nulls_last, "nulls_last")))

    def explain(self):
        """Render the native source with typed placeholders, without executing."""
        from ._lower import lower
        return lower(self)[0]

    def collect(self):
        from ._lower import lower
        import ibex_pyarrow
        result_schema = {}
        source, tables, scalars = lower(self, validate=True, result_schema=result_schema)
        result = ibex_pyarrow.execute_pipeline(source, tables=tables, scalars=scalars)
        # Exclude internal sort flags with a metadata-only Arrow projection.
        internal_columns = set(result.column_names) - set(result_schema)
        result = result.select(list(result_schema))
        if internal_columns and result.schema.metadata:
            metadata = dict(result.schema.metadata)
            # Native null-first emulation orders by flags absent from the
            # public result. Do not export a stale ordering claim on reimport.
            metadata.pop(b"ibex.ordering", None)
            result = result.replace_schema_metadata(metadata or None)
        # Native count is Int64 and categorical keys may export dictionaries.
        # Adapt only output columns whose representation differs; numeric
        # aggregate buffers retain their original zero-copy ownership.
        for index, name in enumerate(result.column_names):
            expected = result_schema[name]
            if result.column(index).type != expected:
                result = result.set_column(index, name, result.column(index).cast(expected))
        return DataFrame(result)


@dataclass(frozen=True)
class LazyGroupBy:
    _frame: LazyFrame
    _keys: tuple
    _maintain_order: bool

    def agg(self, *aggs, **named_aggs):
        expressions = _expressions(aggs, named_aggs)
        if any(not expr._aggregate or expr._row_reference for expr in expressions):
            raise NotImplementedError("grouped agg currently requires scalar reductions")
        if any(expr._name in self._keys for expr in expressions):
            raise ValueError("aggregate output name conflicts with a grouping key")
        return self._frame._append("aggregate", (self._keys, expressions))


class Series:
    def __init__(self, name, array):
        self.name = name
        self._array = array

    @property
    def dtype(self):
        return self._array.type

    def to_list(self):
        return self._array.to_pylist()

    def item(self, index=None):
        if index is None:
            if len(self._array) != 1:
                raise ValueError("item() requires a Series of length one")
            index = 0
        return self._array[index].as_py()


class DataFrame:
    def __init__(self, data):
        if isinstance(data, pa.Table):
            self._table = data
        elif isinstance(data, dict):
            self._table = pa.table(data)
        else:
            raise TypeError("DataFrame currently accepts an Arrow table or column dictionary")
        self._bound_source = None

    @property
    def columns(self):
        return self._table.column_names

    @property
    def shape(self):
        return self._table.shape

    @property
    def schema(self):
        # Prototype dtype representation: Arrow types, not Polars descriptors.
        return dict(zip(self.columns, self._table.schema.types))

    def to_arrow(self):
        return self._table

    def to_pandas(self):
        return self._table.to_pandas()

    def to_dict(self, *, as_series=True):
        return {name: self[name] if as_series else self[name].to_list() for name in self.columns}

    def __getitem__(self, name):
        if not isinstance(name, str):
            raise NotImplementedError("DataFrame indexing currently accepts a column name")
        return Series(name, self._table[name])

    def item(self, row=None, column=None):
        if row is None and column is None:
            if self.shape != (1, 1):
                raise ValueError("item() requires a DataFrame of shape (1, 1)")
            row, column = 0, 0
        elif row is None or column is None:
            raise ValueError("provide both row and column")
        return self._table.column(column)[row].as_py()

    def lazy(self):
        for name in self.columns:
            _identifier(name)
        supported = (pa.int64(), pa.float64(), pa.bool_(), pa.string(), pa.large_string(), pa.date32())
        if any(dtype not in supported and not (
            pa.types.is_dictionary(dtype) and dtype.value_type in (pa.string(), pa.large_string())
        ) for dtype in self._table.schema.types):
            raise NotImplementedError("lazy in-memory inputs currently support Int64, Float64, Bool, String and Date")
        if self._bound_source is None:
            import ibex_pyarrow
            table = self._table
            # The current native Arrow importer accepts utf8 (32-bit offsets).
            # Normalize large_utf8 once at preparation, retaining the logical
            # frontend schema and avoiding casts on every collection.
            for index, field in enumerate(table.schema):
                if field.type == pa.large_string():
                    table = table.set_column(index, field.name, table.column(index).cast(pa.string()))
                elif pa.types.is_dictionary(field.type):
                    arrays = []
                    for chunk in table.column(index).chunks:
                        if len(chunk.dictionary) >= 2**31:
                            raise NotImplementedError("categorical dictionary exceeds native Int32 code range")
                        indices = chunk.indices
                        if indices.type == pa.uint32():
                            indices = indices.view(pa.int32())
                        elif indices.type != pa.int32():
                            indices = indices.cast(pa.int32())
                        arrays.append(pa.DictionaryArray.from_arrays(
                            indices, chunk.dictionary.cast(pa.string())))
                    table = table.set_column(index, field.name, pa.chunked_array(
                        arrays, type=pa.dictionary(pa.int32(), pa.string())))
            native_input = table if table.num_rows else pa.RecordBatch.from_arrays(
                [column.combine_chunks() for column in table.columns], schema=table.schema
            )
            self._bound_source = _BoundSource(self._table, ibex_pyarrow.bind_table(native_input))
        return LazyFrame(("bound", self._bound_source))

    def select(self, *exprs, **named_exprs):
        return self.lazy().select(*exprs, **named_exprs).collect()


def scan_parquet(source) -> LazyFrame:
    path = os.fsdecode(os.fspath(source))
    if "\0" in path:
        raise ValueError("Parquet path must not contain NUL")
    if "://" in path or any(char in path for char in "*?["):
        raise NotImplementedError("scan_parquet currently supports a single local file")
    return LazyFrame(("parquet", path))
