# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Pinned adapter for Polars 2.0.0's private logical-plan visitor.

Only visitor-provided node/expression IDs are accessed. Unknown nodes/options
fail during plan translation, before input preparation or native execution.
"""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa

import ibex
from ibex._expr import Expr
from ibex._lower import _WIDENED_NUMERIC_TYPES, lower
from ._temporal import NANOS_PER_UNIT, TEMPORAL_TYPES, is_temporal, literal_ticks, prepare_column


VISITOR_VERSION = (15, 2)
POLARS_VERSION = "2.0.0"

# These logical integer types all fit native Int64 storage. UInt32/UInt64
# intentionally remain outside this subset.
_INTEGER_LAYOUTS = {
    pl.Int8: (8, True), pl.Int16: (16, True), pl.Int32: (32, True),
    pl.Int64: (64, True), pl.UInt8: (8, False), pl.UInt16: (16, False),
}
_SUPPORTED_INPUT_TYPES = (*_INTEGER_LAYOUTS, *TEMPORAL_TYPES, pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Date, pl.Categorical)


def _integer_bounds(dtype):
    bits, signed = _INTEGER_LAYOUTS[dtype]
    return (-(2**(bits - 1)) if signed else 0, 2**(bits - int(signed)) - 1)


def _wrap_integer(expr, dtype):
    bits, signed = _INTEGER_LAYOUTS[dtype]
    return expr if bits == 64 else Expr("wrap_integer", (expr, bits, signed))


def check_version():
    if pl.__version__ != POLARS_VERSION:
        raise NotImplementedError(f"IbexEngine supports Polars {POLARS_VERSION}; found {pl.__version__}")


def flag_key(flags):
    # QueryOptFlags has no value repr/hash. Its Rust options expose the actual
    # Boolean settings, including private flags absent from public properties.
    values = tuple((name, getattr(flags._pyoptflags, name)) for name in dir(flags._pyoptflags)
                   if not name.startswith("_") and type(getattr(flags._pyoptflags, name)) is bool)
    for name in ("eager", "gpu", "streaming", "query_monitoring"):
        if dict(values).get(name):
            raise NotImplementedError(f"IbexEngine does not support the {name} optimization mode")
    return values


class Adapter:
    def __init__(self, lf, optimizations):
        check_version()
        self.visitor = lf._ldf.with_optimizations(optimizations._pyoptflags).visit()
        if self.visitor.version() != VISITOR_VERSION:
            raise NotImplementedError(f"unsupported Polars visitor version {self.visitor.version()}")
        self.nodes = {}
        self.expressions = {}
        self.frame_length_columns = {}

    def expression(self, node):
        # Dtypes and proofs of frame-sized lengths belong to the current input
        # node: a filter can change the height without changing the schema.
        key = (node, self.visitor.get_node(), tuple(self.visitor.get_schema().items()))
        if key in self.expressions:
            return self.expressions[key]
        value = self.visitor.view_expression(node)
        kind = type(value).__name__
        if kind == "Column":
            expr = ibex.col(value.name)
        elif kind == "Literal":
            if is_temporal(value.dtype):
                expr = ibex.lit(literal_ticks(value.value, value.dtype))
                self.expressions[key] = expr
                return expr
            if value.dtype not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Date):
                raise NotImplementedError(f"unsupported literal type {value.dtype}")
            expr = ibex.lit(value.value)
        elif kind == "BinaryExpr":
            operators = {"Plus": "+", "Minus": "-", "Multiply": "*", "TrueDivide": "/", "Divide": "/",
                         "Eq": "==", "NotEq": "!=", "Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">=",
                         "And": "&&", "LogicalAnd": "&&", "Or": "||", "LogicalOr": "||"}
            name = str(value.op).split(".")[-1]
            if name not in operators:
                raise NotImplementedError(f"unsupported binary operator {value.op}")
            operator = operators[name]
            left_type, right_type = self.visitor.get_dtype(value.left), self.visitor.get_dtype(value.right)
            if is_temporal(left_type) or is_temporal(right_type):
                if operator not in ("==", "!=", "<", "<=", ">", ">="):
                    raise NotImplementedError("temporal arithmetic is not supported yet")
                if left_type.base_type() != right_type.base_type():
                    raise NotImplementedError("comparison between these temporal types is unsupported")
                if left_type.base_type() == pl.Datetime and (left_type.time_zone is None) != (right_type.time_zone is None):
                    raise NotImplementedError("comparison between naive and zoned timestamps is unsupported")
            if operator in ("==", "!=", "<", "<=", ">", ">="):
                operator = "polars_" + operator
            expr = Expr(operator, (self.expression(value.left), self.expression(value.right)))
        elif kind == "Agg":
            if len(value.arguments) != 1:
                raise NotImplementedError(f"unsupported aggregate {value.name}")
            input_type = self.visitor.get_dtype(value.arguments[0])
            allowed = ("min", "max", "sum") if input_type.base_type() == pl.Duration else ("min", "max")
            if value.name not in ("count", "first", "last", "first_non_null", "last_non_null") and value.name not in (allowed if is_temporal(input_type) else ("sum", "mean")):
                raise NotImplementedError(f"unsupported aggregate {value.name} for {input_type}")
            # The visitor calls both Expr.count() and Expr.len() "count";
            # its Boolean option is include_nulls, not a count execution hint.
            operation = value.name
            if value.name == "count" and value.options is True:
                operation = "column_len"
            elif value.options not in (None, False):
                raise NotImplementedError(f"unsupported {value.name} aggregate options")
            expr = Expr(operation, (self.expression(value.arguments[0]),))
        elif kind == "Len":
            expr = ibex.len()
        elif kind == "Cast":
            original = self.visitor.get_dtype(value.expr)
            if is_temporal(original) or is_temporal(value.dtype):
                if original.base_type() != value.dtype.base_type():
                    raise NotImplementedError("casts between temporal and non-temporal types are unsupported")
                same_zone = original.base_type() != pl.Datetime or original.time_zone == value.dtype.time_zone
                if not same_zone or NANOS_PER_UNIT[value.dtype.time_unit] > NANOS_PER_UNIT[original.time_unit]:
                    raise NotImplementedError("lossy temporal casts and timezone changes are unsupported")
                expr = self.expression(value.expr)
            elif value.dtype in _INTEGER_LAYOUTS and original in _INTEGER_LAYOUTS:
                source_min, source_max = _integer_bounds(original)
                target_min, target_max = _integer_bounds(value.dtype)
                if target_min <= source_min and source_max <= target_max:
                    expr = self.expression(value.expr)
                elif value.options == 2:
                    expr = _wrap_integer(self.expression(value.expr), value.dtype)
                else:
                    raise NotImplementedError(f"unsupported checked/null-producing cast from {original} to {value.dtype}")
            elif value.dtype == pl.Float32 and original == pl.Float32:
                expr = self.expression(value.expr)
            elif value.dtype == pl.Float64 and original in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64):
                expr = Expr("cast", (self.expression(value.expr), pa.float64()))
            else:
                raise NotImplementedError(f"unsupported cast to {value.dtype} with options {value.options}")
        elif kind == "Function":
            name = str(value.function_data[0]).split(".")[-1]
            if name == "repeat":
                if len(value.input) != 2 or len(value.function_data) != 1:
                    raise NotImplementedError("unsupported repeat options")
                length = self.expression(value.input[1])
                frame_length = length._op == "len" or (
                    length._op == "column_len" and length._args[0]._row_reference
                    and not length._args[0]._aggregate
                )
                if length._op == "col":
                    frame_length = length._args[0] in self.frame_length_columns.get(self.visitor.get_node(), ())
                if not frame_length:
                    raise NotImplementedError("repeat length must be the current frame row count")
                scalar = self.visitor.view_expression(value.input[0])
                if type(scalar).__name__ != "Literal":
                    raise NotImplementedError("repeat currently requires a scalar literal")
                if scalar.value is None:
                    dtype = scalar.dtype
                    if dtype in _INTEGER_LAYOUTS or is_temporal(dtype):
                        placeholder = 0
                    elif dtype in (pl.Float32, pl.Float64):
                        placeholder = 0.0
                    elif dtype == pl.Boolean:
                        placeholder = False
                    elif dtype in (pl.String, pl.Categorical):
                        placeholder = ""
                    elif dtype == pl.Date:
                        placeholder = date(1970, 1, 1)
                    else:
                        raise NotImplementedError(f"unsupported repeat null type {dtype}")
                    expr = Expr("repeat_null", (ibex.lit(placeholder),))
                else:
                    expr = Expr("repeat", (self.expression(value.input[0]),))
                # A repeat consumes its length reduction and produces one value
                # per input row; it is not itself an aggregate.
                self.expressions[key] = expr
                return expr
            args = tuple(self.expression(child) for child in value.input)
            if name == "IsBetween" and len(args) == 3 and len(value.function_data) == 2:
                types = [self.visitor.get_dtype(child) for child in value.input]
                if any(is_temporal(dtype) for dtype in types) and len({dtype.base_type() for dtype in types}) != 1:
                    raise NotImplementedError("temporal bounds must have matching kinds")
                if types[0].base_type() == pl.Datetime and len({dtype.time_zone is None for dtype in types}) != 1:
                    raise NotImplementedError("bounds cannot mix naive and zoned timestamps")
                closed = value.function_data[1]
                if closed not in ("left", "right", "both", "none"):
                    raise NotImplementedError(f"unsupported closed interval {closed}")
                expr = Expr("polars_>=" if closed in ("left", "both") else "polars_>", (args[0], args[1])) & Expr(
                    "polars_<=" if closed in ("right", "both") else "polars_<", (args[0], args[2]))
            elif name == "Not" and len(args) == 1:
                expr = ~args[0]
            elif name == "IsNull" and len(args) == 1:
                expr = Expr("is_null", args)
            elif name == "IsNotNull" and len(args) == 1:
                expr = ~Expr("is_null", args)
            elif name == "negate" and len(args) == 1 and self.visitor.get_dtype(node) in (pl.Int8, pl.Int16, pl.Int32):
                expr = ibex.lit(0) - args[0]
            else:
                raise NotImplementedError(f"unsupported function {value.function_data}")
        else:
            raise NotImplementedError(f"unsupported Polars expression {kind} (node {node})")
        # Keep narrow integers as logical types only. Each expression boundary
        # must wrap before a later comparison, division, cast or reduction.
        if kind in ("BinaryExpr", "Agg", "Function") and expr._op not in (
            "polars_==", "polars_!=", "polars_<", "polars_<=", "polars_>", "polars_>=", "&&", "||", "!", "is_null"
        ) and self.visitor.get_dtype(node) in _INTEGER_LAYOUTS:
            expr = _wrap_integer(expr, self.visitor.get_dtype(node))
        self.expressions[key] = expr
        return expr

    def named(self, expr):
        return self.expression(expr.node).alias(expr.output_name)

    def frame(self, node_id):
        if node_id in self.nodes:
            return self.nodes[node_id]
        self.visitor.set_node(node_id)
        node = self.visitor.view_current_node()
        kind = type(node).__name__
        schema = self.visitor.get_schema()
        if kind == "MapFunction" and len(node.function) == 2 and node.function[0] == "hint_sorted":
            # A sortedness assertion leaves rows unchanged. Ignore the claim
            # until native metadata propagation is implemented; existing native
            # ordering proofs and general algorithms remain available.
            frame = self.frame(node.input)
            self.frame_length_columns[node_id] = self.frame_length_columns.get(node.input, set())
        elif kind == "DataFrameScan":
            if node.selection is not None:
                raise NotImplementedError("DataFrameScan selections are not supported yet")
            for name, dtype in schema.items():
                if dtype.base_type() not in _SUPPORTED_INPUT_TYPES:
                    raise NotImplementedError(f"unsupported resident column {name}: {dtype}")
            # PyDataFrame.select projects column handles, without collecting a
            # lazy query or evaluating user expressions in the Polars engine.
            if schema:
                data = pl.DataFrame._from_pydf(node.df.select(list(schema)))
                table = data.to_arrow()
            else:
                # Projection pushdown can remove every column from a len-only
                # scan. Arrow/native tables need a column to retain its height.
                height = pl.DataFrame._from_pydf(node.df).height
                table = pa.table({"_ibex_height": pa.nulls(height, type=pa.int64())})
            for index, field in enumerate(table.schema):
                if pa.types.is_timestamp(field.type) or pa.types.is_duration(field.type):
                    table = table.set_column(index, field.name, prepare_column(table.column(index)))
                elif field.type in _WIDENED_NUMERIC_TYPES:
                    table = table.set_column(index, field.name, table.column(index).cast(_WIDENED_NUMERIC_TYPES[field.type]))
            frame = ibex.LazyFrame(("table", table))
        elif kind == "Scan":
            if node.scan_type[0] != "parquet" or len(node.paths) != 1:
                raise NotImplementedError("only single local Parquet scans are supported")
            if not isinstance(node.paths[0], str) or Path(node.paths[0]).is_dir():
                raise NotImplementedError("only single local Parquet file paths are supported")
            for name, dtype in schema.items():
                if is_temporal(dtype):
                    raise NotImplementedError("temporal Parquet scans are not supported yet")
                if dtype.base_type() not in _SUPPORTED_INPUT_TYPES:
                    raise NotImplementedError(f"unsupported Parquet column {name}: {dtype}")
            options = node.file_options
            for name in ("row_count", "row_index", "include_file_paths", "n_rows", "hive_options",
                         "column_mapping", "default_values", "deletion_files", "table_statistics"):
                if getattr(options, name) is not None:
                    raise NotImplementedError(f"unsupported scan option {name}")
            if options.rechunk or options.missing_columns_policy != "raise" or options.extra_columns_policy != "raise":
                raise NotImplementedError("unsupported Parquet schema/rechunk policy")
            policy = options.cast_columns_policy
            default_policy = {"integer_upcast": False, "integer_to_float_cast": False,
                              "float_upcast": False, "float_downcast": False,
                              "datetime_nanoseconds_downcast": False, "datetime_microseconds_downcast": False,
                              "datetime_milliseconds_upcast": False, "datetime_microseconds_upcast": False,
                              "datetime_convert_timezone": False, "null_upcast": True,
                              "categorical_to_string": False, "missing_struct_fields": "raise",
                              "extra_struct_fields": "raise"}
            if policy != default_policy:
                raise NotImplementedError("unsupported Parquet cast policy")
            scan_options = json.loads(node.scan_type[1])
            if scan_options != {"schema": None, "parallel": "Auto", "low_memory": False, "use_statistics": True}:
                raise NotImplementedError("unsupported Parquet scan configuration")
            cloud = json.loads(node.scan_type[2])
            if cloud.get("config") is not None or cloud.get("credential_provider") is not None:
                raise NotImplementedError("cloud scan configuration is not supported")
            frame = ibex.scan_parquet(node.paths[0])
            if node.predicate is not None:
                frame = frame.filter(self.expression(node.predicate.node))
            frame = frame.select(list(schema))
        elif kind in ("Filter", "SimpleProjection", "Select", "HStack", "GroupBy", "Sort"):
            frame = self.frame(node.input)
            # get_dtype resolves columns against the current visitor node.
            # Resolve expressions against their INPUT, before output aliases
            # or casts can change a column's logical dtype.
            self.visitor.set_node(node.input)
            if kind == "Filter":
                frame = frame.filter(self.expression(node.predicate.node))
            elif kind == "SimpleProjection":
                frame = frame.select(list(schema))
            elif kind == "Select":
                frame = frame.select([self.named(expr) for expr in node.expr])
            elif kind == "HStack":
                expressions = tuple(self.named(expr) for expr in node.exprs)
                # Polars CSE lifts repeat's length into a WITH_COLUMNS field.
                # Only row counts are admitted here, not arbitrary reductions.
                lengths = {expr._name for expr in expressions if expr._args[0]._op in ("len", "column_len")}
                expressions = tuple(
                    Expr("broadcast_length", (expr._args[0],)).alias(expr._name)
                    if expr._name in lengths else expr for expr in expressions
                )
                self.frame_length_columns[node_id] = lengths
                if any(expr._aggregate for expr in expressions):
                    raise NotImplementedError("reduction broadcasting in HStack is not supported yet")
                frame = frame._append("hstack", expressions).select(list(schema))
            elif kind == "GroupBy":
                if node.apply or any(getattr(node.options, name) is not None for name in ("dynamic", "rolling", "slice")):
                    raise NotImplementedError("group UDFs, dynamic/rolling groups and aggregate slices are unsupported")
                keys = []
                for key in node.keys:
                    expr = self.expression(key.node)
                    if expr._op != "col" or expr._name != key.output_name:
                        raise NotImplementedError("computed or renamed grouping keys are not supported yet")
                    if self.visitor.get_dtype(key.node) in (pl.Float32, pl.Float64):
                        raise NotImplementedError("floating-point grouping keys are not supported yet")
                    keys.append(expr._name)
                frame = frame.group_by(*keys, maintain_order=node.maintain_order).agg([self.named(expr) for expr in node.aggs])
            elif kind == "Sort":
                if node.slice is not None:
                    raise NotImplementedError("sort slices/top-k are not supported yet")
                keys = []
                for key in node.by_column:
                    expr = self.expression(key.node)
                    if expr._op != "col":
                        raise NotImplementedError("computed sort keys are not supported yet")
                    keys.append(expr._name)
                stable, nulls_last, descending = node.sort_options
                frame = frame.sort(keys, descending=descending, nulls_last=nulls_last, maintain_order=stable)
        else:
            raise NotImplementedError(f"unsupported Polars plan node {kind} (node {node_id})")
        self.nodes[node_id] = frame
        return frame

    def compile(self):
        root = self.visitor.get_node()
        expected = self.visitor.get_schema()
        frame = self.frame(root)
        # Validate the complete supported plan before native input binding.
        lower(frame, validate=True, widen_numeric_inputs=True)
        if frame._source[0] == "table":
            prepared = ibex.DataFrame(frame._source[1]).lazy()
            frame = ibex.LazyFrame(prepared._source, frame._steps)
        result_schema = {}
        source, bindings, scalars = lower(frame, validate=True, result_schema=result_schema, widen_numeric_inputs=True)
        return source, bindings, scalars, expected, result_schema
