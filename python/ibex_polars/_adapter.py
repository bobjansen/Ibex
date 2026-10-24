# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Pinned adapter for Polars 2.0.0's private logical-plan visitor.

Only visitor-provided node/expression IDs are accessed. Unknown nodes/options
fail during plan translation, before input preparation or native execution.
"""

import json
from pathlib import Path

import polars as pl
import pyarrow as pa

import ibex
from ibex._expr import Expr
from ibex._lower import _WIDENED_NUMERIC_TYPES, lower


VISITOR_VERSION = (15, 2)
POLARS_VERSION = "2.0.0"

# These logical integer types all fit native Int64 storage. UInt32/UInt64
# intentionally remain outside this subset.
_INTEGER_LAYOUTS = {pl.Int32: (32, True), pl.Int64: (64, True)}
_SUPPORTED_INPUT_TYPES = (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64, pl.Boolean, pl.String, pl.Date, pl.Categorical)


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

    def expression(self, node):
        # A column expression can be reused after an update changes its dtype.
        key = (node, tuple(self.visitor.get_schema().items()))
        if key in self.expressions:
            return self.expressions[key]
        value = self.visitor.view_expression(node)
        kind = type(value).__name__
        if kind == "Column":
            expr = ibex.col(value.name)
        elif kind == "Literal":
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
            if operator in ("==", "!=", "<", "<=", ">", ">="):
                operator = "polars_" + operator
            expr = Expr(operator, (self.expression(value.left), self.expression(value.right)))
        elif kind == "Agg":
            if value.name not in ("sum", "mean") or len(value.arguments) != 1:
                raise NotImplementedError(f"unsupported aggregate {value.name}")
            if value.options not in (None, False):
                raise NotImplementedError(f"unsupported {value.name} aggregate options")
            expr = Expr(value.name, (self.expression(value.arguments[0]),))
        elif kind == "Len":
            expr = ibex.len()
        elif kind == "Cast":
            original = self.visitor.get_dtype(value.expr)
            if value.dtype in _INTEGER_LAYOUTS and original in _INTEGER_LAYOUTS:
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
            args = tuple(self.expression(child) for child in value.input)
            name = str(value.function_data[0]).split(".")[-1]
            if name == "IsBetween" and len(args) == 3 and len(value.function_data) == 2:
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
            elif name == "negate" and len(args) == 1 and self.visitor.get_dtype(node) in (pl.Int32,):
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
        if kind == "DataFrameScan":
            if node.selection is not None:
                raise NotImplementedError("DataFrameScan selections are not supported yet")
            for name, dtype in schema.items():
                if dtype.base_type() not in _SUPPORTED_INPUT_TYPES:
                    raise NotImplementedError(f"unsupported resident column {name}: {dtype}")
            # PyDataFrame.select projects column handles, without collecting a
            # lazy query or evaluating user expressions in the Polars engine.
            data = pl.DataFrame._from_pydf(node.df.select(list(schema)))
            table = data.to_arrow()
            for index, field in enumerate(table.schema):
                if field.type in _WIDENED_NUMERIC_TYPES:
                    table = table.set_column(index, field.name, table.column(index).cast(_WIDENED_NUMERIC_TYPES[field.type]))
            frame = ibex.LazyFrame(("table", table))
        elif kind == "Scan":
            if node.scan_type[0] != "parquet" or len(node.paths) != 1:
                raise NotImplementedError("only single local Parquet scans are supported")
            if not isinstance(node.paths[0], str) or Path(node.paths[0]).is_dir():
                raise NotImplementedError("only single local Parquet file paths are supported")
            for name, dtype in schema.items():
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
