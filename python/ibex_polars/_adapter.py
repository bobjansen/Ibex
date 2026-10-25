# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Pinned adapter for Polars 2.0.0's private logical-plan visitor.

Visitor node/expression IDs drive translation. Original plan metadata supplies
join validation flags omitted by the visitor. Unknown nodes/options fail before
native input binding or execution.
"""

import json
import warnings
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


def _null_literal(dtype):
    if dtype == pl.Null:
        return Expr("null", ())
    if dtype in _INTEGER_LAYOUTS:
        placeholder = 0
    elif dtype in (pl.Float32, pl.Float64):
        placeholder = 0.0
    elif dtype == pl.Boolean:
        placeholder = False
    elif dtype == pl.String:
        placeholder = ""
    elif dtype == pl.Date:
        placeholder = date(1970, 1, 1)
    else:
        raise NotImplementedError(f"unsupported null literal type {dtype}")
    return Expr("typed_null", (ibex.lit(placeholder),))


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
        self.original_frame = lf
        self.checked_join_validation = False
        self.nodes = {}
        self.expressions = {}
        self.frame_length_columns = {}
        self.sort_pruning_predicates = set()

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
            if value.value is None:
                expr = _null_literal(value.dtype)
                self.expressions[key] = expr
                return expr
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
        elif kind == "Ternary":
            expr = Expr("case", (self.expression(value.predicate), self.expression(value.truthy),
                                 self.expression(value.falsy)))
        elif kind == "Agg":
            if len(value.arguments) != 1:
                raise NotImplementedError(f"unsupported aggregate {value.name}")
            input_type = self.visitor.get_dtype(value.arguments[0])
            if value.name in ("min", "max") and input_type in (pl.Float32, pl.Float64):
                if value.options not in (None, False):
                    raise NotImplementedError("unsupported extrema options")
                expr = Expr("polars_" + value.name, (self.expression(value.arguments[0]),))
                self.expressions[key] = expr
                return expr
            if value.name == "n_unique" and input_type in (*_INTEGER_LAYOUTS, pl.String, pl.Boolean):
                if value.options not in (None, False):
                    raise NotImplementedError("unsupported distinct-count options")
                expr = Expr("n_unique", (self.expression(value.arguments[0]),))
                self.expressions[key] = expr
                return expr
            allowed = ("min", "max", "sum") if input_type.base_type() == pl.Duration else ("min", "max")
            if value.name not in ("count", "first", "last", "first_non_null", "last_non_null") and value.name not in (allowed if is_temporal(input_type) else ("sum", "mean", "min", "max")):
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
            if original == pl.Null:
                expr = _null_literal(value.dtype)
            elif is_temporal(original) or is_temporal(value.dtype):
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
            if name == "IsIn" and len(value.input) == 2 and len(value.function_data) == 2:
                collection = self.visitor.view_expression(value.input[1])
                if (type(collection).__name__ != "Literal" or collection.dtype.base_type() != pl.List
                        or not isinstance(collection.value, list) or len(collection.value) > 256):
                    raise NotImplementedError("membership requires a literal list of at most 256 scalar values")
                child = self.expression(value.input[0])
                present = None
                for member in collection.value:
                    if member is not None:
                        comparison = Expr("polars_==", (child, ibex.lit(member)))
                        present = comparison if present is None else present | comparison
                nulls_equal = value.function_data[1]
                if type(nulls_equal) is not bool:
                    raise NotImplementedError("unsupported membership null policy")
                on_null = (ibex.lit(any(member is None for member in collection.value)) if nulls_equal
                           else Expr("typed_null", (ibex.lit(False),)))
                # Equality already propagates null. Keeping the comparison tree
                # bare also lets the engine recognize a scan membership filter.
                if present is not None and not nulls_equal:
                    expr = present
                else:
                    expr = Expr("case", (Expr("is_null", (child,)), on_null,
                                         ibex.lit(False) if present is None else present))
                self.expressions[key] = expr
                return expr
            args = tuple(self.expression(child) for child in value.input)
            if name == "fused" and len(args) == 3 and len(value.function_data) == 2:
                operation = value.function_data[1]
                if any(self.visitor.get_dtype(child) not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64)
                       for child in value.input):
                    raise NotImplementedError("fused arithmetic requires supported numeric inputs")
                if operation == "fma":
                    expr = args[0] * args[1] + args[2]
                elif operation == "fms":
                    expr = args[0] * args[1] - args[2]
                elif operation == "fsm":
                    expr = args[0] - args[1] * args[2]
                else:
                    raise NotImplementedError(f"unsupported fused arithmetic {operation}")
            elif name == "IsBetween" and len(args) == 3 and len(value.function_data) == 2:
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
            elif name in ("StartsWith", "EndsWith", "Contains") and len(args) == 2:
                pattern = self.visitor.view_expression(value.input[1])
                if type(pattern).__name__ != "Literal" or pattern.dtype != pl.String or not isinstance(pattern.value, str):
                    raise NotImplementedError("string matching requires a non-null literal pattern")
                if name == "Contains":
                    if len(value.function_data) != 3 or type(value.function_data[1]) is not bool:
                        raise NotImplementedError("unsupported string contains options")
                    if not value.function_data[1] and any(char in r".^$*+?{}[]\|()" for char in pattern.value):
                        raise NotImplementedError("regular-expression operators in contains are unsupported")
                elif len(value.function_data) != 1:
                    raise NotImplementedError("unsupported string matching options")
                escaped = "".join("\\" + char if char in "\\%_" else char for char in pattern.value)
                pattern = ("%" if name != "StartsWith" else "") + escaped + ("%" if name != "EndsWith" else "")
                expr = Expr("like", (args[0], ibex.lit(pattern)))
            elif name == "round" and len(args) == 1 and len(value.function_data) == 3:
                decimals, mode = value.function_data[1:]
                if type(decimals) is not int or not 0 <= decimals <= 15:
                    raise NotImplementedError("round supports decimal places from 0 to 15")
                if mode not in ("half_to_even", "half_away_from_zero"):
                    raise NotImplementedError("unsupported rounding mode")
                dtype = self.visitor.get_dtype(value.input[0])
                if dtype in _INTEGER_LAYOUTS:
                    expr = args[0]
                elif dtype in (pl.Float32, pl.Float64):
                    expr = Expr("polars_round", (args[0], decimals, mode))
                else:
                    raise NotImplementedError(f"unsupported rounding input {dtype}")
            elif name == "Year" and len(args) == 1 and len(value.function_data) == 1:
                if self.visitor.get_dtype(value.input[0]) != pl.Date:
                    raise NotImplementedError("year extraction currently requires Date input")
                expr = Expr("year", args)
            elif name == "dynamic_pred" and len(value.function_data) == 2 and value.function_data[1] in self.sort_pruning_predicates:
                # This predicate is optional top-k pruning for a known sort.
                # A full native sort followed by the slice needs no pruning.
                expr = ibex.lit(True)
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

    def _check_join_validation(self):
        if self.checked_join_validation:
            return
        # Visitor 15.2 omits JoinValidation. Read original plan metadata once
        # so cardinality assertions cannot silently become unchecked joins.
        def discard_payload(value):
            if "df" in value and "schema" in value:
                value["df"] = None
            return value
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            plan = json.loads(self.original_frame.serialize(format="json"), object_hook=discard_payload)
        def walk(value):
            if isinstance(value, dict):
                if "Join" in value:
                    validation = value["Join"]["options"]["args"]["validation"]
                    if validation != "ManyToMany":
                        raise NotImplementedError("join cardinality validation is not supported by this visitor adapter")
                for key, child in value.items():
                    if key != "df":
                        walk(child)
            elif isinstance(value, list):
                for child in value:
                    if isinstance(child, (dict, list)):
                        walk(child)
        walk(plan)
        self.checked_join_validation = True

    def _aggregate_rounding(self, frame, fields, *, keys=None, maintain_order=False):
        def contains_round(expr):
            return expr._op == "polars_round" or any(
                contains_round(arg) for arg in expr._args if isinstance(arg, Expr))
        if not any(expr._aggregate and contains_round(expr) for expr in fields):
            return (frame.select(fields) if keys is None else
                    frame.group_by(*keys, maintain_order=maintain_order).agg(fields))
        # Guarded rounding is a row expression. Materialize its reductions
        # first, then round the aggregate rows in the same native query plan.
        occupied = set(self.visitor.get_schema()) | {expr._name for expr in fields}
        reductions = []
        def rewrite(expr):
            if expr._aggregate and not contains_round(expr):
                name = f"_ibex_round_aggregate_{len(reductions)}"
                while name in occupied:
                    name += "_"
                occupied.add(name)
                reductions.append(expr.alias(name))
                return ibex.col(name)
            return Expr(expr._op, tuple(rewrite(arg) if isinstance(arg, Expr) else arg
                                       for arg in expr._args))
        projected = [rewrite(expr).alias(expr._name) for expr in fields]
        if keys is None:
            return frame.select(reductions).select(projected)
        return frame.group_by(*keys, maintain_order=maintain_order).agg(reductions).select(
            [ibex.col(key) for key in keys] + projected)

    def _hstack(self, frame, expressions, output_names, node_id):
        # Polars CSE lifts repeat's length into a WITH_COLUMNS field.
        lengths = {expr._name for expr in expressions if expr._args[0]._op in ("len", "column_len")}
        expressions = tuple(
            Expr("broadcast_length", (expr._args[0],)).alias(expr._name)
            if expr._name in lengths else expr for expr in expressions
        )
        self.frame_length_columns[node_id] = lengths
        if any(expr._aggregate for expr in expressions):
            raise NotImplementedError("reduction broadcasting in HStack is not supported yet")
        if expressions:
            frame = frame._append("hstack", expressions)
        return frame.select(output_names)

    def _row_selection(self, node_id, *, drop_height=False):
        """Recognize uniform gather/reverse projections as a single row operation."""
        previous = self.visitor.get_node()
        try:
            self.visitor.set_node(node_id)
            node = self.visitor.view_current_node()
            if type(node).__name__ != "Select" or not node.expr:
                return None
            self.visitor.set_node(node.input)
            operation, operands = None, []
            for named in node.expr:
                value = self.visitor.view_expression(named.node)
                if type(value).__name__ != "Function" or len(value.input) != 1:
                    return None
                data = value.function_data
                if not ((data[0] == "reverse" and len(data) == 1) or
                        (data[0] == "gather_every" and len(data) == 3 and data[2] > 0)):
                    return None
                if operation is not None and data != operation:
                    return None
                operation = data
                operands.append((value.input[0], named.output_name))
            # LazyFrame.gather_every inserts an empty struct solely to carry
            # height, then drops it in a surrounding projection. Remove only
            # that exact wrapper; this does not add user-facing struct support.
            marker = self.visitor.view_current_node()
            marker_schema = self.visitor.get_schema()
            height = "__POLARS_INTERNAL_HEIGHT_COLUMN"
            hidden = []
            if drop_height and type(marker).__name__ == "HStack":
                for named in marker.exprs:
                    value = self.visitor.view_expression(named.node)
                    if (named.output_name == height and type(value).__name__ == "Literal"
                            and value.dtype == pl.Struct({}) and type(value.value) is dict and value.value == {}):
                        hidden.append(named.node)
            if hidden:
                visible = []
                for operand, alias in operands:
                    value = self.visitor.view_expression(operand)
                    if type(value).__name__ == "Column" and value.name == height and alias == height:
                        continue
                    visible.append((operand, alias))
                operands = visible
                frame = self.frame(marker.input)
                self.visitor.set_node(marker.input)
                expressions = tuple(self.named(expr) for expr in marker.exprs if expr.node not in hidden)
                frame = self._hstack(frame, expressions, [name for name in marker_schema if name != height], node.input)
            else:
                frame = self.frame(node.input)
            self.visitor.set_node(node.input)
            frame = frame.select([self.expression(operand).alias(alias) for operand, alias in operands])
            if operation[0] == "reverse":
                return frame[::-1]
            return frame.gather_every(operation[2], offset=operation[1])
        finally:
            self.visitor.set_node(previous)

    def frame(self, node_id):
        if node_id in self.nodes:
            return self.nodes[node_id]
        self.visitor.set_node(node_id)
        node = self.visitor.view_current_node()
        kind = type(node).__name__
        schema = self.visitor.get_schema()
        if kind == "Sort" and node.slice is not None and len(node.slice) == 3 and node.slice[2] is not None:
            self.sort_pruning_predicates.add(node.slice[2])
        if kind == "Select":
            selected = self._row_selection(node_id)
            if selected is not None:
                self.nodes[node_id] = selected
                return selected
            if ("__POLARS_INTERNAL_HEIGHT_COLUMN" not in schema and
                    all(type(self.visitor.view_expression(expr.node)).__name__ == "Column" for expr in node.expr)):
                selected = self._row_selection(node.input, drop_height=True)
                if selected is not None:
                    self.visitor.set_node(node.input)
                    frame = selected.select([self.named(expr) for expr in node.expr])
                    self.nodes[node_id] = frame
                    return frame
        elif kind == "SimpleProjection" and "__POLARS_INTERNAL_HEIGHT_COLUMN" not in schema:
            selected = self._row_selection(node.input, drop_height=True)
            if selected is not None:
                frame = selected.select(list(schema))
                self.nodes[node_id] = frame
                return frame
        if kind == "MapFunction" and len(node.function) == 2 and node.function[0] == "hint_sorted":
            # A sortedness assertion leaves rows unchanged. Ignore the claim
            # until native metadata propagation is implemented; existing native
            # ordering proofs and general algorithms remain available.
            frame = self.frame(node.input)
            self.frame_length_columns[node_id] = self.frame_length_columns.get(node.input, set())
        elif kind == "Cache":
            # Supported subplans are deterministic. Reuse the immutable frame
            # graph; native collection may recompute a shared subplan rather
            # than maintaining Polars' physical cache. No Python collection.
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
        elif kind == "Join":
            how, nulls_equal, slice_, suffix, coalesce, maintain_order = node.options
            if not isinstance(how, str):
                raise NotImplementedError(f"unsupported join options {how}")
            how = how.lower()
            left = self.frame(node.input_left)
            right = self.frame(node.input_right)
            def keys(expressions, input_node):
                self.visitor.set_node(input_node)
                result = []
                for named in expressions:
                    expr = self.expression(named.node)
                    if expr._op != "col":
                        raise NotImplementedError("computed join keys are unsupported")
                    result.append(expr._name)
                return result
            left_on = keys(node.left_on, node.input_left)
            self.visitor.set_node(node.input_left)
            left_columns = tuple(self.visitor.get_schema())
            right_on = keys(node.right_on, node.input_right)
            right_columns = tuple(self.visitor.get_schema())
            self._check_join_validation()
            frame = left.join(right, left_on=left_on, right_on=right_on, how=how,
                              suffix=suffix, nulls_equal=nulls_equal, coalesce=coalesce,
                              maintain_order=maintain_order, _left_columns=left_columns, _right_columns=right_columns)
            if slice_ is not None:
                frame = frame.slice(*slice_)
        elif kind == "Slice":
            frame = self.frame(node.input).slice(node.offset, node.len)
        elif kind == "Distinct":
            keep, subset, maintain_order, slice_ = node.options
            frame = self.frame(node.input).unique(subset, keep=keep, maintain_order=maintain_order)
            if slice_ is not None:
                frame = frame.slice(*slice_)
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
                value = self.visitor.view_expression(node.expr[0].node) if len(node.expr) == 1 else None
                if (type(value).__name__ == "Function" and len(value.input) == 1
                        and len(value.function_data) == 2 and value.function_data[0] == "unique"
                        and type(value.function_data[1]) is bool):
                    child = self.expression(value.input[0])
                    if child._aggregate or not child._row_reference:
                        raise NotImplementedError("unique projection requires one row expression")
                    frame = frame.select(child.alias(node.expr[0].output_name)).unique(
                        maintain_order=value.function_data[1])
                else:
                    frame = self._aggregate_rounding(frame, [self.named(expr) for expr in node.expr])
            elif kind == "HStack":
                expressions = tuple(self.named(expr) for expr in node.exprs)
                frame = self._hstack(frame, expressions, list(schema), node_id)
            elif kind == "GroupBy":
                if node.apply or any(getattr(node.options, name) is not None for name in ("dynamic", "rolling")):
                    raise NotImplementedError("group UDFs and dynamic/rolling groups are unsupported")
                keys, representatives = [], []
                occupied = set(self.visitor.get_schema()) | set(schema)
                def temporary(role):
                    name = f"_ibex_group_{node_id}_{role}"
                    while name in occupied:
                        name += "_"
                    occupied.add(name)
                    return name
                for index, key in enumerate(node.keys):
                    expr = self.expression(key.node)
                    if expr._op != "col" or expr._name != key.output_name:
                        raise NotImplementedError("computed or renamed grouping keys are not supported yet")
                    if self.visitor.get_dtype(key.node) in (pl.Float32, pl.Float64):
                        flag, normalized = temporary(f"nan_{index}"), temporary(f"value_{index}")
                        is_nan = Expr("is_nan", (expr,))
                        frame = frame._append("hstack", (
                            Expr("case", (is_nan, ibex.lit(True), ibex.lit(False))).alias(flag),
                            Expr("case", (is_nan, ibex.lit(0.0), expr)).alias(normalized)))
                        keys.extend((flag, normalized))
                        # Return an original key (including null) rather than
                        # exposing the normalized helper values as labels.
                        representatives.append(Expr("first", (expr,)).alias(expr._name))
                    else:
                        keys.append(expr._name)
                frame = self._aggregate_rounding(
                    frame, representatives + [self.named(expr) for expr in node.aggs],
                    keys=keys, maintain_order=node.maintain_order)
                if representatives:
                    frame = frame.select(list(schema))
                if node.options.slice is not None:
                    frame = frame.slice(*node.options.slice)
            elif kind == "Sort":
                keys = []
                for key in node.by_column:
                    expr = self.expression(key.node)
                    if expr._op != "col":
                        raise NotImplementedError("computed sort keys are not supported yet")
                    keys.append(expr._name)
                stable, nulls_last, descending = node.sort_options
                frame = frame.sort(keys, descending=descending, nulls_last=nulls_last, maintain_order=stable)
                if node.slice is not None:
                    frame = frame.slice(*node.slice[:2])
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
        prepared_sources = {}
        def prepare_inputs(current):
            source = current._source
            if source[0] == "table":
                key = id(source[1])
                if key not in prepared_sources:
                    prepared_sources[key] = ibex.DataFrame(source[1]).lazy()._source
                source = prepared_sources[key]
            steps = tuple((operation, (prepare_inputs(args[0]), *args[1:])) if operation == "join"
                          else (operation, args) for operation, args in current._steps)
            return ibex.LazyFrame(source, steps)
        frame = prepare_inputs(frame)
        result_schema = {}
        source, bindings, scalars = lower(frame, validate=True, result_schema=result_schema, widen_numeric_inputs=True)
        return source, bindings, scalars, expected, result_schema
