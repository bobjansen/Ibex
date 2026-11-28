# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Pinned adapter for Polars 2.1.0's private logical-plan visitor.

Visitor node/expression IDs drive translation. Original plan metadata supplies
join validation flags omitted by the visitor. Unknown nodes/options fail before
native input binding or execution.
"""

import functools
import json
import re
import warnings
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa

import ibex
from ibex._expr import Expr
from ibex._lower import _WIDENED_NUMERIC_TYPES, lower
from . import _categories
from ._temporal import NANOS_PER_UNIT, TEMPORAL_TYPES, is_temporal, literal_ticks, prepare_column


VISITOR_VERSION = (16, 0)
POLARS_VERSION = "2.1.0"

# These logical integer types all fit native Int64 storage. UInt64 needs a wider representation.
_INTEGER_LAYOUTS = {
    pl.Int8: (8, True), pl.Int16: (16, True), pl.Int32: (32, True),
    pl.Int64: (64, True), pl.UInt8: (8, False), pl.UInt16: (16, False), pl.UInt32: (32, False),
}
_JOIN_NODE = re.compile(r'\{"Join"\s*:\s*\{')
_JOIN_VALIDATION = re.compile(r'"validation"\s*:\s*"(ManyToMany|ManyToOne|OneToMany|OneToOne)"')
def _streaming_plan(frame):
    return frame._ldf.to_dot_streaming_phys(False)


@functools.cache
def _validation_shows_in_streaming_plan():
    """Whether this Polars plans a validated join as the in-memory fallback.

    The streaming engine runs only joins with the default cardinality check;
    any other validation is planned as an `in-memory-join`. That is visible in
    the plan graph without serializing any resident data. Confirm it on small
    frames, so a Polars that changes it falls back to reading the plan.
    """
    try:
        left = pl.LazyFrame({"k": [1, 2], "x": [1, 2]})
        right = pl.LazyFrame({"k": [1, 2], "y": [1, 2]})
        if "in-memory-join" in _streaming_plan(left.join(right, on="k")):
            return False
        return all("in-memory-join" in _streaming_plan(left.join(right, on="k", validate=validation))
                   for validation in ("1:1", "m:1", "1:m"))
    except Exception:
        return False


def _joins_may_be_validated(frame):
    """False only when no join in the plan can carry a cardinality check."""
    if not _validation_shows_in_streaming_plan():
        return True
    try:
        return "in-memory-join" in _streaming_plan(frame)
    except Exception:
        return True


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


def _like_literal(pattern):
    """The literal text of a LIKE pattern that is X, X%, %X or %X% with no
    other wildcard, or None. Backslash escapes as the adapter emits them."""
    tokens, index = [], 0
    while index < len(pattern):
        char = pattern[index]
        if char == "\\" and index + 1 < len(pattern):
            tokens.append((pattern[index + 1], False))
            index += 2
            continue
        tokens.append((char, char in "%_"))
        index += 1
    if tokens and tokens[0] == ("%", True):
        tokens = tokens[1:]
    if tokens and tokens[-1] == ("%", True):
        tokens = tokens[:-1]
    if any(wildcard for _, wildcard in tokens):
        return None
    return "".join(char for char, _ in tokens)


class _NoPrefixRewrite(Exception):
    pass


def check_version():
    if pl.__version__ != POLARS_VERSION:
        raise NotImplementedError(f"IbexEngine supports Polars {POLARS_VERSION}; found {pl.__version__}")


def _at_most_one_row(frame):
    """Conservative bound: never expand a general inequality join to N*M."""
    one = frame._source[0] == "table" and frame._source[1].num_rows <= 1
    for operation, args in frame._steps:
        if operation == "select" and all(expr._aggregate and not expr._row_reference for expr in args):
            one = True  # global reductions produce one row, including empty input
        elif operation == "join":
            other, _, _, how, *_ = args
            one = one and (how in ("semi", "anti") or
                           (how != "full" and _at_most_one_row(other)))
        elif operation == "slice" and args[1] is not None and 0 <= args[1] <= 1:
            one = True
        elif operation not in ("select", "filter", "update", "hstack", "aggregate", "slice",
                                "python_slice", "sort", "unique", "cache"):
            return False
    return one


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
        self.categories = []
        # (row-number column, left frame): see _passthrough_join.
        self.left_rows = None
        self.root = None
        self.nodes = {}
        self.expressions = {}
        self.has_windows = False
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
                         "EqValidity": "==", "NotEqValidity": "!=", "Eq": "==", "NotEq": "!=", "Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">=",
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
            left, right = self.expression(value.left), self.expression(value.right)
            expr = Expr(operator, (left, right))
            if name in ("EqValidity", "NotEqValidity"):
                left_null = Expr("is_null", (left,))
                right_null = Expr("is_null", (right,))
                both_missing = right_null if name == "EqValidity" else ~right_null
                expr = Expr("case", (left_null, both_missing,
                                     Expr("case", (right_null, ibex.lit(name == "NotEqValidity"), expr))))
        elif kind == "Window":
            if getattr(value.options, "kind", None) not in ("groups_to_rows", "join"):
                raise NotImplementedError("unsupported window mapping")
            keys = [self.expression(key) for key in value.partition_by]
            if not keys or any(key._aggregate for key in keys):
                raise NotImplementedError("window partitions require row expressions")
            ordering = None
            if value.order_by is not None:
                order = self.expression(value.order_by)
                if order._op != "col":
                    raise NotImplementedError("window ordering requires a column key")
                ordering = (order._name, value.order_by_descending, value.order_by_nulls_last)
            call = self.visitor.view_expression(value.function)
            if type(call).__name__ == "Function" and call.function_data in (("cum_sum", False), ("cum_max", False)):
                input_type = self.visitor.get_dtype(call.input[0])
                if input_type not in (*_INTEGER_LAYOUTS, pl.Float64):
                    raise NotImplementedError("cumulative sums require integer or Float64 inputs")
                child = self.expression(call.input[0])
                if not child._row_reference:
                    # Cumulative functions on a scalar see one value per group.
                    function = child
                elif child._op == "col" and call.function_data[0] == "cum_sum":
                    function = Expr("window_cumsum", (child, input_type == pl.Float64))
                else:
                    raise NotImplementedError("unsupported cumulative window input")
            elif type(call).__name__ == "Function" and call.function_data[0] == "rank":
                _, method, descending, seed = call.function_data
                child = self.expression(call.input[0])
                if (child._op != "col" or method not in ("average", "min", "max", "dense", "ordinal")
                        or seed is not None):
                    raise NotImplementedError("rank requires a column and a deterministic tie method")
                function = Expr("window_rank", (child, "first" if method == "ordinal" else method, descending))
            elif type(call).__name__ == "Function" and call.function_data == ("shift",) and len(call.input) == 2:
                child = self.expression(call.input[0])
                offset = self.visitor.view_expression(call.input[1])
                if (child._op != "col" or self.visitor.get_dtype(call.input[0]) not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64)
                        or type(offset).__name__ != "Literal" or type(offset.value) is not int
                        or abs(offset.value) >= 2**63):
                    raise NotImplementedError("window shift requires a numeric column and literal offset")
                function = Expr("window_shift", (child, offset.value))
            else:
                function = self.expression(value.function)
            if value.options.kind == "join" and function._row_reference:
                raise NotImplementedError("list-valued window joins are unsupported")
            self.has_windows = True
            expr = Expr("window", (function, tuple(keys), ordering))
            if function._op == "window_cumsum" and self.visitor.get_dtype(node) in _INTEGER_LAYOUTS:
                expr = _wrap_integer(expr, self.visitor.get_dtype(node))
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
            if value.name in ("median", "std") and input_type in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64):
                # Polars' std option is ddof; native std is the sample (ddof 1).
                if value.options != (None if value.name == "median" else 1):
                    raise NotImplementedError(f"unsupported {value.name} options {value.options!r}")
                # Native median ranks NaN above every number, as Polars does.
                expr = Expr("polars_" + value.name, (self.expression(value.arguments[0]),))
                self.expressions[key] = expr
                return expr
            if value.name == "n_unique" and input_type in (*_INTEGER_LAYOUTS, pl.String, pl.Boolean):
                if value.options not in (None, False):
                    raise NotImplementedError("unsupported distinct-count options")
                child = self.visitor.view_expression(value.arguments[0])
                if type(child).__name__ == "Filter":
                    expr = Expr("filtered_n_unique", (self.expression(child.input), self.expression(child.by)))
                else:
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
            elif value.name == "sum" and value.options is True:
                # Polars folds `when(x.count() > 0).then(x.sum())` into a sum
                # whose option makes empty and all-null input null, not zero.
                operation = "nullable_sum"
            elif value.options not in (None, False):
                raise NotImplementedError(f"unsupported {value.name} aggregate options")
            expr = Expr(operation, (self.expression(value.arguments[0]),))
            if input_type == pl.Boolean and operation in ("sum", "nullable_sum"):
                self.expressions[key] = expr
                return expr
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
                alternatives = [pattern.value]
                if name == "Contains":
                    if len(value.function_data) != 3 or type(value.function_data[1]) is not bool:
                        raise NotImplementedError("unsupported string contains options")
                    if not value.function_data[1]:
                        # Temporary PDS compatibility: ordered literal pieces
                        # separated by .* use LIKE %. Unlike regex dot, this
                        # wildcard may also consume newlines. Other regex
                        # operators retain the explicit unsupported boundary.
                        alternatives = pattern.value.split("|")
                        if len(alternatives) > 256:
                            raise NotImplementedError("contains supports at most 256 literal alternatives")
                        if any(char in r".^$*+?{}[]\()" for alternative in alternatives
                               for part in alternative.split(".*") for char in part):
                            raise NotImplementedError("regular-expression operators other than .* and literal alternatives are unsupported")
                elif len(value.function_data) != 1:
                    raise NotImplementedError("unsupported string matching options")
                expr = None
                for alternative in alternatives:
                    fragments = (alternative.split(".*") if name == "Contains" and not value.function_data[1]
                                 else [alternative])
                    escaped = "%".join("".join("\\" + char if char in "\\%_" else char for char in part)
                                       for part in fragments)
                    pattern = ("%" if name != "StartsWith" else "") + escaped + ("%" if name != "EndsWith" else "")
                    match = Expr("like", (args[0], ibex.lit(pattern)))
                    expr = match if expr is None else expr | match
            elif name == "Slice" and len(args) == 3 and len(value.function_data) == 1:
                if self.visitor.get_dtype(value.input[0]) != pl.String:
                    raise NotImplementedError("string slicing requires String input")
                offset, length = (self.visitor.view_expression(child) for child in value.input[1:])
                if (type(offset).__name__ != "Literal" or type(offset.value) is not int or
                        type(length).__name__ != "Literal" or type(length.value) is not int or length.value < 0):
                    raise NotImplementedError("string slicing requires a literal offset and nonnegative length")
                if offset.value < 0:
                    # Polars clips the requested interval, while native
                    # substring first clips the start and then takes length.
                    begin = Expr("string_length", (args[0],)) + args[1]
                    clipped = Expr("case", (begin < 0, args[2] + begin, args[2]))
                    length_expr = Expr("case", (clipped < 0, ibex.lit(0), clipped))
                    expr = Expr("substring", (args[0], args[1], length_expr))
                else:
                    expr = Expr("substring", args)
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
            elif name == "corr" and value.function_data == ("corr", "pearson") and len(args) == 2:
                if any(self.visitor.get_dtype(child) not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64)
                       for child in value.input):
                    raise NotImplementedError("corr requires numeric inputs")
                expr = Expr("polars_corr", args)
            elif name == "pow" and len(args) == 2 and len(value.function_data) == 1:
                # Only x ** 2 on a float: x * x, which IEEE rounds exactly as
                # pow(x, 2) does. Other exponents and bases stay unsupported.
                exponent = self.visitor.view_expression(value.input[1])
                if (type(exponent).__name__ != "Literal" or exponent.value != 2
                        or type(exponent.value) is not int
                        or self.visitor.get_dtype(value.input[0]) != pl.Float64):
                    raise NotImplementedError("pow is supported only as a Float64 squared")
                expr = Expr("square", (args[0],))
            elif name in ("abs", "sqrt", "exp", "floor", "ceil") and len(args) == 1 and len(value.function_data) == 1:
                if self.visitor.get_dtype(value.input[0]) != pl.Float64:
                    raise NotImplementedError(f"{name} currently requires Float64")
                expr = Expr(name, args)
            elif name in ("LenChars", "LenBytes") and len(args) == 1:
                expr = Expr("string_length" if name == "LenChars" else "byte_length", args)
            elif name in ("IsNan", "IsNotNan") and len(args) == 1:
                if self.visitor.get_dtype(value.input[0]) not in (pl.Float32, pl.Float64):
                    raise NotImplementedError(f"{name} currently requires floating-point input")
                predicate = Expr("is_nan", args)
                expr = predicate if name == "IsNan" else ~predicate
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
            "count", "column_len", "n_unique", "filtered_n_unique",
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
        if not _joins_may_be_validated(self.original_frame):
            self.checked_join_validation = True
            return
        # Visitor 15.2 omits JoinValidation. Read original plan metadata once
        # so cardinality assertions cannot silently become unchecked joins.
        # Plans whose joins all have the default check never reach this point.
        # Resident data serializes as an array of IPC bytes, one JSON number
        # each, so decoding the document would build the data as Python
        # integers. Only the text around the data is of interest, and none of
        # the data can spell a quoted name.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            plan = self.original_frame.serialize(format="json")
        # A field of that name that is not a join's, such as a column in a
        # schema, has some other value and is not matched.
        validations = _JOIN_VALIDATION.findall(plan)
        if len(validations) != len(_JOIN_NODE.findall(plan)):
            raise NotImplementedError("join cardinality validation could not be read from the plan")
        if any(validation != "ManyToMany" for validation in validations):
            raise NotImplementedError("join cardinality validation is not supported by this visitor adapter")
        self.checked_join_validation = True

    def _window_fields(self, frame, fields, output_names):
        if not self.has_windows:
            return frame, fields
        occupied = set(self.visitor.get_schema()) | set(output_names)
        partitions = {}
        columns = {}

        inputs = []
        prepared = {}
        def fresh():
            name = f"_ibex_window_{len(columns) + len(inputs)}"
            while name in occupied:
                name += "_"
            occupied.add(name)
            return name

        def may_have_nulls(column):
            kind, table = frame._source
            return bool(frame._steps or kind != "table" or table[column._name].null_count)

        def input_column(expr):
            if expr._op == "col":
                return expr._name
            if id(expr) not in prepared:
                name = fresh()
                inputs.append(expr.alias(name))
                prepared[id(expr)] = name
            return prepared[id(expr)]

        def rewrite(expr):
            if expr._op == "window":
                if id(expr) not in columns:
                    function, keys, ordering = expr._args
                    partition = tuple(input_column(key) for key in keys)
                    name = fresh()
                    result = ibex.col(name)
                    if function._op == "window_cumsum":
                        child, floating = function._args
                        # A null contributes zero to the running total but
                        # stays null in the output at its original position.
                        if may_have_nulls(child):
                            missing = Expr("is_null", (child,))
                            clean = Expr("case", (missing, ibex.lit(0.0 if floating else 0), child))
                            child = ibex.col(input_column(clean))
                            result = Expr("case", (missing, Expr("null", ()), result))
                        function = Expr("cumsum", (child,))
                    elif function._op == "window_shift":
                        child, offset = function._args
                        direction = "lag" if offset >= 0 else "lead"
                        if may_have_nulls(child):
                            missing = Expr("is_null", (child,))
                            flag = Expr("case", (missing, ibex.lit(1), ibex.lit(0)))
                            flag_column = ibex.col(input_column(flag))
                            flag_name = fresh()
                            values = partitions.setdefault((partition, ordering), [])
                            values.append(Expr(direction, (flag_column, abs(offset))).alias(flag_name))
                            result = Expr("case", (ibex.col(flag_name) == 1, Expr("null", ()), result))
                        function = Expr(direction, (child, abs(offset)))
                    columns[id(expr)] = result
                    partitions.setdefault((partition, ordering), []).append(function.alias(name))
                return columns[id(expr)]
            return Expr(expr._op, tuple(rewrite(arg) if isinstance(arg, Expr) else arg for arg in expr._args))

        fields = [rewrite(expr) for expr in fields]
        if inputs:
            frame = frame._append("hstack", tuple(inputs))
        for (keys, ordering), values in partitions.items():
            frame = frame._append("window", (keys, tuple(values), ordering))
        return frame, fields

    def _aggregate_projection(self, frame, fields, *, keys=None, maintain_order=False):
        def needs_projection(expr):
            if expr._op in ("polars_round", "square", "abs", "sqrt", "exp", "floor", "ceil",
                            "string_length", "byte_length", "is_nan"):
                return expr._aggregate
            return any(needs_projection(arg) for arg in expr._args if isinstance(arg, Expr))
        if not any(expr._aggregate and needs_projection(expr) for expr in fields):
            return (frame.select(fields) if keys is None else
                    frame.group_by(*keys, maintain_order=maintain_order).agg(fields))
        if any(not expr._aggregate or expr._row_reference for expr in fields):
            raise NotImplementedError("aggregate projections require scalar reductions")
        if keys is not None and any(expr._name in keys for expr in fields):
            raise ValueError("aggregate output name conflicts with a grouping key")
        # Evaluate row functions over materialized reductions in the same native plan.
        occupied = set(self.visitor.get_schema()) | {expr._name for expr in fields}
        reductions = []
        def rewrite(expr):
            if expr._aggregate and not needs_projection(expr):
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
                frame, expressions = self._window_fields(frame, expressions, marker_schema)
                frame = self._hstack(frame, expressions, [name for name in marker_schema if name != height], node.input)
            else:
                frame = self.frame(node.input)
            self.visitor.set_node(node.input)
            fields = [self.expression(operand).alias(alias) for operand, alias in operands]
            frame, fields = self._window_fields(frame, fields, [alias for _, alias in operands])
            frame = frame.select(fields)
            if operation[0] == "reverse":
                return frame[::-1]
            return frame.gather_every(operation[2], offset=operation[1])
        finally:
            self.visitor.set_node(previous)

    def _filter_below_prefix_slice(self, input_id, predicate):
        """Apply a filter below a leading string slice it only tests by prefix.

        With c = slice(s, 0, n), c has at most n codepoints, so matching a
        literal of exactly n codepoints anywhere in c (or at either end, or
        exactly) is the same as s starting with it, nulls included. The
        filter then runs before the slice, which only sees surviving rows.
        Any other use of a computed column keeps the original placement.
        """
        projections, current = [], input_id
        while True:
            self.visitor.set_node(current)
            node = self.visitor.view_current_node()
            if type(node).__name__ == "SimpleProjection":
                projections.append(current)
                current = node.input
            elif type(node).__name__ == "HStack":
                break
            else:
                return None
        hstack_id, hstack = current, node
        self.visitor.set_node(hstack.input)
        expressions = tuple(self.named(expr) for expr in hstack.exprs)
        computed = {expr._args[1] for expr in expressions}
        slices = {}
        for expr in expressions:
            value = expr._args[0]
            if value._op != "substring" or len(value._args) != 3:
                continue
            source, offset, length = value._args
            if (source._op == "col" and source._args[0] not in computed and
                    offset._op == "lit" and type(offset._args[0]) is int and offset._args[0] == 0 and
                    length._op == "lit" and type(length._args[0]) is int and length._args[0] >= 0):
                slices[expr._args[1]] = (source._args[0], length._args[0])
        if not slices:
            return None

        def rewrite(expr):
            if expr._op == "like" and expr._args[0]._op == "col" and expr._args[0]._args[0] in slices:
                pattern = expr._args[1]
                text = (_like_literal(pattern._args[0]) if pattern._op == "lit" and
                        isinstance(pattern._args[0], str) else None)
                source, length = slices[expr._args[0]._args[0]]
                if text is None or len(text) != length:
                    raise _NoPrefixRewrite
                escaped = "".join("\\" + char if char in "\\%_" else char for char in text)
                return Expr("like", (ibex.col(source), ibex.lit(escaped + "%")))
            if expr._op == "col" and expr._args[0] in computed:
                raise _NoPrefixRewrite
            return Expr(expr._op, tuple(rewrite(arg) if isinstance(arg, Expr) else arg for arg in expr._args))

        try:
            rewritten = rewrite(predicate)
        except _NoPrefixRewrite:
            return None
        frame = self.frame(hstack.input).filter(rewritten)
        self.visitor.set_node(hstack_id)
        frame = self._hstack(frame, expressions, list(self.visitor.get_schema()), hstack_id)
        for node_id in reversed(projections):
            self.visitor.set_node(node_id)
            frame = frame.select(list(self.visitor.get_schema()))
        return frame

    def _literal_count(self, node):
        value = self.visitor.view_expression(node)
        if (type(value).__name__ != "Literal" or type(value.value) is not int
                or value.dtype not in _INTEGER_LAYOUTS or value.value < 0):
            raise NotImplementedError("per-group row counts must be non-negative integer literals")
        return value.value

    def _exploded_group_head(self, node):
        """Polars' plan for a per-group row selection: lists, then explode.

        `group_by(keys).head(n)` aggregates `col.slice(0, n).implode()` for
        every other column, and `agg(col.top_k(k)).explode(...)` aggregates
        `col.top_k(k).implode()`; both explode the lists back into rows. Each
        becomes the native per-group `head` (SPEC: head with by), top_k after
        a descending sort with nulls last and NaN above every number, as
        Polars ranks them. A group's top_k values keep no promised order.
        """
        _, exploded, _empty_as_null, _keep_nulls = node.function
        self.visitor.set_node(node.input)
        group = self.visitor.view_current_node()
        if type(group).__name__ != "GroupBy":
            raise NotImplementedError("explode is supported only for per-group head and top_k")
        if group.apply or group.options.slice is not None or any(
                getattr(group.options, name) is not None for name in ("dynamic", "rolling")):
            raise NotImplementedError("unsupported group options for a per-group selection")
        # Keys and aggregates resolve against the group's input.
        self.visitor.set_node(group.input)
        keys = []
        for key in group.keys:
            expr = self.expression(key.node)
            if expr._op != "col" or expr._name != key.output_name:
                raise NotImplementedError("computed or renamed grouping keys are not supported yet")
            keys.append(expr._name)
        heads, top = [], None
        for agg in group.aggs:
            value = self.visitor.view_expression(agg.node)
            if type(value).__name__ != "Agg" or value.name != "implode":
                raise NotImplementedError("explode is supported only for per-group head and top_k")
            inner = self.visitor.view_expression(value.arguments[0])
            inner_kind = type(inner).__name__
            if inner_kind == "Slice":
                column = self.visitor.view_expression(inner.input)
                if (type(column).__name__ != "Column" or column.name != agg.output_name
                        or self._literal_count(inner.offset) != 0):
                    raise NotImplementedError("per-group slices must be head(n) of a column")
                heads.append((column.name, self._literal_count(inner.length)))
            elif (inner_kind == "Function" and inner.function_data == ("top_k", False)
                    and len(inner.input) == 2):
                column = self.visitor.view_expression(inner.input[0])
                if type(column).__name__ != "Column":
                    raise NotImplementedError("top_k is supported only on a column")
                if self.visitor.get_dtype(inner.input[0]) not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64):
                    raise NotImplementedError("top_k is supported only on numeric columns")
                top = (column.name, agg.output_name, self._literal_count(inner.input[1]))
            else:
                raise NotImplementedError("explode is supported only for per-group head and top_k")
        outputs = [agg.output_name for agg in group.aggs]
        if sorted(exploded) != sorted(outputs) or len(set(outputs)) != len(outputs):
            raise NotImplementedError("explode must expand every per-group list")
        frame = self.frame(group.input)
        if top is not None:
            if heads or len(group.aggs) != 1:
                raise NotImplementedError("top_k is supported as the only aggregate")
            if group.maintain_order:
                raise NotImplementedError("top_k with maintain_order=True is not supported yet")
            name, output, count = top
            if count == 0:
                raise NotImplementedError("per-group top_k(0) is not supported")
            frame = (frame.select([*keys, name])
                     .sort(name, descending=True, nulls_last=True, maintain_order=True)
                     .group_by(keys).head(count))
            return frame.select([*(ibex.col(key) for key in keys), ibex.col(name).alias(output)])
        counts = {count for _, count in heads}
        if len(counts) != 1:
            raise NotImplementedError("per-group head needs one row count for every column")
        if 0 in counts:
            # Polars explodes each group's empty list into one null row.
            raise NotImplementedError("per-group head(0) is not supported")
        frame = frame.select([*keys, *(name for name, _ in heads)])
        return frame.group_by(keys, maintain_order=group.maintain_order).head(counts.pop())

    def _resident_table(self, node, schema, *, row_number=None, ids=()):
        """The Arrow table a resident DataFrameScan binds, of `schema`'s columns.

        `row_number`, when given, names an extra Int64 column of row positions
        (see `_passthrough_join`). The Categorical columns named in `ids` bind
        as their Polars category ids, compact Int32, instead of a dictionary:
        only for columns nothing but a join reads, since Ibex sees integers.
        """
        if node.selection is not None:
            raise NotImplementedError("DataFrameScan selections are not supported yet")
        for name, dtype in schema.items():
            if dtype.base_type() not in _SUPPORTED_INPUT_TYPES:
                raise NotImplementedError(f"unsupported resident column {name}: {dtype}")
        # PyDataFrame.select projects column handles, without collecting a
        # lazy query or evaluating user expressions in the Polars engine.
        if schema:
            data = pl.DataFrame._from_pydf(node.df.select(list(schema)))
            # Categorical columns go in as one dictionary whose entries
            # remember their Polars ids, which is also far cheaper than
            # Polars' chunk-by-chunk Arrow export and Ibex's merge of it.
            shared = [name for name in schema if name not in ids and _categories.eligible(data.get_column(name))]
            rest = data.drop([*shared, *ids])
            table = rest.to_arrow() if rest.width else None
            for name in ids:
                column = pa.chunked_array([_categories.ids_as_int32(data.get_column(name))])
                table = pa.table({name: column}) if table is None else table.append_column(name, column)
            for name in shared:
                array, known = _categories.unify(data.get_column(name))
                self.categories.append(known)
                column = pa.chunked_array([array])
                table = pa.table({name: column}) if table is None else table.append_column(name, column)
            table = table.select(list(schema))
        else:
            # Projection pushdown can remove every column from a len-only
            # scan. Arrow/native tables need a column to retain its height.
            height = pl.DataFrame._from_pydf(node.df).height
            table = pa.table({"_ibex_height": pa.nulls(height, type=pa.int64())})
        for index, field in enumerate(table.schema):
            if pa.types.is_timestamp(field.type) or pa.types.is_duration(field.type):
                table = table.set_column(index, field.name, prepare_column(table.column(index)))
            elif field.type in _WIDENED_NUMERIC_TYPES and field.type != pa.int32():
                table = table.set_column(index, field.name, table.column(index).cast(_WIDENED_NUMERIC_TYPES[field.type]))
        if row_number is not None:
            height = pl.DataFrame._from_pydf(node.df).height
            table = table.append_column(row_number, pa.array(np.arange(height, dtype=np.int64)))
        return table

    def _passthrough_join(self, node_id, node, schema):
        """A root equality join whose left input is a resident frame, with the
        left columns rebuilt in Polars from row numbers; None when it does not apply.

        The output's left columns are rows of the left frame. The native join
        carries the left keys and one Int64 row number instead, and the result
        takes those rows from the original Polars frame: a filter when the
        row numbers strictly increase (each left row at most once, in order,
        as Ibex's probe emits them), a gather otherwise. That skips exporting
        and re-importing them, and above all the `cat.to` that rebuilding a
        Categorical from Ibex's codes costs, which checks every id against the
        category mapping and dominates a join with millions of output rows of
        a high-cardinality key. The join must be the plan's root, so no later
        step reads the columns.
        """
        how, nulls_equal, slice_, suffix, coalesce, maintain_order = node.options
        if (node_id != self.root or not isinstance(how, str) or how.lower() not in ("inner", "left")
                or slice_ is not None):
            return None
        self.visitor.set_node(node.input_left)
        scan = self.visitor.view_current_node()
        if type(scan).__name__ != "DataFrameScan" or node.input_left in self.nodes or scan.selection is not None:
            return None
        left_columns = dict(self.visitor.get_schema())
        left_on = []
        for named in node.left_on:
            expr = self.expression(named.node)
            if expr._op != "col":
                return None
            left_on.append(expr._name)
        self.visitor.set_node(node.input_right)
        right_columns = tuple(self.visitor.get_schema())
        right_on = []
        for named in node.right_on:
            expr = self.expression(named.node)
            if expr._op != "col":
                return None
            right_on.append(expr._name)
        # Polars' output: every left column, then the right ones (keys only
        # when not coalesced), names the left already has suffixed.
        def layout(columns):
            return list(left_columns) + [name + suffix if name in left_columns else name for name in columns]
        for right_out in ([name for name in right_columns if name not in right_on], list(right_columns)):
            if list(schema) == layout(right_out):
                break
        else:
            return None
        if not any(dtype.base_type() == pl.Categorical for dtype in left_columns.values()):
            return None
        row = "_ibex_join_row"
        while row in left_columns or row in schema:
            row += "_"
        native_left = {name: left_columns[name] for name in dict.fromkeys(left_on)}
        left = ibex.LazyFrame(("table", self._resident_table(scan, native_left, row_number=row)))
        right = self._payload_ids_right(node.input_right, right_on) or self.frame(node.input_right)
        source = pl.DataFrame._from_pydf(scan.df.select(list(left_columns)))
        self.left_rows = (row, source)
        self._check_join_validation()
        return left.join(right, left_on=left_on, right_on=right_on, how=how.lower(),
                         suffix=suffix, nulls_equal=nulls_equal, coalesce=coalesce,
                         maintain_order=maintain_order,
                         _left_columns=tuple(native_left) + (row,), _right_columns=right_columns)

    def _payload_ids_right(self, node_id, right_on):
        """A root join's resident right input with its payload Categoricals as ids, or None.

        Only the join reads that input, and it reads only the keys, so a
        non-key Categorical never needs its labels in Ibex: it binds as its
        Polars ids and comes back as them, needing `cat.to` but no lookup of
        Ibex codes. Compact Int32 storage keeps the ids unwidened.
        """
        if node_id in self.nodes:
            return None
        self.visitor.set_node(node_id)
        scan = self.visitor.view_current_node()
        if type(scan).__name__ != "DataFrameScan" or scan.selection is not None:
            return None
        schema = dict(self.visitor.get_schema())
        data = pl.DataFrame._from_pydf(scan.df.select(list(schema)))
        ids = tuple(name for name, dtype in schema.items()
                    if name not in right_on and dtype.base_type() == pl.Categorical
                    and _categories.ids_fit_int32(data.get_column(name)))
        if not ids:
            return None
        frame = ibex.LazyFrame(("table", self._resident_table(scan, schema, ids=ids)))
        self.nodes[node_id] = frame
        return frame

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
        if kind == "MapFunction" and node.function and node.function[0] == "explode":
            frame = self._exploded_group_head(node)
        elif kind == "MapFunction" and len(node.function) == 2 and node.function[0] == "hint_sorted":
            # A sortedness assertion leaves rows unchanged. Ignore the claim
            # until native metadata propagation is implemented; existing native
            # ordering proofs and general algorithms remain available.
            frame = self.frame(node.input)
            self.frame_length_columns[node_id] = self.frame_length_columns.get(node.input, set())
        elif kind == "Union":
            first, *others = node.inputs
            frame = self.frame(first)._append("union", tuple(self.frame(child) for child in others))
            if node.slice is not None:
                frame = frame.slice(*node.slice)
        elif kind == "Cache":
            # Cache nodes can have different visitor node IDs while sharing
            # one Polars cache identity. Preserve that identity until lowering.
            frame = self.frame(node.input)._append("cache", node.id_)
            self.frame_length_columns[node_id] = self.frame_length_columns.get(node.input, set())
        elif kind == "DataFrameScan":
            frame = ibex.LazyFrame(("table", self._resident_table(node, schema)))
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
            for name in ("row_count", "row_index", "include_file_paths", "hive_options",
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
            # Polars 2.1 adds `decryption_properties`; Ibex reads no encrypted
            # files, so only its unset value is accepted.
            if scan_options != {"schema": None, "parallel": "Auto", "low_memory": False, "use_statistics": True,
                                "decryption_properties": None}:
                raise NotImplementedError("unsupported Parquet scan configuration")
            cloud = json.loads(node.scan_type[2])
            if cloud.get("config") is not None or cloud.get("credential_provider") is not None:
                raise NotImplementedError("cloud scan configuration is not supported")
            frame = ibex.scan_parquet(node.paths[0])
            if options.n_rows is not None:
                # head/slice/tail pushed into the scan: (offset, length), with
                # a negative offset counting from the end. It precedes the
                # predicate, as an explicit scan_parquet(n_rows=...) does.
                offset, length = options.n_rows
                frame = frame.slice(offset, length)
            if node.predicate is not None:
                frame = frame.filter(self.expression(node.predicate.node))
            frame = frame.select(list(schema))
        elif kind == "Join" and getattr(node, "predicate", None) is not None:
            # Polars 2.1 exposes a join's fused filter (a cross join with a
            # filter, or an extra predicate on an inequality join); none of
            # the paths below would apply it.
            raise NotImplementedError("join with a fused predicate")
        elif kind == "Join" and isinstance(node.options[0], tuple) and node.options[0][0] == "IEJoin":
            how, nulls_equal, slice_, suffix, coalesce, maintain_order = node.options
            operators = {"Lt": "<", "LtEq": "<=", "Gt": ">", "GtEq": ">="}
            if len(how) != 3 or nulls_equal or coalesce or maintain_order != "none":
                raise NotImplementedError(f"unsupported inequality join options {node.options}")
            comparisons = [op for op in how[1:] if op is not None]
            if len(node.left_on) != len(comparisons) or len(node.right_on) != len(comparisons):
                raise NotImplementedError("unsupported inequality join key layout")
            left, right = self.frame(node.input_left), self.frame(node.input_right)
            if not (_at_most_one_row(left) or _at_most_one_row(right)):
                raise NotImplementedError("inequality joins require an input with at most one row")
            self.visitor.set_node(node.input_left)
            left_columns = tuple(self.visitor.get_schema())
            left_keys = [self.expression(key.node) for key in node.left_on]
            left_types = [self.visitor.get_dtype(key.node) for key in node.left_on]
            self.visitor.set_node(node.input_right)
            right_columns = tuple(self.visitor.get_schema())
            right_keys = [self.expression(key.node) for key in node.right_on]
            right_types = [self.visitor.get_dtype(key.node) for key in node.right_on]
            if any(dtype not in (*_INTEGER_LAYOUTS, pl.Float32, pl.Float64) for dtype in left_types + right_types):
                raise NotImplementedError("inequality join keys currently require numeric operands")
            rename = {name: name + suffix if name in left_columns else name for name in right_columns}
            def rename_right(expr):
                if expr._op == "col":
                    return ibex.col(rename[expr._args[0]])
                return Expr(expr._op, tuple(rename_right(arg) if isinstance(arg, Expr) else arg for arg in expr._args))
            predicates = []
            for op, lhs, rhs in zip(comparisons, left_keys, right_keys):
                operator = operators.get(str(op).split(".")[-1])
                if operator is None or lhs._aggregate or rhs._aggregate:
                    raise NotImplementedError("unsupported inequality join comparison")
                predicates.append(Expr("polars_" + operator, (lhs, rename_right(rhs))))
            self._check_join_validation()
            frame = left.join(right, how="cross", suffix=suffix,
                              _left_columns=left_columns, _right_columns=right_columns).filter(*predicates)
            if slice_ is not None:
                frame = frame.slice(*slice_)
        elif kind == "Join" and (passed := self._passthrough_join(node_id, node, schema)) is not None:
            frame = passed
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
                predicate = self.expression(node.predicate.node)
                if self.has_windows:
                    expanded, (predicate,) = self._window_fields(frame, [predicate], schema)
                    if expanded is not frame:
                        frame = expanded.filter(predicate).select(list(schema))
                        self.nodes[node_id] = frame
                        return frame
                pushed = self._filter_below_prefix_slice(node.input, predicate)
                frame = pushed if pushed is not None else frame.filter(predicate)
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
                    frame, fields = self._window_fields(frame, [self.named(expr) for expr in node.expr], schema)
                    frame = self._aggregate_projection(frame, fields)
            elif kind == "HStack":
                expressions = tuple(self.named(expr) for expr in node.exprs)
                frame, expressions = self._window_fields(frame, expressions, schema)
                frame = self._hstack(frame, expressions, list(schema), node_id)
            elif kind == "GroupBy":
                if node.apply or any(getattr(node.options, name) is not None for name in ("dynamic", "rolling")):
                    raise NotImplementedError("group UDFs and dynamic/rolling groups are unsupported")
                keys = []
                for key in node.keys:
                    expr = self.expression(key.node)
                    if expr._op != "col" or expr._name != key.output_name:
                        raise NotImplementedError("computed or renamed grouping keys are not supported yet")
                    # A float key goes through as-is: native grouping puts every
                    # NaN in one group and -0.0 with 0.0, as Polars does, and
                    # each group keeps its first row's key value.
                    keys.append(expr._name)
                frame = self._aggregate_projection(
                    frame, [self.named(expr) for expr in node.aggs],
                    keys=keys, maintain_order=node.maintain_order)
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
        elif kind == "Sink":
            # The sink's file options are not exposed to Python by the plan
            # visitor; IbexEngine.sink_parquet receives them as arguments.
            raise NotImplementedError("lazy sinks are not supported: call "
                                      "lf.sink_parquet(path, engine=IbexEngine()) without lazy=True")
        else:
            raise NotImplementedError(f"unsupported Polars plan node {kind} (node {node_id})")
        self.nodes[node_id] = frame
        return frame

    def compile(self):
        root = self.visitor.get_node()
        self.root = root
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
            steps = []
            for operation, args in current._steps:
                if operation == "join":
                    args = (prepare_inputs(args[0]), *args[1:])
                elif operation == "union":
                    args = tuple(prepare_inputs(branch) for branch in args)
                steps.append((operation, args))
            return ibex.LazyFrame(source, tuple(steps))
        frame = prepare_inputs(frame)
        result_schema = {}
        source, bindings, scalars = lower(frame, validate=True, result_schema=result_schema, widen_numeric_inputs=True)
        return source, bindings, scalars, expected, result_schema, self.categories, self.left_rows
