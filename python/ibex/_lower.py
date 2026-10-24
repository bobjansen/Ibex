# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Render one complete pipeline; captured values never become source text."""

import pyarrow as pa

from ._expr import Expr, _identifier


def lower(frame, *, validate=False, result_schema=None):
    scalars = {}
    tables = {}

    step_types, schemas = _validate_types(frame) if validate else ([], [])
    if result_schema is not None:
        result_schema.update(schemas[-1])
    current_types = {}

    def bind(value):
        name = f"_ibex_scalar_{len(scalars)}"
        scalars[name] = value
        return "^" + name

    def render(expr):
        op, args = expr._op, expr._args
        if op == "col":
            return _identifier(args[0])
        if op == "lit":
            return bind(args[0])
        if op == "alias":
            return render(args[0])
        if op == "sum":
            # Polars sum returns zero for empty/all-null inputs; Ibex's
            # native nullable sum returns null. Preserve the frontend contract.
            zero = 0.0 if current_types.get(id(expr)) == pa.float64() else 0
            return f"coalesce(sum({render(args[0])}), {bind(zero)})"
        if op == "mean":
            return f"mean({render(args[0])})"
        if op == "len":
            return "count()"
        if op == "!":
            return f"!({render(args[0])})"
        if op not in ("+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">=", "&&", "||"):
            raise NotImplementedError(f"unsupported expression: {op}")
        return f"({render(args[0])} {op} {render(args[1])})"

    kind, value = frame._source
    if kind == "parquet":
        prefix = 'extern fn parquet::read(path: String) -> DataFrame from "parquet.hpp";\n'
        source = "parquet::read(" + bind(value) + ")"
    elif kind == "bound":
        prefix = ""
        tables["_ibex_input"] = value.capsule
        source = "_ibex_input"
    elif kind == "table":
        prefix = ""
        # Empty Tables yield no batches in the existing bridge. A typed empty
        # RecordBatch retains schema instead of falling back to list inference.
        tables["_ibex_input"] = value if value.num_rows else pa.RecordBatch.from_arrays(
            [column.combine_chunks() for column in value.columns], schema=value.schema
        )
        source = "_ibex_input"
    else:
        raise NotImplementedError(f"unsupported source: {kind}")
    for index, (operation, expressions) in enumerate(frame._steps):
        current_types = step_types[index] if validate else {}
        if operation == "filter":
            predicate = " && ".join(f"({render(expr)})" for expr in expressions)
            source += "[filter " + predicate + "]"
        elif operation == "aggregate":
            keys, aggregates = expressions
            # Keep the complete aggregate in one native operator. The lowerer
            # places scalar coalesce corrections after reduction, not per row.
            fields = ", ".join(_identifier(name) for name in keys)
            fields += ", " + ", ".join(f"{_identifier(expr._name)} = {render(expr)}" for expr in aggregates)
            source += f"[select {{ {fields} }}, by {{ {', '.join(_identifier(name) for name in keys)} }}]"
        elif operation == "sort":
            names, descending, nulls_last = expressions
            order = []
            updates = []
            occupied = set(schemas[index]) if validate else set(names)
            for key_index, (name, reverse, last) in enumerate(zip(names, descending, nulls_last)):
                if not last:
                    temporary = f"_ibex_sort_null_{index}_{key_index}"
                    while temporary in occupied:
                        temporary += "_"
                    occupied.add(temporary)
                    updates.append(f"{_identifier(temporary)} = is_null({_identifier(name)})")
                    order.append(f"{_identifier(temporary)} desc")
                order.append(f"{_identifier(name)} {'desc' if reverse else 'asc'}")
            if updates:
                source += f"[update {{ {', '.join(updates)} }}]"
            source += f"[order {{ {', '.join(order)} }}]"
        else:
            fields = ", ".join(f"{_identifier(expr._name)} = {render(expr)}" for expr in expressions)
            source += f"[{operation} {{ {fields} }}]"
    return prefix + source + ";", tables, scalars


def _validate_types(frame):
    """Check the prototype's supported types without decoding input rows."""
    kind, value = frame._source
    if kind == "parquet":
        import pyarrow.parquet as pq
        schema = pq.read_schema(value)
    elif kind == "bound":
        schema = value.table.schema
    else:
        schema = value.schema
    # The compatible frontend exposes strings as Arrow large_utf8, matching
    # the reference frame's Arrow boundary even for utf8 source buffers.
    columns = {name: pa.large_string() if dtype == pa.string() else dtype
               for name, dtype in zip(schema.names, schema.types)}
    numeric = (pa.int64(), pa.float64())
    supported = numeric + (pa.uint32(), pa.bool_(), pa.string(), pa.large_string(), pa.date32())

    type_map = {}
    step_types = []
    schemas = []

    def dtype(expr):
        result = infer(expr)
        type_map[id(expr)] = result
        return result

    def infer(expr):
        op, args = expr._op, expr._args
        if op == "col":
            if args[0] not in columns:
                raise ValueError(f"column not found: {args[0]}")
            result = columns[args[0]]
            if result not in supported:
                raise NotImplementedError(f"column type {result} is not supported yet")
            return result
        if op == "lit":
            return pa.scalar(args[0]).type
        if op == "alias":
            return dtype(args[0])
        if op in ("sum", "mean"):
            result = dtype(args[0])
            if result not in numeric or not args[0]._row_reference:
                raise NotImplementedError(f"{op} currently requires an Int64 or Float64 row expression")
            return pa.float64() if op == "mean" else result
        if op == "len":
            return pa.uint32()
        if op == "!":
            if dtype(args[0]) != pa.bool_():
                raise TypeError("logical negation requires a Boolean expression")
            return pa.bool_()
        left, right = dtype(args[0]), dtype(args[1])
        if op in ("&&", "||"):
            if left != pa.bool_() or right != pa.bool_():
                raise TypeError("logical operators require Boolean expressions")
            return pa.bool_()
        if op in ("==", "!=", "<", "<=", ">", ">="):
            if left != right and not (left in numeric and right in numeric):
                raise NotImplementedError("comparison between these types is not supported yet")
            return pa.bool_()
        if left not in numeric or right not in numeric:
            raise NotImplementedError("arithmetic currently requires Int64 or Float64 expressions")
        return pa.float64() if pa.float64() in (left, right) or op == "/" else pa.int64()

    for operation, expressions in frame._steps:
        type_map = {}
        if operation == "aggregate":
            keys, expressions = expressions
            key_types = [dtype(Expr("col", (name,))) for name in keys]
        elif operation == "sort":
            for name in expressions[0]:
                dtype(Expr("col", (name,)))
            step_types.append(type_map)
            schemas.append(dict(columns))
            continue
        types = [dtype(expr) for expr in expressions]
        step_types.append(type_map)
        if operation == "filter":
            if any(result != pa.bool_() for result in types):
                raise TypeError("filter requires Boolean predicates")
        elif operation == "aggregate":
            columns = dict(zip(keys, key_types)) | dict(zip((expr._name for expr in expressions), types))
        elif operation == "select":
            columns = dict(zip((expr._name for expr in expressions), types))
        else:
            columns.update(zip((expr._name for expr in expressions), types))
        schemas.append(dict(columns))
    if not schemas:
        schemas.append(dict(columns))
    return step_types, schemas
