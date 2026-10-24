# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Render one complete pipeline; captured values never become source text."""

import pyarrow as pa

from ._expr import Expr, _identifier


_WIDENED_NUMERIC_TYPES = {
    pa.int32(): pa.int64(), pa.float32(): pa.float64(),
}


def lower(frame, *, validate=False, result_schema=None, widen_numeric_inputs=False):
    scalars = {}
    tables = {}

    step_types, schemas = _validate_types(frame, widen_numeric_inputs=widen_numeric_inputs) if validate else ([], [])
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
        if op == "cast":
            names = {pa.float64(): "Float64", pa.int64(): "Int64"}
            return f"{names[args[1]]}({render(args[0])})"
        if op == "is_null":
            return f"is_null({render(args[0])})"
        if op == "wrap_integer":
            # Normalize a signed remainder to the logical integer range. Reduce
            # first so even an Int64 aggregate result cannot overflow while
            # adding the offset. Nulls propagate through native arithmetic.
            value = render(args[0])
            bits, signed = args[1:]
            half = 2**(bits - 1) if signed else 0
            modulus, offset = bind(2**bits), bind(2**bits + half)
            normalized = f"((({value} % {modulus}) + {offset}) % {modulus})"
            return f"({normalized} - {bind(half)})" if signed else normalized
        if op.removeprefix("polars_") in ("==", "!=", "<", "<=", ">", ">="):
            # Both Python frontends follow Polars: NaN equals itself and
            # compares greater than every non-NaN number. Keep this adaptation
            # at the Python boundary; native Ibex comparisons stay unchanged.
            comparison = op.removeprefix("polars_")
            left, right = render(args[0]), render(args[1])
            if not any(current_types.get(id(arg)) == pa.float64() for arg in args):
                return f"({left} {comparison} {right})"
            nan = [f"is_nan({value})" if current_types.get(id(arg)) == pa.float64() else "false"
                   for arg, value in zip(args, (left, right))]
            if comparison in ("==", "!="):
                value = f"(({left} == {right}) || ({nan[0]} && {nan[1]}))"
                if comparison == "!=":
                    value = f"!({value})"
            else:
                a, b = nan if comparison in (">", ">=") else nan[::-1]
                extra = a if comparison in (">=", "<=") else f"({a} && !({b}))"
                value = f"(({left} {comparison} {right}) || {extra})"
            return f"case {{ is_null({left}) || is_null({right}) => NULL, else => {value} }}"
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
                if operation == "sort" and validate and schemas[index].get(name) == pa.float64():
                    # Polars orders NaN above infinity; Ibex's native numeric
                    # comparison uses IEEE unordered semantics. Stage a flag.
                    temporary = f"_ibex_sort_nan_{index}_{key_index}"
                    while temporary in occupied:
                        temporary += "_"
                    occupied.add(temporary)
                    updates.append(f"{_identifier(temporary)} = is_nan({_identifier(name)})")
                    order.append(f"{_identifier(temporary)} {'desc' if reverse else 'asc'}")
                order.append(f"{_identifier(name)} {'desc' if reverse else 'asc'}")
            if updates:
                source += f"[update {{ {', '.join(updates)} }}]"
            source += f"[order {{ {', '.join(order)} }}]"
        elif operation == "hstack":
            if len(expressions) == 1:
                expr = expressions[0]
                source += f"[update {{ {_identifier(expr._name)} = {render(expr)} }}]"
                continue
            # A Polars HStack reads all fields from the input schema. Stage
            # values under fresh names before assigning any output aliases.
            occupied = set(schemas[index]) if validate else {expr._name for expr in expressions}
            temporary_names = []
            for field_index, expr in enumerate(expressions):
                name = f"_ibex_hstack_{index}_{field_index}"
                while name in occupied:
                    name += "_"
                occupied.add(name)
                temporary_names.append(name)
            fields = ", ".join(f"{_identifier(name)} = {render(expr)}" for name, expr in zip(temporary_names, expressions))
            assignments = ", ".join(f"{_identifier(expr._name)} = {_identifier(name)}" for name, expr in zip(temporary_names, expressions))
            source += f"[update {{ {fields} }}][update {{ {assignments} }}]"
        else:
            # Native select assignments are sequential. Python projections
            # read the input schema, so stage expressions if an earlier alias
            # would shadow a column used by a later expression.
            def references(expr):
                if expr._op == "col":
                    return {expr._args[0]}
                return set().union(*(references(arg) for arg in expr._args if isinstance(arg, Expr)))

            inputs = [references(expr) for expr in expressions]
            assigned = set()
            shadowed = False
            for expr, names in zip(expressions, inputs):
                shadowed |= bool(assigned & names)
                assigned.add(expr._name)
            if operation == "select" and shadowed and not any(expr._aggregate for expr in expressions):
                occupied = assigned | set().union(*inputs)
                temporary_names = []
                for field_index in range(len(expressions)):
                    name = f"_ibex_select_{index}_{field_index}"
                    while name in occupied:
                        name += "_"
                    occupied.add(name)
                    temporary_names.append(name)
                fields = ", ".join(f"{_identifier(name)} = {render(expr)}" for name, expr in zip(temporary_names, expressions))
                source += f"[select {{ {fields} }}]"
                fields = ", ".join(f"{_identifier(expr._name)} = {_identifier(name)}" for name, expr in zip(temporary_names, expressions))
                source += f"[select {{ {fields} }}]"
                continue
            fields = ", ".join(f"{_identifier(expr._name)} = {render(expr)}" for expr in expressions)
            source += f"[{operation} {{ {fields} }}]"
    return prefix + source + ";", tables, scalars


def _validate_types(frame, *, widen_numeric_inputs=False):
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
    columns = {name: pa.large_string() if dtype == pa.string() or pa.types.is_dictionary(dtype) else dtype
               for name, dtype in zip(schema.names, schema.types)}
    if widen_numeric_inputs:
        # The Polars adapter retains logical types in its visitor/output
        # schema. Native Parquet decoding already widens narrow numeric types.
        columns = {name: _WIDENED_NUMERIC_TYPES.get(dtype, dtype) for name, dtype in columns.items()}
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
        if op == "cast":
            original = dtype(args[0])
            target = args[1]
            if target != pa.float64() or original not in numeric:
                raise NotImplementedError("only numeric casts to Float64 are supported by this adapter")
            return target
        if op == "is_null":
            dtype(args[0])
            return pa.bool_()
        if op == "wrap_integer":
            if dtype(args[0]) != pa.int64():
                raise TypeError("integer wrapping requires an integral native expression")
            return pa.int64()
        if op == "!":
            if dtype(args[0]) != pa.bool_():
                raise TypeError("logical negation requires a Boolean expression")
            return pa.bool_()
        left, right = dtype(args[0]), dtype(args[1])
        if op in ("&&", "||"):
            if left != pa.bool_() or right != pa.bool_():
                raise TypeError("logical operators require Boolean expressions")
            return pa.bool_()
        if op.removeprefix("polars_") in ("==", "!=", "<", "<=", ">", ">="):
            if left != right and not (left in numeric and right in numeric):
                raise NotImplementedError("comparison between these types is not supported yet")
            return pa.bool_()
        if left not in numeric or right not in numeric:
            raise NotImplementedError("arithmetic currently requires Int64 or Float64 expressions")
        return pa.float64() if pa.float64() in (left, right) or op == "/" else pa.int64()

    for operation, expressions in frame._steps:
        type_map = {}
        keys, key_types = (), []
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
