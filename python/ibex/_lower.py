# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Render one complete pipeline; captured values never become source text."""

import pyarrow as pa

from ._expr import Expr, _identifier
from ._join import join_layout


_WIDENED_NUMERIC_TYPES = {
    pa.int8(): pa.int64(), pa.int16(): pa.int64(), pa.int32(): pa.int64(),
    pa.uint8(): pa.int64(), pa.uint16(): pa.int64(), pa.float32(): pa.float64(),
}


def lower(frame, *, validate=False, result_schema=None, widen_numeric_inputs=False,
          _state=None, _namespace="", _return_parts=False):
    state = _state if _state is not None else {"scalars": {}, "tables": {}, "imports": set(), "next_join": 0}
    scalars, tables = state["scalars"], state["tables"]
    def internal(name):
        return f"_ibex_{_namespace}{name}"

    needs_schema = validate or any(operation in ("unique", "slice", "python_slice", "join") for operation, _ in frame._steps)
    step_types, schemas = _validate_types(frame, widen_numeric_inputs=widen_numeric_inputs) if needs_schema else ([], [])
    if result_schema is not None:
        result_schema.update(schemas[-1])
    current_types = {}
    broadcast_length = None

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
        if op == "broadcast_length":
            return "^" + broadcast_length
        if op == "repeat":
            # A captured scalar is broadcast by the native projection/update
            # operator. Length equality was proved by the Polars adapter.
            return render(args[0])
        if op == "null":
            return "NULL"
        if op == "case":
            predicate, truthy, falsy = args
            left, right = render(truthy), render(falsy)
            if current_types.get(id(expr)) == pa.float64():
                left, right = f"Float64({left})", f"Float64({right})"
            return f"case {{ {render(predicate)} => {left}, else => {right} }}"
        if op in ("repeat_null", "typed_null"):
            # The unreachable placeholder supplies the null's physical type.
            return f"case {{ true => NULL, else => {render(args[0])} }}"
        if op == "sum":
            # Polars sum returns zero for empty/all-null inputs; Ibex's
            # native nullable sum returns null. Preserve the frontend contract.
            zero = 0.0 if current_types.get(id(expr)) == pa.float64() else 0
            value = render(args[0])
            if current_types.get(id(args[0])) == pa.bool_():
                value = f"Int64({value})"
            return f"coalesce(sum({value}), {bind(zero)})"
        if op == "count":
            if args[0]._op == "col":
                return f"count({render(args[0])})"
            # Native count accepts column names; computed inputs use the same
            # non-null indicator reduction, including zero for empty input.
            return f"coalesce(sum(Int64(!is_null({render(args[0])}))), {bind(0)})"
        if op == "mean":
            return f"mean({render(args[0])})"
        if op in ("first", "last", "first_non_null", "last_non_null"):
            native = op.removesuffix("_non_null") if op.endswith("_non_null") else op + "_null"
            return f"{native}({render(args[0])})"
        if op in ("min", "max"):
            return f"{op}({render(args[0])})"
        if op in ("len", "column_len"):
            # Supported column-length operands are row expressions, so their
            # length equals the frame's height, independently of null values.
            return "count()"
        if op == "cast":
            names = {pa.float64(): "Float64", pa.int64(): "Int64"}
            return f"{names[args[1]]}({render(args[0])})"
        if op == "like":
            return f"like({render(args[0])}, {render(args[1])})"
        if op == "year":
            return f"year({render(args[0])})"
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
            # Internal bridge calls lower to a comparison IR policy, preserving
            # Polars' NaN ordering without expanding into scalar case expressions.
            comparison = op.removeprefix("polars_")
            left, right = render(args[0]), render(args[1])
            if not any(current_types.get(id(arg)) == pa.float64() for arg in args):
                return f"({left} {comparison} {right})"
            name = {"==": "eq", "!=": "ne", "<": "lt", "<=": "le", ">": "gt", ">=": "ge"}[comparison]
            return f"__ibex_nan_high_{name}({left}, {right})"
        if op == "!":
            return f"!({render(args[0])})"
        if op not in ("+", "-", "*", "/", "==", "!=", "<", "<=", ">", ">=", "&&", "||"):
            raise NotImplementedError(f"unsupported expression: {op}")
        return f"({render(args[0])} {op} {render(args[1])})"

    kind, value = frame._source
    if kind == "parquet":
        prefix = '' if "parquet" in state["imports"] else 'extern fn parquet::read(path: String) -> DataFrame from "parquet.hpp";\n'
        state["imports"].add("parquet")
        source = "parquet::read(" + bind(value) + ")"
    elif kind == "bound":
        prefix = ""
        tables[internal("input")] = value.capsule
        source = internal("input")
    elif kind == "table":
        prefix = ""
        # Empty Tables yield no batches in the existing bridge. A typed empty
        # RecordBatch retains schema instead of falling back to list inference.
        tables[internal("input")] = value if value.num_rows else pa.RecordBatch.from_arrays(
            [column.combine_chunks() for column in value.columns], schema=value.schema
        )
        source = internal("input")
    else:
        raise NotImplementedError(f"unsupported source: {kind}")
    for index, (operation, expressions) in enumerate(frame._steps):
        current_types = step_types[index] if validate else {}
        if operation == "filter":
            predicate = " && ".join(f"({render(expr)})" for expr in expressions)
            source += "[filter " + predicate + "]"
        elif operation == "join":
            other, left_on, right_on, how, suffix, nulls_equal, coalesce, maintain_order, left_visible, right_visible = expressions
            join_id = state["next_join"]
            state["next_join"] += 1
            (right_prefix, right_source), _, _ = lower(other, validate=validate, widen_numeric_inputs=widen_numeric_inputs,
                _state=state, _namespace=f"join_{join_id}_", _return_parts=True)
            prefix += right_prefix
            right_columns = _validate_types(other, widen_numeric_inputs=widen_numeric_inputs)[1][-1]
            # The current output schema is computed below, so recover the left
            # schema from the preceding step (or the join's input frame).
            before = frame.__class__(frame._source, frame._steps[:index])
            left_columns = _validate_types(before, widen_numeric_inputs=widen_numeric_inputs)[1][-1]
            occupied = set(left_columns) | set(right_columns)
            def fresh(role):
                name = f"_ibex_join_{join_id}_{role}"
                while name in occupied:
                    name += "_"
                occupied.add(name)
                return name
            left_names = {name: fresh(f"l_{i}") for i, name in enumerate(left_columns)}
            right_names = {name: fresh(f"r_{i}") for i, name in enumerate(right_columns)}
            left_fields = [f"{_identifier(alias)} = {_identifier(name)}" for name, alias in left_names.items()]
            right_fields = [f"{_identifier(alias)} = {_identifier(name)}" for name, alias in right_names.items()]
            left_row, right_row = fresh("left_row"), fresh("right_row")
            if maintain_order != "none":
                left_fields.append(f"{_identifier(left_row)} = seq()")
                if how not in ("semi", "anti"):
                    right_fields.append(f"{_identifier(right_row)} = seq()")
            left_source = source + f"[select {{ {', '.join(left_fields)} }}]"
            right_source += f"[select {{ {', '.join(right_fields)} }}]"
            mapped, left_updates, right_updates = [], [], []
            for key_index, (left_key, right_key) in enumerate(zip(left_on, right_on)):
                lkey, rkey = left_names[left_key], right_names[right_key]
                if left_columns[left_key] == pa.float64():
                    ln, rn = fresh(f"ln_{key_index}"), fresh(f"rn_{key_index}")
                    lv, rv = fresh(f"lv_{key_index}"), fresh(f"rv_{key_index}")
                    for updates, key, flag, value in ((left_updates, lkey, ln, lv), (right_updates, rkey, rn, rv)):
                        updates += [f"{_identifier(flag)} = is_nan({_identifier(key)})",
                                    f"{_identifier(value)} = case {{ is_nan({_identifier(key)}) => 0.0, else => {_identifier(key)} }}"]
                    mapped += [(ln, rn), (lv, rv)]
                else:
                    mapped.append((lkey, rkey))
            if left_updates:
                left_source += f"[update {{ {', '.join(left_updates)} }}]"
                right_source += f"[update {{ {', '.join(right_updates)} }}]"
            native_kind = {"inner": "", "full": "outer ", "cross": "cross "}.get(how, how + " ")
            source = f"(({left_source}) {native_kind}join ({right_source})"
            if mapped:
                source += " on { " + ", ".join(f"{_identifier(l)} = {_identifier(r)}" for l, r in mapped) + " }"
                if nulls_equal:
                    source += " nulls equal"
            source += ")"
            if maintain_order != "none":
                order = [left_row] if how in ("semi", "anti") else (
                    [right_row, left_row] if maintain_order in ("right", "right_left") else [left_row, right_row])
                source += "[order { " + ", ".join(_identifier(name) + " asc" for name in order) + " }]"
            visible_left = left_columns if left_visible is None else {name: left_columns[name] for name in left_visible}
            visible_right = right_columns if right_visible is None else {name: right_columns[name] for name in right_visible}
            fields = []
            for output, side, name, paired in join_layout(visible_left, visible_right, left_on, right_on, how, coalesce, suffix):
                value = _identifier(right_names[name]) if side == "right" else _identifier(left_names[name])
                if side == "coalesce":
                    value = f"coalesce({value}, {_identifier(right_names[paired])})"
                fields.append(f"{_identifier(output)} = {value}")
            if not fields:
                fields.append("_ibex_join_height = 0")
            source += f"[select {{ {', '.join(fields)} }}]"
        elif operation == "aggregate":
            keys, aggregates = expressions
            # Keep the complete aggregate in one native operator. The lowerer
            # places scalar coalesce corrections after reduction, not per row.
            fields = ", ".join(_identifier(name) for name in keys)
            fields += ", " + ", ".join(f"{_identifier(expr._name)} = {render(expr)}" for expr in aggregates)
            source += f"[select {{ {fields} }}, by {{ {', '.join(_identifier(name) for name in keys)} }}]"
        elif operation in ("slice", "python_slice"):
            columns = schemas[index]
            row = f"_ibex_slice_row_{index}"
            while row in columns:
                row += "_"
            row_ref = _identifier(row)
            int_max = 2**63 - 1
            def bounded(value):
                return min(int_max, max(-2**63, value))
            if operation == "slice":
                offset, length = expressions
                needs_height = offset < 0
            else:
                start, stop, step = expressions
                needs_height = step < 0 or any(value is not None and value < 0 for value in (start, stop))
            if needs_height:
                shared, height = internal(f"slice_input_{index}"), internal(f"slice_height_{index}")
                prefix += f"let {shared} = {source};\n"
                prefix += f"let {height} = scalar({shared}[select {{ _ibex_slice_count = count() }}]);\n"
                source = shared
                height_ref = "^" + height
            def positive_bound(value):
                if value >= 0:
                    return str(bounded(value))
                candidate = f"({height_ref} + ({bind(bounded(value))}))"
                return f"(case {{ {candidate} < 0 => 0, else => {candidate} }})"
            if operation == "slice":
                begin = positive_bound(offset)
                predicates = [f"{row_ref} >= {begin}"]
                if length is not None:
                    # Polars clips the requested interval, including any part
                    # before row zero. Compute offset+length in Python first,
                    # so extreme bounds cannot overflow native Int64 arithmetic.
                    end = offset + length
                    if offset >= 0:
                        predicates.append(f"{row_ref} < {min(end, int_max)}")
                    elif end < 0:
                        predicates.append(f"{row_ref} < {positive_bound(end)}")
                reverse = False
            elif step > 0:
                begin = positive_bound(start or 0)
                predicates = [f"{row_ref} >= {begin}"]
                if stop is not None:
                    predicates.append(f"{row_ref} < {positive_bound(stop)}")
                predicates.append(f"({row_ref} - {begin}) % {min(step, int_max)} == 0")
                reverse = False
            else:
                def negative_bound(value, default):
                    if value is None:
                        return default
                    if value >= 0:
                        candidate = str(bounded(value))
                        return f"(case {{ {candidate} >= {height_ref} => {height_ref} - 1, else => {candidate} }})"
                    candidate = f"({height_ref} + ({bind(bounded(value))}))"
                    return f"(case {{ {candidate} < -1 => -1, else => {candidate} }})"
                begin = negative_bound(start, f"({height_ref} - 1)")
                end = negative_bound(stop, "(-1)")
                predicates = [f"{row_ref} <= {begin}", f"{row_ref} > {end}",
                              f"({begin} - {row_ref}) % {min(-step, int_max)} == 0"]
                reverse = True
            source += f"[update {{ {row_ref} = seq() }}][filter {' && '.join(predicates)}]"
            if reverse:
                source += f"[order {{ {row_ref} desc }}]"
            source += f"[select {{ {', '.join(_identifier(name) for name in columns)} }}]"
        elif operation == "unique":
            subset, keep, maintain_order = expressions
            columns = schemas[index]
            keys = tuple(columns) if subset is None else subset
            fields = ", ".join(_identifier(name) for name in columns)
            if set(keys) == set(columns) and keep in ("any", "first") and not any(
                columns[name] == pa.float64() for name in keys
            ):
                source += f"[distinct {{ {fields} }}]"
                continue
            occupied = set(columns)
            def temporary(role):
                name = f"_ibex_unique_{index}_{role}"
                while name in occupied:
                    name += "_"
                occupied.add(name)
                return name
            row = temporary("row")
            updates = [f"{_identifier(row)} = seq()"]
            group_keys = []
            for key_index, name in enumerate(keys):
                key = temporary(f"key_{key_index}")
                value = _identifier(name)
                if columns[name] == pa.float64():
                    # Ibex keeps NaN keys distinct; Polars groups all NaNs.
                    # The flag separates NaN from a genuine zero (and null).
                    nan = temporary(f"nan_{key_index}")
                    updates.append(f"{_identifier(nan)} = is_nan({value})")
                    group_keys.append(nan)
                    value = f"case {{ is_nan({value}) => 0.0, else => {value} }}"
                updates.append(f"{_identifier(key)} = {value}")
                group_keys.append(key)
            ordinal, count = temporary("ordinal"), temporary("count")
            edge = "last_null" if keep == "last" else "first_null"
            aggregates = [_identifier(name) for name in group_keys]
            aggregates += [f"{_identifier(name)} = {edge}({_identifier(name)})" for name in columns]
            position = "max" if keep == "last" else "min"
            aggregates += [f"{_identifier(ordinal)} = {position}({_identifier(row)})",
                           f"{_identifier(count)} = count()"]
            source += f"[update {{ {', '.join(updates)} }}]"
            source += f"[select {{ {', '.join(aggregates)} }}, by {{ {', '.join(_identifier(name) for name in group_keys)} }}]"
            if keep == "none":
                source += f"[filter {_identifier(count)} == 1]"
            # Stable order is permitted when maintain_order=False too. For
            # keep=last, order by the selected row's original position.
            source += f"[order {{ {_identifier(ordinal)} asc }}][select {{ {fields} }}]"
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
            if any(expr._args[0]._op == "broadcast_length" for expr in expressions):
                # Share the input before extracting its height. The native
                # scalar binding then broadcasts in the same pipeline call.
                shared = internal(f"repeat_input_{index}")
                broadcast_length = internal(f"repeat_length_{index}")
                prefix += f"let {shared} = {source};\n"
                prefix += f"let {broadcast_length} = scalar({shared}[select {{ _ibex_repeat_count = count() }}]);\n"
                source = shared
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
    return ((prefix, source) if _return_parts else prefix + source + ";"), tables, scalars


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
            return pa.large_string() if isinstance(args[0], str) else pa.scalar(args[0]).type
        if op == "alias":
            return dtype(args[0])
        if op == "null":
            return pa.null()
        if op == "case":
            if dtype(args[0]) != pa.bool_():
                raise TypeError("conditional predicate must be Boolean")
            left, right = dtype(args[1]), dtype(args[2])
            if left == right:
                return left
            if pa.types.is_null(left):
                return right
            if pa.types.is_null(right):
                return left
            if left in numeric and right in numeric:
                return pa.float64()
            raise NotImplementedError("conditional branches must have compatible types")
        if op in ("repeat", "repeat_null", "broadcast_length", "typed_null"):
            return dtype(args[0])
        if op in ("first", "last", "first_non_null", "last_non_null"):
            result = dtype(args[0])
            if not args[0]._row_reference:
                raise NotImplementedError(f"{op} currently requires a row expression")
            if result == pa.date32() and op.endswith("_non_null"):
                raise NotImplementedError("null-skipping Date edge reductions are unsupported by Ibex")
            return result
        if op in ("sum", "mean", "min", "max"):
            result = dtype(args[0])
            if op == "sum" and result == pa.bool_() and args[0]._row_reference:
                return pa.uint32()
            if result not in numeric or not args[0]._row_reference:
                raise NotImplementedError(f"{op} currently requires an Int64 or Float64 row expression")
            return pa.float64() if op == "mean" else result
        if op in ("count", "column_len"):
            dtype(args[0])
            if not args[0]._row_reference:
                raise NotImplementedError(f"{op} currently requires a row expression")
            return pa.uint32()
        if op == "len":
            return pa.uint32()
        if op == "cast":
            original = dtype(args[0])
            target = args[1]
            if target != pa.float64() or original not in numeric:
                raise NotImplementedError("only numeric casts to Float64 are supported by this adapter")
            return target
        if op == "like":
            if any(dtype(arg) not in (pa.string(), pa.large_string()) for arg in args):
                raise TypeError("string matching requires string operands")
            return pa.bool_()
        if op == "year":
            if dtype(args[0]) != pa.date32():
                raise NotImplementedError("year extraction currently requires Date input")
            return pa.int64()
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
        if operation == "join":
            other, left_on, right_on, how, suffix, nulls_equal, coalesce, maintain_order, left_visible, right_visible = expressions
            right = _validate_types(other, widen_numeric_inputs=widen_numeric_inputs)[1][-1]
            for name in columns:
                dtype(Expr("col", (name,)))
            for name in left_on:
                dtype(Expr("col", (name,)))
            if any(dtype_ not in supported for dtype_ in right.values()):
                raise NotImplementedError("unsupported right join column type")
            for name in right_on:
                if name not in right:
                    raise ValueError(f"right join key not found: {name}")
            for left_key, right_key in zip(left_on, right_on):
                if columns[left_key] != right[right_key]:
                    raise NotImplementedError("join key types must have the same native representation")
            visible_left = columns if left_visible is None else {name: columns[name] for name in left_visible}
            visible_right = right if right_visible is None else {name: right[name] for name in right_visible}
            layout = join_layout(visible_left, visible_right, left_on, right_on, how, coalesce, suffix)
            columns = {output: (right[name] if side == "right" else columns[name]) for output, side, name, _ in layout}
            if not columns:
                columns = {"_ibex_join_height": pa.int64()}
            step_types.append(type_map)
            schemas.append(dict(columns))
            continue
        if operation == "aggregate":
            keys, expressions = expressions
            key_types = [dtype(Expr("col", (name,))) for name in keys]
        elif operation in ("slice", "python_slice"):
            step_types.append(type_map)
            schemas.append(dict(columns))
            continue
        elif operation == "unique":
            subset, keep, maintain_order = expressions
            for name in columns:
                dtype(Expr("col", (name,)))
            if subset is not None:
                for name in subset:
                    dtype(Expr("col", (name,)))
            step_types.append(type_map)
            schemas.append(dict(columns))
            continue
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
