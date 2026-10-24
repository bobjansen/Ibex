"""DuckDB's untimed answer exports without loading another DataFrame engine."""
import csv
import os
import pathlib

DIGESTS = []


def record(name, result):
    target, directory = os.environ.get("IBEX_DIGEST_OUT"), os.environ.get("IBEX_ANSWERS_DIR")
    if not (target or directory):
        return
    import pyarrow as pa
    import pyarrow.compute as pc
    if directory:
        columns = []
        for field, column in zip(result.schema, result.columns):
            dtype = field.type
            if pa.types.is_timestamp(dtype):
                column = pc.cast(pc.cast(column, pa.timestamp("ns", tz=dtype.tz)), pa.int64())
            elif pa.types.is_date32(dtype):
                column = pc.cast(column, pa.int32())
            elif pa.types.is_date64(dtype):
                column = pc.divide(pc.cast(column, pa.int64()), 86400000)
            elif pa.types.is_decimal(dtype):
                column = pc.cast(column, pa.int64() if dtype.scale == 0 else pa.float64())
            columns.append(column)
        normalized = pa.Table.from_arrays(columns, names=result.column_names)
        path = pathlib.Path(directory) / (name + ".csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        def cell(value):
            if value is None:
                return "n:"
            if isinstance(value, bool):
                return "b:" + str(int(value))
            if isinstance(value, int):
                return "i:" + str(value)
            if isinstance(value, float):
                return "f:" + repr(value)
            if isinstance(value, str):
                return "s:" + value
            raise TypeError(f"unsupported answer value {type(value).__name__}")
        with path.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(normalized.column_names)
            for row in normalized.to_pylist():
                writer.writerow([cell(row[col]) for col in normalized.column_names])
    if target:
        total = 0.
        # Logical timestamps/dates are normalized only for typed snapshots.
        # The shared screening digest excludes them, like Ibex and Polars.
        for column in result.columns:
            if pa.types.is_floating(column.type):
                missing = pc.is_nan(column)
                if pc.any(missing).as_py():
                    column = pc.if_else(missing, None, column)
            elif pa.types.is_integer(column.type):
                limits = pc.min_max(column).as_py()
                bound = max(abs(limits["min"] or 0), abs(limits["max"] or 0))
                if bound * len(column) > 2**63 - 1:
                    column = pc.cast(column, pa.decimal128(38, 0))
            elif not pa.types.is_decimal(column.type):
                continue
            value = pc.sum(column).as_py()
            if value is not None:
                total += float(value)
        DIGESTS.append((name, result.num_rows, total))


def write():
    target = os.environ.get("IBEX_DIGEST_OUT")
    if target:
        with open(target, "w", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t")
            writer.writerow(["query", "rows", "numeric_sum"])
            writer.writerows(DIGESTS)
