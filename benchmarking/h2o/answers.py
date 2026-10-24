"""H2O answer contracts shared by all three executors and publication.

Scale fingerprints are order-independent screens, not proofs. Exact fields
and float/null classifications use a 256-bit row multiset hash. Float moments
are bound to those fields in 64 buckets with two independent weights. Only
Float32-derived fields receive the wider tolerance required by upstream types.
Bounded validation additionally compares every cell and duplicate row.
"""
import math
import csv
import os
import threading
import io
from itertools import combinations

ENGINES = ("ibex", "polars", "duckdb")
CHECK_VERSION = "h2o-answers-v2"
RUN_CONFIG = {"inputs": "resident-csv", "process_lifetime": "one-per-dataset-per-engine",
              "answer_cleanup": "outside-timing", "polars_engine": "streaming",
              "csv_nulls": "explicit-token;upstream-empty;fixtures-sentinel",
              "all_null_sum": "null",
              "duckdb_storage_compatibility": "latest", "upstream": "duckdblabs/db-benchmark@3b074bc319"}
BUCKETS = 64
MASK = (1 << 256) - 1


def ibex_columns(task, qid):
    table = "small" if qid == "q1" else "big" if qid == "q5" else "medium"
    return [name[6:] + "_" + table if name.startswith("right_") else name
            for name in contract(task, qid)]


class PipeCapture:
    """Drain csv::write after timing without a scale-sized temporary file.

    A parent writer keeps the pipe open until the REPL exits, so a query error
    yields EOF and a failed check rather than a permanently blocked reader.
    """
    def __init__(self, path, task, qid, full, live=False):
        self.path, self.task, self.qid, self.full = path, task, qid, full
        self.answer, self.error = None, None
        self.live = live
        self.lock = threading.Lock()

    def __enter__(self):
        os.mkfifo(self.path)
        read_fd = os.open(self.path, os.O_RDONLY | os.O_NONBLOCK)
        self.keepalive = os.open(self.path, os.O_WRONLY | os.O_NONBLOCK)
        os.set_blocking(read_fd, True)
        self.thread = threading.Thread(target=self.read, args=(read_fd,), daemon=True)
        self.thread.start()
        return self

    def read(self, fd):
        try:
            import pyarrow as pa
            import pyarrow.csv as pacsv
            import polars as pl
            with os.fdopen(fd, "rb", buffering=0) as handle:
                names = ibex_columns(self.task, self.qid)
                flags = [f"__check_null_{i}" for i in range(len(names))]
                header = next(csv.reader([handle.readline().decode()]), None)
                if self.live:
                    self.close_writer()
                if header != names + flags:
                    raise ValueError("missing or invalid Ibex answer header")
                types = {name: pa.int64() if kind == "i" else pa.string() if kind == "s" else pa.float64()
                         for name, kind in zip(names, contract(self.task, self.qid).values())}
                types.update({name: pa.int64() for name in flags})
                buffered = io.BufferedReader(handle)
                if not buffered.peek(1):
                    self.answer = fingerprint_frames(self.task, self.qid, "ibex", names, [], self.full)
                    return
                reader = pacsv.open_csv(buffered,
                    read_options=pacsv.ReadOptions(column_names=names + flags, use_threads=False),
                    convert_options=pacsv.ConvertOptions(column_types=types, null_values=[""],
                                                         strings_can_be_null=False))
                def frames():
                    for batch in reader:
                        frame = pl.from_arrow(batch)
                        for name, flag in zip(names, flags):
                            if (frame[flag].null_count() or not frame[flag].is_in([0, 1]).all()
                                    or frame.filter((pl.col(flag) == 0) & pl.col(name).is_null()).height):
                                raise ValueError("invalid answer null flag/value")
                        yield frame.select([pl.when(pl.col(flag) == 1).then(None).otherwise(pl.col(name)).alias(name)
                                            for name, flag in zip(names, flags)])
                self.answer = fingerprint_frames(self.task, self.qid, "ibex", names, frames(), self.full)
        except Exception as exc:
            self.error = str(exc)

    def close_writer(self):
        with self.lock:
            if self.keepalive is not None:
                os.close(self.keepalive)
                self.keepalive = None

    def __exit__(self, *exc):
        self.close_writer()
        self.thread.join()


def contract(task, qid):
    if task == "groupby":
        return {
            "q1": {"id1": "s", "v1": "i"},
            "q2": {"id1": "s", "id2": "s", "v1": "i"},
            "q3": {"id3": "s", "v1": "i", "v3": "f32"},
            "q4": {"id4": "i", "v1": "f64", "v2": "f64", "v3": "f32"},
            "q5": {"id6": "i", "v1": "i", "v2": "i", "v3": "f32"},
            "q6": {"id4": "i", "id5": "i", "median_v3": "f32", "sd_v3": "f32"},
            "q7": {"id3": "s", "range_v1_v2": "i"},
            "q8": {"id6": "i", "largest2_v3": "f32"},
            "q9": {"id2": "s", "id4": "i", "r2": "f64"},
            "q10": {**{f"id{i}": "s" if i <= 3 else "i" for i in range(1, 7)},
                    "v3": "f32", "count": "i"},
        }[qid]
    fields = {**{f"id{i}": "i" if i <= 3 else "s" for i in range(1, 7)},
              "v1": "f32", "v2": "f32"}
    extras = {"q1": [4], "q2": [1, 4, 5], "q3": [1, 4, 5],
              "q4": [1, 2, 4], "q5": [1, 2, 4, 5, 6]}[qid]
    return fields | {f"right_id{i}": "i" if i <= 3 else "s" for i in extras}


def canonical_names(task, qid, engine, names):
    if task == "groupby":
        return list(names)
    table = "small" if qid == "q1" else "big" if qid == "q5" else "medium"
    result = []
    for name in names:
        if engine == "duckdb" and name.startswith(table + "_"):
            name = "right_" + name[len(table) + 1:]
        elif engine == "ibex" and name.endswith("_" + table):
            name = "right_" + name[:-len(table) - 1]
        elif engine == "polars" and name.endswith("_right"):
            name = "right_" + name[:-6]
        result.append(name)
    return result


def state(value):
    if value is None:
        return "null"
    if isinstance(value, float) and not math.isfinite(value):
        return "nan" if math.isnan(value) else "+inf" if value > 0 else "-inf"
    return "finite"


def float_close(a, b, kind):
    # Float32 input roundoff is ~6e-8 relative; allow accumulated operations
    # some slack. Integral outputs never go through this tolerance.
    return math.isclose(a, b, rel_tol=1e-6 if kind == "f32" else 1e-9,
                        abs_tol=1e-6 if kind == "f32" else 1e-10)


def moment_kinds(schema):
    kinds = {name: kind for name, kind in schema.items() if kind.startswith("f")}
    return kinds | {f"cross:{a}:{b}": "f32" if "f32" in (kinds[a], kinds[b]) else "f64"
                    for a, b in combinations(kinds, 2)}


def fingerprint_frames(task, qid, engine, names, frames, full=False):
    """Batch-native validation; all rows contribute, never a sampled digest."""
    import numpy as np
    import polars as pl
    schema = contract(task, qid)
    canonical = canonical_names(task, qid, engine, names)
    if len(canonical) != len(set(canonical)) or set(canonical) != set(schema):
        raise ValueError(f"{engine} {qid}: columns {canonical}, expected {list(schema)}")
    mapping = dict(zip(names, canonical))
    kinds = moment_kinds(schema)
    moments = {name: np.zeros((BUCKETS, 4)) for name in kinds}
    counts = {name: dict.fromkeys(("null", "finite", "nan", "+inf", "-inf"), 0) for name in schema}
    count, sums, squares = 0, [0] * 4, [0] * 4
    captured = [] if full else None
    for raw in frames:
        if list(raw.columns) != list(names):
            raise ValueError("invalid answer columns")
        raw = raw.rename(mapping).select(list(schema))
        for name, kind in schema.items():
            dtype = raw.schema[name]
            if kind == "i" and dtype != pl.Null and not dtype.is_integer() and not (
                    isinstance(dtype, pl.Decimal) and dtype.scale == 0):
                raise ValueError(f"{name}: expected exact integer, got {dtype}")
            if kind == "s" and dtype not in (pl.Null, pl.String, pl.Categorical):
                raise ValueError(f"{name}: expected string, got {dtype}")
        frame = raw.cast({name: pl.Int64 if kind == "i" else pl.String if kind == "s" else pl.Float64
                          for name, kind in schema.items()}, strict=True)
        states = {}
        exact = []
        for name, kind in schema.items():
            col = pl.col(name)
            status = pl.when(col.is_null()).then(0)
            if kind.startswith("f"):
                status = (status.when(col.is_nan()).then(2)
                          .when(col == float("inf")).then(3).when(col == -float("inf")).then(4))
            states[name] = frame.select(status.otherwise(1).cast(pl.UInt8).alias(name))[name].to_numpy()
            hist = np.bincount(states[name], minlength=5)
            for category, n in zip(counts[name], hist):
                counts[name][category] += int(n)
            exact.append(pl.Series(name, states[name]) if kind.startswith("f") else frame[name])
        exact = pl.DataFrame(exact)
        hashes = [exact.hash_rows(seed=seed).to_numpy() for seed in (0, 1, 104729, 1000000007)]
        for i, h in enumerate(hashes):
            sums[i] = (sums[i] + int(h.sum(dtype=np.uint64))) & ((1 << 64) - 1)
            squares[i] = (squares[i] + int((h * h).sum(dtype=np.uint64))) & ((1 << 64) - 1)
        bucket = (hashes[0] % BUCKETS).astype(np.int64)
        w1 = 1. + (hashes[1] >> 32).astype(np.float64) / 2**32
        w2 = 1. + (hashes[2] >> 32).astype(np.float64) / 2**32
        numeric = {name: frame[name].to_numpy() for name, kind in schema.items() if kind.startswith("f")}
        for name in kinds:
            if name.startswith("cross:"):
                _, a, b = name.split(":")
                mask = (states[a] == 1) & (states[b] == 1)
                value = numeric[a][mask] * numeric[b][mask]
            else:
                mask = states[name] == 1
                value = numeric[name][mask]
            for j, contribution in enumerate((value, value * value, value * w1[mask], value * w2[mask])):
                moments[name][:, j] += np.bincount(bucket[mask], weights=contribution, minlength=BUCKETS)
            if not np.isfinite(moments[name]).all():
                raise ValueError(f"{name}: fingerprint overflow")
        if full:
            captured.extend([list(row) for row in frame.iter_rows()])
        count += frame.height
    packed = lambda parts: str(sum(part << (64 * i) for i, part in enumerate(parts)))
    return {"version": CHECK_VERSION, "algorithm": "polars-four-seed-row-multiset-v1",
            "schema": schema, "rows": count, "hash": [packed(sums), packed(squares)],
            "counts": counts, "moments": {name: value.tolist() for name, value in moments.items()}, "full": captured}


def fingerprint(task, qid, engine, names, rows, full=False):
    import polars as pl
    from itertools import islice
    schema = contract(task, qid)
    canonical = canonical_names(task, qid, engine, names)
    if len(names) != len(set(names)) or set(canonical) != set(schema):
        raise ValueError("invalid answer columns")
    typed = [(name, pl.Int64 if schema[c] == "i" else pl.String if schema[c] == "s" else pl.Float64)
             for name, c in zip(names, canonical)]
    rows = iter(rows)
    def frames():
        while batch := list(islice(rows, 65536)):
            if any(len(row) != len(names) for row in batch):
                raise ValueError("invalid answer row width")
            yield pl.DataFrame(batch, schema=typed, orient="row", strict=True)
    return fingerprint_frames(task, qid, engine, names, frames(), full)


def compare(left, right, require_full=False):
    if not isinstance(left, dict) or not isinstance(right, dict):
        return "missing answer fingerprint"
    for key in ("version", "algorithm", "schema", "rows", "hash", "counts"):
        if key not in left or left[key] != right.get(key):
            return f"answer {key} differs"
    if left["version"] != CHECK_VERSION:
        return "unsupported answer fingerprint"
    if type(left["rows"]) is not int or left["rows"] < 0 or not left["schema"]:
        return "invalid answer shape"
    hashes = left["hash"]
    if (not isinstance(hashes, list) or len(hashes) != 2
            or any(not isinstance(h, str) or not h.isdigit() or int(h) > MASK for h in hashes)):
        return "invalid row multiset hash"
    if set(left["counts"]) != set(left["schema"]):
        return "missing column counts"
    for name, counts in left["counts"].items():
        if (set(counts) != {"null", "finite", "nan", "+inf", "-inf"}
                or any(type(n) is not int or n < 0 for n in counts.values())
                or sum(counts.values()) != left["rows"]):
            return f"{name}: invalid null/value counts"
    for name, kind in moment_kinds(left["schema"]).items():
        a, b = left.get("moments", {}).get(name), right.get("moments", {}).get(name)
        if not isinstance(a, list) or not isinstance(b, list) or len(a) != BUCKETS or len(b) != BUCKETS:
            return f"{name}: missing moments"
        for x, y in zip(a, b):
            if len(x) != 4 or len(y) != 4 or not all(
                math.isfinite(v) for v in (*x, *y)) or not all(float_close(v, w, kind) for v, w in zip(x, y)):
                return f"{name}: keyed float moments differ"
    if require_full:
        a, b = left.get("full"), right.get("full")
        if not isinstance(a, list) or not isinstance(b, list) or len(a) != left["rows"] or len(b) != right["rows"]:
            return "missing or truncated full answers"
        if any(not isinstance(row, list) or len(row) != len(left["schema"]) for row in a + b):
            return "invalid full-answer row width"
        def key(row):
            # Exact columns first keep group associations stable; float columns
            # break ties (top-two and duplicate-key joins) independent of order.
            order = sorted(range(len(row)), key=lambda i: list(left["schema"].values())[i].startswith("f"))
            return tuple((state(row[i]), row[i] if state(row[i]) == "finite" else 0) for i in order)
        for x, y in zip(sorted(a, key=key), sorted(b, key=key)):
            for (name, kind), v, w in zip(left["schema"].items(), x, y):
                if state(v) != state(w) or (state(v) == "finite" and (
                    not float_close(v, w, kind) if kind.startswith("f") else v != w)):
                    return f"full answer column {name} differs: {v!r} vs {w!r}"
    return ""
