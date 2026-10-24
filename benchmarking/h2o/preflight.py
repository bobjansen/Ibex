# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Small deterministic fixtures; these never contribute benchmark samples."""
import copy
import csv
import random
import io
import json

NULL_TOKEN = "__H2O_NULL__"


def fixtures(directory):
    directory.mkdir(exist_ok=True)
    for nullable, sorted_rows in ((False, False), (True, True)):
        rng = random.Random(108)
        n = 768
        suffix = f"{5 if nullable else 0}_{int(sorted_rows)}"
        group = f"G1_768_2_{suffix}"
        labels = ['α,"quoted"', "", "id003", "long repeated label " * 8]
        rows = []
        for i in range(n):
            row = [labels[i % 4], labels[(i // 4) % 4], f"key {i % 53}",
                   (i % 11) * 1009 - 4000, (i // 11) % 7, (i % 67) * 17 - 99,
                   rng.randint(1, 5), rng.randint(1, 15), round(rng.random() * 100, 6)]
            # Preserve several non-null observations per grouping key while
            # exercising null key tuples, pairwise correlation and null v3.
            if nullable:
                if i % 19 == 0:
                    row[0] = None
                if i % 23 == 0:
                    row[3] = None
                if i % 13 == 0:
                    row[6] = None
                if i % 17 == 0:
                    row[7] = None
                if i % 29 == 0:
                    row[8] = None
            rows.append(row)
        if sorted_rows:
            rows.sort(key=lambda row: tuple((v is not None, str(v)) for v in row[:6]))
        write(directory / f"{group}.csv", [f"id{i}" for i in range(1, 7)] + ["v1", "v2", "v3"], rows)
        # A logical 10M name is needed by the standard right-table name mapping;
        # the fixture itself has only 768 rows. No upstream generator is altered.
        join = f"J1_1e7_NA_{suffix}"
        keys = [-17, 0, 1009, 999999]
        def key(i):
            return None if nullable and i % 19 == 0 else keys[i % 4]
        def label(k):
            return None if k is None else f'key α,"{k}"'
        left = []
        for i in range(n):
            a, b, c = key(i), keys[(i // 4) % 4], i % 101
            left.append([a, b, c, label(a), label(b), label(c), float(i % 31)])
        if sorted_rows:
            left.sort(key=lambda row: tuple((v is not None, str(v)) for v in row[:6]))
        write(directory / f"{join}.csv", [f"id{i}" for i in range(1, 7)] + ["v1"], left)
        for size, columns, data in (
            ("1e1", ["id1", "id4", "v2"],
             [[k, label(k), float(i)] for i, k in enumerate(keys[:-1] * 2)]),
            ("1e4", ["id1", "id2", "id4", "id5", "v2"],
             [[keys[i % 4], k, label(keys[i % 4]), label(k), float(i)]
              for i, k in enumerate(keys[:-1] * 2)]),
            ("1e7", [f"id{i}" for i in range(1, 7)] + ["v2"],
             [[keys[i % 4], keys[(i // 4) % 4], i % 89, label(keys[i % 4]),
               label(keys[(i // 4) % 4]), label(i % 89), float(i)] for i in range(178)])):
            if sorted_rows:
                data.sort(key=lambda row: tuple((v is not None, str(v)) for v in row[:-1]))
            write(directory / f"{join.replace('NA', size)}.csv", columns, data)
        yield group, "groupby", nullable
        yield join, "join", nullable


def write(path, columns, rows):
    # Quote empty strings explicitly so they remain distinct from unquoted
    # empty fields (null) in every CSV reader.
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            # Use an unquoted null token, keeping quoted empty strings valid.
            fields = []
            for value in row:
                cell = io.StringIO(newline="")
                csv.writer(cell, quoting=csv.QUOTE_NONNUMERIC, lineterminator="").writerow([value])
                fields.append(NULL_TOKEN if value is None else cell.getvalue())
            handle.write(",".join(fields) + "\n")


def run(harness, args, work):
    original = harness.DATA_DIR
    original_null = harness.NULL_TOKEN
    checks = []
    try:
        harness.DATA_DIR = work / "fixtures"
        harness.NULL_TOKEN = NULL_TOKEN
        for data, task, nas in fixtures(harness.DATA_DIR):
            expected = [q[0] for q in harness.questions_for(task, nas)]
            # Cover serial and parallel execution, independently of timed cores.
            for cores in sorted({1, min(2, args.threads)}):
                small = copy.copy(args)
                small.data_one, small.threads = data, cores
                small.warmup, small.iters, small.questions, small.capture_full = 0, 1, [], True
                rows = []
                for engine in args.engines:
                    if engine == "ibex":
                        rows += harness.ibex_run(data, task, nas, [], 0, 1, cores, args.timeout, full=True)
                    else:
                        rows += harness.run_worker(engine, small)
                (work / f"{data}_{cores}c.json").write_text(json.dumps(rows) + "\n")
                harness.validate_coverage(rows, expected, args.engines)
                for q in expected:
                    results = [r for r in rows if r["question"] == q]
                    status = harness.verify(results, args.engines, full=True)
                    checks.append({"data": data, "task": task, "question": q,
                                   "cores": cores, "engines": args.engines, "status": status})
                    if status != "ok":
                        raise ValueError(f"bounded answers {data} {q} {cores} cores: {status}")
                print(f"bounded answers: {data}, {cores} cores PASS", file=harness.sys.stderr, flush=True)
        return {"status": "passed", "checks": checks}
    finally:
        harness.DATA_DIR = original
        harness.NULL_TOKEN = original_null
