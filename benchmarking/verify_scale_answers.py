#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Mandatory bounded, complete-answer validation before a scale comparison.

Uses the actual timing harness expressions; exports happen after timing and no
samples from this pass enter benchmark results. Two row counts exercise String
and Categorical input keys. Stochastic queries are explicitly excluded.
"""
import argparse
import csv
import importlib.util
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile

from check_scale_answers import IBEX_ONLY, STOCHASTIC, compare_directories, read_answer

ROOT = pathlib.Path(__file__).resolve().parents[1]
SUITES = ("core,cumulative,sort,window,groupagg,pipeline,join,fill,null,filter,"
          "multi,events,reshape,timeframe,transform,stats,scalar,rng")


def check_exported_count(directory, query, rows):
    actual = len(read_answer(directory / (query + ".csv"))[1])
    if actual != rows:
        raise ValueError(f"{query}: exported {actual} rows, harness reported {rows}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=pathlib.Path, default=ROOT / "build-release")
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--with-polars-in-memory", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    work = pathlib.Path(tempfile.mkdtemp(prefix="full_answers_", dir=args.out))
    report = {"status": "failed", "artifacts": str(work), "checks": []}
    report_path = args.out / "answers.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    try:
        spec = importlib.util.spec_from_file_location("scale_gen", ROOT / "benchmarking/data/gen_data.py")
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        base_env = os.environ.copy()
        # Never let ambient perf cutoffs or output paths silently skip checks.
        for name in ("IBEX_SKIP_CELLS", "IBEX_DIGEST_OUT", "IBEX_ANSWERS_DIR", "IBEX_FW_SUFFIX"):
            base_env.pop(name, None)
        base_env["IBEX_CELL_CUTOFF_MS"] = "inf"
        base_env["POLARS_ENGINE_AFFINITY"] = "streaming"
        for n in (720, 4096):
            directory = work / str(n)
            data = directory / "data"
            generator.generate(data, n=n)
            # Include a quoted Unicode key; verify real Strings, not just sums.
            import pandas as pd
            for path in data.glob("*.csv"):
                frame = pd.read_csv(path)
                if "symbol" in frame:
                    frame["symbol"] = frame["symbol"].replace("AAA", 'α,"quoted"')
                    frame.to_csv(path, index=False)
            common = []
            for name, file in (("csv", "prices"), ("csv-multi", "prices_multi"),
                               ("csv-trades", "trades"), ("csv-events", "events"),
                               ("csv-lookup", "lookup"), ("csv-users", "users")):
                common += ["--" + name, str(data / (file + ".csv"))]
            common += ["--reshape-rows", str(n), "--warmup", "0", "--iters", "1"]
            ibex = args.build_dir.resolve() / "tools/ibex_bench"
            expected = set()
            for cores in (1, 2):
                answers = directory / f"ibex_{cores}"
                command = [str(ibex), *common, "--timeframe-rows", str(n), "--suite", SUITES]
                log = directory / f"ibex_{cores}.log"
                with log.open("w") as handle:
                    subprocess.run(command, cwd=ROOT, env=base_env | {
                        "IBEX_CORES": str(cores), "IBEX_ANSWERS_DIR": str(answers)},
                        stdout=handle, stderr=subprocess.STDOUT, check=True)
                counts = re.findall(r"^bench ([^:]+):.*\brows=(\d+)", log.read_text(), re.MULTILINE)
                measured = {query for query, _ in counts}
                if len(measured) != len(counts):
                    raise ValueError("duplicate Ibex timing query")
                for query, count in counts:
                    check_exported_count(answers, query, int(count))
                expected |= measured - IBEX_ONLY - STOCHASTIC - {q for q in measured if q.startswith("parse_")}
            modes = ("streaming", "in-memory") if args.with_polars_in_memory else ("streaming",)
            for cores, engine in ((c, m) for m in modes for c in (1, 2)):
                answers = directory / f"polars_{cores}_{engine}"
                timing = directory / f"polars_{cores}_{engine}.tsv"
                command = [sys.executable, str(ROOT / "benchmarking/bench_python.py"),
                           *common, "--tf-rows", str(n), "--fill-rows", str(n),
                           "--skip-pandas", "--out", str(timing)]
                with (directory / f"polars_{cores}_{engine}.log").open("w") as handle:
                    subprocess.run(command, cwd=ROOT, env=base_env | {
                        "POLARS_MAX_THREADS": str(cores), "POLARS_ENGINE_AFFINITY": engine,
                        "IBEX_ANSWERS_DIR": str(answers)},
                        stdout=handle, stderr=subprocess.STDOUT, check=True)
                with timing.open() as handle:
                    rows = list(csv.DictReader(handle, delimiter="\t"))
                    measured = {row["query"] for row in rows}
                    if len(measured) != len(rows):
                        raise ValueError("duplicate Polars timing query")
                    for row in rows:
                        check_exported_count(answers, row["query"], int(row["rows"]))
                required = expected | (measured - STOCHASTIC)
                errors = compare_directories(directory / f"ibex_{cores}", answers, required)
                report["checks"].append({"rows": n, "cores": cores, "engine": engine, "queries": len(required), "query_names": sorted(required),
                                         "errors": errors})
                if errors:
                    raise ValueError("\n".join(errors))
                print(f"full answers: {n} input rows, {cores} cores, {engine}, {len(required)} queries PASS", flush=True)
            errors = compare_directories(directory / "ibex_1", directory / "ibex_2", expected)
            if errors:
                raise ValueError("serial/parallel: " + "\n".join(errors))
        report["status"] = "passed"
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        report["error"] = str(exc)
        print(f"full-answer validation FAILED: {exc}\nartifacts: {work}", file=sys.stderr)
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
