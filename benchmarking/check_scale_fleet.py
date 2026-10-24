#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Gate a fleet merge using each worker's correctness sidecar, never timings alone."""
import argparse
import json
import pathlib
import tarfile
import tempfile

from check_scale_run import check


def unpack(archive_path, root):
    total = 0
    with tarfile.open(archive_path) as archive:
        for count, member in enumerate(archive):
            total += member.size
            path = pathlib.PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts or
                    member.size > 2_000_000 or total > 20_000_000 or count >= 1000 or
                    "\\" in member.name or ":" in member.name or path.suffix not in {".tsv", ".json"}):
                raise ValueError(f"invalid correctness artifact {member.name!r}")
            target = root.joinpath(*path.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            content = archive.extractfile(member).read()
            if target.exists():
                raise ValueError(f"duplicate correctness artifact {member.name}")
            target.write_bytes(content)


def verify(ibex_archive, polars_archive=None, duckdb_archive=None):
    with tempfile.TemporaryDirectory(prefix="ibex_fleet_checks_") as tmp:
        root = pathlib.Path(tmp)
        left, right, merged = root / "ibex", root / "polars", root / "merged"
        unpack(ibex_archive, left)
        lrun = json.loads((left / "checks_run.json").read_text())
        runs, workers = [lrun], [left]
        polars = []
        if polars_archive is not None:
            unpack(polars_archive, right)
            rrun = json.loads((right / "checks_run.json").read_text())
            runs.append(rrun)
            workers.append(right)
            polars = [v for v in rrun["variants"] if v.startswith("polars")]
            if "polars" not in polars:
                raise ValueError("required Polars worker variant missing")
        extra = root / "duckdb"
        duckdb = []
        if duckdb_archive is not None:
            unpack(duckdb_archive, extra)
            drun = json.loads((extra / "checks_run.json").read_text())
            runs.append(drun)
            workers.append(extra)
            duckdb = [v for v in drun["variants"] if v.startswith("duckdb")]
            if "duckdb" not in duckdb:
                raise ValueError("required DuckDB worker variant missing")
        allowed = {"ibex", "ibex-st", "polars", "polars-st", "polars-in-memory", "duckdb", "duckdb-st"}
        for run in runs:
            if (any(type(n) is not int or n <= 0 for n in run["sizes"]) or
                    len(set(run["sizes"])) != len(run["sizes"]) or
                    not set(run["variants"]) <= allowed):
                raise ValueError("invalid worker run manifest")
        origins = []
        for worker, run in zip(workers, runs):
            if run.get("provenance_required"):
                origin = json.loads((worker / "runtime_provenance.json").read_text())
                if not origin.get("harness") or not origin.get("versions"):
                    raise ValueError("worker benchmark provenance missing")
                if "ibex" in run["variants"] and (origin.get("build_type") != "Release" or
                        not origin.get("artifacts", {}).get("tools/ibex_bench")):
                    raise ValueError("worker Release benchmark binary provenance missing")
                origins.append((origin["harness"], origin["versions"]))
        if origins and (len(origins) != len(runs) or any(o != origins[0] for o in origins)):
            raise ValueError("worker harness or package versions differ")
        if not lrun["sizes"] or any(run["sizes"] != lrun["sizes"] for run in runs):
            raise ValueError("worker scale plans differ or are empty")
        ibex = [v for v in lrun["variants"] if v.startswith("ibex")]
        if "ibex" not in ibex or not (polars or duckdb):
            raise ValueError("required worker variants missing")
        errors = []
        for rows in lrun["sizes"]:
            a, b = left / "scales" / str(rows), right / "scales" / str(rows)
            plan = json.loads((a / "answer_plan.json").read_text())
            if plan.get("rows") != rows or (polars and plan != json.loads((b / "answer_plan.json").read_text())):
                raise ValueError(f"{rows}: worker data/reshape/TimeFrame plans differ")
            if duckdb and plan != json.loads((extra / "scales" / str(rows) / "answer_plan.json").read_text()):
                raise ValueError(f"{rows}: DuckDB worker data/reshape/TimeFrame plans differ")
            directory = merged / str(rows)
            directory.mkdir(parents=True)
            for source, variants in ((a, ibex), (b, polars), (extra / "scales" / str(rows), duckdb)):
                for variant in variants:
                    stem = variant.replace("-", "_")
                    timing = "python.tsv" if variant == "polars" else stem + ".tsv"
                    for name in (stem + "_digest.tsv", timing):
                        (directory / name).write_bytes((source / name).read_bytes())
            errors += check(directory, ibex, polars, left / "correctness/answers.json",
                            without_reshape=plan["reshape_rows"] == 0,
                            without_timeframe=plan["timeframe_rows"] == 0, duckdb_variants=duckdb)
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ibex", type=pathlib.Path)
    parser.add_argument("polars", nargs="?", type=pathlib.Path)
    parser.add_argument("--duckdb", type=pathlib.Path)
    args = parser.parse_args()
    try:
        errors = verify(args.ibex, args.polars, args.duckdb)
    except (OSError, ValueError, KeyError, tarfile.TarError) as exc:
        print(f"fleet answer validation error: {exc}")
        return 2
    for error in errors:
        print("MISMATCH " + error)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
