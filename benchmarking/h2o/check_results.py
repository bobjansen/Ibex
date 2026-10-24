#!/usr/bin/env python3
"""Check the requested complete H2O fleet plan before accepting an artifact."""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from h2o.publication import archive_inputs, validate_plan


def check(directory):
    plan = json.loads((directory / "run.json").read_text())
    files = {p.name: p.read_text() for p in directory.iterdir()
             if p.is_file() and (p.name.startswith("h2o_") or p.name == "failures.txt")}
    validate_plan(plan, files)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("directory", type=pathlib.Path)
    ap.add_argument("--write-plan", action="store_true")
    ap.add_argument("--archive", action="store_true")
    ap.add_argument("--data")
    ap.add_argument("--cores")
    args = ap.parse_args()
    try:
        if args.write_plan:
            args.directory.mkdir(parents=True, exist_ok=True)
            (args.directory / "run.json").write_text(json.dumps({
                "data": args.data.split(","), "cores": [int(c) for c in args.cores.split(",")]
            }) + "\n")
        elif args.archive:
            archive_inputs(args.directory)
        else:
            check(args.directory)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"H2O acceptance FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
