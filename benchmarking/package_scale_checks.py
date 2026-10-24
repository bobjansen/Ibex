#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Package only the current worker's correctness artifacts, excluding stale files."""
import argparse
import json
import pathlib
import tarfile


def package(root, output):
    manifest = root / "checks_run.json"
    run = json.loads(manifest.read_text())
    paths = [manifest]
    for rows in run["sizes"]:
        directory = root / "scales" / str(rows)
        paths.append(directory / "answer_plan.json")
        for variant in run["variants"]:
            stem = variant.replace("-", "_")
            paths.append(directory / (stem + "_digest.tsv"))
            paths.append(directory / ("python.tsv" if variant == "polars" else stem + ".tsv"))
    if "ibex" in run["variants"]:
        paths.append(root / "correctness/answers.json")
    with tarfile.open(output, "w:gz") as archive:
        for path in sorted(set(paths)):
            archive.add(path, arcname=str(path.relative_to(root)), recursive=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()
    package(args.root, args.output)


if __name__ == "__main__":
    main()
