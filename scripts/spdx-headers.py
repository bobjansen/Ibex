#!/usr/bin/env python3
"""Check or insert SPDX license headers in Ibex-owned source files.

Usage:
  scripts/spdx-headers.py --check   # exit 1 and list files missing a header
  scripts/spdx-headers.py --fix     # insert the header where missing

Covers tracked *.cpp *.hpp *.py *.R *.sh *.cmake and CMakeLists.txt files.
Third-party and generated material is excluded below; .ibex sources are
excluded because tests pin line numbers in them.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SPDX_ID = "AGPL-3.0-only"
COPYRIGHT = "Copyright (C) 2026 Bob Jansen"

EXTENSIONS = {".cpp", ".hpp", ".py", ".R", ".sh", ".cmake"}
BASENAMES = {"CMakeLists.txt"}

# Prefix-matched, relative to the repository root.
EXCLUDE = (
    "r/ribex/inst/third-party/",            # Poorman MIT material
    "r/ribex/tests/testthat/test-poorman-conformance.R",  # Poorman-derived, MIT
    "renv/",                                # vendored Posit code
)

LINE_COMMENT = {
    ".cpp": "//",
    ".hpp": "//",
    ".py": "#",
    ".R": "#",
    ".sh": "#",
    ".cmake": "#",
    "CMakeLists.txt": "#",
}


def comment_prefix(path: Path) -> str:
    return LINE_COMMENT.get(path.suffix) or LINE_COMMENT[path.name]


def tracked_files(root: Path) -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, capture_output=True, check=True, text=True
    ).stdout
    files = []
    for rel in out.split("\0"):
        if not rel or rel.startswith(EXCLUDE):
            continue
        p = Path(rel)
        if p.suffix in EXTENSIONS or p.name in BASENAMES:
            files.append(p)
    return files


def has_header(text: str) -> bool:
    return f"SPDX-License-Identifier: {SPDX_ID}" in text[:1024]


def insert_header(path: Path, text: str) -> str:
    prefix = comment_prefix(path)
    header = f"{prefix} SPDX-License-Identifier: {SPDX_ID}\n{prefix} {COPYRIGHT}\n"
    lines = text.splitlines(keepends=True)
    at = 1 if lines and lines[0].startswith("#!") else 0
    rest = lines[at:]
    if rest and rest[0].strip():
        header += "\n"  # blank line between header and existing content
    return "".join(lines[:at]) + header + "".join(rest)


def main() -> int:
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--fix", action="store_true")
    args = ap.parse_args()

    root = Path(
        subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, check=True, text=True,
        ).stdout.strip()
    )

    missing = []
    for rel in tracked_files(root):
        path = root / rel
        text = path.read_text(encoding="utf-8")
        if has_header(text):
            continue
        missing.append(rel)
        if args.fix:
            path.write_text(insert_header(rel, text), encoding="utf-8")

    if args.fix:
        print(f"added SPDX headers to {len(missing)} file(s)")
        return 0
    if missing:
        print(f"{len(missing)} file(s) missing an SPDX header:", file=sys.stderr)
        for rel in missing:
            print(f"  {rel}", file=sys.stderr)
        print(f"run: scripts/spdx-headers.py --fix", file=sys.stderr)
        return 1
    print("all covered files carry an SPDX header")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
