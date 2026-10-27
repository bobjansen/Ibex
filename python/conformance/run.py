# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Run with python -m conformance.run --upstream /path/to/polars."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


PIN = Path(__file__).with_name("upstream.json")


def upstream_digest(root):
    """Content pin works for both Git checkouts and release tarballs."""
    paths = [root / "py-polars/pyproject.toml"]
    paths += sorted(path for path in (root / "py-polars/tests").rglob("*")
                    if path.is_file() and "__pycache__" not in path.parts
                    and path.suffix not in (".pyc", ".pyo"))
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def support_regressions(tests, required, *, full_selection):
    missing = []
    for node, ordinals in required.items():
        if node not in tests and not full_selection:
            continue
        events = {event["query"]: event["status"] for event in tests.get(node, {}).get("queries", [])}
        for ordinal in ordinals:
            if events.get(ordinal) != "pass":
                missing.append({"test": node, "query": ordinal,
                                "status": events.get(ordinal, "not exercised")})
    return missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--report-dir", type=Path, default=Path("polars-conformance-results"))
    parser.add_argument("--select", action="append", help="upstream test path/node ID, relative to py-polars; repeatable")
    args = parser.parse_args()
    pin = json.loads(PIN.read_text())
    upstream = args.upstream.resolve()
    import polars as pl
    import pytest
    import hypothesis
    if pl.__version__ != pin["polars_version"]:
        parser.error(f"requires Polars {pin['polars_version']}, found {pl.__version__}")
    if pytest.__version__ != "9.0.3" or hypothesis.__version__ != "6.170.9":
        parser.error("install the pinned dependencies from python/conformance/requirements.txt")
    if not (upstream / "py-polars/tests").is_dir():
        parser.error("--upstream must name the Polars repository root")
    if upstream_digest(upstream) != pin["test_tree_sha256"]:
        parser.error(f"upstream test tree differs from pinned commit {pin['commit']}; use an unmodified checkout/archive")
    selected = args.select or pin["targets"]
    for target in selected:
        path = (upstream / "py-polars" / target.split("::")[0]).resolve()
        if not path.is_relative_to(upstream / "py-polars/tests") or not path.is_file():
            parser.error(f"selection must name a pinned upstream test file: {target}")
    output = args.report_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps({"status": "running", "upstream": pin}) + "\n")
    env = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", PYTHONDONTWRITEBYTECODE="1", PYTEST_ADDOPTS="")
    for variable in ("IBEX_CONFORMANCE_BASELINE", "IBEX_CONFORMANCE_MODE", "IBEX_CONFORMANCE_OUTPUT", "PYTEST_PLUGINS"):
        env.pop(variable, None)
    # Put upstream tests (but not src/polars) on the import path. Use the
    # installed pinned wheel and this worktree's adapter in both processes.
    env["PYTHONPATH"] = os.pathsep.join([str(Path(__file__).resolve().parents[1]),
                                        str(upstream / "py-polars"), env.get("PYTHONPATH", "")])
    command = [sys.executable, "-m", "pytest", "-p", "conformance.plugin", "-o", "addopts=",
               "--import-mode=importlib", "-m", "not slow and not hypothesis and not write_disk and not release and not benchmark and not ci_only and not docs",
               "-o", f"cache_dir={output / 'pytest-cache'}", "--tb=short", "-q", *selected]
    for mode in ("reference", "candidate"):
        env["IBEX_CONFORMANCE_MODE"] = mode
        env["IBEX_CONFORMANCE_OUTPUT"] = str(output / f"{mode}.json")
        if mode == "candidate":
            env["IBEX_CONFORMANCE_BASELINE"] = str(output / "reference.json")
        result = subprocess.run(command, cwd=upstream / "py-polars", env=env, check=False)
        if result.returncode not in (0, 1) or not (output / f"{mode}.json").exists():
            (output / "report.json").write_text(json.dumps({"status": "incomplete", "phase": mode,
                                                           "exit_code": result.returncode, "upstream": pin}, indent=2) + "\n")
            print(f"{mode} run failed to complete; see {output}", file=sys.stderr)
            return 1
    reference = json.loads((output / "reference.json").read_text())
    candidate = json.loads((output / "candidate.json").read_text())
    regressions = support_regressions(candidate["tests"], pin["required_pass_queries"],
                                     full_selection=args.select is None)
    report = {"status": "complete", "upstream": pin, "selection": selected, "polars_version": pl.__version__,
              "python_version": sys.version, "pytest_version": pytest.__version__,
              "hypothesis_version": hypothesis.__version__, "support_regressions": regressions,
              "summary": candidate["summary"], "queries": candidate["queries"],
              "baseline_summary": reference["summary"], "tests": candidate["tests"],
              "collection_errors": reference["collection_errors"] + candidate["collection_errors"]}
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Ibex upstream conformance:", json.dumps(report["summary"], sort_keys=True))
    print("Query comparisons:", json.dumps(report["queries"], sort_keys=True))
    if regressions:
        print(f"Support regressions: {len(regressions)} previously passing queries")
    print("Report:", output / "report.json")
    return int(bool(regressions or report["collection_errors"] or reference["pytest_exit"]
                    or candidate["pytest_exit"] or candidate["summary"].get("mismatch")
                    or not candidate["queries"].get("pass")))


if __name__ == "__main__":
    raise SystemExit(main())
