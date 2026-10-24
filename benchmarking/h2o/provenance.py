#!/usr/bin/env python3
"""Record a successful Release build and verify the exact H2O executables."""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import importlib.metadata
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def sources(root):
    files = [root / "CMakeLists.txt"]
    for directory in ("src", "include", "libs", "tools", "cmake"):
        files += [p for p in (root / directory).rglob("*") if p.is_file() and
                  (p.suffix in {".cpp", ".hpp", ".h", ".c", ".cc", ".cmake", ".ibex"}
                   or p.name == "CMakeLists.txt")]
    return {str(p.relative_to(root)): sha(p) for p in sorted(files)}


def artifacts(build):
    return {name: sha(build / name) for name in
            ("tools/ibex", "tools/ibex_bench", "tools/csv.so", "tools/csv.ibex", "CMakeCache.txt")}


def require_release(build, root=ROOT):
    lines = (build / "CMakeCache.txt").read_text().splitlines()
    if "CMAKE_BUILD_TYPE:STRING=Release" not in lines:
        raise ValueError("H2O requires a Release build")
    if f"CMAKE_HOME_DIRECTORY:INTERNAL={root.resolve()}" not in lines:
        raise ValueError("build directory belongs to a different checkout")


def verify(build, root=ROOT):
    build = build.resolve()
    require_release(build, root)
    record = json.loads((build / "h2o-build.json").read_text())
    if record.get("sources") != sources(root) or record.get("artifacts") != artifacts(build):
        raise ValueError("Ibex sources/build changed since the recorded build; rerun h2o/provenance.py --build")
    return record


def build_and_record(build, root=ROOT):
    build = build.resolve()
    require_release(build, root)
    before = sources(root)
    subprocess.run(["cmake", "--build", str(build), "-j", "6"], check=True,
                   env=os.environ | {"CCACHE_DISABLE": "1"})
    after = sources(root)
    if before != after:
        raise ValueError("runtime sources changed during build; repeat the build")
    git = lambda *args: subprocess.run(["git", "-C", str(root), *args], check=True,
                                       capture_output=True, text=True).stdout.strip()
    record = {"build_dir": str(build), "build_type": "Release", "sources": after,
              "artifacts": artifacts(build), "build_checkout": git("rev-parse", "HEAD"),
              "source_diff_sha256": hashlib.sha256(git("diff", "HEAD", "--", "src", "include", "libs",
                                                        "tools", "cmake", "CMakeLists.txt").encode()).hexdigest()}
    (build / "h2o-build.json").write_text(json.dumps(record, indent=2) + "\n")
    return record


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build-dir", type=pathlib.Path, default=ROOT / "build-release")
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--out", type=pathlib.Path)
    ap.add_argument("--compare", type=pathlib.Path)
    ap.add_argument("--scale-harness", action="store_true")
    ap.add_argument("--without-ibex", action="store_true")
    args = ap.parse_args()
    record = {} if args.without_ibex else build_and_record(args.build_dir) if args.build else verify(args.build_dir)
    if args.scale_harness:
        names = ("bench_ibex.sh", "bench_python.py", "bench_duckdb.py", "bench_mem.py", "arrow_answers.py",
                 "run_scale_suite.sh", "verify_scale_answers.py", "check_scale_answers.py",
                 "check_scale_run.py", "check_scale_fleet.py", "package_scale_checks.py", "data/gen_data.py",
                 "h2o/provenance.py")
        record["harness"] = {name: sha(ROOT / "benchmarking" / name) for name in names}
        record["versions"] = {name: importlib.metadata.version(name)
                              for name in ("polars", "duckdb", "numpy", "pyarrow", "pandas")}
        record["versions"]["python"] = sys.version
    if args.compare and record != json.loads(args.compare.read_text()):
        raise ValueError("benchmark executable, harness, or package versions changed during the run")
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(record, indent=2) + "\n")
    print("verified benchmark provenance" if args.without_ibex else
          "verified Ibex sha256:" + record["artifacts"]["tools/ibex"])


if __name__ == "__main__":
    main()
