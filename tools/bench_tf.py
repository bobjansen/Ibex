#!/usr/bin/env python3
"""TimeFrame benchmark: ibex script vs C++ bench harness.

Each operation is benchmarked by running a .ibex script that:
  1. Calls gen_tf_data(rows) once to produce the synthetic table.
  2. Repeats the query N times (to reach min_seconds of compute).
  3. The per-operation avg_ms is (total_time - data_gen_overhead) / N.

Usage:
    # from repo root, with the release build and tools/ plugin directory:
    python3 tools/bench_tf.py
    python3 tools/bench_tf.py --rows 1000000 --min-seconds 3
"""

from __future__ import annotations

import argparse
import math
import os
import pathlib
import subprocess
import sys
import tempfile
import time

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

TF_QUERIES: list[tuple[str, str]] = [
    ("as_timeframe",       'as_timeframe(tf_data, "ts")'),
    ("tf_lag1",            'as_timeframe(tf_data, "ts")[update { prev = lag(price, 1) }]'),
    ("tf_rolling_count_1m",'as_timeframe(tf_data, "ts")[window 1m, update { c = rolling_count() }]'),
    ("tf_rolling_sum_1m",  'as_timeframe(tf_data, "ts")[window 1m, update { s = rolling_sum(price) }]'),
    ("tf_rolling_mean_5m", 'as_timeframe(tf_data, "ts")[window 5m, update { m = rolling_mean(price) }]'),
]


def detect_build_dir() -> pathlib.Path:
    for candidate in [REPO_ROOT / "build-release", REPO_ROOT / "build"]:
        if (candidate / "tools" / "ibex").exists():
            return candidate
    raise RuntimeError("ibex binary not found — run cmake --build build-release first")


def make_script(rows: int, query_expr: str, repeats: int) -> str:
    header = (
        'extern fn gen_tf_data(n: Int) -> DataFrame from "gen_tf_data.hpp";\n'
        f'let tf_data = gen_tf_data({rows});\n'
    )
    body = "\n".join(f"let r = {query_expr};" for _ in range(repeats))
    return header + body + "\n"


def make_baseline_script(rows: int) -> str:
    """Script that only generates data — used to measure gen_tf_data overhead."""
    return (
        'extern fn gen_tf_data(n: Int) -> DataFrame from "gen_tf_data.hpp";\n'
        f'let tf_data = gen_tf_data({rows});\n'
    )


def run_ibex(script: str, build_dir: pathlib.Path, plugin_dir: pathlib.Path) -> tuple[float, subprocess.CompletedProcess[str]]:
    ibex_bin = build_dir / "tools" / "ibex"
    env = os.environ.copy()
    env["IBEX_LIBRARY_PATH"] = str(plugin_dir)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ibex", delete=False) as f:
        f.write(script)
        script_path = f.name
    try:
        cmd_input = f":load {script_path}\n:quit\n"
        start = time.perf_counter()
        proc = subprocess.run(
            [str(ibex_bin)],
            cwd=REPO_ROOT,
            env=env,
            input=cmd_input,
            text=True,
            capture_output=True,
            check=False,
        )
        elapsed = time.perf_counter() - start
    finally:
        pathlib.Path(script_path).unlink(missing_ok=True)
    return elapsed, proc


def bench_operation(
    name: str,
    query_expr: str,
    rows: int,
    min_seconds: float,
    warmup: int,
    build_dir: pathlib.Path,
    plugin_dir: pathlib.Path,
    baseline_ms: float,
) -> tuple[int, float]:
    # Probe with a small repeat count to estimate per-operation time.
    probe_repeats = 3
    probe_script = make_script(rows, query_expr, probe_repeats)
    for _ in range(warmup):
        run_ibex(probe_script, build_dir, plugin_dir)
    probe_elapsed, proc = run_ibex(probe_script, build_dir, plugin_dir)
    if proc.returncode != 0 or "error:" in (proc.stdout + proc.stderr).lower():
        raise RuntimeError(f"{name} probe failed:\n{proc.stdout}\n{proc.stderr}")

    op_ms_est = max((probe_elapsed * 1000.0 - baseline_ms) / probe_repeats, 1.0)
    repeats = max(probe_repeats, math.ceil(min_seconds * 1000.0 / op_ms_est))

    bench_script = make_script(rows, query_expr, repeats)
    # Warmup the bench script (loads binary + plugin into OS page cache).
    run_ibex(bench_script, build_dir, plugin_dir)
    elapsed, proc = run_ibex(bench_script, build_dir, plugin_dir)
    if proc.returncode != 0 or "error:" in (proc.stdout + proc.stderr).lower():
        raise RuntimeError(f"{name} bench failed:\n{proc.stdout}\n{proc.stderr}")

    net_ms = elapsed * 1000.0 - baseline_ms
    avg_ms = net_ms / repeats
    return repeats, avg_ms


def main() -> int:
    ap = argparse.ArgumentParser(description="TimeFrame script benchmark")
    ap.add_argument("--rows",        type=int,   default=1_000_000)
    ap.add_argument("--min-seconds", type=float, default=2.0)
    ap.add_argument("--warmup",      type=int,   default=1)
    ap.add_argument("--build-dir",   type=pathlib.Path, default=None)
    args = ap.parse_args()

    build_dir  = args.build_dir or detect_build_dir()
    plugin_dir = build_dir / "tools"

    if not (plugin_dir / "gen_tf_data.so").exists():
        print("error: gen_tf_data.so not found in build dir", file=sys.stderr)
        print(f"       Run: cmake --build {build_dir} --target ibex_gen_tf_data_plugin", file=sys.stderr)
        return 1

    print(f"build : {build_dir}")
    print(f"rows  : {args.rows:,}")
    print(f"warmup: {args.warmup}")
    print()

    # Measure gen_tf_data overhead (startup + data generation, amortised once per script).
    baseline_script = make_baseline_script(args.rows)
    for _ in range(args.warmup):
        run_ibex(baseline_script, build_dir, plugin_dir)
    baseline_elapsed, _ = run_ibex(baseline_script, build_dir, plugin_dir)
    baseline_ms = baseline_elapsed * 1000.0
    print(f"baseline (startup + gen_tf_data): {baseline_ms:.1f} ms  [subtracted from each result]\n")

    print(f"{'benchmark':<24} {'iters':>6} {'avg_ms':>10}")
    print("-" * 44)
    results: list[tuple[str, int, float]] = []
    for name, expr in TF_QUERIES:
        try:
            iters, avg_ms = bench_operation(
                name, expr, args.rows, args.min_seconds,
                args.warmup, build_dir, plugin_dir, baseline_ms,
            )
        except RuntimeError as e:
            print(f"  ERROR: {e}", file=sys.stderr)
            return 1
        results.append((name, iters, avg_ms))
        print(f"{name:<24} {iters:>6} {avg_ms:>10.2f} ms")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
