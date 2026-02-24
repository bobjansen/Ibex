#!/usr/bin/env python3
"""Benchmark examples/quant.* across ibex, pandas, and polars.

Usage:
  uv run --project benchmarking benchmarking/bench_quant.py
  uv run --project benchmarking benchmarking/bench_quant.py --min-seconds 3 --scale 50
"""

from __future__ import annotations

import argparse
import math
import os
import pathlib
import shutil
import subprocess
import sys
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-seconds", type=float, default=3.0, help="minimum runtime per framework")
    parser.add_argument("--scale", type=int, default=50, help="duplicate OHLCV rows N times for heavier workload")
    parser.add_argument("--build-dir", type=pathlib.Path, help="ibex build dir (auto: build then build-release)")
    parser.add_argument("--profile-steps", action="store_true", help="profile ibex cumulative and per-step costs")
    parser.add_argument("--profile-runs", type=int, default=3, help="runs per step for ibex step profile")
    parser.add_argument(
        "--profile-repeat",
        type=int,
        default=8,
        help="repeat each profiled step bundle N times inside one ibex load",
    )
    parser.add_argument(
        "--polars-modes",
        choices=["both", "multi", "single"],
        default="both",
        help="run polars in multi-threaded, single-threaded, or both modes",
    )
    return parser.parse_args()


def ensure_scaled_ohlcv(src: pathlib.Path, dst: pathlib.Path, scale: int) -> None:
    if scale < 1:
        raise ValueError("--scale must be >= 1")
    if dst.exists():
        return

    dst.parent.mkdir(parents=True, exist_ok=True)
    with src.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    if not lines:
        raise RuntimeError(f"empty CSV: {src}")

    header = lines[0]
    rows = lines[1:]
    with dst.open("w", encoding="utf-8") as out:
        out.write(header)
        for _ in range(scale):
            out.writelines(rows)


def create_scaled_ibex_script(src_ibex: pathlib.Path, dst_ibex: pathlib.Path, ohlcv: pathlib.Path, fund: pathlib.Path) -> None:
    text = src_ibex.read_text(encoding="utf-8")
    text = text.replace("examples/data/ohlcv.csv", str(ohlcv))
    text = text.replace("examples/data/fundamentals.csv", str(fund))
    dst_ibex.parent.mkdir(parents=True, exist_ok=True)
    dst_ibex.write_text(text, encoding="utf-8")


IBEX_QUANT_STEPS = [
    """let daily = ohlcv[update {
    ret      = (close - open) / open,
    range    = (high  - low)  / open,
    notional = close * volume
}];""",
    """let annual = daily[select {
    open_price     = first(open),
    year_high      = max(high),
    year_low       = min(low),
    close_price    = last(close),
    avg_daily_vol  = mean(volume),
    total_notional = sum(notional)
}, by symbol];""",
    """let sector_perf = daily[select {
    avg_daily_ret  = mean(ret),
    total_notional = sum(notional),
    avg_range      = mean(range),
    n_sessions     = count()
}, by sector]
[order { avg_daily_ret desc }];""",
    """let up_stats = daily[filter ret > 0.0,
                     select { up_days    = count(),
                              avg_up_ret = mean(ret) },
                     by symbol];
let up_rank = up_stats[order { up_days desc }];""",
    """let vol_rank = daily[select {
    avg_range     = mean(range),
    max_range_day = max(range),
    n_sessions    = count()
}, by symbol]
[order { avg_range desc }];""",
    """let stress = daily[filter range > 0.025,
      select { sessions = count(), avg_vol = mean(volume), avg_range = mean(range) },
      by sector]
[order { sessions desc }];""",
    """let avg_vol_base = ohlcv[select { avg_volume = mean(volume) }, by symbol];
let enriched_vol = ohlcv join avg_vol_base on symbol;
let with_ratio   = enriched_vol[update { vol_ratio = volume / avg_volume }];
let spikes       = with_ratio[filter vol_ratio > 1.8];
let spike_summary = spikes[select { spike_days = count(), max_ratio = max(vol_ratio) }, by symbol]
     [order { spike_days desc }];""",
    """let vwap_sums = daily[select {
    sum_notional = sum(notional),
    sum_volume   = sum(volume)
}, by symbol];
let vwap = vwap_sums[update { vwap = sum_notional / sum_volume }]
         [order { vwap desc }];""",
    """let enriched = daily join fund on symbol;
let sym_stats = enriched[select {
    avg_ret        = mean(ret),
    avg_range      = mean(range),
    avg_volume     = mean(volume),
    pe_ratio       = first(pe_ratio),
    beta           = first(beta),
    market_cap     = first(market_cap_bn),
    div_yield      = first(div_yield),
    analyst_rating = first(analyst_rating)
}, by symbol];""",
    """let value_mom = sym_stats[filter pe_ratio < 25.0][filter avg_ret > 0.0][order { avg_ret desc }];
let income_def = sym_stats[filter div_yield > 2.0][filter beta < 1.0][order { div_yield desc }];
let mega_cap = sym_stats[filter market_cap > 400.0]
         [order { avg_ret desc }];""",
    """let sharpe_like = sym_stats[update { sharpe_proxy = avg_ret / avg_range }]
         [order { sharpe_proxy desc }];
let rated = sym_stats[filter analyst_rating > 4.0]
         [order { avg_ret desc }];""",
    """let sector_macro = enriched[select {
    avg_ret    = mean(ret),
    avg_beta   = mean(beta),
    avg_pe     = mean(pe_ratio),
    total_notl = sum(notional),
    n_sessions = count()
}, by sector]
[order { total_notl desc }];""",
]

IBEX_QUANT_STEP_LABELS = [
    "daily update",
    "annual by symbol",
    "sector rank",
    "up-day momentum",
    "volatility rank",
    "stress by sector",
    "volume spikes",
    "vwap",
    "enrich + sym_stats",
    "screens",
    "sharpe + rating",
    "sector macro",
]


def ibex_quant_script_text(ohlcv: pathlib.Path, fund: pathlib.Path, steps: list[str], repeats: int) -> str:
    script = [
        'extern fn read_csv(path: String) -> DataFrame from "csv.hpp";',
        f'let ohlcv = read_csv("{ohlcv}");',
        f'let fund = read_csv("{fund}");',
    ]
    block = "\n\n".join(steps)
    script.extend(block for _ in range(repeats))
    return "\n\n".join(script) + "\n"


def write_ibex_compute_script(path: pathlib.Path, ohlcv: pathlib.Path, fund: pathlib.Path, repeats: int) -> None:
    text = ibex_quant_script_text(ohlcv, fund, IBEX_QUANT_STEPS, repeats)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def detect_build_dir(repo_root: pathlib.Path, requested: pathlib.Path | None) -> pathlib.Path:
    if requested is not None:
        return requested
    for candidate in [repo_root / "build-release", repo_root / "build"]:
        if (candidate / "tools" / "ibex").exists():
            return candidate
    raise RuntimeError("could not find ibex binary (expected build/tools/ibex or build-release/tools/ibex)")


def run_ibex_script(repo_root: pathlib.Path, build_dir: pathlib.Path, ibex_script: pathlib.Path) -> tuple[float, subprocess.CompletedProcess[str]]:
    ibex_bin = build_dir / "tools" / "ibex"
    if not ibex_bin.exists():
        raise RuntimeError(f"ibex binary not found: {ibex_bin}")

    plugin_dir = build_dir / "libs" / "csv"
    if not plugin_dir.exists():
        plugin_dir = build_dir / "libraries"
    env = os.environ.copy()
    env["IBEX_LIBRARY_PATH"] = str(plugin_dir)
    cmd_input = f":load {ibex_script}\n:quit\n"
    start = time.perf_counter()
    proc = subprocess.run(
        [str(ibex_bin)],
        cwd=repo_root,
        env=env,
        input=cmd_input,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed = time.perf_counter() - start
    return elapsed, proc


def bench_ibex_compute_only(
    repo_root: pathlib.Path, build_dir: pathlib.Path, ohlcv_scaled: pathlib.Path, fund_src: pathlib.Path, min_seconds: float
) -> tuple[int, float, float, pathlib.Path]:
    script_dir = repo_root / "benchmarking" / "data" / "quant_compute"
    probe = script_dir / "quant_probe.ibex"
    write_ibex_compute_script(probe, ohlcv_scaled, fund_src, repeats=1)

    _, warm = run_ibex_script(repo_root, build_dir, probe)
    if warm.returncode != 0 or "error:" in warm.stdout.lower() or "error:" in warm.stderr.lower():
        raise RuntimeError(f"ibex failed during warmup:\n{warm.stdout}\n{warm.stderr}")

    probe_total, probe_proc = run_ibex_script(repo_root, build_dir, probe)
    if probe_proc.returncode != 0 or "error:" in probe_proc.stdout.lower() or "error:" in probe_proc.stderr.lower():
        raise RuntimeError(f"ibex failed during probe:\n{probe_proc.stdout}\n{probe_proc.stderr}")

    repeats = max(1, int(math.ceil(min_seconds / max(probe_total, 1e-6))))
    bench_script = script_dir / f"quant_compute_{repeats}.ibex"
    write_ibex_compute_script(bench_script, ohlcv_scaled, fund_src, repeats=repeats)

    _, warm2 = run_ibex_script(repo_root, build_dir, bench_script)
    if warm2.returncode != 0 or "error:" in warm2.stdout.lower() or "error:" in warm2.stderr.lower():
        raise RuntimeError(f"ibex failed during warmup2:\n{warm2.stdout}\n{warm2.stderr}")

    total, proc = run_ibex_script(repo_root, build_dir, bench_script)
    if proc.returncode != 0 or "error:" in proc.stdout.lower() or "error:" in proc.stderr.lower():
        raise RuntimeError(f"ibex failed:\n{proc.stdout}\n{proc.stderr}")

    if total < min_seconds and total > 0.0:
        scale_up = int(math.ceil(min_seconds / total))
        repeats *= max(scale_up, 1)
        bench_script = script_dir / f"quant_compute_{repeats}.ibex"
        write_ibex_compute_script(bench_script, ohlcv_scaled, fund_src, repeats=repeats)
        _, warm3 = run_ibex_script(repo_root, build_dir, bench_script)
        if warm3.returncode != 0 or "error:" in warm3.stdout.lower() or "error:" in warm3.stderr.lower():
            raise RuntimeError(f"ibex failed during warmup3:\n{warm3.stdout}\n{warm3.stderr}")
        total, proc = run_ibex_script(repo_root, build_dir, bench_script)
        if proc.returncode != 0 or "error:" in proc.stdout.lower() or "error:" in proc.stderr.lower():
            raise RuntimeError(f"ibex failed:\n{proc.stdout}\n{proc.stderr}")

    avg_ms = 1000.0 * total / repeats
    return repeats, total, avg_ms, bench_script


def profile_ibex_steps(
    repo_root: pathlib.Path,
    build_dir: pathlib.Path,
    ohlcv_scaled: pathlib.Path,
    fund_src: pathlib.Path,
    runs: int,
    repeat: int,
) -> list[tuple[int, str, float, float]]:
    script_dir = repo_root / "benchmarking" / "data" / "quant_compute"
    rows: list[tuple[int, str, float, float]] = []
    cumulative_prev = 0.0
    repeat = max(repeat, 1)

    for step_count in range(1, len(IBEX_QUANT_STEPS) + 1):
        path = script_dir / f"quant_profile_{step_count}.ibex"
        text = ibex_quant_script_text(ohlcv_scaled, fund_src, IBEX_QUANT_STEPS[:step_count], repeats=repeat)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

        total = 0.0
        for _ in range(max(runs, 1)):
            elapsed, proc = run_ibex_script(repo_root, build_dir, path)
            if proc.returncode != 0 or "error:" in proc.stdout.lower() or "error:" in proc.stderr.lower():
                raise RuntimeError(f"ibex step profile failed at step {step_count}:\n{proc.stdout}\n{proc.stderr}")
            total += elapsed
        cumulative_ms = 1000.0 * total / (max(runs, 1) * repeat)
        delta_ms = cumulative_ms - cumulative_prev
        label = IBEX_QUANT_STEP_LABELS[step_count - 1]
        rows.append((step_count, label, cumulative_ms, delta_ms))
        cumulative_prev = cumulative_ms

    return rows


def run_cmd(cmd: list[str], cwd: pathlib.Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, check=False, env=env)


def parse_py_benchmark(output: str, label: str) -> tuple[int, float, float]:
    line = ""
    for candidate in output.splitlines():
        if candidate.startswith(f"{label} benchmark:"):
            line = candidate
            break
    if not line:
        raise ValueError(f"Could not parse {label} benchmark output:\n{output}")

    parts = [p.strip() for p in line.split(",")]
    iters = int(parts[0].split("iterations=")[1])
    total = float(parts[1].split("total=")[1].removesuffix("s"))
    avg_ms = float(parts[2].split("avg=")[1].split()[0])
    return iters, total, avg_ms


def ensure_uv() -> str:
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv not found in PATH")
    return uv


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    build_dir = detect_build_dir(repo_root, args.build_dir)
    uv = ensure_uv()

    scaled_dir = repo_root / "benchmarking" / "data" / f"quant_scale_{args.scale}"
    ohlcv_src = repo_root / "examples" / "data" / "ohlcv.csv"
    fund_src = repo_root / "examples" / "data" / "fundamentals.csv"
    ohlcv_scaled = scaled_dir / "ohlcv.csv"
    ibex_script = scaled_dir / "quant_scaled.ibex"

    ensure_scaled_ohlcv(ohlcv_src, ohlcv_scaled, args.scale)
    create_scaled_ibex_script(repo_root / "examples" / "quant.ibex", ibex_script, ohlcv_scaled, fund_src)

    polars_cmd = [
        uv,
        "run",
        "../examples/quant_polars.py",
        "--benchmark",
        "--min-seconds",
        str(args.min_seconds),
        "--ohlcv-path",
        str(ohlcv_scaled),
        "--fundamentals-path",
        str(fund_src),
    ]
    pandas_cmd = [
        uv,
        "run",
        "../examples/quant_pandas.py",
        "--benchmark",
        "--min-seconds",
        str(args.min_seconds),
        "--ohlcv-path",
        str(ohlcv_scaled),
        "--fundamentals-path",
        str(fund_src),
    ]

    plugin_dir = build_dir / "libs" / "csv"
    if not plugin_dir.exists():
        plugin_dir = build_dir / "libraries"
    print(f"scale={args.scale}, min_seconds={args.min_seconds}")
    print("mode      : compute-only")
    print("polars cmd: " + " ".join(polars_cmd))
    if args.polars_modes in ("both", "single"):
        print("polars st : POLARS_MAX_THREADS=1")
    print("pandas cmd: " + " ".join(pandas_cmd))

    polars_results: dict[str, tuple[int, float, float]] = {}
    if args.polars_modes in ("both", "multi"):
        polars_proc = run_cmd(polars_cmd, repo_root / "benchmarking")
        if polars_proc.returncode != 0:
            print(polars_proc.stderr or polars_proc.stdout, file=sys.stderr)
            return 1
        polars_results["polars-mt"] = parse_py_benchmark(polars_proc.stdout, "polars")
    if args.polars_modes in ("both", "single"):
        polars_env = os.environ.copy()
        polars_env["POLARS_MAX_THREADS"] = "1"
        polars_proc_st = run_cmd(polars_cmd, repo_root / "benchmarking", env=polars_env)
        if polars_proc_st.returncode != 0:
            print(polars_proc_st.stderr or polars_proc_st.stdout, file=sys.stderr)
            return 1
        polars_results["polars-st"] = parse_py_benchmark(polars_proc_st.stdout, "polars")

    pandas_proc = run_cmd(pandas_cmd, repo_root / "benchmarking")
    if pandas_proc.returncode != 0:
        print(pandas_proc.stderr or pandas_proc.stdout, file=sys.stderr)
        return 1
    pandas_stats = parse_py_benchmark(pandas_proc.stdout, "pandas")

    try:
        ibex_iters, ibex_total, ibex_avg, ibex_compute_script = bench_ibex_compute_only(
            repo_root, build_dir, ohlcv_scaled, fund_src, args.min_seconds
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        "ibex cmd  : "
        f"echo ':load {ibex_compute_script}' | "
        f"IBEX_LIBRARY_PATH={plugin_dir} {build_dir / 'tools' / 'ibex'} >/dev/null"
    )

    ibex_stats = (ibex_iters, ibex_total, ibex_avg)

    frameworks = {"ibex": ibex_stats, **polars_results, "pandas": pandas_stats}

    print("\nquant benchmark")
    print(f"{'framework':10} {'iters':>7} {'total_s':>10} {'avg_ms':>10} {'vs_ibex':>10}")
    print("-" * 56)
    ibex_avg = frameworks["ibex"][2]
    order = ["ibex"]
    if "polars-mt" in frameworks:
        order.append("polars-mt")
    if "polars-st" in frameworks:
        order.append("polars-st")
    order.append("pandas")
    for name in order:
        iters, total_s, avg_ms = frameworks[name]
        ratio = avg_ms / ibex_avg if ibex_avg > 0 else math.nan
        ratio_text = "1.00x" if name == "ibex" else f"{ratio:.2f}x"
        print(f"{name:10} {iters:7d} {total_s:10.3f} {avg_ms:10.2f} {ratio_text:>10}")

    if args.profile_steps:
        try:
            rows = profile_ibex_steps(
                repo_root,
                build_dir,
                ohlcv_scaled,
                fund_src,
                args.profile_runs,
                args.profile_repeat,
            )
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print(
            f"\nibex step profile (avg over {max(args.profile_runs, 1)} runs, "
            f"{max(args.profile_repeat, 1)} repeats/load)"
        )
        print(f"{'step':>4} {'label':22} {'cumulative_ms':>14} {'delta_ms':>10}")
        print("-" * 60)
        for step, label, cumulative_ms, delta_ms in rows:
            print(f"{step:4d} {label:22} {cumulative_ms:14.2f} {delta_ms:10.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
