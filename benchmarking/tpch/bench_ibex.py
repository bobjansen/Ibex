#!/usr/bin/env python3
"""Time the 22 implemented PDS-H queries in a single warm Ibex process.

`ibex_eval` starts a fresh process per invocation, which pays Arrow/AWS-SDK
dynamic-library load cost (~1-2s of `sys` time) on every single run -- not
representative of query execution speed. Instead this drives the `ibex`
REPL binary once via stdin, using its `:timing on` mode: table loads happen
once, then each query body is re-run warmup+iters times back-to-back in the
same process (so plugin loading and OS page-cache warmup happen only once,
matching Polars' own PDS-H "cached" methodology -- each iteration still
scans the Parquet file, just from a warm page cache rather than warm/reused
in-memory tables).

Two timing modes:
  default        -- each query body is split into statements piped one at a
                    time; a query's time is the sum of its per-statement
                    `time:` lines. Good for stage breakdowns, but the REPL's
                    whole-script batch planner never engages.
  --whole-script -- each query is ONE `:run <file>` of the query file (minus
                    its write_csv sink) in the warm REPL, so the batch planner
                    (source hoisting, whole-plan pushdown, join rewrites)
                    executes it the way `ibex file.ibex` would.

Usage:
  uv run benchmarking/tpch/bench_ibex.py [--whole-script] [--warmup N] [--iters N] [--out path]
"""
import argparse
import pathlib
import re
import statistics
import subprocess
import sys
import tempfile

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
IBEX_ROOT = SCRIPT_DIR.parent.parent
IBEX_BIN = IBEX_ROOT / "build-release/tools/ibex"
PLUGIN_DIR = IBEX_ROOT / "build-release/tools"
QUERIES_DIR = SCRIPT_DIR / "queries"

QUERY_NAMES = ["q01", "q02", "q03", "q04", "q05", "q06", "q07", "q08", "q09", "q10", "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22"]

TIME_RE = re.compile(r"^time:\s*([\d.]+)\s*(us|ms|s)\s*$")


def to_ms(value: float, unit: str) -> float:
    return {"us": value / 1000.0, "ms": value, "s": value * 1000.0}[unit]


def split_top_level_statements(text: str) -> list[str]:
    """Split on top-level (bracket-depth 0) ';', collapsing each statement to
    one physical line. The REPL's `:timing on` only reports a `time:` line
    for statements it reads as a single physical line -- multi-line
    (delimiter-balanced continuation) statements silently produce no timing
    output at all, so query bodies must be reflowed before being piped in.
    """
    statements = []
    depth = 0
    in_string = False
    current = []
    for ch in text:
        current.append(ch)
        if in_string:
            if ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch in "{[(":
            depth += 1
        elif ch in "}])":
            depth -= 1
        elif ch == ";" and depth == 0:
            stmt = re.sub(r"\s+", " ", "".join(current)).strip()
            if stmt:
                statements.append(stmt)
            current = []
    return statements


def extract_setup_and_body(qname: str) -> tuple[str, str, int]:
    """Split a query file into (declaration setup, repeatable body, body statement count).

    Setup = `extern fn` and `import` declarations (declared once, outside the timed
    section). Body = everything else minus the trailing `write_csv(...)`
    and bare `result;` statements, which exist only for check_answers.py
    and would otherwise add CSV-write + result-print noise to the timing.
    """
    text = (QUERIES_DIR / f"{qname}.ibex").read_text()
    lines = [line for line in text.splitlines() if line.strip() and not line.strip().startswith("//")]
    statements = split_top_level_statements("\n".join(lines))

    setup = [s for s in statements if s.startswith("extern fn") or s.startswith("import ")]
    body_statements = [
        s for s in statements
        if not s.startswith("extern fn") and not s.startswith("import ")
        and not s.startswith("write_csv(") and s != "result;"
    ]

    return "\n".join(setup), "\n".join(body_statements) + "\n", len(body_statements)


def run_repl_script(qname: str, script: str) -> list[float]:
    """Drive the warm REPL with `script` and return every `time:` line in ms."""
    proc = subprocess.run(
        [str(IBEX_BIN), "--plugin-path", str(PLUGIN_DIR)],
        input=script,
        cwd=IBEX_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{qname}: ibex REPL failed (exit {proc.returncode}):\n{proc.stderr}")
    return [to_ms(float(m.group(1)), m.group(2)) for m in map(TIME_RE.match, proc.stdout.splitlines()) if m]


def run_query(qname: str, warmup: int, iters: int) -> list[float]:
    setup, body, n_statements = extract_setup_and_body(qname)
    script_parts = [setup, ":timing on"]
    script_parts.extend([body] * (warmup + iters))
    script = "\n".join(script_parts) + "\n:quit\n"

    times_ms = run_repl_script(qname, script)
    expected = n_statements * (warmup + iters)
    if len(times_ms) != expected:
        raise RuntimeError(
            f"{qname}: expected {expected} timing lines ({n_statements} stmts x "
            f"{warmup + iters} runs), got {len(times_ms)}"
        )

    # Sum each iteration's per-statement timings into one total, drop warmup.
    per_iter = [
        sum(times_ms[i * n_statements : (i + 1) * n_statements])
        for i in range(warmup + iters)
    ]
    return per_iter[warmup:]


def run_query_whole_script(qname: str, warmup: int, iters: int, tmpdir: pathlib.Path) -> list[float]:
    """Time the query as ONE :run of the whole file (minus its write_csv sink),
    so the REPL's batch planner sees the complete script: source hoisting,
    whole-plan projection/selection pushdown, and join rewrites all engage,
    exactly as they would for `ibex file.ibex`. One `time:` line per run.
    """
    text = (QUERIES_DIR / f"{qname}.ibex").read_text()
    lines = [line for line in text.splitlines() if not line.strip().startswith("write_csv(")]
    script_path = tmpdir / f"{qname}_whole.ibex"
    script_path.write_text("\n".join(lines) + "\n")

    runs = warmup + iters
    script = "\n".join([":timing on"] + [f":run {script_path}"] * runs) + "\n:quit\n"
    times_ms = run_repl_script(qname, script)
    if len(times_ms) != runs:
        raise RuntimeError(
            f"{qname}: expected {runs} timing lines (1 per :run), got {len(times_ms)}"
        )
    return times_ms[warmup:]


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * p
    f, c = int(k), min(int(k) + 1, len(data) - 1)
    return data[f] + (data[c] - data[f]) * (k - f)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iters", type=int, default=5)
    parser.add_argument("--out", default=None)
    parser.add_argument(
        "--whole-script", action="store_true",
        help="time each query as one :run of the whole file through the batch "
             "planner, instead of summing per-statement timings")
    parser.add_argument('queries', nargs='*')
    args = parser.parse_args()

    if not IBEX_BIN.exists():
        print(f"error: {IBEX_BIN} not found — run cmake --build build-release first", file=sys.stderr)
        return 1

    default_name = "ibex_whole_script.tsv" if args.whole_script else "ibex.tsv"
    out_path = pathlib.Path(args.out) if args.out else SCRIPT_DIR / "results" / default_name
    out_path.parent.mkdir(parents=True, exist_ok=True)

    tmpdir = None
    if args.whole_script:
        tmpdir_handle = tempfile.TemporaryDirectory(prefix="ibex_ws_")
        tmpdir = pathlib.Path(tmpdir_handle.name)

    rows = []
    for qname in args.queries or QUERY_NAMES:
        print(f"=== ibex {qname} ===", file=sys.stderr)
        if args.whole_script:
            durations = run_query_whole_script(qname, args.warmup, args.iters, tmpdir)
        else:
            durations = run_query(qname, args.warmup, args.iters)
        avg_ms = statistics.mean(durations)
        print(f"  avg={avg_ms:.2f}ms min={min(durations):.2f}ms max={max(durations):.2f}ms", file=sys.stderr)
        rows.append({
            "framework": "ibex",
            "query": qname,
            "avg_ms": avg_ms,
            "min_ms": min(durations),
            "max_ms": max(durations),
            "stddev_ms": statistics.pstdev(durations) if len(durations) > 1 else 0.0,
            "p95_ms": percentile(durations, 0.95),
            "p99_ms": percentile(durations, 0.99),
        })

    with open(out_path, "w") as f:
        f.write("framework\tquery\tavg_ms\tmin_ms\tmax_ms\tstddev_ms\tp95_ms\tp99_ms\n")
        for r in rows:
            f.write(
                f"{r['framework']}\t{r['query']}\t{r['avg_ms']:.3f}\t{r['min_ms']:.3f}\t"
                f"{r['max_ms']:.3f}\t{r['stddev_ms']:.3f}\t{r['p95_ms']:.3f}\t{r['p99_ms']:.3f}\n"
            )
    print(f"results written to {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
