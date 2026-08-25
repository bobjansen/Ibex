#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Interleaved A/B of two `ibex_eval` binaries over a set of .ibex queries.

Answers one question: did this change make these queries faster, slower, or
neither, and did it change any output.

`compare_ibex_git.sh` is the tool for comparing two git STATES — it builds each
side in its own temporary worktree and drives the `ibex_bench` suite. This one
compares two BINARIES over .ibex query files, which is what you want when the
thing you care about is a PDS-H query rather than a micro benchmark, and when
you already have both builds.

Build the base side without disturbing your tree:

    cp build-release/tools/ibex_eval /tmp/eval_target
    git stash -q
    CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval
    cp build-release/tools/ibex_eval /tmp/eval_base
    git stash pop -q
    CMAKE_BUILD_PARALLEL_LEVEL=6 cmake --build build-release --target ibex_eval

That last rebuild is not optional: after `git stash pop` the binary sitting in
`build-release/` is still the BASE build, and forgetting it means measuring the
base against itself while believing otherwise.

    python3 benchmarking/ab_queries.py --base /tmp/eval_base --target /tmp/eval_target

Plugins come from the CURRENT `build-release/tools` for both sides. That is
correct only while the change does not alter the plugin ABI (anything in
`Table`/`Chunk` layout, `LazyTable` members, or `ColumnDecodeFn`). If it does,
each side needs its own plugin build and this script is the wrong tool.

How the verdict is decided, and why it is not a fixed percentage band:

* **`min`, not median, is the estimator.** Benchmark noise is one-sided —
  scheduling, contention and cache pollution only ever make a run slower, never
  faster than the work actually costs. The minimum of N runs is therefore the
  best estimate of the true cost, and it is far more stable than the median.
  The median is printed too, so you can see when they disagree (they diverge
  when the box is busy, which is a reason to stop and re-run, not to average).
* **A Wilcoxon signed-rank test on paired runs decides significance.** Sides are
  interleaved, so run `i` of each pair sees near-identical machine conditions,
  and the paired differences are the evidence. Signed-rank rather than a plain
  sign test because the sign test needs ALL 7 pairs to agree to reach p<0.05 —
  verified by injecting a known +10% regression, which came back 6-1 and
  p=0.125, a false negative on an effect that was there by construction. Using
  the magnitude of each pair's difference fixes it. This scales with
  `--repeats`, unlike a fixed band.
* `--min-effect` (default 2%) is a *practical* significance floor, applied on
  top of the statistical one. A change can be perfectly consistent and still too
  small to care about.
* A wide fixed band was the previous design and it was wrong. It came from an
  observation that two untouched queries moved ±13% in single configurations —
  but the lesson there was to corroborate a delta, not to declare everything
  under 13% invisible, which throws away the 5-8% wins that are most of what
  real optimizations deliver.
* `geomean` over per-query ratios is the headline, not the sum of times — a sum
  is dominated by whichever query happens to be longest.
* Byte-identity is checked by default and is not a performance question: the
  engine's contract is that output does not depend on how it was computed.
"""

import argparse
import itertools

import math
import pathlib
import statistics
import subprocess
import sys
import time

import bench_env

ROOT = pathlib.Path(__file__).resolve().parent.parent
DEFAULT_QUERY_DIR = ROOT / "benchmarking/tpch/queries"
PERF_LOG = ROOT / "build-release/post_commit_perf.log"


def warn_if_busy() -> None:
    """The post-commit hook runs a scale regression in the background."""
    if PERF_LOG.exists() and time.time() - PERF_LOG.stat().st_mtime < 180:
        print(
            "WARNING: build-release/post_commit_perf.log was written in the last "
            "3 minutes.\n         The post-commit perf hook is probably still "
            "running and will inflate\n         these numbers. Wait for it, or "
            "commit with IBEX_SKIP_PERF=1.\n",
            file=sys.stderr,
        )


def wilcoxon_p(diffs: list[float]) -> float:
    """Two-sided Wilcoxon signed-rank on paired per-repeat differences.

    A plain sign test was tried first and is not sensitive enough at these
    sample sizes: with 7 pairs it needs all 7 to agree to reach p<0.05, so a
    single unlucky repeat hides a real effect. Injecting a known +10%
    regression produced 6 losses, 1 win, p=0.125 — a false negative on an
    effect that was there by construction.

    Signed-rank fixes that by using the SIZE of each pair's difference and not
    just its direction: one dissenting pair whose difference is tiny barely
    counts against the result, which is exactly the right treatment for a run
    that hit a scheduling hiccup.

    Exact by enumeration for n <= 15 (32768 sign assignments, instant); normal
    approximation above that. Ties on zero are dropped; ties in magnitude get
    averaged ranks.
    """
    nonzero = [d for d in diffs if d != 0.0]
    n = len(nonzero)
    if n == 0:
        return 1.0
    order = sorted(range(n), key=lambda i: abs(nonzero[i]))
    ranks = [0.0] * n
    i = 0
    while i < n:
        j = i
        while j + 1 < n and abs(nonzero[order[j + 1]]) == abs(nonzero[order[i]]):
            j += 1
        average = (i + j + 2) / 2.0  # ranks are 1-based
        for k in range(i, j + 1):
            ranks[order[k]] = average
        i = j + 1
    total_rank = sum(ranks)
    w_plus = sum(r for r, d in zip(ranks, nonzero) if d > 0)
    w = min(w_plus, total_rank - w_plus)
    if n <= 15:
        hits = 0
        for signs in itertools.product((0, 1), repeat=n):
            s = sum(r for r, sign in zip(ranks, signs) if sign)
            if min(s, total_rank - s) <= w + 1e-9:
                hits += 1
        return min(1.0, hits / (2 ** n))
    mean = n * (n + 1) / 4.0
    sd = math.sqrt(n * (n + 1) * (2 * n + 1) / 24.0)
    if sd == 0.0:
        return 1.0
    return min(1.0, math.erfc(abs(w - mean) / sd / math.sqrt(2.0)))


def run_once(binary: pathlib.Path, query: pathlib.Path, args: argparse.Namespace
             ) -> tuple[float, str]:
    """One timed run. Returns (milliseconds, stdout)."""
    cmd: list[str] = []
    if args.taskset:
        cmd += ["taskset", "-c", args.taskset]
    cmd += [str(binary), "--plugin-path", str(args.plugin_path), str(query)]
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(pathlib.Path.home()),
        "IBEX_CORES": str(args.cores),
    }
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True,
                          text=True, timeout=args.timeout)
    elapsed = (time.perf_counter() - start) * 1000.0
    if proc.returncode != 0:
        raise RuntimeError(
            f"{binary.name} failed on {query.name} (rc={proc.returncode}):\n"
            f"{proc.stderr[-600:]}"
        )
    return elapsed, proc.stdout


def compare(query: pathlib.Path, args: argparse.Namespace) -> dict | None:
    # Warm both sides: first touch pays cold page cache on the Parquet files.
    for _ in range(args.warmup):
        run_once(args.base, query, args)
        run_once(args.target, query, args)

    base_ms: list[float] = []
    target_ms: list[float] = []
    base_out = target_out = ""
    for i in range(args.repeats):
        # Alternate which side goes first. Running first is measurably WORSE —
        # the second run of a pair inherits a warm page cache and a
        # already-ramped CPU — so whichever side leads more often is penalised.
        # `--repeats` is forced even for exactly this reason: with 7 repeats
        # base leads 4 times and target 3, and that alone reported a same-binary
        # comparison as "FASTER -12.6%, p=0.047". A confident false positive
        # from nothing but an odd loop count.
        if i % 2 == 0:
            b, base_out = run_once(args.base, query, args)
            t, target_out = run_once(args.target, query, args)
        else:
            t, target_out = run_once(args.target, query, args)
            b, base_out = run_once(args.base, query, args)
        base_ms.append(b)
        target_ms.append(t)

    # Reported COST is the minimum: noise here is one-sided, so the fastest
    # observed run is the closest estimate of what the work actually costs.
    base = min(base_ms)
    target = min(target_ms)

    # Reported EFFECT is the median of the per-pair ratios, NOT the ratio of the
    # two minima. The effect size must be computed from the same paired data the
    # significance test uses, or the two disagree and the table becomes
    # unreadable: on a same-binary run of q01 the min-ratio said "+5.1%" while
    # the paired test said p=0.959, because two independent minima each chase
    # their own luckiest run. Pairing cancels that.
    ratios = [t / b for b, t in zip(base_ms, target_ms) if b > 0]
    ratio = statistics.median(ratios) if ratios else 1.0
    delta = 100.0 * (ratio - 1.0)

    # Paired Wilcoxon signed-rank. Run i of each pair ran under near-identical machine
    # conditions, so pair-by-pair direction is the evidence; the size of the gap
    # is the effect, and the two questions are kept separate on purpose.
    wins = sum(1 for b, t in zip(base_ms, target_ms) if t < b)
    losses = sum(1 for b, t in zip(base_ms, target_ms) if t > b)
    pvalue = wilcoxon_p([t - b for b, t in zip(base_ms, target_ms)])

    identical = base_out == target_out
    if not identical:
        verdict = "DIFFERS"
    elif abs(delta) < args.min_effect:
        verdict = "same"          # too small to care about, regardless of p
    elif pvalue > args.alpha:
        verdict = "unclear"       # plausible effect, not enough evidence yet
    else:
        verdict = "FASTER" if delta < 0 else "SLOWER"

    med_base = statistics.median(base_ms)
    med_target = statistics.median(target_ms)
    return {
        "name": query.stem, "base": base, "target": target, "delta": delta,
        "ratio": ratio,
        "identical": identical, "verdict": verdict, "pvalue": pvalue,
        "wins": wins, "losses": losses,
        # How far the median sits above the min, on the base side. Small means a
        # quiet box; large means most runs were contaminated and the min is
        # carrying the estimate alone.
        "dispersion": 100.0 * (med_base - base) / base if base > 0 else 0.0,
        "median_delta": (100.0 * (med_target - med_base) / med_base
                         if med_base > 0 else 0.0),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", type=pathlib.Path, required=True,
                    help="baseline ibex_eval binary")
    ap.add_argument("--target", type=pathlib.Path, required=True,
                    help="candidate ibex_eval binary")
    ap.add_argument("--queries", nargs="*", default=None,
                    help="query names (q01 q06) or .ibex paths; default all PDS-H")
    ap.add_argument("--query-dir", type=pathlib.Path, default=DEFAULT_QUERY_DIR)
    ap.add_argument("--plugin-path", type=pathlib.Path,
                    default=ROOT / "build-release/tools")
    ap.add_argument("--cores", default="8", help="IBEX_CORES (default 8)")
    ap.add_argument("--repeats", type=int, default=8,
                    help="paired runs per query (default 8). Rounded UP to an "
                         "even number so first-position bias cancels. Raise it "
                         "when a query comes back 'unclear'")
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--taskset", default=None, help="pin with taskset -c CPUSET")
    ap.add_argument("--min-effect", type=float, default=2.0,
                    help="practical-significance floor in percent; smaller "
                         "differences are reported as 'same' however consistent "
                         "(default 2)")
    ap.add_argument("--alpha", type=float, default=0.05,
                    help="sign-test significance level (default 0.05)")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--no-verify", action="store_true",
                    help="skip the byte-identity check (do not use for an "
                         "operator change)")
    args = ap.parse_args()
    if args.repeats % 2 == 1:
        args.repeats += 1  # keep the lead position balanced between the sides

    for binary in (args.base, args.target):
        if not binary.exists():
            print(f"missing binary: {binary}", file=sys.stderr)
            return 1
    if args.base.resolve() == args.target.resolve():
        print("NOTE: base and target are the same file — this measures the "
              "harness's own noise, which is a useful thing to do.\n",
              file=sys.stderr)

    if args.queries:
        queries = [
            pathlib.Path(q) if q.endswith(".ibex") else args.query_dir / f"{q}.ibex"
            for q in args.queries
        ]
    else:
        queries = sorted(args.query_dir.glob("q??.ibex"))
    if not queries:
        print(f"no queries found in {args.query_dir}", file=sys.stderr)
        return 1

    warn_if_busy()
    print(bench_env.scale_factor_line())
    print(f"# base={args.base}  target={args.target}")
    print(f"# IBEX_CORES={args.cores} repeats={args.repeats} "
          f"warmup={args.warmup} min_effect={args.min_effect}% alpha={args.alpha}"
          + (f" taskset={args.taskset}" if args.taskset else ""))
    print("# base_ms/target_ms are the MIN of the repeats (cost); delta is the "
          "MEDIAN of\n#   the per-pair ratios (effect), which is what the p-value "
          "tests. disp = how far\n#   the base median sits above its min — large "
          "means the box was busy.")
    print(f"{'query':8}{'base_ms':>10}{'target_ms':>11}{'delta':>9}"
          f"{'disp':>7}{'pairs':>8}{'p':>8}  {'verdict':<9}{'output':>9}")

    rows = []
    for query in queries:
        if not query.exists():
            print(f"{query.stem:8}{'MISSING':>10}", file=sys.stderr)
            continue
        try:
            row = compare(query, args)
        except RuntimeError as exc:
            print(f"{query.stem:8}  FAILED: {exc}", file=sys.stderr)
            continue
        rows.append(row)
        out = "same" if row["identical"] else "DIFFERS"
        pairs = f"{row['wins']}-{row['losses']}"
        print(f"{row['name']:8}{row['base']:10.1f}{row['target']:11.1f}"
              f"{row['delta']:8.1f}%{row['dispersion']:6.1f}%{pairs:>8}"
              f"{row['pvalue']:8.3f}  {row['verdict']:<9}{out:>9}")

    if not rows:
        print("no results", file=sys.stderr)
        return 1

    geo = math.exp(sum(math.log(r["ratio"]) for r in rows) / len(rows))
    base_total = sum(r["base"] for r in rows)
    target_total = sum(r["target"] for r in rows)
    differing = [r["name"] for r in rows if not r["identical"]]
    # No delta on the TOTAL row on purpose. Summing minima and differencing
    # them is a DIFFERENT estimator from the per-query paired median, and the
    # two disagree loudly when one side's minimum happens to land on an unlucky
    # run — a validation run with a known +6.9% effect showed +19.6% here. The
    # totals are printed as an absolute reference for scale; the geomean of the
    # paired per-query ratios is the headline, and it is the same quantity the
    # p-values test.
    print(f"{'TOTAL':8}{base_total:10.1f}{target_total:11.1f}"
          f"{'—':>9}   (summed min cost, for scale only)")
    print(f"# geomean of per-query paired ratios: {geo:.4f} "
          f"({100 * (geo - 1):+.1f}%)  <- the headline")

    if args.no_verify:
        print("# byte-identity NOT checked (--no-verify)")
    elif differing:
        print(f"# OUTPUT CHANGED on {len(differing)} quer"
              f"{'y' if len(differing) == 1 else 'ies'}: {' '.join(differing)}")
        print("# This is a correctness result, not a perf one. Stop and explain "
              "it before reading any timing above.")
        return 2
    else:
        print(f"# byte-identical on all {len(rows)} queries")

    # Holm-Bonferroni across the queries. Testing 22 queries at alpha=0.05 means
    # roughly one spurious verdict per run BY CONSTRUCTION, and a suite A/B is
    # exactly the setting where someone reads that one row as the finding. Holm
    # is uniformly more powerful than plain Bonferroni and needs no independence
    # assumption, which matters here because the queries share a machine.
    flagged = [r for r in rows if r["verdict"] in ("FASTER", "SLOWER")]
    if flagged and len(rows) > 1:
        ordered = sorted(flagged, key=lambda r: r["pvalue"])
        survivors = []
        for rank, row in enumerate(ordered):
            threshold = args.alpha / (len(rows) - rank)
            if row["pvalue"] <= threshold:
                survivors.append(row["name"])
            else:
                break  # Holm stops at the first failure
        rejected = [r["name"] for r in ordered if r["name"] not in survivors]
        if rejected:
            print(f"# MULTIPLE COMPARISONS: {len(rows)} queries tested at "
                  f"alpha={args.alpha}, so ~{len(rows) * args.alpha:.0f} false "
                  f"verdict(s) are expected per run.")
            print(f"#   Holm-Bonferroni keeps: "
                  f"{' '.join(survivors) if survivors else 'NOTHING'}")
            print(f"#   does NOT survive: {' '.join(rejected)} — do not report "
                  f"{'these' if len(rejected) > 1 else 'this'} as a result "
                  f"without an independent re-run.")

    same = sum(1 for r in rows if r["verdict"] == "same")
    unclear = [r["name"] for r in rows if r["verdict"] == "unclear"]
    moved = [r for r in rows if r["verdict"] in ("FASTER", "SLOWER")]
    if same == len(rows):
        print("# every query is under the practical-effect floor — report this "
              "as a wash, not a win")
    if unclear:
        print(f"# UNCLEAR on {len(unclear)}: {' '.join(unclear)} — an effect "
              f"above {args.min_effect}% that the paired test cannot separate from "
              f"noise. Re-run those with more --repeats before claiming either "
              f"way.")
    if moved:
        worst = max(r["dispersion"] for r in rows)
        if worst > 15.0:
            print(f"# CAUTION: base median sits {worst:.0f}% above its min on at "
                  "least one query. The box was not quiet; corroborate before "
                  "trusting any verdict here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
