"""Per-query peak-RSS measurement shared by the Python benchmark harnesses.

The metric is *absolute* peak resident set size (VmHWM) during a query's
measured iterations, in MiB — including the already-resident base dataframe,
so it is the "memory footprint to run this op" comparable across engines.

Mechanism (Linux only): writing "5" to /proc/self/clear_refs resets the
kernel's per-process VmHWM peak; we reset just before the timed iterations and
read VmHWM from /proc/self/status afterwards. On platforms without these
/proc files (macOS, Windows) reset_peak_rss() is a no-op and peak_rss_mb()
returns 0.0, so harnesses degrade to a blank memory cell rather than failing.
"""
import os
import sys

_CLEAR_REFS = "/proc/self/clear_refs"
_STATUS = "/proc/self/status"
_SUPPORTED = sys.platform.startswith("linux")

# Dynamic per-cell cutoff: if a single (warmup) iteration of a query exceeds this
# many milliseconds, the harness cuts the cell — it skips the remaining measured
# iterations and emits a sentinel row (avg_ms < 0) the suite drops, so one
# pathologically slow op (common at the largest scales) can't dominate the run's
# wall-clock. Override via env.
CELL_CUTOFF_MS = float(os.environ.get("IBEX_CELL_CUTOFF_MS", "120000"))  # 2 min

# Carry-forward skip set: cells cut at a smaller scale (the suite passes them back
# via IBEX_SKIP_CELLS as comma-separated "framework|query"). A cell in this set is
# skipped outright — no warm iteration at all — so a cell that blew the budget at,
# say, 2M is never even attempted (and re-paid as an ever-slower warm iteration) at
# 4M/8M/.../50M. IBEX_FW_SUFFIX lets a single-thread (-st) invocation, whose rows
# are tagged with the base framework then renamed by the suite, match its public
# name (e.g. internal "polars" + "-st" -> "polars-st").
_FW_SUFFIX = os.environ.get("IBEX_FW_SUFFIX", "")
_SKIP_CELLS = frozenset(
    tok for tok in os.environ.get("IBEX_SKIP_CELLS", "").split(",") if tok
)


def should_skip(framework: str, query: str) -> bool:
    """True if this (framework, query) cell was cut at a smaller scale."""
    return f"{framework}{_FW_SUFFIX}|{query}" in _SKIP_CELLS


def cut_row(framework: str, query: str, peak_mb: float = 0.0):
    """A sentinel result row (avg_ms < 0) marking a cut/skipped cell. The suite
    records it into the carry-forward set and drops it from the combined output."""
    return (framework, query, "-1.000", "-1.000", "-1.000", "0.000", "-1.000",
            "-1.000", 0, f"{peak_mb:.1f}")


def reset_peak_rss() -> None:
    """Reset the kernel peak-RSS counter so the next read reflects only work
    done since this call. No-op where /proc is unavailable."""
    if not _SUPPORTED:
        return
    try:
        with open(_CLEAR_REFS, "w") as f:
            f.write("5\n")
    except OSError:
        pass


def peak_rss_mb() -> float:
    """Return VmHWM (peak RSS) in MiB, or 0.0 where unavailable."""
    if not _SUPPORTED:
        return 0.0
    try:
        with open(_STATUS, "r") as f:
            for line in f:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return 0.0
