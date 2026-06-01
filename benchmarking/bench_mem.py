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
# iterations and drops the row, so one pathologically slow op (common at the
# largest scales) can't dominate the run's wall-clock. Override via env.
CELL_CUTOFF_MS = float(os.environ.get("IBEX_CELL_CUTOFF_MS", "120000"))  # 2 min


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
