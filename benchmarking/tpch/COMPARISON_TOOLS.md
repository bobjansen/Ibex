# Benchmark Comparison Tools

This directory contains two utility scripts for comparing Ibex and Polars benchmark results.

## Scripts

### 1. `compare_polars_detailed.py`

Detailed query-by-query benchmark comparison with statistics.

**Features:**
- Query-by-query performance comparison with formatted symbols (✓/✗/≈)
- Geometric mean calculation (correct for performance ratios)
- Win/loss/tie statistics
- Optional comparison against upstream PDS Polars
- Works with any benchmark result files

**Usage:**
```bash
# Basic comparison (uses default filenames)
python3 compare_polars_detailed.py

# Custom files with title
python3 compare_polars_detailed.py \
  --ibex results/ibex_sf2.tsv \
  --polars results/polars_sf2.tsv \
  --title "My Test"

# Include PDS Polars for trend analysis
python3 compare_polars_detailed.py \
  --ibex results/ibex_sf2.tsv \
  --polars results/polars_sf2.tsv \
  --pdsh results/pdsh_polars_sf2.tsv
```

**Output Example:**
```
Query    Ibex (ms)      Polars (ms)    Ibex/Polars      vs PDS
===============================================================================
q01           137.8           132.9      ≈ 1.0x           1.08x↓
q02            50.3            56.4      ✓ 1.12x faster   ≈
q03            73.0            47.9      ✗ 1.52x slower   1.32x↓
...

Results:        7 wins, 3 ties, 12 losses
Geometric mean: Ibex is 1.02x SLOWER than Polars
```

### 2. `compare_runs.sh`

Compare two archived benchmark runs side-by-side with improvement analysis.

**Features:**
- Automatic run detection and metadata display
- Shows before/after results using `compare_polars_detailed.py`
- Calculates improvement metrics (geomean, win count)
- Highlights top 5 biggest changes
- Works with any archived runs

**Usage:**
```bash
# Compare two SF-2 runs
./compare_runs.sh results/runs/run1_sf2 results/runs/run2_sf2

# Compare SF-4 runs
./compare_runs.sh results/runs/run1_sf4 results/runs/run2_sf4 --scale 4

# By timestamp (find runs with `ls results/runs/ | grep sf2`)
./compare_runs.sh \
  results/runs/20260814T125929Z_4e44f019_sf2 \
  results/runs/20260818T185136Z_4616c063_sf2
```

**Output Example:**
```
================================================================================
BENCHMARK RUN COMPARISON
================================================================================

BEFORE: 20260814T125929Z_4e44f019_sf2
        Commit: 4e44f019
        Cores:  8

AFTER:  20260818T185136Z_4616c063_sf2
        Commit: 4616c063
        Cores:  8

================================================================================
IMPROVEMENT ANALYSIS
================================================================================

Geometric Mean Performance (Ibex vs Polars):
  Before: 1.38x slower
  After:  1.02x slower
  Change: +25.7% ✓ IMPROVEMENT

Query Wins:
  Before: 4/22 wins
  After:  7/22 wins
  Change: +3

Top Changes:
  q04: 2.84x → 1.07x (2.66x improvement ✓)
  q05: 1.91x → 0.73x (2.61x improvement ✓)
  ...
```

## Common Workflows

### 1. After running a benchmark, compare results
```bash
cd ~/ibex/benchmarking/tpch

# Show current results (default files)
python3 compare_polars_detailed.py --title "Current SF-2 Results"

# Include upstream PDS for context
python3 compare_polars_detailed.py \
  --pdsh results/pdsh_polars_sf2.tsv \
  --title "SF-2 vs Upstream Polars"
```

### 2. Compare before/after a change
```bash
cd ~/ibex/benchmarking/tpch

# Quick before/after from archived runs
./compare_runs.sh results/runs/BEFORE_RUN results/runs/AFTER_RUN
```

### 3. Monitor improvements across multiple runs
```bash
cd ~/ibex/benchmarking/tpch

# Loop through all recent SF-2 runs
for run in results/runs/*/; do
  if [[ -f "$run/ibex_sf2.tsv" ]]; then
    echo "=== $(basename "$run") ==="
    python3 compare_polars_detailed.py \
      --ibex "$run/ibex_sf2.tsv" \
      --polars "$run/polars_sf2.tsv" \
      --title "$(basename "$run")" | tail -5
  fi
done
```

### 4. Compare different scale factors
```bash
cd ~/ibex/benchmarking/tpch

# SF-1 comparison
python3 compare_polars_detailed.py \
  --ibex results/ibex_sf1.tsv \
  --polars results/polars_sf1.tsv \
  --title "SF-1 Comparison"

# SF-4 comparison
python3 compare_polars_detailed.py \
  --ibex results/ibex_sf4.tsv \
  --polars results/polars_sf4.tsv \
  --title "SF-4 Comparison"
```

## Understanding the Output

### Symbols
- **✓** = Ibex is significantly faster (< 0.95x)
- **✗** = Ibex is significantly slower (> 1.05x)
- **≈** = Essentially tied (within 5%)

### Trends (vs PDS column)
- **1.50x↓** = Ibex performance decreased relative to PDS Polars by 1.5x
- **1.50x↑** = Ibex performance improved relative to PDS Polars by 1.5x
- **≈** = No significant change vs PDS

### Metrics
- **Geometric mean** = correct for performance ratios (use this for overall trend)
- **Arithmetic mean** = sum of all ratios (less useful but shown for reference)
- **Wins/Ties/Losses** = number of queries Ibex is ahead/at-parity/behind

## Tips

1. **Noise in results:** At SF-2 scale, stddev can be high (~5-10%). Larger scale factors (SF-4/SF-5) give clearer trends.

2. **Pinning cores:** The `run_bench.sh` script pins to 8 cores by default for fair comparison:
   ```bash
   ./run_bench.sh --sf 2 --cores 8
   ```

3. **Archived runs:** All benchmark runs are archived in `results/runs/` with manifests showing:
   - Commit hash and branch
   - Whether working tree was clean
   - Core count and scale factor
   - Warmup/iteration counts

4. **Comparing commits:** Use the commit hash from manifests to understand what changed:
   ```bash
   git log --oneline <commit1>..<commit2>
   ```

### 3. `show_history.py` / `append_history.py`

A real time series, not just a pair of archived runs. `results/*.tsv` (what the two
tools above read by default) get overwritten every `run_bench.sh` call, and
`results/runs/<utc>_<commit>_sf<N>/` only supports comparing two runs you pick by
hand (`compare_runs.py old new`). `results/history.tsv` is the long-format,
append-only file that accumulates every archived run — `run_bench.sh` appends to
it automatically now; `append_history.py --backfill` catches up anything archived
before that was wired in.

**Usage:**
```bash
# One query's trend across every archived run, in commit order
python3 show_history.py --framework ibex --query q14 --sf 1 --cores 8

# Every query's LATEST run at a glance
python3 show_history.py --framework ibex --sf 1
```

Pass `--cores` as well as `--sf` if a run matrix-swept core counts under one
timestamp (`sf-cores-matrix`-style runs) — otherwise the trend interleaves
different core counts and looks noisier than it is. Directory names mark a
dirty (uncommitted-tree) run with a `-dirty` suffix on the commit — e.g.
`f9969244-dirty` — visible in `ls results/runs/` directly, not just inside
`manifest.json`, because two runs sharing the same nominal commit label but
one dirty is exactly the kind of thing that makes a real change look like it
"didn't happen" on a quick glance.

## Implementation Details

Both scripts properly handle:
- TSV file format from `run_bench.sh`
- Geometric mean calculation (ln-scale averaging)
- Missing files with clear error messages
- Query ordering (q01-q22 in numerical order)
- Archived run directory structures
