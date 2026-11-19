# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

"""Paired timings of the cross-chunk fixture; preparation and checks are excluded."""
import json
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ab_queries import wilcoxon_p

base, target = sys.argv[1:3]
results = []
for repeat in range(12):
    pair = {}
    for side in (["base", "target"] if repeat % 2 == 0 else ["target", "base"]):
        binary = base if side == "base" else target
        p = subprocess.run(
            ["taskset", "-c", "0,2,4,6,8,10,12,14", binary, "[categorical-growth]"],
            env=dict(os.environ, IBEX_GROWTH_BENCH="1"), capture_output=True, text=True,
            check=True,
        )
        pair[side] = {f"{keys}keys-{cores}cores": float(ms) for keys, cores, ms in
                      re.findall(r"growth keys=(\d+) cores=(\d+) ms=([\d.]+)", p.stderr)}
        if len(pair[side]) != 4:
            raise RuntimeError(p.stdout + p.stderr)
    results.append(pair)
output_dir = Path(__file__).with_name("results")
output_dir.mkdir(exist_ok=True)
(output_dir / "growth-pairs.json").write_text(json.dumps(results, indent=2))
for key in results[0]["base"]:
    before = [p["base"][key] for p in results]
    after = [p["target"][key] for p in results]
    ratios = [b / a for a, b in zip(before, after)]
    print(key, "base_ms", statistics.median(before), "target_ms", statistics.median(after),
          "paired_ratio", statistics.median(ratios), "wins", sum(r < 1 for r in ratios),
          "wilcoxon_p", wilcoxon_p([b - a for a, b in zip(before, after)]), flush=True)
