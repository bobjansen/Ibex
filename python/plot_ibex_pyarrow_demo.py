from __future__ import annotations

import argparse
import os
import pathlib
import sys

os.environ.setdefault("MPLCONFIGDIR", "/tmp/ibex-matplotlib")

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd



def add_bridge_module_path(repo_root: pathlib.Path) -> None:
    for build_dir_name in ("build-release", "build"):
        candidate = repo_root / build_dir_name / "python"
        if candidate.is_dir():
            sys.path.insert(0, str(candidate))
            return
    raise RuntimeError("could not find a built ibex_pyarrow module under build-release/python or build/python")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot an Ibex result via pyarrow -> pandas -> matplotlib.")
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path("bench_results/ibex_pyarrow_matplotlib_demo.png"),
        help="output PNG path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    script_path = pathlib.Path(__file__).with_suffix(".ibex")
    add_bridge_module_path(repo_root)

    import ibex_pyarrow

    result = ibex_pyarrow.eval_file(str(script_path))

    df: pd.DataFrame = result.to_pandas()
    print("aggregated result:")
    print(df.to_string(index=False))

    fig, axes = plt.subplots(2, 1, figsize=(9, 7), sharex=True)

    df.plot(x="bucket", y="avg_spread_bps", marker="o", linewidth=2.0, color="#b33939", ax=axes[0], legend=False)
    axes[0].set_title("Ibex -> pyarrow -> pandas -> matplotlib")
    axes[0].set_ylabel("Avg spread (bps)")
    axes[0].grid(True, alpha=0.3)

    depth_ax = axes[1]
    df.plot(x="bucket", y="avg_depth", marker="o", linewidth=2.0, color="#227093", ax=depth_ax, label="avg_depth")
    imbalance_ax = depth_ax.twinx()
    df.plot(
        x="bucket",
        y="avg_book_imbalance",
        marker="s",
        linewidth=1.8,
        color="#218c74",
        ax=imbalance_ax,
        label="avg_book_imbalance",
    )
    depth_ax.set_ylabel("Avg depth")
    imbalance_ax.set_ylabel("Avg imbalance")
    depth_ax.set_xlabel("Bucket")
    depth_ax.grid(True, alpha=0.3)

    depth_handles, depth_labels = depth_ax.get_legend_handles_labels()
    imbalance_handles, imbalance_labels = imbalance_ax.get_legend_handles_labels()
    depth_ax.legend(depth_handles + imbalance_handles, depth_labels + imbalance_labels, loc="upper left")

    fig.tight_layout()

    out_path = args.out
    if not out_path.is_absolute():
        out_path = repo_root / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160, bbox_inches="tight")
    print(f"\nplot written to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
