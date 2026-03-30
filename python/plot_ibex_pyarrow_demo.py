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
    add_bridge_module_path(repo_root)

    import ibex_pyarrow

    result = ibex_pyarrow.eval_table(
        """
        Table {
            stock = [
                "ALPHA", "ALPHA", "ALPHA", "ALPHA", "ALPHA", "ALPHA",
                "BETA",  "BETA",  "BETA",  "BETA",  "BETA",  "BETA"
            ],
            bucket = [0, 10, 20, 30, 40, 50, 0, 10, 20, 30, 40, 50],
            bid = [
                100.00, 100.08, 100.18, 100.31, 100.45, 100.60,
                49.90, 49.98, 50.07, 50.17, 50.28, 50.40
            ],
            ask = [
                100.14, 100.19, 100.27, 100.38, 100.50, 100.64,
                50.06, 50.11, 50.17, 50.25, 50.33, 50.44
            ],
            bid_sz = [4800.0, 5100.0, 5450.0, 5850.0, 6200.0, 6600.0, 3000.0, 3250.0, 3480.0, 3660.0, 3910.0, 4200.0],
            ask_sz = [5200.0, 5500.0, 5800.0, 6100.0, 6500.0, 6900.0, 3200.0, 3410.0, 3590.0, 3840.0, 4020.0, 4310.0]
        }[update {
            mid = (bid + ask) / 2.0,
            spread_bps = ((ask - bid) / ((bid + ask) / 2.0)) * 10000.0,
            depth = bid_sz + ask_sz,
            book_imbalance = (bid_sz - ask_sz) / (bid_sz + ask_sz)
        }][
            select {
                bucket,
                avg_spread_bps = mean(spread_bps),
                avg_depth = mean(depth),
                avg_book_imbalance = mean(book_imbalance)
            },
            by bucket,
            order bucket
        ];
        """
    )

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
