"""Shared matplotlib style + CSV loading helpers for the bench notebooks."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

RESULTS_DIR = Path(__file__).resolve().parent.parent / "results"

plt.rcParams.update({
    "figure.figsize": (8, 5),
    "figure.dpi": 110,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})


def load(csv_name: str) -> pd.DataFrame:
    path = RESULTS_DIR / csv_name
    if not path.exists():
        raise FileNotFoundError(f"{path} not found - run the matching bench/*.sh script first")
    return pd.read_csv(path, sep="\t")


def agg(df: pd.DataFrame, group_cols: list, value_col: str) -> pd.DataFrame:
    """Collapse repeated-run rows into mean/std/n per group, for error bars.

    Groups with a single run get std=0 rather than NaN, so a chart can
    still plot an (invisible) error bar for it without special-casing.
    """
    out = df.groupby(group_cols)[value_col].agg(["mean", "std", "count"]).reset_index()
    out["std"] = out["std"].fillna(0.0)
    return out
