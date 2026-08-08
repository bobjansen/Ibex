# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

import pandas as pd


def consecutive_numbers(logs: pd.DataFrame) -> pd.DataFrame:
    mask = (
        logs["num"].eq(logs["num"].shift(1)) &
        logs["num"].eq(logs["num"].shift(2))
    )
    return (
        logs.loc[mask, ["num"]]
        .drop_duplicates()
        .rename(columns={"num": "ConsecutiveNums"})
    )


if __name__ == "__main__":
    logs = pd.read_csv("examples/leetcode/data/logs.csv")
    print(consecutive_numbers(logs))
