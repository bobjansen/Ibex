import pandas as pd


def rising_temperature(weather: pd.DataFrame) -> pd.DataFrame:
    w = weather.sort_values("recordDate")

    mask = (
        (w["recordDate"].diff().dt.days == 1)
        & (w["temperature"] > w["temperature"].shift(1))
    )

    return w.loc[mask, ["id"]].rename(columns={"id": "Id"})


if __name__ == "__main__":
    weather = pd.read_csv(
        "examples/leetcode/data/weather.csv",
        parse_dates=["recordDate"],
    )
    print(rising_temperature(weather))
