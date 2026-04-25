import pandas as pd


def second_highest_salary(employee: pd.DataFrame) -> pd.DataFrame:
    s = employee["salary"].drop_duplicates().nlargest(2)
    return pd.DataFrame({
        "SecondHighestSalary": [s.iloc[-1] if len(s) == 2 else None]
    })


if __name__ == "__main__":
    employee = pd.read_csv("examples/leetcode/data/employee_second_highest.csv")
    print(second_highest_salary(employee))
