import pandas as pd


def top_three_salaries(employee: pd.DataFrame, department: pd.DataFrame) -> pd.DataFrame:
    return (
        employee
        .merge(department, left_on="departmentId", right_on="id")
        .loc[
            lambda df: df.groupby("departmentId")["salary"]
            .rank(method="dense", ascending=False) <= 3
        ]
        .rename(
            {"name_y": "Department", "name_x": "Employee", "salary": "Salary"},
            axis=1,
        )[["Department", "Employee", "Salary"]]
        .sort_values(["Department", "Salary", "Employee"], ascending=[True, False, True])
        .reset_index(drop=True)
    )


if __name__ == "__main__":
    employee = pd.read_csv("examples/leetcode/data/employee.csv")
    department = pd.read_csv("examples/leetcode/data/department.csv")
    print(top_three_salaries(employee, department))
