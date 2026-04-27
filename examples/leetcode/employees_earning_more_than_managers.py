import pandas as pd


def find_employees(employee: pd.DataFrame) -> pd.DataFrame:
    with_manager = pd.merge(
        employee,
        employee[["salary", "id"]],
        how="inner",
        left_on="managerId",
        right_on="id",
    )
    with_manager = with_manager.query("salary_x > salary_y")
    return with_manager.rename({"name": "Employee"}, axis=1)[["Employee"]]


if __name__ == "__main__":
    employee = pd.read_csv("examples/leetcode/data/employee_managers.csv")
    print(find_employees(employee))
