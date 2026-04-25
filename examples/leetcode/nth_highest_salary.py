import pandas as pd


def nth_highest_salary(employee: pd.DataFrame, N: int) -> pd.DataFrame:
    salary = employee["salary"].drop_duplicates().nlargest(N)
    ans = None
    if len(salary) == N and N != 0:
        ans = salary.iloc[-1]
    return pd.DataFrame({f"getNthHighestSalary({N})": [ans]})


if __name__ == "__main__":
    employee = pd.read_csv("examples/leetcode/data/employee_second_highest.csv")
    N = 3
    print(nth_highest_salary(employee, N))
