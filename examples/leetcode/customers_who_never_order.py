import pandas as pd


def find_customers(customers: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    ordered_customer_ids = orders["customerId"].drop_duplicates()
    return (
        customers[~customers["id"].isin(ordered_customer_ids)]
        .rename({"name": "Customers"}, axis=1)[["Customers"]]
    )


if __name__ == "__main__":
    customers = pd.read_csv("examples/leetcode/data/customers.csv")
    orders = pd.read_csv("examples/leetcode/data/orders.csv")
    print(find_customers(customers, orders))
