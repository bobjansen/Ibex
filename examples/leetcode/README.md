## LeetCode-style table problems

This directory collects small side-by-side translations from pandas-style
solutions into Ibex.

The intended pattern is:
- `problem_name.py` for the pandas version
- `problem_name.ibex` for the Ibex version
- `data/` for tiny CSV fixtures matching the problem statement tables

Current examples:
- `department_top_three_salaries`
- `second_highest_salary`
- `nth_highest_salary`
- `consecutive_numbers`
- `employees_earning_more_than_managers`
- `customers_who_never_order`
- `rising_temperature`

Run an example from the repository root with:

```bash
uv run examples/leetcode/department_top_three_salaries.py
./build-release/tools/ibex :load examples/leetcode/department_top_three_salaries.ibex

uv run examples/leetcode/second_highest_salary.py
./build-release/tools/ibex :load examples/leetcode/second_highest_salary.ibex

uv run examples/leetcode/nth_highest_salary.py
./build-release/tools/ibex :load examples/leetcode/nth_highest_salary.ibex

uv run examples/leetcode/consecutive_numbers.py
./build-release/tools/ibex :load examples/leetcode/consecutive_numbers.ibex

uv run examples/leetcode/employees_earning_more_than_managers.py
./build-release/tools/ibex :load examples/leetcode/employees_earning_more_than_managers.ibex

uv run examples/leetcode/customers_who_never_order.py
./build-release/tools/ibex :load examples/leetcode/customers_who_never_order.ibex

uv run examples/leetcode/rising_temperature.py
./build-release/tools/ibex :load examples/leetcode/rising_temperature.ibex
```

The LeetCode-style translation pattern in this first example is:
- grouped dense-rank/filter in pandas
- grouped `rank(..., method = dense, ascending = false)` plus `filter` in Ibex

The second example shows another recurring pattern:
- `drop_duplicates() + nlargest(k)` in pandas
- `distinct + order + head` in Ibex
- wrapped in a reusable `fn ... (df: DataFrame<{salary: Int64}>) -> DataFrame`
- plus a slightly clunky one-row-null fallback shape when the result is missing

The third example generalizes the same pattern:
- parameterized by `n`
- reusable `top_n_salaries(...)` and `nth_highest_salary(...)` helpers
- stable output column name in Ibex instead of a dynamic column label

The consecutive-numbers example shows a shift-style pattern:
- two pandas `shift(...)` calls
- direct `lag(...)` calls in an Ibex `filter`

The employee-manager example shows a compact self-join pattern:
- pandas self-merge against `employee[["salary", "id"]]`
- Ibex `select` to shape the manager lookup, then `join` and `filter`

The never-ordered-customers example shows an anti-join pattern:
- pandas `~isin(...)`
- Ibex `anti join` against a shaped key lookup

The rising-temperature example shows an ordered lag pattern over dates:
- pandas `diff().dt.days` plus `shift(1)`
- Ibex `order`, direct `lag(...)` in `filter`, and `Date - Date` day deltas
- the pandas side uses a CSV fixture; the Ibex side uses `Table { ... }` because CSV date parsing is not supported yet

The Ibex versions now also use named CSV arguments where that improves
readability, for example:
- `read_csv("...", schema = "...")`
