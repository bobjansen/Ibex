# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

# Run supported dplyr pipelines with Ibex's native execution backend.
library(dplyr)
library(ribex)

trades <- tibble(
    symbol = c("AAPL", "MSFT", "AAPL", "GOOG", "MSFT"),
    price = c(101.5, 299.0, 102.0, 175.5, 301.0),
    size = c(10L, 5L, 20L, 8L, 4L)
)

# Length-one R values are captured as typed Ibex scalar bindings. They are not
# pasted into the generated source.
minimum_price <- 175

# ibex_tbl() binds the input table once. The dplyr verbs below only extend an
# immutable lazy plan; execution starts when collect() is called.
query <- ibex_tbl(trades, fallback = "error") |>
    filter(price >= minimum_price) |>
    mutate(notional = price * size) |>
    group_by(symbol) |>
    summarise(
        trade_count = n(),
        total_notional = sum(notional),
        average_price = mean(price),
        .groups = "drop"
    ) |>
    arrange(desc(total_notional))

# Inspect the translated Ibex without exposing the captured scalar value.
show_query(query)

# Execute the plan in Ibex and return the result as a tibble.
result <- collect(query)
print(result)
