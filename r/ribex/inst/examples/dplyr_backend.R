# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

library(dplyr)
library(ribex)

trades <- tibble(
    symbol = c("AAPL", "MSFT", "AAPL"),
    price = c(101.5, 299.0, 102.0),
    size = c(10L, 5L, 20L)
)

query <- ibex_tbl(trades, fallback = "error") |>
    filter(price > 100) |>
    mutate(notional = price * size) |>
    group_by(symbol) |>
    summarise(total = sum(notional), .groups = "drop") |>
    arrange(desc(total))

show_query(query)
collect(query)
