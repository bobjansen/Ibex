# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen

library(ibex)
library(ggplot2)

df <- eval_ibex('Table { x = [1, 2, 3, 4], y = [1.0, 4.0, 9.0, 16.0] };')
print(df)

p <- ggplot(df, aes(x, y)) +
    geom_line() +
    geom_point()

print(p)
