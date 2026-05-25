## Benchmark

Scale benchmark snapshot on **1,000,000 rows** (requested 4,000,000; warmup=1, iters=3).

| query                        |     ibex |   polars | polars-st |   duckdb | duckdb-st | datafusion | datafusion-st | clickhouse | clickhouse-st |    sqlite |   pandas | data.table |    dplyr |
| ---------------------------- | -------: | -------: | --------: | -------: | --------: | ---------: | ------------: | ---------: | ------------: | --------: | -------: | ---------: | -------: |
| mean_by_symbol               |  9.02 ms |  13.3 ms |   16.3 ms |  5.99 ms |   11.0 ms |    3.68 ms |       8.72 ms |    11.1 ms |       20.9 ms |  360.9 ms |  16.3 ms |    8.67 ms |  19.7 ms |
| ohlc_by_symbol               |  8.34 ms |  14.1 ms |   17.6 ms |  7.23 ms |   15.6 ms |    7.00 ms |       19.8 ms |    11.0 ms |       22.2 ms |         - |  21.1 ms |    24.0 ms |  30.0 ms |
| update_price_x2              | 0.519 ms |  1.54 ms |   1.14 ms |  31.2 ms |   35.4 ms |    2.35 ms |       2.33 ms |    2.59 ms |       1.55 ms |  526.3 ms | 0.985 ms |    1.33 ms |  1.33 ms |
| distinct_symbol              |  7.38 ms |  13.1 ms |   13.4 ms |        - |         - |          - |             - |          - |             - |         - |  14.6 ms |          - |        - |
| order_head_topk              |  2.65 ms |  21.9 ms |   42.8 ms |        - |         - |          - |             - |          - |             - |         - | 104.7 ms |          - |        - |
| order_head_topk_by_symbol    |  33.7 ms |  36.3 ms |   56.5 ms |        - |         - |          - |             - |          - |             - |         - | 135.5 ms |          - |        - |
| order_tail_topk              |  2.48 ms |  25.5 ms |   44.5 ms |        - |         - |          - |             - |          - |             - |         - | 107.9 ms |          - |        - |
| order_tail_topk_by_symbol    |  34.0 ms |  30.1 ms |   51.4 ms |        - |         - |          - |             - |          - |             - |         - | 154.0 ms |          - |        - |
| cumsum_price                 | 0.505 ms |  3.28 ms |   3.40 ms |  59.8 ms |  161.8 ms |   130.7 ms |      128.4 ms |    24.4 ms |       21.4 ms | 1229.8 ms |  3.27 ms |    20.0 ms |  2.67 ms |
| cumprod_price                |  1.15 ms |  3.30 ms |   3.33 ms |        - |         - |          - |             - |          - |             - |         - |  2.92 ms |    84.7 ms |  83.0 ms |
| rand_uniform                 | 0.503 ms |  2.37 ms |   3.42 ms |  43.9 ms |   44.0 ms |    2.18 ms |       3.87 ms |    1.78 ms |       1.61 ms |         - |  2.44 ms |    5.67 ms |  6.00 ms |
| rand_normal                  |  2.83 ms |  7.24 ms |   7.66 ms |        - |         - |          - |             - |          - |             - |         - |  7.86 ms |    32.0 ms |  19.7 ms |
| rand_int                     |  1.71 ms |  1.84 ms |   2.44 ms |        - |         - |          - |             - |          - |             - |         - |  2.45 ms |    15.3 ms |  16.3 ms |
| rand_bernoulli               |  1.79 ms |  7.05 ms |   7.40 ms |        - |         - |          - |             - |          - |             - |         - |  9.33 ms |    15.7 ms |  15.0 ms |
| fill_null                    | 0.923 ms | 0.888 ms |  0.584 ms |  3.93 ms |   7.63 ms |    2.92 ms |       7.00 ms |    2.06 ms |       1.36 ms |  260.2 ms |  1.62 ms |    2.00 ms |  3.00 ms |
| fill_forward                 | 0.580 ms |  2.16 ms |   2.13 ms |  18.1 ms |   64.8 ms |          - |             - |          - |             - |         - |  1.89 ms |    2.00 ms |  4.67 ms |
| fill_backward                |  1.25 ms |  2.08 ms |   2.03 ms |  18.7 ms |   64.3 ms |          - |             - |          - |             - |         - |  1.61 ms |    2.33 ms |  4.33 ms |
| null_left_join               |  7.73 ms |  14.8 ms |   20.1 ms |  54.3 ms |   79.1 ms |    7.41 ms |       29.0 ms |    9.85 ms |       34.4 ms |  783.2 ms |  70.4 ms |    45.0 ms |  45.0 ms |
| null_semi_join               |  6.12 ms |  24.8 ms |   23.4 ms |  14.8 ms |   28.6 ms |    7.07 ms |       22.1 ms |    11.3 ms |       15.0 ms | 4554.3 ms |  56.0 ms |    10.3 ms |  26.0 ms |
| null_anti_join               |  5.58 ms |  12.0 ms |   23.4 ms |  16.0 ms |   39.3 ms |    6.91 ms |       21.3 ms |    11.0 ms |       15.0 ms | 4643.4 ms |  18.7 ms |    18.3 ms |  26.3 ms |
| null_cross_join_small        | 0.533 ms | 0.663 ms |  0.744 ms |  9.62 ms |   9.88 ms |    1.49 ms |      0.808 ms |    1.58 ms |       1.26 ms |   82.6 ms |  4.31 ms |    4.00 ms |  53.7 ms |
| filter_simple                |  1.97 ms |  2.92 ms |   3.67 ms |  16.3 ms |   23.8 ms |    3.58 ms |       5.77 ms |    3.89 ms |       5.30 ms |  294.2 ms |  5.39 ms |    8.00 ms |  7.67 ms |
| filter_and                   |  1.07 ms |  3.45 ms |   1.75 ms |  4.93 ms |   10.7 ms |    2.70 ms |       3.15 ms |    8.91 ms |       11.0 ms |   96.5 ms |  4.15 ms |    8.00 ms |  10.7 ms |
| filter_arith                 |  2.97 ms |  4.26 ms |   4.95 ms |  20.2 ms |   31.0 ms |    3.50 ms |       6.50 ms |    7.51 ms |       9.47 ms |  390.5 ms |  8.28 ms |    9.67 ms |  9.00 ms |
| filter_or                    |  1.09 ms |  1.48 ms |   1.65 ms |  5.16 ms |   15.6 ms |    2.04 ms |       3.08 ms |    8.06 ms |       7.88 ms |  107.1 ms |  3.40 ms |    9.33 ms |  8.33 ms |
| count_by_symbol_day          |  7.43 ms |  16.5 ms |   27.6 ms |  7.08 ms |   11.5 ms |    4.07 ms |       11.5 ms |    5.36 ms |       14.8 ms |  571.2 ms |  32.4 ms |    6.00 ms |  20.0 ms |
| mean_by_symbol_day           |  9.05 ms |  18.6 ms |   29.1 ms |  5.87 ms |   11.7 ms |    4.33 ms |       11.8 ms |    6.33 ms |       15.7 ms |  675.1 ms |  34.3 ms |    8.00 ms |  26.0 ms |
| ohlc_by_symbol_day           |  3.26 ms |  18.7 ms |   30.7 ms |  7.59 ms |   17.4 ms |    11.1 ms |       37.3 ms |    6.89 ms |       16.0 ms |         - |  39.6 ms |    11.7 ms |  58.3 ms |
| sum_by_user                  |  25.2 ms |  21.2 ms |   42.7 ms |  16.2 ms |   27.9 ms |    14.5 ms |       15.8 ms |    38.2 ms |       22.7 ms |  534.8 ms |  33.9 ms |    36.7 ms | 303.0 ms |
| filter_events                |  2.07 ms |  3.75 ms |   3.09 ms |  22.3 ms |   26.9 ms |    3.98 ms |       5.86 ms |    3.94 ms |       5.23 ms |  304.2 ms |  5.62 ms |    10.0 ms |  8.33 ms |
| melt_wide_to_long            |  12.7 ms |  24.1 ms |   35.9 ms | 269.8 ms |  311.4 ms |    8.07 ms |       7.99 ms |    5.18 ms |       6.87 ms | 2845.9 ms |  40.6 ms |    78.0 ms |  72.0 ms |
| dcast_long_to_wide           | 250.3 ms | 156.9 ms |  308.4 ms | 107.3 ms |  286.6 ms |    69.1 ms |      137.9 ms |   125.7 ms |      318.6 ms | 3958.7 ms | 774.1 ms |   276.0 ms | 318.3 ms |
| dcast_long_to_wide_int_pivot | 253.0 ms |        - |         - |        - |         - |          - |             - |          - |             - |         - |        - |          - |        - |
| dcast_long_to_wide_cat_pivot | 248.9 ms |        - |         - |        - |         - |          - |             - |          - |             - |         - |        - |          - |        - |

_Generated by `benchmarking/run_scale_suite.sh --to-readme` from `benchmarking/results/scales.tsv`._
