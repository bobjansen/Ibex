## Benchmark

Scale benchmark snapshot on **4,000,000 rows** (warmup=2, iters=15).

| query                        |     ibex |   polars | polars-st |    duckdb | duckdb-st | datafusion | datafusion-st | clickhouse | clickhouse-st |     sqlite |    pandas | data.table |     dplyr |
| ---------------------------- | -------: | -------: | --------: | --------: | --------: | ---------: | ------------: | ---------: | ------------: | ---------: | --------: | ---------: | --------: |
| mean_by_symbol               |  25.3 ms |  28.0 ms |   51.7 ms |   8.94 ms |   40.4 ms |    4.54 ms |       25.2 ms |    9.31 ms |       51.8 ms |  1603.2 ms |   57.7 ms |    23.1 ms |   43.6 ms |
| ohlc_by_symbol               |  30.4 ms |  33.3 ms |   76.1 ms |   11.3 ms |   56.8 ms |    8.87 ms |       62.5 ms |    7.32 ms |       53.4 ms |          - |   74.3 ms |    22.9 ms |   54.5 ms |
| update_price_x2              |  3.08 ms |  3.25 ms |   2.85 ms |  114.2 ms |  143.4 ms |    4.27 ms |       5.53 ms |    2.76 ms |       4.12 ms |  2004.6 ms |   2.88 ms |    13.7 ms |   5.13 ms |
| cumsum_price                 |  2.74 ms |  13.0 ms |   12.3 ms |  189.4 ms |  678.3 ms |   494.0 ms |      477.0 ms |    78.3 ms |       83.0 ms |  4841.2 ms |   11.0 ms |    13.2 ms |   8.27 ms |
| cumprod_price                |  3.57 ms |  12.7 ms |   12.3 ms |         - |         - |          - |             - |          - |             - |          - |   11.3 ms |   331.2 ms |  331.1 ms |
| rand_uniform                 |  3.37 ms |  7.60 ms |   7.84 ms |  112.9 ms |  175.7 ms |    5.03 ms |       13.2 ms |    3.10 ms |       4.54 ms |          - |   8.89 ms |    27.7 ms |   22.8 ms |
| rand_normal                  |  23.1 ms |  29.5 ms |   28.8 ms |         - |         - |          - |             - |          - |             - |          - |   31.3 ms |    91.3 ms |   71.9 ms |
| rand_int                     |  3.84 ms |  7.95 ms |   7.33 ms |         - |         - |          - |             - |          - |             - |          - |   9.27 ms |    59.1 ms |   60.3 ms |
| rand_bernoulli               |  2.61 ms |  29.3 ms |   28.5 ms |         - |         - |          - |             - |          - |             - |          - |   30.2 ms |    56.7 ms |   55.5 ms |
| fill_null                    |  3.77 ms |  3.46 ms |   2.61 ms |   13.8 ms |   27.0 ms |    7.93 ms |       26.5 ms |    4.97 ms |       5.65 ms |  1075.9 ms |   7.15 ms |    5.60 ms |   12.9 ms |
| fill_forward                 |  3.52 ms |  8.83 ms |   8.46 ms |   55.7 ms |  216.1 ms |          - |             - |          - |             - |          - |   7.88 ms |    15.3 ms |   10.5 ms |
| fill_backward                |  6.76 ms |  8.18 ms |   8.38 ms |   62.5 ms |  215.7 ms |          - |             - |          - |             - |          - |   7.41 ms |    5.00 ms |   10.9 ms |
| null_left_join               |  73.1 ms |  29.8 ms |   80.5 ms |  186.1 ms |  288.0 ms |    16.3 ms |      104.7 ms |    23.9 ms |      143.7 ms |  2996.1 ms |  308.5 ms |   161.8 ms |  177.1 ms |
| null_semi_join               |  36.7 ms |  25.5 ms |   97.5 ms |   54.9 ms |  106.0 ms |    12.9 ms |       81.3 ms |    19.5 ms |       62.5 ms | 17989.6 ms |  236.6 ms |    40.3 ms |   85.5 ms |
| null_anti_join               |  35.9 ms |  23.7 ms |   94.0 ms |   55.0 ms |  144.1 ms |    12.7 ms |       80.9 ms |    17.9 ms |       57.6 ms | 17795.6 ms |   66.6 ms |    69.7 ms |   97.6 ms |
| null_cross_join_small        |  2.59 ms | 0.597 ms |  0.599 ms |   8.01 ms |   7.22 ms |   0.739 ms |      0.828 ms |    1.63 ms |       2.90 ms |    75.4 ms |   4.02 ms |    17.2 ms |   51.6 ms |
| filter_simple                |  10.8 ms |  7.43 ms |   11.1 ms |   56.6 ms |   90.0 ms |    6.82 ms |       18.1 ms |    5.82 ms |       20.9 ms |  1145.4 ms |   21.7 ms |    30.4 ms |   39.1 ms |
| filter_and                   |  6.67 ms |  4.75 ms |   7.27 ms |   13.3 ms |   40.7 ms |    3.52 ms |       10.5 ms |    8.35 ms |       26.8 ms |   387.5 ms |   17.1 ms |    29.9 ms |   39.4 ms |
| filter_arith                 |  14.5 ms |  8.59 ms |   15.0 ms |   75.1 ms |  114.5 ms |    8.21 ms |       24.1 ms |    8.58 ms |       22.1 ms |  1503.2 ms |   31.1 ms |    32.6 ms |   29.2 ms |
| filter_or                    |  6.95 ms |  5.72 ms |   7.96 ms |   15.9 ms |   54.5 ms |    3.85 ms |       11.0 ms |    6.26 ms |       15.5 ms |   443.5 ms |   14.0 ms |    25.1 ms |   31.5 ms |
| count_by_symbol_day          |  7.82 ms |  52.7 ms |  102.6 ms |   8.83 ms |   45.3 ms |    6.27 ms |       41.2 ms |    12.9 ms |       50.1 ms |  2601.8 ms |  125.3 ms |    21.6 ms |   85.7 ms |
| mean_by_symbol_day           |  8.71 ms |  56.7 ms |  136.6 ms |   9.53 ms |   45.0 ms |    6.48 ms |       41.8 ms |    15.8 ms |       55.8 ms |  2913.9 ms |  129.1 ms |    22.4 ms |  106.9 ms |
| ohlc_by_symbol_day           |  14.0 ms |  67.0 ms |  148.5 ms |   12.7 ms |   68.6 ms |    17.4 ms |      144.6 ms |    16.5 ms |       60.9 ms |          - |  145.9 ms |    27.0 ms |  124.1 ms |
| sum_by_user                  |  51.3 ms |  48.6 ms |  223.6 ms |   33.9 ms |   92.7 ms |    34.1 ms |       56.9 ms |    79.4 ms |       76.5 ms |  2058.6 ms |  116.9 ms |    44.0 ms |  317.5 ms |
| filter_events                |  11.2 ms |  7.01 ms |   11.9 ms |   74.0 ms |  109.5 ms |    8.98 ms |       23.1 ms |    9.42 ms |       19.4 ms |  1160.5 ms |   22.9 ms |    31.1 ms |   31.9 ms |
| melt_wide_to_long            |  54.2 ms |  40.2 ms |   46.0 ms | 1123.1 ms | 1483.0 ms |    22.5 ms |       23.7 ms |    10.2 ms |       19.5 ms | 10534.0 ms |  194.8 ms |   191.3 ms |  262.1 ms |
| dcast_long_to_wide           | 816.6 ms | 639.6 ms | 1557.9 ms |  314.4 ms | 1285.1 ms |   208.3 ms |      584.9 ms |   502.1 ms |     1808.8 ms | 16206.8 ms | 4151.7 ms |  1213.8 ms | 2131.5 ms |
| dcast_long_to_wide_int_pivot | 802.9 ms |        - |         - |         - |         - |          - |             - |          - |             - |          - |         - |          - |         - |
| dcast_long_to_wide_cat_pivot | 836.7 ms |        - |         - |         - |         - |          - |             - |          - |             - |          - |         - |          - |         - |

_Generated by `benchmarking/run_scale_suite.sh --to-readme` from `benchmarking/results/scales.tsv`._
