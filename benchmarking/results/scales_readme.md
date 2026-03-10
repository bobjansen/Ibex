## Benchmark

Scale benchmark snapshot on **4,000,000 rows** (warmup=1, iters=15).

| query                        |     ibex |   polars | polars-st |   duckdb | duckdb-st | datafusion | datafusion-st | clickhouse | clickhouse-st | data.table |
| ---------------------------- | -------: | -------: | --------: | -------: | --------: | ---------: | ------------: | ---------: | ------------: | ---------: |
| mean_by_symbol               |  26.3 ms |  29.9 ms |   61.1 ms |  8.86 ms |   41.0 ms |    4.58 ms |       24.2 ms |    8.05 ms |       49.7 ms |    23.2 ms |
| ohlc_by_symbol               |  32.1 ms |  31.5 ms |   72.1 ms |  11.1 ms |   59.8 ms |    8.95 ms |       60.3 ms |    7.36 ms |       54.7 ms |    25.7 ms |
| update_price_x2              |  3.38 ms |  2.70 ms |   3.49 ms | 115.0 ms |  152.2 ms |    4.54 ms |       5.77 ms |    2.98 ms |       4.48 ms |    18.7 ms |
| cumsum_price                 |  3.66 ms |  12.0 ms |   12.6 ms | 193.7 ms |  660.9 ms |   480.0 ms |      473.2 ms |    81.8 ms |       77.3 ms |    11.1 ms |
| cumprod_price                |  3.54 ms |  12.0 ms |   12.6 ms |        - |         - |          - |             - |          - |             - |   327.3 ms |
| rand_uniform                 |  3.40 ms |  8.09 ms |   8.56 ms | 116.7 ms |  176.7 ms |    5.59 ms |       13.8 ms |    3.55 ms |       4.32 ms |    25.8 ms |
| rand_normal                  |  23.5 ms |  30.0 ms |   30.4 ms |        - |         - |          - |             - |          - |             - |    75.9 ms |
| rand_int                     |  3.98 ms |  7.75 ms |   7.67 ms |        - |         - |          - |             - |          - |             - |    59.7 ms |
| rand_bernoulli               |  2.64 ms |  29.0 ms |   28.4 ms |        - |         - |          - |             - |          - |             - |    54.9 ms |
| fill_null                    |  4.12 ms |  3.09 ms |   2.72 ms |  13.7 ms |   27.4 ms |    7.39 ms |       28.6 ms |    4.46 ms |       4.79 ms |    5.40 ms |
| fill_forward                 |  4.53 ms |  8.60 ms |   8.50 ms |  57.2 ms |  208.7 ms |          - |             - |          - |             - |    5.33 ms |
| fill_backward                |  7.98 ms |  8.51 ms |   8.19 ms |  62.1 ms |  206.9 ms |          - |             - |          - |             - |    8.00 ms |
| null_left_join               |  72.4 ms |  29.8 ms |   78.7 ms | 180.7 ms |  295.2 ms |    16.8 ms |      108.5 ms |    20.4 ms |      142.6 ms |   155.4 ms |
| null_semi_join               |  34.6 ms |  19.1 ms |   94.5 ms |  52.4 ms |  109.9 ms |    12.4 ms |       83.4 ms |    16.6 ms |       57.9 ms |    44.1 ms |
| null_anti_join               |  34.3 ms |  22.3 ms |   93.1 ms |  54.7 ms |  144.5 ms |    12.6 ms |       83.2 ms |    16.9 ms |       56.1 ms |    69.5 ms |
| null_cross_join_small        |  2.48 ms | 0.578 ms |  0.613 ms |  7.11 ms |   7.66 ms |   0.771 ms |      0.790 ms |    1.50 ms |       1.82 ms |    3.53 ms |
| filter_simple                |  10.8 ms |  8.01 ms |   12.4 ms |  56.9 ms |   90.5 ms |    7.24 ms |       20.4 ms |    5.73 ms |       17.8 ms |    26.1 ms |
| filter_and                   |  6.87 ms |  5.05 ms |   7.92 ms |  13.6 ms |   39.6 ms |    3.87 ms |       11.5 ms |    8.05 ms |       27.1 ms |    33.1 ms |
| filter_arith                 |  14.8 ms |  8.33 ms |   16.4 ms |  77.1 ms |  110.3 ms |    8.42 ms |       25.8 ms |    8.49 ms |       22.6 ms |    33.1 ms |
| filter_or                    |  7.20 ms |  4.84 ms |   7.88 ms |  16.0 ms |   53.2 ms |    3.83 ms |       11.6 ms |    6.34 ms |       15.0 ms |    24.5 ms |
| count_by_symbol_day          |  7.42 ms |  51.5 ms |  105.1 ms |  9.15 ms |   43.4 ms |    6.68 ms |       42.3 ms |    12.7 ms |       49.5 ms |    21.6 ms |
| mean_by_symbol_day           |  8.58 ms |  56.5 ms |  135.0 ms |  9.18 ms |   46.0 ms |    6.74 ms |       44.4 ms |    15.1 ms |       54.0 ms |    21.2 ms |
| ohlc_by_symbol_day           |  14.2 ms |  59.1 ms |  150.5 ms |  11.8 ms |   65.6 ms |    18.1 ms |      144.1 ms |    16.2 ms |       56.8 ms |    25.2 ms |
| sum_by_user                  |  51.9 ms |  47.8 ms |  196.3 ms |  35.0 ms |   97.9 ms |    34.9 ms |       56.7 ms |    76.4 ms |       65.0 ms |    42.6 ms |
| filter_events                |  10.9 ms |  7.27 ms |   11.6 ms |  81.1 ms |  118.9 ms |    9.22 ms |       23.3 ms |    8.20 ms |       20.3 ms |    30.9 ms |
| melt_wide_to_long            |  49.9 ms |  55.5 ms |   65.4 ms | 983.5 ms | 1280.8 ms |    23.2 ms |       24.3 ms |    10.0 ms |       18.2 ms |   130.4 ms |
| dcast_long_to_wide           | 861.0 ms | 632.0 ms | 1530.4 ms | 321.2 ms | 1282.1 ms |   214.3 ms |      622.4 ms |   472.1 ms |     1400.3 ms |  1191.1 ms |
| dcast_long_to_wide_int_pivot | 805.2 ms |        - |         - |        - |         - |          - |             - |          - |             - |          - |
| dcast_long_to_wide_cat_pivot | 876.2 ms |        - |         - |        - |         - |          - |             - |          - |             - |          - |

_Generated by `benchmarking/run_scale_suite.sh --to-readme` from `benchmarking/results/scales.tsv`._
